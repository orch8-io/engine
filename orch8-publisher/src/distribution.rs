//! Governed package distribution primitives.
//!
//! These types deliberately stop short of becoming a general package manager.
//! They select already-signed immutable packages, pin their exact inputs, and
//! preserve enough evidence for an operator to reproduce every promotion.

use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use orch8_types::continuity::{CapsuleRequirements, RuntimeCapabilities, RuntimeId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest::canonical_json;
use crate::package::SignedPackage;
use crate::registry::{
    PackageRegistryPublisher, RegistryIndex, RegistryVersion, TransparencyLedger,
};

const MAX_CHANNEL_RELEASES: usize = 1_000;
const MAX_DELTA_FILES: usize = 10_000;
const MAX_DELTA_BYTES: usize = 64 * 1024 * 1024;
const MAX_ATTESTATIONS: usize = 64;
const MAX_LOCK_ENTRIES: usize = 4_096;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DistributionError {
    #[error("invalid distribution input: {0}")]
    Invalid(String),
    #[error("runtime does not satisfy release requirements: {0}")]
    Incompatible(String),
    #[error("access denied for principal {0}")]
    AccessDenied(String),
    #[error("content hash mismatch")]
    HashMismatch,
    #[error("requested release is not present in channel history")]
    UnknownRelease,
}

/// Named rollout lane. Custom names are intentionally unsupported so clients
/// cannot accidentally create an ungoverned production lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChannelName {
    Stable,
    Beta,
    Canary,
}

/// Immutable package reference selected by a channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelRelease {
    pub package_name: String,
    pub version: String,
    pub content_hash: String,
    pub package_url: String,
    #[serde(default)]
    pub requirements: CapsuleRequirements,
    /// Optional placement result binding. If present, only that runtime may
    /// consume this release.
    pub selected_runtime_id: Option<RuntimeId>,
    pub promoted_at: DateTime<Utc>,
}

/// Append-only channel history. Rollback changes the head to an earlier
/// immutable release; it never mutates or republishes package bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseChannel {
    pub schema_version: u32,
    pub tenant_id: String,
    pub name: ReleaseChannelName,
    pub releases: Vec<ChannelRelease>,
    pub head: Option<usize>,
}

/// Signed channel document consumed by remote/mobile runtimes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedReleaseChannel {
    pub channel: ReleaseChannel,
    pub key_id: String,
    pub public_key: String,
    pub signature: String,
}

impl SignedReleaseChannel {
    pub fn sign(
        channel: ReleaseChannel,
        key_id: impl Into<String>,
        signing_key: &SigningKey,
    ) -> Result<Self, DistributionError> {
        let key_id = key_id.into();
        if key_id.is_empty() || key_id.len() > 128 {
            return Err(DistributionError::Invalid(
                "channel key id must contain 1-128 bytes".into(),
            ));
        }
        let canonical = canonical_json(&channel)
            .map_err(|error| DistributionError::Invalid(error.to_string()))?;
        Ok(Self {
            channel,
            key_id,
            public_key: BASE64.encode(signing_key.verifying_key().to_bytes()),
            signature: BASE64.encode(signing_key.sign(canonical.as_bytes()).to_bytes()),
        })
    }

    pub fn verify(&self, trusted_public_keys: &[String]) -> Result<(), DistributionError> {
        if !trusted_public_keys.contains(&self.public_key) {
            return Err(DistributionError::AccessDenied(self.key_id.clone()));
        }
        let key_bytes: [u8; 32] = BASE64
            .decode(&self.public_key)
            .map_err(|_| DistributionError::Invalid("invalid channel public key".into()))?
            .try_into()
            .map_err(|_| {
                DistributionError::Invalid("channel public key must be 32 bytes".into())
            })?;
        let signature_bytes: [u8; 64] = BASE64
            .decode(&self.signature)
            .map_err(|_| DistributionError::Invalid("invalid channel signature".into()))?
            .try_into()
            .map_err(|_| DistributionError::Invalid("channel signature must be 64 bytes".into()))?;
        let key = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|error| DistributionError::Invalid(error.to_string()))?;
        let canonical = canonical_json(&self.channel)
            .map_err(|error| DistributionError::Invalid(error.to_string()))?;
        key.verify(
            canonical.as_bytes(),
            &Signature::from_bytes(&signature_bytes),
        )
        .map_err(|_| DistributionError::HashMismatch)
    }
}

impl ReleaseChannel {
    #[must_use]
    pub fn new(tenant_id: impl Into<String>, name: ReleaseChannelName) -> Self {
        Self {
            schema_version: 1,
            tenant_id: tenant_id.into(),
            name,
            releases: Vec::new(),
            head: None,
        }
    }

    pub fn promote(&mut self, release: ChannelRelease) -> Result<(), DistributionError> {
        validate_hash(&release.content_hash)?;
        if release.package_name.is_empty() || release.package_url.is_empty() {
            return Err(DistributionError::Invalid(
                "package name and URL are required".into(),
            ));
        }
        if self.releases.len() >= MAX_CHANNEL_RELEASES {
            return Err(DistributionError::Invalid(
                "channel history exceeds 1000 releases".into(),
            ));
        }
        if self.releases.iter().any(|existing| {
            existing.package_name == release.package_name
                && existing.version == release.version
                && existing.content_hash != release.content_hash
        }) {
            return Err(DistributionError::Invalid(
                "a package version cannot resolve to different content".into(),
            ));
        }
        self.releases.push(release);
        self.head = Some(self.releases.len() - 1);
        Ok(())
    }

    /// Select the current release using the same capability vocabulary used
    /// by placement and capsule admission.
    pub fn select(
        &self,
        runtime: &RuntimeCapabilities,
        now: DateTime<Utc>,
    ) -> Result<&ChannelRelease, DistributionError> {
        if runtime.draining || runtime.expires_at <= now {
            return Err(DistributionError::Incompatible(
                "runtime is draining or its advertisement expired".into(),
            ));
        }
        let release = self
            .head
            .and_then(|head| self.releases.get(head))
            .ok_or(DistributionError::UnknownRelease)?;
        if release
            .selected_runtime_id
            .is_some_and(|selected| selected != runtime.runtime_id)
        {
            return Err(DistributionError::Incompatible(
                "placement selected a different runtime".into(),
            ));
        }
        require_all("handler", &release.requirements.handlers, &runtime.handlers)?;
        require_all("plugin", &release.requirements.plugins, &runtime.plugins)?;
        require_all(
            "credential",
            &release.requirements.credentials,
            &runtime.credentials,
        )?;
        require_all("region", &release.requirements.regions, &runtime.regions)?;
        require_all(
            "hardware",
            &release.requirements.hardware,
            &runtime.hardware,
        )?;
        if release.requirements.minimum_trust > Some(runtime.trust) {
            return Err(DistributionError::Incompatible(
                "runtime trust is below the release minimum".into(),
            ));
        }
        if release.requirements.requires_network
            && !matches!(
                runtime.connectivity,
                Some(
                    orch8_types::continuity::RuntimeConnectivity::Metered
                        | orch8_types::continuity::RuntimeConnectivity::Wifi
                        | orch8_types::continuity::RuntimeConnectivity::Ethernet,
                )
            )
        {
            return Err(DistributionError::Incompatible(
                "release requires a currently-online runtime".into(),
            ));
        }
        if release.requirements.requires_human_ui
            && !matches!(
                runtime.kind,
                orch8_types::continuity::RuntimeKind::Mobile
                    | orch8_types::continuity::RuntimeKind::Browser
            )
        {
            return Err(DistributionError::Incompatible(
                "release requires a human UI runtime".into(),
            ));
        }
        Ok(release)
    }

    /// Deterministically restore a prior immutable release by exact hash.
    pub fn rollback_to(
        &mut self,
        content_hash: &str,
    ) -> Result<&ChannelRelease, DistributionError> {
        let index = self
            .releases
            .iter()
            .rposition(|release| release.content_hash == content_hash)
            .ok_or(DistributionError::UnknownRelease)?;
        self.head = Some(index);
        Ok(&self.releases[index])
    }
}

fn require_all(
    kind: &str,
    required: &[String],
    available: &[String],
) -> Result<(), DistributionError> {
    if let Some(missing) = required.iter().find(|item| !available.contains(item)) {
        return Err(DistributionError::Incompatible(format!(
            "missing {kind} {missing}"
        )));
    }
    Ok(())
}

/// Whole-file delta. A consumer must verify `base_hash`, apply bounded changes,
/// verify `target_hash`, and otherwise download `full_package_url`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaPackage {
    pub base_hash: String,
    pub target_hash: String,
    pub full_package_url: String,
    pub changed_files: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaApplication {
    Applied(BTreeMap<String, String>),
    FullPackageRequired { url: String, expected_hash: String },
}

impl DeltaPackage {
    pub fn apply(
        &self,
        base_files: &BTreeMap<String, String>,
    ) -> Result<DeltaApplication, DistributionError> {
        validate_hash(&self.base_hash)?;
        validate_hash(&self.target_hash)?;
        let delta_bytes = self
            .changed_files
            .iter()
            .try_fold(0_usize, |total, (path, value)| {
                total
                    .checked_add(path.len())
                    .and_then(|total| total.checked_add(value.as_ref().map_or(0, String::len)))
            })
            .ok_or_else(|| DistributionError::Invalid("delta size overflow".into()))?;
        if self.changed_files.len() > MAX_DELTA_FILES
            || delta_bytes > MAX_DELTA_BYTES
            || self.full_package_url.is_empty()
        {
            return Err(DistributionError::Invalid(
                "delta exceeds 10000 files/64 MiB or has no full-package fallback".into(),
            ));
        }
        if hash_files(base_files)? != self.base_hash {
            return Ok(DeltaApplication::FullPackageRequired {
                url: self.full_package_url.clone(),
                expected_hash: self.target_hash.clone(),
            });
        }
        let mut target = base_files.clone();
        for (path, contents) in &self.changed_files {
            validate_relative_path(path)?;
            match contents {
                Some(contents) => {
                    target.insert(path.clone(), contents.clone());
                }
                None => {
                    target.remove(path);
                }
            }
        }
        if hash_files(&target)? != self.target_hash {
            return Ok(DeltaApplication::FullPackageRequired {
                url: self.full_package_url.clone(),
                expected_hash: self.target_hash.clone(),
            });
        }
        Ok(DeltaApplication::Applied(target))
    }
}

pub fn hash_files(files: &BTreeMap<String, String>) -> Result<String, DistributionError> {
    let canonical =
        canonical_json(files).map_err(|error| DistributionError::Invalid(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

/// Tenant-owned registry root and exact access policy. Secrets are not stored
/// here; keys are public verification roots or opaque encrypted credential ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateRegistryPolicy {
    pub tenant_id: String,
    pub namespace: String,
    pub trusted_signing_roots: BTreeSet<String>,
    pub readers: BTreeSet<String>,
    pub publishers: BTreeSet<String>,
    pub encrypted_credential_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryAction {
    Read,
    Publish,
}

impl PrivateRegistryPolicy {
    pub fn authorize(
        &self,
        tenant_id: &str,
        principal: &str,
        action: RegistryAction,
    ) -> Result<(), DistributionError> {
        if tenant_id != self.tenant_id || principal.is_empty() {
            return Err(DistributionError::AccessDenied(principal.into()));
        }
        let allowed = match action {
            RegistryAction::Read => {
                self.readers.contains(principal) || self.publishers.contains(principal)
            }
            RegistryAction::Publish => self.publishers.contains(principal),
        };
        allowed
            .then_some(())
            .ok_or_else(|| DistributionError::AccessDenied(principal.into()))
    }

    pub fn trusts(&self, public_key: &str) -> bool {
        self.trusted_signing_roots.contains(public_key)
    }
}

/// Policy-enforcing wrapper around the immutable signed package registry.
pub struct PrivatePackageRegistryPublisher {
    publisher: PackageRegistryPublisher,
    policy: PrivateRegistryPolicy,
}

impl PrivatePackageRegistryPublisher {
    pub fn new(
        publisher: PackageRegistryPublisher,
        policy: PrivateRegistryPolicy,
    ) -> Result<Self, DistributionError> {
        if policy.tenant_id.is_empty()
            || policy.namespace.is_empty()
            || policy.trusted_signing_roots.is_empty()
            || policy.publishers.is_empty()
            || policy
                .encrypted_credential_id
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(DistributionError::Invalid(
                "private registry requires tenant, namespace, signing root, publisher, and encrypted credential id".into(),
            ));
        }
        Ok(Self { publisher, policy })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn publish(
        &self,
        tenant_id: &str,
        principal: &str,
        package: &SignedPackage,
        index: &mut RegistryIndex,
        ledger: &mut TransparencyLedger,
        ledger_signing_key: &SigningKey,
        published_at: DateTime<Utc>,
    ) -> Result<RegistryVersion, DistributionError> {
        self.policy
            .authorize(tenant_id, principal, RegistryAction::Publish)?;
        if index.tenant_id != self.policy.tenant_id
            || index.namespace != self.policy.namespace
            || !self.policy.trusts(&package.public_key)
        {
            return Err(DistributionError::AccessDenied(principal.into()));
        }
        self.publisher
            .publish(package, index, ledger, ledger_signing_key, published_at)
            .await
            .map_err(|error| DistributionError::Invalid(error.to_string()))
    }

    #[must_use]
    pub fn encrypted_credential_id(&self) -> &str {
        self.policy
            .encrypted_credential_id
            .as_deref()
            .unwrap_or_default()
    }
}

/// In-toto Statement compatible envelope (v1) with bounded Orch8 predicates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupplyChainAttestation {
    #[serde(rename = "_type")]
    pub statement_type: String,
    pub subject: Vec<AttestationSubject>,
    #[serde(rename = "predicateType")]
    pub predicate_type: String,
    pub predicate: AttestationPredicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttestationSubject {
    pub name: String,
    pub digest: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttestationPredicate {
    pub source_uri: String,
    pub source_digest: String,
    pub builder_id: String,
    pub test_report_digest: String,
    pub policy_digest: String,
    pub sbom_uri: String,
    pub sbom_digest: String,
}

impl SupplyChainAttestation {
    pub fn verify_for(
        &self,
        package_name: &str,
        content_hash: &str,
    ) -> Result<(), DistributionError> {
        if self.statement_type != "https://in-toto.io/Statement/v1"
            || self.predicate_type.is_empty()
            || self.subject.len() != 1
            || self.subject.len() > MAX_ATTESTATIONS
        {
            return Err(DistributionError::Invalid(
                "attestation must be a single in-toto v1 subject".into(),
            ));
        }
        let subject = &self.subject[0];
        if subject.name != package_name
            || subject.digest.get("sha256").map(String::as_str) != Some(content_hash)
        {
            return Err(DistributionError::HashMismatch);
        }
        for (name, value) in [
            ("source URI", self.predicate.source_uri.as_str()),
            ("source digest", self.predicate.source_digest.as_str()),
            ("builder", self.predicate.builder_id.as_str()),
            ("test report", self.predicate.test_report_digest.as_str()),
            ("policy", self.predicate.policy_digest.as_str()),
            ("SBOM URI", self.predicate.sbom_uri.as_str()),
            ("SBOM digest", self.predicate.sbom_digest.as_str()),
        ] {
            if value.is_empty() {
                return Err(DistributionError::Invalid(format!(
                    "attestation {name} is required"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    Connector,
    Plugin,
    ModelPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedDependency {
    pub kind: DependencyKind,
    pub name: String,
    pub version: String,
    pub content_hash: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyLockfile {
    pub schema_version: u32,
    pub dependencies: Vec<LockedDependency>,
    pub lock_hash: String,
}

impl DependencyLockfile {
    pub fn build(mut dependencies: Vec<LockedDependency>) -> Result<Self, DistributionError> {
        if dependencies.len() > MAX_LOCK_ENTRIES {
            return Err(DistributionError::Invalid(
                "dependency lock exceeds 4096 entries".into(),
            ));
        }
        dependencies
            .sort_by(|left, right| (&left.kind, &left.name).cmp(&(&right.kind, &right.name)));
        let mut seen = BTreeSet::new();
        for dependency in &dependencies {
            validate_hash(&dependency.content_hash)?;
            if dependency.name.is_empty()
                || dependency.version.is_empty()
                || dependency.source.is_empty()
                || !seen.insert((dependency.kind, dependency.name.as_str()))
            {
                return Err(DistributionError::Invalid(
                    "dependencies require unique kind/name and exact version/source".into(),
                ));
            }
        }
        let lock_hash = hash_lock_entries(&dependencies)?;
        Ok(Self {
            schema_version: 1,
            dependencies,
            lock_hash,
        })
    }

    pub fn verify(&self) -> Result<(), DistributionError> {
        if self.schema_version != 1 || hash_lock_entries(&self.dependencies)? != self.lock_hash {
            return Err(DistributionError::HashMismatch);
        }
        let rebuilt = Self::build(self.dependencies.clone())?;
        if rebuilt.dependencies != self.dependencies {
            return Err(DistributionError::Invalid(
                "lock entries are not in canonical order".into(),
            ));
        }
        Ok(())
    }
}

fn hash_lock_entries(entries: &[LockedDependency]) -> Result<String, DistributionError> {
    let canonical =
        canonical_json(entries).map_err(|error| DistributionError::Invalid(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

fn validate_hash(hash: &str) -> Result<(), DistributionError> {
    if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(DistributionError::Invalid(
            "SHA-256 digest must be 64 hexadecimal characters".into(),
        ))
    }
}

fn validate_relative_path(path: &str) -> Result<(), DistributionError> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains(['\\', '\0'])
        || path.split('/').any(|part| part.is_empty() || part == "..")
    {
        Err(DistributionError::Invalid(format!(
            "invalid delta path {path}"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use chrono::TimeZone as _;
    use orch8_types::continuity::{RuntimeKind, RuntimeTrustLevel};
    use std::sync::Arc;

    fn hash(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn runtime() -> RuntimeCapabilities {
        let now = Utc::now();
        RuntimeCapabilities {
            runtime_id: RuntimeId::new(),
            kind: RuntimeKind::Mobile,
            trust: RuntimeTrustLevel::Attested,
            handlers: vec!["camera".into()],
            plugins: vec!["ocr".into()],
            credentials: vec![],
            regions: vec!["br".into()],
            hardware: vec![],
            offline_capable: true,
            connectivity: None,
            battery_percent: Some(90),
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            observed_at: now,
            expires_at: now + Duration::minutes(5),
        }
    }

    #[test]
    fn channel_selects_compatible_runtime_and_rolls_back_by_hash() {
        let mut channel = ReleaseChannel::new("tenant-a", ReleaseChannelName::Canary);
        let mut requirements = CapsuleRequirements::default();
        requirements.handlers.push("camera".into());
        for (version, digest) in [("1.0.0", hash('a')), ("1.1.0", hash('b'))] {
            channel
                .promote(ChannelRelease {
                    package_name: "acme/field".into(),
                    version: version.into(),
                    content_hash: digest,
                    package_url: format!("/{version}.orch8pkg"),
                    requirements: requirements.clone(),
                    selected_runtime_id: None,
                    promoted_at: Utc::now(),
                })
                .unwrap();
        }
        assert_eq!(
            channel.select(&runtime(), Utc::now()).unwrap().version,
            "1.1.0"
        );
        assert_eq!(channel.rollback_to(&hash('a')).unwrap().version, "1.0.0");
        let key = SigningKey::from_bytes(&[9; 32]);
        let signed = SignedReleaseChannel::sign(channel, "channel-v1", &key).unwrap();
        assert!(
            signed
                .verify(std::slice::from_ref(&signed.public_key))
                .is_ok()
        );
        let mut tampered = signed.clone();
        tampered.channel.head = Some(1);
        assert_eq!(
            tampered.verify(&[tampered.public_key.clone()]),
            Err(DistributionError::HashMismatch)
        );
    }

    #[test]
    fn channel_rejects_missing_capability_and_expired_runtime() {
        let mut channel = ReleaseChannel::new("tenant-a", ReleaseChannelName::Stable);
        let mut requirements = CapsuleRequirements::default();
        requirements.plugins.push("missing".into());
        channel
            .promote(ChannelRelease {
                package_name: "acme/app".into(),
                version: "1.0.0".into(),
                content_hash: hash('a'),
                package_url: "/full".into(),
                requirements,
                selected_runtime_id: None,
                promoted_at: Utc::now(),
            })
            .unwrap();
        assert!(matches!(
            channel.select(&runtime(), Utc::now()),
            Err(DistributionError::Incompatible(_))
        ));
    }

    #[test]
    fn delta_applies_or_returns_verified_full_fallback() {
        let base = BTreeMap::from([("a.txt".into(), "old".into())]);
        let target = BTreeMap::from([("a.txt".into(), "new".into())]);
        let delta = DeltaPackage {
            base_hash: hash_files(&base).unwrap(),
            target_hash: hash_files(&target).unwrap(),
            full_package_url: "/full.orch8pkg".into(),
            changed_files: BTreeMap::from([("a.txt".into(), Some("new".into()))]),
        };
        assert_eq!(
            delta.apply(&base).unwrap(),
            DeltaApplication::Applied(target)
        );
        assert!(matches!(
            delta.apply(&BTreeMap::new()).unwrap(),
            DeltaApplication::FullPackageRequired { .. }
        ));
    }

    #[test]
    fn private_policy_is_tenant_and_action_scoped() {
        let policy = PrivateRegistryPolicy {
            tenant_id: "tenant-a".into(),
            namespace: "acme".into(),
            trusted_signing_roots: BTreeSet::from(["root".into()]),
            readers: BTreeSet::from(["reader".into()]),
            publishers: BTreeSet::from(["publisher".into()]),
            encrypted_credential_id: Some("credential/private-registry".into()),
        };
        assert!(
            policy
                .authorize("tenant-a", "publisher", RegistryAction::Publish)
                .is_ok()
        );
        assert!(
            policy
                .authorize("tenant-b", "publisher", RegistryAction::Read)
                .is_err()
        );
        assert!(
            policy
                .authorize("tenant-a", "reader", RegistryAction::Publish)
                .is_err()
        );
        assert!(policy.trusts("root"));
    }

    #[tokio::test]
    async fn private_registry_wrapper_enforces_policy_before_publication() {
        let key = SigningKey::from_bytes(&[3; 32]);
        let package = crate::package::build_package(
            crate::package::PackageManifest {
                name: "acme/app".into(),
                version: "1.0.0".into(),
                description: String::new(),
                publisher: "acme".into(),
                requirements: crate::package::PackageRequirements::default(),
                created_at: Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap(),
            },
            BTreeMap::from([("README.md".into(), "app".into())]),
            &key,
        )
        .unwrap();
        let cdn = Arc::new(crate::cdn::MemoryCdnBackend::new());
        let publisher =
            PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
        let policy = PrivateRegistryPolicy {
            tenant_id: "tenant-a".into(),
            namespace: "acme".into(),
            trusted_signing_roots: BTreeSet::from([package.public_key.clone()]),
            readers: BTreeSet::new(),
            publishers: BTreeSet::from(["release-bot".into()]),
            encrypted_credential_id: Some("credential/private".into()),
        };
        let publisher = PrivatePackageRegistryPublisher::new(publisher, policy).unwrap();
        let mut index = RegistryIndex::new("tenant-a", "acme");
        let mut ledger = TransparencyLedger::default();
        assert!(
            publisher
                .publish(
                    "tenant-b",
                    "release-bot",
                    &package,
                    &mut index,
                    &mut ledger,
                    &key,
                    Utc::now()
                )
                .await
                .is_err()
        );
        publisher
            .publish(
                "tenant-a",
                "release-bot",
                &package,
                &mut index,
                &mut ledger,
                &key,
                Utc::now(),
            )
            .await
            .unwrap();
        assert_eq!(ledger.entries.len(), 1);
    }

    #[test]
    fn attestation_requires_all_standard_evidence() {
        let digest = hash('a');
        let mut attestation = SupplyChainAttestation {
            statement_type: "https://in-toto.io/Statement/v1".into(),
            subject: vec![AttestationSubject {
                name: "acme/app".into(),
                digest: BTreeMap::from([("sha256".into(), digest.clone())]),
            }],
            predicate_type: "https://orch8.io/attestation/package/v1".into(),
            predicate: AttestationPredicate {
                source_uri: "git+https://example/repo@abc".into(),
                source_digest: hash('b'),
                builder_id: "https://ci.example/builders/release".into(),
                test_report_digest: hash('c'),
                policy_digest: hash('d'),
                sbom_uri: "pkg:generic/acme/app@1".into(),
                sbom_digest: hash('e'),
            },
        };
        assert!(attestation.verify_for("acme/app", &digest).is_ok());
        attestation.predicate.sbom_uri.clear();
        assert!(attestation.verify_for("acme/app", &digest).is_err());
    }

    #[test]
    fn lockfile_is_canonical_and_tamper_evident() {
        let entries = vec![
            LockedDependency {
                kind: DependencyKind::Plugin,
                name: "ocr".into(),
                version: "1.2.0".into(),
                content_hash: hash('b'),
                source: "registry/acme".into(),
            },
            LockedDependency {
                kind: DependencyKind::Connector,
                name: "camera".into(),
                version: "1.0.0".into(),
                content_hash: hash('a'),
                source: "registry/core".into(),
            },
        ];
        let mut lock = DependencyLockfile::build(entries).unwrap();
        assert!(lock.verify().is_ok());
        lock.dependencies[0].version = "9.9.9".into();
        assert_eq!(lock.verify(), Err(DistributionError::HashMismatch));
    }
}

#[cfg(test)]
#[path = "distribution_boundary_tests.rs"]
mod boundary_tests;

#[cfg(test)]
#[path = "distribution_gates_coverage_tests.rs"]
mod gates_coverage_tests;
