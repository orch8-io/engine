//! Tenant-scoped package discovery and publication transparency.
//!
//! A registry publication writes the immutable package and signed ledger entry
//! before updating the short-lived discovery index and ledger head. Consumers
//! can therefore verify every discovered version without trusting the CDN.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cdn::{CdnBackend, CdnError};
use crate::manifest::canonical_json;
use crate::package::{PackageError, SignedPackage, verify_package};

/// Stable discovery document for one tenant and publisher namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryIndex {
    pub schema_version: u32,
    pub tenant_id: String,
    pub namespace: String,
    /// Package name to versions, ordered oldest to newest by publication.
    pub packages: BTreeMap<String, Vec<RegistryVersion>>,
    pub ledger_head: Option<String>,
    /// `ETag` observed when this index was loaded. It is transport metadata and
    /// is intentionally not serialized into the signed discovery document.
    #[serde(skip)]
    pub source_etag: Option<String>,
}

impl RegistryIndex {
    #[must_use]
    pub fn new(tenant_id: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            schema_version: 1,
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            packages: BTreeMap::new(),
            ledger_head: None,
            source_etag: None,
        }
    }

    /// Attach the CDN `ETag` returned alongside a loaded index.
    #[must_use]
    pub fn with_source_etag(mut self, etag: impl Into<String>) -> Self {
        self.source_etag = Some(etag.into());
        self
    }

    /// Cross-check every discovery record against the signed ledger.
    pub fn verify_against(&self, ledger: &TransparencyLedger) -> Result<(), RegistryError> {
        ledger.verify()?;
        if self.ledger_head.as_deref() != ledger.head() {
            return Err(RegistryError::InvalidLedger(
                "index ledger head does not match supplied ledger".into(),
            ));
        }
        let version_count = self.packages.values().map(Vec::len).sum::<usize>();
        if version_count != ledger.entries.len() {
            return Err(RegistryError::InvalidLedger(
                "index and ledger contain different publication counts".into(),
            ));
        }
        for (package_name, versions) in &self.packages {
            let expected_prefix = format!("{}/", self.namespace);
            let Some(package_leaf) = package_name.strip_prefix(&expected_prefix) else {
                return Err(RegistryError::InvalidLedger(format!(
                    "index package {package_name} is outside namespace {}",
                    self.namespace
                )));
            };
            for version in versions {
                let expected_url = format!(
                    "/{}/registry/{}/packages/{package_leaf}/{}/{}.orch8pkg",
                    self.tenant_id, self.namespace, version.version, version.content_hash
                );
                let matched = ledger.entries.iter().any(|entry| {
                    entry.tenant_id == self.tenant_id
                        && entry.namespace == self.namespace
                        && entry.package_name == *package_name
                        && entry.package_version == version.version
                        && entry.content_hash == version.content_hash
                        && entry.public_key == version.public_key
                        && entry.package_signature == version.signature
                        && entry.published_at == version.published_at
                        && entry.entry_hash == version.ledger_entry_hash
                        && version.package_url == expected_url
                });
                if !matched {
                    return Err(RegistryError::InvalidLedger(format!(
                        "index record {package_name}@{} has no matching signed entry",
                        version.version
                    )));
                }
            }
        }
        Ok(())
    }
}

/// A discoverable, immutable package version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryVersion {
    pub version: String,
    pub content_hash: String,
    pub package_url: String,
    pub public_key: String,
    pub signature: String,
    pub published_at: DateTime<Utc>,
    pub ledger_entry_hash: String,
}

/// Signed append-only record of a package publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransparencyEntry {
    pub sequence: u64,
    pub previous_hash: Option<String>,
    pub tenant_id: String,
    pub namespace: String,
    pub package_name: String,
    pub package_version: String,
    pub content_hash: String,
    pub published_at: DateTime<Utc>,
    pub public_key: String,
    pub package_signature: String,
    pub entry_hash: String,
    /// Ed25519 signature over `entry_hash`.
    pub signature: String,
}

#[derive(Serialize)]
struct EntryPayload<'a> {
    sequence: u64,
    previous_hash: &'a Option<String>,
    tenant_id: &'a str,
    namespace: &'a str,
    package_name: &'a str,
    package_version: &'a str,
    content_hash: &'a str,
    published_at: DateTime<Utc>,
    public_key: &'a str,
    package_signature: &'a str,
}

impl TransparencyEntry {
    fn payload(&self) -> EntryPayload<'_> {
        EntryPayload {
            sequence: self.sequence,
            previous_hash: &self.previous_hash,
            tenant_id: &self.tenant_id,
            namespace: &self.namespace,
            package_name: &self.package_name,
            package_version: &self.package_version,
            content_hash: &self.content_hash,
            published_at: self.published_at,
            public_key: &self.public_key,
            package_signature: &self.package_signature,
        }
    }

    /// Verify the entry digest and publisher signature.
    pub fn verify(&self) -> Result<(), RegistryError> {
        let canonical = canonical_json(&self.payload())
            .map_err(|error| RegistryError::Serialization(error.to_string()))?;
        let actual = hex::encode(Sha256::digest(canonical.as_bytes()));
        if actual != self.entry_hash {
            return Err(RegistryError::InvalidLedger(format!(
                "entry {} hash mismatch",
                self.sequence
            )));
        }
        let key_bytes: [u8; 32] = BASE64
            .decode(&self.public_key)
            .map_err(|error| RegistryError::InvalidLedger(error.to_string()))?
            .try_into()
            .map_err(|_| RegistryError::InvalidLedger("public key must be 32 bytes".into()))?;
        let signature_bytes: [u8; 64] = BASE64
            .decode(&self.signature)
            .map_err(|error| RegistryError::InvalidLedger(error.to_string()))?
            .try_into()
            .map_err(|_| RegistryError::InvalidLedger("signature must be 64 bytes".into()))?;
        let key = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|error| RegistryError::InvalidLedger(error.to_string()))?;
        key.verify(
            self.entry_hash.as_bytes(),
            &Signature::from_bytes(&signature_bytes),
        )
        .map_err(|_| RegistryError::InvalidLedger("entry signature is invalid".into()))
    }
}

/// Ordered publication history. A verifier should retain the last accepted
/// head and require it to be an ancestor of every later ledger it accepts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransparencyLedger {
    pub entries: Vec<TransparencyEntry>,
}

impl TransparencyLedger {
    #[must_use]
    pub fn head(&self) -> Option<&str> {
        self.entries.last().map(|entry| entry.entry_hash.as_str())
    }

    /// Verify signatures, sequence numbers, and every hash link.
    pub fn verify(&self) -> Result<(), RegistryError> {
        let mut previous: Option<&str> = None;
        for (position, entry) in self.entries.iter().enumerate() {
            let expected_sequence = u64::try_from(position)
                .map_err(|_| RegistryError::InvalidLedger("ledger is too large".into()))?;
            if entry.sequence != expected_sequence || entry.previous_hash.as_deref() != previous {
                return Err(RegistryError::InvalidLedger(format!(
                    "entry {} is not the next hash-chain link",
                    entry.sequence
                )));
            }
            entry.verify()?;
            previous = Some(&entry.entry_hash);
        }
        Ok(())
    }

    /// Prove a previously pinned head remains in this ledger.
    pub fn contains_head(&self, pinned_head: &str) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.entry_hash == pinned_head)
    }
}

/// Publishes packages into an isolated tenant/publisher namespace.
pub struct PackageRegistryPublisher {
    cdn: Box<dyn CdnBackend>,
    tenant_id: String,
    namespace: String,
}

impl PackageRegistryPublisher {
    /// Create a publisher. Identifiers are path segments, never raw paths.
    pub fn new(
        cdn: Box<dyn CdnBackend>,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, RegistryError> {
        let tenant_id = tenant_id.into();
        let namespace = namespace.into();
        validate_segment("tenant_id", &tenant_id)?;
        validate_segment("namespace", &namespace)?;
        Ok(Self {
            cdn,
            tenant_id,
            namespace,
        })
    }

    /// Verify, register, and publish a signed package.
    ///
    /// The supplied index and ledger are only mutated after every CDN write
    /// succeeds, so callers can retry safely after a partial network failure.
    pub async fn publish(
        &self,
        package: &SignedPackage,
        index: &mut RegistryIndex,
        ledger: &mut TransparencyLedger,
        signing_key: &SigningKey,
        published_at: DateTime<Utc>,
    ) -> Result<RegistryVersion, RegistryError> {
        let package_leaf = self.validate_publication(package, index, ledger, signing_key)?;

        let sequence = u64::try_from(ledger.entries.len())
            .map_err(|_| RegistryError::InvalidLedger("ledger is too large".into()))?;
        let mut entry = TransparencyEntry {
            sequence,
            previous_hash: ledger.head().map(str::to_owned),
            tenant_id: self.tenant_id.clone(),
            namespace: self.namespace.clone(),
            package_name: package.archive.manifest.name.clone(),
            package_version: package.archive.manifest.version.clone(),
            content_hash: package.content_hash.clone(),
            published_at,
            public_key: package.public_key.clone(),
            package_signature: package.signature.clone(),
            entry_hash: String::new(),
            signature: String::new(),
        };
        let canonical = canonical_json(&entry.payload())
            .map_err(|error| RegistryError::Serialization(error.to_string()))?;
        entry.entry_hash = hex::encode(Sha256::digest(canonical.as_bytes()));
        entry.signature = BASE64.encode(signing_key.sign(entry.entry_hash.as_bytes()).to_bytes());

        let root = format!("{}/registry/{}", self.tenant_id, self.namespace);
        let index_path = format!("{root}/index.json");
        let package_path = format!(
            "{root}/packages/{package_leaf}/{}/{}.orch8pkg",
            package.archive.manifest.version, package.content_hash
        );
        let entry_path = format!(
            "{root}/transparency/entries/{:020}-{}.json",
            entry.sequence, entry.entry_hash
        );
        let version = RegistryVersion {
            version: package.archive.manifest.version.clone(),
            content_hash: package.content_hash.clone(),
            package_url: format!("/{package_path}"),
            public_key: package.public_key.clone(),
            signature: package.signature.clone(),
            published_at,
            ledger_entry_hash: entry.entry_hash.clone(),
        };

        let mut next_index = index.clone();
        next_index
            .packages
            .entry(package.archive.manifest.name.clone())
            .or_default()
            .push(version.clone());
        next_index.ledger_head = Some(entry.entry_hash.clone());
        next_index.source_etag = None;
        let mut next_ledger = ledger.clone();
        next_ledger.entries.push(entry.clone());

        self.upload_json(&package_path, package, "immutable, max-age=31536000")
            .await?;
        self.upload_json(&entry_path, &entry, "immutable, max-age=31536000")
            .await?;
        self.upload_json(
            &format!("{root}/transparency/ledgers/{}.json", entry.entry_hash),
            &next_ledger,
            "immutable, max-age=31536000",
        )
        .await?;
        let index_json = canonical_json(&next_index)
            .map_err(|error| RegistryError::Serialization(error.to_string()))?;
        let new_etag = self
            .cdn
            .upload_if_match(
                &index_path,
                index_json.into_bytes(),
                Some("application/json"),
                Some("max-age=60"),
                index.source_etag.as_deref(),
            )
            .await?;
        next_index.source_etag = Some(new_etag);

        *index = next_index;
        *ledger = next_ledger;
        Ok(version)
    }

    fn validate_publication<'a>(
        &self,
        package: &'a SignedPackage,
        index: &RegistryIndex,
        ledger: &TransparencyLedger,
        signing_key: &SigningKey,
    ) -> Result<&'a str, RegistryError> {
        verify_package(package)?;
        self.validate_state(index, ledger)?;
        let expected_prefix = format!("{}/", self.namespace);
        let package_leaf = package
            .archive
            .manifest
            .name
            .strip_prefix(&expected_prefix)
            .ok_or_else(|| {
                RegistryError::InvalidConfig(format!(
                    "package {} is outside publisher namespace {}",
                    package.archive.manifest.name, self.namespace
                ))
            })?;
        let signing_public_key = BASE64.encode(signing_key.verifying_key().to_bytes());
        if signing_public_key != package.public_key {
            return Err(RegistryError::InvalidConfig(
                "ledger signing key must match package signing key".into(),
            ));
        }
        if index
            .packages
            .get(&package.archive.manifest.name)
            .is_some_and(|versions| {
                versions
                    .iter()
                    .any(|version| version.version == package.archive.manifest.version)
            })
        {
            return Err(RegistryError::DuplicateVersion {
                package: package.archive.manifest.name.clone(),
                version: package.archive.manifest.version.clone(),
            });
        }
        Ok(package_leaf)
    }

    fn validate_state(
        &self,
        index: &RegistryIndex,
        ledger: &TransparencyLedger,
    ) -> Result<(), RegistryError> {
        if index.schema_version != 1
            || index.tenant_id != self.tenant_id
            || index.namespace != self.namespace
        {
            return Err(RegistryError::InvalidConfig(
                "registry index does not match publisher tenant/namespace".into(),
            ));
        }
        index.verify_against(ledger)
    }

    async fn upload_json<T: Serialize>(
        &self,
        path: &str,
        value: &T,
        cache_control: &str,
    ) -> Result<(), RegistryError> {
        let json = canonical_json(value)
            .map_err(|error| RegistryError::Serialization(error.to_string()))?;
        self.cdn
            .upload(
                path,
                json.into_bytes(),
                Some("application/json"),
                Some(cache_control),
            )
            .await?;
        Ok(())
    }
}

fn validate_segment(label: &str, value: &str) -> Result<(), RegistryError> {
    if value.is_empty()
        || !value.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '-' | '_')
        })
    {
        return Err(RegistryError::InvalidConfig(format!(
            "{label} must be non-empty lowercase [a-z0-9-_]"
        )));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("invalid registry configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid transparency ledger: {0}")]
    InvalidLedger(String),
    #[error("package {package} version {version} is already published")]
    DuplicateVersion { package: String, version: String },
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("package verification failed: {0}")]
    Package(#[from] PackageError),
    #[error("CDN error: {0}")]
    Cdn(#[from] CdnError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use chrono::TimeZone;
    use rand_core::OsRng;

    use super::*;
    use crate::cdn::MemoryCdnBackend;
    use crate::package::{PackageManifest, PackageRequirements, build_package};

    fn package(key: &SigningKey, version: &str) -> SignedPackage {
        build_package(
            PackageManifest {
                name: "acme/checkout".into(),
                version: version.into(),
                description: "Checkout workflow".into(),
                publisher: "Acme".into(),
                requirements: PackageRequirements::default(),
                created_at: Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap(),
            },
            BTreeMap::from([("README.md".into(), "# Checkout".into())]),
            key,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn publishes_discoverable_versions_and_signed_chain() {
        let cdn = Arc::new(MemoryCdnBackend::new());
        let publisher =
            PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
        let key = SigningKey::generate(&mut OsRng);
        let mut index = RegistryIndex::new("tenant-a", "acme");
        let mut ledger = TransparencyLedger::default();

        let first = publisher
            .publish(
                &package(&key, "1.0.0"),
                &mut index,
                &mut ledger,
                &key,
                Utc.with_ymd_and_hms(2026, 7, 25, 1, 0, 0).unwrap(),
            )
            .await
            .unwrap();
        publisher
            .publish(
                &package(&key, "1.1.0"),
                &mut index,
                &mut ledger,
                &key,
                Utc.with_ymd_and_hms(2026, 7, 25, 2, 0, 0).unwrap(),
            )
            .await
            .unwrap();

        ledger.verify().unwrap();
        index.verify_against(&ledger).unwrap();
        assert!(ledger.contains_head(&first.ledger_entry_hash));
        assert_eq!(index.packages["acme/checkout"].len(), 2);
        assert_eq!(index.ledger_head.as_deref(), ledger.head());
        let store = cdn.store.lock().await;
        assert!(store.contains_key("tenant-a/registry/acme/index.json"));
        assert!(
            store
                .keys()
                .any(|path| path.contains("/transparency/entries/"))
        );
    }

    #[tokio::test]
    async fn rejects_cross_tenant_state_duplicate_versions_and_tampering() {
        let cdn = Arc::new(MemoryCdnBackend::new());
        let publisher = PackageRegistryPublisher::new(Box::new(cdn), "tenant-a", "acme").unwrap();
        let key = SigningKey::generate(&mut OsRng);
        let mut wrong_index = RegistryIndex::new("tenant-b", "acme");
        let mut ledger = TransparencyLedger::default();
        let timestamp = Utc.with_ymd_and_hms(2026, 7, 25, 1, 0, 0).unwrap();
        assert!(
            publisher
                .publish(
                    &package(&key, "1.0.0"),
                    &mut wrong_index,
                    &mut ledger,
                    &key,
                    timestamp,
                )
                .await
                .is_err()
        );

        let cdn = Arc::new(MemoryCdnBackend::new());
        let publisher = PackageRegistryPublisher::new(Box::new(cdn), "tenant-a", "acme").unwrap();
        let mut index = RegistryIndex::new("tenant-a", "acme");
        publisher
            .publish(
                &package(&key, "1.0.0"),
                &mut index,
                &mut ledger,
                &key,
                timestamp,
            )
            .await
            .unwrap();
        assert!(
            publisher
                .publish(
                    &package(&key, "1.0.0"),
                    &mut index,
                    &mut ledger,
                    &key,
                    timestamp,
                )
                .await
                .is_err()
        );
        ledger.entries[0].content_hash = "tampered".into();
        assert!(ledger.verify().is_err());
        assert!(index.verify_against(&ledger).is_err());
    }

    #[tokio::test]
    async fn rejects_a_competing_writer_with_a_stale_head() {
        let cdn = Arc::new(MemoryCdnBackend::new());
        let first_publisher =
            PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
        let second_publisher =
            PackageRegistryPublisher::new(Box::new(cdn), "tenant-a", "acme").unwrap();
        let key = SigningKey::generate(&mut OsRng);
        let mut first_index = RegistryIndex::new("tenant-a", "acme");
        let mut first_ledger = TransparencyLedger::default();
        let mut stale_index = first_index.clone();
        let mut stale_ledger = first_ledger.clone();
        let timestamp = Utc.with_ymd_and_hms(2026, 7, 25, 1, 0, 0).unwrap();

        first_publisher
            .publish(
                &package(&key, "1.0.0"),
                &mut first_index,
                &mut first_ledger,
                &key,
                timestamp,
            )
            .await
            .unwrap();
        let error = second_publisher
            .publish(
                &package(&key, "1.1.0"),
                &mut stale_index,
                &mut stale_ledger,
                &key,
                timestamp,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, RegistryError::Cdn(CdnError::Conflict)));
        assert!(stale_index.packages.is_empty());
        assert!(stale_ledger.entries.is_empty());
    }
}

#[cfg(test)]
#[path = "registry_coverage_tests.rs"]
mod coverage_tests;
