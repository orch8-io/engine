//! Audience-bound mobile catalog rollouts for authenticated host backends.
//!
//! Persist each device cursor before sending its manifest and serialize cursor
//! updates in a transaction or compare-and-swap. The cursor belongs to the
//! authenticated delivery device and catalog audience, never a request that
//! names an arbitrary device. Send personalized manifests with `no-store`.

use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{SigningKey, VerifyingKey};
use orch8_types::catalog::CatalogAudience;
use orch8_types::ids::SequenceId;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest::{
    ManifestError, ManifestGenerator, ManifestSequence, ManifestSigningKey, SignedCatalogManifest,
};
use crate::publish::{PublishError, SequencePublisher};

const BUCKETS: u64 = 10_000;
const MAX_ENTRIES: usize = 1000;
const MAX_MANIFEST_BYTES: usize = 5 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogReleaseRing {
    Stable,
    Canary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    /// Minimum numeric app version. Independent of entries' SDK minima.
    pub min_app_version: String,
    /// The complete audience's catalog snapshot. Omitted names are withdrawn.
    pub entries: Vec<ManifestSequence>,
    pub signing_keys: Vec<ManifestSigningKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogCanary {
    /// Use the same key to expand a release without reassigning existing devices.
    pub cohort_key: String,
    /// The canary percentage in basis points (0 to 10,000).
    pub basis_points: u16,
    pub snapshot: CatalogSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogRollout {
    /// Monotonic trusted policy revision. Increase for each policy change.
    pub generation: u64,
    pub stable: CatalogSnapshot,
    pub canary: Option<CatalogCanary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDeliveryReceipt {
    pub generation: u64,
    pub ring: CatalogReleaseRing,
    pub manifest_version: i64,
    /// SHA-256 of the canonical signed catalog envelope body.
    pub manifest_sha256: String,
    /// Binds identical signed manifest bodies to distinct device deliveries.
    pub delivery_id: String,
}

pub struct CatalogDelivery {
    pub manifest: SignedCatalogManifest,
    pub receipt: CatalogDeliveryReceipt,
}

impl CatalogDelivery {
    /// Body accepted by the existing scoped catalog endpoint.
    pub fn wire_bytes(&self) -> Vec<u8> {
        format!(
            "{}\n{}",
            self.manifest.signature_b64, self.manifest.canonical_json
        )
        .into_bytes()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct RetainedDelivery {
    key: String,
    selection_sha256: String,
    signature_b64: String,
    canonical_json: String,
}

impl std::fmt::Debug for RetainedDelivery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedDelivery")
            .field("selection_sha256", &self.selection_sha256)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogDeviceCursor {
    device_id: String,
    audience: CatalogAudience,
    last_issued: Option<CatalogDeliveryReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retained_delivery: Option<RetainedDelivery>,
    acknowledged: Option<CatalogDeliveryReceipt>,
    /// Retain version and hash floors even for withdrawn names.
    seen_workflows: BTreeMap<String, (i32, String)>,
}

impl CatalogDeviceCursor {
    pub fn new(device_id: String, audience: CatalogAudience) -> Result<Self, CatalogRolloutError> {
        audience.validate().map_err(CatalogRolloutError::Invalid)?;
        if !valid_identity(&device_id) {
            return Err(CatalogRolloutError::Invalid("invalid device identity"));
        }
        Ok(Self {
            device_id,
            audience,
            last_issued: None,
            retained_delivery: None,
            acknowledged: None,
            seen_workflows: BTreeMap::new(),
        })
    }

    pub fn last_issued(&self) -> Option<&CatalogDeliveryReceipt> {
        self.last_issued.as_ref()
    }

    pub fn acknowledged(&self) -> Option<&CatalogDeliveryReceipt> {
        self.acknowledged.as_ref()
    }

    /// Sign a snapshot for this device. Commit the changed cursor before sending
    /// the envelope. Existing devices must start with their persisted issuance
    /// history, including any rollout obtained before the current process.
    pub fn issue(
        &mut self,
        rollout: &CatalogRollout,
        signer: &ManifestGenerator,
        app_version: &str,
        sdk_version: &str,
    ) -> Result<CatalogDelivery, CatalogRolloutError> {
        self.issue_inner(rollout, signer, app_version, sdk_version, None)
    }

    /// Issue or replay the response for the current idempotency key. Persist
    /// the whole cursor atomically before sending the response so a restarted
    /// publisher can return the exact signed bytes.
    pub fn issue_idempotent(
        &mut self,
        rollout: &CatalogRollout,
        signer: &ManifestGenerator,
        app_version: &str,
        sdk_version: &str,
        key: &str,
    ) -> Result<CatalogDelivery, CatalogRolloutError> {
        if !valid_identity(key) || key.len() > 128 {
            return Err(CatalogRolloutError::Invalid(
                "invalid issuance idempotency key",
            ));
        }
        self.issue_inner(rollout, signer, app_version, sdk_version, Some(key))
    }

    fn issue_inner(
        &mut self,
        rollout: &CatalogRollout,
        signer: &ManifestGenerator,
        app_version: &str,
        sdk_version: &str,
        key: Option<&str>,
    ) -> Result<CatalogDelivery, CatalogRolloutError> {
        if rollout.generation == 0
            || self
                .last_issued
                .as_ref()
                .is_some_and(|last| rollout.generation < last.generation)
        {
            return Err(CatalogRolloutError::Invalid("stale rollout generation"));
        }
        let (ring, snapshot) = self.select(rollout, app_version, sdk_version)?;
        validate_snapshot(snapshot)?;
        for entry in &snapshot.entries {
            if let Some((version, hash)) = self.seen_workflows.get(&entry.name)
                && (entry.version < *version
                    || (entry.version == *version && entry.sha256 != *hash))
            {
                return Err(CatalogRolloutError::WorkflowRollback(entry.name.clone()));
            }
        }
        let selection_sha256 = key
            .map(|_| {
                serde_json::to_vec(&(rollout.generation, ring, snapshot))
                    .map(|bytes| hex::encode(Sha256::digest(bytes)))
                    .map_err(|_| CatalogRolloutError::Invalid("invalid issuance snapshot"))
            })
            .transpose()?;
        if let (Some(key), Some(selection), Some(cached), Some(receipt)) = (
            key,
            &selection_sha256,
            &self.retained_delivery,
            &self.last_issued,
        ) {
            if cached.key == key && &cached.selection_sha256 != selection {
                return Err(CatalogRolloutError::Invalid(
                    "issuance key was reused for a different selection",
                ));
            }
            if &cached.selection_sha256 == selection {
                let body = serde_json::from_str(&cached.canonical_json)
                    .map_err(|_| CatalogRolloutError::Invalid("invalid retained delivery"))?;
                return Ok(CatalogDelivery {
                    manifest: SignedCatalogManifest {
                        body,
                        signature_b64: cached.signature_b64.clone(),
                        canonical_json: cached.canonical_json.clone(),
                    },
                    receipt: receipt.clone(),
                });
            }
        }
        let floor = self
            .last_issued
            .as_ref()
            .map_or(0, |last| last.manifest_version);
        signer.observe_published_version(floor);
        let manifest = signer.generate_catalog(
            self.audience.clone(),
            snapshot.entries.clone(),
            Vec::new(),
            snapshot.signing_keys.clone(),
        )?;
        validate_signed(&manifest, floor)?;
        let manifest_sha256 = hex::encode(Sha256::digest(manifest.canonical_json.as_bytes()));
        let mut delivery = Sha256::new();
        delivery.update(b"orch8/catalog-delivery/v1");
        delivery.update((self.device_id.len() as u64).to_be_bytes());
        delivery.update(self.device_id.as_bytes());
        delivery.update(manifest_sha256.as_bytes());
        let receipt = CatalogDeliveryReceipt {
            generation: rollout.generation,
            ring,
            manifest_version: manifest.body.manifest.manifest_version,
            manifest_sha256,
            delivery_id: hex::encode(delivery.finalize()),
        };
        for entry in &snapshot.entries {
            self.seen_workflows
                .insert(entry.name.clone(), (entry.version, entry.sha256.clone()));
        }
        self.retained_delivery =
            key.zip(selection_sha256)
                .map(|(key, selection_sha256)| RetainedDelivery {
                    key: key.into(),
                    selection_sha256,
                    signature_b64: manifest.signature_b64.clone(),
                    canonical_json: manifest.canonical_json.clone(),
                });
        self.last_issued = Some(receipt.clone());
        Ok(CatalogDelivery { manifest, receipt })
    }

    /// Accept only the latest exact receipt from this authenticated device.
    /// Acknowledgement is idempotent and records the device's report, which
    /// is separate from cryptographic attestation of local installation.
    pub fn acknowledge(
        &mut self,
        authenticated_device_id: &str,
        receipt: &CatalogDeliveryReceipt,
    ) -> Result<(), CatalogRolloutError> {
        if self.device_id != authenticated_device_id || self.last_issued.as_ref() != Some(receipt) {
            return Err(CatalogRolloutError::AcknowledgementMismatch);
        }
        self.acknowledged = Some(receipt.clone());
        Ok(())
    }

    fn select<'a>(
        &self,
        rollout: &'a CatalogRollout,
        app_version: &str,
        sdk_version: &str,
    ) -> Result<(CatalogReleaseRing, &'a CatalogSnapshot), CatalogRolloutError> {
        let app = parse_version(app_version)?;
        let sdk = parse_version(sdk_version)?;
        if let Some(canary) = &rollout.canary {
            if !valid_identity(&canary.cohort_key) || u64::from(canary.basis_points) > BUCKETS {
                return Err(CatalogRolloutError::Invalid("invalid canary cohort"));
            }
            if self.bucket(&canary.cohort_key) < u64::from(canary.basis_points)
                && snapshot_compatible(&canary.snapshot, app, sdk)?
            {
                return Ok((CatalogReleaseRing::Canary, &canary.snapshot));
            }
        }
        if snapshot_compatible(&rollout.stable, app, sdk)? {
            return Ok((CatalogReleaseRing::Stable, &rollout.stable));
        }
        Err(CatalogRolloutError::Incompatible)
    }

    fn bucket(&self, cohort_key: &str) -> u64 {
        let mut digest = Sha256::new();
        digest.update(b"orch8/catalog-rollout/v1");
        for value in [
            self.audience.source.as_str(),
            &self.audience.catalog,
            cohort_key,
            &self.device_id,
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        u64::from_be_bytes(digest.finalize()[..8].try_into().expect("SHA-256 prefix")) % BUCKETS
    }
}

fn valid_identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 4096 && !value.chars().any(char::is_control)
}

fn parse_version(value: &str) -> Result<[u32; 3], CatalogRolloutError> {
    let invalid = || CatalogRolloutError::Invalid("expected numeric app and SDK versions");
    let mut parts = [0; 3];
    for (index, part) in value.split('.').enumerate() {
        if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        *parts.get_mut(index).ok_or_else(invalid)? = part.parse().map_err(|_| invalid())?;
    }
    Ok(parts)
}

fn snapshot_compatible(
    snapshot: &CatalogSnapshot,
    app: [u32; 3],
    sdk: [u32; 3],
) -> Result<bool, CatalogRolloutError> {
    if app < parse_version(&snapshot.min_app_version)? {
        return Ok(false);
    }
    for entry in &snapshot.entries {
        if sdk < parse_version(&entry.min_sdk_version)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_snapshot(snapshot: &CatalogSnapshot) -> Result<(), CatalogRolloutError> {
    if snapshot.entries.len() > MAX_ENTRIES || snapshot.signing_keys.len() >= MAX_ENTRIES {
        return Err(CatalogRolloutError::Invalid(
            "catalog snapshot exceeds entry limits",
        ));
    }
    let mut names = BTreeSet::new();
    for entry in &snapshot.entries {
        if !valid_identity(&entry.name)
            || entry.version <= 0
            || !names.insert(&entry.name)
            || entry.sha256.len() != 64
            || !entry
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CatalogRolloutError::Invalid("invalid catalog entry"));
        }
    }
    Ok(())
}

fn validate_signed(
    manifest: &SignedCatalogManifest,
    floor: i64,
) -> Result<(), CatalogRolloutError> {
    if manifest.body.manifest.manifest_version <= floor {
        return Err(CatalogRolloutError::Invalid(
            "manifest version range exhausted",
        ));
    }
    if manifest.signature_b64.len() + 1 + manifest.canonical_json.len() > MAX_MANIFEST_BYTES {
        return Err(CatalogRolloutError::Invalid(
            "signed catalog exceeds the mobile byte limit",
        ));
    }
    let mut keys = BTreeSet::new();
    for key in &manifest.body.manifest.signing_keys {
        let bytes: [u8; 32] = BASE64
            .decode(&key.public_key)
            .ok()
            .and_then(|decoded| decoded.try_into().ok())
            .ok_or(CatalogRolloutError::Invalid("invalid catalog signing key"))?;
        if !valid_identity(&key.key_id) || VerifyingKey::from_bytes(&bytes).is_err() {
            return Err(CatalogRolloutError::Invalid("invalid catalog signing key"));
        }
        keys.insert(key.key_id.as_str());
    }
    if manifest
        .body
        .manifest
        .sequences
        .iter()
        .any(|entry| !keys.contains(entry.signing_key_id.as_str()))
    {
        return Err(CatalogRolloutError::Invalid(
            "catalog entry signing key is missing",
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogRolloutError {
    #[error("invalid catalog rollout: {0}")]
    Invalid(&'static str),
    #[error("no rollout snapshot is compatible with this device")]
    Incompatible,
    #[error("workflow {0} requires a newer revision before it can be delivered")]
    WorkflowRollback(String),
    #[error("acknowledgement does not match the most recent manifest issued to this device")]
    AcknowledgementMismatch,
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Publish(#[from] PublishError),
}

/// Republish a known compatible catalog under fresh sequence IDs and versions.
/// `version_floors` must contain the highest ever published version of each
/// affected name, including withdrawn versions. Allocate those floors under
/// the host's publication lock. This uploads immutable assets only. Activate
/// them by issuing a new policy with the returned entries and canary disabled.
pub async fn publish_catalog_rollback(
    publisher: &SequencePublisher,
    signing_key: &SigningKey,
    previous: &[SequenceDefinition],
    version_floors: &BTreeMap<String, i32>,
) -> Result<Vec<ManifestSequence>, CatalogRolloutError> {
    let definitions = rollback_definitions(previous, version_floors)?;
    let mut entries = Vec::with_capacity(definitions.len());
    for definition in definitions {
        entries.push(publisher.publish_sequence(&definition, signing_key).await?);
    }
    Ok(entries)
}

fn rollback_definitions(
    previous: &[SequenceDefinition],
    version_floors: &BTreeMap<String, i32>,
) -> Result<Vec<SequenceDefinition>, CatalogRolloutError> {
    let invalid = || {
        CatalogRolloutError::Invalid(
            "rollback requires a complete catalog and current version floors",
        )
    };
    if previous.is_empty() || previous.len() > MAX_ENTRIES {
        return Err(invalid());
    }
    let mut revisions = BTreeMap::new();
    for definition in previous {
        let floor = version_floors
            .get(&definition.name)
            .copied()
            .ok_or_else(invalid)?;
        let next = floor.checked_add(1).ok_or_else(invalid)?;
        if definition.version <= 0
            || floor < definition.version
            || revisions
                .insert(definition.name.as_str(), (definition.version, next))
                .is_some()
        {
            return Err(invalid());
        }
        definition.validate().map_err(|_| invalid())?;
    }
    previous
        .iter()
        .map(|original| {
            let mut definition = original.clone();
            definition.id = SequenceId::new();
            definition.created_at = chrono::Utc::now();
            definition.version = revisions[definition.name.as_str()].1;
            definition.deprecated = false;
            definition.status = SequenceStatus::Production;
            for blocks in std::iter::once(&mut definition.blocks)
                .chain(definition.on_failure.as_mut())
                .chain(definition.on_cancel.as_mut())
            {
                retarget_children(blocks, &revisions)?;
            }
            Ok(definition)
        })
        .collect()
}

fn retarget_children(
    blocks: &mut [BlockDefinition],
    revisions: &BTreeMap<&str, (i32, i32)>,
) -> Result<(), CatalogRolloutError> {
    let invalid =
        || CatalogRolloutError::Invalid("rollback snapshot is missing a referenced child revision");
    for block in blocks {
        match block {
            BlockDefinition::Step(_) => {}
            BlockDefinition::SubSequence(child) => {
                let (old, new) = revisions
                    .get(child.sequence_name.as_str())
                    .copied()
                    .ok_or_else(invalid)?;
                if let Some(version) = child.version {
                    if version != old {
                        return Err(invalid());
                    }
                    child.version = Some(new);
                }
            }
            BlockDefinition::Parallel(block) => {
                for branch in &mut block.branches {
                    retarget_children(branch, revisions)?;
                }
            }
            BlockDefinition::Race(block) => {
                for branch in &mut block.branches {
                    retarget_children(branch, revisions)?;
                }
            }
            BlockDefinition::Loop(block) => retarget_children(&mut block.body, revisions)?,
            BlockDefinition::ForEach(block) => retarget_children(&mut block.body, revisions)?,
            BlockDefinition::Router(block) => {
                for route in &mut block.routes {
                    retarget_children(&mut route.blocks, revisions)?;
                }
                if let Some(default) = &mut block.default {
                    retarget_children(default, revisions)?;
                }
            }
            BlockDefinition::TryCatch(block) => {
                retarget_children(&mut block.try_block, revisions)?;
                retarget_children(&mut block.catch_block, revisions)?;
                if let Some(finally) = &mut block.finally_block {
                    retarget_children(finally, revisions)?;
                }
            }
            BlockDefinition::ABSplit(block) => {
                for variant in &mut block.variants {
                    retarget_children(&mut variant.blocks, revisions)?;
                }
            }
            BlockDefinition::CancellationScope(block) => {
                retarget_children(&mut block.blocks, revisions)?;
            }
            BlockDefinition::Saga(block) => {
                for step in &mut block.steps {
                    retarget_children(std::slice::from_mut(step.action.as_mut()), revisions)?;
                    if let Some(compensation) = &mut step.compensation {
                        retarget_children(std::slice::from_mut(compensation.as_mut()), revisions)?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "catalog_rollout_tests.rs"]
mod tests;
