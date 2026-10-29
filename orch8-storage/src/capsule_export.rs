//! Durable source cuts: freeze once, then seal and publish the same capsule.
use chrono::{DateTime, Utc};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleEffectState, CapsuleExecutionState, CapsuleId, CapsuleManifest, CapsulePayload,
        ContinuityExecution, OwnershipState, RuntimeId,
    },
    error::StorageError,
    instance::{InstanceState, TaskInstance},
    output::BlockOutput,
    sequence::SequenceDefinition,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The first request fixes expiry. Retries bind all semantic options by digest
/// and cannot extend the lifetime of an already prepared transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapsuleCutRequest {
    pub owner: ContinuityExecution,
    pub destination_runtime_id: Option<RuntimeId>,
    pub options_sha256: String,
    /// Trusted mobile session authority; never derived from workflow data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mobile_access: Option<crate::MobileInstanceAccess>,
    pub now: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
impl CapsuleCutRequest {
    pub(crate) fn matches(&self, other: &Self) -> bool {
        same_location(&self.owner, &other.owner)
            && self.destination_runtime_id == other.destination_runtime_id
            && self.options_sha256 == other.options_sha256
            && self.mobile_access == other.mobile_access
    }
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if self.expires_at <= self.now || !is_digest(&self.options_sha256) {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Raw backend rows remain encrypted at rest when an encryption decorator is
/// used. The decorator opens them only after the capture transaction commits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapsuleSourceSnapshot {
    pub instance: TaskInstance,
    pub sequence: SequenceDefinition,
    pub checkpoint: Checkpoint,
    pub outputs: Vec<BlockOutput>,
    pub execution_state: CapsuleExecutionState,
    pub effects: CapsuleEffectState,
    pub provenance_head: Option<String>,
}
impl CapsuleSourceSnapshot {
    /// Covers raw encrypted rows and serialization overhead; the wire payload
    /// still has its independent 64 MiB limit after decryption.
    pub const MAX_CAPTURE_BYTES: usize = 128 * 1024 * 1024;

    pub(crate) fn validate(&self, owner: &ContinuityExecution) -> Result<(), CapsuleCutRefusal> {
        let instance = &self.instance;
        if instance.id != owner.current_instance_id
            || instance.tenant_id != owner.tenant_id
            || self.sequence.id != instance.sequence_id
            || self.sequence.tenant_id != instance.tenant_id
            || self.checkpoint.instance_id != instance.id
            || !matches!(
                instance.state,
                InstanceState::Paused | InstanceState::Waiting
            )
            || self
                .outputs
                .iter()
                .any(|row| row.instance_id != instance.id)
            || self
                .execution_state
                .nodes
                .iter()
                .any(|row| row.instance_id != instance.id)
            || self
                .execution_state
                .payloads
                .iter()
                .any(|row| row.instance_id != instance.id)
            || self.effects.receipts.iter().any(|row| {
                row.tenant_id != owner.tenant_id || row.continuity_id != owner.continuity_id
            })
        {
            return Err(CapsuleCutRefusal::Ownership);
        }
        if self.outputs.len() > CapsulePayload::MAX_OUTPUTS
            || self.execution_state.nodes.len() > CapsulePayload::MAX_EXECUTION_NODES
            || self.execution_state.payloads.len() > CapsulePayload::MAX_EXTERNALIZED_ENTRIES
            || self.effects.receipts.len() > CapsulePayload::MAX_PENDING_ITEMS
            || self.effects.active_attempts.len() > CapsulePayload::MAX_PENDING_ITEMS
            || self.effects.reset_blocks.len() > CapsulePayload::MAX_PENDING_ITEMS
            || self.effects.commit_guards.len() > CapsulePayload::MAX_PENDING_ITEMS
        {
            return Err(CapsuleCutRefusal::PayloadTooLarge);
        }
        if crate::portable_effects::validate_effect_state(
            owner,
            &self.effects,
            self.sequence.id,
            self.sequence.version,
        )
        .is_err()
        {
            return Err(CapsuleCutRefusal::Ownership);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CapsuleSourceCut {
    pub id: CapsuleId,
    pub request: CapsuleCutRequest,
    pub snapshot: Option<CapsuleSourceSnapshot>,
    pub manifest: Option<CapsuleManifest>,
    pub sealed_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapsuleCutRefusal {
    Ownership,
    UnsafeBoundary,
    MissingCheckpoint,
    MissingSequence,
    PayloadTooLarge,
    WorkerTasksInFlight(usize),
}
#[derive(Debug, Clone)]
pub enum CapsuleCutCapture {
    Ready(Box<CapsuleSourceCut>),
    Refused(CapsuleCutRefusal),
}

pub(crate) fn invalid() -> StorageError {
    StorageError::Constraint("capsule source cut identity or phase conflict".into())
}
pub(crate) fn too_large() -> StorageError {
    StorageError::Constraint("capsule source snapshot exceeds capture bounds".into())
}
pub(crate) fn same_location(a: &ContinuityExecution, b: &ContinuityExecution) -> bool {
    a.tenant_id == b.tenant_id
        && a.continuity_id == b.continuity_id
        && a.epoch == b.epoch
        && a.owner_runtime_id == b.owner_runtime_id
        && a.current_instance_id == b.current_instance_id
}
pub(crate) fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}
pub(crate) fn validate_seal(
    cut: &CapsuleSourceCut,
    manifest: &CapsuleManifest,
    bytes: &[u8],
) -> Result<(), StorageError> {
    let request = &cut.request;
    if manifest.schema != orch8_types::continuity::CapsuleSchemaVersion::CURRENT
        || !is_digest(&manifest.sequence.content_sha256)
        || !is_digest(&manifest.checkpoint.sha256)
        || !is_digest(&manifest.requirements_sha256)
        || manifest.signing_key_id.is_empty()
        || manifest.signing_key_id.len() > 128
        || manifest.encryption_key_id.is_empty()
        || manifest.encryption_key_id.len() > 128
        || manifest.capsule_id != cut.id
        || manifest.tenant_id != request.owner.tenant_id
        || manifest.continuity_id != request.owner.continuity_id
        || manifest.epoch != request.owner.epoch
        || manifest.source_runtime_id != request.owner.owner_runtime_id
        || manifest.source_instance_id != request.owner.current_instance_id
        || manifest.allowed_destination_runtime_id != request.destination_runtime_id
        || manifest.issued_at != request.now
        || manifest.expires_at != request.expires_at
        || manifest.payload_artifact.key != format!("{}/{}", manifest.source_instance_id, cut.id)
        || manifest.payload_artifact.bytes != u64::try_from(bytes.len()).map_err(|_| invalid())?
        || bytes.len() > CapsulePayload::MAX_ENCODED_BYTES + 64
        || manifest.payload_artifact.sha256 != digest(bytes)
    {
        return Err(invalid());
    }
    if let Some(snapshot) = &cut.snapshot
        && (manifest.sequence.id != snapshot.sequence.id
            || manifest.sequence.version != snapshot.sequence.version)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn transferring(owner: &ContinuityExecution, now: DateTime<Utc>) -> ContinuityExecution {
    ContinuityExecution {
        state: OwnershipState::Transferring,
        updated_at: now,
        ..owner.clone()
    }
}

/// Count serialized bytes without allocating another full JSON copy.
pub(crate) struct CaptureBudget(usize);
impl CaptureBudget {
    pub(crate) fn new() -> Self {
        Self(0)
    }
    pub(crate) fn add<T: Serialize>(&mut self, value: &T) -> Result<(), StorageError> {
        serde_json::to_writer(&mut *self, value).map_err(|_| too_large())
    }
}
impl std::io::Write for CaptureBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .0
            .checked_add(bytes.len())
            .filter(|size| *size <= CapsuleSourceSnapshot::MAX_CAPTURE_BYTES)
            .ok_or_else(|| std::io::Error::other("capsule capture byte bound"))?;
        self.0 = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut value = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut value, "{byte:02x}").expect("string formatting");
    }
    value
}

/// Compact durable evidence retained after a trusted host confirms destination
/// acceptance. Ciphertext is no longer needed for export retries at that point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapsuleCutReceipt {
    pub manifest: CapsuleManifest,
    pub accepted_owner: ContinuityExecution,
}

pub(crate) fn validate_acknowledgement(
    request: &CapsuleCutRequest,
    id: CapsuleId,
    manifest: &CapsuleManifest,
    accepted: &ContinuityExecution,
) -> Result<(), StorageError> {
    let source = &request.owner;
    if manifest.payload_artifact.key != format!("{}/{}", source.current_instance_id, id)
        || manifest.capsule_id != id
        || manifest.tenant_id != source.tenant_id
        || manifest.continuity_id != source.continuity_id
        || manifest.source_runtime_id != source.owner_runtime_id
        || manifest.source_instance_id != source.current_instance_id
        || manifest.epoch != source.epoch
        || accepted.tenant_id != source.tenant_id
        || accepted.continuity_id != source.continuity_id
        || source.epoch.checked_next().ok() != Some(accepted.epoch)
        || accepted.owner_runtime_id == source.owner_runtime_id
        || accepted.current_instance_id == source.current_instance_id
        || !matches!(
            accepted.state,
            OwnershipState::Owned | OwnershipState::Completed
        )
        || request
            .destination_runtime_id
            .is_some_and(|id| id != accepted.owner_runtime_id)
        || manifest
            .allowed_destination_runtime_id
            .is_some_and(|id| id != accepted.owner_runtime_id)
    {
        return Err(invalid());
    }
    Ok(())
}
