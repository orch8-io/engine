//! Wire types for `orch8 migrate --to <url>`: moving sequences and in-flight
//! instances from an embedded (`SQLite`) engine to a remote engine without
//! restarting runs. Shared by the CLI (source) and the target import API.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::continuity::{ContinuityExecution, EffectReceipt};
use crate::execution::ExecutionNode;
use crate::instance::TaskInstance;
use crate::output::BlockOutput;
use crate::sequence::SequenceDefinition;
use crate::signal::Signal;

/// Instances accepted per import request.
pub const MAX_INSTANCES_PER_IMPORT: usize = 100;
/// Sequences accepted per import request.
pub const MAX_SEQUENCES_PER_IMPORT: usize = 500;
/// Metadata key stamped on migrated instances (source and target).
pub const MIGRATION_METADATA_KEY: &str = "orch8_migration";

/// Complete durable state of one in-flight instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigratedInstance {
    pub instance: TaskInstance,
    #[serde(default)]
    pub execution_tree: Vec<ExecutionNode>,
    #[serde(default)]
    pub block_outputs: Vec<BlockOutput>,
    /// Effect ledger, so at-most-once dispatch evidence and unresolved
    /// (`dispatched`/`unknown`) receipts carry over instead of resetting.
    #[serde(default)]
    pub effect_receipts: Vec<EffectReceipt>,
    #[serde(default)]
    pub pending_signals: Vec<Signal>,
    /// Source ownership record; the target takes ownership at `epoch + 1`.
    pub continuity: ContinuityExecution,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationImportRequest {
    /// Stable id of the migration run; retries reuse it.
    pub migration_id: Uuid,
    /// Free-form label of the source engine (shown in instance metadata).
    pub source_engine_id: String,
    #[serde(default)]
    pub sequences: Vec<SequenceDefinition>,
    #[serde(default)]
    pub instances: Vec<MigratedInstance>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportStatus {
    /// Created by this request.
    Imported,
    /// Already fully imported by an earlier request (idempotent retry).
    AlreadyPresent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedInstance {
    pub instance_id: crate::ids::InstanceId,
    pub status: ImportStatus,
    /// Ownership epoch now held by the target.
    pub epoch: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SequenceImportCounts {
    pub created: u64,
    pub existing: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationImportResponse {
    pub migration_id: Uuid,
    /// Runtime identity that now owns the imported executions. The source
    /// records it when fencing its copy.
    pub target_runtime_id: Uuid,
    pub sequences: SequenceImportCounts,
    pub instances: Vec<ImportedInstance>,
}

/// Deterministic owner runtime for a migration, so retries converge.
#[must_use]
pub fn target_runtime_id(migration_id: Uuid) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"orch8-migration-target-v1\0");
    hasher.update(migration_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
