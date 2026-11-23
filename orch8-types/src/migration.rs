//! Wire types for `orch8 migrate --to <url>`: moving sequences and in-flight
//! instances from an embedded (`SQLite`) engine to a remote engine without
//! restarting runs. Shared by the CLI (source) and the target import API.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::checkpoint::Checkpoint;
use crate::continuity::{CapsuleEffectState, ContinuityExecution, EffectReceipt};
use crate::execution::ExecutionNode;
use crate::instance::TaskInstance;
use crate::output::{BlockOutput, ExternalizedEntry};
use crate::sequence::SequenceDefinition;
use crate::signal::Signal;

/// Instances accepted per import request.
pub const MAX_INSTANCES_PER_IMPORT: usize = 100;
/// Sequences accepted per import request.
pub const MAX_SEQUENCES_PER_IMPORT: usize = 500;
/// Metadata key stamped on migrated instances (source and target).
pub const MIGRATION_METADATA_KEY: &str = "orch8_migration";

pub const MIGRATION_MANIFEST_VERSION: u32 = 1;
pub const MIGRATION_BASE_DOMAINS: &[&str] = &[
    "instance",
    "continuity_owner",
    "execution_tree",
    "block_outputs",
    "effect_receipts",
    "pending_signals",
];
pub const MIGRATION_KV_DOMAIN: &str = "instance_kv";
pub const MIGRATION_CHECKPOINT_DOMAIN: &str = "checkpoints";
pub const MIGRATION_TERMINAL_HISTORY_DOMAIN: &str = "terminal_history";
pub const MIGRATION_DIGEST_DOMAIN: &str = "domain_digests";

pub const MIGRATION_ARTIFACTS_DOMAIN: &str = "artifacts";
pub const MIGRATION_EXTERNALIZED_DOMAIN: &str = "externalized_payloads";
pub const MAX_MIGRATION_ARTIFACT_BYTES: usize = 100 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationArtifact {
    pub artifact_id: Uuid,
    pub sha256: String,
    pub length: usize,
}

pub const MIGRATION_CRON_DOMAIN: &str = "cron_schedules";
pub const MIGRATION_CHUNKS_DOMAIN: &str = "import_chunks";
pub const MIGRATION_CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub const MIGRATION_MAX_UPLOAD_BYTES: usize = 64 * MIGRATION_CHUNK_BYTES;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationUploadCommit {
    pub tenant_id: crate::ids::TenantId,
    pub length: usize,
}

/// Capabilities declared before a source changes ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MigrationCapabilities {
    pub manifest_version: u32,
    /// Each domain lists the wire formats the target can import.
    pub domains: BTreeMap<String, Vec<u32>>,
}

impl MigrationCapabilities {
    #[must_use]
    pub fn current() -> Self {
        Self {
            manifest_version: MIGRATION_MANIFEST_VERSION,
            domains: MIGRATION_BASE_DOMAINS
                .iter()
                .chain([
                    &MIGRATION_KV_DOMAIN,
                    &MIGRATION_CHECKPOINT_DOMAIN,
                    &MIGRATION_TERMINAL_HISTORY_DOMAIN,
                    &MIGRATION_DIGEST_DOMAIN,
                    &MIGRATION_CHUNKS_DOMAIN,
                    &MIGRATION_ARTIFACTS_DOMAIN,
                    &MIGRATION_EXTERNALIZED_DOMAIN,
                    &MIGRATION_CRON_DOMAIN,
                ])
                .map(|&domain| (domain.to_owned(), vec![1]))
                .collect(),
        }
    }

    #[must_use]
    pub fn supports(&self, domain: &str, version: u32) -> bool {
        self.manifest_version == MIGRATION_MANIFEST_VERSION
            && self
                .domains
                .get(domain)
                .is_some_and(|versions| versions.contains(&version))
    }
}

/// Durable evidence attached to a preserved instance's effect ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectArchive {
    pub owner: ContinuityExecution,
    pub sequence_version: i32,
    pub state: CapsuleEffectState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalInstanceHistory {
    pub instance: TaskInstance,
    #[serde(default)]
    pub execution_tree: Vec<ExecutionNode>,
    #[serde(default)]
    pub block_outputs: Vec<BlockOutput>,
    #[serde(default)]
    pub payloads: Vec<ExternalizedEntry>,
    #[serde(default)]
    pub history: MigratedInstanceState,
    #[serde(default)]
    pub signals: Vec<Signal>,
    #[serde(default)]
    pub effects: Option<EffectArchive>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalHistoryImport {
    pub migration_id: Uuid,
    pub entries: Vec<TerminalInstanceHistory>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedHistoryInstance {
    pub instance_id: crate::ids::InstanceId,
    pub status: ImportStatus,
}

/// Persisted instance KV row. The original write timestamp is its version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceKvEntry {
    pub instance_id: crate::ids::InstanceId,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// State domains transferred before the target may resume an instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigratedInstanceState {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instance_kv: Vec<InstanceKvEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoints: Vec<Checkpoint>,
}

impl MigratedInstanceState {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instance_kv.is_empty() && self.checkpoints.is_empty()
    }

    #[must_use]
    pub fn belongs_to(&self, instance_id: crate::ids::InstanceId) -> bool {
        let mut keys = std::collections::HashSet::new();
        let mut ids = std::collections::HashSet::new();
        self.instance_kv
            .iter()
            .all(|row| row.instance_id == instance_id && keys.insert(&row.key))
            && self
                .checkpoints
                .iter()
                .all(|row| row.instance_id == instance_id && ids.insert(row.id))
    }
}

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
    #[serde(default, skip_serializing_if = "MigratedInstanceState::is_empty")]
    pub durable_state: MigratedInstanceState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_digests: Option<MigrationDomainDigests>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub externalized_payloads: Vec<ExternalizedEntry>,
    /// Verified binary objects staged separately from the execution snapshot.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<MigrationArtifact>,
    /// Source ownership record; the target takes ownership at `epoch + 1`.
    pub continuity: ContinuityExecution,
}

impl MigratedInstance {
    #[must_use]
    pub fn requires_v1_import(&self) -> bool {
        !self.durable_state.is_empty()
            || self.domain_digests.is_some()
            || !self.externalized_payloads.is_empty()
            || !self.artifacts.is_empty()
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_digests: Option<MigrationDomainDigests>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationDomainDigests {
    pub execution_tree: String,
    pub block_outputs: String,
    pub effect_receipts: String,
    pub pending_signals: String,
}

impl MigrationDomainDigests {
    /// Hash rows in ID order. The databases share microsecond timestamp
    /// precision for indexed time columns.
    pub fn compute(
        tree: &[ExecutionNode],
        outputs: &[BlockOutput],
        receipts: &[EffectReceipt],
        signals: &[Signal],
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            execution_tree: digest_rows(
                "execution_tree",
                tree,
                |row| row.id,
                &["started_at", "completed_at"],
            )?,
            block_outputs: digest_rows("block_outputs", outputs, |row| row.id, &["created_at"])?,
            effect_receipts: digest_rows(
                "effect_receipts",
                receipts,
                |row| row.id,
                &["created_at", "updated_at"],
            )?,
            pending_signals: digest_rows(
                "pending_signals",
                signals,
                |row| row.id,
                &["created_at", "delivered_at"],
            )?,
        })
    }
}

/// Hash the precise recurring schedule snapshot recorded by the source fence.
pub fn cron_schedule_digest(
    snapshot: &crate::cron::CronMigrationSnapshot,
) -> Result<String, serde_json::Error> {
    digest_rows(
        MIGRATION_CRON_DOMAIN,
        std::slice::from_ref(snapshot),
        |_| (),
        &[],
    )
}

fn digest_rows<T: Serialize, K: Ord>(
    domain: &str,
    rows: &[T],
    key: impl Fn(&T) -> K,
    timestamps: &[&str],
) -> Result<String, serde_json::Error> {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    struct Hasher(Sha256);
    impl std::io::Write for Hasher {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut ordered: Vec<_> = rows.iter().collect();
    ordered.sort_unstable_by_key(|row| key(row));
    let mut hash = Hasher(Sha256::new());
    hash.0.update(b"orch8-migration-domain-v1\0");
    hash.0.update(domain.as_bytes());
    hash.0.update(b"\0");
    for row in ordered {
        let mut row = serde_json::to_value(row)?;
        for timestamp in timestamps {
            if let Some(value) = row.get_mut(timestamp)
                && let Some(value_at) = value.as_str()
                && let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value_at)
            {
                let micros = datetime
                    .with_timezone(&chrono::Utc)
                    .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
                *value = serde_json::Value::String(micros);
            }
        }
        row.sort_all_objects();
        serde_json::to_writer(&mut hash, &row)?;
        hash.0.update(b"\n");
    }
    let mut hex = String::with_capacity(64);
    for byte in hash.0.finalize() {
        write!(&mut hex, "{byte:02x}").expect("writing to a string cannot fail");
    }
    Ok(hex)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_digests_ignore_row_order_and_submicrosecond_storage_precision() {
        let at = "2026-01-01T00:00:00.123456789Z".parse().unwrap();
        let mut first = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: crate::ids::InstanceId::new(),
            block_id: crate::ids::BlockId::new("one"),
            output: serde_json::json!({"ok":true}),
            output_ref: None,
            output_size: 11,
            attempt: 0,
            created_at: at,
        };
        let second = BlockOutput {
            id: Uuid::now_v7(),
            ..first.clone()
        };
        let expected =
            MigrationDomainDigests::compute(&[], &[first.clone(), second.clone()], &[], &[])
                .unwrap();
        first.created_at = "2026-01-01T00:00:00.123456Z".parse().unwrap();
        let ordered =
            MigrationDomainDigests::compute(&[], &[second.clone(), first.clone()], &[], &[])
                .unwrap();
        assert_eq!(expected, ordered);
        first.output = serde_json::json!({"ok":false});
        let changed = MigrationDomainDigests::compute(&[], &[second, first], &[], &[]).unwrap();
        assert_ne!(changed.block_outputs, expected.block_outputs);
        assert_eq!(changed.execution_tree, expected.execution_tree);
    }

    #[test]
    fn capabilities_require_a_known_manifest_and_an_exact_domain_format() {
        let mut capabilities = MigrationCapabilities::current();
        let wire = serde_json::to_value(&capabilities).unwrap();
        assert_eq!(
            serde_json::from_value::<MigrationCapabilities>(wire).unwrap(),
            capabilities
        );
        assert!(capabilities.supports("effect_receipts", 1));
        assert!(!capabilities.supports("effect_receipts", 2));
        assert!(capabilities.supports(MIGRATION_KV_DOMAIN, 1));
        assert!(!capabilities.supports("artifact_bytes", 1));
        capabilities.manifest_version += 1;
        assert!(!capabilities.supports("effect_receipts", 1));
    }

    #[test]
    fn state_rejects_duplicate_keys_and_foreign_checkpoints() {
        let instance_id = crate::ids::InstanceId::new();
        let now = chrono::Utc::now();
        let mut state = MigratedInstanceState {
            instance_kv: vec![InstanceKvEntry {
                instance_id,
                key: "balance".into(),
                value: 42.into(),
                updated_at: now,
                expires_at: Some(now),
            }],
            checkpoints: Vec::new(),
        };
        assert!(state.belongs_to(instance_id));
        state.instance_kv.push(state.instance_kv[0].clone());
        assert!(!state.belongs_to(instance_id));
        state.instance_kv.pop();
        state.checkpoints.push(Checkpoint {
            id: Uuid::now_v7(),
            instance_id: crate::ids::InstanceId::new(),
            checkpoint_data: serde_json::Value::Null,
            created_at: now,
        });
        assert!(!state.belongs_to(instance_id));
    }
}
