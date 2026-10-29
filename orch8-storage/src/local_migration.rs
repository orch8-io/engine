//! Local recovery points preserve physical ownership; they are not transfers.
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::ContinuityExecution,
    continuity_advanced::{LiveMigrationRecord, LiveMigrationState, MigrationPlanId},
};

use crate::{
    MobileInstanceAccess,
    capsule_export::{CapsuleCutRefusal, CapsuleSourceSnapshot},
};

#[derive(Debug, Clone)]
pub struct LocalMigrationSnapshotRequest {
    pub execution: ContinuityExecution,
    pub migration_id: MigrationPlanId,
    pub expected_instance_updated_at: DateTime<Utc>,
    pub mobile_access: Option<MobileInstanceAccess>,
}

#[derive(Debug, Clone)]
pub struct LocalMigrationSnapshot {
    pub record: LiveMigrationRecord,
    pub source: CapsuleSourceSnapshot,
    /// Cleanup is registered durably before artifact I/O, independently of
    /// native/plan history. Expiry never transfers or restores authority.
    pub artifact_key: String,
    /// Opaque proof over raw persisted source and plan rows, retained across
    /// decryption so commit can compare the same database representation.
    pub fingerprint: String,
}

#[derive(Debug, Clone)]
pub enum LocalMigrationSnapshotCapture {
    Ready(Box<LocalMigrationSnapshot>),
    Refused(CapsuleCutRefusal),
}

pub(crate) fn valid_record(
    record: &LiveMigrationRecord,
    request: &LocalMigrationSnapshotRequest,
) -> bool {
    record.plan.id == request.migration_id
        && record.plan.tenant_id == request.execution.tenant_id
        && record.plan.continuity_id == request.execution.continuity_id
        && match record.state {
            LiveMigrationState::Planned => record.expected_epoch == request.execution.epoch,
            LiveMigrationState::Applied => {
                record.applied_epoch == Some(request.execution.epoch)
                    && record.applied_at == Some(request.expected_instance_updated_at)
            }
            LiveMigrationState::RolledBack => false,
        }
        && record.rollback_expires_at > Utc::now()
}

impl crate::LiveMigrationTransition<'_> {
    pub(crate) fn validate(&self) -> Result<(), orch8_types::error::StorageError> {
        use orch8_types::continuity::OwnershipState;
        let old = self.expected_execution;
        let next = self.next_execution;
        let record = self.expected_record;
        let updated = self.next_record;
        let apply = record.state == LiveMigrationState::Planned
            && updated.state == LiveMigrationState::Applied;
        let rollback = record.state == LiveMigrationState::Applied
            && updated.state == LiveMigrationState::RolledBack;
        if self.expected_target_sequence.id != self.next_sequence_id
            || self.expected_target_sequence.tenant_id != *self.tenant_id
            || self.expected_source_fingerprint.len() != 64
            || !self
                .expected_source_fingerprint
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || old.tenant_id != *self.tenant_id
            || next.tenant_id != *self.tenant_id
            || record.plan.tenant_id != *self.tenant_id
            || old.continuity_id != record.plan.continuity_id
            || next.continuity_id != old.continuity_id
            || old.current_instance_id != next.current_instance_id
            || old.owner_runtime_id != next.owner_runtime_id
            || old.state != OwnershipState::Owned
            || next.state != OwnershipState::Owned
            || old.epoch.checked_next().ok() != Some(next.epoch)
            || record.plan != updated.plan
            || record.expected_epoch != updated.expected_epoch
            || record.source_state != updated.source_state
            || record.source_checkpoint.id != updated.source_checkpoint.id
            || record.source_checkpoint.instance_id != old.current_instance_id
            || self.checkpoint.instance_id != old.current_instance_id
            || record.rollback_expires_at != updated.rollback_expires_at
            || !(apply || rollback)
            || (apply
                && (self.next_sequence_id != record.plan.to_sequence_id
                    || self.expected_target_sequence.version != record.plan.to_version
                    || old.epoch != record.expected_epoch
                    || record.applied_epoch.is_some()
                    || record.applied_at.is_some()
                    || record.rolled_back_at.is_some()
                    || updated.applied_epoch != Some(next.epoch)
                    || updated.applied_at != Some(next.updated_at)
                    || updated.rolled_back_at.is_some()
                    || self.forbid_effects_epoch.is_some()))
            || (rollback
                && (self.next_sequence_id != record.plan.from_sequence_id
                    || self.expected_target_sequence.version != record.plan.from_version
                    || record.applied_epoch != Some(old.epoch)
                    || record.applied_at.is_none()
                    || record.rolled_back_at.is_some()
                    || updated.applied_epoch != record.applied_epoch
                    || updated.applied_at != record.applied_at
                    || updated.rolled_back_at != Some(next.updated_at)
                    || self.forbid_effects_epoch != Some(old.epoch)))
        {
            return Err(orch8_types::error::StorageError::Constraint(
                "local migration transition identity mismatch".into(),
            ));
        }
        Ok(())
    }
}

/// Payload fields are independently authenticated/opened by the API/decorator;
/// comparing newly encrypted ciphertext here would reject identical plaintext.
/// These stable metadata fields bind the exact plan and progress read by caller.
pub(crate) fn same_record_metadata(a: &LiveMigrationRecord, b: &LiveMigrationRecord) -> bool {
    a.plan == b.plan
        && a.state == b.state
        && a.expected_epoch == b.expected_epoch
        && a.source_state == b.source_state
        && a.source_checkpoint.id == b.source_checkpoint.id
        && a.source_checkpoint.instance_id == b.source_checkpoint.instance_id
        && a.source_checkpoint.created_at == b.source_checkpoint.created_at
        && a.rollback_expires_at == b.rollback_expires_at
        && a.rollback_capsule == b.rollback_capsule
        && a.applied_epoch == b.applied_epoch
        && a.applied_at == b.applied_at
        && a.rolled_back_at == b.rolled_back_at
}

/// Database reads use stable row order. Stream hashing avoids a second large
/// serialization allocation and includes ciphertext exactly as persisted.
pub(crate) fn fingerprint(
    record: &LiveMigrationRecord,
    source: &CapsuleSourceSnapshot,
) -> Result<String, orch8_types::error::StorageError> {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    struct HashWriter(Sha256);
    impl std::io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn sort_keys(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for child in map.values_mut() {
                    sort_keys(child);
                }
                map.sort_keys();
            }
            serde_json::Value::Array(array) => {
                for child in array {
                    sort_keys(child);
                }
            }
            _ => {}
        }
    }
    let mut value = serde_json::to_value((record, source))?;
    sort_keys(&mut value);
    let mut hash = HashWriter(Sha256::new());
    serde_json::to_writer(&mut hash, &value)?;
    let mut digest = String::with_capacity(64);
    for byte in hash.0.finalize() {
        let _ = write!(digest, "{byte:02x}");
    }
    Ok(digest)
}
