use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::Row;

use orch8_types::continuity::{
    CapsuleId, CapsuleManifest, ContinuationGrant, ContinuationGrantId, ContinuationGrantState,
    ContinuityExecution, ContinuityId, ContinuityLocation, ContinuityStream, EffectDispatchOutcome,
    EffectId, EffectReceipt, EffectState, ExecutionEpoch, ExecutionHandoff, HandoffId,
    HandoffState, PlacementDecision, PlacementDecisionId, ProvenanceEntry, RuntimeCapabilities,
    RuntimeId, StreamFrame, StreamId,
};
use orch8_types::continuity_advanced::{LiveMigrationRecord, MigrationPlanId};
use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::instance::TaskInstance;

use super::SqliteStorage;
use crate::LiveMigrationTransition;

fn encode<T: Serialize>(value: &T) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(StorageError::Serialization)
}

fn decode<T: DeserializeOwned>(value: &str) -> Result<T, StorageError> {
    serde_json::from_str(value).map_err(StorageError::Serialization)
}

fn state_name<T: Serialize>(value: T) -> Result<String, StorageError> {
    serde_json::to_value(value)
        .map_err(StorageError::Serialization)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| StorageError::Query("state did not serialize as a string".into()))
}

fn to_i64(value: u64, field: &str) -> Result<i64, StorageError> {
    i64::try_from(value)
        .map_err(|_| StorageError::Query(format!("{field} exceeds SQLite INTEGER range")))
}

#[async_trait]
impl crate::ContinuityStore for SqliteStorage {
    async fn create_continuity_execution(
        &self,
        execution: &ContinuityExecution,
    ) -> Result<(), StorageError> {
        let location = ContinuityLocation {
            continuity_id: execution.continuity_id,
            tenant_id: execution.tenant_id.clone(),
            epoch: execution.epoch,
            runtime_id: execution.owner_runtime_id,
            instance_id: execution.current_instance_id,
            handoff_id: None,
            entered_at: execution.updated_at,
        };
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO continuity_executions
             (continuity_id, tenant_id, epoch, owner_runtime_id, state, record, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(execution.continuity_id.to_string())
        .bind(execution.tenant_id.as_str())
        .bind(to_i64(execution.epoch.get(), "epoch")?)
        .bind(execution.owner_runtime_id.to_string())
        .bind(state_name(execution.state)?)
        .bind(encode(execution)?)
        .bind(execution.updated_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO continuity_locations
             (tenant_id, continuity_id, epoch, runtime_id, instance_id, handoff_id, record, entered_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(execution.tenant_id.as_str())
        .bind(execution.continuity_id.to_string())
        .bind(to_i64(execution.epoch.get(), "location epoch")?)
        .bind(execution.owner_runtime_id.to_string())
        .bind(execution.current_instance_id.to_string())
        .bind(Option::<String>::None)
        .bind(encode(&location)?)
        .bind(execution.updated_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_continuity_execution(
        &self,
        tenant_id: &TenantId,
        id: ContinuityId,
    ) -> Result<Option<ContinuityExecution>, StorageError> {
        let row = sqlx::query(
            "SELECT record FROM continuity_executions WHERE tenant_id = ? AND continuity_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn get_continuity_execution_by_instance(
        &self,
        tenant_id: &TenantId,
        instance_id: InstanceId,
    ) -> Result<Option<ContinuityExecution>, StorageError> {
        let row = sqlx::query(
            "SELECT record FROM continuity_executions
             WHERE tenant_id = ? AND json_extract(record, '$.current_instance_id') = ?",
        )
        .bind(tenant_id.as_str())
        .bind(instance_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn list_continuity_locations(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<ContinuityLocation>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM continuity_locations
             WHERE tenant_id = ? AND continuity_id = ?
             ORDER BY epoch ASC LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.to_string())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(&row.get::<String, _>("record")))
            .collect()
    }

    async fn cas_continuity_owner(
        &self,
        tenant_id: &TenantId,
        id: ContinuityId,
        expected_epoch: ExecutionEpoch,
        expected_owner: RuntimeId,
        next: &ContinuityExecution,
    ) -> Result<bool, StorageError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let result = sqlx::query(
            "UPDATE continuity_executions
             SET epoch = ?, owner_runtime_id = ?, state = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND continuity_id = ? AND epoch = ?
               AND owner_runtime_id = ?",
        )
        .bind(to_i64(next.epoch.get(), "next epoch")?)
        .bind(next.owner_runtime_id.to_string())
        .bind(state_name(next.state)?)
        .bind(encode(next)?)
        .bind(next.updated_at.to_rfc3339())
        .bind(tenant_id.as_str())
        .bind(id.to_string())
        .bind(to_i64(expected_epoch.get(), "expected epoch")?)
        .bind(expected_owner.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if result.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        if next.epoch > expected_epoch {
            let location = ContinuityLocation {
                continuity_id: next.continuity_id,
                tenant_id: next.tenant_id.clone(),
                epoch: next.epoch,
                runtime_id: next.owner_runtime_id,
                instance_id: next.current_instance_id,
                handoff_id: None,
                entered_at: next.updated_at,
            };
            sqlx::query(
                "INSERT INTO continuity_locations
                 (tenant_id, continuity_id, epoch, runtime_id, instance_id, handoff_id, record, entered_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(tenant_id.as_str())
            .bind(next.continuity_id.to_string())
            .bind(to_i64(next.epoch.get(), "location epoch")?)
            .bind(next.owner_runtime_id.to_string())
            .bind(next.current_instance_id.to_string())
            .bind(Option::<String>::None)
            .bind(encode(&location)?)
            .bind(next.updated_at.to_rfc3339())
            .execute(&mut *transaction)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }

    async fn create_handoff(&self, handoff: &ExecutionHandoff) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO execution_handoffs
             (id, continuity_id, tenant_id, state, version, record, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(handoff.id.to_string())
        .bind(handoff.continuity_id.to_string())
        .bind(handoff.tenant_id.as_str())
        .bind(state_name(handoff.state)?)
        .bind(to_i64(handoff.version, "handoff version")?)
        .bind(encode(handoff)?)
        .bind(handoff.created_at.to_rfc3339())
        .bind(handoff.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_handoff(
        &self,
        tenant_id: &TenantId,
        id: HandoffId,
    ) -> Result<Option<ExecutionHandoff>, StorageError> {
        let row =
            sqlx::query("SELECT record FROM execution_handoffs WHERE tenant_id = ? AND id = ?")
                .bind(tenant_id.as_str())
                .bind(id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn cas_handoff(
        &self,
        tenant_id: &TenantId,
        id: HandoffId,
        expected_state: HandoffState,
        expected_version: u64,
        next: &ExecutionHandoff,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE execution_handoffs SET state = ?, version = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND id = ? AND state = ? AND version = ?",
        )
        .bind(state_name(next.state)?)
        .bind(to_i64(next.version, "next handoff version")?)
        .bind(encode(next)?)
        .bind(next.updated_at.to_rfc3339())
        .bind(tenant_id.as_str())
        .bind(id.to_string())
        .bind(state_name(expected_state)?)
        .bind(to_i64(expected_version, "expected handoff version")?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn accept_handoff(
        &self,
        tenant_id: &TenantId,
        expected_handoff: &ExecutionHandoff,
        accepted_handoff: &ExecutionHandoff,
        expected_execution: &ContinuityExecution,
        accepted_execution: &ContinuityExecution,
    ) -> Result<bool, StorageError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let ownership = sqlx::query(
            "UPDATE continuity_executions
             SET epoch = ?, owner_runtime_id = ?, state = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND continuity_id = ? AND epoch = ?
               AND owner_runtime_id = ? AND state = ?",
        )
        .bind(to_i64(accepted_execution.epoch.get(), "accepted epoch")?)
        .bind(accepted_execution.owner_runtime_id.to_string())
        .bind(state_name(accepted_execution.state)?)
        .bind(encode(accepted_execution)?)
        .bind(accepted_execution.updated_at.to_rfc3339())
        .bind(tenant_id.as_str())
        .bind(expected_execution.continuity_id.to_string())
        .bind(to_i64(expected_execution.epoch.get(), "expected epoch")?)
        .bind(expected_execution.owner_runtime_id.to_string())
        .bind(state_name(expected_execution.state)?)
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if ownership.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        let handoff = sqlx::query(
            "UPDATE execution_handoffs SET state = ?, version = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND id = ? AND state = ? AND version = ?",
        )
        .bind(state_name(accepted_handoff.state)?)
        .bind(to_i64(
            accepted_handoff.version,
            "accepted handoff version",
        )?)
        .bind(encode(accepted_handoff)?)
        .bind(accepted_handoff.updated_at.to_rfc3339())
        .bind(tenant_id.as_str())
        .bind(expected_handoff.id.to_string())
        .bind(state_name(expected_handoff.state)?)
        .bind(to_i64(
            expected_handoff.version,
            "expected handoff version",
        )?)
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if handoff.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        let location = ContinuityLocation {
            continuity_id: accepted_execution.continuity_id,
            tenant_id: accepted_execution.tenant_id.clone(),
            epoch: accepted_execution.epoch,
            runtime_id: accepted_execution.owner_runtime_id,
            instance_id: accepted_execution.current_instance_id,
            handoff_id: Some(accepted_handoff.id),
            entered_at: accepted_execution.updated_at,
        };
        sqlx::query(
            "INSERT INTO continuity_locations
             (tenant_id, continuity_id, epoch, runtime_id, instance_id, handoff_id, record, entered_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(tenant_id.as_str())
        .bind(accepted_execution.continuity_id.to_string())
        .bind(to_i64(accepted_execution.epoch.get(), "location epoch")?)
        .bind(accepted_execution.owner_runtime_id.to_string())
        .bind(accepted_execution.current_instance_id.to_string())
        .bind(accepted_handoff.id.to_string())
        .bind(encode(&location)?)
        .bind(accepted_execution.updated_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }

    async fn commit_handoff_export(
        &self,
        tenant_id: &TenantId,
        expected_handoff: &ExecutionHandoff,
        exported_handoff: &ExecutionHandoff,
        expected_execution: &ContinuityExecution,
        transferring_execution: &ContinuityExecution,
    ) -> Result<bool, StorageError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let ownership = sqlx::query(
            "UPDATE continuity_executions
             SET state = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND continuity_id = ? AND epoch = ?
               AND owner_runtime_id = ? AND state = ?",
        )
        .bind(state_name(transferring_execution.state)?)
        .bind(encode(transferring_execution)?)
        .bind(transferring_execution.updated_at.to_rfc3339())
        .bind(tenant_id.as_str())
        .bind(expected_execution.continuity_id.to_string())
        .bind(to_i64(expected_execution.epoch.get(), "expected epoch")?)
        .bind(expected_execution.owner_runtime_id.to_string())
        .bind(state_name(expected_execution.state)?)
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if ownership.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        let handoff = sqlx::query(
            "UPDATE execution_handoffs SET state = ?, version = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND id = ? AND state = ? AND version = ?",
        )
        .bind(state_name(exported_handoff.state)?)
        .bind(to_i64(
            exported_handoff.version,
            "exported handoff version",
        )?)
        .bind(encode(exported_handoff)?)
        .bind(exported_handoff.updated_at.to_rfc3339())
        .bind(tenant_id.as_str())
        .bind(expected_handoff.id.to_string())
        .bind(state_name(expected_handoff.state)?)
        .bind(to_i64(
            expected_handoff.version,
            "expected handoff version",
        )?)
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if handoff.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }

    async fn resume_handoff(
        &self,
        tenant_id: &TenantId,
        expected_handoff: &ExecutionHandoff,
        resumed_handoff: &ExecutionHandoff,
        destination_instance_id: InstanceId,
    ) -> Result<bool, StorageError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let now = resumed_handoff.updated_at.to_rfc3339();
        let instance = sqlx::query(
            "UPDATE task_instances SET state = 'scheduled', next_fire_at = ?, updated_at = ?
             WHERE id = ? AND tenant_id = ? AND state = 'paused'",
        )
        .bind(&now)
        .bind(&now)
        .bind(destination_instance_id.to_string())
        .bind(tenant_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if instance.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        let handoff = sqlx::query(
            "UPDATE execution_handoffs SET state = ?, version = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND id = ? AND state = ? AND version = ?",
        )
        .bind(state_name(resumed_handoff.state)?)
        .bind(to_i64(resumed_handoff.version, "resumed handoff version")?)
        .bind(encode(resumed_handoff)?)
        .bind(&now)
        .bind(tenant_id.as_str())
        .bind(expected_handoff.id.to_string())
        .bind(state_name(expected_handoff.state)?)
        .bind(to_i64(
            expected_handoff.version,
            "expected handoff version",
        )?)
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if handoff.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }

    async fn save_capsule_manifest(&self, manifest: &CapsuleManifest) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO execution_capsules (id, continuity_id, tenant_id, expires_at, manifest)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(manifest.capsule_id.to_string())
        .bind(manifest.continuity_id.to_string())
        .bind(manifest.tenant_id.as_str())
        .bind(manifest.expires_at.to_rfc3339())
        .bind(encode(manifest)?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_capsule_manifest(
        &self,
        tenant_id: &TenantId,
        id: CapsuleId,
    ) -> Result<Option<CapsuleManifest>, StorageError> {
        let row =
            sqlx::query("SELECT manifest FROM execution_capsules WHERE tenant_id = ? AND id = ?")
                .bind(tenant_id.as_str())
                .bind(id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("manifest")))
            .transpose()
    }

    async fn import_capsule_instance(
        &self,
        capsule_id: CapsuleId,
        destination_runtime_id: RuntimeId,
        instance: &TaskInstance,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<InstanceId, StorageError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let claim = sqlx::query(
            "INSERT OR IGNORE INTO capsule_imports
             (tenant_id,capsule_id,destination_runtime_id,instance_id,imported_at)
             VALUES (?,?,?,?,?)",
        )
        .bind(instance.tenant_id.as_str())
        .bind(capsule_id.to_string())
        .bind(destination_runtime_id.to_string())
        .bind(instance.id.to_string())
        .bind(instance.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if claim.rows_affected() == 0 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            let row = sqlx::query(
                "SELECT instance_id FROM capsule_imports
                 WHERE tenant_id=? AND capsule_id=? AND destination_runtime_id=?",
            )
            .bind(instance.tenant_id.as_str())
            .bind(capsule_id.to_string())
            .bind(destination_runtime_id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
            let id = row
                .get::<String, _>("instance_id")
                .parse::<uuid::Uuid>()
                .map(InstanceId::from_uuid)
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(id);
        }
        super::instances::bind_instance_insert(
            sqlx::query(super::instances::INSTANCE_INSERT_SQL),
            instance,
        )?
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO checkpoints (id,instance_id,checkpoint_data,created_at)
             VALUES (?,?,?,?)",
        )
        .bind(checkpoint.id.to_string())
        .bind(checkpoint.instance_id.to_string())
        .bind(serde_json::to_string(&checkpoint.checkpoint_data)?)
        .bind(checkpoint.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(instance.id)
    }

    async fn is_capsule_import_instance(
        &self,
        tenant_id: &TenantId,
        capsule_id: CapsuleId,
        destination_runtime_id: RuntimeId,
        instance_id: InstanceId,
    ) -> Result<bool, StorageError> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM capsule_imports
             WHERE tenant_id=? AND capsule_id=? AND destination_runtime_id=?
               AND instance_id=?)",
        )
        .bind(tenant_id.as_str())
        .bind(capsule_id.to_string())
        .bind(destination_runtime_id.to_string())
        .bind(instance_id.to_string())
        .fetch_one(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(exists != 0)
    }

    async fn upsert_runtime_capabilities(
        &self,
        tenant_id: &TenantId,
        capabilities: &RuntimeCapabilities,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO runtime_capabilities
             (tenant_id, runtime_id, observed_at, expires_at, record) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, runtime_id) DO UPDATE SET
             observed_at = excluded.observed_at, expires_at = excluded.expires_at,
             record = excluded.record
             WHERE excluded.observed_at >= runtime_capabilities.observed_at",
        )
        .bind(tenant_id.as_str())
        .bind(capabilities.runtime_id.to_string())
        .bind(capabilities.observed_at.to_rfc3339())
        .bind(capabilities.expires_at.to_rfc3339())
        .bind(encode(capabilities)?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn list_runtime_capabilities(
        &self,
        tenant_id: &TenantId,
        observed_after: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<RuntimeCapabilities>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM runtime_capabilities
             WHERE tenant_id = ? AND expires_at > ? ORDER BY observed_at DESC LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(observed_after.to_rfc3339())
        .bind(i64::from(limit.min(1_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(&row.get::<String, _>("record")))
            .collect()
    }

    async fn create_effect_receipt(&self, receipt: &EffectReceipt) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO effect_receipts
             (id, continuity_id, tenant_id, instance_id, state, record, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(receipt.id.to_string())
        .bind(receipt.continuity_id.to_string())
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.instance_id.to_string())
        .bind(state_name(receipt.state)?)
        .bind(encode(receipt)?)
        .bind(receipt.created_at.to_rfc3339())
        .bind(receipt.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn ensure_effect_receipt(
        &self,
        receipt: &EffectReceipt,
    ) -> Result<EffectReceipt, StorageError> {
        sqlx::query(
            "INSERT OR IGNORE INTO effect_receipts
             (id, continuity_id, tenant_id, instance_id, state, record, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(receipt.id.to_string())
        .bind(receipt.continuity_id.to_string())
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.instance_id.to_string())
        .bind(state_name(receipt.state)?)
        .bind(encode(receipt)?)
        .bind(receipt.created_at.to_rfc3339())
        .bind(receipt.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        self.get_effect_receipt(&receipt.tenant_id, receipt.id)
            .await?
            .ok_or_else(|| StorageError::Query("ensured effect receipt disappeared".into()))
    }

    async fn find_unresolved_effect_receipt(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        instance_id: InstanceId,
        block_id: &orch8_types::ids::BlockId,
    ) -> Result<Option<EffectReceipt>, StorageError> {
        let row = sqlx::query(
            "SELECT record FROM effect_receipts
             WHERE tenant_id=? AND continuity_id=? AND instance_id=?
               AND state IN ('dispatched','unknown')
               AND json_extract(record, '$.block_id')=?
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.to_string())
        .bind(instance_id.to_string())
        .bind(block_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn get_effect_receipt(
        &self,
        tenant_id: &TenantId,
        id: EffectId,
    ) -> Result<Option<EffectReceipt>, StorageError> {
        let row = sqlx::query("SELECT record FROM effect_receipts WHERE tenant_id = ? AND id = ?")
            .bind(tenant_id.as_str())
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn cas_effect_receipt(
        &self,
        tenant_id: &TenantId,
        id: EffectId,
        expected_state: EffectState,
        next: &EffectReceipt,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE effect_receipts SET state = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND id = ? AND state = ?",
        )
        .bind(state_name(next.state)?)
        .bind(encode(next)?)
        .bind(next.updated_at.to_rfc3339())
        .bind(tenant_id.as_str())
        .bind(id.to_string())
        .bind(state_name(expected_state)?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn dispatch_effect_receipt_at_most_once(
        &self,
        tenant_id: &TenantId,
        next: &EffectReceipt,
    ) -> Result<EffectDispatchOutcome, StorageError> {
        if next.state != EffectState::Dispatched {
            return Err(StorageError::Query(
                "guarded effect dispatch requires a dispatched receipt".into(),
            ));
        }
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let operation = async {
            let rows = sqlx::query(
                "SELECT record FROM effect_receipts
                 WHERE tenant_id = ? AND continuity_id = ?
                   AND (id = ? OR (
                     json_extract(record, '$.kind') = ?
                     AND json_extract(record, '$.destination_fingerprint') = ?
                     AND json_extract(record, '$.request_sha256') = ?
                   ))
                 ORDER BY created_at, id",
            )
            .bind(tenant_id.as_str())
            .bind(next.continuity_id.to_string())
            .bind(next.id.to_string())
            .bind(state_name(next.kind)?)
            .bind(&next.destination_fingerprint)
            .bind(&next.request_sha256)
            .fetch_all(&mut *conn)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
            let receipts = rows
                .into_iter()
                .map(|row| decode::<EffectReceipt>(&row.get::<String, _>("record")))
                .collect::<Result<Vec<_>, _>>()?;
            let current = receipts.iter().find(|receipt| receipt.id == next.id);
            if current.is_none_or(|receipt| receipt.state != EffectState::Prepared) {
                return Ok(EffectDispatchOutcome::Stale);
            }
            let duplicate = receipts.iter().any(|receipt| {
                receipt.id != next.id
                    && receipt.kind == next.kind
                    && receipt.destination_fingerprint == next.destination_fingerprint
                    && receipt.request_sha256 == next.request_sha256
                    && !matches!(
                        receipt.state,
                        EffectState::Abandoned | EffectState::Compensated
                    )
                    && (matches!(
                        receipt.state,
                        EffectState::Dispatched
                            | EffectState::Committed
                            | EffectState::Unknown
                            | EffectState::Verified
                    ) || (receipt.created_at, receipt.id) < (next.created_at, next.id))
            });
            if duplicate {
                return Ok(EffectDispatchOutcome::Duplicate);
            }
            let updated = sqlx::query(
                "UPDATE effect_receipts SET state = ?, record = ?, updated_at = ?
                 WHERE tenant_id = ? AND id = ? AND state = ?",
            )
            .bind(state_name(next.state)?)
            .bind(encode(next)?)
            .bind(next.updated_at.to_rfc3339())
            .bind(tenant_id.as_str())
            .bind(next.id.to_string())
            .bind(state_name(EffectState::Prepared)?)
            .execute(&mut *conn)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
            Ok(if updated.rows_affected() == 1 {
                EffectDispatchOutcome::Dispatched
            } else {
                EffectDispatchOutcome::Stale
            })
        }
        .await;
        match operation {
            Ok(EffectDispatchOutcome::Dispatched) => {
                sqlx::query("COMMIT")
                    .execute(&mut *conn)
                    .await
                    .map_err(|error| StorageError::Query(error.to_string()))?;
                Ok(EffectDispatchOutcome::Dispatched)
            }
            Ok(outcome) => {
                sqlx::query("ROLLBACK")
                    .execute(&mut *conn)
                    .await
                    .map_err(|error| StorageError::Query(error.to_string()))?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(error)
            }
        }
    }

    async fn list_effect_receipts(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<EffectReceipt>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM effect_receipts WHERE tenant_id = ? AND continuity_id = ?
             ORDER BY created_at, id LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.to_string())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(&row.get::<String, _>("record")))
            .collect()
    }

    async fn append_provenance(&self, entry: &ProvenanceEntry) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO provenance_entries
             (id, continuity_id, tenant_id, epoch, entry_sha256, previous_sha256, record, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(entry.id.to_string())
        .bind(entry.continuity_id.to_string())
        .bind(entry.tenant_id.as_str())
        .bind(to_i64(entry.epoch.get(), "provenance epoch")?)
        .bind(&entry.entry_sha256)
        .bind(&entry.previous_sha256)
        .bind(encode(entry)?)
        .bind(entry.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn list_provenance(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<ProvenanceEntry>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM provenance_entries WHERE tenant_id = ? AND continuity_id = ?
             ORDER BY epoch, created_at, id LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.to_string())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(&row.get::<String, _>("record")))
            .collect()
    }

    async fn create_continuation_grant(
        &self,
        grant: &ContinuationGrant,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO continuation_grants
             (id, tenant_id, continuity_id, state, nonce_sha256, expires_at, consumed_at, record)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(grant.id.to_string())
        .bind(grant.tenant_id.as_str())
        .bind(grant.continuity_id.to_string())
        .bind(state_name(grant.state)?)
        .bind(&grant.nonce_sha256)
        .bind(grant.expires_at.to_rfc3339())
        .bind(grant.consumed_at.map(|value| value.to_rfc3339()))
        .bind(encode(grant)?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_continuation_grant(
        &self,
        tenant_id: &TenantId,
        id: ContinuationGrantId,
    ) -> Result<Option<ContinuationGrant>, StorageError> {
        let row =
            sqlx::query("SELECT record FROM continuation_grants WHERE tenant_id = ? AND id = ?")
                .bind(tenant_id.as_str())
                .bind(id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn consume_continuation_grant(
        &self,
        tenant_id: &TenantId,
        id: ContinuationGrantId,
        nonce_sha256: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let now = now.to_rfc3339();
        let result = sqlx::query(
            "UPDATE continuation_grants
             SET state = 'consumed', consumed_at = ?,
                 record = json_set(record, '$.state', 'consumed', '$.consumed_at', ?)
             WHERE tenant_id = ? AND id = ? AND nonce_sha256 = ?
               AND state = 'active' AND expires_at > ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(tenant_id.as_str())
        .bind(id.to_string())
        .bind(nonce_sha256)
        .bind(&now)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn cas_continuation_grant_state(
        &self,
        tenant_id: &TenantId,
        id: ContinuationGrantId,
        expected: ContinuationGrantState,
        next: &ContinuationGrant,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE continuation_grants SET state = ?, consumed_at = ?, record = ?
             WHERE tenant_id = ? AND id = ? AND state = ?",
        )
        .bind(state_name(next.state)?)
        .bind(next.consumed_at.map(|value| value.to_rfc3339()))
        .bind(encode(next)?)
        .bind(tenant_id.as_str())
        .bind(id.to_string())
        .bind(state_name(expected)?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn save_placement_decision(
        &self,
        decision: &PlacementDecision,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO placement_decisions
             (id, tenant_id, continuity_id, epoch, selected_runtime_id, record, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(decision.id.to_string())
        .bind(decision.tenant_id.as_str())
        .bind(decision.continuity_id.to_string())
        .bind(to_i64(decision.epoch.get(), "placement epoch")?)
        .bind(decision.selected_runtime_id.map(|value| value.to_string()))
        .bind(encode(decision)?)
        .bind(decision.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_placement_decision(
        &self,
        tenant_id: &TenantId,
        id: PlacementDecisionId,
    ) -> Result<Option<PlacementDecision>, StorageError> {
        let row =
            sqlx::query("SELECT record FROM placement_decisions WHERE tenant_id = ? AND id = ?")
                .bind(tenant_id.as_str())
                .bind(id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn create_continuity_stream(
        &self,
        stream: &ContinuityStream,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO continuity_streams
             (stream_id, tenant_id, continuity_id, epoch, created_at, expires_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(stream.stream_id.to_string())
        .bind(stream.tenant_id.as_str())
        .bind(stream.continuity_id.to_string())
        .bind(to_i64(stream.epoch.get(), "stream epoch")?)
        .bind(stream.created_at.to_rfc3339())
        .bind(stream.expires_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_continuity_stream(
        &self,
        tenant_id: &TenantId,
        stream_id: StreamId,
    ) -> Result<Option<ContinuityStream>, StorageError> {
        let row = sqlx::query(
            "SELECT continuity_id, epoch, created_at, expires_at FROM continuity_streams
             WHERE tenant_id = ? AND stream_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(stream_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| {
            let epoch: i64 = row.get("epoch");
            let epoch = u64::try_from(epoch)
                .map(ExecutionEpoch::from_u64)
                .map_err(|_| StorageError::Query("stored stream epoch is negative".into()))?;
            let continuity_id = row
                .get::<String, _>("continuity_id")
                .parse::<uuid::Uuid>()
                .map(ContinuityId::from_uuid)
                .map_err(|error| StorageError::Query(error.to_string()))?;
            let created_at = row
                .get::<String, _>("created_at")
                .parse::<DateTime<Utc>>()
                .map_err(|error| StorageError::Query(error.to_string()))?;
            let expires_at = row
                .get::<String, _>("expires_at")
                .parse::<DateTime<Utc>>()
                .map_err(|error| StorageError::Query(error.to_string()))?;
            Ok(ContinuityStream {
                stream_id,
                tenant_id: tenant_id.clone(),
                continuity_id,
                epoch,
                created_at,
                expires_at,
            })
        })
        .transpose()
    }

    async fn append_stream_frame(&self, frame: &StreamFrame) -> Result<bool, StorageError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let stream = sqlx::query(
            "UPDATE continuity_streams SET next_sequence = next_sequence + 1
             WHERE stream_id = ? AND tenant_id = ? AND continuity_id = ? AND epoch = ?
               AND next_sequence = ? AND expires_at > ? AND expires_at >= ?",
        )
        .bind(frame.stream_id.to_string())
        .bind(frame.tenant_id.as_str())
        .bind(frame.continuity_id.to_string())
        .bind(to_i64(frame.epoch.get(), "stream epoch")?)
        .bind(to_i64(frame.sequence, "stream sequence")?)
        .bind(frame.created_at.to_rfc3339())
        .bind(frame.expires_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if stream.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO continuity_stream_frames
             (stream_id, sequence, tenant_id, continuity_id, epoch, state,
              checkpoint_sha256, record, created_at, expires_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(frame.stream_id.to_string())
        .bind(to_i64(frame.sequence, "stream sequence")?)
        .bind(frame.tenant_id.as_str())
        .bind(frame.continuity_id.to_string())
        .bind(to_i64(frame.epoch.get(), "stream epoch")?)
        .bind(state_name(frame.state)?)
        .bind(&frame.checkpoint_sha256)
        .bind(encode(frame)?)
        .bind(frame.created_at.to_rfc3339())
        .bind(frame.expires_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }

    async fn list_stream_frames(
        &self,
        tenant_id: &TenantId,
        stream_id: StreamId,
        after_sequence: Option<u64>,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StreamFrame>, StorageError> {
        let after = after_sequence.map_or(-1, |value| i64::try_from(value).unwrap_or(i64::MAX));
        let rows = sqlx::query(
            "SELECT record FROM continuity_stream_frames
             WHERE tenant_id = ? AND stream_id = ? AND sequence > ? AND expires_at > ?
             ORDER BY sequence LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(stream_id.to_string())
        .bind(after)
        .bind(now.to_rfc3339())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(&row.get::<String, _>("record")))
            .collect()
    }

    async fn retract_stream_frames(
        &self,
        tenant_id: &TenantId,
        stream_id: StreamId,
        epoch: ExecutionEpoch,
        after_sequence: u64,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "UPDATE continuity_stream_frames
             SET state = 'retracted', record = json_set(record, '$.state', 'retracted')
             WHERE tenant_id = ? AND stream_id = ? AND epoch = ?
               AND sequence > ? AND state = 'committed'",
        )
        .bind(tenant_id.as_str())
        .bind(stream_id.to_string())
        .bind(to_i64(epoch.get(), "stream epoch")?)
        .bind(to_i64(after_sequence, "stream sequence")?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn save_live_migration(&self, record: &LiveMigrationRecord) -> Result<(), StorageError> {
        let created_at = record.plan.created_at.to_rfc3339();
        sqlx::query(
            "INSERT INTO live_migrations
             (id,tenant_id,continuity_id,state,record,created_at,updated_at)
             VALUES (?,?,?,?,?,?,?)",
        )
        .bind(record.plan.id.to_string())
        .bind(record.plan.tenant_id.as_str())
        .bind(record.plan.continuity_id.to_string())
        .bind(state_name(record.state)?)
        .bind(encode(record)?)
        .bind(&created_at)
        .bind(&created_at)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_live_migration(
        &self,
        tenant_id: &TenantId,
        id: MigrationPlanId,
    ) -> Result<Option<LiveMigrationRecord>, StorageError> {
        let row = sqlx::query("SELECT record FROM live_migrations WHERE tenant_id=? AND id=?")
            .bind(tenant_id.as_str())
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn cas_live_migration(
        &self,
        tenant_id: &TenantId,
        expected: &LiveMigrationRecord,
        next: &LiveMigrationRecord,
    ) -> Result<bool, StorageError> {
        let updated_at = next
            .rolled_back_at
            .or(next.applied_at)
            .unwrap_or(next.plan.created_at)
            .to_rfc3339();
        let result = sqlx::query(
            "UPDATE live_migrations SET state=?, record=?, updated_at=?
             WHERE tenant_id=? AND id=? AND state=?",
        )
        .bind(state_name(next.state)?)
        .bind(encode(next)?)
        .bind(updated_at)
        .bind(tenant_id.as_str())
        .bind(expected.plan.id.to_string())
        .bind(state_name(expected.state)?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    #[allow(clippy::too_many_lines)] // one atomic SQL transaction
    async fn commit_live_migration_transition(
        &self,
        transition: LiveMigrationTransition<'_>,
    ) -> Result<bool, StorageError> {
        let LiveMigrationTransition {
            tenant_id,
            expected_record,
            next_record,
            expected_execution,
            next_execution,
            expected_instance_state,
            next_instance_state,
            next_sequence_id,
            next_context,
            checkpoint,
            forbid_effects_epoch,
        } = transition;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        if let Some(epoch) = forbid_effects_epoch {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT EXISTS(
                   SELECT 1 FROM effect_receipts
                   WHERE tenant_id=? AND continuity_id=?
                     AND CAST(json_extract(record, '$.epoch') AS INTEGER)=?
                     AND state IN ('dispatched','committed','unknown','verified'))",
            )
            .bind(tenant_id.as_str())
            .bind(expected_execution.continuity_id.to_string())
            .bind(to_i64(epoch.get(), "effect epoch")?)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
            if blocked != 0 {
                tx.rollback()
                    .await
                    .map_err(|error| StorageError::Query(error.to_string()))?;
                return Ok(false);
            }
        }
        let updated_at = next_execution.updated_at.to_rfc3339();
        let migration = sqlx::query(
            "UPDATE live_migrations SET state=?, record=?, updated_at=?
             WHERE tenant_id=? AND id=? AND state=?",
        )
        .bind(state_name(next_record.state)?)
        .bind(encode(next_record)?)
        .bind(&updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_record.plan.id.to_string())
        .bind(state_name(expected_record.state)?)
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        let continuity = sqlx::query(
            "UPDATE continuity_executions
             SET epoch=?, owner_runtime_id=?, state=?, record=?, updated_at=?
             WHERE tenant_id=? AND continuity_id=? AND epoch=? AND owner_runtime_id=?",
        )
        .bind(to_i64(next_execution.epoch.get(), "next epoch")?)
        .bind(next_execution.owner_runtime_id.to_string())
        .bind(state_name(next_execution.state)?)
        .bind(encode(next_execution)?)
        .bind(&updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_execution.continuity_id.to_string())
        .bind(to_i64(expected_execution.epoch.get(), "expected epoch")?)
        .bind(expected_execution.owner_runtime_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        let instance = sqlx::query(
            "UPDATE task_instances
             SET sequence_id=?, context=?, state=?, next_fire_at=NULL, updated_at=?
             WHERE id=? AND tenant_id=? AND state=?",
        )
        .bind(next_sequence_id.to_string())
        .bind(serde_json::to_string(next_context).map_err(StorageError::Serialization)?)
        .bind(state_name(next_instance_state)?)
        .bind(&updated_at)
        .bind(expected_execution.current_instance_id.to_string())
        .bind(tenant_id.as_str())
        .bind(state_name(expected_instance_state)?)
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if migration.rows_affected() != 1
            || continuity.rows_affected() != 1
            || instance.rows_affected() != 1
        {
            tx.rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        sqlx::query("DELETE FROM execution_tree WHERE instance_id=?")
            .bind(expected_execution.current_instance_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO checkpoints (id,instance_id,checkpoint_data,created_at)
             VALUES (?,?,?,?)",
        )
        .bind(checkpoint.id.to_string())
        .bind(checkpoint.instance_id.to_string())
        .bind(
            serde_json::to_string(&checkpoint.checkpoint_data)
                .map_err(StorageError::Serialization)?,
        )
        .bind(checkpoint.created_at.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        let location = ContinuityLocation {
            continuity_id: next_execution.continuity_id,
            tenant_id: next_execution.tenant_id.clone(),
            epoch: next_execution.epoch,
            runtime_id: next_execution.owner_runtime_id,
            instance_id: next_execution.current_instance_id,
            handoff_id: None,
            entered_at: next_execution.updated_at,
        };
        sqlx::query(
            "INSERT INTO continuity_locations
             (tenant_id,continuity_id,epoch,runtime_id,instance_id,handoff_id,record,entered_at)
             VALUES (?,?,?,?,?,?,?,?)",
        )
        .bind(tenant_id.as_str())
        .bind(next_execution.continuity_id.to_string())
        .bind(to_i64(next_execution.epoch.get(), "location epoch")?)
        .bind(next_execution.owner_runtime_id.to_string())
        .bind(next_execution.current_instance_id.to_string())
        .bind(Option::<String>::None)
        .bind(encode(&location)?)
        .bind(&updated_at)
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        tx.commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }
}
