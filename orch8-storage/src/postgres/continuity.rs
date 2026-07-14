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
use orch8_types::continuity_advanced::{
    CompensationRunId, CompensationRunRecord, LiveMigrationRecord, MigrationPlanId,
};
use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::instance::TaskInstance;

use super::PostgresStorage;
use crate::LiveMigrationTransition;

fn encode<T: Serialize>(value: &T) -> Result<serde_json::Value, StorageError> {
    serde_json::to_value(value).map_err(StorageError::Serialization)
}

fn decode<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(StorageError::Serialization)
}

fn state_name<T: Serialize>(value: T) -> Result<String, StorageError> {
    encode(&value)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| StorageError::Query("state did not serialize as a string".into()))
}

fn to_i64(value: u64, field: &str) -> Result<i64, StorageError> {
    i64::try_from(value)
        .map_err(|_| StorageError::Query(format!("{field} exceeds PostgreSQL BIGINT range")))
}

#[async_trait]
impl crate::ContinuityStore for PostgresStorage {
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
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(execution.continuity_id.into_uuid())
        .bind(execution.tenant_id.as_str())
        .bind(to_i64(execution.epoch.get(), "epoch")?)
        .bind(execution.owner_runtime_id.into_uuid())
        .bind(state_name(execution.state)?)
        .bind(encode(execution)?)
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO continuity_locations
             (tenant_id, continuity_id, epoch, runtime_id, instance_id, handoff_id, record, entered_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(execution.tenant_id.as_str())
        .bind(execution.continuity_id.into_uuid())
        .bind(to_i64(execution.epoch.get(), "location epoch")?)
        .bind(execution.owner_runtime_id.into_uuid())
        .bind(execution.current_instance_id.into_uuid())
        .bind(Option::<uuid::Uuid>::None)
        .bind(encode(&location)?)
        .bind(execution.updated_at)
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
            "SELECT record FROM continuity_executions WHERE tenant_id = $1 AND continuity_id = $2",
        )
        .bind(tenant_id.as_str())
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
    }

    async fn get_continuity_execution_by_instance(
        &self,
        tenant_id: &TenantId,
        instance_id: InstanceId,
    ) -> Result<Option<ContinuityExecution>, StorageError> {
        let row = sqlx::query(
            "SELECT record FROM continuity_executions
             WHERE tenant_id = $1 AND record->>'current_instance_id' = $2",
        )
        .bind(tenant_id.as_str())
        .bind(instance_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
    }

    async fn list_continuity_locations(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<ContinuityLocation>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM continuity_locations
             WHERE tenant_id = $1 AND continuity_id = $2
             ORDER BY epoch ASC LIMIT $3",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.into_uuid())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(row.get("record")))
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
             SET epoch = $1, owner_runtime_id = $2, state = $3, record = $4, updated_at = $5
             WHERE tenant_id = $6 AND continuity_id = $7 AND epoch = $8
               AND owner_runtime_id = $9",
        )
        .bind(to_i64(next.epoch.get(), "next epoch")?)
        .bind(next.owner_runtime_id.into_uuid())
        .bind(state_name(next.state)?)
        .bind(encode(next)?)
        .bind(next.updated_at)
        .bind(tenant_id.as_str())
        .bind(id.into_uuid())
        .bind(to_i64(expected_epoch.get(), "expected epoch")?)
        .bind(expected_owner.into_uuid())
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
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(tenant_id.as_str())
            .bind(next.continuity_id.into_uuid())
            .bind(to_i64(next.epoch.get(), "location epoch")?)
            .bind(next.owner_runtime_id.into_uuid())
            .bind(next.current_instance_id.into_uuid())
            .bind(Option::<uuid::Uuid>::None)
            .bind(encode(&location)?)
            .bind(next.updated_at)
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
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(handoff.id.into_uuid())
        .bind(handoff.continuity_id.into_uuid())
        .bind(handoff.tenant_id.as_str())
        .bind(state_name(handoff.state)?)
        .bind(to_i64(handoff.version, "handoff version")?)
        .bind(encode(handoff)?)
        .bind(handoff.created_at)
        .bind(handoff.updated_at)
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
            sqlx::query("SELECT record FROM execution_handoffs WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id.as_str())
                .bind(id.into_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
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
            "UPDATE execution_handoffs SET state = $1, version = $2, record = $3, updated_at = $4
             WHERE tenant_id = $5 AND id = $6 AND state = $7 AND version = $8",
        )
        .bind(state_name(next.state)?)
        .bind(to_i64(next.version, "next handoff version")?)
        .bind(encode(next)?)
        .bind(next.updated_at)
        .bind(tenant_id.as_str())
        .bind(id.into_uuid())
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
             SET epoch = $1, owner_runtime_id = $2, state = $3, record = $4, updated_at = $5
             WHERE tenant_id = $6 AND continuity_id = $7 AND epoch = $8
               AND owner_runtime_id = $9 AND state = $10",
        )
        .bind(to_i64(accepted_execution.epoch.get(), "accepted epoch")?)
        .bind(accepted_execution.owner_runtime_id.into_uuid())
        .bind(state_name(accepted_execution.state)?)
        .bind(encode(accepted_execution)?)
        .bind(accepted_execution.updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_execution.continuity_id.into_uuid())
        .bind(to_i64(expected_execution.epoch.get(), "expected epoch")?)
        .bind(expected_execution.owner_runtime_id.into_uuid())
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
            "UPDATE execution_handoffs SET state = $1, version = $2, record = $3, updated_at = $4
             WHERE tenant_id = $5 AND id = $6 AND state = $7 AND version = $8",
        )
        .bind(state_name(accepted_handoff.state)?)
        .bind(to_i64(
            accepted_handoff.version,
            "accepted handoff version",
        )?)
        .bind(encode(accepted_handoff)?)
        .bind(accepted_handoff.updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_handoff.id.into_uuid())
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
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(tenant_id.as_str())
        .bind(accepted_execution.continuity_id.into_uuid())
        .bind(to_i64(accepted_execution.epoch.get(), "location epoch")?)
        .bind(accepted_execution.owner_runtime_id.into_uuid())
        .bind(accepted_execution.current_instance_id.into_uuid())
        .bind(accepted_handoff.id.into_uuid())
        .bind(encode(&location)?)
        .bind(accepted_execution.updated_at)
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
             SET state = $1, record = $2, updated_at = $3
             WHERE tenant_id = $4 AND continuity_id = $5 AND epoch = $6
               AND owner_runtime_id = $7 AND state = $8",
        )
        .bind(state_name(transferring_execution.state)?)
        .bind(encode(transferring_execution)?)
        .bind(transferring_execution.updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_execution.continuity_id.into_uuid())
        .bind(to_i64(expected_execution.epoch.get(), "expected epoch")?)
        .bind(expected_execution.owner_runtime_id.into_uuid())
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
            "UPDATE execution_handoffs SET state = $1, version = $2, record = $3, updated_at = $4
             WHERE tenant_id = $5 AND id = $6 AND state = $7 AND version = $8",
        )
        .bind(state_name(exported_handoff.state)?)
        .bind(to_i64(
            exported_handoff.version,
            "exported handoff version",
        )?)
        .bind(encode(exported_handoff)?)
        .bind(exported_handoff.updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_handoff.id.into_uuid())
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
        let instance = sqlx::query(
            "UPDATE task_instances SET state = 'scheduled', next_fire_at = $1, updated_at = $1
             WHERE id = $2 AND tenant_id = $3 AND state = 'paused'",
        )
        .bind(resumed_handoff.updated_at)
        .bind(destination_instance_id.into_uuid())
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
            "UPDATE execution_handoffs SET state = $1, version = $2, record = $3, updated_at = $4
             WHERE tenant_id = $5 AND id = $6 AND state = $7 AND version = $8",
        )
        .bind(state_name(resumed_handoff.state)?)
        .bind(to_i64(resumed_handoff.version, "resumed handoff version")?)
        .bind(encode(resumed_handoff)?)
        .bind(resumed_handoff.updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_handoff.id.into_uuid())
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
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(manifest.capsule_id.into_uuid())
        .bind(manifest.continuity_id.into_uuid())
        .bind(manifest.tenant_id.as_str())
        .bind(manifest.expires_at)
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
            sqlx::query("SELECT manifest FROM execution_capsules WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id.as_str())
                .bind(id.into_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("manifest"))).transpose()
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
            "INSERT INTO capsule_imports
             (tenant_id,capsule_id,destination_runtime_id,instance_id,imported_at)
             VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
        )
        .bind(instance.tenant_id.as_str())
        .bind(capsule_id.into_uuid())
        .bind(destination_runtime_id.into_uuid())
        .bind(instance.id.into_uuid())
        .bind(instance.created_at)
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
                 WHERE tenant_id=$1 AND capsule_id=$2 AND destination_runtime_id=$3",
            )
            .bind(instance.tenant_id.as_str())
            .bind(capsule_id.into_uuid())
            .bind(destination_runtime_id.into_uuid())
            .fetch_one(&self.pool)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(InstanceId::from_uuid(row.get("instance_id")));
        }
        let context = serde_json::to_value(&instance.context)?;
        super::instances::bind_instance_insert(
            sqlx::query(super::instances::INSTANCE_INSERT_SQL),
            instance,
            &context,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO checkpoints (id,instance_id,checkpoint_data,created_at)
             VALUES ($1,$2,$3,$4)",
        )
        .bind(checkpoint.id)
        .bind(checkpoint.instance_id.into_uuid())
        .bind(&checkpoint.checkpoint_data)
        .bind(checkpoint.created_at)
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
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM capsule_imports
             WHERE tenant_id=$1 AND capsule_id=$2 AND destination_runtime_id=$3
               AND instance_id=$4)",
        )
        .bind(tenant_id.as_str())
        .bind(capsule_id.into_uuid())
        .bind(destination_runtime_id.into_uuid())
        .bind(instance_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(exists)
    }

    async fn upsert_runtime_capabilities(
        &self,
        tenant_id: &TenantId,
        capabilities: &RuntimeCapabilities,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO runtime_capabilities
             (tenant_id, runtime_id, observed_at, expires_at, record) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(tenant_id, runtime_id) DO UPDATE SET
             observed_at = EXCLUDED.observed_at, expires_at = EXCLUDED.expires_at,
             record = EXCLUDED.record
             WHERE EXCLUDED.observed_at >= runtime_capabilities.observed_at",
        )
        .bind(tenant_id.as_str())
        .bind(capabilities.runtime_id.into_uuid())
        .bind(capabilities.observed_at)
        .bind(capabilities.expires_at)
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
             WHERE tenant_id = $1 AND expires_at > $2 ORDER BY observed_at DESC LIMIT $3",
        )
        .bind(tenant_id.as_str())
        .bind(observed_after)
        .bind(i64::from(limit.min(1_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(row.get("record")))
            .collect()
    }

    async fn create_effect_receipt(&self, receipt: &EffectReceipt) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO effect_receipts
             (id, continuity_id, tenant_id, instance_id, state, record, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(receipt.id.into_uuid())
        .bind(receipt.continuity_id.into_uuid())
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.instance_id.into_uuid())
        .bind(state_name(receipt.state)?)
        .bind(encode(receipt)?)
        .bind(receipt.created_at)
        .bind(receipt.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn ensure_effect_receipt(
        &self,
        receipt: &EffectReceipt,
    ) -> Result<EffectReceipt, StorageError> {
        let row = sqlx::query(
            "INSERT INTO effect_receipts
             (id, continuity_id, tenant_id, instance_id, state, record, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id
             RETURNING record",
        )
        .bind(receipt.id.into_uuid())
        .bind(receipt.continuity_id.into_uuid())
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.instance_id.into_uuid())
        .bind(state_name(receipt.state)?)
        .bind(encode(receipt)?)
        .bind(receipt.created_at)
        .bind(receipt.updated_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        decode(row.get("record"))
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
             WHERE tenant_id=$1 AND continuity_id=$2 AND instance_id=$3
               AND state IN ('dispatched','unknown')
               AND record->>'block_id'=$4
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.into_uuid())
        .bind(instance_id.into_uuid())
        .bind(block_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
    }

    async fn get_effect_receipt(
        &self,
        tenant_id: &TenantId,
        id: EffectId,
    ) -> Result<Option<EffectReceipt>, StorageError> {
        let row =
            sqlx::query("SELECT record FROM effect_receipts WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id.as_str())
                .bind(id.into_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
    }

    async fn cas_effect_receipt(
        &self,
        tenant_id: &TenantId,
        id: EffectId,
        expected_state: EffectState,
        next: &EffectReceipt,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE effect_receipts SET state = $1, record = $2, updated_at = $3
             WHERE tenant_id = $4 AND id = $5 AND state = $6",
        )
        .bind(state_name(next.state)?)
        .bind(encode(next)?)
        .bind(next.updated_at)
        .bind(tenant_id.as_str())
        .bind(id.into_uuid())
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
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let kind = state_name(next.kind)?;
        let guard_key = format!(
            "effect|{}|{}|{}|{}|{}",
            tenant_id, next.continuity_id, kind, next.destination_fingerprint, next.request_sha256
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(guard_key)
            .execute(&mut *tx)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let rows = sqlx::query(
            "SELECT record FROM effect_receipts
             WHERE tenant_id = $1 AND continuity_id = $2
               AND (id = $3 OR (
                 record->>'kind' = $4
                 AND record->>'destination_fingerprint' = $5
                 AND record->>'request_sha256' = $6
               ))
             ORDER BY created_at, id FOR UPDATE",
        )
        .bind(tenant_id.as_str())
        .bind(next.continuity_id.into_uuid())
        .bind(next.id.into_uuid())
        .bind(kind)
        .bind(&next.destination_fingerprint)
        .bind(&next.request_sha256)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        let receipts = rows
            .into_iter()
            .map(|row| decode::<EffectReceipt>(row.get("record")))
            .collect::<Result<Vec<_>, _>>()?;
        let current = receipts.iter().find(|receipt| receipt.id == next.id);
        if current.is_none_or(|receipt| receipt.state != EffectState::Prepared) {
            tx.rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
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
            tx.rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(EffectDispatchOutcome::Duplicate);
        }
        let updated = sqlx::query(
            "UPDATE effect_receipts SET state = $1, record = $2, updated_at = $3
             WHERE tenant_id = $4 AND id = $5 AND state = $6",
        )
        .bind(state_name(next.state)?)
        .bind(encode(next)?)
        .bind(next.updated_at)
        .bind(tenant_id.as_str())
        .bind(next.id.into_uuid())
        .bind(state_name(EffectState::Prepared)?)
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if updated.rows_affected() != 1 {
            tx.rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(EffectDispatchOutcome::Stale);
        }
        tx.commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(EffectDispatchOutcome::Dispatched)
    }

    async fn list_effect_receipts(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<EffectReceipt>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM effect_receipts WHERE tenant_id = $1 AND continuity_id = $2
             ORDER BY created_at, id LIMIT $3",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.into_uuid())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(row.get("record")))
            .collect()
    }

    async fn append_provenance(&self, entry: &ProvenanceEntry) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO provenance_entries
             (id, continuity_id, tenant_id, epoch, entry_sha256, previous_sha256, record, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(entry.id)
        .bind(entry.continuity_id.into_uuid())
        .bind(entry.tenant_id.as_str())
        .bind(to_i64(entry.epoch.get(), "provenance epoch")?)
        .bind(&entry.entry_sha256)
        .bind(&entry.previous_sha256)
        .bind(encode(entry)?)
        .bind(entry.created_at)
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
            "SELECT record FROM provenance_entries WHERE tenant_id = $1 AND continuity_id = $2
             ORDER BY epoch, created_at, id LIMIT $3",
        )
        .bind(tenant_id.as_str())
        .bind(continuity_id.into_uuid())
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(row.get("record")))
            .collect()
    }

    async fn create_continuation_grant(
        &self,
        grant: &ContinuationGrant,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO continuation_grants
             (id, tenant_id, continuity_id, state, nonce_sha256, expires_at, consumed_at, record)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(grant.id.into_uuid())
        .bind(grant.tenant_id.as_str())
        .bind(grant.continuity_id.into_uuid())
        .bind(state_name(grant.state)?)
        .bind(&grant.nonce_sha256)
        .bind(grant.expires_at)
        .bind(grant.consumed_at)
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
            sqlx::query("SELECT record FROM continuation_grants WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id.as_str())
                .bind(id.into_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
    }

    async fn consume_continuation_grant(
        &self,
        tenant_id: &TenantId,
        id: ContinuationGrantId,
        nonce_sha256: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE continuation_grants
             SET state = 'consumed', consumed_at = $1,
                 record = jsonb_set(jsonb_set(record, '{state}', '\"consumed\"'::jsonb),
                                    '{consumed_at}', to_jsonb($1::timestamptz))
             WHERE tenant_id = $2 AND id = $3 AND nonce_sha256 = $4
               AND state = 'active' AND expires_at > $1",
        )
        .bind(now)
        .bind(tenant_id.as_str())
        .bind(id.into_uuid())
        .bind(nonce_sha256)
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
            "UPDATE continuation_grants SET state = $1, consumed_at = $2, record = $3
             WHERE tenant_id = $4 AND id = $5 AND state = $6",
        )
        .bind(state_name(next.state)?)
        .bind(next.consumed_at)
        .bind(encode(next)?)
        .bind(tenant_id.as_str())
        .bind(id.into_uuid())
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
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(decision.id.into_uuid())
        .bind(decision.tenant_id.as_str())
        .bind(decision.continuity_id.into_uuid())
        .bind(to_i64(decision.epoch.get(), "placement epoch")?)
        .bind(decision.selected_runtime_id.map(RuntimeId::into_uuid))
        .bind(encode(decision)?)
        .bind(decision.created_at)
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
            sqlx::query("SELECT record FROM placement_decisions WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id.as_str())
                .bind(id.into_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
    }

    async fn create_continuity_stream(
        &self,
        stream: &ContinuityStream,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO continuity_streams
             (stream_id, tenant_id, continuity_id, epoch, created_at, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(stream.stream_id.into_uuid())
        .bind(stream.tenant_id.as_str())
        .bind(stream.continuity_id.into_uuid())
        .bind(to_i64(stream.epoch.get(), "stream epoch")?)
        .bind(stream.created_at)
        .bind(stream.expires_at)
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
             WHERE tenant_id = $1 AND stream_id = $2",
        )
        .bind(tenant_id.as_str())
        .bind(stream_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| {
            let epoch: i64 = row.get("epoch");
            let epoch = u64::try_from(epoch)
                .map(ExecutionEpoch::from_u64)
                .map_err(|_| StorageError::Query("stored stream epoch is negative".into()))?;
            Ok(ContinuityStream {
                stream_id,
                tenant_id: tenant_id.clone(),
                continuity_id: ContinuityId::from_uuid(row.get("continuity_id")),
                epoch,
                created_at: row.get("created_at"),
                expires_at: row.get("expires_at"),
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
             WHERE stream_id = $1 AND tenant_id = $2 AND continuity_id = $3 AND epoch = $4
               AND next_sequence = $5 AND expires_at > $6 AND expires_at >= $7",
        )
        .bind(frame.stream_id.into_uuid())
        .bind(frame.tenant_id.as_str())
        .bind(frame.continuity_id.into_uuid())
        .bind(to_i64(frame.epoch.get(), "stream epoch")?)
        .bind(to_i64(frame.sequence, "stream sequence")?)
        .bind(frame.created_at)
        .bind(frame.expires_at)
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
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(frame.stream_id.into_uuid())
        .bind(to_i64(frame.sequence, "stream sequence")?)
        .bind(frame.tenant_id.as_str())
        .bind(frame.continuity_id.into_uuid())
        .bind(to_i64(frame.epoch.get(), "stream epoch")?)
        .bind(state_name(frame.state)?)
        .bind(&frame.checkpoint_sha256)
        .bind(encode(frame)?)
        .bind(frame.created_at)
        .bind(frame.expires_at)
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
             WHERE tenant_id = $1 AND stream_id = $2 AND sequence > $3 AND expires_at > $4
             ORDER BY sequence LIMIT $5",
        )
        .bind(tenant_id.as_str())
        .bind(stream_id.into_uuid())
        .bind(after)
        .bind(now)
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(row.get("record")))
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
             SET state = 'retracted', record = jsonb_set(record, '{state}', '\"retracted\"'::jsonb)
             WHERE tenant_id = $1 AND stream_id = $2 AND epoch = $3
               AND sequence > $4 AND state = 'committed'",
        )
        .bind(tenant_id.as_str())
        .bind(stream_id.into_uuid())
        .bind(to_i64(epoch.get(), "stream epoch")?)
        .bind(to_i64(after_sequence, "stream sequence")?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn save_live_migration(&self, record: &LiveMigrationRecord) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO live_migrations
             (id, tenant_id, continuity_id, state, record, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(record.plan.id.into_uuid())
        .bind(record.plan.tenant_id.as_str())
        .bind(record.plan.continuity_id.into_uuid())
        .bind(state_name(record.state)?)
        .bind(encode(record)?)
        .bind(record.plan.created_at)
        .bind(record.plan.created_at)
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
        let row = sqlx::query("SELECT record FROM live_migrations WHERE tenant_id=$1 AND id=$2")
            .bind(tenant_id.as_str())
            .bind(id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
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
            .unwrap_or(next.plan.created_at);
        let result = sqlx::query(
            "UPDATE live_migrations SET state=$1, record=$2, updated_at=$3
             WHERE tenant_id=$4 AND id=$5 AND state=$6",
        )
        .bind(state_name(next.state)?)
        .bind(encode(next)?)
        .bind(updated_at)
        .bind(tenant_id.as_str())
        .bind(expected.plan.id.into_uuid())
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
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                   SELECT 1 FROM effect_receipts
                   WHERE tenant_id=$1 AND continuity_id=$2
                     AND (record->>'epoch')::bigint=$3
                     AND state IN ('dispatched','committed','unknown','verified'))",
            )
            .bind(tenant_id.as_str())
            .bind(expected_execution.continuity_id.into_uuid())
            .bind(to_i64(epoch.get(), "effect epoch")?)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
            if blocked {
                tx.rollback()
                    .await
                    .map_err(|error| StorageError::Query(error.to_string()))?;
                return Ok(false);
            }
        }
        let migration = sqlx::query(
            "UPDATE live_migrations SET state=$1, record=$2, updated_at=$3
             WHERE tenant_id=$4 AND id=$5 AND state=$6",
        )
        .bind(state_name(next_record.state)?)
        .bind(encode(next_record)?)
        .bind(next_execution.updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_record.plan.id.into_uuid())
        .bind(state_name(expected_record.state)?)
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        let continuity = sqlx::query(
            "UPDATE continuity_executions
             SET epoch=$1, owner_runtime_id=$2, state=$3, record=$4, updated_at=$5
             WHERE tenant_id=$6 AND continuity_id=$7 AND epoch=$8 AND owner_runtime_id=$9",
        )
        .bind(to_i64(next_execution.epoch.get(), "next epoch")?)
        .bind(next_execution.owner_runtime_id.into_uuid())
        .bind(state_name(next_execution.state)?)
        .bind(encode(next_execution)?)
        .bind(next_execution.updated_at)
        .bind(tenant_id.as_str())
        .bind(expected_execution.continuity_id.into_uuid())
        .bind(to_i64(expected_execution.epoch.get(), "expected epoch")?)
        .bind(expected_execution.owner_runtime_id.into_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        let instance = sqlx::query(
            "UPDATE task_instances
             SET sequence_id=$1, context=$2, state=$3, next_fire_at=NULL, updated_at=$4
             WHERE id=$5 AND tenant_id=$6 AND state=$7",
        )
        .bind(next_sequence_id.into_uuid())
        .bind(serde_json::to_value(next_context).map_err(StorageError::Serialization)?)
        .bind(state_name(next_instance_state)?)
        .bind(next_execution.updated_at)
        .bind(expected_execution.current_instance_id.into_uuid())
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
        sqlx::query("DELETE FROM execution_tree WHERE instance_id=$1")
            .bind(expected_execution.current_instance_id.into_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO checkpoints (id,instance_id,checkpoint_data,created_at)
             VALUES ($1,$2,$3,$4)",
        )
        .bind(checkpoint.id)
        .bind(checkpoint.instance_id.into_uuid())
        .bind(&checkpoint.checkpoint_data)
        .bind(checkpoint.created_at)
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
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(tenant_id.as_str())
        .bind(next_execution.continuity_id.into_uuid())
        .bind(to_i64(next_execution.epoch.get(), "location epoch")?)
        .bind(next_execution.owner_runtime_id.into_uuid())
        .bind(next_execution.current_instance_id.into_uuid())
        .bind(Option::<uuid::Uuid>::None)
        .bind(encode(&location)?)
        .bind(next_execution.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        tx.commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }

    async fn create_compensation_run(
        &self,
        run: &CompensationRunRecord,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "INSERT INTO compensation_runs
             (id,tenant_id,continuity_id,state,version,record,created_at,updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT DO NOTHING",
        )
        .bind(run.id.into_uuid())
        .bind(run.tenant_id.as_str())
        .bind(run.continuity_id.into_uuid())
        .bind(state_name(run.state)?)
        .bind(to_i64(run.version, "compensation version")?)
        .bind(encode(run)?)
        .bind(run.created_at)
        .bind(run.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn get_compensation_run(
        &self,
        tenant_id: &TenantId,
        id: CompensationRunId,
    ) -> Result<Option<CompensationRunRecord>, StorageError> {
        let row = sqlx::query("SELECT record FROM compensation_runs WHERE tenant_id=$1 AND id=$2")
            .bind(tenant_id.as_str())
            .bind(id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(row.get("record"))).transpose()
    }

    async fn cas_compensation_run(
        &self,
        tenant_id: &TenantId,
        expected_version: u64,
        next: &CompensationRunRecord,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE compensation_runs SET state=$1,version=$2,record=$3,updated_at=$4
             WHERE tenant_id=$5 AND id=$6 AND version=$7",
        )
        .bind(state_name(next.state)?)
        .bind(to_i64(next.version, "compensation version")?)
        .bind(encode(next)?)
        .bind(next.updated_at)
        .bind(tenant_id.as_str())
        .bind(next.id.into_uuid())
        .bind(to_i64(expected_version, "expected compensation version")?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }
}
