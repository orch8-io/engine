use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::Row;

use orch8_types::continuity::{
    CapsuleId, CapsuleManifest, ContinuityExecution, ContinuityId, EffectId, EffectReceipt,
    EffectState, ExecutionEpoch, ExecutionHandoff, HandoffId, HandoffState, ProvenanceEntry,
    RuntimeCapabilities, RuntimeId,
};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::PostgresStorage;

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
        .execute(&self.pool)
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

    async fn cas_continuity_owner(
        &self,
        tenant_id: &TenantId,
        id: ContinuityId,
        expected_epoch: ExecutionEpoch,
        expected_owner: RuntimeId,
        next: &ContinuityExecution,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE continuity_executions
             SET epoch = $1, owner_runtime_id = $2, state = $3, record = $4, updated_at = $5
             WHERE tenant_id = $6 AND continuity_id = $7 AND epoch = $8 AND owner_runtime_id = $9",
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
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
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
             WHERE tenant_id = $6 AND continuity_id = $7 AND epoch = $8 AND owner_runtime_id = $9",
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
}
