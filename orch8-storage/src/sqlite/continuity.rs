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

use super::SqliteStorage;

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
             SET epoch = ?, owner_runtime_id = ?, state = ?, record = ?, updated_at = ?
             WHERE tenant_id = ? AND continuity_id = ? AND epoch = ? AND owner_runtime_id = ?",
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
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(result.rows_affected() == 1)
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
             WHERE tenant_id = ? AND continuity_id = ? AND epoch = ? AND owner_runtime_id = ?",
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
}
