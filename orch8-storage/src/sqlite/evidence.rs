use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::Row;

use orch8_types::continuity::ContinuityId;
use orch8_types::continuity_advanced::{
    AttentionTask, AttentionTaskId, BudgetReservation, EvaluationScore, InvariantResult,
    WhatIfRunRecord, WorkflowInvariant,
};
use orch8_types::error::StorageError;
use orch8_types::ids::{SequenceId, TenantId};
use orch8_types::instance::{Budget, BudgetUsage};

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

#[async_trait]
impl crate::InvariantStore for SqliteStorage {
    async fn create_workflow_invariant(
        &self,
        invariant: &WorkflowInvariant,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO workflow_invariants
             (id, tenant_id, sequence_id, sequence_version, enabled, record, created_at)
             VALUES (?,?,?,?,?,?,?)",
        )
        .bind(invariant.id.to_string())
        .bind(invariant.tenant_id.as_str())
        .bind(invariant.sequence_id.to_string())
        .bind(invariant.sequence_version)
        .bind(invariant.enabled)
        .bind(encode(invariant)?)
        .bind(invariant.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn list_workflow_invariants(
        &self,
        tenant_id: &TenantId,
        sequence_id: SequenceId,
        sequence_version: i32,
        limit: u32,
    ) -> Result<Vec<WorkflowInvariant>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM workflow_invariants
             WHERE tenant_id = ? AND sequence_id = ? AND enabled = 1
               AND (sequence_version IS NULL OR sequence_version = ?)
             ORDER BY created_at, id LIMIT ?",
        )
        .bind(tenant_id.as_str())
        .bind(sequence_id.to_string())
        .bind(sequence_version)
        .bind(i64::from(limit.min(10_000)))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|row| decode(&row.get::<String, _>("record")))
            .collect()
    }

    async fn append_invariant_result(
        &self,
        tenant_id: &TenantId,
        result: &InvariantResult,
    ) -> Result<bool, StorageError> {
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO invariant_results
             (id,invariant_id,tenant_id,continuity_id,epoch,status,dedupe_key,record,evaluated_at)
             VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(result.id.to_string())
        .bind(result.invariant_id.to_string())
        .bind(tenant_id.as_str())
        .bind(result.continuity_id.to_string())
        .bind(
            i64::try_from(result.epoch.get())
                .map_err(|_| StorageError::Query("invariant epoch exceeds INTEGER".into()))?,
        )
        .bind(state_name(result.status)?)
        .bind(&result.dedupe_key)
        .bind(encode(result)?)
        .bind(result.evaluated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(inserted.rows_affected() == 1)
    }

    async fn list_invariant_results(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<InvariantResult>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM invariant_results WHERE tenant_id=? AND continuity_id=?
             ORDER BY evaluated_at DESC, id DESC LIMIT ?",
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

    async fn save_what_if_run(&self, run: &WhatIfRunRecord) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO what_if_runs (id,tenant_id,continuity_id,record,created_at)
             VALUES (?,?,?,?,?)",
        )
        .bind(run.scenario.id.to_string())
        .bind(run.scenario.tenant_id.as_str())
        .bind(run.scenario.source.continuity_id.to_string())
        .bind(encode(run)?)
        .bind(run.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn list_what_if_runs(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<WhatIfRunRecord>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM what_if_runs WHERE tenant_id=? AND continuity_id=?
             ORDER BY created_at DESC, id DESC LIMIT ?",
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

#[async_trait]
impl crate::EvaluationStore for SqliteStorage {
    async fn append_evaluation_score(&self, score: &EvaluationScore) -> Result<bool, StorageError> {
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO evaluation_scores
             (id,tenant_id,continuity_id,dedupe_key,deferred,record,created_at)
             VALUES (?,?,?,?,?,?,?)",
        )
        .bind(score.id.to_string())
        .bind(score.tenant_id.as_str())
        .bind(score.continuity_id.to_string())
        .bind(&score.dedupe_key)
        .bind(score.deferred)
        .bind(encode(score)?)
        .bind(score.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(inserted.rows_affected() == 1)
    }

    async fn list_evaluation_scores(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<EvaluationScore>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM evaluation_scores WHERE tenant_id=? AND continuity_id=?
             ORDER BY created_at DESC, id DESC LIMIT ?",
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

#[async_trait]
impl crate::AttentionStore for SqliteStorage {
    async fn create_attention_task(&self, task: &AttentionTask) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO attention_tasks
             (id,tenant_id,continuity_id,state,assignee,lease_expires_at,deadline,record)
             VALUES (?,?,?,?,?,?,?,?)",
        )
        .bind(task.id.to_string())
        .bind(task.tenant_id.as_str())
        .bind(task.continuity_id.to_string())
        .bind(state_name(task.state)?)
        .bind(&task.assignee)
        .bind(task.lease_expires_at.map(|value| value.to_rfc3339()))
        .bind(task.deadline.to_rfc3339())
        .bind(encode(task)?)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    async fn get_attention_task(
        &self,
        tenant_id: &TenantId,
        id: AttentionTaskId,
    ) -> Result<Option<AttentionTask>, StorageError> {
        let row = sqlx::query("SELECT record FROM attention_tasks WHERE tenant_id=? AND id=?")
            .bind(tenant_id.as_str())
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|row| decode(&row.get::<String, _>("record")))
            .transpose()
    }

    async fn claim_attention_task(
        &self,
        tenant_id: &TenantId,
        expected: &AttentionTask,
        assigned: &AttentionTask,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let updated = sqlx::query(
            "UPDATE attention_tasks SET state=?, assignee=?, lease_expires_at=?, record=?
             WHERE tenant_id=? AND id=? AND state=? AND deadline>?",
        )
        .bind(state_name(assigned.state)?)
        .bind(&assigned.assignee)
        .bind(assigned.lease_expires_at.map(|value| value.to_rfc3339()))
        .bind(encode(assigned)?)
        .bind(tenant_id.as_str())
        .bind(expected.id.to_string())
        .bind(state_name(expected.state)?)
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(updated.rows_affected() == 1)
    }

    async fn reserve_budget(
        &self,
        reservation: &BudgetReservation,
        budget: &Budget,
    ) -> Result<bool, StorageError> {
        let usage = reservation.requested;
        if usage_values(usage).iter().any(|value| *value < 0) {
            return Ok(false);
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        let locked = sqlx::query(
            "UPDATE continuity_executions SET updated_at=updated_at
             WHERE tenant_id=? AND continuity_id=? AND epoch=?",
        )
        .bind(reservation.tenant_id.as_str())
        .bind(reservation.continuity_id.to_string())
        .bind(
            i64::try_from(reservation.epoch.get())
                .map_err(|_| StorageError::Query("budget epoch exceeds INTEGER".into()))?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        if locked.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        let row = sqlx::query(
            "SELECT COALESCE(SUM(cost_microunits),0) cost,
                    COALESCE(SUM(wall_time_ms),0) wall,
                    COALESCE(SUM(external_calls),0) calls,
                    COALESCE(SUM(bytes_transferred),0) bytes,
                    COALESCE(SUM(energy_millijoules),0) energy,
                    COALESCE(SUM(attention_units),0) attention
             FROM budget_reservations
             WHERE tenant_id=? AND continuity_id=? AND epoch=? AND state='reserved'",
        )
        .bind(reservation.tenant_id.as_str())
        .bind(reservation.continuity_id.to_string())
        .bind(i64::try_from(reservation.epoch.get()).unwrap_or(i64::MAX))
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        let active = BudgetUsage {
            cost_microunits: row.get("cost"),
            wall_time_ms: row.get("wall"),
            external_calls: row.get("calls"),
            bytes_transferred: row.get("bytes"),
            energy_millijoules: row.get("energy"),
            attention_units: row.get("attention"),
        };
        if budget
            .first_extended_breach(add_usage(active, usage))
            .is_some()
        {
            transaction
                .rollback()
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO budget_reservations
             (id,tenant_id,continuity_id,epoch,state,cost_microunits,wall_time_ms,
              external_calls,bytes_transferred,energy_millijoules,attention_units,record,created_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(reservation.id.to_string())
        .bind(reservation.tenant_id.as_str())
        .bind(reservation.continuity_id.to_string())
        .bind(
            i64::try_from(reservation.epoch.get())
                .map_err(|_| StorageError::Query("budget epoch exceeds INTEGER".into()))?,
        )
        .bind(state_name(reservation.state)?)
        .bind(usage.cost_microunits)
        .bind(usage.wall_time_ms)
        .bind(usage.external_calls)
        .bind(usage.bytes_transferred)
        .bind(usage.energy_millijoules)
        .bind(usage.attention_units)
        .bind(encode(reservation)?)
        .bind(reservation.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(true)
    }
}

fn usage_values(usage: BudgetUsage) -> [i64; 6] {
    [
        usage.cost_microunits,
        usage.wall_time_ms,
        usage.external_calls,
        usage.bytes_transferred,
        usage.energy_millijoules,
        usage.attention_units,
    ]
}

fn add_usage(left: BudgetUsage, right: BudgetUsage) -> BudgetUsage {
    let left = usage_values(left);
    let right = usage_values(right);
    BudgetUsage {
        cost_microunits: left[0].saturating_add(right[0]),
        wall_time_ms: left[1].saturating_add(right[1]),
        external_calls: left[2].saturating_add(right[2]),
        bytes_transferred: left[3].saturating_add(right[3]),
        energy_millijoules: left[4].saturating_add(right[4]),
        attention_units: left[5].saturating_add(right[5]),
    }
}
