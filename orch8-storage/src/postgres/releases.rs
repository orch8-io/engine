//! Workflow release storage (release control plane) — PostgreSQL.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::release::{ReleaseDecision, ReleaseState, WorkflowRelease};

use super::PostgresStorage;

fn row_to_release(row: &sqlx::postgres::PgRow) -> Result<WorkflowRelease, StorageError> {
    let state: String = row.get("state");
    let in_flight: String = row.get("in_flight_policy");
    Ok(WorkflowRelease {
        id: row.get("id"),
        tenant_id: TenantId::unchecked(row.get::<String, _>("tenant_id")),
        namespace: Namespace::new(row.get::<String, _>("namespace")),
        sequence_name: row.get("sequence_name"),
        baseline_sequence_id: SequenceId::from_uuid(row.get("baseline_sequence_id")),
        baseline_version: row.get("baseline_version"),
        candidate_sequence_id: SequenceId::from_uuid(row.get("candidate_sequence_id")),
        candidate_version: row.get("candidate_version"),
        state: ReleaseState::from_str(&state).map_err(StorageError::Query)?,
        canary_percent: u8::try_from(row.get::<i16, _>("canary_percent")).unwrap_or(0),
        gates: serde_json::from_value(row.get("gates"))?,
        in_flight_policy: serde_json::from_value(serde_json::Value::String(in_flight))?,
        validation_summary: row.get("validation_summary"),
        canary_started_at: row.get("canary_started_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

const RELEASE_COLUMNS: &str = "id, tenant_id, namespace, sequence_name, baseline_sequence_id, \
     baseline_version, candidate_sequence_id, candidate_version, state, canary_percent, gates, \
     in_flight_policy, validation_summary, canary_started_at, created_at, updated_at";

pub(super) async fn create(
    store: &PostgresStorage,
    release: &WorkflowRelease,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO workflow_releases
            (id, tenant_id, namespace, sequence_name, baseline_sequence_id, baseline_version,
             candidate_sequence_id, candidate_version, state, canary_percent, gates,
             in_flight_policy, validation_summary, canary_started_at, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
    )
    .bind(release.id)
    .bind(release.tenant_id.as_str())
    .bind(release.namespace.as_str())
    .bind(&release.sequence_name)
    .bind(release.baseline_sequence_id.as_uuid())
    .bind(release.baseline_version)
    .bind(release.candidate_sequence_id.as_uuid())
    .bind(release.candidate_version)
    .bind(release.state.as_str())
    .bind(i16::from(release.canary_percent))
    .bind(serde_json::to_value(&release.gates)?)
    .bind(policy_str(release))
    .bind(&release.validation_summary)
    .bind(release.canary_started_at)
    .bind(release.created_at)
    .bind(release.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

fn policy_str(release: &WorkflowRelease) -> String {
    serde_json::to_value(release.in_flight_policy)
        .ok()
        .and_then(|v| v.as_str().map(ToString::to_string))
        .unwrap_or_else(|| "pin".to_string())
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: Uuid,
) -> Result<Option<WorkflowRelease>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {RELEASE_COLUMNS} FROM workflow_releases WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref().map(row_to_release).transpose()
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<WorkflowRelease>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {RELEASE_COLUMNS} FROM workflow_releases
         WHERE ($1::text IS NULL OR tenant_id = $1)
         ORDER BY created_at DESC LIMIT $2"
    ))
    .bind(tenant_id.map(orch8_types::ids::TenantId::as_str))
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    rows.iter().map(row_to_release).collect()
}

pub(super) async fn cas_state(
    store: &PostgresStorage,
    id: Uuid,
    expected: ReleaseState,
    next: ReleaseState,
    canary_percent: Option<u8>,
    canary_started_at: Option<DateTime<Utc>>,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        r"UPDATE workflow_releases
          SET state = $3,
              canary_percent = COALESCE($4, canary_percent),
              canary_started_at = COALESCE($5, canary_started_at),
              updated_at = NOW()
          WHERE id = $1 AND state = $2",
    )
    .bind(id)
    .bind(expected.as_str())
    .bind(next.as_str())
    .bind(canary_percent.map(i16::from))
    .bind(canary_started_at)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn set_validation_summary(
    store: &PostgresStorage,
    id: Uuid,
    summary: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE workflow_releases SET validation_summary = $2, updated_at = NOW() WHERE id = $1",
    )
    .bind(id)
    .bind(summary)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn record_decision(
    store: &PostgresStorage,
    decision: &ReleaseDecision,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO release_decisions
            (id, release_id, from_state, to_state, actor, reason, decided_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(decision.id)
    .bind(decision.release_id)
    .bind(decision.from_state.as_str())
    .bind(decision.to_state.as_str())
    .bind(&decision.actor)
    .bind(&decision.reason)
    .bind(decision.decided_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_decisions(
    store: &PostgresStorage,
    release_id: Uuid,
) -> Result<Vec<ReleaseDecision>, StorageError> {
    let rows = sqlx::query(
        r"SELECT id, release_id, from_state, to_state, actor, reason, decided_at
          FROM release_decisions WHERE release_id = $1 ORDER BY decided_at ASC",
    )
    .bind(release_id)
    .fetch_all(&store.pool)
    .await?;
    rows.iter()
        .map(|row| {
            let from: String = row.get("from_state");
            let to: String = row.get("to_state");
            Ok(ReleaseDecision {
                id: row.get("id"),
                release_id: row.get("release_id"),
                from_state: ReleaseState::from_str(&from).map_err(StorageError::Query)?,
                to_state: ReleaseState::from_str(&to).map_err(StorageError::Query)?,
                actor: row.get("actor"),
                reason: row.get("reason"),
                decided_at: row.get("decided_at"),
            })
        })
        .collect()
}

pub(super) async fn find_routing(
    store: &PostgresStorage,
    baseline_sequence_id: SequenceId,
) -> Result<Option<WorkflowRelease>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {RELEASE_COLUMNS} FROM workflow_releases
         WHERE baseline_sequence_id = $1 AND state IN ('canary', 'promoted')
         ORDER BY created_at DESC LIMIT 1"
    ))
    .bind(baseline_sequence_id.as_uuid())
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref().map(row_to_release).transpose()
}
