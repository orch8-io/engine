//! Workflow release storage (release control plane) — SQLite.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::release::{ReleaseDecision, ReleaseState, WorkflowRelease};

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

fn parse_uuid(s: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(s).map_err(|e| StorageError::Query(e.to_string()))
}

fn row_to_release(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowRelease, StorageError> {
    let state: String = row.get("state");
    let in_flight: String = row.get("in_flight_policy");
    let gates: String = row.get("gates");
    let summary: Option<String> = row.get("validation_summary");
    let canary_started: Option<String> = row.get("canary_started_at");
    Ok(WorkflowRelease {
        id: parse_uuid(row.get::<&str, _>("id"))?,
        tenant_id: TenantId::unchecked(row.get::<String, _>("tenant_id")),
        namespace: Namespace::new(row.get::<String, _>("namespace")),
        sequence_name: row.get("sequence_name"),
        baseline_sequence_id: SequenceId::from_uuid(parse_uuid(
            row.get::<&str, _>("baseline_sequence_id"),
        )?),
        baseline_version: row.get("baseline_version"),
        candidate_sequence_id: SequenceId::from_uuid(parse_uuid(
            row.get::<&str, _>("candidate_sequence_id"),
        )?),
        candidate_version: row.get("candidate_version"),
        state: ReleaseState::from_str(&state).map_err(StorageError::Query)?,
        canary_percent: u8::try_from(row.get::<i64, _>("canary_percent")).unwrap_or(0),
        gates: serde_json::from_str(&gates)?,
        in_flight_policy: serde_json::from_value(serde_json::Value::String(in_flight))?,
        validation_summary: summary.as_deref().map(serde_json::from_str).transpose()?,
        canary_started_at: canary_started.as_deref().map(parse_ts).transpose()?,
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
        updated_at: parse_ts(row.get::<&str, _>("updated_at"))?,
    })
}

const RELEASE_COLUMNS: &str = "id, tenant_id, namespace, sequence_name, baseline_sequence_id, \
     baseline_version, candidate_sequence_id, candidate_version, state, canary_percent, gates, \
     in_flight_policy, validation_summary, canary_started_at, created_at, updated_at";

pub(super) async fn create(
    storage: &SqliteStorage,
    release: &WorkflowRelease,
) -> Result<(), StorageError> {
    let policy = serde_json::to_value(release.in_flight_policy)
        .ok()
        .and_then(|v| v.as_str().map(ToString::to_string))
        .unwrap_or_else(|| "pin".to_string());
    sqlx::query(
        "INSERT INTO workflow_releases
            (id, tenant_id, namespace, sequence_name, baseline_sequence_id, baseline_version,
             candidate_sequence_id, candidate_version, state, canary_percent, gates,
             in_flight_policy, validation_summary, canary_started_at, created_at, updated_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
    )
    .bind(release.id.to_string())
    .bind(release.tenant_id.as_str())
    .bind(release.namespace.as_str())
    .bind(&release.sequence_name)
    .bind(release.baseline_sequence_id.as_uuid().to_string())
    .bind(release.baseline_version)
    .bind(release.candidate_sequence_id.as_uuid().to_string())
    .bind(release.candidate_version)
    .bind(release.state.as_str())
    .bind(i64::from(release.canary_percent))
    .bind(serde_json::to_string(&release.gates)?)
    .bind(policy)
    .bind(
        release
            .validation_summary
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?,
    )
    .bind(release.canary_started_at.map(ts))
    .bind(ts(release.created_at))
    .bind(ts(release.updated_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<WorkflowRelease>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {RELEASE_COLUMNS} FROM workflow_releases WHERE id = ?1"
    ))
    .bind(id.to_string())
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_release).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<WorkflowRelease>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {RELEASE_COLUMNS} FROM workflow_releases
         WHERE (?1 IS NULL OR tenant_id = ?1)
         ORDER BY created_at DESC LIMIT ?2"
    ))
    .bind(tenant_id.map(orch8_types::ids::TenantId::as_str))
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_release).collect()
}

pub(super) async fn cas_state(
    storage: &SqliteStorage,
    id: Uuid,
    expected: ReleaseState,
    next: ReleaseState,
    canary_percent: Option<u8>,
    canary_started_at: Option<DateTime<Utc>>,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE workflow_releases
         SET state = ?3,
             canary_percent = COALESCE(?4, canary_percent),
             canary_started_at = COALESCE(?5, canary_started_at),
             updated_at = ?6
         WHERE id = ?1 AND state = ?2",
    )
    .bind(id.to_string())
    .bind(expected.as_str())
    .bind(next.as_str())
    .bind(canary_percent.map(i64::from))
    .bind(canary_started_at.map(ts))
    .bind(ts(Utc::now()))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn set_validation_summary(
    storage: &SqliteStorage,
    id: Uuid,
    summary: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE workflow_releases SET validation_summary = ?2, updated_at = ?3 WHERE id = ?1",
    )
    .bind(id.to_string())
    .bind(serde_json::to_string(summary)?)
    .bind(ts(Utc::now()))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn record_decision(
    storage: &SqliteStorage,
    decision: &ReleaseDecision,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO release_decisions
            (id, release_id, from_state, to_state, actor, reason, decided_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
    )
    .bind(decision.id.to_string())
    .bind(decision.release_id.to_string())
    .bind(decision.from_state.as_str())
    .bind(decision.to_state.as_str())
    .bind(&decision.actor)
    .bind(&decision.reason)
    .bind(ts(decision.decided_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_decisions(
    storage: &SqliteStorage,
    release_id: Uuid,
) -> Result<Vec<ReleaseDecision>, StorageError> {
    let rows = sqlx::query(
        "SELECT id, release_id, from_state, to_state, actor, reason, decided_at
         FROM release_decisions WHERE release_id = ?1 ORDER BY decided_at ASC",
    )
    .bind(release_id.to_string())
    .fetch_all(&storage.pool)
    .await?;
    rows.iter()
        .map(|row| {
            let from: String = row.get("from_state");
            let to: String = row.get("to_state");
            Ok(ReleaseDecision {
                id: parse_uuid(row.get::<&str, _>("id"))?,
                release_id: parse_uuid(row.get::<&str, _>("release_id"))?,
                from_state: ReleaseState::from_str(&from).map_err(StorageError::Query)?,
                to_state: ReleaseState::from_str(&to).map_err(StorageError::Query)?,
                actor: row.get("actor"),
                reason: row.get("reason"),
                decided_at: parse_ts(row.get::<&str, _>("decided_at"))?,
            })
        })
        .collect()
}

pub(super) async fn find_routing(
    storage: &SqliteStorage,
    baseline_sequence_id: SequenceId,
) -> Result<Option<WorkflowRelease>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {RELEASE_COLUMNS} FROM workflow_releases
         WHERE baseline_sequence_id = ?1 AND state IN ('canary', 'promoted')
         ORDER BY created_at DESC LIMIT 1"
    ))
    .bind(baseline_sequence_id.as_uuid().to_string())
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_release).transpose()
}
