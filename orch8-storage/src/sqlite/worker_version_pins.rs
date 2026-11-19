use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::worker::{WorkerVersionPin, WorkerVersionRollout, WorkerVersionRolloutAction};

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

/// Read the pin after acquiring the same transaction-level fence as pin writers.
pub(super) async fn admits_claim(
    conn: &mut sqlx::SqliteConnection,
    request: &crate::worker_claim::WorkerClaimRequest,
) -> Result<bool, StorageError> {
    // Shared workers consult only the unscoped pin, even when claiming a tenant's task.
    let tenant_id = request
        .tenant_id
        .as_ref()
        .map_or("", orch8_types::ids::TenantId::as_str);
    let minimum: Option<String> = sqlx::query_scalar(
        "SELECT min_version FROM worker_version_pins WHERE tenant_id=?1 AND handler_name=?2 AND active",
    )
    .bind(tenant_id)
    .bind(&request.handler_name)
    .fetch_optional(conn)
    .await?;
    Ok(orch8_types::worker::version_admits(
        request.version.as_deref(),
        minimum.as_deref(),
    ))
}

fn row_to_pin(row: &sqlx::sqlite::SqliteRow) -> Result<WorkerVersionPin, StorageError> {
    Ok(WorkerVersionPin {
        tenant_id: row.get("tenant_id"),
        handler_name: row.get("handler_name"),
        min_version: row.get("min_version"),
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
        updated_at: parse_ts(row.get::<&str, _>("updated_at"))?,
    })
}

pub(super) async fn upsert(
    storage: &SqliteStorage,
    pin: &WorkerVersionPin,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO worker_version_pins (tenant_id, handler_name, min_version, created_at, updated_at) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(tenant_id, handler_name) DO UPDATE SET min_version=excluded.min_version, updated_at=excluded.updated_at, active=true, staged_min_version=NULL, previous_min_version=NULL, can_rollback=false, rollout_revision=NULL",
    )
    .bind(&pin.tenant_id)
    .bind(&pin.handler_name)
    .bind(&pin.min_version)
    .bind(ts(pin.created_at))
    .bind(ts(pin.updated_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    tenant_id: &str,
    handler_name: &str,
) -> Result<Option<WorkerVersionPin>, StorageError> {
    let row = sqlx::query(
        "SELECT tenant_id, handler_name, min_version, created_at, updated_at FROM worker_version_pins WHERE tenant_id = ?1 AND handler_name = ?2 AND active",
    )
    .bind(tenant_id)
    .bind(handler_name)
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_pin).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
) -> Result<Vec<WorkerVersionPin>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        "SELECT tenant_id, handler_name, min_version, created_at, updated_at FROM worker_version_pins WHERE active",
    );
    if let Some(t) = tenant_id {
        qb.push(" AND tenant_id=").push_bind(t);
    }
    qb.push(" ORDER BY tenant_id, handler_name");
    let rows = qb.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_pin).collect()
}

pub(super) async fn delete(
    storage: &SqliteStorage,
    tenant_id: &str,
    handler_name: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM worker_version_pins WHERE tenant_id = ?1 AND handler_name = ?2")
        .bind(tenant_id)
        .bind(handler_name)
        .execute(&storage.pool)
        .await?;
    Ok(())
}

const ROLLOUT_COLUMNS: &str = "tenant_id, handler_name,
    CASE WHEN active THEN min_version END AS active_min_version,
    staged_min_version, previous_min_version, can_rollback, rollout_revision, updated_at";

fn row_to_rollout(row: &sqlx::sqlite::SqliteRow) -> Result<WorkerVersionRollout, StorageError> {
    Ok(WorkerVersionRollout {
        tenant_id: row.get("tenant_id"),
        handler_name: row.get("handler_name"),
        active_min_version: row.get("active_min_version"),
        staged_min_version: row.get("staged_min_version"),
        previous_min_version: row.get("previous_min_version"),
        can_rollback: row.get("can_rollback"),
        revision: row
            .get::<Option<&str>, _>("rollout_revision")
            .map(uuid::Uuid::parse_str)
            .transpose()
            .map_err(|error| StorageError::Query(error.to_string()))?,
        updated_at: parse_ts(row.get::<&str, _>("updated_at"))?,
    })
}

pub(super) async fn list_rollouts(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
) -> Result<Vec<WorkerVersionRollout>, StorageError> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query
        .push(ROLLOUT_COLUMNS)
        .push(" FROM worker_version_pins WHERE (active OR staged_min_version IS NOT NULL)");
    if let Some(tenant_id) = tenant_id {
        query.push(" AND tenant_id=").push_bind(tenant_id);
    }
    query.push(" ORDER BY tenant_id, handler_name");
    let rows = query.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_rollout).collect()
}

pub(super) async fn stage(
    storage: &SqliteStorage,
    tenant_id: &str,
    handler_name: &str,
    min_version: &str,
) -> Result<WorkerVersionRollout, StorageError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO worker_version_pins
            (tenant_id, handler_name, min_version, active, staged_min_version,
             rollout_revision, created_at, updated_at)
         VALUES (?1, ?2, '', false, ?3, ?4, ?5, ?5)
         ON CONFLICT (tenant_id, handler_name) DO UPDATE
         SET staged_min_version = excluded.staged_min_version,
             rollout_revision = excluded.rollout_revision,
             updated_at = excluded.updated_at
         RETURNING {ROLLOUT_COLUMNS}",
    )))
    .bind(tenant_id)
    .bind(handler_name)
    .bind(min_version)
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(ts(chrono::Utc::now()))
    .fetch_one(&storage.pool)
    .await?;
    row_to_rollout(&row)
}

pub(super) async fn change(
    storage: &SqliteStorage,
    tenant_id: &str,
    handler_name: &str,
    revision: uuid::Uuid,
    action: WorkerVersionRolloutAction,
) -> Result<Option<WorkerVersionRollout>, StorageError> {
    let (changes, predicate) = match action {
        WorkerVersionRolloutAction::Promote => (
            "previous_min_version = CASE WHEN active THEN min_version END,
             can_rollback = true, active = true, min_version = staged_min_version",
            "staged_min_version IS NOT NULL",
        ),
        WorkerVersionRolloutAction::Rollback => (
            "active = previous_min_version IS NOT NULL,
             min_version = COALESCE(previous_min_version, ''),
             previous_min_version = NULL, can_rollback = false",
            "can_rollback",
        ),
        WorkerVersionRolloutAction::Discard => {
            ("active = active", "staged_min_version IS NOT NULL")
        }
    };
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE worker_version_pins SET {changes}, staged_min_version = NULL,
                rollout_revision = ?4, updated_at = ?5
         WHERE tenant_id = ?1 AND handler_name = ?2 AND rollout_revision = ?3
               AND {predicate}
         RETURNING {ROLLOUT_COLUMNS}",
    )))
    .bind(tenant_id)
    .bind(handler_name)
    .bind(revision.to_string())
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(ts(chrono::Utc::now()))
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_rollout).transpose()
}
