use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::worker::{WorkerVersionPin, WorkerVersionRollout, WorkerVersionRolloutAction};

use super::PostgresStorage;

/// Read the pin after acquiring the same transaction-level fence as pin writers.
pub(super) async fn admits_claim(
    conn: &mut sqlx::PgConnection,
    request: &crate::worker_claim::WorkerClaimRequest,
) -> Result<bool, StorageError> {
    // Shared workers consult only the unscoped pin, even when claiming a tenant's task.
    let tenant_id = request
        .tenant_id
        .as_ref()
        .map_or("", orch8_types::ids::TenantId::as_str);
    // Use a separate statement so PostgreSQL takes the read snapshot after
    // waiting for an in-progress promotion to commit.
    lock_admission_key(conn, tenant_id, &request.handler_name, false).await?;
    let minimum: Option<String> = sqlx::query_scalar(
        "SELECT min_version FROM worker_version_pins WHERE tenant_id=$1 AND handler_name=$2 AND active",
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

// A row lock alone cannot protect a route whose pin has never been created.
async fn lock_admission_key(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    handler: &str,
    writer: bool,
) -> Result<(), StorageError> {
    let query = if writer {
        "SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array($1::text, $2::text)::text, 654810291))"
    } else {
        "SELECT pg_advisory_xact_lock_shared(hashtextextended(jsonb_build_array($1::text, $2::text)::text, 654810291))"
    };
    sqlx::query(query)
        .bind(tenant_id)
        .bind(handler)
        .execute(conn)
        .await?;
    Ok(())
}

fn row_to_pin(row: &sqlx::postgres::PgRow) -> WorkerVersionPin {
    WorkerVersionPin {
        tenant_id: row.get("tenant_id"),
        handler_name: row.get("handler_name"),
        min_version: row.get("min_version"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(super) async fn upsert(
    store: &PostgresStorage,
    pin: &WorkerVersionPin,
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;
    lock_admission_key(&mut tx, &pin.tenant_id, &pin.handler_name, true).await?;
    sqlx::query(
        r"INSERT INTO worker_version_pins (tenant_id, handler_name, min_version, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5)
          ON CONFLICT (tenant_id, handler_name)
          DO UPDATE SET min_version = EXCLUDED.min_version, updated_at = EXCLUDED.updated_at,
                        active = true, staged_min_version = NULL, previous_min_version = NULL,
                        can_rollback = false, rollout_revision = NULL",
    )
    .bind(&pin.tenant_id)
    .bind(&pin.handler_name)
    .bind(&pin.min_version)
    .bind(pin.created_at)
    .bind(pin.updated_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    tenant_id: &str,
    handler_name: &str,
) -> Result<Option<WorkerVersionPin>, StorageError> {
    let row = sqlx::query(
        r"SELECT tenant_id, handler_name, min_version, created_at, updated_at
          FROM worker_version_pins WHERE tenant_id = $1 AND handler_name = $2 AND active",
    )
    .bind(tenant_id)
    .bind(handler_name)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.as_ref().map(row_to_pin))
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&str>,
) -> Result<Vec<WorkerVersionPin>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        r"SELECT tenant_id, handler_name, min_version, created_at, updated_at
          FROM worker_version_pins WHERE active",
    );
    if let Some(t) = tenant_id {
        qb.push(" AND tenant_id=").push_bind(t.to_string());
    }
    qb.push(" ORDER BY tenant_id, handler_name");
    let rows = qb.build().fetch_all(&store.pool).await?;
    Ok(rows.iter().map(row_to_pin).collect())
}

pub(super) async fn delete(
    store: &PostgresStorage,
    tenant_id: &str,
    handler_name: &str,
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;
    lock_admission_key(&mut tx, tenant_id, handler_name, true).await?;
    sqlx::query("DELETE FROM worker_version_pins WHERE tenant_id = $1 AND handler_name = $2")
        .bind(tenant_id)
        .bind(handler_name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

const ROLLOUT_COLUMNS: &str = "tenant_id, handler_name,
    CASE WHEN active THEN min_version END AS active_min_version,
    staged_min_version, previous_min_version, can_rollback, rollout_revision, updated_at";

fn row_to_rollout(row: &sqlx::postgres::PgRow) -> WorkerVersionRollout {
    WorkerVersionRollout {
        tenant_id: row.get("tenant_id"),
        handler_name: row.get("handler_name"),
        active_min_version: row.get("active_min_version"),
        staged_min_version: row.get("staged_min_version"),
        previous_min_version: row.get("previous_min_version"),
        can_rollback: row.get("can_rollback"),
        revision: row.get("rollout_revision"),
        updated_at: row.get("updated_at"),
    }
}

pub(super) async fn list_rollouts(
    storage: &PostgresStorage,
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
    Ok(rows.iter().map(row_to_rollout).collect())
}

pub(super) async fn stage(
    storage: &PostgresStorage,
    tenant_id: &str,
    handler_name: &str,
    min_version: &str,
) -> Result<WorkerVersionRollout, StorageError> {
    let mut tx = storage.pool.begin().await?;
    lock_admission_key(&mut tx, tenant_id, handler_name, true).await?;
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO worker_version_pins
            (tenant_id, handler_name, min_version, active, staged_min_version,
             rollout_revision, created_at, updated_at)
         VALUES ($1, $2, '', false, $3, $4, $5, $5)
         ON CONFLICT (tenant_id, handler_name) DO UPDATE
         SET staged_min_version = excluded.staged_min_version,
             rollout_revision = excluded.rollout_revision,
             updated_at = excluded.updated_at
         RETURNING {ROLLOUT_COLUMNS}",
    )))
    .bind(tenant_id)
    .bind(handler_name)
    .bind(min_version)
    .bind(uuid::Uuid::new_v4())
    .bind(chrono::Utc::now())
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row_to_rollout(&row))
}

pub(super) async fn change(
    storage: &PostgresStorage,
    tenant_id: &str,
    handler_name: &str,
    revision: uuid::Uuid,
    action: WorkerVersionRolloutAction,
) -> Result<Option<WorkerVersionRollout>, StorageError> {
    let mut tx = storage.pool.begin().await?;
    lock_admission_key(&mut tx, tenant_id, handler_name, true).await?;
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
                rollout_revision = $4, updated_at = $5
         WHERE tenant_id = $1 AND handler_name = $2 AND rollout_revision = $3
               AND {predicate}
         RETURNING {ROLLOUT_COLUMNS}",
    )))
    .bind(tenant_id)
    .bind(handler_name)
    .bind(revision)
    .bind(uuid::Uuid::new_v4())
    .bind(chrono::Utc::now())
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.as_ref().map(row_to_rollout))
}
