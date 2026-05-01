use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::rows::CronRow;
use super::PostgresStorage;

pub(super) async fn create(store: &PostgresStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO cron_schedules
            (id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
             metadata, last_triggered_at, next_fire_at, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
    )
    .bind(s.id)
    .bind(&s.tenant_id.0)
    .bind(&s.namespace.0)
    .bind(s.sequence_id.0)
    .bind(&s.cron_expr)
    .bind(&s.timezone)
    .bind(s.enabled)
    .bind(&s.metadata)
    .bind(s.last_triggered_at)
    .bind(s.next_fire_at)
    .bind(s.created_at)
    .bind(s.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: Uuid,
) -> Result<Option<CronSchedule>, StorageError> {
    let row = sqlx::query_as::<_, CronRow>(
        r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                 metadata, last_triggered_at, next_fire_at, created_at, updated_at
          FROM cron_schedules WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(CronRow::into_schedule))
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<CronSchedule>, StorageError> {
    let cap = i64::from(limit.min(1000));
    let rows = if let Some(tid) = tenant_id {
        sqlx::query_as::<_, CronRow>(
            r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                     metadata, last_triggered_at, next_fire_at, created_at, updated_at
              FROM cron_schedules WHERE tenant_id = $1 ORDER BY created_at LIMIT $2",
        )
        .bind(&tid.0)
        .bind(cap)
        .fetch_all(&store.pool)
        .await?
    } else {
        sqlx::query_as::<_, CronRow>(
            r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                     metadata, last_triggered_at, next_fire_at, created_at, updated_at
              FROM cron_schedules ORDER BY created_at LIMIT $1",
        )
        .bind(cap)
        .fetch_all(&store.pool)
        .await?
    };
    Ok(rows.into_iter().map(CronRow::into_schedule).collect())
}

pub(super) async fn update(store: &PostgresStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE cron_schedules
          SET cron_expr=$2, timezone=$3, enabled=$4, metadata=$5, next_fire_at=$6, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(s.id)
    .bind(&s.cron_expr)
    .bind(&s.timezone)
    .bind(s.enabled)
    .bind(&s.metadata)
    .bind(s.next_fire_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM cron_schedules WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn claim_due(
    store: &PostgresStorage,
    now: DateTime<Utc>,
) -> Result<Vec<CronSchedule>, StorageError> {
    let rows = sqlx::query_as::<_, CronRow>(
        r"UPDATE cron_schedules
          SET last_triggered_at = $1, updated_at = NOW()
          WHERE id IN (
              SELECT id FROM cron_schedules
              WHERE enabled = TRUE
                AND next_fire_at <= $1
                AND (last_triggered_at IS NULL OR last_triggered_at < next_fire_at)
              ORDER BY next_fire_at
              FOR UPDATE SKIP LOCKED
          )
          RETURNING id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                    metadata, last_triggered_at, next_fire_at, created_at, updated_at",
    )
    .bind(now)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(CronRow::into_schedule).collect())
}

pub(super) async fn update_fire_times(
    store: &PostgresStorage,
    id: Uuid,
    last_triggered_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE cron_schedules SET last_triggered_at=$2, next_fire_at=$3, updated_at=NOW() WHERE id=$1",
    )
    .bind(id)
    .bind(last_triggered_at)
    .bind(next_fire_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}
