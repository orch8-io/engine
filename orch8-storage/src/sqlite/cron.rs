use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::helpers::{row_to_cron, ts};
use super::SqliteStorage;

pub(super) async fn create(storage: &SqliteStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO cron_schedules (id,tenant_id,namespace,sequence_id,cron_expr,timezone,enabled,metadata,next_fire_at,last_triggered_at,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)"
    )
    .bind(s.id.to_string())
    .bind(&s.tenant_id.0)
    .bind(&s.namespace.0)
    .bind(s.sequence_id.0.to_string())
    .bind(&s.cron_expr)
    .bind(&s.timezone)
    .bind(s.enabled as i32)
    .bind(serde_json::to_string(&s.metadata).unwrap_or_default())
    .bind(s.next_fire_at.map(ts))
    .bind(s.last_triggered_at.map(ts))
    .bind(ts(s.created_at))
    .bind(ts(s.updated_at))
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<CronSchedule>, StorageError> {
    let row = sqlx::query("SELECT * FROM cron_schedules WHERE id=?1")
        .bind(id.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    row.as_ref().map(row_to_cron).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: Option<&TenantId>,
) -> Result<Vec<CronSchedule>, StorageError> {
    let rows = if let Some(tid) = tenant_id {
        sqlx::query("SELECT * FROM cron_schedules WHERE tenant_id=?1 ORDER BY created_at")
            .bind(&tid.0)
            .fetch_all(&storage.pool)
            .await
    } else {
        sqlx::query("SELECT * FROM cron_schedules ORDER BY created_at")
            .fetch_all(&storage.pool)
            .await
    }
    .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_cron).collect()
}

pub(super) async fn update(storage: &SqliteStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query("UPDATE cron_schedules SET cron_expr=?2, timezone=?3, enabled=?4, metadata=?5, next_fire_at=?6, updated_at=?7 WHERE id=?1")
        .bind(s.id.to_string())
        .bind(&s.cron_expr)
        .bind(&s.timezone)
        .bind(s.enabled as i32)
        .bind(serde_json::to_string(&s.metadata).unwrap_or_default())
        .bind(s.next_fire_at.map(ts))
        .bind(ts(Utc::now()))
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn delete(storage: &SqliteStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM cron_schedules WHERE id=?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn claim_due(
    storage: &SqliteStorage,
    now: DateTime<Utc>,
) -> Result<Vec<CronSchedule>, StorageError> {
    let rows = sqlx::query("SELECT * FROM cron_schedules WHERE enabled=1 AND next_fire_at <= ?1")
        .bind(ts(now))
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_cron).collect()
}

pub(super) async fn update_fire_times(
    storage: &SqliteStorage,
    id: Uuid,
    last_triggered_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE cron_schedules SET last_triggered_at=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
        .bind(id.to_string())
        .bind(ts(last_triggered_at))
        .bind(ts(next_fire_at))
        .bind(ts(Utc::now()))
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
