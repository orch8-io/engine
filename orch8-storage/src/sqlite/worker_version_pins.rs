use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::worker::WorkerVersionPin;

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

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
        "INSERT INTO worker_version_pins (tenant_id, handler_name, min_version, created_at, updated_at) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(tenant_id, handler_name) DO UPDATE SET min_version=excluded.min_version, updated_at=excluded.updated_at",
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
        "SELECT tenant_id, handler_name, min_version, created_at, updated_at FROM worker_version_pins WHERE tenant_id = ?1 AND handler_name = ?2",
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
        "SELECT tenant_id, handler_name, min_version, created_at, updated_at FROM worker_version_pins WHERE 1=1",
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
