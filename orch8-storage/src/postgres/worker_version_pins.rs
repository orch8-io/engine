use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::worker::WorkerVersionPin;

use super::PostgresStorage;

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
    sqlx::query(
        r"INSERT INTO worker_version_pins (tenant_id, handler_name, min_version, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5)
          ON CONFLICT (tenant_id, handler_name)
          DO UPDATE SET min_version = EXCLUDED.min_version, updated_at = EXCLUDED.updated_at",
    )
    .bind(&pin.tenant_id)
    .bind(&pin.handler_name)
    .bind(&pin.min_version)
    .bind(pin.created_at)
    .bind(pin.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    tenant_id: &str,
    handler_name: &str,
) -> Result<Option<WorkerVersionPin>, StorageError> {
    let row = sqlx::query(
        r"SELECT tenant_id, handler_name, min_version, created_at, updated_at
          FROM worker_version_pins WHERE tenant_id = $1 AND handler_name = $2",
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
          FROM worker_version_pins WHERE 1=1",
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
    sqlx::query("DELETE FROM worker_version_pins WHERE tenant_id = $1 AND handler_name = $2")
        .bind(tenant_id)
        .bind(handler_name)
        .execute(&store.pool)
        .await?;
    Ok(())
}
