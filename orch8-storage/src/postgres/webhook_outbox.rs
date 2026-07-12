use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::webhook_outbox::WebhookOutboxEntry;

use super::PostgresStorage;

fn row_to_entry(row: &sqlx::postgres::PgRow) -> WebhookOutboxEntry {
    WebhookOutboxEntry {
        id: row.get("id"),
        url: row.get("url"),
        event_type: row.get("event_type"),
        instance_id: row.get("instance_id"),
        payload: row.get("payload"),
        attempts: row.get("attempts"),
        last_error: row.get("last_error"),
        created_at: row.get("created_at"),
        delivery_id: row.get("delivery_id"),
    }
}

pub(super) async fn park(
    store: &PostgresStorage,
    entry: &WebhookOutboxEntry,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO webhook_outbox
            (id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(entry.id)
    .bind(&entry.url)
    .bind(&entry.event_type)
    .bind(entry.instance_id)
    .bind(&entry.payload)
    .bind(entry.attempts)
    .bind(&entry.last_error)
    .bind(entry.created_at)
    .bind(entry.delivery_id)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list(
    store: &PostgresStorage,
    limit: u32,
) -> Result<Vec<WebhookOutboxEntry>, StorageError> {
    let rows = sqlx::query(
        r"SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id
          FROM webhook_outbox ORDER BY created_at DESC LIMIT $1",
    )
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.iter().map(row_to_entry).collect())
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: Uuid,
) -> Result<Option<WebhookOutboxEntry>, StorageError> {
    let row = sqlx::query(
        r"SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id
          FROM webhook_outbox WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.as_ref().map(row_to_entry))
}

pub(super) async fn delete(store: &PostgresStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM webhook_outbox WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}
