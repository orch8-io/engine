use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::webhook_outbox::WebhookOutboxEntry;

use super::helpers::{parse_ts, ts};
use super::SqliteStorage;

fn row_to_entry(row: &sqlx::sqlite::SqliteRow) -> Result<WebhookOutboxEntry, StorageError> {
    let payload_str: String = row.get("payload");
    let instance_id: Option<String> = row.get("instance_id");
    Ok(WebhookOutboxEntry {
        id: Uuid::parse_str(row.get::<&str, _>("id"))
            .map_err(|e| StorageError::Query(e.to_string()))?,
        url: row.get("url"),
        event_type: row.get("event_type"),
        instance_id: instance_id
            .as_deref()
            .and_then(|s| Uuid::parse_str(s).ok()),
        payload: serde_json::from_str(&payload_str)?,
        attempts: row.get("attempts"),
        last_error: row.get("last_error"),
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
    })
}

pub(super) async fn park(
    storage: &SqliteStorage,
    entry: &WebhookOutboxEntry,
) -> Result<(), StorageError> {
    let payload = serde_json::to_string(&entry.payload)?;
    sqlx::query(
        "INSERT INTO webhook_outbox (id, url, event_type, instance_id, payload, attempts, last_error, created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )
    .bind(entry.id.to_string())
    .bind(&entry.url)
    .bind(&entry.event_type)
    .bind(entry.instance_id.map(|u| u.to_string()))
    .bind(&payload)
    .bind(entry.attempts)
    .bind(&entry.last_error)
    .bind(ts(entry.created_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    limit: u32,
) -> Result<Vec<WebhookOutboxEntry>, StorageError> {
    let rows = sqlx::query(
        "SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at FROM webhook_outbox ORDER BY created_at DESC LIMIT ?1",
    )
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_entry).collect()
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<WebhookOutboxEntry>, StorageError> {
    let row = sqlx::query(
        "SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at FROM webhook_outbox WHERE id = ?1",
    )
    .bind(id.to_string())
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_entry).transpose()
}

pub(super) async fn delete(storage: &SqliteStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM webhook_outbox WHERE id = ?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}
