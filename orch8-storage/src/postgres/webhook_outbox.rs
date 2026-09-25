use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};

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
        status: WebhookOutboxStatus::parse(row.get::<&str, _>("status")),
        next_attempt_at: row.get("next_attempt_at"),
        claimed_at: row.get("claimed_at"),
    }
}

fn insert_query<'a>() -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
    sqlx::query(
        r"INSERT INTO webhook_outbox
            (id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
    )
}

fn bind_entry<'a>(
    q: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    entry: &'a WebhookOutboxEntry,
) -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
    q.bind(entry.id)
        .bind(&entry.url)
        .bind(&entry.event_type)
        .bind(entry.instance_id)
        .bind(&entry.payload)
        .bind(entry.attempts)
        .bind(&entry.last_error)
        .bind(entry.created_at)
        .bind(entry.delivery_id)
        .bind(entry.status.as_str())
        .bind(entry.next_attempt_at)
        .bind(entry.claimed_at)
}

pub(super) async fn park(
    store: &PostgresStorage,
    entry: &WebhookOutboxEntry,
) -> Result<(), StorageError> {
    bind_entry(insert_query(), entry)
        .execute(&store.pool)
        .await?;
    Ok(())
}

/// Insert an outbox row inside an open transaction (see
/// `instances::conditional_update_state_with_outbox`).
pub(super) async fn park_tx(
    tx: &mut sqlx::PgConnection,
    entry: &WebhookOutboxEntry,
) -> Result<(), StorageError> {
    bind_entry(insert_query(), entry).execute(&mut *tx).await?;
    Ok(())
}

pub(super) async fn list(
    store: &PostgresStorage,
    limit: u32,
) -> Result<Vec<WebhookOutboxEntry>, StorageError> {
    let rows = sqlx::query(
        "SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at \
         FROM webhook_outbox WHERE status = 'parked' ORDER BY created_at DESC LIMIT $1",
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
        "SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at \
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

pub(super) async fn claim_due(
    store: &PostgresStorage,
    now: chrono::DateTime<chrono::Utc>,
    limit: u32,
) -> Result<Vec<WebhookOutboxEntry>, StorageError> {
    // Claim-and-mark in one statement: SKIP LOCKED lets multiple engine nodes
    // run drain loops without double-claiming a row.
    let rows = sqlx::query(
        "UPDATE webhook_outbox SET status = 'in_flight', claimed_at = $1 \
         WHERE id IN ( \
             SELECT id FROM webhook_outbox \
             WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= $1) \
             ORDER BY created_at LIMIT $2 \
             FOR UPDATE SKIP LOCKED \
         ) RETURNING id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at",
    )
    .bind(now)
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.iter().map(row_to_entry).collect())
}

pub(super) async fn claim_row(
    store: &PostgresStorage,
    id: Uuid,
    claimed_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE webhook_outbox SET status = 'in_flight', claimed_at = $2 WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .bind(claimed_at)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn fail_attempt(
    store: &PostgresStorage,
    id: Uuid,
    last_error: &str,
    next_attempt_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE webhook_outbox \
         SET attempts = attempts + 1, last_error = $2, \
             status = CASE WHEN $3 IS NULL THEN 'parked' ELSE 'pending' END, \
             next_attempt_at = $3, claimed_at = NULL \
         WHERE id = $1",
    )
    .bind(id)
    .bind(last_error)
    .bind(next_attempt_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn recover_stale(
    store: &PostgresStorage,
    stale_before: chrono::DateTime<chrono::Utc>,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "UPDATE webhook_outbox SET status = 'pending', claimed_at = NULL \
         WHERE status = 'in_flight' AND claimed_at < $1",
    )
    .bind(stale_before)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}
