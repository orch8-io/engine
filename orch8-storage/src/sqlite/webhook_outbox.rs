use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

fn row_to_entry(row: &sqlx::sqlite::SqliteRow) -> Result<WebhookOutboxEntry, StorageError> {
    let payload_str: String = row.get("payload");
    let instance_id: Option<String> = row.get("instance_id");
    let next_attempt_at: Option<String> = row.get("next_attempt_at");
    let claimed_at: Option<String> = row.get("claimed_at");
    Ok(WebhookOutboxEntry {
        id: Uuid::parse_str(row.get::<&str, _>("id"))
            .map_err(|e| StorageError::Query(e.to_string()))?,
        url: row.get("url"),
        event_type: row.get("event_type"),
        instance_id: instance_id.as_deref().and_then(|s| Uuid::parse_str(s).ok()),
        payload: serde_json::from_str(&payload_str)?,
        attempts: row.get("attempts"),
        last_error: row.get("last_error"),
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
        delivery_id: row
            .get::<Option<String>, _>("delivery_id")
            .as_deref()
            .and_then(|s| Uuid::parse_str(s).ok()),
        status: WebhookOutboxStatus::parse(row.get::<&str, _>("status")),
        next_attempt_at: next_attempt_at.as_deref().map(parse_ts).transpose()?,
        claimed_at: claimed_at.as_deref().map(parse_ts).transpose()?,
    })
}

fn insert_query<'a>() -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
    sqlx::query(
        "INSERT INTO webhook_outbox (id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
    )
}

fn bind_entry<'a>(
    q: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
    entry: &'a WebhookOutboxEntry,
    payload: &'a str,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
    q.bind(entry.id.to_string())
        .bind(&entry.url)
        .bind(&entry.event_type)
        .bind(entry.instance_id.map(|u| u.to_string()))
        .bind(payload)
        .bind(entry.attempts)
        .bind(&entry.last_error)
        .bind(ts(entry.created_at))
        .bind(entry.delivery_id.map(|u| u.to_string()))
        .bind(entry.status.as_str())
        .bind(entry.next_attempt_at.map(ts))
        .bind(entry.claimed_at.map(ts))
}

pub(super) async fn park(
    storage: &SqliteStorage,
    entry: &WebhookOutboxEntry,
) -> Result<(), StorageError> {
    let payload = serde_json::to_string(&entry.payload)?;
    bind_entry(insert_query(), entry, &payload)
        .execute(&storage.pool)
        .await?;
    Ok(())
}

/// Insert an outbox row inside an open transaction (see
/// `instances::conditional_update_state_with_outbox`).
pub(super) async fn park_tx(
    tx: &mut sqlx::SqliteConnection,
    entry: &WebhookOutboxEntry,
) -> Result<(), StorageError> {
    let payload = serde_json::to_string(&entry.payload)?;
    bind_entry(insert_query(), entry, &payload)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    limit: u32,
) -> Result<Vec<WebhookOutboxEntry>, StorageError> {
    let rows = sqlx::query(
        "SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at \
         FROM webhook_outbox WHERE status = 'parked' ORDER BY created_at DESC LIMIT ?1",
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
        "SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at \
         FROM webhook_outbox WHERE id = ?1",
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

pub(super) async fn claim_due(
    storage: &SqliteStorage,
    now: chrono::DateTime<chrono::Utc>,
    limit: u32,
) -> Result<Vec<WebhookOutboxEntry>, StorageError> {
    let now_s = ts(now);
    // Same BEGIN IMMEDIATE discipline as `instances::claim_due`: serialize the
    // read-then-mark so two drain loops can't claim the same row (the SQLite
    // analogue of `FOR UPDATE SKIP LOCKED`).
    let mut conn = storage.pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

    let result = async {
        let rows = sqlx::query(
            "SELECT id, url, event_type, instance_id, payload, attempts, last_error, created_at, delivery_id, status, next_attempt_at, claimed_at \
             FROM webhook_outbox \
             WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?1) \
             ORDER BY created_at ASC LIMIT ?2",
        )
        .bind(&now_s)
        .bind(i64::from(limit))
        .fetch_all(&mut *conn)
        .await?;
        let mut entries: Vec<WebhookOutboxEntry> =
            rows.iter().map(row_to_entry).collect::<Result<_, _>>()?;
        if !entries.is_empty() {
            let mut qb = sqlx::QueryBuilder::new(
                "UPDATE webhook_outbox SET status='in_flight', claimed_at=",
            );
            qb.push_bind(now_s);
            qb.push(" WHERE id IN (");
            let mut separated = qb.separated(",");
            for e in &entries {
                separated.push_bind(e.id.to_string());
            }
            separated.push_unseparated(")");
            qb.build().execute(&mut *conn).await?;
            for entry in &mut entries {
                entry.status = WebhookOutboxStatus::InFlight;
                entry.claimed_at = Some(now);
            }
        }
        Ok::<_, StorageError>(entries)
    }
    .await;

    match result {
        Ok(entries) => {
            sqlx::query("COMMIT").execute(&mut *conn).await?;
            Ok(entries)
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            Err(e)
        }
    }
}

pub(super) async fn claim_row(
    storage: &SqliteStorage,
    id: Uuid,
    claimed_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE webhook_outbox SET status='in_flight', claimed_at=?2 WHERE id=?1 AND status='pending'",
    )
    .bind(id.to_string())
    .bind(ts(claimed_at))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn fail_attempt(
    storage: &SqliteStorage,
    id: Uuid,
    last_error: &str,
    next_attempt_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE webhook_outbox \
         SET attempts = attempts + 1, last_error = ?2, \
             status = CASE WHEN ?3 IS NULL THEN 'parked' ELSE 'pending' END, \
             next_attempt_at = ?3, claimed_at = NULL \
         WHERE id = ?1",
    )
    .bind(id.to_string())
    .bind(last_error)
    .bind(next_attempt_at.map(ts))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn recover_stale(
    storage: &SqliteStorage,
    stale_before: chrono::DateTime<chrono::Utc>,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "UPDATE webhook_outbox SET status='pending', claimed_at=NULL \
         WHERE status='in_flight' AND claimed_at < ?1",
    )
    .bind(ts(stale_before))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected())
}
