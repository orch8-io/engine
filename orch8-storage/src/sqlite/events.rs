//! Durable event correlation storage — SQLite.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::event_correlation::{EventEnvelope, EventStatus, EventWait, WaitStatus};
use orch8_types::ids::InstanceId;

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

fn parse_uuid(s: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(s).map_err(|e| StorageError::Query(e.to_string()))
}

fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> Result<EventEnvelope, StorageError> {
    let payload: String = row.get("payload");
    let status: String = row.get("status");
    let consumed_by: Option<String> = row.get("consumed_by");
    Ok(EventEnvelope {
        id: parse_uuid(row.get::<&str, _>("id"))?,
        tenant_id: row.get("tenant_id"),
        event_name: row.get("event_name"),
        producer_event_id: row.get("producer_event_id"),
        correlation_key: row.get("correlation_key"),
        payload: serde_json::from_str(&payload)?,
        status: EventStatus::from_str(&status).map_err(StorageError::Query)?,
        consumed_by: consumed_by.as_deref().and_then(|s| Uuid::parse_str(s).ok()),
        received_at: parse_ts(row.get::<&str, _>("received_at"))?,
    })
}

fn row_to_wait(row: &sqlx::sqlite::SqliteRow) -> Result<EventWait, StorageError> {
    let names: String = row.get("event_names");
    let join: String = row.get("join_mode");
    let matched_names: String = row.get("matched_names");
    let matched_ids: String = row.get("matched_event_ids");
    let status: String = row.get("status");
    Ok(EventWait {
        id: parse_uuid(row.get::<&str, _>("id"))?,
        tenant_id: row.get("tenant_id"),
        instance_id: parse_uuid(row.get::<&str, _>("instance_id"))?,
        block_id: row.get("block_id"),
        event_names: serde_json::from_str(&names)?,
        correlation_key: row.get("correlation_key"),
        join_mode: serde_json::from_str(&join)?,
        status: WaitStatus::from_str(&status).map_err(StorageError::Query)?,
        matched_names: serde_json::from_str(&matched_names)?,
        matched_event_ids: serde_json::from_str(&matched_ids)?,
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
    })
}

const EVENT_COLUMNS: &str = "id, tenant_id, event_name, producer_event_id, correlation_key, \
     payload, status, consumed_by, received_at";
const WAIT_COLUMNS: &str = "id, tenant_id, instance_id, block_id, event_names, correlation_key, \
     join_mode, status, matched_names, matched_event_ids, created_at";

pub(super) async fn ingest(
    storage: &SqliteStorage,
    e: &EventEnvelope,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "INSERT INTO event_inbox
            (id, tenant_id, event_name, producer_event_id, correlation_key, payload, status,
             consumed_by, received_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(tenant_id, event_name, producer_event_id) DO NOTHING",
    )
    .bind(e.id.to_string())
    .bind(&e.tenant_id)
    .bind(&e.event_name)
    .bind(&e.producer_event_id)
    .bind(&e.correlation_key)
    .bind(serde_json::to_string(&e.payload)?)
    .bind(e.status.as_str())
    .bind(e.consumed_by.map(|u| u.to_string()))
    .bind(ts(e.received_at))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<EventEnvelope>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox WHERE id = ?1"
    ))
    .bind(id.to_string())
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_event).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: &str,
    status: Option<EventStatus>,
    limit: u32,
) -> Result<Vec<EventEnvelope>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox
         WHERE tenant_id = ?1 AND (?2 IS NULL OR status = ?2)
         ORDER BY received_at DESC LIMIT ?3"
    ))
    .bind(tenant_id)
    .bind(status.map(EventStatus::as_str))
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_event).collect()
}

pub(super) async fn find_pending(
    storage: &SqliteStorage,
    tenant_id: &str,
    event_names: &[String],
    correlation_key: &str,
) -> Result<Vec<EventEnvelope>, StorageError> {
    // Names are matched app-side to avoid dynamic IN-list SQL.
    let rows = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox
         WHERE tenant_id = ?1 AND correlation_key = ?2 AND status = 'pending'
         ORDER BY received_at ASC"
    ))
    .bind(tenant_id)
    .bind(correlation_key)
    .fetch_all(&storage.pool)
    .await?;
    let mut out = Vec::new();
    for row in &rows {
        let event = row_to_event(row)?;
        if event_names.iter().any(|n| n == &event.event_name) {
            out.push(event);
        }
    }
    Ok(out)
}

pub(super) async fn consume(
    storage: &SqliteStorage,
    event_ids: &[Uuid],
    instance_id: InstanceId,
) -> Result<u64, StorageError> {
    let mut consumed = 0u64;
    for id in event_ids {
        let result = sqlx::query(
            "UPDATE event_inbox SET status = 'consumed', consumed_by = ?2
             WHERE id = ?1 AND status = 'pending'",
        )
        .bind(id.to_string())
        .bind(instance_id.into_uuid().to_string())
        .execute(&storage.pool)
        .await?;
        consumed += result.rows_affected();
    }
    Ok(consumed)
}

pub(super) async fn upsert_wait(
    storage: &SqliteStorage,
    w: &EventWait,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO event_waits
            (id, tenant_id, instance_id, block_id, event_names, correlation_key, join_mode,
             status, matched_names, matched_event_ids, created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
         ON CONFLICT(instance_id, block_id) DO UPDATE SET
            event_names = excluded.event_names,
            correlation_key = excluded.correlation_key,
            join_mode = excluded.join_mode,
            status = excluded.status,
            matched_names = excluded.matched_names,
            matched_event_ids = excluded.matched_event_ids",
    )
    .bind(w.id.to_string())
    .bind(&w.tenant_id)
    .bind(w.instance_id.to_string())
    .bind(&w.block_id)
    .bind(serde_json::to_string(&w.event_names)?)
    .bind(&w.correlation_key)
    .bind(serde_json::to_string(&w.join_mode)?)
    .bind(w.status.as_str())
    .bind(serde_json::to_string(&w.matched_names)?)
    .bind(serde_json::to_string(&w.matched_event_ids)?)
    .bind(ts(w.created_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_wait(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &str,
) -> Result<Option<EventWait>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {WAIT_COLUMNS} FROM event_waits WHERE instance_id = ?1 AND block_id = ?2"
    ))
    .bind(instance_id.into_uuid().to_string())
    .bind(block_id)
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_wait).transpose()
}

pub(super) async fn find_waiting(
    storage: &SqliteStorage,
    tenant_id: &str,
    event_name: &str,
    correlation_key: &str,
) -> Result<Vec<EventWait>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {WAIT_COLUMNS} FROM event_waits
         WHERE tenant_id = ?1 AND correlation_key = ?2 AND status = 'waiting'
         ORDER BY created_at ASC"
    ))
    .bind(tenant_id)
    .bind(correlation_key)
    .fetch_all(&storage.pool)
    .await?;
    let mut out = Vec::new();
    for row in &rows {
        let wait = row_to_wait(row)?;
        if wait.listens_for(event_name) {
            out.push(wait);
        }
    }
    Ok(out)
}

pub(super) async fn update_wait(
    storage: &SqliteStorage,
    w: &EventWait,
    expected: WaitStatus,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE event_waits SET status = ?2, matched_names = ?3, matched_event_ids = ?4
         WHERE id = ?1 AND status = ?5",
    )
    .bind(w.id.to_string())
    .bind(w.status.as_str())
    .bind(serde_json::to_string(&w.matched_names)?)
    .bind(serde_json::to_string(&w.matched_event_ids)?)
    .bind(expected.as_str())
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn expire_before(
    storage: &SqliteStorage,
    cutoff: DateTime<Utc>,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "UPDATE event_inbox SET status = 'expired'
         WHERE status = 'pending' AND received_at < ?1",
    )
    .bind(ts(cutoff))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected())
}
