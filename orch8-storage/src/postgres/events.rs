//! Durable event correlation storage — `PostgreSQL`.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::event_correlation::{EventEnvelope, EventStatus, EventWait, WaitStatus};
use orch8_types::ids::InstanceId;

use super::PostgresStorage;

fn row_to_event(row: &sqlx::postgres::PgRow) -> Result<EventEnvelope, StorageError> {
    let status: String = row.get("status");
    Ok(EventEnvelope {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        event_name: row.get("event_name"),
        producer_event_id: row.get("producer_event_id"),
        correlation_key: row.get("correlation_key"),
        payload: row.get("payload"),
        status: EventStatus::from_str(&status).map_err(StorageError::Query)?,
        consumed_by: row.get("consumed_by"),
        received_at: row.get("received_at"),
    })
}

fn row_to_wait(row: &sqlx::postgres::PgRow) -> Result<EventWait, StorageError> {
    let status: String = row.get("status");
    Ok(EventWait {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        instance_id: row.get("instance_id"),
        block_id: row.get("block_id"),
        event_names: serde_json::from_value(row.get("event_names"))?,
        correlation_key: row.get("correlation_key"),
        join_mode: serde_json::from_value(row.get("join_mode"))?,
        status: WaitStatus::from_str(&status).map_err(StorageError::Query)?,
        matched_names: serde_json::from_value(row.get("matched_names"))?,
        matched_event_ids: serde_json::from_value(row.get("matched_event_ids"))?,
        created_at: row.get("created_at"),
    })
}

const EVENT_COLUMNS: &str = "id, tenant_id, event_name, producer_event_id, correlation_key, \
     payload, status, consumed_by, received_at";
const WAIT_COLUMNS: &str = "id, tenant_id, instance_id, block_id, event_names, correlation_key, \
     join_mode, status, matched_names, matched_event_ids, created_at";

pub(super) async fn ingest(
    store: &PostgresStorage,
    e: &EventEnvelope,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        r"INSERT INTO event_inbox
            (id, tenant_id, event_name, producer_event_id, correlation_key, payload, status,
             consumed_by, received_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
          ON CONFLICT (tenant_id, event_name, producer_event_id) DO NOTHING",
    )
    .bind(e.id)
    .bind(&e.tenant_id)
    .bind(&e.event_name)
    .bind(&e.producer_event_id)
    .bind(&e.correlation_key)
    .bind(&e.payload)
    .bind(e.status.as_str())
    .bind(e.consumed_by)
    .bind(e.received_at)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: Uuid,
) -> Result<Option<EventEnvelope>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref().map(row_to_event).transpose()
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: &str,
    status: Option<EventStatus>,
    limit: u32,
) -> Result<Vec<EventEnvelope>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox
         WHERE tenant_id = $1 AND ($2::text IS NULL OR status = $2)
         ORDER BY received_at DESC LIMIT $3"
    ))
    .bind(tenant_id)
    .bind(status.map(EventStatus::as_str))
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    rows.iter().map(row_to_event).collect()
}

pub(super) async fn find_pending(
    store: &PostgresStorage,
    tenant_id: &str,
    event_names: &[String],
    correlation_key: &str,
) -> Result<Vec<EventEnvelope>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox
         WHERE tenant_id = $1 AND correlation_key = $2 AND status = 'pending'
           AND event_name = ANY($3)
         ORDER BY received_at ASC"
    ))
    .bind(tenant_id)
    .bind(correlation_key)
    .bind(event_names)
    .fetch_all(&store.pool)
    .await?;
    rows.iter().map(row_to_event).collect()
}

pub(super) async fn consume(
    store: &PostgresStorage,
    event_ids: &[Uuid],
    instance_id: InstanceId,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"UPDATE event_inbox SET status = 'consumed', consumed_by = $2
          WHERE id = ANY($1) AND status = 'pending'",
    )
    .bind(event_ids)
    .bind(instance_id.into_uuid())
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn upsert_wait(
    store: &PostgresStorage,
    w: &EventWait,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO event_waits
            (id, tenant_id, instance_id, block_id, event_names, correlation_key, join_mode,
             status, matched_names, matched_event_ids, created_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
          ON CONFLICT (instance_id, block_id) DO UPDATE SET
            event_names = EXCLUDED.event_names,
            correlation_key = EXCLUDED.correlation_key,
            join_mode = EXCLUDED.join_mode,
            status = EXCLUDED.status,
            matched_names = EXCLUDED.matched_names,
            matched_event_ids = EXCLUDED.matched_event_ids",
    )
    .bind(w.id)
    .bind(&w.tenant_id)
    .bind(w.instance_id)
    .bind(&w.block_id)
    .bind(serde_json::to_value(&w.event_names)?)
    .bind(&w.correlation_key)
    .bind(serde_json::to_value(w.join_mode)?)
    .bind(w.status.as_str())
    .bind(serde_json::to_value(&w.matched_names)?)
    .bind(serde_json::to_value(&w.matched_event_ids)?)
    .bind(w.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_wait(
    store: &PostgresStorage,
    instance_id: InstanceId,
    block_id: &str,
) -> Result<Option<EventWait>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {WAIT_COLUMNS} FROM event_waits WHERE instance_id = $1 AND block_id = $2"
    ))
    .bind(instance_id.into_uuid())
    .bind(block_id)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref().map(row_to_wait).transpose()
}

pub(super) async fn find_waiting(
    store: &PostgresStorage,
    tenant_id: &str,
    event_name: &str,
    correlation_key: &str,
) -> Result<Vec<EventWait>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {WAIT_COLUMNS} FROM event_waits
         WHERE tenant_id = $1 AND correlation_key = $2 AND status = 'waiting'
           AND event_names @> to_jsonb($3::text)
         ORDER BY created_at ASC"
    ))
    .bind(tenant_id)
    .bind(correlation_key)
    .bind(event_name)
    .fetch_all(&store.pool)
    .await?;
    rows.iter().map(row_to_wait).collect()
}

pub(super) async fn update_wait(
    store: &PostgresStorage,
    w: &EventWait,
    expected: WaitStatus,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        r"UPDATE event_waits SET status = $2, matched_names = $3, matched_event_ids = $4
          WHERE id = $1 AND status = $5",
    )
    .bind(w.id)
    .bind(w.status.as_str())
    .bind(serde_json::to_value(&w.matched_names)?)
    .bind(serde_json::to_value(&w.matched_event_ids)?)
    .bind(expected.as_str())
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn expire_before(
    store: &PostgresStorage,
    cutoff: DateTime<Utc>,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "UPDATE event_inbox SET status = 'expired'
         WHERE status = 'pending' AND received_at < $1",
    )
    .bind(cutoff)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}
