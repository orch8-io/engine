//! Durable event correlation storage — SQLite.

use sqlx::AssertSqlSafe;
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
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox WHERE id = ?1"
    )))
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
    offset: u32,
) -> Result<Vec<EventEnvelope>, StorageError> {
    let rows = sqlx::query(AssertSqlSafe(format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox
         WHERE tenant_id = ?1 AND (?2 IS NULL OR status = ?2)
         ORDER BY received_at DESC, id DESC LIMIT ?3 OFFSET ?4"
    )))
    .bind(tenant_id)
    .bind(status.map(EventStatus::as_str))
    .bind(i64::from(limit))
    .bind(i64::from(offset))
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
    if event_names.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox
         WHERE tenant_id = "
    ));
    query.push_bind(tenant_id);
    query
        .push(" AND correlation_key = ")
        .push_bind(correlation_key);
    query.push(" AND status = 'pending' AND event_name IN (");
    let mut names = query.separated(", ");
    for event_name in event_names {
        names.push_bind(event_name);
    }
    names.push_unseparated(") ORDER BY received_at ASC LIMIT 10000");
    let rows = query.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_event).collect()
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
    let mut conn = storage.pool.acquire().await?;
    upsert_wait_on(&mut conn, w).await
}

const WAIT_INSERT_SQL: &str = r"INSERT INTO event_waits
            (id, tenant_id, instance_id, block_id, event_names, correlation_key, join_mode,
             status, matched_names, matched_event_ids, created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
         ";
const WAIT_REPLACE_SQL: &str = r"ON CONFLICT(instance_id, block_id) DO UPDATE SET
            event_names = excluded.event_names,
            correlation_key = excluded.correlation_key,
            join_mode = excluded.join_mode,
            status = excluded.status,
            matched_names = excluded.matched_names,
            matched_event_ids = excluded.matched_event_ids";

pub(super) async fn upsert_wait_on(
    conn: &mut sqlx::SqliteConnection,
    w: &EventWait,
) -> Result<(), StorageError> {
    write_wait_on(conn, w, true).await
}

async fn write_wait_on(
    conn: &mut sqlx::SqliteConnection,
    w: &EventWait,
    replace: bool,
) -> Result<(), StorageError> {
    let conflict = if replace {
        WAIT_REPLACE_SQL
    } else {
        "ON CONFLICT (instance_id, block_id) DO NOTHING"
    };
    sqlx::query(AssertSqlSafe(format!("{WAIT_INSERT_SQL}{conflict}")))
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
        .execute(conn)
        .await?;
    Ok(())
}

pub(super) async fn get_wait(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &str,
) -> Result<Option<EventWait>, StorageError> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {WAIT_COLUMNS} FROM event_waits WHERE instance_id = ?1 AND block_id = ?2"
    )))
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
    let rows = sqlx::query(AssertSqlSafe(format!(
        "SELECT {WAIT_COLUMNS} FROM event_waits
         WHERE tenant_id = ?1 AND correlation_key = ?2 AND status = 'waiting'
         ORDER BY created_at ASC"
    )))
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
    expected_matched: &[uuid::Uuid],
) -> Result<bool, StorageError> {
    let mut conn = storage.pool.acquire().await?;
    update_wait_on(&mut conn, w, expected, expected_matched).await
}

async fn update_wait_on(
    conn: &mut sqlx::SqliteConnection,
    w: &EventWait,
    expected: WaitStatus,
    expected_matched: &[uuid::Uuid],
) -> Result<bool, StorageError> {
    // `matched_event_ids` is only ever written by this module via
    // `serde_json::to_string`, so text equality is value equality.
    let result = sqlx::query(
        "UPDATE event_waits SET status = ?2, matched_names = ?3, matched_event_ids = ?4
         WHERE id = ?1 AND status = ?5 AND matched_event_ids = ?6",
    )
    .bind(w.id.to_string())
    .bind(w.status.as_str())
    .bind(serde_json::to_string(&w.matched_names)?)
    .bind(serde_json::to_string(&w.matched_event_ids)?)
    .bind(expected.as_str())
    .bind(serde_json::to_string(expected_matched)?)
    .execute(conn)
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

pub(super) async fn get_by_identity(
    store: &SqliteStorage,
    tenant_id: &str,
    event_name: &str,
    producer_event_id: &str,
) -> Result<Option<EventEnvelope>, StorageError> {
    let row = sqlx::query(AssertSqlSafe(format!("SELECT {EVENT_COLUMNS} FROM event_inbox WHERE tenant_id=?1 AND event_name=?2 AND producer_event_id=?3")))
        .bind(tenant_id).bind(event_name).bind(producer_event_id).fetch_optional(&store.pool).await?;
    row.as_ref().map(row_to_event).transpose()
}

pub(super) async fn delete_waits_on(
    conn: &mut sqlx::SqliteConnection,
    instance_id: InstanceId,
    blocks: &[String],
) -> Result<u64, StorageError> {
    let mut deleted = 0;
    for chunk in blocks.chunks(400) {
        let mut query = sqlx::QueryBuilder::new("DELETE FROM event_waits WHERE instance_id=");
        query
            .push_bind(instance_id.to_string())
            .push(" AND block_id IN (");
        let mut values = query.separated(", ");
        for block in chunk {
            values.push_bind(block);
        }
        values.push_unseparated(")");
        deleted += query.build().execute(&mut *conn).await?.rows_affected();
    }
    Ok(deleted)
}

pub(super) async fn delete_waits(
    store: &SqliteStorage,
    instance_id: InstanceId,
    blocks: &[String],
) -> Result<u64, StorageError> {
    let mut tx = store.pool.begin().await?;
    let deleted = delete_waits_on(&mut tx, instance_id, blocks).await?;
    tx.commit().await?;
    Ok(deleted)
}

// Parent -> registration -> inbox is the shared lock order for registration,
// matching and claimed iteration reset. Explicit helpers may have no parent.
async fn lock_parent(
    conn: &mut sqlx::SqliteConnection,
    wait: &EventWait,
) -> Result<bool, StorageError> {
    let instance_id = InstanceId::from_uuid(wait.instance_id);
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT tenant_id, state FROM task_instances WHERE id=?1")
            .bind(instance_id.to_string())
            .fetch_optional(conn)
            .await?;
    let Some((tenant, state)) = row else {
        return Ok(false);
    };
    if tenant != wait.tenant_id {
        return Err(StorageError::Constraint(
            "event wait tenant differs from parent".into(),
        ));
    }
    let state =
        orch8_types::instance::InstanceState::from_str(&state).map_err(StorageError::Query)?;
    Ok(!state.is_terminal())
}

async fn locked_wait(
    conn: &mut sqlx::SqliteConnection,
    wait: &EventWait,
) -> Result<Option<EventWait>, StorageError> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {WAIT_COLUMNS} FROM event_waits WHERE instance_id=?1 AND block_id=?2"
    )))
    .bind(wait.instance_id.to_string())
    .bind(&wait.block_id)
    .fetch_optional(conn)
    .await?;
    row.as_ref().map(row_to_wait).transpose()
}

pub(super) async fn register_wait_on(
    conn: &mut sqlx::SqliteConnection,
    wait: &EventWait,
) -> Result<EventWait, StorageError> {
    lock_parent(conn, wait).await?;
    let existing = if let Some(existing) = locked_wait(conn, wait).await? {
        existing
    } else {
        // The unique insert also serializes parentless explicit callers.
        write_wait_on(conn, wait, false).await?;
        locked_wait(conn, wait)
            .await?
            .ok_or_else(|| StorageError::Conflict("event registration disappeared".into()))?
    };
    if existing.tenant_id != wait.tenant_id {
        return Err(StorageError::Constraint(
            "event registration belongs to another tenant".into(),
        ));
    }
    if existing.status == WaitStatus::Satisfied {
        return Ok(existing);
    }
    let mut refreshed = wait.clone();
    refreshed.id = existing.id;
    refreshed.created_at = existing.created_at;
    refreshed.matched_names = existing.matched_names;
    refreshed.matched_event_ids = existing.matched_event_ids;
    upsert_wait_on(conn, &refreshed).await?;
    Ok(refreshed)
}

pub(super) async fn register_wait(
    store: &SqliteStorage,
    wait: &EventWait,
) -> Result<EventWait, StorageError> {
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let registered = register_wait_on(&mut tx, wait).await?;
    tx.commit().await?;
    Ok(registered)
}

async fn record_candidate(
    conn: &mut sqlx::SqliteConnection,
    wait: &mut EventWait,
    event_id: Uuid,
) -> Result<bool, StorageError> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {EVENT_COLUMNS} FROM event_inbox WHERE id=?1"
    )))
    .bind(event_id.to_string())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let event = row_to_event(&row)?;
    if event.status != EventStatus::Pending
        || event.tenant_id != wait.tenant_id
        || event.correlation_key != wait.correlation_key
        || !wait.record_match(&event)
    {
        return Ok(false);
    }
    let count = sqlx::query(
        "UPDATE event_inbox SET status='consumed', consumed_by=?2 WHERE id=?1 AND status='pending'",
    )
    .bind(event_id.to_string())
    .bind(wait.instance_id.to_string())
    .execute(conn)
    .await?
    .rows_affected();
    if count != 1 {
        return Err(StorageError::Conflict(
            "locked event changed during matching".into(),
        ));
    }
    Ok(true)
}

pub(super) async fn match_wait(
    store: &SqliteStorage,
    expected: &EventWait,
    event_id: Option<Uuid>,
    notification: &orch8_types::signal::Signal,
) -> Result<Option<crate::EventMatch>, StorageError> {
    if notification.delivered
        || notification.delivered_at.is_some()
        || notification.id != expected.id
        || notification.instance_id.into_uuid() != expected.instance_id
        || notification.signal_type
            != orch8_types::signal::SignalType::Custom(format!("human_input:{}", expected.block_id))
    {
        return Err(StorageError::Constraint(
            "event notification does not address its registration".into(),
        ));
    }
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let active = lock_parent(&mut tx, expected).await?;
    let Some(mut current) = locked_wait(&mut tx, expected).await? else {
        return Ok(None);
    };
    if current.id != expected.id {
        return Ok(None);
    }
    if current.tenant_id != expected.tenant_id {
        return Err(StorageError::Constraint(
            "event registration belongs to another tenant".into(),
        ));
    }
    if current.status != WaitStatus::Waiting {
        tx.commit().await?;
        return Ok(Some(crate::EventMatch {
            wait: current,
            consumed: false,
        }));
    }
    let read_matches = current.matched_event_ids.clone();
    let consumed = if !current.is_satisfied()
        && let Some(event_id) = event_id
    {
        record_candidate(&mut tx, &mut current, event_id).await?
    } else {
        false
    };
    if current.is_satisfied() {
        current.status = WaitStatus::Satisfied;
    }
    if consumed || current.status == WaitStatus::Satisfied {
        if !update_wait_on(&mut tx, &current, WaitStatus::Waiting, &read_matches).await? {
            return Err(StorageError::Conflict(
                "locked event registration changed during matching".into(),
            ));
        }
        if current.status == WaitStatus::Satisfied && active {
            super::signals::enqueue_once_on(&mut tx, notification).await?;
        }
    }
    tx.commit().await?;
    Ok(Some(crate::EventMatch {
        wait: current,
        consumed,
    }))
}
