use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::instance::InstanceState;
use orch8_types::signal::Signal;

use super::SqliteStorage;
use super::helpers::{begin_immediate, row_to_signal, ts};

/// Canonical INSERT for `signal_inbox`. Shared by [`enqueue`] and
/// [`enqueue_if_active`] so adding a column touches one place.
const SIGNAL_INSERT_SQL: &str = "INSERT INTO signal_inbox (id,instance_id,signal_type,payload,delivered,created_at,delivered_at) \
     VALUES (?1,?2,?3,?4,?5,?6,?7)";
const SIGNAL_ID_CHUNK_SIZE: usize = 500;

/// Bind a `Signal` to [`SIGNAL_INSERT_SQL`] in canonical column order.
/// Serialization errors surface as [`StorageError::Serialization`] before
/// the query is ever issued.
fn bind_signal_insert<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    s: &'q Signal,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>, StorageError> {
    Ok(q.bind(s.id.to_string())
        .bind(s.instance_id.into_uuid().to_string())
        .bind(serde_json::to_string(&s.signal_type)?)
        .bind(serde_json::to_string(&s.payload)?)
        .bind(s.delivered as i32)
        .bind(ts(s.created_at))
        .bind(s.delivered_at.map(ts)))
}

pub(super) async fn enqueue(storage: &SqliteStorage, signal: &Signal) -> Result<(), StorageError> {
    bind_signal_insert(sqlx::query(SIGNAL_INSERT_SQL), signal)?
        .execute(&storage.pool)
        .await?;
    Ok(())
}

/// Atomic enqueue gated on target non-terminal state.
///
/// Uses `BEGIN IMMEDIATE` rather than sqlx's default `BEGIN` (DEFERRED) so
/// SQLite acquires a RESERVED write lock up-front — the closest equivalent to
/// Postgres's `SELECT ... FOR UPDATE`. Without this, two concurrent callers
/// could both SELECT a non-terminal `state` before either inserts, weakening
/// the atomicity claim. With IMMEDIATE, one writer wins the lock, the other
/// either waits on `busy_timeout` or fails with `SQLITE_BUSY`.
///
/// SQLx owns the immediate transaction and rolls it back on errors or
/// cancellation before the pooled connection is reused.
///
/// State parsing uses `InstanceState::from_str` so a corrupted `state` column
/// surfaces as [`StorageError::Query`] instead of silently coercing to
/// `Scheduled` (which would let the INSERT proceed on a broken row —
/// previously possible via the permissive `parse_state` helper).
pub(super) async fn enqueue_if_active(
    storage: &SqliteStorage,
    signal: &Signal,
) -> Result<(), StorageError> {
    let mut conn = begin_immediate(&storage.pool).await?;
    let row: Option<(String,)> = sqlx::query_as("SELECT state FROM task_instances WHERE id = ?1")
        .bind(signal.instance_id.into_uuid().to_string())
        .fetch_optional(&mut *conn)
        .await?;

    let Some((state_str,)) = row else {
        return Err(StorageError::NotFound {
            entity: "task_instance",
            id: signal.instance_id.into_uuid().to_string(),
        });
    };

    let state = InstanceState::from_str(&state_str).map_err(StorageError::Query)?;

    if state.is_terminal() {
        return Err(StorageError::TerminalTarget {
            entity: "task_instance".to_string(),
            id: signal.instance_id.into_uuid().to_string(),
        });
    }

    bind_signal_insert(sqlx::query(SIGNAL_INSERT_SQL), signal)?
        .execute(&mut *conn)
        .await?;
    conn.commit().await?;
    Ok(())
}

pub(super) async fn get_pending(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<Signal>, StorageError> {
    let rows = sqlx::query(
        "SELECT * FROM signal_inbox WHERE instance_id=?1 AND delivered=0 ORDER BY created_at ASC",
    )
    .bind(instance_id.into_uuid().to_string())
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_signal).collect()
}

pub(super) async fn get_pending_batch(
    storage: &SqliteStorage,
    instance_ids: &[InstanceId],
) -> Result<HashMap<InstanceId, Vec<Signal>>, StorageError> {
    if instance_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut result: HashMap<InstanceId, Vec<Signal>> =
        instance_ids.iter().map(|id| (*id, Vec::new())).collect();
    let mut seen = HashSet::new();
    let unique_ids = instance_ids
        .iter()
        .copied()
        .filter(|id| seen.insert(*id))
        .collect::<Vec<_>>();
    let mut tx = storage.pool.begin().await?;
    for chunk in unique_ids.chunks(SIGNAL_ID_CHUNK_SIZE) {
        let mut qb = sqlx::QueryBuilder::new("SELECT * FROM signal_inbox WHERE instance_id IN (");
        let mut separated = qb.separated(",");
        for id in chunk {
            separated.push_bind(id.to_string());
        }
        separated.push_unseparated(") AND delivered=0 ORDER BY created_at ASC");
        let rows = qb.build().fetch_all(&mut *tx).await?;
        for row in &rows {
            let signal = row_to_signal(row)?;
            result.entry(signal.instance_id).or_default().push(signal);
        }
    }
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn mark_delivered(
    storage: &SqliteStorage,
    signal_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE signal_inbox SET delivered=1, delivered_at=?2 WHERE id=?1")
        .bind(signal_id.to_string())
        .bind(ts(chrono::Utc::now()))
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn mark_delivered_batch(
    storage: &SqliteStorage,
    signal_ids: &[Uuid],
) -> Result<(), StorageError> {
    if signal_ids.is_empty() {
        return Ok(());
    }
    let now = ts(chrono::Utc::now());
    let mut tx = storage.pool.begin().await?;
    for chunk in signal_ids.chunks(SIGNAL_ID_CHUNK_SIZE) {
        let mut qb = sqlx::QueryBuilder::new("UPDATE signal_inbox SET delivered=1, delivered_at=");
        qb.push_bind(&now);
        qb.push(" WHERE id IN (");
        let mut separated = qb.separated(",");
        for id in chunk {
            separated.push_bind(id.to_string());
        }
        separated.push_unseparated(")");
        qb.build().execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get_signalled_instance_ids(
    storage: &SqliteStorage,
    limit: u32,
) -> Result<Vec<(InstanceId, InstanceState)>, StorageError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        r"
        SELECT ti.id, ti.state
        FROM task_instances ti
        INNER JOIN signal_inbox si ON si.instance_id = ti.id
        WHERE ti.state IN ('paused', 'waiting', 'scheduled')
          AND si.delivered = 0
        GROUP BY ti.id, ti.state
        ORDER BY MIN(si.created_at) ASC
        LIMIT ?1
        ",
    )
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;

    rows.into_iter()
        .map(|(id_str, state_str)| {
            let id = Uuid::parse_str(&id_str)
                .map_err(|e| StorageError::Query(format!("invalid UUID '{id_str}': {e}")))?;
            let state = InstanceState::from_str(&state_str).map_err(StorageError::Query)?;
            Ok((InstanceId::from_uuid(id), state))
        })
        .collect()
}
