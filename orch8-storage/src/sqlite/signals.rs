use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::instance::InstanceState;
use orch8_types::signal::Signal;

use super::helpers::{row_to_signal, ts};
use super::SqliteStorage;

/// Issue `ROLLBACK` on a pooled connection, swallowing any error. Used on
/// failure paths inside `enqueue_if_active` where the caller's real error is
/// the signal to propagate — a secondary rollback failure here would just
/// shadow the root cause, and the connection will be reset by sqlx anyway
/// when it returns to the pool in a broken txn state.
async fn rollback_quiet(conn: &mut sqlx::SqliteConnection) {
    if let Err(e) = sqlx::query("ROLLBACK").execute(&mut *conn).await {
        tracing::warn!(error = %e, "enqueue_if_active: ROLLBACK failed, connection will be reset");
    }
}

/// Canonical INSERT for `signal_inbox`. Shared by [`enqueue`] and
/// [`enqueue_if_active`] so adding a column touches one place.
const SIGNAL_INSERT_SQL: &str =
    "INSERT INTO signal_inbox (id,instance_id,signal_type,payload,delivered,created_at) \
     VALUES (?1,?2,?3,?4,0,?5)";

/// Bind a `Signal` to [`SIGNAL_INSERT_SQL`] in canonical column order.
/// Serialization errors surface as [`StorageError::Serialization`] before
/// the query is ever issued.
fn bind_signal_insert<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    s: &'q Signal,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>, StorageError> {
    Ok(q.bind(s.id.to_string())
        .bind(s.instance_id.0.to_string())
        .bind(serde_json::to_string(&s.signal_type)?)
        .bind(serde_json::to_string(&s.payload)?)
        .bind(ts(s.created_at)))
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
/// Because sqlx's `Transaction` wrapper hard-codes `BEGIN`, we manage the
/// transaction manually on a pooled connection and rely on explicit COMMIT /
/// ROLLBACK. Every early return path runs [`rollback_quiet`] before propagating
/// the caller's error — leaving a live transaction on a returned pool
/// connection would poison subsequent users.
///
/// State parsing uses [`try_parse_state`] so a corrupted `state` column
/// surfaces as [`StorageError::Query`] instead of silently coercing to
/// `Scheduled` (which would let the INSERT proceed on a broken row —
/// previously possible via the permissive `parse_state` helper).
pub(super) async fn enqueue_if_active(
    storage: &SqliteStorage,
    signal: &Signal,
) -> Result<(), StorageError> {
    let mut conn = storage.pool.acquire().await?;

    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

    let row: Option<(String,)> =
        match sqlx::query_as("SELECT state FROM task_instances WHERE id = ?1")
            .bind(signal.instance_id.0.to_string())
            .fetch_optional(&mut *conn)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                rollback_quiet(&mut conn).await;
                return Err(e.into());
            }
        };

    let Some((state_str,)) = row else {
        rollback_quiet(&mut conn).await;
        return Err(StorageError::NotFound {
            entity: "task_instance",
            id: signal.instance_id.0.to_string(),
        });
    };

    let state = match InstanceState::from_str(&state_str) {
        Ok(s) => s,
        Err(e) => {
            rollback_quiet(&mut conn).await;
            return Err(StorageError::Query(e));
        }
    };

    if state.is_terminal() {
        rollback_quiet(&mut conn).await;
        return Err(StorageError::TerminalTarget {
            entity: "task_instance".to_string(),
            id: signal.instance_id.0.to_string(),
        });
    }

    let bound = match bind_signal_insert(sqlx::query(SIGNAL_INSERT_SQL), signal) {
        Ok(q) => q,
        Err(e) => {
            rollback_quiet(&mut conn).await;
            return Err(e);
        }
    };
    if let Err(e) = bound.execute(&mut *conn).await {
        rollback_quiet(&mut conn).await;
        return Err(e.into());
    }

    sqlx::query("COMMIT").execute(&mut *conn).await?;
    Ok(())
}

pub(super) async fn get_pending(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<Signal>, StorageError> {
    let rows = sqlx::query(
        "SELECT * FROM signal_inbox WHERE instance_id=?1 AND delivered=0 ORDER BY created_at",
    )
    .bind(instance_id.0.to_string())
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
    let mut qb = sqlx::QueryBuilder::new("SELECT * FROM signal_inbox WHERE instance_id IN (");
    let mut separated = qb.separated(",");
    for id in instance_ids {
        separated.push_bind(id.0.to_string());
    }
    separated.push_unseparated(") AND delivered=0 ORDER BY created_at");
    let query = qb.build();
    let rows = query.fetch_all(&storage.pool).await?;
    let mut result: HashMap<InstanceId, Vec<Signal>> =
        instance_ids.iter().map(|id| (*id, Vec::new())).collect();
    for row in &rows {
        let signal = row_to_signal(row)?;
        result.entry(signal.instance_id).or_default().push(signal);
    }
    Ok(result)
}

pub(super) async fn mark_delivered(
    storage: &SqliteStorage,
    signal_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE signal_inbox SET delivered=1 WHERE id=?1")
        .bind(signal_id.to_string())
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
    let placeholders: Vec<String> = signal_ids.iter().enumerate().map(|(i, _)| format!("?{}", i + 1)).collect();
    let sql = format!(
        "UPDATE signal_inbox SET delivered=1 WHERE id IN ({})",
        placeholders.join(",")
    );
    let mut query = sqlx::query(&sql);
    for id in signal_ids {
        query = query.bind(id.to_string());
    }
    query.execute(&storage.pool).await?;
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
            Ok((InstanceId(id), state))
        })
        .collect()
}
