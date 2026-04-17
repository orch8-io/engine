use std::collections::HashMap;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::signal::Signal;

use super::helpers::{parse_state, row_to_signal, ts};
use super::SqliteStorage;

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
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

/// Atomic enqueue gated on target non-terminal state.
///
/// BEGIN IMMEDIATE acquires a write lock up-front so no other writer can
/// transition `task_instances` between our SELECT and our INSERT — this is the
/// SQLite equivalent of Postgres's `SELECT ... FOR UPDATE`. Keeps the txn
/// tight: begin → select → (maybe) insert → commit, no unrelated awaits.
pub(super) async fn enqueue_if_active(
    storage: &SqliteStorage,
    signal: &Signal,
) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;

    let row: Option<(String,)> = sqlx::query_as("SELECT state FROM task_instances WHERE id = ?1")
        .bind(signal.instance_id.0.to_string())
        .fetch_optional(&mut *tx)
        .await?;

    let Some((state_str,)) = row else {
        // Rollback is implicit on drop but explicit is clearer.
        tx.rollback().await?;
        return Err(StorageError::NotFound {
            entity: "task_instance",
            id: signal.instance_id.0.to_string(),
        });
    };

    if parse_state(&state_str).is_terminal() {
        tx.rollback().await?;
        return Err(StorageError::Conflict(format!(
            "target instance {} is in terminal state '{}'",
            signal.instance_id.0, state_str
        )));
    }

    bind_signal_insert(sqlx::query(SIGNAL_INSERT_SQL), signal)?
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
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
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_signal).collect()
}

pub(super) async fn get_pending_batch(
    storage: &SqliteStorage,
    instance_ids: &[InstanceId],
) -> Result<HashMap<InstanceId, Vec<Signal>>, StorageError> {
    if instance_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders: Vec<String> = (1..=instance_ids.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT * FROM signal_inbox WHERE instance_id IN ({}) AND delivered=0 ORDER BY created_at",
        placeholders.join(",")
    );
    let mut query = sqlx::query(&sql);
    for id in instance_ids {
        query = query.bind(id.0.to_string());
    }
    let rows = query
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
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
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn mark_delivered_batch(
    storage: &SqliteStorage,
    signal_ids: &[Uuid],
) -> Result<(), StorageError> {
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    for id in signal_ids {
        sqlx::query("UPDATE signal_inbox SET delivered=1 WHERE id=?1")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
