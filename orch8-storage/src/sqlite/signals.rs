use std::collections::HashMap;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::signal::Signal;

use super::helpers::{row_to_signal, ts};
use super::SqliteStorage;

pub(super) async fn enqueue(storage: &SqliteStorage, signal: &Signal) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO signal_inbox (id,instance_id,signal_type,payload,delivered,created_at) VALUES (?1,?2,?3,?4,0,?5)")
        .bind(signal.id.to_string())
        .bind(signal.instance_id.0.to_string())
        .bind(serde_json::to_string(&signal.signal_type)?)
        .bind(serde_json::to_string(&signal.payload)?)
        .bind(ts(signal.created_at))
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
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
    let mut result: HashMap<InstanceId, Vec<Signal>> = instance_ids
        .iter()
        .map(|id| (*id, Vec::new()))
        .collect();
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
