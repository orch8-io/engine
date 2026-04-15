use chrono::Utc;
use sqlx::Row;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::cluster::ClusterNode;
use orch8_types::error::StorageError;

use super::helpers::{row_to_cluster_node, ts};
use super::SqliteStorage;

pub(super) async fn register(
    storage: &SqliteStorage,
    node: &ClusterNode,
) -> Result<(), StorageError> {
    sqlx::query("INSERT OR REPLACE INTO cluster_nodes (id,name,status,registered_at,last_heartbeat_at,drain) VALUES (?1,?2,?3,?4,?5,?6)")
        .bind(node.id.to_string())
        .bind(&node.name)
        .bind(node.status.to_string())
        .bind(ts(node.registered_at))
        .bind(ts(node.last_heartbeat_at))
        .bind(node.drain as i32)
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn heartbeat(storage: &SqliteStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET last_heartbeat_at=?2 WHERE id=?1")
        .bind(node_id.to_string())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn drain(storage: &SqliteStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET drain=1, status='draining' WHERE id=?1")
        .bind(node_id.to_string())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn deregister(storage: &SqliteStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET status='stopped' WHERE id=?1")
        .bind(node_id.to_string())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn list(storage: &SqliteStorage) -> Result<Vec<ClusterNode>, StorageError> {
    let rows = sqlx::query("SELECT * FROM cluster_nodes ORDER BY registered_at")
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(rows.iter().map(row_to_cluster_node).collect())
}

pub(super) async fn should_drain(
    storage: &SqliteStorage,
    node_id: Uuid,
) -> Result<bool, StorageError> {
    let row = sqlx::query("SELECT drain FROM cluster_nodes WHERE id=?1")
        .bind(node_id.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.is_some_and(|r| r.get::<i32, _>("drain") != 0))
}

#[allow(clippy::unused_async)]
pub(super) async fn reap_stale(
    storage: &SqliteStorage,
    _stale_threshold: Duration,
) -> Result<u64, StorageError> {
    // Simplified for SQLite test mode
    let _ = storage;
    Ok(0)
}
