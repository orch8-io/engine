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
        .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn heartbeat(storage: &SqliteStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET last_heartbeat_at=?2 WHERE id=?1")
        .bind(node_id.to_string())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn drain(storage: &SqliteStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET drain=1, status='draining' WHERE id=?1")
        .bind(node_id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn deregister(storage: &SqliteStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET status='stopped' WHERE id=?1")
        .bind(node_id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn list(storage: &SqliteStorage) -> Result<Vec<ClusterNode>, StorageError> {
    let rows = sqlx::query("SELECT * FROM cluster_nodes ORDER BY registered_at")
        .fetch_all(&storage.pool)
        .await?;
    rows.iter().map(row_to_cluster_node).collect()
}

pub(super) async fn should_drain(
    storage: &SqliteStorage,
    node_id: Uuid,
) -> Result<bool, StorageError> {
    let row = sqlx::query("SELECT drain FROM cluster_nodes WHERE id=?1")
        .bind(node_id.to_string())
        .fetch_optional(&storage.pool)
        .await?;
    Ok(row.is_some_and(|r| r.get::<i32, _>("drain") != 0))
}

pub(super) async fn reap_stale(
    storage: &SqliteStorage,
    stale_threshold: Duration,
) -> Result<u64, StorageError> {
    // Mirror the Postgres backend: flip any `active` node whose last
    // heartbeat is older than the cutoff to `stopped`. Previously a no-op
    // stub — which left dead nodes listed as active forever in SQLite
    // mode, and masked cluster-drain behavior in local dev.
    let cutoff = Utc::now()
        - chrono::Duration::from_std(stale_threshold)
            .unwrap_or_else(|_| chrono::Duration::seconds(300));
    let result = sqlx::query(
        "UPDATE cluster_nodes SET status='stopped'
         WHERE status='active' AND last_heartbeat_at < ?1",
    )
    .bind(ts(cutoff))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected())
}
