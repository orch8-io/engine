use uuid::Uuid;

use orch8_types::error::StorageError;

use super::rows::ClusterNodeRow;
use super::PostgresStorage;

pub(super) async fn register(
    store: &PostgresStorage,
    node: &orch8_types::cluster::ClusterNode,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO cluster_nodes (id, name, status, registered_at, last_heartbeat_at, drain)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (id) DO UPDATE SET status = $3, last_heartbeat_at = $5, drain = $6",
    )
    .bind(node.id)
    .bind(&node.name)
    .bind(node.status.to_string())
    .bind(node.registered_at)
    .bind(node.last_heartbeat_at)
    .bind(node.drain)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn heartbeat(store: &PostgresStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET last_heartbeat_at = NOW() WHERE id = $1")
        .bind(node_id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn drain(store: &PostgresStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET drain = TRUE, status = 'draining' WHERE id = $1")
        .bind(node_id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn deregister(store: &PostgresStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET status = 'stopped' WHERE id = $1")
        .bind(node_id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn list(
    store: &PostgresStorage,
) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError> {
    let rows = sqlx::query_as::<_, ClusterNodeRow>(
        "SELECT id, name, status, registered_at, last_heartbeat_at, drain
         FROM cluster_nodes ORDER BY registered_at",
    )
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(ClusterNodeRow::into_node).collect())
}

pub(super) async fn should_drain(
    store: &PostgresStorage,
    node_id: Uuid,
) -> Result<bool, StorageError> {
    let row: (bool,) = sqlx::query_as("SELECT drain FROM cluster_nodes WHERE id = $1")
        .bind(node_id)
        .fetch_optional(&store.pool)
        .await?
        .unwrap_or((false,));
    Ok(row.0)
}

pub(super) async fn reap_stale(
    store: &PostgresStorage,
    stale_threshold: std::time::Duration,
) -> Result<u64, StorageError> {
    let threshold_secs = stale_threshold.as_secs() as i64;
    let result = sqlx::query(
        "UPDATE cluster_nodes SET status = 'stopped'
         WHERE status = 'active'
           AND last_heartbeat_at < NOW() - make_interval(secs => $1)",
    )
    .bind(threshold_secs)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}
