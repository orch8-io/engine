use uuid::Uuid;

use orch8_types::error::StorageError;

use super::PostgresStorage;
use super::rows::ClusterNodeRow;

pub(super) async fn register(
    store: &PostgresStorage,
    node: &orch8_types::cluster::ClusterNode,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO cluster_nodes (id, name, status, registered_at, last_heartbeat_at, drain, drain_started_at, stopped_at, capabilities_withdrawn, execution_handoff_evidence)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (id) DO UPDATE SET status = $3, last_heartbeat_at = $5, drain = $6, drain_started_at = $7, stopped_at = $8, capabilities_withdrawn = $9, execution_handoff_evidence = $10",
    )
    .bind(node.id)
    .bind(&node.name)
    .bind(node.status.to_string())
    .bind(node.registered_at)
    .bind(node.last_heartbeat_at)
    .bind(node.drain)
    .bind(node.drain_started_at)
    .bind(node.stopped_at)
    .bind(node.capabilities_withdrawn)
    .bind(&node.execution_handoff_evidence)
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
    sqlx::query("UPDATE cluster_nodes SET drain = TRUE, status = 'draining', drain_started_at = COALESCE(drain_started_at, NOW()), capabilities_withdrawn = TRUE WHERE id = $1")
        .bind(node_id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn deregister(store: &PostgresStorage, node_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("UPDATE cluster_nodes SET status = 'stopped', stopped_at = NOW(), capabilities_withdrawn = TRUE, execution_handoff_evidence = 'scheduler_drained; in_flight_work_completed_or_durably_recoverable' WHERE id = $1")
        .bind(node_id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn list(
    store: &PostgresStorage,
) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError> {
    let rows = sqlx::query_as::<_, ClusterNodeRow>(
        "SELECT id, name, status, registered_at, last_heartbeat_at, drain, drain_started_at, stopped_at, capabilities_withdrawn, execution_handoff_evidence
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
        "UPDATE cluster_nodes SET status = 'stopped', stopped_at = NOW()
         WHERE status = 'active'
           AND last_heartbeat_at < NOW() - make_interval(secs => $1)",
    )
    .bind(threshold_secs)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}
