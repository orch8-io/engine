use orch8_types::error::StorageError;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::*;

use super::SqliteStorage;
use super::helpers::{row_to_node, ts};

pub(super) async fn create_node(
    storage: &SqliteStorage,
    node: &ExecutionNode,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO execution_tree (id,instance_id,block_id,parent_id,block_type,branch_index,state,started_at,completed_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
    )
    .bind(node.id.into_uuid().to_string())
    .bind(node.instance_id.into_uuid().to_string())
    .bind(node.block_id.as_str())
    .bind(node.parent_id.map(|p| p.to_string()))
    .bind(node.block_type.to_string())
    .bind(node.branch_index.map(|b| b as i32))
    .bind(node.state.to_string())
    .bind(node.started_at.map(ts))
    .bind(node.completed_at.map(ts))
    .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn create_batch(
    storage: &SqliteStorage,
    nodes: &[ExecutionNode],
) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;
    for node in nodes {
        sqlx::query(
            "INSERT INTO execution_tree (id,instance_id,block_id,parent_id,block_type,branch_index,state,started_at,completed_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
        )
        .bind(node.id.into_uuid().to_string())
        .bind(node.instance_id.into_uuid().to_string())
        .bind(node.block_id.as_str())
        .bind(node.parent_id.map(|p| p.to_string()))
        .bind(node.block_type.to_string())
        .bind(node.branch_index.map(|b| b as i32))
        .bind(node.state.to_string())
        .bind(node.started_at.map(ts))
        .bind(node.completed_at.map(ts))
        .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get_tree(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<ExecutionNode>, StorageError> {
    let rows = sqlx::query("SELECT * FROM execution_tree WHERE instance_id=?1 ORDER BY id")
        .bind(instance_id.into_uuid().to_string())
        .fetch_all(&storage.pool)
        .await?;
    rows.iter().map(row_to_node).collect()
}

pub(super) async fn update_node_state(
    storage: &SqliteStorage,
    node_id: ExecutionNodeId,
    state: NodeState,
) -> Result<(), StorageError> {
    let mut conn = storage.pool.acquire().await?;
    update_node_state_on(&mut conn, node_id, state, None).await?;
    Ok(())
}

pub(super) async fn update_node_state_on(
    conn: &mut sqlx::SqliteConnection,
    node_id: ExecutionNodeId,
    state: NodeState,
    instance_id: Option<InstanceId>,
) -> Result<bool, StorageError> {
    let now = super::helpers::ts(chrono::Utc::now());
    let started = (state == NodeState::Running).then_some(&now);
    let completed = matches!(
        state,
        NodeState::Completed | NodeState::Failed | NodeState::Cancelled | NodeState::Skipped
    )
    .then_some(&now);
    let changed = sqlx::query(
        "UPDATE execution_tree SET state=?2, started_at=COALESCE(started_at, ?3), completed_at=COALESCE(?4, completed_at) WHERE id=?1 AND (?5 IS NULL OR instance_id=?5)",
    ).bind(node_id.to_string()).bind(state.to_string()).bind(started).bind(completed)
     .bind(instance_id.map(|id| id.to_string())).execute(&mut *conn).await?;
    Ok(changed.rows_affected() > 0)
}

/// Batch transition `node_ids` to `state` in a single round-trip.
///
/// Mirrors the timestamp rules of [`update_node_state`]:
/// - `Running`  → set `started_at = now`
/// - `Completed`/`Failed`/`Cancelled`/`Skipped` → set `completed_at = now`
/// - other states → leave timestamps alone
///
/// Replaces the per-node UPDATE loop used at iteration boundaries in
/// `Loop` / `ForEach` composites.
pub(super) async fn update_nodes_state(
    storage: &SqliteStorage,
    node_ids: &[ExecutionNodeId],
    state: NodeState,
) -> Result<(), StorageError> {
    if node_ids.is_empty() {
        return Ok(());
    }
    let now = ts(chrono::Utc::now());
    let state_str = state.to_string();

    let mut qb = sqlx::QueryBuilder::new("UPDATE execution_tree SET state=");
    qb.push_bind(state_str);
    match state {
        NodeState::Running => {
            // First-transition-only semantics — see update_node_state.
            qb.push(", started_at=COALESCE(started_at, ");
            qb.push_bind(now);
            qb.push(")");
        }
        NodeState::Completed | NodeState::Failed | NodeState::Cancelled | NodeState::Skipped => {
            qb.push(", completed_at=");
            qb.push_bind(now);
        }
        _ => {}
    }
    qb.push(" WHERE id IN (");
    let mut sep = qb.separated(", ");
    for id in node_ids {
        sep.push_bind(id.to_string());
    }
    sep.push_unseparated(")");
    qb.build().execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn batch_activate_nodes(
    storage: &SqliteStorage,
    node_ids: &[ExecutionNodeId],
) -> Result<(), StorageError> {
    if node_ids.is_empty() {
        return Ok(());
    }
    let now = ts(chrono::Utc::now());
    let mut qb = sqlx::QueryBuilder::new(
        "UPDATE execution_tree SET state='running', started_at=COALESCE(started_at, ",
    );
    qb.push_bind(now);
    qb.push(") WHERE state='pending' AND id IN (");
    let mut sep = qb.separated(", ");
    for id in node_ids {
        sep.push_bind(id.to_string());
    }
    sep.push_unseparated(")");
    qb.build().execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn delete_tree(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM execution_tree WHERE instance_id = ?1")
        .bind(instance_id.into_uuid().to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn get_children(
    storage: &SqliteStorage,
    parent_id: ExecutionNodeId,
) -> Result<Vec<ExecutionNode>, StorageError> {
    let rows =
        sqlx::query("SELECT * FROM execution_tree WHERE parent_id=?1 ORDER BY branch_index, id")
            .bind(parent_id.into_uuid().to_string())
            .fetch_all(&storage.pool)
            .await?;
    rows.iter().map(row_to_node).collect()
}
