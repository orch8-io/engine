use orch8_types::error::StorageError;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::*;

use super::helpers::{row_to_node, ts};
use super::SqliteStorage;

pub(super) async fn create_node(
    storage: &SqliteStorage,
    node: &ExecutionNode,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO execution_tree (id,instance_id,block_id,parent_id,block_type,branch_index,state,started_at,completed_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
    )
    .bind(node.id.0.to_string())
    .bind(node.instance_id.0.to_string())
    .bind(&node.block_id.0)
    .bind(node.parent_id.map(|p| p.0.to_string()))
    .bind(node.block_type.to_string())
    .bind(node.branch_index.map(|b| b as i32))
    .bind(node.state.to_string())
    .bind(node.started_at.map(ts))
    .bind(node.completed_at.map(ts))
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn create_batch(
    storage: &SqliteStorage,
    nodes: &[ExecutionNode],
) -> Result<(), StorageError> {
    for node in nodes {
        create_node(storage, node).await?;
    }
    Ok(())
}

pub(super) async fn get_tree(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<ExecutionNode>, StorageError> {
    let rows = sqlx::query("SELECT * FROM execution_tree WHERE instance_id=?1")
        .bind(instance_id.0.to_string())
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(rows.iter().map(row_to_node).collect())
}

pub(super) async fn update_node_state(
    storage: &SqliteStorage,
    node_id: ExecutionNodeId,
    state: NodeState,
) -> Result<(), StorageError> {
    let now = ts(chrono::Utc::now());
    let (started, completed) = match state {
        NodeState::Running => (Some(now.clone()), None),
        NodeState::Completed | NodeState::Failed | NodeState::Cancelled | NodeState::Skipped => {
            (None, Some(now.clone()))
        }
        _ => (None, None),
    };
    if let Some(ref s) = started {
        sqlx::query("UPDATE execution_tree SET state=?2, started_at=?3 WHERE id=?1")
            .bind(node_id.0.to_string())
            .bind(state.to_string())
            .bind(s)
            .execute(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    } else if let Some(ref c) = completed {
        sqlx::query("UPDATE execution_tree SET state=?2, completed_at=?3 WHERE id=?1")
            .bind(node_id.0.to_string())
            .bind(state.to_string())
            .bind(c)
            .execute(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    } else {
        sqlx::query("UPDATE execution_tree SET state=?2 WHERE id=?1")
            .bind(node_id.0.to_string())
            .bind(state.to_string())
            .execute(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    Ok(())
}

pub(super) async fn get_children(
    storage: &SqliteStorage,
    parent_id: ExecutionNodeId,
) -> Result<Vec<ExecutionNode>, StorageError> {
    let rows = sqlx::query("SELECT * FROM execution_tree WHERE parent_id=?1")
        .bind(parent_id.0.to_string())
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(rows.iter().map(row_to_node).collect())
}
