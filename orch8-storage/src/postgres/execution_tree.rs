use chrono::Utc;

use orch8_types::error::StorageError;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::{ExecutionNodeId, InstanceId};

use super::rows::ExecutionNodeRow;
use super::PostgresStorage;

pub(super) async fn create_node(
    store: &PostgresStorage,
    node: &ExecutionNode,
) -> Result<(), StorageError> {
    sqlx::query(
        r"
        INSERT INTO execution_tree
            (id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ",
    )
    .bind(node.id.0)
    .bind(node.instance_id.0)
    .bind(&node.block_id.0)
    .bind(node.parent_id.map(|p| p.0))
    .bind(node.block_type.to_string())
    .bind(node.branch_index)
    .bind(node.state.to_string())
    .bind(node.started_at)
    .bind(node.completed_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn create_batch(
    store: &PostgresStorage,
    nodes: &[ExecutionNode],
) -> Result<(), StorageError> {
    if nodes.is_empty() {
        return Ok(());
    }
    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO execution_tree (id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at) ",
    );
    qb.push_values(nodes, |mut b, node| {
        b.push_bind(node.id.0)
            .push_bind(node.instance_id.0)
            .push_bind(&node.block_id.0)
            .push_bind(node.parent_id.map(|p| p.0))
            .push_bind(node.block_type.to_string())
            .push_bind(node.branch_index)
            .push_bind(node.state.to_string())
            .push_bind(node.started_at)
            .push_bind(node.completed_at);
    });
    qb.build().execute(&store.pool).await?;
    Ok(())
}

pub(super) async fn get_tree(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Vec<ExecutionNode>, StorageError> {
    let rows = sqlx::query_as::<_, ExecutionNodeRow>(
        r"SELECT id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at
           FROM execution_tree WHERE instance_id = $1 ORDER BY id",
    )
    .bind(instance_id.0)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(ExecutionNodeRow::into_node).collect())
}

pub(super) async fn update_node_state(
    store: &PostgresStorage,
    node_id: ExecutionNodeId,
    state: NodeState,
) -> Result<(), StorageError> {
    let now = Utc::now();
    let completed_at = if matches!(
        state,
        NodeState::Completed | NodeState::Failed | NodeState::Cancelled
    ) {
        Some(now)
    } else {
        None
    };
    // Set started_at on the first transition to Running (COALESCE keeps any
    // existing value so re-dispatch after Waiting→Running doesn't overwrite).
    let started_at = if state == NodeState::Running {
        Some(now)
    } else {
        None
    };
    sqlx::query(
        "UPDATE execution_tree SET state = $2, completed_at = COALESCE($3, completed_at), started_at = COALESCE($4, started_at) WHERE id = $1",
    )
    .bind(node_id.0)
    .bind(state.to_string())
    .bind(completed_at)
    .bind(started_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn batch_activate_nodes(
    store: &PostgresStorage,
    node_ids: &[ExecutionNodeId],
) -> Result<(), StorageError> {
    if node_ids.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    let mut qb = sqlx::QueryBuilder::new(
        "UPDATE execution_tree SET state = 'running', started_at = COALESCE(started_at, ",
    );
    qb.push_bind(now);
    qb.push(") WHERE state = 'pending' AND id IN (");
    let mut sep = qb.separated(", ");
    for id in node_ids {
        sep.push_bind(id.0);
    }
    sep.push_unseparated(")");
    qb.build().execute(&store.pool).await?;
    Ok(())
}

/// Batch transition `node_ids` to `state` in a single round-trip. Mirrors
/// the timestamp rules of [`update_node_state`].
pub(super) async fn update_nodes_state(
    store: &PostgresStorage,
    node_ids: &[ExecutionNodeId],
    state: NodeState,
) -> Result<(), StorageError> {
    if node_ids.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    let completed_at = if matches!(
        state,
        NodeState::Completed | NodeState::Failed | NodeState::Cancelled | NodeState::Skipped
    ) {
        Some(now)
    } else {
        None
    };
    let started_at = if state == NodeState::Running {
        Some(now)
    } else {
        None
    };
    let ids: Vec<uuid::Uuid> = node_ids.iter().map(|id| id.0).collect();
    sqlx::query(
        "UPDATE execution_tree \
         SET state = $1, \
             completed_at = COALESCE($2, completed_at), \
             started_at   = COALESCE($3, started_at) \
         WHERE id = ANY($4)",
    )
    .bind(state.to_string())
    .bind(completed_at)
    .bind(started_at)
    .bind(&ids)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete_tree(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM execution_tree WHERE instance_id = $1")
        .bind(instance_id.0)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn get_children(
    store: &PostgresStorage,
    parent_id: ExecutionNodeId,
) -> Result<Vec<ExecutionNode>, StorageError> {
    let rows = sqlx::query_as::<_, ExecutionNodeRow>(
        r"SELECT id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at
           FROM execution_tree WHERE parent_id = $1 ORDER BY branch_index, id",
    )
    .bind(parent_id.0)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(ExecutionNodeRow::into_node).collect())
}
