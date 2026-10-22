//! Claim-bound step writes serialize with recovery on the instance row.
use super::PostgresStorage;
use chrono::{DateTime, Utc};
use orch8_types::{error::StorageError, ids::InstanceId, output::BlockOutput};

async fn begin(
    store: &PostgresStorage,
    instance_id: InstanceId,
    claim_token: DateTime<Utc>,
) -> Result<Option<sqlx::Transaction<'static, sqlx::Postgres>>, StorageError> {
    let mut tx = store.pool.begin().await?;
    let held: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT id FROM task_instances WHERE id=$1 AND state='running' AND claimed_at=$2 FOR UPDATE",
    ).bind(instance_id.into_uuid()).bind(claim_token).fetch_optional(&mut *tx).await?;
    if held.is_none() {
        tx.rollback().await?;
        return Ok(None);
    }
    Ok(Some(tx))
}

pub(super) async fn output(
    store: &PostgresStorage,
    output: &BlockOutput,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, output.instance_id, claim_token).await? else {
        return Ok(false);
    };
    super::outputs::save_on(&mut tx, output).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn cache(
    store: &PostgresStorage,
    instance_id: InstanceId,
    key: &str,
    value: &serde_json::Value,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    super::kv_state::set_instance_kv_on(&mut tx, instance_id, key, value).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn delete_kv(
    store: &PostgresStorage,
    instance_id: InstanceId,
    key: &str,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    sqlx::query("DELETE FROM instance_kv_state WHERE instance_id=$1 AND key=$2")
        .bind(instance_id.into_uuid())
        .bind(key)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn externalized(
    store: &PostgresStorage,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    super::externalized::save_on(&mut tx, instance_id, ref_key, payload).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn node(
    store: &PostgresStorage,
    instance_id: InstanceId,
    node_id: orch8_types::ids::ExecutionNodeId,
    state: orch8_types::execution::NodeState,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    let changed =
        super::execution_tree::update_node_state_on(&mut tx, node_id, state, Some(instance_id))
            .await?;
    tx.commit().await?;
    Ok(changed)
}

pub(super) async fn create_nodes(
    store: &PostgresStorage,
    instance_id: InstanceId,
    nodes: &[orch8_types::execution::ExecutionNode],
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    let ids: std::collections::HashSet<_> = nodes.iter().map(|node| node.id).collect();
    let parents: Vec<_> = nodes
        .iter()
        .filter_map(|node| node.parent_id)
        .filter(|id| !ids.contains(id))
        .collect();
    if nodes.iter().any(|node| node.instance_id != instance_id)
        || !owns_nodes(&mut tx, instance_id, &parents).await?
    {
        tx.rollback().await?;
        return Ok(false);
    }
    super::execution_tree::create_batch_on(&mut tx, nodes).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn nodes_state(
    store: &PostgresStorage,
    instance_id: InstanceId,
    node_ids: &[orch8_types::ids::ExecutionNodeId],
    state: orch8_types::execution::NodeState,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    if !owns_nodes(&mut tx, instance_id, node_ids).await? {
        tx.rollback().await?;
        return Ok(false);
    }
    super::execution_tree::update_nodes_state_on(&mut tx, node_ids, state).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn activate_nodes(
    store: &PostgresStorage,
    instance_id: InstanceId,
    node_ids: &[orch8_types::ids::ExecutionNodeId],
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    if !owns_nodes(&mut tx, instance_id, node_ids).await? {
        tx.rollback().await?;
        return Ok(false);
    }
    super::execution_tree::batch_activate_nodes_on(&mut tx, node_ids).await?;
    tx.commit().await?;
    Ok(true)
}

async fn owns_nodes(
    conn: &mut sqlx::PgConnection,
    instance_id: InstanceId,
    node_ids: &[orch8_types::ids::ExecutionNodeId],
) -> Result<bool, StorageError> {
    if node_ids.is_empty() {
        return Ok(true);
    }
    let ids: std::collections::HashSet<_> = node_ids.iter().map(|id| id.into_uuid()).collect();
    let expected = ids.len();
    let ids: Vec<_> = ids.into_iter().collect();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_tree WHERE instance_id=$1 AND id=ANY($2)",
    )
    .bind(instance_id.into_uuid())
    .bind(&ids)
    .fetch_one(conn)
    .await?;
    Ok(usize::try_from(count).ok() == Some(expected))
}

pub(super) async fn cancel_worker_blocks(
    store: &PostgresStorage,
    instance_id: InstanceId,
    block_ids: &[String],
    claim_token: DateTime<Utc>,
) -> Result<Option<u64>, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(None);
    };
    let changed =
        super::workers::cancel_for_blocks_on(&mut tx, instance_id.into_uuid(), block_ids).await?;
    tx.commit().await?;
    Ok(Some(changed))
}

pub(super) async fn context_cas(
    store: &PostgresStorage,
    id: InstanceId,
    context: &orch8_types::context::ExecutionContext,
    expected: DateTime<Utc>,
    claim_token: DateTime<Utc>,
) -> Result<Option<bool>, StorageError> {
    let Some(mut tx) = begin(store, id, claim_token).await? else {
        return Ok(None);
    };
    let changed = super::instances::update_context_cas_on(&mut tx, id, context, expected).await?;
    tx.commit().await?;
    Ok(Some(changed))
}
pub(super) async fn merge_context(
    store: &PostgresStorage,
    id: InstanceId,
    key: &str,
    value: &serde_json::Value,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, id, claim_token).await? else {
        return Ok(false);
    };
    super::instances::merge_context_data_on(&mut tx, id, key, value).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn delete_outputs(
    store: &PostgresStorage,
    id: InstanceId,
    blocks: &[orch8_types::ids::BlockId],
    token: DateTime<Utc>,
) -> Result<Option<u64>, StorageError> {
    let Some(mut tx) = begin(store, id, token).await? else {
        return Ok(None);
    };
    let count = super::outputs::delete_for_blocks_on(&mut tx, id, blocks).await?;
    tx.commit().await?;
    Ok(Some(count))
}
pub(super) async fn delete_output(
    store: &PostgresStorage,
    instance_id: InstanceId,
    id: uuid::Uuid,
    token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, token).await? else {
        return Ok(false);
    };
    super::outputs::delete_owned_by_id_on(&mut tx, instance_id, id).await?;
    tx.commit().await?;
    Ok(true)
}
pub(super) async fn merge_metadata(
    store: &PostgresStorage,
    id: InstanceId,
    patch: &serde_json::Value,
    token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, id, token).await? else {
        return Ok(false);
    };
    super::instances::merge_metadata_on(&mut tx, id, patch).await?;
    tx.commit().await?;
    Ok(true)
}
pub(super) async fn delete_receipts(
    store: &PostgresStorage,
    tenant: &orch8_types::ids::TenantId,
    id: InstanceId,
    blocks: &[orch8_types::ids::BlockId],
    token: DateTime<Utc>,
) -> Result<Option<u64>, StorageError> {
    let Some(mut tx) = begin(store, id, token).await? else {
        return Ok(None);
    };
    let count =
        super::continuity::delete_receipts_for_blocks_on(&mut tx, tenant, id, blocks).await?;
    tx.commit().await?;
    Ok(Some(count))
}
