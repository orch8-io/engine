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

/// The parent row stays locked until the new child has committed.
pub(super) async fn create_child(
    store: &PostgresStorage,
    parent_id: InstanceId,
    child: &orch8_types::instance::TaskInstance,
    token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    if child.parent_instance_id != Some(parent_id) || child.id == parent_id {
        return Ok(false);
    }
    let Some(mut tx) = begin(store, parent_id, token).await? else {
        return Ok(false);
    };
    if !same_child_scope(&mut tx, parent_id, child).await? {
        tx.rollback().await?;
        return Ok(false);
    }
    let context = serde_json::to_value(&child.context)?;
    super::instances::bind_instance_insert(
        sqlx::query(super::instances::INSTANCE_INSERT_SQL),
        child,
        &context,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

async fn same_child_scope(
    conn: &mut sqlx::PgConnection,
    parent_id: InstanceId,
    child: &orch8_types::instance::TaskInstance,
) -> Result<bool, StorageError> {
    if child.parent_instance_id != Some(parent_id) || child.id == parent_id {
        return Ok(false);
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances WHERE id=$1 AND tenant_id=$2 AND namespace=$3 AND sub_tenant IS NOT DISTINCT FROM $4")
        .bind(parent_id.into_uuid())
        .bind(child.tenant_id.as_str())
        .bind(child.namespace.as_str())
        .bind(child.sub_tenant.as_deref())
        .fetch_one(&mut *conn).await?;
    Ok(count == 1)
}

pub(super) async fn adopt_child(
    store: &PostgresStorage,
    parent_id: InstanceId,
    child_id: InstanceId,
    expected_activation: &str,
    activation: &str,
    token: DateTime<Utc>,
) -> Result<Option<crate::ChildAdoption>, StorageError> {
    use orch8_types::instance::InstanceState;
    let Some(mut tx) = begin(store, parent_id, token).await? else {
        return Ok(None);
    };
    let locked: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT id FROM task_instances WHERE id=$1 FOR UPDATE")
            .bind(child_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await?;
    if locked.is_none() {
        tx.rollback().await?;
        return Ok(Some(crate::ChildAdoption::Ineligible));
    }
    let Some(child) = super::instances::get_on(&mut tx, child_id).await? else {
        tx.rollback().await?;
        return Ok(Some(crate::ChildAdoption::Ineligible));
    };
    if child.state == InstanceState::Cancelled
        || child.metadata.get("_superseded").is_some()
        || !same_child_scope(&mut tx, parent_id, &child).await?
    {
        tx.rollback().await?;
        return Ok(Some(crate::ChildAdoption::Ineligible));
    }
    let current_activation = child
        .metadata
        .get("_parent_activation")
        .and_then(serde_json::Value::as_str);
    // A retry of a committed old -> new request must not reset the child's
    // run a second time, even if it has since run and failed again.
    if current_activation == Some(activation) && expected_activation != activation {
        tx.commit().await?;
        return Ok(Some(crate::ChildAdoption::Adopted(Box::new(child))));
    }
    if current_activation != Some(expected_activation) {
        tx.rollback().await?;
        return Ok(Some(crate::ChildAdoption::Ineligible));
    }
    super::instances::merge_metadata_on(
        &mut tx,
        child_id,
        &serde_json::json!({"_parent_activation": activation}),
    )
    .await?;
    if child.state == InstanceState::Failed {
        super::execution_tree::delete_tree_on(&mut tx, child_id).await?;
        super::outputs::delete_sentinels_for_instance_on(&mut tx, child_id).await?;
        super::instances::reset_run_on(&mut tx, child_id, &uuid::Uuid::now_v7().to_string())
            .await?;
        if !super::instances::conditional_update_state_on(
            &mut tx,
            child_id,
            InstanceState::Failed,
            InstanceState::Scheduled,
            Some(Utc::now()),
        )
        .await?
        {
            tx.rollback().await?;
            return Ok(Some(crate::ChildAdoption::Ineligible));
        }
    }
    let adopted = super::instances::get_on(&mut tx, child_id)
        .await?
        .ok_or_else(|| {
            StorageError::Query("adopted child disappeared inside its locked transaction".into())
        })?;
    tx.commit().await?;
    Ok(Some(crate::ChildAdoption::Adopted(Box::new(adopted))))
}

pub(super) async fn ensure_run_started(
    store: &PostgresStorage,
    id: InstanceId,
    run_id: &str,
    started_at: DateTime<Utc>,
    token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, id, token).await? else {
        return Ok(false);
    };
    super::instances::ensure_run_started_on(&mut tx, id, run_id, started_at).await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn deliver_signal(
    store: &PostgresStorage,
    instance_id: InstanceId,
    signal_id: uuid::Uuid,
    token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, token).await? else {
        return Ok(false);
    };
    super::signals::mark_owned_delivered_on(&mut tx, instance_id, signal_id).await?;
    tx.commit().await?;
    Ok(true)
}
