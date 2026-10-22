//! Claim-bound step writes serialize with recovery on the instance row.
use super::SqliteStorage;
use chrono::{DateTime, Utc};
use orch8_types::{error::StorageError, ids::InstanceId, output::BlockOutput};

async fn begin(
    store: &SqliteStorage,
    instance_id: InstanceId,
    claim_token: DateTime<Utc>,
) -> Result<Option<sqlx::Transaction<'static, sqlx::Sqlite>>, StorageError> {
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let held: Option<String> = sqlx::query_scalar(
        "SELECT id FROM task_instances WHERE id=?1 AND state='running' AND claimed_at=?2",
    )
    .bind(instance_id.to_string())
    .bind(super::helpers::ts(claim_token))
    .fetch_optional(&mut *tx)
    .await?;
    if held.is_none() {
        tx.rollback().await?;
        return Ok(None);
    }
    Ok(Some(tx))
}

pub(super) async fn output(
    store: &SqliteStorage,
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
    store: &SqliteStorage,
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
    store: &SqliteStorage,
    instance_id: InstanceId,
    key: &str,
    claim_token: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let Some(mut tx) = begin(store, instance_id, claim_token).await? else {
        return Ok(false);
    };
    sqlx::query("DELETE FROM instance_kv_state WHERE instance_id=?1 AND key=?2")
        .bind(instance_id.to_string())
        .bind(key)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn externalized(
    store: &SqliteStorage,
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
    store: &SqliteStorage,
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
