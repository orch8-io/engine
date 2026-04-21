use chrono::{DateTime, Utc};
use sqlx::Row;
use std::collections::HashMap;

use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;

use super::helpers::{row_to_output, ts};
use super::SqliteStorage;

pub(super) async fn save(
    storage: &SqliteStorage,
    output: &BlockOutput,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO block_outputs (id,instance_id,block_id,output,output_ref,output_size,attempt,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
    )
    .bind(output.id.to_string())
    .bind(output.instance_id.0.to_string())
    .bind(&output.block_id.0)
    .bind(serde_json::to_string(&output.output)?)
    .bind(&output.output_ref)
    .bind(output.output_size as i64)
    .bind(output.attempt as i64)
    .bind(ts(output.created_at))
    .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<Option<BlockOutput>, StorageError> {
    let row = sqlx::query("SELECT * FROM block_outputs WHERE instance_id=?1 AND block_id=?2 ORDER BY created_at DESC LIMIT 1")
        .bind(instance_id.0.to_string()).bind(&block_id.0)
        .fetch_optional(&storage.pool).await?;
    row.as_ref().map(row_to_output).transpose()
}

pub(super) async fn get_all(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<BlockOutput>, StorageError> {
    let rows = sqlx::query("SELECT * FROM block_outputs WHERE instance_id=?1 ORDER BY created_at")
        .bind(instance_id.0.to_string())
        .fetch_all(&storage.pool)
        .await?;
    rows.iter().map(row_to_output).collect()
}

pub(super) async fn get_after_created_at(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    after: Option<DateTime<Utc>>,
) -> Result<Vec<BlockOutput>, StorageError> {
    let rows = if let Some(after) = after {
        sqlx::query(
            "SELECT * FROM block_outputs WHERE instance_id=?1 AND created_at > ?2 ORDER BY created_at"
        )
        .bind(instance_id.0.to_string())
        .bind(ts(after))
        .fetch_all(&storage.pool)
        .await?
    } else {
        sqlx::query("SELECT * FROM block_outputs WHERE instance_id=?1 ORDER BY created_at")
            .bind(instance_id.0.to_string())
            .fetch_all(&storage.pool)
            .await?
    };
    rows.iter().map(row_to_output).collect()
}

pub(super) async fn get_completed_ids(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<BlockId>, StorageError> {
    let rows = sqlx::query("SELECT DISTINCT block_id FROM block_outputs WHERE instance_id=?1")
        .bind(instance_id.0.to_string())
        .fetch_all(&storage.pool)
        .await?;
    Ok(rows
        .iter()
        .map(|r| BlockId(r.get::<String, _>("block_id")))
        .collect())
}

pub(super) async fn get_completed_ids_batch(
    storage: &SqliteStorage,
    instance_ids: &[InstanceId],
) -> Result<HashMap<InstanceId, Vec<BlockId>>, StorageError> {
    if instance_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders: Vec<String> = (1..=instance_ids.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT DISTINCT instance_id, block_id FROM block_outputs WHERE instance_id IN ({})",
        placeholders.join(",")
    );
    let mut query = sqlx::query(&sql);
    for id in instance_ids {
        query = query.bind(id.0.to_string());
    }
    let rows = query.fetch_all(&storage.pool).await?;
    let mut result: HashMap<InstanceId, Vec<BlockId>> =
        instance_ids.iter().map(|id| (*id, Vec::new())).collect();
    for row in &rows {
        let iid_str: String = row.get("instance_id");
        let block_id = BlockId(row.get::<String, _>("block_id"));
        if let Ok(uuid) = iid_str.parse::<uuid::Uuid>() {
            let iid = InstanceId(uuid);
            result.entry(iid).or_default().push(block_id);
        }
    }
    Ok(result)
}

/// Delete every `block_outputs` row matching `(instance_id, block_id)`.
///
/// Mirror of the Postgres impl: used by the loop / for_each iteration-reset
/// path to purge composite-block iteration-counter markers from descendants
/// when an outer iteration advances.
pub(super) async fn delete_for_block(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<u64, StorageError> {
    let result = sqlx::query("DELETE FROM block_outputs WHERE instance_id=?1 AND block_id=?2")
        .bind(instance_id.0.to_string())
        .bind(&block_id.0)
        .execute(&storage.pool)
        .await?;
    Ok(result.rows_affected())
}

/// Batch variant of [`delete_for_block`]: single `DELETE ... IN (...)`
/// round-trip for an arbitrary set of block IDs under one instance.
pub(super) async fn delete_for_blocks(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_ids: &[BlockId],
) -> Result<u64, StorageError> {
    if block_ids.is_empty() {
        return Ok(0);
    }
    let mut qb = sqlx::QueryBuilder::new("DELETE FROM block_outputs WHERE instance_id=");
    qb.push_bind(instance_id.0.to_string());
    qb.push(" AND block_id IN (");
    let mut sep = qb.separated(", ");
    for bid in block_ids {
        sep.push_bind(&bid.0);
    }
    sep.push_unseparated(")");
    let result = qb.build().execute(&storage.pool).await?;
    Ok(result.rows_affected())
}

pub(super) async fn delete_all_for_instance(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<u64, StorageError> {
    let result = sqlx::query("DELETE FROM block_outputs WHERE instance_id=?1")
        .bind(instance_id.0.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(result.rows_affected())
}

pub(super) async fn save_output_and_transition(
    storage: &SqliteStorage,
    output: &BlockOutput,
    instance_id: InstanceId,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;
    sqlx::query(
        "INSERT INTO block_outputs (id,instance_id,block_id,output,output_ref,output_size,attempt,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
    )
    .bind(output.id.to_string())
    .bind(output.instance_id.0.to_string())
    .bind(&output.block_id.0)
    .bind(serde_json::to_string(&output.output)?)
    .bind(&output.output_ref)
    .bind(output.output_size as i64)
    .bind(output.attempt as i64)
    .bind(ts(output.created_at))
    .execute(&mut *tx).await?;

    sqlx::query("UPDATE task_instances SET state=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
        .bind(instance_id.0.to_string())
        .bind(new_state.to_string())
        .bind(next_fire_at.map(ts))
        .bind(ts(chrono::Utc::now()))
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

/// Atomic: INSERT block_outputs + UPDATE task_instances (context, state,
/// next_fire_at) in a single transaction.
///
/// Used by the external-worker completion path to close the window where
/// the previous two-call sequence (update_instance_context then
/// save_output_and_transition) could crash between calls and leave the
/// instance with merged context but the old state.
pub(super) async fn save_output_merge_context_and_transition(
    storage: &SqliteStorage,
    output: &BlockOutput,
    instance_id: InstanceId,
    context: &ExecutionContext,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;
    sqlx::query(
        "INSERT INTO block_outputs (id,instance_id,block_id,output,output_ref,output_size,attempt,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
    )
    .bind(output.id.to_string())
    .bind(output.instance_id.0.to_string())
    .bind(&output.block_id.0)
    .bind(serde_json::to_string(&output.output)?)
    .bind(&output.output_ref)
    .bind(output.output_size as i64)
    .bind(output.attempt as i64)
    .bind(ts(output.created_at))
    .execute(&mut *tx).await?;

    sqlx::query(
        "UPDATE task_instances SET context=?2, state=?3, next_fire_at=?4, updated_at=?5 WHERE id=?1",
    )
    .bind(instance_id.0.to_string())
    .bind(serde_json::to_string(context)?)
    .bind(new_state.to_string())
    .bind(next_fire_at.map(ts))
    .bind(ts(chrono::Utc::now()))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

pub(super) async fn delete_sentinels_for_instance(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "DELETE FROM block_outputs WHERE instance_id=?1 AND output_ref='__in_progress__'",
    )
    .bind(instance_id.0.to_string())
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn delete_by_id(
    storage: &SqliteStorage,
    id: uuid::Uuid,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM block_outputs WHERE id=?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}
