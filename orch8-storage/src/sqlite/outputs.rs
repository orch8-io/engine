use chrono::{DateTime, Utc};
use sqlx::Row;
use std::collections::HashMap;

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
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<Option<BlockOutput>, StorageError> {
    let row = sqlx::query("SELECT * FROM block_outputs WHERE instance_id=?1 AND block_id=?2 ORDER BY created_at DESC LIMIT 1")
        .bind(instance_id.0.to_string()).bind(&block_id.0)
        .fetch_optional(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    row.as_ref().map(row_to_output).transpose()
}

pub(super) async fn get_all(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<BlockOutput>, StorageError> {
    let rows = sqlx::query("SELECT * FROM block_outputs WHERE instance_id=?1 ORDER BY created_at")
        .bind(instance_id.0.to_string())
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_output).collect()
}

pub(super) async fn get_completed_ids(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<BlockId>, StorageError> {
    let rows = sqlx::query("SELECT DISTINCT block_id FROM block_outputs WHERE instance_id=?1")
        .bind(instance_id.0.to_string())
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
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
    let rows = query
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
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
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn save_output_and_transition(
    storage: &SqliteStorage,
    output: &BlockOutput,
    instance_id: InstanceId,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
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
    .execute(&mut *tx).await.map_err(|e| StorageError::Query(e.to_string()))?;

    sqlx::query("UPDATE task_instances SET state=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
        .bind(instance_id.0.to_string())
        .bind(new_state.to_string())
        .bind(next_fire_at.map(ts))
        .bind(ts(chrono::Utc::now()))
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
