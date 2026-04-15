use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;

use super::rows::BlockOutputRow;
use super::PostgresStorage;

pub(super) async fn save(
    store: &PostgresStorage,
    output: &BlockOutput,
) -> Result<(), StorageError> {
    sqlx::query(
        r"
        INSERT INTO block_outputs (id, instance_id, block_id, output, output_ref, output_size, attempt, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (instance_id, block_id) DO UPDATE
        SET output = $4, output_ref = $5, output_size = $6, attempt = $7
        ",
    )
    .bind(output.id)
    .bind(output.instance_id.0)
    .bind(&output.block_id.0)
    .bind(&output.output)
    .bind(&output.output_ref)
    .bind(output.output_size)
    .bind(output.attempt)
    .bind(output.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<Option<BlockOutput>, StorageError> {
    let row = sqlx::query_as::<_, BlockOutputRow>(
        r"SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
           FROM block_outputs WHERE instance_id = $1 AND block_id = $2",
    )
    .bind(instance_id.0)
    .bind(&block_id.0)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(BlockOutputRow::into_output))
}

pub(super) async fn get_all(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Vec<BlockOutput>, StorageError> {
    let rows = sqlx::query_as::<_, BlockOutputRow>(
        r"SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
           FROM block_outputs WHERE instance_id = $1 ORDER BY created_at",
    )
    .bind(instance_id.0)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(BlockOutputRow::into_output).collect())
}

pub(super) async fn get_completed_ids(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Vec<BlockId>, StorageError> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT block_id FROM block_outputs WHERE instance_id = $1")
            .bind(instance_id.0)
            .fetch_all(&store.pool)
            .await?;
    Ok(rows.into_iter().map(|(id,)| BlockId(id)).collect())
}

pub(super) async fn get_completed_ids_batch(
    store: &PostgresStorage,
    instance_ids: &[InstanceId],
) -> Result<std::collections::HashMap<InstanceId, Vec<BlockId>>, StorageError> {
    if instance_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let uuids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT instance_id, block_id FROM block_outputs WHERE instance_id = ANY($1)",
    )
    .bind(&uuids)
    .fetch_all(&store.pool)
    .await?;

    let mut map: std::collections::HashMap<InstanceId, Vec<BlockId>> =
        std::collections::HashMap::new();
    for (iid, bid) in rows {
        map.entry(InstanceId(iid)).or_default().push(BlockId(bid));
    }
    Ok(map)
}

pub(super) async fn save_output_and_transition(
    store: &PostgresStorage,
    output: &BlockOutput,
    instance_id: InstanceId,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;

    sqlx::query(
        r"
        INSERT INTO block_outputs (id, instance_id, block_id, output, output_ref, output_size, attempt, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (instance_id, block_id) DO UPDATE
        SET output = $4, output_ref = $5, output_size = $6, attempt = $7
        ",
    )
    .bind(output.id)
    .bind(output.instance_id.0)
    .bind(&output.block_id.0)
    .bind(&output.output)
    .bind(&output.output_ref)
    .bind(output.output_size)
    .bind(output.attempt)
    .bind(output.created_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE task_instances SET state = $2, next_fire_at = $3, updated_at = NOW() WHERE id = $1",
    )
    .bind(instance_id.0)
    .bind(new_state.to_string())
    .bind(next_fire_at)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}
