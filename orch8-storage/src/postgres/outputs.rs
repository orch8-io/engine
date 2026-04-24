use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;

use super::rows::BlockOutputRow;
use super::PostgresStorage;

/// Append a new `block_outputs` row.
///
/// `block_outputs` is a write-append log: every execution of a block — first
/// attempt, each retry, and each loop / `for_each` iteration — writes its own
/// row. The pair `(instance_id, block_id)` is NOT unique, so callers that
/// want "the current state" of a block must read the most recent row (see
/// [`get`] below). See migration 027 for the schema change that removed the
/// previous UNIQUE constraint.
pub(super) async fn save(
    store: &PostgresStorage,
    output: &BlockOutput,
) -> Result<(), StorageError> {
    sqlx::query(
        r"
        INSERT INTO block_outputs (id, instance_id, block_id, output, output_ref, output_size, attempt, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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

/// Return the most recent `block_outputs` row for `(instance_id, block_id)`.
///
/// With the write-append model (migration 027) multiple rows can share the
/// same `(instance_id, block_id)` pair. Every caller of this function wants
/// "the current state" — the latest attempt / iteration — so we sort by
/// `created_at DESC` and take the first. The supporting composite index
/// `idx_block_outputs_instance_block_created` keeps this cheap.
pub(super) async fn get(
    store: &PostgresStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<Option<BlockOutput>, StorageError> {
    let row = sqlx::query_as::<_, BlockOutputRow>(
        r"SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
           FROM block_outputs
           WHERE instance_id = $1 AND block_id = $2
           ORDER BY created_at DESC
           LIMIT 1",
    )
    .bind(instance_id.0)
    .bind(&block_id.0)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(BlockOutputRow::into_output))
}

pub(super) async fn get_batch(
    store: &PostgresStorage,
    keys: &[(InstanceId, BlockId)],
) -> Result<std::collections::HashMap<(InstanceId, BlockId), BlockOutput>, StorageError> {
    if keys.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let instance_ids: Vec<uuid::Uuid> = keys.iter().map(|(i, _)| i.0).collect();
    let block_ids: Vec<&str> = keys.iter().map(|(_, b)| b.0.as_str()).collect();

    let rows = sqlx::query_as::<_, BlockOutputRow>(
        r"
        WITH pairs(instance_id, block_id) AS (
            SELECT * FROM UNNEST($1::uuid[], $2::text[])
        )
        SELECT DISTINCT ON (b.instance_id, b.block_id)
            b.id, b.instance_id, b.block_id, b.output, b.output_ref, b.output_size, b.attempt, b.created_at
        FROM block_outputs b
        JOIN pairs p ON b.instance_id = p.instance_id AND b.block_id = p.block_id
        ORDER BY b.instance_id, b.block_id, b.created_at DESC
        ",
    )
    .bind(&instance_ids)
    .bind(&block_ids)
    .fetch_all(&store.pool)
    .await?;

    let mut map = std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        let key = (InstanceId(row.instance_id), BlockId(row.block_id.clone()));
        map.insert(key, row.into_output());
    }
    Ok(map)
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

pub(super) async fn get_after_created_at(
    store: &PostgresStorage,
    instance_id: InstanceId,
    after: Option<DateTime<Utc>>,
) -> Result<Vec<BlockOutput>, StorageError> {
    let rows = if let Some(after) = after {
        sqlx::query_as::<_, BlockOutputRow>(
            r"SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
               FROM block_outputs WHERE instance_id = $1 AND created_at > $2 ORDER BY created_at",
        )
        .bind(instance_id.0)
        .bind(after)
        .fetch_all(&store.pool)
        .await?
    } else {
        sqlx::query_as::<_, BlockOutputRow>(
            r"SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
               FROM block_outputs WHERE instance_id = $1 ORDER BY created_at",
        )
        .bind(instance_id.0)
        .fetch_all(&store.pool)
        .await?
    };
    Ok(rows.into_iter().map(BlockOutputRow::into_output).collect())
}

/// Distinct `block_id`s that have produced at least one output for this
/// instance. `DISTINCT` is required because under the write-append model a
/// single block can have multiple rows (loop iterations, retries).
pub(super) async fn get_completed_ids(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Vec<BlockId>, StorageError> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT DISTINCT block_id FROM block_outputs WHERE instance_id = $1")
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
        "SELECT DISTINCT instance_id, block_id FROM block_outputs WHERE instance_id = ANY($1)",
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

/// Delete every `block_outputs` row matching `(instance_id, block_id)`.
///
/// Used by the `loop` / `for_each` iteration-reset path to purge stale
/// composite iteration-counter markers from descendants without disturbing
/// the rest of the write-append history (other instances, other blocks,
/// sibling markers).
pub(super) async fn delete_for_block(
    store: &PostgresStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<u64, StorageError> {
    let result = sqlx::query(r"DELETE FROM block_outputs WHERE instance_id = $1 AND block_id = $2")
        .bind(instance_id.0)
        .bind(&block_id.0)
        .execute(&store.pool)
        .await?;
    Ok(result.rows_affected())
}

/// Batch variant of [`delete_for_block`]: single `DELETE ... = ANY($2)`
/// round-trip for an arbitrary set of block IDs under one instance.
pub(super) async fn delete_for_blocks(
    store: &PostgresStorage,
    instance_id: InstanceId,
    block_ids: &[BlockId],
) -> Result<u64, StorageError> {
    if block_ids.is_empty() {
        return Ok(0);
    }
    let ids: Vec<&str> = block_ids.iter().map(|b| b.0.as_str()).collect();
    let result =
        sqlx::query(r"DELETE FROM block_outputs WHERE instance_id = $1 AND block_id = ANY($2)")
            .bind(instance_id.0)
            .bind(&ids)
            .execute(&store.pool)
            .await?;
    Ok(result.rows_affected())
}

/// Delete ALL `block_outputs` rows for an instance (DLQ retry clean slate).
pub(super) async fn delete_all_for_instance(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<u64, StorageError> {
    let result = sqlx::query(r"DELETE FROM block_outputs WHERE instance_id = $1")
        .bind(instance_id.0)
        .execute(&store.pool)
        .await?;
    Ok(result.rows_affected())
}

/// Append a `block_outputs` row and transition the instance state in one
/// transaction. Like [`save`], this is a pure INSERT under the write-append
/// model — no ON CONFLICT clause.
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

/// Atomic: `INSERT block_outputs` + `UPDATE task_instances` (context, state,
/// `next_fire_at`) in a single transaction.
///
/// Closes the external-worker completion crash window where the previous
/// sequence (`update_instance_context` then `save_output_and_transition`)
/// could leave an instance with merged context but no state transition.
pub(super) async fn save_output_merge_context_and_transition(
    store: &PostgresStorage,
    output: &BlockOutput,
    instance_id: InstanceId,
    context: &orch8_types::context::ExecutionContext,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    let ctx_json = serde_json::to_value(context)?;
    let mut tx = store.pool.begin().await?;

    sqlx::query(
        r"
        INSERT INTO block_outputs (id, instance_id, block_id, output, output_ref, output_size, attempt, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
        "UPDATE task_instances SET context = $2, state = $3, next_fire_at = $4, updated_at = NOW() WHERE id = $1",
    )
    .bind(instance_id.0)
    .bind(&ctx_json)
    .bind(new_state.to_string())
    .bind(next_fire_at)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Delete only sentinel rows (`output_ref = '__in_progress__'`) for an instance.
pub(super) async fn delete_sentinels_for_instance(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"DELETE FROM block_outputs WHERE instance_id = $1 AND output_ref = '__in_progress__'",
    )
    .bind(instance_id.0)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

/// Delete a single `block_outputs` row by its primary key.
pub(super) async fn delete_by_id(store: &PostgresStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query(r"DELETE FROM block_outputs WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}
