use orch8_types::checkpoint::Checkpoint;
use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::helpers::{row_to_checkpoint, ts};
use super::SqliteStorage;

pub(super) async fn save(storage: &SqliteStorage, cp: &Checkpoint) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO checkpoints (id,instance_id,checkpoint_data,created_at) VALUES (?1,?2,?3,?4)",
    )
    .bind(cp.id.to_string())
    .bind(cp.instance_id.0.to_string())
    .bind(serde_json::to_string(&cp.checkpoint_data)?)
    .bind(ts(cp.created_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_latest(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Option<Checkpoint>, StorageError> {
    let row = sqlx::query(
        "SELECT * FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(instance_id.0.to_string())
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_checkpoint).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<Checkpoint>, StorageError> {
    let rows =
        sqlx::query("SELECT * FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC")
            .bind(instance_id.0.to_string())
            .fetch_all(&storage.pool)
            .await?;
    rows.iter().map(row_to_checkpoint).collect()
}

pub(super) async fn prune(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    keep: u32,
) -> Result<u64, StorageError> {
    // Keep the latest N, delete the rest.
    let result = sqlx::query(
        "DELETE FROM checkpoints WHERE instance_id=?1 AND id NOT IN (SELECT id FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC LIMIT ?2)"
    )
    .bind(instance_id.0.to_string())
    .bind(keep as i64)
    .execute(&storage.pool)
    .await
    ?;
    Ok(result.rows_affected())
}
