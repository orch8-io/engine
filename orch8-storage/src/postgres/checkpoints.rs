use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::PostgresStorage;

pub(super) async fn save(
    store: &PostgresStorage,
    cp: &orch8_types::checkpoint::Checkpoint,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO checkpoints (id, instance_id, checkpoint_data, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(cp.id)
    .bind(cp.instance_id.0)
    .bind(&cp.checkpoint_data)
    .bind(cp.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_latest(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
    let row: Option<(Uuid, Uuid, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, instance_id, checkpoint_data, created_at FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(instance_id.0)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(
        |(id, inst_id, data, created_at)| orch8_types::checkpoint::Checkpoint {
            id,
            instance_id: InstanceId(inst_id),
            checkpoint_data: data,
            created_at,
        },
    ))
}

pub(super) async fn list(
    store: &PostgresStorage,
    instance_id: InstanceId,
    limit: u32,
) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
    let rows: Vec<(Uuid, Uuid, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, instance_id, checkpoint_data, created_at FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC LIMIT $2",
    )
    .bind(instance_id.0)
    .bind(i64::from(limit.min(1000)))
    .fetch_all(&store.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, inst_id, data, created_at)| orch8_types::checkpoint::Checkpoint {
                id,
                instance_id: InstanceId(inst_id),
                checkpoint_data: data,
                created_at,
            },
        )
        .collect())
}

pub(super) async fn prune(
    store: &PostgresStorage,
    instance_id: InstanceId,
    keep: u32,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"DELETE FROM checkpoints WHERE instance_id = $1 AND id NOT IN (
            SELECT id FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC LIMIT $2
          )",
    )
    .bind(instance_id.0)
    .bind(i64::from(keep))
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}
