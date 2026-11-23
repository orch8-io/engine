use orch8_types::checkpoint::Checkpoint;
use orch8_types::error::StorageError;
use orch8_types::ids::*;
use uuid::Uuid;

use super::SqliteStorage;
use super::helpers::{row_to_checkpoint, ts};

pub(super) async fn save(storage: &SqliteStorage, cp: &Checkpoint) -> Result<(), StorageError> {
    let mut conn = storage.pool.acquire().await?;
    save_on(&mut conn, cp).await
}

pub(super) async fn save_on(
    conn: &mut sqlx::SqliteConnection,
    cp: &Checkpoint,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO checkpoints (id,instance_id,checkpoint_data,created_at) VALUES (?1,?2,?3,?4)",
    )
    .bind(cp.id.to_string())
    .bind(cp.instance_id.into_uuid().to_string())
    .bind(serde_json::to_string(&cp.checkpoint_data)?)
    .bind(ts(cp.created_at))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub(super) async fn get_latest(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Option<Checkpoint>, StorageError> {
    let mut conn = storage.pool.acquire().await?;
    get_latest_on(&mut conn, instance_id).await
}

pub(super) async fn get_latest_on(
    conn: &mut sqlx::SqliteConnection,
    instance_id: InstanceId,
) -> Result<Option<Checkpoint>, StorageError> {
    let row = sqlx::query(
        "SELECT * FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(instance_id.into_uuid().to_string())
    .fetch_optional(&mut *conn)
    .await?;
    row.as_ref().map(row_to_checkpoint).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    limit: u32,
) -> Result<Vec<Checkpoint>, StorageError> {
    let cap = limit.min(1000) as i64;
    let rows = sqlx::query(
        "SELECT * FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC, id DESC LIMIT ?2",
    )
    .bind(instance_id.into_uuid().to_string())
    .bind(cap)
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_checkpoint).collect()
}

pub(super) async fn page(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    after: Option<Uuid>,
    limit: u32,
) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
    let sql = if after.is_some() {
        "SELECT * FROM checkpoints WHERE instance_id = ?1 AND id > ?3 ORDER BY id LIMIT ?2"
    } else {
        "SELECT * FROM checkpoints WHERE instance_id = ?1 ORDER BY id LIMIT ?2"
    };
    let mut query = sqlx::query(sql)
        .bind(instance_id.to_string())
        .bind(i64::from(limit.min(1000)));
    if let Some(after) = after.map(|id| id.to_string()) {
        query = query.bind(after);
    }
    let rows = query.fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_checkpoint).collect()
}

pub(super) async fn prune(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    keep: u32,
) -> Result<u64, StorageError> {
    let mut connection = storage.pool.acquire().await?;
    prune_on(&mut connection, instance_id, keep).await
}

pub(super) async fn prune_on(
    connection: &mut sqlx::SqliteConnection,
    instance_id: InstanceId,
    keep: u32,
) -> Result<u64, StorageError> {
    // Keep the latest N, delete the rest.
    let result = sqlx::query(
        "DELETE FROM checkpoints WHERE instance_id=?1 AND id NOT IN (SELECT id FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC, id DESC LIMIT ?2)"
    )
    .bind(instance_id.into_uuid().to_string())
    .bind(keep as i64)
    .execute(connection)
    .await
    ?;
    Ok(result.rows_affected())
}

pub(super) async fn owns_epoch_on(
    connection: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    instance_id: InstanceId,
    expected_epoch: u64,
) -> Result<bool, StorageError> {
    let expected_epoch = i64::try_from(expected_epoch)
        .map_err(|_| StorageError::Constraint("invalid checkpoint epoch".into()))?;
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_instances WHERE id = ?1 AND tenant_id = ?2)
            AND NOT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id = ?1)
            AND NOT EXISTS(SELECT 1 FROM readonly_instance_history WHERE instance_id = ?1)",
    )
    .bind(instance_id.to_string())
    .bind(tenant.as_str())
    .fetch_one(&mut *connection)
    .await?;
    if !valid {
        return Ok(false);
    }
    let owner: Option<(i64, String)> = sqlx::query_as(
        "SELECT epoch, state FROM continuity_executions
         WHERE tenant_id = ? AND json_extract(record, '$.current_instance_id') = ?",
    )
    .bind(tenant.as_str())
    .bind(instance_id.to_string())
    .fetch_optional(connection)
    .await?;
    Ok(match owner {
        None => expected_epoch == 0,
        Some((epoch, state)) => {
            epoch == expected_epoch && matches!(state.as_str(), "owned" | "completed")
        }
    })
}
