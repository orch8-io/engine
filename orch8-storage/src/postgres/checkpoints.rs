use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::PostgresStorage;

pub(super) async fn save(
    store: &PostgresStorage,
    cp: &orch8_types::checkpoint::Checkpoint,
) -> Result<(), StorageError> {
    let mut conn = store.pool.acquire().await?;
    save_on(&mut conn, cp).await
}
pub(super) async fn save_on(
    conn: &mut sqlx::PgConnection,
    cp: &orch8_types::checkpoint::Checkpoint,
) -> Result<(), StorageError> {
    let encoded = super::json_encoding::encode(&cp.checkpoint_data)?;
    sqlx::query("INSERT INTO checkpoints(id,instance_id,checkpoint_data,created_at,data_encoding) VALUES($1,$2,$3,$4,$5)")
        .bind(cp.id).bind(cp.instance_id.into_uuid()).bind(encoded.value).bind(cp.created_at).bind(encoded.encoding)
        .execute(&mut *conn).await?;
    Ok(())
}

type CheckpointRow = (Uuid, Uuid, serde_json::Value, DateTime<Utc>, Option<String>);
fn decode(row: CheckpointRow) -> Result<orch8_types::checkpoint::Checkpoint, StorageError> {
    let (id, instance, data, created_at, encoding) = row;
    Ok(orch8_types::checkpoint::Checkpoint {
        id,
        instance_id: InstanceId::from_uuid(instance),
        checkpoint_data: super::json_encoding::decode(data, encoding.as_deref())?,
        created_at,
    })
}

pub(super) async fn get_latest(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
    let mut conn = store.pool.acquire().await?;
    get_latest_on(&mut conn, instance_id).await
}

pub(super) async fn get_latest_on(
    conn: &mut sqlx::PgConnection,
    instance_id: InstanceId,
) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
    let row: Option<CheckpointRow> = sqlx::query_as(
        "SELECT id, instance_id, checkpoint_data, created_at, data_encoding FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(instance_id.into_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    row.map(decode).transpose()
}

pub(super) async fn list(
    store: &PostgresStorage,
    instance_id: InstanceId,
    limit: u32,
) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
    let rows: Vec<CheckpointRow> = sqlx::query_as(
        "SELECT id, instance_id, checkpoint_data, created_at, data_encoding FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2",
    )
    .bind(instance_id.into_uuid())
    .bind(i64::from(limit.min(1000)))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(decode).collect()
}

pub(super) async fn page(
    store: &PostgresStorage,
    instance_id: InstanceId,
    after: Option<Uuid>,
    limit: u32,
) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
    let sql = if after.is_some() {
        "SELECT id, instance_id, checkpoint_data, created_at, data_encoding FROM checkpoints WHERE instance_id = $1 AND id > $3 ORDER BY id LIMIT $2"
    } else {
        "SELECT id, instance_id, checkpoint_data, created_at, data_encoding FROM checkpoints WHERE instance_id = $1 ORDER BY id LIMIT $2"
    };
    let mut query = sqlx::query_as::<_, CheckpointRow>(sql)
        .bind(instance_id.into_uuid())
        .bind(i64::from(limit.min(1000)));
    if let Some(after) = after {
        query = query.bind(after);
    }
    let rows = query.fetch_all(&store.pool).await?;
    rows.into_iter().map(decode).collect()
}

pub(super) async fn prune(
    store: &PostgresStorage,
    instance_id: InstanceId,
    keep: u32,
) -> Result<u64, StorageError> {
    let mut connection = store.pool.acquire().await?;
    prune_on(&mut connection, instance_id, keep).await
}

pub(super) async fn prune_on(
    connection: &mut sqlx::PgConnection,
    instance_id: InstanceId,
    keep: u32,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"DELETE FROM checkpoints WHERE instance_id = $1 AND id NOT IN (
            SELECT id FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2
          )",
    )
    .bind(instance_id.into_uuid())
    .bind(i64::from(keep))
    .execute(connection)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn owns_epoch_on(
    connection: &mut sqlx::PgConnection,
    tenant: &orch8_types::ids::TenantId,
    instance_id: InstanceId,
    expected_epoch: u64,
) -> Result<bool, StorageError> {
    let expected_epoch = i64::try_from(expected_epoch)
        .map_err(|_| StorageError::Constraint("invalid checkpoint epoch".into()))?;
    super::portable_effects::lock_instance_on(connection, tenant, instance_id).await?;
    let owner: Option<(i64, String)> = sqlx::query_as(
        "SELECT epoch, state FROM continuity_executions
         WHERE tenant_id = $1 AND record->>'current_instance_id' = $2 FOR UPDATE",
    )
    .bind(tenant.as_str())
    .bind(instance_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_instances WHERE id = $1 AND tenant_id = $2)
            AND NOT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id = $1)
            AND NOT EXISTS(SELECT 1 FROM readonly_instance_history WHERE instance_id = $1)",
    )
    .bind(instance_id.into_uuid())
    .bind(tenant.as_str())
    .fetch_one(connection)
    .await?;
    if !valid {
        return Ok(false);
    }
    Ok(match owner {
        None => expected_epoch == 0,
        Some((epoch, state)) => {
            epoch == expected_epoch && matches!(state.as_str(), "owned" | "completed")
        }
    })
}
