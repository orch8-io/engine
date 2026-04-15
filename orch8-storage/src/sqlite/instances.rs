use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::*;
use orch8_types::instance::{InstanceState, TaskInstance};

use super::helpers::{apply_filter_sql, row_to_instance, ts};
use super::SqliteStorage;

pub(super) async fn create(storage: &SqliteStorage, i: &TaskInstance) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO task_instances (id,sequence_id,tenant_id,namespace,state,next_fire_at,priority,timezone,metadata,context,concurrency_key,max_concurrency,idempotency_key,session_id,parent_instance_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"
    )
    .bind(i.id.0.to_string())
    .bind(i.sequence_id.0.to_string())
    .bind(&i.tenant_id.0)
    .bind(&i.namespace.0)
    .bind(i.state.to_string())
    .bind(i.next_fire_at.map(ts))
    .bind(i.priority as i16)
    .bind(&i.timezone)
    .bind(serde_json::to_string(&i.metadata).unwrap_or_default())
    .bind(serde_json::to_string(&i.context).unwrap_or_default())
    .bind(&i.concurrency_key)
    .bind(i.max_concurrency)
    .bind(&i.idempotency_key)
    .bind(i.session_id.map(|u| u.to_string()))
    .bind(i.parent_instance_id.map(|u| u.0.to_string()))
    .bind(ts(i.created_at))
    .bind(ts(i.updated_at))
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn create_batch(
    storage: &SqliteStorage,
    instances: &[TaskInstance],
) -> Result<u64, StorageError> {
    for i in instances {
        create(storage, i).await?;
    }
    Ok(instances.len() as u64)
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: InstanceId,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query("SELECT * FROM task_instances WHERE id=?1")
        .bind(id.0.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.as_ref().map(row_to_instance))
}

pub(super) async fn claim_due(
    storage: &SqliteStorage,
    now: DateTime<Utc>,
    limit: u32,
    _max_per_tenant: u32,
) -> Result<Vec<TaskInstance>, StorageError> {
    let now_s = ts(now);
    let rows = sqlx::query(
        "SELECT * FROM task_instances WHERE state='scheduled' AND (next_fire_at IS NULL OR next_fire_at <= ?1) ORDER BY priority DESC, next_fire_at ASC LIMIT ?2"
    )
    .bind(&now_s).bind(limit as i64)
    .fetch_all(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;

    let instances: Vec<TaskInstance> = rows.iter().map(row_to_instance).collect();
    // Mark as running
    for inst in &instances {
        sqlx::query("UPDATE task_instances SET state='running', updated_at=?2 WHERE id=?1")
            .bind(inst.id.0.to_string())
            .bind(&now_s)
            .execute(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    Ok(instances)
}

pub(super) async fn update_state(
    storage: &SqliteStorage,
    id: InstanceId,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE task_instances SET state=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(new_state.to_string())
        .bind(next_fire_at.map(ts))
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn update_context(
    storage: &SqliteStorage,
    id: InstanceId,
    context: &ExecutionContext,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(serde_json::to_string(context).unwrap_or_default())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn update_sequence(
    storage: &SqliteStorage,
    id: InstanceId,
    new_sequence_id: SequenceId,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE task_instances SET sequence_id=?2, updated_at=?3 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(new_sequence_id.0.to_string())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn merge_context_data(
    storage: &SqliteStorage,
    id: InstanceId,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), StorageError> {
    // Read-modify-write for SQLite (no JSONB).
    let row = sqlx::query("SELECT context FROM task_instances WHERE id=?1")
        .bind(id.0.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    if let Some(row) = row {
        let ctx_str: String = row.get("context");
        let mut ctx: ExecutionContext = serde_json::from_str(&ctx_str).unwrap_or_default();
        if let Some(obj) = ctx.data.as_object_mut() {
            obj.insert(key.to_string(), value.clone());
        }
        sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
            .bind(id.0.to_string())
            .bind(serde_json::to_string(&ctx).unwrap_or_default())
            .bind(ts(Utc::now()))
            .execute(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    pagination: &Pagination,
) -> Result<Vec<TaskInstance>, StorageError> {
    let mut sql = String::from("SELECT * FROM task_instances WHERE 1=1");
    apply_filter_sql(&mut sql, filter);
    sql.push_str(&format!(
        " ORDER BY created_at DESC LIMIT {} OFFSET {}",
        pagination.limit, pagination.offset
    ));
    let rows = sqlx::query(&sql)
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(rows.iter().map(row_to_instance).collect())
}

pub(super) async fn count(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
) -> Result<u64, StorageError> {
    let mut sql = String::from("SELECT COUNT(*) as cnt FROM task_instances WHERE 1=1");
    apply_filter_sql(&mut sql, filter);
    let row = sqlx::query(&sql)
        .fetch_one(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.get::<i64, _>("cnt") as u64)
}

pub(super) async fn bulk_update_state(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    new_state: InstanceState,
) -> Result<u64, StorageError> {
    let mut sql = format!(
        "UPDATE task_instances SET state='{}', updated_at='{}' WHERE 1=1",
        new_state,
        ts(Utc::now())
    );
    apply_filter_sql(&mut sql, filter);
    let result = sqlx::query(&sql)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn bulk_reschedule(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    offset_secs: i64,
) -> Result<u64, StorageError> {
    // SQLite: use datetime function to shift
    let mut sql = format!(
        "UPDATE task_instances SET next_fire_at=datetime(next_fire_at, '+{offset_secs} seconds'), updated_at='{}' WHERE state='scheduled'",
        ts(Utc::now())
    );
    apply_filter_sql(&mut sql, filter);
    let result = sqlx::query(&sql)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}
