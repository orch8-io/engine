use sqlx::Row;
use std::time::Duration;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::instance::TaskInstance;
use orch8_types::worker::WorkerTask;

use super::helpers::{row_to_instance, row_to_worker_task, ts};
use super::SqliteStorage;

// === Idempotency ===

pub(super) async fn find_by_idempotency_key(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    idempotency_key: &str,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query(
        "SELECT * FROM task_instances WHERE tenant_id=?1 AND idempotency_key=?2 LIMIT 1",
    )
    .bind(&tenant_id.0)
    .bind(idempotency_key)
    .fetch_optional(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.as_ref().map(row_to_instance))
}

// === Concurrency ===

pub(super) async fn count_running_by_concurrency_key(
    storage: &SqliteStorage,
    concurrency_key: &str,
) -> Result<i64, StorageError> {
    let row = sqlx::query(
        "SELECT COUNT(*) as cnt FROM task_instances WHERE concurrency_key=?1 AND state='running'",
    )
    .bind(concurrency_key)
    .fetch_one(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.get::<i64, _>("cnt"))
}

pub(super) async fn concurrency_position(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    concurrency_key: &str,
) -> Result<i64, StorageError> {
    let rows = sqlx::query(
        "SELECT id FROM task_instances WHERE concurrency_key=?1 AND state='running' ORDER BY id",
    )
    .bind(concurrency_key)
    .fetch_all(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    let id_str = instance_id.0.to_string();
    let pos = rows
        .iter()
        .position(|r| r.get::<String, _>("id") == id_str)
        .map_or(0, |p| p as i64 + 1);
    Ok(pos)
}

// === Recovery ===

pub(super) async fn recover_stale_instances(
    storage: &SqliteStorage,
    _stale_threshold: Duration,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "UPDATE task_instances SET state='scheduled', updated_at=?1 WHERE state='running'",
    )
    .bind(ts(chrono::Utc::now()))
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

// === Sub-Sequences ===

pub(super) async fn get_child_instances(
    storage: &SqliteStorage,
    parent_instance_id: InstanceId,
) -> Result<Vec<TaskInstance>, StorageError> {
    let rows = sqlx::query("SELECT * FROM task_instances WHERE parent_instance_id=?1")
        .bind(parent_instance_id.0.to_string())
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(rows.iter().map(row_to_instance).collect())
}

// === Task Queue Routing ===

pub(super) async fn claim_worker_tasks_from_queue(
    storage: &SqliteStorage,
    queue_name: &str,
    handler_name: &str,
    worker_id: &str,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let now = ts(chrono::Utc::now());
    let rows = sqlx::query("SELECT * FROM worker_tasks WHERE queue_name=?1 AND handler_name=?2 AND state='pending' LIMIT ?3")
        .bind(queue_name)
        .bind(handler_name)
        .bind(limit as i64)
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    let tasks: Vec<WorkerTask> = rows.iter().map(row_to_worker_task).collect();
    for t in &tasks {
        sqlx::query("UPDATE worker_tasks SET state='claimed', worker_id=?2, claimed_at=?3, heartbeat_at=?3 WHERE id=?1")
            .bind(t.id.to_string())
            .bind(worker_id)
            .bind(&now)
            .execute(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    Ok(tasks)
}

// === Dynamic Step Injection ===

pub(super) async fn inject_blocks(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    blocks_json: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query("INSERT OR REPLACE INTO injected_blocks (instance_id, blocks) VALUES (?1, ?2)")
        .bind(instance_id.0.to_string())
        .bind(serde_json::to_string(blocks_json).unwrap_or_default())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get_injected_blocks(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row = sqlx::query("SELECT blocks FROM injected_blocks WHERE instance_id=?1")
        .bind(instance_id.0.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.map(|r| serde_json::from_str(r.get::<&str, _>("blocks")).unwrap_or_default()))
}

// === Health ===

pub(super) async fn ping(storage: &SqliteStorage) -> Result<(), StorageError> {
    sqlx::query("SELECT 1")
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;
    Ok(())
}
