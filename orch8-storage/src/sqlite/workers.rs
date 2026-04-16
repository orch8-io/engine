use chrono::Utc;
use std::collections::HashMap;
use std::time::Duration;
use tracing::instrument;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::worker::WorkerTask;

use super::helpers::{row_to_worker_task, ts};
use super::SqliteStorage;

#[instrument(skip(storage, t), fields(task_id = %t.id, handler = %t.handler_name))]
pub(super) async fn create(storage: &SqliteStorage, t: &WorkerTask) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO worker_tasks (id,instance_id,block_id,handler_name,params,context,state,worker_id,queue_name,output,error_message,error_retryable,attempt,timeout_ms,claimed_at,heartbeat_at,completed_at,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18) ON CONFLICT(instance_id,block_id) DO NOTHING"
    )
    .bind(t.id.to_string())
    .bind(t.instance_id.0.to_string())
    .bind(&t.block_id.0)
    .bind(&t.handler_name)
    .bind(serde_json::to_string(&t.params).unwrap_or_default())
    .bind(serde_json::to_string(&t.context).unwrap_or_default())
    .bind(t.state.to_string())
    .bind(&t.worker_id)
    .bind(&t.queue_name)
    .bind(t.output.as_ref().map(|v| serde_json::to_string(v).unwrap_or_default()))
    .bind(&t.error_message)
    .bind(t.error_retryable.map(|b| b as i32))
    .bind(i64::from(t.attempt))
    .bind(t.timeout_ms)
    .bind(t.claimed_at.map(ts))
    .bind(t.heartbeat_at.map(ts))
    .bind(t.completed_at.map(ts))
    .bind(ts(t.created_at))
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    task_id: Uuid,
) -> Result<Option<WorkerTask>, StorageError> {
    let row = sqlx::query("SELECT * FROM worker_tasks WHERE id=?1")
        .bind(task_id.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    row.as_ref().map(row_to_worker_task).transpose()
}

#[instrument(skip(storage), fields(handler_name, worker_id, limit))]
pub(super) async fn claim(
    storage: &SqliteStorage,
    handler_name: &str,
    worker_id: &str,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let now = ts(Utc::now());
    // Use IMMEDIATE transaction to prevent concurrent claims on the same tasks.
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    // SQLite: BEGIN IMMEDIATE is implicit with sqlx when using write operations in a tx.
    let rows = sqlx::query(
        "SELECT * FROM worker_tasks WHERE handler_name=?1 AND state='pending' LIMIT ?2",
    )
    .bind(handler_name)
    .bind(limit as i64)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    let tasks: Vec<WorkerTask> = rows
        .iter()
        .map(row_to_worker_task)
        .collect::<Result<Vec<_>, _>>()?;
    for t in &tasks {
        sqlx::query("UPDATE worker_tasks SET state='claimed', worker_id=?2, claimed_at=?3, heartbeat_at=?3 WHERE id=?1")
            .bind(t.id.to_string())
            .bind(worker_id)
            .bind(&now)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(tasks)
}

#[instrument(skip(storage, output), fields(%task_id, worker_id))]
pub(super) async fn complete(
    storage: &SqliteStorage,
    task_id: Uuid,
    worker_id: &str,
    output: &serde_json::Value,
) -> Result<bool, StorageError> {
    let result = sqlx::query("UPDATE worker_tasks SET state='completed', output=?3, completed_at=?4 WHERE id=?1 AND worker_id=?2")
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(serde_json::to_string(output).unwrap_or_default())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected() > 0)
}

#[instrument(skip(storage), fields(%task_id, worker_id, retryable))]
pub(super) async fn fail(
    storage: &SqliteStorage,
    task_id: Uuid,
    worker_id: &str,
    message: &str,
    retryable: bool,
) -> Result<bool, StorageError> {
    let result = sqlx::query("UPDATE worker_tasks SET state='failed', error_message=?3, error_retryable=?4, completed_at=?5 WHERE id=?1 AND worker_id=?2")
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(message)
        .bind(retryable as i32)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn heartbeat(
    storage: &SqliteStorage,
    task_id: Uuid,
    worker_id: &str,
) -> Result<bool, StorageError> {
    let result =
        sqlx::query("UPDATE worker_tasks SET heartbeat_at=?3 WHERE id=?1 AND worker_id=?2")
            .bind(task_id.to_string())
            .bind(worker_id)
            .bind(ts(Utc::now()))
            .execute(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn delete(storage: &SqliteStorage, task_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM worker_tasks WHERE id=?1")
        .bind(task_id.to_string())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn reap_stale(
    storage: &SqliteStorage,
    stale_threshold: Duration,
) -> Result<u64, StorageError> {
    let cutoff = Utc::now() - chrono::Duration::from_std(stale_threshold).unwrap_or_default();
    let result = sqlx::query(
        "UPDATE worker_tasks SET state='pending', worker_id=NULL WHERE state='claimed' AND (heartbeat_at IS NULL OR heartbeat_at < ?1)",
    )
    .bind(ts(cutoff))
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn cancel_for_block(
    storage: &SqliteStorage,
    instance_id: Uuid,
    block_id: &str,
) -> Result<u64, StorageError> {
    let result = sqlx::query("DELETE FROM worker_tasks WHERE instance_id=?1 AND block_id=?2 AND state IN ('pending','claimed')")
        .bind(instance_id.to_string())
        .bind(block_id)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    filter: &orch8_types::worker_filter::WorkerTaskFilter,
    pagination: &orch8_types::filter::Pagination,
) -> Result<Vec<WorkerTask>, StorageError> {
    let mut sql = String::from("SELECT * FROM worker_tasks WHERE 1=1");
    let mut args: Vec<String> = Vec::new();
    if let Some(ref tid) = filter.tenant_id {
        args.push(tid.0.clone());
        sql.push_str(&format!(
            " AND instance_id IN (SELECT id FROM instances WHERE tenant_id=?{})",
            args.len()
        ));
    }
    if let Some(ref states) = filter.states {
        if !states.is_empty() {
            let placeholders: Vec<String> = states.iter().map(|s| format!("'{s}'")).collect();
            sql.push_str(&format!(" AND state IN ({})", placeholders.join(",")));
        }
    }
    if let Some(ref handler) = filter.handler_name {
        args.push(handler.clone());
        sql.push_str(&format!(" AND handler_name=?{}", args.len()));
    }
    if let Some(ref wid) = filter.worker_id {
        args.push(wid.clone());
        sql.push_str(&format!(" AND worker_id=?{}", args.len()));
    }
    if let Some(ref queue) = filter.queue_name {
        args.push(queue.clone());
        sql.push_str(&format!(" AND queue_name=?{}", args.len()));
    }
    sql.push_str(" ORDER BY created_at DESC");
    let limit = i64::from(pagination.limit.min(1000));
    let offset = i64::try_from(pagination.offset).unwrap_or(i64::MAX);
    args.push(limit.to_string());
    sql.push_str(&format!(" LIMIT ?{}", args.len()));
    args.push(offset.to_string());
    sql.push_str(&format!(" OFFSET ?{}", args.len()));

    let mut q = sqlx::query(&sql);
    for arg in &args {
        q = q.bind(arg);
    }
    let rows = q
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_worker_task).collect()
}

pub(super) async fn stats(
    storage: &SqliteStorage,
    tenant_id: Option<&orch8_types::ids::TenantId>,
) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
    let tenant_clause = if tenant_id.is_some() {
        " WHERE instance_id IN (SELECT id FROM instances WHERE tenant_id=?1)"
    } else {
        ""
    };

    let count_sql = format!(
        "SELECT state, handler_name, COUNT(*) as cnt FROM worker_tasks{tenant_clause} GROUP BY state, handler_name"
    );
    let mut q = sqlx::query_as::<_, (String, String, i64)>(&count_sql);
    if let Some(tid) = tenant_id {
        q = q.bind(&tid.0);
    }
    let counts = q
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    let mut by_state = HashMap::<String, u64>::new();
    let mut by_handler = HashMap::<String, HashMap<String, u64>>::new();
    for (state, handler, cnt) in counts {
        #[allow(clippy::cast_sign_loss)]
        let cnt = cnt.max(0) as u64;
        *by_state.entry(state.clone()).or_default() += cnt;
        *by_handler
            .entry(handler)
            .or_default()
            .entry(state)
            .or_default() += cnt;
    }

    let workers_sql = format!(
        "SELECT DISTINCT worker_id FROM worker_tasks WHERE state = 'claimed' AND worker_id IS NOT NULL{}",
        if tenant_id.is_some() {
            " AND instance_id IN (SELECT id FROM instances WHERE tenant_id=?1)"
        } else {
            ""
        }
    );
    let mut wq = sqlx::query_as::<_, (String,)>(&workers_sql);
    if let Some(tid) = tenant_id {
        wq = wq.bind(&tid.0);
    }
    let workers = wq
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    let active_workers = workers.into_iter().map(|(w,)| w).collect();

    Ok(orch8_types::worker_filter::WorkerTaskStats {
        by_state,
        by_handler,
        active_workers,
    })
}
