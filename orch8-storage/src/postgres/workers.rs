use std::time::Duration;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::worker::WorkerTask;

use super::rows::WorkerTaskRow;
use super::PostgresStorage;

pub(super) async fn create(store: &PostgresStorage, task: &WorkerTask) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO worker_tasks
            (id, instance_id, block_id, handler_name, queue_name, params, context,
             attempt, timeout_ms, state, created_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
          ON CONFLICT (instance_id, block_id) DO NOTHING",
    )
    .bind(task.id)
    .bind(task.instance_id.0)
    .bind(&task.block_id.0)
    .bind(&task.handler_name)
    .bind(&task.queue_name)
    .bind(&task.params)
    .bind(&task.context)
    .bind(task.attempt)
    .bind(task.timeout_ms)
    .bind(task.state.to_string())
    .bind(task.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    task_id: Uuid,
) -> Result<Option<WorkerTask>, StorageError> {
    let row = sqlx::query_as::<_, WorkerTaskRow>(
        r"SELECT id, instance_id, block_id, handler_name, queue_name, params, context,
                 attempt, timeout_ms, state, worker_id, claimed_at, heartbeat_at,
                 completed_at, output, error_message, error_retryable, created_at
          FROM worker_tasks WHERE id = $1",
    )
    .bind(task_id)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(WorkerTaskRow::into_task))
}

pub(super) async fn claim(
    store: &PostgresStorage,
    handler_name: &str,
    worker_id: &str,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(
        r"UPDATE worker_tasks
          SET state = 'claimed', worker_id = $2, claimed_at = NOW(), heartbeat_at = NOW()
          WHERE id IN (
              SELECT id FROM worker_tasks
              WHERE handler_name = $1 AND state = 'pending'
              ORDER BY created_at
              LIMIT $3
              FOR UPDATE SKIP LOCKED
          )
          RETURNING id, instance_id, block_id, handler_name, queue_name, params, context,
                    attempt, timeout_ms, state, worker_id, claimed_at, heartbeat_at,
                    completed_at, output, error_message, error_retryable, created_at",
    )
    .bind(handler_name)
    .bind(worker_id)
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(WorkerTaskRow::into_task).collect())
}

/// Tenant-scoped claim: the `task_instances` join is inside the same
/// `SELECT ... FOR UPDATE SKIP LOCKED` so the tenant predicate is
/// enforced atomically with the lock. The old "claim then filter" shape
/// in the HTTP handler would mark a foreign tenant's row claimed, then
/// discard it from the response — leaving a ghost `claimed` row invisible
/// to the owning tenant until the stale-task reaper reset it.
pub(super) async fn claim_for_tenant(
    store: &PostgresStorage,
    handler_name: &str,
    worker_id: &str,
    tenant_id: &orch8_types::TenantId,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(
        r"UPDATE worker_tasks
          SET state = 'claimed', worker_id = $2, claimed_at = NOW(), heartbeat_at = NOW()
          WHERE id IN (
              SELECT wt.id FROM worker_tasks wt
              JOIN task_instances ti ON ti.id = wt.instance_id
              WHERE wt.handler_name = $1
                AND wt.state = 'pending'
                AND ti.tenant_id = $4
              ORDER BY wt.created_at
              LIMIT $3
              FOR UPDATE SKIP LOCKED
          )
          RETURNING id, instance_id, block_id, handler_name, queue_name, params, context,
                    attempt, timeout_ms, state, worker_id, claimed_at, heartbeat_at,
                    completed_at, output, error_message, error_retryable, created_at",
    )
    .bind(handler_name)
    .bind(worker_id)
    .bind(i64::from(limit))
    .bind(&tenant_id.0)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(WorkerTaskRow::into_task).collect())
}

pub(super) async fn complete(
    store: &PostgresStorage,
    task_id: Uuid,
    worker_id: &str,
    output: &serde_json::Value,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        r"UPDATE worker_tasks
          SET state = 'completed', output = $3, completed_at = NOW()
          WHERE id = $1 AND worker_id = $2 AND state = 'claimed'",
    )
    .bind(task_id)
    .bind(worker_id)
    .bind(output)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn fail(
    store: &PostgresStorage,
    task_id: Uuid,
    worker_id: &str,
    message: &str,
    retryable: bool,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        r"UPDATE worker_tasks
          SET state = 'failed', error_message = $3, error_retryable = $4, completed_at = NOW()
          WHERE id = $1 AND worker_id = $2 AND state = 'claimed'",
    )
    .bind(task_id)
    .bind(worker_id)
    .bind(message)
    .bind(retryable)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn heartbeat(
    store: &PostgresStorage,
    task_id: Uuid,
    worker_id: &str,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE worker_tasks SET heartbeat_at = NOW() WHERE id = $1 AND worker_id = $2 AND state = 'claimed'",
    )
    .bind(task_id)
    .bind(worker_id)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn delete(store: &PostgresStorage, task_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM worker_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn reap_stale(
    store: &PostgresStorage,
    stale_threshold: Duration,
) -> Result<u64, StorageError> {
    let threshold_secs = stale_threshold.as_secs_f64();
    let result = sqlx::query(
        r"UPDATE worker_tasks
          SET state = 'pending', worker_id = NULL, claimed_at = NULL, heartbeat_at = NULL
          WHERE state = 'claimed'
            AND heartbeat_at < NOW() - make_interval(secs => $1::double precision)",
    )
    .bind(threshold_secs)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

/// Fail worker tasks whose `timeout_ms` has elapsed since `created_at`.
/// Only affects tasks in `pending` or `claimed` state that have a non-NULL
/// `timeout_ms`. Sets state to `failed` with a descriptive error message.
pub(super) async fn expire_timed_out(store: &PostgresStorage) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"UPDATE worker_tasks
          SET state = 'failed',
              error_message = 'task timed out (timeout_ms exceeded)',
              error_retryable = false,
              completed_at = NOW()
          WHERE state IN ('pending', 'claimed')
            AND timeout_ms IS NOT NULL
            AND created_at + make_interval(secs => timeout_ms::double precision / 1000.0) < NOW()",
    )
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn cancel_for_block(
    store: &PostgresStorage,
    instance_id: Uuid,
    block_id: &str,
) -> Result<u64, StorageError> {
    // Delete all rows for (instance_id, block_id) regardless of state.
    // Two callers rely on this:
    //   1. Race cancellation — losing branch purges any task record.
    //   2. ForEach/Loop iteration reset — must purge `completed` rows so
    //      the next iteration's INSERT isn't blocked by the
    //      UNIQUE(instance_id, block_id) constraint on worker_tasks.
    let result = sqlx::query("DELETE FROM worker_tasks WHERE instance_id = $1 AND block_id = $2")
        .bind(instance_id)
        .bind(block_id)
        .execute(&store.pool)
        .await?;
    Ok(result.rows_affected())
}

/// Batch variant of [`cancel_for_block`]: single `DELETE ... = ANY($2)`
/// round-trip for an arbitrary set of block IDs under one instance.
pub(super) async fn cancel_for_blocks(
    store: &PostgresStorage,
    instance_id: Uuid,
    block_ids: &[String],
) -> Result<u64, StorageError> {
    if block_ids.is_empty() {
        return Ok(0);
    }
    let result =
        sqlx::query("DELETE FROM worker_tasks WHERE instance_id = $1 AND block_id = ANY($2)")
            .bind(instance_id)
            .bind(block_ids)
            .execute(&store.pool)
            .await?;
    Ok(result.rows_affected())
}

pub(super) async fn list(
    store: &PostgresStorage,
    filter: &orch8_types::worker_filter::WorkerTaskFilter,
    pagination: &orch8_types::filter::Pagination,
) -> Result<Vec<WorkerTask>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        r"SELECT id, instance_id, block_id, handler_name, queue_name, params, context,
                 attempt, timeout_ms, state, worker_id, claimed_at, heartbeat_at,
                 completed_at, output, error_message, error_retryable, created_at
           FROM worker_tasks WHERE 1=1",
    );
    apply_worker_task_filter(&mut qb, filter);
    qb.push(" ORDER BY created_at DESC");
    qb.push(" LIMIT ")
        .push_bind(i64::from(pagination.limit.min(1000)));
    qb.push(" OFFSET ")
        .push_bind(i64::try_from(pagination.offset).unwrap_or(i64::MAX));

    let rows = qb
        .build_query_as::<WorkerTaskRow>()
        .fetch_all(&store.pool)
        .await?;

    Ok(rows.into_iter().map(WorkerTaskRow::into_task).collect())
}

pub(super) async fn stats(
    store: &PostgresStorage,
    tenant_id: Option<&orch8_types::ids::TenantId>,
) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
    let tenant_clause = if tenant_id.is_some() {
        " WHERE instance_id IN (SELECT id FROM instances WHERE tenant_id = $1)"
    } else {
        ""
    };

    // Count by state + handler_name
    let count_sql = format!(
        "SELECT state, handler_name, COUNT(*) as cnt FROM worker_tasks{tenant_clause} GROUP BY state, handler_name"
    );
    let mut q = sqlx::query_as::<_, (String, String, i64)>(&count_sql);
    if let Some(tid) = tenant_id {
        q = q.bind(&tid.0);
    }
    let counts = q.fetch_all(&store.pool).await?;

    let mut by_state = std::collections::HashMap::<String, u64>::new();
    let mut by_handler =
        std::collections::HashMap::<String, std::collections::HashMap<String, u64>>::new();
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

    // Active workers
    let workers_sql = format!(
        "SELECT DISTINCT worker_id FROM worker_tasks WHERE state = 'claimed' AND worker_id IS NOT NULL{}",
        if tenant_id.is_some() {
            " AND instance_id IN (SELECT id FROM instances WHERE tenant_id = $1)"
        } else {
            ""
        }
    );
    let mut wq = sqlx::query_as::<_, (String,)>(&workers_sql);
    if let Some(tid) = tenant_id {
        wq = wq.bind(&tid.0);
    }
    let workers = wq.fetch_all(&store.pool).await?;

    let active_workers = workers.into_iter().map(|(w,)| w).collect();

    Ok(orch8_types::worker_filter::WorkerTaskStats {
        by_state,
        by_handler,
        active_workers,
    })
}

/// Apply `WorkerTaskFilter` conditions to a query builder.
fn apply_worker_task_filter<'a>(
    qb: &mut sqlx::QueryBuilder<'a, sqlx::Postgres>,
    filter: &'a orch8_types::worker_filter::WorkerTaskFilter,
) {
    if let Some(ref tid) = filter.tenant_id {
        qb.push(" AND instance_id IN (SELECT id FROM instances WHERE tenant_id = ")
            .push_bind(&tid.0)
            .push(")");
    }
    if let Some(ref states) = filter.states {
        if !states.is_empty() {
            let state_strings: Vec<String> = states.iter().map(ToString::to_string).collect();
            qb.push(" AND state = ANY(")
                .push_bind(state_strings)
                .push(")");
        }
    }
    if let Some(ref handler) = filter.handler_name {
        qb.push(" AND handler_name = ").push_bind(handler);
    }
    if let Some(ref wid) = filter.worker_id {
        qb.push(" AND worker_id = ").push_bind(wid);
    }
    if let Some(ref queue) = filter.queue_name {
        qb.push(" AND queue_name = ").push_bind(queue);
    }
}
