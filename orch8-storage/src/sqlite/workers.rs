use chrono::Utc;
use std::collections::HashMap;
use std::time::Duration;
use tracing::instrument;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::worker::{
    WorkerAttemptEventKind, WorkerClaim, WorkerTask, WorkerTaskAttemptEvent,
};

use super::SqliteStorage;
use super::helpers::{row_to_worker_task, ts};

#[instrument(skip(storage, t), fields(task_id = %t.id, handler = %t.handler_name))]
pub(super) async fn create(storage: &SqliteStorage, t: &WorkerTask) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO worker_tasks (id,instance_id,block_id,handler_name,params,context,state,worker_id,queue_name,output,error_message,error_retryable,attempt,timeout_ms,claimed_at,heartbeat_at,claim_epoch,resume_checkpoint,checkpoint_seq,completed_at,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21) ON CONFLICT(instance_id,block_id) DO NOTHING"
    )
    .bind(t.id.to_string())
    .bind(t.instance_id.into_uuid().to_string())
    .bind(t.block_id.as_str())
    .bind(&t.handler_name)
    .bind(serde_json::to_string(&t.params)?)
    .bind(serde_json::to_string(&t.context)?)
    .bind(t.state.to_string())
    .bind(&t.worker_id)
    .bind(&t.queue_name)
    .bind(t.output.as_ref().map(serde_json::to_string).transpose()?)
    .bind(&t.error_message)
    .bind(t.error_retryable.map(|b| b as i32))
    .bind(i64::from(t.attempt))
    .bind(t.timeout_ms)
    .bind(t.claimed_at.map(ts))
    .bind(t.heartbeat_at.map(ts))
    .bind(i64::try_from(t.claim_epoch).unwrap_or(i64::MAX))
    .bind(
        t.resume_checkpoint
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?,
    )
    .bind(i64::try_from(t.checkpoint_seq).unwrap_or(i64::MAX))
    .bind(t.completed_at.map(ts))
    .bind(ts(t.created_at))
    .execute(&storage.pool).await?;
    Ok(())
}

pub(super) fn transition_event(
    task_id: Uuid,
    claim_epoch: u64,
    worker_id: Option<String>,
    event: WorkerAttemptEventKind,
    reason: Option<String>,
) -> WorkerTaskAttemptEvent {
    WorkerTaskAttemptEvent {
        id: Uuid::now_v7(),
        task_id,
        claim_epoch,
        worker_id,
        event,
        reason,
        created_at: Utc::now(),
    }
}

pub(super) async fn insert_attempt_events(
    conn: &mut sqlx::SqliteConnection,
    events: &[WorkerTaskAttemptEvent],
) -> Result<(), StorageError> {
    if events.is_empty() {
        return Ok(());
    }
    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO worker_task_attempt_events (id,task_id,claim_epoch,worker_id,event,reason,created_at) ",
    );
    qb.push_values(events, |mut row, event| {
        row.push_bind(event.id.to_string())
            .push_bind(event.task_id.to_string())
            .push_bind(i64::try_from(event.claim_epoch).unwrap_or(i64::MAX))
            .push_bind(&event.worker_id)
            .push_bind(event.event.as_str())
            .push_bind(&event.reason)
            .push_bind(ts(event.created_at));
    });
    qb.build().execute(conn).await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    task_id: Uuid,
) -> Result<Option<WorkerTask>, StorageError> {
    let row = sqlx::query("SELECT * FROM worker_tasks WHERE id=?1")
        .bind(task_id.to_string())
        .fetch_optional(&storage.pool)
        .await?;
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

    // Use `BEGIN IMMEDIATE` rather than sqlx's default `BEGIN` (DEFERRED) so
    // SQLite acquires a RESERVED write lock up-front — the closest equivalent
    // to Postgres's `SELECT ... FOR UPDATE SKIP LOCKED`. Without this, two
    // concurrent workers could both SELECT the same pending rows before
    // either UPDATE, racing on the claim. With IMMEDIATE, one writer wins
    // the lock, the other either waits on `busy_timeout` or fails with
    // `SQLITE_BUSY`.
    //
    // sqlx's `Transaction` wrapper hard-codes `BEGIN`, so we manage the
    // transaction manually on a pooled connection and run explicit
    // COMMIT / ROLLBACK. Mirrors the pattern in
    // `sqlite::signals::enqueue_if_active`.
    let mut conn = storage.pool.acquire().await?;

    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

    let select_res = sqlx::query(
        "SELECT * FROM worker_tasks WHERE handler_name=?1 AND state='pending' ORDER BY created_at ASC LIMIT ?2",
    )
    .bind(handler_name)
    .bind(limit as i64)
    .fetch_all(&mut *conn)
    .await;

    let rows = match select_res {
        Ok(r) => r,
        Err(e) => {
            rollback_quiet(&mut conn).await;
            return Err(StorageError::Query(e.to_string()));
        }
    };

    let mut tasks: Vec<WorkerTask> = match rows.iter().map(row_to_worker_task).collect() {
        Ok(t) => t,
        Err(e) => {
            rollback_quiet(&mut conn).await;
            return Err(e);
        }
    };

    if !tasks.is_empty() {
        // Reflect the state change in the returned objects so callers don't
        // see stale 'pending' values after a successful claim.
        let now_dt = chrono::Utc::now();
        for t in &mut tasks {
            t.state = orch8_types::worker::WorkerTaskState::Claimed;
            t.worker_id = Some(worker_id.to_string());
            t.claimed_at = Some(now_dt);
            t.heartbeat_at = Some(now_dt);
            t.claim_epoch = t.claim_epoch.saturating_add(1);
        }
        let mut qb = sqlx::QueryBuilder::new("UPDATE worker_tasks SET state='claimed', worker_id=");
        qb.push_bind(worker_id);
        qb.push(", claimed_at=");
        qb.push_bind(&now);
        qb.push(", heartbeat_at=");
        qb.push_bind(&now);
        qb.push(", claim_epoch=claim_epoch+1");
        qb.push(" WHERE id IN (");
        let mut separated = qb.separated(",");
        for t in &tasks {
            separated.push_bind(t.id.to_string());
        }
        separated.push_unseparated(")");

        if let Err(e) = qb.build().execute(&mut *conn).await {
            rollback_quiet(&mut conn).await;
            return Err(StorageError::Query(e.to_string()));
        }
        let events: Vec<_> = tasks
            .iter()
            .map(|task| {
                transition_event(
                    task.id,
                    task.claim_epoch,
                    task.worker_id.clone(),
                    WorkerAttemptEventKind::Claimed,
                    None,
                )
            })
            .collect();
        if let Err(e) = insert_attempt_events(&mut conn, &events).await {
            rollback_quiet(&mut conn).await;
            return Err(e);
        }
    }

    if let Err(e) = sqlx::query("COMMIT").execute(&mut *conn).await {
        rollback_quiet(&mut conn).await;
        return Err(StorageError::Query(e.to_string()));
    }
    Ok(tasks)
}

/// Tenant-scoped variant of [`claim`]. The tenant predicate is evaluated
/// inside the same BEGIN IMMEDIATE window as the UPDATE so a claim can
/// never mark a foreign tenant's row claimed (then be invisible to that
/// tenant until reap). Matches `postgres::workers::claim_for_tenant`.
#[instrument(skip(storage), fields(handler_name, worker_id, tenant_id = %tenant_id, limit))]
pub(super) async fn claim_for_tenant(
    storage: &SqliteStorage,
    handler_name: &str,
    worker_id: &str,
    tenant_id: &orch8_types::TenantId,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let now = ts(Utc::now());
    let mut conn = storage.pool.acquire().await?;

    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

    let select_res = sqlx::query(
        "SELECT wt.* FROM worker_tasks wt
         JOIN task_instances ti ON ti.id = wt.instance_id
         WHERE wt.handler_name=?1 AND wt.state='pending' AND ti.tenant_id=?3
         ORDER BY wt.created_at ASC
         LIMIT ?2",
    )
    .bind(handler_name)
    .bind(limit as i64)
    .bind(tenant_id.as_str())
    .fetch_all(&mut *conn)
    .await;

    let rows = match select_res {
        Ok(r) => r,
        Err(e) => {
            rollback_quiet(&mut conn).await;
            return Err(StorageError::Query(e.to_string()));
        }
    };

    let mut tasks: Vec<WorkerTask> = match rows.iter().map(row_to_worker_task).collect() {
        Ok(t) => t,
        Err(e) => {
            rollback_quiet(&mut conn).await;
            return Err(e);
        }
    };

    if !tasks.is_empty() {
        let now_dt = chrono::Utc::now();
        for t in &mut tasks {
            t.state = orch8_types::worker::WorkerTaskState::Claimed;
            t.worker_id = Some(worker_id.to_string());
            t.claimed_at = Some(now_dt);
            t.heartbeat_at = Some(now_dt);
            t.claim_epoch = t.claim_epoch.saturating_add(1);
        }
        let mut qb = sqlx::QueryBuilder::new("UPDATE worker_tasks SET state='claimed', worker_id=");
        qb.push_bind(worker_id);
        qb.push(", claimed_at=");
        qb.push_bind(&now);
        qb.push(", heartbeat_at=");
        qb.push_bind(&now);
        qb.push(", claim_epoch=claim_epoch+1");
        qb.push(" WHERE id IN (");
        let mut separated = qb.separated(",");
        for t in &tasks {
            separated.push_bind(t.id.to_string());
        }
        separated.push_unseparated(")");

        if let Err(e) = qb.build().execute(&mut *conn).await {
            rollback_quiet(&mut conn).await;
            return Err(StorageError::Query(e.to_string()));
        }
        let events: Vec<_> = tasks
            .iter()
            .map(|task| {
                transition_event(
                    task.id,
                    task.claim_epoch,
                    task.worker_id.clone(),
                    WorkerAttemptEventKind::Claimed,
                    None,
                )
            })
            .collect();
        if let Err(e) = insert_attempt_events(&mut conn, &events).await {
            rollback_quiet(&mut conn).await;
            return Err(e);
        }
    }

    if let Err(e) = sqlx::query("COMMIT").execute(&mut *conn).await {
        rollback_quiet(&mut conn).await;
        return Err(StorageError::Query(e.to_string()));
    }
    Ok(tasks)
}

/// Issue `ROLLBACK` on a pooled connection, swallowing any error. The
/// caller's real error is the signal to propagate — a secondary rollback
/// failure would just shadow the root cause, and sqlx resets the
/// connection when it returns to the pool in a broken txn state.
async fn rollback_quiet(conn: &mut sqlx::SqliteConnection) {
    if let Err(e) = sqlx::query("ROLLBACK").execute(&mut *conn).await {
        tracing::warn!(error = %e, "claim: ROLLBACK failed, connection will be reset");
    }
}

#[instrument(skip(storage, output), fields(%task_id, worker_id))]
pub(super) async fn complete(
    storage: &SqliteStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
    output: &serde_json::Value,
) -> Result<bool, StorageError> {
    let mut tx = storage.pool.begin().await?;
    let result = sqlx::query("UPDATE worker_tasks SET state='completed', output=?3, completed_at=?4 WHERE id=?1 AND worker_id=?2 AND state='claimed' AND claim_epoch=?5")
        .bind(task_id.to_string())
        .bind(&claim.worker_id)
        .bind(serde_json::to_string(output)?)
        .bind(ts(Utc::now()))
        .bind(i64::try_from(claim.claim_epoch).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        ?;
    let changed = result.rows_affected() > 0;
    if changed {
        insert_attempt_events(
            &mut tx,
            &[transition_event(
                task_id,
                claim.claim_epoch,
                Some(claim.worker_id.clone()),
                WorkerAttemptEventKind::Completed,
                None,
            )],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(changed)
}

#[instrument(skip(storage), fields(%task_id, worker_id, retryable))]
pub(super) async fn fail(
    storage: &SqliteStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
    message: &str,
    retryable: bool,
) -> Result<bool, StorageError> {
    let mut tx = storage.pool.begin().await?;
    let result = sqlx::query("UPDATE worker_tasks SET state='failed', error_message=?3, error_retryable=?4, completed_at=?5 WHERE id=?1 AND worker_id=?2 AND state='claimed' AND claim_epoch=?6")
        .bind(task_id.to_string())
        .bind(&claim.worker_id)
        .bind(message)
        .bind(retryable as i32)
        .bind(ts(Utc::now()))
        .bind(i64::try_from(claim.claim_epoch).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        ?;
    let changed = result.rows_affected() > 0;
    if changed {
        insert_attempt_events(
            &mut tx,
            &[transition_event(
                task_id,
                claim.claim_epoch,
                Some(claim.worker_id.clone()),
                WorkerAttemptEventKind::Failed,
                Some(message.to_string()),
            )],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(changed)
}

pub(super) async fn heartbeat(
    storage: &SqliteStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE worker_tasks SET heartbeat_at=?4 WHERE id=?1 AND worker_id=?2 AND claim_epoch=?3 AND state='claimed'",
    )
    .bind(task_id.to_string())
    .bind(&claim.worker_id)
    .bind(i64::try_from(claim.claim_epoch).unwrap_or(i64::MAX))
    .bind(ts(Utc::now()))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn checkpoint(
    storage: &SqliteStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
    expected_seq: u64,
    checkpoint: &serde_json::Value,
) -> Result<Option<u64>, StorageError> {
    let expected = i64::try_from(expected_seq)
        .map_err(|_| StorageError::Query("checkpoint sequence exceeds INTEGER".into()))?;
    let next = expected
        .checked_add(1)
        .ok_or_else(|| StorageError::Query("checkpoint sequence overflow".into()))?;
    let result = sqlx::query(
        "UPDATE worker_tasks
         SET heartbeat_at=?5, resume_checkpoint=?6, checkpoint_seq=?7
         WHERE id=?1 AND worker_id=?2 AND claim_epoch=?3 AND state='claimed' AND checkpoint_seq=?4",
    )
    .bind(task_id.to_string())
    .bind(&claim.worker_id)
    .bind(i64::try_from(claim.claim_epoch).unwrap_or(i64::MAX))
    .bind(expected)
    .bind(ts(Utc::now()))
    .bind(serde_json::to_string(checkpoint)?)
    .bind(next)
    .execute(&storage.pool)
    .await?;
    Ok((result.rows_affected() == 1).then_some(next as u64))
}

pub(super) async fn record_attempt_event(
    storage: &SqliteStorage,
    event: &WorkerTaskAttemptEvent,
) -> Result<(), StorageError> {
    let mut conn = storage.pool.acquire().await?;
    insert_attempt_events(&mut conn, std::slice::from_ref(event)).await
}

pub(super) async fn list_attempt_events(
    storage: &SqliteStorage,
    task_id: Uuid,
    limit: u32,
) -> Result<Vec<WorkerTaskAttemptEvent>, StorageError> {
    use std::str::FromStr;
    type Row = (
        String,
        String,
        i64,
        Option<String>,
        String,
        Option<String>,
        String,
    );
    let rows: Vec<Row> = sqlx::query_as("SELECT id,task_id,claim_epoch,worker_id,event,reason,created_at FROM worker_task_attempt_events WHERE task_id=?1 ORDER BY created_at ASC,id ASC LIMIT ?2")
        .bind(task_id.to_string()).bind(i64::from(limit.min(1000))).fetch_all(&storage.pool).await?;
    rows.into_iter()
        .map(
            |(id, task_id, epoch, worker_id, event, reason, created_at)| {
                Ok(WorkerTaskAttemptEvent {
                    id: Uuid::parse_str(&id).map_err(|e| StorageError::Query(e.to_string()))?,
                    task_id: Uuid::parse_str(&task_id)
                        .map_err(|e| StorageError::Query(e.to_string()))?,
                    claim_epoch: u64::try_from(epoch)
                        .map_err(|_| StorageError::Query("negative claim_epoch".into()))?,
                    worker_id,
                    event: WorkerAttemptEventKind::from_str(&event).map_err(StorageError::Query)?,
                    reason,
                    created_at: super::helpers::parse_ts(&created_at)?,
                })
            },
        )
        .collect()
}

pub(super) async fn delete(storage: &SqliteStorage, task_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM worker_tasks WHERE id=?1")
        .bind(task_id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn reap_stale(
    storage: &SqliteStorage,
    stale_threshold: Duration,
) -> Result<u64, StorageError> {
    let cutoff = Utc::now()
        - chrono::Duration::from_std(stale_threshold)
            .unwrap_or_else(|_| chrono::Duration::seconds(300));
    let mut conn = storage.pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
    let rows: Vec<(String, i64, Option<String>)> = sqlx::query_as(
        "SELECT id,claim_epoch,worker_id FROM worker_tasks WHERE state='claimed' AND (heartbeat_at IS NULL OR heartbeat_at < ?1)",
    )
    .bind(ts(cutoff))
    .fetch_all(&mut *conn)
    .await?;
    if !rows.is_empty() {
        let mut update = sqlx::QueryBuilder::new(
            "UPDATE worker_tasks SET state='pending',worker_id=NULL,claimed_at=NULL,heartbeat_at=NULL WHERE id IN (",
        );
        let mut ids = update.separated(",");
        for (id, _, _) in &rows {
            ids.push_bind(id);
        }
        ids.push_unseparated(")");
        if let Err(error) = update.build().execute(&mut *conn).await {
            rollback_quiet(&mut conn).await;
            return Err(error.into());
        }
    }
    let events: Vec<_> = rows
        .iter()
        .filter_map(|(id, epoch, worker)| {
            Some(transition_event(
                Uuid::parse_str(id).ok()?,
                u64::try_from(*epoch).ok()?,
                worker.clone(),
                WorkerAttemptEventKind::Reclaimed,
                Some("heartbeat lease expired".into()),
            ))
        })
        .collect();
    if let Err(error) = insert_attempt_events(&mut conn, &events).await {
        rollback_quiet(&mut conn).await;
        return Err(error);
    }
    if let Err(error) = sqlx::query("COMMIT").execute(&mut *conn).await {
        rollback_quiet(&mut conn).await;
        return Err(error.into());
    }
    Ok(rows.len() as u64)
}

/// Fail worker tasks whose `timeout_ms` has elapsed since `created_at`.
pub(super) async fn expire_timed_out(storage: &SqliteStorage) -> Result<u64, StorageError> {
    let now = ts(Utc::now());
    let mut tx = storage.pool.begin().await?;
    let rows: Vec<(String, i64, Option<String>)> = sqlx::query_as(
        "UPDATE worker_tasks
         SET state='failed',
             error_message='task timed out (timeout_ms exceeded)',
             error_retryable=0,
             completed_at=?1
         WHERE state IN ('pending','claimed')
           AND timeout_ms IS NOT NULL
           AND datetime(created_at, '+' || (timeout_ms / 1000.0) || ' seconds') < datetime(?1)
         RETURNING id,claim_epoch,worker_id",
    )
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;
    let events: Vec<_> = rows
        .iter()
        .filter_map(|(id, epoch, worker)| {
            Some(transition_event(
                Uuid::parse_str(id).ok()?,
                u64::try_from(*epoch).ok()?,
                worker.clone(),
                WorkerAttemptEventKind::TimedOut,
                Some("timeout_ms exceeded".into()),
            ))
        })
        .collect();
    insert_attempt_events(&mut tx, &events).await?;
    tx.commit().await?;
    Ok(rows.len() as u64)
}

pub(super) async fn cancel_for_block(
    storage: &SqliteStorage,
    instance_id: Uuid,
    block_id: &str,
) -> Result<u64, StorageError> {
    // Delete all rows for (instance_id, block_id) regardless of state.
    // Mirrors postgres::workers::cancel_for_block — iteration reset in
    // ForEach/Loop must remove `completed` rows so the next iteration's
    // INSERT isn't dropped by UNIQUE(instance_id, block_id).
    let mut tx = storage.pool.begin().await?;
    let rows: Vec<(String, i64, Option<String>)> = sqlx::query_as("DELETE FROM worker_tasks WHERE instance_id=?1 AND block_id=?2 RETURNING id,claim_epoch,worker_id")
        .bind(instance_id.to_string())
        .bind(block_id)
        .fetch_all(&mut *tx)
        .await?;
    let events: Vec<_> = rows
        .iter()
        .filter_map(|(id, epoch, worker)| {
            Some(transition_event(
                Uuid::parse_str(id).ok()?,
                u64::try_from(*epoch).ok()?,
                worker.clone(),
                WorkerAttemptEventKind::Cancelled,
                Some("workflow branch cancelled or reset".into()),
            ))
        })
        .collect();
    insert_attempt_events(&mut tx, &events).await?;
    tx.commit().await?;
    Ok(rows.len() as u64)
}

/// Batch variant of [`cancel_for_block`]: single `DELETE ... IN (...)`
/// round-trip for an arbitrary set of block IDs under one instance.
pub(super) async fn cancel_for_blocks(
    storage: &SqliteStorage,
    instance_id: Uuid,
    block_ids: &[String],
) -> Result<u64, StorageError> {
    if block_ids.is_empty() {
        return Ok(0);
    }
    let mut tx = storage.pool.begin().await?;
    let mut qb = sqlx::QueryBuilder::new("DELETE FROM worker_tasks WHERE instance_id=");
    qb.push_bind(instance_id.to_string());
    qb.push(" AND block_id IN (");
    let mut sep = qb.separated(", ");
    for bid in block_ids {
        sep.push_bind(bid);
    }
    sep.push_unseparated(") RETURNING id,claim_epoch,worker_id");
    let rows: Vec<(String, i64, Option<String>)> = qb.build_query_as().fetch_all(&mut *tx).await?;
    let events: Vec<_> = rows
        .iter()
        .filter_map(|(id, epoch, worker)| {
            Some(transition_event(
                Uuid::parse_str(id).ok()?,
                u64::try_from(*epoch).ok()?,
                worker.clone(),
                WorkerAttemptEventKind::Cancelled,
                Some("workflow branches cancelled or reset".into()),
            ))
        })
        .collect();
    insert_attempt_events(&mut tx, &events).await?;
    tx.commit().await?;
    Ok(rows.len() as u64)
}

pub(super) async fn list(
    storage: &SqliteStorage,
    filter: &orch8_types::worker_filter::WorkerTaskFilter,
    pagination: &orch8_types::filter::Pagination,
) -> Result<Vec<WorkerTask>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new("SELECT * FROM worker_tasks WHERE 1=1");
    if let Some(ref tid) = filter.tenant_id {
        qb.push(" AND instance_id IN (SELECT id FROM task_instances WHERE tenant_id=");
        qb.push_bind(tid.as_str());
        qb.push(")");
    }
    if let Some(ref states) = filter.states
        && !states.is_empty()
    {
        qb.push(" AND state IN (");
        let mut separated = qb.separated(",");
        for state in states {
            separated.push_bind(state.to_string());
        }
        separated.push_unseparated(")");
    }
    if let Some(ref handler) = filter.handler_name {
        qb.push(" AND handler_name=");
        qb.push_bind(handler);
    }
    if let Some(ref wid) = filter.worker_id {
        qb.push(" AND worker_id=");
        qb.push_bind(wid);
    }
    if let Some(ref queue) = filter.queue_name {
        qb.push(" AND queue_name=");
        qb.push_bind(queue);
    }
    if pagination.sort_ascending {
        qb.push(" ORDER BY created_at ASC");
    } else {
        qb.push(" ORDER BY created_at DESC");
    }

    let limit = i64::from(pagination.limit.min(1000));
    let offset = i64::try_from(pagination.offset).unwrap_or(i64::MAX);

    qb.push(" LIMIT ");
    qb.push_bind(limit);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let rows = qb.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_worker_task).collect()
}

pub(super) async fn stats(
    storage: &SqliteStorage,
    tenant_id: Option<&orch8_types::ids::TenantId>,
) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
    let mut cqb =
        sqlx::QueryBuilder::new("SELECT state, handler_name, COUNT(*) as cnt FROM worker_tasks");
    if let Some(tid) = tenant_id {
        cqb.push(" WHERE instance_id IN (SELECT id FROM task_instances WHERE tenant_id=");
        cqb.push_bind(tid.as_str());
        cqb.push(")");
    }
    cqb.push(" GROUP BY state, handler_name");

    let counts = cqb
        .build_query_as::<(String, String, i64)>()
        .fetch_all(&storage.pool)
        .await?;

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

    let mut wqb = sqlx::QueryBuilder::new(
        "SELECT DISTINCT worker_id FROM worker_tasks WHERE state = 'claimed' AND worker_id IS NOT NULL",
    );
    if let Some(tid) = tenant_id {
        wqb.push(" AND instance_id IN (SELECT id FROM task_instances WHERE tenant_id=");
        wqb.push_bind(tid.as_str());
        wqb.push(")");
    }

    let workers = wqb
        .build_query_as::<(String,)>()
        .fetch_all(&storage.pool)
        .await?;

    let active_workers = workers.into_iter().map(|(w,)| w).collect();

    Ok(orch8_types::worker_filter::WorkerTaskStats {
        by_state,
        by_handler,
        active_workers,
    })
}

// === Worker Registry ===

pub(super) async fn upsert_registration(
    storage: &SqliteStorage,
    reg: &orch8_types::worker::WorkerRegistration,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO worker_registrations (worker_id, handler_name, queue_name, version, tenant_id, last_seen_at)
         VALUES (?1,?2,?3,?4,?5,?6)
         ON CONFLICT(worker_id, handler_name) DO UPDATE
         SET queue_name = excluded.queue_name,
             version = excluded.version,
             tenant_id = excluded.tenant_id,
             last_seen_at = excluded.last_seen_at",
    )
    .bind(&reg.worker_id)
    .bind(&reg.handler_name)
    .bind(&reg.queue_name)
    .bind(&reg.version)
    .bind(&reg.tenant_id)
    .bind(ts(reg.last_seen_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_registrations(
    storage: &SqliteStorage,
    seen_within_secs: Option<i64>,
) -> Result<Vec<orch8_types::worker::WorkerRegistration>, StorageError> {
    type Row = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    );
    let rows: Vec<Row> = if let Some(secs) = seen_within_secs {
        let cutoff = ts(Utc::now() - chrono::Duration::seconds(secs));
        sqlx::query_as(
            "SELECT worker_id, handler_name, queue_name, version, tenant_id, last_seen_at
             FROM worker_registrations
             WHERE last_seen_at >= ?1
             ORDER BY last_seen_at DESC",
        )
        .bind(cutoff)
        .fetch_all(&storage.pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT worker_id, handler_name, queue_name, version, tenant_id, last_seen_at
             FROM worker_registrations
             ORDER BY last_seen_at DESC",
        )
        .fetch_all(&storage.pool)
        .await?
    };
    rows.into_iter()
        .map(
            |(worker_id, handler_name, queue_name, version, tenant_id, last_seen_at)| {
                Ok(orch8_types::worker::WorkerRegistration {
                    worker_id,
                    handler_name,
                    queue_name,
                    version,
                    tenant_id,
                    last_seen_at: super::helpers::parse_ts(&last_seen_at)?,
                })
            },
        )
        .collect()
}

pub(super) async fn claimed_counts_by_worker(
    storage: &SqliteStorage,
) -> Result<Vec<(String, i64)>, StorageError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT worker_id, COUNT(*) FROM worker_tasks
         WHERE state = 'claimed' AND worker_id IS NOT NULL
         GROUP BY worker_id",
    )
    .fetch_all(&storage.pool)
    .await?;
    Ok(rows)
}
