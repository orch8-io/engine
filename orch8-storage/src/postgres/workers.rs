use sqlx::AssertSqlSafe;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::worker::{
    WorkerAttemptEventKind, WorkerClaim, WorkerTask, WorkerTaskAttemptEvent,
};
use sqlx::Postgres;

use super::PostgresStorage;
use super::rows::{WorkerTaskRow, worker_task_columns};

const REAPER_BATCH_SIZE: i64 = 1_000;

pub(super) async fn insert_attempt_events(
    conn: &mut sqlx::PgConnection,
    events: &[WorkerTaskAttemptEvent],
) -> Result<(), StorageError> {
    if events.is_empty() {
        return Ok(());
    }
    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO worker_task_attempt_events \
         (id, task_id, claim_epoch, worker_id, event, reason, created_at) ",
    );
    let epochs = events
        .iter()
        .map(|event| crate::worker_claim::sql_counter(event.claim_epoch))
        .collect::<Result<Vec<_>, _>>()?;
    qb.push_values(events.iter().zip(epochs), |mut row, (event, epoch)| {
        row.push_bind(event.id)
            .push_bind(event.task_id)
            .push_bind(epoch)
            .push_bind(&event.worker_id)
            .push_bind(event.event.as_str())
            .push_bind(&event.reason)
            .push_bind(event.created_at);
    });
    qb.build().execute(conn).await?;
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
        created_at: chrono::Utc::now(),
    }
}

/// INSERT statement for one worker task. On a `(instance_id, block_id)`
/// conflict with a still-pending row of the same attempt (a retry row
/// pre-inserted by a failure/lease resolution), the dispatch-time effect id,
/// ownership epoch, and credential flag are filled in so the re-dispatch's
/// receipt binds to the row a worker will actually claim.
///
/// `awaiting_dispatch = true` (retry rows only) makes the row unclaimable
/// until that re-dispatch binds it: the same upsert clears the flag in the
/// statement that writes `effect_id`, so a worker can never claim a retry
/// attempt before its effect id is present.
pub(super) fn insert_task_query(
    task: &WorkerTask,
    awaiting_dispatch: bool,
) -> Result<sqlx::query::Query<'static, Postgres, sqlx::postgres::PgArguments>, StorageError> {
    let params = super::json_encoding::encode(&task.params)?;
    let context = super::json_encoding::encode(&task.context)?;
    let checkpoint = task
        .resume_checkpoint
        .as_ref()
        .map(super::json_encoding::encode)
        .transpose()?;
    let checkpoint_encoding = checkpoint.as_ref().and_then(|value| value.encoding);
    Ok(sqlx::query(
        r"INSERT INTO worker_tasks
            (id, instance_id, block_id, handler_name, queue_name, requirements, params, context,
             attempt, timeout_ms, state, claim_epoch, resume_checkpoint, checkpoint_seq, created_at,
             effect_id, continuity_epoch, carries_credentials, awaiting_dispatch, tenant_id, params_encoding, context_encoding, checkpoint_encoding)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
                  (SELECT tenant_id FROM task_instances WHERE id = $2),$20,$21,$22)
          ON CONFLICT (instance_id, block_id) DO UPDATE SET
              effect_id = COALESCE(worker_tasks.effect_id, EXCLUDED.effect_id),
              continuity_epoch = COALESCE(worker_tasks.continuity_epoch, EXCLUDED.continuity_epoch),
              carries_credentials = worker_tasks.carries_credentials OR EXCLUDED.carries_credentials,
              awaiting_dispatch = EXCLUDED.awaiting_dispatch
          WHERE worker_tasks.state = 'pending' AND worker_tasks.attempt = EXCLUDED.attempt RETURNING id",
    )
    .bind(task.id)
    .bind(task.instance_id.into_uuid())
    .bind(task.block_id.as_str().to_owned())
    .bind(task.handler_name.clone())
    .bind(task.queue_name.clone())
    .bind(serde_json::to_value(&task.requirements)?)
    .bind(params.value)
    .bind(context.value)
    .bind(task.attempt as i16)
    .bind(task.timeout_ms)
    .bind(task.state.to_string())
    .bind(crate::worker_claim::sql_counter(task.claim_epoch)?)
    .bind(checkpoint.map(|value|value.value))
    .bind(crate::worker_claim::sql_counter(task.checkpoint_seq)?)
    .bind(task.created_at)
    .bind(
        task.effect_id
            .map(orch8_types::continuity::EffectId::into_uuid),
    )
    .bind(
        task.continuity_epoch
            .map(crate::worker_claim::sql_counter).transpose()?,
    )
    .bind(task.carries_credentials)
    .bind(awaiting_dispatch)
    .bind(params.encoding)
    .bind(context.encoding)
    .bind(checkpoint_encoding))
}

pub(super) async fn create(store: &PostgresStorage, task: &WorkerTask) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;
    let result = async {
        if let Some(row) = insert_task_query(task, false)?
            .fetch_optional(&mut *tx)
            .await?
        {
            use sqlx::Row as _;
            super::worker_invocations::capture(&mut tx, row.get("id"), None).await?;
        }
        Ok(())
    }
    .await;
    super::finish_transaction(tx, result).await
}

pub(super) async fn get(
    store: &PostgresStorage,
    task_id: Uuid,
) -> Result<Option<WorkerTask>, StorageError> {
    let row = sqlx::query_as::<_, WorkerTaskRow>(concat!(
        "SELECT ",
        worker_task_columns!(),
        " FROM worker_tasks WHERE id = $1"
    ))
    .bind(task_id)
    .fetch_optional(&store.pool)
    .await?;
    row.map(WorkerTaskRow::into_task).transpose()
}

pub(super) async fn claim(
    store: &PostgresStorage,
    handler_name: &str,
    worker_id: &str,
    limit: u32,
    principal: Option<&crate::worker_claim::WorkerClaimPrincipal>,
) -> Result<Vec<WorkerTask>, StorageError> {
    let mut tx = store.pool.begin().await?;
    let result=async {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(
        concat!(r"UPDATE worker_tasks
          SET state = 'claimed', worker_id = $2, claimed_at = NOW(), heartbeat_at = NOW(),
              claim_epoch = claim_epoch + 1, lease_secs = NULL, claimed_runtime_kind = NULL
          WHERE id IN (
              SELECT id FROM worker_tasks
              WHERE handler_name = $1 AND state = 'pending' AND claim_epoch>=0 AND claim_epoch<9223372036854775807 AND NOT awaiting_dispatch
                AND (requirements = '{}'::jsonb OR (requirements - 'prefer' = '{}'::jsonb AND (requirements->'prefer'->>'worker_id' = $2 OR (requirements->'prefer'->>'until_ms')::bigint <= (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint)))
                AND NOT EXISTS (SELECT 1 FROM task_instances tix WHERE tix.id = worker_tasks.instance_id AND tix.state IN ('completed', 'failed', 'cancelled')) AND NOT EXISTS (SELECT 1 FROM delegation_proxy_native_history bad_relay WHERE bad_relay.instance_id=worker_tasks.instance_id) AND NOT EXISTS (SELECT 1 FROM physical_instance_retirements retired WHERE retired.instance_id=worker_tasks.instance_id) AND NOT EXISTS (SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=worker_tasks.instance_id AND cut.state IN ('captured','sealed')) AND NOT EXISTS (SELECT 1 FROM continuity_executions ce WHERE ce.state='compensating' AND ce.tenant_id=(SELECT ti_owner.tenant_id FROM task_instances ti_owner WHERE ti_owner.id=worker_tasks.instance_id) AND ce.record->>'current_instance_id'=worker_tasks.instance_id::text)
              ORDER BY created_at
              LIMIT $3
              FOR UPDATE SKIP LOCKED
          )
          RETURNING ",
            worker_task_columns!()
        ),
    )
    .bind(handler_name)
    .bind(worker_id)
    .bind(i64::from(limit))
    .fetch_all(&mut *tx)
    .await?;
    let tasks: Vec<_> = rows
        .into_iter()
        .map(WorkerTaskRow::into_task)
        .collect::<Result<_, _>>()?;
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
    super::worker_claim::record_on(&mut tx, &tasks, principal).await?;
    insert_attempt_events(&mut tx, &events).await?;
    Ok(tasks)
    }.await;
    super::finish_transaction(tx, result).await
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
    principal: Option<&crate::worker_claim::WorkerClaimPrincipal>,
) -> Result<Vec<WorkerTask>, StorageError> {
    fill_pending_tenants(store, handler_name).await?;
    let mut tx = store.pool.begin().await?;
    let result=async {
    // `wt.tenant_id` (L4-03) lets the poll walk only this tenant's pending
    // rows (idx_worker_tasks_tenant_poll); the instance join only re-checks
    // that the instance is still live.
    let rows = sqlx::query_as::<_, WorkerTaskRow>(concat!(
        r"UPDATE worker_tasks
          SET state = 'claimed', worker_id = $2, claimed_at = NOW(), heartbeat_at = NOW(),
              claim_epoch = claim_epoch + 1, lease_secs = NULL, claimed_runtime_kind = NULL
          WHERE id IN (
              SELECT wt.id FROM worker_tasks wt
              JOIN task_instances ti ON ti.id = wt.instance_id
              WHERE wt.handler_name = $1
                AND wt.tenant_id = $4
                AND wt.state = 'pending' AND wt.claim_epoch>=0 AND wt.claim_epoch<9223372036854775807
                AND NOT wt.awaiting_dispatch
                AND (wt.requirements = '{}'::jsonb OR (wt.requirements - 'prefer' = '{}'::jsonb AND (wt.requirements->'prefer'->>'worker_id' = $2 OR (wt.requirements->'prefer'->>'until_ms')::bigint <= (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint)))
                AND ti.tenant_id = $4
                AND ti.state NOT IN ('completed', 'failed', 'cancelled') AND NOT EXISTS (SELECT 1 FROM delegation_proxy_native_history bad_relay WHERE bad_relay.instance_id=wt.instance_id) AND NOT EXISTS (SELECT 1 FROM physical_instance_retirements retired WHERE retired.instance_id=wt.instance_id) AND NOT EXISTS (SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=wt.instance_id AND cut.state IN ('captured','sealed')) AND NOT EXISTS (SELECT 1 FROM continuity_executions ce WHERE ce.state='compensating' AND ce.tenant_id=(SELECT ti_owner.tenant_id FROM task_instances ti_owner WHERE ti_owner.id=wt.instance_id) AND ce.record->>'current_instance_id'=wt.instance_id::text)
              ORDER BY wt.created_at
              LIMIT $3
              FOR UPDATE OF wt SKIP LOCKED
          )
          RETURNING ",
        worker_task_columns!()
    ))
    .bind(handler_name)
    .bind(worker_id)
    .bind(i64::from(limit))
    .bind(tenant_id.as_str())
    .fetch_all(&mut *tx)
    .await?;
    let tasks: Vec<_> = rows
        .into_iter()
        .map(WorkerTaskRow::into_task)
        .collect::<Result<_, _>>()?;
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
    super::worker_claim::record_on(&mut tx, &tasks, principal).await?;
    insert_attempt_events(&mut tx, &events).await?;
    Ok(tasks)
    }.await;
    super::finish_transaction(tx, result).await
}

/// Stamp `tenant_id` onto pending rows of `handler_name` that lack it (rows
/// inserted by a pre-103 binary during a rolling deploy), so tenant-scoped
/// polls, which filter on `worker_tasks.tenant_id`, can see them. Served by
/// the partial `idx_worker_tasks_untenanted_pending` index: once no such row
/// is left this is an empty index probe.
async fn fill_pending_tenants(
    store: &PostgresStorage,
    handler_name: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE worker_tasks wt SET tenant_id = ti.tenant_id
          FROM task_instances ti
          WHERE ti.id = wt.instance_id
            AND wt.handler_name = $1
            AND wt.state = 'pending' AND wt.claim_epoch>=0 AND wt.claim_epoch<9223372036854775807
            AND wt.tenant_id IS NULL",
    )
    .bind(handler_name)
    .execute(&store.pool)
    .await?;
    Ok(())
}

/// Upper bound on pending rows one `claim_matching` poll inspects.
const CLAIM_MATCHING_MAX_SCAN: usize = 4096;

#[allow(clippy::too_many_lines)] // two-phase scan + fenced claim reads best as one unit
pub(super) async fn claim_matching(
    store: &PostgresStorage,
    handler_name: &str,
    worker_id: &str,
    tenant_id: Option<&orch8_types::TenantId>,
    queue_name: Option<&str>,
    runtime: crate::worker_claim::ClaimRuntime<'_>,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let crate::worker_claim::ClaimRuntime {
        capabilities,
        principal,
    } = runtime;
    // An unqualified session poll grants the default queue only. Named queues
    // require the transport's explicit allowlist check and named-queue selector.
    let default_queue_only = queue_name.is_none()
        && matches!(
            principal,
            Some(crate::worker_claim::WorkerClaimPrincipal::Runtime { .. })
        );
    if limit == 0 {
        return Ok(Vec::new());
    }
    let now = chrono::Utc::now();
    // Phase 1: scan pending candidates WITHOUT row locks, filtering on the
    // capability requirements in Rust. Locking during the scan (the old
    // shape) held FOR UPDATE on every scanned row -- including the ones
    // rejected in Rust -- until commit, so one worker with narrow
    // capabilities blocked (SKIP LOCKED: hid) the whole backlog from every
    // other worker. The scan is capped so a deep backlog of unsatisfiable
    // tasks can't turn one poll into a full-table walk. Over-collect a
    // little so rows lost to concurrent claimers can be backfilled.
    if tenant_id.is_some() {
        fill_pending_tenants(store, handler_name).await?;
    }
    let want = (limit as usize).saturating_mul(2);
    let mut ids: Vec<Uuid> = Vec::with_capacity(want);
    let mut cursor: Option<(chrono::DateTime<chrono::Utc>, Uuid)> = None;
    let mut scanned = 0usize;
    while ids.len() < want && scanned < CLAIM_MATCHING_MAX_SCAN {
        let mut query = sqlx::QueryBuilder::<Postgres>::new(
            "SELECT wt.id, wt.created_at, wt.requirements, wt.carries_credentials FROM worker_tasks wt",
        );
        query
            .push(" WHERE wt.handler_name = ")
            .push_bind(handler_name);
        // A retry row is claimable only once its re-dispatch bound the
        // attempt's effect id (see `insert_task_query`).
        query.push(" AND wt.state = 'pending' AND wt.claim_epoch>=0 AND wt.claim_epoch<9223372036854775807 AND NOT wt.awaiting_dispatch");
        // Never hand out work for an instance that already finished or was
        // cancelled (its tasks are purged on cancel, but a racing dispatch
        // or an older row may remain).
        query.push(" AND NOT EXISTS (SELECT 1 FROM task_instances tix WHERE tix.id = wt.instance_id AND tix.state IN ('completed', 'failed', 'cancelled')) AND NOT EXISTS (SELECT 1 FROM delegation_proxy_native_history bad_relay WHERE bad_relay.instance_id=wt.instance_id) AND NOT EXISTS (SELECT 1 FROM physical_instance_retirements retired WHERE retired.instance_id=wt.instance_id) AND NOT EXISTS (SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=wt.instance_id AND cut.state IN ('captured','sealed')) AND NOT EXISTS (SELECT 1 FROM continuity_executions ce WHERE ce.state='compensating' AND ce.tenant_id=(SELECT ti_owner.tenant_id FROM task_instances ti_owner WHERE ti_owner.id=wt.instance_id) AND ce.record->>'current_instance_id'=wt.instance_id::text)");
        // Push the cheap hard requirements into SQL so unclaimable rows never
        // consume the bounded scan window and hide eligible ones behind them
        // (a phone advertises few labels, and every older task needing a
        // label / another runtime kind / a named runtime would otherwise sit
        // in front of the work it can actually take). Each predicate is a
        // strict superset of what `claim_allowed` accepts -- the Rust check
        // below stays authoritative.
        query
            .push(" AND (NOT (wt.requirements ? 'runtime_kinds') OR wt.requirements->'runtime_kinds' @> to_jsonb(")
            .push_bind(capabilities.kind.as_str())
            .push("::text))");
        query
            .push(" AND (NOT (wt.requirements ? 'runtime_id') OR wt.requirements->>'runtime_id' = ")
            .push_bind(capabilities.runtime_id.to_string())
            .push(")");
        query
            .push(" AND (NOT (wt.requirements ? 'labels') OR wt.requirements->'labels' <@ ")
            .push_bind(
                serde_json::to_value(&capabilities.labels).map_err(StorageError::Serialization)?,
            )
            .push("::jsonb)");
        query
            .push(" AND (NOT (wt.requirements ? 'residency') OR wt.requirements->>'residency' = ")
            .push_bind(
                capabilities
                    .labels
                    .get(orch8_types::placement::RESIDENCY_LABEL)
                    .cloned(),
            )
            .push("::text)");
        if capabilities.kind == orch8_types::continuity::RuntimeKind::Browser {
            // A browser never receives credential-bearing work.
            query.push(" AND NOT wt.carries_credentials");
        }
        if let Some(queue) = queue_name {
            query.push(" AND wt.queue_name = ").push_bind(queue);
        } else if default_queue_only {
            query.push(" AND wt.queue_name IS NULL");
        }
        if let Some(tenant) = tenant_id {
            // Denormalised tenant (L4-03): no per-row instance join, so the
            // scan never walks other tenants' backlog.
            query
                .push(" AND wt.tenant_id = ")
                .push_bind(tenant.as_str());
        }
        if let Some((created_at, id)) = cursor {
            query
                .push(" AND (wt.created_at, wt.id) > (")
                .push_bind(created_at)
                .push(", ")
                .push_bind(id)
                .push(")");
        }
        query.push(" ORDER BY wt.created_at, wt.id LIMIT 256");
        let page: Vec<(Uuid, chrono::DateTime<chrono::Utc>, serde_json::Value, bool)> =
            query.build_query_as().fetch_all(&store.pool).await?;
        let Some(&(last_id, last_created_at, _, _)) = page.last() else {
            break;
        };
        scanned += page.len();
        cursor = Some((last_created_at, last_id));
        for (id, _, requirements, carries_credentials) in page {
            if ids.len() >= want {
                break;
            }
            let requirements: orch8_types::continuity::CapsuleRequirements =
                serde_json::from_value(requirements).map_err(StorageError::Serialization)?;
            if orch8_types::worker::claim_allowed(
                &requirements,
                carries_credentials,
                capabilities,
                now,
            ) {
                ids.push(id);
            }
        }
    }
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    // Phase 2: lock only the chosen rows (re-checking `pending`, since the
    // scan was unlocked) and claim at most `limit` of them, oldest first.
    let mut tx = store.pool.begin().await?;
    let result=async {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(concat!(
        r"UPDATE worker_tasks SET state='claimed', worker_id=$1, claimed_at=NOW(),
             heartbeat_at=NOW(), claim_epoch=claim_epoch+1,
             lease_secs=$4, claimed_runtime_kind=$5
           WHERE id IN (
             SELECT id FROM worker_tasks
             WHERE id = ANY($2) AND ($6::text IS NULL OR queue_name=$6) AND (NOT $7 OR queue_name IS NULL) AND ($8::text IS NULL OR tenant_id=$8) AND state = 'pending' AND claim_epoch>=0 AND claim_epoch<9223372036854775807 AND NOT awaiting_dispatch AND NOT EXISTS (SELECT 1 FROM delegation_proxy_native_history bad_relay WHERE bad_relay.instance_id=worker_tasks.instance_id) AND NOT EXISTS (SELECT 1 FROM physical_instance_retirements retired WHERE retired.instance_id=worker_tasks.instance_id) AND NOT EXISTS (SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=worker_tasks.instance_id AND cut.state IN ('captured','sealed')) AND NOT EXISTS (SELECT 1 FROM continuity_executions ce WHERE ce.state='compensating' AND ce.tenant_id=(SELECT ti_owner.tenant_id FROM task_instances ti_owner WHERE ti_owner.id=worker_tasks.instance_id) AND ce.record->>'current_instance_id'=worker_tasks.instance_id::text)
             ORDER BY created_at, id
             LIMIT $3
             FOR UPDATE SKIP LOCKED
           )
           RETURNING ",
        worker_task_columns!()
    ))
    .bind(worker_id)
    .bind(&ids)
    .bind(i64::from(limit))
    .bind(
        capabilities
            .kind
            .default_lease_secs()
            .map(|secs| secs as i32),
    )
    .bind(capabilities.kind.as_str())
    .bind(queue_name)
    .bind(default_queue_only)
    .bind(tenant_id.map(orch8_types::TenantId::as_str))
    .fetch_all(&mut *tx)
    .await?;
    let tasks = rows
        .into_iter()
        .map(WorkerTaskRow::into_task)
        .collect::<Result<Vec<_>, _>>()?;
    let events = tasks
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
        .collect::<Vec<_>>();
    super::worker_claim::record_on(&mut tx, &tasks, principal).await?;
    insert_attempt_events(&mut tx, &events).await?;
    Ok(tasks)
    }.await;
    super::finish_transaction(tx, result).await
}

pub(super) async fn complete(
    store: &PostgresStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
    completion: &crate::worker_outcome::WorkerCompletion,
) -> Result<bool, StorageError> {
    completion.validate_stored()?;
    let output = super::json_encoding::encode(&completion.output)?;
    let epoch = i64::try_from(claim.claim_epoch)
        .map_err(|_| StorageError::Constraint("worker claim epoch exceeds SQL range".into()))?;
    let mut tx = store.pool.begin().await?;
    let result = async {
        let Some((instance_id,effect_id)) = super::worker_outcome::lock_on(&mut tx,task_id).await? else { return Ok(false); };
        let row: Option<(i64,)> = sqlx::query_as(
            r"UPDATE worker_tasks
          SET state = 'completed', output = $3, output_encoding=$7, completed_at = NOW()
          WHERE id = $1 AND worker_id = $2 AND claim_epoch = $4 AND state = 'claimed' AND instance_id=$5 AND effect_id IS NOT DISTINCT FROM $6
          RETURNING claim_epoch",
        )
        .bind(task_id)
        .bind(&claim.worker_id)
        .bind(&output.value)
        .bind(epoch)
        .bind(instance_id.into_uuid())
        .bind(effect_id)
        .bind(output.encoding)
        .fetch_optional(&mut *tx)
        .await?;
        if row.is_some() {
            super::worker_outcome::settle_on(&mut tx, task_id, completion).await?;
            super::worker_outcome::insert_on(&mut tx, task_id, claim, completion).await?;
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
        Ok(row.is_some())
    }
    .await;
    super::finish_transaction(tx, result).await
}

pub(super) async fn fail(
    store: &PostgresStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
    message: &str,
    retryable: bool,
) -> Result<bool, StorageError> {
    let mut tx = store.pool.begin().await?;
    let row: Option<(i64,)> = sqlx::query_as(
        r"UPDATE worker_tasks
          SET state = 'failed', error_message = $3, error_retryable = $4, completed_at = NOW()
          WHERE id = $1 AND worker_id = $2 AND claim_epoch = $5 AND state = 'claimed'
          RETURNING claim_epoch",
    )
    .bind(task_id)
    .bind(&claim.worker_id)
    .bind(message)
    .bind(retryable)
    .bind(crate::worker_claim::sql_counter(claim.claim_epoch)?)
    .fetch_optional(&mut *tx)
    .await?;
    if row.is_some() {
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
    Ok(row.is_some())
}

pub(super) async fn heartbeat(
    store: &PostgresStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE worker_tasks SET heartbeat_at = NOW() \
         WHERE id = $1 AND worker_id = $2 AND claim_epoch = $3 AND state = 'claimed'",
    )
    .bind(task_id)
    .bind(&claim.worker_id)
    .bind(crate::worker_claim::sql_counter(claim.claim_epoch)?)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn checkpoint(
    store: &PostgresStorage,
    task_id: Uuid,
    claim: &WorkerClaim,
    expected_seq: u64,
    checkpoint: &serde_json::Value,
) -> Result<Option<u64>, StorageError> {
    let expected = i64::try_from(expected_seq)
        .map_err(|_| StorageError::Query("checkpoint sequence exceeds BIGINT".into()))?;
    let next = expected
        .checked_add(1)
        .ok_or_else(|| StorageError::Query("checkpoint sequence overflow".into()))?;
    let encoded = super::json_encoding::encode(checkpoint)?;
    let result = sqlx::query(
        "UPDATE worker_tasks
         SET heartbeat_at = NOW(), resume_checkpoint = $5, checkpoint_seq = $6, checkpoint_encoding=$7
         WHERE id = $1 AND worker_id = $2 AND claim_epoch = $3
           AND state = 'claimed' AND checkpoint_seq = $4",
    )
    .bind(task_id)
    .bind(&claim.worker_id)
    .bind(crate::worker_claim::sql_counter(claim.claim_epoch)?)
    .bind(expected)
    .bind(encoded.value)
    .bind(next)
    .bind(encoded.encoding)
    .execute(&store.pool)
    .await?;
    Ok((result.rows_affected() == 1).then_some(next as u64))
}

pub(super) async fn record_attempt_event(
    store: &PostgresStorage,
    event: &WorkerTaskAttemptEvent,
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;
    insert_attempt_events(&mut tx, std::slice::from_ref(event)).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn list_attempt_events(
    store: &PostgresStorage,
    task_id: Uuid,
    limit: u32,
) -> Result<Vec<WorkerTaskAttemptEvent>, StorageError> {
    type Row = (
        Uuid,
        Uuid,
        i64,
        Option<String>,
        String,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id,task_id,claim_epoch,worker_id,event,reason,created_at \
         FROM worker_task_attempt_events WHERE task_id=$1 \
         ORDER BY created_at ASC,id ASC LIMIT $2",
    )
    .bind(task_id)
    .bind(i64::from(limit.min(1000)))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter()
        .map(
            |(id, task_id, epoch, worker_id, event, reason, created_at)| {
                Ok(WorkerTaskAttemptEvent {
                    id,
                    task_id,
                    claim_epoch: u64::try_from(epoch)
                        .map_err(|_| StorageError::Query("negative claim_epoch".into()))?,
                    worker_id,
                    event: event.parse().map_err(StorageError::Query)?,
                    reason,
                    created_at,
                })
            },
        )
        .collect()
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
    let mut tx = store.pool.begin().await?;
    let rows: Vec<(Uuid, i64, Option<String>)> = sqlx::query_as(
        r"WITH stale AS (
              SELECT id, claim_epoch, worker_id FROM worker_tasks
              WHERE state = 'claimed'
                -- A claimed row with no heartbeat yet ages from its claim
                -- time (a row with neither is reclaimable at once); a bare
                -- `heartbeat_at < cutoff` never matched NULL, so such rows
                -- were stranded forever on PG while SQLite reclaimed them.
                -- The per-claim lease (browser/mobile) wins over the default.
                AND (COALESCE(heartbeat_at, claimed_at) IS NULL
                     OR COALESCE(heartbeat_at, claimed_at)
                        < NOW() - make_interval(secs => COALESCE(lease_secs::double precision, $1::double precision)))
                -- Never blindly requeue a task whose side effect may already
                -- have happened: those go through the engine's
                -- ambiguous-effect resolution instead.
                AND NOT EXISTS (
                    SELECT 1 FROM effect_receipts er
                    WHERE er.id = worker_tasks.effect_id
                      AND er.state IN ('dispatched', 'unknown')
                )
              ORDER BY COALESCE(heartbeat_at, claimed_at) ASC NULLS FIRST
              LIMIT $2
              FOR UPDATE SKIP LOCKED
          )
          UPDATE worker_tasks wt
          SET state = 'pending', worker_id = NULL, claimed_at = NULL, heartbeat_at = NULL,
              lease_secs = NULL, claimed_runtime_kind = NULL
          FROM stale
          WHERE wt.id = stale.id
          RETURNING wt.id, stale.claim_epoch, stale.worker_id",
    )
    .bind(threshold_secs)
    .bind(REAPER_BATCH_SIZE)
    .fetch_all(&mut *tx)
    .await?;
    let events: Vec<_> = rows
        .iter()
        .map(|(id, epoch, worker)| {
            transition_event(
                *id,
                u64::try_from(*epoch).unwrap_or(0),
                worker.clone(),
                WorkerAttemptEventKind::Reclaimed,
                Some("heartbeat lease expired".into()),
            )
        })
        .collect();
    insert_attempt_events(&mut tx, &events).await?;
    tx.commit().await?;
    Ok(rows.len() as u64)
}

/// Fail worker tasks whose `timeout_ms` has elapsed since `created_at`.
/// Only affects tasks in `pending` or `claimed` state that have a non-NULL
/// `timeout_ms`. Sets state to `failed` with a descriptive error message.
pub(super) async fn expire_timed_out(store: &PostgresStorage) -> Result<u64, StorageError> {
    let mut tx = store.pool.begin().await?;
    let rows: Vec<(Uuid, i64, Option<String>)> = sqlx::query_as(
        r"UPDATE worker_tasks
          SET state = 'failed',
              error_message = 'task timed out (timeout_ms exceeded)',
              error_retryable = false,
              completed_at = NOW()
          WHERE state IN ('pending', 'claimed')
            AND timeout_ms IS NOT NULL
            AND created_at + make_interval(secs => timeout_ms::double precision / 1000.0) < NOW()
          RETURNING id, claim_epoch, worker_id",
    )
    .fetch_all(&mut *tx)
    .await?;
    let events: Vec<_> = rows
        .iter()
        .map(|(id, epoch, worker)| {
            transition_event(
                *id,
                u64::try_from(*epoch).unwrap_or(0),
                worker.clone(),
                WorkerAttemptEventKind::TimedOut,
                Some("timeout_ms exceeded".into()),
            )
        })
        .collect();
    insert_attempt_events(&mut tx, &events).await?;
    tx.commit().await?;
    Ok(rows.len() as u64)
}

pub(super) async fn list_expired_leases(
    store: &PostgresStorage,
    default_lease: Duration,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(concat!(
        "SELECT ",
        worker_task_columns!(),
        r" FROM worker_tasks
           WHERE state = 'claimed'
             AND (COALESCE(heartbeat_at, claimed_at) IS NULL
                  OR COALESCE(heartbeat_at, claimed_at)
                     < NOW() - make_interval(secs => COALESCE(lease_secs::double precision, $1::double precision)))
           ORDER BY COALESCE(heartbeat_at, claimed_at) ASC NULLS FIRST
           LIMIT $2"
    ))
    .bind(default_lease.as_secs_f64())
    .bind(i64::from(limit).min(REAPER_BATCH_SIZE))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(WorkerTaskRow::into_task).collect()
}

pub(super) async fn list_timed_out(
    store: &PostgresStorage,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(concat!(
        "SELECT ",
        worker_task_columns!(),
        r" FROM worker_tasks
           WHERE state IN ('pending', 'claimed')
             AND timeout_ms IS NOT NULL
             AND created_at + make_interval(secs => timeout_ms::double precision / 1000.0) < NOW()
           ORDER BY created_at ASC
           LIMIT $1"
    ))
    .bind(i64::from(limit).min(REAPER_BATCH_SIZE))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(WorkerTaskRow::into_task).collect()
}

/// Retry rows still awaiting dispatch, created before `created_before`.
pub(super) async fn list_stranded_dispatches(
    store: &PostgresStorage,
    created_before: chrono::DateTime<chrono::Utc>,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(concat!(
        "SELECT ",
        worker_task_columns!(),
        r" FROM worker_tasks
           WHERE state = 'pending' AND awaiting_dispatch AND created_at < $1
           ORDER BY created_at ASC
           LIMIT $2"
    ))
    .bind(created_before)
    .bind(i64::from(limit).min(REAPER_BATCH_SIZE))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(WorkerTaskRow::into_task).collect()
}

/// Bind a stranded retry row and make it claimable (fenced, idempotent).
pub(super) async fn finalize_stranded_dispatch(
    store: &PostgresStorage,
    task_id: Uuid,
    attempt: u16,
    effect: Option<&crate::worker_invocation::WorkerDispatchEffect>,
    continuity_epoch: Option<u64>,
) -> Result<bool, StorageError> {
    let effect_id = effect.map(|proof| proof.effect_id);
    let attempt = i16::try_from(attempt)
        .map_err(|_| StorageError::Constraint("worker attempt out of range".into()))?;
    let instance:Option<Uuid>=sqlx::query_scalar("SELECT instance_id FROM worker_tasks WHERE id=$1 AND state='pending' AND awaiting_dispatch")
        .bind(task_id).fetch_optional(&store.pool).await?;
    let Some(instance) = instance else {
        return Ok(false);
    };
    let instance_id = orch8_types::ids::InstanceId::from_uuid(instance);
    let mut tx = store.pool.begin().await?;
    let result=async {
        if !super::worker_invocations::lock_source_on(&mut tx,instance_id).await? {return Ok(false)}
        let parent:Option<Uuid>=sqlx::query_scalar("SELECT id FROM task_instances WHERE id=$1 AND state='waiting' FOR UPDATE")
            .bind(instance).fetch_optional(&mut *tx).await?;
        if parent.is_none() {return Ok(false)}
        let held:Option<Option<Uuid>>=sqlx::query_scalar("SELECT effect_id FROM worker_tasks WHERE id=$1 AND instance_id=$2 AND attempt=$3 AND state='pending' AND awaiting_dispatch FOR UPDATE")
            .bind(task_id).bind(instance).bind(attempt).fetch_optional(&mut *tx).await?;
        let Some(existing)=held else {return Ok(false)};
        if existing.is_some() && existing != effect_id.map(orch8_types::continuity::EffectId::into_uuid) {return Ok(false)}
        let Some(source)=super::worker_invocations::recovery_source_on(&mut tx,task_id,effect,continuity_epoch).await? else {return Ok(false)};
        let epoch=source.continuity_epoch.map(i64::try_from).transpose().map_err(|_|StorageError::Constraint("worker epoch out of range".into()))?;
        let affected=sqlx::query("UPDATE worker_tasks SET effect_id=COALESCE(effect_id,$2),continuity_epoch=COALESCE(continuity_epoch,$3),awaiting_dispatch=FALSE WHERE id=$1 AND state='pending' AND awaiting_dispatch")
            .bind(task_id).bind(effect_id.map(orch8_types::continuity::EffectId::into_uuid)).bind(epoch).execute(&mut *tx).await?.rows_affected();
        Ok(affected==1)
    }.await;
    super::finish_transaction(tx, result).await
}

/// Apply one fenced worker-task resolution atomically. See
/// `WorkerStore::resolve_worker_task`.
#[allow(clippy::too_many_lines)] // one transaction: fence, action, evidence
pub(super) async fn resolve(
    store: &PostgresStorage,
    resolution: &orch8_types::worker::WorkerTaskResolution,
    evidence: &crate::worker_resolution::WorkerResolutionEvidence,
) -> Result<bool, StorageError> {
    use orch8_types::worker::WorkerTaskResolutionAction as Action;
    evidence.validate(resolution)?;

    let expected_epoch = i64::try_from(resolution.expected_claim_epoch)
        .map_err(|_| StorageError::Constraint("worker claim epoch out of range".into()))?;
    let mut tx = store.pool.begin().await?;
    let result = async {
    let locked=super::worker_outcome::lock_on(&mut tx,resolution.task_id).await?;
    if locked.is_none_or(|(instance,_)|instance!=resolution.instance_id) {return Ok(false);}

    let parent: Option<Uuid> = sqlx::query_scalar("SELECT ti.id FROM task_instances ti WHERE ti.id=$1 AND EXISTS(SELECT 1 FROM worker_tasks wt WHERE wt.id=$2 AND wt.instance_id=ti.id AND wt.state=$3 AND wt.claim_epoch=$4 AND ($5::text IS NULL OR wt.worker_id=$5)) FOR UPDATE OF ti")
        .bind(resolution.instance_id.into_uuid()).bind(resolution.task_id)
        .bind(resolution.expected_state.to_string()).bind(expected_epoch)
        .bind(resolution.expected_worker_id.as_deref()).fetch_optional(&mut *tx).await?;
    if parent.is_none() {
        return Ok(false);
    }
    // Parent-before-worker ordering matches enqueue. Lock the dispatch before
    // validating its optional node so concurrent resolution cannot retarget it.
    let block: Option<String> = sqlx::query_scalar("SELECT block_id FROM worker_tasks WHERE id=$1 AND instance_id=$2 AND state=$3 AND claim_epoch=$4 AND ($5::text IS NULL OR worker_id=$5) FOR UPDATE")
        .bind(resolution.task_id).bind(resolution.instance_id.into_uuid())
        .bind(resolution.expected_state.to_string()).bind(expected_epoch)
        .bind(resolution.expected_worker_id.as_deref()).fetch_optional(&mut *tx).await?;
    let Some(block) = block else {
        return Ok(false);
    };
    let node_id = match &resolution.action {
        Action::Retry {
            retry_task,
            node_id,
            ..
        } => {
            if retry_task.instance_id != resolution.instance_id
                || retry_task.block_id.as_str() != block
            {
                return Ok(false);
            }
            *node_id
        }
        Action::FailNode { node_id, .. } => *node_id,
        _ => None,
    };
    if let Some(node_id) = node_id {
        let valid: Option<Uuid> = sqlx::query_scalar("SELECT id FROM execution_tree WHERE id=$1 AND instance_id=$2 AND block_id=$3 FOR UPDATE")
            .bind(node_id.into_uuid()).bind(resolution.instance_id.into_uuid()).bind(&block)
            .fetch_optional(&mut *tx).await?;
        if valid.is_none() {
            return Ok(false);
        }
    }
    let source = if matches!(&resolution.action, Action::FailTaskOnly) {
        None
    } else {
        let Some(source) =
            super::worker_invocations::source_on(&mut tx, resolution.task_id).await?
        else {
            return Ok(false);
        };
        if matches!(
            &resolution.action,
            Action::Retry { .. } | Action::FailNode { .. }
        ) && !source.accepts_node(node_id)
        {
            return Ok(false);
        }
        if let Action::Retry {
            retry_task,
            retry_output,
            ..
        } = &resolution.action
        {
            source.validate_retry_task(retry_task)?;
            source.validate_retry_output(retry_output.as_deref())?;
        }
        Some(source)
    };
    let report_source=super::worker_resolution::capture_on(&mut tx,resolution.task_id).await?;
    let fence = " WHERE id = $1 AND state = $2 AND claim_epoch = $3 \
                  AND ($4::text IS NULL OR worker_id = $4)";
    let affected = match &resolution.action {
        Action::Requeue => sqlx::query(AssertSqlSafe(format!(
            "UPDATE worker_tasks SET state = 'pending', worker_id = NULL, claimed_at = NULL, \
                 heartbeat_at = NULL, lease_secs = NULL, claimed_runtime_kind = NULL{fence}"
        )))
        .bind(resolution.task_id)
        .bind(resolution.expected_state.to_string())
        .bind(expected_epoch)
        .bind(resolution.expected_worker_id.as_deref())
        .execute(&mut *tx)
        .await?
        .rows_affected(),
        Action::Retry { .. } => {
            sqlx::query(AssertSqlSafe(format!("DELETE FROM worker_tasks{fence}")))
                .bind(resolution.task_id)
                .bind(resolution.expected_state.to_string())
                .bind(expected_epoch)
                .bind(resolution.expected_worker_id.as_deref())
                .execute(&mut *tx)
                .await?
                .rows_affected()
        }
        Action::FailNode { .. } | Action::FailInstance | Action::FailTaskOnly => {
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE worker_tasks SET state = 'failed', error_message = $5, \
                 error_retryable = $6, completed_at = NOW(){fence}"
            )))
            .bind(resolution.task_id)
            .bind(resolution.expected_state.to_string())
            .bind(expected_epoch)
            .bind(resolution.expected_worker_id.as_deref())
            .bind(crate::worker_resolution::diagnostic_reason(&resolution.reason).as_ref())
            .bind(resolution.retryable)
            .execute(&mut *tx)
            .await?
            .rows_affected()
        }
    };
    if affected != 1 {
        return Ok(false);
    }
    super::worker_resolution::record_on(&mut tx,&report_source,resolution,evidence).await?;
    apply_resolution_action_on(&mut tx,resolution.instance_id,&resolution.action,source.as_ref()).await?;
    insert_attempt_events(
        &mut tx,
        &[transition_event(
            resolution.task_id,
            resolution.expected_claim_epoch,
            report_source.worker_id.clone(),
            resolution.event,
            Some(crate::worker_resolution::diagnostic_reason(&resolution.reason).into_owned()),
        )],
    )
    .await?;
    Ok(true)
    }.await;
    super::finish_transaction(tx, result).await
}

/// Wake a non-terminal, non-paused instance so the evaluator observes a
/// resolved worker step. A paused instance stays paused (its resume path
/// re-schedules it); a terminal one is never resurrected.
async fn schedule_live_instance(
    tx: &mut sqlx::PgConnection,
    instance_id: Uuid,
    fire_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE task_instances SET state = 'scheduled', next_fire_at = $2, updated_at = NOW() \
         WHERE id = $1 AND state NOT IN ('completed', 'failed', 'cancelled', 'paused')",
    )
    .bind(instance_id)
    .bind(fire_at)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

pub(super) async fn retry(
    store: &PostgresStorage,
    old_task_id: Uuid,
    new_task: &WorkerTask,
    node_id: Option<orch8_types::ids::ExecutionNodeId>,
    instance_id: orch8_types::ids::InstanceId,
    fire_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;
    let result = async {
    if !super::worker_invocations::lock_source_on(&mut tx, instance_id).await? {
        return Err(StorageError::Conflict("retry parent is missing".into()));
    }

    // Match enqueue/resolution ordering: native parent before worker row/index.
    sqlx::query("SELECT id FROM task_instances WHERE id=$1 FOR UPDATE")
        .bind(instance_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await?;

    let block: Option<String> = sqlx::query_scalar(
        "SELECT block_id FROM worker_tasks WHERE id=$1 AND instance_id=$2 FOR UPDATE",
    )
    .bind(old_task_id)
    .bind(instance_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await?;
    if new_task.instance_id != instance_id || block.as_deref() != Some(new_task.block_id.as_str()) {
        return Err(StorageError::Conflict(
            "retry targets do not match the original worker dispatch".into(),
        ));
    }
    if let Some(nid) = node_id {
        let valid: Option<Uuid> = sqlx::query_scalar("SELECT id FROM execution_tree WHERE id=$1 AND instance_id=$2 AND block_id=$3 FOR UPDATE")
            .bind(nid.into_uuid()).bind(instance_id.into_uuid()).bind(new_task.block_id.as_str())
            .fetch_optional(&mut *tx).await?;
        if valid.is_none() {
            return Err(StorageError::Conflict(
                "retry node does not match the original worker dispatch".into(),
            ));
        }
    }

    let Some(source) = super::worker_invocations::source_on(&mut tx, old_task_id).await? else {
        return Err(StorageError::Conflict(
            "retry source invocation changed or is unknown".into(),
        ));
    };
    if !source.accepts_node(node_id) {
        return Err(StorageError::Conflict(
            "retry node differs from original invocation".into(),
        ));
    }
    source.validate_retry_task(new_task)?;

    sqlx::query("DELETE FROM worker_tasks WHERE id = $1")
        .bind(old_task_id)
        .execute(&mut *tx)
        .await?;

    super::worker_invocations::reserve_retry_on(&mut tx, &source, new_task, node_id).await?;

    sqlx::query("UPDATE task_instances SET state = 'scheduled', next_fire_at = $2, updated_at = NOW() WHERE id = $1")
        .bind(instance_id.into_uuid())
        .bind(fire_at)
        .execute(&mut *tx)
        .await?;
    Ok(())
    }.await;
    super::finish_transaction(tx, result).await
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
    let mut tx = store.pool.begin().await?;
    let rows: Vec<(Uuid, i64, Option<String>)> = sqlx::query_as("DELETE FROM worker_tasks WHERE instance_id = $1 AND block_id = $2 RETURNING id,claim_epoch,worker_id")
        .bind(instance_id)
        .bind(block_id)
        .fetch_all(&mut *tx)
        .await?;
    let events: Vec<_> = rows
        .iter()
        .map(|(id, epoch, worker)| {
            transition_event(
                *id,
                u64::try_from(*epoch).unwrap_or(0),
                worker.clone(),
                WorkerAttemptEventKind::Cancelled,
                Some("workflow branch cancelled or reset".into()),
            )
        })
        .collect();
    insert_attempt_events(&mut tx, &events).await?;
    tx.commit().await?;
    Ok(rows.len() as u64)
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
    let mut tx = store.pool.begin().await?;
    let changed = cancel_for_blocks_on(&mut tx, instance_id, block_ids).await?;
    tx.commit().await?;
    Ok(changed)
}

pub(super) async fn cancel_for_blocks_on(
    conn: &mut sqlx::PgConnection,
    instance_id: Uuid,
    block_ids: &[String],
) -> Result<u64, StorageError> {
    if block_ids.is_empty() {
        return Ok(0);
    }
    let rows: Vec<(Uuid, i64, Option<String>)> =
        sqlx::query_as("DELETE FROM worker_tasks WHERE instance_id = $1 AND block_id = ANY($2) RETURNING id,claim_epoch,worker_id")
            .bind(instance_id)
            .bind(block_ids)
            .fetch_all(&mut *conn)
            .await?;
    let events: Vec<_> = rows
        .iter()
        .map(|(id, epoch, worker)| {
            transition_event(
                *id,
                u64::try_from(*epoch).unwrap_or(0),
                worker.clone(),
                WorkerAttemptEventKind::Cancelled,
                Some("workflow branches cancelled or reset".into()),
            )
        })
        .collect();
    insert_attempt_events(&mut *conn, &events).await?;
    Ok(rows.len() as u64)
}

pub(super) async fn list(
    store: &PostgresStorage,
    filter: &orch8_types::worker_filter::WorkerTaskFilter,
    pagination: &orch8_types::filter::Pagination,
) -> Result<Vec<WorkerTask>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(concat!(
        "SELECT ",
        worker_task_columns!(),
        " FROM worker_tasks WHERE 1=1"
    ));
    apply_worker_task_filter(&mut qb, filter);
    if pagination.sort_ascending {
        qb.push(" ORDER BY created_at ASC");
    } else {
        qb.push(" ORDER BY created_at DESC");
    }
    qb.push(" LIMIT ")
        .push_bind(i64::from(pagination.limit.min(1000)));
    qb.push(" OFFSET ")
        .push_bind(i64::try_from(pagination.offset).unwrap_or(i64::MAX));

    let rows = qb
        .build_query_as::<WorkerTaskRow>()
        .fetch_all(&store.pool)
        .await?;

    rows.into_iter().map(WorkerTaskRow::into_task).collect()
}

pub(super) async fn stats(
    store: &PostgresStorage,
    tenant_id: Option<&orch8_types::ids::TenantId>,
    window_secs: Option<u32>,
) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
    // Count by state + handler_name
    let mut qb = sqlx::QueryBuilder::new(
        "SELECT state, handler_name, COUNT(*) as cnt FROM worker_tasks WHERE TRUE",
    );
    if let Some(tid) = tenant_id {
        qb.push(" AND instance_id IN (SELECT id FROM task_instances WHERE tenant_id =")
            .push_bind(tid.as_str())
            .push(")");
    }
    if let Some(secs) = window_secs {
        // Terminal rows only count inside the window; live gauges always count.
        qb.push(" AND (state NOT IN ('completed', 'failed') OR completed_at >= NOW() - make_interval(secs => ")
            .push_bind(f64::from(secs))
            .push("))");
    }
    qb.push(" GROUP BY state, handler_name");
    let counts = qb
        .build_query_as::<(String, String, i64)>()
        .fetch_all(&store.pool)
        .await?;

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
    let mut wqb = sqlx::QueryBuilder::new(
        "SELECT DISTINCT worker_id FROM worker_tasks WHERE state = 'claimed' AND worker_id IS NOT NULL",
    );
    if let Some(tid) = tenant_id {
        wqb.push(" AND instance_id IN (SELECT id FROM task_instances WHERE tenant_id =")
            .push_bind(tid.as_str())
            .push(")");
    }
    let workers = wqb
        .build_query_as::<(String,)>()
        .fetch_all(&store.pool)
        .await?;

    let active_workers = workers.into_iter().map(|(w,)| w).collect();

    Ok(orch8_types::worker_filter::WorkerTaskStats {
        by_state,
        by_handler,
        active_workers,
    })
}

/// Apply `WorkerTaskFilter` conditions to a query builder.
fn apply_worker_task_filter(
    qb: &mut sqlx::QueryBuilder<sqlx::Postgres>,
    filter: &orch8_types::worker_filter::WorkerTaskFilter,
) {
    if let Some(ref tid) = filter.tenant_id {
        qb.push(" AND instance_id IN (SELECT id FROM task_instances WHERE tenant_id =")
            .push_bind(tid.as_str())
            .push(")");
    }
    if let Some(ref states) = filter.states
        && !states.is_empty()
    {
        let state_strings: Vec<String> = states.iter().map(ToString::to_string).collect();
        qb.push(" AND state = ANY(")
            .push_bind(state_strings)
            .push(")");
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
    if let Some(instance_id) = filter.instance_id {
        qb.push(" AND instance_id = ")
            .push_bind(instance_id.into_uuid());
    }
}

// === Worker Registry ===

pub(super) async fn upsert_registration(
    store: &PostgresStorage,
    reg: &orch8_types::worker::WorkerRegistration,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO worker_registrations
            (worker_id, handler_name, queue_name, version, tenant_id, last_seen_at)
          VALUES ($1,$2,$3,$4,$5,$6)
          ON CONFLICT (worker_id, handler_name) DO UPDATE
          SET queue_name = EXCLUDED.queue_name,
              version = EXCLUDED.version,
              tenant_id = EXCLUDED.tenant_id,
              last_seen_at = EXCLUDED.last_seen_at
          WHERE COALESCE(worker_registrations.tenant_id, '') =
                COALESCE(EXCLUDED.tenant_id, '')",
    )
    .bind(&reg.worker_id)
    .bind(&reg.handler_name)
    .bind(&reg.queue_name)
    .bind(&reg.version)
    .bind(&reg.tenant_id)
    .bind(reg.last_seen_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_registrations(
    store: &PostgresStorage,
    seen_within_secs: Option<i64>,
) -> Result<Vec<orch8_types::worker::WorkerRegistration>, StorageError> {
    type Row = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    );
    let rows: Vec<Row> = if let Some(secs) = seen_within_secs {
        sqlx::query_as(
            r"SELECT worker_id, handler_name, queue_name, version, tenant_id, last_seen_at
              FROM worker_registrations
              WHERE last_seen_at >= NOW() - ($1::bigint * INTERVAL '1 second')
              ORDER BY last_seen_at DESC",
        )
        .bind(secs)
        .fetch_all(&store.pool)
        .await?
    } else {
        sqlx::query_as(
            r"SELECT worker_id, handler_name, queue_name, version, tenant_id, last_seen_at
              FROM worker_registrations
              ORDER BY last_seen_at DESC",
        )
        .fetch_all(&store.pool)
        .await?
    };
    Ok(rows
        .into_iter()
        .map(
            |(worker_id, handler_name, queue_name, version, tenant_id, last_seen_at)| {
                orch8_types::worker::WorkerRegistration {
                    worker_id,
                    handler_name,
                    queue_name,
                    version,
                    tenant_id,
                    last_seen_at,
                }
            },
        )
        .collect())
}

pub(super) async fn claimed_counts_by_worker(
    store: &PostgresStorage,
    tenant_id: Option<&orch8_types::ids::TenantId>,
) -> Result<Vec<(String, i64)>, StorageError> {
    let rows: Vec<(String, i64)> = if let Some(tenant_id) = tenant_id {
        sqlx::query_as(
            r"SELECT wt.worker_id, COUNT(*)
              FROM worker_tasks wt
              JOIN task_instances ti ON ti.id = wt.instance_id
              WHERE wt.state = 'claimed' AND wt.worker_id IS NOT NULL
                AND ti.tenant_id = $1
              GROUP BY wt.worker_id",
        )
        .bind(tenant_id.as_str())
        .fetch_all(&store.pool)
        .await?
    } else {
        sqlx::query_as(
            r"SELECT worker_id, COUNT(*) FROM worker_tasks
              WHERE state = 'claimed' AND worker_id IS NOT NULL
              GROUP BY worker_id",
        )
        .fetch_all(&store.pool)
        .await?
    };
    Ok(rows)
}

pub(super) async fn apply_resolution_action_on(
    conn: &mut sqlx::PgConnection,
    instance: orch8_types::ids::InstanceId,
    action: &orch8_types::worker::WorkerTaskResolutionAction,
    source: Option<&crate::worker_invocation::WorkerInvocation>,
) -> Result<(), StorageError> {
    use orch8_types::worker::WorkerTaskResolutionAction as Action;
    let instance_id = instance.into_uuid();
    match action {
        Action::Requeue | Action::FailTaskOnly => {}
        Action::Retry {
            retry_task,
            retry_output,
            node_id,
            fire_at,
        } => {
            let source =
                source.ok_or_else(|| StorageError::Constraint("missing retry source".into()))?;
            super::worker_invocations::reserve_retry_on(&mut *conn, source, retry_task, *node_id)
                .await?;
            if let Some(output) = retry_output.as_deref() {
                super::outputs::save_on(&mut *conn, output).await?;
            }
            schedule_live_instance(&mut *conn, instance_id, *fire_at).await?;
        }
        Action::FailNode { node_id, fire_at } => {
            if let Some(node_id) = node_id {
                sqlx::query(
                "UPDATE execution_tree SET state = 'failed', completed_at = COALESCE(completed_at, NOW()) \
                 WHERE id = $1 AND state IN ('running', 'waiting')",
            )
            .bind(node_id.into_uuid())
            .execute(&mut *conn)
            .await?;
            }
            schedule_live_instance(&mut *conn, instance_id, *fire_at).await?;
        }
        Action::FailInstance => {
            sqlx::query(
                "UPDATE task_instances SET state = 'failed', next_fire_at = NULL, updated_at = NOW() \
                 WHERE id = $1 AND state NOT IN ('completed', 'failed', 'cancelled')",
            )
            .bind(instance_id)
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(())
}
