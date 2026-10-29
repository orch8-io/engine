use chrono::{DateTime, Utc};
use sqlx::Row;
use std::time::Duration;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::instance::TaskInstance;
use orch8_types::worker::{WorkerAttemptEventKind, WorkerTask};

use super::SqliteStorage;
use super::helpers::{row_to_instance, row_to_worker_task, ts};

// === Idempotency ===

pub(super) async fn find_by_idempotency_key(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    idempotency_key: &str,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query(
        "SELECT * FROM task_instances WHERE tenant_id=?1 AND idempotency_key=?2 LIMIT 1",
    )
    .bind(tenant_id.as_str())
    .bind(idempotency_key)
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_instance).transpose()
}

// === Concurrency ===

pub(super) async fn count_running_by_concurrency_key(
    storage: &SqliteStorage,
    tenant_id: &str,
    concurrency_key: &str,
) -> Result<i64, StorageError> {
    let row = sqlx::query(
        "SELECT COUNT(*) as cnt FROM task_instances \
         WHERE tenant_id=?1 AND concurrency_key=?2 AND state='running'",
    )
    .bind(tenant_id)
    .bind(concurrency_key)
    .fetch_one(&storage.pool)
    .await?;
    Ok(row.get::<i64, _>("cnt"))
}

pub(super) async fn count_running_by_concurrency_keys(
    storage: &SqliteStorage,
    keys: &[(&str, &str)],
) -> Result<std::collections::HashMap<(String, String), i64>, StorageError> {
    if keys.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let mut map = std::collections::HashMap::with_capacity(keys.len());
    // 2 binds per pair; stay well under SQLite's bind-parameter limit.
    for chunk in keys.chunks(400) {
        let mut qb = sqlx::QueryBuilder::new(
            "SELECT tenant_id, concurrency_key, COUNT(*) as cnt FROM task_instances \
             WHERE state='running' AND (",
        );
        for (i, (tenant, key)) in chunk.iter().enumerate() {
            if i > 0 {
                qb.push(" OR ");
            }
            qb.push("(tenant_id=");
            qb.push_bind(*tenant);
            qb.push(" AND concurrency_key=");
            qb.push_bind(*key);
            qb.push(")");
        }
        qb.push(") GROUP BY tenant_id, concurrency_key");
        for row in qb.build().fetch_all(&storage.pool).await? {
            let tenant: String = row.get("tenant_id");
            let key: String = row.get("concurrency_key");
            map.insert((tenant, key), row.get::<i64, _>("cnt"));
        }
    }
    Ok(map)
}

pub(super) async fn concurrency_position(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    concurrency_key: &str,
) -> Result<i64, StorageError> {
    // Order by `(created_at, id)`. IDs are UUIDv7 so the 48-bit timestamp
    // prefix gives us FIFO-on-`id` at ms granularity — within a single ms
    // v7 falls back to 74 random bits, so `created_at` leads and `id`
    // tiebreaks to keep position assignment deterministic for same-ms
    // arrivals. Matches the Postgres backend.
    //
    // Uses a COUNT query instead of fetching all rows and linearly
    // searching, avoiding O(n) memory and CPU per check on busy keys.
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM task_instances
         WHERE concurrency_key = ?1 AND state = 'running'
           AND tenant_id = (SELECT tenant_id FROM task_instances WHERE id = ?2)
           AND (created_at < (SELECT created_at FROM task_instances WHERE id = ?2)
                OR (created_at = (SELECT created_at FROM task_instances WHERE id = ?2)
                    AND id <= ?2))",
    )
    .bind(concurrency_key)
    .bind(instance_id.into_uuid().to_string())
    .fetch_one(&storage.pool)
    .await?;
    Ok(row.0)
}

// === Recovery ===

pub(super) async fn recover_stale_instances(
    storage: &SqliteStorage,
    stale_threshold: Duration,
) -> Result<u64, StorageError> {
    let cutoff = chrono::Utc::now()
        - chrono::Duration::from_std(stale_threshold)
            .unwrap_or_else(|_| chrono::Duration::seconds(300));
    // `running` only — never `waiting`: parked waiters (human-review gates,
    // wait-for-event) never heartbeat, so every reaper pass used to recycle
    // them (claim → re-park, one audit row per park). Their liveness
    // contract is the deadline/timeout sweep, not the lease. Parity with the
    // Postgres impl.
    //
    // Reset next_fire_at as well (parity with the Postgres impl): a stale
    // instance may carry a far-future next_fire_at from before it wedged, and
    // claim_due would otherwise not pick the recovered instance up until that
    // time naturally arrives.
    let result = sqlx::query(
        "UPDATE task_instances SET state='scheduled', next_fire_at=?1, updated_at=?1, claimed_at=NULL WHERE state='running' AND updated_at < ?2",
    )
    .bind(ts(chrono::Utc::now()))
    .bind(ts(cutoff))
    .execute(&storage.pool)
    .await
    ?;
    Ok(result.rows_affected())
}

pub(super) async fn heartbeat_instance(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    claimed_at: DateTime<Utc>,
) -> Result<crate::InstanceLease, StorageError> {
    let id = instance_id.into_uuid().to_string();
    let token = ts(claimed_at);
    let renewed = sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE task_instances SET updated_at=?1 WHERE id=?2 AND state IN ('running', 'waiting') AND claimed_at=?3 AND {scope}", scope = super::mobile_execution::ALLOWED)))
    .bind(ts(chrono::Utc::now()))
    .bind(&id)
    .bind(&token)
    .execute(&storage.pool)
    .await?;
    if renewed.rows_affected() > 0 {
        return Ok(crate::InstanceLease::Held);
    }
    // Not renewed: either the instance legitimately left Running/Waiting
    // under this claim (still ours), or the token was cleared/replaced.
    let owned: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT claimed_at = ?2 FROM task_instances WHERE id = ?1 AND {scope}",
        scope = super::mobile_execution::ALLOWED
    )))
    .bind(&id)
    .bind(&token)
    .fetch_optional(&storage.pool)
    .await?
    .flatten();
    Ok(if owned == Some(1) {
        crate::InstanceLease::Held
    } else {
        crate::InstanceLease::Lost
    })
}

// === Sub-Sequences ===

pub(super) async fn get_child_instances(
    storage: &SqliteStorage,
    parent_instance_id: InstanceId,
) -> Result<Vec<TaskInstance>, StorageError> {
    let rows = sqlx::query(
        "SELECT * FROM task_instances WHERE parent_instance_id=?1 ORDER BY created_at, id",
    )
    .bind(parent_instance_id.into_uuid().to_string())
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_instance).collect()
}

pub(super) async fn get_child_for_activation(
    storage: &SqliteStorage,
    parent: InstanceId,
    block: &str,
    activation: &str,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query(crate::child_lookup::SQLITE)
        .bind(parent.into_uuid().to_string())
        .bind(block)
        .bind(activation)
        .fetch_optional(&storage.pool)
        .await?;
    row.as_ref().map(row_to_instance).transpose()
}

pub(super) async fn get_child_for_adoption(
    storage: &SqliteStorage,
    parent: InstanceId,
    block: &str,
    suffix: &str,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query(crate::child_adoption::SQLITE)
        .bind(parent.into_uuid().to_string())
        .bind(block)
        .bind(suffix)
        .fetch_optional(&storage.pool)
        .await?;
    row.as_ref().map(row_to_instance).transpose()
}

pub(super) async fn list_orphaned_children(
    storage: &SqliteStorage,
    limit: u32,
) -> Result<Vec<InstanceId>, StorageError> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT c.id FROM task_instances c \
         WHERE c.state IN ('scheduled', 'running', 'waiting', 'paused') \
           AND c.parent_instance_id IS NOT NULL \
           AND EXISTS ( \
               SELECT 1 FROM task_instances p \
               WHERE p.id = c.parent_instance_id \
                 AND p.state IN ('failed', 'cancelled')) \
         LIMIT ?1",
    )
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;
    rows.iter()
        .map(|id| {
            uuid::Uuid::parse_str(id)
                .map(InstanceId::from_uuid)
                .map_err(|e| StorageError::Query(format!("invalid instance id {id}: {e}")))
        })
        .collect()
}

// === Task Queue Routing ===

pub(super) async fn claim_worker_tasks_from_queue(
    storage: &SqliteStorage,
    queue_name: &str,
    handler_name: &str,
    worker_id: &str,
    limit: u32,
    principal: Option<&crate::worker_claim::WorkerClaimPrincipal>,
) -> Result<Vec<WorkerTask>, StorageError> {
    // `BEGIN IMMEDIATE` (not sqlx's default DEFERRED) — the SELECT and UPDATE
    // form a read-then-write claim; under DEFERRED two concurrent pollers can
    // both SELECT the same pending rows before either UPDATE runs and claim
    // the same task twice. Same convention as `workers::claim`.
    let mut conn = super::helpers::begin_immediate(&storage.pool).await?;
    let result = claim_from_queue_inner(
        &mut conn,
        queue_name,
        handler_name,
        worker_id,
        None,
        limit,
        principal,
    )
    .await;
    super::finish_transaction(conn, result).await
}

/// Shared SELECT + UPDATE claim body for the queue-routing claims. When
/// `tenant_id` is `Some`, the SELECT joins `task_instances` to scope the
/// claim to that tenant.
async fn claim_from_queue_inner(
    conn: &mut sqlx::SqliteConnection,
    queue_name: &str,
    handler_name: &str,
    worker_id: &str,
    tenant_id: Option<&TenantId>,
    limit: u32,
    principal: Option<&crate::worker_claim::WorkerClaimPrincipal>,
) -> Result<Vec<WorkerTask>, StorageError> {
    let rows = if let Some(tenant) = tenant_id {
        sqlx::query(
            "SELECT wt.* FROM worker_tasks wt
             JOIN task_instances ti ON ti.id = wt.instance_id
             WHERE wt.queue_name=?1 AND wt.handler_name=?2 AND wt.state='pending' AND wt.claim_epoch>=0 AND wt.claim_epoch<9223372036854775807
               AND wt.awaiting_dispatch=0 AND (wt.requirements='{}' OR (json_remove(wt.requirements,'$.prefer')='{}' AND (json_extract(wt.requirements,'$.prefer.worker_id')=?5 OR json_extract(wt.requirements,'$.prefer.until_ms') <= CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER))))
               AND ti.tenant_id=?4
               AND ti.state NOT IN ('completed','failed','cancelled')
               AND NOT EXISTS (SELECT 1 FROM continuity_executions ce WHERE ce.state='compensating' AND ce.tenant_id=(SELECT ti_owner.tenant_id FROM task_instances ti_owner WHERE ti_owner.id=wt.instance_id) AND json_extract(ce.record,'$.current_instance_id')=wt.instance_id)
             ORDER BY wt.created_at ASC
             LIMIT ?3",
        )
        .bind(queue_name)
        .bind(handler_name)
        .bind(limit as i64)
        .bind(tenant.as_str())
        .bind(worker_id)
        .fetch_all(&mut *conn)
        .await?
    } else {
        sqlx::query("SELECT * FROM worker_tasks WHERE queue_name=?1 AND handler_name=?2 AND state='pending' AND claim_epoch>=0 AND claim_epoch<9223372036854775807 AND awaiting_dispatch=0 AND (requirements='{}' OR (json_remove(requirements,'$.prefer')='{}' AND (json_extract(requirements,'$.prefer.worker_id')=?4 OR json_extract(requirements,'$.prefer.until_ms') <= CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)))) AND NOT EXISTS (SELECT 1 FROM task_instances ti WHERE ti.id=worker_tasks.instance_id AND ti.state IN ('completed','failed','cancelled')) AND NOT EXISTS (SELECT 1 FROM continuity_executions ce WHERE ce.state='compensating' AND ce.tenant_id=(SELECT ti_owner.tenant_id FROM task_instances ti_owner WHERE ti_owner.id=worker_tasks.instance_id) AND json_extract(ce.record,'$.current_instance_id')=worker_tasks.instance_id) ORDER BY created_at ASC LIMIT ?3")
            .bind(queue_name)
            .bind(handler_name)
            .bind(limit as i64)
            .bind(worker_id)
            .fetch_all(&mut *conn)
            .await?
    };
    let mut tasks: Vec<WorkerTask> = rows
        .iter()
        .map(row_to_worker_task)
        .collect::<Result<Vec<_>, _>>()?;
    let now_dt = chrono::Utc::now();
    let now = ts(now_dt);
    for t in &mut tasks {
        t.state = orch8_types::worker::WorkerTaskState::Claimed;
        t.worker_id = Some(worker_id.to_string());
        t.claimed_at = Some(now_dt);
        t.heartbeat_at = Some(now_dt);
        t.claim_epoch = crate::worker_claim::next_epoch(t.claim_epoch)?;
        t.lease_secs = None;
        t.claimed_runtime_kind = None;
    }
    if !tasks.is_empty() {
        let mut qb = sqlx::QueryBuilder::new("UPDATE worker_tasks SET state='claimed', worker_id=");
        qb.push_bind(worker_id);
        qb.push(", claimed_at=");
        qb.push_bind(&now);
        qb.push(", heartbeat_at=");
        qb.push_bind(&now);
        qb.push(", claim_epoch=claim_epoch+1, lease_secs=NULL, claimed_runtime_kind=NULL");
        qb.push(" WHERE id IN (");
        let mut separated = qb.separated(",");
        for t in &tasks {
            separated.push_bind(t.id.to_string());
        }
        separated.push_unseparated(")");
        qb.build().execute(&mut *conn).await?;
        let events: Vec<_> = tasks
            .iter()
            .map(|task| {
                super::workers::transition_event(
                    task.id,
                    task.claim_epoch,
                    task.worker_id.clone(),
                    WorkerAttemptEventKind::Claimed,
                    None,
                )
            })
            .collect();
        super::worker_claim::record_on(conn, &tasks, principal).await?;
        super::workers::insert_attempt_events(conn, &events).await?;
    }
    Ok(tasks)
}

/// Tenant-scoped variant — SELECT and UPDATE share a single transaction so
/// the tenant predicate and claim happen atomically. See
/// `postgres::workers::claim_for_tenant` for the anti-pattern this
/// replaces.
pub(super) async fn claim_worker_tasks_from_queue_for_tenant(
    storage: &SqliteStorage,
    queue_name: &str,
    handler_name: &str,
    worker_id: &str,
    tenant_id: &TenantId,
    limit: u32,
    principal: Option<&crate::worker_claim::WorkerClaimPrincipal>,
) -> Result<Vec<WorkerTask>, StorageError> {
    // BEGIN IMMEDIATE for the same double-claim reason as
    // `claim_worker_tasks_from_queue` above.
    let mut conn = super::helpers::begin_immediate(&storage.pool).await?;
    let result = claim_from_queue_inner(
        &mut conn,
        queue_name,
        handler_name,
        worker_id,
        Some(tenant_id),
        limit,
        principal,
    )
    .await;
    super::finish_transaction(conn, result).await
}

// === Dynamic Step Injection ===

pub(super) async fn inject_blocks(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    blocks_json: &serde_json::Value,
) -> Result<(), StorageError> {
    // `inject_blocks` is append-only. Use the transactional positional path
    // with an out-of-range index so it reads the current value under the
    // SQLite write reservation and clamps the insertion point to the end.
    inject_blocks_at_position(storage, instance_id, blocks_json, Some(usize::MAX))
        .await
        .map(|_| ())
}

pub(super) async fn get_injected_blocks(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row = sqlx::query("SELECT blocks FROM injected_blocks WHERE instance_id=?1")
        .bind(instance_id.into_uuid().to_string())
        .fetch_optional(&storage.pool)
        .await?;
    Ok(row
        .map(|r| serde_json::from_str(r.get::<&str, _>("blocks")))
        .transpose()?)
}

pub(super) async fn inject_blocks_at_position(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    new_blocks_json: &serde_json::Value,
    position: Option<usize>,
) -> Result<serde_json::Value, StorageError> {
    // `BEGIN DEFERRED` (the sqlx default) allows two callers to read the same
    // snapshot before either attempts the write. One then fails with
    // SQLITE_BUSY, dropping an otherwise-valid injection. Acquire SQLite's
    // write reservation before the read, mirroring claim_due's locking
    // strategy and making the documented atomic merge reliable under load.
    let mut conn = super::helpers::begin_immediate(&storage.pool).await?;

    let result = async {
        let final_value = if let Some(pos) = position {
            let row = sqlx::query("SELECT blocks FROM injected_blocks WHERE instance_id=?1")
                .bind(instance_id.into_uuid().to_string())
                .fetch_optional(&mut *conn)
                .await?;
            let existing: Option<serde_json::Value> = row
                .map(|r| serde_json::from_str::<serde_json::Value>(r.get::<&str, _>("blocks")))
                .transpose()?;

            let mut arr = existing
                .and_then(|v| v.as_array().cloned())
                .unwrap_or_default();

            let new_arr = new_blocks_json.as_array().cloned().unwrap_or_default();
            let insert_at = pos.min(arr.len());
            for (i, block) in new_arr.into_iter().enumerate() {
                arr.insert(insert_at + i, block);
            }
            serde_json::Value::Array(arr)
        } else {
            new_blocks_json.clone()
        };

        sqlx::query("INSERT OR REPLACE INTO injected_blocks (instance_id, blocks) VALUES (?1, ?2)")
            .bind(instance_id.into_uuid().to_string())
            .bind(serde_json::to_string(&final_value)?)
            .execute(&mut *conn)
            .await?;

        Ok::<_, StorageError>(final_value)
    }
    .await;

    match result {
        Ok(final_value) => {
            conn.commit().await?;
            Ok(final_value)
        }
        Err(e) => {
            let _ = conn.rollback().await;
            Err(e)
        }
    }
}

// === Emit Event Dedupe ===

pub(super) async fn record_or_get_emit_dedupe(
    storage: &SqliteStorage,
    scope: &crate::DedupeScope,
    key: &str,
    candidate_child: InstanceId,
) -> Result<crate::EmitDedupeOutcome, StorageError> {
    let scope_kind = scope.kind();
    let scope_value = scope.value();
    let cand_str = candidate_child.to_string();

    let inserted = sqlx::query(
        "INSERT INTO emit_event_dedupe (scope_kind, scope_value, dedupe_key, child_instance_id)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(scope_kind, scope_value, dedupe_key) DO NOTHING",
    )
    .bind(scope_kind)
    .bind(&scope_value)
    .bind(key)
    .bind(&cand_str)
    .execute(&storage.pool)
    .await?
    .rows_affected();

    if inserted == 1 {
        return Ok(crate::EmitDedupeOutcome::Inserted);
    }

    let row: (String,) = sqlx::query_as(
        "SELECT child_instance_id FROM emit_event_dedupe
         WHERE scope_kind = ?1 AND scope_value = ?2 AND dedupe_key = ?3",
    )
    .bind(scope_kind)
    .bind(&scope_value)
    .bind(key)
    .fetch_one(&storage.pool)
    .await?;

    let existing = uuid::Uuid::parse_str(&row.0)
        .map_err(|e| StorageError::Query(format!("invalid uuid in dedupe row: {e}")))?;
    Ok(crate::EmitDedupeOutcome::AlreadyExists(
        InstanceId::from_uuid(existing),
    ))
}

/// Atomically record the dedupe row AND insert the child `TaskInstance` in a
/// single transaction. See `StorageBackend::create_instance_with_dedupe`.
///
/// If `(parent, key)` is already taken we return `AlreadyExists` without
/// touching `task_instances`. Otherwise both rows land in the same commit,
/// so a crash between the two inserts is impossible.
pub(super) async fn create_instance_with_dedupe(
    storage: &SqliteStorage,
    scope: &crate::DedupeScope,
    key: &str,
    instance: &TaskInstance,
) -> Result<crate::EmitDedupeOutcome, StorageError> {
    let scope_kind = scope.kind();
    let scope_value = scope.value();
    let cand_str = instance.id.into_uuid().to_string();

    // `BEGIN IMMEDIATE` acquires the write lock up-front. With sqlx's default
    // DEFERRED begin, a concurrent dedupe race upgrades mid-transaction and
    // can fail with SQLITE_BUSY_SNAPSHOT instead of cleanly serialising —
    // same convention as `claim_due` / `workers::claim`.
    let mut conn = super::helpers::begin_immediate(&storage.pool).await?;

    let result: Result<u64, StorageError> = async {
        // Insert the child instance FIRST so any FK on emit_event_dedupe is
        // satisfied (mirrors the Postgres path after migration 031).
        super::instances::bind_instance_insert(
            sqlx::query(super::instances::INSTANCE_INSERT_SQL),
            instance,
        )?
        .execute(&mut *conn)
        .await?;

        let inserted = sqlx::query(
            "INSERT INTO emit_event_dedupe (scope_kind, scope_value, dedupe_key, child_instance_id)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_kind, scope_value, dedupe_key) DO NOTHING",
        )
        .bind(scope_kind)
        .bind(&scope_value)
        .bind(key)
        .bind(&cand_str)
        .execute(&mut *conn)
        .await?
        .rows_affected();
        Ok(inserted)
    }
    .await;

    match result {
        Ok(inserted) if inserted > 0 => {
            conn.commit().await?;
            return Ok(crate::EmitDedupeOutcome::Inserted);
        }
        Ok(_) => {
            // Key already taken — rollback the unnecessary child instance,
            // then look up the existing child id outside the transaction.
            conn.rollback().await?;
        }
        Err(e) => {
            let _ = conn.rollback().await;
            return Err(e);
        }
    }
    // rollback consumes the guard and releases its pooled connection.
    let row: (String,) = sqlx::query_as(
        "SELECT child_instance_id FROM emit_event_dedupe
         WHERE scope_kind = ?1 AND scope_value = ?2 AND dedupe_key = ?3",
    )
    .bind(scope_kind)
    .bind(&scope_value)
    .bind(key)
    .fetch_one(&storage.pool)
    .await?;

    let existing = uuid::Uuid::parse_str(&row.0)
        .map_err(|e| StorageError::Query(format!("invalid uuid in dedupe row: {e}")))?;
    Ok(crate::EmitDedupeOutcome::AlreadyExists(
        InstanceId::from_uuid(existing),
    ))
}

/// Delete up to `limit` `emit_event_dedupe` rows whose `created_at` is older
/// than `older_than`. `created_at` is stored as ISO-8601 text; we normalize
/// both sides through `datetime(...)` to avoid lexicographic byte comparison
/// masking real timestamp ordering — same convention as `delete_expired` for
/// externalized state.
pub(super) async fn delete_expired_emit_event_dedupe(
    storage: &SqliteStorage,
    older_than: chrono::DateTime<chrono::Utc>,
    limit: u32,
) -> Result<u64, StorageError> {
    let cutoff = older_than.to_rfc3339();
    let result = sqlx::query(
        r"DELETE FROM emit_event_dedupe
          WHERE rowid IN (
              SELECT rowid FROM emit_event_dedupe
              WHERE datetime(created_at) < datetime(?1)
              LIMIT ?2
          )",
    )
    .bind(&cutoff)
    .bind(i64::from(limit))
    .execute(&storage.pool)
    .await?;
    Ok(result.rows_affected())
}

// === Health ===

pub(super) async fn ping(storage: &SqliteStorage) -> Result<(), StorageError> {
    sqlx::query("SELECT 1")
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;
    Ok(())
}

// === Auxiliary retention ===

/// See [`crate::ResourceStore::delete_expired_auxiliary`]. Each statement
/// deletes at most `limit` rows and commits on its own, so the writer lock is
/// never held across the whole sweep.
pub(super) async fn delete_expired_auxiliary(
    storage: &SqliteStorage,
    retention: &crate::AuxiliaryRetention,
    limit: u32,
) -> Result<u64, StorageError> {
    // (table, predicate on ?1 = cutoff). Constants only — never input.
    // SQLite has no `telemetry_mobile_errors` table (server-only telemetry),
    // so `mobile_errors_before` is a no-op here.
    let sweeps: [(Option<DateTime<Utc>>, &str, &str); 7] = [
        (
            retention.worker_attempt_events_before,
            "worker_task_attempt_events",
            "created_at < ?1",
        ),
        (
            retention.settled_events_before,
            "event_inbox",
            "status IN ('consumed', 'expired') AND received_at < ?1",
        ),
        (
            retention.sub_tenant_executions_before,
            "sub_tenant_executions",
            "started_at < ?1",
        ),
        (
            retention.mobile_terminal_status_before,
            "mobile_instance_status",
            "state IN ('completed', 'failed', 'cancelled') AND updated_at < ?1",
        ),
        (
            retention.mobile_settled_approvals_before,
            "mobile_approval_requests",
            "state <> 'pending' AND created_at < ?1",
        ),
        (
            retention.stopped_nodes_before,
            "cluster_nodes",
            "status = 'stopped' AND last_heartbeat_at < ?1",
        ),
        (
            retention.worker_registrations_before,
            "worker_registrations",
            "last_seen_at < ?1",
        ),
    ];
    let mut max_deleted = 0;
    for (cutoff, table, predicate) in sweeps {
        let Some(cutoff) = cutoff else { continue };
        let sql = format!(
            "DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE {predicate} LIMIT ?2)"
        );
        let deleted = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(ts(cutoff))
            .bind(i64::from(limit))
            .execute(&storage.pool)
            .await?
            .rows_affected();
        max_deleted = max_deleted.max(deleted);
    }
    Ok(max_deleted)
}
