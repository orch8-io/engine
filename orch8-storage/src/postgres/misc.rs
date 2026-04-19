use std::time::Duration;

use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::instance::TaskInstance;
use orch8_types::worker::WorkerTask;

use super::rows::{InstanceRow, WorkerTaskRow};
use super::PostgresStorage;

// === Idempotency ===

pub(super) async fn find_by_idempotency_key(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    idempotency_key: &str,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query_as::<_, InstanceRow>(
        r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                  priority, timezone, metadata, context,
                  concurrency_key, max_concurrency, idempotency_key,
                  session_id, parent_instance_id, created_at, updated_at
           FROM task_instances
           WHERE tenant_id = $1 AND idempotency_key = $2",
    )
    .bind(&tenant_id.0)
    .bind(idempotency_key)
    .fetch_optional(&store.pool)
    .await?;
    row.map(InstanceRow::into_instance).transpose()
}

// === Concurrency ===

pub(super) async fn count_running_by_concurrency_key(
    store: &PostgresStorage,
    concurrency_key: &str,
) -> Result<i64, StorageError> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM task_instances WHERE concurrency_key = $1 AND state = 'running'",
    )
    .bind(concurrency_key)
    .fetch_one(&store.pool)
    .await?;
    Ok(row.0)
}

pub(super) async fn concurrency_position(
    store: &PostgresStorage,
    instance_id: InstanceId,
    concurrency_key: &str,
) -> Result<i64, StorageError> {
    // Order by `(created_at, id)`. Instance IDs are UUIDv7 so the 48-bit
    // timestamp prefix already gives us FIFO on `id` alone at millisecond
    // granularity — but within the same millisecond v7 falls back to 74
    // random bits, and three rapid HTTP creates comfortably land in the
    // same ms. The `created_at` leading column plus `id` tiebreaker keeps
    // position-1 assignment deterministic across ties so a same-ms newer
    // instance can't sneak ahead of an already-running older one and
    // exceed max_concurrency.
    let row: (i64,) = sqlx::query_as(
        r"SELECT COUNT(*) FROM task_instances
          WHERE concurrency_key = $1 AND state = 'running'
            AND (created_at, id) <= (
                SELECT created_at, id FROM task_instances WHERE id = $2
            )",
    )
    .bind(concurrency_key)
    .bind(instance_id.0)
    .fetch_one(&store.pool)
    .await?;
    Ok(row.0)
}

// === Recovery ===

pub(super) async fn recover_stale_instances(
    store: &PostgresStorage,
    stale_threshold: Duration,
) -> Result<u64, StorageError> {
    let threshold_secs = stale_threshold.as_secs_f64();
    let result = sqlx::query(
        r"
        UPDATE task_instances
        SET state = 'scheduled', next_fire_at = NOW(), updated_at = NOW()
        WHERE state IN ('running', 'waiting')
          AND updated_at < NOW() - make_interval(secs => $1::double precision)
        ",
    )
    .bind(threshold_secs)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

// === Sub-Sequences ===

pub(super) async fn get_child_instances(
    store: &PostgresStorage,
    parent_instance_id: InstanceId,
) -> Result<Vec<TaskInstance>, StorageError> {
    let rows = sqlx::query_as::<_, InstanceRow>(
        r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                  priority, timezone, metadata, context,
                  concurrency_key, max_concurrency, idempotency_key,
                  session_id, parent_instance_id, created_at, updated_at
           FROM task_instances WHERE parent_instance_id = $1 ORDER BY created_at",
    )
    .bind(parent_instance_id.0)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(InstanceRow::into_instance).collect()
}

// === Task Queue Routing ===

pub(super) async fn claim_worker_tasks_from_queue(
    store: &PostgresStorage,
    queue_name: &str,
    handler_name: &str,
    worker_id: &str,
    limit: u32,
) -> Result<Vec<WorkerTask>, StorageError> {
    let rows = sqlx::query_as::<_, WorkerTaskRow>(
        r"
        UPDATE worker_tasks
        SET state = 'claimed', worker_id = $4, claimed_at = NOW(), heartbeat_at = NOW()
        WHERE id IN (
            SELECT id FROM worker_tasks
            WHERE handler_name = $1 AND state = 'pending' AND queue_name = $5
            ORDER BY created_at ASC
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        ",
    )
    .bind(handler_name)
    .bind(worker_id)
    .bind(i64::from(limit))
    .bind(worker_id)
    .bind(queue_name)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(WorkerTaskRow::into_task).collect())
}

// === Dynamic Step Injection ===

pub(super) async fn inject_blocks(
    store: &PostgresStorage,
    instance_id: InstanceId,
    blocks_json: &serde_json::Value,
) -> Result<(), StorageError> {
    // Store injected blocks in instance metadata under `_injected_blocks`.
    sqlx::query(
        r"UPDATE task_instances
          SET metadata = jsonb_set(
              CASE WHEN metadata IS NULL OR metadata = 'null'::jsonb THEN '{}'::jsonb ELSE metadata END,
              '{_injected_blocks}',
              COALESCE(
                  CASE WHEN metadata IS NULL OR metadata = 'null'::jsonb THEN NULL ELSE metadata->'_injected_blocks' END,
                  '[]'::jsonb
              ) || $2),
              updated_at = NOW()
          WHERE id = $1",
    )
    .bind(instance_id.0)
    .bind(blocks_json)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_injected_blocks(
    store: &PostgresStorage,
    instance_id: InstanceId,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row: Option<(Option<serde_json::Value>,)> =
        sqlx::query_as("SELECT metadata->'_injected_blocks' FROM task_instances WHERE id = $1")
            .bind(instance_id.0)
            .fetch_optional(&store.pool)
            .await?;
    Ok(row
        .and_then(|(v,)| v)
        .and_then(|v| if v.is_null() { None } else { Some(v) }))
}

// === Emit Event Dedupe ===

pub(super) async fn record_or_get_emit_dedupe(
    store: &PostgresStorage,
    scope: &crate::DedupeScope,
    key: &str,
    candidate_child: InstanceId,
) -> Result<crate::EmitDedupeOutcome, StorageError> {
    let scope_kind = scope.kind();
    let scope_value = scope.value();

    let inserted: Option<(uuid::Uuid,)> = sqlx::query_as(
        r"INSERT INTO emit_event_dedupe (scope_kind, scope_value, dedupe_key, child_instance_id)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (scope_kind, scope_value, dedupe_key) DO NOTHING
          RETURNING child_instance_id",
    )
    .bind(scope_kind)
    .bind(&scope_value)
    .bind(key)
    .bind(candidate_child.0)
    .fetch_optional(&store.pool)
    .await?;

    if inserted.is_some() {
        return Ok(crate::EmitDedupeOutcome::Inserted);
    }

    let (existing,): (uuid::Uuid,) = sqlx::query_as(
        r"SELECT child_instance_id FROM emit_event_dedupe
          WHERE scope_kind = $1 AND scope_value = $2 AND dedupe_key = $3",
    )
    .bind(scope_kind)
    .bind(&scope_value)
    .bind(key)
    .fetch_one(&store.pool)
    .await?;

    Ok(crate::EmitDedupeOutcome::AlreadyExists(InstanceId(
        existing,
    )))
}

/// Atomically record the dedupe row AND insert the child `TaskInstance` in a
/// single transaction. See `StorageBackend::create_instance_with_dedupe`.
///
/// Closes the orphan window between the dedupe insert and the instance insert
/// — if either statement fails, both are rolled back so the caller never
/// observes a dedupe row pointing at a non-existent instance.
pub(super) async fn create_instance_with_dedupe(
    store: &PostgresStorage,
    scope: &crate::DedupeScope,
    key: &str,
    instance: &TaskInstance,
) -> Result<crate::EmitDedupeOutcome, StorageError> {
    let scope_kind = scope.kind();
    let scope_value = scope.value();

    let mut tx = store.pool.begin().await?;

    let inserted: Option<(uuid::Uuid,)> = sqlx::query_as(
        r"INSERT INTO emit_event_dedupe (scope_kind, scope_value, dedupe_key, child_instance_id)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (scope_kind, scope_value, dedupe_key) DO NOTHING
          RETURNING child_instance_id",
    )
    .bind(scope_kind)
    .bind(&scope_value)
    .bind(key)
    .bind(instance.id.0)
    .fetch_optional(&mut *tx)
    .await?;

    if inserted.is_none() {
        // Key already taken — load the existing child id, commit the read-only
        // tx, and return AlreadyExists without creating an instance.
        let (existing,): (uuid::Uuid,) = sqlx::query_as(
            r"SELECT child_instance_id FROM emit_event_dedupe
              WHERE scope_kind = $1 AND scope_value = $2 AND dedupe_key = $3",
        )
        .bind(scope_kind)
        .bind(&scope_value)
        .bind(key)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(crate::EmitDedupeOutcome::AlreadyExists(InstanceId(
            existing,
        )));
    }

    let context = serde_json::to_value(&instance.context)?;
    // Insert SQL + bindings live in `super::instances` so a schema change
    // touches exactly one place per backend.
    super::instances::bind_instance_insert(
        sqlx::query(super::instances::INSTANCE_INSERT_SQL),
        instance,
        &context,
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(crate::EmitDedupeOutcome::Inserted)
}

/// Delete up to `limit` `emit_event_dedupe` rows whose `created_at` is older
/// than `older_than`. Returns the affected row count.
///
/// Postgres `DELETE` doesn't accept `LIMIT` directly, so we pre-select the
/// primary-key tuples to bound sweep size. `FOR UPDATE SKIP LOCKED` lets
/// multiple engine nodes sweep concurrently without contending — mirrors the
/// externalized-state GC pattern.
pub(super) async fn delete_expired_emit_event_dedupe(
    store: &PostgresStorage,
    older_than: chrono::DateTime<chrono::Utc>,
    limit: u32,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"DELETE FROM emit_event_dedupe
          WHERE (scope_kind, scope_value, dedupe_key) IN (
              SELECT scope_kind, scope_value, dedupe_key FROM emit_event_dedupe
              WHERE created_at < $1
              ORDER BY created_at ASC
              LIMIT $2
              FOR UPDATE SKIP LOCKED
          )",
    )
    .bind(older_than)
    .bind(i64::from(limit))
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

// === Health ===

pub(super) async fn ping(store: &PostgresStorage) -> Result<(), StorageError> {
    sqlx::query("SELECT 1")
        .execute(&store.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;
    Ok(())
}
