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
    row.as_ref().map(row_to_instance).transpose()
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
    // Order by `(created_at, id)`. IDs are UUIDv7 so the 48-bit timestamp
    // prefix gives us FIFO-on-`id` at ms granularity — within a single ms
    // v7 falls back to 74 random bits, so `created_at` leads and `id`
    // tiebreaks to keep position assignment deterministic for same-ms
    // arrivals. Matches the Postgres backend.
    let rows = sqlx::query(
        "SELECT id FROM task_instances
         WHERE concurrency_key=?1 AND state='running'
         ORDER BY created_at, id",
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
    stale_threshold: Duration,
) -> Result<u64, StorageError> {
    let cutoff = chrono::Utc::now()
        - chrono::Duration::from_std(stale_threshold)
            .unwrap_or_else(|_| chrono::Duration::seconds(300));
    let result = sqlx::query(
        "UPDATE task_instances SET state='scheduled', updated_at=?1 WHERE state IN ('running', 'waiting') AND updated_at < ?2",
    )
    .bind(ts(chrono::Utc::now()))
    .bind(ts(cutoff))
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
    let rows =
        sqlx::query("SELECT * FROM task_instances WHERE parent_instance_id=?1 ORDER BY created_at")
            .bind(parent_instance_id.0.to_string())
            .fetch_all(&storage.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_instance).collect()
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
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    let rows = sqlx::query("SELECT * FROM worker_tasks WHERE queue_name=?1 AND handler_name=?2 AND state='pending' LIMIT ?3")
        .bind(queue_name)
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

// === Dynamic Step Injection ===

pub(super) async fn inject_blocks(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    blocks_json: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query("INSERT OR REPLACE INTO injected_blocks (instance_id, blocks) VALUES (?1, ?2)")
        .bind(instance_id.0.to_string())
        .bind(serde_json::to_string(blocks_json)?)
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
    Ok(row
        .map(|r| serde_json::from_str(r.get::<&str, _>("blocks")))
        .transpose()?)
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
    let cand_str = candidate_child.0.to_string();

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
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?
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
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;

    let existing = uuid::Uuid::parse_str(&row.0)
        .map_err(|e| StorageError::Query(format!("invalid uuid in dedupe row: {e}")))?;
    Ok(crate::EmitDedupeOutcome::AlreadyExists(InstanceId(
        existing,
    )))
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
    let cand_str = instance.id.0.to_string();

    // `?` leans on the `From<sqlx::Error> for StorageError` impl so each
    // failure surfaces with its real variant (PoolTimedOut → PoolExhausted,
    // etc.) instead of being flattened to `Query`.
    let mut tx = storage.pool.begin().await?;

    let inserted = sqlx::query(
        "INSERT INTO emit_event_dedupe (scope_kind, scope_value, dedupe_key, child_instance_id)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(scope_kind, scope_value, dedupe_key) DO NOTHING",
    )
    .bind(scope_kind)
    .bind(&scope_value)
    .bind(key)
    .bind(&cand_str)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if inserted == 0 {
        // Key already taken — look up the existing child id and abort without
        // creating an instance. Commit (a no-op read) and return AlreadyExists.
        let row: (String,) = sqlx::query_as(
            "SELECT child_instance_id FROM emit_event_dedupe
             WHERE scope_kind = ?1 AND scope_value = ?2 AND dedupe_key = ?3",
        )
        .bind(scope_kind)
        .bind(&scope_value)
        .bind(key)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        let existing = uuid::Uuid::parse_str(&row.0)
            .map_err(|e| StorageError::Query(format!("invalid uuid in dedupe row: {e}")))?;
        return Ok(crate::EmitDedupeOutcome::AlreadyExists(InstanceId(
            existing,
        )));
    }

    // Inserted the dedupe row — now insert the instance in the SAME tx. If this
    // fails the dedupe row is rolled back with it. The insert SQL + bindings
    // live in `super::instances` so a schema change touches exactly one place.
    super::instances::bind_instance_insert(
        sqlx::query(super::instances::INSTANCE_INSERT_SQL),
        instance,
    )?
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(crate::EmitDedupeOutcome::Inserted)
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
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
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
