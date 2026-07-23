use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::SqliteStorage;
use super::helpers::{row_to_cron, ts};

pub(super) async fn create(storage: &SqliteStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO cron_schedules (id,tenant_id,namespace,sequence_id,cron_expr,timezone,enabled,metadata,overlap_policy,skipped_fires,last_skipped_at,next_fire_at,last_triggered_at,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)"
    )
    .bind(s.id.to_string())
    .bind(s.tenant_id.as_str())
    .bind(s.namespace.as_str())
    .bind(s.sequence_id.into_uuid().to_string())
    .bind(&s.cron_expr)
    .bind(&s.timezone)
    .bind(s.enabled as i32)
    .bind(serde_json::to_string(&s.metadata)?)
    .bind(s.overlap_policy.to_string())
    .bind(s.skipped_fires)
    .bind(s.last_skipped_at.map(ts))
    .bind(s.next_fire_at.map(ts))
    .bind(s.last_triggered_at.map(ts))
    .bind(ts(s.created_at))
    .bind(ts(s.updated_at))
    .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<CronSchedule>, StorageError> {
    let row = sqlx::query("SELECT * FROM cron_schedules WHERE id=?1")
        .bind(id.to_string())
        .fetch_optional(&storage.pool)
        .await?;
    row.as_ref().map(row_to_cron).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<CronSchedule>, StorageError> {
    let cap = limit.min(1000) as i64;
    let rows = if let Some(tid) = tenant_id {
        sqlx::query("SELECT * FROM cron_schedules WHERE tenant_id=?1 ORDER BY created_at LIMIT ?2")
            .bind(tid.as_str())
            .bind(cap)
            .fetch_all(&storage.pool)
            .await
    } else {
        sqlx::query("SELECT * FROM cron_schedules ORDER BY created_at LIMIT ?1")
            .bind(cap)
            .fetch_all(&storage.pool)
            .await
    }?;
    rows.iter().map(row_to_cron).collect()
}

pub(super) async fn update(storage: &SqliteStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query("UPDATE cron_schedules SET cron_expr=?2, timezone=?3, enabled=?4, metadata=?5, next_fire_at=?6, overlap_policy=?7, updated_at=?8 WHERE id=?1")
        .bind(s.id.to_string())
        .bind(&s.cron_expr)
        .bind(&s.timezone)
        .bind(s.enabled as i32)
        .bind(serde_json::to_string(&s.metadata)?)
        .bind(s.next_fire_at.map(ts))
        .bind(s.overlap_policy.to_string())
        .bind(ts(Utc::now()))
        .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn delete(storage: &SqliteStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM cron_schedules WHERE id=?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}

/// Maximum schedules claimed in one tick. After a long outage an unbounded
/// claim would burst-spawn every due schedule at once — the exact thundering
/// herd the missed-fire policy exists to avoid. Stragglers are picked up on
/// subsequent ticks.
const MAX_CLAIM_PER_TICK: i64 = 100;

async fn claim_due_inner(
    conn: &mut sqlx::SqliteConnection,
    now_str: &str,
) -> Result<Vec<CronSchedule>, StorageError> {
    // SQLite lacks FOR UPDATE SKIP LOCKED, so claiming runs inside BEGIN
    // IMMEDIATE (see `claim_due` below). One statement: mark the oldest due
    // schedules triggered and return them, instead of the previous
    // SELECT-then-2N-round-trips loop under the write lock.
    let rows = sqlx::query(
        "UPDATE cron_schedules SET last_triggered_at = ?1, updated_at = ?1
         WHERE id IN (
             SELECT id FROM cron_schedules
             WHERE enabled = 1 AND next_fire_at <= ?1
               AND (last_triggered_at IS NULL OR last_triggered_at < next_fire_at)
             ORDER BY next_fire_at
             LIMIT ?2
         )
         RETURNING *",
    )
    .bind(now_str)
    .bind(MAX_CLAIM_PER_TICK)
    .fetch_all(&mut *conn)
    .await?;

    rows.iter().map(row_to_cron).collect()
}

pub(super) async fn claim_due(
    storage: &SqliteStorage,
    now: DateTime<Utc>,
) -> Result<Vec<CronSchedule>, StorageError> {
    let now_str = ts(now);

    // `BEGIN IMMEDIATE` acquires a RESERVED write lock up-front, closing the
    // check-then-act window between the SELECT and the per-id UPDATE above --
    // sqlx's `pool.begin()` uses DEFERRED, which lets two concurrent claimers
    // both read the same due rows before either takes the write lock,
    // double-firing the loser's schedule. Same pattern as
    // `signals.rs::enqueue_if_active` and `instances.rs::claim_due`.
    let mut conn = storage.pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

    let result = claim_due_inner(&mut conn, &now_str).await;

    match result {
        Ok(schedules) => {
            sqlx::query("COMMIT").execute(&mut *conn).await?;
            Ok(schedules)
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            Err(e)
        }
    }
}

pub(super) async fn update_fire_times(
    storage: &SqliteStorage,
    id: Uuid,
    last_triggered_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE cron_schedules SET last_triggered_at=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
        .bind(id.to_string())
        .bind(ts(last_triggered_at))
        .bind(ts(next_fire_at))
        .bind(ts(Utc::now()))
        .execute(&storage.pool).await?;
    Ok(())
}

/// Record a skipped occurrence for the `skip` overlap policy: bump the skip
/// counters and advance the fire times in one statement so the schedule does
/// not stay due.
pub(super) async fn record_skip(
    storage: &SqliteStorage,
    id: Uuid,
    now: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE cron_schedules SET skipped_fires = skipped_fires + 1, last_skipped_at = ?2, last_triggered_at = ?2, next_fire_at = ?3, updated_at = ?2 WHERE id = ?1",
    )
    .bind(id.to_string())
    .bind(ts(now))
    .bind(ts(next_fire_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

/// Non-terminal instances created by this schedule, attributed via the
/// `metadata.cron_schedule_id` stamp the cron loop writes on every fire.
pub(super) async fn active_instance_ids_for_cron(
    storage: &SqliteStorage,
    cron_id: Uuid,
    limit: u32,
) -> Result<Vec<InstanceId>, StorageError> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT id FROM task_instances \
         WHERE json_extract(metadata, '$.cron_schedule_id') = ?1 \
           AND state IN ('scheduled', 'running', 'waiting', 'paused') \
         ORDER BY created_at LIMIT ?2",
    )
    .bind(cron_id.to_string())
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;
    rows.into_iter()
        .map(|(id,)| {
            let uuid = Uuid::parse_str(&id)
                .map_err(|e| StorageError::Query(format!("invalid instance uuid {id}: {e}")))?;
            Ok(InstanceId::from_uuid(uuid))
        })
        .collect()
}
