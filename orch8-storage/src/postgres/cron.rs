use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::PostgresStorage;
use super::rows::CronRow;

pub(super) async fn create(store: &PostgresStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO cron_schedules
            (id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
             metadata, overlap_policy, skipped_fires, last_skipped_at,
             last_triggered_at, next_fire_at, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
    )
    .bind(s.id)
    .bind(s.tenant_id.as_str())
    .bind(s.namespace.as_str())
    .bind(s.sequence_id.into_uuid())
    .bind(&s.cron_expr)
    .bind(&s.timezone)
    .bind(s.enabled)
    .bind(&s.metadata)
    .bind(s.overlap_policy.to_string())
    .bind(s.skipped_fires)
    .bind(s.last_skipped_at)
    .bind(s.last_triggered_at)
    .bind(s.next_fire_at)
    .bind(s.created_at)
    .bind(s.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: Uuid,
) -> Result<Option<CronSchedule>, StorageError> {
    let row = sqlx::query_as::<_, CronRow>(
        r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                 metadata, overlap_policy, skipped_fires, last_skipped_at,
                 last_triggered_at, next_fire_at, created_at, updated_at
          FROM cron_schedules WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(CronRow::into_schedule))
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<CronSchedule>, StorageError> {
    let cap = i64::from(limit.min(1000));
    let rows = if let Some(tid) = tenant_id {
        sqlx::query_as::<_, CronRow>(
            r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                     metadata, overlap_policy, skipped_fires, last_skipped_at,
                 last_triggered_at, next_fire_at, created_at, updated_at
              FROM cron_schedules WHERE tenant_id = $1 ORDER BY created_at LIMIT $2",
        )
        .bind(tid.as_str())
        .bind(cap)
        .fetch_all(&store.pool)
        .await?
    } else {
        sqlx::query_as::<_, CronRow>(
            r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                     metadata, overlap_policy, skipped_fires, last_skipped_at,
                 last_triggered_at, next_fire_at, created_at, updated_at
              FROM cron_schedules ORDER BY created_at LIMIT $1",
        )
        .bind(cap)
        .fetch_all(&store.pool)
        .await?
    };
    Ok(rows.into_iter().map(CronRow::into_schedule).collect())
}

pub(super) async fn update(store: &PostgresStorage, s: &CronSchedule) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE cron_schedules
          SET cron_expr=$2, timezone=$3, enabled=$4, metadata=$5, next_fire_at=$6,
              overlap_policy=$7, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(s.id)
    .bind(&s.cron_expr)
    .bind(&s.timezone)
    .bind(s.enabled)
    .bind(&s.metadata)
    .bind(s.next_fire_at)
    .bind(s.overlap_policy.to_string())
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM cron_schedules WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn claim_due(
    store: &PostgresStorage,
    now: DateTime<Utc>,
) -> Result<Vec<CronSchedule>, StorageError> {
    // `LIMIT 100` caps one tick's claim: after a long outage an unbounded
    // claim would burst-spawn every due schedule at once. Stragglers are
    // picked up on subsequent ticks.
    let rows = sqlx::query_as::<_, CronRow>(
        r"UPDATE cron_schedules
          SET last_triggered_at = $1, updated_at = NOW()
          WHERE id IN (
              SELECT id FROM cron_schedules
              WHERE enabled = TRUE
                AND next_fire_at <= $1
                AND (last_triggered_at IS NULL OR last_triggered_at < next_fire_at)
              ORDER BY next_fire_at
              LIMIT 100
              FOR UPDATE SKIP LOCKED
          )
          RETURNING id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                    metadata, overlap_policy, skipped_fires, last_skipped_at,
                 last_triggered_at, next_fire_at, created_at, updated_at",
    )
    .bind(now)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(CronRow::into_schedule).collect())
}

pub(super) async fn update_fire_times(
    store: &PostgresStorage,
    id: Uuid,
    last_triggered_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE cron_schedules SET last_triggered_at=$2, next_fire_at=$3, updated_at=NOW() WHERE id=$1",
    )
    .bind(id)
    .bind(last_triggered_at)
    .bind(next_fire_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

/// Record a skipped occurrence for the `skip` overlap policy: bump the skip
/// counters and advance the fire times in one statement so the schedule does
/// not stay due.
pub(super) async fn record_skip(
    store: &PostgresStorage,
    id: Uuid,
    now: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE cron_schedules
          SET skipped_fires = skipped_fires + 1,
              last_skipped_at = $2,
              last_triggered_at = $2,
              next_fire_at = $3,
              updated_at = NOW()
          WHERE id = $1",
    )
    .bind(id)
    .bind(now)
    .bind(next_fire_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

/// Non-terminal instances created by this schedule, attributed via the
/// `metadata.cron_schedule_id` stamp the cron loop writes on every fire.
/// Unindexed JSON probe — fine at cron-fire frequency.
pub(super) async fn active_instance_ids_for_cron(
    store: &PostgresStorage,
    cron_id: Uuid,
    limit: u32,
) -> Result<Vec<orch8_types::ids::InstanceId>, StorageError> {
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        r"SELECT id FROM task_instances
          WHERE metadata->>'cron_schedule_id' = $1
            AND state IN ('scheduled', 'running', 'waiting', 'paused')
          ORDER BY created_at
          LIMIT $2",
    )
    .bind(cron_id.to_string())
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id,)| orch8_types::ids::InstanceId::from_uuid(id))
        .collect())
}
