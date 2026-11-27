use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::cron::{CronInventoryFilter, CronMigrationSnapshot, CronSchedule};
use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::SqliteStorage;
use super::helpers::{begin_immediate, parse_ts_opt, row_to_cron, ts};

pub(super) async fn create(
    storage: &SqliteStorage,
    schedule: &CronSchedule,
) -> Result<(), StorageError> {
    let mut conn = storage.pool.acquire().await?;
    create_on(&mut conn, schedule).await
}

async fn create_on(
    conn: &mut sqlx::SqliteConnection,
    s: &CronSchedule,
) -> Result<(), StorageError> {
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
    .execute(conn).await?;
    Ok(())
}

pub(super) async fn import_migration(
    storage: &SqliteStorage,
    snapshot: &CronMigrationSnapshot,
    schedule: &CronSchedule,
    epoch: i64,
    digest: &str,
) -> Result<(), StorageError> {
    let mut tx = begin_immediate(&storage.pool).await?;
    let inserted = sqlx::query(
        "INSERT INTO cron_migration_imports(schedule_id,tenant_id,migration_id,ownership_epoch,last_fire_at,snapshot_sha256) VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT DO NOTHING",
    )
    .bind(schedule.id.to_string()).bind(schedule.tenant_id.as_str()).bind(snapshot.migration_id.to_string())
    .bind(epoch).bind(snapshot.last_fire_at.map(ts)).bind(digest)
    .execute(&mut *tx).await?.rows_affected() == 1;
    if inserted {
        create_on(&mut tx, schedule).await?;
    } else {
        let same: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM cron_migration_imports ci JOIN cron_schedules cs ON cs.id=ci.schedule_id WHERE ci.schedule_id=?1 AND ci.tenant_id=?2 AND ci.migration_id=?3 AND ci.ownership_epoch=?4 AND ci.snapshot_sha256=?5)",
        ).bind(schedule.id.to_string()).bind(schedule.tenant_id.as_str()).bind(snapshot.migration_id.to_string())
          .bind(epoch).bind(digest).fetch_one(&mut *tx).await?;
        if !same {
            return Err(StorageError::Conflict(
                "cron schedule already belongs to another migration".into(),
            ));
        }
    }
    tx.commit().await?;
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

pub(super) async fn list_all(
    storage: &SqliteStorage,
    tenant_id: Option<&TenantId>,
) -> Result<Vec<CronSchedule>, StorageError> {
    let rows = if let Some(tid) = tenant_id {
        sqlx::query("SELECT * FROM cron_schedules WHERE tenant_id=?1 ORDER BY created_at")
            .bind(tid.as_str())
            .fetch_all(&storage.pool)
            .await
    } else {
        sqlx::query("SELECT * FROM cron_schedules ORDER BY created_at")
            .fetch_all(&storage.pool)
            .await
    }?;
    rows.iter().map(row_to_cron).collect()
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

pub(super) async fn list_page(
    storage: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    before: Option<Uuid>,
    filter: &CronInventoryFilter,
    limit: u32,
) -> Result<Vec<CronSchedule>, StorageError> {
    let mut query =
        sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT * FROM cron_schedules WHERE TRUE");
    if let Some(tenant_id) = tenant_id {
        query
            .push(" AND tenant_id = ")
            .push_bind(tenant_id.as_str());
    }
    if let Some(before) = before {
        query.push(" AND id < ").push_bind(before.to_string());
    }
    if let Some(namespace) = &filter.namespace {
        query.push(" AND namespace = ").push_bind(namespace);
    }
    if let Some(sequence_id) = filter.sequence_id {
        query
            .push(" AND sequence_id = ")
            .push_bind(sequence_id.to_string());
    }
    if let Some(enabled) = filter.enabled {
        query.push(" AND enabled = ").push_bind(enabled);
    }
    query
        .push(" ORDER BY id DESC LIMIT ")
        .push_bind(i64::from(limit.min(201)));
    let rows = query.build().fetch_all(&storage.pool).await?;
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

async fn matches_revision(
    conn: &mut sqlx::SqliteConnection,
    id: Uuid,
    tenant_id: &TenantId,
    revision: &str,
) -> Result<bool, StorageError> {
    let row = sqlx::query("SELECT * FROM cron_schedules WHERE id = ?1 AND tenant_id = ?2")
        .bind(id.to_string())
        .bind(tenant_id.as_str())
        .fetch_optional(conn)
        .await?;
    let schedule = row.as_ref().map(row_to_cron).transpose()?;
    match schedule {
        Some(schedule) => Ok(schedule.configuration_revision()? == revision),
        None => Ok(false),
    }
}

pub(super) async fn update_reviewed(
    storage: &SqliteStorage,
    s: &CronSchedule,
    revision: &str,
) -> Result<Option<CronSchedule>, StorageError> {
    let mut tx = begin_immediate(&storage.pool).await?;
    if !matches_revision(&mut tx, s.id, &s.tenant_id, revision).await? {
        return Ok(None);
    }
    let row = sqlx::query(
        "UPDATE cron_schedules
         SET cron_expr=?2, timezone=?3, enabled=?4, metadata=?5, next_fire_at=?6,
             overlap_policy=?7, updated_at=?8
         WHERE id=?1 RETURNING *",
    )
    .bind(s.id.to_string())
    .bind(&s.cron_expr)
    .bind(&s.timezone)
    .bind(s.enabled as i32)
    .bind(serde_json::to_string(&s.metadata)?)
    .bind(s.next_fire_at.map(ts))
    .bind(s.overlap_policy.to_string())
    .bind(ts(Utc::now()))
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    row.as_ref().map(row_to_cron).transpose()
}

pub(super) async fn delete_reviewed(
    storage: &SqliteStorage,
    id: Uuid,
    tenant_id: &TenantId,
    revision: &str,
) -> Result<bool, StorageError> {
    let mut tx = begin_immediate(&storage.pool).await?;
    if !matches_revision(&mut tx, id, tenant_id, revision).await? {
        return Ok(false);
    }
    let removed = sqlx::query("DELETE FROM cron_schedules WHERE id = ?1")
        .bind(id.to_string())
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(removed)
}

/// Maximum schedules claimed in one tick. After a long outage an unbounded
/// claim would burst-spawn every due schedule at once — the exact thundering
/// herd the missed-fire policy exists to avoid. Stragglers are picked up on
/// subsequent ticks.
const MAX_CLAIM_PER_TICK: i64 = 100;

async fn claim_due_inner(
    conn: &mut sqlx::SqliteConnection,
    now_str: &str,
    lease_until_str: &str,
) -> Result<Vec<CronSchedule>, StorageError> {
    // SQLite lacks FOR UPDATE SKIP LOCKED, so claiming runs inside BEGIN
    // IMMEDIATE (see `claim_due` below). One statement: mark the oldest due
    // schedules triggered and return them, instead of the previous
    // SELECT-then-2N-round-trips loop under the write lock.
    //
    // Lease, not a permanent mark — see the Postgres twin: a failed/crashed
    // fire becomes re-claimable once `claimed_until` passes.
    let rows = sqlx::query(
        "UPDATE cron_schedules SET claimed_until = ?3, updated_at = ?1
         WHERE id IN (
             SELECT id FROM cron_schedules
             WHERE enabled = 1 AND next_fire_at <= ?1
               AND (claimed_until IS NULL OR claimed_until < ?1)
             ORDER BY next_fire_at
             LIMIT ?2
         )
         RETURNING *",
    )
    .bind(now_str)
    .bind(MAX_CLAIM_PER_TICK)
    .bind(lease_until_str)
    .fetch_all(&mut *conn)
    .await?;

    rows.iter().map(row_to_cron).collect()
}

pub(super) async fn claim_due(
    storage: &SqliteStorage,
    now: DateTime<Utc>,
) -> Result<Vec<CronSchedule>, StorageError> {
    let now_str = ts(now);
    let lease_until_str = ts(now + chrono::Duration::seconds(crate::CRON_CLAIM_LEASE_SECS));

    // Retain transaction ownership through row decoding: errors and cancelled
    // futures roll back the claim before the connection is reused.
    let mut conn = begin_immediate(&storage.pool).await?;
    let schedules = claim_due_inner(&mut conn, &now_str, &lease_until_str).await?;
    conn.commit().await?;
    Ok(schedules)
}

pub(super) async fn update_fire_times(
    storage: &SqliteStorage,
    id: Uuid,
    last_triggered_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE cron_schedules SET last_triggered_at=?2, next_fire_at=?3, claimed_until=NULL, updated_at=?4 WHERE id=?1")
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
        "UPDATE cron_schedules SET skipped_fires = skipped_fires + 1, last_skipped_at = ?2, last_triggered_at = ?2, next_fire_at = ?3, claimed_until = NULL, updated_at = ?2 WHERE id = ?1",
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

impl SqliteStorage {
    /// Disable and snapshot the source schedule under one write lock.
    pub async fn fence_cron_for_cloud_migration(
        &self,
        tenant: &TenantId,
        id: Uuid,
        migration_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<CronMigrationSnapshot, StorageError> {
        use sqlx::Row as _;
        let mut tx = begin_immediate(&self.pool).await?;
        if let Some(record) = sqlx::query_scalar::<_, String>(
            "SELECT record FROM cron_migration_sources WHERE schedule_id=?1 AND tenant_id=?2",
        )
        .bind(id.to_string())
        .bind(tenant.as_str())
        .fetch_optional(&mut *tx)
        .await?
        {
            let snapshot: CronMigrationSnapshot = serde_json::from_str(&record)?;
            if snapshot.migration_id != migration_id {
                return Err(StorageError::Conflict(
                    "cron schedule is fenced by a different migration".into(),
                ));
            }
            tx.commit().await?;
            return Ok(snapshot);
        }

        let row = sqlx::query("SELECT * FROM cron_schedules WHERE id=?1 AND tenant_id=?2")
            .bind(id.to_string())
            .bind(tenant.as_str())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "cron schedule",
                id: id.to_string(),
            })?;
        if parse_ts_opt(row.try_get("claimed_until")?)?.is_some_and(|until| until >= now) {
            return Err(StorageError::Conflict(
                "cron schedule has an active fire lease".into(),
            ));
        }
        let schedule = row_to_cron(&row)?;
        let epoch = sqlx::query_scalar::<_, i64>(
            "SELECT ownership_epoch FROM cron_migration_imports WHERE schedule_id=?1 AND tenant_id=?2",
        ).bind(id.to_string()).bind(tenant.as_str()).fetch_optional(&mut *tx).await?.unwrap_or(0);
        let mut last_fire_at = schedule.last_triggered_at;
        if let Some(key) = schedule.occurrence_key() {
            let fired: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_instances WHERE tenant_id=?1 AND idempotency_key=?2)",
            ).bind(tenant.as_str()).bind(key).fetch_one(&mut *tx).await?;
            if fired {
                last_fire_at = last_fire_at.max(schedule.next_fire_at);
            }
        }
        let snapshot = CronMigrationSnapshot {
            migration_id,
            source_epoch: u64::try_from(epoch)
                .map_err(|_| StorageError::Constraint("invalid cron ownership epoch".into()))?,
            last_fire_at,
            schedule,
        };
        sqlx::query("INSERT INTO cron_migration_sources(schedule_id,tenant_id,migration_id,record) VALUES (?1,?2,?3,?4)")
            .bind(id.to_string()).bind(tenant.as_str()).bind(migration_id.to_string())
            .bind(serde_json::to_string(&snapshot)?).execute(&mut *tx).await?;
        sqlx::query("UPDATE cron_schedules SET enabled=0, claimed_until=NULL WHERE id=?1")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(snapshot)
    }
}
