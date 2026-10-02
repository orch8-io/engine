//! SQLite [`crate::TenancyStore`]: sub-tenant admission + metering, the embed
//! theme, and release rollout targets (Postgres migration 097).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::instance::TaskInstance;
use orch8_types::release::ReleaseTarget;
use orch8_types::sub_tenant::{EmbedTheme, SubTenantLimits, SubTenantUsage, month_start};

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};
use super::instances::{INSTANCE_INSERT_SQL, bind_instance_insert};
use crate::tenancy::{UsageAccumulator, batch_scope, check_caps};

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

fn limits_from_row(row: &sqlx::sqlite::SqliteRow) -> SubTenantLimits {
    SubTenantLimits {
        max_executions_per_month: row
            .get::<Option<i64>, _>("max_executions_per_month")
            .map(to_u64),
        max_concurrent: row
            .get::<Option<i64>, _>("max_concurrent")
            .map(|v| u32::try_from(v).unwrap_or(0)),
    }
}

fn parse_last(value: Option<&str>) -> Result<Option<DateTime<Utc>>, StorageError> {
    value.map(parse_ts).transpose()
}

async fn admit_and_insert(
    connection: &mut sqlx::SqliteConnection,
    instances: &[TaskInstance],
    max_active_instances: u64,
    now: DateTime<Utc>,
) -> Result<u64, StorageError> {
    let (tenant, sub) = batch_scope(instances)?;
    let requested = u64::try_from(instances.len()).unwrap_or(u64::MAX);
    let tenant_active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_instances WHERE tenant_id=? \
         AND state IN ('scheduled','running','waiting','paused')",
    )
    .bind(tenant.as_str())
    .fetch_one(&mut *connection)
    .await?;
    if to_u64(tenant_active).saturating_add(requested) > max_active_instances {
        return Err(StorageError::QuotaExceeded(
            "active-instance entitlement exhausted".into(),
        ));
    }
    let limits = sqlx::query(
        "SELECT max_executions_per_month, max_concurrent FROM sub_tenant_limits \
         WHERE tenant_id=? AND sub_tenant=?",
    )
    .bind(tenant.as_str())
    .bind(sub)
    .fetch_optional(&mut *connection)
    .await?
    .map(|row| limits_from_row(&row));
    if limits.is_some_and(|l| !l.is_unlimited()) {
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_instances WHERE tenant_id=? AND sub_tenant=? \
             AND state IN ('scheduled','running','waiting','paused')",
        )
        .bind(tenant.as_str())
        .bind(sub)
        .fetch_one(&mut *connection)
        .await?;
        let started: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sub_tenant_executions \
             WHERE tenant_id=? AND sub_tenant=? AND started_at >= ?",
        )
        .bind(tenant.as_str())
        .bind(sub)
        .bind(ts(month_start(now)))
        .fetch_one(&mut *connection)
        .await?;
        check_caps(limits, to_u64(active), to_u64(started), requested)?;
    }
    let mut count = 0u64;
    for instance in instances {
        let result = bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), instance)?
            .execute(&mut *connection)
            .await?;
        count += result.rows_affected();
        sqlx::query(
            "INSERT OR IGNORE INTO sub_tenant_executions \
                (instance_id, tenant_id, sub_tenant, started_at) VALUES (?, ?, ?, ?)",
        )
        .bind(instance.id.into_uuid().to_string())
        .bind(tenant.as_str())
        .bind(sub)
        .bind(ts(instance.created_at))
        .execute(&mut *connection)
        .await?;
    }
    Ok(count)
}

#[async_trait]
impl crate::TenancyStore for SqliteStorage {
    async fn create_sub_tenant_instances_admitted(
        &self,
        instances: &[TaskInstance],
        max_active_instances: u64,
        now: DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        if instances.is_empty() {
            return Ok(0);
        }
        let mut connection = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await?;
        match admit_and_insert(&mut connection, instances, max_active_instances, now).await {
            Ok(count) => {
                sqlx::query("COMMIT").execute(&mut *connection).await?;
                Ok(count)
            }
            Err(error) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
                Err(error)
            }
        }
    }

    async fn get_sub_tenant_limits(
        &self,
        tenant_id: &TenantId,
        sub_tenant: &str,
    ) -> Result<Option<SubTenantLimits>, StorageError> {
        let row = sqlx::query(
            "SELECT max_executions_per_month, max_concurrent FROM sub_tenant_limits \
             WHERE tenant_id=? AND sub_tenant=?",
        )
        .bind(tenant_id.as_str())
        .bind(sub_tenant)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| limits_from_row(&row)))
    }

    async fn put_sub_tenant_limits(
        &self,
        tenant_id: &TenantId,
        sub_tenant: &str,
        limits: &SubTenantLimits,
    ) -> Result<(), StorageError> {
        let monthly = limits
            .max_executions_per_month
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX));
        sqlx::query(
            "INSERT INTO sub_tenant_limits \
                (tenant_id, sub_tenant, max_executions_per_month, max_concurrent, updated_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, sub_tenant) DO UPDATE SET \
                max_executions_per_month = excluded.max_executions_per_month, \
                max_concurrent = excluded.max_concurrent, updated_at = excluded.updated_at",
        )
        .bind(tenant_id.as_str())
        .bind(sub_tenant)
        .bind(monthly)
        .bind(limits.max_concurrent.map(i64::from))
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn sub_tenant_usage(
        &self,
        tenant_id: &TenantId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<SubTenantUsage>, StorageError> {
        let (from, to) = (ts(from), ts(to));
        let mut acc = UsageAccumulator::default();
        for row in sqlx::query(
            "SELECT sub_tenant, COUNT(*) AS n, MAX(started_at) AS last \
             FROM sub_tenant_executions \
             WHERE tenant_id=? AND started_at >= ? AND started_at < ? \
             GROUP BY sub_tenant",
        )
        .bind(tenant_id.as_str())
        .bind(&from)
        .bind(&to)
        .fetch_all(&self.pool)
        .await?
        {
            acc.started(
                row.get("sub_tenant"),
                to_u64(row.get("n")),
                parse_last(row.get::<Option<&str>, _>("last"))?,
            );
        }
        for row in sqlx::query(
            "SELECT sub_tenant, COUNT(*) AS n, MAX(updated_at) AS last \
             FROM task_instances \
             WHERE tenant_id=? AND sub_tenant IS NOT NULL AND state='completed' \
               AND updated_at >= ? AND updated_at < ? \
             GROUP BY sub_tenant",
        )
        .bind(tenant_id.as_str())
        .bind(&from)
        .bind(&to)
        .fetch_all(&self.pool)
        .await?
        {
            acc.completed(
                row.get("sub_tenant"),
                to_u64(row.get("n")),
                parse_last(row.get::<Option<&str>, _>("last"))?,
            );
        }
        for row in sqlx::query(
            "SELECT ti.sub_tenant AS sub_tenant, COUNT(*) AS n, MAX(bo.created_at) AS last \
             FROM block_outputs bo JOIN task_instances ti ON ti.id = bo.instance_id \
             WHERE ti.tenant_id=? AND ti.sub_tenant IS NOT NULL \
               AND bo.created_at >= ? AND bo.created_at < ? \
               AND substr(bo.block_id, 1, 1) <> '_' \
             GROUP BY ti.sub_tenant",
        )
        .bind(tenant_id.as_str())
        .bind(&from)
        .bind(&to)
        .fetch_all(&self.pool)
        .await?
        {
            acc.steps(
                row.get("sub_tenant"),
                to_u64(row.get("n")),
                parse_last(row.get::<Option<&str>, _>("last"))?,
            );
        }
        Ok(acc.finish())
    }

    async fn count_active_sub_tenants(&self, since: DateTime<Utc>) -> Result<u64, StorageError> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (SELECT DISTINCT tenant_id, sub_tenant \
             FROM sub_tenant_executions WHERE started_at >= ?)",
        )
        .bind(ts(since))
        .fetch_one(&self.pool)
        .await?;
        Ok(to_u64(n))
    }

    async fn get_embed_theme(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<EmbedTheme>, StorageError> {
        let record: Option<String> =
            sqlx::query_scalar("SELECT record FROM embed_themes WHERE tenant_id=?")
                .bind(tenant_id.as_str())
                .fetch_optional(&self.pool)
                .await?;
        record
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(StorageError::Serialization)
    }

    async fn put_embed_theme(
        &self,
        tenant_id: &TenantId,
        theme: &EmbedTheme,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO embed_themes (tenant_id, record, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT (tenant_id) DO UPDATE SET record = excluded.record, \
                updated_at = excluded.updated_at",
        )
        .bind(tenant_id.as_str())
        .bind(serde_json::to_string(theme)?)
        .bind(ts(Utc::now()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn set_release_target(
        &self,
        release_id: Uuid,
        target: Option<&ReleaseTarget>,
    ) -> Result<bool, StorageError> {
        let result =
            sqlx::query("UPDATE workflow_releases SET target = ?2, updated_at = ?3 WHERE id = ?1")
                .bind(release_id.to_string())
                .bind(target.map(serde_json::to_string).transpose()?)
                .bind(ts(Utc::now()))
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }
}
