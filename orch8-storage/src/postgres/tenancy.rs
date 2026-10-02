//! `PostgreSQL` [`crate::TenancyStore`]: sub-tenant admission + metering, the
//! embed theme, and release rollout targets (migration 097).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::instance::TaskInstance;
use orch8_types::release::ReleaseTarget;
use orch8_types::sub_tenant::{EmbedTheme, SubTenantLimits, SubTenantUsage, month_start};

use super::PostgresStorage;
use super::instances::{INSTANCE_INSERT_SQL, bind_instance_insert};
use crate::tenancy::{UsageAccumulator, batch_scope, check_caps};

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

fn limits_from_row(row: &sqlx::postgres::PgRow) -> SubTenantLimits {
    SubTenantLimits {
        max_executions_per_month: row
            .get::<Option<i64>, _>("max_executions_per_month")
            .map(to_u64),
        max_concurrent: row
            .get::<Option<i32>, _>("max_concurrent")
            .map(|v| u32::try_from(v).unwrap_or(0)),
    }
}

#[async_trait]
impl crate::TenancyStore for PostgresStorage {
    async fn create_sub_tenant_instances_admitted(
        &self,
        instances: &[TaskInstance],
        max_active_instances: u64,
        now: DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        if instances.is_empty() {
            return Ok(0);
        }
        let (tenant, sub) = batch_scope(instances)?;
        let requested = u64::try_from(instances.len()).unwrap_or(u64::MAX);
        let mut tx = self.pool.begin().await?;
        // Same lock key as tenant-level admission, so pool and sub-tenant
        // caps are evaluated against one serialized view of the tenant.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,1))")
            .bind(tenant.as_str())
            .execute(&mut *tx)
            .await?;
        let tenant_active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_instances WHERE tenant_id=$1 \
             AND state IN ('scheduled','running','waiting','paused')",
        )
        .bind(tenant.as_str())
        .fetch_one(&mut *tx)
        .await?;
        if to_u64(tenant_active).saturating_add(requested) > max_active_instances {
            return Err(StorageError::QuotaExceeded(
                "active-instance entitlement exhausted".into(),
            ));
        }
        let limits = sqlx::query(
            "SELECT max_executions_per_month, max_concurrent FROM sub_tenant_limits \
             WHERE tenant_id=$1 AND sub_tenant=$2",
        )
        .bind(tenant.as_str())
        .bind(sub)
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| limits_from_row(&row));
        if limits.is_some_and(|l| !l.is_unlimited()) {
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_instances WHERE tenant_id=$1 AND sub_tenant=$2 \
                 AND state IN ('scheduled','running','waiting','paused')",
            )
            .bind(tenant.as_str())
            .bind(sub)
            .fetch_one(&mut *tx)
            .await?;
            let started: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sub_tenant_executions \
                 WHERE tenant_id=$1 AND sub_tenant=$2 AND started_at >= $3",
            )
            .bind(tenant.as_str())
            .bind(sub)
            .bind(month_start(now))
            .fetch_one(&mut *tx)
            .await?;
            check_caps(limits, to_u64(active), to_u64(started), requested)?;
        }
        let mut count = 0u64;
        for instance in instances {
            let context = serde_json::to_value(&instance.context)?;
            let result = bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), instance, &context)
                .execute(&mut *tx)
                .await?;
            count += result.rows_affected();
            sqlx::query(
                "INSERT INTO sub_tenant_executions (instance_id, tenant_id, sub_tenant, started_at) \
                 VALUES ($1, $2, $3, $4) ON CONFLICT (instance_id) DO NOTHING",
            )
            .bind(instance.id.into_uuid())
            .bind(tenant.as_str())
            .bind(sub)
            .bind(instance.created_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(count)
    }

    async fn get_sub_tenant_limits(
        &self,
        tenant_id: &TenantId,
        sub_tenant: &str,
    ) -> Result<Option<SubTenantLimits>, StorageError> {
        let row = sqlx::query(
            "SELECT max_executions_per_month, max_concurrent FROM sub_tenant_limits \
             WHERE tenant_id=$1 AND sub_tenant=$2",
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
        let concurrent = limits
            .max_concurrent
            .map(|v| i32::try_from(v).unwrap_or(i32::MAX));
        sqlx::query(
            "INSERT INTO sub_tenant_limits \
                (tenant_id, sub_tenant, max_executions_per_month, max_concurrent, updated_at) \
             VALUES ($1, $2, $3, $4, now()) \
             ON CONFLICT (tenant_id, sub_tenant) DO UPDATE SET \
                max_executions_per_month = EXCLUDED.max_executions_per_month, \
                max_concurrent = EXCLUDED.max_concurrent, updated_at = now()",
        )
        .bind(tenant_id.as_str())
        .bind(sub_tenant)
        .bind(monthly)
        .bind(concurrent)
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
        let mut acc = UsageAccumulator::default();
        for row in sqlx::query(
            "SELECT sub_tenant, COUNT(*) AS n, MAX(started_at) AS last \
             FROM sub_tenant_executions \
             WHERE tenant_id=$1 AND started_at >= $2 AND started_at < $3 \
             GROUP BY sub_tenant",
        )
        .bind(tenant_id.as_str())
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?
        {
            acc.started(row.get("sub_tenant"), to_u64(row.get("n")), row.get("last"));
        }
        for row in sqlx::query(
            "SELECT sub_tenant, COUNT(*) AS n, MAX(updated_at) AS last \
             FROM task_instances \
             WHERE tenant_id=$1 AND sub_tenant IS NOT NULL AND state='completed' \
               AND updated_at >= $2 AND updated_at < $3 \
             GROUP BY sub_tenant",
        )
        .bind(tenant_id.as_str())
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?
        {
            acc.completed(row.get("sub_tenant"), to_u64(row.get("n")), row.get("last"));
        }
        for row in sqlx::query(
            "SELECT ti.sub_tenant AS sub_tenant, COUNT(*) AS n, MAX(bo.created_at) AS last \
             FROM block_outputs bo JOIN task_instances ti ON ti.id = bo.instance_id \
             WHERE ti.tenant_id=$1 AND ti.sub_tenant IS NOT NULL \
               AND bo.created_at >= $2 AND bo.created_at < $3 \
               AND left(bo.block_id, 1) <> '_' \
             GROUP BY ti.sub_tenant",
        )
        .bind(tenant_id.as_str())
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?
        {
            acc.steps(row.get("sub_tenant"), to_u64(row.get("n")), row.get("last"));
        }
        Ok(acc.finish())
    }

    async fn count_active_sub_tenants(&self, since: DateTime<Utc>) -> Result<u64, StorageError> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (SELECT DISTINCT tenant_id, sub_tenant \
             FROM sub_tenant_executions WHERE started_at >= $1) AS active",
        )
        .bind(since)
        .fetch_one(&self.pool)
        .await?;
        Ok(to_u64(n))
    }

    async fn get_embed_theme(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<EmbedTheme>, StorageError> {
        let record: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT record FROM embed_themes WHERE tenant_id=$1")
                .bind(tenant_id.as_str())
                .fetch_optional(&self.pool)
                .await?;
        record
            .map(serde_json::from_value)
            .transpose()
            .map_err(StorageError::Serialization)
    }

    async fn put_embed_theme(
        &self,
        tenant_id: &TenantId,
        theme: &EmbedTheme,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO embed_themes (tenant_id, record, updated_at) VALUES ($1, $2, now()) \
             ON CONFLICT (tenant_id) DO UPDATE SET record = EXCLUDED.record, updated_at = now()",
        )
        .bind(tenant_id.as_str())
        .bind(serde_json::to_value(theme)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn set_release_target(
        &self,
        release_id: Uuid,
        target: Option<&ReleaseTarget>,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE workflow_releases SET target = $2, updated_at = now() WHERE id = $1",
        )
        .bind(release_id)
        .bind(target.map(serde_json::to_value).transpose()?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }
}
