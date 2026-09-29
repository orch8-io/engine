//! Backend-neutral helpers shared by the SQL [`crate::TenancyStore`]
//! implementations.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::instance::TaskInstance;
use orch8_types::sub_tenant::{SUB_TENANT_QUOTA_PREFIX, SubTenantLimits, SubTenantUsage};

/// The single `(tenant, sub_tenant)` every instance of an admitted batch
/// shares; mixed batches are a caller bug.
pub fn batch_scope(instances: &[TaskInstance]) -> Result<(&TenantId, &str), StorageError> {
    let first = instances
        .first()
        .ok_or_else(|| StorageError::Query("empty sub-tenant admission batch".into()))?;
    let sub = first
        .sub_tenant
        .as_deref()
        .ok_or_else(|| StorageError::Query("sub-tenant admission without sub_tenant".into()))?;
    if instances
        .iter()
        .any(|i| i.tenant_id != first.tenant_id || i.sub_tenant.as_deref() != Some(sub))
    {
        return Err(StorageError::Query(
            "sub-tenant admission batch spans several (tenant, sub_tenant) scopes".into(),
        ));
    }
    Ok((&first.tenant_id, sub))
}

/// Shared cap check: `Err(QuotaExceeded)` when admitting `requested` more
/// would exceed a cap.
pub fn check_caps(
    limits: Option<SubTenantLimits>,
    active: u64,
    started_this_month: u64,
    requested: u64,
) -> Result<(), StorageError> {
    let Some(limits) = limits else {
        return Ok(());
    };
    if let Some(max) = limits.max_concurrent
        && active.saturating_add(requested) > u64::from(max)
    {
        return Err(StorageError::QuotaExceeded(format!(
            "{SUB_TENANT_QUOTA_PREFIX}: sub-tenant concurrent cap ({max}) reached"
        )));
    }
    if let Some(max) = limits.max_executions_per_month
        && started_this_month.saturating_add(requested) > max
    {
        return Err(StorageError::QuotaExceeded(format!(
            "{SUB_TENANT_QUOTA_PREFIX}: sub-tenant monthly execution cap ({max}) reached"
        )));
    }
    Ok(())
}

/// Merges the three per-sub-tenant aggregates into one ordered report.
#[derive(Default)]
pub struct UsageAccumulator {
    rows: BTreeMap<String, SubTenantUsage>,
}

impl UsageAccumulator {
    fn entry(&mut self, sub: String) -> &mut SubTenantUsage {
        self.rows
            .entry(sub.clone())
            .or_insert_with(|| SubTenantUsage {
                sub_tenant: sub,
                executions_started: 0,
                executions_completed: 0,
                steps_executed: 0,
                last_active_at: None,
            })
    }

    fn touch(entry: &mut SubTenantUsage, last: Option<DateTime<Utc>>) {
        if let Some(last) = last
            && entry.last_active_at.is_none_or(|current| last > current)
        {
            entry.last_active_at = Some(last);
        }
    }

    pub fn started(&mut self, sub: String, n: u64, last: Option<DateTime<Utc>>) {
        let entry = self.entry(sub);
        entry.executions_started += n;
        Self::touch(entry, last);
    }

    pub fn completed(&mut self, sub: String, n: u64, last: Option<DateTime<Utc>>) {
        let entry = self.entry(sub);
        entry.executions_completed += n;
        Self::touch(entry, last);
    }

    pub fn steps(&mut self, sub: String, n: u64, last: Option<DateTime<Utc>>) {
        let entry = self.entry(sub);
        entry.steps_executed += n;
        Self::touch(entry, last);
    }

    pub fn finish(self) -> Vec<SubTenantUsage> {
        self.rows.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_admit_up_to_the_limit_and_reject_beyond() {
        let limits = Some(SubTenantLimits {
            max_executions_per_month: Some(10),
            max_concurrent: Some(2),
        });
        assert!(check_caps(limits, 1, 0, 1).is_ok());
        let err = check_caps(limits, 2, 0, 1).unwrap_err();
        assert!(
            matches!(err, StorageError::QuotaExceeded(m) if m.starts_with(SUB_TENANT_QUOTA_PREFIX))
        );
        assert!(check_caps(limits, 0, 9, 1).is_ok());
        assert!(check_caps(limits, 0, 10, 1).is_err());
        assert!(check_caps(None, u64::MAX, u64::MAX, 1).is_ok());
    }

    #[test]
    fn usage_accumulator_merges_and_keeps_latest_activity() {
        let t1 = Utc::now();
        let t2 = t1 + chrono::Duration::seconds(5);
        let mut acc = UsageAccumulator::default();
        acc.started("b".into(), 2, Some(t1));
        acc.completed("b".into(), 1, Some(t2));
        acc.steps("a".into(), 4, None);
        let rows = acc.finish();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].sub_tenant, "a");
        assert_eq!(rows[0].last_active_at, None);
        assert_eq!(rows[1].executions_started, 2);
        assert_eq!(rows[1].executions_completed, 1);
        assert_eq!(rows[1].last_active_at, Some(t2));
    }
}
