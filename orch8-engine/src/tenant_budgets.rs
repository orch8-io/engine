//! Tenant spend budgets (v2): per-tenant, optionally per-model, daily or
//! monthly USD limits over the estimated LLM cost the engine already records
//! as `usage_events` (priced with [`crate::model_pricing`], the same table
//! `GET /usage` uses).
//!
//! - **Soft thresholds** (default 50/80/100%) each produce exactly one
//!   durable [`BudgetAlert`] per budget period (insert-if-absent), plus a
//!   `tracing` event named [`BUDGET_THRESHOLD_EVENT`] at `WARN` on target
//!   `orch8::budget`. Alert destinations consume either.
//! - **Hard cap** (default on): once spend reaches 100% of the limit, new
//!   `llm_call` provider dispatches for matching models fail closed with a
//!   permanent `tenant_budget_exceeded` error. Cache hits still succeed —
//!   they cost nothing.
//!
//! This layers on — does not replace — the per-instance `budget`
//! (token/step caps that pause one execution): the instance budget bounds a
//! single run, the tenant budget bounds aggregate spend across all runs.
//! Both read the same `usage_events` rows; only `kind = "llm_tokens"` is
//! billable (cache-hit savings are recorded as `llm_cache_hit`).
//!
//! Enforcement reads a per-tenant status snapshot cached for
//! [`STATUS_CACHE_TTL`]; every `llm_call` completion in this process
//! refreshes it, so the cap is exact within one node and at most a few
//! seconds stale across nodes ("one more call" semantics, like instance
//! budgets). Usage from models missing from the pricing table is not
//! counted (reported as `unpriced_events`).

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use moka::future::Cache;
use serde_json::json;
use tracing::warn;

use orch8_storage::{StorageBackend, UsageAggregate};
use orch8_types::ai::{
    BUDGET_THRESHOLD_EVENT, BudgetAlert, BudgetPeriod, BudgetStatus, TenantBudget,
};
use orch8_types::error::{StepError, StorageError};

/// Billable LLM usage kind.
pub const BILLABLE_USAGE_KIND: &str = "llm_tokens";
/// Usage kind recording tokens a response-cache hit saved (not billable).
pub const CACHE_HIT_USAGE_KIND: &str = "llm_cache_hit";
/// How long enforcement may reuse a tenant's status snapshot.
pub const STATUS_CACHE_TTL: Duration = Duration::from_secs(5);
/// Error code carried in `details.code` of a hard-cap rejection.
pub const BUDGET_EXCEEDED_CODE: &str = "tenant_budget_exceeded";

type StatusCache = Cache<String, Arc<Vec<BudgetStatus>>>;

fn status_cache() -> &'static StatusCache {
    static CACHE: OnceLock<StatusCache> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(10_000)
            .time_to_live(STATUS_CACHE_TTL)
            .build()
    })
}

/// Drop the cached status snapshot for `tenant_id` (after a budget write).
pub async fn invalidate(tenant_id: &str) {
    status_cache().invalidate(tenant_id).await;
}

/// Estimated USD spend of the billable aggregates matching `budget`'s model
/// filter, plus the number of matching events whose model has no price.
#[must_use]
pub fn spend_for_budget(aggregates: &[UsageAggregate], budget: &TenantBudget) -> (f64, i64) {
    let mut spend = 0.0;
    let mut unpriced = 0;
    for agg in aggregates
        .iter()
        .filter(|a| a.kind == BILLABLE_USAGE_KIND && budget.matches_model(&a.model))
    {
        match crate::model_pricing::estimate_cost_usd(
            &agg.model,
            agg.input_tokens,
            agg.output_tokens,
        ) {
            Some(cost) => spend += cost,
            None => unpriced += agg.events,
        }
    }
    (spend, unpriced)
}

/// One period's usage aggregation, shared by every budget of that period.
struct Window {
    period: BudgetPeriod,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    usage: Vec<UsageAggregate>,
}

/// Current-period status of every budget of `tenant_id`, computed fresh.
pub async fn budget_statuses(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    now: DateTime<Utc>,
) -> Result<Vec<BudgetStatus>, StorageError> {
    let budgets = storage.list_tenant_budgets(tenant_id).await?;
    if budgets.is_empty() {
        return Ok(Vec::new());
    }
    // At most one usage aggregation per distinct period.
    let mut windows: Vec<Window> = Vec::new();
    for period in [BudgetPeriod::Daily, BudgetPeriod::Monthly] {
        if budgets.iter().any(|b| b.period == period) {
            let (start, end) = period.window(now);
            let usage = storage.query_usage(tenant_id, start, end).await?;
            windows.push(Window {
                period,
                start,
                end,
                usage,
            });
        }
    }
    Ok(budgets
        .into_iter()
        .filter_map(|budget| {
            let w = windows.iter().find(|w| w.period == budget.period)?;
            let (spend, unpriced) = spend_for_budget(&w.usage, &budget);
            Some(BudgetStatus::compute(
                budget, w.start, w.end, spend, unpriced,
            ))
        })
        .collect())
}

/// Status snapshot for enforcement (cached for [`STATUS_CACHE_TTL`]).
pub async fn cached_budget_statuses(
    storage: &dyn StorageBackend,
    tenant_id: &str,
) -> Result<Arc<Vec<BudgetStatus>>, StorageError> {
    if let Some(hit) = status_cache().get(tenant_id).await {
        return Ok(hit);
    }
    let fresh = Arc::new(budget_statuses(storage, tenant_id, Utc::now()).await?);
    status_cache()
        .insert(tenant_id.to_string(), Arc::clone(&fresh))
        .await;
    Ok(fresh)
}

/// The permanent error for a dispatch blocked by `status`, if `model` is
/// governed by a blocking (hard-capped, exhausted) budget.
#[must_use]
pub fn blocking_error(statuses: &[BudgetStatus], model: &str) -> Option<StepError> {
    let status = statuses
        .iter()
        .find(|s| s.blocking && s.budget.matches_model(model))?;
    let b = &status.budget;
    let scope = b
        .model
        .as_deref()
        .map_or_else(|| "all models".to_string(), |m| format!("models '{m}*'"));
    Some(StepError::Permanent {
        message: format!(
            "tenant budget exceeded: tenant '{}' {} budget for {scope} spent ${:.4} of ${:.4}; \
             new llm_call dispatches are blocked until {}",
            b.tenant_id,
            b.period.as_str(),
            status.spend_usd,
            b.limit_usd,
            status.period_end.to_rfc3339(),
        ),
        details: Some(json!({
            "code": BUDGET_EXCEEDED_CODE,
            "budget_id": b.id,
            "tenant_id": b.tenant_id,
            "model": b.model,
            "period": b.period,
            "spend_usd": status.spend_usd,
            "limit_usd": b.limit_usd,
            "resets_at": status.period_end,
        })),
    })
}

/// Fail closed when `model` is blocked by a tenant budget. Storage errors
/// are retryable (the check could not be made, so nothing is dispatched).
pub async fn enforce(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    model: &str,
) -> Result<(), StepError> {
    let statuses = cached_budget_statuses(storage, tenant_id)
        .await
        .map_err(|e| StepError::Retryable {
            message: format!("tenant budget check failed: {e}"),
            details: None,
        })?;
    match blocking_error(&statuses, model) {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Recompute statuses after new usage and record each newly crossed
/// threshold once per period. Returns the alerts this call created.
/// Best-effort: failures are logged, never propagated to the step.
pub async fn evaluate_thresholds(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    now: DateTime<Utc>,
) -> Vec<BudgetAlert> {
    // Cheap exit for tenants without budgets (cached for a few seconds).
    match status_cache().get(tenant_id).await {
        Some(cached) if cached.is_empty() => return Vec::new(),
        _ => {}
    }
    let statuses = match budget_statuses(storage, tenant_id, now).await {
        Ok(s) => s,
        Err(e) => {
            warn!(tenant_id, error = %e, "tenant budgets: status evaluation failed");
            return Vec::new();
        }
    };
    let mut created = Vec::new();
    for status in &statuses {
        for threshold in status.crossed_thresholds() {
            let alert = BudgetAlert {
                id: uuid::Uuid::now_v7(),
                event: BUDGET_THRESHOLD_EVENT.to_string(),
                tenant_id: tenant_id.to_string(),
                budget_id: status.budget.id,
                model: status.budget.model.clone(),
                period: status.budget.period,
                period_start: status.period_start,
                threshold_percent: threshold,
                spend_usd: status.spend_usd,
                limit_usd: status.budget.limit_usd,
                blocking: threshold >= 100 && status.budget.hard_cap,
                created_at: now,
            };
            match storage.record_budget_alert(&alert).await {
                Ok(true) => {
                    warn!(
                        target: "orch8::budget",
                        event = BUDGET_THRESHOLD_EVENT,
                        tenant_id,
                        budget_id = %alert.budget_id,
                        threshold_percent = threshold,
                        spend_usd = alert.spend_usd,
                        limit_usd = alert.limit_usd,
                        period = alert.period.as_str(),
                        blocking = alert.blocking,
                        "budget.threshold_crossed"
                    );
                    created.push(alert);
                }
                Ok(false) => {} // already alerted this period
                Err(e) => warn!(tenant_id, error = %e, "tenant budgets: alert record failed"),
            }
        }
    }
    status_cache()
        .insert(tenant_id.to_string(), Arc::new(statuses))
        .await;
    created
}

/// Validate and persist a budget, then drop the cached snapshot so
/// enforcement sees it immediately in this process.
pub async fn save_budget(
    storage: &dyn StorageBackend,
    budget: &TenantBudget,
) -> Result<(), StorageError> {
    budget.validate().map_err(StorageError::Constraint)?;
    storage.upsert_tenant_budget(budget).await?;
    invalidate(&budget.tenant_id).await;
    Ok(())
}

/// Delete a budget and drop the cached snapshot.
pub async fn delete_budget(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    id: uuid::Uuid,
) -> Result<bool, StorageError> {
    let deleted = storage.delete_tenant_budget(tenant_id, id).await?;
    invalidate(tenant_id).await;
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::{AiStore, UsageEvent};
    use orch8_types::ai::{BudgetState, default_budget_thresholds};

    fn budget(tenant: &str, model: Option<&str>, limit: f64, hard: bool) -> TenantBudget {
        let now = Utc::now();
        TenantBudget {
            id: uuid::Uuid::now_v7(),
            tenant_id: tenant.into(),
            model: model.map(Into::into),
            period: BudgetPeriod::Monthly,
            limit_usd: limit,
            thresholds: default_budget_thresholds(),
            hard_cap: hard,
            created_at: now,
            updated_at: now,
        }
    }

    async fn usage(
        storage: &dyn StorageBackend,
        tenant: &str,
        kind: &str,
        model: &str,
        tokens: i64,
    ) {
        storage
            .record_usage_event(&UsageEvent {
                tenant_id: tenant.into(),
                instance_id: None,
                block_id: None,
                kind: kind.into(),
                model: model.into(),
                input_tokens: tokens,
                output_tokens: 0,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
    }

    fn tenant() -> String {
        format!("budget-{}", uuid::Uuid::now_v7())
    }

    #[tokio::test]
    async fn spend_counts_only_billable_priced_matching_usage() {
        let s = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let t = tenant();
        // gpt-4o input: $2.50 / 1M → 400k tokens = $1.00
        usage(&s, &t, BILLABLE_USAGE_KIND, "gpt-4o", 400_000).await;
        usage(&s, &t, CACHE_HIT_USAGE_KIND, "gpt-4o", 4_000_000).await;
        usage(&s, &t, BILLABLE_USAGE_KIND, "claude-opus-5", 1_000_000).await;
        // Matches the budget's `gpt-4o` prefix but has no price entry.
        usage(&s, &t, BILLABLE_USAGE_KIND, "gpt-4oz", 1).await;
        save_budget(&s, &budget(&t, Some("gpt-4o"), 2.0, true))
            .await
            .unwrap();

        let statuses = budget_statuses(&s, &t, Utc::now()).await.unwrap();
        assert_eq!(statuses.len(), 1);
        let st = &statuses[0];
        assert!((st.spend_usd - 1.0).abs() < 1e-9, "spend {}", st.spend_usd);
        assert_eq!(st.state, BudgetState::Warning);
        assert_eq!(st.unpriced_events, 1, "gpt-4oz is unpriced");
    }

    #[tokio::test]
    async fn hard_cap_blocks_matching_models_only_and_alerts_once() {
        let s = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let t = tenant();
        save_budget(&s, &budget(&t, Some("gpt-4o"), 1.0, true))
            .await
            .unwrap();
        enforce(&s, &t, "gpt-4o").await.unwrap();

        usage(&s, &t, BILLABLE_USAGE_KIND, "gpt-4o", 400_000).await; // $1.00 = 100%
        let alerts = evaluate_thresholds(&s, &t, Utc::now()).await;
        let mut crossed: Vec<u8> = alerts.iter().map(|a| a.threshold_percent).collect();
        crossed.sort_unstable();
        assert_eq!(crossed, vec![50, 80, 100]);
        assert!(alerts.iter().any(|a| a.blocking));
        assert!(
            evaluate_thresholds(&s, &t, Utc::now()).await.is_empty(),
            "once per period"
        );
        assert_eq!(s.list_budget_alerts(&t, 10).await.unwrap().len(), 3);

        let err = enforce(&s, &t, "gpt-4o-2024-08-06").await.unwrap_err();
        let StepError::Permanent { details, message } = err else {
            panic!("hard cap must be permanent")
        };
        assert!(message.contains("tenant budget exceeded"));
        assert_eq!(details.unwrap()["code"], BUDGET_EXCEEDED_CODE);
        enforce(&s, &t, "claude-opus-5")
            .await
            .expect("other models unaffected");
    }

    #[tokio::test]
    async fn soft_only_budget_never_blocks_and_no_budget_is_free() {
        let s = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let t = tenant();
        enforce(&s, &t, "gpt-4o").await.unwrap();
        assert!(evaluate_thresholds(&s, &t, Utc::now()).await.is_empty());
        save_budget(&s, &budget(&t, None, 0.5, false))
            .await
            .unwrap();
        usage(&s, &t, BILLABLE_USAGE_KIND, "gpt-4o", 400_000).await;
        assert_eq!(evaluate_thresholds(&s, &t, Utc::now()).await.len(), 3);
        enforce(&s, &t, "gpt-4o")
            .await
            .expect("soft budget only alerts");
        let mut bad = budget(&t, None, -1.0, true);
        bad.thresholds = vec![];
        assert!(save_budget(&s, &bad).await.is_err());
    }
}
