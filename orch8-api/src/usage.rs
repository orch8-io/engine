//! Usage/cost read endpoint.
//!
//! Surfaces the engine-captured `usage_events` (LLM token consumption emitted
//! by `llm_call`/`agent`) as a tenant-scoped aggregation so a control plane can
//! build a cost dashboard without scanning block outputs.
//!
//! Also reports the tenant's spend budgets for the current period
//! (`budgets`) and what the `llm_call` response cache saved in the window
//! (`cache_savings`, from `llm_cache_hit` usage rows — never billed).

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use crate::AppState;
use crate::api_keys::require_admin;
use crate::auth::{OptionalAdmin, TenantContext};
use crate::error::ApiError;
use crate::model_pricing;
use orch8_engine::tenant_budgets;

/// Round a USD amount to 6 decimal places for stable API output.
fn round6(v: f64) -> f64 {
    if v.abs() > f64::MAX / 1_000_000.0 {
        // Multiplying would overflow, and six-decimal rounding cannot change
        // a value this large at f64 precision anyway.
        v
    } else {
        (v * 1_000_000.0).round() / 1_000_000.0
    }
}

fn resolve_window(
    start: Option<DateTime<Utc>>,
    end: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ApiError> {
    let start = match start {
        Some(start) => start,
        None => end.checked_sub_signed(Duration::days(30)).ok_or_else(|| {
            ApiError::InvalidArgument("end is too early for the default 30-day usage window".into())
        })?,
    };
    if start > end {
        return Err(ApiError::InvalidArgument(
            "usage start must not be after end".into(),
        ));
    }
    Ok((start, end))
}

/// Query params for [`get_usage`].
#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    /// Tenant to report on. Honored only when no `X-Tenant-Id` header is present
    /// (i.e. an unscoped/admin caller); a header-scoped caller always reports on
    /// its own tenant and cannot read another's.
    pub tenant: Option<String>,
    /// Window start (RFC 3339). Defaults to 30 days before `end`.
    pub start: Option<DateTime<Utc>>,
    /// Window end (RFC 3339). Defaults to now.
    pub end: Option<DateTime<Utc>>,
}

#[utoipa::path(
    get, path = "/usage", tag = "usage",
    params(
        ("tenant" = Option<String>, Query, description = "Tenant (admin/unscoped callers only)"),
        ("start" = Option<String>, Query, description = "Window start (RFC 3339)"),
        ("end" = Option<String>, Query, description = "Window end (RFC 3339)"),
    ),
    responses(
        (status = 200, description = "Usage aggregated by (kind, model) with estimated USD costs, cache savings and current budget status", body = serde_json::Value),
        (status = 400, description = "No tenant resolvable"),
    )
)]
pub async fn get_usage(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<TenantContext>>,
    admin_ctx: OptionalAdmin,
    Query(q): Query<UsageQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // A header-scoped caller is locked to its own tenant (the `?tenant=` param
    // is ignored, so it can't read another tenant's usage). Only an unscoped
    // caller may select a tenant via the query param.
    let tenant = if let Some(axum::Extension(ctx)) = &tenant_ctx {
        ctx.tenant_id.as_str().to_string()
    } else {
        require_admin(&admin_ctx)?;
        q.tenant.clone().ok_or_else(|| {
            ApiError::InvalidArgument(
                "usage requires a tenant (X-Tenant-Id header or ?tenant=)".into(),
            )
        })?
    };

    let (start, end) = resolve_window(q.start, q.end.unwrap_or_else(Utc::now))?;

    let usage = state
        .storage
        .query_usage(&tenant, start, end)
        .await
        .map_err(|e| ApiError::from_storage(e, "usage"))?;

    // Attach an estimated USD cost to each aggregate (null for unknown
    // models) plus a window-wide total over the known ones. Costs are list
    // prices from the static pricing table — hence `cost_is_estimate`.
    // Cache-hit rows are savings, not spend: they carry `saved_usd` instead
    // and are summed into `cache_savings`, never into `total_cost_usd`.
    let mut total_cost_usd = Some(0.0_f64);
    let mut total_cost_is_complete = true;
    let mut savings = CacheSavings::default();
    let usage: Vec<serde_json::Value> = usage
        .into_iter()
        .map(|u| {
            let estimate =
                model_pricing::estimate_cost_usd(&u.model, u.input_tokens, u.output_tokens);
            if u.kind == tenant_budgets::CACHE_HIT_USAGE_KIND {
                savings.add(&u, estimate);
                return serde_json::json!({
                    "kind": u.kind,
                    "model": u.model,
                    "events": u.events,
                    "input_tokens": u.input_tokens,
                    "output_tokens": u.output_tokens,
                    "cost_usd": 0.0,
                    "saved_usd": estimate.map(round6),
                });
            }
            if let Some(c) = estimate {
                total_cost_usd = total_cost_usd.and_then(|total| {
                    let next = total + c;
                    next.is_finite().then_some(next)
                });
            } else {
                total_cost_is_complete = false;
            }
            serde_json::json!({
                "kind": u.kind,
                "model": u.model,
                "events": u.events,
                "input_tokens": u.input_tokens,
                "output_tokens": u.output_tokens,
                "cost_usd": estimate.map(round6),
            })
        })
        .collect();

    // Budgets are always reported for their *current* period, independent
    // of the requested window.
    let budgets = tenant_budgets::budget_statuses(state.storage.as_ref(), &tenant, Utc::now())
        .await
        .map_err(|e| ApiError::from_storage(e, "usage"))?;

    Ok(Json(serde_json::json!({
        "tenant": tenant,
        "start": start,
        "end": end,
        "usage": usage,
        "total_cost_usd": total_cost_usd.map(round6),
        "total_cost_is_complete": total_cost_is_complete && total_cost_usd.is_some(),
        "cost_is_estimate": true,
        "cache_savings": savings.to_json(),
        "budgets": budgets,
    })))
}

/// Window totals of `llm_cache_hit` usage: tokens (and estimated USD) the
/// response cache saved.
#[derive(Default)]
struct CacheSavings {
    hits: i64,
    input_tokens: i64,
    output_tokens: i64,
    saved_usd: f64,
    saved_is_complete: bool,
    any: bool,
}

impl CacheSavings {
    fn add(&mut self, u: &orch8_storage::UsageAggregate, estimate: Option<f64>) {
        if !self.any {
            self.any = true;
            self.saved_is_complete = true;
        }
        self.hits = self.hits.saturating_add(u.events);
        self.input_tokens = self.input_tokens.saturating_add(u.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(u.output_tokens);
        match estimate {
            Some(c) => self.saved_usd += c,
            None => self.saved_is_complete = false,
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "hits": self.hits,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "saved_usd": round6(self.saved_usd),
            "saved_is_complete": self.saved_is_complete || !self.any,
        })
    }
}

/// Query params for [`purge_llm_cache`].
#[derive(Debug, Deserialize)]
pub struct PurgeQuery {
    /// Tenant (unscoped/admin callers only).
    pub tenant: Option<String>,
}

#[utoipa::path(
    delete, path = "/llm-cache", tag = "usage",
    params(("tenant" = Option<String>, Query, description = "Tenant (admin/unscoped callers only)")),
    responses(
        (status = 200, description = "Entries removed: {\"deleted\": n}", body = serde_json::Value),
        (status = 400, description = "No tenant resolvable"),
    )
)]
pub async fn purge_llm_cache(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<TenantContext>>,
    admin_ctx: OptionalAdmin,
    Query(q): Query<PurgeQuery>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant = if let Some(axum::Extension(ctx)) = &tenant_ctx {
        ctx.tenant_id.as_str().to_string()
    } else {
        require_admin(&admin_ctx)?;
        q.tenant.clone().ok_or_else(|| {
            ApiError::InvalidArgument(
                "purge requires a tenant (X-Tenant-Id header or ?tenant=)".into(),
            )
        })?
    };
    let deleted = state
        .storage
        .purge_llm_cache(&tenant)
        .await
        .map_err(|e| ApiError::from_storage(e, "llm_cache"))?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "deleted": deleted })),
    ))
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/usage", get(get_usage))
        .route("/llm-cache", delete(purge_llm_cache))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_window_handles_date_bounds_and_preserves_explicit_start() {
        assert!(matches!(
            resolve_window(None, DateTime::<Utc>::MIN_UTC),
            Err(ApiError::InvalidArgument(_))
        ));
        let lower = DateTime::<Utc>::MIN_UTC;
        assert_eq!(resolve_window(Some(lower), lower).unwrap(), (lower, lower));
        let end = DateTime::<Utc>::MAX_UTC;
        let (start, actual_end) = resolve_window(None, end).unwrap();
        assert_eq!(actual_end, end);
        assert_eq!(end - start, Duration::days(30));
        assert!(matches!(
            resolve_window(Some(end), start),
            Err(ApiError::InvalidArgument(_))
        ));
    }

    #[test]
    fn rounding_large_finite_cost_stays_finite() {
        assert_eq!(round6(f64::MAX).to_bits(), f64::MAX.to_bits());
    }
}
