//! Usage/cost read endpoint.
//!
//! Surfaces the engine-captured `usage_events` (LLM token consumption emitted
//! by `llm_call`/`agent`) as a tenant-scoped aggregation so a control plane can
//! build a cost dashboard without scanning block outputs.

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use crate::auth::TenantContext;
use crate::error::ApiError;
use crate::AppState;

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
        (status = 200, description = "Usage aggregated by (kind, model)", body = serde_json::Value),
        (status = 400, description = "No tenant resolvable"),
    )
)]
pub async fn get_usage(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<TenantContext>>,
    Query(q): Query<UsageQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // A header-scoped caller is locked to its own tenant (the `?tenant=` param
    // is ignored, so it can't read another tenant's usage). Only an unscoped
    // caller may select a tenant via the query param.
    let tenant = match &tenant_ctx {
        Some(axum::Extension(ctx)) => ctx.tenant_id.as_str().to_string(),
        None => q.tenant.clone().ok_or_else(|| {
            ApiError::InvalidArgument(
                "usage requires a tenant (X-Tenant-Id header or ?tenant=)".into(),
            )
        })?,
    };

    let end = q.end.unwrap_or_else(Utc::now);
    let start = q.start.unwrap_or_else(|| end - Duration::days(30));

    let usage = state
        .storage
        .query_usage(&tenant, start, end)
        .await
        .map_err(|e| ApiError::from_storage(e, "usage"))?;

    Ok(Json(serde_json::json!({
        "tenant": tenant,
        "start": start,
        "end": end,
        "usage": usage,
    })))
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/usage", get(get_usage))
}
