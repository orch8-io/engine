//! Tenant placement policies and global rate budgets (see
//! `docs/PLACEMENT.md`).
//!
//! - `GET|PUT /placement/policies` — the tenant's ordered policy list.
//!   Policies are matched at dispatch and compiled into the step's
//!   capability requirements; residency is never relaxed.
//! - `GET /rate-budgets`, `PUT|DELETE /rate-budgets/{key}` — durable token
//!   buckets shared by every node; steps declare `"rate_budget": "<key>"`.
//!
//! The tenant is the `X-Tenant-Id` principal, or `?tenant_id=` for the root
//! key (default `default`).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::ids::TenantId;
use orch8_types::placement::{PlacementPolicies, RateBudget};

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/placement/policies", get(get_policies).put(put_policies))
        .route("/rate-budgets", get(list_budgets))
        .route("/rate-budgets/{key}", put(put_budget).delete(delete_budget))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct TenantQuery {
    tenant_id: Option<String>,
}

fn tenant(tenant_ctx: &OptionalTenant, query: &TenantQuery) -> Result<TenantId, ApiError> {
    crate::auth::enforce_tenant_create(
        tenant_ctx,
        &TenantId::unchecked(query.tenant_id.clone().unwrap_or_default()),
    )
}

#[utoipa::path(get, path = "/placement/policies", tag = "placement",
    params(("tenant_id" = Option<String>, Query, description = "Tenant (root key only; defaults to the X-Tenant-Id principal)")),
    responses((status = 200, description = "The tenant's placement policies", body = PlacementPolicies))
)]
pub(crate) async fn get_policies(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(query): Query<TenantQuery>,
) -> Result<Json<PlacementPolicies>, ApiError> {
    let tenant_id = tenant(&tenant_ctx, &query)?;
    let policies = state
        .storage
        .get_placement_policies(&tenant_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "placement_policies"))?;
    Ok(Json(policies))
}

#[utoipa::path(put, path = "/placement/policies", tag = "placement",
    params(("tenant_id" = Option<String>, Query, description = "Tenant (root key only; defaults to the X-Tenant-Id principal)")),
    request_body = PlacementPolicies,
    responses(
        (status = 200, description = "Policies replaced", body = PlacementPolicies),
        (status = 400, description = "Invalid policies"),
    )
)]
pub(crate) async fn put_policies(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(query): Query<TenantQuery>,
    Json(policies): Json<PlacementPolicies>,
) -> Result<Json<PlacementPolicies>, ApiError> {
    let tenant_id = tenant(&tenant_ctx, &query)?;
    policies.validate().map_err(ApiError::InvalidArgument)?;
    state
        .storage
        .put_placement_policies(&tenant_id, &policies)
        .await
        .map_err(|e| ApiError::from_storage(e, "placement_policies"))?;
    orch8_engine::step_placement::invalidate_policies(&tenant_id).await;
    Ok(Json(policies))
}

/// `GET /rate-budgets` response.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct RateBudgetList {
    items: Vec<RateBudget>,
}

/// `PUT /rate-budgets/{key}` body.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct RateBudgetRequest {
    /// Bucket size: the maximum burst.
    capacity: u32,
    /// Tokens refilled per second (e.g. `25` for 25 req/s, `0.5` for 30/min).
    refill_per_sec: f64,
}

#[utoipa::path(get, path = "/rate-budgets", tag = "placement",
    params(("tenant_id" = Option<String>, Query, description = "Tenant (root key only)")),
    responses((status = 200, description = "The tenant's rate budgets", body = RateBudgetList))
)]
pub(crate) async fn list_budgets(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(query): Query<TenantQuery>,
) -> Result<Json<RateBudgetList>, ApiError> {
    let tenant_id = tenant(&tenant_ctx, &query)?;
    let items = state
        .storage
        .list_rate_budgets(&tenant_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "rate_budget"))?;
    Ok(Json(RateBudgetList { items }))
}

#[utoipa::path(put, path = "/rate-budgets/{key}", tag = "placement",
    params(
        ("key" = String, Path, description = "Budget key, e.g. `stripe-api`"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (root key only)"),
    ),
    request_body = RateBudgetRequest,
    responses(
        (status = 200, description = "Budget created (full) or reshaped", body = RateBudget),
        (status = 400, description = "Invalid key or bucket parameters"),
    )
)]
pub(crate) async fn put_budget(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(key): Path<String>,
    Query(query): Query<TenantQuery>,
    Json(req): Json<RateBudgetRequest>,
) -> Result<Json<RateBudget>, ApiError> {
    let tenant_id = tenant(&tenant_ctx, &query)?;
    orch8_types::placement::validate_rate_budget_key(&key).map_err(ApiError::InvalidArgument)?;
    orch8_types::placement::validate_rate_budget(req.capacity, req.refill_per_sec)
        .map_err(ApiError::InvalidArgument)?;
    let budget = RateBudget {
        tenant_id: tenant_id.as_str().to_string(),
        key,
        capacity: req.capacity,
        refill_per_sec: req.refill_per_sec,
        tokens: f64::from(req.capacity),
        updated_at: Utc::now(),
    };
    let stored = state
        .storage
        .upsert_rate_budget(&budget)
        .await
        .map_err(|e| ApiError::from_storage(e, "rate_budget"))?;
    Ok(Json(stored))
}

#[utoipa::path(delete, path = "/rate-budgets/{key}", tag = "placement",
    params(
        ("key" = String, Path, description = "Budget key"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (root key only)"),
    ),
    responses((status = 204, description = "Deleted"), (status = 404, description = "Not found"))
)]
pub(crate) async fn delete_budget(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(key): Path<String>,
    Query(query): Query<TenantQuery>,
) -> Result<StatusCode, ApiError> {
    let tenant_id = tenant(&tenant_ctx, &query)?;
    let deleted = state
        .storage
        .delete_rate_budget(&tenant_id, &key)
        .await
        .map_err(|e| ApiError::from_storage(e, "rate_budget"))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("rate_budget {key}")))
    }
}
