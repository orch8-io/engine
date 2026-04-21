use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::auth::{enforce_tenant_access, OptionalTenant};
use crate::error::ApiError;

use std::sync::Arc;

use orch8_engine::circuit_breaker::CircuitBreakerRegistry;
use orch8_types::ids::TenantId;

/// If the caller has a tenant header set, ensure the path's `tenant_id`
/// matches it. Otherwise any authenticated caller could list/reset/read
/// another tenant's breaker state simply by changing the URL segment.
///
/// Returns `NotFound` (not `Forbidden`) on mismatch to mirror
/// `enforce_tenant_access` elsewhere in the API — avoids leaking which
/// tenants have breakers registered.
fn guard_tenant(tenant_ctx: &OptionalTenant, url_tenant_id: &str) -> Result<TenantId, ApiError> {
    let url_tid = TenantId(url_tenant_id.to_string());
    enforce_tenant_access(tenant_ctx, &url_tid, "circuit_breaker")?;
    Ok(url_tid)
}

#[derive(Clone)]
pub struct CircuitBreakerState {
    pub registry: Arc<CircuitBreakerRegistry>,
}

pub fn routes() -> Router<CircuitBreakerState> {
    Router::new()
        // Cross-tenant admin list. Keep unscoped for operator tooling.
        .route("/circuit-breakers", get(list_all_breakers))
        // Per-tenant operations — align with the rest of the API surface
        // which scopes tenant-owned resources under `/tenants/{tenant_id}/...`.
        .route(
            "/tenants/{tenant_id}/circuit-breakers",
            get(list_breakers_for_tenant),
        )
        .route(
            "/tenants/{tenant_id}/circuit-breakers/{handler}",
            get(get_breaker),
        )
        .route(
            "/tenants/{tenant_id}/circuit-breakers/{handler}/reset",
            post(reset_breaker),
        )
}

#[utoipa::path(
    get,
    path = "/circuit-breakers",
    responses((status = 200, body = Vec<orch8_types::circuit_breaker::CircuitBreakerState>))
)]
async fn list_all_breakers(
    State(state): State<CircuitBreakerState>,
    tenant_ctx: OptionalTenant,
) -> impl IntoResponse {
    // A caller with an `x-tenant-id` header should see their tenant's
    // breakers only — `list_all` would otherwise leak cross-tenant
    // handler names and state via the unscoped admin endpoint.
    if let Some(axum::Extension(ctx)) = tenant_ctx {
        return Json(state.registry.list_for_tenant(&ctx.tenant_id));
    }
    Json(state.registry.list_all())
}

#[utoipa::path(
    get,
    path = "/tenants/{tenant_id}/circuit-breakers",
    params(("tenant_id" = String, Path, description = "Tenant id")),
    responses((status = 200, body = Vec<orch8_types::circuit_breaker::CircuitBreakerState>))
)]
async fn list_breakers_for_tenant(
    State(state): State<CircuitBreakerState>,
    tenant_ctx: OptionalTenant,
    Path(tenant_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let tid = guard_tenant(&tenant_ctx, &tenant_id)?;
    Ok(Json(state.registry.list_for_tenant(&tid)))
}

#[utoipa::path(
    get,
    path = "/tenants/{tenant_id}/circuit-breakers/{handler}",
    params(
        ("tenant_id" = String, Path, description = "Tenant id"),
        ("handler" = String, Path, description = "Handler name"),
    ),
    responses(
        (status = 200, body = orch8_types::circuit_breaker::CircuitBreakerState),
        (status = 404, description = "Handler not found"),
    )
)]
async fn get_breaker(
    State(state): State<CircuitBreakerState>,
    tenant_ctx: OptionalTenant,
    Path((tenant_id, handler)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let tid = guard_tenant(&tenant_ctx, &tenant_id)?;
    state
        .registry
        .get(&tid, &handler)
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("circuit_breaker {handler}")))
}

#[utoipa::path(
    post,
    path = "/tenants/{tenant_id}/circuit-breakers/{handler}/reset",
    params(
        ("tenant_id" = String, Path, description = "Tenant id"),
        ("handler" = String, Path, description = "Handler name"),
    ),
    responses((status = 200, description = "Circuit breaker reset"))
)]
async fn reset_breaker(
    State(state): State<CircuitBreakerState>,
    tenant_ctx: OptionalTenant,
    Path((tenant_id, handler)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let tid = guard_tenant(&tenant_ctx, &tenant_id)?;
    state.registry.reset(&tid, &handler);
    Ok(StatusCode::OK)
}
