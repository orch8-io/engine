use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::error::ApiError;

use std::sync::Arc;

use orch8_engine::circuit_breaker::CircuitBreakerRegistry;
use orch8_types::ids::TenantId;

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
async fn list_all_breakers(State(state): State<CircuitBreakerState>) -> impl IntoResponse {
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
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    Json(state.registry.list_for_tenant(&TenantId(tenant_id)))
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
    Path((tenant_id, handler)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .registry
        .get(&TenantId(tenant_id), &handler)
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
    Path((tenant_id, handler)): Path<(String, String)>,
) -> impl IntoResponse {
    state.registry.reset(&TenantId(tenant_id), &handler);
    StatusCode::OK
}
