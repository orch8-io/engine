use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::auth::{OptionalTenant, enforce_tenant_access};
use crate::error::ApiError;

use orch8_types::circuit_breaker::CircuitBreakerState;
use orch8_types::ids::TenantId;

const REVIEWED_RESET: &str = "x-orch8-breaker-reviewed-reset";

/// If the caller has a tenant header set, ensure the path's `tenant_id`
/// matches it. Otherwise any authenticated caller could list/reset/read
/// another tenant's breaker state simply by changing the URL segment.
///
/// Returns `NotFound` (not `Forbidden`) on mismatch to mirror
/// `enforce_tenant_access` elsewhere in the API — avoids leaking which
/// tenants have breakers registered.
fn guard_tenant(tenant_ctx: &OptionalTenant, url_tenant_id: &str) -> Result<TenantId, ApiError> {
    let url_tid = TenantId::unchecked(url_tenant_id.to_string());
    enforce_tenant_access(tenant_ctx, &url_tid, "circuit_breaker")?;
    Ok(url_tid)
}

pub fn routes() -> Router<crate::AppState> {
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
        .route(
            "/tenants/{tenant_id}/circuit-breakers/{handler}/reset-reviewed",
            post(reset_reviewed_breaker),
        )
}

#[utoipa::path(
    get,
    path = "/circuit-breakers",
    responses((status = 200, body = Vec<orch8_types::circuit_breaker::CircuitBreakerState>))
)]
async fn list_all_breakers(
    State(state): State<crate::AppState>,
    tenant_ctx: OptionalTenant,
    admin_ctx: crate::auth::OptionalAdmin,
) -> impl IntoResponse {
    let Some(ref registry) = state.circuit_breakers else {
        return Json(Vec::new());
    };

    if admin_ctx.is_some() {
        return Json(registry.list_all());
    } else if let Some(axum::Extension(ctx)) = tenant_ctx {
        return Json(registry.list_for_tenant(&ctx.tenant_id));
    }

    Json(Vec::new())
}

#[utoipa::path(
    get,
    path = "/tenants/{tenant_id}/circuit-breakers",
    params(("tenant_id" = String, Path, description = "Tenant id")),
    responses((status = 200, body = Vec<orch8_types::circuit_breaker::CircuitBreakerState>))
)]
async fn list_breakers_for_tenant(
    State(state): State<crate::AppState>,
    tenant_ctx: OptionalTenant,
    Path(tenant_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let tid = guard_tenant(&tenant_ctx, &tenant_id)?;
    let registry = state
        .circuit_breakers
        .ok_or_else(|| ApiError::Unavailable("circuit breaker registry not available".into()))?;
    Ok(Json(registry.list_for_tenant(&tid)))
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
    State(state): State<crate::AppState>,
    tenant_ctx: OptionalTenant,
    Path((tenant_id, handler)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let tid = guard_tenant(&tenant_ctx, &tenant_id)?;
    let registry = state
        .circuit_breakers
        .ok_or_else(|| ApiError::Unavailable("circuit breaker registry not available".into()))?;
    let breaker = registry
        .get(&tid, &handler)
        .ok_or_else(|| ApiError::NotFound(format!("circuit_breaker {handler}")))?;
    Ok(([(REVIEWED_RESET, "1")], Json(breaker)))
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
    State(state): State<crate::AppState>,
    tenant_ctx: OptionalTenant,
    Path((tenant_id, handler)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let tid = guard_tenant(&tenant_ctx, &tenant_id)?;
    let registry = state
        .circuit_breakers
        .ok_or_else(|| ApiError::Unavailable("circuit breaker registry not available".into()))?;
    registry.reset(&tid, &handler);
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/tenants/{tenant_id}/circuit-breakers/{handler}/reset-reviewed",
    params(
        ("tenant_id" = String, Path, description = "Tenant id"),
        ("handler" = String, Path, description = "Handler name"),
    ),
    request_body = CircuitBreakerState,
    responses(
        (status = 200, description = "The reviewed breaker was reset"),
        (status = 400, description = "The supplied state targets a different breaker"),
        (status = 409, description = "The reviewed breaker changed or disappeared"),
    )
)]
async fn reset_reviewed_breaker(
    State(state): State<crate::AppState>,
    tenant_ctx: OptionalTenant,
    Path((tenant_id, handler)): Path<(String, String)>,
    Json(expected): Json<CircuitBreakerState>,
) -> Result<impl IntoResponse, ApiError> {
    let tid = guard_tenant(&tenant_ctx, &tenant_id)?;
    if expected.tenant_id != tid || expected.handler != handler {
        return Err(ApiError::InvalidArgument(
            "Reviewed breaker must match the requested tenant and handler.".into(),
        ));
    }
    let registry = state
        .circuit_breakers
        .ok_or_else(|| ApiError::Unavailable("circuit breaker registry not available".into()))?;
    let status = if registry.reset_reviewed(&expected) {
        StatusCode::OK
    } else {
        StatusCode::CONFLICT
    };
    Ok((status, [(REVIEWED_RESET, "1")]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn reviewed_reset_advertises_support_and_refuses_a_changed_breaker() {
        let storage = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let registry = Arc::new(orch8_engine::circuit_breaker::CircuitBreakerRegistry::new(
            2, 30,
        ));
        let mut app = crate::test_harness::test_state(
            storage,
            tokio_util::sync::CancellationToken::new(),
            false,
            0,
            None,
        );
        app.circuit_breakers = Some(registry.clone());
        let tenant = TenantId::unchecked("test");
        let handler = "fixture";
        let path = || Path((tenant.as_str().to_owned(), handler.to_owned()));
        registry.record_failure(&tenant, handler);
        let before = get_breaker(State(app.clone()), None, path())
            .await
            .unwrap()
            .into_response();
        assert_eq!(before.headers()[REVIEWED_RESET], "1");
        let stale = registry.get(&tenant, handler).unwrap();

        registry.record_failure(&tenant, handler);
        let changed = registry.get(&tenant, handler).unwrap();
        let refusal = reset_reviewed_breaker(State(app.clone()), None, path(), Json(stale))
            .await
            .unwrap()
            .into_response();
        assert_eq!(refusal.status(), StatusCode::CONFLICT);
        assert_eq!(refusal.headers()[REVIEWED_RESET], "1");
        assert_eq!(registry.get(&tenant, handler).unwrap(), changed);

        let accepted = reset_reviewed_breaker(State(app), None, path(), Json(changed))
            .await
            .unwrap()
            .into_response();
        assert_eq!(accepted.status(), StatusCode::OK);
        assert_eq!(accepted.headers()[REVIEWED_RESET], "1");
        assert_eq!(registry.get(&tenant, handler).unwrap().failure_count, 0);
    }
}
