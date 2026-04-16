use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;

use orch8_types::ids::TenantId;

use crate::error::ApiError;

/// Tenant context extracted from the `X-Tenant-Id` header.
/// When present, all API operations are scoped to this tenant.
#[derive(Clone, Debug)]
pub struct TenantContext {
    pub tenant_id: TenantId,
}

/// Extract the tenant context from request extensions (if present).
pub type OptionalTenant = Option<axum::Extension<TenantContext>>;

/// For create/write endpoints: if a tenant header is present, enforce it matches
/// the body's `tenant_id`. Returns the authoritative `tenant_id` to use.
pub fn enforce_tenant_create(
    tenant_ctx: &OptionalTenant,
    body_tenant_id: &TenantId,
) -> Result<TenantId, ApiError> {
    if let Some(axum::Extension(ctx)) = tenant_ctx {
        if !body_tenant_id.0.is_empty() && *body_tenant_id != ctx.tenant_id {
            return Err(ApiError::Forbidden(
                "tenant_id in body does not match X-Tenant-Id header".into(),
            ));
        }
        Ok(ctx.tenant_id.clone())
    } else {
        Ok(body_tenant_id.clone())
    }
}

/// For get/update/delete endpoints: after fetching a resource, verify the
/// resource's tenant matches the caller. Returns 404 (not 403) to avoid
/// leaking existence of cross-tenant resources.
pub fn enforce_tenant_access(
    tenant_ctx: &OptionalTenant,
    resource_tenant_id: &TenantId,
    entity_label: &str,
) -> Result<(), ApiError> {
    if let Some(axum::Extension(ctx)) = tenant_ctx {
        if *resource_tenant_id != ctx.tenant_id {
            return Err(ApiError::NotFound(entity_label.to_string()));
        }
    }
    Ok(())
}

/// For list endpoints: if a tenant header is present, override the query filter
/// to scope results to the caller's tenant.
pub fn scoped_tenant_id(
    tenant_ctx: &OptionalTenant,
    query_tenant_id: Option<&str>,
) -> Option<TenantId> {
    if let Some(axum::Extension(ctx)) = tenant_ctx {
        Some(ctx.tenant_id.clone())
    } else {
        query_tenant_id
            .filter(|s| !s.is_empty())
            .map(|s| TenantId(s.to_string()))
    }
}

/// API key authentication middleware.
pub async fn api_key_middleware(
    expected_key: String,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if expected_key.is_empty() {
        return Ok(next.run(request).await);
    }

    let provided = request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok());

    match provided {
        Some(key)
            if key.len() == expected_key.len()
                && key.as_bytes().ct_eq(expected_key.as_bytes()).into() =>
        {
            Ok(next.run(request).await)
        }
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

/// Tenant isolation middleware.
///
/// Extracts `X-Tenant-Id` header and injects [`TenantContext`] into request
/// extensions. When `require_tenant` is true, requests without the header are
/// rejected with `400 Bad Request`.
pub async fn tenant_middleware(
    require_tenant: bool,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(tenant_id) = request
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
    {
        if tenant_id.is_empty() && require_tenant {
            return Err(StatusCode::BAD_REQUEST);
        }
        if !tenant_id.is_empty() {
            request.extensions_mut().insert(TenantContext {
                tenant_id: TenantId(tenant_id),
            });
        }
    } else if require_tenant {
        return Err(StatusCode::BAD_REQUEST);
    }

    Ok(next.run(request).await)
}
