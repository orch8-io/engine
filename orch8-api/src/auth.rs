use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use orch8_types::ids::TenantId;

/// Tenant context extracted from the `X-Tenant-Id` header.
/// When present, all API operations are scoped to this tenant.
#[derive(Clone, Debug)]
pub struct TenantContext {
    pub tenant_id: TenantId,
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
        Some(key) if key == expected_key => Ok(next.run(request).await),
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
