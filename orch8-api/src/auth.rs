use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

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

    if provided.is_some()
        && orch8_types::auth::verify_secret_constant_time(provided.unwrap_or(""), &expected_key)
    {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
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

#[cfg(test)]
mod tests {
    //! Unit tests for auth helpers (#276-280 from `TEST_PLAN.md`).
    //!
    //! Middleware tests only exercise the pure logic we can test without
    //! spinning up a full Axum router — header extraction, tenant scoping,
    //! and tenant-access enforcement. End-to-end middleware wiring is
    //! covered by `tests/e2e/security/api_key_auth_enforcement.test.ts`.
    use super::*;
    use axum::Extension;

    #[allow(clippy::unnecessary_wraps)] // matches `OptionalTenant` alias
    fn ctx(t: &str) -> OptionalTenant {
        Some(Extension(TenantContext {
            tenant_id: TenantId(t.to_string()),
        }))
    }

    #[test]
    fn enforce_tenant_access_rejects_cross_tenant_with_not_found() {
        // #276: rejects cross-tenant read — note the API returns NotFound
        // (not Forbidden) to avoid leaking existence of foreign resources.
        let caller = ctx("tenant-a");
        let resource_tenant = TenantId("tenant-b".to_string());
        let err = enforce_tenant_access(&caller, &resource_tenant, "instance")
            .expect_err("cross-tenant access must be rejected");
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[test]
    fn enforce_tenant_access_allows_same_tenant() {
        let caller = ctx("tenant-a");
        let resource_tenant = TenantId("tenant-a".to_string());
        assert!(enforce_tenant_access(&caller, &resource_tenant, "instance").is_ok());
    }

    #[test]
    fn enforce_tenant_access_with_no_ctx_is_permissive() {
        // When no tenant header is set, cross-tenant reads are not
        // blocked — only the middleware+create path enforces isolation.
        let resource_tenant = TenantId("tenant-b".to_string());
        assert!(enforce_tenant_access(&None, &resource_tenant, "instance").is_ok());
    }

    #[test]
    fn enforce_tenant_create_validates_header_matches_body() {
        // #277: tenant header present + body tenant_id present → must match.
        let caller = ctx("tenant-a");
        let body = TenantId("tenant-b".to_string());
        let err = enforce_tenant_create(&caller, &body)
            .expect_err("header/body mismatch must be rejected");
        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn enforce_tenant_create_allows_matching() {
        let caller = ctx("tenant-a");
        let body = TenantId("tenant-a".to_string());
        let id = enforce_tenant_create(&caller, &body).expect("match must pass");
        assert_eq!(id.0, "tenant-a");
    }

    #[test]
    fn enforce_tenant_create_with_empty_body_trusts_header() {
        // Empty body tenant_id is allowed — the header becomes authoritative.
        let caller = ctx("tenant-a");
        let body = TenantId(String::new());
        let id = enforce_tenant_create(&caller, &body).expect("must succeed");
        assert_eq!(id.0, "tenant-a");
    }

    #[test]
    fn enforce_tenant_create_with_no_header_uses_body() {
        // No header → body value passes through unchanged (legacy path
        // for clients that haven't adopted the header yet).
        let body = TenantId("tenant-x".to_string());
        let id = enforce_tenant_create(&None, &body).expect("must succeed");
        assert_eq!(id.0, "tenant-x");
    }

    #[test]
    fn scoped_tenant_id_prefers_header_over_query_param() {
        // #280: header takes priority — a query-param attempt to scope to
        // a different tenant must be overridden by the header.
        let caller = ctx("tenant-header");
        let id = scoped_tenant_id(&caller, Some("tenant-query"));
        assert_eq!(id, Some(TenantId("tenant-header".to_string())));
    }

    #[test]
    fn scoped_tenant_id_falls_back_to_query_when_no_header() {
        let id = scoped_tenant_id(&None, Some("tenant-query"));
        assert_eq!(id, Some(TenantId("tenant-query".to_string())));
    }

    #[test]
    fn scoped_tenant_id_ignores_empty_query() {
        // An empty query param should not produce an empty TenantId filter.
        let id = scoped_tenant_id(&None, Some(""));
        assert!(id.is_none());
    }

    #[test]
    fn api_key_constant_time_comparison_matches_semantics() {
        assert!(orch8_types::auth::verify_secret_constant_time(
            "secret-key-abcd",
            "secret-key-abcd"
        ));
        assert!(!orch8_types::auth::verify_secret_constant_time(
            "WRONG-key-abcd.",
            "secret-key-abcd"
        ));
        assert!(!orch8_types::auth::verify_secret_constant_time(
            "short",
            "secret-key-abcd"
        ));
    }
}
