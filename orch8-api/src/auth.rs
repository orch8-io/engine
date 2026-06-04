use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use orch8_storage::StorageBackend;
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

/// Marker injected when a request authenticated with the unscoped **root** API
/// key (or in `--insecure` mode). Key-management endpoints require it, so only
/// the root key can mint or revoke per-tenant keys.
#[derive(Clone, Debug)]
pub struct AdminContext;

/// Extract the admin marker from request extensions (if present).
pub type OptionalAdmin = Option<axum::Extension<AdminContext>>;

/// For create/write endpoints: if a tenant header is present, enforce it matches
/// the body's `tenant_id`. Returns the authoritative `tenant_id` to use.
pub fn enforce_tenant_create(
    tenant_ctx: &OptionalTenant,
    body_tenant_id: &TenantId,
) -> Result<TenantId, ApiError> {
    if let Some(axum::Extension(ctx)) = tenant_ctx {
        if !body_tenant_id.as_str().is_empty() && *body_tenant_id != ctx.tenant_id {
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
            .map(|s| TenantId::unchecked(s.to_string()))
    }
}

/// API key authentication middleware.
///
/// Accepts two kinds of `x-api-key` secret:
///
///   - the **root** key (`root_key`) — unscoped/admin. It authenticates the
///     request but binds no tenant; the [`tenant_middleware`] then derives the
///     tenant from the `X-Tenant-Id` header (the legacy / bootstrap / admin
///     path). Only the root key may manage other keys.
///   - a **per-tenant** key stored in the database. Identity is taken from the
///     key record and stamped into request extensions as a [`TenantContext`],
///     so a caller can only act within the tenant the key was minted for —
///     the `X-Tenant-Id` header can no longer be used to cross tenants. If the
///     header is present it must match the key's tenant, else `403`.
///
/// When `root_key_digest` is `None` the server is in `--insecure` mode (it
/// warns at startup) and all requests pass through unauthenticated, matching
/// the prior behaviour. Otherwise it is the precomputed SHA-256 digest of the
/// root key — hashed once when the layer is built rather than on every request.
pub async fn api_key_middleware(
    storage: Arc<dyn StorageBackend>,
    root_key_digest: Option<[u8; 32]>,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(expected_digest) = root_key_digest else {
        // --insecure mode: authentication disabled (server warns at startup).
        // Treat everyone as admin so management endpoints remain usable in dev.
        request.extensions_mut().insert(AdminContext);
        return Ok(next.run(request).await);
    };

    let Some(provided) = request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
    else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    // Root/admin key: unscoped. Tenant (if any) comes from the header, handled
    // downstream by `tenant_middleware`. Mark the request as admin so it can
    // manage per-tenant keys.
    if orch8_types::auth::verify_secret_against_digest(&provided, &expected_digest) {
        request.extensions_mut().insert(AdminContext);
        return Ok(next.run(request).await);
    }

    // Otherwise it must be a per-tenant key. Resolve it by hash (cached);
    // identity is bound to the record, never to the (unauthenticated) header.
    let hash = orch8_types::api_key::hash_api_key(&provided);
    match orch8_storage::api_key_cache::authenticate(&storage, &hash).await {
        Ok(Some(record)) if record.is_active(chrono::Utc::now()) => {
            // A supplied X-Tenant-Id must agree with the key's tenant — never
            // let a keyed caller act as a different tenant.
            if let Some(hdr) = request
                .headers()
                .get("x-tenant-id")
                .and_then(|v| v.to_str().ok())
            {
                if !hdr.is_empty() && hdr != record.tenant_id {
                    return Err(StatusCode::FORBIDDEN);
                }
            }
            request.extensions_mut().insert(TenantContext {
                tenant_id: TenantId::unchecked(record.tenant_id.clone()),
            });
            Ok(next.run(request).await)
        }
        // No match, revoked, or expired.
        Ok(_) => Err(StatusCode::UNAUTHORIZED),
        Err(e) => {
            tracing::error!(error = %e, "api key lookup failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
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
    // A per-tenant API key (resolved in `api_key_middleware`) already bound the
    // tenant identity from the key record. Honour it and skip header handling —
    // the header was already cross-checked against the key, and `require_tenant`
    // is satisfied because we have an authenticated tenant. This is the only
    // exemption: the root key is *not* exempt, so `require_tenant` applies to it
    // uniformly (it must still present an `X-Tenant-Id`, scoping the operation).
    if request.extensions().get::<TenantContext>().is_some() {
        return Ok(next.run(request).await);
    }

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
                tenant_id: TenantId::unchecked(tenant_id),
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
            tenant_id: TenantId::unchecked(t.to_string()),
        }))
    }

    #[test]
    fn enforce_tenant_access_rejects_cross_tenant_with_not_found() {
        // #276: rejects cross-tenant read — note the API returns NotFound
        // (not Forbidden) to avoid leaking existence of foreign resources.
        let caller = ctx("tenant-a");
        let resource_tenant = TenantId::unchecked("tenant-b");
        let err = enforce_tenant_access(&caller, &resource_tenant, "instance")
            .expect_err("cross-tenant access must be rejected");
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[test]
    fn enforce_tenant_access_allows_same_tenant() {
        let caller = ctx("tenant-a");
        let resource_tenant = TenantId::unchecked("tenant-a");
        assert!(enforce_tenant_access(&caller, &resource_tenant, "instance").is_ok());
    }

    #[test]
    fn enforce_tenant_access_with_no_ctx_is_permissive() {
        // When no tenant header is set, cross-tenant reads are not
        // blocked — only the middleware+create path enforces isolation.
        let resource_tenant = TenantId::unchecked("tenant-b");
        assert!(enforce_tenant_access(&None, &resource_tenant, "instance").is_ok());
    }

    #[test]
    fn enforce_tenant_create_validates_header_matches_body() {
        // #277: tenant header present + body tenant_id present → must match.
        let caller = ctx("tenant-a");
        let body = TenantId::unchecked("tenant-b");
        let err = enforce_tenant_create(&caller, &body)
            .expect_err("header/body mismatch must be rejected");
        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn enforce_tenant_create_allows_matching() {
        let caller = ctx("tenant-a");
        let body = TenantId::unchecked("tenant-a");
        let id = enforce_tenant_create(&caller, &body).expect("match must pass");
        assert_eq!(id.as_str(), "tenant-a");
    }

    #[test]
    fn enforce_tenant_create_with_empty_body_trusts_header() {
        // Empty body tenant_id is allowed — the header becomes authoritative.
        let caller = ctx("tenant-a");
        let body = TenantId::unchecked("");
        let id = enforce_tenant_create(&caller, &body).expect("must succeed");
        assert_eq!(id.as_str(), "tenant-a");
    }

    #[test]
    fn enforce_tenant_create_with_no_header_uses_body() {
        // No header → body value passes through unchanged (legacy path
        // for clients that haven't adopted the header yet).
        let body = TenantId::unchecked("tenant-x");
        let id = enforce_tenant_create(&None, &body).expect("must succeed");
        assert_eq!(id.as_str(), "tenant-x");
    }

    #[test]
    fn scoped_tenant_id_prefers_header_over_query_param() {
        // #280: header takes priority — a query-param attempt to scope to
        // a different tenant must be overridden by the header.
        let caller = ctx("tenant-header");
        let id = scoped_tenant_id(&caller, Some("tenant-query"));
        assert_eq!(id, Some(TenantId::unchecked("tenant-header")));
    }

    #[test]
    fn scoped_tenant_id_falls_back_to_query_when_no_header() {
        let id = scoped_tenant_id(&None, Some("tenant-query"));
        assert_eq!(id, Some(TenantId::unchecked("tenant-query")));
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
