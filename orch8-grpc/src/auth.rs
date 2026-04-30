//! gRPC auth & tenant-isolation primitives.
//!
//! The HTTP API has a matching pair of Axum middlewares in
//! `orch8-api/src/auth.rs`:
//!
//!   * `api_key_middleware`  — validates `x-api-key` header
//!   * `tenant_middleware`   — parses `x-tenant-id` header into a
//!     `TenantContext` for downstream handlers
//!
//! Previously the gRPC surface had **neither**. Every RPC on
//! `Orch8GrpcService` was callable anonymously from any source reachable by
//! the gRPC listener, and every `instance_id` / `sequence_id` could be
//! referenced regardless of tenant — the HTTP contract and the gRPC
//! contract diverged on the same storage. This module gives the gRPC path
//! parity with the HTTP path via:
//!
//!   1. A [`auth_interceptor`] that runs before every RPC, rejecting
//!      requests without a valid API key and stamping the caller's
//!      `x-tenant-id` into request extensions as a [`CallerTenant`].
//!   2. A [`caller_tenant`] helper that handlers can call to pull the
//!      stamped `TenantId` back out, plus [`enforce_tenant_match`] which
//!      mirrors the HTTP-side `enforce_tenant_access` (returns `NotFound`
//!      on cross-tenant reads so existence doesn't leak).
//!
//! The interceptor is typed on `Request<()>` because tonic's `AsyncInterceptor`
//! contract only gives it access to metadata + extensions — the typed body
//! is threaded through untouched.

use tonic::{Request, Status};

use orch8_types::ids::TenantId;

/// Caller's tenant identity, as parsed from `x-tenant-id` metadata and
/// stamped into request extensions by [`auth_interceptor`].
///
/// Wrapped in a newtype rather than inserting a raw `TenantId` so other
/// code putting `TenantId` into extensions (e.g. for resource lookup)
/// cannot accidentally masquerade as caller identity.
#[derive(Clone, Debug)]
pub struct CallerTenant(pub TenantId);

/// Build a tonic interceptor that enforces API-key auth and extracts
/// tenant identity from metadata.
///
/// # Arguments
/// * `expected_api_key` — `Some(key)` → every RPC must carry a matching
///   `x-api-key` metadata entry. `None` → auth is disabled (insecure mode);
///   use only when the server is bound to a trusted interface.
/// * `require_tenant` — when `true`, missing or empty `x-tenant-id`
///   metadata results in `InvalidArgument`. When `false`, the absence of
///   a tenant header is accepted and downstream handlers see no
///   `CallerTenant` extension (matches the HTTP "permissive" mode).
pub fn auth_interceptor(
    expected_api_key: Option<&str>,
    require_tenant: bool,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone + Send + Sync + 'static {
    let expected_digest: Option<[u8; 32]> =
        expected_api_key.map(orch8_types::auth::precompute_secret_digest);
    move |mut req: Request<()>| {
        // 1. API key: hash both provided and expected to a fixed-size
        //    digest, then compare constant-time. Earlier versions gated on
        //    `key.len() == expected.len()` before ct_eq, which short-
        //    circuited on different lengths and leaked the expected key
        //    length via response timing. SHA-256 here is purely a
        //    length-normaliser — collision resistance is not load-bearing.
        if let Some(expected_digest) = expected_digest {
            let provided = req
                .metadata()
                .get("x-api-key")
                .and_then(|v| v.to_str().ok());
            let ok = provided.is_some()
                && orch8_types::auth::verify_secret_against_digest(
                    provided.unwrap_or(""),
                    &expected_digest,
                );
            if !ok {
                return Err(Status::unauthenticated("invalid or missing x-api-key"));
            }
        }

        // 2. Tenant extraction. An empty value counts as missing so an
        //    attacker can't bypass `require_tenant` by sending the header
        //    with an empty string.
        let tenant_raw: Option<String> = req
            .metadata()
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        match tenant_raw {
            Some(tid) => {
                req.extensions_mut().insert(CallerTenant(TenantId(tid)));
            }
            None if require_tenant => {
                return Err(Status::invalid_argument("missing x-tenant-id metadata"));
            }
            None => {}
        }

        Ok(req)
    }
}

/// Pull the caller's tenant out of request extensions, if the interceptor
/// stamped one. Returns `None` in insecure/permissive mode.
pub fn caller_tenant<T>(req: &Request<T>) -> Option<&TenantId> {
    req.extensions().get::<CallerTenant>().map(|c| &c.0)
}

/// Enforce that a fetched resource belongs to the caller's tenant.
///
/// Mirrors `orch8_api::auth::enforce_tenant_access`: in permissive mode
/// (no caller tenant) this is a no-op; when the caller is tenant-scoped,
/// any mismatch produces a `NotFound` status (not `PermissionDenied`) so
/// a foreign resource's existence cannot be probed.
pub fn enforce_tenant_match<T>(
    req: &Request<T>,
    resource_tenant: &TenantId,
    entity_label: &str,
) -> Result<(), Status> {
    if let Some(caller) = caller_tenant(req) {
        if caller != resource_tenant {
            return Err(Status::not_found(entity_label.to_string()));
        }
    }
    Ok(())
}

/// Caller-authoritative tenant for create paths.
///
/// Mirrors `orch8_api::auth::enforce_tenant_create`:
///   * If the caller has a tenant and the body also carries a tenant, they
///     must match — mismatch → `PermissionDenied`.
///   * If the caller has a tenant, that tenant wins (returned).
///   * If the caller has no tenant (permissive mode), the body's tenant is
///     passed through unchanged (possibly empty).
///
/// Callers should use the returned `TenantId` as the source of truth when
/// persisting the resource, so clients can't create rows for other tenants
/// by putting a foreign `tenant_id` in the payload.
pub fn enforce_tenant_create<T>(
    req: &Request<T>,
    body_tenant_id: &TenantId,
) -> Result<TenantId, Status> {
    if let Some(caller) = caller_tenant(req) {
        if !body_tenant_id.0.is_empty() && body_tenant_id != caller {
            return Err(Status::permission_denied(
                "tenant_id in body does not match x-tenant-id metadata",
            ));
        }
        Ok(caller.clone())
    } else {
        Ok(body_tenant_id.clone())
    }
}

/// For list endpoints: return the caller's tenant when present (overrides any
/// tenant filter the client supplied), otherwise the supplied body tenant.
///
/// Guarantees that a tenant-scoped caller cannot list resources outside their
/// own tenant by passing a different `tenant_id` in the request.
pub fn scoped_tenant_id<T>(
    req: &Request<T>,
    body_tenant_id: Option<&TenantId>,
) -> Option<TenantId> {
    caller_tenant(req)
        .cloned()
        .or_else(|| body_tenant_id.cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_req(headers: &[(&'static str, &str)]) -> Request<()> {
        let mut req = Request::new(());
        for (k, v) in headers {
            req.metadata_mut().insert(*k, v.parse().unwrap());
        }
        req
    }

    #[test]
    fn interceptor_rejects_missing_api_key() {
        let ic = auth_interceptor(Some("s3cret"), false);
        let err = ic(make_req(&[])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_rejects_wrong_api_key() {
        let ic = auth_interceptor(Some("s3cret"), false);
        let err = ic(make_req(&[("x-api-key", "nope123")])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_accepts_matching_api_key() {
        let ic = auth_interceptor(Some("s3cret"), false);
        assert!(ic(make_req(&[("x-api-key", "s3cret")])).is_ok());
    }

    #[test]
    fn interceptor_open_when_no_key_configured() {
        let ic = auth_interceptor(None, false);
        assert!(ic(make_req(&[])).is_ok());
    }

    #[test]
    fn interceptor_requires_tenant_when_configured() {
        let ic = auth_interceptor(None, true);
        let err = ic(make_req(&[])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn interceptor_rejects_empty_tenant_with_required() {
        let ic = auth_interceptor(None, true);
        let err = ic(make_req(&[("x-tenant-id", "   ")])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn interceptor_stamps_tenant_into_extensions() {
        let ic = auth_interceptor(None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        assert_eq!(caller_tenant(&req).map(|t| t.0.as_str()), Some("tenant-a"));
    }

    #[test]
    fn enforce_returns_not_found_on_mismatch() {
        let ic = auth_interceptor(None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let err = enforce_tenant_match(&req, &TenantId("tenant-b".into()), "instance").unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[test]
    fn enforce_allows_same_tenant() {
        let ic = auth_interceptor(None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        assert!(enforce_tenant_match(&req, &TenantId("tenant-a".into()), "instance").is_ok());
    }

    #[test]
    fn enforce_is_permissive_without_caller_tenant() {
        // Insecure mode: no caller tenant, no enforcement.
        let ic = auth_interceptor(None, false);
        let req = ic(make_req(&[])).unwrap();
        assert!(enforce_tenant_match(&req, &TenantId("tenant-b".into()), "instance").is_ok());
    }

    #[test]
    fn enforce_create_returns_caller_when_body_matches() {
        let ic = auth_interceptor(None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let t = enforce_tenant_create(&req, &TenantId("tenant-a".into())).unwrap();
        assert_eq!(t.0, "tenant-a");
    }

    #[test]
    fn enforce_create_accepts_empty_body_tenant() {
        // Client omits tenant_id in body — caller header wins.
        let ic = auth_interceptor(None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let t = enforce_tenant_create(&req, &TenantId(String::new())).unwrap();
        assert_eq!(t.0, "tenant-a");
    }

    #[test]
    fn enforce_create_rejects_cross_tenant_body() {
        let ic = auth_interceptor(None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let err = enforce_tenant_create(&req, &TenantId("tenant-b".into())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn enforce_create_passthrough_in_permissive_mode() {
        let ic = auth_interceptor(None, false);
        let req = ic(make_req(&[])).unwrap();
        let t = enforce_tenant_create(&req, &TenantId("whatever".into())).unwrap();
        assert_eq!(t.0, "whatever");
    }

    #[test]
    fn scoped_tenant_prefers_caller() {
        let ic = auth_interceptor(None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let t = scoped_tenant_id(&req, Some(&TenantId("tenant-b".into())));
        assert_eq!(t.map(|t| t.0), Some("tenant-a".into()));
    }

    #[test]
    fn scoped_tenant_falls_back_to_body_in_permissive_mode() {
        let ic = auth_interceptor(None, false);
        let req = ic(make_req(&[])).unwrap();
        let t = scoped_tenant_id(&req, Some(&TenantId("tenant-b".into())));
        assert_eq!(t.map(|t| t.0), Some("tenant-b".into()));
    }
}
