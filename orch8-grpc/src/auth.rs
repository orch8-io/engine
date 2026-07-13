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
//!   1. A [`GrpcAuthLayer`] that runs asynchronously before every RPC, rejecting
//!      requests without a valid API key and stamping the caller's
//!      `x-tenant-id` into request extensions as a [`CallerTenant`].
//!   2. A [`caller_tenant`] helper that handlers can call to pull the
//!      stamped `TenantId` back out, plus [`enforce_tenant_match`] which
//!      mirrors the HTTP-side `enforce_tenant_access` (returns `NotFound`
//!      on cross-tenant reads so existence doesn't leak).
//!
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::codegen::http;
use tonic::{Request, Status};
use tower::{Layer, Service};

use orch8_storage::StorageBackend;
use orch8_types::ids::TenantId;

/// Caller's tenant identity, as parsed from `x-tenant-id` metadata and
/// stamped into request extensions by [`auth_interceptor`].
///
/// Wrapped in a newtype rather than inserting a raw `TenantId` so other
/// code putting `TenantId` into extensions (e.g. for resource lookup)
/// cannot accidentally masquerade as caller identity.
#[derive(Clone, Debug)]
pub struct CallerTenant(pub TenantId);

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Async authentication middleware for tonic services.
#[derive(Clone)]
pub struct GrpcAuthLayer {
    storage: Arc<dyn StorageBackend>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
}

impl GrpcAuthLayer {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        expected_digest: Option<[u8; 32]>,
        require_tenant: bool,
    ) -> Self {
        Self {
            storage,
            expected_digest,
            require_tenant,
        }
    }
}

impl<S> Layer<S> for GrpcAuthLayer {
    type Service = GrpcAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcAuthService {
            inner,
            storage: Arc::clone(&self.storage),
            expected_digest: self.expected_digest,
            require_tenant: self.require_tenant,
        }
    }
}

#[derive(Clone)]
pub struct GrpcAuthService<S> {
    inner: S,
    storage: Arc<dyn StorageBackend>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
}

impl<S> Service<http::Request<tonic::body::Body>> for GrpcAuthService<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);
        let storage = Arc::clone(&self.storage);
        let expected_digest = self.expected_digest;
        let require_tenant = self.require_tenant;

        Box::pin(async move {
            let (mut parts, body) = request.into_parts();
            if let Err(status) = authenticate_request(
                &parts.headers,
                &mut parts.extensions,
                &storage,
                expected_digest,
                require_tenant,
            )
            .await
            {
                return Ok(status.into_http());
            }
            inner.call(http::Request::from_parts(parts, body)).await
        })
    }
}

async fn authenticate_request(
    headers: &http::HeaderMap,
    extensions: &mut http::Extensions,
    storage: &Arc<dyn StorageBackend>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
) -> Result<(), Status> {
    let tenant = headers
        .get("x-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if expected_digest.is_none() {
        if let Some(tenant) = tenant {
            extensions.insert(CallerTenant(TenantId::unchecked(tenant.to_owned())));
            return Ok(());
        }
        return if require_tenant {
            Err(Status::invalid_argument("missing x-tenant-id metadata"))
        } else {
            Ok(())
        };
    }

    let provided = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if expected_digest
        .as_ref()
        .is_some_and(|digest| orch8_types::auth::verify_secret_against_digest(provided, digest))
    {
        if let Some(tenant) = tenant {
            extensions.insert(CallerTenant(TenantId::unchecked(tenant.to_owned())));
            return Ok(());
        }
        return if require_tenant {
            Err(Status::invalid_argument("missing x-tenant-id metadata"))
        } else {
            Ok(())
        };
    }

    if provided.is_empty() {
        return Err(Status::unauthenticated("invalid or missing x-api-key"));
    }

    let hash = orch8_types::api_key::hash_api_key(provided);
    match orch8_storage::api_key_cache::authenticate(storage, &hash).await {
        Ok(Some(record)) if record.is_active(chrono::Utc::now()) => {
            if tenant.is_some_and(|tenant| tenant != record.tenant_id) {
                return Err(Status::permission_denied(
                    "x-tenant-id does not match key tenant",
                ));
            }
            extensions.insert(CallerTenant(TenantId::unchecked(record.tenant_id)));
            Ok(())
        }
        Ok(_) => Err(Status::unauthenticated("invalid or missing x-api-key")),
        Err(error) => {
            tracing::error!(%error, "grpc api key lookup failed");
            Err(Status::internal("authentication failed"))
        }
    }
}

/// Extract tenant from metadata and stamp it into request extensions.
fn stamp_tenant(req: &mut Request<()>, require_tenant: bool) -> Result<(), Status> {
    let tenant_raw: Option<String> = req
        .metadata()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    match tenant_raw {
        Some(tid) => {
            req.extensions_mut()
                .insert(CallerTenant(TenantId::unchecked(tid)));
            Ok(())
        }
        None if require_tenant => Err(Status::invalid_argument("missing x-tenant-id metadata")),
        None => Ok(()),
    }
}

/// Build a tonic interceptor that enforces API-key auth and extracts
/// tenant identity from metadata.
///
/// # Arguments
/// * `storage` — `Some(backend)` enables per-tenant API key lookup by hash.
///   `None` disables per-tenant key resolution (tests / legacy path).
/// * `expected_api_key` — `Some(key)` → every RPC must carry a matching
///   `x-api-key` metadata entry (root key). `None` → auth is disabled
///   (insecure mode); use only when the server is bound to a trusted interface.
/// * `require_tenant` — when `true`, missing or empty `x-tenant-id`
///   metadata results in `InvalidArgument` for root-key callers. Per-tenant
///   key callers are exempt because their tenant is bound to the key.
pub fn auth_interceptor(
    _storage: Option<Arc<dyn StorageBackend>>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone + Send + Sync + 'static {
    move |mut req: Request<()>| {
        // Insecure mode: no key check, just tenant extraction.
        if expected_digest.is_none() {
            stamp_tenant(&mut req, require_tenant)?;
            return Ok(req);
        }

        let provided = req
            .metadata()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok());

        // 1. Root key check (sync, constant-time).
        let root_ok = if let (Some(provided), Some(digest)) = (provided, expected_digest.as_ref()) {
            orch8_types::auth::verify_secret_against_digest(provided, digest)
        } else {
            false
        };

        if root_ok {
            stamp_tenant(&mut req, require_tenant)?;
            return Ok(req);
        }

        Err(Status::unauthenticated("invalid or missing x-api-key"))
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
    if let Some(caller) = caller_tenant(req)
        && caller != resource_tenant
    {
        return Err(Status::not_found(entity_label.to_string()));
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
        if !body_tenant_id.as_str().is_empty() && body_tenant_id != caller {
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

    fn digest_of(key: &str) -> [u8; 32] {
        orch8_types::auth::precompute_secret_digest(key)
    }

    #[test]
    fn interceptor_rejects_missing_api_key() {
        let ic = auth_interceptor(None, Some(digest_of("s3cret")), false);
        let err = ic(make_req(&[])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_rejects_wrong_api_key() {
        let ic = auth_interceptor(None, Some(digest_of("s3cret")), false);
        let err = ic(make_req(&[("x-api-key", "nope123")])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_accepts_matching_api_key() {
        let ic = auth_interceptor(None, Some(digest_of("s3cret")), false);
        assert!(ic(make_req(&[("x-api-key", "s3cret")])).is_ok());
    }

    #[test]
    fn interceptor_open_when_no_key_configured() {
        let ic = auth_interceptor(None, None, false);
        assert!(ic(make_req(&[])).is_ok());
    }

    #[test]
    fn interceptor_requires_tenant_when_configured() {
        let ic = auth_interceptor(None, None, true);
        let err = ic(make_req(&[])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn interceptor_rejects_empty_tenant_with_required() {
        let ic = auth_interceptor(None, None, true);
        let err = ic(make_req(&[("x-tenant-id", "   ")])).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn interceptor_stamps_tenant_into_extensions() {
        let ic = auth_interceptor(None, None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        assert_eq!(
            caller_tenant(&req).map(orch8_types::TenantId::as_str),
            Some("tenant-a")
        );
    }

    #[test]
    fn enforce_returns_not_found_on_mismatch() {
        let ic = auth_interceptor(None, None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let err =
            enforce_tenant_match(&req, &TenantId::unchecked("tenant-b"), "instance").unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[test]
    fn enforce_allows_same_tenant() {
        let ic = auth_interceptor(None, None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        assert!(enforce_tenant_match(&req, &TenantId::unchecked("tenant-a"), "instance").is_ok());
    }

    #[test]
    fn enforce_is_permissive_without_caller_tenant() {
        // Insecure mode: no caller tenant, no enforcement.
        let ic = auth_interceptor(None, None, false);
        let req = ic(make_req(&[])).unwrap();
        assert!(enforce_tenant_match(&req, &TenantId::unchecked("tenant-b"), "instance").is_ok());
    }

    #[test]
    fn enforce_create_returns_caller_when_body_matches() {
        let ic = auth_interceptor(None, None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let t = enforce_tenant_create(&req, &TenantId::unchecked("tenant-a")).unwrap();
        assert_eq!(t.as_str(), "tenant-a");
    }

    #[test]
    fn enforce_create_accepts_empty_body_tenant() {
        // Client omits tenant_id in body — caller header wins.
        let ic = auth_interceptor(None, None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let t = enforce_tenant_create(&req, &TenantId::unchecked("")).unwrap();
        assert_eq!(t.as_str(), "tenant-a");
    }

    #[test]
    fn enforce_create_rejects_cross_tenant_body() {
        let ic = auth_interceptor(None, None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let err = enforce_tenant_create(&req, &TenantId::unchecked("tenant-b")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn enforce_create_passthrough_in_permissive_mode() {
        let ic = auth_interceptor(None, None, false);
        let req = ic(make_req(&[])).unwrap();
        let t = enforce_tenant_create(&req, &TenantId::unchecked("whatever")).unwrap();
        assert_eq!(t.as_str(), "whatever");
    }

    #[test]
    fn scoped_tenant_prefers_caller() {
        let ic = auth_interceptor(None, None, true);
        let req = ic(make_req(&[("x-tenant-id", "tenant-a")])).unwrap();
        let t = scoped_tenant_id(&req, Some(&TenantId::unchecked("tenant-b")));
        assert_eq!(
            t.as_ref().map(orch8_types::TenantId::as_str),
            Some("tenant-a")
        );
    }

    #[test]
    fn scoped_tenant_falls_back_to_body_in_permissive_mode() {
        let ic = auth_interceptor(None, None, false);
        let req = ic(make_req(&[])).unwrap();
        let t = scoped_tenant_id(&req, Some(&TenantId::unchecked("tenant-b")));
        assert_eq!(
            t.as_ref().map(orch8_types::TenantId::as_str),
            Some("tenant-b")
        );
    }

    // --- asynchronous per-tenant API key parity tests ---

    #[tokio::test]
    async fn middleware_accepts_per_tenant_key_and_stamps_tenant() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let minted = orch8_types::api_key::mint("acme", "ci", None);
        storage.create_api_key(&minted.record).await.unwrap();

        let mut headers = http::HeaderMap::new();
        headers.insert("x-api-key", minted.secret.parse().unwrap());
        let mut extensions = http::Extensions::new();
        authenticate_request(
            &headers,
            &mut extensions,
            &storage,
            Some(digest_of("root-key")),
            true,
        )
        .await
        .expect("per-tenant key must authenticate");
        assert_eq!(
            extensions
                .get::<CallerTenant>()
                .map(|caller| caller.0.as_str()),
            Some("acme")
        );
    }

    #[tokio::test]
    async fn middleware_rejects_per_tenant_key_with_wrong_tenant_header() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let minted = orch8_types::api_key::mint("acme", "ci", None);
        storage.create_api_key(&minted.record).await.unwrap();

        let mut headers = http::HeaderMap::new();
        headers.insert("x-api-key", minted.secret.parse().unwrap());
        headers.insert("x-tenant-id", "evil".parse().unwrap());
        let err = authenticate_request(
            &headers,
            &mut http::Extensions::new(),
            &storage,
            Some(digest_of("root-key")),
            true,
        )
        .await
        .expect_err("mismatched tenant header must fail");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn middleware_rejects_revoked_per_tenant_key() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let minted = orch8_types::api_key::mint("acme", "ci", None);
        storage.create_api_key(&minted.record).await.unwrap();
        storage.revoke_api_key(&minted.record.id).await.unwrap();

        let mut headers = http::HeaderMap::new();
        headers.insert("x-api-key", minted.secret.parse().unwrap());
        let err = authenticate_request(
            &headers,
            &mut http::Extensions::new(),
            &storage,
            Some(digest_of("root-key")),
            true,
        )
        .await
        .expect_err("revoked key must fail");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}
