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
//!   1. A [`GrpcAuthLayer`] that runs asynchronously before every RPC, accepting
//!      a valid API key or mapped mTLS workload certificate and stamping the
//!      authoritative tenant into request extensions as a [`CallerTenant`].
//!   2. A [`caller_tenant`] helper that handlers can call to pull the
//!      stamped `TenantId` back out, plus [`enforce_tenant_match`] which
//!      mirrors the HTTP-side `enforce_tenant_access` (returns `NotFound`
//!      on cross-tenant reads so existence doesn't leak).
//!
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::codegen::http;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::{Request, Status};
use tower::{Layer, Service};

use orch8_storage::StorageBackend;
use orch8_types::ids::TenantId;

/// Caller's tenant identity, as parsed from `x-tenant-id` metadata and
/// stamped into request extensions by [`GrpcAuthLayer`].
///
/// Wrapped in a newtype rather than inserting a raw `TenantId` so other
/// code putting `TenantId` into extensions (e.g. for resource lookup)
/// cannot accidentally masquerade as caller identity.
#[derive(Clone, Debug)]
pub struct CallerTenant(pub TenantId);

/// Verified workload identity mapped from the SHA-256 fingerprint of the
/// connection's leaf client certificate.
#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq)]
pub struct WorkloadIdentity {
    pub tenant_id: String,
    pub identity: String,
}

/// Immutable certificate-fingerprint registry used by the auth layer.
#[derive(Clone, Debug, Default)]
pub struct WorkloadIdentityRegistry(Arc<HashMap<String, WorkloadIdentity>>);

impl WorkloadIdentityRegistry {
    /// Parse a JSON object keyed by a 64-character SHA-256 certificate
    /// fingerprint. Colons and ASCII case in keys are normalized.
    pub fn from_json(json: &str) -> Result<Self, String> {
        if json.trim().is_empty() {
            return Ok(Self::default());
        }
        let raw: HashMap<String, WorkloadIdentity> =
            serde_json::from_str(json).map_err(|error| error.to_string())?;
        let mut normalized = HashMap::with_capacity(raw.len());
        for (fingerprint, identity) in raw {
            let fingerprint = normalize_fingerprint(&fingerprint)?;
            let identity = WorkloadIdentity {
                tenant_id: identity.tenant_id.trim().to_owned(),
                identity: identity.identity.trim().to_owned(),
            };
            if identity.tenant_id.is_empty()
                || identity.identity.is_empty()
                || identity.tenant_id.len() > 128
                || identity.identity.len() > 128
            {
                return Err(
                    "workload identity tenant_id and identity must contain 1..=128 bytes".into(),
                );
            }
            if normalized.insert(fingerprint, identity).is_some() {
                return Err("duplicate workload certificate fingerprint".into());
            }
        }
        Ok(Self(Arc::new(normalized)))
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn identity_for_certificate(&self, certificate_der: &[u8]) -> Option<&WorkloadIdentity> {
        let fingerprint = hex_sha256(certificate_der);
        self.0.get(&fingerprint)
    }

    fn identity_for_extensions(&self, extensions: &http::Extensions) -> Option<WorkloadIdentity> {
        let connect_info = extensions.get::<TlsConnectInfo<TcpConnectInfo>>()?;
        let certificates = connect_info.peer_certs()?;
        let leaf = certificates.first()?;
        self.identity_for_certificate(leaf.as_ref()).cloned()
    }
}

fn normalize_fingerprint(value: &str) -> Result<String, String> {
    let normalized: String = value
        .chars()
        .filter(|character| *character != ':')
        .flat_map(char::to_lowercase)
        .collect();
    if normalized.len() != 64 || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("workload certificate fingerprints must be 64 hexadecimal characters".into());
    }
    Ok(normalized)
}

fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Async authentication middleware for tonic services.
#[derive(Clone)]
pub struct GrpcAuthLayer {
    storage: Arc<dyn StorageBackend>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
    workload_identities: WorkloadIdentityRegistry,
    allowed_rpc_paths: Option<Arc<HashSet<&'static str>>>,
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
            workload_identities: WorkloadIdentityRegistry::default(),
            allowed_rpc_paths: None,
        }
    }

    #[must_use]
    pub fn with_workload_identities(mut self, identities: WorkloadIdentityRegistry) -> Self {
        self.workload_identities = identities;
        self
    }

    /// Restrict this listener to an explicit set of fully-qualified tonic RPC
    /// paths. Used by role-specific nodes to fail closed at the transport.
    #[must_use]
    pub fn with_allowed_rpc_paths(mut self, paths: &'static [&'static str]) -> Self {
        self.allowed_rpc_paths = Some(Arc::new(paths.iter().copied().collect()));
        self
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
            workload_identities: self.workload_identities.clone(),
            allowed_rpc_paths: self.allowed_rpc_paths.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcAuthService<S> {
    inner: S,
    storage: Arc<dyn StorageBackend>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
    workload_identities: WorkloadIdentityRegistry,
    allowed_rpc_paths: Option<Arc<HashSet<&'static str>>>,
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
        let workload_identities = self.workload_identities.clone();
        let allowed_rpc_paths = self.allowed_rpc_paths.clone();

        Box::pin(async move {
            let (mut parts, body) = request.into_parts();
            if allowed_rpc_paths
                .as_ref()
                .is_some_and(|paths| !paths.contains(parts.uri.path()))
            {
                return Ok(
                    Status::permission_denied("RPC is disabled for this node role").into_http(),
                );
            }
            if let Err(status) = authenticate_request_with_workloads(
                &parts.headers,
                &mut parts.extensions,
                &storage,
                expected_digest,
                require_tenant,
                &workload_identities,
            )
            .await
            {
                return Ok(status.into_http());
            }
            inner.call(http::Request::from_parts(parts, body)).await
        })
    }
}

async fn authenticate_request_with_workloads(
    headers: &http::HeaderMap,
    extensions: &mut http::Extensions,
    storage: &Arc<dyn StorageBackend>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
    workload_identities: &WorkloadIdentityRegistry,
) -> Result<(), Status> {
    let tenant = headers
        .get("x-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let provided = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if provided.is_empty()
        && let Some(identity) = workload_identities.identity_for_extensions(extensions)
    {
        if tenant.is_some_and(|tenant| tenant != identity.tenant_id) {
            return Err(Status::permission_denied(
                "x-tenant-id does not match workload identity tenant",
            ));
        }
        let identity_tenant = TenantId::new(identity.tenant_id.clone())
            .map_err(|_| Status::internal("workload identity has an invalid tenant"))?;
        extensions.insert(CallerTenant(identity_tenant));
        extensions.insert(identity);
        return Ok(());
    }

    if provided.is_empty() && !workload_identities.is_empty() && expected_digest.is_none() {
        return Err(Status::unauthenticated(
            "client certificate is not mapped to a workload identity",
        ));
    }

    if expected_digest.is_none() {
        if let Some(tenant) = tenant {
            let tenant = TenantId::new(tenant.to_owned())
                .map_err(|message| Status::invalid_argument(message))?;
            extensions.insert(CallerTenant(tenant));
            return Ok(());
        }
        return if require_tenant {
            Err(Status::invalid_argument("missing x-tenant-id metadata"))
        } else {
            Ok(())
        };
    }

    if expected_digest
        .as_ref()
        .is_some_and(|digest| orch8_types::auth::verify_secret_against_digest(provided, digest))
    {
        if let Some(tenant) = tenant {
            let tenant = TenantId::new(tenant.to_owned())
                .map_err(|message| Status::invalid_argument(message))?;
            extensions.insert(CallerTenant(tenant));
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
            let tenant = TenantId::new(record.tenant_id)
                .map_err(|_| Status::internal("API key has an invalid tenant"))?;
            extensions.insert(CallerTenant(tenant));
            Ok(())
        }
        Ok(_) => Err(Status::unauthenticated("invalid or missing x-api-key")),
        Err(error) => {
            tracing::error!(%error, "grpc api key lookup failed");
            Err(Status::internal("authentication failed"))
        }
    }
}

#[cfg(test)]
async fn authenticate_request(
    headers: &http::HeaderMap,
    extensions: &mut http::Extensions,
    storage: &Arc<dyn StorageBackend>,
    expected_digest: Option<[u8; 32]>,
    require_tenant: bool,
) -> Result<(), Status> {
    authenticate_request_with_workloads(
        headers,
        extensions,
        storage,
        expected_digest,
        require_tenant,
        &WorkloadIdentityRegistry::default(),
    )
    .await
}

/// Pull the caller's tenant out of request extensions, if the auth layer
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

    fn headers(pairs: &[(&'static str, &str)]) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        for (k, v) in pairs {
            map.insert(*k, v.parse().unwrap());
        }
        map
    }

    async fn empty_storage() -> Arc<dyn StorageBackend> {
        Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        )
    }

    /// Build a request with a caller tenant stamped, as the auth layer would.
    fn req_with_caller(tenant: &str) -> Request<()> {
        let mut req = Request::new(());
        req.extensions_mut()
            .insert(CallerTenant(TenantId::unchecked(tenant.to_owned())));
        req
    }

    fn bare_req() -> Request<()> {
        Request::new(())
    }

    fn digest_of(key: &str) -> [u8; 32] {
        orch8_types::auth::precompute_secret_digest(key)
    }

    #[test]
    fn workload_registry_maps_normalized_certificate_fingerprint() {
        let certificate = b"test client certificate DER";
        let fingerprint = hex_sha256(certificate);
        let colonized = fingerprint
            .as_bytes()
            .chunks(2)
            .map(|chunk| std::str::from_utf8(chunk).unwrap().to_ascii_uppercase())
            .collect::<Vec<_>>()
            .join(":");
        let json = serde_json::json!({
            (colonized): { "tenant_id": "acme", "identity": "worker-prod" }
        });
        let registry = WorkloadIdentityRegistry::from_json(&json.to_string()).unwrap();

        assert_eq!(
            registry.identity_for_certificate(certificate),
            Some(&WorkloadIdentity {
                tenant_id: "acme".into(),
                identity: "worker-prod".into(),
            })
        );
    }

    #[test]
    fn workload_registry_rejects_ambiguous_or_incomplete_entries() {
        let invalid_fingerprint = r#"{"abcd":{"tenant_id":"acme","identity":"worker"}}"#;
        assert!(WorkloadIdentityRegistry::from_json(invalid_fingerprint).is_err());
        let fingerprint = "0".repeat(64);
        let missing_identity = serde_json::json!({
            (fingerprint): { "tenant_id": "acme", "identity": "" }
        });
        assert!(WorkloadIdentityRegistry::from_json(&missing_identity.to_string()).is_err());
    }

    #[tokio::test]
    async fn rpc_allowlist_rejects_methods_outside_role_surface() {
        use tower::ServiceExt as _;

        let storage = empty_storage().await;
        let layer = GrpcAuthLayer::new(storage, None, false)
            .with_allowed_rpc_paths(&["/orch8.Orch8Service/Health"]);
        let inner = tower::service_fn(|_request| async {
            Ok::<_, std::convert::Infallible>(http::Response::new(tonic::body::Body::empty()))
        });
        let response = layer
            .layer(inner)
            .oneshot(
                http::Request::builder()
                    .uri("/orch8.Orch8Service/CreateInstance")
                    .body(tonic::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok()),
            Some("7")
        );
    }

    #[tokio::test]
    async fn rejects_missing_api_key() {
        let storage = empty_storage().await;
        let err = authenticate_request(
            &headers(&[]),
            &mut http::Extensions::new(),
            &storage,
            Some(digest_of("s3cret")),
            false,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn rejects_wrong_api_key() {
        let storage = empty_storage().await;
        let err = authenticate_request(
            &headers(&[("x-api-key", "nope123")]),
            &mut http::Extensions::new(),
            &storage,
            Some(digest_of("s3cret")),
            false,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn accepts_matching_api_key() {
        let storage = empty_storage().await;
        authenticate_request(
            &headers(&[("x-api-key", "s3cret")]),
            &mut http::Extensions::new(),
            &storage,
            Some(digest_of("s3cret")),
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn open_when_no_key_configured() {
        let storage = empty_storage().await;
        authenticate_request(
            &headers(&[]),
            &mut http::Extensions::new(),
            &storage,
            None,
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn requires_tenant_when_configured() {
        let storage = empty_storage().await;
        let err = authenticate_request(
            &headers(&[]),
            &mut http::Extensions::new(),
            &storage,
            None,
            true,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn rejects_empty_tenant_with_required() {
        let storage = empty_storage().await;
        let err = authenticate_request(
            &headers(&[("x-tenant-id", "   ")]),
            &mut http::Extensions::new(),
            &storage,
            None,
            true,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn stamps_tenant_into_extensions() {
        let storage = empty_storage().await;
        let mut extensions = http::Extensions::new();
        authenticate_request(
            &headers(&[("x-tenant-id", "tenant-a")]),
            &mut extensions,
            &storage,
            None,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            extensions.get::<CallerTenant>().map(|c| c.0.as_str()),
            Some("tenant-a")
        );
    }

    #[test]
    fn enforce_returns_not_found_on_mismatch() {
        let req = req_with_caller("tenant-a");
        let err =
            enforce_tenant_match(&req, &TenantId::unchecked("tenant-b"), "instance").unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[test]
    fn enforce_allows_same_tenant() {
        let req = req_with_caller("tenant-a");
        assert!(enforce_tenant_match(&req, &TenantId::unchecked("tenant-a"), "instance").is_ok());
    }

    #[test]
    fn enforce_is_permissive_without_caller_tenant() {
        // Insecure mode: no caller tenant, no enforcement.
        let req = bare_req();
        assert!(enforce_tenant_match(&req, &TenantId::unchecked("tenant-b"), "instance").is_ok());
    }

    #[test]
    fn enforce_create_returns_caller_when_body_matches() {
        let req = req_with_caller("tenant-a");
        let t = enforce_tenant_create(&req, &TenantId::unchecked("tenant-a")).unwrap();
        assert_eq!(t.as_str(), "tenant-a");
    }

    #[test]
    fn enforce_create_accepts_empty_body_tenant() {
        // Client omits tenant_id in body — caller header wins.
        let req = req_with_caller("tenant-a");
        let t = enforce_tenant_create(&req, &TenantId::unchecked("")).unwrap();
        assert_eq!(t.as_str(), "tenant-a");
    }

    #[test]
    fn enforce_create_rejects_cross_tenant_body() {
        let req = req_with_caller("tenant-a");
        let err = enforce_tenant_create(&req, &TenantId::unchecked("tenant-b")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn enforce_create_passthrough_in_permissive_mode() {
        let req = bare_req();
        let t = enforce_tenant_create(&req, &TenantId::unchecked("whatever")).unwrap();
        assert_eq!(t.as_str(), "whatever");
    }

    #[test]
    fn scoped_tenant_prefers_caller() {
        let req = req_with_caller("tenant-a");
        let t = scoped_tenant_id(&req, Some(&TenantId::unchecked("tenant-b")));
        assert_eq!(
            t.as_ref().map(orch8_types::TenantId::as_str),
            Some("tenant-a")
        );
    }

    #[test]
    fn scoped_tenant_falls_back_to_body_in_permissive_mode() {
        let req = bare_req();
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
