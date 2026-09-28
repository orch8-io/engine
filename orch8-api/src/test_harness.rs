//! Shared integration-test bootstrap for `orch8-api`.
//!
//! Spins up the full axum router (with tenant middleware) backed by an
//! in-memory `SQLite` storage on an ephemeral port. Integration tests in
//! `orch8-api/tests/*.rs` consume [`spawn_test_server`] directly so that
//! routing + auth middleware + response shapes are exercised end-to-end.
//!
//! This module is intentionally public and lightweight — it does NOT mirror
//! the production `orch8-server::main` wiring (no gRPC, no engine loop, no
//! metrics). It exists so the API crate can own its own integration-test
//! surface without pulling in the whole server binary.

use std::sync::Arc;

use axum::Router;
use orch8_storage::artifacts::ObjectArtifactStore;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::config::ExternalizationMode;
use tokio_util::sync::CancellationToken;

use crate::{AppState, build_router, webhooks};

/// A running test server. The listener is bound to `127.0.0.1:0` so tests
/// can run concurrently without port collisions.
pub struct TestServer {
    /// Root URL of the server (e.g. `http://127.0.0.1:12345`).
    /// Routes are available at both `{base_url}/...` (deprecated) and
    /// `{base_url}/api/v1/...` (canonical).
    pub base_url: String,
    pub shutdown: CancellationToken,
    pub storage: Arc<orch8_storage::sqlite::SqliteStorage>,
}

impl TestServer {
    /// Returns the canonical versioned base URL (`{base_url}/api/v1`).
    /// New tests should prefer this over bare `base_url` paths.
    pub fn v1_url(&self) -> String {
        format!("{}{}", self.base_url, crate::API_V1_PREFIX)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Cancel the server task so the tokio runtime can exit cleanly
        // when the test function returns.
        self.shutdown.cancel();
    }
}

/// Spawn a fully-wired API server on an ephemeral loopback port.
///
/// The tenant middleware is attached with `require_tenant = false` so tests
/// can pick whether to send `X-Tenant-Id`. API-key auth is disabled.
///
/// # Panics
/// Panics if the in-memory storage cannot be initialised or the TCP listener
/// fails to bind — both indicate a broken test environment, not a product bug.
pub async fn spawn_test_server() -> TestServer {
    spawn_test_server_inner(false, false, 0).await
}

/// Like [`spawn_test_server`] but with mobile sync endpoints enabled.
pub async fn spawn_test_server_with_mobile_sync() -> TestServer {
    spawn_test_server_inner(true, false, 0).await
}

/// Like [`spawn_test_server`] but with an ephemeral artifact backend enabled.
pub async fn spawn_test_server_with_artifacts() -> TestServer {
    spawn_test_server_inner(false, true, 0).await
}

/// Spawn a test server enforcing the supplied serialized-context byte limit.
pub async fn spawn_test_server_with_context_limit(max_context_bytes: u32) -> TestServer {
    spawn_test_server_inner(false, false, max_context_bytes).await
}

/// A running test server over any storage backend (e.g. Postgres), with
/// optional root API-key authentication.
pub struct BackendTestServer {
    pub base_url: String,
    pub shutdown: CancellationToken,
    pub storage: Arc<dyn orch8_storage::StorageBackend>,
}

impl BackendTestServer {
    #[must_use]
    pub fn v1_url(&self) -> String {
        format!("{}{}", self.base_url, crate::API_V1_PREFIX)
    }
}

impl Drop for BackendTestServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Spawn the router over `storage`. With `root_api_key`, API-key auth is
/// enforced exactly like `orch8-server` (root key = admin, per-tenant keys,
/// browser-session tokens); without it the server runs in insecure mode.
///
/// # Panics
/// Panics if the TCP listener fails to bind.
pub async fn spawn_test_server_on(
    storage: Arc<dyn orch8_storage::StorageBackend>,
    root_api_key: Option<&str>,
) -> BackendTestServer {
    let root_key_digest = root_api_key.map(orch8_types::auth::precompute_secret_digest);
    let shutdown = CancellationToken::new();
    let state = test_state(storage.clone(), shutdown.clone(), false, 0, root_key_digest);
    let base_url = serve(state, storage.clone(), root_key_digest, shutdown.clone()).await;
    BackendTestServer {
        base_url,
        shutdown,
        storage,
    }
}

/// Options for [`spawn_test_server_with`].
#[derive(Debug, Clone, Default)]
pub struct TestServerOptions {
    /// Root API key; `None` runs the server in insecure mode.
    pub root_api_key: Option<String>,
    /// Mount the `/mobile/*` device endpoints.
    pub mobile_sync_enabled: bool,
}

/// Spawn the router over `storage` with [`TestServerOptions`] (auth exactly
/// like `orch8-server` when a root key is set).
///
/// # Panics
/// Panics if the TCP listener fails to bind.
pub async fn spawn_test_server_with(
    storage: Arc<dyn orch8_storage::StorageBackend>,
    options: TestServerOptions,
) -> BackendTestServer {
    let root_key_digest = options
        .root_api_key
        .as_deref()
        .map(orch8_types::auth::precompute_secret_digest);
    let shutdown = CancellationToken::new();
    let state = test_state(
        storage.clone(),
        shutdown.clone(),
        options.mobile_sync_enabled,
        0,
        root_key_digest,
    );
    let base_url = serve(state, storage.clone(), root_key_digest, shutdown.clone()).await;
    BackendTestServer {
        base_url,
        shutdown,
        storage,
    }
}

fn test_state(
    storage: Arc<dyn orch8_storage::StorageBackend>,
    shutdown: CancellationToken,
    mobile_sync_enabled: bool,
    max_context_bytes: u32,
    root_key_digest: Option<[u8; 32]>,
) -> AppState {
    AppState {
        storage,
        shutdown,
        max_context_bytes,
        externalization_mode: ExternalizationMode::default(),
        worker_lease_secs: 60,
        worker_heartbeat_interval_secs: 15,
        circuit_breakers: None,
        stream_limiter: Arc::new(tokio::sync::Semaphore::new(
            crate::DEFAULT_MAX_CONCURRENT_STREAMS,
        )),
        publisher: None,
        push_provider: Arc::new(orch8_push::NoopPushProvider),
        mobile_sync_enabled,
        entitlements: crate::entitlements::unlimited_provider(),
        builtin_handlers: Arc::new(crate::builtin_handler_names()),
        engine_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        continuity_crypto: Some(Arc::new(
            crate::ContinuityCrypto::from_master_key(&"11".repeat(32))
                .expect("test continuity key is valid"),
        )),
        continuity_trusted_signing_keys: Arc::new(std::collections::BTreeMap::new()),
        federation_peers: Arc::new(Vec::new()),
        continuity_lab_enabled: false,
        browser_sessions: std::sync::Arc::new(
            crate::browser_sessions::BrowserSessionSigner::for_root(root_key_digest),
        ),
        browser_output_max_bytes: crate::DEFAULT_BROWSER_OUTPUT_MAX_BYTES,
    }
}

async fn spawn_test_server_inner(
    mobile_sync_enabled: bool,
    artifacts_enabled: bool,
    max_context_bytes: u32,
) -> TestServer {
    let mut storage = SqliteStorage::in_memory()
        .await
        .expect("in-memory sqlite storage must initialise for tests");
    if artifacts_enabled {
        storage = storage.with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
    }
    let storage = Arc::new(storage);
    let shutdown = CancellationToken::new();
    let state = test_state(
        storage.clone(),
        shutdown.clone(),
        mobile_sync_enabled,
        max_context_bytes,
        None,
    );
    let base_url = serve(state, storage.clone(), None, shutdown.clone()).await;
    TestServer {
        base_url,
        shutdown,
        storage,
    }
}

async fn serve(
    state: AppState,
    storage: Arc<dyn orch8_storage::StorageBackend>,
    root_key_digest: Option<[u8; 32]>,
    shutdown: CancellationToken,
) -> String {
    // Attach auth + tenant middleware. API-key auth is disabled for the
    // harness (root_key_digest = None), which marks every request as admin so
    // operator endpoints that require `OptionalAdmin` remain testable. The
    // tenant middleware still parses `X-Tenant-Id` when present without
    // requiring it.
    // Mount health/info routes the same way `orch8-server` does: outside the
    // auth/tenant layers. `build_router` no longer includes them (see note
    // there), so the harness adds them explicitly to exercise /info and
    // /health/* in the e2e suite.
    let storage_for_auth = storage.clone();
    let app: Router = build_router(state.clone())
        .layer(axum::middleware::from_fn(move |req, next| {
            let storage = storage_for_auth.clone();
            async move {
                crate::auth::api_key_middleware(storage, root_key_digest, req, next).await
            }
        }))
        .layer(axum::middleware::from_fn(|req, next| async move {
            crate::auth::tenant_middleware(false, req, next).await
        }))
        .merge(crate::health::routes().with_state(state.clone()))
        .merge(webhooks::public_routes().with_state(state.clone()))
        .merge(crate::public_routes().with_state(state.clone()))
        .layer(axum::middleware::from_fn(
            crate::request_id::request_id_middleware,
        ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port for test server");
    let addr = listener.local_addr().expect("test listener has local addr");
    let base_url = format!("http://{addr}");

    let cancel_child = shutdown.clone();
    // Ref#9: the listener is already bound (so incoming SYNs hit the kernel
    // backlog), but the spawned task may not have been scheduled by the time
    // the first test request fires. Send a oneshot from the serve task so we
    // can await readiness deterministically instead of relying on a sleep.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        // Handing the listener to `axum::serve` is the last thing before the
        // accept loop starts. Signalling ready *just before* the await point
        // guarantees the serve future has polled at least once by the time
        // the channel completes.
        let _ = ready_tx.send(());
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel_child.cancelled().await })
            .await;
    });
    // A dropped sender means the server task never started — surface the
    // failure rather than hanging on a retry loop inside the test.
    ready_rx
        .await
        .expect("test server task failed to start before first request");
    base_url
}
