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
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::config::ExternalizationMode;
use tokio_util::sync::CancellationToken;

use crate::{build_router, AppState};

/// A running test server. The listener is bound to `127.0.0.1:0` so tests
/// can run concurrently without port collisions.
pub struct TestServer {
    pub base_url: String,
    pub shutdown: CancellationToken,
    pub storage: Arc<orch8_storage::sqlite::SqliteStorage>,
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
    let storage = Arc::new(
        SqliteStorage::in_memory()
            .await
            .expect("in-memory sqlite storage must initialise for tests"),
    );
    let shutdown = CancellationToken::new();
    let state = AppState {
        storage: storage.clone(),
        shutdown: shutdown.clone(),
        max_context_bytes: 0,
        externalization_mode: ExternalizationMode::default(),
        circuit_breakers: None,
    };

    // Attach tenant middleware (require_tenant = false) so `X-Tenant-Id`
    // gets parsed into a `TenantContext` extension when present but its
    // absence is not a 400. This matches the default server config.
    let app: Router =
        build_router(state).layer(axum::middleware::from_fn(|req, next| async move {
            crate::auth::tenant_middleware(false, req, next).await
        }));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port for test server");
    let addr = listener.local_addr().expect("test listener has local addr");
    let base_url = format!("http://{addr}");

    let cancel_child = shutdown.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel_child.cancelled().await })
            .await;
    });

    TestServer {
        base_url,
        shutdown,
        storage,
    }
}
