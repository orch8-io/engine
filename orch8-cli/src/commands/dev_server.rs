//! HTTP API server for `orch8 dev --server`.
//!
//! Boots a lightweight local server with `SQLite` file-backed storage, the full
//! REST API, and (when a built dashboard is available) the embedded dashboard
//! SPA. Intended for local development only — runs without auth (insecure).

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::DefaultBodyLimit;
use axum::http::{StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use owo_colors::OwoColorize;
use rust_embed::Embed;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

use orch8_api::{AppState, DEFAULT_MAX_CONCURRENT_STREAMS, build_router};
use orch8_engine::circuit_breaker::CircuitBreakerRegistry;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::StorageBackend;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::config::SchedulerConfig;

#[derive(Embed)]
#[folder = "../dashboard/dist/"]
#[include = "*.html"]
#[include = "*.js"]
#[include = "*.css"]
#[include = "*.svg"]
#[include = "*.png"]
#[include = "*.ico"]
#[include = "*.json"]
#[include = "*.woff"]
#[include = "*.woff2"]
struct DashboardAssets;

/// Running dev server state — kept alive for the session's lifetime.
pub struct DevServer {
    _storage: Arc<dyn StorageBackend>,
    pub shutdown: CancellationToken,
    _engine_handle: tokio::task::JoinHandle<()>,
    _http_handle: tokio::task::JoinHandle<()>,
}

impl DevServer {
    /// Boot the local dev server: `SQLite` storage, engine tick loop, HTTP API.
    pub async fn start(port: u16, db_path: &str) -> Result<Self> {
        let storage: Arc<dyn StorageBackend> =
            Arc::new(SqliteStorage::file(db_path).await.context("SQLite open")?);

        orch8_engine::step_logs::init_step_log_sink(Arc::clone(&storage));

        let shutdown = CancellationToken::new();
        let cb_registry =
            Arc::new(CircuitBreakerRegistry::new(5, 60).with_storage(storage.clone()));
        cb_registry.load_from_storage().await;

        let app_state = AppState {
            storage: storage.clone(),
            shutdown: shutdown.clone(),
            max_context_bytes: 1_048_576,
            externalization_mode: orch8_types::config::ExternalizationMode::default(),
            circuit_breakers: Some(cb_registry.clone()),
            stream_limiter: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_CONCURRENT_STREAMS)),
            publisher: None,
            push_provider: Arc::new(orch8_push::NoopPushProvider),
            mobile_sync_enabled: false,
            builtin_handlers: Arc::new(orch8_api::builtin_handler_names()),
            // Dev server runs the engine in-process for the lifetime of the
            // command; report ready unconditionally.
            engine_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            continuity_crypto: None,
            federation_peers: Arc::new(Vec::new()),
            continuity_lab_enabled: false,
        };

        let cb_routes = orch8_api::circuit_breakers::routes().with_state(app_state.clone());
        let app = build_router(app_state.clone())
            .nest(orch8_api::API_V1_PREFIX, cb_routes.clone())
            .merge(cb_routes)
            .merge(orch8_api::webhooks::public_routes().with_state(app_state.clone()))
            .merge(orch8_api::health::routes().with_state(app_state))
            .layer(axum::middleware::from_fn(
                orch8_api::request_id::request_id_middleware,
            ))
            .layer(
                CorsLayer::new()
                    .allow_origin(AllowOrigin::any())
                    .allow_methods(tower_http::cors::Any)
                    .allow_headers(tower_http::cors::Any),
            )
            .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
            .fallback(dashboard_handler);

        // M-25: this server runs with no auth (see module docs) and is
        // "intended for local development only" -- binding `0.0.0.0` instead
        // of loopback silently exposed it to every other device on the same
        // network/Wi-Fi, not just the machine running it.
        let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .with_context(|| format!("bind port {port}"))?;

        let http_shutdown = shutdown.clone();
        let http_handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    http_shutdown.cancelled().await;
                })
                .await
            {
                tracing::error!(error = %e, "dev HTTP server error");
            }
        });

        let engine_handle = spawn_engine(storage.clone(), shutdown.clone(), cb_registry);

        eprintln!(
            "  {} API:       http://localhost:{port}/api/v1",
            "→".cyan().bold(),
        );
        eprintln!(
            "  {} Dashboard: http://localhost:{port}/",
            "→".cyan().bold(),
        );
        eprintln!(
            "  {} Swagger:   http://localhost:{port}/swagger-ui",
            "→".cyan().bold(),
        );

        Ok(Self {
            _storage: storage,
            shutdown,
            _engine_handle: engine_handle,
            _http_handle: http_handle,
        })
    }
}

fn spawn_engine(
    storage: Arc<dyn StorageBackend>,
    shutdown: CancellationToken,
    cb_registry: Arc<CircuitBreakerRegistry>,
) -> tokio::task::JoinHandle<()> {
    let mut handlers = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut handlers);
    let handlers = handlers.with_circuit_breakers(cb_registry);
    let engine = orch8_engine::Engine::new(storage, SchedulerConfig::default(), handlers, shutdown);

    tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            tracing::error!(error = %e, "dev engine tick loop error");
        }
    })
}

// -- Dashboard SPA serving ---------------------------------------------------

async fn dashboard_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if let Some(content) = DashboardAssets::get(path) {
        let mime = mime_from_path(path);
        ([(header::CONTENT_TYPE, mime)], content.data).into_response()
    } else if let Some(index) = DashboardAssets::get("index.html") {
        Html(index.data).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            "dashboard not built — run `cd dashboard && npm run build`",
        )
            .into_response()
    }
}

fn mime_from_path(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("json") => "application/json; charset=utf-8",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}
