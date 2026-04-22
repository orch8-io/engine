#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
#[cfg(unix)]
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use tower_http::cors::{AllowOrigin, CorsLayer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use orch8_api::metrics::MetricsState;
use orch8_api::openapi::ApiDoc;
use orch8_api::{build_router, AppState};
use orch8_engine::circuit_breaker::CircuitBreakerRegistry;
use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::Engine;
use orch8_grpc::service::Orch8GrpcService;
use orch8_grpc::Orch8ServiceServer;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::config::EngineConfig;

#[derive(Parser)]
#[command(
    name = "orch8",
    version,
    about = "Orch8.io — Durable Task Sequencing Engine"
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "orch8.toml")]
    config: String,

    /// Acknowledge running without API-key auth. Without this flag, startup
    /// fails when no API key is configured. Intended for local development only.
    #[arg(long)]
    insecure: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load configuration: TOML file (optional) -> env vars -> defaults.
    let config = load_config(&cli.config)?;
    if let Err(errors) = config.validate() {
        return Err(anyhow::anyhow!(
            "configuration invalid: {}",
            errors.join(", ")
        ));
    }

    // Initialize logging.
    init_logging(&config.logging);

    print_startup_banner(&config, cli.insecure);

    // Connect to storage backend.
    let storage = init_storage(&config).await?;
    let storage = wrap_encryption(storage, &config)?;

    // Install Prometheus metrics recorder.
    let metrics_state = init_prometheus();

    // Graceful shutdown token. Shared with HTTP, gRPC, engine, and any long-lived
    // request handlers (SSE streams) via `AppState`.
    let shutdown_token = CancellationToken::new();

    // Inject storage so `Open` transitions survive process restarts, then
    // rehydrate any previously persisted rows. Load failures are non-fatal —
    // the registry logs and boots with an empty in-memory state.
    let cb_registry = Arc::new(CircuitBreakerRegistry::new(5, 60).with_storage(storage.clone()));
    cb_registry.load_from_storage().await;

    // cb_registry is already inside app_state.circuit_breakers below.
    // Build HTTP router. `AppState` carries the breaker registry so the
    // worker-task HTTP handlers (`/workers/tasks/{id}/complete` and `/fail`)
    // can roll external-worker success/failure into the same registry the
    // scheduler and inspection API share.
    let app_state = build_app_state(
        storage.clone(),
        &config,
        shutdown_token.clone(),
        cb_registry.clone(),
    );
    let cors = build_cors_layer(&config.api.cors_origins);
    let api_key = config.api.api_key.expose().to_string();
    validate_auth_config(&api_key, cli.insecure, &config.api.cors_origins)?;

    let require_tenant = config.api.require_tenant_header;
    // gRPC shares the same API-key / tenant-header contract as HTTP. Capture
    // the values here (before `api_key` is moved into the HTTP middleware
    // closure) so the tonic interceptor below can enforce parity.
    let grpc_api_key = if api_key.is_empty() {
        None
    } else {
        Some(api_key.clone())
    };
    let grpc_require_tenant = require_tenant;

    let mut app = build_router(app_state.clone())
        .merge(orch8_api::circuit_breakers::routes().with_state(app_state.clone()))
        .merge(orch8_api::metrics::routes().with_state(metrics_state))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(axum::middleware::from_fn(move |req, next| {
            orch8_api::auth::tenant_middleware(require_tenant, req, next)
        }))
        .layer(axum::middleware::from_fn(move |req, next| {
            orch8_api::auth::api_key_middleware(api_key.clone(), req, next)
        }))
        .merge(orch8_api::webhooks::public_routes().with_state(app_state.clone()))
        // Health probes live outside the auth layer so k8s/LB liveness checks
        // keep working when ORCH8_API_KEY / ORCH8_REQUIRE_TENANT_HEADER are set.
        .merge(orch8_api::health::routes().with_state(app_state))
        .layer(axum::middleware::from_fn(
            orch8_api::request_id::request_id_middleware,
        ))
        .layer(cors);

    // Apply global concurrency limit if configured (caps in-flight requests).
    if config.api.max_concurrent_requests > 0 {
        let limit = config.api.max_concurrent_requests;
        tracing::info!(max_concurrent = limit, "API concurrency limiting enabled");
        #[allow(clippy::cast_possible_truncation)]
        let concurrency_limit = limit.min(usize::MAX as u64) as usize;
        app = app.layer(tower::limit::ConcurrencyLimitLayer::new(concurrency_limit));
    }

    // Start HTTP server.
    let http_addr: std::net::SocketAddr = config
        .api
        .http_addr
        .parse()
        .context("Invalid HTTP listen address")?;

    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .context("Failed to bind HTTP listener")?;

    tracing::info!(
        "Health endpoints: http://{}/health/live, /health/ready",
        http_addr
    );

    // Graceful shutdown — `shutdown_token` is the single cancellation source
    // shared across HTTP, gRPC, engine, and long-lived handlers.
    spawn_signal_handler(shutdown_token.clone());

    let grpc_handle = spawn_grpc_server(
        storage.clone(),
        &config,
        shutdown_token.clone(),
        grpc_api_key.as_deref(),
        grpc_require_tenant,
    );

    let engine_handle = spawn_engine(
        storage.clone(),
        &config,
        shutdown_token.clone(),
        cb_registry.clone(),
    );

    tracing::info!("Engine ready");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_token.cancelled().await;
            tracing::info!("Shutting down gracefully...");
        })
        .await
        .context("HTTP server error")?;

    drain_shutdown(engine_handle, grpc_handle, cb_registry).await;

    tracing::info!("Shutdown complete");
    Ok(())
}

fn build_app_state(
    storage: Arc<dyn StorageBackend>,
    config: &EngineConfig,
    shutdown: CancellationToken,
    cb_registry: Arc<CircuitBreakerRegistry>,
) -> AppState {
    AppState {
        storage,
        shutdown,
        max_context_bytes: config.engine.max_context_bytes,
        externalization_mode: config.engine.externalization_mode,
        circuit_breakers: Some(cb_registry),
        stream_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(
            orch8_api::DEFAULT_MAX_CONCURRENT_STREAMS,
        )),
    }
}

fn validate_auth_config(api_key: &str, insecure: bool, cors_origins: &str) -> anyhow::Result<()> {
    if api_key.is_empty() {
        if !insecure {
            anyhow::bail!(
                "No API key configured. Set ORCH8_API_KEY (or api.api_key in the config file) \
                 to enable authentication, or pass --insecure to explicitly run without auth."
            );
        }
        tracing::warn!(
            "Running with --insecure: all endpoints are unauthenticated. \
             Never use this flag in production."
        );
    } else if cors_origins.trim() == "*" {
        tracing::warn!("CORS allows all origins ('*') while API key auth is enabled. Consider restricting ORCH8_CORS_ORIGINS to trusted origins.");
    }
    Ok(())
}

async fn init_storage(config: &EngineConfig) -> anyhow::Result<Arc<dyn StorageBackend>> {
    if config.database.backend == "sqlite" {
        let url = config.database.url.expose();
        let sqlite = if url == "sqlite::memory:" || url.is_empty() {
            SqliteStorage::in_memory()
                .await
                .context("Failed to create in-memory SQLite storage")?
        } else {
            SqliteStorage::file(url)
                .await
                .context("Failed to open SQLite database")?
        };
        tracing::info!("Connected to SQLite");
        Ok(Arc::new(sqlite))
    } else {
        if config.database.url.is_empty() {
            anyhow::bail!(
                "database.url is empty. Set ORCH8_DATABASE_URL (or database.url in the config \
                 file) to a PostgreSQL connection string, or set backend=\"sqlite\" for local use."
            );
        }
        let pg = PostgresStorage::new(
            config.database.url.expose(),
            config.database.max_connections,
        )
        .await
        .context("Failed to connect to PostgreSQL")?;

        tracing::info!("Connected to PostgreSQL");

        if config.database.run_migrations {
            pg.run_migrations()
                .await
                .context("Failed to run migrations")?;
            tracing::info!("Migrations applied");
        }
        Ok(Arc::new(pg))
    }
}

fn wrap_encryption(
    storage: Arc<dyn StorageBackend>,
    config: &EngineConfig,
) -> anyhow::Result<Arc<dyn StorageBackend>> {
    let env_key = std::env::var("ORCH8_ENCRYPTION_KEY").unwrap_or_default();
    let key = if config.engine.encryption_key.is_empty() {
        &env_key
    } else {
        config.engine.encryption_key.expose()
    };
    if key.is_empty() {
        return Ok(storage);
    }

    let mut encryptor = orch8_types::encryption::FieldEncryptor::from_hex_key(key)
        .context("Invalid encryption key (expected 64 hex chars for AES-256-GCM)")?;

    // Support key rotation: when ORCH8_OLD_ENCRYPTION_KEY is set, the
    // encryptor will try the old key as a fallback during decryption,
    // allowing reads of rows encrypted with the previous key.
    let old_env_key = std::env::var("ORCH8_OLD_ENCRYPTION_KEY").unwrap_or_default();
    if !old_env_key.is_empty() {
        encryptor = encryptor
            .with_old_key(&old_env_key)
            .context("Invalid old encryption key (expected 64 hex chars for AES-256-GCM)")?;
        tracing::info!(
            "Encryption key rotation enabled: new writes use primary key, \
             old key retained for decryption"
        );
    }

    tracing::info!("Encryption at rest enabled for context.data and credentials");
    Ok(Arc::new(orch8_storage::encrypting::EncryptingStorage::new(
        storage, encryptor,
    )))
}

fn init_prometheus() -> MetricsState {
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = prometheus_handle.handle();
    metrics::set_global_recorder(prometheus_handle).expect("failed to install Prometheus recorder");
    MetricsState { handle }
}

fn spawn_signal_handler(shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut sigint =
                tokio::signal::unix::signal(SignalKind::interrupt()).expect("SIGINT handler");
            let mut sigterm =
                tokio::signal::unix::signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = sigint.recv() => tracing::info!("Received SIGINT"),
                _ = sigterm.recv() => tracing::info!("Received SIGTERM"),
                _ = tokio::signal::ctrl_c() => tracing::info!("Received Ctrl+C"),
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!(error = %e, "Ctrl+C handler error");
            } else {
                tracing::info!("Received Ctrl+C");
            }
        }
        shutdown.cancel();
    })
}

fn spawn_grpc_server(
    storage: Arc<dyn StorageBackend>,
    config: &EngineConfig,
    shutdown: CancellationToken,
    api_key: Option<&str>,
    require_tenant: bool,
) -> tokio::task::JoinHandle<()> {
    let grpc_addr: std::net::SocketAddr = config
        .api
        .grpc_addr
        .parse()
        .expect("Invalid gRPC listen address");

    let grpc_service = Orch8GrpcService::new(storage);
    let grpc_interceptor = orch8_grpc::auth::auth_interceptor(api_key, require_tenant);
    tokio::spawn(async move {
        tracing::info!("gRPC server listening on {}", grpc_addr);
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(Orch8ServiceServer::with_interceptor(
                grpc_service,
                grpc_interceptor,
            ))
            .serve_with_shutdown(grpc_addr, async move {
                shutdown.cancelled().await;
            })
            .await
        {
            tracing::error!(error = %e, "gRPC server error");
        }
    })
}

fn spawn_engine(
    storage: Arc<dyn StorageBackend>,
    config: &EngineConfig,
    shutdown: CancellationToken,
    cb_registry: Arc<CircuitBreakerRegistry>,
) -> tokio::task::JoinHandle<()> {
    let mut handlers = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut handlers);
    // Share the same breaker registry the HTTP API exposes, so admin resets
    // hit the same in-memory state the scheduler consults — and so the
    // engine's check/record_* calls roll up into the rows persisted by the
    // inspection endpoints.
    let handlers = handlers.with_circuit_breakers(cb_registry);
    let engine = Engine::new(storage, config.engine.clone(), handlers, shutdown);

    tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            tracing::error!(error = %e, "Engine tick loop exited with error");
        }
    })
}

async fn drain_shutdown(
    engine_handle: tokio::task::JoinHandle<()>,
    grpc_handle: tokio::task::JoinHandle<()>,
    cb_registry: Arc<CircuitBreakerRegistry>,
) {
    // Wait for engine and gRPC to finish draining (with timeout).
    // Circuit-breaker persistence lives inside a TaskTracker owned by
    // `cb_registry`; we drain it *after* the engine stops so no new
    // transitions land during the flush. Without this flush, fire-and-forget
    // upserts could be aborted when the Tokio runtime tore down, leaving
    // persisted breaker state lagging the in-memory state the next process
    // rehydrated against at boot.
    let drain_timeout = tokio::time::Duration::from_secs(30);
    if tokio::time::timeout(drain_timeout, async {
        let _ = engine_handle.await;
        let _ = grpc_handle.await;
        cb_registry.flush().await;
    })
    .await
    .is_err()
    {
        tracing::warn!("Shutdown drain timed out after {drain_timeout:?}, forcing exit");
    }
}

fn load_config(path: &str) -> anyhow::Result<EngineConfig> {
    let mut config = match std::fs::read_to_string(path) {
        Ok(contents) => {
            toml::from_str::<EngineConfig>(&contents).context("Failed to parse config TOML")?
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Not an error — config file is optional.
            eprintln!("No config file found at {path}, using defaults + env vars");
            EngineConfig::default()
        }
        Err(e) => {
            // File exists but can't be read — that's a real error.
            return Err(anyhow::anyhow!("Failed to read config file {path}: {e}"));
        }
    };
    apply_env_overrides(&mut config);
    Ok(config)
}

fn apply_env_overrides(config: &mut EngineConfig) {
    if let Ok(val) = std::env::var("ORCH8_STORAGE_BACKEND") {
        config.database.backend = val;
    }
    if let Ok(url) = std::env::var("ORCH8_DATABASE_URL") {
        config.database.url = url.into();
    }
    if let Ok(val) = std::env::var("ORCH8_DATABASE_MAX_CONNECTIONS") {
        if let Ok(n) = val.parse() {
            config.database.max_connections = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_LOG_LEVEL") {
        config.logging.level = val;
    }
    if let Ok(val) = std::env::var("ORCH8_LOG_JSON") {
        config.logging.json = val == "true" || val == "1";
    }
    if let Ok(val) = std::env::var("ORCH8_HTTP_ADDR") {
        config.api.http_addr = val;
    }
    if let Ok(val) = std::env::var("ORCH8_GRPC_ADDR") {
        config.api.grpc_addr = val;
    }
    if let Ok(val) = std::env::var("ORCH8_CORS_ORIGINS") {
        config.api.cors_origins = val;
    }
    if let Ok(val) = std::env::var("ORCH8_TICK_INTERVAL_MS") {
        if let Ok(n) = val.parse() {
            config.engine.tick_interval_ms = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_CRON_TICK_SECS") {
        if let Ok(n) = val.parse() {
            config.engine.cron_tick_secs = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_BATCH_SIZE") {
        if let Ok(n) = val.parse() {
            config.engine.batch_size = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_MAX_INSTANCES_PER_TENANT") {
        if let Ok(n) = val.parse() {
            config.engine.max_instances_per_tenant = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_MAX_CONCURRENT_STEPS") {
        if let Ok(n) = val.parse() {
            config.engine.max_concurrent_steps = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_EXTERNALIZE_THRESHOLD") {
        if let Ok(n) = val.parse() {
            config.engine.externalize_output_threshold = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_WEBHOOK_URLS") {
        config.engine.webhooks.urls = val.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(val) = std::env::var("ORCH8_API_KEY") {
        config.api.api_key = val.into();
    }
    // `ORCH8_MAX_CONCURRENT_REQUESTS` is the preferred name; the older
    // `ORCH8_RATE_LIMIT_RPS` is still accepted as an alias (Perf#10).
    if let Ok(val) = std::env::var("ORCH8_MAX_CONCURRENT_REQUESTS")
        .or_else(|_| std::env::var("ORCH8_RATE_LIMIT_RPS"))
    {
        if let Ok(n) = val.parse() {
            config.api.max_concurrent_requests = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_REQUIRE_TENANT_HEADER") {
        config.api.require_tenant_header = val == "true" || val == "1";
    }
}

fn init_logging(config: &orch8_types::config::LoggingConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    // Write logs to stderr so stdout stays clean for well-behaved pipe
    // consumers. When the server is spawned as a child process (e.g. the
    // e2e test harness) stdout is often left unconsumed; routing tracing
    // output to stderr prevents the stdout pipe buffer from filling up
    // (~64KB on macOS/Linux) and blocking the scheduler's tick loop on
    // its next log write.
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false);

    if config.json {
        subscriber.json().init();
    } else {
        subscriber.init();
    }
}

fn print_startup_banner(config: &EngineConfig, insecure: bool) {
    let version = env!("CARGO_PKG_VERSION");
    let backend = &config.database.backend;
    let http = &config.api.http_addr;
    let grpc = &config.api.grpc_addr;
    let auth = if insecure {
        "disabled (--insecure)"
    } else if config.api.api_key.is_empty() {
        "none"
    } else {
        "api-key"
    };
    let encryption = if config.engine.encryption_key.is_empty() {
        "off"
    } else {
        "AES-256-GCM"
    };
    let tenant = if config.api.require_tenant_header {
        "required"
    } else {
        "optional"
    };
    let tick = config.engine.tick_interval_ms;
    let batch = config.engine.batch_size;
    let concurrency = config.engine.max_concurrent_steps;

    tracing::info!("Starting Orch8.io engine v{version}");
    tracing::info!("  storage={backend}  http={http}  grpc={grpc}");
    tracing::info!("  auth={auth}  encryption={encryption}  tenant-header={tenant}");
    tracing::info!("  tick={tick}ms  batch={batch}  max-concurrent-steps={concurrency}");
    tracing::info!("License: BUSL-1.1 | Managed cloud: https://orch8.io/pricing");
}

fn build_cors_layer(origins: &str) -> CorsLayer {
    use http::header::{HeaderName, AUTHORIZATION, CONTENT_TYPE};
    use http::Method;

    let layer = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            CONTENT_TYPE,
            AUTHORIZATION,
            HeaderName::from_static("x-api-key"),
            // Tenant header is required by `tenant_middleware` — browsers
            // that don't see it in the preflight `Access-Control-Allow-Headers`
            // response strip it from the actual request and the API returns
            // 400 BAD_REQUEST, which looks like an auth bug to the SPA.
            HeaderName::from_static("x-tenant-id"),
            // Trigger secret + replay-protection headers — webhooks called
            // from browsers (dashboard test fire, SaaS-embedded widgets) need
            // these to survive the preflight.
            HeaderName::from_static("x-trigger-secret"),
            HeaderName::from_static("x-trigger-timestamp"),
            HeaderName::from_static("x-trigger-nonce"),
        ]);

    if origins.trim() == "*" {
        layer.allow_origin(AllowOrigin::any())
    } else {
        let parsed: Vec<http::HeaderValue> = origins
            .split(',')
            .filter_map(|o| o.trim().parse().ok())
            .collect();
        layer.allow_origin(parsed)
    }
}
