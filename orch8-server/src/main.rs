#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use tower_http::cors::{AllowOrigin, CorsLayer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use orch8_api::circuit_breakers::CircuitBreakerState;
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
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load configuration: TOML file (optional) -> env vars -> defaults.
    let config = load_config(&cli.config)?;

    // Initialize logging.
    init_logging(&config.logging);

    tracing::info!("Starting Orch8.io engine v{}", env!("CARGO_PKG_VERSION"));

    // Connect to storage backend.
    let storage: Arc<dyn StorageBackend> = if config.database.backend == "sqlite" {
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
        Arc::new(sqlite)
    } else {
        let pg = PostgresStorage::new(config.database.url.expose(), config.database.max_connections)
            .await
            .context("Failed to connect to PostgreSQL")?;

        tracing::info!("Connected to PostgreSQL");

        if config.database.run_migrations {
            pg.run_migrations()
                .await
                .context("Failed to run migrations")?;
            tracing::info!("Migrations applied");
        }
        Arc::new(pg)
    };

    // Wrap storage with encryption layer if an encryption key is configured.
    let storage: Arc<dyn StorageBackend> = {
        let env_key = std::env::var("ORCH8_ENCRYPTION_KEY").unwrap_or_default();
        let key = if config.engine.encryption_key.is_empty() {
            &env_key
        } else {
            config.engine.encryption_key.expose()
        };
        if key.is_empty() {
            storage
        } else {
            let encryptor = orch8_types::encryption::FieldEncryptor::from_hex_key(key)
                .context("Invalid encryption key (expected 64 hex chars for AES-256-GCM)")?;
            tracing::info!("Encryption at rest enabled for context.data");
            Arc::new(orch8_storage::encrypting::EncryptingStorage::new(
                storage, encryptor,
            ))
        }
    };

    // Install Prometheus metrics recorder.
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = prometheus_handle.handle();
    metrics::set_global_recorder(prometheus_handle).expect("failed to install Prometheus recorder");

    // Build HTTP router.
    let app_state = AppState {
        storage: storage.clone(),
    };
    let metrics_state = MetricsState { handle };
    let cb_registry = Arc::new(CircuitBreakerRegistry::new(5, 60));
    let cb_state = CircuitBreakerState {
        registry: cb_registry,
    };
    let cors = build_cors_layer(&config.api.cors_origins);
    let api_key = config.api.api_key.expose().to_string();
    if api_key.is_empty() {
        tracing::warn!("No API key configured — all endpoints are unauthenticated. Set ORCH8_API_KEY to enable auth.");
    } else if config.api.cors_origins.trim() == "*" {
        tracing::warn!("CORS allows all origins ('*') while API key auth is enabled. Consider restricting ORCH8_CORS_ORIGINS to trusted origins.");
    }
    let require_tenant = config.api.require_tenant_header;
    let mut app = build_router(app_state)
        .merge(orch8_api::circuit_breakers::routes().with_state(cb_state))
        .merge(orch8_api::metrics::routes().with_state(metrics_state))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(axum::middleware::from_fn(move |req, next| {
            orch8_api::auth::tenant_middleware(require_tenant, req, next)
        }))
        .layer(axum::middleware::from_fn(move |req, next| {
            orch8_api::auth::api_key_middleware(api_key.clone(), req, next)
        }))
        .layer(cors);

    // Apply global concurrency limit if configured (caps in-flight requests).
    if config.api.rate_limit_rps > 0 {
        let limit = config.api.rate_limit_rps;
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

    // Graceful shutdown.
    let shutdown_token = CancellationToken::new();
    let token = shutdown_token.clone();
    tokio::spawn(async move {
        let mut sigint =
            tokio::signal::unix::signal(SignalKind::interrupt()).expect("SIGINT handler");
        let mut sigterm =
            tokio::signal::unix::signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => tracing::info!("Received SIGINT"),
            _ = sigterm.recv() => tracing::info!("Received SIGTERM"),
        }
        token.cancel();
    });

    // Start gRPC server.
    let grpc_addr: std::net::SocketAddr = config
        .api
        .grpc_addr
        .parse()
        .context("Invalid gRPC listen address")?;

    let grpc_service = Orch8GrpcService::new(storage.clone());
    let grpc_shutdown = shutdown_token.clone();
    let grpc_handle = tokio::spawn(async move {
        tracing::info!("gRPC server listening on {}", grpc_addr);
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(Orch8ServiceServer::new(grpc_service))
            .serve_with_shutdown(grpc_addr, async move {
                grpc_shutdown.cancelled().await;
            })
            .await
        {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    // Build and start the scheduling engine.
    let mut handlers = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut handlers);
    let engine = Engine::new(
        storage.clone(),
        config.engine.clone(),
        handlers,
        shutdown_token.clone(),
    );

    let engine_handle = tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            tracing::error!(error = %e, "Engine tick loop exited with error");
        }
    });

    tracing::info!("Engine ready");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_token.cancelled().await;
            tracing::info!("Shutting down gracefully...");
        })
        .await
        .context("HTTP server error")?;

    // Wait for engine and gRPC to finish draining (with timeout).
    let drain_timeout = tokio::time::Duration::from_secs(30);
    if tokio::time::timeout(drain_timeout, async {
        let _ = engine_handle.await;
        let _ = grpc_handle.await;
    })
    .await
    .is_err()
    {
        tracing::warn!("Shutdown drain timed out after {drain_timeout:?}, forcing exit");
    }

    tracing::info!("Shutdown complete");
    Ok(())
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
    if let Ok(val) = std::env::var("ORCH8_RATE_LIMIT_RPS") {
        if let Ok(n) = val.parse() {
            config.api.rate_limit_rps = n;
        }
    }
    if let Ok(val) = std::env::var("ORCH8_REQUIRE_TENANT_HEADER") {
        config.api.require_tenant_header = val == "true" || val == "1";
    }
}

fn init_logging(config: &orch8_types::config::LoggingConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
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

fn build_cors_layer(origins: &str) -> CorsLayer {
    use http::header::{AUTHORIZATION, CONTENT_TYPE};
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
        .allow_headers([CONTENT_TYPE, AUTHORIZATION, "x-api-key".parse().unwrap()]);

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
