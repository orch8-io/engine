use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use orch8_api::metrics::MetricsState;
use orch8_api::{build_router, AppState};
use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::Engine;
use orch8_storage::postgres::PostgresStorage;
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
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load configuration: TOML file (optional) -> env vars -> defaults.
    let config = load_config(&cli.config)?;

    // Initialize logging.
    init_logging(&config.logging);

    tracing::info!("Starting Orch8.io engine v{}", env!("CARGO_PKG_VERSION"));

    // Connect to Postgres.
    let storage = PostgresStorage::new(&config.database.url, config.database.max_connections)
        .await
        .context("Failed to connect to PostgreSQL")?;

    tracing::info!("Connected to PostgreSQL");

    // Run migrations.
    if config.database.run_migrations {
        storage
            .run_migrations()
            .await
            .context("Failed to run migrations")?;
        tracing::info!("Migrations applied");
    }

    let storage = Arc::new(storage);

    // Install Prometheus metrics recorder.
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = prometheus_handle.handle();
    metrics::set_global_recorder(prometheus_handle).expect("failed to install Prometheus recorder");

    // Build HTTP router.
    let app_state = AppState {
        storage: storage.clone(),
    };
    let metrics_state = MetricsState { handle };
    let app = build_router(app_state).merge(orch8_api::metrics::routes().with_state(metrics_state));

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

    // Wait for engine to finish draining.
    let _ = engine_handle.await;

    tracing::info!("Shutdown complete");
    Ok(())
}

fn load_config(path: &str) -> anyhow::Result<EngineConfig> {
    let mut config = if let Ok(contents) = std::fs::read_to_string(path) {
        toml::from_str::<EngineConfig>(&contents).context("Failed to parse config TOML")?
    } else {
        tracing::debug!("No config file found at {path}, using defaults + env vars");
        EngineConfig::default()
    };
    apply_env_overrides(&mut config);
    Ok(config)
}

fn apply_env_overrides(config: &mut EngineConfig) {
    if let Ok(url) = std::env::var("ORCH8_DATABASE_URL") {
        config.database.url = url;
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
    if let Ok(val) = std::env::var("ORCH8_WEBHOOK_URLS") {
        config.engine.webhooks.urls = val.split(',').map(|s| s.trim().to_string()).collect();
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
