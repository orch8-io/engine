pub mod auth;
pub mod circuit_breakers;
pub mod cluster;
pub mod cron;
pub mod error;
pub mod health;
pub mod instances;
pub mod metrics;
#[allow(clippy::needless_for_each)]
pub mod openapi;
pub mod plugins;
pub mod pools;
pub mod sequences;
pub mod sessions;
pub mod streaming;
pub mod triggers;
pub mod workers;

use std::sync::Arc;

use axum::Router;
use orch8_storage::StorageBackend;
use tokio_util::sync::CancellationToken;

/// Shared application state injected into all handlers.
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StorageBackend>,
    /// Shutdown signal — long-lived handlers (SSE streams, polling loops) should
    /// observe this to exit cleanly on graceful shutdown.
    pub shutdown: CancellationToken,
}

/// Build the axum router with all routes.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(health::routes())
        .merge(sequences::routes())
        .merge(instances::routes())
        .merge(cron::routes())
        .merge(workers::routes())
        .merge(pools::routes())
        .merge(sessions::routes())
        .merge(cluster::routes())
        .merge(triggers::routes())
        .merge(plugins::routes())
        .with_state(state)
}
