pub mod approvals;
pub mod auth;
pub mod circuit_breakers;
pub mod cluster;
pub mod credentials;
pub mod cron;
pub mod error;
pub mod health;
pub mod instances;
pub mod metrics;
#[allow(clippy::needless_for_each)]
pub mod openapi;
pub mod plugins;
pub mod pools;
pub mod request_id;
pub mod rollback;
pub mod sequences;
pub mod sessions;
pub mod streaming;
pub mod telemetry;
pub mod test_harness;
pub mod triggers;
pub mod webhooks;
pub mod workers;

use serde::Serialize;
use std::sync::Arc;

use axum::Router;
use orch8_engine::circuit_breaker::CircuitBreakerRegistry;
use orch8_storage::StorageBackend;
use orch8_types::config::ExternalizationMode;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Default ceiling on concurrent SSE streams per process.
pub const DEFAULT_MAX_CONCURRENT_STREAMS: usize = 256;

/// Generic paginated response envelope for list endpoints.
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub has_more: bool,
}

impl<T: Serialize> PaginatedResponse<T> {
    pub fn from_vec(items: Vec<T>, limit: u32) -> Self {
        let has_more = u32::try_from(items.len()).unwrap_or(u32::MAX) >= limit && limit > 0;
        Self { items, has_more }
    }
}

/// Shared application state injected into all handlers.
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StorageBackend>,
    pub shutdown: CancellationToken,
    pub max_context_bytes: u32,
    pub externalization_mode: ExternalizationMode,
    pub circuit_breakers: Option<Arc<CircuitBreakerRegistry>>,
    pub stream_limiter: Arc<Semaphore>,
    /// Optional publisher for manifest/sequence publishing.
    pub publisher: Option<Arc<orch8_publisher::SequencePublisher>>,
}

/// Build the axum router with all routes.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(sequences::routes())
        .merge(approvals::routes())
        .merge(instances::routes())
        .merge(cron::routes())
        .merge(workers::routes())
        .merge(pools::routes())
        .merge(sessions::routes())
        .merge(cluster::routes())
        .merge(triggers::routes())
        .merge(plugins::routes())
        .merge(credentials::routes())
        .merge(telemetry::routes())
        .merge(rollback::routes())
        .with_state(state)
}
