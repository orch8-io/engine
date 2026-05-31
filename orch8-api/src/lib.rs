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
pub mod mobile_sync;
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
pub mod usage;
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

/// Canonical API version prefix. All versioned endpoints live under this path.
/// Operational endpoints (health, metrics) remain at the root.
pub const API_V1_PREFIX: &str = "/api/v1";

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
    pub push_provider: Arc<dyn orch8_push::PushProvider>,
    pub mobile_sync_enabled: bool,
}

/// Assemble the versioned API sub-router (without state applied).
///
/// This is the set of business-logic routes that belong under `/api/v1`.
/// Operational endpoints (health, metrics, swagger) are **not** included.
fn api_routes() -> Router<AppState> {
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
        .merge(usage::routes())
}

/// Build the axum router with all routes.
///
/// Business-logic routes are served under `/api/v1/...` (canonical) and also
/// at the bare path (`/...`) for backward compatibility with existing clients.
/// The bare-path mount is deprecated and will be removed in a future major
/// release.
pub fn build_router(state: AppState) -> Router {
    let mut api = api_routes();

    if state.mobile_sync_enabled {
        api = api.merge(mobile_sync::routes());
    }

    Router::new()
        // Canonical versioned mount — clients should migrate to these paths.
        .nest(API_V1_PREFIX, api.clone())
        // Backward-compat: keep the same routes at the root so existing
        // clients and SDKs continue to work during the migration window.
        // TODO(v2): remove this bare merge once all clients use /api/v1.
        .merge(api)
        .with_state(state)
}
