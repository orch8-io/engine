pub mod api_keys;
pub mod approvals;
pub mod auth;
pub mod circuit_breakers;
pub mod cluster;
pub mod credentials;
pub mod cron;
pub mod diagnosis;
pub mod error;
pub mod health;
pub mod input_schema;
pub mod inspect;
pub mod instances;
pub mod mcp_server;
pub mod metrics;
pub mod mobile_sync;
pub mod model_pricing;
#[allow(clippy::needless_for_each)]
pub mod openapi;
pub mod plugins;
pub mod pools;
pub mod preflight;
pub mod queue_dispatch;
pub mod queue_routing;
pub mod request_id;
pub mod rollback;
pub mod security;
pub mod sequences;
pub mod sessions;
pub mod streaming;
pub mod telemetry;
pub mod test_harness;
pub mod triggers;
pub mod usage;
pub mod webhook_outbox;
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
    pub fn from_vec(mut items: Vec<T>, limit: u32) -> Self {
        let has_more = items.len() > limit as usize;
        if has_more {
            items.truncate(limit as usize);
        }
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
    /// Names of handlers the engine executes in-process. Served by
    /// `GET /handlers` alongside externally-registered handler names.
    pub builtin_handlers: Arc<Vec<String>>,
    /// Liveness flag for the engine tick loop. Starts `true`; the engine task
    /// flips it to `false` if `run()` exits (error or clean stop). `/health/ready`
    /// reports 503 when it is `false` so a load balancer stops routing traffic to
    /// a server whose scheduler has died — otherwise the process would keep
    /// answering HTTP while executing nothing.
    pub engine_ready: Arc<std::sync::atomic::AtomicBool>,
}

/// Compute the built-in handler name list for [`AppState::builtin_handlers`].
///
/// Builds a throwaway [`orch8_engine::handlers::HandlerRegistry`] with all
/// built-ins registered and returns its sorted name list.
#[must_use]
pub fn builtin_handler_names() -> Vec<String> {
    let mut registry = orch8_engine::handlers::HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut registry);
    registry
        .handler_names()
        .into_iter()
        .map(ToString::to_string)
        .collect()
}

/// Assemble the versioned API sub-router (without state applied).
///
/// This is the set of business-logic routes that belong under `/api/v1`.
/// Operational endpoints (health, metrics, swagger) are **not** included.
fn api_routes() -> Router<AppState> {
    Router::new()
        .merge(sequences::routes())
        .merge(preflight::routes())
        .merge(approvals::routes())
        .merge(instances::routes())
        .merge(diagnosis::routes())
        .merge(inspect::routes())
        .merge(cron::routes())
        .merge(workers::routes())
        .merge(pools::routes())
        .merge(sessions::routes())
        .merge(cluster::routes())
        .merge(triggers::routes())
        .merge(plugins::routes())
        .merge(credentials::routes())
        .merge(api_keys::routes())
        .merge(telemetry::routes())
        .merge(rollback::routes())
        .merge(usage::routes())
        .merge(webhook_outbox::routes())
        .merge(queue_routing::routes())
        .merge(queue_dispatch::routes())
        .merge(mcp_server::routes())
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
        // NOTE: health/info routes are intentionally NOT mounted here.
        // `orch8-server` mounts them *after* the auth layers so liveness/
        // readiness probes stay reachable when an API key is configured.
        // Mounting them here too would double-register the handlers (axum
        // panics on overlapping routes) and would also place them behind
        // auth. The test harness mounts them separately.
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::PaginatedResponse;

    #[test]
    fn paginated_response_marks_more_only_when_an_extra_row_was_fetched() {
        let exact = PaginatedResponse::from_vec(vec![1, 2], 2);
        assert!(!exact.has_more);
        assert_eq!(exact.items, vec![1, 2]);

        let extra = PaginatedResponse::from_vec(vec![1, 2, 3], 2);
        assert!(extra.has_more);
        assert_eq!(extra.items, vec![1, 2]);
    }
}
