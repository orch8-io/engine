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
pub mod sequences;
pub mod sessions;
pub mod streaming;
pub mod test_harness;
pub mod triggers;
pub mod webhooks;
pub mod workers;

use std::sync::Arc;

use axum::Router;
use orch8_engine::circuit_breaker::CircuitBreakerRegistry;
use orch8_storage::StorageBackend;
use orch8_types::config::ExternalizationMode;
use tokio_util::sync::CancellationToken;

/// Shared application state injected into all handlers.
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StorageBackend>,
    /// Shutdown signal — long-lived handlers (SSE streams, polling loops) should
    /// observe this to exit cleanly on graceful shutdown.
    pub shutdown: CancellationToken,
    /// Maximum serialized bytes accepted for an instance `ExecutionContext`
    /// on write paths (`POST /instances`, `PATCH /instances/{id}/context`).
    /// `0` disables the check. Mirrors `SchedulerConfig::max_context_bytes`
    /// so the API rejects oversized contexts before they hit the DB.
    pub max_context_bytes: u32,
    /// Policy controlling when context fields / block outputs are externalized
    /// into `externalized_state` and replaced in the inline context by a
    /// `{"_externalized": true, "_ref": "..."}` marker. Mirrors
    /// `SchedulerConfig::externalization_mode`.
    pub externalization_mode: ExternalizationMode,
    /// Optional handle to the circuit-breaker registry. Present in production
    /// wiring so worker-task success/failure reports from external workers
    /// can record into the same registry the scheduler and inspection API use.
    /// `None` in unit-test harnesses that don't need breaker integration.
    pub circuit_breakers: Option<Arc<CircuitBreakerRegistry>>,
}

/// Build the axum router with all routes.
///
/// Note: `health::routes()` is intentionally NOT included here so main.rs can
/// merge it after the auth middleware — health/liveness probes must remain
/// reachable regardless of the configured API key or tenant-header policy.
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
        .with_state(state)
}
