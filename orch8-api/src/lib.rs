pub mod error;
pub mod health;
pub mod instances;
pub mod sequences;

use std::sync::Arc;

use axum::Router;
use orch8_storage::StorageBackend;

/// Shared application state injected into all handlers.
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StorageBackend>,
}

/// Build the axum router with all routes.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(health::routes())
        .merge(sequences::routes())
        .merge(instances::routes())
        .with_state(state)
}
