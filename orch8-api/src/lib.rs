mod error;
pub mod health;

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
    Router::new().merge(health::routes()).with_state(state)
}
