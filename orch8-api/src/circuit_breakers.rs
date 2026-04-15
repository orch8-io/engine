use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::error::ApiError;

use std::sync::Arc;

use orch8_engine::circuit_breaker::CircuitBreakerRegistry;

#[derive(Clone)]
pub struct CircuitBreakerState {
    pub registry: Arc<CircuitBreakerRegistry>,
}

pub fn routes() -> Router<CircuitBreakerState> {
    Router::new()
        .route("/circuit-breakers", get(list_breakers))
        .route("/circuit-breakers/{handler}", get(get_breaker))
        .route("/circuit-breakers/{handler}/reset", post(reset_breaker))
}

#[utoipa::path(
    get,
    path = "/circuit-breakers",
    responses((status = 200, body = Vec<orch8_types::circuit_breaker::CircuitBreakerState>))
)]
async fn list_breakers(State(state): State<CircuitBreakerState>) -> impl IntoResponse {
    Json(state.registry.list_all())
}

#[utoipa::path(
    get,
    path = "/circuit-breakers/{handler}",
    params(("handler" = String, Path, description = "Handler name")),
    responses(
        (status = 200, body = orch8_types::circuit_breaker::CircuitBreakerState),
        (status = 404, description = "Handler not found"),
    )
)]
async fn get_breaker(
    State(state): State<CircuitBreakerState>,
    Path(handler): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .registry
        .get(&handler)
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("circuit_breaker {handler}")))
}

#[utoipa::path(
    post,
    path = "/circuit-breakers/{handler}/reset",
    params(("handler" = String, Path, description = "Handler name")),
    responses((status = 200, description = "Circuit breaker reset"))
)]
async fn reset_breaker(
    State(state): State<CircuitBreakerState>,
    Path(handler): Path<String>,
) -> impl IntoResponse {
    state.registry.reset(&handler);
    StatusCode::OK
}
