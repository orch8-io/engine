use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/info", get(info))
}

/// Liveness probe: returns 200 if the process is running.
#[utoipa::path(get, path = "/health/live", tag = "health",
    responses((status = 200, description = "Process is alive"))
)]
pub(crate) async fn liveness() -> impl IntoResponse {
    StatusCode::OK
}

/// Readiness probe: returns 200 if the database is reachable.
#[utoipa::path(get, path = "/health/ready", tag = "health",
    responses(
        (status = 200, description = "Database is reachable"),
        (status = 503, description = "Database is unreachable"),
    )
)]
pub(crate) async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.ping().await {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Deployment info for UI chrome: engine version plus the optional
/// operator-set environment label/color (`ORCH8_ENV_LABEL`,
/// `ORCH8_ENV_COLOR`). Read straight from the environment — this is
/// cosmetic metadata, not engine configuration, so it does not flow
/// through `EngineConfig`.
#[utoipa::path(get, path = "/info", tag = "health",
    responses((status = 200, description = "Engine version and environment label"))
)]
pub(crate) async fn info() -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "env_label": std::env::var("ORCH8_ENV_LABEL").ok(),
        "env_color": std::env::var("ORCH8_ENV_COLOR").ok(),
    }))
}
