use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;

use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/info", get(info))
}

/// Liveness probe: returns 200 while the process is running, unless an engine
/// runs here and its tick loop has not completed an iteration within
/// `engine.liveness_stale_secs` — then 503, so the orchestrator restarts a
/// wedged scheduler instead of keeping it forever (I-H11).
#[utoipa::path(get, path = "/health/live", tag = "health",
    responses(
        (status = 200, description = "Process is alive"),
        (status = 503, description = "Engine tick loop is wedged"),
    )
)]
pub(crate) async fn liveness(State(state): State<AppState>) -> impl IntoResponse {
    if state
        .engine_liveness
        .as_ref()
        .is_some_and(|l| l.is_live_stale())
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    StatusCode::OK
}

/// Readiness probe: returns 200 only if the database is reachable AND the
/// engine tick loop is still running and ticking. A dead or wedged scheduler
/// with a live DB must not report ready — otherwise the LB keeps routing to a
/// server that accepts work but never executes it.
#[utoipa::path(get, path = "/health/ready", tag = "health",
    responses(
        (status = 200, description = "Database reachable and engine running"),
        (status = 503, description = "Database unreachable or engine stopped or stalled"),
    )
)]
pub(crate) async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    if !state
        .engine_ready
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if state
        .engine_liveness
        .as_ref()
        .is_some_and(|l| l.is_ready_stale())
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
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
        "storage_schema_version": orch8_storage::STORAGE_SCHEMA_VERSION,
        "contract_schema_version": orch8_types::contract::CONTRACT_SCHEMA_VERSION,
        "env_label": std::env::var("ORCH8_ENV_LABEL").ok(),
        "env_color": std::env::var("ORCH8_ENV_COLOR").ok(),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use orch8_engine::liveness::EngineLiveness;
    use tokio_util::sync::CancellationToken;

    use super::{liveness, readiness};

    async fn state(liveness: Option<Arc<EngineLiveness>>) -> crate::AppState {
        let storage: Arc<dyn orch8_storage::StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let mut state =
            crate::test_harness::test_state(storage, CancellationToken::new(), false, 0, None);
        state.engine_liveness = liveness;
        state
    }

    async fn status(resp: impl IntoResponse) -> StatusCode {
        resp.into_response().status()
    }

    /// I-H11: a wedged tick loop fails liveness (so the orchestrator restarts
    /// it); an engine that ticks, or no engine at all, stays live.
    #[tokio::test]
    async fn liveness_fails_when_armed_engine_stops_ticking() {
        let l = Arc::new(EngineLiveness::new(
            Duration::from_millis(1),
            Duration::from_millis(20),
        ));
        let st = state(Some(Arc::clone(&l))).await;
        assert_eq!(
            status(liveness(State(st.clone())).await).await,
            StatusCode::OK,
            "unarmed (no engine running yet) is live"
        );
        l.arm();
        assert_eq!(
            status(liveness(State(st.clone())).await).await,
            StatusCode::OK
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            status(liveness(State(st.clone())).await).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        l.record_tick();
        assert_eq!(status(liveness(State(st)).await).await, StatusCode::OK);
        let no_engine = state(None).await;
        assert_eq!(
            status(liveness(State(no_engine)).await).await,
            StatusCode::OK
        );
    }

    /// Readiness is 200 for a ticking engine and 503 once the engine flag
    /// is cleared (engine exited or panicked).
    #[tokio::test]
    async fn readiness_reflects_engine_flag_and_tick() {
        let l = Arc::new(EngineLiveness::new(
            Duration::from_millis(100),
            Duration::from_secs(300),
        ));
        l.arm();
        let st = state(Some(l)).await;
        assert_eq!(
            status(readiness(State(st.clone())).await).await,
            StatusCode::OK
        );
        st.engine_ready
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            status(readiness(State(st)).await).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
