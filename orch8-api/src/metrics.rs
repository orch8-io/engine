use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;

/// Metrics-specific state (separate from `AppState` to avoid circular deps).
#[derive(Clone)]
pub struct MetricsState {
    pub handle: PrometheusHandle,
}

pub fn routes() -> Router<MetricsState> {
    Router::new().route("/metrics", get(prometheus_metrics))
}

async fn prometheus_metrics(State(state): State<MetricsState>) -> impl IntoResponse {
    let output = state.handle.render();
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
}
