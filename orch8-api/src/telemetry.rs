//! Mobile telemetry ingestion endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/telemetry/mobile", post(ingest_telemetry))
        .route("/telemetry/mobile/errors", post(ingest_errors))
        .route("/telemetry/mobile/dashboard", post(dashboard_queries))
}

#[derive(Debug, Deserialize)]
pub struct TelemetryBatchItem {
    pub event_type: String,
    pub payload: String,
    pub timestamp: String,
    pub device: DeviceContext,
}

#[derive(Debug, Deserialize)]
pub struct DeviceContext {
    pub device_id: String,
    pub os_name: String,
    pub os_version: String,
    pub app_version: String,
    pub sdk_version: String,
}

#[derive(Debug, Deserialize)]
pub struct IngestTelemetryRequest {
    pub events: Vec<TelemetryBatchItem>,
    pub tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IngestErrorRequest {
    pub error_type: String,
    pub message: String,
    pub stack_trace: Option<String>,
    pub device: DeviceContext,
    pub tenant_id: Option<String>,
    pub instance_id: Option<String>,
    pub sequence_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub accepted: usize,
}

/// Ingest batched telemetry events from mobile devices.
pub(crate) async fn ingest_telemetry(
    State(state): State<AppState>,
    Json(req): Json<IngestTelemetryRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    let tenant = req.tenant_id.unwrap_or_else(|| "default".to_string());
    let mut accepted = 0;

    for event in &req.events {
        let created_at = event.timestamp.parse().unwrap_or_else(|_| Utc::now());

        let result = state
            .storage
            .ingest_telemetry_event(
                &event.event_type,
                &event.payload,
                &event.device.device_id,
                &event.device.os_name,
                &event.device.os_version,
                &event.device.app_version,
                &event.device.sdk_version,
                &tenant,
                created_at,
            )
            .await;

        match result {
            Ok(()) => accepted += 1,
            Err(e) => {
                warn!(error = %e, "failed to insert telemetry event");
            }
        }
    }

    debug!(accepted, total = req.events.len(), "telemetry ingested");
    Ok((StatusCode::ACCEPTED, Json(IngestResponse { accepted })))
}

/// Ingest a structured error report from a mobile device.
pub(crate) async fn ingest_errors(
    State(state): State<AppState>,
    Json(req): Json<IngestErrorRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant = req.tenant_id.unwrap_or_else(|| "default".to_string());

    let result = state
        .storage
        .ingest_telemetry_error(
            &req.error_type,
            &req.message,
            req.stack_trace.as_deref(),
            &req.device.device_id,
            &req.device.os_name,
            &req.device.os_version,
            &req.device.app_version,
            &req.device.sdk_version,
            &tenant,
            req.instance_id.as_deref(),
            req.sequence_name.as_deref(),
        )
        .await;

    match result {
        Ok(()) => {
            // Trigger auto-rollback check if sequence_name is present.
            if let Some(ref seq_name) = req.sequence_name {
                if let Err(e) = check_rollback(&state, &tenant, seq_name).await {
                    warn!(error = %e, "rollback check failed");
                }
            }
            Ok(StatusCode::ACCEPTED)
        }
        Err(e) => {
            warn!(error = %e, "failed to insert error report");
            Err(ApiError::Internal(format!("DB error: {e}")))
        }
    }
}

/// Check if the error rate for a sequence has exceeded its rollback policy
/// threshold, and if so, record a rollback event and unpublish the sequence.
async fn check_rollback(
    state: &AppState,
    tenant_id: &str,
    sequence_name: &str,
) -> Result<(), ApiError> {
    let policy = match state
        .storage
        .get_rollback_policy(tenant_id, sequence_name)
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?
    {
        Some(p) if p.enabled => p,
        _ => return Ok(()),
    };

    let Some(error_rate) = state
        .storage
        .query_error_rate(tenant_id, sequence_name, i64::from(policy.time_window_secs))
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?
    else {
        return Ok(());
    };

    if error_rate < policy.error_rate_threshold {
        return Ok(());
    }

    // Threshold breached — record rollback.
    state
        .storage
        .record_rollback(
            tenant_id,
            sequence_name,
            error_rate,
            policy.error_rate_threshold,
            "error_rate_threshold_breach",
        )
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;

    warn!(
        sequence = %sequence_name,
        tenant = %tenant_id,
        error_rate = %error_rate,
        threshold = %policy.error_rate_threshold,
        "auto-rollback triggered for sequence"
    );

    Ok(())
}

// ── Dashboard Queries ──

#[derive(Debug, Deserialize)]
pub struct DashboardQueryRequest {
    pub query_type: DashboardQueryType,
    pub tenant_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardQueryType {
    SyncCompletedVersions,
    ErrorRatePerSequence,
    TopFailingSteps,
    DeviceOsBreakdown,
}

#[derive(Debug, Serialize)]
pub struct DashboardRow {
    pub dimension: String,
    pub count: i64,
    pub percentage: f64,
}

#[derive(Debug, Serialize)]
pub struct DashboardResponse {
    pub rows: Vec<DashboardRow>,
}

pub(crate) async fn dashboard_queries(
    State(state): State<AppState>,
    Json(req): Json<DashboardQueryRequest>,
) -> Result<Json<DashboardResponse>, ApiError> {
    let tenant = req.tenant_id.unwrap_or_else(|| "default".to_string());
    let start = req
        .start_time
        .unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
    let end = req.end_time.unwrap_or_else(Utc::now);

    let query_type = match req.query_type {
        DashboardQueryType::SyncCompletedVersions => "sync_completed_versions",
        DashboardQueryType::ErrorRatePerSequence => "error_rate_per_sequence",
        DashboardQueryType::TopFailingSteps => "top_failing_steps",
        DashboardQueryType::DeviceOsBreakdown => "device_os_breakdown",
    };

    let raw = state
        .storage
        .query_telemetry_dashboard(query_type, &tenant, start, end)
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;

    let total: i64 = raw.iter().map(|(_, c)| c).sum();
    let rows = raw
        .into_iter()
        .map(|(dim, count)| DashboardRow {
            dimension: dim,
            count,
            percentage: if total > 0 {
                #[allow(clippy::cast_precision_loss)]
                {
                    (count as f64 / total as f64) * 100.0
                }
            } else {
                0.0
            },
        })
        .collect();

    Ok(Json(DashboardResponse { rows }))
}
