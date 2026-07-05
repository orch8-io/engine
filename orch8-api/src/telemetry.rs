//! Mobile telemetry ingestion endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::AppState;
use crate::auth::{OptionalTenant, scoped_tenant_id};
use crate::error::ApiError;

const MAX_BATCH_SIZE: usize = 500;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/telemetry/mobile", post(ingest_telemetry))
        .route("/telemetry/mobile/errors", post(ingest_errors))
        .route("/telemetry/mobile/dashboard", get(dashboard_queries))
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
    tenant_ctx: OptionalTenant,
    Json(req): Json<IngestTelemetryRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    if req.events.len() > MAX_BATCH_SIZE {
        return Err(ApiError::PayloadTooLarge(format!(
            "batch size {} exceeds maximum of {MAX_BATCH_SIZE}",
            req.events.len()
        )));
    }
    let tenant = scoped_tenant_id(&tenant_ctx, req.tenant_id.as_deref())
        .map_or_else(|| "default".to_string(), |t| t.as_str().to_string());

    let events: Vec<orch8_storage::TelemetryEvent> = req
        .events
        .iter()
        .map(|e| orch8_storage::TelemetryEvent {
            event_type: e.event_type.clone(),
            payload: e.payload.clone(),
            device_id: e.device.device_id.clone(),
            os_name: e.device.os_name.clone(),
            os_version: e.device.os_version.clone(),
            app_version: e.device.app_version.clone(),
            sdk_version: e.device.sdk_version.clone(),
            tenant_id: tenant.clone(),
            created_at: e.timestamp.parse().unwrap_or_else(|_| Utc::now()),
        })
        .collect();

    let accepted = state
        .storage
        .ingest_telemetry_events_batch(&events)
        .await
        .map_err(|e| {
            warn!(error = %e, "batch telemetry insert failed");
            ApiError::Internal(format!("DB error: {e}"))
        })?;

    #[allow(clippy::cast_possible_truncation)]
    let accepted = accepted as usize;

    debug!(accepted, total = req.events.len(), "telemetry ingested");
    Ok((StatusCode::ACCEPTED, Json(IngestResponse { accepted })))
}

/// Ingest a structured error report from a mobile device.
pub(crate) async fn ingest_errors(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(req): Json<IngestErrorRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant = scoped_tenant_id(&tenant_ctx, req.tenant_id.as_deref())
        .map_or_else(|| "default".to_string(), |t| t.as_str().to_string());

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
            if let Some(ref seq_name) = req.sequence_name
                && let Err(e) = check_rollback(&state, &tenant, seq_name).await
            {
                warn!(error = %e, "rollback check failed");
            }
            Ok(StatusCode::ACCEPTED)
        }
        Err(e) => {
            warn!(error = %e, "failed to insert error report");
            Err(ApiError::Internal(format!("DB error: {e}")))
        }
    }
}

fn webhook_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .build()
            .unwrap_or_default()
    })
}
/// Check if the error rate for a sequence has exceeded its rollback policy
/// threshold, and if so, record a rollback event and unpublish the sequence.
///
/// Hysteresis guards:
/// 1. **Cooldown**: if a rollback was triggered within `cooldown_secs`, skip.
/// 2. **Sustained breach**: if `confirmation_window_secs > 0`, also check
///    the error rate over that shorter window — both must exceed threshold.
#[allow(clippy::too_many_lines)]
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

    // Guard 1: cooldown — skip if a rollback happened recently.
    if policy.cooldown_secs > 0 {
        let recent = state
            .storage
            .list_rollback_history(Some(tenant_id), Some(sequence_name), 1)
            .await
            .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;
        if let Some(last) = recent.first() {
            let elapsed = Utc::now()
                .signed_duration_since(last.triggered_at)
                .num_seconds();
            if elapsed < i64::from(policy.cooldown_secs) {
                debug!(
                    sequence = %sequence_name,
                    cooldown_remaining = i64::from(policy.cooldown_secs) - elapsed,
                    "rollback check skipped — cooldown active"
                );
                return Ok(());
            }
        }
    }

    // Primary check: error rate over the full time window.
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

    // Guard 2: sustained breach — also check the confirmation window.
    if policy.confirmation_window_secs > 0
        && policy.confirmation_window_secs < policy.time_window_secs
    {
        let confirm_rate = state
            .storage
            .query_error_rate(
                tenant_id,
                sequence_name,
                i64::from(policy.confirmation_window_secs),
            )
            .await
            .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;
        match confirm_rate {
            Some(r) if r >= policy.error_rate_threshold => {}
            _ => {
                debug!(
                    sequence = %sequence_name,
                    full_window_rate = %error_rate,
                    "rollback check skipped — confirmation window rate below threshold"
                );
                return Ok(());
            }
        }
    }

    // Threshold breached + hysteresis passed — record rollback.
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

    // Deprecate + unpublish the sequence so mobile clients stop using it on next sync.
    let tenant = orch8_types::ids::TenantId::unchecked(tenant_id.to_string());
    let ns = orch8_types::ids::Namespace::new("default");
    if let Ok(Some(seq)) = state
        .storage
        .get_sequence_by_name(&tenant, &ns, sequence_name, None)
        .await
    {
        if let Err(e) = state.storage.deprecate_sequence(seq.id).await {
            warn!(error = %e, "failed to deprecate sequence during rollback");
        }
        if let Err(e) = state
            .storage
            .update_sequence_status(seq.id, "unpublished")
            .await
        {
            warn!(error = %e, "failed to update sequence status during rollback");
        }
    }

    // Attempt manifest regeneration so the rolled-back sequence is removed on next sync.
    if let Some(ref publisher) = state.publisher {
        if let Err(e) = state.storage.acquire_manifest_lock(tenant_id).await {
            warn!(error = %e, "failed to acquire manifest lock during rollback");
        } else {
            let removed = vec![orch8_publisher::ManifestRemoved {
                name: sequence_name.to_string(),
                removed_at: chrono::Utc::now(),
            }];
            if let Err(e) = publisher.publish_manifest(vec![], removed, vec![]).await {
                warn!(error = %e, "failed to regenerate manifest during rollback");
            }
            if let Err(e) = state.storage.release_manifest_lock(tenant_id).await {
                warn!(error = %e, "failed to release manifest lock during rollback");
            }
        }
    }

    warn!(
        sequence = %sequence_name,
        tenant = %tenant_id,
        error_rate = %error_rate,
        threshold = %policy.error_rate_threshold,
        cooldown_secs = %policy.cooldown_secs,
        "auto-rollback triggered — sequence deprecated and unpublished"
    );

    if let Some(ref url) = policy.webhook_url {
        let payload = serde_json::json!({
            "event": "rollback_triggered",
            "tenant_id": tenant_id,
            "sequence_name": sequence_name,
            "error_rate": error_rate,
            "threshold": policy.error_rate_threshold,
            "timestamp": Utc::now().to_rfc3339(),
        });
        let client = webhook_client().clone();
        let url = url.clone();
        tokio::spawn(async move {
            match client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
            {
                Ok(resp) => {
                    debug!(status = %resp.status(), url = %url, "rollback webhook delivered");
                }
                Err(e) => {
                    warn!(error = %e, url = %url, "rollback webhook delivery failed");
                }
            }
        });
    }

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
    tenant_ctx: OptionalTenant,
    axum::extract::Query(req): axum::extract::Query<DashboardQueryRequest>,
) -> Result<Json<DashboardResponse>, ApiError> {
    let tenant = scoped_tenant_id(&tenant_ctx, req.tenant_id.as_deref())
        .map_or_else(|| "default".to_string(), |t| t.as_str().to_string());
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
