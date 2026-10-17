//! Mobile telemetry ingestion endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use orch8_storage::telemetry_validation::{
    MAX_ERROR_MESSAGE_BYTES, MAX_ERROR_STACK_BYTES, device_field_limits, validate_event_fields,
};

use crate::AppState;
use crate::auth::{OptionalTenant, scoped_tenant_id};
use crate::error::ApiError;

const MAX_BATCH_SIZE: usize = 500;
/// Widest time range one dashboard query may scan.
const MAX_DASHBOARD_WINDOW_DAYS: i64 = 31;

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

#[derive(Debug, Clone, Deserialize)]
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

/// Backward-compatible wire model. New mobile clients send one batch-level
/// device while older clients may continue sending a device on every event.
#[derive(Debug, Deserialize)]
pub(crate) struct IngestTelemetryWireRequest {
    events: Vec<TelemetryBatchItemWire>,
    device: Option<DeviceContext>,
    tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelemetryBatchItemWire {
    event_type: String,
    payload: String,
    timestamp: String,
    device: Option<DeviceContext>,
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
    Json(wire_request): Json<IngestTelemetryWireRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    if wire_request.events.len() > MAX_BATCH_SIZE {
        return Err(ApiError::PayloadTooLarge(format!(
            "batch size {} exceeds maximum of {MAX_BATCH_SIZE}",
            wire_request.events.len()
        )));
    }
    let req = normalize_ingest_request(wire_request)?;
    let tenant = scoped_tenant_id(&tenant_ctx, req.tenant_id.as_deref())
        .map_or_else(|| "default".to_string(), |t| t.as_str().to_string());

    let total = req.events.len();
    // Same field/payload/event-type bounds as the gRPC transport. A bad event
    // rejects the batch with the offending index so the device can drop it
    // instead of retrying the whole batch forever.
    let mut accepted_payload_bytes = 0usize;
    let mut events: Vec<orch8_storage::TelemetryEvent> = Vec::with_capacity(total);
    let now = Utc::now();
    for (index, e) in req.events.into_iter().enumerate() {
        let event_type = validate_event_fields(
            &e.event_type,
            &e.payload,
            device_field_limits(
                &e.device.device_id,
                &e.device.os_name,
                &e.device.os_version,
                &e.device.app_version,
                &e.device.sdk_version,
            ),
            accepted_payload_bytes,
        )
        .map_err(|r| {
            ApiError::InvalidArgument(format!("events[{index}]: {}: {}", r.code, r.message))
        })?
        .to_owned();
        accepted_payload_bytes = accepted_payload_bytes.saturating_add(e.payload.len());
        // Client time is advisory only (rollback windows use server receive
        // time); a skewed or malicious future timestamp is clamped to now.
        let created_at = e
            .timestamp
            .parse::<DateTime<Utc>>()
            .map_or(now, |t| t.min(now));
        events.push(orch8_storage::TelemetryEvent {
            event_type,
            payload: e.payload,
            device_id: e.device.device_id,
            os_name: e.device.os_name,
            os_version: e.device.os_version,
            app_version: e.device.app_version,
            sdk_version: e.device.sdk_version,
            tenant_id: tenant.clone(),
            created_at,
        });
    }

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

    debug!(accepted, total, "telemetry ingested");
    Ok((StatusCode::ACCEPTED, Json(IngestResponse { accepted })))
}

fn normalize_ingest_request(
    request: IngestTelemetryWireRequest,
) -> Result<IngestTelemetryRequest, ApiError> {
    let IngestTelemetryWireRequest {
        events,
        device: batch_device,
        tenant_id,
    } = request;
    let events = events
        .into_iter()
        .map(|event| {
            let device = event
                .device
                .or_else(|| batch_device.clone())
                .ok_or_else(|| {
                    ApiError::InvalidArgument(
                        "device is required at batch or telemetry-event level".into(),
                    )
                })?;
            Ok(TelemetryBatchItem {
                event_type: event.event_type,
                payload: event.payload,
                timestamp: event.timestamp,
                device,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(IngestTelemetryRequest { events, tenant_id })
}

/// Ingest a structured error report from a mobile device.
pub(crate) async fn ingest_errors(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(req): Json<IngestErrorRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant = scoped_tenant_id(&tenant_ctx, req.tenant_id.as_deref())
        .map_or_else(|| "default".to_string(), |t| t.as_str().to_string());

    if req.error_type.is_empty() || req.error_type.len() > 128 {
        return Err(ApiError::InvalidArgument(
            "error_type must contain 1..=128 bytes".into(),
        ));
    }
    if req.message.len() > MAX_ERROR_MESSAGE_BYTES {
        return Err(ApiError::PayloadTooLarge(format!(
            "message exceeds {MAX_ERROR_MESSAGE_BYTES} bytes"
        )));
    }
    if req
        .stack_trace
        .as_ref()
        .is_some_and(|t| t.len() > MAX_ERROR_STACK_BYTES)
    {
        return Err(ApiError::PayloadTooLarge(format!(
            "stack_trace exceeds {MAX_ERROR_STACK_BYTES} bytes"
        )));
    }
    if req.sequence_name.as_ref().is_some_and(|n| n.len() > 256)
        || req.instance_id.as_ref().is_some_and(|n| n.len() > 256)
    {
        return Err(ApiError::InvalidArgument(
            "sequence_name and instance_id must be at most 256 bytes".into(),
        ));
    }
    for (name, value, max) in device_field_limits(
        &req.device.device_id,
        &req.device.os_name,
        &req.device.os_version,
        &req.device.app_version,
        &req.device.sdk_version,
    ) {
        if value.len() > max {
            return Err(ApiError::InvalidArgument(format!(
                "{name} exceeds {max} bytes"
            )));
        }
    }

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
            // Auto-rollback check runs off the request path and is debounced
            // per (tenant, sequence): an error burst costs one evaluation per
            // interval instead of several queries per report.
            if let Some(seq_name) = req.sequence_name {
                schedule_rollback_check(&state, &tenant, &seq_name);
            }
            Ok(StatusCode::ACCEPTED)
        }
        Err(e) => {
            warn!(error = %e, "failed to insert error report");
            Err(ApiError::Internal(format!("DB error: {e}")))
        }
    }
}

/// Minimum spacing between rollback evaluations for one (tenant, sequence).
pub(crate) const ROLLBACK_CHECK_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(1);
/// Hard cap on tracked keys; stale ones are pruned past this.
const ROLLBACK_DEBOUNCE_MAX_KEYS: usize = 10_000;

struct DebounceEntry {
    last_run: std::time::Instant,
    trailing_scheduled: bool,
}

type DebounceMap = std::collections::HashMap<(String, String), DebounceEntry>;

fn debounce_map() -> &'static std::sync::Mutex<DebounceMap> {
    static MAP: std::sync::OnceLock<std::sync::Mutex<DebounceMap>> = std::sync::OnceLock::new();
    MAP.get_or_init(Default::default)
}

/// What the debouncer decided for one report.
#[derive(Debug, PartialEq, Eq)]
enum Debounce {
    /// Run now.
    Now,
    /// Run once after the given delay (a trailing run, so the last report of a
    /// burst is always evaluated).
    After(std::time::Duration),
    /// A run is already pending; nothing to do.
    Skip,
}

fn debounce_decision(tenant_id: &str, sequence_name: &str, now: std::time::Instant) -> Debounce {
    let mut map = debounce_map()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if map.len() >= ROLLBACK_DEBOUNCE_MAX_KEYS {
        map.retain(|_, e| {
            e.trailing_scheduled || now.duration_since(e.last_run) < ROLLBACK_CHECK_DEBOUNCE * 60
        });
    }
    let key = (tenant_id.to_owned(), sequence_name.to_owned());
    match map.get_mut(&key) {
        None => {
            map.insert(
                key,
                DebounceEntry {
                    last_run: now,
                    trailing_scheduled: false,
                },
            );
            Debounce::Now
        }
        Some(e) if e.trailing_scheduled => Debounce::Skip,
        Some(e) => {
            let since = now.duration_since(e.last_run);
            if since >= ROLLBACK_CHECK_DEBOUNCE {
                e.last_run = now;
                Debounce::Now
            } else {
                e.trailing_scheduled = true;
                Debounce::After(ROLLBACK_CHECK_DEBOUNCE.saturating_sub(since))
            }
        }
    }
}

fn mark_trailing_ran(tenant_id: &str, sequence_name: &str) {
    let mut map = debounce_map()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(e) = map.get_mut(&(tenant_id.to_owned(), sequence_name.to_owned())) {
        e.trailing_scheduled = false;
        e.last_run = std::time::Instant::now();
    }
}

fn schedule_rollback_check(state: &AppState, tenant_id: &str, sequence_name: &str) {
    let delay = match debounce_decision(tenant_id, sequence_name, std::time::Instant::now()) {
        Debounce::Skip => return,
        Debounce::Now => None,
        Debounce::After(d) => Some(d),
    };
    let state = state.clone();
    let tenant = tenant_id.to_owned();
    let seq = sequence_name.to_owned();
    tokio::spawn(async move {
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
            mark_trailing_ran(&tenant, &seq);
        }
        if let Err(e) = check_rollback(&state, &tenant, &seq).await {
            warn!(error = %e, "rollback check failed");
        }
    });
}

fn webhook_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        orch8_engine::outbound::build(
            orch8_engine::outbound::builder(orch8_engine::outbound::Profile::Untrusted)
                .timeout(std::time::Duration::from_secs(10))
                .pool_max_idle_per_host(2),
        )
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

    // Guard 0: the name is client-supplied. Only a published (production,
    // non-deprecated) sequence of this tenant can be rolled back.
    let tenant = orch8_types::ids::TenantId::unchecked(tenant_id.to_string());
    let ns = orch8_types::ids::Namespace::new("default");
    let published = state
        .storage
        .get_sequence_by_name(&tenant, &ns, sequence_name, None)
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?
        .is_some_and(|s| {
            !s.deprecated && s.status == orch8_types::sequence::SequenceStatus::Production
        });
    if !published {
        debug!(sequence = %sequence_name, "rollback check skipped: not a published sequence of this tenant");
        return Ok(());
    }

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

    // Primary check: error rate over the full time window, once the window
    // holds at least `min_samples` observed runs.
    let (failed, total) = state
        .storage
        .query_error_stats(tenant_id, sequence_name, i64::from(policy.time_window_secs))
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;
    if total < i64::from(policy.min_samples.max(1)) {
        debug!(
            sequence = %sequence_name,
            total,
            min_samples = policy.min_samples,
            "rollback check skipped: below minimum sample size"
        );
        return Ok(());
    }
    #[allow(clippy::cast_precision_loss)]
    let error_rate = failed as f64 / total as f64;

    if error_rate < policy.error_rate_threshold {
        return Ok(());
    }

    // Guard 2: sustained breach — also check the confirmation window.
    if policy.confirmation_window_secs > 0
        && policy.confirmation_window_secs < policy.time_window_secs
    {
        let (c_failed, c_total) = state
            .storage
            .query_error_stats(
                tenant_id,
                sequence_name,
                i64::from(policy.confirmation_window_secs),
            )
            .await
            .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;
        #[allow(clippy::cast_precision_loss)]
        let confirmed =
            c_total > 0 && (c_failed as f64 / c_total as f64) >= policy.error_rate_threshold;
        if !confirmed {
            debug!(
                sequence = %sequence_name,
                full_window_rate = %error_rate,
                "rollback check skipped — confirmation window rate below threshold"
            );
            return Ok(());
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
        match state.storage.acquire_manifest_lock(tenant_id).await {
            Err(e) => warn!(error = %e, "failed to acquire manifest lock during rollback"),
            Ok(_guard) => {
                // Read-modify-write: drop only this sequence from the current
                // manifest. Publishing `vec![]` here would unpublish every
                // other sequence for the tenant (the publisher refuses when it
                // does not know the current manifest).
                if let Err(e) = publisher
                    .remove_from_manifest(sequence_name, chrono::Utc::now())
                    .await
                {
                    warn!(error = %e, "failed to regenerate manifest during rollback");
                }
                // `_guard` drops here (success, error, or panic), releasing the lock.
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
                    debug!(status = %resp.status(), url = %orch8_engine::outbound::redact_url(&url), "rollback webhook delivered");
                }
                Err(e) => {
                    warn!(error = %orch8_engine::outbound::redact_error(&e), url = %orch8_engine::outbound::redact_url(&url), "rollback webhook delivery failed");
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
    // Cap the scanned window so one request cannot aggregate the whole table.
    let start = start.max(end - chrono::Duration::days(MAX_DASHBOARD_WINDOW_DAYS));

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

#[cfg(test)]
mod tests {
    use super::*;

    fn device(device_id: &str) -> DeviceContext {
        DeviceContext {
            device_id: device_id.to_string(),
            os_name: "iOS".to_string(),
            os_version: "18".to_string(),
            app_version: "1".to_string(),
            sdk_version: "1".to_string(),
        }
    }

    #[test]
    fn event_device_overrides_batch_device() {
        let request = IngestTelemetryWireRequest {
            events: vec![
                TelemetryBatchItemWire {
                    event_type: "fallback".into(),
                    payload: "{}".into(),
                    timestamp: "2026-01-01T00:00:00Z".into(),
                    device: None,
                },
                TelemetryBatchItemWire {
                    event_type: "override".into(),
                    payload: "{}".into(),
                    timestamp: "2026-01-01T00:00:00Z".into(),
                    device: Some(device("event")),
                },
            ],
            device: Some(device("batch")),
            tenant_id: None,
        };

        let normalized = normalize_ingest_request(request).unwrap();
        assert_eq!(normalized.events[0].device.device_id, "batch");
        assert_eq!(normalized.events[1].device.device_id, "event");
    }
}
