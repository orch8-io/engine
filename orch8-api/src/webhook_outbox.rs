//! Webhook outbox management.
//!
//! When an outbound webhook exhausts its retries the engine parks it instead of
//! dropping it. These operator endpoints list parked deliveries, redeliver one
//! (a fresh send-with-retry pass through the engine's webhook config), and
//! discard one. The outbox is global (webhook config is operator-level, not
//! per-tenant), so these endpoints are not tenant-scoped.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::AppState;
use crate::api_keys::require_admin;
use crate::auth::OptionalAdmin;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/webhooks/outbox", get(list_outbox))
        .route("/webhooks/outbox/{id}", delete(discard_outbox))
        .route("/webhooks/outbox/{id}/redeliver", post(redeliver_outbox))
        .route(
            "/webhooks/outbox/{id}/redeliver-preview",
            get(redeliver_preview),
        )
        .route("/webhooks/deliveries", get(list_deliveries))
        .route("/webhooks/deliveries/{delivery_id}", get(get_delivery))
}

#[derive(Deserialize)]
pub(crate) struct ListOutboxQuery {
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    100
}

#[derive(serde::Serialize, ToSchema)]
pub struct RedeliverResponse {
    /// Whether the redelivery succeeded (and the outbox row was removed).
    redelivered: bool,
    /// The new attempt group recorded for this redelivery pass — inspect
    /// it via `GET /webhooks/deliveries/{delivery_id}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery_id: Option<Uuid>,
}

#[utoipa::path(get, path = "/webhooks/outbox", tag = "webhooks",
    params(("limit" = Option<u32>, Query, description = "Max rows (default 100, max 1000)")),
    responses((status = 200, description = "Parked webhook deliveries",
        body = Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>))
)]
pub(crate) async fn list_outbox(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Query(q): Query<ListOutboxQuery>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    let limit = q.limit.clamp(1, 1000);
    let entries = state
        .storage
        .list_webhook_outbox(limit)
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?;
    Ok(Json(entries))
}

#[utoipa::path(post, path = "/webhooks/outbox/{id}/redeliver", tag = "webhooks",
    params(("id" = Uuid, Path, description = "Parked delivery id")),
    responses(
        (status = 200, description = "Redelivered (row removed)", body = RedeliverResponse),
        (status = 404, description = "No such parked delivery"),
        (status = 502, description = "Redelivery failed (row kept)"),
    )
)]
pub(crate) async fn redeliver_outbox(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    let entry = state
        .storage
        .get_webhook_outbox(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?
        .ok_or_else(|| ApiError::NotFound(format!("webhook_outbox {id}")))?;

    match orch8_engine::webhooks::redeliver(&entry, &state.shutdown).await {
        Ok(delivery_id) => {
            // Delivered — remove the parked row.
            state
                .storage
                .delete_webhook_outbox(id)
                .await
                .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?;
            Ok((
                StatusCode::OK,
                Json(RedeliverResponse {
                    redelivered: true,
                    delivery_id: Some(delivery_id),
                }),
            ))
        }
        Err(reason) => Err(ApiError::BadGateway(format!(
            "redelivery to {} failed: {reason}",
            entry.url
        ))),
    }
}

#[utoipa::path(delete, path = "/webhooks/outbox/{id}", tag = "webhooks",
    params(("id" = Uuid, Path, description = "Parked delivery id")),
    responses((status = 204, description = "Discarded"))
)]
pub(crate) async fn discard_outbox(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    state
        .storage
        .delete_webhook_outbox(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Delivery inspector
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub(crate) struct ListDeliveriesQuery {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    event_type: Option<String>,
    /// `true` = only delivered, `false` = only failed.
    #[serde(default)]
    delivered: Option<bool>,
    /// Normalized error class of the final attempt (e.g. `timeout`,
    /// `redirect_rejected`, `http_status`).
    #[serde(default)]
    error_class: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

#[utoipa::path(get, path = "/webhooks/deliveries", tag = "webhooks",
    params(
        ("url" = Option<String>, Query, description = "Filter by endpoint URL"),
        ("event_type" = Option<String>, Query, description = "Filter by event type"),
        ("delivered" = Option<bool>, Query, description = "true = delivered, false = failed"),
        ("error_class" = Option<String>, Query, description = "Final-attempt error class"),
        ("limit" = Option<u32>, Query, description = "Max rows (default 100, max 1000)"),
    ),
    responses((status = 200, description = "Delivery summaries, newest first",
        body = Vec<orch8_types::webhook_delivery::WebhookDeliverySummary>))
)]
pub(crate) async fn list_deliveries(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Query(q): Query<ListDeliveriesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    let error_class = match q.error_class.as_deref() {
        None => None,
        Some(s) => Some(
            serde_json::from_value::<orch8_types::webhook_delivery::DeliveryErrorClass>(
                serde_json::Value::String(s.to_string()),
            )
            .map_err(|_| ApiError::InvalidArgument(format!("unknown error_class '{s}'")))?,
        ),
    };
    let filter = orch8_types::webhook_delivery::DeliveryFilter {
        url: q.url,
        event_type: q.event_type,
        delivered: q.delivered,
        error_class,
    };
    let summaries = state
        .storage
        .list_webhook_deliveries(&filter, q.limit.clamp(1, 1000))
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_deliveries"))?;
    Ok(Json(summaries))
}

#[utoipa::path(get, path = "/webhooks/deliveries/{delivery_id}", tag = "webhooks",
    params(("delivery_id" = Uuid, Path, description = "Delivery (attempt group) id")),
    responses(
        (status = 200, description = "Attempt timeline for one delivery",
            body = Vec<orch8_types::webhook_delivery::WebhookDeliveryAttempt>),
        (status = 404, description = "No attempts recorded for this delivery"),
    )
)]
pub(crate) async fn get_delivery(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(delivery_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    let attempts = state
        .storage
        .get_webhook_delivery_attempts(delivery_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_deliveries"))?;
    if attempts.is_empty() {
        return Err(ApiError::NotFound(format!("delivery {delivery_id}")));
    }
    Ok(Json(attempts))
}

/// What a redelivery *would* send, without sending it. Secrets never
/// appear: the signature is described, not exposed.
#[derive(serde::Serialize, ToSchema)]
pub struct RedeliverPreview {
    pub url: String,
    pub event_type: String,
    /// Serialized payload size in bytes.
    pub payload_bytes: usize,
    /// Whether the request will carry `X-Orch8-Timestamp` /
    /// `X-Orch8-Signature` HMAC headers.
    pub will_be_signed: bool,
    /// Attempt history of the pass that parked this entry, if linked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_delivery_id: Option<Uuid>,
    /// How many attempts were made before parking.
    pub previous_attempts: i32,
    /// The last error seen before parking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[utoipa::path(get, path = "/webhooks/outbox/{id}/redeliver-preview", tag = "webhooks",
    params(("id" = Uuid, Path, description = "Parked delivery id")),
    responses(
        (status = 200, description = "What a redelivery would send (no side effect)",
            body = RedeliverPreview),
        (status = 404, description = "No such parked delivery"),
    )
)]
pub(crate) async fn redeliver_preview(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    let entry = state
        .storage
        .get_webhook_outbox(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?
        .ok_or_else(|| ApiError::NotFound(format!("webhook_outbox {id}")))?;
    let payload_bytes = serde_json::to_vec(&entry.payload).map_or(0, |v| v.len());
    Ok(Json(RedeliverPreview {
        url: entry.url,
        event_type: entry.event_type,
        payload_bytes,
        will_be_signed: orch8_engine::webhooks::redelivery_will_sign(),
        previous_delivery_id: entry.delivery_id,
        previous_attempts: entry.attempts,
        last_error: entry.last_error,
    }))
}
