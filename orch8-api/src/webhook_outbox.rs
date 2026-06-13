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

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/webhooks/outbox", get(list_outbox))
        .route("/webhooks/outbox/{id}", delete(discard_outbox))
        .route("/webhooks/outbox/{id}/redeliver", post(redeliver_outbox))
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
}

#[utoipa::path(get, path = "/webhooks/outbox", tag = "webhooks",
    params(("limit" = Option<u32>, Query, description = "Max rows (default 100, max 1000)")),
    responses((status = 200, description = "Parked webhook deliveries",
        body = Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>))
)]
pub(crate) async fn list_outbox(
    State(state): State<AppState>,
    Query(q): Query<ListOutboxQuery>,
) -> Result<impl IntoResponse, ApiError> {
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
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let entry = state
        .storage
        .get_webhook_outbox(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?
        .ok_or_else(|| ApiError::NotFound(format!("webhook_outbox {id}")))?;

    match orch8_engine::webhooks::redeliver(&entry, &state.shutdown).await {
        Ok(()) => {
            // Delivered — remove the parked row.
            state
                .storage
                .delete_webhook_outbox(id)
                .await
                .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?;
            Ok((StatusCode::OK, Json(RedeliverResponse { redelivered: true })))
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
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .storage
        .delete_webhook_outbox(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "webhook_outbox"))?;
    Ok(StatusCode::NO_CONTENT)
}
