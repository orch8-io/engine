//! Durable event correlation API: ingest and inspect events.
//!
//! `POST /events` ingests one event (idempotent by producer id) and
//! immediately tries to match it into a waiting `wait_for_event` block.
//! Inspection endpoints list the inbox so operators can see unmatched,
//! consumed, and expired events.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::event_correlation::{EventEnvelope, EventStatus, IngestOutcome};
use orch8_types::ids::TenantId;
use orch8_types::redaction::RedactionPolicy;

use crate::AppState;
use crate::error::ApiError;

const MAX_OUTBOX_BATCH: usize = 100;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/events", post(ingest_event).get(list_events))
        .route("/events/batch", post(ingest_event_batch))
        .route("/events/{id}", get(get_event))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BatchIngestRequest {
    /// Events read from a transactional outbox. Retries are safe because each
    /// item carries its producer idempotency identity.
    events: Vec<IngestEventRequest>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct BatchIngestResponse {
    outcomes: Vec<IngestOutcome>,
}

fn validate_ingest_request(req: &IngestEventRequest) -> Result<(), ApiError> {
    for (field, value) in [
        ("event_name", &req.event_name),
        ("producer_event_id", &req.producer_event_id),
        ("correlation_key", &req.correlation_key),
    ] {
        if value.trim().is_empty() {
            return Err(ApiError::InvalidArgument(format!("{field} is required")));
        }
    }
    Ok(())
}

fn validate_batch_size(size: usize) -> Result<(), ApiError> {
    if size == 0 || size > MAX_OUTBOX_BATCH {
        return Err(ApiError::InvalidArgument(format!(
            "events must contain 1..={MAX_OUTBOX_BATCH} items"
        )));
    }
    Ok(())
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct IngestEventRequest {
    pub tenant_id: String,
    pub event_name: String,
    /// Producer idempotency identity — re-sending the same id is a safe
    /// no-op.
    pub producer_event_id: String,
    pub correlation_key: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[utoipa::path(post, path = "/events", tag = "events",
    request_body = IngestEventRequest,
    responses(
        (status = 200, description = "Duplicate producer id — already ingested", body = IngestOutcome),
        (status = 201, description = "Event ingested (and possibly matched)", body = IngestOutcome),
        (status = 400, description = "Missing fields"),
    )
)]
pub(crate) async fn ingest_event(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<IngestEventRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(&req.tenant_id))?;
    validate_ingest_request(&req)?;

    let envelope = orch8_engine::event_correlation::envelope(
        tenant_id.as_str(),
        &req.event_name,
        &req.producer_event_id,
        &req.correlation_key,
        req.payload,
    );
    let outcome = orch8_engine::event_correlation::ingest(&state.storage, envelope)
        .await
        .map_err(|e| ApiError::from_storage(e, "event"))?;
    let status = if outcome.duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(outcome)))
}

#[utoipa::path(post, path = "/events/batch", tag = "events",
    request_body = BatchIngestRequest,
    responses(
        (status = 200, description = "Outbox batch accepted; individual duplicates are reported", body = BatchIngestResponse),
        (status = 400, description = "Empty, oversized, or invalid batch"),
    )
)]
pub(crate) async fn ingest_event_batch(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(batch): Json<BatchIngestRequest>,
) -> Result<Json<BatchIngestResponse>, ApiError> {
    validate_batch_size(batch.events.len())?;

    // Validate the complete batch before writing its first item. A backend
    // interruption can still yield a partial batch, which is safe to replay
    // because storage deduplicates producer_event_id atomically per item.
    let mut tenant_ids = Vec::with_capacity(batch.events.len());
    for request in &batch.events {
        validate_ingest_request(request)?;
        tenant_ids.push(crate::auth::enforce_tenant_create(
            &tenant_ctx,
            &TenantId::unchecked(&request.tenant_id),
        )?);
    }

    let mut outcomes = Vec::with_capacity(batch.events.len());
    // Intentionally serial: events sharing a correlation key must observe
    // producer order while they CAS a wait's accumulated matches. The hard
    // batch cap bounds the query work; relays page their outbox above it.
    for (request, tenant_id) in batch.events.into_iter().zip(tenant_ids) {
        let envelope = orch8_engine::event_correlation::envelope(
            tenant_id.as_str(),
            &request.event_name,
            &request.producer_event_id,
            &request.correlation_key,
            request.payload,
        );
        outcomes.push(
            orch8_engine::event_correlation::ingest(&state.storage, envelope)
                .await
                .map_err(|error| ApiError::from_storage(error, "event"))?,
        );
    }
    Ok(Json(BatchIngestResponse { outcomes }))
}

#[derive(Deserialize)]
pub(crate) struct ListEventsQuery {
    #[serde(default)]
    tenant_id: Option<String>,
    /// `pending` (unmatched), `consumed`, or `expired`.
    #[serde(default)]
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    100
}

#[utoipa::path(get, path = "/events", tag = "events",
    params(
        ("tenant_id" = Option<String>, Query, description = "Tenant"),
        ("status" = Option<String>, Query, description = "pending | consumed | expired"),
        ("limit" = Option<u32>, Query, description = "Max rows (default 100, max 1000)"),
    ),
    responses((status = 200, description = "Events, newest first (payloads redacted)",
        body = Vec<EventEnvelope>))
)]
pub(crate) async fn list_events(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListEventsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref())
        .ok_or_else(|| ApiError::InvalidArgument("tenant_id is required".into()))?;
    let status = match q.status.as_deref() {
        None => None,
        Some(s) => Some(
            s.parse::<EventStatus>()
                .map_err(ApiError::InvalidArgument)?,
        ),
    };
    let mut events = state
        .storage
        .list_events(scoped.as_str(), status, q.limit.clamp(1, 1000))
        .await
        .map_err(|e| ApiError::from_storage(e, "event"))?;
    let redaction = RedactionPolicy::default();
    for e in &mut events {
        e.payload = redaction.redacted(&e.payload);
    }
    Ok(Json(events))
}

#[utoipa::path(get, path = "/events/{id}", tag = "events",
    params(("id" = Uuid, Path, description = "Event id")),
    responses(
        (status = 200, description = "Event (payload redacted)", body = EventEnvelope),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn get_event(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let mut event = state
        .storage
        .get_event(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "event"))?
        .ok_or_else(|| ApiError::NotFound(format!("event {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId::unchecked(&event.tenant_id),
        &format!("event {id}"),
    )?;
    event.payload = RedactionPolicy::default().redacted(&event.payload);
    Ok(Json(event))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        event_name: &str,
        producer_event_id: &str,
        correlation_key: &str,
    ) -> IngestEventRequest {
        IngestEventRequest {
            tenant_id: "tenant-a".into(),
            event_name: event_name.into(),
            producer_event_id: producer_event_id.into(),
            correlation_key: correlation_key.into(),
            payload: serde_json::json!({"ok": true}),
        }
    }

    #[test]
    fn outbox_item_accepts_complete_identity() {
        assert!(validate_ingest_request(&request("order.created", "evt-1", "order-7")).is_ok());
    }

    #[test]
    fn outbox_item_rejects_blank_event_name() {
        assert!(validate_ingest_request(&request(" \t", "evt-1", "order-7")).is_err());
    }

    #[test]
    fn outbox_item_rejects_blank_producer_event_id() {
        assert!(validate_ingest_request(&request("order.created", "\n", "order-7")).is_err());
    }

    #[test]
    fn outbox_item_rejects_blank_correlation_key() {
        assert!(validate_ingest_request(&request("order.created", "evt-1", "  ")).is_err());
    }

    #[test]
    fn outbox_batch_accepts_lower_boundary() {
        assert!(validate_batch_size(1).is_ok());
    }

    #[test]
    fn outbox_batch_accepts_upper_boundary() {
        assert!(validate_batch_size(MAX_OUTBOX_BATCH).is_ok());
    }

    #[test]
    fn outbox_batch_rejects_empty_input() {
        assert!(validate_batch_size(0).is_err());
    }

    #[test]
    fn outbox_batch_rejects_over_limit_input() {
        assert!(validate_batch_size(MAX_OUTBOX_BATCH + 1).is_err());
    }
}
