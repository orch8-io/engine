//! Signal dispatch for instances.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use uuid::Uuid;

use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::signal::Signal;

use super::types::SendSignalRequest;
use crate::error::ApiError;
use crate::AppState;

#[utoipa::path(post, path = "/instances/{id}/signals", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = SendSignalRequest,
    responses(
        (status = 201, description = "Signal enqueued", body = serde_json::Value),
        (status = 400, description = "Instance is in terminal state"),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn send_signal(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<SendSignalRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    if instance.state.is_terminal() {
        return Err(ApiError::InvalidArgument(format!(
            "cannot send signal to instance in {} state",
            instance.state
        )));
    }

    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id,
        signal_type: req.signal_type,
        payload: req.payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };

    state
        .storage
        .enqueue_signal(&signal)
        .await
        .map_err(|e| ApiError::from_storage(e, "signal"))?;

    // Wake the instance if it's sitting in Scheduled with a future next_fire_at.
    // This is critical for HITL flows where check_human_input defers the instance
    // for up to 5s — without this, the signal would sit unprocessed until the
    // deferred fire time. Re-fetch the instance to avoid acting on stale state
    // (it may have transitioned to Running while we were enqueuing the signal).
    if let Ok(Some(fresh)) = state.storage.get_instance(instance_id).await {
        if fresh.state == InstanceState::Scheduled {
            let _ = state
                .storage
                .update_instance_state(instance_id, InstanceState::Scheduled, Some(Utc::now()))
                .await;
        }
    }

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "signal_id": signal.id })),
    ))
}
