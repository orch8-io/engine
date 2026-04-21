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

    // Tenant enforcement still needs the current instance row. This read is
    // the TOCTOU window — between reading here and persisting the signal
    // below, the instance can transition to a terminal state. We close the
    // window by persisting via `enqueue_signal_if_active`, which re-checks
    // state inside the same transaction as the INSERT. The tenant check
    // below stays read-side because the tenant_id is immutable for the
    // life of an instance, so no race is possible.
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

    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id,
        signal_type: req.signal_type,
        payload: req.payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };

    // Atomic enqueue: fails with `TerminalTarget` if the instance
    // transitioned to Completed / Failed / Cancelled while we were
    // preparing the signal. Map that to a 400 here (not the default 409)
    // so the handler surfaces the same "cannot send signal to terminal
    // instance" shape the pre-check used to return.
    state
        .storage
        .enqueue_signal_if_active(&signal)
        .await
        .map_err(|e| match e {
            orch8_types::error::StorageError::TerminalTarget { .. } => ApiError::InvalidArgument(
                format!("cannot send signal to instance {id}: target is in a terminal state"),
            ),
            orch8_types::error::StorageError::NotFound { .. } => {
                ApiError::NotFound(format!("instance {id}"))
            }
            other => ApiError::from_storage(other, "signal"),
        })?;

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
