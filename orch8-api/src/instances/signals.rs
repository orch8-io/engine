//! Signal dispatch for instances.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use uuid::Uuid;

use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::signal::{Signal, SignalType};

use super::types::SendSignalRequest;
use crate::AppState;
use crate::error::ApiError;

#[utoipa::path(post, path = "/instances/{id}/signals", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = SendSignalRequest,
    responses(
        (status = 201, description = "Signal enqueued", body = serde_json::Value),
        (status = 400, description = "Instance is in terminal state"),
        (status = 404, description = "Instance not found"),
    )
)]
pub async fn send_signal(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    principal: Option<axum::Extension<crate::auth::PrincipalContext>>,
    Path(id): Path<Uuid>,
    Json(req): Json<SendSignalRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId::from_uuid(id);

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

    // The route is also reachable with the scoped `Approver` capability, which
    // must only be able to answer human-in-the-loop gates — never pause,
    // cancel, rewrite context, or fire arbitrary custom events. Checked after
    // the tenant lookup so unknown/foreign instances still answer 404.
    if !crate::auth::principal_is_operator(principal.as_ref().map(|axum::Extension(p)| p))
        && !is_human_input_signal(&req.signal_type)
    {
        return Err(ApiError::Forbidden(
            "API key may only send human_input signals".into(),
        ));
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
    if let Ok(Some(fresh)) = state.storage.get_instance(instance_id).await
        && fresh.state == InstanceState::Scheduled
    {
        // CAS on Scheduled: the instance may have transitioned (Running,
        // terminal) between the re-fetch and this write — an unconditional
        // update would resurrect it as Scheduled.
        let _ = state
            .storage
            .conditional_update_instance_state(
                instance_id,
                InstanceState::Scheduled,
                InstanceState::Scheduled,
                Some(Utc::now()),
            )
            .await;
    }

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "signal_id": signal.id })),
    ))
}

/// `custom:human_input:<block>` — the reply to a human-in-the-loop gate.
fn is_human_input_signal(signal_type: &SignalType) -> bool {
    matches!(signal_type, SignalType::Custom(name) if name.starts_with("human_input:"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_human_input_custom_signals_are_approver_signals() {
        assert!(is_human_input_signal(&SignalType::Custom(
            "human_input:review".into()
        )));
        assert!(!is_human_input_signal(&SignalType::Custom("go".into())));
        assert!(!is_human_input_signal(&SignalType::Cancel));
        assert!(!is_human_input_signal(&SignalType::Pause));
        assert!(!is_human_input_signal(&SignalType::UpdateContext));
    }
}
