//! Retarget a webhook while preserving its credentials and other configuration.
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::{AppState, error::ApiError};
use orch8_types::ids::SequenceId;

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RetargetTriggerRequest {
    pub sequence_id: SequenceId,
    pub expected_updated_at: DateTime<Utc>,
}

#[utoipa::path(patch, path = "/triggers/{slug}/target", tag = "triggers",
    params(("slug" = String, Path, description = "Trigger slug")),
    request_body = RetargetTriggerRequest,
    responses(
        (status = 204, description = "Trigger target updated; credentials and settings preserved"),
        (status = 404, description = "Trigger or sequence not found"),
        (status = 409, description = "Trigger changed concurrently or sequence name/version is ambiguous"),
    )
)]
pub async fn retarget_trigger(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(slug): Path<String>,
    Json(request): Json<RetargetTriggerRequest>,
) -> Result<StatusCode, ApiError> {
    let mut trigger = state
        .storage
        .get_trigger(
            tenant_ctx
                .as_ref()
                .map(|axum::Extension(context)| &context.tenant_id),
            &slug,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "trigger"))?
        .ok_or_else(|| ApiError::NotFound("trigger".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &trigger.tenant_id, "trigger")?;
    if trigger.updated_at != request.expected_updated_at {
        return Err(ApiError::Conflict(
            "trigger changed; reload before retargeting".into(),
        ));
    }
    let sequence = state
        .storage
        .get_sequence(request.sequence_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?
        .filter(|sequence| sequence.tenant_id == trigger.tenant_id)
        .ok_or_else(|| ApiError::NotFound("sequence".into()))?;
    // Trigger execution resolves a pinned namespace/name/version tuple. Verify
    // that this lookup selects the requested sequence before changing anything.
    let resolved = state
        .storage
        .get_sequence_by_name(
            &trigger.tenant_id,
            &sequence.namespace,
            &sequence.name,
            Some(sequence.version),
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?;
    if resolved.as_ref().map(|value| value.id) != Some(sequence.id) {
        return Err(ApiError::Conflict(
            "sequence name/version is ambiguous".into(),
        ));
    }
    trigger.sequence_name = sequence.name;
    trigger.namespace = sequence.namespace.as_str().to_owned();
    trigger.version = Some(sequence.version);
    if !state
        .storage
        .update_trigger_cas(&trigger, request.expected_updated_at)
        .await
        .map_err(|error| ApiError::from_storage(error, "trigger"))?
    {
        return Err(ApiError::Conflict(
            "trigger changed; reload before retargeting".into(),
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}
