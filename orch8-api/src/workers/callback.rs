use axum::{Json, extract::State, http::StatusCode};
use orch8_storage::worker_outcome::WorkerCompletion;
use serde::Deserialize;
use utoipa::ToSchema;

use crate::{AppState, auth::OptionalTenant, error::ApiError};

#[derive(Deserialize, ToSchema)]
pub(crate) struct CompleteCallbackRequest {
    /// Opaque token delivered with the original claimed task.
    pub token: String,
    pub output: serde_json::Value,
}

#[utoipa::path(post, path = "/workers/callbacks/complete", tag = "workers",
    request_body = CompleteCallbackRequest,
    responses(
        (status = 200, description = "Callback output accepted or already recorded"),
        (status = 404, description = "Callback token not found, expired, or outside the caller's tenant"),
        (status = 409, description = "Callback task claim has changed"),
    )
)]
pub(crate) async fn complete(
    State(state): State<AppState>,
    tenant: OptionalTenant,
    Json(request): Json<CompleteCallbackRequest>,
) -> Result<StatusCode, ApiError> {
    let storage = state.storage.as_ref();
    let binding = storage
        .get_worker_callback(tenant.as_ref().map(|ctx| &ctx.tenant_id), &request.token)
        .await
        .map_err(|error| ApiError::from_storage(error, "worker callback"))?
        .ok_or_else(|| ApiError::NotFound("worker callback".into()))?;
    let task_id = binding.task_id;
    let owner = binding.tenant_id.clone();
    let claim = binding.claim.clone();

    if storage
        .get_worker_accepted_outcome(Some(&owner), task_id, &claim)
        .await
        .map_err(|error| ApiError::from_storage(error, "worker callback"))?
        .is_none()
    {
        let task = storage
            .get_worker_task(task_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "worker callback"))?
            .ok_or_else(|| ApiError::NotFound("worker callback".into()))?;
        let instance = storage
            .get_instance(task.instance_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "worker callback"))?
            .filter(|instance| instance.tenant_id == owner)
            .ok_or_else(|| ApiError::NotFound("worker callback".into()))?;
        super::enforce_ownership_fence(&state, &owner, &task, &claim, "callback").await?;
        let projected = orch8_storage::worker_outcome::project_worker_completion_context(
            storage,
            &instance,
            &task,
            &claim,
            &request.output,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "worker callback"))?;
        projected.check_size(state.max_context_bytes)?;

        let completion = WorkerCompletion::for_callback(request.output.clone(), binding);
        let committed = storage
            .accept_worker_completion(task_id, &claim, &completion)
            .await
            .map_err(|error| ApiError::from_storage(error, "worker callback"))?;
        if committed {
            orch8_engine::provenance::record_worker_output_provenance(
                storage,
                state.provenance_signer(),
                &owner,
                &task,
                &request.output,
            )
            .await;
        } else if storage
            .get_worker_accepted_outcome(Some(&owner), task_id, &claim)
            .await
            .map_err(|error| ApiError::from_storage(error, "worker callback"))?
            .is_none()
        {
            return Err(ApiError::Conflict(
                "worker callback claim changed or expired".into(),
            ));
        }
    }
    orch8_engine::delegation::integrate_completed_worker_outcome(
        storage,
        &owner,
        task_id,
        &claim,
        state.max_context_bytes,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(StatusCode::OK)
}
