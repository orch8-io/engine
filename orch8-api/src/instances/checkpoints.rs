//! Checkpoint save/list/latest/prune handlers.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::ids::InstanceId;

use super::types::CountResponse;
use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize, ToSchema)]
pub struct SaveCheckpointRequest {
    pub(crate) checkpoint_data: serde_json::Value,
}

#[derive(Deserialize, ToSchema)]
pub struct PruneCheckpointsRequest {
    /// Number of most recent checkpoints to keep.
    pub(crate) keep: u32,
}

#[utoipa::path(post, path = "/instances/{id}/checkpoints", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = SaveCheckpointRequest,
    responses(
        (status = 201, description = "Checkpoint saved", body = serde_json::Value),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn save_checkpoint(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<SaveCheckpointRequest>,
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

    let checkpoint = orch8_types::checkpoint::Checkpoint {
        id: Uuid::now_v7(),
        instance_id,
        checkpoint_data: req.checkpoint_data,
        created_at: Utc::now(),
    };

    state
        .storage
        .save_checkpoint(&checkpoint)
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoint"))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "id": checkpoint.id })),
    ))
}

#[utoipa::path(get, path = "/instances/{id}/checkpoints", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, description = "List of checkpoints", body = Vec<orch8_types::checkpoint::Checkpoint>))
)]
pub(crate) async fn list_checkpoints(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let checkpoints = state
        .storage
        .list_checkpoints(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoints"))?;

    Ok(Json(checkpoints))
}

#[utoipa::path(get, path = "/instances/{id}/checkpoints/latest", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Latest checkpoint", body = orch8_types::checkpoint::Checkpoint),
        (status = 404, description = "No checkpoint found"),
    )
)]
pub(crate) async fn get_latest_checkpoint(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let checkpoint = state
        .storage
        .get_latest_checkpoint(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoint"))?
        .ok_or_else(|| ApiError::NotFound(format!("checkpoint for instance {id}")))?;

    Ok(Json(checkpoint))
}

#[utoipa::path(post, path = "/instances/{id}/checkpoints/prune", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = PruneCheckpointsRequest,
    responses((status = 200, description = "Pruned checkpoints", body = CountResponse))
)]
pub(crate) async fn prune_checkpoints(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<PruneCheckpointsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let count = state
        .storage
        .prune_checkpoints(InstanceId(id), req.keep)
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoints"))?;

    Ok(Json(CountResponse { count }))
}
