//! Read-side handlers: block outputs and execution tree.

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use uuid::Uuid;

use orch8_types::ids::InstanceId;

use crate::error::ApiError;
use crate::AppState;

#[utoipa::path(get, path = "/instances/{id}/outputs", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, description = "Block outputs", body = Vec<orch8_types::output::BlockOutput>))
)]
pub(crate) async fn get_outputs(
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

    let mut outputs = state
        .storage
        .get_all_outputs(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "outputs"))?;

    // Inflate externalization markers in-place so API consumers see real data.
    // See `docs/CONTEXT_MANAGEMENT.md` §8.5.
    for out in &mut outputs {
        if let Some(ref_key) = orch8_engine::externalized::extract_ref_key(&out.output) {
            if let Some(resolved) = state
                .storage
                .get_externalized_state(ref_key)
                .await
                .map_err(|e| ApiError::from_storage(e, "externalized_state"))?
            {
                out.output = resolved;
            }
            // Missing payload: leave marker visible so callers can detect.
        }
    }

    Ok(Json(outputs))
}

#[utoipa::path(get, path = "/instances/{id}/tree", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Execution tree", body = Vec<orch8_types::execution::ExecutionNode>),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn get_execution_tree(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
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

    let tree = state
        .storage
        .get_execution_tree(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;

    Ok(Json(tree))
}
