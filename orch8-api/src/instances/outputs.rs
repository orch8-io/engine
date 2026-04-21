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
    //
    // Ref#7: tolerate per-output resolution failures. A storage error or a
    // missing externalized ref on a single output must not fail the entire
    // endpoint — return what we have, log the failure, and leave the marker
    // visible so callers can detect partial results.
    for out in &mut outputs {
        let Some(ref_key) = orch8_engine::externalized::extract_ref_key(&out.output) else {
            continue;
        };
        match state.storage.get_externalized_state(ref_key).await {
            Ok(Some(resolved)) => {
                out.output = resolved;
            }
            Ok(None) => {
                tracing::warn!(
                    instance_id = %id,
                    block_id = %out.block_id,
                    ref_key,
                    "get_outputs: externalized payload missing — returning marker unchanged"
                );
            }
            Err(e) => {
                tracing::warn!(
                    instance_id = %id,
                    block_id = %out.block_id,
                    ref_key,
                    error = %e,
                    "get_outputs: failed to resolve externalized payload — returning marker unchanged"
                );
            }
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
