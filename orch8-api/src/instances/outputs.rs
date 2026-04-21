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
    //
    // Perf: collect all ref_keys and resolve them in a single batched query
    // instead of N+1 round-trips.
    let mut ref_keys = Vec::new();
    let mut output_indices = Vec::new();
    for (idx, out) in outputs.iter().enumerate() {
        if let Some(ref_key) = orch8_engine::externalized::extract_ref_key(&out.output) {
            ref_keys.push(ref_key.to_string());
            output_indices.push(idx);
        }
    }
    if !ref_keys.is_empty() {
        match state.storage.batch_get_externalized_state(&ref_keys).await {
            Ok(resolved_map) => {
                for (i, ref_key) in ref_keys.iter().enumerate() {
                    let idx = output_indices[i];
                    match resolved_map.get(ref_key) {
                        Some(resolved) => {
                            outputs[idx].output = resolved.clone();
                        }
                        None => {
                            tracing::warn!(
                                instance_id = %id,
                                block_id = %outputs[idx].block_id,
                                ref_key,
                                "get_outputs: externalized payload missing — returning marker unchanged"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    instance_id = %id,
                    error = %e,
                    "get_outputs: batched externalized resolution failed — returning markers unchanged"
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
