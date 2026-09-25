//! Read-side handlers: block outputs and execution tree.

use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use uuid::Uuid;

use orch8_types::ids::InstanceId;

use crate::AppState;
use crate::error::ApiError;

#[utoipa::path(get, path = "/instances/{id}/outputs", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, description = "Block outputs", body = Vec<orch8_types::output::BlockOutput>))
)]
pub async fn get_outputs(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId::from_uuid(id))
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
        .get_all_outputs(InstanceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "outputs"))?;

    strip_internal_outputs(&mut outputs);

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
    let mut ref_keys = Vec::with_capacity(outputs.len());
    let mut output_indices = Vec::with_capacity(outputs.len());
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
pub async fn get_execution_tree(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId::from_uuid(id);

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

/// Drop internal bookkeeping rows that are not user-facing outputs.
///
/// Two kinds exist, neither a real step output:
///   - `__in_progress__`: a crash-recovery sentinel written before handler
///     execution.
///   - `__retry__`: a retry marker written after a retryable failure to
///     advance the attempt counter (see `scheduler/step_exec.rs`).
///
/// Once a real output exists for the block these are noise and are removed —
/// otherwise a caller doing `outputs.find(block_id)` could pick up the marker
/// instead of the answer. On a terminal/mid-retry path where the marker or
/// sentinel is the ONLY row for the block it is kept, so callers can still see
/// the step ran.
fn strip_internal_outputs(outputs: &mut Vec<orch8_types::output::BlockOutput>) {
    fn is_internal(o: &orch8_types::output::BlockOutput) -> bool {
        matches!(
            o.output_ref.as_deref(),
            Some("__in_progress__" | "__retry__")
        )
    }
    let blocks_with_real_output: std::collections::HashSet<String> = outputs
        .iter()
        .filter(|o| !is_internal(o))
        .map(|o| o.block_id.as_str().to_owned())
        .collect();
    outputs.retain(|o| !is_internal(o) || !blocks_with_real_output.contains(o.block_id.as_str()));
}

#[cfg(test)]
mod tests {
    use super::strip_internal_outputs;
    use orch8_types::ids::{BlockId, InstanceId};
    use orch8_types::output::BlockOutput;

    fn out(block: &str, output_ref: Option<&str>, value: &str) -> BlockOutput {
        BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: InstanceId::new(),
            block_id: BlockId::new(block),
            output: serde_json::json!({ "v": value }),
            output_ref: output_ref.map(str::to_owned),
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn strips_retry_marker_and_sentinel_when_real_output_exists() {
        // Order mimics storage (created_at ASC): marker, sentinel, then the
        // real output. A caller's `find(block)` must land on the real output.
        let mut outputs = vec![
            out("s1", Some("__retry__"), "retry-bookkeeping"),
            out("s1", Some("__in_progress__"), "sentinel"),
            out("s1", None, "ok after retry"),
        ];
        strip_internal_outputs(&mut outputs);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].output["v"], "ok after retry");
    }

    #[test]
    fn keeps_marker_when_it_is_the_only_row() {
        // Mid-retry / permanent-failure with no real output: keep the row so
        // callers can see the step ran rather than seeing nothing.
        let mut outputs = vec![out("s1", Some("__retry__"), "still-retrying")];
        strip_internal_outputs(&mut outputs);
        assert_eq!(outputs.len(), 1);
    }
}
