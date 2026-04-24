//! Dynamic step injection.

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::ids::InstanceId;

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize, ToSchema)]
pub struct InjectBlocksRequest {
    /// Blocks must be a valid JSON array of `BlockDefinition` objects.
    pub blocks: serde_json::Value,
    /// Position to inject at (0-indexed). If omitted, blocks are appended.
    #[serde(default)]
    pub position: Option<usize>,
}

/// Validate injected blocks and return their IDs.
fn validate_injected_blocks(blocks: &serde_json::Value) -> Result<Vec<String>, ApiError> {
    let arr = blocks
        .as_array()
        .ok_or_else(|| ApiError::InvalidArgument("blocks must be a JSON array".into()))?;
    if arr.is_empty() {
        return Err(ApiError::InvalidArgument(
            "blocks array must not be empty".into(),
        ));
    }
    let mut ids = Vec::with_capacity(arr.len());
    for (i, block) in arr.iter().enumerate() {
        let def = serde_json::from_value::<orch8_types::sequence::BlockDefinition>(block.clone())
            .map_err(|_| {
            ApiError::InvalidArgument(format!("blocks[{i}] is not a valid BlockDefinition"))
        })?;
        ids.push(block_def_id(&def));
    }
    Ok(ids)
}

/// Extract the ID from any block definition variant.
fn block_def_id(def: &orch8_types::sequence::BlockDefinition) -> String {
    use orch8_types::sequence::BlockDefinition;
    match def {
        BlockDefinition::Step(s) => s.id.0.clone(),
        BlockDefinition::Parallel(p) => p.id.0.clone(),
        BlockDefinition::Race(r) => r.id.0.clone(),
        BlockDefinition::Loop(l) => l.id.0.clone(),
        BlockDefinition::ForEach(f) => f.id.0.clone(),
        BlockDefinition::Router(r) => r.id.0.clone(),
        BlockDefinition::TryCatch(t) => t.id.0.clone(),
        BlockDefinition::SubSequence(s) => s.id.0.clone(),
        BlockDefinition::ABSplit(a) => a.id.0.clone(),
        BlockDefinition::CancellationScope(cs) => cs.id.0.clone(),
    }
}

#[utoipa::path(
    post,
    path = "/instances/{id}/inject-blocks",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = InjectBlocksRequest,
    responses((status = 200, description = "Blocks injected"))
)]
pub async fn inject_blocks(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(body): Json<InjectBlocksRequest>,
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

    let block_ids = validate_injected_blocks(&body.blocks)?;

    // Merge + write in a single storage transaction so two concurrent
    // position-targeted injections can't both read the same pre-image and
    // clobber each other on the write-back.
    let final_blocks = state
        .storage
        .inject_blocks_at_position(InstanceId(id), &body.blocks, body.position)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok(Json(serde_json::json!({
        "injected_block_ids": block_ids,
        "position": body.position,
        "total_injected": final_blocks.as_array().map_or(0, Vec::len),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::ids::BlockId;
    use orch8_types::sequence::{BlockDefinition, StepDef};

    #[test]
    fn block_def_id_extracts_step_id() {
        let def = BlockDefinition::Step(Box::new(StepDef {
            id: BlockId("step_1".into()),
            handler: "noop".into(),
            params: serde_json::Value::Null,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
        }));
        assert_eq!(block_def_id(&def), "step_1");
    }

    #[test]
    fn validate_injected_blocks_rejects_non_array() {
        let val = serde_json::json!({"not": "array"});
        let err = validate_injected_blocks(&val).unwrap_err();
        assert!(err.to_string().contains("blocks must be a JSON array"));
    }

    #[test]
    fn validate_injected_blocks_rejects_empty_array() {
        let val = serde_json::json!([]);
        let err = validate_injected_blocks(&val).unwrap_err();
        assert!(err.to_string().contains("blocks array must not be empty"));
    }

    #[test]
    fn validate_injected_blocks_accepts_valid_step() {
        let val = serde_json::json!([{
            "type": "step",
            "id": "injected_1",
            "handler": "noop",
            "params": {},
            "delay": null,
            "retry": null,
            "timeout": null
        }]);
        let ids = validate_injected_blocks(&val).unwrap();
        assert_eq!(ids, vec!["injected_1"]);
    }

    #[test]
    fn validate_injected_blocks_rejects_invalid_block() {
        let val = serde_json::json!([{"UnknownVariant": {}}]);
        let err = validate_injected_blocks(&val).unwrap_err();
        assert!(err
            .to_string()
            .contains("blocks[0] is not a valid BlockDefinition"));
    }
}
