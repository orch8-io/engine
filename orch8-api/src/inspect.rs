//! Template Resolution Inspector endpoints.
//!
//! Shows the exact value and type every parameter of a block resolves
//! to, with provenance (source path, fallback used) and a status that
//! distinguishes missing, explicit null, redacted, and evaluation
//! errors. Strictly read-only: the block's handler is never invoked and
//! no side effect is performed.

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_engine::template_trace::{debug_template, trace_params};
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, SequenceId};
use orch8_types::redaction::RedactionPolicy;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::template_trace::{DebugTemplateRequest, DebugTemplateResponse, ResolutionTrace};

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sequences/inspect-template", post(inspect_draft))
        .route("/debug/template", post(debug_template_endpoint))
        .route(
            "/instances/{id}/blocks/{block_id}/resolved-input",
            get(inspect_instance_block),
        )
}

/// Request body for draft inspection: a definition (or stored sequence
/// id), the block to inspect, and the context/outputs fixture to resolve
/// against.
#[derive(Deserialize, ToSchema)]
pub(crate) struct InspectTemplateRequest {
    /// Draft sequence definition. Exactly one of `sequence` /
    /// `sequence_id` must be set.
    #[serde(default)]
    pub sequence: Option<SequenceDefinition>,
    /// Stored sequence id.
    #[serde(default)]
    pub sequence_id: Option<Uuid>,
    /// The step block whose params to inspect.
    pub block_id: String,
    /// `context.data` fixture.
    #[serde(default)]
    pub context_data: serde_json::Value,
    /// `context.config` fixture.
    #[serde(default)]
    pub context_config: serde_json::Value,
    /// Prior block outputs fixture: `{block_id: output}`.
    #[serde(default)]
    pub outputs: serde_json::Value,
}

#[utoipa::path(post, path = "/sequences/inspect-template", tag = "sequences",
    request_body = InspectTemplateRequest,
    responses(
        (status = 200, description = "Resolution trace for the block's params", body = ResolutionTrace),
        (status = 400, description = "Invalid request"),
        (status = 404, description = "Sequence or block not found"),
    )
)]
pub(crate) async fn inspect_draft(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<InspectTemplateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let seq = match (&req.sequence, req.sequence_id) {
        (Some(seq), None) => {
            crate::auth::enforce_tenant_create(&tenant_ctx, &seq.tenant_id)?;
            seq.clone()
        }
        (None, Some(id)) => {
            let seq = state
                .storage
                .get_sequence(SequenceId::from_uuid(id))
                .await
                .map_err(|e| ApiError::from_storage(e, "sequence"))?
                .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;
            crate::auth::enforce_tenant_access(
                &tenant_ctx,
                &seq.tenant_id,
                &format!("sequence {id}"),
            )?;
            seq
        }
        _ => {
            return Err(ApiError::InvalidArgument(
                "pass exactly one of 'sequence' or 'sequence_id'".into(),
            ));
        }
    };

    let step = crate::approvals::find_step_by_id(&seq, &BlockId::new(&req.block_id))
        .ok_or_else(|| ApiError::NotFound(format!("step block '{}'", req.block_id)))?;

    let context = ExecutionContext {
        data: req.context_data,
        config: req.context_config,
        ..Default::default()
    };
    let trace = trace_params(
        &req.block_id,
        &step.params,
        &context,
        &req.outputs,
        None,
        &RedactionPolicy::default(),
    );
    Ok(Json(trace))
}

#[utoipa::path(post, path = "/debug/template", tag = "debug",
    request_body = DebugTemplateRequest,
    responses(
        (status = 200, description = "Resolved template value with provenance trace", body = DebugTemplateResponse),
    )
)]
pub(crate) async fn debug_template_endpoint(
    Json(req): Json<DebugTemplateRequest>,
) -> impl IntoResponse {
    let context = ExecutionContext {
        data: req.context_data,
        config: req.context_config,
        ..Default::default()
    };
    let resp = debug_template(
        &req.template,
        &context,
        &req.outputs,
        &RedactionPolicy::default(),
    );
    Json(resp)
}

#[derive(Deserialize)]
pub(crate) struct ResolvedInputQuery {
    /// Optional historical boundary: only outputs recorded strictly
    /// before this block's first output are visible to resolution.
    #[serde(default)]
    at_block: Option<String>,
}

#[utoipa::path(get, path = "/instances/{id}/blocks/{block_id}/resolved-input", tag = "instances",
    params(
        ("id" = Uuid, Path, description = "Instance id"),
        ("block_id" = String, Path, description = "Step block id"),
        ("at_block" = Option<String>, Query, description = "Historical boundary block"),
    ),
    responses(
        (status = 200, description = "Resolution trace for the block against the instance snapshot", body = ResolutionTrace),
        (status = 404, description = "Instance, sequence, or block not found"),
    )
)]
pub(crate) async fn inspect_instance_block(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((id, block_id)): Path<(Uuid, String)>,
    Query(q): Query<ResolvedInputQuery>,
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

    let seq = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound("sequence for instance".into()))?;

    let step = crate::approvals::find_step_by_id(&seq, &BlockId::new(&block_id))
        .ok_or_else(|| ApiError::NotFound(format!("step block '{block_id}'")))?;

    // Outputs snapshot in storage order; last attempt per block wins,
    // truncated at the requested boundary (default: the inspected block —
    // a block never sees its own output).
    let boundary = q.at_block.as_deref().unwrap_or(&block_id);
    let raw = state
        .storage
        .get_all_outputs(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "outputs"))?;
    let mut outputs = serde_json::Map::new();
    for o in &raw {
        let bid = o.block_id.as_str();
        if bid.starts_with('_') {
            continue;
        }
        if bid == boundary {
            break;
        }
        outputs.insert(bid.to_string(), o.output.clone());
    }

    let trace = trace_params(
        &block_id,
        &step.params,
        &instance.context,
        &serde_json::Value::Object(outputs),
        None,
        &RedactionPolicy::default(),
    );
    Ok(Json(trace))
}
