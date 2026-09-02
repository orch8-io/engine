//! Static typed-dataflow analysis and deterministic SDK binding generation.

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use orch8_engine::dataflow::{DataflowReport, GeneratedDataflowTypes};
use orch8_types::ids::SequenceId;
use orch8_types::sequence::SequenceDefinition;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;

#[derive(Debug, Serialize, ToSchema)]
pub struct DataflowResponse {
    report: DataflowReport,
    generated: GeneratedDataflowTypes,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sequences/dataflow", post(compile_draft))
        .route("/sequences/{id}/dataflow", get(compile_stored))
}

#[utoipa::path(post, path = "/sequences/dataflow", tag = "sequences",
    request_body = SequenceDefinition,
    responses(
        (status = 200, description = "Typed dataflow report and deterministic bindings", body = DataflowResponse),
        (status = 400, description = "Invalid sequence or generation bounds exceeded"),
    )
)]
pub(crate) async fn compile_draft(
    State(_state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(options): Query<crate::sequences::DraftDecodeOptions>,
    Json(value): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let (sequence, decode_warnings) =
        crate::sequences::decode_draft_sequence(&value, options.strict)?;
    crate::auth::enforce_tenant_create(&tenant_ctx, &sequence.tenant_id)?;
    let mut response = compile_response(&sequence)?;
    response
        .report
        .findings
        .extend(decode_warnings.into_iter().map(|summary| {
            orch8_engine::dataflow::DataflowFinding {
                code: "UNKNOWN_SEQUENCE_FIELD".into(),
                severity: orch8_engine::dataflow::DataflowSeverity::Warning,
                consumer: "sequence".into(),
                reference: String::new(),
                summary,
            }
        }));
    Ok(Json(response))
}

#[utoipa::path(get, path = "/sequences/{id}/dataflow", tag = "sequences",
    params(("id" = Uuid, Path, description = "Sequence id")),
    responses(
        (status = 200, description = "Typed dataflow report and deterministic bindings", body = DataflowResponse),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn compile_stored(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let sequence = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &sequence.tenant_id,
        &format!("sequence {id}"),
    )?;
    compile_response(&sequence).map(Json)
}

fn compile_response(sequence: &SequenceDefinition) -> Result<DataflowResponse, ApiError> {
    let report = orch8_engine::dataflow::compile(sequence);
    let generated = orch8_engine::dataflow::generate_types(sequence)
        .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
    Ok(DataflowResponse { report, generated })
}
