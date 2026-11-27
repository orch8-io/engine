use std::collections::BTreeSet;

use axum::Json;
use axum::extract::{Path, Query, State};
use chrono::Utc;
use orch8_types::continuity::{ContinuityId, DataClassification, ExecutionEpoch, RuntimeId};
use orch8_types::continuity_advanced::{
    RecordedResidencyEvaluation, ResidencyEvaluationDetails, ResidencyEvaluationSummary,
};
use orch8_types::ids::TenantId;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use super::{RESIDENCY_OPERATIONS, TenantQuery, hex_sha256, query_tenant, residency_report};
use crate::{AppState, auth::OptionalTenant, error::ApiError};

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(crate) struct RecordResidencyRequest {
    tenant_id: TenantId,
    expected_owner_runtime_id: RuntimeId,
    expected_epoch: ExecutionEpoch,
    destination_runtime_id: RuntimeId,
    classification: DataClassification,
    operation: String,
    #[serde(default)]
    allowed_regions: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResidencyListQuery {
    tenant_id: String,
    before: Option<Uuid>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ResidencyEvaluationPage {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    evaluations: Vec<ResidencyEvaluationSummary>,
    next_before: Option<Uuid>,
}

/// Browse saved residency evaluations for a continuity execution.
#[utoipa::path(
    get,
    operation_id = "list_residency_evaluations",
    path = "/continuity/executions/{id}/residency-evaluations",
    tag = "continuity",
    params(
        ("id" = ContinuityId, Path, description = "Continuity execution ID"),
        ("tenant_id" = String, Query, description = "Tenant ID"),
        ("before" = Option<Uuid>, Query, description = "Cursor returned by the prior page"),
        ("limit" = Option<u32>, Query, description = "Evaluations per page (1–50, default 25)")
    ),
    responses(
        (status = 200, description = "Saved evaluation summaries", body = ResidencyEvaluationPage),
        (status = 404, description = "Continuity execution not found")
    )
)]
pub(crate) async fn list(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<ResidencyListQuery>,
) -> Result<Json<ResidencyEvaluationPage>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    state
        .storage
        .get_continuity_execution(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let limit = query.limit.unwrap_or(25).clamp(1, 50);
    let mut evaluations = state
        .storage
        .list_residency_evaluations(&tenant_id, id, query.before, limit + 1)
        .await
        .map_err(|error| ApiError::from_storage(error, "residency evaluation inventory"))?;
    let mut next_before = None;
    if evaluations.len() > limit as usize {
        evaluations.truncate(limit as usize);
        next_before = evaluations.last().map(|evaluation| evaluation.id);
    }
    Ok(Json(ResidencyEvaluationPage {
        tenant_id,
        continuity_id: id,
        evaluations,
        next_before,
    }))
}

/// Retrieve one saved evaluation and its provenance entry.
#[utoipa::path(
    get,
    operation_id = "read_residency_evaluation",
    path = "/continuity/executions/{id}/residency-evaluations/{evaluation_id}",
    tag = "continuity",
    params(
        ("id" = ContinuityId, Path, description = "Continuity execution ID"),
        ("evaluation_id" = Uuid, Path, description = "Evaluation ID"),
        ("tenant_id" = String, Query, description = "Tenant ID")
    ),
    responses(
        (status = 200, description = "Stored evaluation", body = RecordedResidencyEvaluation),
        (status = 404, description = "Evaluation not found")
    )
)]
pub(crate) async fn read(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((id, evaluation_id)): Path<(ContinuityId, Uuid)>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<RecordedResidencyEvaluation>, ApiError> {
    let tenant = query_tenant(&tenant_ctx, &query.tenant_id)?;
    stored(&state, &tenant, id, evaluation_id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::NotFound("residency evaluation".into()))
}

/// Record a residency evaluation once for an ID and retrieve the original result on replay.
#[utoipa::path(
    put,
    operation_id = "record_residency_evaluation",
    path = "/continuity/executions/{id}/residency-evaluations/{evaluation_id}",
    tag = "continuity",
    params(
        ("id" = ContinuityId, Path, description = "Continuity execution ID"),
        ("evaluation_id" = Uuid, Path, description = "Caller-generated evaluation ID")
    ),
    request_body = RecordResidencyRequest,
    responses(
        (status = 200, description = "Stored evaluation", body = RecordedResidencyEvaluation),
        (status = 404, description = "Continuity execution not found"),
        (status = 409, description = "The reviewed owner changed or the ID is unavailable for this request")
    )
)]
pub(crate) async fn record(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((id, evaluation_id)): Path<(ContinuityId, Uuid)>,
    Json(body): Json<RecordResidencyRequest>,
) -> Result<Json<RecordedResidencyEvaluation>, ApiError> {
    let tenant = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if !RESIDENCY_OPERATIONS.contains(&body.operation.as_str())
        || body.allowed_regions.len() > 256
        || evaluation_id.is_nil()
    {
        return Err(ApiError::InvalidArgument(
            "residency operation, evaluation ID or allowed-region policy is invalid".into(),
        ));
    }
    let request_sha256 = digest(&body)?;
    if let Some(record) = stored(&state, &tenant, id, evaluation_id).await? {
        return replay(record, &request_sha256).map(Json);
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    if execution.epoch != body.expected_epoch
        || execution.owner_runtime_id != body.expected_owner_runtime_id
    {
        if let Some(record) = stored(&state, &tenant, id, evaluation_id).await? {
            return replay(record, &request_sha256).map(Json);
        }
        return Err(ApiError::Conflict(
            "review the current continuity owner and epoch".into(),
        ));
    }
    let evidence = residency_report(
        &state,
        &execution,
        body.classification,
        body.operation,
        body.destination_runtime_id,
        &body.allowed_regions,
    )
    .await?;
    let details = ResidencyEvaluationDetails {
        request_sha256,
        source_runtime_id: execution.owner_runtime_id,
        destination_runtime_id: body.destination_runtime_id,
        allowed_regions: body.allowed_regions,
        evidence,
    };
    let payload_sha256 = digest(&(evaluation_id, &details))?;
    for _ in 0..8 {
        let previous = state
            .storage
            .get_provenance_head(&tenant, id)
            .await
            .map_err(|error| ApiError::from_storage(error, "provenance head"))?
            .map(|entry| entry.entry_sha256);
        let mut provenance = orch8_engine::continuity::build_provenance_entry_with_summary(
            &execution,
            "residency_evaluated",
            Some("runtime-derived residency evidence recorded".into()),
            &payload_sha256,
            previous,
            Utc::now(),
        );
        provenance.id = evaluation_id;
        if let Some(signer) = state.provenance_signer() {
            provenance = orch8_engine::continuity::sign_provenance_entry(
                provenance,
                signer.key_id.to_owned(),
                signer.signing_key,
            );
        }
        let record = RecordedResidencyEvaluation {
            provenance,
            residency_evaluation: details.clone(),
        };
        if state
            .storage
            .commit_residency_evaluation(&record)
            .await
            .map_err(|error| ApiError::from_storage(error, "residency evaluation"))?
        {
            return Ok(Json(record));
        }
        if let Some(record) = stored(&state, &tenant, id, evaluation_id).await? {
            return replay(record, &details.request_sha256).map(Json);
        }
        tokio::task::yield_now().await;
    }
    Err(ApiError::Conflict(
        "the evaluation could not be stored for the reviewed owner and ID".into(),
    ))
}

fn digest(value: &impl Serialize) -> Result<String, ApiError> {
    serde_json::to_vec(value)
        .map(|bytes| hex_sha256(&bytes))
        .map_err(|error| ApiError::Internal(format!("serialize residency evidence: {error}")))
}

async fn stored(
    state: &AppState,
    tenant: &TenantId,
    id: ContinuityId,
    evaluation_id: Uuid,
) -> Result<Option<RecordedResidencyEvaluation>, ApiError> {
    let record = state
        .storage
        .get_residency_evaluation(tenant, id, evaluation_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "residency evaluation"))?;
    if let Some(record) = &record
        && (record.provenance.id != evaluation_id
            || record.provenance.tenant_id != *tenant
            || record.provenance.continuity_id != id
            || record.provenance.kind != "residency_evaluated"
            || record.provenance.payload_sha256
                != digest(&(evaluation_id, &record.residency_evaluation))?)
    {
        return Err(ApiError::Internal(
            "stored residency evidence binding is invalid".into(),
        ));
    }
    Ok(record)
}

fn replay(
    record: RecordedResidencyEvaluation,
    expected: &str,
) -> Result<RecordedResidencyEvaluation, ApiError> {
    if record.residency_evaluation.request_sha256 != expected {
        return Err(ApiError::Conflict(
            "this evaluation ID belongs to another residency request".into(),
        ));
    }
    Ok(record)
}
