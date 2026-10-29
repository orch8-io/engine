//! Explicit operator acknowledgement for reports without an authenticated issuer.
use crate::{AppState, error::ApiError};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use orch8_storage::{
    worker_outcome::WorkerCompletion, worker_reconciliation::WorkerReconciliationApproval,
};
use orch8_types::worker::WorkerClaim;
use serde::Deserialize;
use serde_json::Value;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReconcileCompletionRequest {
    worker_id: String,
    claim_epoch: u64,
    approval_id: Uuid,
    /// Opaque app/backend acknowledgement reference; never credentials or raw evidence.
    evidence_reference: String,
    output: Value,
}
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ReconciliationReceipt {
    task_id: Uuid,
    accepted_at: chrono::DateTime<chrono::Utc>,
    /// Retained approver, exact scope, output digest and original history classification.
    decision: Value,
}
fn receipt(
    outcome: &orch8_storage::worker_outcome::WorkerAcceptedOutcome,
) -> Result<Json<ReconciliationReceipt>, ApiError> {
    let decision = outcome.reconciliation.as_ref().ok_or_else(|| {
        ApiError::Conflict("completion was not accepted under this operator decision".into())
    })?;
    Ok(Json(ReconciliationReceipt {
        task_id: outcome.task_id,
        accepted_at: outcome.accepted_at,
        decision: serde_json::to_value(decision).map_err(|e| ApiError::Internal(e.to_string()))?,
    }))
}

async fn target_tenant(
    state: &AppState,
    tenant: &crate::auth::OptionalTenant,
    task_id: Uuid,
    accepted: Option<&orch8_storage::worker_outcome::WorkerAcceptedOutcome>,
) -> Result<orch8_types::ids::TenantId, ApiError> {
    let tenant_id = if let Some(outcome) = accepted {
        outcome.tenant_id.clone()
    } else {
        let task = state
            .storage
            .get_worker_task(task_id)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker task"))?
            .ok_or_else(|| ApiError::NotFound(format!("worker task {task_id}")))?;
        let native = state
            .storage
            .get_instance(task.instance_id)
            .await
            .map_err(|e| ApiError::from_storage(e, "instance"))?
            .ok_or_else(|| ApiError::NotFound(format!("instance {}", task.instance_id)))?;
        crate::auth::enforce_tenant_access(tenant, &native.tenant_id, "worker task")?;
        native.tenant_id
    };
    Ok(tenant_id)
}

#[utoipa::path(post,path="/workers/tasks/{id}/reconcile-completion",tag="workers",
    params(("id"=Uuid,Path,description="Legacy worker task ID")),
    request_body=ReconcileCompletionRequest,
    responses((status=202,description="Operator decision and original result retained; native application recovers separately",body=ReconciliationReceipt),
              (status=403,description="Authenticated Operator or root required"),
              (status=409,description="Claim changed, authenticated issuer exists, or decision conflicts")))]
pub(crate) async fn reconcile_completion(
    State(state): State<AppState>,
    caller: crate::auth::WorkerCaller,
    admin: crate::auth::OptionalAdmin,
    principal: Option<axum::Extension<crate::auth::PrincipalContext>>,
    tenant: crate::auth::OptionalTenant,
    Path(task_id): Path<Uuid>,
    Json(body): Json<ReconcileCompletionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if admin.is_none()
        && !principal
            .as_ref()
            .is_some_and(|p| crate::auth::principal_is_operator(Some(&p.0)))
    {
        return Err(ApiError::Forbidden(
            "worker reconciliation requires Operator or root".into(),
        ));
    }
    if !caller.0.has_authenticated_owner() {
        return Err(ApiError::Forbidden(
            "reconciliation requires an authenticated approver".into(),
        ));
    }
    let claim = WorkerClaim::new(body.worker_id, body.claim_epoch);
    let accepted = state
        .storage
        .get_worker_accepted_outcome(tenant.as_ref().map(|t| &t.tenant_id), task_id, &claim)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker outcome"))?;
    let tenant_id = target_tenant(&state, &tenant, task_id, accepted.as_ref()).await?;
    let approval = WorkerReconciliationApproval::new(
        caller.0,
        tenant_id.clone(),
        task_id,
        claim.clone(),
        body.approval_id,
        body.evidence_reference,
    )
    .map_err(|_| {
        ApiError::InvalidArgument("invalid reconciliation decision scope or reference".into())
    })?;
    let completion = WorkerCompletion::for_legacy_reconciliation(body.output, approval)
        .map_err(|e| ApiError::from_storage(e, "reconciliation"))?;
    if let Some(outcome) = accepted {
        if !completion.matches_reconciled_outcome(&outcome) {
            return Err(ApiError::Conflict(
                "a different completion or operator decision was already accepted".into(),
            ));
        }
        return Ok((StatusCode::ACCEPTED, receipt(&outcome)?));
    }
    let changed = state
        .storage
        .accept_worker_completion(task_id, &claim, &completion)
        .await
        .map_err(|e| ApiError::from_storage(e, "reconciliation"))?;
    if !changed {
        // Another identical request may have won while this request waited.
        if let Some(outcome) = state
            .storage
            .get_worker_accepted_outcome(Some(&tenant_id), task_id, &claim)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker outcome"))?
            && completion.matches_reconciled_outcome(&outcome)
        {
            return Ok((StatusCode::ACCEPTED, receipt(&outcome)?));
        }
        return Err(ApiError::Conflict(
            "worker claim changed; reload before reconciling".into(),
        ));
    }
    let outcome = state
        .storage
        .get_worker_accepted_outcome(tenant.as_ref().map(|t| &t.tenant_id), task_id, &claim)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker outcome"))?
        .ok_or_else(|| ApiError::Internal("accepted reconciliation receipt missing".into()))?;
    Ok((StatusCode::ACCEPTED, receipt(&outcome)?))
}
