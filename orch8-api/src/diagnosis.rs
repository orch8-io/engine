//! `GET /instances/{id}/diagnosis` — the Stuck Instance Doctor.
//!
//! Collects evidence (instance, sequence, signals, worker tasks, live
//! registrations, version pins, open breakers, children, pending
//! approvals) and runs the pure diagnostic rules from
//! `orch8_engine::doctor`. Strictly read-only: recovery actions are only
//! *described* in the response.

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_engine::doctor::{InstanceDiagnosticContext, diagnose, remediation_previews};
use orch8_types::diagnosis::{InstanceDiagnosisReport, RemediationAction, RemediationPreview};
use orch8_types::execution::NodeState;
use orch8_types::filter::Pagination;
use orch8_types::ids::InstanceId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::worker_filter::WorkerTaskFilter;

use crate::AppState;
use crate::error::ApiError;

/// Same liveness window the preflight uses for worker registrations.
const WORKER_LIVENESS_SECS: i64 = 120;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/instances/{id}/diagnosis", get(get_diagnosis))
        .route("/instances/{id}/remediations", get(preview_remediations))
        .route(
            "/instances/{id}/remediations/apply",
            post(apply_remediation),
        )
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ApplyRemediationRequest {
    pub preview_id: String,
    /// Required for recipes which may repeat an external side effect.
    #[serde(default)]
    pub acknowledge_side_effect_risk: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RemediationApplyEvidence {
    pub preview_id: String,
    pub action: RemediationAction,
    pub before_state: String,
    pub after_state: String,
    pub applied_at: chrono::DateTime<Utc>,
}

#[utoipa::path(get, path = "/instances/{id}/diagnosis", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance id")),
    responses(
        (status = 200, description = "Ranked diagnosis of why the instance is not progressing", body = InstanceDiagnosisReport),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn get_diagnosis(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(diagnose_instance(&state, &tenant_ctx, id).await?))
}

#[utoipa::path(get, path = "/instances/{id}/remediations", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance id")),
    responses(
        (status = 200, description = "State-bound remediation previews", body = Vec<RemediationPreview>),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn preview_remediations(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let report = diagnose_instance(&state, &tenant_ctx, id).await?;
    Ok(Json(remediation_previews(&report)))
}

#[utoipa::path(post, path = "/instances/{id}/remediations/apply", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance id")),
    request_body = ApplyRemediationRequest,
    responses(
        (status = 200, description = "Post-action remediation evidence", body = RemediationApplyEvidence),
        (status = 400, description = "Stale, manual, or unacknowledged remediation"),
        (status = 404, description = "Instance or preview not found"),
    )
)]
pub(crate) async fn apply_remediation(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(request): Json<ApplyRemediationRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let report = diagnose_instance(&state, &tenant_ctx, id).await?;
    let preview = remediation_previews(&report)
        .into_iter()
        .find(|preview| preview.preview_id == request.preview_id)
        .ok_or_else(|| {
            ApiError::InvalidArgument(
                "remediation preview is stale or does not belong to this instance".into(),
            )
        })?;
    if preview.side_effect_risk && !request.acknowledge_side_effect_risk {
        return Err(ApiError::InvalidArgument(
            "acknowledge_side_effect_risk=true is required for this remediation".into(),
        ));
    }

    let instance_id = InstanceId::from_uuid(id);
    match preview.action {
        RemediationAction::ResumeInstance => resume_paused(&state, instance_id).await?,
        RemediationAction::RetryInstance => {
            retry_failed(&state, instance_id).await?;
        }
        RemediationAction::Manual => {
            return Err(ApiError::InvalidArgument(
                "this remediation must be completed manually in its owning system".into(),
            ));
        }
    }
    let after = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    Ok(Json(RemediationApplyEvidence {
        preview_id: preview.preview_id,
        action: preview.action,
        before_state: report.state,
        after_state: after.state.to_string(),
        applied_at: Utc::now(),
    }))
}

async fn diagnose_instance(
    state: &AppState,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<InstanceDiagnosisReport, ApiError> {
    let instance = state
        .storage
        .get_instance(InstanceId::from_uuid(id))
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(tenant_ctx, &instance.tenant_id, &format!("instance {id}"))?;
    let ctx = collect_context(state, instance).await;
    Ok(diagnose(&ctx, Utc::now()))
}

async fn resume_paused(state: &AppState, instance_id: InstanceId) -> Result<(), ApiError> {
    let changed = state
        .storage
        .conditional_update_instance_state(
            instance_id,
            InstanceState::Paused,
            InstanceState::Scheduled,
            Some(Utc::now()),
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?;
    if !changed {
        return Err(ApiError::InvalidArgument(
            "instance state changed after remediation preview".into(),
        ));
    }
    Ok(())
}

async fn retry_failed(state: &AppState, instance_id: InstanceId) -> Result<(), ApiError> {
    let changed = state
        .storage
        .conditional_update_instance_state(
            instance_id,
            InstanceState::Failed,
            InstanceState::Paused,
            None,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?;
    if !changed {
        return Err(ApiError::InvalidArgument(
            "instance state changed after remediation preview".into(),
        ));
    }
    let reset = async {
        state
            .storage
            .delete_execution_tree(instance_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "execution_tree"))?;
        state
            .storage
            .delete_sentinel_block_outputs(instance_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "block_outputs"))?;
        state
            .storage
            .reset_instance_run(instance_id, &Uuid::now_v7().to_string())
            .await
            .map_err(|error| ApiError::from_storage(error, "instance"))?;
        state
            .storage
            .update_instance_state(instance_id, InstanceState::Scheduled, Some(Utc::now()))
            .await
            .map_err(|error| ApiError::from_storage(error, "instance"))
    }
    .await;
    if reset.is_err() {
        let _ = state
            .storage
            .conditional_update_instance_state(
                instance_id,
                InstanceState::Paused,
                InstanceState::Failed,
                None,
            )
            .await;
    }
    reset
}

/// Gather every evidence section, degrading to `None` (not failing) when
/// a source is unavailable — the doctor reports incomplete evidence
/// explicitly.
async fn collect_context(state: &AppState, instance: TaskInstance) -> InstanceDiagnosticContext {
    let storage = &state.storage;
    let instance_id = instance.id;
    let tenant = instance.tenant_id.clone();

    let mut ctx = InstanceDiagnosticContext::new(instance);

    // Sequence + approval detection share the fetched definition.
    let sequence = storage.get_sequence(ctx.instance.sequence_id).await.ok();
    ctx.sequence_exists = sequence.as_ref().map(Option::is_some);

    ctx.pending_signals = storage.get_pending_signals(instance_id).await.ok();

    ctx.worker_tasks = storage
        .list_worker_tasks(
            &WorkerTaskFilter {
                tenant_id: Some(tenant.clone()),
                states: None,
                handler_name: None,
                worker_id: None,
                queue_name: None,
            },
            &Pagination::default(),
        )
        .await
        .ok()
        .map(|tasks| {
            tasks
                .into_iter()
                .filter(|t| t.instance_id == instance_id)
                .collect()
        });

    ctx.worker_registrations = storage
        .list_worker_registrations(Some(WORKER_LIVENESS_SECS))
        .await
        .ok();

    ctx.version_pins = storage.list_worker_version_pins(None).await.ok();

    ctx.open_breakers = storage.list_open_circuit_breakers().await.ok();

    ctx.children = storage.get_child_instances(instance_id).await.ok();

    ctx.pending_approval_blocks =
        collect_pending_approvals(state, &ctx, sequence.flatten().as_ref()).await;

    // Event waits: for every block waiting on human input, check whether
    // it is actually a `wait_for_event` registration so the doctor can
    // report WAITING_EVENT with the missing event names.
    if let Some(blocks) = ctx.pending_approval_blocks.clone() {
        let mut waits = Vec::new();
        for block in &blocks {
            if let Ok(Some(wait)) = state.storage.get_event_wait(instance_id, block).await {
                waits.push(wait);
            }
        }
        ctx.event_waits = Some(waits);
    }

    ctx
}

/// Block ids currently waiting for human input, mirroring the approvals
/// endpoint's matching: tree nodes in `Waiting` whose step declares
/// `wait_for_input`, or — for flat-path instances with no tree — the
/// first uncompleted `wait_for_input` step.
async fn collect_pending_approvals(
    state: &AppState,
    ctx: &InstanceDiagnosticContext,
    sequence: Option<&orch8_types::sequence::SequenceDefinition>,
) -> Option<Vec<String>> {
    let seq = sequence?;
    if ctx.instance.state != InstanceState::Waiting {
        return Some(vec![]);
    }
    let tree = state
        .storage
        .get_execution_tree(ctx.instance.id)
        .await
        .ok()?;

    let mut blocks = Vec::new();
    if tree.is_empty() {
        let completed = state
            .storage
            .get_completed_block_ids_batch(&[ctx.instance.id])
            .await
            .ok()?
            .remove(&ctx.instance.id)
            .unwrap_or_default();
        for block in &seq.blocks {
            if let orch8_types::sequence::BlockDefinition::Step(step) = block {
                if completed.contains(&step.id) {
                    continue;
                }
                if step.wait_for_input.is_some() {
                    blocks.push(step.id.as_str().to_string());
                    break; // flat path runs sequentially
                }
            }
        }
    } else {
        for node in &tree {
            if node.state == NodeState::Waiting
                && crate::approvals::find_step_by_id(seq, &node.block_id)
                    .is_some_and(|s| s.wait_for_input.is_some())
            {
                blocks.push(node.block_id.as_str().to_string());
            }
        }
    }
    Some(blocks)
}
