//! `GET /instances/{id}/diagnosis` — the Stuck Instance Doctor.
//!
//! Collects evidence (instance, sequence, signals, worker tasks, live
//! registrations, version pins, open breakers, children, pending
//! approvals) and runs the pure diagnostic rules from
//! `orch8_engine::doctor`. Strictly read-only: recovery actions are only
//! *described* in the response.

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use uuid::Uuid;

use orch8_engine::doctor::{InstanceDiagnosticContext, diagnose};
use orch8_types::diagnosis::InstanceDiagnosisReport;
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
    Router::new().route("/instances/{id}/diagnosis", get(get_diagnosis))
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
    let instance_id = InstanceId::from_uuid(id);
    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &instance.tenant_id, &format!("instance {id}"))?;

    let ctx = collect_context(&state, instance).await;
    Ok(Json(diagnose(&ctx, Utc::now())))
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
