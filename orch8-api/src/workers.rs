use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::filter::Pagination;
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use orch8_types::worker::WorkerTaskState;
use orch8_types::worker_filter::WorkerTaskFilter;

use orch8_types::execution::NodeState;

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/workers/tasks", get(list_tasks))
        .route("/workers/tasks/stats", get(task_stats))
        .route("/workers/tasks/poll", post(poll_tasks))
        .route("/workers/tasks/poll/queue", post(poll_tasks_from_queue))
        .route("/workers/tasks/{id}/complete", post(complete_task))
        .route("/workers/tasks/{id}/fail", post(fail_task))
        .route("/workers/tasks/{id}/heartbeat", post(heartbeat_task))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct PollRequest {
    handler_name: String,
    worker_id: String,
    #[serde(default = "default_poll_limit")]
    limit: u32,
}

fn default_poll_limit() -> u32 {
    1
}

#[utoipa::path(post, path = "/workers/tasks/poll", tag = "workers",
    request_body = PollRequest,
    responses((status = 200, description = "Claimed worker tasks", body = Vec<orch8_types::worker::WorkerTask>))
)]
pub(crate) async fn poll_tasks(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<PollRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = req.limit.min(1000);
    // When a tenant is scoped, route through the tenant-aware claim so
    // the predicate is enforced INSIDE the lock window. The previous
    // "claim then filter" path would mark a foreign tenant's row claimed
    // with this worker_id, drop it from the response, and leave a ghost
    // `claimed` row invisible to its owning tenant until the stale-task
    // reaper reset it.
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let tasks = if let Some(ref tid) = scoped {
        state
            .storage
            .claim_worker_tasks_for_tenant(&req.handler_name, &req.worker_id, tid, limit)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    } else {
        state
            .storage
            .claim_worker_tasks(&req.handler_name, &req.worker_id, limit)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    };

    Ok(Json(tasks))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct QueuePollRequest {
    queue_name: String,
    handler_name: String,
    worker_id: String,
    #[serde(default = "default_poll_limit")]
    limit: u32,
}

#[utoipa::path(post, path = "/workers/tasks/poll/queue", tag = "workers",
    request_body = QueuePollRequest,
    responses((status = 200, description = "Claimed worker tasks from queue", body = Vec<orch8_types::worker::WorkerTask>))
)]
pub(crate) async fn poll_tasks_from_queue(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<QueuePollRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = req.limit.min(1000);
    // Tenant-scoped claim path — see `poll_tasks` comment for the rationale.
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let tasks = if let Some(ref tid) = scoped {
        state
            .storage
            .claim_worker_tasks_from_queue_for_tenant(
                &req.queue_name,
                &req.handler_name,
                &req.worker_id,
                tid,
                limit,
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    } else {
        state
            .storage
            .claim_worker_tasks_from_queue(
                &req.queue_name,
                &req.handler_name,
                &req.worker_id,
                limit,
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    };

    Ok(Json(tasks))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CompleteRequest {
    worker_id: String,
    output: serde_json::Value,
}

#[utoipa::path(post, path = "/workers/tasks/{id}/complete", tag = "workers",
    params(("id" = Uuid, Path, description = "Worker task ID")),
    request_body = CompleteRequest,
    responses(
        (status = 200, description = "Task completed"),
        (status = 404, description = "Worker task not found"),
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn complete_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(task_id): Path<Uuid>,
    Json(req): Json<CompleteRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Fetch task first to verify tenant access via its instance.
    let pre_task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    // Capture the tenant for later breaker bookkeeping. We only enforce access
    // when the instance still exists; a missing instance makes the task
    // meaningless but we let the transaction below 404/no-op as it always has.
    let tenant_for_cb: Option<orch8_types::TenantId> =
        match state.storage.get_instance(pre_task.instance_id).await {
            Ok(Some(inst)) => {
                crate::auth::enforce_tenant_access(
                    &tenant_ctx,
                    &inst.tenant_id,
                    &format!("worker_task {task_id}"),
                )?;
                Some(inst.tenant_id.clone())
            }
            _ => None,
        };

    let updated = state
        .storage
        .complete_worker_task(task_id, &req.worker_id, &req.output)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    if !updated {
        return Err(ApiError::NotFound(format!("worker_task {task_id}")));
    }

    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;

    let output_json = serde_json::to_string(&req.output).map_err(|e| {
        ApiError::InvalidArgument(format!("failed to serialize worker output: {e}"))
    })?;
    let task_block_id = task.block_id.clone();
    let block_output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: task.instance_id,
        block_id: task.block_id,
        output: req.output,
        output_ref: None,
        output_size: i32::try_from(output_json.len()).unwrap_or(i32::MAX),
        attempt: task.attempt,
        created_at: chrono::Utc::now(),
    };

    // Re-read instance state before propagating. Between task-claim and
    // completion a cancel/admin-fail could have terminated the instance;
    // `save_output_and_transition` updates by id only, so without this
    // guard a late completion resurrects a terminal instance back to
    // Scheduled. See `save_output_and_transition` in
    // orch8-storage/src/{postgres,sqlite}/outputs.rs — there is no
    // conditional predicate on from-state.
    let Some(mut instance) = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
    else {
        return Ok(StatusCode::OK);
    };
    if instance.state.is_terminal() {
        tracing::info!(
            instance_id = %task.instance_id,
            state = %instance.state,
            block_id = %task_block_id,
            "external worker completion arrived for terminal instance — task accepted, transition skipped"
        );
        // Still roll the external work into the breaker — the handler's
        // dependency did succeed, and withholding the signal would leave
        // a spurious failure in the breaker window.
        if let (Some(cb), Some(tenant)) = (state.circuit_breakers.as_ref(), tenant_for_cb.as_ref())
        {
            if orch8_engine::circuit_breaker::is_breaker_tracked(&task.handler_name) {
                cb.record_success(tenant, &task.handler_name);
            }
        }
        return Ok(StatusCode::OK);
    }

    // Merge step output into context.data BEFORE transitioning to Scheduled.
    // This prevents a race where the scheduler claims the instance before
    // the context is updated, causing downstream blocks (routers, loops)
    // to evaluate conditions against stale context.
    //
    // Crash-safety: when there is a context merge we go through the atomic
    // `save_output_merge_context_and_transition` so output INSERT + context
    // UPDATE + state transition commit together. A process crash between
    // the two legacy calls could leave merged context but the old state
    // (`Running`), stranding the instance until manual intervention.
    let merged_context = if let Some(obj) = block_output.output.as_object() {
        // Ensure context.data is an object (it may be null/Null for
        // instances created without initial context data).
        if instance.context.data.is_null() || !instance.context.data.is_object() {
            instance.context.data = serde_json::Value::Object(serde_json::Map::new());
        }
        if let Some(data_obj) = instance.context.data.as_object_mut() {
            for (k, v) in obj {
                data_obj.insert(k.clone(), v.clone());
            }
            true
        } else {
            false
        }
    } else {
        false
    };

    if merged_context {
        state
            .storage
            .save_output_merge_context_and_transition(
                &block_output,
                task.instance_id,
                &instance.context,
                InstanceState::Scheduled,
                Some(chrono::Utc::now()),
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
    } else {
        state
            .storage
            .save_output_and_transition(
                &block_output,
                task.instance_id,
                InstanceState::Scheduled,
                Some(chrono::Utc::now()),
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
    }

    let tree = state
        .storage
        .get_execution_tree(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    if let Some(node) = tree.iter().find(|n| {
        n.block_id == task_block_id && matches!(n.state, NodeState::Running | NodeState::Waiting)
    }) {
        state
            .storage
            .update_node_state(node.id, NodeState::Completed)
            .await
            .map_err(|e| ApiError::from_storage(e, "execution_node"))?;
    }

    // Roll external-worker success into the same breaker registry the
    // in-process step-exec path uses. Skip-listed handlers (pure control-flow
    // built-ins) bypass bookkeeping — they have no external dep to break.
    if let (Some(cb), Some(tenant)) = (state.circuit_breakers.as_ref(), tenant_for_cb.as_ref()) {
        if orch8_engine::circuit_breaker::is_breaker_tracked(&task.handler_name) {
            cb.record_success(tenant, &task.handler_name);
        }
    }

    Ok(StatusCode::OK)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct FailRequest {
    worker_id: String,
    message: String,
    #[serde(default)]
    retryable: bool,
}

#[utoipa::path(post, path = "/workers/tasks/{id}/fail", tag = "workers",
    params(("id" = Uuid, Path, description = "Worker task ID")),
    request_body = FailRequest,
    responses(
        (status = 200, description = "Task failed"),
        (status = 404, description = "Worker task not found"),
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn fail_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(task_id): Path<Uuid>,
    Json(req): Json<FailRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Fetch task first to verify tenant access via its instance.
    let pre_task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    // Capture the tenant for later breaker bookkeeping. Both retryable and
    // non-retryable external-worker failures count toward trip — a handler
    // that keeps retry-failing N times is still a handler whose dependency
    // is in distress, and withholding the signal would defeat the breaker.
    let tenant_for_cb: Option<orch8_types::TenantId> =
        match state.storage.get_instance(pre_task.instance_id).await {
            Ok(Some(inst)) => {
                crate::auth::enforce_tenant_access(
                    &tenant_ctx,
                    &inst.tenant_id,
                    &format!("worker_task {task_id}"),
                )?;
                Some(inst.tenant_id.clone())
            }
            _ => None,
        };

    let updated = state
        .storage
        .fail_worker_task(task_id, &req.worker_id, &req.message, req.retryable)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    if !updated {
        return Err(ApiError::NotFound(format!("worker_task {task_id}")));
    }

    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;

    // Guard: if the instance has already reached a terminal state (completed,
    // failed, cancelled), accept the failure report but skip state mutation.
    // Without this, a late worker failure can resurrect or overwrite a terminal
    // instance — the same race that `complete_task` guards against.
    if let Ok(Some(inst)) = state.storage.get_instance(task.instance_id).await {
        if inst.state.is_terminal() {
            tracing::info!(
                instance_id = %task.instance_id,
                state = %inst.state,
                block_id = %task.block_id,
                "external worker failure arrived for terminal instance — task accepted, transition skipped"
            );
            if let (Some(cb), Some(tenant)) =
                (state.circuit_breakers.as_ref(), tenant_for_cb.as_ref())
            {
                if orch8_engine::circuit_breaker::is_breaker_tracked(&task.handler_name) {
                    cb.record_failure(tenant, &task.handler_name);
                }
            }
            return Ok(StatusCode::OK);
        }
    }

    let tree = state
        .storage
        .get_execution_tree(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    let has_tree = !tree.is_empty();

    if req.retryable && has_tree {
        // Tree-based execution: look up the step's retry policy to decide
        // whether to retry (reset node to Pending) or exhaust (fail node).
        let can_retry = 'retry_check: {
            let instance = match state.storage.get_instance(task.instance_id).await {
                Ok(Some(v)) => v,
                Ok(None) => break 'retry_check false,
                Err(e) => {
                    return Err(ApiError::from_storage(e, "worker_task_retry_lookup"));
                }
            };
            let seq = match state.storage.get_sequence(instance.sequence_id).await {
                Ok(Some(v)) => v,
                Ok(None) => break 'retry_check false,
                Err(e) => {
                    return Err(ApiError::from_storage(e, "worker_task_retry_lookup"));
                }
            };
            let block = orch8_engine::evaluator::find_block(&seq.blocks, &task.block_id);
            match block {
                Some(orch8_types::sequence::BlockDefinition::Step(step_def)) => {
                    if let Some(retry) = &step_def.retry {
                        #[allow(clippy::cast_sign_loss)]
                        {
                            ((task.attempt + 1) as u32) < retry.max_attempts
                        }
                    } else {
                        false // no retry policy → fail immediately
                    }
                }
                _ => false,
            }
        };

        if can_retry {
            // Reset for retry: delete old task, create a new pending task
            // with incremented attempt, and reset the node to Pending so
            // the evaluator re-dispatches on the next tick.
            let retry_task = orch8_types::worker::WorkerTask {
                id: Uuid::now_v7(),
                instance_id: task.instance_id,
                block_id: task.block_id.clone(),
                handler_name: task.handler_name.clone(),
                queue_name: task.queue_name.clone(),
                params: task.params.clone(),
                context: task.context.clone(),
                attempt: task.attempt + 1,
                timeout_ms: task.timeout_ms,
                state: WorkerTaskState::Pending,
                worker_id: None,
                claimed_at: None,
                heartbeat_at: None,
                completed_at: None,
                output: None,
                error_message: None,
                error_retryable: None,
                created_at: chrono::Utc::now(),
            };
            state
                .storage
                .delete_worker_task(task_id)
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
            state
                .storage
                .create_worker_task(&retry_task)
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
            if let Some(node) = tree.iter().find(|n| {
                n.block_id == task.block_id
                    && matches!(n.state, NodeState::Running | NodeState::Waiting)
            }) {
                state
                    .storage
                    .update_node_state(node.id, NodeState::Pending)
                    .await
                    .map_err(|e| ApiError::from_storage(e, "execution_node"))?;
            }
            state
                .storage
                .update_instance_state(
                    task.instance_id,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
        } else {
            // Retries exhausted or no retry policy: fail the node.
            if let Some(node) = tree.iter().find(|n| {
                n.block_id == task.block_id
                    && matches!(n.state, NodeState::Running | NodeState::Waiting)
            }) {
                state
                    .storage
                    .update_node_state(node.id, NodeState::Failed)
                    .await
                    .map_err(|e| ApiError::from_storage(e, "execution_node"))?;
            }
            state
                .storage
                .update_instance_state(
                    task.instance_id,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
        }
    } else if req.retryable {
        // No tree (fast path): look up the step's retry policy and either
        // create a new pending worker task with incremented attempt or fail
        // the instance directly when retries are exhausted / no policy.
        let can_retry = 'fp_retry: {
            let Ok(Some(instance)) = state.storage.get_instance(task.instance_id).await else {
                break 'fp_retry false;
            };
            let Ok(Some(seq)) = state.storage.get_sequence(instance.sequence_id).await else {
                break 'fp_retry false;
            };
            let block = orch8_engine::evaluator::find_block(&seq.blocks, &task.block_id);
            match block {
                Some(orch8_types::sequence::BlockDefinition::Step(step_def)) => {
                    if let Some(retry) = &step_def.retry {
                        #[allow(clippy::cast_sign_loss)]
                        {
                            ((task.attempt + 1) as u32) < retry.max_attempts
                        }
                    } else {
                        false
                    }
                }
                _ => false,
            }
        };

        if can_retry {
            let retry_task = orch8_types::worker::WorkerTask {
                id: Uuid::now_v7(),
                instance_id: task.instance_id,
                block_id: task.block_id.clone(),
                handler_name: task.handler_name.clone(),
                queue_name: task.queue_name.clone(),
                params: task.params.clone(),
                context: task.context.clone(),
                attempt: task.attempt + 1,
                timeout_ms: task.timeout_ms,
                state: WorkerTaskState::Pending,
                worker_id: None,
                claimed_at: None,
                heartbeat_at: None,
                completed_at: None,
                output: None,
                error_message: None,
                error_retryable: None,
                created_at: chrono::Utc::now(),
            };
            state
                .storage
                .delete_worker_task(task_id)
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
            state
                .storage
                .create_worker_task(&retry_task)
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
            state
                .storage
                .update_instance_state(
                    task.instance_id,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
        } else {
            // Retries exhausted or no retry policy: fail immediately.
            state
                .storage
                .delete_worker_task(task_id)
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
            state
                .storage
                .update_instance_state(task.instance_id, InstanceState::Failed, None)
                .await
                .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
        }
    } else if has_tree {
        if let Some(node) = tree.iter().find(|n| {
            n.block_id == task.block_id
                && matches!(n.state, NodeState::Running | NodeState::Waiting)
        }) {
            state
                .storage
                .update_node_state(node.id, NodeState::Failed)
                .await
                .map_err(|e| ApiError::from_storage(e, "execution_node"))?;
        }
        state
            .storage
            .update_instance_state(
                task.instance_id,
                InstanceState::Scheduled,
                Some(chrono::Utc::now()),
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
    } else {
        state
            .storage
            .update_instance_state(task.instance_id, InstanceState::Failed, None)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
    }

    // Roll external-worker failure into the breaker. Done unconditionally (for
    // both retryable and non-retryable) to mirror in-process step-exec, which
    // records a failure on every Err arm. Control-flow built-ins stay
    // skip-listed.
    if let (Some(cb), Some(tenant)) = (state.circuit_breakers.as_ref(), tenant_for_cb.as_ref()) {
        if orch8_engine::circuit_breaker::is_breaker_tracked(&task.handler_name) {
            cb.record_failure(tenant, &task.handler_name);
        }
    }

    Ok(StatusCode::OK)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct HeartbeatRequest {
    worker_id: String,
}

#[utoipa::path(post, path = "/workers/tasks/{id}/heartbeat", tag = "workers",
    params(("id" = Uuid, Path, description = "Worker task ID")),
    request_body = HeartbeatRequest,
    responses(
        (status = 200, description = "Heartbeat updated"),
        (status = 404, description = "Worker task not found"),
    )
)]
pub(crate) async fn heartbeat_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(task_id): Path<Uuid>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Fetch task first to verify tenant access via its instance.
    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    if let Ok(Some(inst)) = state.storage.get_instance(task.instance_id).await {
        crate::auth::enforce_tenant_access(
            &tenant_ctx,
            &inst.tenant_id,
            &format!("worker_task {task_id}"),
        )?;
    }

    let updated = state
        .storage
        .heartbeat_worker_task(task_id, &req.worker_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    if !updated {
        return Err(ApiError::NotFound(format!("worker_task {task_id}")));
    }

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub(crate) struct ListTasksQuery {
    tenant_id: Option<String>,
    state: Option<String>,
    handler_name: Option<String>,
    worker_id: Option<String>,
    queue_name: Option<String>,
    #[serde(default = "default_list_limit")]
    limit: u32,
    #[serde(default)]
    offset: u64,
}

fn default_list_limit() -> u32 {
    50
}

fn parse_states(raw: &str) -> Result<Vec<WorkerTaskState>, ApiError> {
    raw.split(',')
        .map(|s| match s.trim() {
            "pending" => Ok(WorkerTaskState::Pending),
            "claimed" => Ok(WorkerTaskState::Claimed),
            "completed" => Ok(WorkerTaskState::Completed),
            "failed" => Ok(WorkerTaskState::Failed),
            other => Err(ApiError::InvalidArgument(format!(
                "unknown worker task state: {other}"
            ))),
        })
        .collect()
}

#[utoipa::path(get, path = "/workers/tasks", tag = "workers",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("state" = Option<String>, Query, description = "Comma-separated worker task states"),
        ("handler_name" = Option<String>, Query, description = "Filter by handler name"),
        ("worker_id" = Option<String>, Query, description = "Filter by claiming worker id"),
        ("queue_name" = Option<String>, Query, description = "Filter by queue name"),
        ("limit" = u32, Query, description = "Max rows to return (≤ 1000)"),
        ("offset" = u32, Query, description = "Skip N rows"),
    ),
    responses((status = 200, description = "Worker tasks", body = Vec<orch8_types::worker::WorkerTask>))
)]
pub(crate) async fn list_tasks(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<ListTasksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let states = query.state.as_deref().map(parse_states).transpose()?;
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref());

    let filter = WorkerTaskFilter {
        tenant_id: scoped_tenant,
        states,
        handler_name: query.handler_name,
        worker_id: query.worker_id,
        queue_name: query.queue_name,
    };

    let pagination = Pagination {
        limit: query.limit.min(1000),
        offset: query.offset,
    };

    let tasks = state
        .storage
        .list_worker_tasks(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    Ok(Json(tasks))
}

#[utoipa::path(get, path = "/workers/tasks/stats", tag = "workers",
    responses((status = 200, description = "Aggregate worker task stats", body = orch8_types::worker_filter::WorkerTaskStats))
)]
pub(crate) async fn task_stats(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let result = state
        .storage
        .worker_task_stats(scoped_tenant.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    Ok(Json(result))
}
