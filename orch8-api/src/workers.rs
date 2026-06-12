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
        .route("/workers", get(list_workers))
        .route("/handlers", get(list_handlers))
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
    /// Optional worker build/deploy version, recorded on the worker registry.
    #[serde(default)]
    version: Option<String>,
}

const fn default_poll_limit() -> u32 {
    1
}

/// Record a worker registration from a poll. Best-effort: a registry write
/// failure must never fail the poll itself, so errors are logged and dropped.
async fn record_registration(
    state: &AppState,
    worker_id: &str,
    handler_name: &str,
    queue_name: Option<&str>,
    version: Option<&str>,
    tenant_id: Option<&orch8_types::ids::TenantId>,
) {
    let registration = orch8_types::worker::WorkerRegistration {
        worker_id: worker_id.to_string(),
        handler_name: handler_name.to_string(),
        queue_name: queue_name.map(ToString::to_string),
        version: version.map(ToString::to_string),
        tenant_id: tenant_id.map(|t| t.as_str().to_string()),
        last_seen_at: chrono::Utc::now(),
    };
    if let Err(e) = state.storage.upsert_worker_registration(&registration).await {
        tracing::warn!(
            error = %e,
            worker_id,
            handler_name,
            "failed to record worker registration"
        );
    }
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

    record_registration(
        &state,
        &req.worker_id,
        &req.handler_name,
        None,
        req.version.as_deref(),
        scoped.as_ref(),
    )
    .await;

    Ok(Json(tasks))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct QueuePollRequest {
    queue_name: String,
    handler_name: String,
    worker_id: String,
    #[serde(default = "default_poll_limit")]
    limit: u32,
    /// Optional worker build/deploy version, recorded on the worker registry.
    #[serde(default)]
    version: Option<String>,
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

    record_registration(
        &state,
        &req.worker_id,
        &req.handler_name,
        Some(&req.queue_name),
        req.version.as_deref(),
        scoped.as_ref(),
    )
    .await;

    Ok(Json(tasks))
}

/// Aggregated view of one worker on the fleet, grouped from its
/// per-handler registrations.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct WorkerInfo {
    pub worker_id: String,
    /// Handler names this worker has polled for.
    pub handlers: Vec<String>,
    /// Named queues this worker has polled (empty when default queue only).
    pub queues: Vec<String>,
    /// Most recently reported version string, if any.
    pub version: Option<String>,
    pub last_seen_at: chrono::DateTime<chrono::Utc>,
    /// True when the worker polled within the liveness window.
    pub alive: bool,
    /// Number of tasks currently claimed by this worker.
    pub in_flight: i64,
}

#[derive(Deserialize)]
pub(crate) struct ListWorkersQuery {
    /// Liveness window in seconds (default 60).
    #[serde(default = "default_alive_within_secs")]
    alive_within_secs: i64,
    /// Include workers whose last poll is older than the liveness window.
    #[serde(default)]
    include_stale: bool,
}

const fn default_alive_within_secs() -> i64 {
    60
}

#[utoipa::path(get, path = "/workers", tag = "workers",
    params(
        ("alive_within_secs" = Option<i64>, Query, description = "Liveness window in seconds (default 60)"),
        ("include_stale" = Option<bool>, Query, description = "Include workers not seen within the window"),
    ),
    responses((status = 200, description = "Worker fleet with liveness", body = Vec<WorkerInfo>))
)]
pub(crate) async fn list_workers(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<ListWorkersQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let registrations = state
        .storage
        .list_worker_registrations(None)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_registration"))?;
    let in_flight: std::collections::HashMap<String, i64> = state
        .storage
        .claimed_task_counts_by_worker()
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .into_iter()
        .collect();

    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let mut by_worker: std::collections::BTreeMap<String, WorkerInfo> =
        std::collections::BTreeMap::new();
    for reg in registrations {
        // Tenant-scoped callers see their own workers plus unscoped ones.
        if let Some(ref tid) = scoped {
            if reg.tenant_id.as_deref().is_some_and(|t| t != tid.as_str()) {
                continue;
            }
        }
        let entry = by_worker
            .entry(reg.worker_id.clone())
            .or_insert_with(|| WorkerInfo {
                worker_id: reg.worker_id.clone(),
                handlers: Vec::new(),
                queues: Vec::new(),
                version: None,
                last_seen_at: reg.last_seen_at,
                alive: false,
                in_flight: 0,
            });
        if !entry.handlers.contains(&reg.handler_name) {
            entry.handlers.push(reg.handler_name);
        }
        if let Some(queue) = reg.queue_name {
            if !entry.queues.contains(&queue) {
                entry.queues.push(queue);
            }
        }
        // Registrations arrive newest-first, so the first version wins.
        if entry.version.is_none() {
            entry.version = reg.version;
        }
        if reg.last_seen_at > entry.last_seen_at {
            entry.last_seen_at = reg.last_seen_at;
        }
    }

    let alive_cutoff =
        chrono::Utc::now() - chrono::Duration::seconds(query.alive_within_secs.max(1));
    let mut workers: Vec<WorkerInfo> = by_worker
        .into_values()
        .map(|mut w| {
            w.alive = w.last_seen_at >= alive_cutoff;
            w.in_flight = in_flight.get(&w.worker_id).copied().unwrap_or(0);
            w.handlers.sort_unstable();
            w.queues.sort_unstable();
            w
        })
        .filter(|w| query.include_stale || w.alive)
        .collect();
    workers.sort_by_key(|w| std::cmp::Reverse(w.last_seen_at));

    Ok(Json(workers))
}

/// Catalog of every handler name the engine can serve.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct HandlerCatalog {
    /// Handlers executed in-process by the engine.
    pub builtin: Vec<String>,
    /// Handler names served by external workers (from the worker registry).
    pub external: Vec<String>,
}

#[utoipa::path(get, path = "/handlers", tag = "workers",
    responses((status = 200, description = "Handler catalog", body = HandlerCatalog))
)]
pub(crate) async fn list_handlers(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
) -> Result<impl IntoResponse, ApiError> {
    let registrations = state
        .storage
        .list_worker_registrations(None)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_registration"))?;
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let mut external: Vec<String> = registrations
        .into_iter()
        .filter(|reg| {
            scoped.as_ref().is_none_or(|tid| {
                reg.tenant_id.as_deref().is_none_or(|t| t == tid.as_str())
            })
        })
        .map(|reg| reg.handler_name)
        .collect();
    external.sort_unstable();
    external.dedup();

    Ok(Json(HandlerCatalog {
        builtin: state.builtin_handlers.as_ref().clone(),
        external,
    }))
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
    // Verify tenant access via the task's owning instance. If the instance is
    // missing we cannot confirm ownership — treat as NotFound so a tenant-scoped
    // caller cannot operate on orphaned tasks from another tenant.
    let inst = state
        .storage
        .get_instance(pre_task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", pre_task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &inst.tenant_id,
        &format!("worker_task {task_id}"),
    )?;
    let tenant_for_cb = Some(inst.tenant_id);

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
        output_size: u32::try_from(output_json.len()).unwrap_or(u32::MAX),
        attempt: task.attempt,
        created_at: chrono::Utc::now(),
    };

    // Re-read instance state before propagating. Between task-claim and
    // completion a cancel/admin-fail could have terminated the instance.
    let Some(mut instance) = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
    else {
        return Ok(StatusCode::OK);
    };
    if instance.state.is_terminal() || instance.state == InstanceState::Paused {
        tracing::info!(
            instance_id = %task.instance_id,
            state = %instance.state,
            block_id = %task_block_id,
            "external worker completion arrived for terminal/paused instance — task accepted, transition skipped"
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

    // Merge step output into context.data BEFORE the atomic write.
    let merged_context = if let Some(obj) = block_output.output.as_object() {
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

    // Find the execution node that this worker task corresponds to.
    // We do this BEFORE the atomic write so we can include the node
    // completion in the same transaction, closing the race where the
    // scheduler claims the instance between output-save and node-complete.
    let tree = state
        .storage
        .get_execution_tree(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    let node = tree.iter().find(|n| {
        n.block_id == task_block_id && matches!(n.state, NodeState::Running | NodeState::Waiting)
    });

    let cas_err = if let Some(node) = node {
        let result = if merged_context {
            state
                .storage
                .save_output_complete_node_merge_context_and_transition(
                    &block_output,
                    node.id,
                    task.instance_id,
                    &instance.context,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        } else {
            state
                .storage
                .save_output_complete_node_and_transition(
                    &block_output,
                    node.id,
                    task.instance_id,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        };
        match result {
            Ok(()) => false,
            Err(orch8_types::error::StorageError::TerminalTarget { .. }) => true,
            Err(e) => return Err(ApiError::from_storage(e, "worker_task")),
        }
    } else {
        // Node not found or already terminal — fall back to the non-atomic
        // path so the instance is still transitioned. This mirrors the
        // fail_task handler which always transitions the instance regardless
        // of whether the node is found in the tree.
        tracing::warn!(
            instance_id = %task.instance_id,
            block_id = %task_block_id,
            "worker completion: execution node not in Running/Waiting state — falling back to non-atomic transition"
        );
        let result = if merged_context {
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
        };
        match result {
            Ok(()) => false,
            Err(orch8_types::error::StorageError::TerminalTarget { .. }) => true,
            Err(e) => return Err(ApiError::from_storage(e, "worker_task")),
        }
    };

    if cas_err {
        tracing::info!(
            instance_id = %task.instance_id,
            block_id = %task_block_id,
            "worker completion CAS failed — instance became terminal/paused after read"
        );
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
    // Verify tenant access via the task's owning instance. If the instance is
    // missing we cannot confirm ownership — treat as NotFound so a tenant-scoped
    // caller cannot operate on orphaned tasks from another tenant.
    let inst = state
        .storage
        .get_instance(pre_task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", pre_task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &inst.tenant_id,
        &format!("worker_task {task_id}"),
    )?;
    let tenant_for_cb = Some(inst.tenant_id);

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
        if inst.state.is_terminal() || inst.state == InstanceState::Paused {
            tracing::info!(
                instance_id = %task.instance_id,
                state = %inst.state,
                block_id = %task.block_id,
                "external worker failure arrived for terminal/paused instance — task accepted, transition skipped"
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
                        u32::from(task.attempt) + 1 < retry.max_attempts
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
            let node_id = tree
                .iter()
                .find(|n| {
                    n.block_id == task.block_id
                        && matches!(n.state, NodeState::Running | NodeState::Waiting)
                })
                .map(|n| n.id);
            state
                .storage
                .retry_worker_task(
                    task_id,
                    &retry_task,
                    node_id,
                    task.instance_id,
                    chrono::Utc::now(),
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
                        u32::from(task.attempt) + 1 < retry.max_attempts
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
    let inst = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "task_instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("task_instance {}", task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &inst.tenant_id,
        &format!("worker_task {task_id}"),
    )?;

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

const fn default_list_limit() -> u32 {
    50
}

fn parse_states(raw: &str) -> Result<Vec<WorkerTaskState>, ApiError> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_worker_state() {
        assert_eq!(
            parse_states("claimed").unwrap(),
            vec![WorkerTaskState::Claimed]
        );
    }

    #[test]
    fn parse_multiple_worker_states() {
        assert_eq!(
            parse_states("pending,claimed,completed").unwrap(),
            vec![
                WorkerTaskState::Pending,
                WorkerTaskState::Claimed,
                WorkerTaskState::Completed
            ]
        );
    }

    #[test]
    fn parse_worker_states_with_whitespace() {
        assert_eq!(
            parse_states(" pending , failed ").unwrap(),
            vec![WorkerTaskState::Pending, WorkerTaskState::Failed]
        );
    }

    #[test]
    fn parse_all_worker_states() {
        let all = "pending,claimed,completed,failed";
        assert_eq!(
            parse_states(all).unwrap(),
            vec![
                WorkerTaskState::Pending,
                WorkerTaskState::Claimed,
                WorkerTaskState::Completed,
                WorkerTaskState::Failed,
            ]
        );
    }

    #[test]
    fn parse_empty_worker_string_returns_empty() {
        assert_eq!(parse_states("").unwrap(), Vec::<WorkerTaskState>::new());
    }

    #[test]
    fn parse_unknown_worker_state_errors() {
        let err = parse_states("claimed,bogus").unwrap_err();
        assert!(err.to_string().contains("unknown worker task state: bogus"));
    }

    #[test]
    fn default_list_limit_is_50() {
        assert_eq!(default_list_limit(), 50);
    }
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
        sort_ascending: false,
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
