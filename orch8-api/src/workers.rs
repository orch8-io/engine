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
    let tasks = state
        .storage
        .claim_worker_tasks(&req.handler_name, &req.worker_id, req.limit)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    // When a tenant header is present, filter claimed tasks to only those belonging to that tenant.
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    if let Some(ref tid) = scoped {
        let mut filtered = Vec::with_capacity(tasks.len());
        for task in tasks {
            if let Ok(Some(inst)) = state.storage.get_instance(task.instance_id).await {
                if &inst.tenant_id == tid {
                    filtered.push(task);
                }
            }
        }
        return Ok(Json(filtered));
    }

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
    let tasks = state
        .storage
        .claim_worker_tasks_from_queue(
            &req.queue_name,
            &req.handler_name,
            &req.worker_id,
            req.limit,
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    // When a tenant header is present, filter claimed tasks to only those belonging to that tenant.
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    if let Some(ref tid) = scoped {
        let mut filtered = Vec::with_capacity(tasks.len());
        for task in tasks {
            if let Ok(Some(inst)) = state.storage.get_instance(task.instance_id).await {
                if &inst.tenant_id == tid {
                    filtered.push(task);
                }
            }
        }
        return Ok(Json(filtered));
    }

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
    if let Ok(Some(inst)) = state.storage.get_instance(pre_task.instance_id).await {
        crate::auth::enforce_tenant_access(&tenant_ctx, &inst.tenant_id, &format!("worker_task {task_id}"))?;
    }

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

    let output_json = serde_json::to_string(&req.output).unwrap_or_default();
    let task_block_id = task.block_id.clone();
    let block_output = BlockOutput {
        id: Uuid::new_v4(),
        instance_id: task.instance_id,
        block_id: task.block_id,
        output: req.output,
        output_ref: None,
        output_size: i32::try_from(output_json.len()).unwrap_or(i32::MAX),
        attempt: task.attempt,
        created_at: chrono::Utc::now(),
    };

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
    if let Ok(Some(inst)) = state.storage.get_instance(pre_task.instance_id).await {
        crate::auth::enforce_tenant_access(&tenant_ctx, &inst.tenant_id, &format!("worker_task {task_id}"))?;
    }

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

    let tree = state
        .storage
        .get_execution_tree(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    let has_tree = !tree.is_empty();

    if req.retryable {
        state
            .storage
            .delete_worker_task(task_id)
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
        crate::auth::enforce_tenant_access(&tenant_ctx, &inst.tenant_id, &format!("worker_task {task_id}"))?;
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
        limit: query.limit,
        offset: query.offset,
    };

    let tasks = state
        .storage
        .list_worker_tasks(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    Ok(Json(tasks))
}

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
