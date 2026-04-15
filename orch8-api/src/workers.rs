use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;

use orch8_types::execution::NodeState;

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/workers/tasks/poll", post(poll_tasks))
        .route("/workers/tasks/{id}/complete", post(complete_task))
        .route("/workers/tasks/{id}/fail", post(fail_task))
        .route("/workers/tasks/{id}/heartbeat", post(heartbeat_task))
}

#[derive(Deserialize)]
struct PollRequest {
    handler_name: String,
    worker_id: String,
    #[serde(default = "default_poll_limit")]
    limit: u32,
}

fn default_poll_limit() -> u32 {
    1
}

async fn poll_tasks(
    State(state): State<AppState>,
    Json(req): Json<PollRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tasks = state
        .storage
        .claim_worker_tasks(&req.handler_name, &req.worker_id, req.limit)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    Ok(Json(tasks))
}

#[derive(Deserialize)]
struct CompleteRequest {
    worker_id: String,
    output: serde_json::Value,
}

async fn complete_task(
    State(state): State<AppState>,
    Path(task_id): Path<Uuid>,
    Json(req): Json<CompleteRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Mark the worker task as completed.
    let updated = state
        .storage
        .complete_worker_task(task_id, &req.worker_id, &req.output)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    if !updated {
        return Err(ApiError::NotFound(format!("worker_task {task_id}")));
    }

    // Fetch the task to get instance_id and block_id for the block output.
    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;

    // Build BlockOutput from the worker's result.
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

    // Save block output and transition instance Waiting → Scheduled in one transaction.
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

    // If this instance uses an execution tree, mark the corresponding node as completed.
    let tree = state
        .storage
        .get_execution_tree(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    if let Some(node) = tree
        .iter()
        .find(|n| n.block_id == task_block_id && n.state == NodeState::Running)
    {
        state
            .storage
            .update_node_state(node.id, NodeState::Completed)
            .await
            .map_err(|e| ApiError::from_storage(e, "execution_node"))?;
    }

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct FailRequest {
    worker_id: String,
    message: String,
    #[serde(default)]
    retryable: bool,
}

async fn fail_task(
    State(state): State<AppState>,
    Path(task_id): Path<Uuid>,
    Json(req): Json<FailRequest>,
) -> Result<impl IntoResponse, ApiError> {
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

    // Check if this instance uses an execution tree.
    let tree = state
        .storage
        .get_execution_tree(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    let has_tree = !tree.is_empty();

    if req.retryable {
        // Delete the failed task so the scheduler can re-create it on re-dispatch.
        state
            .storage
            .delete_worker_task(task_id)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

        // Re-schedule instance so the scheduler re-dispatches the step.
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
        // Permanent failure in a tree-based instance — mark the execution node
        // as failed and re-schedule the instance so the evaluator can handle it
        // (e.g., try-catch can catch the failure).
        if let Some(node) = tree
            .iter()
            .find(|n| n.block_id == task.block_id && n.state == NodeState::Running)
        {
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
        // Permanent failure — mark instance as failed.
        state
            .storage
            .update_instance_state(task.instance_id, InstanceState::Failed, None)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
    }

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct HeartbeatRequest {
    worker_id: String,
}

async fn heartbeat_task(
    State(state): State<AppState>,
    Path(task_id): Path<Uuid>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<impl IntoResponse, ApiError> {
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
