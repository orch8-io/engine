use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::StepDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::step::StepExecParams;
use crate::handlers::HandlerRegistry;

/// Execute a step node within the execution tree.
/// Returns `true` if the instance has more work (should re-schedule).
pub async fn execute_step_node(
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    step_def: &StepDef,
) -> Result<bool, EngineError> {
    // If the handler is not registered in-process, dispatch to external worker queue.
    if !handlers.contains(&step_def.handler) {
        return dispatch_step_to_external_worker(storage, instance, node, step_def).await;
    }

    let exec_params = StepExecParams {
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        params: step_def.params.clone(),
        context: instance.context.clone(),
        attempt: 0,
        timeout: step_def.timeout,
    };

    match crate::handlers::step::execute_step(storage, handlers, exec_params).await {
        Ok(_output) => {
            evaluator::complete_node(storage, node.id).await?;
            Ok(true)
        }
        Err(EngineError::StepFailed {
            retryable: true, ..
        }) => {
            // Leave node as Running for retry on next tick.
            Ok(true)
        }
        Err(e) => {
            evaluator::fail_node(storage, node.id).await?;
            Err(e)
        }
    }
}

/// Dispatch a step within the execution tree to the external worker queue.
/// The node stays Running; the instance transitions to Waiting.
async fn dispatch_step_to_external_worker(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    _node: &ExecutionNode,
    step_def: &StepDef,
) -> Result<bool, EngineError> {
    use orch8_types::instance::InstanceState;
    use orch8_types::worker::{WorkerTask, WorkerTaskState};

    let task = WorkerTask {
        id: uuid::Uuid::new_v4(),
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        params: step_def.params.clone(),
        context: serde_json::to_value(&instance.context).unwrap_or_default(),
        attempt: 0,
        timeout_ms: step_def.timeout.map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)),
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

    storage.create_worker_task(&task).await?;

    // Keep the node as Running — it will be resolved when the worker completes.
    // Transition instance to Waiting so the scheduler doesn't re-claim it.
    crate::lifecycle::transition_instance(
        storage,
        instance.id,
        InstanceState::Running,
        InstanceState::Waiting,
        None,
    )
    .await?;

    // Mark step node as a special "waiting" state. Since execution nodes don't
    // have a Waiting state, we keep it Running — the evaluate loop will find
    // no actionable work and return Ok(true), causing re-schedule. The instance
    // is Waiting so the scheduler won't pick it up until the worker completes
    // and transitions it back to Scheduled.

    tracing::info!(
        instance_id = %instance.id,
        block_id = %step_def.id,
        handler = %step_def.handler,
        "dispatched tree step to external worker queue"
    );

    Ok(false) // No more work in this tick — instance is now Waiting.
}

/// Called when an external worker completes a task that belongs to an execution tree.
/// Marks the corresponding execution node as completed.
pub async fn complete_external_step_node(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
    block_id: &orch8_types::ids::BlockId,
) -> Result<(), EngineError> {
    let tree = storage.get_execution_tree(instance_id).await?;
    if let Some(node) = tree.iter().find(|n| n.block_id == *block_id && n.state == NodeState::Running) {
        evaluator::complete_node(storage, node.id).await?;
    }
    Ok(())
}

/// Called when an external worker permanently fails a task in an execution tree.
/// Marks the corresponding execution node as failed.
pub async fn fail_external_step_node(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
    block_id: &orch8_types::ids::BlockId,
) -> Result<(), EngineError> {
    let tree = storage.get_execution_tree(instance_id).await?;
    if let Some(node) = tree.iter().find(|n| n.block_id == *block_id && n.state == NodeState::Running) {
        evaluator::fail_node(storage, node.id).await?;
    }
    Ok(())
}
