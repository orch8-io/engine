use std::future::Future;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::plugin::PluginType;
use orch8_types::sequence::StepDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::step::StepExecParams;
use crate::handlers::HandlerRegistry;

/// Dispatch a plugin handler and map the `StepError` result to node state transitions.
async fn dispatch_plugin<F, Fut>(
    storage: &dyn StorageBackend,
    node: &ExecutionNode,
    handler_fn: F,
) -> Result<bool, EngineError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<serde_json::Value, orch8_types::error::StepError>>,
{
    match handler_fn().await {
        Ok(_output) => {
            evaluator::complete_node(storage, node.id).await?;
            Ok(true)
        }
        Err(orch8_types::error::StepError::Retryable { .. }) => Ok(true),
        Err(_) => {
            evaluator::fail_node(storage, node.id).await?;
            Ok(false)
        }
    }
}

/// Execute a step node within the execution tree.
/// Returns `true` if the instance has more work (should re-schedule).
pub async fn execute_step_node(
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    step_def: &StepDef,
) -> Result<bool, EngineError> {
    // If the handler is a gRPC plugin, resolve via the plugin registry then dispatch.
    if super::grpc_plugin::is_grpc_handler(&step_def.handler) {
        let endpoint = resolve_plugin_source(storage, &step_def.handler, PluginType::Grpc).await
            .unwrap_or_else(|| step_def.handler.clone());
        let mut params = step_def.params.clone();
        params["_grpc_endpoint"] = serde_json::Value::String(endpoint);
        let ctx = super::StepContext {
            instance_id: instance.id,
            block_id: step_def.id.clone(),
            params,
            context: instance.context.clone(),
            attempt: 0,
        };
        return dispatch_plugin(storage, node, || {
            super::grpc_plugin::handle_grpc_plugin(ctx)
        }).await;
    }

    // If the handler is a WASM plugin, resolve via the plugin registry then dispatch.
    if super::wasm_plugin::is_wasm_handler(&step_def.handler) {
        if let Some(plugin_name) = super::wasm_plugin::parse_plugin_name(&step_def.handler) {
            let wasm_path = resolve_plugin_source(storage, plugin_name, PluginType::Wasm).await
                .unwrap_or_else(|| plugin_name.to_string());
            let ctx = super::StepContext {
                instance_id: instance.id,
                block_id: step_def.id.clone(),
                params: step_def.params.clone(),
                context: instance.context.clone(),
                attempt: 0,
            };
            return dispatch_plugin(storage, node, || {
                super::wasm_plugin::handle_wasm_plugin(ctx, &wasm_path)
            }).await;
        }
    }

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
        externalize_threshold: 0, // Tree evaluator does not externalize (no config available)
    };

    match crate::handlers::step::execute_step(storage, handlers, exec_params).await {
        Ok(output) => {
            // Check for self-modify output: inject blocks into the instance.
            if output.get("_self_modify").and_then(serde_json::Value::as_bool) == Some(true) {
                if let Some(blocks) = output.get("blocks").filter(|v| v.is_array()) {
                    let position = output.get("position").and_then(serde_json::Value::as_u64);
                    let final_blocks = if let Some(pos) = position {
                        let existing = storage
                            .get_injected_blocks(instance.id)
                            .await
                            .unwrap_or(None);
                        let mut arr = existing
                            .and_then(|v| v.as_array().cloned())
                            .unwrap_or_default();
                        let new = blocks.as_array().cloned().unwrap_or_default();
                        #[allow(clippy::cast_possible_truncation)]
                        let at = (pos.min(usize::MAX as u64) as usize).min(arr.len());
                        for (i, b) in new.into_iter().enumerate() {
                            arr.insert(at + i, b);
                        }
                        serde_json::Value::Array(arr)
                    } else {
                        blocks.clone()
                    };
                    if let Err(e) = storage.inject_blocks(instance.id, &final_blocks).await {
                        tracing::warn!(
                            instance_id = %instance.id,
                            error = %e,
                            "failed to inject self-modify blocks"
                        );
                    }
                }
            }
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
/// The node is marked Waiting; the instance state is NOT changed here.
async fn dispatch_step_to_external_worker(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node: &ExecutionNode,
    step_def: &StepDef,
) -> Result<bool, EngineError> {
    use orch8_types::worker::{WorkerTask, WorkerTaskState};

    let task = WorkerTask {
        id: uuid::Uuid::new_v4(),
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        queue_name: step_def.queue_name.clone(),
        params: step_def.params.clone(),
        context: serde_json::to_value(&instance.context)
            .map_err(orch8_types::error::StorageError::Serialization)?,
        attempt: 0,
        timeout_ms: step_def
            .timeout
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)),
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

    // Mark the execution node as Waiting so the evaluator won't re-dispatch it.
    // The instance state is NOT changed here — the evaluator may have other steps
    // to execute within the same composite. The caller (evaluator / process_instance_tree)
    // is responsible for transitioning the instance to Waiting when no more work remains.
    storage
        .update_node_state(node.id, orch8_types::execution::NodeState::Waiting)
        .await?;

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
    if let Some(node) = tree.iter().find(|n| {
        n.block_id == *block_id && matches!(n.state, NodeState::Running | NodeState::Waiting)
    }) {
        evaluator::complete_node(storage, node.id).await?;
    }
    Ok(())
}

/// Look up a plugin name in the registry and return its source path/endpoint.
/// Returns `None` if the plugin isn't registered (caller falls back to the raw handler name).
async fn resolve_plugin_source(
    storage: &dyn StorageBackend,
    name: &str,
    expected_type: PluginType,
) -> Option<String> {
    let plugin = match storage.get_plugin(name).await {
        Ok(Some(p)) => p,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(plugin = %name, error = %e, "failed to resolve plugin source");
            return None;
        }
    };
    if plugin.enabled && plugin.plugin_type == expected_type {
        Some(plugin.source)
    } else {
        None
    }
}

/// Called when an external worker permanently fails a task in an execution tree.
/// Marks the corresponding execution node as failed.
pub async fn fail_external_step_node(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
    block_id: &orch8_types::ids::BlockId,
) -> Result<(), EngineError> {
    let tree = storage.get_execution_tree(instance_id).await?;
    if let Some(node) = tree.iter().find(|n| {
        n.block_id == *block_id && matches!(n.state, NodeState::Running | NodeState::Waiting)
    }) {
        evaluator::fail_node(storage, node.id).await?;
    }
    Ok(())
}
