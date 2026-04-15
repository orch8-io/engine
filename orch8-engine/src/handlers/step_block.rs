use orch8_storage::StorageBackend;
use orch8_types::execution::ExecutionNode;
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
