use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::ParallelDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a parallel block: all branches run concurrently.
/// Completes when all branches are done. Fails if any branch fails.
/// Returns `true` if more work to do.
pub async fn execute_parallel(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    par_def: &ParallelDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = evaluator::children_of(tree, node.id, None);

    if children.is_empty() {
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // Activate all pending children so they can be dispatched.
    for child in &children {
        if child.state == NodeState::Pending {
            storage
                .update_node_state(child.id, NodeState::Running)
                .await?;
        }
    }

    // Check if all children are done.
    if evaluator::all_terminal(&children) {
        if evaluator::all_completed(&children) {
            evaluator::complete_node(storage, node.id).await?;
            debug!(
                instance_id = %instance.id,
                block_id = %par_def.id,
                "parallel block completed — all branches succeeded"
            );
        } else {
            evaluator::fail_node(storage, node.id).await?;
            debug!(
                instance_id = %instance.id,
                block_id = %par_def.id,
                "parallel block failed — one or more branches failed"
            );
        }
        return Ok(true);
    }

    // Not all done yet — more work on next tick.
    Ok(true)
}
