use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::CancellationScopeDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a cancellation scope block.
/// Children execute sequentially. External cancel signals do NOT propagate
/// into children — the scope completes normally even if the parent instance
/// receives a cancel request.
/// Returns `true` if more work remains.
pub async fn execute_cancellation_scope(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    _instance: &TaskInstance,
    node: &ExecutionNode,
    _scope_def: &CancellationScopeDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = evaluator::children_of(tree, node.id, None);

    if children.is_empty() {
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // If all children are terminal, the scope is done.
    if evaluator::all_terminal(&children) {
        if evaluator::any_failed(&children) {
            storage
                .update_node_state(node.id, NodeState::Failed)
                .await?;
        } else {
            evaluator::complete_node(storage, node.id).await?;
        }
        return Ok(true);
    }

    // Activate the next pending child (sequential execution).
    for child in &children {
        if child.state == NodeState::Pending {
            storage
                .update_node_state(child.id, NodeState::Running)
                .await?;
            return Ok(true);
        }
        // If a child is still running/waiting, wait for it.
        if matches!(child.state, NodeState::Running | NodeState::Waiting) {
            return Ok(true);
        }
    }

    Ok(true)
}
