use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::RaceDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a race block: first branch to complete wins.
/// Remaining branches are cancelled. Returns `true` if more work.
pub async fn execute_race(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    race_def: &RaceDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = evaluator::children_of(tree, node.id, None);

    if children.is_empty() {
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // Activate all pending children so they can race.
    evaluator::activate_pending_children(storage, &children).await?;

    // Check if any branch completed (winner).
    if evaluator::any_completed(&children) {
        // Cancel all non-terminal branches and their worker tasks.
        for child in &children {
            if !matches!(
                child.state,
                NodeState::Completed
                    | NodeState::Failed
                    | NodeState::Cancelled
                    | NodeState::Skipped
            ) {
                // If the node is Waiting, cancel its pending worker task.
                if child.state == NodeState::Waiting {
                    storage
                        .cancel_worker_tasks_for_block(instance.id.0, &child.block_id.0)
                        .await?;
                }
                storage
                    .update_node_state(child.id, NodeState::Cancelled)
                    .await?;
            }
        }
        evaluator::complete_node(storage, node.id).await?;
        debug!(
            instance_id = %instance.id,
            block_id = %race_def.id,
            "race block completed — winner found"
        );
        return Ok(true);
    }

    // If all failed (no winner), the race fails.
    if evaluator::all_terminal(&children) {
        evaluator::fail_node(storage, node.id).await?;
        debug!(
            instance_id = %instance.id,
            block_id = %race_def.id,
            "race block failed — all branches failed"
        );
        return Ok(true);
    }

    // Still waiting for a winner.
    Ok(true)
}
