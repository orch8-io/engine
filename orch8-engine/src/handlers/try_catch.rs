use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::TryCatchDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a try-catch-finally block.
/// Branch 0 = try, Branch 1 = catch, Branch 2 = finally.
/// Returns `true` if more work.
pub async fn execute_try_catch(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    tc_def: &TryCatchDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let try_children = evaluator::children_of(tree, node.id, Some(0));
    let catch_children = evaluator::children_of(tree, node.id, Some(1));
    let finally_children = evaluator::children_of(tree, node.id, Some(2));

    // Phase 1: Try block executing.
    if !try_children.is_empty() && !evaluator::all_terminal(&try_children) {
        return Ok(true);
    }

    let try_failed = evaluator::any_failed(&try_children);

    // Phase 2: If try failed, activate catch block.
    if try_failed && !catch_children.is_empty() {
        let catch_done = evaluator::all_terminal(&catch_children);

        if !catch_done {
            // Catch not yet started or still running — let it proceed.
            // Activate catch children that are still pending.
            for child in &catch_children {
                if child.state == NodeState::Pending {
                    storage
                        .update_node_state(child.id, NodeState::Running)
                        .await?;
                }
            }
            return Ok(true);
        }
    } else if !try_failed {
        // Try succeeded — skip catch.
        for child in &catch_children {
            if matches!(child.state, NodeState::Pending | NodeState::Running) {
                storage
                    .update_node_state(child.id, NodeState::Skipped)
                    .await?;
            }
        }
    }

    // Phase 3: Finally block always runs.
    if !finally_children.is_empty() {
        let finally_done = evaluator::all_terminal(&finally_children);
        if !finally_done {
            for child in &finally_children {
                if child.state == NodeState::Pending {
                    storage
                        .update_node_state(child.id, NodeState::Running)
                        .await?;
                }
            }
            return Ok(true);
        }
    }

    // All phases complete. Node succeeds if try succeeded (or catch recovered).
    if try_failed && evaluator::any_failed(&catch_children) {
        evaluator::fail_node(storage, node.id).await?;
    } else {
        evaluator::complete_node(storage, node.id).await?;
    }

    debug!(
        instance_id = %instance.id,
        block_id = %tc_def.id,
        try_failed = try_failed,
        "try-catch-finally completed"
    );

    Ok(true)
}
