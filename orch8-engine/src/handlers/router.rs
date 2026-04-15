use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::RouterDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a router block: evaluate conditions and execute the matching branch.
/// Non-matching branches are skipped. Returns `true` if more work.
pub async fn execute_router(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    router_def: &RouterDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    // Determine which branch to take.
    let selected_branch = select_branch(router_def, &instance.context);

    let all_children = evaluator::children_of(tree, node.id, None);

    // Skip all non-selected branches.
    for child in &all_children {
        let is_selected = child
            .branch_index
            .is_some_and(|bi| bi == i16::try_from(selected_branch).unwrap_or(0));

        if !is_selected
            && !matches!(
                child.state,
                NodeState::Skipped
                    | NodeState::Cancelled
                    | NodeState::Completed
                    | NodeState::Failed
            )
        {
            storage
                .update_node_state(child.id, NodeState::Skipped)
                .await?;
        }
    }

    // Activate selected branch children.
    let branch_idx = i16::try_from(selected_branch).unwrap_or(0);
    let branch_children = evaluator::children_of(tree, node.id, Some(branch_idx));

    for child in &branch_children {
        if child.state == NodeState::Pending {
            storage
                .update_node_state(child.id, NodeState::Running)
                .await?;
        }
    }

    if branch_children.is_empty() || evaluator::all_terminal(&branch_children) {
        if !branch_children.is_empty() && evaluator::any_failed(&branch_children) {
            evaluator::fail_node(storage, node.id).await?;
        } else {
            evaluator::complete_node(storage, node.id).await?;
        }
        debug!(
            instance_id = %instance.id,
            block_id = %router_def.id,
            selected_branch = selected_branch,
            "router completed"
        );
        return Ok(true);
    }

    // Branch still executing.
    Ok(true)
}

/// Select the branch index by evaluating route conditions.
fn select_branch(
    router_def: &RouterDef,
    context: &orch8_types::context::ExecutionContext,
) -> usize {
    let empty_outputs = serde_json::Value::Object(serde_json::Map::new());
    for (i, route) in router_def.routes.iter().enumerate() {
        if crate::expression::evaluate_condition(&route.condition, context, &empty_outputs) {
            return i;
        }
    }
    // Default branch is at index routes.len().
    router_def.routes.len()
}

#[cfg(test)]
mod tests {
    use crate::expression::evaluate_condition;
    use orch8_types::context::ExecutionContext;
    use serde_json::json;

    fn empty() -> serde_json::Value {
        json!({})
    }

    #[test]
    fn equality_condition() {
        let ctx = ExecutionContext {
            data: json!({"status": "active"}),
            ..Default::default()
        };
        assert!(evaluate_condition("status == \"active\"", &ctx, &empty()));
        assert!(!evaluate_condition("status == \"inactive\"", &ctx, &empty()));
    }

    #[test]
    fn truthy_condition() {
        let ctx = ExecutionContext {
            data: json!({"enabled": true, "disabled": false}),
            ..Default::default()
        };
        assert!(evaluate_condition("enabled", &ctx, &empty()));
        assert!(!evaluate_condition("disabled", &ctx, &empty()));
        assert!(!evaluate_condition("missing", &ctx, &empty()));
    }

    #[test]
    fn comparison_condition() {
        let ctx = ExecutionContext {
            data: json!({"count": 10}),
            ..Default::default()
        };
        assert!(evaluate_condition("count > 5", &ctx, &empty()));
        assert!(!evaluate_condition("count < 5", &ctx, &empty()));
        assert!(evaluate_condition("count >= 10", &ctx, &empty()));
    }
}
