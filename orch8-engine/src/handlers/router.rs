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
    for (i, route) in router_def.routes.iter().enumerate() {
        if evaluate_route_condition(&route.condition, context) {
            return i;
        }
    }
    // Default branch is at index routes.len().
    router_def.routes.len()
}

/// Simple condition evaluation against context.data.
fn evaluate_route_condition(
    condition: &str,
    context: &orch8_types::context::ExecutionContext,
) -> bool {
    // Support simple "path == value" conditions or truthy path checks.
    if let Some((path, expected)) = condition.split_once("==") {
        let path = path.trim();
        let expected = expected.trim().trim_matches('"');
        let actual = resolve_path(path, &context.data);
        return match actual {
            Some(serde_json::Value::String(s)) => s == expected,
            Some(serde_json::Value::Number(n)) => n.to_string() == expected,
            Some(serde_json::Value::Bool(b)) => b.to_string() == expected,
            _ => false,
        };
    }

    // Truthy check.
    let value = resolve_path(condition, &context.data);
    value.is_some_and(|v| is_truthy(&v))
}

fn resolve_path(path: &str, data: &serde_json::Value) -> Option<serde_json::Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = data;
    for part in &parts {
        current = current.get(part)?;
    }
    Some(current.clone())
}

fn is_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use serde_json::json;

    #[test]
    fn equality_condition() {
        let ctx = ExecutionContext {
            data: json!({"status": "active"}),
            ..Default::default()
        };
        assert!(evaluate_route_condition("status == active", &ctx));
        assert!(evaluate_route_condition("status == \"active\"", &ctx));
        assert!(!evaluate_route_condition("status == inactive", &ctx));
    }

    #[test]
    fn truthy_condition() {
        let ctx = ExecutionContext {
            data: json!({"enabled": true, "disabled": false}),
            ..Default::default()
        };
        assert!(evaluate_route_condition("enabled", &ctx));
        assert!(!evaluate_route_condition("disabled", &ctx));
        assert!(!evaluate_route_condition("missing", &ctx));
    }
}
