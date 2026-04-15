use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};

use orch8_types::instance::TaskInstance;
use orch8_types::sequence::LoopDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a loop block: repeatedly execute body while condition is true.
/// The iteration count is tracked via completed outputs for the loop block.
/// Returns `true` if more work.
pub async fn execute_loop(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    loop_def: &LoopDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = evaluator::children_of(tree, node.id, None);

    // Count completed iterations via block outputs.
    let outputs = storage.get_all_outputs(instance.id).await?;
    let iteration_count = outputs
        .iter()
        .filter(|o| {
            o.block_id
                .0
                .starts_with(&format!("{}__iter_", loop_def.id.0))
        })
        .count();
    let iteration = u32::try_from(iteration_count).unwrap_or(u32::MAX);

    if iteration >= loop_def.max_iterations {
        warn!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            max = loop_def.max_iterations,
            "loop max iterations reached"
        );
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // Evaluate condition using the shared expression evaluator.
    let empty_outputs = serde_json::Value::Object(serde_json::Map::new());
    let condition_value =
        crate::expression::evaluate_condition(&loop_def.condition, &instance.context, &empty_outputs);
    if !condition_value {
        evaluator::complete_node(storage, node.id).await?;
        debug!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            iterations = iteration,
            "loop condition false, completing"
        );
        return Ok(true);
    }

    // Activate pending body children.
    for child in &children {
        if child.state == NodeState::Pending {
            storage
                .update_node_state(child.id, NodeState::Running)
                .await?;
        }
    }

    // If body children are all done, it means one iteration completed.
    // Reset them for the next iteration.
    if !children.is_empty() && evaluator::all_terminal(&children) {
        if evaluator::any_failed(&children) {
            evaluator::fail_node(storage, node.id).await?;
            return Ok(true);
        }
        // Iteration succeeded — loop around for condition check on next tick.
        debug!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            iteration = iteration,
            "loop iteration completed"
        );
        return Ok(true);
    }

    // Body still executing.
    Ok(true)
}


#[cfg(test)]
mod tests {
    use crate::expression::{evaluate_condition, is_truthy};
    use orch8_types::context::ExecutionContext;
    use serde_json::json;

    fn empty() -> serde_json::Value {
        json!({})
    }

    #[test]
    fn truthy_values() {
        assert!(is_truthy(&json!(true)));
        assert!(is_truthy(&json!(1)));
        assert!(is_truthy(&json!("hello")));
        assert!(is_truthy(&json!([1])));
        assert!(is_truthy(&json!({"a": 1})));
    }

    #[test]
    fn falsy_values() {
        assert!(!is_truthy(&json!(null)));
        assert!(!is_truthy(&json!(false)));
        assert!(!is_truthy(&json!(0)));
        assert!(!is_truthy(&json!("")));
        assert!(!is_truthy(&json!([])));
    }

    #[test]
    fn condition_evaluation() {
        let ctx = ExecutionContext {
            data: json!({"loop": {"active": true}}),
            ..Default::default()
        };
        assert!(evaluate_condition("loop.active", &ctx, &empty()));
        assert!(!evaluate_condition("loop.missing", &ctx, &empty()));
    }
}
