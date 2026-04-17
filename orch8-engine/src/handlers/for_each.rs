use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::ForEachDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a forEach block: iterate over a collection from context.
/// Returns `true` if more work.
pub async fn execute_for_each(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    fe_def: &ForEachDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = evaluator::children_of(tree, node.id, None);

    // Resolve the collection from context.data.
    let collection = resolve_collection(&fe_def.collection, &instance.context);
    let Some(items) = collection else {
        warn!(
            instance_id = %instance.id,
            block_id = %fe_def.id,
            collection = %fe_def.collection,
            "forEach collection not found or not an array, completing"
        );
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    };

    if items.is_empty() {
        evaluator::complete_node(storage, node.id).await?;
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

    // If body children are done, the forEach is complete.
    if !children.is_empty() && evaluator::all_terminal(&children) {
        if evaluator::any_failed(&children) {
            evaluator::fail_node(storage, node.id).await?;
        } else {
            evaluator::complete_node(storage, node.id).await?;
            debug!(
                instance_id = %instance.id,
                block_id = %fe_def.id,
                "forEach completed"
            );
        }
        return Ok(true);
    }

    // Body still executing.
    Ok(true)
}

fn resolve_collection(
    path: &str,
    context: &orch8_types::context::ExecutionContext,
) -> Option<Vec<serde_json::Value>> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = &context.data;
    for part in &parts {
        current = current.get(part)?;
    }
    current.as_array().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use serde_json::json;

    #[test]
    fn resolve_collection_from_context() {
        let ctx = ExecutionContext {
            data: json!({"users": [1, 2, 3]}),
            ..Default::default()
        };
        let items = resolve_collection("users", &ctx);
        assert_eq!(items, Some(vec![json!(1), json!(2), json!(3)]));
    }

    #[test]
    fn resolve_nested_collection() {
        let ctx = ExecutionContext {
            data: json!({"data": {"items": ["a", "b"]}}),
            ..Default::default()
        };
        let items = resolve_collection("data.items", &ctx);
        assert_eq!(items, Some(vec![json!("a"), json!("b")]));
    }

    #[test]
    fn resolve_missing_returns_none() {
        let ctx = ExecutionContext::default();
        assert!(resolve_collection("missing", &ctx).is_none());
    }

    #[test]
    fn resolve_collection_returns_none_when_value_is_not_array() {
        let ctx = ExecutionContext {
            data: json!({"users": "not-an-array"}),
            ..Default::default()
        };
        assert!(resolve_collection("users", &ctx).is_none());
    }

    #[test]
    fn resolve_collection_returns_empty_vec_for_empty_array() {
        let ctx = ExecutionContext {
            data: json!({"xs": []}),
            ..Default::default()
        };
        let v = resolve_collection("xs", &ctx).expect("empty array must still resolve");
        assert!(v.is_empty());
    }

    #[test]
    fn resolve_collection_stops_descending_on_non_object() {
        // Path "a.b.c" where a is a primitive — must not panic.
        let ctx = ExecutionContext {
            data: json!({"a": 42}),
            ..Default::default()
        };
        assert!(resolve_collection("a.b.c", &ctx).is_none());
    }

    #[test]
    fn resolve_collection_deep_nested_array() {
        let ctx = ExecutionContext {
            data: json!({"l1": {"l2": {"l3": [10, 20]}}}),
            ..Default::default()
        };
        let v = resolve_collection("l1.l2.l3", &ctx).unwrap();
        assert_eq!(v, vec![json!(10), json!(20)]);
    }
}
