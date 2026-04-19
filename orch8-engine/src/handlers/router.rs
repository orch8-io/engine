use std::borrow::Cow;

use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::RouterDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::externalized;
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
    // Inflate any externalization markers in context.data before evaluating
    // route conditions. Without this, a route like `{{big_field}} == "foo"`
    // would compare against the literal `{_externalized: true, _ref: …}`
    // marker object instead of the real value. Mirrors the inflation that
    // `step_block::context_for_step` performs for step params.
    //
    // Fast path: if no top-level `data` field is a marker, avoid the clone
    // entirely. `is_marker_present` is a sync walk over the existing JSON
    // object and does not touch storage.
    let ctx_for_conditions: Cow<'_, orch8_types::context::ExecutionContext> =
        if is_marker_present(&instance.context) {
            Cow::Owned(
                externalized::resolve_context_markers(storage, instance.context.clone())
                    .await
                    .map_err(EngineError::Storage)?,
            )
        } else {
            Cow::Borrowed(&instance.context)
        };

    // Load block outputs so route conditions can reference `outputs.step_id.field`.
    let outputs = crate::handlers::param_resolve::build_outputs_shape(storage, instance.id)
        .await
        .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));

    // Determine which branch to take.
    let selected_branch = select_branch(router_def, ctx_for_conditions.as_ref(), &outputs);

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

    evaluator::activate_pending_children(storage, &branch_children).await?;

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

/// Cheap sync check: does any top-level `context.data` field look like an
/// externalization marker? Returning `false` lets the caller skip the clone
/// + async lookup path entirely.
fn is_marker_present(ctx: &orch8_types::context::ExecutionContext) -> bool {
    ctx.data
        .as_object()
        .is_some_and(|obj| obj.values().any(externalized::is_ref_marker))
}

/// Select the branch index by evaluating route conditions.
fn select_branch(
    router_def: &RouterDef,
    context: &orch8_types::context::ExecutionContext,
    outputs: &serde_json::Value,
) -> usize {
    for (i, route) in router_def.routes.iter().enumerate() {
        if crate::expression::evaluate_condition(&route.condition, context, outputs) {
            return i;
        }
    }
    // Default branch is at index routes.len().
    router_def.routes.len()
}

#[cfg(test)]
mod tests {
    use super::{is_marker_present, select_branch};
    use crate::expression::evaluate_condition;
    use crate::externalized;
    use chrono::Utc;
    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::sequence::{Route, RouterDef};
    use serde_json::json;

    fn empty() -> serde_json::Value {
        json!({})
    }

    /// Seed parent `task_instances` row so the `externalized_state` FK holds.
    async fn seed_instance(storage: &SqliteStorage, id: InstanceId) {
        let now = Utc::now();
        let inst = TaskInstance {
            id,
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&inst).await.unwrap();
    }

    #[test]
    fn equality_condition() {
        let ctx = ExecutionContext {
            data: json!({"status": "active"}),
            ..Default::default()
        };
        assert!(evaluate_condition("status == \"active\"", &ctx, &empty()));
        assert!(!evaluate_condition(
            "status == \"inactive\"",
            &ctx,
            &empty()
        ));
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

    #[test]
    fn is_marker_present_detects_top_level_marker() {
        let ctx = ExecutionContext {
            data: json!({
                "plain": "x",
                "big": {"_externalized": true, "_ref": "k"}
            }),
            ..Default::default()
        };
        assert!(is_marker_present(&ctx));
    }

    #[test]
    fn is_marker_present_false_when_no_marker() {
        let ctx = ExecutionContext {
            data: json!({"plain": "x", "n": 1, "nested": {"_ref": "k"}}),
            ..Default::default()
        };
        // `_ref` inside a non-marker-shaped nested object is not a top-level marker.
        assert!(!is_marker_present(&ctx));
    }

    #[test]
    fn is_marker_present_false_for_non_object_data() {
        let ctx = ExecutionContext {
            data: json!("scalar"),
            ..Default::default()
        };
        assert!(!is_marker_present(&ctx));
    }

    /// End-to-end: a router condition that references a field stored as an
    /// externalization marker selects the correct branch only when the marker
    /// has been inflated. This is the invariant `execute_router` enforces
    /// before delegating to `select_branch`.
    #[tokio::test]
    async fn router_selects_branch_after_marker_inflation() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;
        storage
            .save_externalized_state(instance_id, "inst:ctx:data:status", &json!("active"))
            .await
            .unwrap();

        let router = RouterDef {
            id: BlockId("r".into()),
            routes: vec![
                Route {
                    condition: "status == \"active\"".into(),
                    blocks: vec![],
                },
                Route {
                    condition: "status == \"inactive\"".into(),
                    blocks: vec![],
                },
            ],
            default: None,
        };

        let ctx = ExecutionContext {
            data: json!({
                "status": {"_externalized": true, "_ref": "inst:ctx:data:status"}
            }),
            ..ExecutionContext::default()
        };

        // Before inflation, the condition compares against the marker object
        // and falls through to the default branch (index == routes.len()).
        assert_eq!(select_branch(&router, &ctx, &empty()), router.routes.len());

        // After inflation, the first route matches.
        assert!(is_marker_present(&ctx));
        let inflated = externalized::resolve_context_markers(&storage, ctx)
            .await
            .unwrap();
        assert_eq!(inflated.data["status"], json!("active"));
        assert_eq!(select_branch(&router, &inflated, &empty()), 0);
    }

    /// A missing externalized payload leaves the marker in place. The router
    /// must not panic and must fall through to the default branch, letting
    /// downstream code surface the broken ref.
    #[tokio::test]
    async fn router_broken_ref_falls_through_to_default() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let router = RouterDef {
            id: BlockId("r".into()),
            routes: vec![Route {
                condition: "status == \"active\"".into(),
                blocks: vec![],
            }],
            default: None,
        };
        let ctx = ExecutionContext {
            data: json!({
                "status": {"_externalized": true, "_ref": "missing:key"}
            }),
            ..ExecutionContext::default()
        };
        let resolved = externalized::resolve_context_markers(&storage, ctx)
            .await
            .unwrap();
        // Marker is still present because payload was never written.
        assert!(is_marker_present(&resolved));
        assert_eq!(
            select_branch(&router, &resolved, &empty()),
            router.routes.len()
        );
    }
}
