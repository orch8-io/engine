use std::borrow::Cow;

use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::RouterDef;

use crate::error::EngineError;
use crate::evaluator::{self, SeqProgress};
use crate::externalized;
use crate::handlers::HandlerRegistry;
use crate::handlers::param_resolve::OutputsSnapshot;

/// Marker field recording the router's branch decision in its own
/// `BlockOutput`, so the decision is made exactly once per activation.
const SELECTED_BRANCH_KEY: &str = "_selected_branch";

/// Execute a router block: evaluate conditions and execute the matching branch.
/// Non-matching branches are skipped. Returns `true` if more work.
///
/// The branch is decided ONCE — on the first tick — and persisted as a
/// `{ "_selected_branch": i }` marker output keyed by the router's block id.
/// Later ticks reuse the memoized decision, so context/output changes made
/// by the running branch can never flip the route mid-flight (which would
/// skip the Running branch and start another). `reset_subtree_to_pending`
/// clears the marker when an enclosing loop/`for_each` starts a new
/// iteration, so each iteration decides afresh.
pub async fn execute_router(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    router_def: &RouterDef,
    tree: &[ExecutionNode],
    outputs: &OutputsSnapshot,
) -> Result<bool, EngineError> {
    let memoized = storage
        .get_block_output(instance.id, &router_def.id)
        .await?
        .and_then(|o| {
            o.output
                .get(SELECTED_BRANCH_KEY)
                .and_then(serde_json::Value::as_u64)
        })
        .and_then(|n| usize::try_from(n).ok());

    let selected_branch = if let Some(branch) = memoized {
        branch
    } else {
        let branch = decide_branch(storage, instance, router_def, outputs).await?;
        // Persist before acting on it: a crash after this write replays the
        // same decision instead of re-deciding against newer state.
        let marker = BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: router_def.id.clone(),
            output: serde_json::json!({ SELECTED_BRANCH_KEY: branch }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        };
        storage.save_block_output(&marker).await?;
        branch
    };

    // Ref#3: the execution-tree schema stores `branch_index` as i16, so a
    // router with more than 32 767 branches cannot be addressed. Rather than
    // silently falling back to `unwrap_or(0)` (which would skip every selected
    // branch past the cap), fail the node permanently with a diagnostic.
    let branch_idx = i16::try_from(selected_branch).map_err(|_| {
        EngineError::InvalidConfig(format!(
            "router selected branch {selected_branch} exceeds the execution-tree \
             branch_index range (max {}). Split the router or widen the schema.",
            i16::MAX
        ))
    })?;

    let all_children = evaluator::children_of(tree, node.id, None);

    // Skip entire non-selected branch subtrees. A branch child may itself be a
    // composite; skipping only that direct child strands its descendants in
    // Pending even though the route can never execute. With a memoized
    // decision this is a no-op after the first tick.
    let non_selected_roots: Vec<_> = all_children
        .iter()
        .filter(|child| child.branch_index != Some(branch_idx))
        .map(|child| child.id)
        .collect();
    evaluator::skip_subtrees(storage, instance.id, tree, &non_selected_roots).await?;

    // Run the selected branch — sequential cursor with fail-fast.
    let branch_children = evaluator::children_of(tree, node.id, Some(branch_idx));
    let final_state = match evaluator::advance_sequence(storage, &branch_children).await? {
        SeqProgress::Advanced | SeqProgress::Blocked => return Ok(true),
        SeqProgress::Failed | SeqProgress::Cancelled => NodeState::Failed,
        SeqProgress::Done => NodeState::Completed,
    };
    evaluator::settle_composite(storage, instance.id, tree, node.id, final_state).await?;
    debug!(
        instance_id = %instance.id,
        block_id = %router_def.id,
        selected_branch = selected_branch,
        state = %final_state,
        "router completed"
    );
    Ok(true)
}

/// Evaluate route conditions against the (marker-inflated) context and the
/// instance's outputs, returning the selected branch index.
async fn decide_branch(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    router_def: &RouterDef,
    outputs: &OutputsSnapshot,
) -> Result<usize, EngineError> {
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
                externalized::resolve_context_markers(
                    storage,
                    instance.id,
                    instance.context.clone(),
                )
                .await
                .map_err(EngineError::Storage)?,
            )
        } else {
            Cow::Borrowed(&instance.context)
        };

    // Load block outputs so route conditions can reference `outputs.step_id.field`.
    // Uses the shared per-iteration snapshot — if earlier handlers in the same
    // iteration already fetched, this is a no-op; on miss falls back to an
    // empty map to keep route-selection deterministic.
    let empty = serde_json::Value::Object(serde_json::Map::new());
    let outputs_val = outputs.get(storage, instance.id).await.unwrap_or(&empty);

    Ok(select_branch(
        router_def,
        ctx_for_conditions.as_ref(),
        outputs_val,
    ))
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
#[allow(clippy::similar_names)]
mod tests {
    use super::{OutputsSnapshot, is_marker_present, select_branch};
    use crate::expression::evaluate_condition;
    use crate::externalized;
    use chrono::Utc;
    use orch8_storage::{
        ExecutionTreeStore, InstanceStore, OutputStore, ResourceStore, sqlite::SqliteStorage,
    };
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
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
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
            budget: None,
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
            id: BlockId::new("r"),
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
        let inflated = externalized::resolve_context_markers(&storage, instance_id, ctx)
            .await
            .unwrap();
        assert_eq!(inflated.data["status"], json!("active"));
        assert_eq!(select_branch(&router, &inflated, &empty()), 0);
    }

    // ------------------------------------------------------------------
    // execute_router integration tests (RT1-RT9)
    // ------------------------------------------------------------------

    use super::execute_router;
    use crate::handlers::HandlerRegistry;
    use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
    use orch8_types::ids::ExecutionNodeId;

    fn mk_node_rt(
        parent: Option<ExecutionNodeId>,
        bid: &str,
        bt: BlockType,
        state: NodeState,
        branch_index: Option<i16>,
        inst: InstanceId,
    ) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: inst,
            block_id: BlockId::new(bid),
            parent_id: parent,
            block_type: bt,
            branch_index,
            state,
            started_at: None,
            completed_at: None,
        }
    }

    fn mk_instance_rt(id: InstanceId, ctx: ExecutionContext) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id,
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ctx,
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn setup_rt(
        nodes: Vec<ExecutionNode>,
        inst_id: InstanceId,
    ) -> (SqliteStorage, Vec<ExecutionNode>) {
        let s = SqliteStorage::in_memory().await.unwrap();
        seed_instance(&s, inst_id).await;
        s.create_execution_nodes_batch(&nodes).await.unwrap();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        (s, tree)
    }

    // RT1: No route matches + default exists → default branch (index == routes.len()) selected.
    #[tokio::test]
    async fn router_falls_back_to_default() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let parent_id = parent.id;
        let route0 = mk_node_rt(
            Some(parent_id),
            "r0",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let default_child = mk_node_rt(
            Some(parent_id),
            "rd",
            BlockType::Step,
            NodeState::Pending,
            Some(1),
            inst_id,
        );
        let (s, tree) = setup_rt(
            vec![parent.clone(), route0.clone(), default_child.clone()],
            inst_id,
        )
        .await;
        let inst = mk_instance_rt(
            inst_id,
            ExecutionContext {
                data: json!({"x": 0}),
                ..Default::default()
            },
        );
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![Route {
                condition: "x == 1".into(),
                blocks: vec![],
            }],
            default: Some(vec![]),
        };
        let handlers = HandlerRegistry::new();
        execute_router(
            &s,
            &handlers,
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        let r0 = after.iter().find(|n| n.id == route0.id).unwrap();
        let rd = after.iter().find(|n| n.id == default_child.id).unwrap();
        assert_eq!(r0.state, NodeState::Skipped, "non-matching route skipped");
        assert_eq!(rd.state, NodeState::Running, "default branch activated");
    }

    /// The branch decision is memoized: once branch 0 is chosen and running,
    /// a context change that would now select the default must NOT skip the
    /// running branch and start another.
    #[tokio::test]
    async fn router_decision_is_memoized_across_ticks() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let r0a = mk_node_rt(
            Some(parent.id),
            "r0a",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let r0b = mk_node_rt(
            Some(parent.id),
            "r0b",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let rd = mk_node_rt(
            Some(parent.id),
            "rd",
            BlockType::Step,
            NodeState::Pending,
            Some(1),
            inst_id,
        );
        let (s, tree) = setup_rt(
            vec![parent.clone(), r0a.clone(), r0b.clone(), rd.clone()],
            inst_id,
        )
        .await;
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![Route {
                condition: "x == 1".into(),
                blocks: vec![],
            }],
            default: Some(vec![]),
        };
        let handlers = HandlerRegistry::new();
        let ctx = |x: i64| ExecutionContext {
            data: json!({ "x": x }),
            ..Default::default()
        };

        // Tick 1: x == 1 → branch 0.
        execute_router(
            &s,
            &handlers,
            &mk_instance_rt(inst_id, ctx(1)),
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        // The running branch completes its first block and flips `x`.
        s.update_node_state(r0a.id, NodeState::Completed)
            .await
            .unwrap();
        let tree = s.get_execution_tree(inst_id).await.unwrap();

        // Tick 2: x == 0 would now pick the default — must be ignored.
        execute_router(
            &s,
            &handlers,
            &mk_instance_rt(inst_id, ctx(0)),
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        let state = |id| after.iter().find(|n| n.id == id).unwrap().state;
        assert_eq!(state(r0b.id), NodeState::Running, "chosen branch continues");
        assert_eq!(state(rd.id), NodeState::Skipped, "default never starts");
        assert_eq!(state(parent.id), NodeState::Running);
    }

    /// Run-past-failure: a failed block in the selected branch stops the
    /// branch and fails the router; later blocks never start.
    #[tokio::test]
    async fn router_failed_block_stops_branch() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let a = mk_node_rt(
            Some(parent.id),
            "a",
            BlockType::Step,
            NodeState::Failed,
            Some(0),
            inst_id,
        );
        let b = mk_node_rt(
            Some(parent.id),
            "b",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let (s, tree) = setup_rt(vec![parent.clone(), a.clone(), b.clone()], inst_id).await;
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![],
            }],
            default: None,
        };
        execute_router(
            &s,
            &HandlerRegistry::new(),
            &mk_instance_rt(inst_id, ExecutionContext::default()),
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        let state = |id| after.iter().find(|n| n.id == id).unwrap().state;
        assert_eq!(state(parent.id), NodeState::Failed);
        assert_eq!(state(b.id), NodeState::Skipped, "successor must never run");
    }

    // RT2: First matching route wins, later matching routes are skipped.
    #[tokio::test]
    async fn router_first_match_wins() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let parent_id = parent.id;
        let r0 = mk_node_rt(
            Some(parent_id),
            "r0",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let r1 = mk_node_rt(
            Some(parent_id),
            "r1",
            BlockType::Step,
            NodeState::Pending,
            Some(1),
            inst_id,
        );
        let (s, tree) = setup_rt(vec![parent.clone(), r0.clone(), r1.clone()], inst_id).await;
        let inst = mk_instance_rt(
            inst_id,
            ExecutionContext {
                data: json!({"x": 1}),
                ..Default::default()
            },
        );
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![
                Route {
                    condition: "x == 1".into(),
                    blocks: vec![],
                },
                Route {
                    condition: "x == 1".into(),
                    blocks: vec![],
                },
            ],
            default: None,
        };
        let handlers = HandlerRegistry::new();
        execute_router(
            &s,
            &handlers,
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            after.iter().find(|n| n.id == r0.id).unwrap().state,
            NodeState::Running
        );
        assert_eq!(
            after.iter().find(|n| n.id == r1.id).unwrap().state,
            NodeState::Skipped
        );
    }

    // RT3: No match and no default → router auto-completes (no branch_children to wait on).
    #[tokio::test]
    async fn router_no_match_no_default_completes() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let parent_id = parent.id;
        let r0 = mk_node_rt(
            Some(parent_id),
            "r0",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let (s, tree) = setup_rt(vec![parent.clone(), r0.clone()], inst_id).await;
        let inst = mk_instance_rt(inst_id, ExecutionContext::default());
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![Route {
                condition: "never".into(),
                blocks: vec![],
            }],
            default: None,
        };
        let handlers = HandlerRegistry::new();
        execute_router(
            &s,
            &handlers,
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        let p = after.iter().find(|n| n.id == parent_id).unwrap();
        assert_eq!(
            p.state,
            NodeState::Completed,
            "no-match no-default router completes"
        );
        let r0_after = after.iter().find(|n| n.id == r0.id).unwrap();
        assert_eq!(r0_after.state, NodeState::Skipped);
    }

    // RT4: Route condition referencing prior block output selects correctly.
    #[tokio::test]
    async fn router_condition_reads_block_output() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let parent_id = parent.id;
        let r0 = mk_node_rt(
            Some(parent_id),
            "r0",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let r1 = mk_node_rt(
            Some(parent_id),
            "r1",
            BlockType::Step,
            NodeState::Pending,
            Some(1),
            inst_id,
        );
        let (s, tree) = setup_rt(vec![parent.clone(), r0.clone(), r1.clone()], inst_id).await;
        // Seed a prior output.
        let bo = orch8_types::output::BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: inst_id,
            block_id: BlockId::new("prev"),
            output: json!({"val": 7}),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: Utc::now(),
        };
        s.save_block_output(&bo).await.unwrap();
        let inst = mk_instance_rt(inst_id, ExecutionContext::default());
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![
                Route {
                    condition: "outputs.prev.val > 100".into(),
                    blocks: vec![],
                },
                Route {
                    condition: "outputs.prev.val < 10".into(),
                    blocks: vec![],
                },
            ],
            default: None,
        };
        let handlers = HandlerRegistry::new();
        execute_router(
            &s,
            &handlers,
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            after.iter().find(|n| n.id == r0.id).unwrap().state,
            NodeState::Skipped
        );
        assert_eq!(
            after.iter().find(|n| n.id == r1.id).unwrap().state,
            NodeState::Running
        );
    }

    // RT5: Selected branch with no children auto-completes router.
    #[tokio::test]
    async fn router_empty_selected_branch_completes_router() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let (s, tree) = setup_rt(vec![parent.clone()], inst_id).await;
        let inst = mk_instance_rt(
            inst_id,
            ExecutionContext {
                data: json!({"x": 1}),
                ..Default::default()
            },
        );
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![Route {
                condition: "x == 1".into(),
                blocks: vec![],
            }],
            default: None,
        };
        let handlers = HandlerRegistry::new();
        execute_router(
            &s,
            &handlers,
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        let p = after.iter().find(|n| n.id == parent.id).unwrap();
        assert_eq!(p.state, NodeState::Completed);
    }

    // RT6: Failed branch child fails the router.
    #[tokio::test]
    async fn router_fails_when_branch_child_failed() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let parent_id = parent.id;
        let r0 = mk_node_rt(
            Some(parent_id),
            "r0",
            BlockType::Step,
            NodeState::Failed,
            Some(0),
            inst_id,
        );
        let (s, tree) = setup_rt(vec![parent.clone(), r0.clone()], inst_id).await;
        let inst = mk_instance_rt(
            inst_id,
            ExecutionContext {
                data: json!({"x": 1}),
                ..Default::default()
            },
        );
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![Route {
                condition: "x == 1".into(),
                blocks: vec![],
            }],
            default: None,
        };
        let handlers = HandlerRegistry::new();
        execute_router(
            &s,
            &handlers,
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        let p = after.iter().find(|n| n.id == parent_id).unwrap();
        assert_eq!(p.state, NodeState::Failed, "router reflects branch failure");
    }

    // RT7: Already-terminal non-selected children are preserved (not overwritten to Skipped).
    #[tokio::test]
    async fn router_preserves_terminal_non_selected_branch() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let parent_id = parent.id;
        let r0_completed = mk_node_rt(
            Some(parent_id),
            "r0",
            BlockType::Step,
            NodeState::Completed,
            Some(0),
            inst_id,
        );
        let r1_selected = mk_node_rt(
            Some(parent_id),
            "r1",
            BlockType::Step,
            NodeState::Pending,
            Some(1),
            inst_id,
        );
        let (s, tree) = setup_rt(
            vec![parent.clone(), r0_completed.clone(), r1_selected.clone()],
            inst_id,
        )
        .await;
        let inst = mk_instance_rt(
            inst_id,
            ExecutionContext {
                data: json!({"x": 2}),
                ..Default::default()
            },
        );
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![
                Route {
                    condition: "x == 1".into(),
                    blocks: vec![],
                },
                Route {
                    condition: "x == 2".into(),
                    blocks: vec![],
                },
            ],
            default: None,
        };
        let handlers = HandlerRegistry::new();
        execute_router(
            &s,
            &handlers,
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        // Previously-Completed child untouched.
        assert_eq!(
            after
                .iter()
                .find(|n| n.id == r0_completed.id)
                .unwrap()
                .state,
            NodeState::Completed
        );
        assert_eq!(
            after.iter().find(|n| n.id == r1_selected.id).unwrap().state,
            NodeState::Running
        );
    }

    // RT8: Skipping a route recursively skips nested non-terminal descendants.
    #[tokio::test]
    async fn router_skips_entire_non_selected_subtree() {
        let inst_id = InstanceId::new();
        let parent = mk_node_rt(
            None,
            "r",
            BlockType::Router,
            NodeState::Running,
            None,
            inst_id,
        );
        let selected = mk_node_rt(
            Some(parent.id),
            "selected",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
            inst_id,
        );
        let skipped_root = mk_node_rt(
            Some(parent.id),
            "skipped_root",
            BlockType::Parallel,
            NodeState::Pending,
            Some(1),
            inst_id,
        );
        let completed_descendant = mk_node_rt(
            Some(skipped_root.id),
            "already_done",
            BlockType::Step,
            NodeState::Completed,
            None,
            inst_id,
        );
        let pending_descendant = mk_node_rt(
            Some(completed_descendant.id),
            "never_runs",
            BlockType::Step,
            NodeState::Pending,
            None,
            inst_id,
        );
        let (s, tree) = setup_rt(
            vec![
                parent.clone(),
                selected.clone(),
                skipped_root.clone(),
                completed_descendant.clone(),
                pending_descendant.clone(),
            ],
            inst_id,
        )
        .await;
        let inst = mk_instance_rt(
            inst_id,
            ExecutionContext {
                data: json!({"take_selected": true}),
                ..Default::default()
            },
        );
        let router = RouterDef {
            id: BlockId::new("r"),
            routes: vec![
                Route {
                    condition: "take_selected".into(),
                    blocks: vec![],
                },
                Route {
                    condition: "false".into(),
                    blocks: vec![],
                },
            ],
            default: None,
        };

        execute_router(
            &s,
            &HandlerRegistry::new(),
            &inst,
            &parent,
            &router,
            &tree,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        let state = |id| after.iter().find(|node| node.id == id).unwrap().state;
        assert_eq!(state(selected.id), NodeState::Running);
        assert_eq!(state(skipped_root.id), NodeState::Skipped);
        assert_eq!(state(completed_descendant.id), NodeState::Completed);
        assert_eq!(state(pending_descendant.id), NodeState::Skipped);
    }

    /// A missing externalized payload leaves the marker in place. The router
    /// must not panic and must fall through to the default branch, letting
    /// downstream code surface the broken ref.
    #[tokio::test]
    async fn router_broken_ref_falls_through_to_default() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let router = RouterDef {
            id: BlockId::new("r"),
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
        let resolved = externalized::resolve_context_markers(&storage, InstanceId::new(), ctx)
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
