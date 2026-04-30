use std::collections::BTreeMap;

use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::ParallelDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a parallel block: all branches run concurrently, but within each
/// branch, blocks execute sequentially.
///
/// ## Per-branch cursor semantics
///
/// A branch is an ordered list of blocks. At most one block per branch is
/// active at a time — just like a top-level sequence. Multiple branches run
/// concurrently with each other.
///
/// Each direct child of the parallel node is tagged with `branch_index`
/// (set by [`evaluator::build_nodes`]). On every tick this handler:
///   1. Groups children by `branch_index`, preserving source order within
///      each group.
///   2. For each branch, finds the first non-terminal node (the branch's
///      "cursor"). If that node is Pending, it is activated (transitioned
///      to Running) so the scheduler can dispatch it. If Running/Waiting,
///      the branch is in progress and nothing is done.
///   3. Once every branch has drained (all nodes terminal), the parallel
///      node itself transitions: Completed if every branch completed,
///      Failed if any branch had a failed node.
///
/// Prior behaviour (pre-fix) flattened all branches and activated every
/// pending child at once, producing Temporal-style "fork all" semantics
/// rather than "N concurrent sequences". That broke nested composites,
/// per-branch cancellation, and concurrency guards.
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

    // Group by branch_index. BTreeMap keeps branches in declaration order,
    // which makes log output deterministic. Within each group, children
    // are preserved in the insertion order from `children_of` — which
    // mirrors source order because `build_nodes` appends in
    // block-sequence order.
    let mut branches: BTreeMap<i16, Vec<&ExecutionNode>> = BTreeMap::new();
    for c in &children {
        // Direct children of a parallel should always be tagged by
        // build_nodes. Any stray untagged child is treated as
        // branch 0 defensively.
        let idx = c.branch_index.unwrap_or(0);
        branches.entry(idx).or_default().push(*c);
    }

    let mut all_branches_done = true;
    let mut any_branch_failed = false;

    for (branch_idx, branch_nodes) in &branches {
        // Cursor = first non-terminal node in this branch.
        let cursor = branch_nodes.iter().find(|n| {
            !matches!(
                n.state,
                NodeState::Completed
                    | NodeState::Failed
                    | NodeState::Cancelled
                    | NodeState::Skipped
            )
        });

        match cursor {
            Some(n) if n.state == NodeState::Pending => {
                // Activate the branch's next step. Subsequent steps in
                // this branch stay Pending until this one is terminal.
                storage.update_node_state(n.id, NodeState::Running).await?;
                all_branches_done = false;
                debug!(
                    instance_id = %instance.id,
                    block_id = %par_def.id,
                    branch = branch_idx,
                    cursor_block = %n.block_id,
                    "parallel: activating branch cursor"
                );
            }
            Some(_) => {
                // Cursor is Running/Waiting — branch is making progress.
                all_branches_done = false;
            }
            None => {
                // Every node in this branch is terminal.
                if branch_nodes
                    .iter()
                    .any(|n| matches!(n.state, NodeState::Failed))
                {
                    any_branch_failed = true;
                }
            }
        }
    }

    if all_branches_done {
        if any_branch_failed {
            evaluator::fail_node(storage, node.id).await?;
            debug!(
                instance_id = %instance.id,
                block_id = %par_def.id,
                "parallel block failed — one or more branches failed"
            );
        } else {
            evaluator::complete_node(storage, node.id).await?;
            debug!(
                instance_id = %instance.id,
                block_id = %par_def.id,
                "parallel block completed — all branches succeeded"
            );
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::execution::BlockType;
    use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority};
    use serde_json::json;

    fn mk_node(
        instance_id: InstanceId,
        block_id: &str,
        block_type: BlockType,
        parent_id: Option<ExecutionNodeId>,
        branch_index: Option<i16>,
        state: NodeState,
    ) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId(block_id.into()),
            parent_id,
            block_type,
            branch_index,
            state,
            started_at: None,
            completed_at: None,
        }
    }

    async fn seed_instance(s: &SqliteStorage, inst: InstanceId) {
        use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
        let now = chrono::Utc::now();
        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            name: "par_test".into(),
            version: 1,
            deprecated: false,
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId("noop".into()),
                handler: "noop".into(),
                params: json!({}),
                delay: None,
                retry: None,
                timeout: None,
                rate_limit_key: None,
                send_window: None,
                context_access: None,
                cancellable: true,
                wait_for_input: None,
                queue_name: None,
                deadline: None,
                on_deadline_breach: None,
                fallback_handler: None,
                cache_key: None,
            }))],
            interceptors: None,
            created_at: now,
        };
        s.create_sequence(&seq).await.unwrap();
        let inst_row = TaskInstance {
            id: inst,
            sequence_id: seq.id,
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data: json!({}),
                config: json!({}),
                audit: vec![],
                runtime: RuntimeContext::default(),
            },
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        };
        s.create_instance(&inst_row).await.unwrap();
    }

    fn mk_instance(inst_id: InstanceId) -> TaskInstance {
        let now = chrono::Utc::now();
        TaskInstance {
            id: inst_id,
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
        }
    }

    fn par_def(id: &str) -> ParallelDef {
        ParallelDef {
            id: BlockId(id.into()),
            branches: vec![],
        }
    }

    async fn setup_with_nodes(
        nodes: Vec<ExecutionNode>,
        inst_id: InstanceId,
    ) -> (SqliteStorage, Vec<ExecutionNode>) {
        let s = SqliteStorage::in_memory().await.unwrap();
        seed_instance(&s, inst_id).await;
        s.create_execution_nodes_batch(&nodes).await.unwrap();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        (s, tree)
    }

    fn node_by_block<'a>(tree: &'a [ExecutionNode], block: &str) -> &'a ExecutionNode {
        tree.iter()
            .find(|n| n.block_id.0 == block)
            .unwrap_or_else(|| panic!("node not found: {block}"))
    }

    // P1: Parallel with no children completes immediately.
    #[tokio::test]
    async fn p1_empty_children_completes_immediately() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone()], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "par").state, NodeState::Completed);
    }

    // P2: Single pending cursor in a single branch is activated to Running.
    #[tokio::test]
    async fn p2_single_branch_activates_first_pending_cursor() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Pending,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), a.clone()], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "a").state, NodeState::Running);
        assert_eq!(
            node_by_block(&after, "par").state,
            NodeState::Running,
            "parallel must not complete while a branch is in progress"
        );
    }

    // P3: Multiple branches all activate their own cursor on the same tick.
    #[tokio::test]
    async fn p3_multiple_branches_activate_concurrently() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let b0 = mk_node(
            inst_id,
            "b0",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Pending,
        );
        let b1 = mk_node(
            inst_id,
            "b1",
            BlockType::Step,
            Some(par.id),
            Some(1),
            NodeState::Pending,
        );
        let b2 = mk_node(
            inst_id,
            "b2",
            BlockType::Step,
            Some(par.id),
            Some(2),
            NodeState::Pending,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), b0, b1, b2], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        for block in ["b0", "b1", "b2"] {
            assert_eq!(
                node_by_block(&after, block).state,
                NodeState::Running,
                "branch {block} cursor must activate",
            );
        }
    }

    // P4: Within a branch, only the first non-terminal block is activated.
    // Later blocks in the same branch stay Pending.
    #[tokio::test]
    async fn p4_within_branch_only_first_pending_activates() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        // Two blocks in branch 0 — both pending. Only first should run.
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Pending,
        );
        let b = mk_node(
            inst_id,
            "b",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Pending,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), a, b], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "a").state, NodeState::Running);
        assert_eq!(
            node_by_block(&after, "b").state,
            NodeState::Pending,
            "second block in same branch must stay Pending",
        );
    }

    // P5: A branch whose cursor is Running/Waiting is treated as in-progress —
    // no state change, and parallel does not complete yet.
    #[tokio::test]
    async fn p5_running_cursor_preserves_branch_progress() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Running,
        );
        let b = mk_node(
            inst_id,
            "b",
            BlockType::Step,
            Some(par.id),
            Some(1),
            NodeState::Waiting,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), a, b], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "a").state, NodeState::Running);
        assert_eq!(node_by_block(&after, "b").state, NodeState::Waiting);
        assert_eq!(node_by_block(&after, "par").state, NodeState::Running);
    }

    // P6: When every branch drains with Completed cursors, the parallel completes.
    #[tokio::test]
    async fn p6_all_branches_completed_triggers_completion() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let b0 = mk_node(
            inst_id,
            "b0",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Completed,
        );
        let b1 = mk_node(
            inst_id,
            "b1",
            BlockType::Step,
            Some(par.id),
            Some(1),
            NodeState::Completed,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), b0, b1], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "par").state, NodeState::Completed);
    }

    // P7: One failed branch (with all others complete) fails the parallel.
    #[tokio::test]
    async fn p7_single_branch_failure_fails_parallel() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let good = mk_node(
            inst_id,
            "good",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Completed,
        );
        let bad = mk_node(
            inst_id,
            "bad",
            BlockType::Step,
            Some(par.id),
            Some(1),
            NodeState::Failed,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), good, bad], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "par").state, NodeState::Failed);
    }

    // P8: Multiple simultaneous branch failures → parallel fails (still fails once).
    #[tokio::test]
    async fn p8_multiple_branch_failures_fail_parallel() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let f0 = mk_node(
            inst_id,
            "f0",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Failed,
        );
        let f1 = mk_node(
            inst_id,
            "f1",
            BlockType::Step,
            Some(par.id),
            Some(1),
            NodeState::Failed,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), f0, f1], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "par").state, NodeState::Failed);
    }

    // P9: An untagged child (branch_index=None) is defensively treated as branch 0.
    #[tokio::test]
    async fn p9_untagged_child_defaults_to_branch_zero() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        // Two untagged children → must be treated as a single branch (branch 0).
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(par.id),
            None,
            NodeState::Pending,
        );
        let b = mk_node(
            inst_id,
            "b",
            BlockType::Step,
            Some(par.id),
            None,
            NodeState::Pending,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), a, b], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        // If defaulting worked, "a" ran and "b" stayed pending (cursor semantics
        // in branch 0). If the code had activated both, we'd have two Running.
        assert_eq!(node_by_block(&after, "a").state, NodeState::Running);
        assert_eq!(node_by_block(&after, "b").state, NodeState::Pending);
    }

    // P10: Terminal but non-Failed states (Cancelled, Skipped) do not trigger
    // parallel failure if no other branch failed.
    #[tokio::test]
    async fn p10_cancelled_and_skipped_branches_do_not_fail_parallel() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let done = mk_node(
            inst_id,
            "done",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Completed,
        );
        let cancelled = mk_node(
            inst_id,
            "cancelled",
            BlockType::Step,
            Some(par.id),
            Some(1),
            NodeState::Cancelled,
        );
        let skipped = mk_node(
            inst_id,
            "skipped",
            BlockType::Step,
            Some(par.id),
            Some(2),
            NodeState::Skipped,
        );
        let (s, tree) =
            setup_with_nodes(vec![par.clone(), done, cancelled, skipped], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "par").state,
            NodeState::Completed,
            "non-Failed terminal states must not fail the parallel",
        );
    }

    // P11: Cursor correctly skips past Completed blocks to the first non-terminal
    // one in the same branch.
    #[tokio::test]
    async fn p11_cursor_skips_past_completed_within_branch() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        // Branch 0: first step already done, second is pending → activate second.
        let done = mk_node(
            inst_id,
            "done",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Completed,
        );
        let pend = mk_node(
            inst_id,
            "pend",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Pending,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), done, pend], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "done").state, NodeState::Completed);
        assert_eq!(node_by_block(&after, "pend").state, NodeState::Running);
    }

    // P12: Mixed branches — one complete, one still running — parallel stays Running.
    #[tokio::test]
    async fn p12_mixed_complete_and_running_branches_keeps_parallel_running() {
        let inst_id = InstanceId::new();
        let par = mk_node(
            inst_id,
            "par",
            BlockType::Parallel,
            None,
            None,
            NodeState::Running,
        );
        let done = mk_node(
            inst_id,
            "done",
            BlockType::Step,
            Some(par.id),
            Some(0),
            NodeState::Completed,
        );
        let busy = mk_node(
            inst_id,
            "busy",
            BlockType::Step,
            Some(par.id),
            Some(1),
            NodeState::Running,
        );
        let (s, tree) = setup_with_nodes(vec![par.clone(), done, busy], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let par_node = node_by_block(&tree, "par").clone();

        execute_parallel(&s, &registry, &inst, &par_node, &par_def("par"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "par").state,
            NodeState::Running,
            "parallel cannot complete while any branch has a live cursor",
        );
    }
}
