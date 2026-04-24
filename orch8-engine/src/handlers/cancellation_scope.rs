use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::CancellationScopeDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a cancellation scope block.
///
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
            evaluator::fail_node(storage, node.id).await?;
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
        state: NodeState,
    ) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId(block_id.into()),
            parent_id,
            block_type,
            branch_index: None,
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
            name: "cs_test".into(),
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

    fn scope_def(id: &str) -> CancellationScopeDef {
        CancellationScopeDef {
            id: BlockId(id.into()),
            blocks: vec![],
        }
    }

    async fn setup(
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

    // CS1: Empty scope completes immediately.
    #[tokio::test]
    async fn cs1_empty_scope_completes() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let (s, tree) = setup(vec![sc.clone()], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Completed);
    }

    // CS2: First pending child activated, later pending children stay Pending
    // (sequential execution).
    #[tokio::test]
    async fn cs2_sequential_execution_activates_only_first_pending() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(sc.id),
            NodeState::Pending,
        );
        let b = mk_node(
            inst_id,
            "b",
            BlockType::Step,
            Some(sc.id),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![sc.clone(), a, b], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "a").state, NodeState::Running);
        assert_eq!(node_by_block(&after, "b").state, NodeState::Pending);
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Running);
    }

    // CS3: When all children complete successfully, scope completes.
    #[tokio::test]
    async fn cs3_all_completed_children_completes_scope() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(sc.id),
            NodeState::Completed,
        );
        let b = mk_node(
            inst_id,
            "b",
            BlockType::Step,
            Some(sc.id),
            NodeState::Completed,
        );
        let (s, tree) = setup(vec![sc.clone(), a, b], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Completed);
    }

    // CS4: Any failed child → scope fails.
    #[tokio::test]
    async fn cs4_failed_child_fails_scope() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let ok = mk_node(
            inst_id,
            "ok",
            BlockType::Step,
            Some(sc.id),
            NodeState::Completed,
        );
        let bad = mk_node(
            inst_id,
            "bad",
            BlockType::Step,
            Some(sc.id),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![sc.clone(), ok, bad], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Failed);
    }

    // CS5: Child in Running state keeps scope running, no state mutation.
    #[tokio::test]
    async fn cs5_running_child_holds_scope() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let busy = mk_node(
            inst_id,
            "busy",
            BlockType::Step,
            Some(sc.id),
            NodeState::Running,
        );
        let (s, tree) = setup(vec![sc.clone(), busy], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "busy").state, NodeState::Running);
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Running);
    }

    // CS6: Child in Waiting state keeps scope running (no activation).
    #[tokio::test]
    async fn cs6_waiting_child_holds_scope() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let dispatched = mk_node(
            inst_id,
            "dispatched",
            BlockType::Step,
            Some(sc.id),
            NodeState::Waiting,
        );
        let (s, tree) = setup(vec![sc.clone(), dispatched], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "dispatched").state,
            NodeState::Waiting
        );
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Running);
    }

    // CS7: Skipped or Cancelled children (all terminal, no failures) still
    // complete the scope.
    #[tokio::test]
    async fn cs7_skipped_cancelled_terminal_completes_scope() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let sk = mk_node(
            inst_id,
            "sk",
            BlockType::Step,
            Some(sc.id),
            NodeState::Skipped,
        );
        let cn = mk_node(
            inst_id,
            "cn",
            BlockType::Step,
            Some(sc.id),
            NodeState::Cancelled,
        );
        let (s, tree) = setup(vec![sc.clone(), sk, cn], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Completed);
    }

    // CS8: Advance path — first child Completed, activate next Pending child.
    #[tokio::test]
    async fn cs8_advances_past_completed_to_next_pending() {
        let inst_id = InstanceId::new();
        let sc = mk_node(
            inst_id,
            "sc",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        );
        let done = mk_node(
            inst_id,
            "done",
            BlockType::Step,
            Some(sc.id),
            NodeState::Completed,
        );
        let next = mk_node(
            inst_id,
            "next",
            BlockType::Step,
            Some(sc.id),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![sc.clone(), done, next], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let sc_node = node_by_block(&tree, "sc").clone();

        execute_cancellation_scope(&s, &registry, &inst, &sc_node, &scope_def("sc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "done").state, NodeState::Completed);
        assert_eq!(node_by_block(&after, "next").state, NodeState::Running);
        assert_eq!(node_by_block(&after, "sc").state, NodeState::Running);
    }
}
