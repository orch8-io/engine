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

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::execution::BlockType;
    use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority};
    use orch8_types::sequence::RaceSemantics;
    use orch8_types::worker::{WorkerTask, WorkerTaskState};
    use serde_json::json;
    use uuid::Uuid;

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
            name: "race_test".into(),
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

    fn race_def(id: &str) -> RaceDef {
        RaceDef {
            id: BlockId(id.into()),
            branches: vec![],
            semantics: RaceSemantics::default(),
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

    // R1: Race with no children completes immediately.
    #[tokio::test]
    async fn r1_empty_children_completes_immediately() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let (s, tree) = setup(vec![race.clone()], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "race").state, NodeState::Completed);
    }

    // R2: All pending children are activated so they can race.
    #[tokio::test]
    async fn r2_all_pending_children_activated() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(race.id),
            Some(0),
            NodeState::Pending,
        );
        let b = mk_node(
            inst_id,
            "b",
            BlockType::Step,
            Some(race.id),
            Some(1),
            NodeState::Pending,
        );
        let c = mk_node(
            inst_id,
            "c",
            BlockType::Step,
            Some(race.id),
            Some(2),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![race.clone(), a, b, c], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        for block in ["a", "b", "c"] {
            assert_eq!(node_by_block(&after, block).state, NodeState::Running);
        }
        // Race still running until a winner emerges.
        assert_eq!(node_by_block(&after, "race").state, NodeState::Running);
    }

    // R3: On a winner (first Completed), all non-terminal branches are Cancelled
    // and the race completes.
    #[tokio::test]
    async fn r3_winner_cancels_siblings_and_completes() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let winner = mk_node(
            inst_id,
            "winner",
            BlockType::Step,
            Some(race.id),
            Some(0),
            NodeState::Completed,
        );
        let still_running = mk_node(
            inst_id,
            "busy",
            BlockType::Step,
            Some(race.id),
            Some(1),
            NodeState::Running,
        );
        let (s, tree) = setup(vec![race.clone(), winner, still_running], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "race").state, NodeState::Completed);
        assert_eq!(node_by_block(&after, "winner").state, NodeState::Completed);
        assert_eq!(
            node_by_block(&after, "busy").state,
            NodeState::Cancelled,
            "losing branch must be cancelled"
        );
    }

    // R4: A Waiting child (dispatched to external worker) must have its pending
    // worker_tasks row cancelled before the node state flips to Cancelled.
    #[tokio::test]
    async fn r4_waiting_sibling_cancels_worker_task() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let winner = mk_node(
            inst_id,
            "winner",
            BlockType::Step,
            Some(race.id),
            Some(0),
            NodeState::Completed,
        );
        let waiting = mk_node(
            inst_id,
            "waiting",
            BlockType::Step,
            Some(race.id),
            Some(1),
            NodeState::Waiting,
        );
        let (s, tree) = setup(vec![race.clone(), winner, waiting.clone()], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        // Simulate a pending worker_tasks row for the waiting branch.
        let wt = WorkerTask {
            id: Uuid::now_v7(),
            instance_id: inst_id,
            block_id: BlockId("waiting".into()),
            handler_name: "ext".into(),
            queue_name: None,
            params: json!({}),
            context: json!({}),
            attempt: 1,
            timeout_ms: None,
            state: WorkerTaskState::Pending,
            worker_id: None,
            claimed_at: None,
            heartbeat_at: None,
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: chrono::Utc::now(),
        };
        s.create_worker_task(&wt).await.unwrap();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "race").state, NodeState::Completed);
        assert_eq!(node_by_block(&after, "waiting").state, NodeState::Cancelled);

        // The previously-pending worker task row must have been cancelled
        // (not claimable any more).
        let claimed = s.claim_worker_tasks("ext", "any_worker", 1).await.unwrap();
        assert!(
            claimed.is_empty(),
            "cancel_worker_tasks_for_block must prevent further claims"
        );
    }

    // R5: All branches terminal and none completed → race fails.
    #[tokio::test]
    async fn r5_all_failed_fails_race() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let f0 = mk_node(
            inst_id,
            "f0",
            BlockType::Step,
            Some(race.id),
            Some(0),
            NodeState::Failed,
        );
        let f1 = mk_node(
            inst_id,
            "f1",
            BlockType::Step,
            Some(race.id),
            Some(1),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![race.clone(), f0, f1], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "race").state, NodeState::Failed);
    }

    // R6: Mix of Failed + Cancelled terminals (no winner) → race fails.
    #[tokio::test]
    async fn r6_mix_failed_cancelled_with_no_winner_fails() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let failed = mk_node(
            inst_id,
            "failed",
            BlockType::Step,
            Some(race.id),
            Some(0),
            NodeState::Failed,
        );
        let cancelled = mk_node(
            inst_id,
            "cancelled",
            BlockType::Step,
            Some(race.id),
            Some(1),
            NodeState::Cancelled,
        );
        let (s, tree) = setup(vec![race.clone(), failed, cancelled], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "race").state,
            NodeState::Failed,
            "no winner and all terminal → race must fail"
        );
    }

    // R7: Still racing — some Running, none Completed, none all-terminal → race
    // stays Running (returns Ok without state change on the race node).
    #[tokio::test]
    async fn r7_race_in_progress_no_state_change() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let a = mk_node(
            inst_id,
            "a",
            BlockType::Step,
            Some(race.id),
            Some(0),
            NodeState::Running,
        );
        let b = mk_node(
            inst_id,
            "b",
            BlockType::Step,
            Some(race.id),
            Some(1),
            NodeState::Running,
        );
        let (s, tree) = setup(vec![race.clone(), a, b], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "race").state, NodeState::Running);
        for block in ["a", "b"] {
            assert_eq!(node_by_block(&after, block).state, NodeState::Running);
        }
    }

    // R8: When a winner is found, already-terminal siblings (e.g. previously
    // Failed) stay in their terminal state — they are NOT re-flipped to Cancelled.
    #[tokio::test]
    async fn r8_terminal_siblings_not_recancelled_when_winner_found() {
        let inst_id = InstanceId::new();
        let race = mk_node(
            inst_id,
            "race",
            BlockType::Race,
            None,
            None,
            NodeState::Running,
        );
        let winner = mk_node(
            inst_id,
            "winner",
            BlockType::Step,
            Some(race.id),
            Some(0),
            NodeState::Completed,
        );
        let prior_fail = mk_node(
            inst_id,
            "prior_fail",
            BlockType::Step,
            Some(race.id),
            Some(1),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![race.clone(), winner, prior_fail], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let race_node = node_by_block(&tree, "race").clone();

        execute_race(&s, &registry, &inst, &race_node, &race_def("race"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "race").state, NodeState::Completed);
        assert_eq!(
            node_by_block(&after, "prior_fail").state,
            NodeState::Failed,
            "terminal sibling must retain its Failed state, not be re-cancelled",
        );
    }
}
