use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::ExecutionNode;
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::SagaDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Branch index for a saga step's forward action.
fn action_branch(step_index: usize) -> Option<i16> {
    i16::try_from(step_index * 2).ok()
}

/// Branch index for a saga step's compensating action.
fn compensation_branch(step_index: usize) -> Option<i16> {
    i16::try_from(step_index * 2 + 1).ok()
}

/// Execute a saga block.
///
/// Steps run sequentially. If every step's action completes, the saga
/// succeeds. If a step's action fails, the compensating actions for every
/// already-completed step run in reverse order (LIFO), best-effort — a
/// compensation failure does not stop the rollback, it just gets logged.
/// Once rollback finishes, the saga node always fails: the original
/// action failure is never silently absorbed.
///
/// Returns `true` if more work.
pub async fn execute_saga(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    saga_def: &SagaDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let n = saga_def.steps.len();

    // Phase 1: run actions sequentially, step 0 first.
    let mut failed_step: Option<usize> = None;
    for i in 0..n {
        let action_children = evaluator::children_of(tree, node.id, action_branch(i));
        if !evaluator::all_terminal(&action_children) {
            evaluator::activate_first_pending_child(storage, &action_children).await?;
            return Ok(true);
        }
        if evaluator::any_failed(&action_children) {
            failed_step = Some(i);
            break;
        }
    }

    let Some(failed_step) = failed_step else {
        // Every step's action completed — saga succeeds, no compensation runs.
        evaluator::complete_node(storage, node.id).await?;
        debug!(
            instance_id = %instance.id,
            block_id = %saga_def.id,
            "saga completed, all steps succeeded"
        );
        return Ok(true);
    };

    // Phase 2: roll back completed steps in reverse order.
    let error_ctx = serde_json::json!({
        "failed_step": saga_def.steps[failed_step].id.as_str(),
        "source": "saga",
        "block_id": saga_def.id.as_str(),
    });
    storage
        .merge_context_data(instance.id, "_error", &error_ctx)
        .await
        .map_err(|e| {
            warn!(
                instance_id = %instance.id,
                error = %e,
                "failed to inject error context for saga compensation"
            );
            EngineError::from(e)
        })?;

    for i in (0..failed_step).rev() {
        if saga_def.steps[i].compensation.is_none() {
            continue;
        }
        let comp_children = evaluator::children_of(tree, node.id, compensation_branch(i));
        if !evaluator::all_terminal(&comp_children) {
            evaluator::activate_first_pending_child(storage, &comp_children).await?;
            return Ok(true);
        }
        if evaluator::any_failed(&comp_children) {
            warn!(
                instance_id = %instance.id,
                block_id = %saga_def.id,
                step_id = %saga_def.steps[i].id,
                "saga compensation failed, continuing rollback best-effort"
            );
        }
    }

    // All compensations attempted (or skipped where none was declared).
    // The saga itself always fails — a rolled-back saga is still a failure.
    evaluator::fail_node(storage, node.id).await?;
    debug!(
        instance_id = %instance.id,
        block_id = %saga_def.id,
        failed_step = failed_step,
        "saga rolled back and failed"
    );

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::{ExecutionTreeStore, InstanceStore, SequenceStore, sqlite::SqliteStorage};
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::execution::{BlockType, NodeState};
    use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority};
    use orch8_types::sequence::{BlockDefinition, SagaStep, StepDef};
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
            block_id: BlockId::new(block_id),
            parent_id,
            block_type,
            branch_index,
            state,
            started_at: None,
            completed_at: None,
        }
    }

    fn mk_step_def(id: &str) -> StepDef {
        StepDef {
            id: BlockId::new(id),
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
            output_schema: None,
            when: None,
        }
    }

    async fn seed_instance(s: &SqliteStorage, inst: InstanceId) {
        let now = chrono::Utc::now();
        let seq = orch8_types::sequence::SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            name: "saga_test".into(),
            version: 1,
            deprecated: false,
            status: orch8_types::sequence::SequenceStatus::default(),
            blocks: vec![BlockDefinition::Step(Box::new(mk_step_def("noop")))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: now,
        };
        s.create_sequence(&seq).await.unwrap();
        let inst_row = TaskInstance {
            id: inst,
            sequence_id: seq.id,
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
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
            budget: None,
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
            .find(|n| n.block_id.as_str() == block)
            .unwrap_or_else(|| panic!("node not found: {block}"))
    }

    fn saga_step(id: &str, action_id: &str, compensation_id: Option<&str>) -> SagaStep {
        SagaStep {
            id: BlockId::new(id),
            action: Box::new(BlockDefinition::Step(Box::new(mk_step_def(action_id)))),
            compensation: compensation_id
                .map(|c| Box::new(BlockDefinition::Step(Box::new(mk_step_def(c))))),
        }
    }

    fn saga_def(id: &str, steps: Vec<SagaStep>) -> SagaDef {
        SagaDef {
            id: BlockId::new(id),
            steps,
        }
    }

    // S1: All action steps succeed → saga completes, no compensation runs.
    #[tokio::test]
    async fn s1_all_actions_succeed_completes() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let a0 = mk_node(
            inst_id,
            "a0",
            BlockType::Step,
            Some(saga.id),
            Some(0),
            NodeState::Completed,
        );
        let a1 = mk_node(
            inst_id,
            "a1",
            BlockType::Step,
            Some(saga.id),
            Some(2),
            NodeState::Completed,
        );
        let (s, tree) = setup(vec![saga.clone(), a0, a1], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def(
            "saga",
            vec![saga_step("s0", "a0", None), saga_step("s1", "a1", None)],
        );
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "saga").state, NodeState::Completed);
    }

    // S2: First step's action fails, no completed steps to compensate →
    // saga fails immediately.
    #[tokio::test]
    async fn s2_first_step_fails_no_compensation_needed() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let a0 = mk_node(
            inst_id,
            "a0",
            BlockType::Step,
            Some(saga.id),
            Some(0),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![saga.clone(), a0], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def("saga", vec![saga_step("s0", "a0", Some("c0"))]);
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "saga").state, NodeState::Failed);
    }

    // S3: Step 0 succeeds, step 1 fails → step 0's compensation is activated.
    #[tokio::test]
    async fn s3_second_step_fails_activates_first_compensation() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let a0 = mk_node(
            inst_id,
            "a0",
            BlockType::Step,
            Some(saga.id),
            Some(0),
            NodeState::Completed,
        );
        let c0 = mk_node(
            inst_id,
            "c0",
            BlockType::Step,
            Some(saga.id),
            Some(1),
            NodeState::Pending,
        );
        let a1 = mk_node(
            inst_id,
            "a1",
            BlockType::Step,
            Some(saga.id),
            Some(2),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![saga.clone(), a0, c0, a1], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def(
            "saga",
            vec![
                saga_step("s0", "a0", Some("c0")),
                saga_step("s1", "a1", None),
            ],
        );
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        // Compensation activated (Pending -> Running), saga still in-flight.
        assert_eq!(node_by_block(&after, "c0").state, NodeState::Running);
        assert_eq!(node_by_block(&after, "saga").state, NodeState::Running);
    }

    // S4: Step 0 succeeds (no compensation declared), step 1 fails → saga
    // fails immediately since there's nothing to roll back.
    #[tokio::test]
    async fn s4_completed_step_without_compensation_is_skipped() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let a0 = mk_node(
            inst_id,
            "a0",
            BlockType::Step,
            Some(saga.id),
            Some(0),
            NodeState::Completed,
        );
        let a1 = mk_node(
            inst_id,
            "a1",
            BlockType::Step,
            Some(saga.id),
            Some(2),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![saga.clone(), a0, a1], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def(
            "saga",
            vec![saga_step("s0", "a0", None), saga_step("s1", "a1", None)],
        );
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "saga").state, NodeState::Failed);
    }

    // S5: Both compensations already terminal (one failed, one completed) →
    // saga fails once rollback finishes, regardless of compensation outcome.
    #[tokio::test]
    async fn s5_rollback_completes_saga_still_fails() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let a0 = mk_node(
            inst_id,
            "a0",
            BlockType::Step,
            Some(saga.id),
            Some(0),
            NodeState::Completed,
        );
        let c0 = mk_node(
            inst_id,
            "c0",
            BlockType::Step,
            Some(saga.id),
            Some(1),
            NodeState::Failed,
        );
        let a1 = mk_node(
            inst_id,
            "a1",
            BlockType::Step,
            Some(saga.id),
            Some(2),
            NodeState::Completed,
        );
        let c1 = mk_node(
            inst_id,
            "c1",
            BlockType::Step,
            Some(saga.id),
            Some(3),
            NodeState::Completed,
        );
        let a2 = mk_node(
            inst_id,
            "a2",
            BlockType::Step,
            Some(saga.id),
            Some(4),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![saga.clone(), a0, c0, a1, c1, a2], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def(
            "saga",
            vec![
                saga_step("s0", "a0", Some("c0")),
                saga_step("s1", "a1", Some("c1")),
                saga_step("s2", "a2", None),
            ],
        );
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "saga").state, NodeState::Failed);
    }

    // S6: Empty saga (no steps) → completes immediately.
    #[tokio::test]
    async fn s6_empty_saga_completes() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let (s, tree) = setup(vec![saga.clone()], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def("saga", vec![]);
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "saga").state, NodeState::Completed);
    }

    // S7: Error context is injected before compensation activates.
    #[tokio::test]
    async fn s7_error_context_injected_on_failure() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let a0 = mk_node(
            inst_id,
            "a0",
            BlockType::Step,
            Some(saga.id),
            Some(0),
            NodeState::Completed,
        );
        let c0 = mk_node(
            inst_id,
            "c0",
            BlockType::Step,
            Some(saga.id),
            Some(1),
            NodeState::Pending,
        );
        let a1 = mk_node(
            inst_id,
            "a1",
            BlockType::Step,
            Some(saga.id),
            Some(2),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![saga.clone(), a0, c0, a1], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def(
            "saga",
            vec![
                saga_step("s0", "a0", Some("c0")),
                saga_step("s1", "a1", None),
            ],
        );
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let updated_inst = s.get_instance(inst_id).await.unwrap().unwrap();
        let error_ctx = updated_inst.context.data.get("_error").cloned();
        assert!(error_ctx.is_some(), "expected _error to be injected");
        assert_eq!(error_ctx.unwrap()["failed_step"], "s1");
    }

    // S8: Multi-step rollback runs compensations in strict reverse order —
    // c1 (step 1) must activate before c0 (step 0) even becomes Pending-eligible.
    #[tokio::test]
    async fn s8_compensations_run_in_reverse_order() {
        let inst_id = InstanceId::new();
        let saga = mk_node(
            inst_id,
            "saga",
            BlockType::Saga,
            None,
            None,
            NodeState::Running,
        );
        let a0 = mk_node(
            inst_id,
            "a0",
            BlockType::Step,
            Some(saga.id),
            Some(0),
            NodeState::Completed,
        );
        let c0 = mk_node(
            inst_id,
            "c0",
            BlockType::Step,
            Some(saga.id),
            Some(1),
            NodeState::Pending,
        );
        let a1 = mk_node(
            inst_id,
            "a1",
            BlockType::Step,
            Some(saga.id),
            Some(2),
            NodeState::Completed,
        );
        let c1 = mk_node(
            inst_id,
            "c1",
            BlockType::Step,
            Some(saga.id),
            Some(3),
            NodeState::Pending,
        );
        let a2 = mk_node(
            inst_id,
            "a2",
            BlockType::Step,
            Some(saga.id),
            Some(4),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![saga.clone(), a0, c0, a1, c1, a2], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let def = saga_def(
            "saga",
            vec![
                saga_step("s0", "a0", Some("c0")),
                saga_step("s1", "a1", Some("c1")),
                saga_step("s2", "a2", None),
            ],
        );
        let saga_node = node_by_block(&tree, "saga").clone();

        execute_saga(&s, &registry, &inst, &saga_node, &def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        // c1 (later step) activates first; c0 stays Pending until c1 finishes.
        assert_eq!(node_by_block(&after, "c1").state, NodeState::Running);
        assert_eq!(node_by_block(&after, "c0").state, NodeState::Pending);
    }
}
