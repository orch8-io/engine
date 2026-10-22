use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
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
        if !evaluator::all_children_terminal(tree, node.id, action_branch(i)) {
            let action_children = evaluator::children_of(tree, node.id, action_branch(i));
            evaluator::activate_first_pending_child(storage, &action_children).await?;
            return Ok(true);
        }
        if evaluator::any_child_failed(tree, node.id, action_branch(i)) {
            failed_step = Some(i);
            break;
        }
    }

    let Some(failed_step) = failed_step else {
        // Every step's action completed — saga succeeds, no compensation
        // runs. Settling skips the never-needed compensation nodes.
        evaluator::settle_composite(storage, instance.id, tree, node.id, NodeState::Completed)
            .await?;
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
    // Inject once: rollback spans many ticks, and re-merging the same value
    // on each one is a pointless context write (and bumps `updated_at`).
    // The evaluator refreshes the instance snapshot after every saga
    // dispatch, so a prior injection is visible here.
    if instance.context.data.get("_error") != Some(&error_ctx) {
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
    }

    for i in (0..failed_step).rev() {
        if saga_def.steps[i].compensation.is_none() {
            continue;
        }
        if !evaluator::all_children_terminal(tree, node.id, compensation_branch(i)) {
            let comp_children = evaluator::children_of(tree, node.id, compensation_branch(i));
            evaluator::activate_first_pending_child(storage, &comp_children).await?;
            return Ok(true);
        }
        if evaluator::any_child_failed(tree, node.id, compensation_branch(i)) {
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
    // Settling skips the actions after the failed step (never started).
    evaluator::settle_composite(storage, instance.id, tree, node.id, NodeState::Failed).await?;
    debug!(
        instance_id = %instance.id,
        block_id = %saga_def.id,
        failed_step = failed_step,
        "saga rolled back and failed"
    );

    Ok(true)
}

/// Run the compensations of sagas an instance was torn away from (L3-11).
///
/// [`execute_saga`] only rolls back when one of its own actions fails. If the
/// instance is instead cancelled, killed by the step budget, or aborted by an
/// evaluation error while a saga is mid-flight, the saga node never gets
/// another pass and the effects of its completed steps are left standing.
/// This is called once the instance has reached its terminal state, and for
/// every saga that is not already `Completed`/`Failed` (those finished or
/// rolled back themselves) it runs the compensation of each step whose
/// action completed and whose compensation has not run yet, in reverse
/// order, best effort: like `on_failure` / `on_cancel` hooks only `Step`
/// compensations run, errors are logged and swallowed, and each result is
/// recorded under the compensation block's id.
///
/// Idempotent: a compensation node that is `Completed`/`Failed` (it ran in a
/// rollback that was interrupted, or in an earlier call) is not run again,
/// and every compensation run here is marked on its node.
pub(crate) async fn compensate_aborted_sagas(
    storage: &std::sync::Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    sequence: &orch8_types::sequence::SequenceDefinition,
    reason: &'static str,
) {
    use orch8_types::sequence::BlockDefinition;

    let tree = match storage.get_execution_tree(instance.id).await {
        Ok(t) => t,
        Err(e) => {
            warn!(instance_id = %instance.id, error = %e, "saga abort: failed to load execution tree");
            return;
        }
    };
    let saga_nodes: Vec<&ExecutionNode> = tree
        .iter()
        .filter(|n| {
            n.block_type == orch8_types::execution::BlockType::Saga
                && !matches!(n.state, NodeState::Completed | NodeState::Failed)
        })
        .collect();
    if saga_nodes.is_empty() {
        return;
    }
    // Final context + outputs: compensations template against what the
    // instance actually did.
    let instance = match storage.get_instance(instance.id).await {
        Ok(Some(fresh)) => fresh,
        _ => instance.clone(),
    };
    let block_map = evaluator::flatten_blocks(&sequence.blocks);
    let outputs = crate::handlers::param_resolve::OutputsSnapshot::new();

    for node in saga_nodes {
        let Some(BlockDefinition::Saga(saga_def)) = block_map
            .binary_search_by_key(&&node.block_id, |(id, _)| *id)
            .ok()
            .map(|idx| block_map[idx].1)
        else {
            continue;
        };
        for i in (0..saga_def.steps.len()).rev() {
            let Some(BlockDefinition::Step(comp_step)) = saga_def.steps[i].compensation.as_deref()
            else {
                if saga_def.steps[i].compensation.is_some() {
                    warn!(
                        instance_id = %instance.id,
                        block_id = %saga_def.id,
                        "saga abort: skipped a non-step compensation block (v1 runs step compensations only)"
                    );
                }
                continue;
            };
            let actions = evaluator::children_of(&tree, node.id, action_branch(i));
            if actions.is_empty() || actions.iter().any(|a| a.state != NodeState::Completed) {
                continue; // the action never (fully) ran: nothing to undo
            }
            let comp_nodes = evaluator::children_of(&tree, node.id, compensation_branch(i));
            if comp_nodes
                .iter()
                .any(|c| matches!(c.state, NodeState::Completed | NodeState::Failed))
            {
                continue; // already ran
            }
            run_abort_compensation(
                storage,
                handlers,
                &instance,
                saga_def,
                comp_step,
                &comp_nodes,
                &outputs,
                reason,
            )
            .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_abort_compensation(
    storage: &std::sync::Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    saga_def: &SagaDef,
    comp_step: &orch8_types::sequence::StepDef,
    comp_nodes: &[&ExecutionNode],
    outputs: &crate::handlers::param_resolve::OutputsSnapshot,
    reason: &'static str,
) {
    let mut context = instance.context.clone();
    if !context.data.is_object() {
        context.data = serde_json::json!({});
    }
    if let Some(data) = context.data.as_object_mut() {
        data.insert(
            "_error".into(),
            serde_json::json!({
                "source": "saga",
                "block_id": saga_def.id.as_str(),
                "reason": reason,
            }),
        );
    }
    let params = match crate::handlers::param_resolve::resolve_templates_in_params(
        storage.as_ref(),
        instance,
        &context,
        &comp_step.params,
        outputs,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(
                instance_id = %instance.id,
                block_id = %comp_step.id,
                error = %e,
                "saga abort: failed to resolve compensation params (skipped)"
            );
            return;
        }
    };
    let step_ctx = crate::handlers::StepContext {
        claim_token: None,
        instance_id: instance.id,
        tenant_id: instance.tenant_id.clone(),
        block_id: comp_step.id.clone(),
        params,
        context: std::sync::Arc::new(context),
        attempt: 0,
        storage: std::sync::Arc::clone(storage),
        wait_for_input: None,
    };
    let Some(result) = crate::scheduler::dispatch_cleanup_step(
        storage,
        handlers,
        instance,
        comp_step,
        "saga_abort",
        step_ctx,
    )
    .await
    else {
        return;
    };
    let state = if result.is_ok() {
        NodeState::Completed
    } else {
        NodeState::Failed
    };
    crate::scheduler::record_cleanup_output(storage, instance, comp_step, "saga_abort", result)
        .await;
    for node in comp_nodes {
        if let Err(e) = storage.update_node_state(node.id, state).await {
            warn!(instance_id = %instance.id, error = %e, "saga abort: failed to mark compensation node");
        }
    }
    debug!(
        instance_id = %instance.id,
        block_id = %saga_def.id,
        step = %comp_step.id,
        reason,
        "saga abort: compensation ran"
    );
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
            rate_budget: None,
            placement: None,
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
            compensation: None,
        }
    }

    async fn seed_instance(s: &SqliteStorage, inst: InstanceId) {
        let now = chrono::Utc::now();
        let seq = orch8_types::sequence::SequenceDefinition {
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
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
            placement: None,
            created_at: now,
        };
        s.create_sequence(&seq).await.unwrap();
        let inst_row = TaskInstance {
            claimed_at: None,
            sub_tenant: None,
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
            claimed_at: None,
            sub_tenant: None,
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
