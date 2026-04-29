use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::TryCatchDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a try-catch-finally block.
/// Branch 0 = try, Branch 1 = catch, Branch 2 = finally.
/// Returns `true` if more work.
pub async fn execute_try_catch(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    tc_def: &TryCatchDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let try_children = evaluator::children_of(tree, node.id, Some(0));
    let catch_children = evaluator::children_of(tree, node.id, Some(1));
    let finally_children = evaluator::children_of(tree, node.id, Some(2));

    // Phase 1: Activate and wait for try block.
    if !try_children.is_empty() && !evaluator::all_terminal(&try_children) {
        // Sequential cursor: only the first Pending try child should start.
        evaluator::activate_first_pending_child(storage, &try_children).await?;
        return Ok(true);
    }

    let try_failed = evaluator::any_failed(&try_children);

    // Phase 2: Handle catch block.
    if try_failed && !catch_children.is_empty() {
        if !evaluator::all_terminal(&catch_children) {
            // Inject error context before activating catch children.
            // Find which try block(s) failed and expose via context.data._error.
            let failed_blocks: Vec<String> = try_children
                .iter()
                .filter(|c| c.state == NodeState::Failed)
                .map(|c| c.block_id.0.clone())
                .collect();
            let error_ctx = serde_json::json!({
                "failed_blocks": failed_blocks,
                "source": "try_catch",
                "block_id": tc_def.id.0,
            });
            storage
                .merge_context_data(instance.id, "_error", &error_ctx)
                .await
                .map_err(|e| {
                    tracing::warn!(
                        instance_id = %instance.id,
                        error = %e,
                        "failed to inject error context for catch block"
                    );
                    EngineError::from(e)
                })?;

            // Sequential cursor: only the first Pending catch child should start.
            evaluator::activate_first_pending_child(storage, &catch_children).await?;
            return Ok(true);
        }
    } else if !try_failed {
        // Try succeeded — skip catch.
        for child in &catch_children {
            if matches!(child.state, NodeState::Pending | NodeState::Running) {
                storage
                    .update_node_state(child.id, NodeState::Skipped)
                    .await?;
            }
        }
    }

    // Phase 3: Finally block always runs.
    if !finally_children.is_empty() && !evaluator::all_terminal(&finally_children) {
        // Sequential cursor: only the first Pending finally child should start.
        evaluator::activate_first_pending_child(storage, &finally_children).await?;
        return Ok(true);
    }

    // All phases complete. Node succeeds if try succeeded (or catch recovered)
    // AND finally did not fail. Recovery means one of:
    //   - catch children exist and none failed (catch logic ran successfully)
    //   - catch block is declared (even empty) and no catch children exist,
    //     i.e. the author wrote `catch: []` as an explicit "swallow errors"
    //     marker — the tree has no catch nodes to fail, so the error is absorbed.
    // A try failure without *any* catch declaration propagates, but since
    // `TryCatchDef.catch_block` is always present, the empty-vec case is the
    // canonical "ignore errors" pattern.
    //
    // Finally-failure semantics: if the finally block itself throws, the
    // try_catch fails — a failed cleanup is a programming error and must
    // not be silently absorbed, regardless of whether try or catch succeeded.
    // Matches Java/JS: a finally exception overrides an earlier try/catch
    // result.
    let catch_recovered = if catch_children.is_empty() {
        // No catch children in tree — treat as swallowed (empty catch body).
        true
    } else {
        !evaluator::any_failed(&catch_children)
    };
    let finally_failed = evaluator::any_failed(&finally_children);
    if finally_failed || (try_failed && !catch_recovered) {
        evaluator::fail_node(storage, node.id).await?;
    } else {
        evaluator::complete_node(storage, node.id).await?;
    }

    debug!(
        instance_id = %instance.id,
        block_id = %tc_def.id,
        try_failed = try_failed,
        finally_failed = finally_failed,
        "try-catch-finally completed"
    );

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
            name: "tc_test".into(),
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

    fn tc_def(id: &str) -> TryCatchDef {
        TryCatchDef {
            id: BlockId(id.into()),
            try_block: vec![],
            catch_block: vec![],
            finally_block: None,
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

    // TC1: No try/catch/finally children → node completes immediately.
    #[tokio::test]
    async fn tc1_empty_all_branches_completes() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let (s, tree) = setup(vec![tc.clone()], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Completed);
    }

    // TC2: Try succeeds + catch present → catch children transitioned to Skipped.
    #[tokio::test]
    async fn tc2_try_success_skips_catch_children() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_ok = mk_node(
            inst_id,
            "try_ok",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Completed,
        );
        let catch_pend = mk_node(
            inst_id,
            "catch_pend",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![tc.clone(), try_ok, catch_pend], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "catch_pend").state,
            NodeState::Skipped
        );
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Completed);
    }

    // TC3: Try succeeds + finally present → finally children activated before
    // the node can complete.
    #[tokio::test]
    async fn tc3_try_success_activates_finally() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_ok = mk_node(
            inst_id,
            "try_ok",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Completed,
        );
        let finally_pend = mk_node(
            inst_id,
            "finally_pend",
            BlockType::Step,
            Some(tc.id),
            Some(2),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![tc.clone(), try_ok, finally_pend], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "finally_pend").state,
            NodeState::Running
        );
        assert_eq!(
            node_by_block(&after, "tc").state,
            NodeState::Running,
            "try-catch cannot complete while finally is still running",
        );
    }

    // TC4: Try succeeds, no catch, no finally → node completes.
    #[tokio::test]
    async fn tc4_try_success_no_catch_no_finally_completes() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_ok = mk_node(
            inst_id,
            "try_ok",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Completed,
        );
        let (s, tree) = setup(vec![tc.clone(), try_ok], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Completed);
    }

    // TC5: Try fails + catch recovers (completed) → node completes.
    #[tokio::test]
    async fn tc5_try_fail_catch_recovers_completes() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_bad = mk_node(
            inst_id,
            "try_bad",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let catch_ok = mk_node(
            inst_id,
            "catch_ok",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Completed,
        );
        let (s, tree) = setup(vec![tc.clone(), try_bad, catch_ok], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Completed);
    }

    // TC6: Try fails + no catch children in tree → empty catch swallows.
    //
    // Every `TryCatchDef` has a `catch_block` field. When the author
    // provides an empty catch (`catch: []`), no catch nodes are created in
    // the execution tree. The handler treats this as "catch and ignore",
    // so the try_catch node completes rather than propagating the failure.
    #[tokio::test]
    async fn tc6_try_fail_empty_catch_swallows() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_bad = mk_node(
            inst_id,
            "try_bad",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![tc.clone(), try_bad], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Completed);
    }

    // TC7: Try fails AND catch also fails → node fails.
    #[tokio::test]
    async fn tc7_try_and_catch_fail_node_fails() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_bad = mk_node(
            inst_id,
            "try_bad",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let catch_bad = mk_node(
            inst_id,
            "catch_bad",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![tc.clone(), try_bad, catch_bad], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Failed);
    }

    // TC8: Try fails with catch present but not yet terminal → _error context
    // is injected with failed_blocks, source, block_id.
    #[tokio::test]
    async fn tc8_error_context_injected_before_catch_activation() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_bad = mk_node(
            inst_id,
            "try_bad",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let catch_pend = mk_node(
            inst_id,
            "catch_pend",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![tc.clone(), try_bad, catch_pend], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        // Catch must have been activated.
        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "catch_pend").state,
            NodeState::Running
        );

        // And _error context must contain the injected fields.
        let inst_row = s.get_instance(inst_id).await.unwrap().unwrap();
        let err = inst_row
            .context
            .data
            .get("_error")
            .expect("_error must be injected");
        assert_eq!(
            err.get("source").and_then(|v| v.as_str()),
            Some("try_catch")
        );
        assert_eq!(err.get("block_id").and_then(|v| v.as_str()), Some("tc"));
        let blocks = err
            .get("failed_blocks")
            .and_then(|v| v.as_array())
            .expect("failed_blocks must be an array");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].as_str(), Some("try_bad"));
    }

    // TC9: Try fails + catch recovers + finally block activated next tick.
    #[tokio::test]
    async fn tc9_try_fail_catch_recover_then_finally() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_bad = mk_node(
            inst_id,
            "try_bad",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let catch_ok = mk_node(
            inst_id,
            "catch_ok",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Completed,
        );
        let finally_pend = mk_node(
            inst_id,
            "finally_pend",
            BlockType::Step,
            Some(tc.id),
            Some(2),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![tc.clone(), try_bad, catch_ok, finally_pend], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "finally_pend").state,
            NodeState::Running
        );
        assert_eq!(
            node_by_block(&after, "tc").state,
            NodeState::Running,
            "node waits for finally to complete",
        );
    }

    // TC10: Finally runs after try/catch both complete; on the following tick
    // with finally Completed, node transitions to Completed.
    #[tokio::test]
    async fn tc10_finally_terminal_completes_node() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_ok = mk_node(
            inst_id,
            "try_ok",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Completed,
        );
        let finally_done = mk_node(
            inst_id,
            "finally_done",
            BlockType::Step,
            Some(tc.id),
            Some(2),
            NodeState::Completed,
        );
        let (s, tree) = setup(vec![tc.clone(), try_ok, finally_done], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Completed);
    }

    // TC11: Multiple failed try blocks are all captured in _error.failed_blocks.
    #[tokio::test]
    async fn tc11_multiple_failed_try_blocks_captured() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_a = mk_node(
            inst_id,
            "try_a",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let try_b = mk_node(
            inst_id,
            "try_b",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let catch_pend = mk_node(
            inst_id,
            "catch_pend",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Pending,
        );
        let (s, tree) = setup(vec![tc.clone(), try_a, try_b, catch_pend], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let inst_row = s.get_instance(inst_id).await.unwrap().unwrap();
        let err = inst_row.context.data.get("_error").unwrap();
        let blocks: Vec<String> = err
            .get("failed_blocks")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .map(|b| b.as_str().unwrap().to_string())
            .collect();
        assert_eq!(blocks.len(), 2);
        assert!(blocks.contains(&"try_a".to_string()));
        assert!(blocks.contains(&"try_b".to_string()));
    }

    // TC12: Try succeeds, catch children in Completed/Failed (already terminal) —
    // skip-phase must NOT re-assign terminal nodes; node just completes.
    #[tokio::test]
    async fn tc12_try_success_does_not_overwrite_terminal_catch_nodes() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_ok = mk_node(
            inst_id,
            "try_ok",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Completed,
        );
        // catch_a already Completed from a prior resume — must not be re-flipped.
        let catch_a = mk_node(
            inst_id,
            "catch_a",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Completed,
        );
        // catch_b running — per handler logic only Pending|Running are flipped to Skipped.
        // After the call catch_b becomes Skipped.
        let catch_b = mk_node(
            inst_id,
            "catch_b",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Running,
        );
        let (s, tree) = setup(vec![tc.clone(), try_ok, catch_a, catch_b], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "catch_a").state,
            NodeState::Completed,
            "terminal catch node must not be overwritten",
        );
        assert_eq!(
            node_by_block(&after, "catch_b").state,
            NodeState::Skipped,
            "non-terminal catch node must be skipped when try succeeds",
        );
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Completed);
    }

    // TC13: Finally failure overrides a successful try/catch. A failed
    // cleanup is treated as a programming error and must fail the
    // enclosing try_catch block — mirrors Java/JS "finally throws".
    #[tokio::test]
    async fn tc13_finally_failure_fails_node_even_when_try_succeeded() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_ok = mk_node(
            inst_id,
            "try_ok",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Completed,
        );
        let finally_bad = mk_node(
            inst_id,
            "finally_bad",
            BlockType::Step,
            Some(tc.id),
            Some(2),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![tc.clone(), try_ok, finally_bad], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(
            node_by_block(&after, "tc").state,
            NodeState::Failed,
            "finally failure must fail the try_catch node",
        );
    }

    // TC14: Finally failure also overrides a catch-recovered try failure.
    #[tokio::test]
    async fn tc14_finally_failure_fails_node_even_when_catch_recovered() {
        let inst_id = InstanceId::new();
        let tc = mk_node(
            inst_id,
            "tc",
            BlockType::TryCatch,
            None,
            None,
            NodeState::Running,
        );
        let try_bad = mk_node(
            inst_id,
            "try_bad",
            BlockType::Step,
            Some(tc.id),
            Some(0),
            NodeState::Failed,
        );
        let catch_ok = mk_node(
            inst_id,
            "catch_ok",
            BlockType::Step,
            Some(tc.id),
            Some(1),
            NodeState::Completed,
        );
        let finally_bad = mk_node(
            inst_id,
            "finally_bad",
            BlockType::Step,
            Some(tc.id),
            Some(2),
            NodeState::Failed,
        );
        let (s, tree) = setup(vec![tc.clone(), try_bad, catch_ok, finally_bad], inst_id).await;
        let inst = mk_instance(inst_id);
        let registry = HandlerRegistry::new();
        let tc_node = node_by_block(&tree, "tc").clone();

        execute_try_catch(&s, &registry, &inst, &tc_node, &tc_def("tc"), &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        assert_eq!(node_by_block(&after, "tc").state, NodeState::Failed);
    }
}
