use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId};
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::LoopDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Absolute upper bound on loop iterations.
///
/// This cap supersedes any `loop_def.max_iterations` value: no matter what
/// the sequence declares, the engine will never run a single loop more than
/// this many times. The intent is defensive — a workflow-level configuration
/// mistake (e.g. `max_iterations: u32::MAX` with `condition: "true"`) should
/// not be capable of pinning a scheduler worker forever.
///
/// Future work: submit-time workflow validation should reject sequences
/// whose loops lack a meaningful termination condition or carry absurdly
/// high caps, so this runtime guard is never the one that trips in
/// production. See `docs/plans/workflow-validation-infinite-loops.md`.
pub const LOOP_ABSOLUTE_MAX: u32 = 1_000_000;

/// Execute a loop block: repeatedly execute the body while `condition`
/// evaluates truthy, up to the lesser of `loop_def.max_iterations` and
/// [`LOOP_ABSOLUTE_MAX`].
///
/// Iteration bookkeeping is persisted as a `BlockOutput` keyed by the
/// loop's own `block_id`, with the running count stored under the
/// `_iterations` field. Under the write-append storage model (see
/// migration 027) every save appends a new row, so the handler reads the
/// most recent marker via `get_block_output` (which returns the row with
/// the highest `created_at`).
///
/// On each tick the handler:
///   1. Reads the current iteration counter from its marker output.
///   2. Trips the hard cap (completes the node) if the counter has reached
///      the effective max.
///   3. Evaluates `condition`; if falsy, completes normally.
///   4. Activates any `Pending` body children to `Running`.
///   5. If all body children are terminal, either fails the loop (on child
///      failure) or increments the counter, persists it, and resets the
///      body subtree to `Pending` so the next tick re-executes it.
///
/// Returns `Ok(true)` to indicate more work; the scheduler will re-dispatch.
#[allow(clippy::too_many_lines)]
pub async fn execute_loop(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    loop_def: &LoopDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    // Empty body: nothing to run. Completing immediately prevents a
    // condition-truthy loop from spinning on a no-op body forever.
    if loop_def.body.is_empty() {
        warn!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            "loop has empty body; completing"
        );
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // Misconfiguration: a loop with `max_iterations == 0` can never do
    // useful work. Fail the node so the sequence author sees the mistake
    // loudly rather than silently completing on tick one.
    if loop_def.max_iterations == 0 {
        warn!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            "loop misconfigured: max_iterations=0; failing"
        );
        evaluator::fail_node(storage, node.id).await?;
        return Ok(true);
    }

    // Effective cap = min(user config, absolute safety limit).
    let effective_max = loop_def.max_iterations.min(LOOP_ABSOLUTE_MAX);

    // Recover the current iteration counter from the loop's own BlockOutput
    // marker. First entry into the handler: no output yet → iteration = 0.
    let iteration: u32 = storage
        .get_block_output(instance.id, &loop_def.id)
        .await?
        .as_ref()
        .and_then(|o| o.output.get("_iterations"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0);

    // Hard cap: checked BEFORE condition evaluation so a perpetually-truthy
    // condition cannot keep the loop alive past the cap.
    if iteration >= effective_max {
        warn!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            iteration,
            max = effective_max,
            absolute_max = LOOP_ABSOLUTE_MAX,
            "loop reached iteration cap; completing"
        );
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // Condition evaluation. The loop's own outputs are not exposed to its
    // condition expression — the condition reads from instance context.
    let empty_outputs = serde_json::Value::Object(serde_json::Map::new());
    let condition_truthy = crate::expression::evaluate_condition(
        &loop_def.condition,
        &instance.context,
        &empty_outputs,
    );
    if !condition_truthy {
        evaluator::complete_node(storage, node.id).await?;
        debug!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            iterations = iteration,
            "loop condition false; completing"
        );
        return Ok(true);
    }

    let children = evaluator::children_of(tree, node.id, None);

    // Start-of-iteration: activate the first Pending body child so the next
    // evaluator tick will run it. Sequential cursor semantics — we must not
    // fan-out all pending blocks in the loop body.
    evaluator::activate_first_pending_child(storage, &children).await?;

    // End-of-iteration: if every body child is terminal, either fail (on
    // any child failure) or advance the counter and reset for the next
    // iteration. Without this reset the handler would re-observe
    // `all_terminal` on every subsequent tick and spin forever.
    if !children.is_empty() && evaluator::all_terminal(&children) {
        if evaluator::any_failed(&children) {
            if loop_def.continue_on_error {
                debug!(
                    instance_id = %instance.id,
                    block_id = %loop_def.id,
                    iteration,
                    "loop body failed but continue_on_error=true; advancing"
                );
            } else {
                evaluator::fail_node(storage, node.id).await?;
                return Ok(true);
            }
        }

        // break_on: evaluate after body completion; exit loop on match.
        if let Some(ref break_expr) = loop_def.break_on {
            if crate::expression::evaluate_condition(break_expr, &instance.context, &empty_outputs)
            {
                debug!(
                    instance_id = %instance.id,
                    block_id = %loop_def.id,
                    iteration,
                    "loop break_on condition met; completing"
                );
                evaluator::complete_node(storage, node.id).await?;
                return Ok(true);
            }
        }

        let next_iteration = iteration.saturating_add(1);
        let marker = BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: loop_def.id.clone(),
            output: serde_json::json!({ "_iterations": next_iteration }),
            output_ref: None,
            output_size: 0,
            attempt: i16::try_from(next_iteration).unwrap_or(i16::MAX),
            created_at: chrono::Utc::now(),
        };
        storage.save_block_output(&marker).await?;

        debug!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            iteration = next_iteration,
            "loop iteration completed"
        );

        // Cap reached after this iteration: leave body children terminal so
        // the evaluator can close out cleanly on the next tick (the
        // top-of-function guard will fire on `iteration >= effective_max`).
        if next_iteration >= effective_max {
            return Ok(true);
        }

        // Reset the entire body subtree (direct children plus every
        // descendant) back to Pending so the next tick re-activates it.
        reset_subtree_to_pending(storage, tree, instance.id, node.id).await?;

        // poll_interval: defer re-execution by setting next_fire_at.
        if let Some(interval_secs) = loop_def.poll_interval {
            let next_fire = chrono::Utc::now()
                + chrono::Duration::seconds(i64::try_from(interval_secs).unwrap_or(5));
            storage
                .update_instance_state(
                    instance.id,
                    orch8_types::instance::InstanceState::Scheduled,
                    Some(next_fire),
                )
                .await?;
        }
    }

    Ok(true)
}

/// Walk the subtree rooted at `root_id` (exclusive) and transition every
/// descendant back to [`NodeState::Pending`]. Additionally purge composite
/// iteration-counter markers from descendant `Loop` / `ForEach` blocks via
/// [`StorageBackend::delete_block_outputs`].
///
/// Called at loop-iteration boundaries so the body can run again without
/// stale terminal state from the previous iteration. Step body outputs are
/// left in place — under the write-append storage model each iteration
/// appends its own row per step, preserving iteration history. Composite
/// markers are internal iteration state, not user-observable outputs, so
/// they MUST be purged or the descendant's top-of-handler cap guard would
/// observe the previous iteration's counter on the next tick and complete
/// the descendant immediately without ever running its body.
///
/// The root node's own marker is intentionally NOT purged: the caller is
/// the composite that owns it, and that caller has already advanced its
/// counter for the next outer iteration via `save_block_output`.
async fn reset_subtree_to_pending(
    storage: &dyn StorageBackend,
    tree: &[ExecutionNode],
    instance_id: InstanceId,
    root_id: ExecutionNodeId,
) -> Result<(), EngineError> {
    // BFS over the parent_id graph to collect every descendant of root_id
    // along with the data we need to purge composite markers.
    let mut frontier: Vec<ExecutionNodeId> = vec![root_id];
    let mut descendants: Vec<(ExecutionNodeId, BlockType, BlockId)> = Vec::new();
    while let Some(parent) = frontier.pop() {
        for node in tree.iter().filter(|n| n.parent_id == Some(parent)) {
            descendants.push((node.id, node.block_type, node.block_id.clone()));
            frontier.push(node.id);
        }
    }
    if descendants.is_empty() {
        return Ok(());
    }

    // Three batched round-trips replace O(descendants × 3) per-node calls.
    let node_ids: Vec<ExecutionNodeId> = descendants.iter().map(|(id, _, _)| *id).collect();
    storage
        .update_nodes_state(&node_ids, NodeState::Pending)
        .await?;

    let composite_block_ids: Vec<BlockId> = descendants
        .iter()
        .filter(|(_, bt, _)| matches!(bt, BlockType::Loop | BlockType::ForEach))
        .map(|(_, _, bid)| bid.clone())
        .collect();
    if !composite_block_ids.is_empty() {
        storage
            .delete_block_outputs_batch(instance_id, &composite_block_ids)
            .await?;
    }

    // Purge any stale worker_tasks row for every descendant. Composite
    // descendants have no worker_tasks row, so this is a no-op for them.
    let all_block_ids: Vec<String> = descendants
        .iter()
        .map(|(_, _, bid)| bid.0.clone())
        .collect();
    storage
        .cancel_worker_tasks_for_blocks(instance_id.0, &all_block_ids)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{evaluate_condition, is_truthy};
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority};
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

    #[test]
    fn absolute_max_is_one_million() {
        assert_eq!(LOOP_ABSOLUTE_MAX, 1_000_000);
    }

    // ----- subtree-reset purge tests (L1..L6) -----

    fn mk_node(
        instance_id: InstanceId,
        block_id: &str,
        block_type: BlockType,
        parent_id: Option<ExecutionNodeId>,
    ) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId(block_id.into()),
            parent_id,
            block_type,
            branch_index: None,
            state: NodeState::Completed,
            started_at: None,
            completed_at: None,
        }
    }

    fn mk_marker(inst: InstanceId, block: &str, value: i64) -> BlockOutput {
        BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: inst,
            block_id: BlockId(block.into()),
            output: json!({ "_iterations": value }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        }
    }

    async fn seed_instance(s: &SqliteStorage, inst: InstanceId) {
        use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
        let now = chrono::Utc::now();
        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            name: "test".into(),
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
        let inst_row = orch8_types::instance::TaskInstance {
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

    #[tokio::test]
    async fn l1_reset_subtree_purges_descendant_loop_markers() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst).await;

        let outer = mk_node(inst, "outer_fe", BlockType::ForEach, None);
        let inner = mk_node(inst, "inner_loop", BlockType::Loop, Some(outer.id));
        let step = mk_node(inst, "step", BlockType::Step, Some(inner.id));
        let tree = vec![outer.clone(), inner.clone(), step];

        s.save_block_output(&mk_marker(inst, "inner_loop", 2))
            .await
            .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();

        assert!(s
            .get_block_output(inst, &inner.block_id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn l2_reset_subtree_purges_descendant_for_each_markers() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst).await;

        let outer = mk_node(inst, "outer_lp", BlockType::Loop, None);
        let inner = mk_node(inst, "inner_fe", BlockType::ForEach, Some(outer.id));
        let tree = vec![outer.clone(), inner.clone()];

        s.save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: inst,
            block_id: inner.block_id.clone(),
            output: json!({"_index": 1, "_total": 3}),
            output_ref: None,
            output_size: 0,
            attempt: 1,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();

        assert!(s
            .get_block_output(inst, &inner.block_id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn l3_reset_subtree_preserves_step_outputs() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst).await;

        let outer = mk_node(inst, "outer_lp", BlockType::Loop, None);
        let step = mk_node(inst, "body_step", BlockType::Step, Some(outer.id));
        let tree = vec![outer.clone(), step.clone()];

        s.save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: inst,
            block_id: step.block_id.clone(),
            output: json!({"result": "ok"}),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();

        let got = s
            .get_block_output(inst, &step.block_id)
            .await
            .unwrap()
            .expect("step output preserved");
        assert_eq!(got.output["result"], "ok");
    }

    /// Regression: `reset_subtree_to_pending` in `loop_block` must purge
    /// stale `worker_tasks` rows for each descendant `block_id`. Mirror of
    /// `for_each::tests::fe_reset_subtree_purges_descendant_worker_tasks`.
    /// Without this, the `UNIQUE(instance_id, block_id)` constraint on
    /// `worker_tasks` combined with `ON CONFLICT DO NOTHING` silently drops
    /// iteration 1+ external dispatches.
    #[tokio::test]
    async fn lp_reset_subtree_purges_descendant_worker_tasks() {
        use orch8_types::worker::{WorkerTask, WorkerTaskState};
        use uuid::Uuid;

        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst).await;

        let outer = mk_node(inst, "outer_lp", BlockType::Loop, None);
        let step = mk_node(inst, "body_step", BlockType::Step, Some(outer.id));
        let tree = vec![outer.clone(), step.clone()];

        // Simulate iteration 0's completed worker_tasks row for the body step.
        let iter0 = WorkerTask {
            id: Uuid::now_v7(),
            instance_id: inst,
            block_id: step.block_id.clone(),
            handler_name: "external_handler".into(),
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
        s.create_worker_task(&iter0).await.unwrap();
        s.claim_worker_tasks("external_handler", "w1", 1)
            .await
            .unwrap();
        s.complete_worker_task(iter0.id, "w1", &json!({"ok": true}))
            .await
            .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();

        assert!(
            s.get_worker_task(iter0.id).await.unwrap().is_none(),
            "completed worker_tasks row must be purged by reset_subtree"
        );

        // Iteration 1 INSERT for the same block_id must now succeed.
        let iter1 = WorkerTask {
            id: Uuid::now_v7(),
            instance_id: inst,
            block_id: step.block_id.clone(),
            handler_name: "external_handler".into(),
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
        s.create_worker_task(&iter1).await.unwrap();
        let claimed = s
            .claim_worker_tasks("external_handler", "w2", 1)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, iter1.id);
    }

    #[tokio::test]
    async fn l4_reset_subtree_purges_transitive_descendants() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst).await;

        let l1 = mk_node(inst, "l1", BlockType::Loop, None);
        let l2 = mk_node(inst, "l2", BlockType::Loop, Some(l1.id));
        let l3 = mk_node(inst, "l3", BlockType::Loop, Some(l2.id));
        let tree = vec![l1.clone(), l2.clone(), l3.clone()];

        s.save_block_output(&mk_marker(inst, "l2", 1))
            .await
            .unwrap();
        s.save_block_output(&mk_marker(inst, "l3", 1))
            .await
            .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, l1.id)
            .await
            .unwrap();

        assert!(s
            .get_block_output(inst, &l2.block_id)
            .await
            .unwrap()
            .is_none());
        assert!(s
            .get_block_output(inst, &l3.block_id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn l5_reset_subtree_does_not_purge_sibling_markers() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst).await;

        let par = mk_node(inst, "par", BlockType::Parallel, None);
        let lp_a = mk_node(inst, "lp_a", BlockType::Loop, Some(par.id));
        let lp_b = mk_node(inst, "lp_b", BlockType::Loop, Some(par.id));
        let tree = vec![par, lp_a.clone(), lp_b.clone()];

        s.save_block_output(&mk_marker(inst, "lp_a", 2))
            .await
            .unwrap();
        s.save_block_output(&mk_marker(inst, "lp_b", 2))
            .await
            .unwrap();

        // Reset only branch lp_a — lp_b's marker must remain.
        reset_subtree_to_pending(&s, &tree, inst, lp_a.id)
            .await
            .unwrap();

        assert!(s
            .get_block_output(inst, &lp_b.block_id)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn l6_reset_subtree_is_idempotent() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst).await;

        let outer = mk_node(inst, "outer", BlockType::ForEach, None);
        let inner = mk_node(inst, "inner_loop", BlockType::Loop, Some(outer.id));
        let tree = vec![outer.clone(), inner.clone()];

        s.save_block_output(&mk_marker(inst, "inner_loop", 1))
            .await
            .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();
        // Second call should not error.
        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();

        assert!(s
            .get_block_output(inst, &inner.block_id)
            .await
            .unwrap()
            .is_none());
    }

    // ----- handler behavior tests (H1, H3, H4 — H2 lives in for_each.rs) -----

    fn mk_instance_for(inst_id: InstanceId, ctx: serde_json::Value) -> TaskInstance {
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
            context: ExecutionContext {
                data: ctx,
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
        }
    }

    #[tokio::test]
    async fn h1_inner_loop_runs_fresh_after_outer_iteration_advance() {
        // Build inner_loop with a single step body. Pre-load a stale marker
        // (_iterations=2) and a step output (preserved). Then reset, then
        // call execute_loop and verify body child became Running.
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&s, inst_id).await;
        let inst = mk_instance_for(inst_id, json!({}));

        let outer = mk_node(inst_id, "outer", BlockType::ForEach, None);
        let mut inner = mk_node(inst_id, "inner_loop", BlockType::Loop, Some(outer.id));
        inner.state = NodeState::Running;
        let mut step = mk_node(inst_id, "body", BlockType::Step, Some(inner.id));
        step.state = NodeState::Completed;

        s.create_execution_nodes_batch(&[outer.clone(), inner.clone(), step.clone()])
            .await
            .unwrap();

        // Stale marker from a previous outer iteration.
        s.save_block_output(&mk_marker(inst_id, "inner_loop", 2))
            .await
            .unwrap();
        // A step output from the previous iteration — must be preserved.
        s.save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: inst_id,
            block_id: BlockId("body".into()),
            output: json!({"prev": true}),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

        // Reset the inner_loop subtree as the outer for_each would.
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        reset_subtree_to_pending(&s, &tree, inst_id, outer.id)
            .await
            .unwrap();

        let loop_def = LoopDef {
            id: BlockId("inner_loop".into()),
            condition: "true".into(),
            body: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: BlockId("body".into()),
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
                },
            ))],
            max_iterations: 2,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        };
        let registry = HandlerRegistry::new();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        let inner_node = tree
            .iter()
            .find(|n| n.block_id.0 == "inner_loop")
            .unwrap()
            .clone();

        execute_loop(&s, &registry, &inst, &inner_node, &loop_def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        let step_after = after.iter().find(|n| n.block_id.0 == "body").unwrap();
        assert_eq!(
            step_after.state,
            NodeState::Running,
            "body child must be activated, not skipped via stale-counter cap"
        );
        let inner_after = after.iter().find(|n| n.block_id.0 == "inner_loop").unwrap();
        assert_ne!(
            inner_after.state,
            NodeState::Completed,
            "inner_loop must NOT short-circuit via stale marker"
        );

        // Step output preserved.
        assert!(s
            .get_block_output(inst_id, &BlockId("body".into()))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn h3_cap_reached_path_still_works_within_a_single_run() {
        // Pre-populate the marker AT the cap without resetting; the
        // top-of-function guard must still complete the node.
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&s, inst_id).await;
        let inst = mk_instance_for(inst_id, json!({}));

        let mut lp = mk_node(inst_id, "lp", BlockType::Loop, None);
        lp.state = NodeState::Running;
        let mut step = mk_node(inst_id, "step_body", BlockType::Step, Some(lp.id));
        step.state = NodeState::Completed;
        s.create_execution_nodes_batch(&[lp.clone(), step])
            .await
            .unwrap();

        s.save_block_output(&mk_marker(inst_id, "lp", 3))
            .await
            .unwrap();

        let loop_def = LoopDef {
            id: BlockId("lp".into()),
            condition: "true".into(),
            body: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: BlockId("step_body".into()),
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
                },
            ))],
            max_iterations: 3,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        };
        let registry = HandlerRegistry::new();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        let lp_node = tree.iter().find(|n| n.block_id.0 == "lp").unwrap().clone();

        execute_loop(&s, &registry, &inst, &lp_node, &loop_def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        let lp_after = after.iter().find(|n| n.block_id.0 == "lp").unwrap();
        assert_eq!(lp_after.state, NodeState::Completed);
    }

    #[tokio::test]
    async fn h4_failure_in_body_still_fails_loop_without_purge() {
        // Body child Failed → loop fails via fail_node. Reset is NOT called
        // on the failure branch, so any pre-existing marker stays.
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&s, inst_id).await;
        let inst = mk_instance_for(inst_id, json!({}));

        let mut lp = mk_node(inst_id, "lp", BlockType::Loop, None);
        lp.state = NodeState::Running;
        let mut step = mk_node(inst_id, "body", BlockType::Step, Some(lp.id));
        step.state = NodeState::Failed;
        s.create_execution_nodes_batch(&[lp.clone(), step])
            .await
            .unwrap();

        // Marker from iteration 0 — must remain after the failure path.
        s.save_block_output(&mk_marker(inst_id, "lp", 0))
            .await
            .unwrap();

        let loop_def = LoopDef {
            id: BlockId("lp".into()),
            condition: "true".into(),
            body: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: BlockId("body".into()),
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
                },
            ))],
            max_iterations: 5,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        };
        let registry = HandlerRegistry::new();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        let lp_node = tree.iter().find(|n| n.block_id.0 == "lp").unwrap().clone();

        execute_loop(&s, &registry, &inst, &lp_node, &loop_def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        let lp_after = after.iter().find(|n| n.block_id.0 == "lp").unwrap();
        assert_eq!(lp_after.state, NodeState::Failed);
        assert!(s
            .get_block_output(inst_id, &BlockId("lp".into()))
            .await
            .unwrap()
            .is_some());
    }
}
