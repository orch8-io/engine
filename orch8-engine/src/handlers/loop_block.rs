use tracing::{debug, warn};

use orch8_storage::StorageBackend;
#[cfg(test)]
use orch8_types::execution::BlockType;
use orch8_types::execution::{ExecutionNode, NodeState};
#[cfg(test)]
use orch8_types::ids::{BlockId, ExecutionNodeId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::LoopDef;

use crate::error::EngineError;
use crate::evaluator::{self, SeqProgress};
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

/// Upper bound on `poll_interval` (one year). Larger values are clamped so
/// the reschedule timestamp can never overflow `DateTime<Utc>`.
const MAX_POLL_INTERVAL_SECS: u64 = 365 * 24 * 60 * 60;

/// Marker field set while an iteration advance is in flight: the incremented
/// counter is durable but the body reset may not have finished. See
/// [`advance_iteration`].
const RESET_PENDING_KEY: &str = "_reset_pending";

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
///   1. Reads the current iteration counter from its marker output (and
///      finishes an interrupted iteration advance, if any).
///   2. Trips the hard cap (completes the node) if the counter has reached
///      the effective max.
///   3. At an iteration boundary only (every body child still `Pending`),
///      evaluates `condition`; if falsy, completes normally. The condition
///      is never re-evaluated mid-iteration.
///   4. Advances the body's sequential cursor. A failed body block stops
///      the iteration: the loop fails, or — with `continue_on_error` —
///      advances to the next iteration.
///   5. When the iteration settles, checks `break_on`, then durably
///      advances the counter and resets the body subtree to `Pending`.
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
    let clock = orch8_types::clock::SharedClock::default();
    execute_loop_with_clock(storage, instance, node, loop_def, tree, &clock).await
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) async fn execute_loop_with_clock(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node: &ExecutionNode,
    loop_def: &LoopDef,
    tree: &[ExecutionNode],
    clock: &orch8_types::clock::SharedClock,
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
    let marker = storage.get_block_output(instance.id, &loop_def.id).await?;
    let marker_output = marker.as_ref().map(|o| &o.output);
    let iteration: u32 = marker_output
        .and_then(|o| o.get("_iterations"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0);

    // Crash recovery: a previous tick durably advanced the counter but may
    // have died before (or while) resetting the body. Finish the reset now —
    // it is idempotent — and clear the flag. The body cannot have run in
    // between: every node is still terminal (or already Pending).
    if marker_output
        .and_then(|o| o.get(RESET_PENDING_KEY))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        finish_iteration_reset(storage, instance, node, loop_def, tree, iteration).await?;
        return Ok(true);
    }

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
        evaluator::settle_composite(storage, instance.id, tree, node.id, NodeState::Completed)
            .await?;
        return Ok(true);
    }

    let children = evaluator::children_of(tree, node.id, None);
    let empty_outputs = serde_json::Value::Object(serde_json::Map::new());

    // Condition evaluation happens ONLY at an iteration boundary (no body
    // block started yet). Re-evaluating mid-iteration would complete the
    // loop while body steps are still Running. The loop's own outputs are
    // not exposed to its condition expression — it reads instance context.
    let at_iteration_start = children.iter().all(|c| c.state == NodeState::Pending);
    if at_iteration_start
        && !crate::expression::evaluate_condition(
            &loop_def.condition,
            &instance.context,
            &empty_outputs,
        )
    {
        evaluator::settle_composite(storage, instance.id, tree, node.id, NodeState::Completed)
            .await?;
        debug!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            iterations = iteration,
            "loop condition false; completing"
        );
        return Ok(true);
    }

    // Sequential cursor with fail-fast: a failed body block stops the
    // iteration instead of letting its successors run.
    match evaluator::advance_sequence(storage, &children).await? {
        SeqProgress::Advanced | SeqProgress::Blocked => return Ok(true),
        SeqProgress::Failed | SeqProgress::Cancelled => {
            if !loop_def.continue_on_error {
                evaluator::settle_composite(storage, instance.id, tree, node.id, NodeState::Failed)
                    .await?;
                return Ok(true);
            }
            debug!(
                instance_id = %instance.id,
                block_id = %loop_def.id,
                iteration,
                "loop body failed but continue_on_error=true; advancing"
            );
        }
        SeqProgress::Done => {}
    }

    // break_on: evaluate after body completion; exit loop on match.
    if let Some(ref break_expr) = loop_def.break_on
        && crate::expression::evaluate_condition(break_expr, &instance.context, &empty_outputs)
    {
        debug!(
            instance_id = %instance.id,
            block_id = %loop_def.id,
            iteration,
            "loop break_on condition met; completing"
        );
        evaluator::settle_composite(storage, instance.id, tree, node.id, NodeState::Completed)
            .await?;
        return Ok(true);
    }

    let next_iteration = iteration.saturating_add(1);
    if next_iteration < effective_max {
        advance_iteration(storage, instance, node, loop_def, tree, next_iteration).await?;
    } else {
        // Cap reached after this iteration: persist the final count and leave
        // the body terminal; the top-of-function guard completes the node on
        // the next tick.
        storage
            .save_block_output(&iteration_marker(instance, loop_def, next_iteration, false))
            .await?;
    }

    // Compact old body-step outputs once the retained window is exceeded.
    if let Some(retain) = loop_def.retain_iterations {
        match crate::evaluator::compact_iteration_outputs(
            storage,
            instance.id,
            &loop_def.body,
            retain,
        )
        .await
        {
            Ok(n) if n > 0 => crate::metrics::inc_by(crate::metrics::LOOP_OUTPUTS_COMPACTED, n),
            Ok(_) => {}
            Err(e) => warn!(
                instance_id = %instance.id,
                block_id = %loop_def.id,
                error = %e,
                "loop output compaction failed (continuing)"
            ),
        }
    }

    debug!(
        instance_id = %instance.id,
        block_id = %loop_def.id,
        iteration = next_iteration,
        "loop iteration completed"
    );

    if next_iteration >= effective_max {
        return Ok(true);
    }

    // poll_interval: defer re-execution by setting next_fire_at.
    // Use CAS (conditional_update_instance_state) so that a Cancel
    // signal processed between this write and the next evaluator tick
    // is not silently overwritten. The instance is Running when the
    // loop handler executes; if a cancel has already transitioned it
    // to Cancelled, the CAS returns false and the write is safely
    // skipped.
    if let Some(interval_secs) = loop_def.poll_interval {
        let next_fire = poll_fire_at(clock.now(), interval_secs);
        let transitioned = storage
            .conditional_update_instance_state(
                instance.id,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(next_fire),
            )
            .await?;
        if !transitioned {
            debug!(
                instance_id = %instance.id,
                block_id = %loop_def.id,
                "CAS for poll_interval reschedule failed — instance state changed concurrently"
            );
        }
    }

    Ok(true)
}

/// `now + interval_secs`, with the interval clamped to
/// [`MAX_POLL_INTERVAL_SECS`] so an absurd author-supplied value can never
/// overflow (and panic) the timestamp arithmetic.
fn poll_fire_at(
    now: chrono::DateTime<chrono::Utc>,
    interval_secs: u64,
) -> chrono::DateTime<chrono::Utc> {
    let secs = i64::try_from(interval_secs.min(MAX_POLL_INTERVAL_SECS)).unwrap_or(0);
    now.checked_add_signed(chrono::Duration::seconds(secs))
        .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC)
}

fn iteration_marker(
    instance: &TaskInstance,
    loop_def: &LoopDef,
    iterations: u32,
    reset_pending: bool,
) -> BlockOutput {
    let output = if reset_pending {
        serde_json::json!({ "_iterations": iterations, RESET_PENDING_KEY: true })
    } else {
        serde_json::json!({ "_iterations": iterations })
    };
    BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        block_id: loop_def.id.clone(),
        output,
        output_ref: None,
        output_size: 0,
        attempt: u16::try_from(iterations).unwrap_or(u16::MAX),
        created_at: chrono::Utc::now(),
    }
}

/// Durably advance the loop to `next_iteration` and reset its body.
///
/// The reset deletes the body's effect receipts, so it must never run
/// before the advanced counter is durable: were the process to crash in
/// between, the next tick would re-run the SAME iteration with its
/// idempotency records gone (double side effects). Order instead:
///   1. persist the advanced counter flagged `_reset_pending`,
///   2. reset the body subtree (idempotent),
///   3. persist the counter again without the flag.
///
/// A crash after (1) is finished by the top-of-handler recovery branch; a
/// crash before (1) leaves counter and terminal body consistent, so the
/// next tick simply redoes the advance. No iteration is ever lost or run
/// twice.
async fn advance_iteration(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node: &ExecutionNode,
    loop_def: &LoopDef,
    tree: &[ExecutionNode],
    next_iteration: u32,
) -> Result<(), EngineError> {
    storage
        .save_block_output(&iteration_marker(instance, loop_def, next_iteration, true))
        .await?;
    finish_iteration_reset(storage, instance, node, loop_def, tree, next_iteration).await
}

/// Steps 2 and 3 of [`advance_iteration`].
async fn finish_iteration_reset(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node: &ExecutionNode,
    loop_def: &LoopDef,
    tree: &[ExecutionNode],
    iteration: u32,
) -> Result<(), EngineError> {
    evaluator::reset_subtree_to_pending(storage, tree, &instance.tenant_id, instance.id, node.id)
        .await?;
    storage
        .save_block_output(&iteration_marker(instance, loop_def, iteration, false))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluator::reset_subtree_to_pending;
    use crate::expression::{evaluate_condition, is_truthy};
    use orch8_storage::{
        ExecutionTreeStore, InstanceStore, OutputStore, SequenceStore, WorkerStore,
        sqlite::SqliteStorage,
    };
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
            block_id: BlockId::new(block_id),
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
            block_id: BlockId::new(block),
            output: json!({ "_iterations": value }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        }
    }

    async fn seed_instance(s: &SqliteStorage, inst: InstanceId) {
        use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};
        let now = chrono::Utc::now();
        let seq = SequenceDefinition {
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            name: "test".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("noop"),
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
                compensation: None,
            }))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: now,
        };
        s.create_sequence(&seq).await.unwrap();
        let inst_row = orch8_types::instance::TaskInstance {
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

        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, outer.id)
            .await
            .unwrap();

        assert!(
            s.get_block_output(inst, &inner.block_id)
                .await
                .unwrap()
                .is_none()
        );
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

        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, outer.id)
            .await
            .unwrap();

        assert!(
            s.get_block_output(inst, &inner.block_id)
                .await
                .unwrap()
                .is_none()
        );
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

        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, outer.id)
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
            requirements: orch8_types::continuity::CapsuleRequirements::default(),
            params: json!({}),
            context: json!({}),
            attempt: 1,
            timeout_ms: None,
            state: WorkerTaskState::Pending,
            worker_id: None,
            claimed_at: None,
            heartbeat_at: None,
            claim_epoch: 0,
            resume_checkpoint: None,
            checkpoint_seq: 0,
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
        s.complete_worker_task(
            iter0.id,
            &orch8_types::worker::WorkerClaim::new("w1", 1),
            &json!({"ok": true}),
        )
        .await
        .unwrap();

        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, outer.id)
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
            requirements: orch8_types::continuity::CapsuleRequirements::default(),
            params: json!({}),
            context: json!({}),
            attempt: 1,
            timeout_ms: None,
            state: WorkerTaskState::Pending,
            worker_id: None,
            claimed_at: None,
            heartbeat_at: None,
            claim_epoch: 0,
            resume_checkpoint: None,
            checkpoint_seq: 0,
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

        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, l1.id)
            .await
            .unwrap();

        assert!(
            s.get_block_output(inst, &l2.block_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            s.get_block_output(inst, &l3.block_id)
                .await
                .unwrap()
                .is_none()
        );
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
        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, lp_a.id)
            .await
            .unwrap();

        assert!(
            s.get_block_output(inst, &lp_b.block_id)
                .await
                .unwrap()
                .is_some()
        );
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

        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, outer.id)
            .await
            .unwrap();
        // Second call should not error.
        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst, outer.id)
            .await
            .unwrap();

        assert!(
            s.get_block_output(inst, &inner.block_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    // ----- handler behavior tests (H1, H3, H4 — H2 lives in for_each.rs) -----

    fn mk_instance_for(inst_id: InstanceId, ctx: serde_json::Value) -> TaskInstance {
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
            budget: None,
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
            block_id: BlockId::new("body"),
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
        reset_subtree_to_pending(&s, &tree, &TenantId::unchecked("t"), inst_id, outer.id)
            .await
            .unwrap();

        let loop_def = LoopDef {
            id: BlockId::new("inner_loop"),
            condition: "true".into(),
            body: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: BlockId::new("body"),
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
                    compensation: None,
                },
            ))],
            max_iterations: 2,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        };
        let registry = HandlerRegistry::new();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        let inner_node = tree
            .iter()
            .find(|n| n.block_id.as_str() == "inner_loop")
            .unwrap()
            .clone();

        execute_loop(&s, &registry, &inst, &inner_node, &loop_def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        let step_after = after
            .iter()
            .find(|n| n.block_id.as_str() == "body")
            .unwrap();
        assert_eq!(
            step_after.state,
            NodeState::Running,
            "body child must be activated, not skipped via stale-counter cap"
        );
        let inner_after = after
            .iter()
            .find(|n| n.block_id.as_str() == "inner_loop")
            .unwrap();
        assert_ne!(
            inner_after.state,
            NodeState::Completed,
            "inner_loop must NOT short-circuit via stale marker"
        );

        // Step output preserved.
        assert!(
            s.get_block_output(inst_id, &BlockId::new("body"))
                .await
                .unwrap()
                .is_some()
        );
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
            id: BlockId::new("lp"),
            condition: "true".into(),
            body: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: BlockId::new("step_body"),
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
                    compensation: None,
                },
            ))],
            max_iterations: 3,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        };
        let registry = HandlerRegistry::new();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        let lp_node = tree
            .iter()
            .find(|n| n.block_id.as_str() == "lp")
            .unwrap()
            .clone();

        execute_loop(&s, &registry, &inst, &lp_node, &loop_def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        let lp_after = after.iter().find(|n| n.block_id.as_str() == "lp").unwrap();
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
            id: BlockId::new("lp"),
            condition: "true".into(),
            body: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: BlockId::new("body"),
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
                    compensation: None,
                },
            ))],
            max_iterations: 5,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        };
        let registry = HandlerRegistry::new();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        let lp_node = tree
            .iter()
            .find(|n| n.block_id.as_str() == "lp")
            .unwrap()
            .clone();

        execute_loop(&s, &registry, &inst, &lp_node, &loop_def, &tree)
            .await
            .unwrap();

        let after = s.get_execution_tree(inst_id).await.unwrap();
        let lp_after = after.iter().find(|n| n.block_id.as_str() == "lp").unwrap();
        assert_eq!(lp_after.state, NodeState::Failed);
        assert!(
            s.get_block_output(inst_id, &BlockId::new("lp"))
                .await
                .unwrap()
                .is_some()
        );
    }

    fn body_step(id: &str) -> orch8_types::sequence::BlockDefinition {
        orch8_types::sequence::BlockDefinition::Step(Box::new(orch8_types::sequence::StepDef {
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
            compensation: None,
        }))
    }

    fn two_step_loop(condition: &str, continue_on_error: bool) -> LoopDef {
        LoopDef {
            id: BlockId::new("lp"),
            condition: condition.into(),
            body: vec![body_step("a"), body_step("b")],
            max_iterations: 5,
            break_on: None,
            continue_on_error,
            poll_interval: None,
            retain_iterations: None,
        }
    }

    /// Seed `lp` (Running) with body `[a, b]` in the given states and run one
    /// loop tick. Returns the storage and the post-tick tree.
    async fn run_two_step_loop(
        loop_def: &LoopDef,
        ctx: serde_json::Value,
        a: NodeState,
        b: NodeState,
        marker: Option<serde_json::Value>,
    ) -> (SqliteStorage, InstanceId, Vec<ExecutionNode>) {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&s, inst_id).await;
        let inst = mk_instance_for(inst_id, ctx);
        let mut lp = mk_node(inst_id, "lp", BlockType::Loop, None);
        lp.state = NodeState::Running;
        let mut na = mk_node(inst_id, "a", BlockType::Step, Some(lp.id));
        na.state = a;
        let mut nb = mk_node(inst_id, "b", BlockType::Step, Some(lp.id));
        nb.state = b;
        s.create_execution_nodes_batch(&[lp.clone(), na, nb])
            .await
            .unwrap();
        if let Some(output) = marker {
            let mut m = mk_marker(inst_id, "lp", 0);
            m.output = output;
            s.save_block_output(&m).await.unwrap();
        }
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        execute_loop(&s, &HandlerRegistry::new(), &inst, &lp, loop_def, &tree)
            .await
            .unwrap();
        let after = s.get_execution_tree(inst_id).await.unwrap();
        (s, inst_id, after)
    }

    fn state_of(tree: &[ExecutionNode], block: &str) -> NodeState {
        tree.iter()
            .find(|n| n.block_id.as_str() == block)
            .unwrap()
            .state
    }

    /// ENG-C-N3: the condition is evaluated only at an iteration boundary. A
    /// condition that turns false mid-iteration must not complete the loop
    /// while body steps are still running.
    #[tokio::test]
    async fn condition_not_evaluated_mid_iteration() {
        let loop_def = two_step_loop("keep_going", false);
        let (_s, _id, after) = run_two_step_loop(
            &loop_def,
            json!({ "keep_going": false }),
            NodeState::Completed,
            NodeState::Running,
            Some(json!({ "_iterations": 0 })),
        )
        .await;
        assert_eq!(state_of(&after, "lp"), NodeState::Running);
        assert_eq!(state_of(&after, "b"), NodeState::Running);
    }

    /// At the boundary a false condition completes the loop and the
    /// never-started body is settled (no orphaned Pending nodes).
    #[tokio::test]
    async fn condition_false_at_boundary_completes_and_settles_body() {
        let loop_def = two_step_loop("keep_going", false);
        let (_s, _id, after) = run_two_step_loop(
            &loop_def,
            json!({ "keep_going": false }),
            NodeState::Pending,
            NodeState::Pending,
            None,
        )
        .await;
        assert_eq!(state_of(&after, "lp"), NodeState::Completed);
        assert_eq!(state_of(&after, "a"), NodeState::Skipped);
        assert_eq!(state_of(&after, "b"), NodeState::Skipped);
    }

    /// Run-past-failure: a failed body step stops the iteration — its
    /// successor never starts — and fails the loop.
    #[tokio::test]
    async fn failed_body_step_stops_iteration_and_fails_loop() {
        let loop_def = two_step_loop("true", false);
        let (_s, _id, after) = run_two_step_loop(
            &loop_def,
            json!({}),
            NodeState::Failed,
            NodeState::Pending,
            None,
        )
        .await;
        assert_eq!(state_of(&after, "lp"), NodeState::Failed);
        assert_eq!(
            state_of(&after, "b"),
            NodeState::Skipped,
            "successor of a failed step must never run"
        );
    }

    /// `continue_on_error` skips the rest of the failed iteration and
    /// advances to the next one.
    #[tokio::test]
    async fn continue_on_error_advances_without_running_rest_of_iteration() {
        let loop_def = two_step_loop("true", true);
        let (s, inst_id, after) = run_two_step_loop(
            &loop_def,
            json!({}),
            NodeState::Failed,
            NodeState::Pending,
            None,
        )
        .await;
        assert_eq!(state_of(&after, "lp"), NodeState::Running);
        assert_eq!(state_of(&after, "a"), NodeState::Pending);
        assert_eq!(state_of(&after, "b"), NodeState::Pending);
        let marker = s
            .get_block_output(inst_id, &BlockId::new("lp"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.output, json!({ "_iterations": 1 }));
    }

    /// ENG-C-N5: the advanced counter is written BEFORE the body reset
    /// (flagged `_reset_pending`). A crash between the two leaves the flag;
    /// the next tick must finish the reset without re-running or counting
    /// the iteration again.
    #[tokio::test]
    async fn interrupted_iteration_advance_is_finished_not_rerun() {
        let loop_def = two_step_loop("true", false);
        // Body still terminal from the finished iteration (reset never ran).
        let (s, inst_id, after) = run_two_step_loop(
            &loop_def,
            json!({}),
            NodeState::Completed,
            NodeState::Completed,
            Some(json!({ "_iterations": 2, "_reset_pending": true })),
        )
        .await;
        assert_eq!(state_of(&after, "lp"), NodeState::Running);
        assert_eq!(state_of(&after, "a"), NodeState::Pending);
        assert_eq!(state_of(&after, "b"), NodeState::Pending);
        let marker = s
            .get_block_output(inst_id, &BlockId::new("lp"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            marker.output,
            json!({ "_iterations": 2 }),
            "counter must not advance twice; flag cleared"
        );
    }

    /// ENG-C-N6: an absurd `poll_interval` must not overflow the timestamp.
    #[test]
    fn poll_fire_at_clamps_instead_of_overflowing() {
        let now = chrono::Utc::now();
        let fire = poll_fire_at(now, u64::MAX);
        assert!(fire > now);
        assert!(fire <= now + chrono::Duration::days(366));
        let near_max = chrono::DateTime::<chrono::Utc>::MAX_UTC - chrono::Duration::seconds(5);
        assert_eq!(
            poll_fire_at(near_max, 3600),
            chrono::DateTime::<chrono::Utc>::MAX_UTC
        );
        assert_eq!(poll_fire_at(now, 5), now + chrono::Duration::seconds(5));
    }
}
