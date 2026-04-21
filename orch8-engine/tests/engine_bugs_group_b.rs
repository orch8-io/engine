#![allow(clippy::too_many_lines)]
//! Engine bug reproducers — Group B.
//!
//! Group A covered per-branch cursor bugs surfaced by the e2e suite. Group B
//! covers the retry/backoff + persistence-contract issues found during the
//! external-worker-path triage:
//!
//!   Issue 3 — tree-path retryable failures previously left the node
//!             Running and returned Ok, causing the evaluator's
//!             `find_running_step` to re-dispatch the same node every
//!             iteration within a tick (hot loop), while also ignoring
//!             `retry.max_attempts` and any configured backoff. The fix
//!             mirrors the fast-path semantics in
//!             `scheduler::step_exec::handle_retryable_failure`.
//!
//!   Issue 28 — `apply_self_modify` with `position = None` (documented as
//!              "append") was overwriting every previously-injected block
//!              instead of extending the list. Broke the iterative agent
//!              self-extension pattern across multiple steps.
//!
//! Running only this file:
//!
//! ```text
//! cargo test -p orch8-engine --test engine_bugs_group_b
//! ```

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{Duration, Utc};
use serde_json::json;

use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, RetryPolicy, SequenceDefinition, StepDef};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn mk_retry(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_backoff: StdDuration::from_millis(50),
        max_backoff: StdDuration::from_secs(5),
        backoff_multiplier: 2.0,
    }
}

fn mk_step_with_retry(id: &str, handler: &str, retry: Option<RetryPolicy>) -> StepDef {
    StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry,
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
    }
}

fn mk_instance(seq_id: SequenceId) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
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
    }
}

/// Seed a single-step tree and return storage + instance + the step's
/// execution node. The node starts in `Running` state — the caller can
/// invoke `execute_step_node` directly against it.
async fn setup_single_step(
    step: StepDef,
) -> (
    Arc<dyn StorageBackend>,
    TaskInstance,
    ExecutionNode,
    StepDef,
) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let step_id = step.id.clone();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "bug-b-repro".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(step.clone()))],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();

    let instance = mk_instance(seq.id);
    storage.create_instance(&instance).await.unwrap();

    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance.id,
        block_id: step_id,
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&node).await.unwrap();

    let storage_dyn: Arc<dyn StorageBackend> = Arc::new(storage);
    (storage_dyn, instance, node, step)
}

// --------------------------------------------------------------------------
// Issue 3 — tree-path retryable failures must honour retry policy.
// --------------------------------------------------------------------------

/// Happy path for the new retry logic: a retryable failure with an
/// unexhausted retry policy must transition the INSTANCE to `Scheduled`
/// with a future `next_fire_at`, and leave the node Running so the next
/// `evaluate()` tick (after the scheduler picks the instance back up)
/// re-dispatches it. The critical assertion is the instance state change —
/// this is what breaks the evaluator's hot-spin loop.
#[tokio::test]
async fn b3_tree_retryable_reschedules_instance_with_backoff() {
    let step = mk_step_with_retry("s1", "flaky", Some(mk_retry(5)));
    let (storage, instance, node, step_def) = setup_single_step(step).await;

    let mut registry = HandlerRegistry::new();
    registry.set_mock_error("flaky", "transient boom".into(), true /* retryable */);

    let outputs = orch8_engine::handlers::param_resolve::OutputsSnapshot::new();
    let before = Utc::now();
    let res = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;
    assert!(res.is_ok(), "retryable path should not surface an error");

    // Instance must now be Scheduled with fire_at in the future.
    let reloaded = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(
        reloaded.state,
        InstanceState::Scheduled,
        "retryable failure must re-schedule the instance (was Running, now Scheduled)"
    );
    let fire_at = reloaded
        .next_fire_at
        .expect("rescheduled instance must carry a next_fire_at");
    assert!(
        fire_at >= before,
        "fire_at must be in the future relative to the dispatch timestamp"
    );
    // Backoff for attempt=0 with initial=50ms, multiplier=2.0 ≈ 50ms. Allow
    // up to 1s to accommodate jitter + CI clocks.
    let delay = fire_at - before;
    assert!(
        delay <= Duration::seconds(1),
        "backoff for first attempt should be ~50ms, got {} ms",
        delay.num_milliseconds()
    );

    // A retry marker must have been persisted so `compute_attempt` advances
    // on the next tick. Without this, every tick would see attempt=0 and
    // never trip max_attempts.
    let latest = storage
        .get_block_output(instance.id, &BlockId("s1".into()))
        .await
        .unwrap()
        .expect("retry marker output must be persisted");
    assert_eq!(latest.attempt, 0, "marker carries the just-failed attempt");
    assert_eq!(
        latest.output_ref.as_deref(),
        Some("__retry__"),
        "marker tagged with __retry__ so recovery can identify it"
    );
}

/// Retry policy exhaustion: when `attempt >= max_attempts` the node must be
/// transitioned to `Failed` and the instance must NOT be re-scheduled.
/// Mirrors the fast-path's "max retry attempts exhausted" branch. We drive
/// this by pre-seeding a prior output whose `attempt` field is already at
/// the limit, so `compute_attempt` returns `max_attempts` on this call.
#[tokio::test]
async fn b3_tree_retryable_fails_node_when_attempts_exhausted() {
    let step = mk_step_with_retry("s1", "flaky", Some(mk_retry(2)));
    let (storage, instance, node, step_def) = setup_single_step(step).await;

    // Seed a prior retry marker so compute_attempt → max_attempts on the
    // next call.
    let marker = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        block_id: BlockId("s1".into()),
        output: json!({"_retry_marker": true}),
        output_ref: Some("__retry__".into()),
        output_size: 0,
        attempt: 1, // next compute_attempt returns 2 == max_attempts
        created_at: Utc::now(),
    };
    storage.save_block_output(&marker).await.unwrap();

    let mut registry = HandlerRegistry::new();
    registry.set_mock_error("flaky", "still bad".into(), true);

    let outputs = orch8_engine::handlers::param_resolve::OutputsSnapshot::new();
    orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await
    .unwrap();

    // Node must now be Failed.
    let fetched_node = storage
        .get_execution_tree(instance.id)
        .await
        .unwrap()
        .into_iter()
        .find(|n| n.id == node.id)
        .expect("node exists");
    assert_eq!(
        fetched_node.state,
        NodeState::Failed,
        "exhausted retry policy must fail the node"
    );

    // Instance must remain Running (the evaluator owns terminal transition
    // once the root-level nodes settle — retryable exhaustion is at
    // node-granularity, not instance-granularity).
    let reloaded = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(
        reloaded.state,
        InstanceState::Running,
        "node-level exhaustion must not directly transition the instance"
    );
    assert!(
        reloaded.next_fire_at.is_none(),
        "exhausted node must not schedule a backoff retry"
    );
}

/// No retry policy configured: a retryable failure with `retry=None` must
/// fail the node (mirroring the fast-path's no-retry-policy fallthrough in
/// `handle_retryable_failure`). Previously this returned Ok and left the
/// node Running, hot-spinning forever inside one tick.
#[tokio::test]
async fn b3_tree_retryable_no_policy_fails_node() {
    let step = mk_step_with_retry("s1", "flaky", None);
    let (storage, instance, node, step_def) = setup_single_step(step).await;

    let mut registry = HandlerRegistry::new();
    registry.set_mock_error("flaky", "transient".into(), true);

    let outputs = orch8_engine::handlers::param_resolve::OutputsSnapshot::new();
    orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await
    .unwrap();

    let fetched_node = storage
        .get_execution_tree(instance.id)
        .await
        .unwrap()
        .into_iter()
        .find(|n| n.id == node.id)
        .expect("node exists");
    assert_eq!(
        fetched_node.state,
        NodeState::Failed,
        "no retry policy + retryable failure must fail the node (mirrors fast path)"
    );
}

// --------------------------------------------------------------------------
// Issue 28 — `apply_self_modify(position=None)` must append, not replace.
// --------------------------------------------------------------------------

fn mk_injectable_step(id: &str) -> serde_json::Value {
    serde_json::to_value(BlockDefinition::Step(Box::new(mk_step_with_retry(
        id, "noop", None,
    ))))
    .unwrap()
}

/// Two successive `self_modify` outputs with `position = None` must merge —
/// the second call must not delete the first call's blocks. Previously the
/// positionless path did `final_blocks = blocks.clone()`, overwriting the
/// entire injected list every time.
#[tokio::test]
async fn b28_apply_self_modify_append_preserves_prior_blocks() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let storage_dyn: Arc<dyn StorageBackend> = Arc::new(storage);

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "sm-append".into(),
        version: 1,
        deprecated: false,
        blocks: vec![],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage_dyn.create_sequence(&seq).await.unwrap();
    let instance = mk_instance(seq.id);
    storage_dyn.create_instance(&instance).await.unwrap();

    // Simulate output from the first self_modify step (append, no position).
    let out1 = serde_json::json!({
        "_self_modify": true,
        "blocks": [mk_injectable_step("first")],
        "position": serde_json::Value::Null,
    });
    let applied1 =
        orch8_engine::handlers::param_resolve::apply_self_modify(&*storage_dyn, instance.id, &out1)
            .await;
    assert_eq!(
        applied1,
        orch8_engine::handlers::param_resolve::SelfModifyResult::Applied,
        "first self_modify should apply"
    );

    // Second call, also with position = None — should APPEND `second`, not
    // replace `first`.
    let out2 = serde_json::json!({
        "_self_modify": true,
        "blocks": [mk_injectable_step("second")],
        "position": serde_json::Value::Null,
    });
    let applied2 =
        orch8_engine::handlers::param_resolve::apply_self_modify(&*storage_dyn, instance.id, &out2)
            .await;
    assert_eq!(
        applied2,
        orch8_engine::handlers::param_resolve::SelfModifyResult::Applied,
        "second self_modify should apply"
    );

    let injected = storage_dyn
        .get_injected_blocks(instance.id)
        .await
        .unwrap()
        .expect("injected_blocks must be present");
    let arr = injected.as_array().expect("injected is an array");
    assert_eq!(
        arr.len(),
        2,
        "append must preserve the first block and add the second; got: {injected:?}"
    );
    // Order matters for the agent pattern — first-injected stays first.
    let id_of = |v: &serde_json::Value| -> String {
        // BlockDefinition serializes with `#[serde(tag = "type")]`, so a
        // Step ends up as `{"type":"step","id":"...",...}` (no wrapper).
        v.get("id")
            .and_then(|i| i.as_str())
            .unwrap_or("")
            .to_string()
    };
    assert_eq!(id_of(&arr[0]), "first");
    assert_eq!(id_of(&arr[1]), "second");
}

/// Position-specific inserts must also preserve existing injected blocks
/// (this path was already correct; pinned here to catch any regression in
/// the shared prelude that now feeds both branches).
#[tokio::test]
async fn b28_apply_self_modify_position_still_preserves_prior() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let storage_dyn: Arc<dyn StorageBackend> = Arc::new(storage);

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "sm-pos".into(),
        version: 1,
        deprecated: false,
        blocks: vec![],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage_dyn.create_sequence(&seq).await.unwrap();
    let instance = mk_instance(seq.id);
    storage_dyn.create_instance(&instance).await.unwrap();

    let out1 = serde_json::json!({
        "_self_modify": true,
        "blocks": [mk_injectable_step("a"), mk_injectable_step("c")],
        "position": serde_json::Value::Null,
    });
    orch8_engine::handlers::param_resolve::apply_self_modify(&*storage_dyn, instance.id, &out1)
        .await;

    // Insert at index 1 — expect [a, b, c].
    let out2 = serde_json::json!({
        "_self_modify": true,
        "blocks": [mk_injectable_step("b")],
        "position": 1,
    });
    orch8_engine::handlers::param_resolve::apply_self_modify(&*storage_dyn, instance.id, &out2)
        .await;

    let injected = storage_dyn
        .get_injected_blocks(instance.id)
        .await
        .unwrap()
        .unwrap();
    let arr = injected.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    let ids: Vec<String> = arr
        .iter()
        .map(|v| {
            v.get("id")
                .and_then(|i| i.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

/// Regression target: before the fix, calling `execute_step_node` twice in
/// quick succession against a still-Running node hot-spun — both calls
/// returned Ok(true) and left the node Running. After the fix, the first
/// call transitions the instance to Scheduled; a hypothetical second call
/// within the same tick would be prevented by the evaluator's top-of-loop
/// instance-state guard.
///
/// This test asserts the state-change side of the contract directly — the
/// evaluator guard itself has coverage via the full-tick tests in
/// `e2e_coverage.rs`.
#[tokio::test]
async fn b3_tree_retryable_breaks_hot_loop_via_instance_state() {
    let step = mk_step_with_retry("s1", "flaky", Some(mk_retry(10)));
    let (storage, instance, node, step_def) = setup_single_step(step).await;

    let mut registry = HandlerRegistry::new();
    registry.set_mock_error("flaky", "retry me".into(), true);

    let outputs = orch8_engine::handlers::param_resolve::OutputsSnapshot::new();
    orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await
    .unwrap();

    // After a single retryable failure, instance state is Scheduled —
    // the evaluator's top-of-loop re-read would see this and exit the
    // tick instead of re-dispatching.
    let reloaded = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_ne!(
        reloaded.state,
        InstanceState::Running,
        "instance must leave Running after retryable failure so evaluator exits (was the hot-loop cause)"
    );
}
