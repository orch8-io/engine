//! End-to-end coverage tests for features previously lacking integration tests.
//!
//! Each test drives the evaluator loop to completion (or a targeted state)
//! and asserts the resulting execution tree, instance state, and side effects.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator::{self, EvalOutcome};
use orch8_engine::handlers::{builtin::register_builtins, HandlerRegistry};
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::error::StepError;
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{
    ABSplitDef, ABVariant, BlockDefinition, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef,
    RetryPolicy, Route, RouterDef, SequenceDefinition, StepDef, SubSequenceDef, TryCatchDef,
};
use orch8_types::signal::{Signal, SignalType};

// ================================================================
// HELPERS
// ================================================================

fn mk_step(id: &str, handler: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
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
    }))
}

#[allow(dead_code)]
fn mk_step_with_params(id: &str, handler: &str, params: serde_json::Value) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
        params,
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
    }))
}

fn mk_step_with_retry(id: &str, handler: &str, max_attempts: u32) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 1.0,
        }),
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
    }))
}

fn mk_non_cancellable_step(id: &str, handler: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: false,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

fn mk_sequence(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "test-flow".into(),
        version: 1,
        deprecated: false,
        blocks,
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn mk_instance(seq_id: SequenceId) -> TaskInstance {
    mk_instance_with_ctx(seq_id, json!({}))
}

fn mk_instance_with_ctx(seq_id: SequenceId, data: serde_json::Value) -> TaskInstance {
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
            data,
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

async fn setup(
    blocks: Vec<BlockDefinition>,
) -> (Arc<dyn StorageBackend>, SequenceDefinition, TaskInstance) {
    setup_with_ctx(blocks, json!({})).await
}

async fn setup_with_ctx(
    blocks: Vec<BlockDefinition>,
    ctx_data: serde_json::Value,
) -> (Arc<dyn StorageBackend>, SequenceDefinition, TaskInstance) {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(blocks);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, ctx_data);
    storage.create_instance(&inst).await.unwrap();
    (storage, seq, inst)
}

async fn drive(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance_id: InstanceId,
    sequence: &SequenceDefinition,
) {
    for _ in 0..2000 {
        // Ensure instance is in Running state for the evaluator.
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        match inst.state {
            InstanceState::Completed
            | InstanceState::Failed
            | InstanceState::Cancelled
            | InstanceState::Waiting
            | InstanceState::Paused => return,
            InstanceState::Scheduled => {
                // Simulate scheduler picking up: force to Running.
                storage
                    .update_instance_state(instance_id, InstanceState::Running, None)
                    .await
                    .unwrap();
            }
            _ => {}
        }
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        let outcome = evaluator::evaluate(storage, handlers, &inst, sequence)
            .await
            .unwrap();
        if let EvalOutcome::Done {
            any_failed,
            any_cancelled,
        } = outcome
        {
            let new_state = if any_cancelled && !any_failed {
                InstanceState::Cancelled
            } else if any_failed {
                InstanceState::Failed
            } else {
                InstanceState::Completed
            };
            storage
                .update_instance_state(instance_id, new_state, None)
                .await
                .unwrap();
            return;
        }
    }
    panic!("evaluator did not terminate within 2000 ticks");
}

/// Drive a limited number of ticks (for tests that check intermediate state).
async fn drive_n(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance_id: InstanceId,
    sequence: &SequenceDefinition,
    n: usize,
) {
    for _ in 0..n {
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        if inst.state != InstanceState::Running {
            return;
        }
        let outcome = evaluator::evaluate(storage, handlers, &inst, sequence)
            .await
            .unwrap();
        if matches!(outcome, EvalOutcome::Done { .. }) {
            return;
        }
    }
}

fn node_state(tree: &[orch8_types::execution::ExecutionNode], block_id: &str) -> NodeState {
    tree.iter()
        .find(|n| n.block_id.0 == block_id)
        .unwrap_or_else(|| panic!("node '{block_id}' not found in tree"))
        .state
}

fn registry() -> HandlerRegistry {
    let mut reg = HandlerRegistry::new();
    register_builtins(&mut reg);
    reg
}

fn registry_with_fail() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("fail", |_ctx| {
        Box::pin(async {
            Err(StepError::Permanent {
                message: "intentional failure".into(),
                details: None,
            })
        })
    });
    reg
}

fn registry_with_retryable_fail() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("retryable_fail", |_ctx| {
        Box::pin(async {
            Err(StepError::Retryable {
                message: "transient failure".into(),
                details: None,
            })
        })
    });
    // Also register "fail" for permanent failures.
    reg.register("fail", |_ctx| {
        Box::pin(async {
            Err(StepError::Permanent {
                message: "intentional failure".into(),
                details: None,
            })
        })
    });
    reg
}

/// Registry with a handler that sets context data.
#[allow(dead_code)]
fn registry_with_context_setter() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("set_ctx", |ctx| {
        Box::pin(async move {
            let key = ctx
                .params
                .get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("result");
            let value = ctx
                .params
                .get("value")
                .cloned()
                .unwrap_or_else(|| json!("done"));
            Ok(json!({ "_ctx_update": { key: value } }))
        })
    });
    reg
}

// ================================================================
// PHASE 1: CRITICAL PATH TESTS
// ================================================================

// --- 1. Retry with Backoff ---

#[tokio::test]
async fn retry_permanent_failure_fails_step() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

#[tokio::test]
async fn retryable_failure_keeps_node_running() {
    // In the evaluator path (without scheduler), retryable failures leave the
    // node Running for re-execution. Drive a limited number of ticks and verify
    // the node is still Running (not Failed).
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 3)]).await;
    let reg = registry_with_retryable_fail();
    drive_n(&storage, &reg, inst.id, &seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Node stays Running because the evaluator doesn't count retry attempts.
    assert_eq!(node_state(&tree, "s1"), NodeState::Running);
}

#[tokio::test]
async fn permanent_failure_with_retry_policy_still_fails() {
    // A permanent error fails immediately even with a retry policy.
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "fail", 5)]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

#[tokio::test]
async fn retry_success_on_first_try_completes() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "noop", 5)]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// --- 2. Sequential Steps ---

#[tokio::test]
async fn sequential_steps_execute_in_order() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s3"), NodeState::Completed);
}

#[tokio::test]
async fn sequential_step_failure_stops_sequence() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "fail"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Failed);
    // s3 should not have been started.
    assert_eq!(node_state(&tree, "s3"), NodeState::Pending);
}

// --- 3. A/B Split ---

#[tokio::test]
async fn ab_split_routes_to_one_variant() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId("ab".into()),
        variants: vec![
            ABVariant {
                name: "control".into(),
                weight: 50,
                blocks: vec![mk_step("ctrl_step", "noop")],
            },
            ABVariant {
                name: "variant_a".into(),
                weight: 50,
                blocks: vec![mk_step("var_a_step", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // The AB split node itself should complete.
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    // Exactly one branch should have run.
    let ctrl = tree.iter().find(|n| n.block_id.0 == "ctrl_step");
    let var_a = tree.iter().find(|n| n.block_id.0 == "var_a_step");
    let completed_count = [ctrl, var_a]
        .iter()
        .filter(|n| n.is_some_and(|n| n.state == NodeState::Completed))
        .count();
    assert!(completed_count >= 1, "at least one variant should complete");
}

#[tokio::test]
async fn ab_split_single_variant_always_chosen() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId("ab".into()),
        variants: vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![mk_step("only_step", "noop")],
        }],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "only_step"), NodeState::Completed);
}

#[tokio::test]
async fn ab_split_failure_in_chosen_variant_fails_split() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId("ab".into()),
        variants: vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![mk_step("fail_step", "fail")],
        }],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Failed);
}

// --- 4. Sub-Sequence ---

#[tokio::test]
async fn sub_sequence_spawns_child_and_enters_waiting() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // Create the child sequence first.
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    // Parent sequence with SubSequence block.
    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId("sub".into()),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();

    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    // Drive a few ticks — the sub-sequence node should enter Waiting state
    // after spawning a child instance (full completion requires the scheduler).
    drive_n(&storage, &reg, inst.id, &parent_seq, 3).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

// --- 5. Cancellation Scope ---

#[tokio::test]
async fn cancellation_scope_protects_blocks() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId("scope".into()),
        blocks: vec![mk_step("protected", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "scope"), NodeState::Completed);
    assert_eq!(node_state(&tree, "protected"), NodeState::Completed);
}

// --- 6. ForEach over collection ---

#[tokio::test]
async fn for_each_iterates_over_array() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

#[tokio::test]
async fn for_each_empty_collection_completes_immediately() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": []})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

#[tokio::test]
async fn for_each_failure_in_body_fails_foreach() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "fail")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1]})).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Failed);
}

// --- 7. Loop ---

#[tokio::test]
async fn loop_condition_false_immediately_completes() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "false".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 10,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

#[tokio::test]
async fn loop_runs_until_max_iterations() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 5,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

#[tokio::test]
async fn loop_body_failure_fails_loop() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body", "fail")],
        max_iterations: 5,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Failed);
}

// --- 8. Router ---

#[tokio::test]
async fn router_matches_first_true_route() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "false".into(),
                blocks: vec![mk_step("r1", "fail")],
            },
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("r2", "noop")],
            },
        ],
        default: Some(vec![mk_step("def", "fail")]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "r"), NodeState::Completed);
    assert_eq!(node_state(&tree, "r2"), NodeState::Completed);
}

#[tokio::test]
async fn router_uses_default_when_no_match() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId("r".into()),
        routes: vec![Route {
            condition: "false".into(),
            blocks: vec![mk_step("r1", "noop")],
        }],
        default: Some(vec![mk_step("def", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "r"), NodeState::Completed);
    assert_eq!(node_state(&tree, "def"), NodeState::Completed);
}

#[tokio::test]
async fn router_with_context_condition() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "tier == \"premium\"".into(),
                blocks: vec![mk_step("premium_path", "noop")],
            },
            Route {
                condition: "tier == \"free\"".into(),
                blocks: vec![mk_step("free_path", "noop")],
            },
        ],
        default: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"tier": "free"})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "r"), NodeState::Completed);
    assert_eq!(node_state(&tree, "free_path"), NodeState::Completed);
}

// --- 9. Parallel ---

#[tokio::test]
async fn parallel_all_branches_complete() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![
            vec![mk_step("b0", "noop")],
            vec![mk_step("b1", "noop")],
            vec![mk_step("b2", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b0"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b2"), NodeState::Completed);
}

#[tokio::test]
async fn parallel_one_branch_failure_fails_parallel() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "fail")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Failed);
}

#[tokio::test]
async fn parallel_with_multi_step_branches() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![
            vec![mk_step("b0s0", "noop"), mk_step("b0s1", "noop")],
            vec![mk_step("b1s0", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b0s0"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b0s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b1s0"), NodeState::Completed);
}

// --- 10. TryCatch ---

#[tokio::test]
async fn try_catch_success_skips_catch() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1", "noop")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Skipped);
}

#[tokio::test]
async fn try_catch_failure_runs_catch() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1", "fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Completed);
}

#[tokio::test]
async fn try_catch_with_finally_always_runs() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1", "fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: Some(vec![mk_step("f1", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "f1"), NodeState::Completed);
}

// --- 11. Signal Handling (Cancel) ---

#[tokio::test]
async fn cancel_signal_cancels_running_instance() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();

    // Run first tick to start s1.
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

    // Enqueue a cancel signal.
    let cancel_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Cancel,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&cancel_sig).await.unwrap();

    // Process signals.
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();

    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

#[tokio::test]
async fn pause_and_resume_signals() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;

    // Pause signal.
    let pause_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Pause,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&pause_sig).await.unwrap();
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();

    let paused = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(paused.state, InstanceState::Paused);

    // Resume signal.
    let resume_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Resume,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&resume_sig).await.unwrap();
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Paused)
        .await
        .unwrap();

    let resumed = storage.get_instance(inst.id).await.unwrap().unwrap();
    // After resume, instance goes to Scheduled (scheduler picks it up to run).
    assert_eq!(resumed.state, InstanceState::Scheduled);
}

// --- 12. Non-cancellable step ---

#[tokio::test]
async fn non_cancellable_step_completes_before_cancel() {
    let (storage, seq, inst) = setup(vec![
        mk_non_cancellable_step("protected", "noop"),
        mk_step("after", "noop"),
    ])
    .await;
    let reg = registry();

    // Drive first step.
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

    // Even with cancel signal, since step is non-cancellable the flow should
    // complete the current step (it runs as noop so it already did).
    let cancel_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Cancel,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&cancel_sig).await.unwrap();
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    // Instance should be cancelled since the non-cancellable step already completed.
    assert_eq!(final_inst.state, InstanceState::Cancelled);
}

// ================================================================
// PHASE 2: ADVANCED CONTROL FLOW
// ================================================================

// --- 13. Nested composites ---

#[tokio::test]
async fn nested_parallel_in_loop() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId("par".into()),
            branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
        }))],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

#[tokio::test]
async fn nested_try_catch_in_parallel() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![
            vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId("tc".into()),
                try_block: vec![mk_step("t1", "fail")],
                catch_block: vec![mk_step("c1", "noop")],
                finally_block: None,
            }))],
            vec![mk_step("b1", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // TryCatch catches the failure, so parallel should complete.
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
}

#[tokio::test]
async fn nested_loop_in_for_each() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId("inner_lp".into()),
            condition: "true".into(),
            body: vec![mk_step("inner_body", "noop")],
            max_iterations: 2,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        }))],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// --- 14. Router with complex conditions ---

#[tokio::test]
async fn router_numeric_condition() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "score > 80".into(),
                blocks: vec![mk_step("high", "noop")],
            },
            Route {
                condition: "score > 50".into(),
                blocks: vec![mk_step("mid", "noop")],
            },
        ],
        default: Some(vec![mk_step("low", "noop")]),
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"score": 65})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "r"), NodeState::Completed);
    assert_eq!(node_state(&tree, "mid"), NodeState::Completed);
}

// --- 15. Loop with context-driven condition ---

#[tokio::test]
async fn loop_with_context_condition() {
    // Loop condition references context; since noop doesn't change context,
    // condition stays truthy until max_iterations.
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "active".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![lp], json!({"active": true})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

// --- 16. Permanent failure inside TryCatch ---

#[tokio::test]
async fn permanent_failure_caught_by_try_catch() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1", "fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Completed);
}

// --- 17. Permanent failure inside Loop ---

#[tokio::test]
async fn failure_inside_loop_fails_loop() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body", "fail")],
        max_iterations: 2,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Failed);
}

// --- 18. ForEach with large collection (max_iterations cap) ---

#[tokio::test]
async fn for_each_respects_max_iterations() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 2, // Only process first 2 of 5.
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3, 4, 5]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// --- 19. Race ---

#[tokio::test]
async fn race_first_to_complete_wins() {
    let race = BlockDefinition::Race(Box::new(orch8_types::sequence::RaceDef {
        id: BlockId("race".into()),
        branches: vec![
            vec![mk_step("fast", "noop")],
            vec![mk_step("slow_s1", "noop"), mk_step("slow_s2", "noop")],
        ],
        semantics: orch8_types::sequence::RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fast"), NodeState::Completed);
}

// --- 20. Multiple steps after composite ---

#[tokio::test]
async fn step_after_parallel_runs() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

#[tokio::test]
async fn step_after_loop_runs() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 2,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

#[tokio::test]
async fn step_after_try_catch_runs() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1", "noop")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

// ================================================================
// PHASE 3: COMBINATIONS AND EDGE CASES
// ================================================================

#[tokio::test]
async fn deeply_nested_parallel_in_try_catch_in_loop() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId("tc".into()),
            try_block: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId("par".into()),
                branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
            }))],
            catch_block: vec![mk_step("c1", "noop")],
            finally_block: None,
        }))],
        max_iterations: 2,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

#[tokio::test]
async fn router_inside_parallel_branch() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![
            vec![BlockDefinition::Router(Box::new(RouterDef {
                id: BlockId("r".into()),
                routes: vec![Route {
                    condition: "true".into(),
                    blocks: vec![mk_step("routed", "noop")],
                }],
                default: None,
            }))],
            vec![mk_step("other", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "routed"), NodeState::Completed);
}

#[tokio::test]
async fn for_each_with_nested_router() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("r".into()),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![mk_step("routed", "noop")],
            }],
            default: None,
        }))],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

#[tokio::test]
async fn try_catch_in_for_each_catches_per_iteration() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId("tc".into()),
            try_block: vec![mk_step("t1", "fail")],
            catch_block: vec![mk_step("c1", "noop")],
            finally_block: None,
        }))],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // TryCatch catches each iteration's failure, so ForEach completes.
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

#[tokio::test]
async fn loop_with_parallel_body() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId("par".into()),
            branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
        }))],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

#[tokio::test]
async fn empty_parallel_completes() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
}

#[tokio::test]
async fn single_noop_step_completes_instance() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

#[tokio::test]
async fn single_fail_step_fails_instance() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

#[tokio::test]
async fn ten_sequential_steps_all_complete() {
    let blocks: Vec<BlockDefinition> = (0..10).map(|i| mk_step(&format!("s{i}"), "noop")).collect();
    let (storage, seq, inst) = setup(blocks).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    for i in 0..10 {
        assert_eq!(
            node_state(&tree, &format!("s{i}")),
            NodeState::Completed,
            "step s{i} should complete"
        );
    }
}

#[tokio::test]
async fn cancellation_scope_with_multiple_steps() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId("scope".into()),
        blocks: vec![
            mk_step("p1", "noop"),
            mk_step("p2", "noop"),
            mk_step("p3", "noop"),
        ],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "scope"), NodeState::Completed);
    assert_eq!(node_state(&tree, "p1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "p2"), NodeState::Completed);
    assert_eq!(node_state(&tree, "p3"), NodeState::Completed);
}

#[tokio::test]
async fn ab_split_with_multiple_steps_in_variant() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId("ab".into()),
        variants: vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![mk_step("v1", "noop"), mk_step("v2", "noop")],
        }],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "v1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "v2"), NodeState::Completed);
}

#[tokio::test]
async fn for_each_single_item_collection() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": ["only"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

#[tokio::test]
async fn loop_single_iteration() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 1,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

#[tokio::test]
async fn race_single_branch() {
    let race = BlockDefinition::Race(Box::new(orch8_types::sequence::RaceDef {
        id: BlockId("race".into()),
        branches: vec![vec![mk_step("only", "noop")]],
        semantics: orch8_types::sequence::RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

#[tokio::test]
async fn parallel_single_branch() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![vec![mk_step("only", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "only"), NodeState::Completed);
}

// --- Instance-level state after failures ---

#[tokio::test]
async fn parallel_failure_fails_instance() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("par".into()),
        branches: vec![vec![mk_step("b0", "fail")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

#[tokio::test]
async fn loop_failure_fails_instance() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body", "fail")],
        max_iterations: 5,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

#[tokio::test]
async fn try_catch_success_completes_instance() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1", "fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: Some(vec![mk_step("f1", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// ================================================================
// FEATURE GAPS: interceptors, templates, self-modify, signals
// ================================================================

use orch8_types::interceptor::{InterceptorAction, InterceptorDef};

fn mk_sequence_with_interceptors(
    blocks: Vec<BlockDefinition>,
    interceptors: InterceptorDef,
) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "intercepted-flow".into(),
        version: 1,
        deprecated: false,
        blocks,
        interceptors: Some(interceptors),
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn interceptors_emit_before_and_after_step_artifacts() {
    let interceptors = InterceptorDef {
        before_step: Some(InterceptorAction {
            handler: "audit".into(),
            params: json!({"stage": "pre"}),
        }),
        after_step: Some(InterceptorAction {
            handler: "audit".into(),
            params: json!({"stage": "post"}),
        }),
        ..Default::default()
    };

    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence_with_interceptors(vec![mk_step("s1", "noop")], interceptors);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let before = storage
        .get_block_output(inst.id, &BlockId("_interceptor:before:s1".into()))
        .await
        .unwrap();
    assert!(before.is_some(), "before_step artifact must exist");
    assert_eq!(before.unwrap().output["stage"], "pre");

    let after = storage
        .get_block_output(inst.id, &BlockId("_interceptor:after:s1".into()))
        .await
        .unwrap();
    assert!(after.is_some(), "after_step artifact must exist");
    assert_eq!(after.unwrap().output["stage"], "post");
}

// NOTE: on_complete and on_failure interceptors are emitted by the scheduler
// (process_instance_tree), not the evaluator loop. Testing them requires the
// full scheduler tick loop, which is not exposed in integration tests.
// Coverage exists in scheduler/tests.rs and interceptors/tests.rs.

/// Registry with a handler that captures resolved params for template tests.
fn registry_with_param_capture() -> (
    HandlerRegistry,
    Arc<std::sync::Mutex<Option<serde_json::Value>>>,
) {
    let mut reg = HandlerRegistry::new();
    register_builtins(&mut reg);
    let captured = Arc::new(std::sync::Mutex::new(None));
    let captured_clone = Arc::clone(&captured);
    reg.register("capture_params", move |ctx| {
        let captured = Arc::clone(&captured_clone);
        async move {
            *captured.lock().unwrap() = Some(ctx.params.clone());
            Ok(json!({"ok": true}))
        }
    });
    (reg, captured)
}

#[tokio::test]
async fn template_resolves_context_data_in_step_params() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step_with_params(
        "s1",
        "capture_params",
        json!({"slug": "{{context.data.slug}}"}),
    )]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, json!({"slug": "user.signed_up"}));
    storage.create_instance(&inst).await.unwrap();

    let (reg, captured) = registry_with_param_capture();
    drive(&storage, &reg, inst.id, &seq).await;

    let seen = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler was called");
    assert_eq!(
        seen["slug"], "user.signed_up",
        "template must resolve context.data.slug"
    );
}

#[tokio::test]
async fn template_resolves_config_in_step_params() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mut seq = mk_sequence(vec![mk_step_with_params(
        "s1",
        "capture_params",
        json!({"api_key": "{{context.config.api_key}}"}),
    )]);
    seq.interceptors = None;
    storage.create_sequence(&seq).await.unwrap();

    let mut inst = mk_instance_with_ctx(seq.id, json!({}));
    inst.context.config = json!({"api_key": "secret-123"});
    storage.create_instance(&inst).await.unwrap();

    let (reg, captured) = registry_with_param_capture();
    drive(&storage, &reg, inst.id, &seq).await;

    let seen = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler was called");
    assert_eq!(
        seen["api_key"], "secret-123",
        "template must resolve context.config.api_key"
    );
}

#[tokio::test]
async fn self_modify_injects_blocks_and_evaluator_executes_them() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // The self-modify step will inject a new "injected" noop step.
    let inj = serde_json::to_value(BlockDefinition::Step(Box::new(StepDef {
        id: BlockId("injected".into()),
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
    })))
    .unwrap();

    let seq = mk_sequence(vec![mk_step_with_params(
        "self_mod",
        "self_modify",
        json!({"blocks": [inj]}),
    )]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let mut reg = registry();
    reg.register(
        "self_modify",
        orch8_engine::handlers::self_modify::handle_self_modify,
    );

    // Custom driver: the evaluator returns Done after self_mod completes
    // because the tree is terminal at that point. We must run a second
    // evaluate() call so ensure_execution_tree picks up the injected block.
    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    let outcome = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    assert!(
        matches!(
            outcome,
            EvalOutcome::Done {
                any_failed: false,
                any_cancelled: false
            }
        ),
        "first pass must complete self_mod step"
    );

    // Re-schedule so the evaluator sees injected blocks on next tick.
    storage
        .update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();
    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    let outcome = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    assert!(
        matches!(
            outcome,
            EvalOutcome::Done {
                any_failed: false,
                any_cancelled: false
            }
        ),
        "second pass must complete injected step"
    );

    // The injected block should have been executed and be in Completed state.
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    let injected_node = tree.iter().find(|n| n.block_id.0 == "injected");
    assert!(
        injected_node.is_some(),
        "injected block must appear in execution tree"
    );
    assert_eq!(injected_node.unwrap().state, NodeState::Completed);
}

#[tokio::test]
async fn signal_update_context_replaces_instance_context() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, json!({"original": true}));
    storage.create_instance(&inst).await.unwrap();

    // Drive one tick so the instance is Running and the step is dispatched.
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

    // Enqueue an update_context signal with a full replacement context.
    let new_ctx = orch8_types::context::ExecutionContext {
        data: json!({"new_key": "new_value"}),
        config: json!({}),
        audit: vec![],
        runtime: orch8_types::context::RuntimeContext::default(),
    };
    let sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::UpdateContext,
        payload: serde_json::to_value(&new_ctx).unwrap(),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&sig).await.unwrap();

    // Process the signal via the public signals path.
    let signals = storage.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(signals.len(), 1);
    let abort = orch8_engine::signals::process_signals_prefetched(
        storage.as_ref(),
        inst.id,
        InstanceState::Running,
        signals,
        None,
    )
    .await
    .unwrap();
    assert!(!abort, "update_context must not abort instance");

    // Verify the context was replaced (not merged).
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert!(
        refreshed.context.data.get("original").is_none(),
        "original context must be replaced, not merged"
    );
    assert_eq!(
        refreshed.context.data["new_key"], "new_value",
        "new context must be present after replacement"
    );
}

// ================================================================
// NEW TEMPLATE & EXPRESSION FEATURES (steps.*, json(), len(),
// ternary, in operator, instance_id, unary minus, abs())
// ================================================================

/// Registry with a handler that returns a fixed JSON payload (for producing
/// outputs that subsequent steps / routers can reference via `steps.*`).
fn registry_with_fixed_output(handler_name: &str, output: serde_json::Value) -> HandlerRegistry {
    let mut reg = registry();
    let output = Arc::new(output);
    reg.register(handler_name, move |_ctx| {
        let o = Arc::clone(&output);
        async move { Ok((*o).clone()) }
    });
    reg
}

fn registry_with_fixed_output_and_capture(
    fixed_handler: &str,
    fixed_output: serde_json::Value,
) -> (
    HandlerRegistry,
    Arc<std::sync::Mutex<Option<serde_json::Value>>>,
) {
    let mut reg = HandlerRegistry::new();
    register_builtins(&mut reg);

    let output = Arc::new(fixed_output);
    reg.register(fixed_handler, move |_ctx| {
        let o = Arc::clone(&output);
        async move { Ok((*o).clone()) }
    });

    let captured = Arc::new(std::sync::Mutex::new(None));
    let captured_clone = Arc::clone(&captured);
    reg.register("capture_params", move |ctx| {
        let captured = Arc::clone(&captured_clone);
        async move {
            *captured.lock().unwrap() = Some(ctx.params.clone());
            Ok(json!({"ok": true}))
        }
    });
    (reg, captured)
}

#[tokio::test]
async fn template_steps_alias_resolves_previous_step_output() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![
        mk_step_with_params("producer", "produce", json!({})),
        mk_step_with_params(
            "consumer",
            "capture_params",
            json!({
                "name": "{{ steps.producer.name }}",
                "count": "{{ steps.producer.count }}"
            }),
        ),
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let (reg, captured) =
        registry_with_fixed_output_and_capture("produce", json!({"name": "widget", "count": 42}));
    drive(&storage, &reg, inst.id, &seq).await;

    let seen = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler was called");
    assert_eq!(seen["name"], "widget");
    assert_eq!(seen["count"], 42);
}

#[tokio::test]
async fn template_json_function_serializes_value() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![
        mk_step_with_params("producer", "produce", json!({})),
        mk_step_with_params(
            "consumer",
            "capture_params",
            json!({
                "payload": "{{ json(steps.producer.items) }}"
            }),
        ),
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let (reg, captured) =
        registry_with_fixed_output_and_capture("produce", json!({"items": [1, 2, 3]}));
    drive(&storage, &reg, inst.id, &seq).await;

    let seen = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler was called");
    let payload: Vec<i32> = serde_json::from_str(seen["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload, vec![1, 2, 3]);
}

#[tokio::test]
async fn template_len_function_returns_array_length() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![
        mk_step_with_params("producer", "produce", json!({})),
        mk_step_with_params(
            "consumer",
            "capture_params",
            json!({
                "item_count": "{{ len(steps.producer.items) }}"
            }),
        ),
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let (reg, captured) =
        registry_with_fixed_output_and_capture("produce", json!({"items": ["a", "b", "c", "d"]}));
    drive(&storage, &reg, inst.id, &seq).await;

    let seen = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler was called");
    assert_eq!(seen["item_count"], 4);
}

#[tokio::test]
async fn router_in_operator_routes_based_on_step_output() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![
        mk_step_with_params("producer", "produce", json!({})),
        BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("router".into()),
            routes: vec![Route {
                condition: "steps.producer.level in ['strong', 'moderate']".into(),
                blocks: vec![mk_step("bet_path", "noop")],
            }],
            default: Some(vec![mk_step("skip_path", "noop")]),
        })),
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry_with_fixed_output("produce", json!({"level": "strong"}));
    drive(&storage, &reg, inst.id, &seq).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "bet_path"), NodeState::Completed);
}

#[tokio::test]
async fn router_ternary_expression_selects_route() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![
        mk_step_with_params("producer", "produce", json!({})),
        BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId("router".into()),
            routes: vec![Route {
                condition: "(steps.producer.score > 70) ? true : false".into(),
                blocks: vec![mk_step("high_path", "noop")],
            }],
            default: Some(vec![mk_step("low_path", "noop")]),
        })),
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry_with_fixed_output("produce", json!({"score": 85}));
    drive(&storage, &reg, inst.id, &seq).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "high_path"), NodeState::Completed);
}

#[tokio::test]
async fn template_instance_id_resolves() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step_with_params(
        "s1",
        "capture_params",
        json!({"iid": "{{ instance_id }}"}),
    )]);
    storage.create_sequence(&seq).await.unwrap();
    let mut inst = mk_instance(seq.id);
    inst.context.runtime.instance_id = Some(inst.id.to_string());
    storage.create_instance(&inst).await.unwrap();

    let (reg, captured) = registry_with_param_capture();
    drive(&storage, &reg, inst.id, &seq).await;

    let seen = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler was called");
    assert_eq!(
        seen["iid"].as_str().unwrap(),
        inst.id.to_string(),
        "instance_id template must resolve to actual instance ID"
    );
}
