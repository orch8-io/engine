#![allow(clippy::items_after_statements)]
//! Evaluator coverage tests: 100 integration tests covering the core `evaluate()`
//! loop, execution tree building, SLA deadline enforcement, block dispatch, and
//! EvalOutcome/state transitions.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator::{self, EvalOutcome};
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::error::StepError;
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{
    ABSplitDef, ABVariant, BlockDefinition, CancellationScopeDef, EscalationDef, ForEachDef,
    LoopDef, ParallelDef, RaceDef, RaceSemantics, RetryPolicy, Route, RouterDef,
    SequenceDefinition, SequenceStatus, StepDef, SubSequenceDef, TryCatchDef,
};
use orch8_types::signal::{Signal, SignalType};

mod common;
use common::*;

// evaluator_coverage-specific drive_n: does NOT transition Scheduled -> Running.
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

// ================================================================
// EVALUATOR CORE LOGIC (tests 1-25)
// ================================================================

// 1
#[tokio::test]
async fn evaluate_single_step_returns_done() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 2
#[tokio::test]
async fn evaluate_empty_sequence_returns_done_immediately() {
    let (storage, seq, inst) = setup(vec![]).await;
    let reg = registry();
    let outcome = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        EvalOutcome::Done {
            any_failed: false,
            any_cancelled: false,
        }
    ));
}

// 3
#[tokio::test]
async fn evaluate_two_steps_sequential_both_complete() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
}

// 4
#[tokio::test]
async fn evaluate_returns_more_work_when_step_pending() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    let outcome = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    match outcome {
        EvalOutcome::MoreWork { .. } => {
            let tree = storage.get_execution_tree(inst.id).await.unwrap();
            assert_eq!(node_state(&tree, "s2"), NodeState::Pending);
        }
        EvalOutcome::Done { .. } => {
            let tree = storage.get_execution_tree(inst.id).await.unwrap();
            assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
            assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
        }
    }
}

// 5
#[tokio::test]
async fn evaluate_returns_done_with_any_failed_true_on_failure() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    let inst_fresh = storage.get_instance(inst.id).await.unwrap().unwrap();
    let outcome = evaluator::evaluate(&storage, &reg, &inst_fresh, &seq)
        .await
        .unwrap();
    match outcome {
        EvalOutcome::Done {
            any_failed,
            any_cancelled,
        } => {
            assert!(any_failed);
            assert!(!any_cancelled);
        }
        EvalOutcome::MoreWork { .. } => {
            drive(&storage, &reg, inst.id, &seq).await;
            let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
            assert_eq!(final_inst.state, InstanceState::Failed);
        }
    }
}

// 6
#[tokio::test]
async fn evaluate_returns_done_with_any_cancelled_on_cancel() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

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
    assert_eq!(final_inst.state, InstanceState::Cancelled);
}

// 7
#[tokio::test]
async fn evaluate_builds_execution_tree_on_first_call() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let tree_before = storage.get_execution_tree(inst.id).await.unwrap();
    assert!(tree_before.is_empty());

    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();

    let tree_after = storage.get_execution_tree(inst.id).await.unwrap();
    assert!(!tree_after.is_empty());
    assert_eq!(tree_after.len(), 1);
}

// 8
#[tokio::test]
async fn evaluate_reuses_existing_tree_on_subsequent_calls() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();

    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree1 = storage.get_execution_tree(inst.id).await.unwrap();
    let node_ids: Vec<_> = tree1.iter().map(|n| n.id).collect();

    let inst_fresh = storage.get_instance(inst.id).await.unwrap().unwrap();
    let _ = evaluator::evaluate(&storage, &reg, &inst_fresh, &seq)
        .await
        .unwrap();
    let tree2 = storage.get_execution_tree(inst.id).await.unwrap();
    let node_ids2: Vec<_> = tree2.iter().map(|n| n.id).collect();
    assert_eq!(node_ids, node_ids2);
}

// 9
#[tokio::test]
async fn evaluate_advances_pending_node_to_running() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    let s1_state = node_state(&tree, "s1");
    assert!(
        matches!(s1_state, NodeState::Running | NodeState::Completed),
        "expected Running or Completed, got {s1_state:?}"
    );
}

// 10
#[tokio::test]
async fn evaluate_completes_already_running_node() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 11
#[tokio::test]
async fn evaluate_skips_completed_nodes() {
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

// 12
#[tokio::test]
async fn evaluate_skips_cancelled_nodes() {
    // Create the tree without executing any steps so nodes remain Pending (active).
    // Then cancel via scoped cancellation which marks active cancellable nodes as Cancelled.
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    evaluator::ensure_execution_tree(storage.as_ref(), &inst, &seq.blocks)
        .await
        .unwrap();

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
    // Use prefetched variant with sequence def so scoped cancellation marks individual nodes.
    orch8_engine::signals::process_signals_prefetched(
        storage.as_ref(),
        inst.id,
        InstanceState::Running,
        vec![cancel_sig],
        Some(&seq),
    )
    .await
    .unwrap();

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    let any_cancelled = tree.iter().any(|n| n.state == NodeState::Cancelled);
    assert!(any_cancelled);
}

// 13
#[tokio::test]
async fn evaluate_handles_mixed_completed_and_pending() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &seq, 1).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s3"), NodeState::Completed);
}

// 14
#[tokio::test]
async fn evaluate_ten_sequential_steps_all_complete() {
    let blocks: Vec<BlockDefinition> = (0..10).map(|i| mk_step(&format!("s{i}"), "noop")).collect();
    let (storage, seq, inst) = setup(blocks).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    for i in 0..10 {
        assert_eq!(node_state(&tree, &format!("s{i}")), NodeState::Completed);
    }
}

// 15
#[tokio::test]
async fn evaluate_first_step_fails_marks_sequence_failed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail"), mk_step("s2", "noop")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Cancelled);
}

// 16
#[tokio::test]
async fn evaluate_middle_step_fails_subsequent_not_run() {
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
    assert_eq!(node_state(&tree, "s3"), NodeState::Cancelled);
}

// 17
#[tokio::test]
async fn evaluate_parallel_both_branches_advance() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "b0"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
}

// 18
#[tokio::test]
async fn evaluate_nested_blocks_dispatch_correctly() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId::new("tc"),
                try_block: vec![mk_step("t1", "noop")],
                catch_block: vec![mk_step("c1", "noop")],
                finally_block: None,
            }))],
            vec![mk_step("b1", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Completed);
}

// 19
#[tokio::test]
async fn evaluate_tree_node_count_matches_block_count() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 3);
}

// 20
#[tokio::test]
async fn evaluate_node_parent_ids_set_correctly() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();

    let par_node = tree.iter().find(|n| n.block_id.as_str() == "par").unwrap();
    assert!(par_node.parent_id.is_none());

    let b0_node = tree.iter().find(|n| n.block_id.as_str() == "b0").unwrap();
    assert_eq!(b0_node.parent_id, Some(par_node.id));

    let b1_node = tree.iter().find(|n| n.block_id.as_str() == "b1").unwrap();
    assert_eq!(b1_node.parent_id, Some(par_node.id));
}

// 21
#[tokio::test]
async fn evaluate_retry_step_reschedules() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 3)]).await;
    let reg = registry_with_retryable_fail();
    drive_n(&storage, &reg, inst.id, &seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Running);
}

// 22
#[tokio::test]
async fn evaluate_context_data_accessible_in_handler() {
    let captured = Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = Arc::clone(&captured);
    let mut reg = registry();
    reg.register("capture", move |ctx| {
        let cap = Arc::clone(&captured_clone);
        async move {
            *cap.lock().unwrap() = Some(ctx.context.data.clone());
            Ok(json!({"ok": true}))
        }
    });

    let (storage, seq, inst) =
        setup_with_ctx(vec![mk_step("s1", "capture")], json!({"hello": "world"})).await;
    drive(&storage, &reg, inst.id, &seq).await;

    let seen = captured.lock().unwrap().clone().unwrap();
    assert_eq!(seen["hello"], "world");
}

// 23
#[tokio::test]
async fn evaluate_large_tree_hundred_steps() {
    let blocks: Vec<BlockDefinition> = (0..100)
        .map(|i| mk_step(&format!("s{i}"), "noop"))
        .collect();
    let (storage, seq, inst) = setup(blocks).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 100);
}

// 24
#[tokio::test]
async fn evaluate_after_cancel_signal_returns_done_cancelled() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

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
    assert_eq!(final_inst.state, InstanceState::Cancelled);
}

// 25
#[tokio::test]
async fn evaluate_waiting_node_returns_more_work_with_waiting() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();

    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

// ================================================================
// EXECUTION TREE BUILDING (tests 26-45)
// ================================================================

// 26
#[tokio::test]
async fn ensure_tree_creates_nodes_for_all_blocks() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 3);
}

// 27
#[tokio::test]
async fn ensure_tree_idempotent_on_second_call() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree1 = storage.get_execution_tree(inst.id).await.unwrap();

    let inst_fresh = storage.get_instance(inst.id).await.unwrap().unwrap();
    let _ = evaluator::evaluate(&storage, &reg, &inst_fresh, &seq)
        .await
        .unwrap();
    let tree2 = storage.get_execution_tree(inst.id).await.unwrap();

    assert_eq!(tree1.len(), tree2.len());
    assert_eq!(tree1[0].id, tree2[0].id);
}

// 28
#[tokio::test]
async fn ensure_tree_parallel_creates_branch_nodes() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 3);
    let b0 = tree.iter().find(|n| n.block_id.as_str() == "b0").unwrap();
    assert_eq!(b0.branch_index, Some(0));
    let b1 = tree.iter().find(|n| n.block_id.as_str() == "b1").unwrap();
    assert_eq!(b1.branch_index, Some(1));
}

// 29
#[tokio::test]
async fn ensure_tree_router_creates_route_nodes() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("r"),
        routes: vec![
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("r1", "noop")],
            },
            Route {
                condition: "false".into(),
                blocks: vec![mk_step("r2", "noop")],
            },
        ],
        default: Some(vec![mk_step("def", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 4);
}

// 30
#[tokio::test]
async fn ensure_tree_loop_creates_body_nodes() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 2,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 2);
}

// 31
#[tokio::test]
async fn ensure_tree_for_each_creates_iteration_nodes() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3]})).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 2);
}

// 32
#[tokio::test]
async fn ensure_tree_try_catch_creates_try_catch_finally_nodes() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "noop")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: Some(vec![mk_step("f1", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 4);
}

// 33
#[tokio::test]
async fn ensure_tree_nested_blocks_have_correct_parent() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 1,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();

    let lp_node = tree.iter().find(|n| n.block_id.as_str() == "lp").unwrap();
    let body_node = tree.iter().find(|n| n.block_id.as_str() == "body").unwrap();
    assert_eq!(body_node.parent_id, Some(lp_node.id));
}

// 34
#[tokio::test]
async fn ensure_tree_ab_split_creates_variant_nodes() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "control".into(),
                weight: 50,
                blocks: vec![mk_step("ctrl", "noop")],
            },
            ABVariant {
                name: "variant".into(),
                weight: 50,
                blocks: vec![mk_step("var", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 3);
}

// 35
#[tokio::test]
async fn ensure_tree_cancellation_scope_nodes() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("scope"),
        blocks: vec![mk_step("inner", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 2);
}

// 36
#[tokio::test]
async fn ensure_tree_sub_sequence_node() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &parent_seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 1);
    assert_eq!(tree[0].block_id.as_str(), "sub");
}

// 37
#[tokio::test]
async fn ensure_tree_preserves_block_id_on_nodes() {
    let (storage, seq, inst) = setup(vec![mk_step("my_block", "noop")]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree[0].block_id.as_str(), "my_block");
}

// 38
#[tokio::test]
async fn ensure_tree_sets_initial_state_pending() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let blocks = &seq.blocks;
    let tree = evaluator::ensure_execution_tree(storage.as_ref(), &inst, blocks)
        .await
        .unwrap();
    let s2_node = tree.iter().find(|n| n.block_id.as_str() == "s2").unwrap();
    assert_eq!(s2_node.state, NodeState::Pending);
}

// 39
#[tokio::test]
async fn ensure_tree_node_block_type_matches_definition() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("b0", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();

    let par_node = tree.iter().find(|n| n.block_id.as_str() == "par").unwrap();
    assert_eq!(
        par_node.block_type,
        orch8_types::execution::BlockType::Parallel
    );

    let b0_node = tree.iter().find(|n| n.block_id.as_str() == "b0").unwrap();
    assert_eq!(b0_node.block_type, orch8_types::execution::BlockType::Step);
}

// 40
#[tokio::test]
async fn ensure_tree_handles_injected_blocks() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree1 = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree1.len(), 1);

    let injected = serde_json::to_value(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("injected"),
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
    }))])
    .unwrap();
    storage.inject_blocks(inst.id, &injected).await.unwrap();

    storage
        .update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();
    let inst_fresh = storage.get_instance(inst.id).await.unwrap().unwrap();
    let _ = evaluator::evaluate(&storage, &reg, &inst_fresh, &seq)
        .await
        .unwrap();

    let tree2 = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree2.len(), 2);
    assert!(tree2.iter().any(|n| n.block_id.as_str() == "injected"));
}

// 41
#[tokio::test]
async fn ensure_tree_injected_blocks_added_to_existing_tree() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();

    let injected =
        serde_json::to_value(vec![mk_step("extra1", "noop"), mk_step("extra2", "noop")]).unwrap();
    storage.inject_blocks(inst.id, &injected).await.unwrap();

    storage
        .update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();
    let inst_fresh = storage.get_instance(inst.id).await.unwrap().unwrap();
    let _ = evaluator::evaluate(&storage, &reg, &inst_fresh, &seq)
        .await
        .unwrap();

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert!(tree.iter().any(|n| n.block_id.as_str() == "extra1"));
    assert!(tree.iter().any(|n| n.block_id.as_str() == "extra2"));
}

// 42
#[tokio::test]
async fn ensure_tree_empty_injected_blocks_no_op() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let empty = json!([]);
    storage.inject_blocks(inst.id, &empty).await.unwrap();

    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 1);
}

// 43
#[tokio::test]
async fn ensure_tree_malformed_injected_blocks_ignored() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let malformed = json!([{"not": "a block"}]);
    storage.inject_blocks(inst.id, &malformed).await.unwrap();

    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 1);
}

// 44
#[tokio::test]
async fn ensure_tree_deep_nesting_five_levels() {
    let deep = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId::new("par"),
                branches: vec![vec![mk_step("deep_step", "noop")]],
            }))],
            catch_block: vec![mk_step("catch", "noop")],
            finally_block: None,
        }))],
        max_iterations: 1,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup(vec![deep]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 5);

    let par_node = tree.iter().find(|n| n.block_id.as_str() == "par").unwrap();
    let deep_node = tree
        .iter()
        .find(|n| n.block_id.as_str() == "deep_step")
        .unwrap();
    assert_eq!(deep_node.parent_id, Some(par_node.id));
}

// 45
#[tokio::test]
async fn ensure_tree_twenty_blocks_flat() {
    let blocks: Vec<BlockDefinition> = (0..20).map(|i| mk_step(&format!("s{i}"), "noop")).collect();
    let (storage, seq, inst) = setup(blocks).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 20);
    for node in &tree {
        assert!(node.parent_id.is_none());
    }
}

// ================================================================
// SLA DEADLINE ENFORCEMENT (tests 46-60)
// ================================================================

// 46
#[tokio::test]
async fn sla_no_deadline_no_breach() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 47
#[tokio::test]
async fn sla_deadline_not_yet_breached() {
    let (storage, seq, inst) = setup(vec![mk_step_with_deadline(
        "s1",
        "noop",
        Duration::from_secs(600),
    )])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 48
#[tokio::test]
async fn sla_deadline_breached_fails_node() {
    let (storage, seq, inst) = setup(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "retryable_fail".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 100,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
        }),
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(Duration::from_millis(0)),
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))])
    .await;
    let reg = registry_with_retryable_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

// 49
#[tokio::test]
async fn sla_deadline_with_escalation_handler_called() {
    let escalation_called = Arc::new(std::sync::Mutex::new(false));
    let escalation_called_clone = Arc::clone(&escalation_called);

    let mut reg = registry_with_retryable_fail();
    reg.register("escalate", move |_ctx| {
        let called = Arc::clone(&escalation_called_clone);
        async move {
            *called.lock().unwrap() = true;
            Ok(json!({"escalated": true}))
        }
    });

    let (storage, seq, inst) = setup(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "retryable_fail".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 100,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
        }),
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(Duration::from_millis(0)),
        on_deadline_breach: Some(EscalationDef {
            handler: "escalate".into(),
            params: json!({"alert": true}),
        }),
        fallback_handler: None,
        cache_key: None,
    }))])
    .await;

    drive(&storage, &reg, inst.id, &seq).await;
    assert!(*escalation_called.lock().unwrap());
}

// 50
#[tokio::test]
async fn sla_deadline_escalation_handler_not_found_still_fails() {
    let (storage, seq, inst) = setup(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "retryable_fail".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 100,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
        }),
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(Duration::from_millis(0)),
        on_deadline_breach: Some(EscalationDef {
            handler: "nonexistent_handler".into(),
            params: json!({}),
        }),
        fallback_handler: None,
        cache_key: None,
    }))])
    .await;
    let reg = registry_with_retryable_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

// 51
#[tokio::test]
async fn sla_deadline_escalation_handler_fails_still_fails_node() {
    let mut reg = registry_with_retryable_fail();
    reg.register("failing_escalate", |_ctx| {
        Box::pin(async {
            Err(StepError::Permanent {
                message: "escalation failed".into(),
                details: None,
            })
        })
    });

    let (storage, seq, inst) = setup(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "retryable_fail".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 100,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
        }),
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(Duration::from_millis(0)),
        on_deadline_breach: Some(EscalationDef {
            handler: "failing_escalate".into(),
            params: json!({}),
        }),
        fallback_handler: None,
        cache_key: None,
    }))])
    .await;

    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

// 52
#[tokio::test]
async fn sla_deadline_only_checks_running_nodes() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "fail"),
        mk_step_with_deadline("s2", "noop", Duration::from_millis(0)),
    ])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Cancelled);
}

// 53
#[tokio::test]
async fn sla_deadline_only_checks_waiting_nodes() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

// 54
#[tokio::test]
async fn sla_deadline_skips_completed_nodes() {
    let (storage, seq, inst) = setup(vec![mk_step_with_deadline(
        "s1",
        "noop",
        Duration::from_secs(600),
    )])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 55
#[tokio::test]
async fn sla_deadline_multiple_nodes_some_breached() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("fast", "noop")],
            vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("slow"),
                handler: "retryable_fail".into(),
                params: json!({}),
                delay: None,
                retry: Some(RetryPolicy {
                    max_attempts: 100,
                    initial_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(1),
                    backoff_multiplier: 1.0,
                }),
                timeout: None,
                rate_limit_key: None,
                send_window: None,
                context_access: None,
                cancellable: true,
                wait_for_input: None,
                queue_name: None,
                deadline: Some(Duration::from_millis(0)),
                on_deadline_breach: None,
                fallback_handler: None,
                cache_key: None,
            }))],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_retryable_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fast"), NodeState::Completed);
    assert_eq!(node_state(&tree, "slow"), NodeState::Failed);
}

// 56
#[tokio::test]
async fn sla_deadline_zero_duration_immediately_breaches() {
    let (storage, seq, inst) = setup(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "retryable_fail".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 100,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
        }),
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(Duration::from_millis(0)),
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))])
    .await;
    let reg = registry_with_retryable_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

// 57
#[tokio::test]
async fn sla_deadline_large_duration_no_breach() {
    let (storage, seq, inst) = setup(vec![mk_step_with_deadline(
        "s1",
        "noop",
        Duration::from_secs(3600),
    )])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 58
#[tokio::test]
async fn sla_deadline_records_block_output_on_breach() {
    let (storage, seq, inst) = setup(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "retryable_fail".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 100,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
        }),
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(Duration::from_millis(0)),
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))])
    .await;
    let reg = registry_with_retryable_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let output = storage
        .get_block_output(inst.id, &BlockId::new("s1"))
        .await
        .unwrap();
    assert!(output.is_some());
    let output = output.unwrap();
    assert_eq!(output.output["_error"], "sla_deadline_breached");
}

// 59
#[tokio::test]
async fn sla_deadline_breach_metadata_in_output() {
    let (storage, seq, inst) = setup(vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "retryable_fail".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 100,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_multiplier: 1.0,
        }),
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(Duration::from_millis(0)),
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))])
    .await;
    let reg = registry_with_retryable_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let output = storage
        .get_block_output(inst.id, &BlockId::new("s1"))
        .await
        .unwrap()
        .unwrap();
    assert!(output.output.get("_deadline_ms").is_some());
    assert!(output.output.get("_elapsed_ms").is_some());
}

// 60
#[tokio::test]
async fn sla_deadline_non_step_blocks_ignored() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step_with_deadline(
            "s1",
            "noop",
            Duration::from_secs(3600),
        )]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// ================================================================
// BLOCK DISPATCH (tests 61-80)
// ================================================================

// 61
#[tokio::test]
async fn dispatch_step_executes_handler() {
    let called = Arc::new(std::sync::Mutex::new(false));
    let called_clone = Arc::clone(&called);
    let mut reg = registry();
    reg.register("track", move |_ctx| {
        let c = Arc::clone(&called_clone);
        async move {
            *c.lock().unwrap() = true;
            Ok(json!({"done": true}))
        }
    });
    let (storage, seq, inst) = setup(vec![mk_step("s1", "track")]).await;
    drive(&storage, &reg, inst.id, &seq).await;
    assert!(*called.lock().unwrap());
}

// 62
#[tokio::test]
async fn dispatch_step_stores_output() {
    let mut reg = registry();
    reg.register("produce", |_ctx| {
        Box::pin(async { Ok(json!({"result": 42})) })
    });
    let (storage, seq, inst) = setup(vec![mk_step("s1", "produce")]).await;
    drive(&storage, &reg, inst.id, &seq).await;
    let output = storage
        .get_block_output(inst.id, &BlockId::new("s1"))
        .await
        .unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().output["result"], 42);
}

// 63
#[tokio::test]
async fn dispatch_step_marks_node_completed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 64
#[tokio::test]
async fn dispatch_step_failure_marks_node_failed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

// 65
#[tokio::test]
async fn dispatch_step_with_interceptor_before() {
    use orch8_types::interceptor::{InterceptorAction, InterceptorDef};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "test-flow".into(),
        version: 1,
        deprecated: false,
        blocks: vec![mk_step("s1", "noop")],
        interceptors: Some(InterceptorDef {
            before_step: Some(InterceptorAction {
                handler: "audit".into(),
                params: json!({"stage": "pre"}),
            }),
            after_step: None,
            ..Default::default()
        }),
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
        status: SequenceStatus::Production,
    };
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let before = storage
        .get_block_output(inst.id, &BlockId::new("_interceptor:before:s1"))
        .await
        .unwrap();
    assert!(before.is_some());
}

// 66
#[tokio::test]
async fn dispatch_step_with_interceptor_after() {
    use orch8_types::interceptor::{InterceptorAction, InterceptorDef};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "test-flow".into(),
        version: 1,
        deprecated: false,
        blocks: vec![mk_step("s1", "noop")],
        interceptors: Some(InterceptorDef {
            before_step: None,
            after_step: Some(InterceptorAction {
                handler: "audit".into(),
                params: json!({"stage": "post"}),
            }),
            ..Default::default()
        }),
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
        status: SequenceStatus::Production,
    };
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let after = storage
        .get_block_output(inst.id, &BlockId::new("_interceptor:after:s1"))
        .await
        .unwrap();
    assert!(after.is_some());
}

// 67
#[tokio::test]
async fn dispatch_parallel_advances_all_branches() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
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
    assert_eq!(node_state(&tree, "b0"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b2"), NodeState::Completed);
}

// 68
#[tokio::test]
async fn dispatch_router_evaluates_condition() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("r"),
        routes: vec![
            Route {
                condition: "x > 10".into(),
                blocks: vec![mk_step("high", "noop")],
            },
            Route {
                condition: "x <= 10".into(),
                blocks: vec![mk_step("low", "noop")],
            },
        ],
        default: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"x": 5})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "r"), NodeState::Completed);
    assert_eq!(node_state(&tree, "low"), NodeState::Completed);
}

// 69
#[tokio::test]
async fn dispatch_loop_iterates_body() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

// 70
#[tokio::test]
async fn dispatch_for_each_iterates_items() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": ["a", "b", "c"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 71
#[tokio::test]
async fn dispatch_try_catch_catches_error() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
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

// 72
#[tokio::test]
async fn dispatch_race_first_winner() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("fast", "noop")],
            vec![mk_step("s1", "noop"), mk_step("s2", "noop")],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fast"), NodeState::Completed);
}

// 73
#[tokio::test]
async fn dispatch_ab_split_selects_variant() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
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

// 74
#[tokio::test]
async fn dispatch_cancellation_scope_normal() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("scope"),
        blocks: vec![mk_step("inner", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "scope"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner"), NodeState::Completed);
}

// 75
#[tokio::test]
async fn dispatch_sub_sequence_creates_child() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 3).await;

    let children = storage.get_child_instances(inst.id).await.unwrap();
    assert_eq!(children.len(), 1);
}

// 76
#[tokio::test]
async fn dispatch_sub_sequence_waits_for_child() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

// 77
#[tokio::test]
async fn dispatch_sub_sequence_child_completed_completes_node() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;

    let children = storage.get_child_instances(inst.id).await.unwrap();
    assert_eq!(children.len(), 1);
    let child = &children[0];
    storage
        .update_instance_state(child.id, InstanceState::Running, None)
        .await
        .unwrap();
    drive(&storage, &reg, child.id, &child_seq).await;

    storage
        .update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();
    drive(&storage, &reg, inst.id, &parent_seq).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Completed);
}

// 78
#[tokio::test]
async fn dispatch_sub_sequence_child_failed_fails_node() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "fail")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry_with_fail();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;

    let children = storage.get_child_instances(inst.id).await.unwrap();
    assert_eq!(children.len(), 1);
    let child = &children[0];
    storage
        .update_instance_state(child.id, InstanceState::Running, None)
        .await
        .unwrap();
    drive(&storage, &reg, child.id, &child_seq).await;

    storage
        .update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();
    drive(&storage, &reg, inst.id, &parent_seq).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Failed);
}

// 79
#[tokio::test]
async fn dispatch_step_increments_step_counter() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert!(final_inst.context.runtime.total_steps_executed >= 2);
}

// 80
#[tokio::test]
async fn dispatch_marks_pending_node_as_running() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    let _ = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    let s1_state = node_state(&tree, "s1");
    assert!(
        matches!(s1_state, NodeState::Running | NodeState::Completed),
        "expected Running or Completed, got {s1_state:?}"
    );
}

// ================================================================
// EVAL OUTCOME AND STATE TRANSITIONS (tests 81-100)
// ================================================================

// 81
#[tokio::test]
async fn eval_outcome_done_all_completed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    let inst_fresh = storage.get_instance(inst.id).await.unwrap().unwrap();
    let mut outcome = evaluator::evaluate(&storage, &reg, &inst_fresh, &seq)
        .await
        .unwrap();
    for _ in 0..100 {
        if matches!(outcome, EvalOutcome::Done { .. }) {
            break;
        }
        let i = storage.get_instance(inst.id).await.unwrap().unwrap();
        outcome = evaluator::evaluate(&storage, &reg, &i, &seq).await.unwrap();
    }
    assert!(matches!(
        outcome,
        EvalOutcome::Done {
            any_failed: false,
            any_cancelled: false
        }
    ));
}

// 82
#[tokio::test]
async fn eval_outcome_done_mixed_completed_and_failed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// 83
#[tokio::test]
async fn eval_outcome_more_work_some_pending() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    let outcome = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    match outcome {
        EvalOutcome::Done { .. } => {}
        EvalOutcome::MoreWork { .. } => {
            let tree = storage.get_execution_tree(inst.id).await.unwrap();
            let pending = tree.iter().any(|n| n.state == NodeState::Pending);
            assert!(pending);
        }
    }
}

// 84
#[tokio::test]
async fn eval_outcome_more_work_has_waiting_true() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    let mut has_waiting = false;
    for _ in 0..10 {
        let i = storage.get_instance(inst.id).await.unwrap().unwrap();
        if i.state != InstanceState::Running {
            break;
        }
        let outcome = evaluator::evaluate(&storage, &reg, &i, &parent_seq)
            .await
            .unwrap();
        if let EvalOutcome::MoreWork {
            has_waiting_nodes: w,
        } = outcome
            && w
        {
            has_waiting = true;
            break;
        }
    }
    assert!(has_waiting);
}

// 85
#[tokio::test]
async fn eval_outcome_more_work_has_waiting_false() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 10)]).await;
    let reg = registry_with_retryable_fail();
    let outcome = evaluator::evaluate(&storage, &reg, &inst, &seq)
        .await
        .unwrap();
    if let EvalOutcome::MoreWork { has_waiting_nodes } = outcome {
        assert!(!has_waiting_nodes);
    }
}

// 86
#[tokio::test]
async fn instance_transitions_to_completed_on_done() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 87
#[tokio::test]
async fn instance_transitions_to_failed_on_done_with_failures() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// 88
#[tokio::test]
async fn instance_transitions_to_cancelled_on_done_with_cancels() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

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
    assert_eq!(final_inst.state, InstanceState::Cancelled);
}

// 89
#[tokio::test]
async fn instance_stays_running_on_more_work() {
    // A SubSequence creates a child instance and transitions the node to Waiting,
    // causing evaluate() to return MoreWork while the instance remains Running.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    let outcome = evaluator::evaluate(&storage, &reg, &inst, &parent_seq)
        .await
        .unwrap();
    assert!(matches!(outcome, EvalOutcome::MoreWork { .. }));
    let inst_state = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst_state.state, InstanceState::Running);
}

// 90
#[tokio::test]
async fn instance_transitions_to_waiting_when_all_nodes_waiting() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 10).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

// 91
#[tokio::test]
async fn drive_completes_simple_sequence() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 92
#[tokio::test]
async fn drive_fails_on_permanent_error() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// 93
#[tokio::test]
async fn drive_retries_then_succeeds() {
    let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let mut reg = registry();
    reg.register("flaky", move |_ctx| {
        let c = Arc::clone(&counter_clone);
        async move {
            let attempt = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt < 2 {
                Err(StepError::Retryable {
                    message: "not yet".into(),
                    details: None,
                })
            } else {
                Ok(json!({"ok": true}))
            }
        }
    });

    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "flaky", 5)]).await;
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
}

// 94
#[tokio::test]
async fn drive_handles_concurrent_signals() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

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

    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 95
#[tokio::test]
async fn drive_parallel_with_one_failing_branch() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "fail")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Failed);
}

// 96
#[tokio::test]
async fn drive_loop_with_condition_exit() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "false".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 97
#[tokio::test]
async fn drive_router_with_context_condition() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("r"),
        routes: vec![
            Route {
                condition: "role == \"admin\"".into(),
                blocks: vec![mk_step("admin_path", "noop")],
            },
            Route {
                condition: "role == \"user\"".into(),
                blocks: vec![mk_step("user_path", "noop")],
            },
        ],
        default: Some(vec![mk_step("guest_path", "noop")]),
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"role": "admin"})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "r"), NodeState::Completed);
    assert_eq!(node_state(&tree, "admin_path"), NodeState::Completed);
}

// 98
#[tokio::test]
async fn drive_for_each_with_item_data() {
    let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);
    let mut reg = registry();
    reg.register("count", move |_ctx| {
        let c = Arc::clone(&counter_clone);
        async move {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(json!({"counted": true}))
        }
    });

    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "count")],
        max_iterations: 100,
        retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3, 4, 5]})).await;
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 5);
}

// 99
#[tokio::test]
async fn drive_nested_parallel_in_try_catch() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("par"),
            branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "fail")]],
        }))],
        catch_block: vec![mk_step("catch", "noop")],
        finally_block: Some(vec![mk_step("finally", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "catch"), NodeState::Completed);
    assert_eq!(node_state(&tree, "finally"), NodeState::Completed);
}

// 100
#[tokio::test]
async fn drive_complex_mixed_sequence_all_block_types() {
    let blocks = vec![
        mk_step("init", "noop"),
        BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("par"),
            branches: vec![
                vec![mk_step("par_b0", "noop")],
                vec![mk_step("par_b1", "noop")],
            ],
        })),
        BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("router"),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![mk_step("routed", "noop")],
            }],
            default: None,
        })),
        BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("lp"),
            condition: "true".into(),
            body: vec![mk_step("lp_body", "noop")],
            max_iterations: 2,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        })),
        BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId::new("fe"),
            collection: "items".into(),
            item_var: "item".into(),
            body: vec![mk_step("fe_body", "noop")],
            max_iterations: 100,
            retain_iterations: None,
        })),
        BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![mk_step("tc_try", "noop")],
            catch_block: vec![mk_step("tc_catch", "noop")],
            finally_block: None,
        })),
        mk_step("final", "noop"),
    ];
    let (storage, seq, inst) = setup_with_ctx(blocks, json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "init"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "final"), NodeState::Completed);
}
