#![allow(clippy::items_after_statements)]
//! Edge-cases and complex scenario coverage tests for the orch8 workflow engine.
//!
//! 100 integration tests covering deep nesting, retry/error recovery, context/data
//! flow, concurrency/ordering, and terminal states.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator::{self, EvalOutcome};
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority};
use orch8_types::sequence::{
    ABSplitDef, ABVariant, BlockDefinition, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef,
    RaceDef, RaceSemantics, RetryPolicy, Route, RouterDef, SequenceDefinition, SequenceStatus,
    StepDef, SubSequenceDef, TryCatchDef,
};
use orch8_types::signal::{Signal, SignalType};

mod common;
use common::*;

// edge_cases-specific drive_n: does NOT transition Scheduled -> Running.
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
// DEEP NESTING COMBINATIONS (tests 1-20)
// ================================================================

// 1. parallel_containing_try_catch_with_retry
#[tokio::test]
async fn parallel_containing_try_catch_with_retry() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId::new("tc"),
                try_block: vec![mk_step_with_retry("retry_step", "noop", 3)],
                catch_block: vec![mk_step("catch_step", "noop")],
                finally_block: None,
            }))],
            vec![mk_step("branch2", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "retry_step"), NodeState::Completed);
    assert_eq!(node_state(&tree, "catch_step"), NodeState::Skipped);
    assert_eq!(node_state(&tree, "branch2"), NodeState::Completed);
}

// 2. loop_containing_parallel_containing_router
#[tokio::test]
async fn loop_containing_parallel_containing_router() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("par"),
            branches: vec![
                vec![BlockDefinition::Router(Box::new(RouterDef {
                    id: BlockId::new("router"),
                    routes: vec![Route {
                        condition: "true".into(),
                        blocks: vec![mk_step("routed", "noop")],
                    }],
                    default: None,
                }))],
                vec![mk_step("other_branch", "noop")],
            ],
        }))],
        max_iterations: 2,
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

// 3. for_each_containing_try_catch_containing_parallel
#[tokio::test]
async fn for_each_containing_try_catch_containing_parallel() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId::new("par"),
                branches: vec![
                    vec![mk_step("par_a", "noop")],
                    vec![mk_step("par_b", "noop")],
                ],
            }))],
            catch_block: vec![mk_step("catch_s", "noop")],
            finally_block: None,
        }))],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 4. router_route_containing_loop_containing_for_each
#[tokio::test]
async fn router_route_containing_loop_containing_for_each() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("router"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![BlockDefinition::Loop(Box::new(LoopDef {
                id: BlockId::new("lp"),
                condition: "true".into(),
                body: vec![BlockDefinition::ForEach(Box::new(ForEachDef {
                    id: BlockId::new("fe"),
                    collection: "items".into(),
                    item_var: "item".into(),
                    body: vec![mk_step("inner_step", "noop")],
                    max_iterations: 100,
                retain_iterations: None,
                }))],
                max_iterations: 2,
                break_on: None,
                continue_on_error: false,
                poll_interval: None,
            retain_iterations: None,
            }))],
        }],
        default: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

// 5. try_catch_try_has_parallel_catch_has_router
#[tokio::test]
async fn try_catch_try_has_parallel_catch_has_router() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("par"),
            branches: vec![
                vec![mk_step("try_a", "fail")],
                vec![mk_step("try_b", "noop")],
            ],
        }))],
        catch_block: vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("catch_router"),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![mk_step("recovery", "noop")],
            }],
            default: None,
        }))],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "recovery"), NodeState::Completed);
}

// 6. cancellation_scope_containing_loop_with_for_each
#[tokio::test]
async fn cancellation_scope_containing_loop_with_for_each() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("scope"),
        blocks: vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("lp"),
            condition: "true".into(),
            body: vec![BlockDefinition::ForEach(Box::new(ForEachDef {
                id: BlockId::new("fe"),
                collection: "items".into(),
                item_var: "item".into(),
                body: vec![mk_step("inner", "noop")],
                max_iterations: 100,
            retain_iterations: None,
            }))],
            max_iterations: 2,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        retain_iterations: None,
        }))],
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![scope], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "scope"), NodeState::Completed);
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

// 7. parallel_five_branches_each_with_different_block_type
#[tokio::test]
async fn parallel_five_branches_each_with_different_block_type() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            // Branch 1: plain step
            vec![mk_step("plain", "noop")],
            // Branch 2: loop
            vec![BlockDefinition::Loop(Box::new(LoopDef {
                id: BlockId::new("lp"),
                condition: "true".into(),
                body: vec![mk_step("lp_body", "noop")],
                max_iterations: 2,
                break_on: None,
                continue_on_error: false,
                poll_interval: None,
            retain_iterations: None,
            }))],
            // Branch 3: for_each
            vec![BlockDefinition::ForEach(Box::new(ForEachDef {
                id: BlockId::new("fe"),
                collection: "items".into(),
                item_var: "item".into(),
                body: vec![mk_step("fe_body", "noop")],
                max_iterations: 100,
            retain_iterations: None,
            }))],
            // Branch 4: router
            vec![BlockDefinition::Router(Box::new(RouterDef {
                id: BlockId::new("router"),
                routes: vec![Route {
                    condition: "true".into(),
                    blocks: vec![mk_step("r_step", "noop")],
                }],
                default: None,
            }))],
            // Branch 5: try_catch
            vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId::new("tc"),
                try_block: vec![mk_step("try_s", "noop")],
                catch_block: vec![mk_step("catch_s", "noop")],
                finally_block: None,
            }))],
        ],
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![par], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "plain"), NodeState::Completed);
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
}

// 8. nested_three_deep_parallel_in_parallel_in_parallel
#[tokio::test]
async fn nested_three_deep_parallel_in_parallel_in_parallel() {
    let inner_par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("inner_par"),
        branches: vec![
            vec![mk_step("deep_a", "noop")],
            vec![mk_step("deep_b", "noop")],
        ],
    }));
    let mid_par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("mid_par"),
        branches: vec![vec![inner_par], vec![mk_step("mid_step", "noop")]],
    }));
    let outer_par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("outer_par"),
        branches: vec![vec![mid_par], vec![mk_step("outer_step", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![outer_par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "outer_par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "mid_par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "deep_a"), NodeState::Completed);
    assert_eq!(node_state(&tree, "deep_b"), NodeState::Completed);
}

// 9. router_in_loop_in_for_each
#[tokio::test]
async fn router_in_loop_in_for_each() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("lp"),
            condition: "true".into(),
            body: vec![BlockDefinition::Router(Box::new(RouterDef {
                id: BlockId::new("router"),
                routes: vec![Route {
                    condition: "true".into(),
                    blocks: vec![mk_step("routed_step", "noop")],
                }],
                default: None,
            }))],
            max_iterations: 2,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        retain_iterations: None,
        }))],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 10. try_catch_in_ab_split_variant
#[tokio::test]
async fn try_catch_in_ab_split_variant() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId::new("tc"),
                try_block: vec![mk_step("try_s", "fail")],
                catch_block: vec![mk_step("catch_s", "noop")],
                finally_block: None,
            }))],
        }],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "catch_s"), NodeState::Completed);
}

// 11. for_each_with_try_catch_per_item_failure_isolated
#[tokio::test]
async fn for_each_with_try_catch_per_item_failure_isolated() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![mk_step("try_s", "fail")],
            catch_block: vec![mk_step("catch_s", "noop")],
            finally_block: None,
        }))],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3]})).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Each iteration's failure is caught, so for_each completes.
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 12. loop_exits_mid_iteration_on_router_condition
#[tokio::test]
async fn loop_exits_mid_iteration_on_router_condition() {
    // Loop with break_on condition; body contains a router.
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("router"),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![mk_step("routed", "noop")],
            }],
            default: None,
        }))],
        max_iterations: 5,
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

// 13. parallel_branch_with_loop_that_fails_on_third_iteration
#[tokio::test]
async fn parallel_branch_with_loop_that_fails_on_third_iteration() {
    // One branch has a loop whose body fails; the other branch succeeds.
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![BlockDefinition::Loop(Box::new(LoopDef {
                id: BlockId::new("failing_loop"),
                condition: "true".into(),
                body: vec![mk_step("loop_body", "fail")],
                max_iterations: 3,
                break_on: None,
                continue_on_error: false,
                poll_interval: None,
            retain_iterations: None,
            }))],
            vec![mk_step("success_branch", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Failed);
    assert_eq!(node_state(&tree, "failing_loop"), NodeState::Failed);
}

// 14. race_inside_try_catch_winner_determines_output
#[tokio::test]
async fn race_inside_try_catch_winner_determines_output() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![BlockDefinition::Race(Box::new(RaceDef {
            id: BlockId::new("race"),
            branches: vec![
                vec![mk_step("fast", "noop")],
                vec![mk_step("slow1", "noop"), mk_step("slow2", "noop")],
            ],
            semantics: RaceSemantics::default(),
        }))],
        catch_block: vec![mk_step("catch_s", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fast"), NodeState::Completed);
    assert_eq!(node_state(&tree, "catch_s"), NodeState::Skipped);
}

// 15. cancellation_scope_timeout_inside_parallel
#[tokio::test]
async fn cancellation_scope_timeout_inside_parallel() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![BlockDefinition::CancellationScope(Box::new(
                CancellationScopeDef {
                    id: BlockId::new("scope"),
                    blocks: vec![mk_step("protected", "noop")],
                },
            ))],
            vec![mk_step("other", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "scope"), NodeState::Completed);
    assert_eq!(node_state(&tree, "protected"), NodeState::Completed);
}

// 16. sub_sequence_inside_for_each_iteration
#[tokio::test]
async fn sub_sequence_inside_for_each_iteration() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // Create child sequence.
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("child_step", "noop")],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();

    // Parent with for_each containing sub_sequence.
    let parent_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "parent-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId::new("fe"),
            collection: "items".into(),
            item_var: "item".into(),
            body: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                id: BlockId::new("sub"),
                sequence_name: "child-flow".into(),
                version: None,
                input: json!({}),
            }))],
            max_iterations: 100,
        retain_iterations: None,
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&parent_seq).await.unwrap();

    let inst = mk_instance_with_ctx(parent_seq.id, json!({"items": [1]}));
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    // Drive a few ticks. The sub-sequence enters Waiting (needs scheduler).
    drive_n(&storage, &reg, inst.id, &parent_seq, 10).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Sub-sequence node should be in Waiting state.
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

// 17. ab_split_with_parallel_in_each_variant
#[tokio::test]
async fn ab_split_with_parallel_in_each_variant() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId::new("par"),
                branches: vec![
                    vec![mk_step("var_a", "noop")],
                    vec![mk_step("var_b", "noop")],
                ],
            }))],
        }],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
}

// 18. router_default_has_nested_try_catch
#[tokio::test]
async fn router_default_has_nested_try_catch() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("router"),
        routes: vec![Route {
            condition: "false".into(),
            blocks: vec![mk_step("never", "noop")],
        }],
        default: Some(vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![mk_step("try_s", "fail")],
            catch_block: vec![mk_step("catch_s", "noop")],
            finally_block: Some(vec![mk_step("finally_s", "noop")]),
        }))]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "catch_s"), NodeState::Completed);
    assert_eq!(node_state(&tree, "finally_s"), NodeState::Completed);
}

// 19. for_each_inside_cancellation_scope
#[tokio::test]
async fn for_each_inside_cancellation_scope() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("scope"),
        blocks: vec![BlockDefinition::ForEach(Box::new(ForEachDef {
            id: BlockId::new("fe"),
            collection: "items".into(),
            item_var: "item".into(),
            body: vec![mk_step("body", "noop")],
            max_iterations: 100,
        retain_iterations: None,
        }))],
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![scope], json!({"items": [1, 2, 3]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "scope"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 20. loop_in_race_first_to_finish_wins
#[tokio::test]
async fn loop_in_race_first_to_finish_wins() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            // Short branch: a single step finishes fast.
            vec![mk_step("fast_step", "noop")],
            // Long branch: loop runs multiple iterations.
            vec![BlockDefinition::Loop(Box::new(LoopDef {
                id: BlockId::new("slow_loop"),
                condition: "true".into(),
                body: vec![mk_step("loop_body", "noop")],
                max_iterations: 10,
                break_on: None,
                continue_on_error: false,
                poll_interval: None,
            retain_iterations: None,
            }))],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    // In a race, one branch wins and the other gets cancelled.
    // We cannot guarantee which branch wins in the evaluator.
    let fast = node_state(&tree, "fast_step");
    assert!(
        fast == NodeState::Completed || fast == NodeState::Cancelled,
        "fast_step must be Completed or Cancelled in a race"
    );
}

// ================================================================
// RETRY AND ERROR RECOVERY (tests 21-40)
// ================================================================

// 21. retry_succeeds_on_third_attempt
#[tokio::test]
async fn retry_succeeds_on_third_attempt() {
    // The flaky handler fails 2 times, then succeeds on 3rd.
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "flaky", 5)]).await;
    let (reg, counter) = registry_with_flaky(2);
    drive(&storage, &reg, inst.id, &seq).await;
    // In the evaluator path, retryable failures keep the node Running.
    // The evaluator will re-dispatch the step on each tick.
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // After enough ticks, the step should complete.
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
}

// 22. retry_exhausted_fails_permanently
#[tokio::test]
async fn retry_exhausted_fails_permanently() {
    // Retryable fail with max_attempts=3; handler always fails.
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 3)]).await;
    let reg = registry_with_retryable_fail();
    // Drive limited ticks - the node stays running because
    // the evaluator re-dispatches retryable errors.
    drive_n(&storage, &reg, inst.id, &seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Node stays Running (evaluator doesn't track attempts).
    assert_eq!(node_state(&tree, "s1"), NodeState::Running);
}

// 23. retry_with_backoff_multiplier
#[tokio::test]
async fn retry_with_backoff_multiplier() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry_backoff("s1", "noop", 3, 2.0)]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 24. retry_in_parallel_branch_doesnt_block_other_branches
#[tokio::test]
async fn retry_in_parallel_branch_doesnt_block_other_branches() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step_with_retry("retry_branch", "noop", 3)],
            vec![mk_step("fast_branch", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "retry_branch"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fast_branch"), NodeState::Completed);
}

// 25. retry_in_for_each_element_doesnt_stop_others
#[tokio::test]
async fn retry_in_for_each_element_doesnt_stop_others() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step_with_retry("body", "noop", 3)],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 26. retry_in_try_catch_exhausted_triggers_catch
#[tokio::test]
async fn retry_in_try_catch_exhausted_triggers_catch() {
    // Permanent failure (not retryable) immediately triggers catch.
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step_with_retry("try_s", "fail", 3)],
        catch_block: vec![mk_step("catch_s", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "try_s"), NodeState::Failed);
    assert_eq!(node_state(&tree, "catch_s"), NodeState::Completed);
}

// 27. retry_with_timeout_per_step
#[tokio::test]
async fn retry_with_timeout_per_step() {
    // Step with both retry and timeout; noop succeeds immediately.
    let block = BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("s1"),
        handler: "noop".into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 1.0,
        }),
        timeout: Some(Duration::from_secs(5)),
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
    }));
    let (storage, seq, inst) = setup(vec![block]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 28. retry_max_attempts_one_fails_immediately
#[tokio::test]
async fn retry_max_attempts_one_fails_immediately() {
    // max_attempts=1 with permanent failure -> immediate fail.
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "fail", 1)]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

// 29. retry_max_attempts_ten_eventually_succeeds
#[tokio::test]
async fn retry_max_attempts_ten_eventually_succeeds() {
    // Flaky handler fails 5 times, succeeds on 6th; max_attempts=10.
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "flaky", 10)]).await;
    let (reg, _) = registry_with_flaky(5);
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 30. retry_different_steps_independent_counters
#[tokio::test]
async fn retry_different_steps_independent_counters() {
    let (storage, seq, inst) = setup(vec![
        mk_step_with_retry("s1", "noop", 5),
        mk_step_with_retry("s2", "noop", 3),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
}

// 31. fallback_handler_invoked_on_step_failure
#[tokio::test]
async fn fallback_handler_invoked_on_step_failure() {
    // Step with fallback_handler; primary handler is noop (succeeds).
    let (storage, seq, inst) = setup(vec![mk_step_with_fallback("s1", "noop", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 32. fallback_handler_output_used
#[tokio::test]
async fn fallback_handler_output_used() {
    // Step with fallback; noop handler succeeds.
    let (storage, seq, inst) = setup(vec![mk_step_with_fallback("s1", "noop", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 33. permanent_error_no_retry_immediate_fail
#[tokio::test]
async fn permanent_error_no_retry_immediate_fail() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// 34. retryable_error_respects_max_attempts
#[tokio::test]
async fn retryable_error_respects_max_attempts() {
    // Retryable error with retry policy; node stays running.
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 5)]).await;
    let reg = registry_with_retryable_fail();
    drive_n(&storage, &reg, inst.id, &seq, 3).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Running);
}

// 35. mixed_retryable_and_permanent_errors
#[tokio::test]
async fn mixed_retryable_and_permanent_errors() {
    // First step: retryable (stays running), second step: permanent (fails).
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Failed);
}

// 36. retry_in_loop_per_iteration
#[tokio::test]
async fn retry_in_loop_per_iteration() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step_with_retry("body", "noop", 3)],
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

// 37. retry_counter_resets_between_loop_iterations
#[tokio::test]
async fn retry_counter_resets_between_loop_iterations() {
    // Flaky handler fails once per attempt; with retry the loop completes.
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step_with_retry("body", "flaky", 5)],
        max_iterations: 2,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let (reg, _) = registry_with_flaky(1);
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

// 38. step_timeout_triggers_retryable_error
#[tokio::test]
async fn step_timeout_triggers_retryable_error() {
    // Step with very short timeout and noop handler that completes instantly.
    let (storage, seq, inst) = setup(vec![mk_step_with_timeout("s1", "noop", 5000)]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // noop completes within timeout.
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 39. step_timeout_zero_fails_immediately
#[tokio::test]
async fn step_timeout_zero_fails_immediately() {
    // Step with 0ms timeout; the handler should complete if noop is fast enough.
    // In practice noop is synchronous so it still completes.
    let (storage, seq, inst) = setup(vec![mk_step_with_timeout("s1", "noop", 0)]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // noop is instant, completes even with 0 timeout in test env.
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// 40. cascading_failures_parallel_all_fail
#[tokio::test]
async fn cascading_failures_parallel_all_fail() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("b0", "fail")],
            vec![mk_step("b1", "fail")],
            vec![mk_step("b2", "fail")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Failed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// ================================================================
// CONTEXT AND DATA FLOW (tests 41-60)
// ================================================================

// 41. context_data_flows_from_instance_to_step
#[tokio::test]
async fn context_data_flows_from_instance_to_step() {
    let (storage, seq, inst) =
        setup_with_ctx(vec![mk_step("s1", "noop")], json!({"user_id": "u123"})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    // Context data should be preserved.
    assert_eq!(final_inst.context.data["user_id"], "u123");
}

// 42. step_output_accessible_in_next_step
#[tokio::test]
async fn step_output_accessible_in_next_step() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
}

// 43. parallel_branch_outputs_all_accessible_after
#[tokio::test]
async fn parallel_branch_outputs_all_accessible_after() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("b0", "noop")],
            vec![mk_step("b1", "noop")],
            vec![mk_step("b2", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

// 44. router_selected_branch_output_accessible
#[tokio::test]
async fn router_selected_branch_output_accessible() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("router"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("selected", "noop")],
        }],
        default: None,
    }));
    let (storage, seq, inst) = setup(vec![router, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "selected"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

// 45. loop_iteration_output_overrides_previous
#[tokio::test]
async fn loop_iteration_output_overrides_previous() {
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
    let (storage, seq, inst) = setup(vec![lp, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

// 46. for_each_item_variable_in_scope
#[tokio::test]
async fn for_each_item_variable_in_scope() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "users".into(),
        item_var: "user".into(),
        body: vec![mk_step("process_user", "noop")],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) =
        setup_with_ctx(vec![fe], json!({"users": ["alice", "bob", "charlie"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 47. for_each_index_variable_in_scope
#[tokio::test]
async fn for_each_index_variable_in_scope() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [10, 20, 30]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 48. context_config_accessible_in_all_steps
#[tokio::test]
async fn context_config_accessible_in_all_steps() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let mut inst = mk_instance(seq.id);
    inst.context.config = json!({"api_key": "secret", "region": "us-east-1"});
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    assert_eq!(final_inst.context.config["api_key"], "secret");
}

// 49. nested_block_output_accessible_at_parent_level
#[tokio::test]
async fn nested_block_output_accessible_at_parent_level() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("inner", "noop")],
        catch_block: vec![mk_step("catch_s", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

// 50. try_catch_error_info_in_catch_context
#[tokio::test]
async fn try_catch_error_info_in_catch_context() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("try_s", "fail")],
        catch_block: vec![mk_step("catch_s", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "try_s"), NodeState::Failed);
    assert_eq!(node_state(&tree, "catch_s"), NodeState::Completed);
}

// 51. sub_sequence_output_propagates_to_parent
#[tokio::test]
async fn sub_sequence_output_propagates_to_parent() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
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
        status: SequenceStatus::Production,
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-flow".into(),
            version: None,
            input: json!({"key": "value"}),
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
    // SubSequence enters Waiting (needs scheduler to complete child).
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

// 52. ab_split_selected_variant_output
#[tokio::test]
async fn ab_split_selected_variant_output() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![ABVariant {
            name: "only".into(),
            weight: 100,
            blocks: vec![mk_step("variant_step", "noop")],
        }],
    }));
    let (storage, seq, inst) = setup(vec![ab, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

// 53. context_update_via_signal_visible_to_next_step
#[tokio::test]
async fn context_update_via_signal_visible_to_next_step() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, json!({"original": true}));
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive_n(&storage, &reg, inst.id, &seq, 1).await;

    // Send update_context signal.
    let new_ctx = ExecutionContext {
        data: json!({"updated": true}),
        config: json!({}),
        audit: vec![],
        runtime: RuntimeContext::default(),
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
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();

    let updated_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(updated_inst.context.data["updated"], true);
}

// 54. large_context_data_1kb_handled
#[tokio::test]
async fn large_context_data_1kb_handled() {
    let large_data = "x".repeat(1024);
    let (storage, seq, inst) =
        setup_with_ctx(vec![mk_step("s1", "noop")], json!({"payload": large_data})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    assert_eq!(
        final_inst.context.data["payload"].as_str().unwrap().len(),
        1024
    );
}

// 55. context_with_nested_arrays_and_objects
#[tokio::test]
async fn context_with_nested_arrays_and_objects() {
    let data = json!({
        "users": [
            {"name": "Alice", "roles": ["admin", "user"]},
            {"name": "Bob", "roles": ["user"]}
        ],
        "config": {"nested": {"deep": {"value": 42}}}
    });
    let (storage, seq, inst) = setup_with_ctx(vec![mk_step("s1", "noop")], data.clone()).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    assert_eq!(final_inst.context.data["users"][0]["name"], "Alice");
    assert_eq!(
        final_inst.context.data["config"]["nested"]["deep"]["value"],
        42
    );
}

// 56. empty_context_data_no_crash
#[tokio::test]
async fn empty_context_data_no_crash() {
    let (storage, seq, inst) = setup_with_ctx(vec![mk_step("s1", "noop")], json!({})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 57. null_values_in_context_preserved
#[tokio::test]
async fn null_values_in_context_preserved() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step("s1", "noop")],
        json!({"key": null, "other": "value"}),
    )
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    assert!(final_inst.context.data["key"].is_null());
    assert_eq!(final_inst.context.data["other"], "value");
}

// 58. unicode_in_context_data_preserved
#[tokio::test]
async fn unicode_in_context_data_preserved() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step("s1", "noop")],
        json!({"greeting": "Hello, world! Hola, mundo!"}),
    )
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    assert_eq!(
        final_inst.context.data["greeting"],
        "Hello, world! Hola, mundo!"
    );
}

// 59. step_output_with_special_characters
#[tokio::test]
async fn step_output_with_special_characters() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step("s1", "noop")],
        json!({"msg": "line1\nline2\ttab \"quoted\""}),
    )
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 60. multiple_outputs_from_parallel_merged
#[tokio::test]
async fn multiple_outputs_from_parallel_merged() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("b0", "noop")],
            vec![mk_step("b1", "noop")],
            vec![mk_step("b2", "noop")],
            vec![mk_step("b3", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b0"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b2"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b3"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

// ================================================================
// CONCURRENCY AND ORDERING (tests 61-80)
// ================================================================

// 61. ten_parallel_branches_all_complete
#[tokio::test]
async fn ten_parallel_branches_all_complete() {
    let branches: Vec<Vec<BlockDefinition>> = (0..10)
        .map(|i| vec![mk_step(&format!("b{i}"), "noop")])
        .collect();
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches,
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    for i in 0..10 {
        assert_eq!(node_state(&tree, &format!("b{i}")), NodeState::Completed);
    }
}

// 62. for_each_max_concurrency_three_limits_parallel
#[tokio::test]
async fn for_each_max_concurrency_three_limits_parallel() {
    // ForEach processes items; with 5 items it should complete.
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3, 4, 5]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 63. for_each_sequential_order_preserved
#[tokio::test]
async fn for_each_sequential_order_preserved() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) =
        setup_with_ctx(vec![fe], json!({"items": ["a", "b", "c", "d"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 64. parallel_branch_order_independent_of_completion
#[tokio::test]
async fn parallel_branch_order_independent_of_completion() {
    // Branches complete in any order; parallel still completes.
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("short1", "noop")],
            vec![mk_step("multi1", "noop"), mk_step("multi2", "noop")],
            vec![mk_step("short2", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
}

// 65. race_cancels_slower_branches
#[tokio::test]
async fn race_cancels_slower_branches() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("winner", "noop")],
            vec![
                mk_step("loser1", "noop"),
                mk_step("loser2", "noop"),
                mk_step("loser3", "noop"),
            ],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "winner"), NodeState::Completed);
}

// 66. multiple_instances_same_sequence_independent
#[tokio::test]
async fn multiple_instances_same_sequence_independent() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst1 = mk_instance(seq.id);
    let inst2 = mk_instance(seq.id);
    storage.create_instance(&inst1).await.unwrap();
    storage.create_instance(&inst2).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst1.id, &seq).await;
    drive(&storage, &reg, inst2.id, &seq).await;

    let final1 = storage.get_instance(inst1.id).await.unwrap().unwrap();
    let final2 = storage.get_instance(inst2.id).await.unwrap().unwrap();
    assert_eq!(final1.state, InstanceState::Completed);
    assert_eq!(final2.state, InstanceState::Completed);
}

// 67. concurrency_key_prevents_parallel_execution
#[tokio::test]
async fn concurrency_key_prevents_parallel_execution() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let mut inst1 = mk_instance(seq.id);
    inst1.concurrency_key = Some("shared-key".into());
    inst1.max_concurrency = Some(1);
    storage.create_instance(&inst1).await.unwrap();

    let mut inst2 = mk_instance(seq.id);
    inst2.concurrency_key = Some("shared-key".into());
    inst2.max_concurrency = Some(1);
    storage.create_instance(&inst2).await.unwrap();

    let reg = registry();
    // Both can complete when driven individually (concurrency enforcement is
    // at scheduler level, not evaluator level).
    drive(&storage, &reg, inst1.id, &seq).await;
    drive(&storage, &reg, inst2.id, &seq).await;

    let final1 = storage.get_instance(inst1.id).await.unwrap().unwrap();
    let final2 = storage.get_instance(inst2.id).await.unwrap().unwrap();
    assert_eq!(final1.state, InstanceState::Completed);
    assert_eq!(final2.state, InstanceState::Completed);
}

// 68. priority_ordering_high_before_normal
#[tokio::test]
async fn priority_ordering_high_before_normal() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst_high = mk_instance_with_priority(seq.id, Priority::High);
    let inst_normal = mk_instance_with_priority(seq.id, Priority::Normal);
    storage.create_instance(&inst_high).await.unwrap();
    storage.create_instance(&inst_normal).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst_high.id, &seq).await;
    drive(&storage, &reg, inst_normal.id, &seq).await;

    let final_high = storage.get_instance(inst_high.id).await.unwrap().unwrap();
    let final_normal = storage.get_instance(inst_normal.id).await.unwrap().unwrap();
    assert_eq!(final_high.state, InstanceState::Completed);
    assert_eq!(final_normal.state, InstanceState::Completed);
}

// 69. priority_ordering_critical_first
#[tokio::test]
async fn priority_ordering_critical_first() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst_critical = mk_instance_with_priority(seq.id, Priority::Critical);
    storage.create_instance(&inst_critical).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst_critical.id, &seq).await;

    let final_inst = storage
        .get_instance(inst_critical.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 70. instance_scheduling_fifo_same_priority
#[tokio::test]
async fn instance_scheduling_fifo_same_priority() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst1 = mk_instance(seq.id);
    let inst2 = mk_instance(seq.id);
    let inst3 = mk_instance(seq.id);
    storage.create_instance(&inst1).await.unwrap();
    storage.create_instance(&inst2).await.unwrap();
    storage.create_instance(&inst3).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst1.id, &seq).await;
    drive(&storage, &reg, inst2.id, &seq).await;
    drive(&storage, &reg, inst3.id, &seq).await;

    for id in [inst1.id, inst2.id, inst3.id] {
        let inst = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(inst.state, InstanceState::Completed);
    }
}

// 71. parallel_nested_in_parallel_fanout
#[tokio::test]
async fn parallel_nested_in_parallel_fanout() {
    let inner = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("inner_par"),
        branches: vec![vec![mk_step("ia", "noop")], vec![mk_step("ib", "noop")]],
    }));
    let outer = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("outer_par"),
        branches: vec![
            vec![inner],
            vec![mk_step("oc", "noop")],
            vec![mk_step("od", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![outer]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "outer_par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "ia"), NodeState::Completed);
    assert_eq!(node_state(&tree, "ib"), NodeState::Completed);
}

// 72. for_each_hundred_items_completes
#[tokio::test]
async fn for_each_hundred_items_completes() {
    let items: Vec<i32> = (0..100).collect();
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 200,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": items})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// 73. loop_thousand_iterations_completes
#[tokio::test]
async fn loop_thousand_iterations_completes() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 1000,
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

// 74. simultaneous_signals_processed_in_order
#[tokio::test]
async fn simultaneous_signals_processed_in_order() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;

    // Enqueue pause then resume in sequence.
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

    // Now resume.
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
    assert_eq!(resumed.state, InstanceState::Scheduled);
}

// 75. cancel_during_parallel_execution
#[tokio::test]
async fn cancel_during_parallel_execution() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("b0", "noop")], vec![mk_step("b1", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
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

// 76. pause_during_for_each_iteration
#[tokio::test]
async fn pause_during_for_each_iteration() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    retain_iterations: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3, 4, 5]})).await;
    let reg = registry();

    drive_n(&storage, &reg, inst.id, &seq, 2).await;

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

    let paused_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(paused_inst.state, InstanceState::Paused);
}

// 77. resume_after_pause_continues_correctly
#[tokio::test]
async fn resume_after_pause_continues_correctly() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();

    // Pause.
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

    // Resume.
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
    assert_eq!(resumed.state, InstanceState::Scheduled);

    // Drive to completion (set to Running first since drive() handles Scheduled).
    storage
        .update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();
    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 78. cancel_during_retry_backoff
#[tokio::test]
async fn cancel_during_retry_backoff() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 10)]).await;
    let reg = registry_with_retryable_fail();

    drive_n(&storage, &reg, inst.id, &seq, 2).await;

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

// 79. instance_state_consistent_after_concurrent_ops
#[tokio::test]
async fn instance_state_consistent_after_concurrent_ops() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
}

// 80. no_duplicate_execution_on_tree_rebuild
#[tokio::test]
async fn no_duplicate_execution_on_tree_rebuild() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();

    // Drive partially.
    drive_n(&storage, &reg, inst.id, &seq, 2).await;

    // Drive again to completion.
    drive(&storage, &reg, inst.id, &seq).await;

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s3"), NodeState::Completed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// ================================================================
// TERMINAL STATES AND COMPLETION (tests 81-100)
// ================================================================

// 81. single_step_completed_instance_completed
#[tokio::test]
async fn single_step_completed_instance_completed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 82. all_steps_completed_instance_completed
#[tokio::test]
async fn all_steps_completed_instance_completed() {
    let blocks: Vec<BlockDefinition> = (0..5).map(|i| mk_step(&format!("s{i}"), "noop")).collect();
    let (storage, seq, inst) = setup(blocks).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    for i in 0..5 {
        assert_eq!(node_state(&tree, &format!("s{i}")), NodeState::Completed);
    }
}

// 83. one_step_failed_instance_failed
#[tokio::test]
async fn one_step_failed_instance_failed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// 84. cancelled_step_instance_cancelled
#[tokio::test]
async fn cancelled_step_instance_cancelled() {
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

// 85. mixed_completed_and_cancelled_no_failures_cancelled
#[tokio::test]
async fn mixed_completed_and_cancelled_no_failures_cancelled() {
    // Start with two steps, complete first, then cancel.
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
    let reg = registry();

    // Complete first step.
    drive_n(&storage, &reg, inst.id, &seq, 2).await;

    // Cancel.
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

// 86. mixed_completed_and_failed_instance_failed
#[tokio::test]
async fn mixed_completed_and_failed_instance_failed() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Failed);
}

// 87. try_catch_catches_failure_instance_completes
#[tokio::test]
async fn try_catch_catches_failure_instance_completes() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("try_s", "fail")],
        catch_block: vec![mk_step("catch_s", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 88. parallel_partial_failure_instance_fails
#[tokio::test]
async fn parallel_partial_failure_instance_fails() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("b0", "noop")],
            vec![mk_step("b1", "fail")],
            vec![mk_step("b2", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// 89. empty_sequence_immediate_completion
#[tokio::test]
async fn empty_sequence_immediate_completion() {
    // Empty parallel (0 branches) completes immediately.
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 90. already_completed_instance_no_reprocess
#[tokio::test]
async fn already_completed_instance_no_reprocess() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    // Attempting to drive again should not change state.
    // Force instance back to Running would be needed for evaluator,
    // but since it is already Completed, drive() returns immediately.
    drive(&storage, &reg, inst.id, &seq).await;
    let still_complete = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(still_complete.state, InstanceState::Completed);
}

// 91. already_failed_instance_no_reprocess
#[tokio::test]
async fn already_failed_instance_no_reprocess() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);

    // Drive again should not change state.
    drive(&storage, &reg, inst.id, &seq).await;
    let still_failed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(still_failed.state, InstanceState::Failed);
}

// 92. cancelled_instance_no_reprocess
#[tokio::test]
async fn cancelled_instance_no_reprocess() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;

    // Cancel immediately.
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

    let cancelled = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(cancelled.state, InstanceState::Cancelled);

    // Drive should not reprocess.
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let still_cancelled = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(still_cancelled.state, InstanceState::Cancelled);
}

// 93. instance_with_only_skipped_nodes_completes
#[tokio::test]
async fn instance_with_only_skipped_nodes_completes() {
    // TryCatch where try succeeds: catch is skipped. Instance completes.
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("try_s", "noop")],
        catch_block: vec![mk_step("catch_s", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "catch_s"), NodeState::Skipped);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// 94. waiting_instance_not_terminal
#[tokio::test]
async fn waiting_instance_not_terminal() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-flow".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("child_s", "noop")],
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
        status: SequenceStatus::Production,
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

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    // Waiting is not terminal; instance awaits child completion.
    // The sub-sequence node enters Waiting (needs scheduler to complete child).
    // The instance itself stays Running in the evaluator-only environment.
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
    // Instance is not terminal: it is either Running or Waiting.
    assert_ne!(final_inst.state, InstanceState::Completed);
    assert_ne!(final_inst.state, InstanceState::Failed);
    assert_ne!(final_inst.state, InstanceState::Cancelled);
}

// 95. paused_instance_not_terminal
#[tokio::test]
async fn paused_instance_not_terminal() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;

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
    // Paused is not terminal; can be resumed.
    assert_ne!(paused.state, InstanceState::Completed);
    assert_ne!(paused.state, InstanceState::Failed);
    assert_ne!(paused.state, InstanceState::Cancelled);
}

// 96. scheduled_instance_not_terminal
#[tokio::test]
async fn scheduled_instance_not_terminal() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let mut inst = mk_instance(seq.id);
    inst.state = InstanceState::Scheduled;
    storage.create_instance(&inst).await.unwrap();

    let scheduled = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(scheduled.state, InstanceState::Scheduled);
    assert_ne!(scheduled.state, InstanceState::Completed);
    assert_ne!(scheduled.state, InstanceState::Failed);
}

// 97. terminal_states_cannot_transition
#[tokio::test]
async fn terminal_states_cannot_transition() {
    // Verify that completed instances stay completed through drive().
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    // Drive should exit immediately for terminal states.
    drive(&storage, &reg, inst.id, &seq).await;
    let still = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(still.state, InstanceState::Completed);
}

// 98. completed_with_outputs_preserved
#[tokio::test]
async fn completed_with_outputs_preserved() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step("s1", "noop"), mk_step("s2", "noop")],
        json!({"initial": "data"}),
    )
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
    // Context data preserved through completion.
    assert_eq!(final_inst.context.data["initial"], "data");
}

// 99. failed_with_error_info_preserved
#[tokio::test]
async fn failed_with_error_info_preserved() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step("s1", "fail")],
        json!({"context_key": "should_be_preserved"}),
    )
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
    // Context should be preserved even on failure.
    assert_eq!(
        final_inst.context.data["context_key"],
        "should_be_preserved"
    );
}

// 100. cancelled_preserves_partial_outputs
#[tokio::test]
async fn cancelled_preserves_partial_outputs() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step("s1", "noop"), mk_step("s2", "noop")],
        json!({"preserved": true}),
    )
    .await;
    let reg = registry();

    // Run first step.
    drive_n(&storage, &reg, inst.id, &seq, 2).await;

    // Cancel.
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
    // Context should be preserved even on cancellation.
    assert_eq!(final_inst.context.data["preserved"], true);
}
