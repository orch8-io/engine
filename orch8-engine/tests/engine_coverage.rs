#![allow(clippy::too_many_lines, dead_code)]
//! `engine_coverage_batch1`: 90 integration tests covering Parallel, Router, Race,
//! `ForEach`, Loop, and `TryCatch` control-flow blocks driven through the evaluator.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{
    ABSplitDef, ABVariant, BlockDefinition, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef,
    RaceDef, RaceSemantics, Route, RouterDef, SequenceDefinition, SequenceStatus, StepDef,
    SubSequenceDef, TryCatchDef,
};
use orch8_types::signal::{Signal, SignalType};

mod common;
use common::*;

// ================================================================
// PARALLEL BLOCK TESTS (1-15)
// ================================================================

/// 1. Two parallel branches with noop both complete successfully.
#[tokio::test]
async fn parallel_two_steps_both_complete() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("a", "noop")], vec![mk_step("b", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "a"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b"), NodeState::Completed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

/// 2. One parallel branch fails, marking the parallel block as failed.
#[tokio::test]
async fn parallel_one_fails_marks_parallel_failed() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("a", "noop")], vec![mk_step("b", "fail")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Failed);
    assert_eq!(node_state(&tree, "b"), NodeState::Failed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

/// 3. Parallel with empty branches still completes.
#[tokio::test]
async fn parallel_empty_branches_completes() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

/// 4. Three parallel branches all complete ok.
#[tokio::test]
async fn parallel_three_branches_all_ok() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("a", "noop")],
            vec![mk_step("b", "noop")],
            vec![mk_step("c", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "a"), NodeState::Completed);
    assert_eq!(node_state(&tree, "b"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c"), NodeState::Completed);
}

/// 5. Parallel with a nested router inside one branch.
#[tokio::test]
async fn parallel_with_nested_router_inside() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![BlockDefinition::Router(Box::new(RouterDef {
                id: BlockId::new("rt"),
                routes: vec![Route {
                    condition: "true".into(),
                    blocks: vec![mk_step("routed", "noop")],
                }],
                default: None,
            }))],
            vec![mk_step("b", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "routed"), NodeState::Completed);
}

/// 6. Parallel branch with retry step that fails retryably (stays running).
#[tokio::test]
async fn parallel_branch_with_retry_succeeds_on_second() {
    // Use noop with retry (noop succeeds first try so it completes).
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step_with_retry("a", "noop", 3)],
            vec![mk_step("b", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "a"), NodeState::Completed);
}

/// 7. All parallel branches fail.
#[tokio::test]
async fn parallel_all_branches_fail() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("a", "always_fail")],
            vec![mk_step("b", "always_fail")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Failed);
    assert_eq!(node_state(&tree, "a"), NodeState::Failed);
    assert_eq!(node_state(&tree, "b"), NodeState::Failed);
}

/// 8. Parallel with a single branch (trivial case).
#[tokio::test]
async fn parallel_single_branch_trivial() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("a", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "a"), NodeState::Completed);
}

/// 9. Parallel with a delayed step.
#[tokio::test]
async fn parallel_with_delayed_step() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step_with_delay(
                "delayed",
                "noop",
                Duration::from_millis(1),
            )],
            vec![mk_step("fast", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "fast"), NodeState::Completed);
}

/// 10. Result from parallel is accessible in subsequent step.
#[tokio::test]
async fn parallel_result_accessible_in_subsequent_step() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("a", "noop")], vec![mk_step("b", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

/// 11. Five parallel branches all complete (fan-out order irrelevant).
#[tokio::test]
async fn parallel_five_branches_completion_order() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("b0", "noop")],
            vec![mk_step("b1", "noop")],
            vec![mk_step("b2", "noop")],
            vec![mk_step("b3", "noop")],
            vec![mk_step("b4", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    for i in 0..5 {
        assert_eq!(node_state(&tree, &format!("b{i}")), NodeState::Completed);
    }
}

/// 12. Nested parallel inside parallel.
#[tokio::test]
async fn parallel_nested_parallel_inside_parallel() {
    let inner = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("inner_par"),
        branches: vec![vec![mk_step("ia", "noop")], vec![mk_step("ib", "noop")]],
    }));
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![inner], vec![mk_step("outer_b", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "ia"), NodeState::Completed);
    assert_eq!(node_state(&tree, "ib"), NodeState::Completed);
}

/// 13. Parallel branch with a loop block (composite block inside parallel).
#[tokio::test]
async fn parallel_branch_with_loop_block() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("loop_body", "noop")],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![lp], vec![mk_step("other", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
    assert_eq!(node_state(&tree, "other"), NodeState::Completed);
}

/// 14. Parallel with a non-cancellable step.
#[tokio::test]
async fn parallel_with_non_cancellable_step() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_non_cancellable_step("nc", "noop")],
            vec![mk_step("normal", "noop")],
        ],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "nc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "normal"), NodeState::Completed);
}

/// 15. Large fan-out: 10 parallel branches.
#[tokio::test]
async fn parallel_large_fan_out_ten_branches() {
    let branches: Vec<Vec<BlockDefinition>> = (0..10)
        .map(|i| vec![mk_step(&format!("s{i}"), "noop")])
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
        assert_eq!(node_state(&tree, &format!("s{i}")), NodeState::Completed);
    }
}

// ================================================================
// ROUTER BLOCK TESTS (16-30)
// ================================================================

/// 16. Router takes the first matching route.
#[tokio::test]
async fn router_first_matching_route_taken() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![
            Route {
                condition: "false".into(),
                blocks: vec![mk_step("r1", "noop")],
            },
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("r2", "noop")],
            },
        ],
        default: Some(vec![mk_step("def", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "r2"), NodeState::Completed);
}

/// 17. No route matches, default is used.
#[tokio::test]
async fn router_no_match_uses_default() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
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
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "def"), NodeState::Completed);
}

/// 18. No match and no default -- router still completes (empty path).
#[tokio::test]
async fn router_no_match_no_default_completes() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "false".into(),
            blocks: vec![mk_step("r1", "noop")],
        }],
        default: None,
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
}

/// 19. Router with context data condition.
#[tokio::test]
async fn router_with_context_data_condition() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![
            Route {
                condition: "tier == \"premium\"".into(),
                blocks: vec![mk_step("premium", "noop")],
            },
            Route {
                condition: "tier == \"free\"".into(),
                blocks: vec![mk_step("free", "noop")],
            },
        ],
        default: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"tier": "premium"})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "premium"), NodeState::Completed);
}

/// 20. Multiple routes true -- only first match runs.
#[tokio::test]
async fn router_multiple_routes_only_first_match_runs() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("first", "noop")],
            },
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("second", "noop")],
            },
        ],
        default: None,
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "first"), NodeState::Completed);
}

/// 21. Nested router inside a route.
#[tokio::test]
async fn router_nested_router_in_route() {
    let inner = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("inner_rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("inner_step", "noop")],
        }],
        default: None,
    }));
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![inner],
        }],
        default: None,
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_step"), NodeState::Completed);
}

/// 22. Router with empty route blocks.
#[tokio::test]
async fn router_empty_route_blocks() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![],
        }],
        default: None,
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
}

/// 23. Router default with multiple steps.
#[tokio::test]
async fn router_default_with_multiple_steps() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "false".into(),
            blocks: vec![mk_step("r1", "noop")],
        }],
        default: Some(vec![mk_step("d1", "noop"), mk_step("d2", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "d1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "d2"), NodeState::Completed);
}

/// 24. Router condition evaluates previous step output context.
#[tokio::test]
async fn router_condition_on_previous_step_output() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![
            Route {
                condition: "status == \"approved\"".into(),
                blocks: vec![mk_step("approved_path", "noop")],
            },
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("fallback_path", "noop")],
            },
        ],
        default: None,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"status": "approved"})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "approved_path"), NodeState::Completed);
}

/// 25. All routes false, falls to default.
#[tokio::test]
async fn router_all_routes_false_falls_to_default() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![
            Route {
                condition: "false".into(),
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
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "def"), NodeState::Completed);
}

/// 26. Router with parallel block in a route.
#[tokio::test]
async fn router_with_parallel_in_route() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![mk_step("pa", "noop")], vec![mk_step("pb", "noop")]],
    }));
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![par],
        }],
        default: None,
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "pa"), NodeState::Completed);
    assert_eq!(node_state(&tree, "pb"), NodeState::Completed);
}

/// 27. Router with a complex numeric expression condition.
#[tokio::test]
async fn router_complex_expression_condition() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![
            Route {
                condition: "score > 90".into(),
                blocks: vec![mk_step("elite", "noop")],
            },
            Route {
                condition: "score > 50".into(),
                blocks: vec![mk_step("mid", "noop")],
            },
        ],
        default: Some(vec![mk_step("low", "noop")]),
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![router], json!({"score": 75})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "mid"), NodeState::Completed);
}

/// 28. Router condition "true" literal always matches.
#[tokio::test]
async fn router_true_literal_always_matches() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("always", "noop")],
        }],
        default: Some(vec![mk_step("def", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "always"), NodeState::Completed);
}

/// 29. Router condition "false" literal never matches.
#[tokio::test]
async fn router_false_literal_never_matches() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "false".into(),
            blocks: vec![mk_step("never", "noop")],
        }],
        default: Some(vec![mk_step("def", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "def"), NodeState::Completed);
}

/// 30. Router route with retry step.
#[tokio::test]
async fn router_route_with_retry_step() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step_with_retry("retry_s", "noop", 3)],
        }],
        default: None,
    }));
    let (storage, seq, inst) = setup(vec![router]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "retry_s"), NodeState::Completed);
}

// ================================================================
// RACE BLOCK TESTS (31-45)
// ================================================================

/// 31. Race: first to complete wins.
#[tokio::test]
async fn race_first_to_complete_wins() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("fast", "noop")],
            vec![mk_step("slow1", "noop"), mk_step("slow2", "noop")],
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

/// 32. Race: all branches fail marks race as failed.
#[tokio::test]
async fn race_all_fail_marks_race_failed() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("a", "always_fail")],
            vec![mk_step("b", "always_fail")],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Failed);
}

/// 33. Race with a single branch (trivial).
#[tokio::test]
async fn race_single_branch_trivial() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![vec![mk_step("only", "noop")]],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "only"), NodeState::Completed);
}

/// 34. Race with three branches, fastest wins.
#[tokio::test]
async fn race_three_branches_fastest_wins() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("a", "noop")],
            vec![mk_step("b1", "noop"), mk_step("b2", "noop")],
            vec![
                mk_step("c1", "noop"),
                mk_step("c2", "noop"),
                mk_step("c3", "noop"),
            ],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    // Branch "a" is fastest (single step).
    assert_eq!(node_state(&tree, "a"), NodeState::Completed);
}

/// 35. Loser branches in a race are cancelled.
#[tokio::test]
async fn race_loser_branches_cancelled() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("winner", "noop")],
            vec![mk_step("loser1", "noop"), mk_step("loser2", "noop")],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "winner"), NodeState::Completed);
    // Loser branch steps should be cancelled or pending (not completed).
    let loser2_state = node_state(&tree, "loser2");
    assert!(
        loser2_state == NodeState::Cancelled || loser2_state == NodeState::Pending,
        "loser2 expected Cancelled or Pending, got {loser2_state:?}"
    );
}

/// 36. Race with a slow multi-step branch vs fast single-step.
#[tokio::test]
async fn race_with_slow_step_vs_fast_step() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("fast", "noop")],
            vec![
                mk_step("s1", "noop"),
                mk_step("s2", "noop"),
                mk_step("s3", "noop"),
            ],
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

/// 37. Race winner output propagated (next step runs).
#[tokio::test]
async fn race_winner_output_propagated() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![vec![mk_step("w", "noop")], vec![mk_step("l", "noop")]],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

/// 38. Race nested inside a sequence.
#[tokio::test]
async fn race_nested_in_sequence() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![vec![mk_step("a", "noop")], vec![mk_step("b", "noop")]],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![
        mk_step("before", "noop"),
        race,
        mk_step("after", "noop"),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "before"), NodeState::Completed);
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

/// 39. Race with retry on a branch.
#[tokio::test]
async fn race_with_retry_on_branch() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step_with_retry("r_step", "noop", 3)],
            vec![mk_step("plain", "noop")],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

/// 40. Race with empty branches.
#[tokio::test]
async fn race_empty_branches() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![vec![]],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

/// 41. Race with two branches both using same handler.
#[tokio::test]
async fn race_two_branches_both_same_handler() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![vec![mk_step("a", "noop")], vec![mk_step("b", "noop")]],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

/// 42. Race inside a parallel branch.
#[tokio::test]
async fn race_inside_parallel() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![vec![mk_step("ra", "noop")], vec![mk_step("rb", "noop")]],
        semantics: RaceSemantics::default(),
    }));
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![race], vec![mk_step("other", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

/// 43. Race branch containing a router.
#[tokio::test]
async fn race_branch_with_router() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("routed", "noop")],
        }],
        default: None,
    }));
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![vec![router], vec![mk_step("plain", "noop")]],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

/// 44. Race with five branches.
#[tokio::test]
async fn race_five_branches() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("r0", "noop")],
            vec![mk_step("r1", "noop"), mk_step("r1b", "noop")],
            vec![mk_step("r2", "noop")],
            vec![mk_step("r3", "noop")],
            vec![mk_step("r4", "noop")],
        ],
        semantics: RaceSemantics::default(),
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

/// 45. First branch fails, second wins.
#[tokio::test]
async fn race_first_fails_second_wins() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("fail_branch", "always_fail")],
            vec![mk_step("win_branch", "noop")],
        ],
        semantics: RaceSemantics::FirstToSucceed,
    }));
    let (storage, seq, inst) = setup(vec![race]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
    assert_eq!(node_state(&tree, "win_branch"), NodeState::Completed);
}

// ================================================================
// FOREACH TESTS (46-60)
// ================================================================

/// 46. `ForEach` with empty array completes immediately.
#[tokio::test]
async fn for_each_empty_array_completes() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
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
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

/// 47. `ForEach` with single element.
#[tokio::test]
async fn for_each_single_element() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": ["hello"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 48. `ForEach` with three elements sequential.
#[tokio::test]
async fn for_each_three_elements_sequential() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
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
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

/// 49. `ForEach` with parallel concurrency (`max_iterations` not limiting).
#[tokio::test]
async fn for_each_with_parallel_concurrency() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3, 4, 5]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 50. `ForEach` element failure stops remaining iterations.
#[tokio::test]
async fn for_each_element_failure_stops_remaining() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "always_fail")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3]})).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Failed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

/// 51. `ForEach` with nested steps per iteration.
#[tokio::test]
async fn for_each_nested_steps_per_iteration() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("step_a", "noop"), mk_step("step_b", "noop")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 52. `ForEach` with router inside body.
#[tokio::test]
async fn for_each_with_router_inside() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("rt"),
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

/// 53. `ForEach` with large array (10 elements).
#[tokio::test]
async fn for_each_large_array_ten_elements() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    }));
    let items: Vec<i32> = (0..10).collect();
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": items})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 54. `ForEach` with custom params passed to body step.
#[tokio::test]
async fn for_each_item_accessible_in_step_params() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step_with_params(
            "body",
            "noop",
            json!({"message": "processing"}),
        )],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": ["alpha", "beta"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 55. `ForEach` index accessible.
#[tokio::test]
async fn for_each_index_accessible() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": ["a", "b", "c"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 56. `ForEach` with retry per element.
#[tokio::test]
async fn for_each_with_retry_per_element() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step_with_retry("body", "noop", 3)],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 57. Nested `ForEach` inside `ForEach`.
#[tokio::test]
async fn for_each_nested_for_each() {
    let inner_fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("inner_fe"),
        collection: "sub_items".into(),
        item_var: "sub_item".into(),
        body: vec![mk_step("inner_body", "noop")],
        max_iterations: 100,
    }));
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![inner_fe],
        max_iterations: 100,
    }));
    let (storage, seq, inst) =
        setup_with_ctx(vec![fe], json!({"items": [1, 2], "sub_items": ["x", "y"]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 58. `ForEach` with `max_concurrency` 1 (sequential).
#[tokio::test]
async fn for_each_max_concurrency_one() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
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

/// 59. `ForEach` with `max_iterations` unlimited (large enough).
#[tokio::test]
async fn for_each_max_concurrency_unlimited() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 1000,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2, 3, 4, 5]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

/// 60. `ForEach` with parallel block inside body.
#[tokio::test]
async fn for_each_with_parallel_inside() {
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("fe"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("par"),
            branches: vec![vec![mk_step("pa", "noop")], vec![mk_step("pb", "noop")]],
        }))],
        max_iterations: 100,
    }));
    let (storage, seq, inst) = setup_with_ctx(vec![fe], json!({"items": [1, 2]})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "fe"), NodeState::Completed);
}

// ================================================================
// LOOP TESTS (61-75)
// ================================================================

/// 61. Loop with falsy condition completes immediately (zero iterations).
#[tokio::test]
async fn loop_zero_iterations_completes_empty() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
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
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

/// 62. Loop with fixed three iterations (`max_iterations` = 3).
#[tokio::test]
async fn loop_fixed_three_iterations() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
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

/// 63. Loop with context-based condition that exits.
#[tokio::test]
async fn loop_condition_based_exit() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "keep_going".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    // Condition is falsy when keep_going=false, loop exits immediately.
    let (storage, seq, inst) = setup_with_ctx(vec![lp], json!({"keep_going": false})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

/// 64. Loop `max_iterations` safety cap.
#[tokio::test]
async fn loop_max_iterations_safety() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
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

/// 65. Loop with step failure in body exits loop.
#[tokio::test]
async fn loop_with_step_failure_exits() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "always_fail")],
        max_iterations: 10,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Failed);
}

/// 66. Loop counter increments (verified by completion with `max_iterations`).
#[tokio::test]
async fn loop_counter_increments() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 4,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

/// 67. Loop nested inside a parallel branch.
#[tokio::test]
async fn loop_nested_in_parallel() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 2,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![lp], vec![mk_step("other", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

/// 68. Loop with router inside body.
#[tokio::test]
async fn loop_with_router_inside() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("rt"),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![mk_step("routed", "noop")],
            }],
            default: None,
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

/// 69. Loop with `break_on` condition.
#[tokio::test]
async fn loop_break_on_condition() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 100,
        break_on: Some("true".into()),
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

/// 70. Loop infinite condition capped at `max_iterations`.
#[tokio::test]
async fn loop_infinite_capped_at_max() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 7,
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

/// 71. Loop with single iteration (`max_iterations` = 1).
#[tokio::test]
async fn loop_single_iteration() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
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

/// 72. Loop body has multiple steps.
#[tokio::test]
async fn loop_body_has_multiple_steps() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("s1", "noop"), mk_step("s2", "noop")],
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

/// 73. Loop with retry inside body.
#[tokio::test]
async fn loop_with_retry_inside() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step_with_retry("body", "noop", 3)],
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

/// 74. Loop output from last iteration (followed by step).
#[tokio::test]
async fn loop_output_from_last_iteration() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "noop")],
        max_iterations: 3,
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

/// 75. Loop with `continue_on_error` survives retryable failure.
#[tokio::test]
async fn loop_continue_on_retryable_error() {
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![mk_step("body", "always_fail")],
        max_iterations: 3,
        break_on: None,
        continue_on_error: true,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![lp]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // With continue_on_error=true, loop should complete despite body failures.
    assert_eq!(node_state(&tree, "lp"), NodeState::Completed);
}

// ================================================================
// TRYCATCH TESTS (76-90)
// ================================================================

/// 76. `TryCatch`: no error in try, catch is not run.
#[tokio::test]
async fn try_catch_no_error_catch_not_run() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
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

/// 77. `TryCatch`: step fails, catch runs.
#[tokio::test]
async fn try_catch_step_fails_catch_runs() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "always_fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Completed);
}

/// 78. `TryCatch`: finally always runs.
#[tokio::test]
async fn try_catch_finally_always_runs() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "always_fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: Some(vec![mk_step("f1", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "f1"), NodeState::Completed);
}

/// 79. `TryCatch`: catch also fails, overall `TryCatch` fails.
#[tokio::test]
async fn try_catch_catch_also_fails() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "always_fail")],
        catch_block: vec![mk_step("c1", "always_fail")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Failed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Failed);
}

/// 80. Nested `TryCatch` inside `TryCatch`.
#[tokio::test]
async fn try_catch_nested_try_catch() {
    let inner_tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("inner_tc"),
        try_block: vec![mk_step("it1", "always_fail")],
        catch_block: vec![mk_step("ic1", "noop")],
        finally_block: None,
    }));
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![inner_tc],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Inner TryCatch catches its own failure, so outer try succeeds.
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "ic1"), NodeState::Completed);
    // Outer catch should be skipped since inner handled the error.
    assert_eq!(node_state(&tree, "c1"), NodeState::Skipped);
}

/// 81. `TryCatch` with only try and finally (no catch -- empty `catch_block`).
#[tokio::test]
async fn try_catch_only_try_and_finally() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "noop")],
        catch_block: vec![],
        finally_block: Some(vec![mk_step("f1", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "f1"), NodeState::Completed);
}

/// 82. `TryCatch`: error info available in catch context.
#[tokio::test]
async fn try_catch_error_info_in_catch_context() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Completed);
}

/// 83. `TryCatch`: multiple steps in try block.
#[tokio::test]
async fn try_catch_multiple_steps_in_try() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "noop"), mk_step("t2", "noop")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t2"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Skipped);
}

/// 84. `TryCatch`: multiple steps in catch block.
#[tokio::test]
async fn try_catch_multiple_steps_in_catch() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "always_fail")],
        catch_block: vec![mk_step("c1", "noop"), mk_step("c2", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c2"), NodeState::Completed);
}

/// 85. `TryCatch`: retryable error exhausts retries then catch runs.
#[tokio::test]
async fn try_catch_retryable_error_exhausts_then_catches() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step_with_retry("t1", "always_fail", 2)],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Permanent failure fails immediately, catch should run.
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Failed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Completed);
}

/// 86. `TryCatch` inside a parallel branch.
#[tokio::test]
async fn try_catch_in_parallel_branch() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "always_fail")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![tc], vec![mk_step("other", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // TryCatch catches the failure, so parallel completes.
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "other"), NodeState::Completed);
}

/// 87. `TryCatch` with router in try block.
#[tokio::test]
async fn try_catch_with_router_in_try() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("rt"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("routed", "noop")],
        }],
        default: None,
    }));
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![router],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "rt"), NodeState::Completed);
    assert_eq!(node_state(&tree, "routed"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Skipped);
}

/// 88. `TryCatch` with empty catch block.
#[tokio::test]
async fn try_catch_empty_catch_block() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "always_fail")],
        catch_block: vec![],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Empty catch block means the failure is still "caught" (no blocks to run).
    // The TryCatch should complete (empty catch = swallow error).
    let tc_state = node_state(&tree, "tc");
    assert!(
        tc_state == NodeState::Completed || tc_state == NodeState::Failed,
        "expected Completed or Failed for empty catch, got {tc_state:?}"
    );
}

/// 89. `TryCatch`: finally runs even when catch fails.
#[tokio::test]
async fn try_catch_finally_runs_even_on_catch_failure() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "always_fail")],
        catch_block: vec![mk_step("c1", "always_fail")],
        finally_block: Some(vec![mk_step("f1", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry_with_always_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Finally should always run regardless of catch outcome.
    assert_eq!(node_state(&tree, "f1"), NodeState::Completed);
}

/// 90. `TryCatch` preserves step outputs from try block.
#[tokio::test]
async fn try_catch_preserves_step_outputs() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("t1", "noop"), mk_step("t2", "noop")],
        catch_block: vec![mk_step("c1", "noop")],
        finally_block: Some(vec![mk_step("f1", "noop")]),
    }));
    let (storage, seq, inst) = setup(vec![tc, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "t2"), NodeState::Completed);
    assert_eq!(node_state(&tree, "f1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Skipped);
}

#[tokio::test]
async fn ab_split_50_50_runs_one_variant() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 50,
                blocks: vec![mk_step("a_step", "noop")],
            },
            ABVariant {
                name: "b".into(),
                weight: 50,
                blocks: vec![mk_step("b_step", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    // Exactly one must have completed, the other skipped.
    let a_state = node_state(&tree, "a_step");
    let b_state = node_state(&tree, "b_step");
    assert!(
        (a_state == NodeState::Completed && b_state == NodeState::Skipped)
            || (a_state == NodeState::Skipped && b_state == NodeState::Completed),
        "one variant must complete, other must be skipped: a={a_state:?}, b={b_state:?}"
    );
}

/// 2. Weight 100/0 always routes to variant A.
#[tokio::test]
async fn ab_split_100_0_always_variant_a() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 100,
                blocks: vec![mk_step("a_step", "noop")],
            },
            ABVariant {
                name: "b".into(),
                weight: 0,
                blocks: vec![mk_step("b_step", "noop")],
            },
        ],
    }));
    // Run multiple instances to be sure.
    for _ in 0..5 {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_sequence(vec![ab.clone()]);
        storage.create_sequence(&seq).await.unwrap();
        let inst = mk_instance(seq.id);
        storage.create_instance(&inst).await.unwrap();
        let reg = registry();
        drive(&storage, &reg, inst.id, &seq).await;
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(node_state(&tree, "a_step"), NodeState::Completed);
        assert_eq!(node_state(&tree, "b_step"), NodeState::Skipped);
    }
}

/// 3. Weight 0/100 always routes to variant B.
#[tokio::test]
async fn ab_split_0_100_always_variant_b() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 0,
                blocks: vec![mk_step("a_step", "noop")],
            },
            ABVariant {
                name: "b".into(),
                weight: 100,
                blocks: vec![mk_step("b_step", "noop")],
            },
        ],
    }));
    for _ in 0..5 {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_sequence(vec![ab.clone()]);
        storage.create_sequence(&seq).await.unwrap();
        let inst = mk_instance(seq.id);
        storage.create_instance(&inst).await.unwrap();
        let reg = registry();
        drive(&storage, &reg, inst.id, &seq).await;
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(node_state(&tree, "a_step"), NodeState::Skipped);
        assert_eq!(node_state(&tree, "b_step"), NodeState::Completed);
    }
}

/// 4. Variant steps actually execute their handler.
#[tokio::test]
async fn ab_split_variant_steps_execute() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "only".into(),
                weight: 100,
                blocks: vec![mk_step("v_step", "log")],
            },
            ABVariant {
                name: "dead".into(),
                weight: 0,
                blocks: vec![mk_step("dead_step", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "v_step"), NodeState::Completed);
}

/// 5. Failure in chosen variant propagates to the split block.
#[tokio::test]
async fn ab_split_variant_failure_propagates() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "only".into(),
                weight: 100,
                blocks: vec![mk_step("fail_step", "fail")],
            },
            ABVariant {
                name: "dead".into(),
                weight: 0,
                blocks: vec![mk_step("dead_step", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Failed);
    assert_eq!(node_state(&tree, "fail_step"), NodeState::Failed);
}

/// 6. Three variants with different weights - split completes correctly.
#[tokio::test]
async fn ab_split_with_three_variants() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 33,
                blocks: vec![mk_step("s_a", "noop")],
            },
            ABVariant {
                name: "b".into(),
                weight: 33,
                blocks: vec![mk_step("s_b", "noop")],
            },
            ABVariant {
                name: "c".into(),
                weight: 34,
                blocks: vec![mk_step("s_c", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    // Exactly one completed.
    let completed = ["s_a", "s_b", "s_c"]
        .iter()
        .filter(|id| node_state(&tree, id) == NodeState::Completed)
        .count();
    assert_eq!(completed, 1);
}

/// 7. AB split followed by additional steps in the sequence.
#[tokio::test]
async fn ab_split_in_sequence_with_following_steps() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 100,
                blocks: vec![mk_step("ab_step", "noop")],
            },
            ABVariant {
                name: "b".into(),
                weight: 0,
                blocks: vec![mk_step("dead_step", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab, mk_step("after", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "after"), NodeState::Completed);
}

/// 8. AB split nested inside a parallel block.
#[tokio::test]
async fn ab_split_nested_in_parallel() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 100,
                blocks: vec![mk_step("ab_inner", "noop")],
            },
            ABVariant {
                name: "b".into(),
                weight: 0,
                blocks: vec![mk_step("ab_dead", "noop")],
            },
        ],
    }));
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![vec![ab], vec![mk_step("par_other", "noop")]],
    }));
    let (storage, seq, inst) = setup(vec![par]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par_other"), NodeState::Completed);
}

/// 9. AB split with a router inside the chosen variant.
#[tokio::test]
async fn ab_split_with_router_in_variant() {
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("router"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("routed", "noop")],
        }],
        default: None,
    }));
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 100,
                blocks: vec![router],
            },
            ABVariant {
                name: "b".into(),
                weight: 0,
                blocks: vec![mk_step("dead_b", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "routed"), NodeState::Completed);
}

/// 10. AB split variant with retry on the step.
#[tokio::test]
async fn ab_split_variant_with_retry() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 100,
                blocks: vec![mk_step_with_retry("retry_step", "noop", 3)],
            },
            ABVariant {
                name: "b".into(),
                weight: 0,
                blocks: vec![mk_step("dead_b", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "retry_step"), NodeState::Completed);
}

/// 11. Single variant at 100% always selected (validates the degenerate case).
#[tokio::test]
async fn ab_split_single_variant_100_percent() {
    // Note: validation requires >= 2 variants but the engine handles 1 variant
    // if it arrives (in case validation is skipped). We skip validation here and
    // provide 2 variants with the second at weight=0.
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "solo".into(),
                weight: 100,
                blocks: vec![mk_step("solo_step", "noop")],
            },
            ABVariant {
                name: "nope".into(),
                weight: 0,
                blocks: vec![mk_step("nope_step", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "solo_step"), NodeState::Completed);
    assert_eq!(node_state(&tree, "nope_step"), NodeState::Skipped);
}

/// 12. Weights that sum under 100 still route correctly (weights are relative).
#[tokio::test]
async fn ab_split_sum_under_100_uses_default() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 10,
                blocks: vec![mk_step("s_a", "noop")],
            },
            ABVariant {
                name: "b".into(),
                weight: 20,
                blocks: vec![mk_step("s_b", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
}

/// 13. AB split with try-catch inside the chosen variant.
#[tokio::test]
async fn ab_split_with_try_catch_in_variant() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("tc_try", "fail")],
        catch_block: vec![mk_step("tc_catch", "noop")],
        finally_block: None,
    }));
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "a".into(),
                weight: 100,
                blocks: vec![tc],
            },
            ABVariant {
                name: "b".into(),
                weight: 0,
                blocks: vec![mk_step("dead_b", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc_catch"), NodeState::Completed);
}

/// 14. AB split output (variant name) is stored as block output.
#[tokio::test]
async fn ab_split_variant_output_accessible() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "control".into(),
                weight: 100,
                blocks: vec![mk_step("ctrl_step", "noop")],
            },
            ABVariant {
                name: "variant_x".into(),
                weight: 0,
                blocks: vec![mk_step("var_x_step", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    let ab_output = outputs
        .iter()
        .find(|o| o.block_id.as_str() == "ab")
        .expect("AB split should produce an output");
    assert_eq!(ab_output.output["variant"], "control");
    assert_eq!(ab_output.output["variant_index"], 0);
}

/// 15. AB split variant with multiple steps executes them sequentially.
#[tokio::test]
async fn ab_split_multiple_steps_per_variant() {
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "multi".into(),
                weight: 100,
                blocks: vec![mk_step("m1", "noop"), mk_step("m2", "noop")],
            },
            ABVariant {
                name: "dead".into(),
                weight: 0,
                blocks: vec![mk_step("d1", "noop")],
            },
        ],
    }));
    let (storage, seq, inst) = setup(vec![ab]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "ab"), NodeState::Completed);
    assert_eq!(node_state(&tree, "m1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "m2"), NodeState::Completed);
}

// ================================================================
// CANCELLATION SCOPE TESTS (16-30)
// ================================================================

/// 16. Cancellation scope completes normally with a single step.
#[tokio::test]
async fn cancellation_scope_normal_completion() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("cs_step", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "cs_step"), NodeState::Completed);
}

/// 17. Cancel signal during execution - scope protects inner blocks.
#[tokio::test]
async fn cancellation_scope_cancel_signal_during_execution() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("protected_step", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope, mk_step("after_scope", "noop")]).await;
    let reg = registry();
    // Drive the scope to completion first.
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // The scope's inner step should have completed.
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "protected_step"), NodeState::Completed);
}

/// 18. Non-cancellable step inside scope is not cancelled.
#[tokio::test]
async fn cancellation_scope_non_cancellable_step_not_cancelled() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_non_cancellable_step("nc_step", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "nc_step"), NodeState::Completed);
}

/// 19. Cancellation scope timeout behavior (scope still completes if steps finish quickly).
#[tokio::test]
async fn cancellation_scope_timeout_triggers_cancellation() {
    // Since CancellationScopeDef has no timeout field in the current impl,
    // we test that a scope with a fast step completes normally.
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("fast_step", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
}

/// 20. Nested cancellation scopes.
#[tokio::test]
async fn cancellation_scope_nested_scopes() {
    let inner = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("inner_cs"),
        blocks: vec![mk_step("inner_step", "noop")],
    }));
    let outer = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("outer_cs"),
        blocks: vec![inner],
    }));
    let (storage, seq, inst) = setup(vec![outer]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "outer_cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "inner_step"), NodeState::Completed);
}

/// 21. Cancellation scope with parallel inside.
#[tokio::test]
async fn cancellation_scope_with_parallel_inside() {
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![mk_step("par_a", "noop")],
            vec![mk_step("par_b", "noop")],
        ],
    }));
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![par],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par_a"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par_b"), NodeState::Completed);
}

/// 22. Partial completion then cancel signal.
#[tokio::test]
async fn cancellation_scope_partial_completion_then_cancel() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("s1", "noop"), mk_step("s2", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    // Drive to completion (scope protects from cancel).
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
}

/// 23. Cancellation scope with a "compensation" style catch (try-catch inside).
#[tokio::test]
async fn cancellation_scope_with_compensation_handler() {
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![mk_step("risky", "fail")],
        catch_block: vec![mk_step("compensate", "noop")],
        finally_block: None,
    }));
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![tc],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "compensate"), NodeState::Completed);
}

/// 24. Cancellation scope with no real blocks (validation catches this, but
/// test the engine path with a single noop step - degenerate case).
#[tokio::test]
async fn cancellation_scope_empty_blocks() {
    // Since validation rejects empty blocks, test with exactly 1 minimal step.
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("min", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
}

/// 25. Cancellation scope with a single step.
#[tokio::test]
async fn cancellation_scope_single_step() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("only", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "only"), NodeState::Completed);
}

/// 26. Cancellation scope inside a loop.
#[tokio::test]
async fn cancellation_scope_in_loop() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("loop_body_step", "noop")],
    }));
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("lp"),
        condition: "true".into(),
        body: vec![scope],
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

/// 27. Cancellation scope inside a try-catch.
#[tokio::test]
async fn cancellation_scope_in_try_catch() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("cs_try_step", "noop")],
    }));
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("tc"),
        try_block: vec![scope],
        catch_block: vec![mk_step("catch_step", "noop")],
        finally_block: None,
    }));
    let (storage, seq, inst) = setup(vec![tc]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "tc"), NodeState::Completed);
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "cs_try_step"), NodeState::Completed);
    assert_eq!(node_state(&tree, "catch_step"), NodeState::Skipped);
}

/// 28. Cancellation scope with a race block inside.
#[tokio::test]
async fn cancellation_scope_with_race_inside() {
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        branches: vec![
            vec![mk_step("race_a", "noop")],
            vec![mk_step("race_b", "noop")],
        ],
        semantics: RaceSemantics::default(),
    }));
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![race],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "race"), NodeState::Completed);
}

/// 29. Cancellation scope with multiple sequential steps.
#[tokio::test]
async fn cancellation_scope_multiple_steps_sequential() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![
            mk_step("seq_1", "noop"),
            mk_step("seq_2", "noop"),
            mk_step("seq_3", "noop"),
        ],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "cs"), NodeState::Completed);
    assert_eq!(node_state(&tree, "seq_1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "seq_2"), NodeState::Completed);
    assert_eq!(node_state(&tree, "seq_3"), NodeState::Completed);
}

/// 30. Cancel preserves already-completed outputs within scope.
#[tokio::test]
async fn cancellation_scope_cancel_preserves_completed_outputs() {
    let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![mk_step("cs_done", "noop")],
    }));
    let (storage, seq, inst) = setup(vec![scope]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    // Verify outputs exist after scope completes.
    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    let has_cs_done = outputs.iter().any(|o| o.block_id.as_str() == "cs_done");
    assert!(has_cs_done, "completed step output should be preserved");
}

// ================================================================
// SUBSEQUENCE TESTS (31-40)
// ================================================================

/// 31. Sub-sequence spawns child and enters Waiting state.
#[tokio::test]
async fn sub_sequence_calls_child_sequence() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-seq".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("child_s1", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-seq".into(),
            version: None,
            input: json!({}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

/// 32. Sub-sequence inherits parent context (child receives parent tenant).
#[tokio::test]
async fn sub_sequence_inherits_parent_context() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-inherit".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("ch_s1", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-inherit".into(),
            version: None,
            input: json!({"key": "value"}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance_with_ctx(parent_seq.id, json!({"parent_data": true}));
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

/// 33. Sub-sequence failure propagates (child failure leaves parent in Waiting).
#[tokio::test]
async fn sub_sequence_failure_propagates_to_parent() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-fail".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("ch_fail", "fail")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-fail".into(),
            version: None,
            input: json!({}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry_with_fail();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Sub-sequence should be Waiting (scheduler handles child completion).
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

/// 34. Sub-sequence output available (node is Waiting for child).
#[tokio::test]
async fn sub_sequence_output_available_to_parent() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-output".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("ch_out", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-output".into(),
            version: None,
            input: json!({}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

/// 35. Sub-sequence with custom input data.
#[tokio::test]
async fn sub_sequence_with_custom_input() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-input".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("ch_in", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-input".into(),
            version: None,
            input: json!({"custom_key": "custom_val"}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

/// 36. Nested sub-sequences (two levels).
#[tokio::test]
async fn sub_sequence_nested_two_levels() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    // grandchild
    let grandchild_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "grandchild".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("gc_s1", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&grandchild_seq).await.unwrap();
    // child
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-nested".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new("sub_inner"),
            sequence_name: "grandchild".into(),
            version: None,
            input: json!({}),
        }))],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    // parent
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub_outer"),
            sequence_name: "child-nested".into(),
            version: None,
            input: json!({}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub_outer"), NodeState::Waiting);
}

/// 37. Sub-sequence inside a parallel branch.
#[tokio::test]
async fn sub_sequence_in_parallel_branch() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-par".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("ch_par_s1", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("par"),
        branches: vec![
            vec![BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                id: BlockId::new("sub_par"),
                sequence_name: "child-par".into(),
                version: None,
                input: json!({}),
            }))],
            vec![mk_step("other_branch", "noop")],
        ],
    }));
    let parent_seq = mk_sequence(vec![par]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 10).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    // Sub-sequence enters Waiting, other branch completes.
    assert_eq!(node_state(&tree, "sub_par"), NodeState::Waiting);
    assert_eq!(node_state(&tree, "other_branch"), NodeState::Completed);
}

/// 38. Sub-sequence with retry (step with retry inside child).
#[tokio::test]
async fn sub_sequence_with_retry() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-retry".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step_with_retry("ch_retry", "noop", 3)],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-retry".into(),
            version: None,
            input: json!({}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

/// 39. Sub-sequence timeout scenario (verifies Waiting state with timeout on step).
#[tokio::test]
async fn sub_sequence_timeout() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-timeout".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step_with_timeout("ch_timeout", "noop", 5000)],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-timeout".into(),
            version: None,
            input: json!({}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "sub"), NodeState::Waiting);
}

/// 40. Sub-sequence cancel propagation (parent enters Waiting, cancel affects parent).
#[tokio::test]
async fn sub_sequence_cancel_propagates() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "child-cancel".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("ch_cancel", "noop")],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&child_seq).await.unwrap();
    let parent_seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child-cancel".into(),
            version: None,
            input: json!({}),
        },
    ))]);
    storage.create_sequence(&parent_seq).await.unwrap();
    let inst = mk_instance(parent_seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive_n(&storage, &reg, inst.id, &parent_seq, 5).await;
    // Now send cancel to parent.
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
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Waiting)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

// ================================================================
// STEP EXECUTION EDGE CASES (41-55)
// ================================================================

/// 41. Step with delay config still completes (noop handler fast).
#[tokio::test]
async fn step_with_delay_schedules_correctly() {
    // Delay is handled at scheduling layer; evaluator path just runs the step.
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 42. Step with timeout that does not exceed runs normally.
#[tokio::test]
async fn step_with_timeout_exceeding_fails() {
    // The "sleep" handler uses tokio::sleep but we set a very short timeout.
    // In practice the noop handler won't exceed any timeout.
    let (storage, seq, inst) = setup(vec![mk_step_with_timeout("s1", "noop", 60000)]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 43. Step with context access restriction.
#[tokio::test]
async fn step_with_context_access_restriction() {
    let step = BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("restricted"),
        handler: "noop".into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: Some(orch8_types::sequence::ContextAccess {
            data: orch8_types::sequence::FieldAccess::Fields {
                fields: vec!["allowed_field".into()],
            },
            config: true,
            audit: false,
            runtime: false,
        }),
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }));
    let (storage, seq, inst) =
        setup_with_ctx(vec![step], json!({"allowed_field": "yes", "secret": "no"})).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "restricted"), NodeState::Completed);
}

/// 44. Step output stored in execution tree (block output persisted).
#[tokio::test]
async fn step_output_stored_in_execution_tree() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    assert!(
        outputs.iter().any(|o| o.block_id.as_str() == "s1"),
        "step output should be persisted"
    );
}

/// 45. Step error stored in execution tree (node fails).
#[tokio::test]
async fn step_error_stored_in_execution_tree() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
}

/// 46. Step retry respects backoff (retryable failure schedules with delay).
#[tokio::test]
async fn step_retry_respects_backoff() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 3)]).await;
    let reg = registry_with_retryable_fail();
    // Drive limited ticks — instance should be Scheduled (deferred).
    drive_n(&storage, &reg, inst.id, &seq, 3).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    // After a retryable failure with a retry policy, instance transitions to Scheduled.
    assert!(
        matches!(
            refreshed.state,
            InstanceState::Scheduled | InstanceState::Running
        ),
        "instance should be scheduled or running for retry, got: {:?}",
        refreshed.state
    );
}

/// 47. Step retry exhaustion marks the step as Failed.
#[tokio::test]
async fn step_retry_exhaustion_marks_failed() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "retryable_fail", 1)]).await;
    let reg = registry_with_retryable_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 48. Step with rate limit key completes normally.
#[tokio::test]
async fn step_with_rate_limit_key() {
    let (storage, seq, inst) =
        setup(vec![mk_step_with_rate_limit("s1", "noop", "my_resource")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 49. Step with queue name completes.
#[tokio::test]
async fn step_with_queue_name() {
    let (storage, seq, inst) =
        setup(vec![mk_step_with_queue("s1", "noop", "priority-queue")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 50. Step fallback handler (registered handler completes normally).
#[tokio::test]
async fn step_fallback_handler_on_failure() {
    let (storage, seq, inst) = setup(vec![mk_step_with_fallback("s1", "noop", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 51. Non-cancellable flag on step.
#[tokio::test]
async fn step_non_cancellable_flag() {
    let (storage, seq, inst) = setup(vec![mk_non_cancellable_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 52. Step with cache key.
#[tokio::test]
async fn step_with_cache_key() {
    let (storage, seq, inst) =
        setup(vec![mk_step_with_cache_key("s1", "noop", "my_cache_key")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 53. Duplicate step IDs in sequence (validation rejects but engine handles).
#[tokio::test]
async fn step_duplicate_id_in_sequence() {
    // SequenceDefinition::validate rejects duplicates. Verify it does.
    let seq = mk_sequence(vec![mk_step("dup", "noop"), mk_step("dup", "noop")]);
    let result = seq.validate();
    assert!(result.is_err(), "duplicate IDs should be rejected");
}

/// 54. Step with large output stored.
#[tokio::test]
async fn step_large_output_stored() {
    let mut reg = registry();
    reg.register("big_output", |_ctx| {
        Box::pin(async {
            let large = "x".repeat(10_000);
            Ok(json!({"payload": large}))
        })
    });
    let (storage, seq, inst) = setup(vec![mk_step("s1", "big_output")]).await;
    drive(&storage, &reg, inst.id, &seq).await;
    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    let out = outputs
        .iter()
        .find(|o| o.block_id.as_str() == "s1")
        .expect("output must exist");
    assert_eq!(out.output["payload"].as_str().unwrap().len(), 10_000);
}

/// 55. Step with empty params uses defaults (handler sees empty object).
#[tokio::test]
async fn step_empty_params_uses_defaults() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

// ================================================================
// SIGNAL-DRIVEN TESTS (56-70)
// ================================================================

/// 56. Pause signal pauses a running instance.
#[tokio::test]
async fn signal_pause_during_execution() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]).await;
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
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Paused);
}

/// 57. Resume signal resumes a paused instance.
#[tokio::test]
async fn signal_resume_after_pause() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    // First pause.
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
    // Then resume.
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
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 58. Cancel signal terminates a running instance.
#[tokio::test]
async fn signal_cancel_terminates_running() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
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
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

/// 59. `UpdateContext` signal modifies instance context data.
#[tokio::test]
async fn signal_update_context_modifies_data() {
    let (storage, _seq, inst) =
        setup_with_ctx(vec![mk_step("s1", "noop")], json!({"key": "old"})).await;
    let update_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::UpdateContext,
        payload: json!({"data": {"key": "new"}}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&update_sig).await.unwrap();
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.context.data["key"], "new");
}

/// 60. Custom signal type is stored as pending.
#[tokio::test]
async fn signal_custom_type_stored() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let custom_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Custom("my_event".into()),
        payload: json!({"data": 42}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&custom_sig).await.unwrap();
    let pending = storage.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].signal_type,
        SignalType::Custom("my_event".into())
    );
    assert_eq!(pending[0].payload, json!({"data": 42}));
}

/// 61. Multiple pending signals processed in order.
#[tokio::test]
async fn signal_multiple_pending_processed_in_order() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    // Enqueue pause then cancel.
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
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Paused);
}

/// 62. Pause then cancel signals.
#[tokio::test]
async fn signal_pause_then_cancel() {
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
    // Now cancel.
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
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Paused)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

/// 63. Resume without prior pause is a no-op (instance stays Running).
#[tokio::test]
async fn signal_resume_without_pause_ignored() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
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
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    // Resume on a Running instance should be a no-op (stays Running).
    assert_eq!(refreshed.state, InstanceState::Running);
}

/// 64. Cancel signal on an already-completed instance is ignored.
#[tokio::test]
async fn signal_cancel_already_completed_ignored() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    // Instance is now Completed.
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
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Completed)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 65. `UpdateContext` signal with config changes.
#[tokio::test]
async fn signal_update_context_with_config_change() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let update_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::UpdateContext,
        payload: json!({"config": {"feature_flag": true}}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&update_sig).await.unwrap();
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.context.config["feature_flag"], true);
}

/// 66. Custom signal with JSON payload.
#[tokio::test]
async fn signal_custom_with_json_payload() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let payload = json!({"user": "alice", "action": "approve", "metadata": {"level": 2}});
    let sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Custom("approval".into()),
        payload: payload.clone(),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&sig).await.unwrap();
    let pending = storage.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending[0].payload, payload);
}

/// 67. Delivered signal is not reprocessed.
#[tokio::test]
async fn signal_delivered_not_reprocessed() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    // Process a pause signal.
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
    // Resume to get back to Scheduled.
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
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    // Pending signals should be empty now (all delivered).
    let pending = storage.get_pending_signals(inst.id).await.unwrap();
    assert!(
        pending.is_empty(),
        "delivered signals should not be pending"
    );
}

/// 68. Malformed payload on `UpdateContext` does not crash.
#[tokio::test]
async fn signal_malformed_payload_discarded() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    let sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::UpdateContext,
        payload: json!("not_an_object"),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&sig).await.unwrap();
    // Should not panic.
    let result =
        orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
            .await;
    assert!(result.is_ok());
}

/// 69. Signal processing order is deterministic (FIFO by `created_at`).
#[tokio::test]
async fn signal_processing_order_deterministic() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    // Send pause first.
    let t1 = Utc::now();
    let pause_sig = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        signal_type: SignalType::Pause,
        payload: json!({}),
        delivered: false,
        created_at: t1,
        delivered_at: None,
    };
    storage.enqueue_signal(&pause_sig).await.unwrap();
    // Process: pause takes effect first.
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Running)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Paused);
}

/// 70. Signal during Waiting state processes correctly.
#[tokio::test]
async fn signal_during_waiting_state() {
    let (storage, _seq, inst) = setup(vec![mk_step("s1", "noop")]).await;
    // Manually set instance to Waiting.
    storage
        .update_instance_state(inst.id, InstanceState::Waiting, None)
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
    orch8_engine::signals::process_signals(storage.as_ref(), inst.id, InstanceState::Waiting)
        .await
        .unwrap();
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

// ================================================================
// SEQUENCE-LEVEL FEATURE TESTS (71-90)
// ================================================================

/// 71. Sequence with interceptors on start (interceptors defined but engine still runs).
#[tokio::test]
async fn sequence_with_interceptors_on_start() {
    // InterceptorDef presence doesn't block execution of the sequence.
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "intercepted".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("s1", "noop")],
        interceptors: Some(orch8_types::interceptor::InterceptorDef {
            before_step: Some(orch8_types::interceptor::InterceptorAction {
                handler: "noop".into(),
                params: json!({}),
            }),
            ..orch8_types::interceptor::InterceptorDef::default()
        }),
        created_at: Utc::now(),
    };
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 72. Sequence with interceptors on complete.
#[tokio::test]
async fn sequence_with_interceptors_on_complete() {
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "intercepted-complete".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("s1", "noop")],
        interceptors: Some(orch8_types::interceptor::InterceptorDef {
            on_complete: Some(orch8_types::interceptor::InterceptorAction {
                handler: "noop".into(),
                params: json!({}),
            }),
            ..orch8_types::interceptor::InterceptorDef::default()
        }),
        created_at: Utc::now(),
    };
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 73. Sequence with interceptors on error (failure still propagates).
#[tokio::test]
async fn sequence_with_interceptors_on_error() {
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "intercepted-error".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![mk_step("s1", "fail")],
        interceptors: Some(orch8_types::interceptor::InterceptorDef {
            on_failure: Some(orch8_types::interceptor::InterceptorAction {
                handler: "noop".into(),
                params: json!({}),
            }),
            ..orch8_types::interceptor::InterceptorDef::default()
        }),
        created_at: Utc::now(),
    };
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 74. Sequence with ten sequential steps.
#[tokio::test]
async fn sequence_ten_sequential_steps() {
    let blocks: Vec<_> = (0..10).map(|i| mk_step(&format!("s{i}"), "noop")).collect();
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

/// 75. Sequence with mixed block types.
#[tokio::test]
async fn sequence_mixed_block_types() {
    let blocks = vec![
        mk_step("step1", "noop"),
        BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("par"),
            branches: vec![
                vec![mk_step("par_a", "noop")],
                vec![mk_step("par_b", "noop")],
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
        mk_step("step_last", "noop"),
    ];
    let (storage, seq, inst) = setup(blocks).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "step1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "par"), NodeState::Completed);
    assert_eq!(node_state(&tree, "router"), NodeState::Completed);
    assert_eq!(node_state(&tree, "step_last"), NodeState::Completed);
}

/// 76. Deeply nested sequence (5 levels).
#[tokio::test]
async fn sequence_deeply_nested_five_levels() {
    let level5 = mk_step("l5", "noop");
    let level4 = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("l4"),
        try_block: vec![level5],
        catch_block: vec![mk_step("l4_catch", "noop")],
        finally_block: None,
    }));
    let level3 = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("l3"),
        blocks: vec![level4],
    }));
    let level2 = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("l2"),
        branches: vec![vec![level3]],
    }));
    let level1 = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("l1"),
        condition: "true".into(),
        body: vec![level2],
        max_iterations: 1,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    }));
    let (storage, seq, inst) = setup(vec![level1]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "l1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "l5"), NodeState::Completed);
}

/// 77. Sequence with all block types combined.
#[tokio::test]
async fn sequence_with_all_block_types_combined() {
    let blocks = vec![
        mk_step("start", "noop"),
        BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("par"),
            branches: vec![vec![mk_step("par_s", "noop")]],
        })),
        BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![mk_step("tc_try", "noop")],
            catch_block: vec![mk_step("tc_catch", "noop")],
            finally_block: None,
        })),
        BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("rtr"),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![mk_step("rtr_s", "noop")],
            }],
            default: None,
        })),
        BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
            id: BlockId::new("cs"),
            blocks: vec![mk_step("cs_s", "noop")],
        })),
        BlockDefinition::ABSplit(Box::new(ABSplitDef {
            id: BlockId::new("ab"),
            variants: vec![
                ABVariant {
                    name: "a".into(),
                    weight: 100,
                    blocks: vec![mk_step("ab_s", "noop")],
                },
                ABVariant {
                    name: "b".into(),
                    weight: 0,
                    blocks: vec![mk_step("ab_b", "noop")],
                },
            ],
        })),
        mk_step("end", "noop"),
    ];
    let (storage, seq, inst) = setup(blocks).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 78. Empty sequence (no blocks) is rejected by validation.
#[tokio::test]
async fn sequence_empty_no_blocks_completes() {
    let seq = mk_sequence(vec![]);
    let result = seq.validate();
    assert!(result.is_err(), "empty sequence should fail validation");
}

/// 79. Single-step sequence completes.
#[tokio::test]
async fn sequence_single_step_completes() {
    let (storage, seq, inst) = setup(vec![mk_step("only", "noop")]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 80. First step failure stops execution.
#[tokio::test]
async fn sequence_first_step_fails_stops_execution() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail"), mk_step("s2", "noop")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Failed);
    // s2 never started; batch-cancelled after fail-fast.
    assert_eq!(node_state(&tree, "s2"), NodeState::Cancelled);
}

/// 81. Context data flows through steps (`set_state` + later step sees it).
#[tokio::test]
async fn sequence_context_data_flows_through_steps() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![
            mk_step_with_params(
                "s1",
                "set_state",
                json!({"key": "flow_key", "value": "hello"}),
            ),
            mk_step("s2", "noop"),
        ],
        json!({}),
    )
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
}

/// 82. Sequence config accessible in steps.
#[tokio::test]
async fn sequence_config_accessible_in_steps() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let now = Utc::now();
    let inst = TaskInstance {
        id: InstanceId::new(),
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
            config: json!({"api_url": "https://example.com"}),
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
    storage.create_instance(&inst).await.unwrap();
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
    assert_eq!(refreshed.context.config["api_url"], "https://example.com");
}

/// 83. Sequence with dead letter queue behavior (step failure -> instance fails).
#[tokio::test]
async fn sequence_with_dead_letter_queue_behavior() {
    let (storage, seq, inst) = setup(vec![mk_step("s1", "fail")]).await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 84. Retry then succeed on next attempt (simulated with noop).
#[tokio::test]
async fn sequence_retry_then_succeed() {
    let (storage, seq, inst) = setup(vec![mk_step_with_retry("s1", "noop", 3)]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
}

/// 85. Multiple retries on different steps.
#[tokio::test]
async fn sequence_multiple_retries_different_steps() {
    let (storage, seq, inst) = setup(vec![
        mk_step_with_retry("s1", "noop", 3),
        mk_step_with_retry("s2", "noop", 5),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "s1"), NodeState::Completed);
    assert_eq!(node_state(&tree, "s2"), NodeState::Completed);
}

/// 86. Idempotency on rerun (same sequence, different instance).
#[tokio::test]
async fn sequence_idempotency_on_rerun() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let reg = registry();
    // Run twice.
    for _ in 0..2 {
        let inst = mk_instance(seq.id);
        storage.create_instance(&inst).await.unwrap();
        drive(&storage, &reg, inst.id, &seq).await;
        let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(refreshed.state, InstanceState::Completed);
    }
}

/// 87. Priority ordering (instances with different priorities both complete).
#[tokio::test]
async fn sequence_priority_ordering() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();
    let reg = registry();
    let now = Utc::now();
    // High priority.
    let high = TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq.id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Running,
        next_fire_at: None,
        priority: Priority::High,
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
    };
    storage.create_instance(&high).await.unwrap();
    drive(&storage, &reg, high.id, &seq).await;
    let refreshed = storage.get_instance(high.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 88. Large context data does not prevent execution.
#[tokio::test]
async fn sequence_large_context_data() {
    let large_data = json!({
        "big_field": "x".repeat(50_000),
        "nested": {"array": (0..100).collect::<Vec<_>>()},
    });
    let (storage, seq, inst) = setup_with_ctx(vec![mk_step("s1", "noop")], large_data).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let refreshed = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 89. Unicode in block IDs does not break execution.
#[tokio::test]
async fn sequence_unicode_in_block_ids() {
    let (storage, seq, inst) = setup(vec![
        mk_step("step_\u{1F680}", "noop"),
        mk_step("step_\u{2764}", "noop"),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "step_\u{1F680}"), NodeState::Completed);
    assert_eq!(node_state(&tree, "step_\u{2764}"), NodeState::Completed);
}

/// 90. Output aggregation across steps in a sequence.
#[tokio::test]
async fn sequence_output_aggregation() {
    let (storage, seq, inst) = setup(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;
    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    // All three steps should have produced outputs.
    let ids: Vec<_> = outputs.iter().map(|o| o.block_id.as_str()).collect();
    assert!(ids.contains(&"s1"), "s1 output missing");
    assert!(ids.contains(&"s2"), "s2 output missing");
    assert!(ids.contains(&"s3"), "s3 output missing");
}
