#![allow(clippy::too_many_lines)]
//! Router handler coverage (#164-171 from `TEST_PLAN.md`).
//!
//! Drives `execute_router` against seeded storage so the branch-selection,
//! sibling-skip, default-branch, and marker-inflation invariants are pinned
//! at the handler boundary. The inline tests in `router.rs` cover the
//! `select_branch` / `is_marker_present` helpers in isolation; the tests
//! here exercise the handler's storage-mutation side effects end-to-end so
//! regressions in sibling-skip or completion propagation surface without
//! running the whole scheduler.

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator;
use orch8_engine::handlers::param_resolve::OutputsSnapshot;
use orch8_engine::handlers::router::execute_router;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, Route, RouterDef, SequenceDefinition, StepDef};

fn mk_step(id: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: "builtin.noop".into(),
        params: serde_json::Value::Null,
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

async fn setup(
    router: RouterDef,
    ctx: serde_json::Value,
) -> (SqliteStorage, TaskInstance, Vec<ExecutionNode>) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let block = BlockDefinition::Router(Box::new(router));

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "router-cov".into(),
        version: 1,
        deprecated: false,
        blocks: vec![block.clone()],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();

    let now = Utc::now();
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq.id,
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
    };
    storage.create_instance(&instance).await.unwrap();

    let tree = evaluator::ensure_execution_tree(&storage, &instance, &[block])
        .await
        .unwrap();
    (storage, instance, tree)
}

fn router_node(tree: &[ExecutionNode]) -> ExecutionNode {
    tree.iter()
        .find(|n| matches!(n.block_type, BlockType::Router))
        .expect("router node")
        .clone()
}

fn find_by_block<'a>(tree: &'a [ExecutionNode], id: &str) -> &'a ExecutionNode {
    tree.iter().find(|n| n.block_id.0 == id).expect("node")
}

async fn refresh(storage: &SqliteStorage, instance: &TaskInstance) -> Vec<ExecutionNode> {
    storage.get_execution_tree(instance.id).await.unwrap()
}

// #164 — router picks first matching route (activates that branch's children).
#[tokio::test]
async fn first_matching_route_activates_its_branch() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "status == \"active\"".into(),
                blocks: vec![mk_step("branch0")],
            },
            Route {
                condition: "status == \"pending\"".into(),
                blocks: vec![mk_step("branch1")],
            },
        ],
        default: Some(vec![mk_step("defaultc")]),
    };
    let (storage, instance, tree) = setup(router.clone(), json!({"status": "active"})).await;
    let reg = HandlerRegistry::new();

    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Branch 0 child Running; branches 1 and default Skipped.
    assert_eq!(find_by_block(&tree, "branch0").state, NodeState::Running);
    assert_eq!(find_by_block(&tree, "branch1").state, NodeState::Skipped);
    assert_eq!(find_by_block(&tree, "defaultc").state, NodeState::Skipped);
}

// #165 — non-matching branches are marked Skipped.
#[tokio::test]
async fn non_matching_siblings_get_skipped() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "false".into(),
                blocks: vec![mk_step("b0")],
            },
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("b1")],
            },
            Route {
                condition: "false".into(),
                blocks: vec![mk_step("b2")],
            },
        ],
        default: None,
    };
    let (storage, instance, tree) = setup(router.clone(), json!({})).await;
    let reg = HandlerRegistry::new();

    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;

    assert_eq!(find_by_block(&tree, "b0").state, NodeState::Skipped);
    assert_eq!(find_by_block(&tree, "b1").state, NodeState::Running);
    assert_eq!(find_by_block(&tree, "b2").state, NodeState::Skipped);
}

// #166 — when no route matches, the default branch activates.
#[tokio::test]
async fn default_activates_when_nothing_matches() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "status == \"x\"".into(),
                blocks: vec![mk_step("b0")],
            },
            Route {
                condition: "status == \"y\"".into(),
                blocks: vec![mk_step("b1")],
            },
        ],
        default: Some(vec![mk_step("defc")]),
    };
    let (storage, instance, tree) = setup(router.clone(), json!({"status": "other"})).await;
    let reg = HandlerRegistry::new();

    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;

    assert_eq!(find_by_block(&tree, "b0").state, NodeState::Skipped);
    assert_eq!(find_by_block(&tree, "b1").state, NodeState::Skipped);
    assert_eq!(find_by_block(&tree, "defc").state, NodeState::Running);
}

// #167 — with no default and no matching route, the router completes immediately
// because the selected branch (routes.len()) has no children. Every real branch
// is Skipped. This pins the "empty selected branch" completion path.
#[tokio::test]
async fn no_match_no_default_completes_router() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "false".into(),
                blocks: vec![mk_step("b0")],
            },
            Route {
                condition: "false".into(),
                blocks: vec![mk_step("b1")],
            },
        ],
        default: None,
    };
    let (storage, instance, tree) = setup(router.clone(), json!({})).await;
    let reg = HandlerRegistry::new();

    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;

    assert_eq!(find_by_block(&tree, "b0").state, NodeState::Skipped);
    assert_eq!(find_by_block(&tree, "b1").state, NodeState::Skipped);
    assert_eq!(router_node(&tree).state, NodeState::Completed);
}

// #168 — conditions are checked against instance.context.data. Changing the
// context before the call flips which branch activates.
#[tokio::test]
async fn conditions_check_current_context() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "n > 5".into(),
                blocks: vec![mk_step("big")],
            },
            Route {
                condition: "n <= 5".into(),
                blocks: vec![mk_step("small")],
            },
        ],
        default: None,
    };

    // n=10 → big.
    let (storage, instance, tree) = setup(router.clone(), json!({"n": 10})).await;
    let reg = HandlerRegistry::new();
    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "big").state, NodeState::Running);
    assert_eq!(find_by_block(&tree, "small").state, NodeState::Skipped);

    // Separate setup with n=2 → small. Fresh storage ensures no cross-talk.
    let (storage2, instance2, tree2) = setup(router.clone(), json!({"n": 2})).await;
    execute_router(
        &storage2,
        &reg,
        &instance2,
        &router_node(&tree2),
        &router,
        &tree2,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree2 = refresh(&storage2, &instance2).await;
    assert_eq!(find_by_block(&tree2, "big").state, NodeState::Skipped);
    assert_eq!(find_by_block(&tree2, "small").state, NodeState::Running);
}

// #169 — single-route router: either the route matches, or the default
// activates. This test exercises the match-and-only-one-branch path.
#[tokio::test]
async fn single_route_activates_when_matched() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![Route {
            condition: "ok".into(),
            blocks: vec![mk_step("only")],
        }],
        default: Some(vec![mk_step("defc")]),
    };
    let (storage, instance, tree) = setup(router.clone(), json!({"ok": true})).await;
    let reg = HandlerRegistry::new();

    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "only").state, NodeState::Running);
    assert_eq!(find_by_block(&tree, "defc").state, NodeState::Skipped);
}

// #170 — externalized markers in context.data must be inflated before
// conditions are checked. A marker compared against a literal string
// would otherwise fall through to the default.
#[tokio::test]
async fn externalized_marker_inflated_before_check() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                condition: "status == \"active\"".into(),
                blocks: vec![mk_step("active_branch")],
            },
            Route {
                condition: "status == \"inactive\"".into(),
                blocks: vec![mk_step("inactive_branch")],
            },
        ],
        default: Some(vec![mk_step("defc")]),
    };

    // Context stores a marker pointing at the externalized payload.
    let ctx = json!({
        "status": {"_externalized": true, "_ref": "inst:ctx:data:status"}
    });
    let (storage, instance, tree) = setup(router.clone(), ctx).await;
    // Save the real value in externalized_state.
    storage
        .save_externalized_state(instance.id, "inst:ctx:data:status", &json!("active"))
        .await
        .unwrap();

    let reg = HandlerRegistry::new();
    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;

    // First branch matches once inflation swaps the marker for "active".
    assert_eq!(
        find_by_block(&tree, "active_branch").state,
        NodeState::Running
    );
    assert_eq!(
        find_by_block(&tree, "inactive_branch").state,
        NodeState::Skipped
    );
    assert_eq!(find_by_block(&tree, "defc").state, NodeState::Skipped);
}

// #171 — an invalid / unparseable condition is treated as false. Processing
// proceeds to the next route so a typo cannot deadlock the workflow.
#[tokio::test]
async fn invalid_condition_falls_through_to_next_route() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![
            Route {
                // Intentionally malformed — parser returns false, handler
                // must not panic and must try the next route.
                condition: "((( bogus !!".into(),
                blocks: vec![mk_step("broken")],
            },
            Route {
                condition: "true".into(),
                blocks: vec![mk_step("fallback")],
            },
        ],
        default: None,
    };
    let (storage, instance, tree) = setup(router.clone(), json!({})).await;
    let reg = HandlerRegistry::new();

    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;

    assert_eq!(find_by_block(&tree, "broken").state, NodeState::Skipped);
    assert_eq!(find_by_block(&tree, "fallback").state, NodeState::Running);
}

// Additional: empty selected branch (route matches but has zero children) →
// router completes immediately on the same tick. Pins the `branch_children
// .is_empty()` early-completion path when the match is a real route rather
// than the fall-through default.
#[tokio::test]
async fn empty_matched_branch_completes_router() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![],
        }],
        default: Some(vec![mk_step("defc")]),
    };
    let (storage, instance, tree) = setup(router.clone(), json!({})).await;
    let reg = HandlerRegistry::new();

    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(router_node(&tree).state, NodeState::Completed);
    assert_eq!(find_by_block(&tree, "defc").state, NodeState::Skipped);
}

// Additional: branch child failure propagates to the router on the next tick.
// Mirrors the `any_failed` path in the handler's completion block.
#[tokio::test]
async fn failed_branch_child_fails_router() {
    let router = RouterDef {
        id: BlockId("r".into()),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("b0")],
        }],
        default: None,
    };
    let (storage, instance, tree) = setup(router.clone(), json!({})).await;
    let reg = HandlerRegistry::new();

    // Tick 1: activate branch 0 child.
    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    // Child fails externally (simulating handler/exec error).
    storage
        .update_node_state(find_by_block(&tree, "b0").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2: router observes the failure and marks itself Failed.
    execute_router(
        &storage,
        &reg,
        &instance,
        &router_node(&tree),
        &router,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(router_node(&tree).state, NodeState::Failed);
}
