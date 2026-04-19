#![allow(clippy::too_many_lines)]
//! `CancellationScope` handler coverage (#339-345 from `TEST_PLAN.md`).
//!
//! Drives `execute_cancellation_scope` against seeded storage. Pins the
//! sequential-activation + terminal-aggregation invariants that make the
//! block useful for "protected" work (external cancel must not interrupt
//! the group, but an internal failure must still propagate).

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator;
use orch8_engine::handlers::cancellation_scope::execute_cancellation_scope;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::{BlockType, NodeState};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, CancellationScopeDef, SequenceDefinition, StepDef};

fn mk_step(id: &str) -> BlockDefinition {
    BlockDefinition::Step(StepDef {
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
    })
}

async fn setup(
    scope: CancellationScopeDef,
) -> (
    SqliteStorage,
    TaskInstance,
    Vec<orch8_types::execution::ExecutionNode>,
) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let block = BlockDefinition::CancellationScope(scope);

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "cs-cov".into(),
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
    storage.create_instance(&instance).await.unwrap();

    let tree = evaluator::ensure_execution_tree(&storage, &instance, &[block])
        .await
        .unwrap();
    (storage, instance, tree)
}

fn scope_node(
    tree: &[orch8_types::execution::ExecutionNode],
) -> orch8_types::execution::ExecutionNode {
    tree.iter()
        .find(|n| matches!(n.block_type, BlockType::CancellationScope))
        .expect("cancellation scope node")
        .clone()
}

fn node_by_block<'a>(
    tree: &'a [orch8_types::execution::ExecutionNode],
    block_id: &str,
) -> &'a orch8_types::execution::ExecutionNode {
    tree.iter()
        .find(|n| n.block_id.0 == block_id)
        .expect("block in tree")
}

async fn refresh(
    storage: &SqliteStorage,
    instance: &TaskInstance,
) -> Vec<orch8_types::execution::ExecutionNode> {
    storage.get_execution_tree(instance.id).await.unwrap()
}

// #339 — CancellationScope protects children from external cancel.
// This test covers the handler-layer invariant: activation is sequential and
// does not respond to any external cancel signal. The cancel-propagation
// suppression itself lives in the scheduler's signal path — here we pin that
// `execute_cancellation_scope` itself never transitions children to Cancelled
// as a side effect of its own logic.
#[tokio::test]
async fn children_are_activated_one_at_a_time() {
    let scope = CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![mk_step("a"), mk_step("b"), mk_step("c")],
    };
    let (storage, instance, tree) = setup(scope.clone()).await;
    let reg = HandlerRegistry::new();

    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Only the first child should be Running; the rest stay Pending.
    assert_eq!(node_by_block(&tree, "a").state, NodeState::Running);
    assert_eq!(node_by_block(&tree, "b").state, NodeState::Pending);
    assert_eq!(node_by_block(&tree, "c").state, NodeState::Pending);
}

// #340 — CancellationScope allows internal failure to propagate.
#[tokio::test]
async fn internal_child_failure_fails_the_scope() {
    let scope = CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![mk_step("a")],
    };
    let (storage, instance, tree) = setup(scope.clone()).await;
    let reg = HandlerRegistry::new();

    // Activate.
    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    // Child fails.
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "a").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Next tick: scope should observe failure and mark itself Failed.
    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(scope_node(&tree).state, NodeState::Failed);
}

// #341 — CancellationScope completes normally when not cancelled.
#[tokio::test]
async fn scope_completes_when_all_children_succeed() {
    let scope = CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![mk_step("a"), mk_step("b")],
    };
    let (storage, instance, tree) = setup(scope.clone()).await;
    let reg = HandlerRegistry::new();

    // Tick 1: activate a.
    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "a").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2: activate b.
    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "b").state, NodeState::Running);
    storage
        .update_node_state(node_by_block(&tree, "b").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 3: all terminal → scope completes.
    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(scope_node(&tree).state, NodeState::Completed);
}

// #342 — scope yields to in-flight children (Running/Waiting) without flipping them.
#[tokio::test]
async fn scope_waits_on_running_child_without_touching_state() {
    let scope = CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![mk_step("a"), mk_step("b")],
    };
    let (storage, instance, tree) = setup(scope.clone()).await;
    let reg = HandlerRegistry::new();

    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    let a_id = node_by_block(&tree, "a").id;

    // Second tick while a is still Running — must not activate b or flip a.
    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    let a_after = tree.iter().find(|n| n.id == a_id).unwrap();
    assert_eq!(a_after.state, NodeState::Running);
    assert_eq!(node_by_block(&tree, "b").state, NodeState::Pending);
}

// #343 — nested CancellationScope protects inner scope independently.
// Direct-call variant: we drive both scope nodes in sequence and verify the
// inner scope activates its own children without being affected by the
// outer scope's state.
#[tokio::test]
async fn nested_scope_activates_independently() {
    let inner = CancellationScopeDef {
        id: BlockId("inner".into()),
        blocks: vec![mk_step("inner_a")],
    };
    let outer = CancellationScopeDef {
        id: BlockId("outer".into()),
        blocks: vec![BlockDefinition::CancellationScope(inner.clone())],
    };
    let (storage, instance, tree) = setup(outer.clone()).await;
    let reg = HandlerRegistry::new();

    // Tick 1 on outer: activate inner.
    let outer_node = tree
        .iter()
        .find(|n| n.block_id == BlockId("outer".into()))
        .unwrap()
        .clone();
    execute_cancellation_scope(&storage, &reg, &instance, &outer_node, &outer, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    let inner_node = tree
        .iter()
        .find(|n| n.block_id == BlockId("inner".into()))
        .unwrap()
        .clone();
    assert_eq!(inner_node.state, NodeState::Running);

    // Tick on inner: activate inner_a.
    execute_cancellation_scope(&storage, &reg, &instance, &inner_node, &inner, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "inner_a").state, NodeState::Running);
}

// #344 — scope with no children short-circuits to Completed.
#[tokio::test]
async fn empty_scope_completes_immediately() {
    let scope = CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![],
    };
    let (storage, instance, tree) = setup(scope.clone()).await;
    let reg = HandlerRegistry::new();

    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(scope_node(&tree).state, NodeState::Completed);
}

// #345 — scope marks Completed after all children finish.
#[tokio::test]
async fn single_child_completion_completes_scope() {
    let scope = CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![mk_step("only")],
    };
    let (storage, instance, tree) = setup(scope.clone()).await;
    let reg = HandlerRegistry::new();

    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "only").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_cancellation_scope(&storage, &reg, &instance, &scope_node(&tree), &scope, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(scope_node(&tree).state, NodeState::Completed);
}
