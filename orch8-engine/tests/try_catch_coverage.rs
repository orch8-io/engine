#![allow(clippy::too_many_lines)]
//! Try-Catch handler coverage (#136-145 from `TEST_PLAN.md`).
//!
//! Directly drives `execute_try_catch` against a seeded in-memory storage
//! and asserts phase transitions (try → catch → finally). Covers success,
//! failure, recovery, error-context injection, and empty-branch edge cases.

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator;
use orch8_engine::handlers::try_catch::execute_try_catch;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef, TryCatchDef};

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
    tc: TryCatchDef,
) -> (
    SqliteStorage,
    TaskInstance,
    Vec<orch8_types::execution::ExecutionNode>,
) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let block = BlockDefinition::TryCatch(tc);

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "tc-cov".into(),
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

fn tc_node(
    tree: &[orch8_types::execution::ExecutionNode],
) -> orch8_types::execution::ExecutionNode {
    tree.iter()
        .find(|n| matches!(n.block_type, orch8_types::execution::BlockType::TryCatch))
        .expect("try_catch node")
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

// #136 — try success skips catch, runs finally
#[tokio::test]
async fn try_success_skips_catch_then_runs_finally() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![mk_step("c1")],
        finally_block: Some(vec![mk_step("f1")]),
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    // Tick 1: activate try.
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "t1").state, NodeState::Running);
    assert_eq!(node_by_block(&tree, "c1").state, NodeState::Pending);

    // Simulate try success.
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2: should skip catch and activate finally.
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "c1").state, NodeState::Skipped);
    assert_eq!(node_by_block(&tree, "f1").state, NodeState::Running);
}

// #137 — try failure runs catch then finally
#[tokio::test]
async fn try_failure_runs_catch_then_finally() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![mk_step("c1")],
        finally_block: Some(vec![mk_step("f1")]),
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(
        node_by_block(&tree, "c1").state,
        NodeState::Running,
        "catch should activate on try failure"
    );
    assert_eq!(node_by_block(&tree, "f1").state, NodeState::Pending);
}

// #138 — catch success recovers from try failure (node completes)
#[tokio::test]
async fn catch_success_recovers_node_completes() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![mk_step("c1")],
        finally_block: None,
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "c1").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    let tc_n = tc_node(&tree);
    assert_eq!(
        tc_n.state,
        NodeState::Completed,
        "try_catch node completes when catch recovers"
    );
}

// #139 — catch failure → instance fails (after finally)
#[tokio::test]
async fn catch_failure_fails_node_even_after_finally_runs() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![mk_step("c1")],
        finally_block: Some(vec![mk_step("f1")]),
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "c1").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    // Finally should be Running now.
    assert_eq!(node_by_block(&tree, "f1").state, NodeState::Running);
    storage
        .update_node_state(node_by_block(&tree, "f1").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    assert_eq!(tc_node(&tree).state, NodeState::Failed);
}

// #140 — finally always runs even when try+catch both fail
#[tokio::test]
async fn finally_runs_when_try_and_catch_both_fail() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![mk_step("c1")],
        finally_block: Some(vec![mk_step("f1")]),
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    // Fail try.
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Failed)
        .await
        .unwrap();

    // Fail catch.
    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "c1").id, NodeState::Failed)
        .await
        .unwrap();

    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    assert_eq!(
        node_by_block(&tree, "f1").state,
        NodeState::Running,
        "finally runs regardless of try/catch outcome"
    );
}

// #141 — error context injected into catch block
#[tokio::test]
async fn error_context_injected_into_catch() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![mk_step("c1")],
        finally_block: None,
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();

    // After catch activation, context.data._error must be populated.
    let updated = storage.get_instance(instance.id).await.unwrap().unwrap();
    let err = updated
        .context
        .data
        .get("_error")
        .expect("_error key injected into context.data");
    assert_eq!(
        err.get("source").and_then(|v| v.as_str()),
        Some("try_catch")
    );
    assert_eq!(err.get("block_id").and_then(|v| v.as_str()), Some("tc"));
}

// #142 — error context contains failed block IDs
#[tokio::test]
async fn error_context_lists_failed_block_ids() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1"), mk_step("t2")],
        catch_block: vec![mk_step("c1")],
        finally_block: None,
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    // Fail only t2, leave t1 completed.
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Completed)
        .await
        .unwrap();
    storage
        .update_node_state(node_by_block(&tree, "t2").id, NodeState::Failed)
        .await
        .unwrap();

    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();

    let updated = storage.get_instance(instance.id).await.unwrap().unwrap();
    let failed_blocks = updated
        .context
        .data
        .get("_error")
        .and_then(|e| e.get("failed_blocks"))
        .and_then(|f| f.as_array())
        .expect("failed_blocks array");
    let ids: Vec<&str> = failed_blocks.iter().filter_map(|v| v.as_str()).collect();
    assert!(ids.contains(&"t2"), "failed blocks must include t2");
    assert!(!ids.contains(&"t1"), "completed blocks must not appear");
}

// #143 — empty try block → immediate success
#[tokio::test]
async fn empty_try_block_immediate_success() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![],
        catch_block: vec![mk_step("c1")],
        finally_block: None,
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    // With no try children, try is trivially "not failed" — catch is skipped,
    // finally is empty, node completes in one tick.
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(tc_node(&tree).state, NodeState::Completed);
    assert_eq!(node_by_block(&tree, "c1").state, NodeState::Skipped);
}

// #144 — empty catch block → noop on failure (silently swallows error).
// Pinned behaviour: with no catch children, the handler has no "failed catch"
// to observe, so `try_catch.rs` treats the node as recovered and completes it.
// This lets authors write `try: [...], catch: []` as an "ignore errors" marker.
#[tokio::test]
async fn empty_catch_block_swallows_try_failure() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![],
        finally_block: None,
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();

    let tree = refresh(&storage, &instance).await;
    assert_eq!(tc_node(&tree).state, NodeState::Completed);
}

// #145 — empty finally block → noop, node completes when try succeeds
#[tokio::test]
async fn empty_finally_block_is_noop() {
    let tc = TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t1")],
        catch_block: vec![mk_step("c1")],
        finally_block: Some(vec![]),
    };
    let (storage, instance, tree) = setup(tc.clone()).await;
    let reg = HandlerRegistry::new();

    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "t1").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_try_catch(&storage, &reg, &instance, &tc_node(&tree), &tc, &tree)
        .await
        .unwrap();

    let tree = refresh(&storage, &instance).await;
    assert_eq!(tc_node(&tree).state, NodeState::Completed);
}
