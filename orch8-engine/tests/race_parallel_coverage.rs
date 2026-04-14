#![allow(clippy::too_many_lines)]
//! Race + Parallel handler coverage (`TEST_PLAN.md` §Race, §Parallel).
//!
//! Focus: winner arbitration, per-branch cursor, terminal-aggregation.

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator;
use orch8_engine::handlers::parallel::execute_parallel;
use orch8_engine::handlers::race::execute_race;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{
    BlockDefinition, ParallelDef, RaceDef, RaceSemantics, SequenceDefinition, StepDef,
};

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
    }))
}

async fn setup(blocks: Vec<BlockDefinition>) -> (SqliteStorage, TaskInstance, Vec<ExecutionNode>) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "rp-cov".into(),
        version: 1,
        deprecated: false,
        blocks: blocks.clone(),
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

    let tree = evaluator::ensure_execution_tree(&storage, &instance, &blocks)
        .await
        .unwrap();
    (storage, instance, tree)
}

fn find_node(tree: &[ExecutionNode], bt: BlockType) -> ExecutionNode {
    tree.iter()
        .find(|n| n.block_type == bt)
        .expect("composite node")
        .clone()
}

fn node_by_block<'a>(tree: &'a [ExecutionNode], block_id: &str) -> &'a ExecutionNode {
    tree.iter()
        .find(|n| n.block_id.0 == block_id)
        .expect("block")
}

async fn refresh(storage: &SqliteStorage, instance: &TaskInstance) -> Vec<ExecutionNode> {
    storage.get_execution_tree(instance.id).await.unwrap()
}

// ========================= RACE =========================

// First branch to complete wins; other branches are cancelled.
#[tokio::test]
async fn race_winner_cancels_other_branches() {
    let race_def = RaceDef {
        id: BlockId("r".into()),
        branches: vec![vec![mk_step("a")], vec![mk_step("b")]],
        semantics: RaceSemantics::FirstToSucceed,
    };
    let block = BlockDefinition::Race(Box::new(race_def.clone()));
    let (storage, instance, tree) = setup(vec![block]).await;
    let reg = HandlerRegistry::new();

    // Tick 1 — activate both.
    let race_node = find_node(&tree, BlockType::Race);
    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "a").state, NodeState::Running);
    assert_eq!(node_by_block(&tree, "b").state, NodeState::Running);

    // a wins.
    storage
        .update_node_state(node_by_block(&tree, "a").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2 — race must cancel b and complete the race node.
    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "b").state, NodeState::Cancelled);
    assert_eq!(
        find_node(&tree, BlockType::Race).state,
        NodeState::Completed
    );
}

// All branches fail → race fails (no winner).
#[tokio::test]
async fn race_all_branches_fail_node_fails() {
    let race_def = RaceDef {
        id: BlockId("r".into()),
        branches: vec![vec![mk_step("a")], vec![mk_step("b")]],
        semantics: RaceSemantics::FirstToSucceed,
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Race(Box::new(race_def.clone()))]).await;
    let reg = HandlerRegistry::new();
    let race_node = find_node(&tree, BlockType::Race);

    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "a").id, NodeState::Failed)
        .await
        .unwrap();
    storage
        .update_node_state(node_by_block(&tree, "b").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_node(&tree, BlockType::Race).state, NodeState::Failed);
}

// Empty branch list — race completes immediately.
#[tokio::test]
async fn race_empty_branches_completes_immediately() {
    let race_def = RaceDef {
        id: BlockId("r".into()),
        branches: vec![],
        semantics: RaceSemantics::FirstToSucceed,
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Race(Box::new(race_def.clone()))]).await;
    let reg = HandlerRegistry::new();
    let race_node = find_node(&tree, BlockType::Race);

    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(
        find_node(&tree, BlockType::Race).state,
        NodeState::Completed
    );
}

// While branches are still Running, race yields without touching state.
#[tokio::test]
async fn race_yields_while_all_branches_running() {
    let race_def = RaceDef {
        id: BlockId("r".into()),
        branches: vec![vec![mk_step("a")], vec![mk_step("b")]],
        semantics: RaceSemantics::FirstToSucceed,
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Race(Box::new(race_def.clone()))]).await;
    let reg = HandlerRegistry::new();
    let race_node = find_node(&tree, BlockType::Race);

    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    // Both running — second tick yields.
    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    let race_n = find_node(&tree, BlockType::Race);
    assert!(
        !matches!(race_n.state, NodeState::Completed | NodeState::Failed),
        "race must not terminate while branches still run"
    );
    assert_eq!(node_by_block(&tree, "a").state, NodeState::Running);
    assert_eq!(node_by_block(&tree, "b").state, NodeState::Running);
}

// Waiting (dispatched) branch gets its worker task cancelled when another branch wins.
#[tokio::test]
async fn race_cancels_worker_task_for_waiting_loser() {
    let race_def = RaceDef {
        id: BlockId("r".into()),
        branches: vec![vec![mk_step("a")], vec![mk_step("b")]],
        semantics: RaceSemantics::FirstToSucceed,
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Race(Box::new(race_def.clone()))]).await;
    let reg = HandlerRegistry::new();
    let race_node = find_node(&tree, BlockType::Race);

    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    // Put b into Waiting (simulating dispatch to a worker).
    storage
        .update_node_state(node_by_block(&tree, "b").id, NodeState::Waiting)
        .await
        .unwrap();
    // a wins.
    storage
        .update_node_state(node_by_block(&tree, "a").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_race(&storage, &reg, &instance, &race_node, &race_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    // b is transitioned to Cancelled even though it was Waiting.
    assert_eq!(node_by_block(&tree, "b").state, NodeState::Cancelled);
}

// ========================= PARALLEL =========================

// All branches complete → parallel completes.
#[tokio::test]
async fn parallel_all_branches_succeed_node_completes() {
    let par = ParallelDef {
        id: BlockId("p".into()),
        branches: vec![vec![mk_step("a")], vec![mk_step("b")]],
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Parallel(Box::new(par.clone()))]).await;
    let reg = HandlerRegistry::new();
    let par_node = find_node(&tree, BlockType::Parallel);

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "a").id, NodeState::Completed)
        .await
        .unwrap();
    storage
        .update_node_state(node_by_block(&tree, "b").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(
        find_node(&tree, BlockType::Parallel).state,
        NodeState::Completed
    );
}

// Any branch fails → parallel fails (after all branches finish).
#[tokio::test]
async fn parallel_any_branch_fails_node_fails() {
    let par = ParallelDef {
        id: BlockId("p".into()),
        branches: vec![vec![mk_step("a")], vec![mk_step("b")]],
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Parallel(Box::new(par.clone()))]).await;
    let reg = HandlerRegistry::new();
    let par_node = find_node(&tree, BlockType::Parallel);

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "a").id, NodeState::Completed)
        .await
        .unwrap();
    storage
        .update_node_state(node_by_block(&tree, "b").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(
        find_node(&tree, BlockType::Parallel).state,
        NodeState::Failed
    );
}

// Per-branch cursor: activate only first step of each branch.
#[tokio::test]
async fn parallel_activates_only_first_step_per_branch() {
    let par = ParallelDef {
        id: BlockId("p".into()),
        branches: vec![
            vec![mk_step("b0s0"), mk_step("b0s1")],
            vec![mk_step("b1s0"), mk_step("b1s1")],
        ],
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Parallel(Box::new(par.clone()))]).await;
    let reg = HandlerRegistry::new();
    let par_node = find_node(&tree, BlockType::Parallel);

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "b0s0").state, NodeState::Running);
    assert_eq!(node_by_block(&tree, "b0s1").state, NodeState::Pending);
    assert_eq!(node_by_block(&tree, "b1s0").state, NodeState::Running);
    assert_eq!(node_by_block(&tree, "b1s1").state, NodeState::Pending);
}

// Per-branch cursor advances when the first step completes.
#[tokio::test]
async fn parallel_advances_branch_cursor_after_completion() {
    let par = ParallelDef {
        id: BlockId("p".into()),
        branches: vec![vec![mk_step("b0s0"), mk_step("b0s1")]],
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Parallel(Box::new(par.clone()))]).await;
    let reg = HandlerRegistry::new();
    let par_node = find_node(&tree, BlockType::Parallel);

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(node_by_block(&tree, "b0s0").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(node_by_block(&tree, "b0s1").state, NodeState::Running);
}

// Empty branch list — parallel completes immediately.
#[tokio::test]
async fn parallel_empty_branches_completes_immediately() {
    let par = ParallelDef {
        id: BlockId("p".into()),
        branches: vec![],
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Parallel(Box::new(par.clone()))]).await;
    let reg = HandlerRegistry::new();
    let par_node = find_node(&tree, BlockType::Parallel);

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(
        find_node(&tree, BlockType::Parallel).state,
        NodeState::Completed
    );
}

// Uneven branch lengths — short branch finishes first, long branch keeps advancing.
#[tokio::test]
async fn parallel_uneven_branches_short_finishes_first() {
    let par = ParallelDef {
        id: BlockId("p".into()),
        branches: vec![
            vec![mk_step("short")],
            vec![mk_step("long0"), mk_step("long1")],
        ],
    };
    let (storage, instance, tree) =
        setup(vec![BlockDefinition::Parallel(Box::new(par.clone()))]).await;
    let reg = HandlerRegistry::new();
    let par_node = find_node(&tree, BlockType::Parallel);

    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    // Complete the short branch.
    storage
        .update_node_state(node_by_block(&tree, "short").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Parallel should not complete yet — long branch still has work.
    execute_parallel(&storage, &reg, &instance, &par_node, &par, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_ne!(
        find_node(&tree, BlockType::Parallel).state,
        NodeState::Completed
    );
    // long0 is already Running from the initial activation.
    assert_eq!(node_by_block(&tree, "long0").state, NodeState::Running);
}
