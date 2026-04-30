#![allow(clippy::too_many_lines)]
//! Loop + `ForEach` handler coverage (#146-163 from `TEST_PLAN.md`).
//!
//! Drives `execute_loop` and `execute_for_each` against seeded storage to
//! pin iteration/cap/fail/empty-body invariants at the handler boundary.
//! The inline unit tests in `loop_block.rs` / `for_each.rs` cover the
//! subtree-reset plumbing (L1..L6, H1..H4, fe_*). These tests exercise the
//! handler's own branching so regressions in cap/condition/collection
//! handling surface without running the whole scheduler.

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator;
use orch8_engine::handlers::for_each::execute_for_each;
use orch8_engine::handlers::loop_block::execute_loop;
use orch8_engine::handlers::param_resolve::OutputsSnapshot;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{BlockDefinition, ForEachDef, LoopDef, SequenceDefinition, StepDef};

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
    block: BlockDefinition,
    ctx: serde_json::Value,
) -> (SqliteStorage, TaskInstance, Vec<ExecutionNode>) {
    let storage = SqliteStorage::in_memory().await.unwrap();

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "loop-fe-cov".into(),
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

fn find_by_block<'a>(tree: &'a [ExecutionNode], id: &str) -> &'a ExecutionNode {
    tree.iter().find(|n| n.block_id.0 == id).expect("node")
}

async fn refresh(storage: &SqliteStorage, instance: &TaskInstance) -> Vec<ExecutionNode> {
    storage.get_execution_tree(instance.id).await.unwrap()
}

// =====================================================================
// LOOP
// =====================================================================

// #146 — loop with empty body completes immediately.
#[tokio::test]
async fn loop_empty_body_completes_immediately() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![],
        max_iterations: 10,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "lp").state, NodeState::Completed);
}

// #147 — loop with max_iterations = 0 fails the node.
#[tokio::test]
async fn loop_max_iterations_zero_fails() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body")],
        max_iterations: 0,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "lp").state, NodeState::Failed);
}

// #148 — loop with falsy condition completes without running body.
#[tokio::test]
async fn loop_falsy_condition_completes_without_activating_body() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "keep_going".into(),
        body: vec![mk_step("body")],
        max_iterations: 10,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    // context.data lacks `keep_going` → falsy via expression engine.
    let (storage, instance, tree) = setup(block, json!({"keep_going": false})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "lp").state, NodeState::Completed);
    // Body child must not have been activated.
    assert_eq!(find_by_block(&tree, "body").state, NodeState::Pending);
}

// #149 — first tick with truthy condition activates body.
#[tokio::test]
async fn loop_first_tick_activates_body() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body")],
        max_iterations: 10,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "body").state, NodeState::Running);
    // No iteration marker yet — one is only persisted after the body
    // goes terminal on a subsequent tick.
    assert!(storage
        .get_block_output(instance.id, &BlockId("lp".into()))
        .await
        .unwrap()
        .is_none());
}

// #150 — body completion increments the iteration marker.
#[tokio::test]
async fn loop_body_completion_increments_iteration_marker() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body")],
        max_iterations: 5,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    // Tick 1: activate body.
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(find_by_block(&tree, "body").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2: should observe all terminal → advance counter.
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let marker = storage
        .get_block_output(instance.id, &BlockId("lp".into()))
        .await
        .unwrap()
        .expect("iteration marker persisted");
    assert_eq!(marker.output["_iterations"].as_u64(), Some(1));

    // Body must have been reset to Pending for iteration 1.
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "body").state, NodeState::Pending);
}

// #151 — cap reached after increment does NOT reset body subtree.
#[tokio::test]
async fn loop_cap_reached_after_increment_leaves_body_terminal() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body")],
        max_iterations: 1,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    // Tick 1: activate body.
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(find_by_block(&tree, "body").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2: increment hits the cap → body is NOT reset.
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(
        find_by_block(&tree, "body").state,
        NodeState::Completed,
        "body must remain terminal when cap reached after increment"
    );

    // Tick 3: top-of-function guard fires (iteration >= cap) → loop completes.
    let lp = find_by_block(&tree, "lp").clone();
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "lp").state, NodeState::Completed);
}

// #152 — body child failure fails the loop.
#[tokio::test]
async fn loop_body_failure_fails_loop() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body")],
        max_iterations: 5,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(find_by_block(&tree, "body").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "lp").state, NodeState::Failed);
}

// #153 — pre-existing marker AT cap short-circuits via top-of-function guard.
#[tokio::test]
async fn loop_preexisting_marker_at_cap_short_circuits() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body")],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();

    // Seed a marker at the cap BEFORE the first tick.
    storage
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: BlockId("lp".into()),
            output: json!({"_iterations": 3}),
            output_ref: None,
            output_size: 0,
            attempt: 3,
            created_at: Utc::now(),
        })
        .await
        .unwrap();

    let lp = find_by_block(&tree, "lp").clone();
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "lp").state, NodeState::Completed);
    assert_eq!(find_by_block(&tree, "body").state, NodeState::Pending);
}

// #154 — running body leaves loop untouched (no flipping, no counter bump).
#[tokio::test]
async fn loop_running_body_does_not_increment_counter() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "true".into(),
        body: vec![mk_step("body")],
        max_iterations: 5,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    // body is Running now. Call again without transitioning it.
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    assert_eq!(find_by_block(&tree, "body").state, NodeState::Running);
    assert!(
        storage
            .get_block_output(instance.id, &BlockId("lp".into()))
            .await
            .unwrap()
            .is_none(),
        "counter must not advance while body is still Running"
    );
}

// #155 — two full iterations then condition false on iteration 3 → complete.
#[tokio::test]
async fn loop_condition_flips_false_mid_run_completes_cleanly() {
    let loop_def = LoopDef {
        id: BlockId("lp".into()),
        condition: "keep".into(),
        body: vec![mk_step("body")],
        max_iterations: 10,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
    };
    let block = BlockDefinition::Loop(Box::new(loop_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"keep": true})).await;
    let reg = HandlerRegistry::new();
    let lp = find_by_block(&tree, "lp").clone();

    // Iteration 0: activate + complete body.
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(find_by_block(&tree, "body").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_loop(&storage, &reg, &instance, &lp, &loop_def, &tree)
        .await
        .unwrap();

    // Mid-run: flip the condition by updating context.data.
    let mut ctx = instance.context.clone();
    ctx.data = json!({"keep": false});
    storage
        .update_instance_context(instance.id, &ctx)
        .await
        .unwrap();

    // Re-fetch the instance (context updated) and tick the loop — condition
    // is now falsy, so the handler should complete the loop cleanly.
    let updated = storage.get_instance(instance.id).await.unwrap().unwrap();
    let tree = refresh(&storage, &instance).await;
    execute_loop(&storage, &reg, &updated, &lp, &loop_def, &tree)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "lp").state, NodeState::Completed);
}

// =====================================================================
// FOR EACH
// =====================================================================

// #156 — empty body → immediate complete.
#[tokio::test]
async fn for_each_empty_body_completes_immediately() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "it".into(),
        body: vec![],
        max_iterations: 100,
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"items": [1, 2, 3]})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "fe").state, NodeState::Completed);
}

// #157 — missing collection path → complete.
#[tokio::test]
async fn for_each_missing_collection_completes() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "missing".into(),
        item_var: "it".into(),
        body: vec![mk_step("body")],
        max_iterations: 100,
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "fe").state, NodeState::Completed);
    assert_eq!(find_by_block(&tree, "body").state, NodeState::Pending);
}

// #158 — non-array path → complete.
#[tokio::test]
async fn for_each_non_array_completes() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "it".into(),
        body: vec![mk_step("body")],
        max_iterations: 100,
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"items": "not-an-array"})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "fe").state, NodeState::Completed);
}

// #159 — empty collection → complete without body activation.
#[tokio::test]
async fn for_each_empty_collection_completes() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "items".into(),
        item_var: "it".into(),
        body: vec![mk_step("body")],
        max_iterations: 100,
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"items": []})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "fe").state, NodeState::Completed);
    assert_eq!(find_by_block(&tree, "body").state, NodeState::Pending);
}

// #160 — first tick binds items[0] into item_var and activates body.
#[tokio::test]
async fn for_each_first_tick_binds_item_var_and_activates_body() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "xs".into(),
        item_var: "cur".into(),
        body: vec![mk_step("body")],
        max_iterations: 100,
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"xs": ["alpha", "beta", "gamma"]})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();

    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "body").state, NodeState::Running);

    let updated = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(updated.context.data["cur"], json!("alpha"));

    // Snapshot marker persisted on first visit.
    let marker = storage
        .get_block_output(instance.id, &BlockId("fe".into()))
        .await
        .unwrap()
        .expect("snapshot marker persisted");
    assert_eq!(marker.output["_index"], json!(0));
    assert_eq!(marker.output["_total"], json!(3));
    assert_eq!(marker.output["_item_var"], json!("cur"));
    let snapshot_ref = marker
        .output
        .get("_snapshot_ref")
        .and_then(|v| v.as_str())
        .expect("marker must carry _snapshot_ref");
    let snap = storage
        .get_externalized_state(snapshot_ref)
        .await
        .unwrap()
        .expect("snapshot must be externalized");
    assert_eq!(
        snap,
        json!(["alpha", "beta", "gamma"]),
        "collection snapshot must be cached on first visit"
    );
}

// #161 — snapshot is stable: mutating context.data between ticks does not
// change the iteration list (fix for A5a).
#[tokio::test]
async fn for_each_snapshot_is_stable_under_context_mutation() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "xs".into(),
        item_var: "cur".into(),
        body: vec![mk_step("body")],
        max_iterations: 100,
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"xs": ["a", "b"]})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    // Tick 1: snapshot ["a","b"].
    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();

    // Mutate context.data BEFORE body terminates.
    let mut ctx = instance.context.clone();
    ctx.data = json!({"xs": ["x", "y", "z", "q"], "cur": "a"});
    storage
        .update_instance_context(instance.id, &ctx)
        .await
        .unwrap();
    let updated = storage.get_instance(instance.id).await.unwrap().unwrap();

    // Terminate body so the handler increments _index.
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(find_by_block(&tree, "body").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2: advance index. Marker `_total` must remain 2 (original snapshot).
    execute_for_each(
        &storage,
        &reg,
        &updated,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let marker = storage
        .get_block_output(instance.id, &BlockId("fe".into()))
        .await
        .unwrap()
        .expect("marker present");
    assert_eq!(marker.output["_index"], json!(1));
    assert_eq!(
        marker.output["_total"],
        json!(2),
        "snapshot must not be re-resolved from the mutated context"
    );
    let snapshot_ref = marker
        .output
        .get("_snapshot_ref")
        .and_then(|v| v.as_str())
        .expect("marker must carry _snapshot_ref");
    let snap = storage
        .get_externalized_state(snapshot_ref)
        .await
        .unwrap()
        .expect("snapshot must be externalized");
    assert_eq!(snap, json!(["a", "b"]));
}

// #162 — body child failure fails the for_each.
#[tokio::test]
async fn for_each_body_failure_fails_node() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "xs".into(),
        item_var: "it".into(),
        body: vec![mk_step("body")],
        max_iterations: 100,
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"xs": [1, 2]})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(find_by_block(&tree, "body").id, NodeState::Failed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "fe").state, NodeState::Failed);
}

// #163 — max_iterations cap honored below the collection length.
#[tokio::test]
async fn for_each_max_iterations_caps_below_collection_length() {
    let fe_def = ForEachDef {
        id: BlockId("fe".into()),
        collection: "xs".into(),
        item_var: "it".into(),
        body: vec![mk_step("body")],
        max_iterations: 1, // collection has 3 items but cap is 1.
    };
    let block = BlockDefinition::ForEach(Box::new(fe_def.clone()));
    let (storage, instance, tree) = setup(block, json!({"xs": [1, 2, 3]})).await;
    let reg = HandlerRegistry::new();
    let fe = find_by_block(&tree, "fe").clone();

    // Tick 1: bind & activate body for items[0].
    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    storage
        .update_node_state(find_by_block(&tree, "body").id, NodeState::Completed)
        .await
        .unwrap();
    let tree = refresh(&storage, &instance).await;

    // Tick 2: increment to _index=1, which equals effective_max (1 from cap).
    // Cap-reached branch leaves body terminal without reset.
    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(
        find_by_block(&tree, "body").state,
        NodeState::Completed,
        "body must remain terminal after cap-reaching increment"
    );

    // Tick 3: top-of-function cap guard completes the node.
    execute_for_each(
        &storage,
        &reg,
        &instance,
        &fe,
        &fe_def,
        &tree,
        &OutputsSnapshot::new(),
    )
    .await
    .unwrap();
    let tree = refresh(&storage, &instance).await;
    assert_eq!(find_by_block(&tree, "fe").state, NodeState::Completed);
}
