//! E2E tests for Feature #11: Conditional Step Guard (`when`).
//!
//! Validates that the `when` field on [`StepDef`] controls step execution:
//! - Truthy `when` → step executes normally
//! - Falsy `when` → step is skipped ([`NodeState::Skipped`])
//! - No `when` → step always executes
//! - Expression can reference `data.*` and `outputs.*`
//! - Multi-step flows with mixed guards
//! - Skipped step doesn't block subsequent steps
//! - Release diff detects `when` guard changes
//! - Preflight validates `when` expressions

use serde_json::json;

use orch8_engine::handlers::{HandlerRegistry, builtin::register_builtins};
use orch8_types::execution::NodeState;
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;

mod common;
use common::*;

fn handlers_with_produce() -> HandlerRegistry {
    let mut reg = HandlerRegistry::new();
    register_builtins(&mut reg);
    reg.register("produce_output", |ctx| {
        Box::pin(async move {
            let value = ctx
                .params
                .get("output")
                .cloned()
                .unwrap_or_else(|| json!({"result": "produced"}));
            Ok(value)
        })
    });
    reg
}

// ================================================================
// when = truthy → step executes
// ================================================================

#[tokio::test]
async fn when_guard_truthy_executes_step() {
    let (storage, seq, inst) =
        setup_with_ctx(vec![mk_step_with_when("s1", "noop", "true")], json!({})).await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    let node = nodes
        .iter()
        .find(|n| n.block_id == BlockId::new("s1"))
        .unwrap();
    assert_eq!(node.state, NodeState::Completed);
}

// ================================================================
// when = falsy → step is skipped
// ================================================================

#[tokio::test]
async fn when_guard_falsy_skips_step() {
    let (storage, seq, inst) =
        setup_with_ctx(vec![mk_step_with_when("s1", "noop", "false")], json!({})).await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    let node = nodes
        .iter()
        .find(|n| n.block_id == BlockId::new("s1"))
        .unwrap();
    assert_eq!(node.state, NodeState::Skipped);
}

// ================================================================
// when = unparseable → step fails loudly (no silent skip)
// ================================================================

#[tokio::test]
async fn when_guard_parse_error_fails_instance() {
    // M7: an unparseable `when` expression is a malformed step definition,
    // not a falsy condition. It must surface as a failure — instance Failed,
    // node Failed — never as a silent skip that lets the flow complete.
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_when("s1", "noop", "this is not ( valid")],
        json!({}),
    )
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    let node = nodes
        .iter()
        .find(|n| n.block_id == BlockId::new("s1"))
        .unwrap();
    assert_eq!(node.state, NodeState::Failed);
}

// ================================================================
// No when → step always executes
// ================================================================

#[tokio::test]
async fn no_when_guard_always_executes() {
    let (storage, seq, inst) = setup_with_ctx(vec![mk_step("s1", "noop")], json!({})).await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    let node = nodes
        .iter()
        .find(|n| n.block_id == BlockId::new("s1"))
        .unwrap();
    assert_eq!(node.state, NodeState::Completed);
}

// ================================================================
// when references data.* context
// ================================================================

#[tokio::test]
async fn when_guard_references_data_context() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_when("s1", "noop", "data.enabled == true")],
        json!({"enabled": true}),
    )
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "s1"), NodeState::Completed);
}

#[tokio::test]
async fn when_guard_data_false_skips() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_when("s1", "noop", "data.enabled == true")],
        json!({"enabled": false}),
    )
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "s1"), NodeState::Skipped);
}

// ================================================================
// when references outputs.*
// ================================================================

#[tokio::test]
async fn when_guard_references_outputs() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![
            mk_step_with_params(
                "producer",
                "produce_output",
                json!({"output": {"should_run": true}}),
            ),
            mk_step_with_when("gated", "noop", "outputs.producer.should_run == true"),
        ],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "producer"), NodeState::Completed);
    assert_eq!(node_state(&nodes, "gated"), NodeState::Completed);
}

#[tokio::test]
async fn when_guard_outputs_false_skips() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![
            mk_step_with_params(
                "producer",
                "produce_output",
                json!({"output": {"should_run": false}}),
            ),
            mk_step_with_when("gated", "noop", "outputs.producer.should_run == true"),
        ],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "producer"), NodeState::Completed);
    assert_eq!(node_state(&nodes, "gated"), NodeState::Skipped);
}

// ================================================================
// Multi-step: skipped step doesn't block subsequent steps
// ================================================================

#[tokio::test]
async fn when_guard_skipped_step_does_not_block_next() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![
            mk_step_with_when("skip_me", "noop", "false"),
            mk_step("after_skip", "noop"),
        ],
        json!({}),
    )
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "skip_me"), NodeState::Skipped);
    assert_eq!(node_state(&nodes, "after_skip"), NodeState::Completed);
}

// ================================================================
// Multi-step: all skipped → instance completes
// ================================================================

#[tokio::test]
async fn when_guard_all_skipped_completes() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![
            mk_step_with_when("a", "noop", "false"),
            mk_step_with_when("b", "noop", "false"),
        ],
        json!({}),
    )
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "a"), NodeState::Skipped);
    assert_eq!(node_state(&nodes, "b"), NodeState::Skipped);
}

// ================================================================
// when with comparison operators
// ================================================================

#[tokio::test]
async fn when_guard_comparison_gt() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_when("s1", "noop", "data.score > 50")],
        json!({"score": 75}),
    )
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "s1"), NodeState::Completed);
}

#[tokio::test]
async fn when_guard_comparison_gt_fails() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_when("s1", "noop", "data.score > 50")],
        json!({"score": 30}),
    )
    .await;
    let handlers = registry();
    drive(&storage, &handlers, inst.id, &seq).await;

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "s1"), NodeState::Skipped);
}

// ================================================================
// Release diff detects when guard change
// ================================================================

#[test]
fn release_diff_detects_when_guard_change() {
    use orch8_engine::release_diff::{DiffSeverity, semantic_diff};

    let v1: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "when-diff",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "when": "data.enabled == true"
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let v2: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "when-diff",
        "version": 2,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "when": "data.enabled == false"
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let diff = semantic_diff(&v1, &v2);
    let guard_entry = diff
        .entries
        .iter()
        .find(|e| e.category == "when_guard_changed");
    assert!(
        guard_entry.is_some(),
        "diff should detect when guard change: {diff:?}"
    );
    assert_eq!(
        guard_entry.unwrap().severity,
        DiffSeverity::Behavioral,
        "when guard change should be Behavioral severity"
    );
}

// ================================================================
// Release diff: adding when guard detected
// ================================================================

#[test]
fn release_diff_detects_when_guard_added() {
    use orch8_engine::release_diff::semantic_diff;

    let v1: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "when-added",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {}
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let v2: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "when-added",
        "version": 2,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "when": "data.flag == true"
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let diff = semantic_diff(&v1, &v2);
    assert!(
        diff.entries
            .iter()
            .any(|e| e.category == "when_guard_changed"),
        "adding a when guard should be detected: {diff:?}"
    );
}

// ================================================================
// Preflight: valid when guard passes
// ================================================================

#[test]
fn preflight_valid_when_guard_passes() {
    use chrono::{TimeZone, Utc};
    use orch8_engine::preflight::{RuntimeInventory, run_preflight};

    let now = Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap();
    let seq: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "when-preflight",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "when": "data.enabled == true"
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let inv = RuntimeInventory {
        worker_registrations: Some(vec![]),
        version_pins: Some(vec![]),
        credentials: Some(vec![]),
        plugins: Some(vec![]),
        queue_dispatch: Some(vec![]),
        routing_rules: Some(vec![]),
        sequences: Some(vec![]),
    };
    let report = run_preflight(&seq, &inv, now);
    let check = report
        .checks
        .iter()
        .find(|c| c.id == "when_guards_valid")
        .expect("missing when_guards_valid check");
    assert_eq!(
        check.status,
        orch8_types::preflight::PreflightStatus::Pass,
        "valid when guard should pass preflight: {check:?}"
    );
}

// ================================================================
// Preflight: empty when guard warns
// ================================================================

#[test]
fn preflight_empty_when_guard_fails() {
    use chrono::{TimeZone, Utc};
    use orch8_engine::preflight::{RuntimeInventory, run_preflight};

    let now = Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap();
    let seq: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "when-empty",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "when": ""
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let inv = RuntimeInventory {
        worker_registrations: Some(vec![]),
        version_pins: Some(vec![]),
        credentials: Some(vec![]),
        plugins: Some(vec![]),
        queue_dispatch: Some(vec![]),
        routing_rules: Some(vec![]),
        sequences: Some(vec![]),
    };
    let report = run_preflight(&seq, &inv, now);
    let check = report
        .checks
        .iter()
        .find(|c| c.id == "when_guards_valid")
        .expect("missing when_guards_valid check");
    assert_eq!(
        check.status,
        orch8_types::preflight::PreflightStatus::Fail,
        "empty when guard should fail preflight: {check:?}"
    );
}

// ================================================================
// Serde round-trip: when field survives serialization
// ================================================================

#[test]
fn when_field_serde_roundtrip() {
    let step_json = json!({
        "type": "step",
        "id": "s1",
        "handler": "noop",
        "params": {},
        "when": "data.x > 10"
    });
    let block: orch8_types::sequence::BlockDefinition =
        serde_json::from_value(step_json.clone()).unwrap();
    if let orch8_types::sequence::BlockDefinition::Step(ref sd) = block {
        assert_eq!(sd.when.as_deref(), Some("data.x > 10"));
    } else {
        panic!("expected Step variant");
    }

    let serialized = serde_json::to_value(&block).unwrap();
    assert_eq!(serialized["when"], "data.x > 10");
}

#[test]
fn when_field_absent_is_none() {
    let step_json = json!({
        "type": "step",
        "id": "s1",
        "handler": "noop",
        "params": {}
    });
    let block: orch8_types::sequence::BlockDefinition = serde_json::from_value(step_json).unwrap();
    if let orch8_types::sequence::BlockDefinition::Step(ref sd) = block {
        assert!(sd.when.is_none());
    } else {
        panic!("expected Step variant");
    }
}

// ================================================================
// when with mock handler: guard controls dispatch
// ================================================================

#[tokio::test]
async fn when_guard_with_mock_handler_skip() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_when("m1", "my_handler", "false")],
        json!({}),
    )
    .await;
    let mut handlers = registry();
    handlers.set_mock("my_handler", json!({"status": "should_not_run"}));
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "m1"), NodeState::Skipped);

    let output = storage
        .get_block_output(inst.id, &BlockId::new("m1"))
        .await
        .unwrap();
    assert!(output.is_none(), "skipped step should have no output");
}

#[tokio::test]
async fn when_guard_with_mock_handler_execute() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_when("m1", "my_handler", "true")],
        json!({}),
    )
    .await;
    let mut handlers = registry();
    handlers.set_mock("my_handler", json!({"status": "ran"}));
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&nodes, "m1"), NodeState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId::new("m1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["status"], "ran");
}
