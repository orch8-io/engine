//! E2E tests for Feature #09: Block Output Schemas.
//!
//! Validates that `output_schema` on `StepDef` gates handler output at
//! runtime — conforming output succeeds, non-conforming output causes a
//! permanent failure. Also covers preflight and release-diff integration.

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
// Conforming output + output_schema → success
// ================================================================

#[tokio::test]
async fn output_schema_conforming_output_completes() {
    let schema = json!({
        "type": "object",
        "properties": {
            "score": {"type": "number"},
            "label": {"type": "string"}
        },
        "required": ["score"]
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_output_schema(
            "s1",
            "produce_output",
            json!({"output": {"score": 95, "label": "A+"}}),
            schema,
        )],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId::new("s1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["score"], 95);
    assert_eq!(output.output["label"], "A+");
}

// ================================================================
// Non-conforming output + output_schema → permanent failure
// ================================================================

#[tokio::test]
async fn output_schema_non_conforming_output_fails_permanently() {
    let schema = json!({
        "type": "object",
        "properties": {
            "score": {"type": "number"}
        },
        "required": ["score"]
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_output_schema(
            "s1",
            "produce_output",
            json!({"output": {"score": "not_a_number"}}),
            schema,
        )],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
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
// Missing required field → permanent failure
// ================================================================

#[tokio::test]
async fn output_schema_missing_required_field_fails() {
    let schema = json!({
        "type": "object",
        "required": ["name", "age"]
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_output_schema(
            "s1",
            "produce_output",
            json!({"output": {"name": "Alice"}}),
            schema,
        )],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// ================================================================
// No output_schema → any output accepted
// ================================================================

#[tokio::test]
async fn no_output_schema_accepts_any_output() {
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_params(
            "s1",
            "produce_output",
            json!({"output": {"arbitrary": [1, 2, 3]}}),
        )],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// ================================================================
// Multi-step: first step validates, second step has no schema
// ================================================================

#[tokio::test]
async fn output_schema_multi_step_mixed() {
    let schema = json!({
        "type": "object",
        "properties": {"count": {"type": "integer"}},
        "required": ["count"]
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![
            mk_step_with_output_schema(
                "validated",
                "produce_output",
                json!({"output": {"count": 10}}),
                schema,
            ),
            mk_step_with_params(
                "unvalidated",
                "produce_output",
                json!({"output": {"free": "form"}}),
            ),
        ],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let out1 = storage
        .get_block_output(inst.id, &BlockId::new("validated"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out1.output["count"], 10);

    let out2 = storage
        .get_block_output(inst.id, &BlockId::new("unvalidated"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out2.output["free"], "form");
}

// ================================================================
// Transform handler output validated against schema
// ================================================================

#[tokio::test]
async fn output_schema_with_transform_handler() {
    let schema = json!({
        "type": "object",
        "properties": {
            "greeting": {"type": "string"},
            "count": {"type": "number"}
        },
        "required": ["greeting", "count"]
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_output_schema(
            "t1",
            "transform",
            json!({"greeting": "hello", "count": 42}),
            schema,
        )],
        json!({}),
    )
    .await;
    let mut handlers = HandlerRegistry::new();
    register_builtins(&mut handlers);
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

// ================================================================
// additionalProperties: false rejects unexpected fields
// ================================================================

#[tokio::test]
async fn output_schema_strict_rejects_additional_properties() {
    let schema = json!({
        "type": "object",
        "properties": {"a": {"type": "number"}},
        "additionalProperties": false
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_output_schema(
            "s1",
            "produce_output",
            json!({"output": {"a": 1, "b": "surprise"}}),
            schema,
        )],
        json!({}),
    )
    .await;
    let handlers = handlers_with_produce();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}

// ================================================================
// Preflight: well-formed output schema passes
// ================================================================

#[test]
fn preflight_valid_output_schema_passes() {
    use chrono::{TimeZone, Utc};
    use orch8_engine::preflight::{RuntimeInventory, run_preflight};

    let now = Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap();
    let seq: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "schema-test",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "output_schema": {
                "type": "object",
                "properties": {"x": {"type": "number"}},
                "required": ["x"]
            }
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
        .find(|c| c.id == "output_schemas_valid")
        .expect("missing output_schemas_valid check");
    assert_eq!(
        check.status,
        orch8_types::preflight::PreflightStatus::Pass,
        "well-formed output schema should pass preflight: {check:?}"
    );
}

// ================================================================
// Preflight: malformed output schema (non-object) fails
// ================================================================

#[test]
fn preflight_non_object_output_schema_fails() {
    use chrono::{TimeZone, Utc};
    use orch8_engine::preflight::{RuntimeInventory, run_preflight};

    let now = Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap();
    let seq: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "bad-schema-test",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "output_schema": "not-an-object"
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
        .find(|c| c.id == "output_schemas_valid")
        .expect("missing output_schemas_valid check");
    assert_eq!(
        check.status,
        orch8_types::preflight::PreflightStatus::Fail,
        "non-object output schema should fail preflight: {check:?}"
    );
    assert!(check.findings[0].summary.contains("string"));
}

// ================================================================
// Release diff: output_schema change detected
// ================================================================

#[test]
fn release_diff_detects_output_schema_change() {
    use orch8_engine::release_diff::{DiffSeverity, semantic_diff};

    let v1: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "diff-test",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "output_schema": {"type": "object", "properties": {"a": {"type": "number"}}}
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let v2: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "diff-test",
        "version": 2,
        "blocks": [{
            "type": "step",
            "id": "s1",
            "handler": "noop",
            "params": {},
            "output_schema": {"type": "object", "properties": {"a": {"type": "string"}}}
        }],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap();

    let diff = semantic_diff(&v1, &v2);
    let schema_entry = diff
        .entries
        .iter()
        .find(|e| e.category == "output_schema_changed");
    assert!(
        schema_entry.is_some(),
        "diff should detect output_schema change: {diff:?}"
    );
    assert_eq!(
        schema_entry.unwrap().severity,
        DiffSeverity::Behavioral,
        "output_schema change should be Behavioral severity"
    );
}

// ================================================================
// Output schema with mock handler via set_mock
// ================================================================

#[tokio::test]
async fn output_schema_with_mock_handler_conforming() {
    let schema = json!({
        "type": "object",
        "properties": {"status": {"type": "string"}},
        "required": ["status"]
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_output_schema(
            "m1",
            "my_handler",
            json!({}),
            schema,
        )],
        json!({}),
    )
    .await;
    let mut handlers = registry();
    handlers.set_mock("my_handler", json!({"status": "ok"}));
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);
}

#[tokio::test]
async fn output_schema_with_mock_handler_non_conforming() {
    let schema = json!({
        "type": "object",
        "properties": {"status": {"type": "string"}},
        "required": ["status"]
    });
    let (storage, seq, inst) = setup_with_ctx(
        vec![mk_step_with_output_schema(
            "m1",
            "my_handler",
            json!({}),
            schema,
        )],
        json!({}),
    )
    .await;
    let mut handlers = registry();
    handlers.set_mock("my_handler", json!({"status": 999}));
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);
}
