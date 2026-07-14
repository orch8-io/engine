//! End-to-end tests for the Saga block type.
//!
//! A saga runs steps sequentially. If all actions succeed, the saga succeeds.
//! If any action fails, compensations for all previously completed steps run
//! in reverse order (LIFO), best-effort, then the saga fails.

mod common;

use common::*;
use orch8_types::execution::NodeState;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, SagaDef, SagaStep};

fn mk_saga(id: &str, steps: Vec<SagaStep>) -> BlockDefinition {
    BlockDefinition::Saga(Box::new(SagaDef {
        id: orch8_types::ids::BlockId::new(id),
        steps,
    }))
}

fn saga_step(id: &str, action: BlockDefinition, compensation: Option<BlockDefinition>) -> SagaStep {
    SagaStep {
        id: orch8_types::ids::BlockId::new(id),
        action: Box::new(action),
        compensation: compensation.map(Box::new),
    }
}

// --- Happy path ---

#[tokio::test]
async fn saga_all_actions_succeed_completes() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), Some(mk_step("c0", "noop"))),
            saga_step("s1", mk_step("a1", "noop"), Some(mk_step("c1", "noop"))),
            saga_step("s2", mk_step("a2", "noop"), Some(mk_step("c2", "noop"))),
        ],
    )])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Completed);
}

#[tokio::test]
async fn saga_single_step_success() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![saga_step(
            "s0",
            mk_step("a0", "noop"),
            Some(mk_step("c0", "noop")),
        )],
    )])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Completed);
}

#[tokio::test]
async fn saga_empty_completes() {
    let (storage, seq, inst) = setup(vec![mk_saga("saga1", vec![])]).await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Completed);
}

// --- Failure and compensation ---

#[tokio::test]
async fn saga_first_step_fails_no_compensation_needed() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "fail"), Some(mk_step("c0", "noop"))),
            saga_step("s1", mk_step("a1", "noop"), Some(mk_step("c1", "noop"))),
        ],
    )])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Failed);
}

#[tokio::test]
async fn saga_second_step_fails_first_compensated() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), Some(mk_step("c0", "noop"))),
            saga_step("s1", mk_step("a1", "fail"), Some(mk_step("c1", "noop"))),
        ],
    )])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Failed);

    // Verify comp_0 ran (it should be Completed).
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "c0"), NodeState::Completed);
}

#[tokio::test]
async fn saga_third_step_fails_compensations_reverse_order() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), Some(mk_step("c0", "noop"))),
            saga_step("s1", mk_step("a1", "noop"), Some(mk_step("c1", "noop"))),
            saga_step("s2", mk_step("a2", "fail"), Some(mk_step("c2", "noop"))),
        ],
    )])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Failed);

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "c0"), NodeState::Completed);
    assert_eq!(node_state(&tree, "c1"), NodeState::Completed);
}

#[tokio::test]
async fn saga_step_without_compensation_skipped_in_rollback() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), None),
            saga_step("s1", mk_step("a1", "fail"), Some(mk_step("c1", "noop"))),
        ],
    )])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Failed);
}

#[tokio::test]
async fn saga_compensation_failure_is_best_effort() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), Some(mk_step("c0", "fail"))),
            saga_step("s1", mk_step("a1", "fail"), None),
        ],
    )])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    // Saga still fails even though comp also failed.
    assert_eq!(inst.state, InstanceState::Failed);

    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(node_state(&tree, "c0"), NodeState::Failed);
}

// --- Error context injection ---

#[tokio::test]
async fn saga_error_context_injected_on_failure() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), Some(mk_step("c0", "noop"))),
            saga_step("s1", mk_step("a1", "fail"), None),
        ],
    )])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    let error = inst.context.data.get("_error").expect("_error must be set");
    assert_eq!(error.get("source").and_then(|v| v.as_str()), Some("saga"));
    assert_eq!(
        error.get("block_id").and_then(|v| v.as_str()),
        Some("saga1")
    );
    assert_eq!(
        error.get("failed_step").and_then(|v| v.as_str()),
        Some("s1")
    );
}

// --- Actions produce output ---

#[tokio::test]
async fn saga_action_outputs_are_preserved_on_success() {
    let (storage, seq, inst) = setup(vec![mk_saga(
        "saga1",
        vec![
            saga_step(
                "s0",
                mk_step("a0", "produce_output"),
                Some(mk_step("c0", "noop")),
            ),
            saga_step("s1", mk_step("a1", "produce_output"), None),
        ],
    )])
    .await;
    let reg = registry_with_output();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Completed);

    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    assert!(outputs.iter().any(|o| o.block_id.as_str() == "a0"));
    assert!(outputs.iter().any(|o| o.block_id.as_str() == "a1"));
}

// --- Saga followed by another step ---

#[tokio::test]
async fn saga_followed_by_step_both_run() {
    let (storage, seq, inst) = setup(vec![
        mk_saga(
            "saga1",
            vec![saga_step(
                "s0",
                mk_step("a0", "noop"),
                Some(mk_step("c0", "noop")),
            )],
        ),
        mk_step("after", "noop"),
    ])
    .await;
    let reg = registry();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Completed);
}

#[tokio::test]
async fn saga_failure_blocks_subsequent_step() {
    let (storage, seq, inst) = setup(vec![
        mk_saga("saga1", vec![saga_step("s0", mk_step("a0", "fail"), None)]),
        mk_step("after", "noop"),
    ])
    .await;
    let reg = registry_with_fail();
    drive(&storage, &reg, inst.id, &seq).await;

    let inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Failed);
}

// --- Serde round-trip ---

#[tokio::test]
async fn saga_serde_roundtrip() {
    let saga = mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), Some(mk_step("c0", "noop"))),
            saga_step("s1", mk_step("a1", "noop"), None),
        ],
    );
    let json_str = serde_json::to_string(&saga).unwrap();
    let deserialized: BlockDefinition = serde_json::from_str(&json_str).unwrap();
    if let BlockDefinition::Saga(sg) = &deserialized {
        assert_eq!(sg.steps.len(), 2);
        assert_eq!(sg.steps[0].id.as_str(), "s0");
        assert!(sg.steps[0].compensation.is_some());
        assert_eq!(sg.steps[1].id.as_str(), "s1");
        assert!(sg.steps[1].compensation.is_none());
    } else {
        panic!("expected Saga variant");
    }
}

// --- Release diff detects saga changes ---

#[tokio::test]
async fn saga_release_diff_detects_added_step() {
    use orch8_engine::release_diff::semantic_diff;

    let old = mk_sequence(vec![mk_saga(
        "saga1",
        vec![saga_step("s0", mk_step("a0", "noop"), None)],
    )]);
    let new = mk_sequence(vec![mk_saga(
        "saga1",
        vec![
            saga_step("s0", mk_step("a0", "noop"), None),
            saga_step("s1", mk_step("a1", "noop"), Some(mk_step("c1", "noop"))),
        ],
    )]);

    let diff = semantic_diff(&old, &new);
    // The new action step "a1" should appear in the diff as an addition.
    let added_ids: Vec<&str> = diff
        .entries
        .iter()
        .filter(|e| e.category == "block_added")
        .filter_map(|e| e.block_id.as_deref())
        .collect();
    assert!(
        added_ids.contains(&"a1"),
        "expected diff to detect added saga step, got: {added_ids:?}"
    );
}

// --- Lint warns on empty saga ---

#[tokio::test]
async fn saga_lint_warns_on_empty() {
    use orch8_engine::lint::lint_sequence;

    let seq = mk_sequence(vec![mk_saga("saga1", vec![])]);
    let warnings = lint_sequence(&seq);
    assert!(
        warnings
            .iter()
            .any(|w| w.block_id == "saga1" && w.message.contains("no steps")),
        "expected lint warning for empty saga, got: {warnings:?}"
    );
}
