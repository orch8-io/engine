#![allow(clippy::too_many_lines)]
//! `check_human_input` strict payload validation coverage.
//!
//! Task 6 of the human-in-the-loop redesign (see
//! `docs/plans/2026-04-19-human-in-the-loop-redesign.md`). Drives the
//! scheduler's `check_human_input` directly against a seeded in-memory
//! storage and asserts that:
//!
//! - `{"value": "<valid>"}` → block output written with canonical
//!   `{"value": ...}` shape, `context.data[store_as or block_id]` merged,
//!   signal marked delivered.
//! - `{"value": "<invalid>"}` or malformed payload → no block output, no
//!   context change, signal marked delivered (so it doesn't replay
//!   forever), block stays waiting (the call re-schedules and reports
//!   "deferred" on the next tick).

use chrono::Utc;
use serde_json::json;

use orch8_engine::scheduler::check_human_input;
use orch8_engine::signals::process_signals;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{
    BlockDefinition, HumanChoice, HumanInputDef, SequenceDefinition, StepDef,
};
use orch8_types::signal::{Signal, SignalType};
use uuid::Uuid;

fn mk_step(id: &str, human: HumanInputDef) -> StepDef {
    StepDef {
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
        wait_for_input: Some(human),
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }
}

async fn setup(step: StepDef) -> (SqliteStorage, TaskInstance, StepDef) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let block = BlockDefinition::Step(Box::new(step.clone()));
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "hitl-cov".into(),
        version: 1,
        deprecated: false,
        blocks: vec![block],
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
            runtime: RuntimeContext {
                started_at: Some(now),
                ..RuntimeContext::default()
            },
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
    (storage, instance, step)
}

async fn enqueue_human_signal(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &str,
    payload: serde_json::Value,
) -> Uuid {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id,
        signal_type: SignalType::Custom(format!("human_input:{block_id}")),
        payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&sig).await.unwrap();
    sig.id
}

fn advanced_def() -> HumanInputDef {
    HumanInputDef {
        prompt: "decide".into(),
        timeout: None,
        escalation_handler: None,
        choices: Some(vec![
            HumanChoice {
                label: "Approve".into(),
                value: "approve".into(),
            },
            HumanChoice {
                label: "Reject".into(),
                value: "reject".into(),
            },
        ]),
        store_as: Some("decision".into()),
    }
}

fn default_yes_no_def() -> HumanInputDef {
    HumanInputDef {
        prompt: "ok?".into(),
        timeout: None,
        escalation_handler: None,
        choices: None,
        store_as: None,
    }
}

/// Tokio runtime wrapper: default `#[tokio::test]` is multi-threaded in this
/// repo (see other `*_coverage.rs` files) — mirror the convention.
#[tokio::test]
async fn valid_value_completes_block_and_writes_context_data() {
    let (storage, instance, step) = setup(mk_step("review", advanced_def())).await;
    let human_def = step.wait_for_input.clone().unwrap();

    enqueue_human_signal(&storage, instance.id, "review", json!({"value": "approve"})).await;

    let deferred = check_human_input(&storage, &instance, &step, &human_def)
        .await
        .expect("check_human_input ok");
    assert!(!deferred, "valid input should not defer");

    // Block output is canonical `{"value": "approve"}`.
    let out = storage
        .get_block_output(instance.id, &step.id)
        .await
        .unwrap()
        .expect("block output written");
    assert_eq!(out.output, json!({"value": "approve"}));

    // Context data merged.
    let refreshed = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(
        refreshed.context.data.get("decision"),
        Some(&json!("approve"))
    );

    // Signal marked delivered.
    let pending = storage.get_pending_signals(instance.id).await.unwrap();
    assert!(
        pending.is_empty(),
        "delivered signal should not remain pending"
    );
}

#[tokio::test]
async fn invalid_value_keeps_block_waiting_and_marks_signal_delivered() {
    let (storage, instance, step) = setup(mk_step("review", advanced_def())).await;
    let human_def = step.wait_for_input.clone().unwrap();

    enqueue_human_signal(&storage, instance.id, "review", json!({"value": "maybe"})).await;

    let deferred = check_human_input(&storage, &instance, &step, &human_def)
        .await
        .expect("check_human_input ok");
    assert!(
        deferred,
        "invalid input should keep block waiting (deferred)"
    );

    // No block output.
    let out = storage
        .get_block_output(instance.id, &step.id)
        .await
        .unwrap();
    assert!(out.is_none(), "invalid payload must not write block output");

    // Context untouched.
    let refreshed = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert!(
        refreshed.context.data.get("decision").is_none(),
        "invalid payload must not write context.data.decision"
    );

    // Signal marked delivered (not in pending anymore).
    let pending = storage.get_pending_signals(instance.id).await.unwrap();
    assert!(
        pending.is_empty(),
        "poison signal must be marked delivered so it doesn't replay forever"
    );
}

#[tokio::test]
async fn malformed_payload_keeps_block_waiting_and_marks_delivered() {
    let (storage, instance, step) = setup(mk_step("review", advanced_def())).await;
    let human_def = step.wait_for_input.clone().unwrap();

    // Missing `value` field entirely.
    enqueue_human_signal(&storage, instance.id, "review", json!({"approved": true})).await;

    let deferred = check_human_input(&storage, &instance, &step, &human_def)
        .await
        .expect("check_human_input ok");
    assert!(deferred);

    assert!(storage
        .get_block_output(instance.id, &step.id)
        .await
        .unwrap()
        .is_none());
    let refreshed = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert!(refreshed.context.data.get("decision").is_none());
    let pending = storage.get_pending_signals(instance.id).await.unwrap();
    assert!(pending.is_empty());
}

#[tokio::test]
async fn default_yes_no_accepts_yes() {
    let (storage, instance, step) = setup(mk_step("ask", default_yes_no_def())).await;
    let human_def = step.wait_for_input.clone().unwrap();

    enqueue_human_signal(&storage, instance.id, "ask", json!({"value": "yes"})).await;

    let deferred = check_human_input(&storage, &instance, &step, &human_def)
        .await
        .expect("check_human_input ok");
    assert!(!deferred);

    let out = storage
        .get_block_output(instance.id, &step.id)
        .await
        .unwrap()
        .expect("block output");
    assert_eq!(out.output, json!({"value": "yes"}));

    // No store_as → key defaults to block id.
    let refreshed = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(refreshed.context.data.get("ask"), Some(&json!("yes")));
}

#[tokio::test]
async fn default_yes_no_rejects_maybe() {
    let (storage, instance, step) = setup(mk_step("ask", default_yes_no_def())).await;
    let human_def = step.wait_for_input.clone().unwrap();

    enqueue_human_signal(&storage, instance.id, "ask", json!({"value": "maybe"})).await;

    let deferred = check_human_input(&storage, &instance, &step, &human_def)
        .await
        .expect("check_human_input ok");
    assert!(deferred, "default yes/no must reject `maybe`");

    assert!(storage
        .get_block_output(instance.id, &step.id)
        .await
        .unwrap()
        .is_none());
    let refreshed = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert!(refreshed.context.data.get("ask").is_none());
    let pending = storage.get_pending_signals(instance.id).await.unwrap();
    assert!(pending.is_empty());
}

/// Regression: the generic `SignalAction::Custom` path in `signals.rs` used
/// to mark every custom signal delivered before `check_human_input` could
/// consume it, which caused human-input signals to be permanently lost. The
/// fix: custom signals whose name starts with `human_input:` stay pending
/// so the block evaluator can consume them.
#[tokio::test]
async fn process_signals_leaves_human_input_signals_pending() {
    let (storage, instance, _step) = setup(mk_step("review", advanced_def())).await;

    // Human-input signal: must stay pending after process_signals.
    enqueue_human_signal(&storage, instance.id, "review", json!({"value": "approve"})).await;

    // Non-human-input custom signal: must be delivered (existing behavior).
    let other = Signal {
        id: Uuid::now_v7(),
        instance_id: instance.id,
        signal_type: SignalType::Custom("ping".into()),
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&other).await.unwrap();

    let aborted = process_signals(&storage, instance.id, InstanceState::Running)
        .await
        .expect("process_signals ok");
    assert!(!aborted, "custom signals must not abort execution");

    let pending = storage.get_pending_signals(instance.id).await.unwrap();
    assert_eq!(pending.len(), 1, "human_input signal must stay pending");
    match &pending[0].signal_type {
        SignalType::Custom(name) => {
            assert_eq!(name, "human_input:review");
        }
        other => panic!("unexpected signal type: {other:?}"),
    }
}
