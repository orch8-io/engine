//! Integration tests for the embeddable `orch8` facade: builder, background
//! tick loop, manual ticking, signals, and graceful shutdown — all through
//! the public API only.

use std::time::Duration;

use orch8::{
    CreateInstanceOptions, Engine, InstanceId, InstanceState, SignalType, StepContext, Storage,
};

/// Bounded-wait helper: poll `get_instance` until the predicate holds.
async fn wait_for_state(
    engine: &Engine,
    id: InstanceId,
    timeout: Duration,
    pred: impl Fn(InstanceState) -> bool,
) -> InstanceState {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let state = engine.get_instance(id).await.expect("get_instance").state;
        if pred(state) {
            return state;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for instance {id} (last state: {state:?})"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn two_step_sequence(name: &str, handler: &str) -> orch8::SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": name,
        "version": 1,
        "blocks": [
            { "type": "step", "id": "s1", "handler": handler, "params": {} },
            { "type": "step", "id": "s2", "handler": "noop", "params": {} }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    }))
    .expect("valid sequence definition")
}

async fn build_engine() -> Engine {
    Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .tick_interval(Duration::from_millis(20))
        .handler("custom_step", |_ctx: StepContext| async move {
            Ok(serde_json::json!({ "ok": true }))
        })
        .build()
        .await
        .expect("engine builds")
}

/// Builder + in-memory sqlite + custom handler: the background loop runs a
/// two-step sequence to completion, observed by polling `get_instance`.
#[tokio::test]
async fn start_runs_sequence_to_completion() {
    let engine = build_engine().await;
    engine.start();

    let seq_id = engine
        .upsert_sequence(two_step_sequence("bg-seq", "custom_step"))
        .await
        .expect("upsert");
    let inst = engine
        .create_instance(seq_id, CreateInstanceOptions::default())
        .await
        .expect("create");

    let state = wait_for_state(&engine, inst, Duration::from_secs(10), |s| {
        matches!(
            s,
            InstanceState::Completed | InstanceState::Failed | InstanceState::Cancelled
        )
    })
    .await;
    assert_eq!(state, InstanceState::Completed);

    engine.shutdown().await;
}

/// Manual-tick mode: without `start()`, repeated `tick_once` calls advance
/// the instance to completion.
#[tokio::test]
async fn manual_tick_once_completes_instance() {
    let engine = build_engine().await;

    let seq_id = engine
        .upsert_sequence(two_step_sequence("manual-seq", "custom_step"))
        .await
        .expect("upsert");
    let inst = engine
        .create_instance(seq_id, CreateInstanceOptions::default())
        .await
        .expect("create");

    let mut completed = false;
    for _ in 0..100 {
        let result = engine.tick_once().await.expect("tick");
        let state = engine.get_instance(inst).await.expect("get").state;
        if state == InstanceState::Completed {
            assert!(
                !result.has_pending_work || result.instances_advanced > 0 || completed,
                "tick result should be coherent with the observed state"
            );
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(completed, "instance did not complete via manual ticking");
}

/// `send_signal` resolves a `wait_for_input` gate: the instance parks
/// waiting for human input, a custom `human_input:<block>` signal wakes it,
/// and the sequence then runs to completion (mirrors the engine's HITL
/// signal tests through the public facade).
#[tokio::test]
async fn send_signal_wakes_waiting_instance() {
    let engine = build_engine().await;
    engine.start();

    let seq: orch8::SequenceDefinition = serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": "gated-seq",
        "version": 1,
        "blocks": [
            {
                "type": "step",
                "id": "gate",
                "handler": "noop",
                "params": {},
                "wait_for_input": { "prompt": "approve?" }
            },
            { "type": "step", "id": "after", "handler": "custom_step", "params": {} }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    }))
    .expect("valid sequence");

    let seq_id = engine.upsert_sequence(seq).await.expect("upsert");
    let inst = engine
        .create_instance(seq_id, CreateInstanceOptions::default())
        .await
        .expect("create");

    // Give the scheduler time to reach the gate; the instance must NOT
    // complete while the human-input gate is unresolved.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let parked = engine.get_instance(inst).await.expect("get").state;
    assert!(
        !matches!(
            parked,
            InstanceState::Completed | InstanceState::Failed | InstanceState::Cancelled
        ),
        "instance should be parked at the gate, was {parked:?}"
    );

    // Resolve the gate. The signal name is `human_input:<block_id>` and the
    // payload must carry one of the effective choices (default yes/no).
    engine
        .send_signal(
            inst,
            SignalType::Custom("human_input:gate".to_string()),
            serde_json::json!({ "value": "yes" }),
        )
        .await
        .expect("send_signal");

    let state = wait_for_state(&engine, inst, Duration::from_secs(10), |s| {
        matches!(
            s,
            InstanceState::Completed | InstanceState::Failed | InstanceState::Cancelled
        )
    })
    .await;
    assert_eq!(state, InstanceState::Completed);

    // The chosen value lands in context.data under the block id.
    let snapshot = engine.get_instance(inst).await.expect("get");
    assert_eq!(snapshot.context.data["gate"], "yes");

    engine.shutdown().await;
}

/// Graceful shutdown: start, create work, shutdown completes without
/// hanging (bounded by an outer timeout) and signals to terminal instances
/// are rejected.
#[tokio::test]
async fn shutdown_is_graceful_and_bounded() {
    let engine = build_engine().await;
    engine.start();

    let seq_id = engine
        .upsert_sequence(two_step_sequence("shutdown-seq", "custom_step"))
        .await
        .expect("upsert");
    let inst = engine
        .create_instance(seq_id, CreateInstanceOptions::default())
        .await
        .expect("create");

    // Let it finish, then shut down; must not hang.
    wait_for_state(&engine, inst, Duration::from_secs(10), |s| {
        s == InstanceState::Completed
    })
    .await;

    tokio::time::timeout(Duration::from_secs(30), engine.shutdown())
        .await
        .expect("shutdown must complete within the grace period");

    // Signalling a terminal instance is rejected with TerminalInstance.
    let err = engine
        .send_signal(inst, SignalType::Cancel, serde_json::json!({}))
        .await
        .expect_err("signal to terminal instance must fail");
    assert!(matches!(err, orch8::Error::TerminalInstance(_)), "{err:?}");

    // Shutdown is idempotent.
    tokio::time::timeout(Duration::from_secs(5), engine.shutdown())
        .await
        .expect("second shutdown returns promptly");
}

/// Facade conveniences: upsert is idempotent per (name, version), missing
/// instances surface `NotFound`, idempotency keys dedupe instance creation,
/// and `list_instances` sees created work.
#[tokio::test]
async fn facade_crud_semantics() {
    let engine = build_engine().await;

    let seq = two_step_sequence("crud-seq", "custom_step");
    let first_id = seq.id;
    let seq_id = engine.upsert_sequence(seq).await.expect("upsert");
    assert_eq!(seq_id, first_id);

    // Re-registering the same (name, version) returns the existing id.
    let again = engine
        .upsert_sequence(two_step_sequence("crud-seq", "custom_step"))
        .await
        .expect("second upsert");
    assert_eq!(again, first_id);

    // Unknown instance -> NotFound.
    let missing = engine.get_instance(InstanceId::new()).await;
    assert!(matches!(missing, Err(orch8::Error::NotFound(_))));

    // Idempotency key dedupes.
    let opts = CreateInstanceOptions {
        idempotency_key: Some("order-42".to_string()),
        ..Default::default()
    };
    let a = engine
        .create_instance(seq_id, opts.clone())
        .await
        .expect("create a");
    let b = engine
        .create_instance(seq_id, opts)
        .await
        .expect("create b");
    assert_eq!(a, b, "same idempotency key must return the same instance");

    let all = engine
        .list_instances(&orch8::InstanceFilter::default())
        .await
        .expect("list");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, a);
    assert_eq!(all[0].tenant_id, *engine.tenant());
}
