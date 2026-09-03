//! Integration tests for the embeddable `orch8` facade: builder, background
//! tick loop, manual ticking, signals, and graceful shutdown — all through
//! the public API only.

use std::time::Duration;

use orch8::{
    AgentRuntime, CapsuleExportOptions, CapsuleRequirements, CapsuleSigningKey,
    CreateInstanceOptions, EffectContext, Engine, FieldEncryptor, InstanceId, InstanceState,
    RuntimeId, SignalType, StepContext, Storage, run_sequence_once,
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

#[tokio::test]
async fn one_shot_runner_uses_the_embedded_engine() {
    let result = run_sequence_once(
        two_step_sequence("one-shot", "noop"),
        serde_json::json!({"request": "native-host"}),
        100,
    )
    .await
    .expect("one-shot execution");

    assert_eq!(result.state, InstanceState::Completed);
    assert_eq!(result.context.data["request"], "native-host");
    assert_eq!(result.outputs.len(), 2);
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

/// Effect handlers receive the exact durable dispatch identity before their
/// external call, and successful provider evidence is committed to the
/// instance effect ledger.
#[tokio::test]
async fn effect_handler_exposes_and_commits_durable_receipt() {
    let engine = Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .effect_handler("charge", |ctx: EffectContext| async move {
            let dispatch_key = ctx
                .dispatch_idempotency_key()
                .expect("live effect has dispatch identity");
            let receipt_id = ctx
                .receipt()
                .expect("live effect has receipt")
                .id
                .to_string();
            assert_eq!(dispatch_key, receipt_id);
            Ok(serde_json::json!({
                "dispatch_key": dispatch_key,
                "provider_receipt_id": "provider-charge-42"
            }))
        })
        .build()
        .await
        .expect("engine builds");

    let seq_id = engine
        .upsert_sequence(two_step_sequence("effect-seq", "charge"))
        .await
        .expect("upsert");
    let inst = engine
        .create_instance(seq_id, CreateInstanceOptions::default())
        .await
        .expect("create");

    for _ in 0..100 {
        engine.tick_once().await.expect("tick");
        if engine.get_instance(inst).await.expect("get").state == InstanceState::Completed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        engine.get_instance(inst).await.expect("get").state,
        InstanceState::Completed
    );

    let receipts = engine.effect_receipts(inst).await.expect("effect ledger");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].state, orch8::EffectState::Committed);
    assert_eq!(
        receipts[0].provider_receipt_id.as_deref(),
        Some("provider-charge-42")
    );
}

/// Dry runs preserve handler control flow but never mint a dispatch identity,
/// making an accidental provider call straightforward to reject in user code.
#[tokio::test]
async fn effect_handler_dry_run_has_no_dispatch_identity() {
    let engine = Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .effect_handler("charge", |ctx: EffectContext| async move {
            assert!(ctx.is_dry_run());
            assert!(ctx.dispatch_idempotency_key().is_none());
            assert!(ctx.receipt().is_none());
            Ok(serde_json::json!({ "dry_run": true }))
        })
        .build()
        .await
        .expect("engine builds");
    let seq_id = engine
        .upsert_sequence(two_step_sequence("effect-dry-run", "charge"))
        .await
        .expect("upsert");
    let mut options = CreateInstanceOptions::default();
    options.context.runtime.dry_run = true;
    let inst = engine
        .create_instance(seq_id, options)
        .await
        .expect("create");

    for _ in 0..100 {
        engine.tick_once().await.expect("tick");
        if engine.get_instance(inst).await.expect("get").state == InstanceState::Completed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        engine.get_instance(inst).await.expect("get").state,
        InstanceState::Completed
    );
    assert!(
        engine
            .effect_receipts(inst)
            .await
            .expect("ledger")
            .is_empty()
    );
}

async fn portable_fixture() -> (Engine, Engine, InstanceId) {
    let source = Engine::builder()
        .storage(Storage::sqlite_in_memory().artifacts_in_memory())
        .build()
        .await
        .expect("source engine");
    let destination = Engine::builder()
        .storage(Storage::sqlite_in_memory().artifacts_in_memory())
        .build()
        .await
        .expect("destination engine");
    let sequence: orch8::SequenceDefinition = serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": "portable-facade",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "approval",
            "handler": "noop",
            "params": {},
            "wait_for_input": { "prompt": "continue?" }
        }],
        "created_at": chrono::Utc::now().to_rfc3339()
    }))
    .expect("sequence");
    let sequence_id = source
        .upsert_sequence(sequence.clone())
        .await
        .expect("source sequence");
    destination
        .upsert_sequence(sequence)
        .await
        .expect("destination sequence");
    let source_instance = source
        .create_instance(sequence_id, CreateInstanceOptions::default())
        .await
        .expect("source instance");
    for _ in 0..20 {
        source.tick_once().await.expect("source tick");
        if source
            .get_instance(source_instance)
            .await
            .expect("source snapshot")
            .state
            == InstanceState::Waiting
        {
            break;
        }
    }
    assert_eq!(
        source
            .get_instance(source_instance)
            .await
            .expect("source snapshot")
            .state,
        InstanceState::Waiting
    );
    source
        .portable_checkpoint(
            source_instance,
            serde_json::json!({"safe_boundary": "awaiting_approval"}),
        )
        .await
        .expect("checkpoint");

    (source, destination, source_instance)
}

async fn export_test_capsule(
    source: &Engine,
    source_instance: InstanceId,
) -> (orch8::PortableCapsule, RuntimeId, FieldEncryptor) {
    let source_runtime = RuntimeId::new();
    let destination_runtime = RuntimeId::new();
    let signing_key = CapsuleSigningKey::from_bytes(&[7_u8; 32]);
    let encryptor = FieldEncryptor::from_bytes(&[9_u8; 32]);
    let capsule = source
        .export_portable_capsule(
            source_instance,
            CapsuleExportOptions {
                source_runtime_id: source_runtime,
                destination_runtime_id: Some(destination_runtime),
                requirements: CapsuleRequirements::default(),
                expires_in_seconds: 300,
                signing_key_id: "test-signing-key".into(),
                encryption_key_id: "test-payload-key".into(),
            },
            &signing_key,
            &encryptor,
        )
        .await
        .expect("capsule export");
    (capsule, destination_runtime, encryptor)
}

async fn assert_capsule_rejections(
    destination: &Engine,
    capsule: &orch8::PortableCapsule,
    destination_runtime: RuntimeId,
    trusted_keys: &[String],
    encryptor: &FieldEncryptor,
) {
    let wrong_tenant = Engine::builder()
        .storage(Storage::sqlite_in_memory().artifacts_in_memory())
        .tenant("other")
        .build()
        .await
        .expect("wrong-tenant engine");
    assert!(matches!(
        wrong_tenant
            .import_portable_capsule(capsule, destination_runtime, None, trusted_keys, encryptor,)
            .await,
        Err(orch8::Error::NotFound(_))
    ));

    let mut tampered = capsule.clone();
    tampered.encrypted_payload[0] ^= 1;
    assert!(
        destination
            .import_portable_capsule(
                &tampered,
                destination_runtime,
                None,
                trusted_keys,
                encryptor,
            )
            .await
            .is_err()
    );
}

/// The facade moves a bounded checkpoint between isolated stores while
/// enforcing signature, payload integrity, tenant, destination, and
/// idempotent-redelivery rules.
#[tokio::test]
async fn portable_capsule_round_trip_is_verified_and_idempotent() {
    let (source, destination, source_instance) = portable_fixture().await;
    let (capsule, destination_runtime, encryptor) =
        export_test_capsule(&source, source_instance).await;
    let trusted_keys = [capsule.signed_manifest.public_key.clone()];
    assert_capsule_rejections(
        &destination,
        &capsule,
        destination_runtime,
        &trusted_keys,
        &encryptor,
    )
    .await;

    let imported = destination
        .import_portable_capsule(
            &capsule,
            destination_runtime,
            None,
            &trusted_keys,
            &encryptor,
        )
        .await
        .expect("capsule import");
    let redelivered = destination
        .import_portable_capsule(
            &capsule,
            destination_runtime,
            Some(imported.id),
            &trusted_keys,
            &encryptor,
        )
        .await
        .expect("idempotent capsule redelivery");
    assert_eq!(redelivered.id, imported.id);
    assert_eq!(redelivered.state, InstanceState::Paused);
}

/// The agent preset includes the native durable agent stack and can execute a
/// no-network dry run through the same Engine API exposed by `Deref`.
#[tokio::test]
async fn agent_runtime_preset_executes_bounded_dry_run() {
    let runtime_id = RuntimeId::new();
    let runtime = AgentRuntime::builder(Storage::sqlite_in_memory())
        .runtime_id(runtime_id)
        .build()
        .await
        .expect("agent runtime");
    assert_eq!(runtime.runtime_id(), runtime_id);
    let sequence: orch8::SequenceDefinition = serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": "agent-runtime-preset",
        "version": 1,
        "blocks": [{
            "type": "step",
            "id": "agent",
            "handler": "agent",
            "params": {"goal": "do not call a provider", "max_iterations": 1}
        }],
        "created_at": chrono::Utc::now().to_rfc3339()
    }))
    .expect("sequence");
    let sequence_id = runtime.upsert_sequence(sequence).await.expect("upsert");
    let mut options = CreateInstanceOptions::default();
    options.context.runtime.dry_run = true;
    let instance_id = runtime
        .create_instance(sequence_id, options)
        .await
        .expect("instance");

    for _ in 0..20 {
        runtime.tick_once().await.expect("tick");
        if runtime
            .get_instance(instance_id)
            .await
            .expect("snapshot")
            .state
            == InstanceState::Completed
        {
            break;
        }
    }
    let outputs = runtime.block_outputs(instance_id).await.expect("outputs");
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].output["stop_reason"], "dry_run");
    assert_eq!(outputs[0].output["iterations"], 0);
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

/// Regression (audit M-3): an empty-string idempotency key is normalized to
/// `None` — two creates with `Some("")` must both succeed with distinct ids
/// instead of the second hitting the unique index on `""`.
#[tokio::test]
async fn empty_idempotency_key_is_not_deduped() {
    let engine = build_engine().await;
    let seq_id = engine
        .upsert_sequence(two_step_sequence("empty-key-seq", "custom_step"))
        .await
        .expect("upsert");

    let opts = CreateInstanceOptions {
        idempotency_key: Some(String::new()),
        ..Default::default()
    };
    let a = engine
        .create_instance(seq_id, opts.clone())
        .await
        .expect("create a");
    let b = engine
        .create_instance(seq_id, opts)
        .await
        .expect("create b with an empty key must not hit the unique index");
    assert_ne!(a, b, "empty key opts out of idempotency");
}

/// Regression (audit H-2): concurrent creates with the same idempotency key
/// race the check-then-insert; the loser must resolve to the winner's id via
/// the unique-index conflict path, not fail with a raw storage error.
#[tokio::test]
async fn concurrent_create_with_same_idempotency_key_dedupes() {
    const TASKS: usize = 16;

    let engine = build_engine().await;
    let seq_id = engine
        .upsert_sequence(two_step_sequence("race-seq", "custom_step"))
        .await
        .expect("upsert");

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(TASKS));
    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let engine = engine.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            engine
                .create_instance(
                    seq_id,
                    CreateInstanceOptions {
                        idempotency_key: Some("shared-key".to_string()),
                        ..Default::default()
                    },
                )
                .await
        }));
    }

    let mut ids = std::collections::HashSet::new();
    for handle in handles {
        let id = handle
            .await
            .expect("task panicked")
            .expect("concurrent create must not fail");
        ids.insert(id.to_string());
    }
    assert_eq!(ids.len(), 1, "all racing creates must return the same id");

    let all = engine
        .list_instances(&orch8::InstanceFilter::default())
        .await
        .expect("list");
    assert_eq!(all.len(), 1, "exactly one instance must be stored");
}
