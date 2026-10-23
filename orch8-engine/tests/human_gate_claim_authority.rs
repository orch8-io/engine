//! Human gate persistence must retain the evaluation's original claim.
use chrono::Utc;
use orch8_engine::testing::TestEngine;
use orch8_storage::{
    StorageBackend,
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{SequenceBuilder, step},
    },
};
use orch8_types::{
    ids::{BlockId, InstanceId},
    instance::{InstanceState, TaskInstance},
    output::BlockOutput,
    sequence::BlockDefinition,
    signal::{Signal, SignalType},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Clone, Copy)]
enum Boundary {
    Acceptance,
    Context,
    Delivery,
}

async fn replace_gate_owner(store: &dyn StorageBackend, id: InstanceId) -> TaskInstance {
    let old = store.get_instance(id).await.unwrap().unwrap();
    let token = old.claimed_at.unwrap();
    let next = Utc::now() + chrono::Duration::seconds(2);
    assert!(
        store
            .conditional_update_claimed_instance_state(
                id,
                token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(next)
            )
            .await
            .unwrap()
    );
    let mut replacement = store
        .claim_due_instances(next, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(replacement.id, id);
    let current = replacement.claimed_at.unwrap();
    assert_ne!(current, token);
    // The replacement accepted the gate, completed its handler, and advanced.
    assert!(
        store
            .save_block_output_claimed(
                &BlockOutput {
                    id: uuid::Uuid::now_v7(),
                    instance_id: id,
                    block_id: BlockId::new("work"),
                    output: json!({"owner":"replacement"}),
                    output_ref: None,
                    output_size: 23,
                    attempt: 0,
                    created_at: Utc::now(),
                },
                current
            )
            .await
            .unwrap()
    );
    replacement.context.data = json!({"owner":"replacement", "work":{"processed":true}});
    replacement.context.runtime.current_step = Some(BlockId::new("next"));
    replacement.context.runtime.current_step_started_at = Some(next);
    assert_eq!(
        store
            .update_instance_context_cas_claimed(
                id,
                &replacement.context,
                replacement.updated_at,
                current
            )
            .await
            .unwrap(),
        Some(true)
    );
    store.get_instance(id).await.unwrap().unwrap()
}

async fn gate_handoff(boundary: Boundary) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let (ordinary, claimed) = match boundary {
        Boundary::Acceptance => ("save_block_output", "save_block_output_claimed"),
        Boundary::Context => ("merge_context_data", "merge_context_data_claimed"),
        Boundary::Delivery => ("mark_signal_delivered", "mark_signal_delivered_claimed"),
    };
    let pause = faults.on(ordinary).times(1).pause();
    let guarded = faults.on(claimed).times(1).pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let engine = Arc::new(
        TestEngine::builder()
            .storage(storage)
            .register("noop", move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Ok(json!({})) }
            })
            .build()
            .await,
    );
    let mut work = step("work", "noop");
    work.wait_for_input = Some(serde_json::from_value(json!({"prompt":"approve"})).unwrap());
    let sequence = SequenceBuilder::new("gate-handoff")
        .block(BlockDefinition::Step(Box::new(work)))
        .step("next", "noop")
        .build();
    let id = engine.start(&sequence).await;
    inner
        .enqueue_signal(&Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: id,
            signal_type: SignalType::Custom("human_input:work".into()),
            payload: json!({"value":"yes"}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    let old_engine = Arc::clone(&engine);
    let mut old_tick = tokio::spawn(async move { old_engine.try_tick().await });
    tokio::select! {
        () = pause.entered() => {},
        () = guarded.entered() => {},
        result = &mut old_tick => panic!("tick ended before gate persistence: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("gate did not reach persistence"),
    }
    let before = replace_gate_owner(inner.as_ref(), id).await;
    let outputs = serde_json::to_value(inner.get_all_outputs(id).await.unwrap()).unwrap();
    let signals = serde_json::to_value(inner.get_pending_signals(id).await.unwrap()).unwrap();
    pause.release();
    guarded.release();
    tokio::time::timeout(Duration::from_secs(5), old_tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = inner.get_instance(id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&after.context).unwrap(),
        serde_json::to_value(&before.context).unwrap(),
        "stale gate acceptance overwrote replacement handler context"
    );
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(
        serde_json::to_value(inner.get_all_outputs(id).await.unwrap()).unwrap(),
        outputs,
        "stale gate appended acceptance after replacement completion"
    );
    assert_eq!(
        serde_json::to_value(inner.get_pending_signals(id).await.unwrap()).unwrap(),
        signals,
        "stale gate consumed a signal owned by the replacement execution"
    );
    assert_eq!(after.claimed_at, before.claimed_at);
    assert_eq!(after.state, before.state);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stale_gate_cannot_append_acceptance_after_replacement_completion() {
    gate_handoff(Boundary::Acceptance).await;
}

#[tokio::test]
async fn stale_gate_cannot_merge_a_response_after_claim_replacement() {
    gate_handoff(Boundary::Context).await;
}

#[tokio::test]
async fn stale_gate_cannot_deliver_a_signal_after_claim_replacement() {
    gate_handoff(Boundary::Delivery).await;
}
