//! Each loop activation of an event gate must listen for a fresh event.
use orch8_engine::{
    event_correlation::{envelope, ingest},
    testing::TestEngine,
};
use orch8_storage::testing::fixtures::{SequenceBuilder, step};
use orch8_types::{
    event_correlation::WaitStatus,
    ids::BlockId,
    instance::InstanceState,
    sequence::{BlockDefinition, LoopDef},
};
use serde_json::json;
use std::time::Duration;

fn event_gate() -> orch8_types::sequence::StepDef {
    let mut gate = step("event_gate", "wait_for_event");
    gate.params = json!({"events":"ready","correlation_key":"job-key"});
    gate.wait_for_input = Some(serde_json::from_value(json!({"prompt":"wait for ready"})).unwrap());
    gate
}

async fn dispatch_after_signal(engine: &TestEngine) {
    engine.advance(Duration::from_secs(1));
    // tick_once sweeps parked signals after its claim/dispatch phase.
    engine.tick().await;
    engine.tick().await;
}

fn event_reply(
    instance_id: orch8_types::ids::InstanceId,
    wait_id: uuid::Uuid,
) -> orch8_types::signal::Signal {
    orch8_types::signal::Signal {
        id: uuid::Uuid::now_v7(),
        instance_id,
        signal_type: orch8_types::signal::SignalType::Custom("human_input:event_gate".into()),
        payload: json!({"value":"yes", "event_wait":wait_id}),
        delivered: false,
        created_at: chrono::Utc::now(),
        delivered_at: None,
    }
}

#[tokio::test]
async fn repeated_event_gate_rearms_with_a_new_registration_and_consumes_each_event_once() {
    let engine = TestEngine::builder().build().await;
    let gate = event_gate();
    let sequence = SequenceBuilder::new("repeated-event-gate")
        .block(BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("loop"),
            condition: "true".into(),
            body: vec![BlockDefinition::Step(Box::new(gate))],
            max_iterations: 2,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        })))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    let first = engine
        .storage
        .get_event_wait(id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, WaitStatus::Waiting);
    let event = envelope(
        sequence.tenant_id.as_str(),
        "ready",
        "first",
        "job-key",
        json!({"ordinal":1}),
    );
    let outcome = ingest(&engine.storage, event).await.unwrap();
    assert!(outcome.satisfied);
    dispatch_after_signal(&engine).await;
    let current = engine
        .storage
        .get_event_wait(id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.status,
        WaitStatus::Waiting,
        "next iteration retained the previous satisfied event registration"
    );
    assert_ne!(
        current.id, first.id,
        "a new activation must not accept an old matcher's CAS"
    );
    assert!(current.matched_event_ids.is_empty());
    assert!(current.matched_names.is_empty());
    engine
        .storage
        .enqueue_signal(&event_reply(id, first.id))
        .await
        .unwrap();
    dispatch_after_signal(&engine).await;
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Waiting,
        "a delayed notification from the previous activation opened the new gate"
    );
    assert_eq!(
        engine
            .storage
            .get_event_wait(id, "event_gate")
            .await
            .unwrap(),
        Some(current),
        "the stale notification changed the current registration"
    );
    let event = envelope(
        sequence.tenant_id.as_str(),
        "ready",
        "second",
        "job-key",
        json!({"ordinal":2}),
    );
    let outcome = ingest(&engine.storage, event).await.unwrap();
    assert!(
        outcome.satisfied,
        "fresh event did not reach the next loop activation"
    );
    dispatch_after_signal(&engine).await;
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
}

#[tokio::test]
async fn retrying_a_persisted_event_after_match_read_failure_resumes_the_wait_once() {
    use orch8_storage::{StorageBackend, sqlite::SqliteStorage, testing::faulty::FaultStorage};
    use std::sync::Arc;
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let engine = TestEngine::builder().storage(storage).build().await;
    let gate = event_gate();
    let sequence = SequenceBuilder::new("event-ingest-retry")
        .block(BlockDefinition::Step(Box::new(gate)))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    let wait = inner
        .get_event_wait(id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    faults
        .on("find_waiting_event_waits")
        .times(1)
        .fail_transient();
    let original = envelope(
        sequence.tenant_id.as_str(),
        "ready",
        "stable-producer-id",
        "job-key",
        json!({"original":true}),
    );
    let error = ingest(&engine.storage, original.clone()).await.unwrap_err();
    assert!(error.is_transient());
    // A producer retry may have a new transport UUID and different payload.
    // Deduplication must retry matching the persisted canonical event.
    let mut retry = original.clone();
    retry.id = uuid::Uuid::now_v7();
    retry.payload = json!({"retry":true});
    let outcome = ingest(&engine.storage, retry.clone()).await.unwrap();
    assert!(outcome.duplicate);
    assert!(
        outcome.satisfied,
        "persisted pending event was abandoned after a transient match read failure"
    );
    // Receipts retain the submitted transport ID; durable matches use the
    // canonical original ID, as asserted below.
    assert_eq!(outcome.event_id, retry.id);
    assert_eq!(outcome.matched_wait, Some(wait.id));
    let current = inner
        .get_event_wait(id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.matched_event_ids, vec![original.id]);
    assert_eq!(inner.get_pending_signals(id).await.unwrap().len(), 1);
    let again = ingest(&engine.storage, retry).await.unwrap();
    assert!(again.duplicate);
    assert!(
        !again.satisfied,
        "an already-consumed event satisfied a wait twice"
    );
    assert_eq!(inner.get_pending_signals(id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn transient_prepark_registration_failure_retries_instead_of_parking_without_a_listener() {
    use orch8_storage::{StorageBackend, sqlite::SqliteStorage, testing::faulty::FaultStorage};
    use std::sync::Arc;
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    faults
        .on("upsert_event_wait_claimed")
        .times(1)
        .fail_transient();
    let engine = TestEngine::builder().storage(storage).build().await;
    let gate = event_gate();
    let sequence = SequenceBuilder::new("event-registration-retry")
        .block(BlockDefinition::Step(Box::new(gate)))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert_eq!(
        inner.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Scheduled,
        "a transient registration outage parked the workflow without any event listener"
    );
    assert!(
        inner
            .get_event_wait(id, "event_gate")
            .await
            .unwrap()
            .is_none()
    );
    engine.advance(Duration::from_secs(60));
    engine.tick().await;
    assert_eq!(
        inner.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Waiting
    );
    assert_eq!(
        inner
            .get_event_wait(id, "event_gate")
            .await
            .unwrap()
            .unwrap()
            .status,
        WaitStatus::Waiting
    );
}
