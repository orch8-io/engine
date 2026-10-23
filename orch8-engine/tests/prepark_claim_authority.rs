//! Gate registration must retain the scheduler's original execution claim.
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
use orch8_types::{instance::InstanceState, sequence::BlockDefinition};
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Copy)]
enum Registration {
    EventWait,
    Federation,
}

async fn handoff_before_registration(kind: Registration) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let (handler, method) = match kind {
        Registration::EventWait => ("wait_for_event", "upsert_event_wait_claimed"),
        Registration::Federation => ("federate", "create_federation_call_claimed"),
    };
    let pause = faults.on(method).times(1).pause();
    let engine = Arc::new(TestEngine::builder().storage(storage).build().await);
    let mut work = step("work", handler);
    work.params = match kind {
        Registration::EventWait => json!({"events":"old-event", "correlation_key":"old-key"}),
        // An invalid old registration is still durable and can poison the
        // deterministic call identity before a replacement resolves its input.
        Registration::Federation => json!({}),
    };
    work.wait_for_input = Some(serde_json::from_value(json!({"prompt":"wait"})).unwrap());
    let sequence = SequenceBuilder::new("prepark-handoff")
        .block(BlockDefinition::Step(Box::new(work)))
        .build();
    let id = engine.start(&sequence).await;
    let old_engine = Arc::clone(&engine);
    let mut old_tick = tokio::spawn(async move { old_engine.try_tick().await });
    tokio::select! {
        () = pause.entered() => {},
        result = &mut old_tick => panic!("tick ended before registration: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("gate did not reach durable registration"),
    }
    let old = inner.get_instance(id).await.unwrap().unwrap();
    let token = old.claimed_at.unwrap();
    let next = Utc::now() + chrono::Duration::seconds(2);
    assert!(
        inner
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
    let replacement = inner
        .claim_due_instances(next, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(replacement.id, id);
    assert_ne!(replacement.claimed_at, Some(token));
    let current_wait = if matches!(kind, Registration::EventWait) {
        let wait = orch8_engine::event_correlation::parse_wait_params(
            &json!({"events":"current-event","correlation_key":"current-key"}),
            replacement.tenant_id.as_str(),
            id.into_uuid(),
            "work",
        )
        .unwrap();
        inner.upsert_event_wait(&wait).await.unwrap();
        Some(serde_json::to_value(inner.get_event_wait(id, "work").await.unwrap()).unwrap())
    } else {
        None
    };
    let before = inner.get_instance(id).await.unwrap().unwrap();
    pause.release();
    tokio::time::timeout(Duration::from_secs(5), old_tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match current_wait {
        Some(expected) => assert_eq!(
            serde_json::to_value(inner.get_event_wait(id, "work").await.unwrap()).unwrap(),
            expected,
            "stale gate overwrote replacement event registration"
        ),
        None => assert!(
            inner
                .list_due_federation_calls(next + chrono::Duration::hours(1), 100)
                .await
                .unwrap()
                .is_empty(),
            "stale gate registered a federation call after losing its claim"
        ),
    }
    let after = inner.get_instance(id).await.unwrap().unwrap();
    assert_eq!(after.claimed_at, before.claimed_at);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.state, before.state);
}

#[tokio::test]
async fn stale_gate_cannot_replace_current_event_registration() {
    handoff_before_registration(Registration::EventWait).await;
}
#[tokio::test]
async fn stale_gate_cannot_register_an_outbound_federation_call() {
    handoff_before_registration(Registration::Federation).await;
}

#[tokio::test]
async fn transient_peer_lookup_cannot_poison_a_federation_call_identity() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    faults.on("list_federation_peers").times(1).fail_transient();
    let engine = TestEngine::builder().storage(storage).build().await;
    let mut work = step("work", "federate");
    work.params = json!({"peer":"peer","sequence":"remote","input":{}});
    work.wait_for_input =
        Some(serde_json::from_value(json!({"prompt":"wait for remote"})).unwrap());
    let sequence = SequenceBuilder::new("federation-peer-retry")
        .block(BlockDefinition::Step(Box::new(work)))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert!(
        inner
            .list_due_federation_calls(Utc::now() + chrono::Duration::hours(1), 100)
            .await
            .unwrap()
            .is_empty(),
        "transient peer lookup was persisted as a permanently failed call"
    );
    assert_eq!(
        inner.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Scheduled,
        "transient peer lookup did not retain infrastructure retry semantics"
    );
}
