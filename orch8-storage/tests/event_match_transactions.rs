//! Consumption, join progress and a sealed notification form one durable write.
use chrono::{Timelike, Utc};
use orch8_storage::{
    backend_test,
    testing::{
        backends::{RawPool, TestBackend},
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    event_correlation::{EventEnvelope, EventStatus, EventWait, JoinMode, WaitStatus},
    ids::InstanceId,
    instance::InstanceState,
    signal::{Signal, SignalType},
};
use serde_json::json;
use sqlx::AssertSqlSafe;

async fn fixture(b: &TestBackend) -> (EventWait, EventEnvelope) {
    let sequence = SequenceBuilder::new("atomic-match")
        .step("work", "noop")
        .build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let parent = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    b.storage.create_instance(&parent).await.unwrap();
    let wait = EventWait {
        id: uuid::Uuid::now_v7(),
        tenant_id: parent.tenant_id.to_string(),
        instance_id: parent.id.into_uuid(),
        block_id: "work".into(),
        event_names: vec!["ready".into()],
        correlation_key: "key".into(),
        join_mode: JoinMode::All,
        status: WaitStatus::Waiting,
        matched_names: vec![],
        matched_event_ids: vec![],
        // Always exercise sub-microsecond input, independently of the host clock.
        created_at: Utc::now().with_nanosecond(123_456_789).unwrap(),
    };
    let wait = b.storage.register_event_wait(&wait).await.unwrap();
    let event = EventEnvelope {
        id: uuid::Uuid::now_v7(),
        tenant_id: wait.tenant_id.clone(),
        event_name: "ready".into(),
        producer_event_id: "producer".into(),
        correlation_key: "key".into(),
        payload: json!({"fact":true}),
        status: EventStatus::Pending,
        consumed_by: None,
        received_at: Utc::now().with_nanosecond(987_654_321).unwrap(),
    };
    b.storage.ingest_event(&event).await.unwrap();
    let event = b.storage.get_event(event.id).await.unwrap().unwrap();
    (wait, event)
}
fn notification(wait: &EventWait) -> Signal {
    Signal {
        id: wait.id,
        instance_id: InstanceId::from_uuid(wait.instance_id),
        signal_type: SignalType::Custom(format!("human_input:{}", wait.block_id)),
        payload: json!({"value":"yes","event_wait":wait.id}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    }
}

#[derive(Clone, Copy)]
enum Failure {
    Consume,
    Wait,
    Notification,
}
impl Failure {
    fn table(self) -> &'static str {
        match self {
            Self::Consume => "event_inbox",
            Self::Wait => "event_waits",
            Self::Notification => "signal_inbox",
        }
    }
    fn operation(self) -> &'static str {
        match self {
            Self::Notification => "BEFORE INSERT",
            _ => "AFTER UPDATE",
        }
    }
}
async fn install_failure(b: &TestBackend, phase: Failure) {
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query(AssertSqlSafe(format!("CREATE TRIGGER reject_match {} ON {} BEGIN SELECT RAISE(ABORT,'simulated match failure'); END",phase.operation(),phase.table()))).execute(pool).await.unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("CREATE FUNCTION reject_match() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='simulated match failure', ERRCODE='55P03'; END $$").execute(pool).await.unwrap();
            sqlx::query(AssertSqlSafe(format!(
                "CREATE TRIGGER reject_match {} ON {} FOR EACH ROW EXECUTE FUNCTION reject_match()",
                phase.operation(),
                phase.table()
            )))
            .execute(pool)
            .await
            .unwrap();
        }
    }
}
async fn clear_failure(b: &TestBackend, phase: Failure) {
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("DROP TRIGGER reject_match")
                .execute(pool)
                .await
                .unwrap();
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query(AssertSqlSafe(format!(
                "DROP TRIGGER reject_match ON {}",
                phase.table()
            )))
            .execute(pool)
            .await
            .unwrap();
        }
    }
}
async fn rollback_then_retry(b: TestBackend, phase: Failure) {
    let (wait, event) = fixture(&b).await;
    let signal = notification(&wait);
    install_failure(&b, phase).await;
    let error = b
        .storage
        .match_event_wait(&wait, Some(event.id), &signal)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("simulated match failure"));
    assert_eq!(error.is_transient(), b.kind.is_postgres());
    assert_eq!(b.storage.get_event(event.id).await.unwrap().unwrap(), event);
    assert_eq!(
        b.storage
            .get_event_wait(signal.instance_id, &wait.block_id)
            .await
            .unwrap()
            .unwrap(),
        wait
    );
    assert!(
        b.storage
            .get_pending_signals(signal.instance_id)
            .await
            .unwrap()
            .is_empty()
    );
    clear_failure(&b, phase).await;
    let matched = b
        .storage
        .match_event_wait(&wait, Some(event.id), &signal)
        .await
        .unwrap()
        .unwrap();
    assert!(matched.consumed);
    assert_eq!(matched.wait.status, WaitStatus::Satisfied);
    assert_eq!(matched.wait.matched_event_ids, vec![event.id]);
    let delivered = b
        .storage
        .get_pending_signals(signal.instance_id)
        .await
        .unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].payload, signal.payload);
    let raw: String = match &b.raw {
        RawPool::Sqlite(pool) => sqlx::query_scalar("SELECT payload FROM signal_inbox")
            .fetch_one(pool)
            .await
            .unwrap(),
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => sqlx::query_scalar("SELECT payload::text FROM signal_inbox")
            .fetch_one(pool)
            .await
            .unwrap(),
    };
    let raw: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if b.kind.is_encrypted() {
        assert!(
            raw.get("value").is_none(),
            "atomic match bypassed signal encryption"
        );
    } else {
        assert_eq!(raw, signal.payload);
    }
    assert!(
        !b.storage
            .match_event_wait(&wait, Some(event.id), &signal)
            .await
            .unwrap()
            .unwrap()
            .consumed
    );
    assert_eq!(
        b.storage
            .get_pending_signals(signal.instance_id)
            .await
            .unwrap()
            .len(),
        1
    );
    b.storage.mark_signal_delivered(signal.id).await.unwrap();
    assert!(
        !b.storage
            .match_event_wait(&wait, Some(event.id), &signal)
            .await
            .unwrap()
            .unwrap()
            .consumed
    );
    assert!(
        b.storage
            .get_pending_signals(signal.instance_id)
            .await
            .unwrap()
            .is_empty()
    );
}
backend_test!(async fn inbox_write_failure_rolls_back_the_whole_match(b) { rollback_then_retry(b,Failure::Consume).await; });
backend_test!(async fn wait_write_failure_rolls_back_the_whole_match(b) { rollback_then_retry(b,Failure::Wait).await; });
backend_test!(async fn notification_write_failure_rolls_back_the_whole_match(b) { rollback_then_retry(b,Failure::Notification).await; });

backend_test!(async fn old_generation_cannot_consume_for_a_replacement_wait(b) {
    let (old,event) = fixture(&b).await;
    let instance = InstanceId::from_uuid(old.instance_id);
    b.storage.delete_event_waits_for_blocks(instance,std::slice::from_ref(&old.block_id)).await.unwrap();
    let mut replacement = old.clone(); replacement.id = uuid::Uuid::now_v7();
    let current = b.storage.register_event_wait(&replacement).await.unwrap();
    assert_ne!(old.id,current.id);
    assert!(b.storage.match_event_wait(&old,Some(event.id),&notification(&old)).await.unwrap().is_none());
    assert_eq!(b.storage.get_event(event.id).await.unwrap().unwrap(),event);
    assert_eq!(b.storage.get_event_wait(instance,&old.block_id).await.unwrap().unwrap(),current);
    assert!(b.storage.get_pending_signals(instance).await.unwrap().is_empty());
});
backend_test!(async fn matching_uses_current_routing_and_preserves_progress_during_refresh(b) {
    let (old,event) = fixture(&b).await;
    let mut changed = old.clone(); changed.id = uuid::Uuid::now_v7(); changed.correlation_key = "changed".into();
    let current = b.storage.register_event_wait(&changed).await.unwrap();
    assert_eq!(current.id,old.id);
    assert!(!b.storage.match_event_wait(&old,Some(event.id),&notification(&old)).await.unwrap().unwrap().consumed);
    assert_eq!(b.storage.get_event(event.id).await.unwrap().unwrap().status,EventStatus::Pending);
    let mut relevant = event.clone(); relevant.id = uuid::Uuid::now_v7(); relevant.producer_event_id = "changed".into(); relevant.correlation_key = "changed".into();
    b.storage.ingest_event(&relevant).await.unwrap();
    let matched = b.storage.match_event_wait(&current,Some(relevant.id),&notification(&current)).await.unwrap().unwrap();
    let again = b.storage.register_event_wait(&old).await.unwrap();
    assert_eq!(again,matched.wait); // stale refresh must not reopen a satisfied join
});
backend_test!(async fn registration_adopts_partial_matches_instead_of_replacing_them(b) {
    let (mut wait,event) = fixture(&b).await;
    wait.event_names.push("second".into());
    let wait = b.storage.register_event_wait(&wait).await.unwrap();
    let matched = b.storage.match_event_wait(&wait,Some(event.id),&notification(&wait)).await.unwrap().unwrap();
    assert_eq!(matched.wait.status,WaitStatus::Waiting);
    let refreshed = b.storage.register_event_wait(&wait).await.unwrap();
    assert_eq!(refreshed.matched_event_ids,vec![event.id]);
    assert_eq!(refreshed.matched_names,vec!["ready".to_owned()]);
});

backend_test!(async fn a_foreign_tenant_candidate_cannot_be_consumed(b) {
    let (wait, mut event) = fixture(&b).await;
    event.id = uuid::Uuid::now_v7(); event.tenant_id = "foreign".into();
    b.storage.ingest_event(&event).await.unwrap();
    let matched = b.storage.match_event_wait(&wait,Some(event.id),&notification(&wait)).await.unwrap().unwrap();
    assert!(!matched.consumed); assert_eq!(matched.wait,wait);
    assert_eq!(b.storage.get_event(event.id).await.unwrap().unwrap().status, EventStatus::Pending);
    assert!(b.storage.get_pending_signals(InstanceId::from_uuid(wait.instance_id)).await.unwrap().is_empty());
});
backend_test!(async fn a_delivered_notification_is_rejected_without_consumption(b) {
    let (wait,event) = fixture(&b).await; let mut signal = notification(&wait); signal.delivered = true;
    assert!(b.storage.match_event_wait(&wait,Some(event.id),&signal).await.is_err());
    assert_eq!(b.storage.get_event(event.id).await.unwrap().unwrap(),event);
    assert_eq!(b.storage.get_event_wait(signal.instance_id,&wait.block_id).await.unwrap().unwrap(),wait);
});
backend_test!(async fn an_unrelated_signal_id_collision_cannot_commit_a_partial_match(b) {
    let (wait,event) = fixture(&b).await; let signal = notification(&wait);
    let mut unrelated = signal.clone(); unrelated.signal_type = SignalType::Custom("unrelated".into());
    b.storage.enqueue_signal(&unrelated).await.unwrap();
    assert!(b.storage.match_event_wait(&wait,Some(event.id),&signal).await.is_err());
    assert_eq!(b.storage.get_event(event.id).await.unwrap().unwrap(),event);
    assert_eq!(b.storage.get_event_wait(signal.instance_id,&wait.block_id).await.unwrap().unwrap(),wait);
    let pending = b.storage.get_pending_signals(signal.instance_id).await.unwrap();
    assert_eq!(pending.len(),1); assert_eq!(pending[0].signal_type,unrelated.signal_type);
});
