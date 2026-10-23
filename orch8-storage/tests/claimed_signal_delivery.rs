//! Signal acknowledgement belongs to the instance's original execution claim.
use chrono::{DateTime, Utc};
use orch8_storage::{
    StorageBackend, backend_test,
    testing::{
        backends::{RawPool, TestBackend},
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    ids::InstanceId,
    instance::{InstanceState, TaskInstance},
    signal::{Signal, SignalType},
};

fn reply(instance_id: InstanceId) -> Signal {
    Signal {
        id: uuid::Uuid::now_v7(),
        instance_id,
        signal_type: SignalType::Custom("human_input:work".into()),
        payload: serde_json::json!({"private_reply":"yes"}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    }
}

async fn claimed_parent(store: &dyn StorageBackend) -> TaskInstance {
    let sequence = SequenceBuilder::new("signal-claim")
        .step("work", "noop")
        .build();
    store.create_sequence(&sequence).await.unwrap();
    let parent = InstanceBuilder::for_sequence(&sequence).due_now().build();
    store.create_instance(&parent).await.unwrap();
    store
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn delivery_time(b: &TestBackend, id: uuid::Uuid) -> Option<DateTime<Utc>> {
    match &b.raw {
        RawPool::Sqlite(pool) => {
            let value: Option<String> =
                sqlx::query_scalar("SELECT delivered_at FROM signal_inbox WHERE id=?1")
                    .bind(id.to_string())
                    .fetch_one(pool)
                    .await
                    .unwrap();
            value.map(|value| {
                DateTime::parse_from_rfc3339(&value)
                    .unwrap()
                    .with_timezone(&Utc)
            })
        }
        #[cfg(feature = "postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query_scalar("SELECT delivered_at FROM signal_inbox WHERE id=$1")
                .bind(id)
                .fetch_one(pool)
                .await
                .unwrap()
        }
    }
}

backend_test!(async fn delivery_rejects_stale_claims_and_keeps_foreign_and_delivered_rows_intact(b) {
    let parent = claimed_parent(b.storage.as_ref()).await;
    let token = parent.claimed_at.unwrap();
    let mut other = parent.clone(); other.id = InstanceId::new(); other.state = InstanceState::Completed;
    other.claimed_at = None; other.next_fire_at = None;
    b.storage.create_instance(&other).await.unwrap();
    let own = reply(parent.id); let foreign = reply(other.id);
    b.storage.enqueue_signal(&own).await.unwrap(); b.storage.enqueue_signal(&foreign).await.unwrap();
    assert!(b.storage.mark_signal_delivered_claimed(parent.id, foreign.id, token).await.unwrap());
    assert!(b.storage.mark_signal_delivered_claimed(parent.id, uuid::Uuid::now_v7(), token).await.unwrap());
    let pending = b.storage.get_pending_signals(other.id).await.unwrap();
    assert_eq!(pending.len(), 1); assert_eq!(pending[0].id, foreign.id);
    assert_eq!(pending[0].payload, foreign.payload);
    assert!(delivery_time(&b, foreign.id).await.is_none());
    assert!(b.storage.mark_signal_delivered_claimed(parent.id, own.id, token).await.unwrap());
    assert!(b.storage.get_pending_signals(parent.id).await.unwrap().is_empty());
    let first = delivery_time(&b, own.id).await;
    assert!(first.is_some());
    assert!(b.storage.mark_signal_delivered_claimed(parent.id, own.id, token).await.unwrap());
    assert_eq!(delivery_time(&b, own.id).await, first);
    let next = Utc::now() + chrono::Duration::seconds(2);
    assert!(b.storage.conditional_update_claimed_instance_state(parent.id, token, InstanceState::Running,
        InstanceState::Scheduled, Some(next)).await.unwrap());
    let fresh = reply(parent.id); b.storage.enqueue_signal(&fresh).await.unwrap();
    assert!(!b.storage.mark_signal_delivered_claimed(parent.id, fresh.id, token).await.unwrap());
    let current = b.storage.claim_due_instances(next, 1, 0).await.unwrap().pop().unwrap();
    assert_eq!(current.id, parent.id); let current_token = current.claimed_at.unwrap();
    assert_ne!(current_token, token);
    assert!(!b.storage.mark_signal_delivered_claimed(parent.id, fresh.id, token).await.unwrap());
    assert!(!b.storage.mark_signal_delivered_claimed(parent.id, own.id, token).await.unwrap());
    assert_eq!(delivery_time(&b, own.id).await, first);
    assert!(delivery_time(&b, fresh.id).await.is_none());
    assert_eq!(b.storage.get_pending_signals(parent.id).await.unwrap()[0].id, fresh.id);
    assert!(b.storage.mark_signal_delivered_claimed(parent.id, fresh.id, current_token).await.unwrap());
    assert!(b.storage.get_pending_signals(parent.id).await.unwrap().is_empty());
    assert_eq!(b.storage.get_pending_signals(other.id).await.unwrap()[0].id, foreign.id);
});

backend_test!(async fn a_delivery_error_preserves_pending_signal_and_claim_for_retry(b) {
    let parent = claimed_parent(b.storage.as_ref()).await;
    let token = parent.claimed_at.unwrap();
    let signal = reply(parent.id); b.storage.enqueue_signal(&signal).await.unwrap();
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("CREATE TRIGGER reject_delivery AFTER UPDATE ON signal_inbox BEGIN SELECT RAISE(ABORT,'simulated signal delivery failure'); END").execute(pool).await.unwrap();
        }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => {
            sqlx::query("CREATE FUNCTION reject_delivery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='simulated signal delivery failure', ERRCODE='55P03'; END $$").execute(pool).await.unwrap();
            sqlx::query("CREATE TRIGGER reject_delivery AFTER UPDATE ON signal_inbox FOR EACH ROW EXECUTE FUNCTION reject_delivery()").execute(pool).await.unwrap();
        }
    }
    let error = b.storage.mark_signal_delivered_claimed(parent.id, signal.id, token).await.unwrap_err();
    assert!(error.to_string().contains("simulated signal delivery failure"));
    assert_eq!(error.is_transient(), b.kind.is_postgres());
    assert!(delivery_time(&b, signal.id).await.is_none());
    let pending = b.storage.get_pending_signals(parent.id).await.unwrap();
    assert_eq!(pending.len(), 1); assert_eq!(pending[0].payload, signal.payload);
    assert_eq!(b.storage.get_instance(parent.id).await.unwrap().unwrap().claimed_at, Some(token));
    match &b.raw {
        RawPool::Sqlite(pool) => { sqlx::query("DROP TRIGGER reject_delivery").execute(pool).await.unwrap(); }
        #[cfg(feature="postgres")]
        RawPool::Postgres(pool) => { sqlx::query("DROP TRIGGER reject_delivery ON signal_inbox").execute(pool).await.unwrap(); }
    }
    assert!(b.storage.mark_signal_delivered_claimed(parent.id, signal.id, token).await.unwrap());
    assert!(b.storage.get_pending_signals(parent.id).await.unwrap().is_empty());
    assert!(delivery_time(&b, signal.id).await.is_some());
});
