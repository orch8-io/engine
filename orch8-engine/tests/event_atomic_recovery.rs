//! Real write failures must not strand consumed facts or satisfaction delivery.
use orch8_engine::{
    event_correlation::{envelope, ingest},
    testing::TestEngine,
};
use orch8_storage::{
    InstanceStore, OutputStore, SignalStore, StorageBackend,
    sqlite::SqliteStorage,
    testing::{
        faulty::{FaultHandle, FaultStorage},
        fixtures::{SequenceBuilder, step},
    },
};
use orch8_types::{
    error::StorageError,
    event_correlation::{EventEnvelope, EventStatus, WaitStatus},
    ids::InstanceId,
    instance::InstanceState,
    sequence::BlockDefinition,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

struct Fixture {
    inner: Arc<SqliteStorage>,
    engine: Arc<TestEngine>,
    faults: FaultHandle,
    id: InstanceId,
    original: EventEnvelope,
}
impl Fixture {
    async fn new() -> Self {
        let inner = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let backend: Arc<dyn StorageBackend> = inner.clone();
        let (storage, faults) = FaultStorage::wrap(backend);
        let engine = Arc::new(TestEngine::builder().storage(storage).build().await);
        let mut gate = step("event_gate", "wait_for_event");
        gate.params = json!({"events":"ready","correlation_key":"job-key"});
        gate.wait_for_input = Some(serde_json::from_value(json!({"prompt":"wait"})).unwrap());
        let sequence = SequenceBuilder::new("atomic-event")
            .block(BlockDefinition::Step(Box::new(gate)))
            .build();
        let id = engine.start(&sequence).await;
        engine.tick().await;
        let original = envelope(
            sequence.tenant_id.as_str(),
            "ready",
            "producer",
            "job-key",
            json!({"original":true}),
        );
        Self {
            inner,
            engine,
            faults,
            id,
            original,
        }
    }
    async fn retry_and_complete(&self) {
        let mut retry = self.original.clone();
        retry.id = uuid::Uuid::now_v7();
        retry.payload = json!({"retry":true});
        let outcome = ingest(&self.engine.storage, retry.clone()).await.unwrap();
        assert!(outcome.duplicate);
        assert_eq!(outcome.event_id, retry.id);
        let current = self
            .inner
            .get_event_wait(self.id, "event_gate")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, WaitStatus::Satisfied);
        assert_eq!(current.matched_event_ids, vec![self.original.id]);
        assert_eq!(
            self.inner.get_pending_signals(self.id).await.unwrap().len(),
            1
        );
        assert!(!ingest(&self.engine.storage, retry).await.unwrap().satisfied);
        assert_eq!(
            self.inner.get_pending_signals(self.id).await.unwrap().len(),
            1
        );
        self.engine.advance(Duration::from_secs(1));
        self.engine.tick().await;
        self.engine.tick().await;
        assert_eq!(
            self.inner
                .get_instance(self.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Completed
        );
        let output = self
            .inner
            .get_block_output(self.id, &orch8_types::ids::BlockId::new("event_gate"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            output.output["events"]["ready"]["payload"],
            self.original.payload
        );
    }
}
async fn rollback_and_retry(trigger: &'static str) {
    let f = Fixture::new().await;
    sqlx::query(trigger).execute(f.inner.pool()).await.unwrap();
    ingest(&f.engine.storage, f.original.clone())
        .await
        .expect_err("native write fault was not exercised");
    // These are the partial states the former multi-write protocol stranded.
    assert_eq!(
        f.inner
            .get_event(f.original.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        EventStatus::Pending
    );
    let wait = f
        .inner
        .get_event_wait(f.id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(wait.status, WaitStatus::Waiting);
    assert!(wait.matched_event_ids.is_empty());
    assert!(f.inner.get_pending_signals(f.id).await.unwrap().is_empty());
    sqlx::query("DROP TRIGGER reject_match")
        .execute(f.inner.pool())
        .await
        .unwrap();
    f.retry_and_complete().await;
}
#[tokio::test]
async fn retry_after_consumption_write_failure_keeps_the_event_recoverable() {
    rollback_and_retry("CREATE TRIGGER reject_match AFTER UPDATE ON event_waits BEGIN SELECT RAISE(ABORT,'simulated match persistence failure'); END").await;
}
#[tokio::test]
async fn retry_after_notification_failure_delivers_the_satisfied_wait() {
    rollback_and_retry("CREATE TRIGGER reject_match BEFORE INSERT ON signal_inbox BEGIN SELECT RAISE(ABORT,'simulated notification failure'); END").await;
}
#[tokio::test]
async fn retry_after_commit_acknowledgement_loss_does_not_notify_twice() {
    let f = Fixture::new().await;
    f.faults
        .on("match_event_wait")
        .times(1)
        .fail_after_commit(StorageError::Connection(
            "lost commit acknowledgement".into(),
        ));
    let error = ingest(&f.engine.storage, f.original.clone())
        .await
        .unwrap_err();
    assert!(error.is_transient());
    f.retry_and_complete().await;
}
#[tokio::test]
async fn retry_after_transient_atomic_match_failure_resumes_once() {
    let f = Fixture::new().await;
    f.faults.on("match_event_wait").times(1).fail_transient();
    assert!(
        ingest(&f.engine.storage, f.original.clone())
            .await
            .unwrap_err()
            .is_transient()
    );
    f.retry_and_complete().await;
}
#[tokio::test]
async fn a_stale_concurrent_duplicate_does_not_report_a_second_match() {
    let f = Fixture::new().await;
    let pause = f.faults.on("match_event_wait").times(1).pause();
    let engine = Arc::clone(&f.engine);
    let event = f.original.clone();
    let first = tokio::spawn(async move { ingest(&engine.storage, event).await });
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    let mut duplicate = f.original.clone();
    duplicate.id = uuid::Uuid::now_v7();
    let second = ingest(&f.engine.storage, duplicate).await.unwrap();
    pause.release();
    let first = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        usize::from(first.matched_wait.is_some()) + usize::from(second.matched_wait.is_some()),
        1
    );
    assert_eq!(f.inner.get_pending_signals(f.id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn repark_registration_cannot_overwrite_a_concurrent_partial_match() {
    use orch8_engine::handlers::{StepContext, wait_for_event::handle_wait_for_event};
    use orch8_types::ids::BlockId;
    let f = Fixture::new().await;
    let mut wait = f
        .inner
        .get_event_wait(f.id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    wait.event_names.push("second".into());
    f.inner.upsert_event_wait(&wait).await.unwrap();
    f.inner
        .update_instance_state(f.id, InstanceState::Scheduled, Some(chrono::Utc::now()))
        .await
        .unwrap();
    let parent = f
        .inner
        .claim_due_instances(chrono::Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let pause = f.faults.on("upsert_event_wait_claimed").times(1).pause();
    let context = StepContext {
        effect_admission: None,
        claim_token: parent.claimed_at,
        instance_id: f.id,
        tenant_id: parent.tenant_id,
        block_id: BlockId::new("event_gate"),
        params: json!({"events":["ready","second"],"correlation_key":"job-key"}),
        context: Arc::new(parent.context),
        attempt: 1,
        storage: Arc::clone(&f.engine.storage),
        wait_for_input: None,
    };
    let registering = tokio::spawn(async move { handle_wait_for_event(context).await });
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    assert!(
        !ingest(&f.engine.storage, f.original.clone())
            .await
            .unwrap()
            .satisfied
    );
    pause.release();
    let output = tokio::time::timeout(Duration::from_secs(5), registering)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(output["satisfied"], false);
    let current = f
        .inner
        .get_event_wait(f.id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.matched_event_ids, vec![f.original.id]);
    let mut second = f.original.clone();
    second.id = uuid::Uuid::now_v7();
    second.event_name = "second".into();
    second.producer_event_id = "second".into();
    assert!(
        ingest(&f.engine.storage, second.clone())
            .await
            .unwrap()
            .satisfied
    );
    let current = f
        .inner
        .get_event_wait(f.id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.matched_event_ids, vec![f.original.id, second.id]);
    assert_eq!(f.inner.get_pending_signals(f.id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn an_ingest_paused_before_matching_cannot_consume_for_a_rearmed_generation() {
    let f = Fixture::new().await;
    let old = f
        .inner
        .get_event_wait(f.id, "event_gate")
        .await
        .unwrap()
        .unwrap();
    let pause = f.faults.on("match_event_wait").times(1).pause();
    let engine = Arc::clone(&f.engine);
    let event = f.original.clone();
    let matching = tokio::spawn(async move { ingest(&engine.storage, event).await });
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .unwrap();
    f.inner
        .delete_event_waits_for_blocks(f.id, std::slice::from_ref(&old.block_id))
        .await
        .unwrap();
    let mut fresh = old.clone();
    fresh.id = uuid::Uuid::now_v7();
    let fresh = f.inner.register_event_wait(&fresh).await.unwrap();
    assert_ne!(fresh.id, old.id);
    pause.release();
    let old_result = tokio::time::timeout(Duration::from_secs(5), matching)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(old_result.matched_wait.is_none());
    assert_eq!(
        f.inner
            .get_event(f.original.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        EventStatus::Pending
    );
    assert!(f.inner.get_pending_signals(f.id).await.unwrap().is_empty());
    f.retry_and_complete().await;
}
