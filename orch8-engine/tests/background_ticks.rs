//! Single-pass entry points of the background loops, driven on virtual
//! "now" instead of sleeping through the loops' intervals: `gc::gc_tick`,
//! `webhooks::outbox_tick` (against `testing::http::MockHttp`), and
//! and `credentials::refresh_pass_at` (no test here yet; same shape).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use orch8_engine::gc::{GcOptions, gc_tick};
use orch8_engine::testing::TestEngine;
use orch8_engine::testing::http::{MockHttp, Reply};
use orch8_engine::webhooks::{instance_event, outbox_tick};
use orch8_storage::{DedupeScope, EmitDedupeOutcome, StorageBackend};
use orch8_types::config::WebhookConfig;
use orch8_types::ids::InstanceId;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};
use tokio_util::sync::CancellationToken;

mod common;

async fn seeded_dedupe_row(storage: &Arc<dyn StorageBackend>) -> (DedupeScope, &'static str) {
    let scope = DedupeScope::Parent(InstanceId::new());
    storage
        .record_or_get_emit_dedupe(&scope, "k", InstanceId::new())
        .await
        .unwrap();
    (scope, "k")
}

/// `Inserted` = the old dedupe row is gone, `AlreadyExists` = still there.
async fn probe(storage: &Arc<dyn StorageBackend>, scope: &DedupeScope, key: &str) -> bool {
    matches!(
        storage
            .record_or_get_emit_dedupe(scope, key, InstanceId::new())
            .await
            .unwrap(),
        EmitDedupeOutcome::AlreadyExists(_)
    )
}

#[tokio::test]
async fn gc_tick_past_the_dedupe_ttl_deletes_the_dedupe_row() {
    let storage = common::storage().await;
    let (scope, key) = seeded_dedupe_row(&storage).await;
    let ttl = Duration::from_secs(3600);
    let options = GcOptions {
        dedupe_ttl: ttl,
        ..GcOptions::default()
    };

    gc_tick(
        storage.as_ref(),
        &options,
        Utc::now() + chrono::Duration::hours(2),
    )
    .await;

    assert!(
        !probe(&storage, &scope, key).await,
        "expired row must be swept"
    );
}

#[tokio::test]
async fn gc_tick_within_the_dedupe_ttl_keeps_the_dedupe_row() {
    let storage = common::storage().await;
    let (scope, key) = seeded_dedupe_row(&storage).await;
    let options = GcOptions {
        dedupe_ttl: Duration::from_secs(3600),
        ..GcOptions::default()
    };

    gc_tick(
        storage.as_ref(),
        &options,
        Utc::now() + chrono::Duration::minutes(30),
    )
    .await;

    assert!(probe(&storage, &scope, key).await, "fresh row must survive");
}

/// A pending durable delivery of one event to `url`.
async fn enqueue_delivery(storage: &Arc<dyn StorageBackend>, url: String) -> uuid::Uuid {
    let event = instance_event(
        "instance.completed",
        InstanceId::new(),
        serde_json::json!({}),
    );
    let entry = WebhookOutboxEntry {
        id: uuid::Uuid::now_v7(),
        url,
        event_type: event.event_type.clone(),
        instance_id: event.instance_id.map(InstanceId::into_uuid),
        payload: serde_json::to_value(&event).unwrap(),
        attempts: 0,
        last_error: None,
        created_at: Utc::now(),
        delivery_id: Some(uuid::Uuid::now_v7()),
        status: WebhookOutboxStatus::Pending,
        next_attempt_at: None,
        claimed_at: None,
    };
    storage.park_webhook(&entry).await.unwrap();
    entry.id
}

/// An engine (for its storage and virtual clock), a receiver that fails
/// once with 503 and then accepts, and one pending delivery to it.
async fn failing_once_receiver() -> (TestEngine, MockHttp, uuid::Uuid) {
    let engine = TestEngine::builder().build().await;
    let mock = MockHttp::start(vec![Reply::status(503)]).await;
    let id = enqueue_delivery(&engine.storage, mock.url("/hook")).await;
    (engine, mock, id)
}

async fn outbox_pass(engine: &TestEngine) -> usize {
    outbox_tick(
        &engine.storage,
        &WebhookConfig::default(),
        &engine.shared_clock(),
        &CancellationToken::new(),
    )
    .await
}

#[tokio::test]
async fn outbox_tick_reschedules_a_503_with_the_first_backoff() {
    let (engine, mock, id) = failing_once_receiver().await;
    let ticked_at = engine.now();

    outbox_pass(&engine).await;

    let row = engine
        .storage
        .get_webhook_outbox(id)
        .await
        .unwrap()
        .expect("kept");
    assert_eq!(row.status, WebhookOutboxStatus::Pending);
    assert_eq!(row.attempts, 1);
    let wait_ms = (row.next_attempt_at.expect("retry scheduled") - ticked_at).num_milliseconds();
    // outbox_backoff(0): 5 s jittered to 50-100 %.
    assert!((2_500..=5_000).contains(&wait_ms), "backoff {wait_ms} ms");
    assert_eq!(mock.request_count(), 1);
}

#[tokio::test]
async fn outbox_tick_does_not_redeliver_before_the_backoff() {
    let (engine, mock, _id) = failing_once_receiver().await;
    outbox_pass(&engine).await;

    let attempted = outbox_pass(&engine).await;

    assert_eq!(attempted, 0);
    assert_eq!(mock.request_count(), 1);
}

#[tokio::test]
async fn outbox_tick_redelivers_after_the_backoff_and_removes_the_row() {
    let (engine, mock, id) = failing_once_receiver().await;
    outbox_pass(&engine).await;
    let due = engine
        .storage
        .get_webhook_outbox(id)
        .await
        .unwrap()
        .unwrap()
        .next_attempt_at
        .unwrap();

    engine.clock.set(due);
    outbox_pass(&engine).await;

    assert_eq!(mock.request_count(), 2);
    assert!(
        engine
            .storage
            .get_webhook_outbox(id)
            .await
            .unwrap()
            .is_none()
    );
    let redelivered = &mock.requests()[1];
    assert_eq!(redelivered.method, "POST");
    assert_eq!(redelivered.body_json()["event_type"], "instance.completed");
}
