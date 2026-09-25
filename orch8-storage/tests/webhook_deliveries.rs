//! Integration tests for the webhook delivery attempt store (`SQLite`).

use chrono::{Duration, Utc};
use orch8_storage::WorkerStore;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::webhook_delivery::{DeliveryErrorClass, DeliveryFilter, WebhookDeliveryAttempt};
use uuid::Uuid;

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

fn attempt(delivery_id: Uuid, number: i32, success: bool) -> WebhookDeliveryAttempt {
    let mut a = WebhookDeliveryAttempt {
        id: Uuid::now_v7(),
        delivery_id,
        url: "https://hooks.example.com/a".into(),
        event_type: "instance.failed".into(),
        instance_id: Some(Uuid::now_v7()),
        attempt_number: number,
        attempted_at: Utc::now() + Duration::milliseconds(i64::from(number)),
        duration_ms: 25,
        success,
        status_code: None,
        error_class: None,
        error_excerpt: None,
        signed: true,
    };
    if !success {
        a.set_error("http 503", Some(503));
    }
    a
}

#[tokio::test]
async fn record_and_fetch_attempts_in_order() {
    let s = store().await;
    let delivery = Uuid::now_v7();
    // Insert out of order; fetch must sort by attempt_number.
    for n in [2, 1, 3] {
        s.record_webhook_attempt(&attempt(delivery, n, n == 3))
            .await
            .unwrap();
    }
    let attempts = s.get_webhook_delivery_attempts(delivery).await.unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts
            .iter()
            .map(|a| a.attempt_number)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(attempts[2].success);
    assert_eq!(
        attempts[0].error_class,
        Some(DeliveryErrorClass::HttpStatus)
    );
    assert_eq!(attempts[0].error_excerpt.as_deref(), Some("http 503"));
    assert!(attempts[0].signed);
}

#[tokio::test]
async fn summaries_aggregate_per_delivery() {
    let s = store().await;
    let failed = Uuid::now_v7();
    let delivered = Uuid::now_v7();
    for n in 1..=3 {
        s.record_webhook_attempt(&attempt(failed, n, false))
            .await
            .unwrap();
    }
    s.record_webhook_attempt(&attempt(delivered, 1, false))
        .await
        .unwrap();
    s.record_webhook_attempt(&attempt(delivered, 2, true))
        .await
        .unwrap();

    let all = s
        .list_webhook_deliveries(&DeliveryFilter::default(), 100)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);

    let failed_summary = all.iter().find(|d| d.delivery_id == failed).unwrap();
    assert_eq!(failed_summary.attempts, 3);
    assert!(!failed_summary.delivered);
    assert_eq!(
        failed_summary.last_error_class,
        Some(DeliveryErrorClass::HttpStatus)
    );
    assert!(failed_summary.first_attempt_at <= failed_summary.last_attempt_at);

    let ok_summary = all.iter().find(|d| d.delivery_id == delivered).unwrap();
    assert_eq!(ok_summary.attempts, 2);
    assert!(ok_summary.delivered);
}

#[tokio::test]
async fn summaries_filter_by_outcome_and_event_type() {
    let s = store().await;
    let failed = Uuid::now_v7();
    let delivered = Uuid::now_v7();
    s.record_webhook_attempt(&attempt(failed, 1, false))
        .await
        .unwrap();
    let mut other = attempt(delivered, 1, true);
    other.event_type = "instance.completed".into();
    s.record_webhook_attempt(&other).await.unwrap();

    let only_failed = s
        .list_webhook_deliveries(
            &DeliveryFilter {
                delivered: Some(false),
                ..Default::default()
            },
            100,
        )
        .await
        .unwrap();
    assert_eq!(only_failed.len(), 1);
    assert_eq!(only_failed[0].delivery_id, failed);

    let by_event = s
        .list_webhook_deliveries(
            &DeliveryFilter {
                event_type: Some("instance.completed".into()),
                ..Default::default()
            },
            100,
        )
        .await
        .unwrap();
    assert_eq!(by_event.len(), 1);
    assert_eq!(by_event[0].delivery_id, delivered);

    let by_class = s
        .list_webhook_deliveries(
            &DeliveryFilter {
                error_class: Some(DeliveryErrorClass::HttpStatus),
                ..Default::default()
            },
            100,
        )
        .await
        .unwrap();
    assert_eq!(by_class.len(), 1);
    assert_eq!(by_class[0].delivery_id, failed);
}

#[tokio::test]
async fn retention_deletes_only_old_attempts() {
    let s = store().await;
    let delivery = Uuid::now_v7();
    let mut old = attempt(delivery, 1, false);
    old.attempted_at = Utc::now() - Duration::days(60);
    s.record_webhook_attempt(&old).await.unwrap();
    s.record_webhook_attempt(&attempt(delivery, 2, true))
        .await
        .unwrap();

    let removed = s
        .delete_webhook_attempts_before(Utc::now() - Duration::days(30))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    let remaining = s.get_webhook_delivery_attempts(delivery).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].attempt_number, 2);
}

#[tokio::test]
async fn parked_outbox_row_links_to_delivery() {
    let s = store().await;
    let delivery = Uuid::now_v7();
    let entry = orch8_types::webhook_outbox::WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: "https://hooks.example.com/a".into(),
        event_type: "instance.failed".into(),
        instance_id: None,
        payload: serde_json::json!({"event_type": "instance.failed"}),
        attempts: 4,
        last_error: Some("http 503".into()),
        created_at: Utc::now(),
        delivery_id: Some(delivery),
        status: orch8_types::webhook_outbox::WebhookOutboxStatus::Parked,
        next_attempt_at: None,
        claimed_at: None,
    };
    s.park_webhook(&entry).await.unwrap();
    let fetched = s.get_webhook_outbox(entry.id).await.unwrap().unwrap();
    assert_eq!(fetched.delivery_id, Some(delivery));

    let listed = s.list_webhook_outbox(10).await.unwrap();
    assert_eq!(listed[0].delivery_id, Some(delivery));
}
