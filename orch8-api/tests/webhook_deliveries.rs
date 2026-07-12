//! E2E tests for the webhook delivery inspector endpoints.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::WorkerStore;
use reqwest::StatusCode;
use serde_json::Value;
use uuid::Uuid;

fn attempt(
    delivery_id: Uuid,
    n: i32,
    success: bool,
) -> orch8_types::webhook_delivery::WebhookDeliveryAttempt {
    let mut a = orch8_types::webhook_delivery::WebhookDeliveryAttempt {
        id: Uuid::now_v7(),
        delivery_id,
        url: "https://hooks.example.com/x".into(),
        event_type: "instance.failed".into(),
        instance_id: None,
        attempt_number: n,
        attempted_at: chrono::Utc::now() + chrono::Duration::milliseconds(i64::from(n)),
        duration_ms: 10,
        success,
        status_code: None,
        error_class: None,
        error_excerpt: None,
        signed: false,
    };
    if !success {
        a.status_code = Some(503);
        a.set_error("http 503", Some(503));
    }
    a
}

#[tokio::test]
async fn delivery_timeline_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let delivery = Uuid::now_v7();

    for n in [1, 2] {
        srv.storage
            .record_webhook_attempt(&attempt(delivery, n, n == 2))
            .await
            .unwrap();
    }

    // Summary list.
    let resp = client
        .get(format!("{}/webhooks/deliveries", srv.v1_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let summaries: Value = resp.json().await.unwrap();
    let arr = summaries.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["attempts"], 2);
    assert_eq!(arr[0]["delivered"], true);

    // Attempt timeline.
    let resp = client
        .get(format!("{}/webhooks/deliveries/{delivery}", srv.v1_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let attempts: Value = resp.json().await.unwrap();
    let arr = attempts.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["attempt_number"], 1);
    assert_eq!(arr[0]["error_class"], "http_status");
    assert_eq!(arr[0]["status_code"], 503);
    assert_eq!(arr[1]["success"], true);
}

#[tokio::test]
async fn unknown_delivery_is_404_and_bad_error_class_is_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{}/webhooks/deliveries/{}",
            srv.v1_url(),
            Uuid::now_v7()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = client
        .get(format!(
            "{}/webhooks/deliveries?error_class=gremlins",
            srv.v1_url()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn redeliver_preview_has_no_side_effects_and_links_history() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let delivery = Uuid::now_v7();

    let entry = orch8_types::webhook_outbox::WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: "https://hooks.example.com/parked".into(),
        event_type: "instance.failed".into(),
        instance_id: None,
        payload: serde_json::json!({"event_type": "instance.failed", "timestamp": "t", "data": {}}),
        attempts: 4,
        last_error: Some("http 503".into()),
        created_at: chrono::Utc::now(),
        delivery_id: Some(delivery),
    };
    srv.storage.park_webhook(&entry).await.unwrap();

    let resp = client
        .get(format!(
            "{}/webhooks/outbox/{}/redeliver-preview",
            srv.v1_url(),
            entry.id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let preview: Value = resp.json().await.unwrap();
    assert_eq!(preview["url"], "https://hooks.example.com/parked");
    assert_eq!(preview["previous_delivery_id"], delivery.to_string());
    assert_eq!(preview["previous_attempts"], 4);
    assert_eq!(preview["last_error"], "http 503");
    assert!(preview["payload_bytes"].as_u64().unwrap() > 0);

    // Preview must not consume the parked row.
    let listed = srv.storage.list_webhook_outbox(10).await.unwrap();
    assert_eq!(listed.len(), 1);
}
