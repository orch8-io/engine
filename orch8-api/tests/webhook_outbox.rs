//! E2E tests for the webhook outbox API.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::WorkerStore;
use orch8_types::webhook_outbox::WebhookOutboxEntry;
use reqwest::StatusCode;
use serde_json::json;

fn mk_parked(url: &str) -> WebhookOutboxEntry {
    WebhookOutboxEntry {
        id: uuid::Uuid::now_v7(),
        url: url.into(),
        event_type: "instance.failed".into(),
        instance_id: Some(uuid::Uuid::now_v7()),
        payload: json!({
            "event_type": "instance.failed",
            "instance_id": uuid::Uuid::now_v7(),
            "timestamp": "2026-01-01T00:00:00Z",
            "data": {}
        }),
        attempts: 4,
        last_error: Some("http 500".into()),
        created_at: chrono::Utc::now(),
        delivery_id: None,
        status: orch8_types::webhook_outbox::WebhookOutboxStatus::Parked,
        next_attempt_at: None,
        claimed_at: None,
    }
}

#[tokio::test]
async fn list_and_discard_outbox() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let entry = mk_parked("https://example.invalid/hook");
    srv.storage.park_webhook(&entry).await.unwrap();

    // List shows the parked delivery.
    let resp = client
        .get(format!("{}/webhooks/outbox", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["event_type"], "instance.failed");
    assert_eq!(rows[0]["attempts"], 4);

    // Discard removes it.
    let resp = client
        .delete(format!("{}/webhooks/outbox/{}", srv.base_url, entry.id))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = client
        .get(format!("{}/webhooks/outbox", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let rows: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn redeliver_to_unreachable_url_returns_502_and_keeps_row() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // A URL that cannot resolve — redelivery fails, the row must remain parked.
    let entry = mk_parked("http://127.0.0.1:1/dead");
    srv.storage.park_webhook(&entry).await.unwrap();

    let resp = client
        .post(format!(
            "{}/webhooks/outbox/{}/redeliver",
            srv.base_url, entry.id
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

    // Still parked.
    let still = srv.storage.get_webhook_outbox(entry.id).await.unwrap();
    assert!(still.is_some(), "failed redelivery must keep the row");
}

#[tokio::test]
async fn redeliver_unknown_id_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/webhooks/outbox/{}/redeliver",
            srv.base_url,
            uuid::Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
