//! E2E tests for the event ingestion/inspection API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};

#[tokio::test]
async fn ingest_is_idempotent_by_producer_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let body = json!({
        "tenant_id": "t1",
        "event_name": "payment_received",
        "producer_event_id": "stripe-evt-1",
        "correlation_key": "order-1",
        "payload": {"amount": 100, "api_key": "sk_live_secret"},
    });
    let first = client
        .post(format!("{base}/events"))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);
    let outcome: Value = first.json().await.unwrap();
    assert_eq!(outcome["duplicate"], false);

    let second = client
        .post(format!("{base}/events"))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let outcome2: Value = second.json().await.unwrap();
    assert_eq!(outcome2["duplicate"], true);

    // Listing shows one pending event with a REDACTED payload secret.
    let listed: Vec<Value> = client
        .get(format!("{base}/events?status=pending"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["payload"]["amount"], 100);
    assert_eq!(listed[0]["payload"]["api_key"], "[REDACTED]");
}

#[tokio::test]
async fn events_are_tenant_scoped() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    client
        .post(format!("{base}/events"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1",
            "event_name": "paid",
            "producer_event_id": "p-1",
            "correlation_key": "k",
        }))
        .send()
        .await
        .unwrap();

    // The other tenant's list is empty; direct get is 404.
    let listed: Vec<Value> = client
        .get(format!("{base}/events"))
        .header("X-Tenant-Id", "other")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(listed.is_empty());

    let mine: Vec<Value> = client
        .get(format!("{base}/events"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let event_id = mine[0]["id"].as_str().unwrap();
    let resp = client
        .get(format!("{base}/events/{event_id}"))
        .header("X-Tenant-Id", "other")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn missing_fields_and_bad_status_are_rejected() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let resp = client
        .post(format!("{base}/events"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1",
            "event_name": "  ",
            "producer_event_id": "p",
            "correlation_key": "k",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let resp = client
        .get(format!("{base}/events?status=bogus"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn outbox_batch_is_replay_safe_and_validates_before_writing() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let batch = json!({"events": [
        {"tenant_id":"t1","event_name":"order.created","producer_event_id":"outbox-1","correlation_key":"order-1","payload":{"n":1}},
        {"tenant_id":"t1","event_name":"order.created","producer_event_id":"outbox-2","correlation_key":"order-2","payload":{"n":2}}
    ]});
    for expected_duplicates in [[false, false], [true, true]] {
        let response = client
            .post(format!("{base}/events/batch"))
            .header("X-Tenant-Id", "t1")
            .json(&batch)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["outcomes"][0]["duplicate"], expected_duplicates[0]);
        assert_eq!(body["outcomes"][1]["duplicate"], expected_duplicates[1]);
    }

    let invalid = json!({"events": [
        {"tenant_id":"t1","event_name":"valid","producer_event_id":"not-written","correlation_key":"k"},
        {"tenant_id":"t1","event_name":" ","producer_event_id":"bad","correlation_key":"k"}
    ]});
    let response = client
        .post(format!("{base}/events/batch"))
        .header("X-Tenant-Id", "t1")
        .json(&invalid)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let listed: Vec<Value> = client
        .get(format!("{base}/events"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !listed
            .iter()
            .any(|event| event["producer_event_id"] == "not-written")
    );
}
