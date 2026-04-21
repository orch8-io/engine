//! E2E tests for the Instances API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

fn mk_sequence_body(id: Uuid) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "test-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

async fn create_sequence(client: &reqwest::Client, base_url: &str) -> Uuid {
    let seq_id = Uuid::now_v7();
    let resp = client
        .post(format!("{base_url}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&mk_sequence_body(seq_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    seq_id
}

#[tokio::test]
async fn create_instance_and_get_by_id_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "key": "val" }, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["sequence_id"], seq_id.to_string());
    assert_eq!(fetched["state"], "scheduled");
}

#[tokio::test]
async fn create_instance_with_invalid_sequence_id_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "sequence_id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_instance_with_idempotency_key_returns_same_id_on_retry() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "idempotency_key": "idem-123",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let first_id = created["id"].as_str().unwrap();

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let second: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(second["id"].as_str().unwrap(), first_id);
    assert_eq!(second["deduplicated"], true);
}

#[tokio::test]
async fn batch_create_instances_returns_correct_count() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "instances": [
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } },
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } },
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } }
        ]
    });

    let resp = client
        .post(format!("{}/instances/batch", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["count"], 3);
}

/// Stab#13: batch endpoint previously skipped the empty-tenant /
/// empty-namespace validation that single-create enforces, letting blank
/// strings leak into the default-tenant view and bypass isolation.
#[tokio::test]
async fn batch_create_rejects_empty_tenant_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    // No X-Tenant-Id header (so enforce_tenant_create passes through);
    // an empty tenant_id in the body must be rejected.
    let body = json!({
        "instances": [
            { "sequence_id": seq_id, "tenant_id": "", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } },
        ]
    });

    let resp = client
        .post(format!("{}/instances/batch", srv.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_create_rejects_empty_namespace() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "instances": [
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "", "context": { "data": {}, "config": {}, "audit": [] } },
        ]
    });

    let resp = client
        .post(format!("{}/instances/batch", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_instances_filters_by_state() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .get(format!(
            "{}/instances?tenant_id=t1&state=scheduled",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let list = body["items"].as_array().unwrap();
    assert_eq!(list.len(), 1);

    let resp = client
        .get(format!(
            "{}/instances?tenant_id=t1&state=running",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let list = body["items"].as_array().unwrap();
    assert_eq!(list.len(), 0);
}

#[tokio::test]
async fn update_instance_state_and_context() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "a": 1 }, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    // Update state to paused.
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "paused" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Update context.
    let resp = client
        .patch(format!("{}/instances/{inst_id}/context", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "context": { "data": { "b": 2 }, "config": {}, "audit": [] } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["state"], "paused");
}

#[tokio::test]
async fn get_instance_returns_404_for_unknown_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/instances/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invalid_state_transition_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    // Try to transition from Scheduled -> Completed (invalid).
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "completed" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
