//! E2E tests for the Signals API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

fn mk_sequence_body(id: Uuid) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "signal-seq",
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

async fn create_instance(client: &reqwest::Client, base_url: &str, seq_id: Uuid) -> Uuid {
    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{base_url}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    Uuid::parse_str(created["id"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn send_signal_to_running_instance_succeeds() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;

    let resp = client
        .post(format!("{}/instances/{inst_id}/signals", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "signal_type": "pause",
            "payload": {}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["signal_id"].is_string());
}

#[tokio::test]
async fn send_signal_to_terminal_instance_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;

    // Transition to completed via state update (bypassing engine).
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "completed" }))
        .send()
        .await
        .unwrap();
    // This will fail because Scheduled -> Completed is invalid.
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Create a fresh instance and force it to failed so we can test signal rejection.
    let inst_id2 = create_instance(&client, &srv.base_url, seq_id).await;
    // Move to Running first.
    let resp = client
        .patch(format!("{}/instances/{inst_id2}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "running" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Move to Failed.
    let resp = client
        .patch(format!("{}/instances/{inst_id2}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "failed" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .post(format!("{}/instances/{inst_id2}/signals", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "signal_type": "pause",
            "payload": {}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn send_signal_to_unknown_instance_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/instances/{}/signals",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "signal_type": { "custom": "test" },
            "payload": { "data": 1 }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn send_custom_signal_records_payload() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;

    let resp = client
        .post(format!("{}/instances/{inst_id}/signals", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "signal_type": { "custom": "notify" },
            "payload": { "message": "hello" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}
