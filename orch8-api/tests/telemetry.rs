//! E2E tests for the Telemetry API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn ingest_telemetry_events() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "events": [
            {
                "event_type": "SyncCompleted",
                "payload": r#"{"version":1}"#,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "device": {
                    "device_id": "dev-1",
                    "os_name": "iOS",
                    "os_version": "17.0",
                    "app_version": "1.0.0",
                    "sdk_version": "0.4.0"
                }
            }
        ],
        "tenant_id": "t1"
    });

    let resp = client
        .post(format!("{}/telemetry/mobile", srv.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["accepted"], 1);
}

#[tokio::test]
async fn ingest_telemetry_events_with_batch_device() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "events": [{
            "event_type": "SyncCompleted",
            "payload": r#"{"version":1}"#,
            "timestamp": chrono::Utc::now().to_rfc3339()
        }],
        "device": {
            "device_id": "dev-batch",
            "os_name": "iOS",
            "os_version": "17.0",
            "app_version": "1.0.0",
            "sdk_version": "0.7.0"
        },
        "tenant_id": "t1"
    });

    let response = client
        .post(format!("{}/telemetry/mobile", srv.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["accepted"],
        1
    );
}

#[tokio::test]
async fn ingest_telemetry_events_requires_a_device_context() {
    let srv = spawn_test_server().await;
    let response = reqwest::Client::new()
        .post(format!("{}/telemetry/mobile", srv.base_url))
        .json(&json!({
            "events": [{
                "event_type": "SyncCompleted",
                "payload": "{}",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn ingest_telemetry_errors() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "error_type": "HandlerPanic",
        "message": "step handler panicked",
        "stack_trace": null,
        "device": {
            "device_id": "dev-1",
            "os_name": "iOS",
            "os_version": "17.0",
            "app_version": "1.0.0",
            "sdk_version": "0.4.0"
        },
        "tenant_id": "t1",
        "instance_id": null,
        "sequence_name": "test_seq"
    });

    let resp = client
        .post(format!("{}/telemetry/mobile/errors", srv.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}
