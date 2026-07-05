//! Security-focused end-to-end tests for fixes from the Rust audit.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;

#[tokio::test]
async fn request_id_is_truncated_to_bound_header_size() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let huge_id = "a".repeat(10_000);
    let resp = client
        .get(format!("{}/health/live", srv.base_url))
        .header("x-request-id", &huge_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let echoed = resp
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-request-id must be echoed");
    assert!(
        echoed.len() < huge_id.len(),
        "request id must be truncated, got {} chars",
        echoed.len()
    );
    // Only safe ASCII chars survive sanitization.
    assert!(
        echoed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    );
}

#[tokio::test]
async fn public_webhook_rejects_trigger_without_secret() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create a sequence.
    let resp = client
        .post(format!("{}/sequences", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000001",
            "tenant_id": "t1",
            "namespace": "default",
            "name": "webhook-seq",
            "version": 1,
            "deprecated": false,
            "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {} }],
            "created_at": "2024-01-01T00:00:00Z"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create a webhook trigger without a secret.
    let resp = client
        .post(format!("{}/triggers", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&serde_json::json!({
            "slug": "unprotected",
            "sequence_name": "webhook-seq",
            "tenant_id": "t1",
            "trigger_type": "webhook"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Public webhook endpoint must refuse an unauthenticated trigger.
    let resp = client
        .post(format!("{}/webhooks/unprotected", srv.base_url))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
