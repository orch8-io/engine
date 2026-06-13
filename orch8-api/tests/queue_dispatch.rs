//! E2E tests for the queue dispatch config API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn dispatch_config_crud() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // push mode requires a push_url.
    let resp = client
        .post(format!("{}/queues/dispatch", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "tenant_id": "t1", "queue_name": "q1", "mode": "push" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Valid push config.
    let resp = client
        .post(format!("{}/queues/dispatch", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1", "queue_name": "q1", "mode": "push",
            "push_url": "https://example.invalid/hook", "secret": "shhh"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cfg: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(cfg["mode"], "push");
    // Secret is never echoed.
    assert!(cfg.get("secret").is_none() || cfg["secret"].is_null());

    // List.
    let resp = client
        .get(format!("{}/queues/dispatch?tenant_id=t1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let configs: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(configs.as_array().unwrap().len(), 1);
    assert_eq!(configs[0]["queue_name"], "q1");

    // Delete.
    let resp = client
        .delete(format!("{}/queues/dispatch/t1/q1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = client
        .get(format!("{}/queues/dispatch", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<serde_json::Value>().await.unwrap().as_array().unwrap().len(), 0);
}
