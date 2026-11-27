//! E2E tests for the queue dispatch config API.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::WorkerStore;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn queue_metadata_can_select_one_literal_queue_without_exposing_secrets() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("{}/queues/dispatch", server.base_url);
    let selected = " queue #é?% ";
    for (tenant, queue) in [("t1", selected), ("t1", selected.trim()), ("t2", selected)] {
        let response = client
            .post(&url)
            .header("X-Tenant-Id", tenant)
            .json(&json!({"tenant_id":tenant,"queue_name":queue,"mode":"poll","secret":"fixture-secret"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let get = |queue| {
        client
            .get(&url)
            .header("X-Tenant-Id", "t1")
            .query(&[("tenant_id", "t1"), ("queue_name", queue)])
    };
    let response = get(selected)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.headers()["x-orch8-queue-dispatch-scope"], "exact");
    assert_eq!(response.headers()["cache-control"], "no-store");
    let rows = response.json::<Vec<serde_json::Value>>().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["tenant_id"], "t1");
    assert_eq!(rows[0]["queue_name"], selected);
    assert!(rows[0].get("secret").is_none());
    assert_eq!(
        get("absent")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap(),
        json!([])
    );
    assert_eq!(
        get(" ").send().await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn push_url_rejects_internal_addresses() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    for bad_url in [
        "http://127.0.0.1/hook",
        "http://192.168.1.1/hook",
        "http://169.254.169.254/latest/meta-data/",
        "file:///etc/passwd",
    ] {
        let resp = client
            .post(format!("{}/queues/dispatch", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&json!({
                "tenant_id": "t1", "queue_name": "q1", "mode": "push",
                "push_url": bad_url
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{bad_url} should be rejected"
        );
    }
}

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
    assert_eq!(
        resp.json::<serde_json::Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn updating_dispatch_preserves_secret_and_creation_time_unless_explicitly_cleared() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("{}/queues/dispatch", srv.base_url);
    let create = client
        .post(&url)
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1", "queue_name": "q1", "mode": "push",
            "push_url": "https://example.invalid/first", "secret": "original"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let created: serde_json::Value = create.json().await.unwrap();
    let original_created_at = created["created_at"].clone();

    let update = client
        .post(&url)
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1", "queue_name": "q1", "mode": "push",
            "push_url": "https://example.invalid/second"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);
    let updated: serde_json::Value = update.json().await.unwrap();
    assert_eq!(updated["created_at"], original_created_at);
    assert!(updated.get("secret").is_none());
    let stored = srv
        .storage
        .get_queue_dispatch("t1", "q1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.secret.as_deref(), Some("original"));
    assert_eq!(
        stored.push_url.as_deref(),
        Some("https://example.invalid/second")
    );

    let clear = client
        .post(&url)
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1", "queue_name": "q1", "mode": "poll", "secret": null
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(clear.status(), StatusCode::OK);
    let cleared: serde_json::Value = clear.json().await.unwrap();
    assert_eq!(cleared["created_at"], original_created_at);
    assert!(
        srv.storage
            .get_queue_dispatch("t1", "q1")
            .await
            .unwrap()
            .unwrap()
            .secret
            .is_none()
    );
}
