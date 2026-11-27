//! E2E tests for the worker control channel.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn enqueue_list_and_ack_worker_commands() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Queue a drain command for worker-1.
    let resp = client
        .post(format!("{}/workers/commands", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "worker_id": "worker-1", "command": "drain", "payload": { "deadline_secs": 30 } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let cmd: serde_json::Value = resp.json().await.unwrap();
    let cmd_id = cmd["id"].as_str().unwrap().to_string();
    assert_eq!(cmd["command"], "drain");

    // Queue a ping too.
    client
        .post(format!("{}/workers/commands", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "worker_id": "worker-1", "command": "ping" }))
        .send()
        .await
        .unwrap();

    // worker-1 sees both, oldest first.
    let resp = client
        .get(format!("{}/workers/worker-1/commands", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cmds: serde_json::Value = resp.json().await.unwrap();
    let arr = cmds.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["command"], "drain");
    assert_eq!(arr[0]["payload"]["deadline_secs"], 30);
    assert_eq!(arr[1]["command"], "ping");

    // A different worker sees nothing.
    let resp = client
        .get(format!("{}/workers/worker-2/commands", srv.base_url))
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

    // Ack the drain command.
    let resp = client
        .delete(format!("{}/workers/commands/{cmd_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Only the ping remains.
    let resp = client
        .get(format!("{}/workers/worker-1/commands", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let cmds: serde_json::Value = resp.json().await.unwrap();
    let arr = cmds.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["command"], "ping");
}

#[tokio::test]
async fn enqueue_command_rejects_unknown_command() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/workers/commands", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "worker_id": "w", "command": "explode" }))
        .send()
        .await
        .unwrap();
    // Unknown enum variant → 422 deserialization error from axum.
    assert!(
        resp.status() == StatusCode::UNPROCESSABLE_ENTITY
            || resp.status() == StatusCode::BAD_REQUEST,
        "got {}",
        resp.status()
    );
}

#[tokio::test]
async fn inspection_filters_a_shared_worker_without_changing_delivery() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    for tenant in ["alpha", "beta", ""] {
        let response = client
            .post(format!("{}/workers/commands", srv.base_url))
            .json(&json!({"worker_id":"shared", "tenant_id":tenant, "command":"ping"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let url = format!("{}/workers/shared/commands", srv.base_url);
    let delivery: serde_json::Value = client
        .get(&url)
        .header("X-Tenant-Id", "alpha")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(delivery.as_array().unwrap().len(), 3);

    for (query, caller, expected) in [("alpha", None, "alpha"), ("beta", Some("alpha"), "alpha")] {
        let mut request = client.get(&url).query(&[("tenant_id", query)]);
        if let Some(caller) = caller {
            request = request.header("X-Tenant-Id", caller);
        }
        let page: serde_json::Value = request
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(page.as_array().unwrap().len(), 1);
        assert_eq!(page[0]["tenant_id"], expected);
    }
    let invalid = client
        .get(&url)
        .query(&[("tenant_id", "")])
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
}
