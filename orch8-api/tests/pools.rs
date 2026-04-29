use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn add_resource_rejects_invalid_warmup_date() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let pool_resp = client
        .post(format!("{}/pools", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1",
            "name": "test-pool"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(pool_resp.status(), StatusCode::CREATED);
    let pool: serde_json::Value = pool_resp.json().await.unwrap();
    let pool_id = pool["id"].as_str().unwrap();

    let resp = client
        .post(format!("{}/pools/{pool_id}/resources", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "resource_key": "rk1",
            "name": "res1",
            "warmup_start": "not-a-date"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap_or("");
    assert!(
        msg.contains("warmup_start"),
        "error should mention warmup_start: {msg}"
    );
}

#[tokio::test]
async fn update_resource_rejects_invalid_warmup_date() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let pool_resp = client
        .post(format!("{}/pools", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1",
            "name": "date-pool"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(pool_resp.status(), StatusCode::CREATED);
    let pool: serde_json::Value = pool_resp.json().await.unwrap();
    let pool_id = pool["id"].as_str().unwrap();

    let res_resp = client
        .post(format!("{}/pools/{pool_id}/resources", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "resource_key": "rk2",
            "name": "res2"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res_resp.status(), StatusCode::CREATED);
    let resource: serde_json::Value = res_resp.json().await.unwrap();
    let resource_id = resource["id"].as_str().unwrap();

    let resp = client
        .put(format!(
            "{}/pools/{pool_id}/resources/{resource_id}",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "warmup_start": "31-12-2025"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_resource_accepts_valid_warmup_date() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let pool_resp = client
        .post(format!("{}/pools", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1",
            "name": "valid-date-pool"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(pool_resp.status(), StatusCode::CREATED);
    let pool: serde_json::Value = pool_resp.json().await.unwrap();
    let pool_id = pool["id"].as_str().unwrap();

    let resp = client
        .post(format!("{}/pools/{pool_id}/resources", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "resource_key": "rk3",
            "name": "res3",
            "warmup_start": "2025-06-15"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["warmup_start"], "2025-06-15");
}
