use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;

#[tokio::test]
async fn approvals_endpoint_returns_empty_when_no_waiting_instances() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/approvals", srv.base_url))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(body["total"], 0);
}
