//! E2E tests for health/info endpoints.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;

#[tokio::test]
async fn info_returns_version_and_env_banner_fields() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/info", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = resp.json().await.unwrap();
    // Version is the compiled crate version — always present and non-empty.
    let version = body["version"].as_str().expect("version must be a string");
    assert!(!version.is_empty());
    // The banner fields are always present in the shape (string when the operator
    // sets ORCH8_ENV_LABEL/ORCH8_ENV_COLOR, JSON null otherwise) — never absent,
    // so the dashboard can rely on the keys existing.
    assert!(body.get("env_label").is_some(), "env_label key must be present");
    assert!(body.get("env_color").is_some(), "env_color key must be present");
}

#[tokio::test]
async fn liveness_is_always_ok() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/health/live", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
