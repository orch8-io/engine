//! Proof is derived from actual device authentication, never caller-supplied scope.
use orch8_api::test_harness::{TestServerOptions, spawn_test_server_with};
use orch8_storage::testing::backends::TestBackend;
use orch8_types::{continuity::RuntimeId, ids::TenantId, worker_identity::WorkerIdentityResponse};
use reqwest::StatusCode;
use serde_json::{Value, json};
const ROOT: &str = "worker-identity-test-root";
async fn identity(b: TestBackend) {
    let server = spawn_test_server_with(
        b.storage.clone(),
        TestServerOptions {
            root_api_key: Some(ROOT.into()),
            ..TestServerOptions::default()
        },
    )
    .await;
    let http = reqwest::Client::new();
    let tenant = TenantId::new("identity-tenant").unwrap();
    let runtime = RuntimeId::new();
    let minted=http.post(format!("{}/runtimes/device-sessions",server.v1_url())).header("x-api-key",ROOT).header("x-tenant-id",tenant.as_str()).json(&json!({"tenant_id":tenant,"device_id":"device","runtime_id":runtime,"handlers":["orch8.delegation"],"ttl_secs":30})).send().await.unwrap();
    assert_eq!(
        minted.status(),
        StatusCode::CREATED,
        "{}",
        minted.text().await.unwrap()
    );
    let minted: Value = minted.json().await.unwrap();
    let token = minted["token"].as_str().unwrap();
    let nonce = uuid::Uuid::new_v4().to_string();
    let url = format!("{}/workers/identity", server.v1_url());
    let response = http
        .post(&url)
        .header("x-api-key", token)
        .json(&json!({"nonce":nonce}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    let proof: WorkerIdentityResponse = response.json().await.unwrap();
    proof
        .validate_for(&nonce, &tenant, runtime, "device", chrono::Utc::now())
        .unwrap();
    assert!(
        proof.expires_at
            <= serde_json::from_value::<chrono::DateTime<chrono::Utc>>(
                minted["expires_at"].clone()
            )
            .unwrap()
    );
    assert!(
        proof
            .validate_for(
                &uuid::Uuid::new_v4().to_string(),
                &tenant,
                runtime,
                "device",
                chrono::Utc::now()
            )
            .is_err()
    );
    for key in [ROOT, "dst_INVALID_PRIVATE_TOKEN"] {
        let denied = http
            .post(&url)
            .header("x-api-key", key)
            .json(&json!({"nonce":nonce}))
            .send()
            .await
            .unwrap();
        assert!(matches!(
            denied.status(),
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED
        ));
    }
    for body in [
        json!({"nonce":"PRIVATE_CHALLENGE"}),
        json!({"nonce":nonce,"tenant_id":"foreign"}),
        json!({"nonce":"PRIVATE_CHALLENGE".repeat(1000)}),
    ] {
        let invalid = http
            .post(&url)
            .header("x-api-key", token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert!(!invalid.text().await.unwrap().contains("PRIVATE_CHALLENGE"));
    }
    let browser=http.post(format!("{}/runtimes/browser-sessions",server.v1_url())).header("x-api-key",ROOT).header("x-tenant-id",tenant.as_str()).json(&json!({"tenant_id":tenant,"runtime_id":RuntimeId::new(),"handlers":["orch8.delegation"]})).send().await.unwrap();
    assert_eq!(browser.status(), StatusCode::CREATED);
    let browser: Value = browser.json().await.unwrap();
    let denied = http
        .post(&url)
        .header("x-api-key", browser["token"].as_str().unwrap())
        .json(&json!({"nonce":nonce}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
}
orch8_storage::backend_test!(async fn worker_identity_is_fresh_device_bound_and_not_an_operator_grant(b){identity(b).await;});
