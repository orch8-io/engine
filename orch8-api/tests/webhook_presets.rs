//! End-to-end: provider signature presets on public webhook triggers.

use base64::Engine as _;
use hmac::{KeyInit, Mac};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use reqwest::StatusCode;
use serde_json::{Value, json};

fn hmac(secret: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).unwrap();
    for p in parts {
        mac.update(p);
    }
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn setup(verify: Value, credential: Option<(&str, &str)>) -> (TestServer, reqwest::Client) {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    client
        .post(format!("{}/sequences", server.base_url))
        .header("X-Tenant-Id", "acme")
        .json(&json!({"id": uuid::Uuid::now_v7(), "created_at": chrono::Utc::now().to_rfc3339(),
            "tenant_id": "acme", "namespace": "default", "name": "on-event", "version": 1,
            "blocks": [{"type": "step", "id": "s1", "handler": "noop", "params": {}}]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    if let Some((id, value)) = credential {
        client
            .post(format!("{}/credentials", server.base_url))
            .header("X-Tenant-Id", "acme")
            .json(&json!({"id": id, "name": id, "kind": "api_key", "value": value, "tenant_id": "acme"}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let resp = client
        .post(format!("{}/triggers", server.base_url))
        .header("X-Tenant-Id", "acme")
        .json(&json!({"slug": "hook", "sequence_name": "on-event", "tenant_id": "acme",
            "trigger_type": "webhook", "config": {"verify": verify}}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create trigger: {}", resp.text().await.unwrap());
    (server, client)
}

async fn post(
    client: &reqwest::Client,
    server: &TestServer,
    headers: &[(&str, String)],
    body: &str,
) -> StatusCode {
    let mut req = client
        .post(format!("{}/webhooks/hook", server.base_url))
        .header("content-type", "application/json")
        .body(body.to_string());
    for (k, v) in headers {
        req = req.header(*k, v);
    }
    req.send().await.unwrap().status()
}

#[tokio::test]
async fn stripe_preset_accepts_signed_raw_body_and_rejects_replay() {
    let (server, client) = setup(
        json!({"preset": "stripe", "secret_ref": "credentials://stripe-hook"}),
        Some(("stripe-hook", "whsec_test_secret")),
    )
    .await;
    // Deliberately non-canonical JSON: verification must use raw bytes.
    let body = "{ \"id\": \"evt_1\",  \"type\": \"payment_intent.succeeded\" }";
    let t = chrono::Utc::now().timestamp().to_string();
    let sig = hex(&hmac(b"whsec_test_secret", &[t.as_bytes(), b".", body.as_bytes()]));
    let headers = [("stripe-signature", format!("t={t},v1={sig}"))];
    assert_eq!(post(&client, &server, &headers, body).await, StatusCode::ACCEPTED);
    // Exact replay inside the tolerance window is refused.
    assert_eq!(post(&client, &server, &headers, body).await, StatusCode::UNAUTHORIZED);
    // Tampered body.
    let t2 = (chrono::Utc::now().timestamp() - 1).to_string();
    let sig2 = hex(&hmac(b"whsec_test_secret", &[t2.as_bytes(), b".", body.as_bytes()]));
    assert_eq!(
        post(&client, &server, &[("stripe-signature", format!("t={t2},v1={sig2}"))], "{\"id\":\"evt_2\"}").await,
        StatusCode::UNAUTHORIZED
    );
    // Stale timestamp.
    let old = (chrono::Utc::now().timestamp() - 3600).to_string();
    let sig3 = hex(&hmac(b"whsec_test_secret", &[old.as_bytes(), b".", body.as_bytes()]));
    assert_eq!(
        post(&client, &server, &[("stripe-signature", format!("t={old},v1={sig3}"))], body).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn github_preset_with_trigger_secret_fallback() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    client
        .post(format!("{}/sequences", server.base_url))
        .header("X-Tenant-Id", "acme")
        .json(&json!({"id": uuid::Uuid::now_v7(), "created_at": chrono::Utc::now().to_rfc3339(),
            "tenant_id": "acme", "namespace": "default", "name": "on-event", "version": 1,
            "blocks": [{"type": "step", "id": "s1", "handler": "noop", "params": {}}]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    client
        .post(format!("{}/triggers", server.base_url))
        .header("X-Tenant-Id", "acme")
        .json(&json!({"slug": "hook", "sequence_name": "on-event", "tenant_id": "acme",
            "trigger_type": "webhook", "secret": "gh-secret", "config": {"verify": {"preset": "github"}}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let body = r#"{"action":"opened","pull_request":{"number":7}}"#;
    let sig = hex(&hmac(b"gh-secret", &[body.as_bytes()]));
    let headers = [
        ("x-hub-signature-256", format!("sha256={sig}")),
        ("x-github-delivery", uuid::Uuid::now_v7().to_string()),
        ("x-github-event", "pull_request".to_string()),
    ];
    assert_eq!(post(&client, &server, &headers, body).await, StatusCode::ACCEPTED);
    assert_eq!(post(&client, &server, &headers, body).await, StatusCode::UNAUTHORIZED);
    // Native Orch8 headers are not accepted in place of the preset.
    assert_eq!(post(&client, &server, &[], body).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn svix_preset_verifies_clerk_style_delivery() {
    let key = b"clerk-signing-key";
    let secret = format!("whsec_{}", base64::engine::general_purpose::STANDARD.encode(key));
    let (server, client) = setup(
        json!({"preset": "svix", "secret_ref": "clerk-hook"}),
        Some(("clerk-hook", &secret)),
    )
    .await;
    let body = r#"{"type":"user.created","data":{"id":"user_1"}}"#;
    let ts = chrono::Utc::now().timestamp().to_string();
    let sig = base64::engine::general_purpose::STANDARD
        .encode(hmac(key, &[b"msg_abc", b".", ts.as_bytes(), b".", body.as_bytes()]));
    let headers = [
        ("svix-id", "msg_abc".to_string()),
        ("svix-timestamp", ts),
        ("svix-signature", format!("v1,{sig}")),
    ];
    assert_eq!(post(&client, &server, &headers, body).await, StatusCode::ACCEPTED);
    assert_eq!(post(&client, &server, &headers, body).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn shopify_preset_and_missing_secret_fails_closed() {
    let (server, client) = setup(
        json!({"preset": "shopify", "secret_ref": "shopify-hook"}),
        Some(("shopify-hook", "shpss_secret")),
    )
    .await;
    let body = r#"{"id":820982911946154508,"email":"jon@example.com"}"#;
    let sig = base64::engine::general_purpose::STANDARD.encode(hmac(b"shpss_secret", &[body.as_bytes()]));
    let headers = [
        ("x-shopify-hmac-sha256", sig),
        ("x-shopify-webhook-id", uuid::Uuid::now_v7().to_string()),
        ("x-shopify-topic", "orders/create".to_string()),
    ];
    assert_eq!(post(&client, &server, &headers, body).await, StatusCode::ACCEPTED);

    // Deleting the credential makes the trigger fail closed (401, not 5xx).
    client
        .delete(format!("{}/credentials/shopify-hook", server.base_url))
        .header("X-Tenant-Id", "acme")
        .send()
        .await
        .unwrap();
    let headers2 = [
        ("x-shopify-hmac-sha256", headers[0].1.clone()),
        ("x-shopify-webhook-id", uuid::Uuid::now_v7().to_string()),
    ];
    assert_eq!(post(&client, &server, &headers2, body).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn trigger_creation_rejects_bad_verify_config() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/triggers", server.base_url))
        .header("X-Tenant-Id", "acme")
        .json(&json!({"slug": "x", "sequence_name": "nope", "tenant_id": "acme",
            "trigger_type": "webhook", "config": {"verify": {"preset": "stripe"}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let resp = client
        .post(format!("{}/triggers", server.base_url))
        .header("X-Tenant-Id", "acme")
        .json(&json!({"slug": "x", "sequence_name": "nope", "tenant_id": "acme",
            "trigger_type": "webhook", "config": {"verify": {"preset": "paypal", "secret_ref": "a"}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn recipe_sequences_parse_validate_and_lint_clean() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/recipes");
    let mut seen = 0;
    for name in [
        "stripe-payment-succeeded",
        "github-pr-opened",
        "shopify-order-created",
        "clerk-user-created",
    ] {
        let dir = root.join(name);
        assert!(dir.join("README.md").is_file(), "{name} README");
        let raw = std::fs::read_to_string(dir.join("sequence.json")).unwrap();
        let seq: orch8_types::sequence::SequenceDefinition = serde_json::from_str(&raw).unwrap();
        seq.validate().unwrap_or_else(|e| panic!("{name}: {e}"));
        let warnings = orch8_engine::lint::lint_sequence(&seq);
        assert!(warnings.is_empty(), "{name}: {warnings:?}");
        seen += 1;
    }
    assert_eq!(seen, 4);
}
