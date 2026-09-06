use base64::Engine;
use hmac::{KeyInit, Mac};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::AdminStore;
use orch8_types::ids::TenantId;
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn sequence(
    client: &reqwest::Client,
    server: &TestServer,
    tenant: &str,
    name: &str,
) -> Value {
    client.post(format!("{}/sequences", server.base_url))
        .header("X-Tenant-Id", tenant)
        .json(&json!({"id":uuid::Uuid::now_v7(),"created_at":chrono::Utc::now().to_rfc3339(),"tenant_id":tenant,"namespace":"custom","name":name,"version":1,"blocks":[{"type":"step","id":"s1","handler":"noop","params":{}}]}))
        .send().await.unwrap().error_for_status().unwrap().json().await.unwrap()
}
async fn setup() -> (TestServer, reqwest::Client, Value, Value) {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    sequence(&client, &server, "t1", "original").await;
    let target = sequence(&client, &server, "t1", "replacement").await;
    let trigger = client.post(format!("{}/triggers", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"slug":"hook","sequence_name":"original","namespace":"custom","tenant_id":"t1","secret":"retained-secret","config":{"retained":true}}))
        .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
    (server, client, trigger, target)
}
async fn patch(
    client: &reqwest::Client,
    server: &TestServer,
    tenant: &str,
    body: Value,
) -> StatusCode {
    client
        .patch(format!("{}/triggers/hook/target", server.base_url))
        .header("X-Tenant-Id", tenant)
        .json(&body)
        .send()
        .await
        .unwrap()
        .status()
}
#[tokio::test]
async fn retarget_preserves_secret_config_and_disabled_state() {
    let (server, client, _, target) = setup().await;
    let tenant = TenantId::new("t1").unwrap();
    let mut original = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    original.enabled = false;
    server.storage.update_trigger(&original).await.unwrap();
    let before = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        patch(
            &client,
            &server,
            "t1",
            json!({"sequence_id":target["id"],"expected_updated_at":before.updated_at})
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let after = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.sequence_name, "replacement");
    assert_eq!(after.namespace, "custom");
    assert_eq!(after.version, Some(1));
    assert_eq!(after.secret.unwrap().expose(), "retained-secret");
    assert_eq!(after.config, before.config);
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(after.trigger_type, before.trigger_type);
    assert!(!after.enabled);
}
#[tokio::test]
async fn stale_target_update_is_rejected_without_overwriting_new_target() {
    let (server, client, trigger, target) = setup().await;
    let body = json!({"sequence_id":target["id"],"expected_updated_at":trigger["updated_at"]});
    assert_eq!(
        patch(&client, &server, "t1", body.clone()).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        patch(&client, &server, "t1", body).await,
        StatusCode::CONFLICT
    );
}
#[tokio::test]
async fn wrong_tenant_and_cross_tenant_sequence_are_rejected() {
    let (server, client, trigger, target) = setup().await;
    let body = json!({"sequence_id":target["id"],"expected_updated_at":trigger["updated_at"]});
    assert_eq!(
        patch(&client, &server, "t2", body).await,
        StatusCode::NOT_FOUND
    );
    let foreign = sequence(&client, &server, "t2", "foreign").await;
    assert_eq!(
        patch(
            &client,
            &server,
            "t1",
            json!({"sequence_id":foreign["id"],"expected_updated_at":trigger["updated_at"]})
        )
        .await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        server
            .storage
            .get_trigger(Some(&TenantId::new("t1").unwrap()), "hook")
            .await
            .unwrap()
            .unwrap()
            .sequence_name,
        "original"
    );
}
#[tokio::test]
async fn request_cannot_replace_credentials_or_other_settings() {
    let (server, client, trigger, target) = setup().await;
    assert_eq!(patch(&client, &server, "t1", json!({"sequence_id":target["id"],"expected_updated_at":trigger["updated_at"],"secret":"replacement"})).await, StatusCode::UNPROCESSABLE_ENTITY);
    let retained = server
        .storage
        .get_trigger(Some(&TenantId::new("t1").unwrap()), "hook")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.sequence_name, "original");
    assert_eq!(retained.secret.unwrap().expose(), "retained-secret");
}
#[tokio::test]
async fn concurrent_updates_with_one_receipt_have_one_winner() {
    let (server, client, trigger, target) = setup().await;
    let other = sequence(&client, &server, "t1", "other").await;
    let (first, second) = tokio::join!(
        patch(
            &client,
            &server,
            "t1",
            json!({"sequence_id":target["id"],"expected_updated_at":trigger["updated_at"]})
        ),
        patch(
            &client,
            &server,
            "t1",
            json!({"sequence_id":other["id"],"expected_updated_at":trigger["updated_at"]})
        ),
    );
    assert!(
        (first == StatusCode::NO_CONTENT && second == StatusCode::CONFLICT)
            || (second == StatusCode::NO_CONTENT && first == StatusCode::CONFLICT)
    );
}

#[tokio::test]
async fn encrypted_storage_cas_preserves_secret_encryption_and_rejects_stale_writes() {
    use orch8_storage::encrypting::EncryptingStorage;
    use orch8_types::encryption::FieldEncryptor;
    let (server, _, _, _) = setup().await;
    let tenant = TenantId::new("t1").unwrap();
    let mut trigger = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    trigger.slug = "encrypted-hook".into();
    let wrapped = EncryptingStorage::new(
        server.storage.clone(),
        FieldEncryptor::from_hex_key(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap(),
    );
    wrapped.create_trigger(&trigger).await.unwrap();
    let mut before = wrapped
        .get_trigger(Some(&tenant), "encrypted-hook")
        .await
        .unwrap()
        .unwrap();
    let expected = before.updated_at;
    before.sequence_name = "replacement".into();
    assert!(wrapped.update_trigger_cas(&before, expected).await.unwrap());
    assert!(!wrapped.update_trigger_cas(&before, expected).await.unwrap());
    let plain = wrapped
        .get_trigger(Some(&tenant), "encrypted-hook")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(plain.secret.unwrap().expose(), "retained-secret");
    let raw = server
        .storage
        .get_trigger(Some(&tenant), "encrypted-hook")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(raw.secret.unwrap().expose(), "retained-secret");
}

#[tokio::test]
async fn existing_webhook_secret_creates_an_instance_of_the_replacement() {
    let (server, client, trigger, target) = setup().await;
    assert_eq!(
        patch(
            &client,
            &server,
            "t1",
            json!({"sequence_id":target["id"],"expected_updated_at":trigger["updated_at"]})
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let timestamp = chrono::Utc::now().timestamp().to_string();
    let nonce = uuid::Uuid::now_v7().to_string();
    let body = json!({"sample":"retargeted"}).to_string();
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"retained-secret").unwrap();
    mac.update(format!("{timestamp}.{nonce}.").as_bytes());
    mac.update(body.as_bytes());
    let signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    let receipt: Value = client
        .post(format!("{}/webhooks/hook", server.base_url))
        .header("x-orch8-signature", format!("v1={signature}"))
        .header("x-trigger-timestamp", timestamp)
        .header("x-trigger-nonce", nonce)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let instance: Value = client
        .get(format!(
            "{}/instances/{}",
            server.base_url,
            receipt["instance_id"].as_str().unwrap()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(instance["sequence_id"], target["id"]);
    assert_eq!(instance["context"]["data"]["sample"], "retargeted");
}
