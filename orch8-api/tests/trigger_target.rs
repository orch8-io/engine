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

async fn settings(
    client: &reqwest::Client,
    server: &TestServer,
    tenant: &str,
    body: Value,
) -> reqwest::Response {
    client
        .patch(format!("{}/triggers/hook/settings", server.base_url))
        .header("X-Tenant-Id", tenant)
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn settings_merge_updates_state_and_preserves_unmodified_private_fields() {
    let (server, client, _, _) = setup().await;
    let tenant = TenantId::new("t1").unwrap();
    let mut original = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    original.config = json!({"auth":{"token":"private-token", "legacy":true}, "topics":["previous"], "retained":true});
    server.storage.update_trigger(&original).await.unwrap();
    let original = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    let request = json!({
        "expected_created_at": original.created_at, "expected_updated_at": original.updated_at,
        "enabled":false, "config_patch":{"auth":{"legacy":null, "mode":"current"}, "topics":["one"]}
    });
    assert_eq!(
        settings(&client, &server, "other", request.clone())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let reply: Value = settings(&client, &server, "t1", request.clone())
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let stored = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, json!({"updated_at":stored.updated_at}));
    assert!(!stored.enabled);
    assert_eq!(
        stored.config,
        json!({"auth":{"token":"private-token","mode":"current"},"topics":["one"],"retained":true})
    );
    assert_eq!(stored.secret.unwrap().expose(), "retained-secret");
    assert_eq!(stored.sequence_name, original.sequence_name);
    assert_eq!(
        settings(&client, &server, "t1", request).await.status(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn settings_secret_replacement_and_clear_are_revision_guarded() {
    let (server, client, trigger, _) = setup().await;
    let tenant = TenantId::new("t1").unwrap();
    let replacement = json!({"expected_created_at": trigger["created_at"],"expected_updated_at": trigger["updated_at"],
        "secret_change":{"mode":"replace","value":"replacement-secret"}});
    let response = settings(&client, &server, "t1", replacement)
        .await
        .error_for_status()
        .unwrap();
    let receipt: Value = response.json().await.unwrap();
    let stored = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.secret.unwrap().expose(), "replacement-secret");
    assert_eq!(receipt, json!({"updated_at":stored.updated_at}));
    let mut clear = json!({"expected_created_at": trigger["updated_at"], "expected_updated_at": receipt["updated_at"], "secret_change":{"mode":"clear"}});
    clear["expected_created_at"] = json!("2025-01-01T00:00:00Z");
    assert_eq!(
        settings(&client, &server, "t1", clear.clone())
            .await
            .status(),
        StatusCode::CONFLICT
    );
    clear["expected_created_at"] = trigger["created_at"].clone();
    assert_eq!(
        settings(&client, &server, "t1", clear).await.status(),
        StatusCode::OK
    );
    assert!(
        server
            .storage
            .get_trigger(Some(&tenant), "hook")
            .await
            .unwrap()
            .unwrap()
            .secret
            .is_none()
    );
}

#[tokio::test]
async fn trigger_inventory_pages_and_searches_past_the_legacy_ceiling() {
    let (server, client, _, _) = setup().await;
    let tenant = TenantId::new("t1").unwrap();
    let original = server
        .storage
        .get_trigger(Some(&tenant), "hook")
        .await
        .unwrap()
        .unwrap();
    for index in 0..1005 {
        let mut row = original.clone();
        row.slug = format!("row-{index:04}");
        row.config = json!({"private": "PRIVATE-CURSOR"});
        server.storage.create_trigger(&row).await.unwrap();
    }
    let mut other = original;
    other.slug = "row-other".into();
    other.tenant_id = TenantId::new("t-other").unwrap();
    server.storage.create_trigger(&other).await.unwrap();

    let url = format!("{}/triggers", server.base_url);
    let legacy: Value = client
        .get(&url)
        .header("X-Tenant-Id", "t1")
        .query(&[("limit", "1000")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(legacy.as_array().unwrap().len(), 1000);
    let mut after = None::<String>;
    let mut slugs = vec![];
    for _ in 0..6 {
        let mut request = client
            .get(&url)
            .header("X-Tenant-Id", "t1")
            .query(&[("view", "page"), ("page_size", "200")]);
        if let Some(after) = &after {
            request = request.query(&[("after", after)]);
        }
        let page: Value = request
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(page["after"], json!(after));
        let rows = page["items"].as_array().unwrap();
        for row in rows {
            assert_eq!(row["tenant_id"], "t1");
            assert_eq!(row["secret_field_present"], true);
            assert!(row.get("secret").is_none() && row.get("config").is_none());
            slugs.push(row["slug"].as_str().unwrap().to_owned());
        }
        after = page["next_after"].as_str().map(str::to_owned);
        if after.is_none() {
            break;
        }
        assert_eq!(after.as_ref(), slugs.last());
    }
    assert_eq!(after, None);
    assert_eq!(slugs.len(), 1006);
    assert!(slugs.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(slugs.last().unwrap(), "row-1004");
    let filtered: Value = client
        .get(&url)
        .header("X-Tenant-Id", "t1")
        .query(&[("view", "page"), ("page_size", "1"), ("search", "row-100")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered["search"], "row-100");
    assert_eq!(filtered["items"][0]["slug"], "row-1000");
    assert_eq!(filtered["next_after"], "row-1000");
}
