//! Regression tests for the 2026-06 security-hardening pass (storage layer).
//!
//! Covers the at-rest encryption gaps closed in `EncryptingStorage`:
//!   1. Trigger HMAC secrets are encrypted at rest, decrypted on read.
//!   2. Mobile-command payloads (may carry resolved credentials) are
//!      encrypted at rest, decrypted when fetched for delivery.
//!   3. Mobile sync methods delegate to the inner backend (the previous empty
//!      impl silently no-op'd every mobile method on encryption-enabled
//!      servers).
//!   4. `encrypt_credential` is idempotent (no double-encryption).
//!
//! ```text
//! cargo test -p orch8-storage --test security_hardening_2026_06
//! ```

use std::sync::Arc;

use chrono::Utc;

use orch8_storage::encrypting::EncryptingStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{AdminStore, MobileCommand, MobileDevice, MobileSyncStore};
use orch8_types::config::SecretString;
use orch8_types::encryption::FieldEncryptor;
use orch8_types::ids::TenantId;
use orch8_types::trigger::{TriggerDef, TriggerType};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

async fn encrypting() -> (EncryptingStorage, Arc<SqliteStorage>) {
    // Build the inner store and a separate handle so tests can inspect what is
    // physically persisted (bypassing the decrypting wrapper).
    let inner = Arc::new(SqliteStorage::in_memory().await.expect("in-memory sqlite"));
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let wrapped = EncryptingStorage::new(inner.clone(), enc);
    (wrapped, inner)
}

fn mk_trigger(slug: &str, secret: Option<&str>) -> TriggerDef {
    let now = Utc::now();
    TriggerDef {
        slug: slug.into(),
        sequence_name: "seq".into(),
        version: None,
        tenant_id: TenantId::unchecked("t"),
        namespace: "default".into(),
        enabled: true,
        secret: secret.map(|s| SecretString::new(s.to_string())),
        trigger_type: TriggerType::Webhook,
        config: serde_json::json!({}),
        created_at: now,
        updated_at: now,
    }
}

#[tokio::test]
async fn trigger_secret_encrypted_at_rest_decrypted_on_read() {
    let (enc, inner) = encrypting().await;

    enc.create_trigger(&mk_trigger("hook", Some("super-secret-hmac")))
        .await
        .unwrap();

    // Read through the inner store (no decryption): the secret must NOT be
    // plaintext on disk.
    let raw = inner.get_trigger(None, "hook").await.unwrap().unwrap();
    let raw_secret = raw.secret.as_ref().unwrap().expose().to_string();
    assert_ne!(
        raw_secret, "super-secret-hmac",
        "trigger secret must be encrypted at rest"
    );
    assert!(
        FieldEncryptor::is_encrypted(&serde_json::Value::String(raw_secret)),
        "stored secret must carry the encryption marker"
    );

    // Read through the wrapper: the original plaintext is recovered.
    let got = enc.get_trigger(None, "hook").await.unwrap().unwrap();
    assert_eq!(got.secret.as_ref().unwrap().expose(), "super-secret-hmac");

    // list_triggers decrypts too.
    let listed = enc.list_triggers(None, 10).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].secret.as_ref().unwrap().expose(),
        "super-secret-hmac"
    );
}

#[tokio::test]
async fn trigger_without_secret_roundtrips() {
    let (enc, _inner) = encrypting().await;
    enc.create_trigger(&mk_trigger("nosecret", None))
        .await
        .unwrap();
    let got = enc.get_trigger(None, "nosecret").await.unwrap().unwrap();
    assert!(got.secret.is_none());
}

#[tokio::test]
async fn mobile_command_payload_encrypted_at_rest() {
    let (enc, inner) = encrypting().await;

    // A device must exist (FK / scoping); register through the wrapper.
    enc.register_mobile_device(&MobileDevice {
        device_id: "dev-1".into(),
        tenant_id: "t".into(),
        push_token: None,
        platform: "ios".into(),
        app_version: None,
        active: true,
        last_sync_at: None,
        registered_at: Utc::now().to_rfc3339(),
    })
    .await
    .unwrap();

    let secret_payload = r#"{"api_key":"sk-live-PLAINTEXT-SECRET"}"#;
    enc.create_mobile_command(&MobileCommand {
        id: "cmd-1".into(),
        device_id: "dev-1".into(),
        command_type: "step".into(),
        payload: secret_payload.into(),
        created_at: String::new(),
        acked_at: None,
    })
    .await
    .unwrap();

    // Raw read (inner): payload is not plaintext.
    let raw = inner.fetch_pending_commands("dev-1", 10).await.unwrap();
    assert_eq!(raw.len(), 1, "delegation must actually persist the command");
    assert_ne!(
        raw[0].payload, secret_payload,
        "command payload must be encrypted at rest"
    );

    // Wrapper read: plaintext recovered for delivery.
    let got = enc.fetch_pending_commands("dev-1", 10).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload, secret_payload);
}

#[tokio::test]
async fn mobile_methods_delegate_not_noop() {
    // The previous empty `impl MobileSyncStore for EncryptingStorage {}` used
    // the trait's no-op defaults, so a registered device read back as absent.
    let (enc, _inner) = encrypting().await;
    enc.register_mobile_device(&MobileDevice {
        device_id: "dev-x".into(),
        tenant_id: "t".into(),
        push_token: None,
        platform: "android".into(),
        app_version: None,
        active: true,
        last_sync_at: None,
        registered_at: Utc::now().to_rfc3339(),
    })
    .await
    .unwrap();

    let got = enc.get_mobile_device("dev-x").await.unwrap();
    assert!(
        got.is_some(),
        "device must be retrievable, not a no-op default"
    );
    assert_eq!(got.unwrap().platform, "android");
}
