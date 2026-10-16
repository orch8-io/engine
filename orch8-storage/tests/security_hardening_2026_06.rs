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
//!   5. The command+wake path encrypts the payload too, while the device push
//!      token stays plaintext (the wake claim joins on it).
//!   6. The wrapper forwards the push-outbox methods whose trait defaults are
//!      silent no-ops (`enqueue_collapsible_wake` would not collapse,
//!      `prune_wakes` would delete nothing).
//!
//! ```text
//! cargo test -p orch8-storage --test security_hardening_2026_06
//! ```

use std::sync::Arc;

use chrono::{Duration, Utc};
use orch8_push::{CollapsibleWake, PushOutboxStore};

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

fn push_device() -> MobileDevice {
    MobileDevice {
        device_id: "device-a".into(),
        tenant_id: "tenant-a".into(),
        push_token: Some("token-a".into()),
        platform: "ios".into(),
        app_version: None,
        active: true,
        last_sync_at: None,
        registered_at: String::new(),
    }
}

fn collapsible(command_id: &str, offset_secs: i64) -> CollapsibleWake {
    CollapsibleWake {
        tenant_id: "tenant-a".into(),
        device_id: "device-a".into(),
        execution_id: "exec-a".into(),
        topic: "resume".into(),
        command_id: command_id.into(),
        created_at: Utc::now() + Duration::seconds(offset_secs),
    }
}

#[tokio::test]
async fn command_with_wake_payload_encrypted_at_rest_and_decrypted_for_delivery() {
    let (enc, inner) = encrypting().await;
    enc.register_mobile_device(&push_device()).await.unwrap();
    let secret_payload = r#"{"secret":"hunter2"}"#;
    enc.create_mobile_command_with_wake(
        &MobileCommand {
            id: "cmd-1".into(),
            device_id: "device-a".into(),
            command_type: "resume".into(),
            payload: secret_payload.into(),
            created_at: String::new(),
            acked_at: None,
        },
        "tenant-a",
        Utc::now(),
    )
    .await
    .unwrap();

    let raw = inner.fetch_pending_commands("device-a", 10).await.unwrap();
    assert_eq!(raw.len(), 1);
    assert!(
        !raw[0].payload.contains("hunter2"),
        "plaintext leaked into the stored command: {}",
        raw[0].payload
    );
    let delivered = enc.fetch_pending_commands("device-a", 10).await.unwrap();
    assert_eq!(delivered[0].payload, secret_payload);
}

#[tokio::test]
async fn device_push_token_is_stored_plaintext_so_wakes_can_be_claimed() {
    let (enc, inner) = encrypting().await;
    enc.register_mobile_device(&push_device()).await.unwrap();
    let now = Utc::now();
    enc.enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();

    let stored = inner.get_mobile_device("device-a").await.unwrap().unwrap();
    assert_eq!(stored.push_token.as_deref(), Some("token-a"));
    let wakes = enc
        .claim_due_wakes(now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(wakes.len(), 1);
    assert_eq!(wakes[0].push_token, "token-a");
}

#[tokio::test]
async fn wrapper_collapsible_wake_supersedes_older_pending_wake() {
    // The trait default enqueues without collapsing; both wakes would be due.
    let (enc, _inner) = encrypting().await;
    enc.register_mobile_device(&push_device()).await.unwrap();
    enc.enqueue_collapsible_wake(&collapsible("old", 0))
        .await
        .unwrap();
    let newer = enc
        .enqueue_collapsible_wake(&collapsible("new", 1))
        .await
        .unwrap();

    let at = Utc::now() + Duration::seconds(2);
    let wakes = enc
        .claim_due_wakes(at, at + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(wakes.len(), 1);
    assert_eq!(wakes[0].id, newer);
}

#[tokio::test]
async fn wrapper_prune_wakes_deletes_expired_rows() {
    // The trait default deletes nothing and returns 0.
    let (enc, _inner) = encrypting().await;
    enc.register_mobile_device(&push_device()).await.unwrap();
    enc.enqueue_wake(
        "tenant-a",
        "device-a",
        "old",
        Utc::now() - Duration::days(30),
    )
    .await
    .unwrap();
    enc.enqueue_wake("tenant-a", "device-a", "fresh", Utc::now())
        .await
        .unwrap();

    let pruned = enc
        .prune_wakes(Utc::now() - Duration::days(7))
        .await
        .unwrap();
    assert_eq!(pruned, 1);
    let now = Utc::now();
    let left = enc
        .claim_due_wakes(now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].command_id, "fresh");
}
