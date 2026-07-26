//! Coverage tests for the encrypting wrapper's mobile push paths: command
//! payload encryption on `create_mobile_command_with_wake` and the
//! `PushOutboxStore` pass-throughs added with the wake outbox.
//!
//! Count contract: 15 independently named unit tests.

use super::*;
use crate::sqlite::SqliteStorage;
use crate::{MobileCommand, MobileDevice, MobileSyncStore};
use chrono::Duration;
use orch8_push::{PushOutboxStore, WakeAttemptOutcome};

fn encryptor() -> FieldEncryptor {
    FieldEncryptor::from_hex_key("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        .unwrap()
}

async fn stack() -> (Arc<SqliteStorage>, EncryptingStorage) {
    let inner = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let wrapped = EncryptingStorage::new(inner.clone(), encryptor());
    // Registration goes through the wrapper (a plaintext pass-through) so
    // tests assert on what the wrapper actually stores, not on a bypassed
    // inner store.
    wrapped
        .register_mobile_device(&MobileDevice {
            device_id: "device-a".into(),
            tenant_id: "tenant-a".into(),
            push_token: Some("token-a".into()),
            platform: "ios".into(),
            app_version: None,
            active: true,
            last_sync_at: None,
            registered_at: String::new(),
        })
        .await
        .unwrap();
    (inner, wrapped)
}

fn command(id: &str, payload: &str) -> MobileCommand {
    MobileCommand {
        id: id.into(),
        device_id: "device-a".into(),
        command_type: "resume".into(),
        payload: payload.into(),
        created_at: String::new(),
        acked_at: None,
    }
}

async fn raw_payload(inner: &SqliteStorage, command_id: &str) -> String {
    sqlx::query_scalar("SELECT payload FROM mobile_commands WHERE id=?")
        .bind(command_id)
        .fetch_one(&inner.pool)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// create_mobile_command_with_wake: payload encrypted at rest, wake persisted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_enc_push_001_payload_is_encrypted_at_rest() {
    let (inner, wrapped) = stack().await;
    wrapped
        .create_mobile_command_with_wake(
            &command("cmd-1", "{\"secret\":\"hunter2\"}"),
            "tenant-a",
            Utc::now(),
        )
        .await
        .unwrap();
    let stored = raw_payload(&inner, "cmd-1").await;
    assert!(
        stored.starts_with("enc:v1:"),
        "expected ciphertext, got {stored}"
    );
    assert!(!stored.contains("hunter2"), "plaintext leaked into the row");
}

#[tokio::test]
async fn coverage_enc_push_002_wake_row_uses_returned_id() {
    let (inner, wrapped) = stack().await;
    let wake_id = wrapped
        .create_mobile_command_with_wake(&command("cmd-1", "{}"), "tenant-a", Utc::now())
        .await
        .unwrap();
    let stored: String =
        sqlx::query_scalar("SELECT id FROM push_wake_outbox WHERE command_id='cmd-1'")
            .fetch_one(&inner.pool)
            .await
            .unwrap();
    assert_eq!(wake_id.to_string(), stored);
}

#[tokio::test]
async fn coverage_enc_push_003_fetch_decrypts_payload_round_trip() {
    let (_, wrapped) = stack().await;
    wrapped
        .create_mobile_command_with_wake(
            &command("cmd-1", "{\"secret\":\"hunter2\"}"),
            "tenant-a",
            Utc::now(),
        )
        .await
        .unwrap();
    let pending = wrapped
        .fetch_pending_commands("device-a", 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].payload, "{\"secret\":\"hunter2\"}");
}

#[tokio::test]
async fn coverage_enc_push_004_unicode_payload_round_trips() {
    let (_, wrapped) = stack().await;
    let payload = "{\"note\":\"デバイスを再起動 🚀\"}";
    wrapped
        .create_mobile_command_with_wake(&command("cmd-u", payload), "tenant-a", Utc::now())
        .await
        .unwrap();
    let pending = wrapped
        .fetch_pending_commands("device-a", 10)
        .await
        .unwrap();
    assert_eq!(pending[0].payload, payload);
}

#[tokio::test]
async fn coverage_enc_push_005_large_payload_round_trips() {
    let (inner, wrapped) = stack().await;
    let payload = format!("{{\"blob\":\"{}\"}}", "x".repeat(10_000));
    wrapped
        .create_mobile_command_with_wake(&command("cmd-big", &payload), "tenant-a", Utc::now())
        .await
        .unwrap();
    assert!(raw_payload(&inner, "cmd-big").await.starts_with("enc:v1:"));
    let pending = wrapped
        .fetch_pending_commands("device-a", 10)
        .await
        .unwrap();
    assert_eq!(pending[0].payload, payload);
}

#[tokio::test]
async fn coverage_enc_push_006_empty_payload_round_trips() {
    let (_, wrapped) = stack().await;
    wrapped
        .create_mobile_command_with_wake(&command("cmd-empty", ""), "tenant-a", Utc::now())
        .await
        .unwrap();
    let pending = wrapped
        .fetch_pending_commands("device-a", 10)
        .await
        .unwrap();
    assert_eq!(pending[0].payload, "");
}

#[tokio::test]
async fn coverage_enc_push_007_commands_decrypt_to_their_own_payloads() {
    let (_, wrapped) = stack().await;
    wrapped
        .create_mobile_command_with_wake(&command("cmd-1", "{\"n\":1}"), "tenant-a", Utc::now())
        .await
        .unwrap();
    wrapped
        .create_mobile_command_with_wake(&command("cmd-2", "{\"n\":2}"), "tenant-a", Utc::now())
        .await
        .unwrap();
    let pending = wrapped
        .fetch_pending_commands("device-a", 10)
        .await
        .unwrap();
    let by_id: std::collections::HashMap<_, _> = pending
        .iter()
        .map(|cmd| (cmd.id.as_str(), cmd.payload.as_str()))
        .collect();
    assert_eq!(by_id.get("cmd-1"), Some(&"{\"n\":1}"));
    assert_eq!(by_id.get("cmd-2"), Some(&"{\"n\":2}"));
}

// ---------------------------------------------------------------------------
// PushOutboxStore pass-throughs reach the inner backend unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_enc_push_008_enqueue_wake_passthrough() {
    let (inner, wrapped) = stack().await;
    let id = wrapped
        .enqueue_wake("tenant-a", "device-a", "cmd-1", Utc::now())
        .await
        .unwrap();
    let (stored, status): (String, String) =
        sqlx::query_as("SELECT id, status FROM push_wake_outbox WHERE command_id='cmd-1'")
            .fetch_one(&inner.pool)
            .await
            .unwrap();
    assert_eq!(id.to_string(), stored);
    assert_eq!(status, "pending");
}

#[tokio::test]
async fn coverage_enc_push_009_claim_due_wakes_passthrough() {
    let (_, wrapped) = stack().await;
    let now = Utc::now();
    wrapped
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wakes = wrapped
        .claim_due_wakes(now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(wakes.len(), 1);
    assert_eq!(wakes[0].push_token, "token-a");
}

#[tokio::test]
async fn coverage_enc_push_010_record_wake_outcome_passthrough() {
    let (inner, wrapped) = stack().await;
    let now = Utc::now();
    wrapped
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = wrapped
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    wrapped
        .record_wake_outcome(&wake, &WakeAttemptOutcome::Delivered, now)
        .await
        .unwrap();
    let status: String =
        sqlx::query_scalar("SELECT status FROM push_wake_outbox WHERE command_id='cmd-1'")
            .fetch_one(&inner.pool)
            .await
            .unwrap();
    assert_eq!(status, "delivered");
}

#[tokio::test]
async fn coverage_enc_push_011_record_command_acks_passthrough() {
    let (inner, wrapped) = stack().await;
    let now = Utc::now();
    wrapped
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let acked = wrapped
        .record_command_acks("device-a", &["cmd-1".into()], now)
        .await
        .unwrap();
    assert_eq!(acked, 1);
    let marked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM push_wake_outbox WHERE command_acked_at IS NOT NULL",
    )
    .fetch_one(&inner.pool)
    .await
    .unwrap();
    assert_eq!(marked, 1);
}

#[tokio::test]
async fn coverage_enc_push_012_collapsible_wake_uses_trait_default() {
    // The wrapper does not override `enqueue_collapsible_wake`; the trait
    // default must still land a row (no collapse) rather than trap.
    let (inner, wrapped) = stack().await;
    let now = Utc::now();
    let id = wrapped
        .enqueue_collapsible_wake(&orch8_push::CollapsibleWake {
            tenant_id: "tenant-a".into(),
            device_id: "device-a".into(),
            execution_id: "exec-a".into(),
            topic: "resume".into(),
            command_id: "cmd-1".into(),
            created_at: now,
        })
        .await
        .unwrap();
    let stored: String =
        sqlx::query_scalar("SELECT id FROM push_wake_outbox WHERE command_id='cmd-1'")
            .fetch_one(&inner.pool)
            .await
            .unwrap();
    assert_eq!(id.to_string(), stored);
}

#[tokio::test]
async fn coverage_enc_push_013_stale_lease_outcome_still_fenced_through_wrapper() {
    let (_, wrapped) = stack().await;
    let now = Utc::now();
    wrapped
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let first = wrapped
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    let reclaimed_at = now + Duration::seconds(31);
    wrapped
        .claim_due_wakes(reclaimed_at, reclaimed_at + Duration::seconds(30), 1)
        .await
        .unwrap();
    assert!(
        wrapped
            .record_wake_outcome(&first, &WakeAttemptOutcome::Delivered, reclaimed_at)
            .await
            .is_err(),
        "the lease CAS must survive the encrypting wrapper"
    );
}

#[tokio::test]
async fn coverage_enc_push_014_device_registration_is_plaintext_passthrough() {
    // Device rows carry no secret-bearing columns; the wrapper must not
    // encrypt the push token (the drain loop reads it directly). The
    // `stack()` fixture registers through the wrapper, so the raw row
    // below reflects the wrapper's pass-through behavior.
    let (inner, _) = stack().await;
    let token: String =
        sqlx::query_scalar("SELECT push_token FROM mobile_devices WHERE device_id='device-a'")
            .fetch_one(&inner.pool)
            .await
            .unwrap();
    assert_eq!(token, "token-a");
}

#[tokio::test]
async fn coverage_enc_push_015_create_mobile_command_encrypts_payload_at_rest() {
    // The wake-less command path goes through the same encrypt-then-store
    // wrapper override as create_mobile_command_with_wake.
    let (inner, wrapped) = stack().await;
    wrapped
        .create_mobile_command(&command("cmd-plain", "{\"secret\":\"hunter2\"}"))
        .await
        .unwrap();
    let stored = raw_payload(&inner, "cmd-plain").await;
    assert!(
        stored.starts_with("enc:v1:"),
        "expected ciphertext, got {stored}"
    );
    assert!(!stored.contains("hunter2"), "plaintext leaked into the row");
    let pending = wrapped
        .fetch_pending_commands("device-a", 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].payload, "{\"secret\":\"hunter2\"}");
}
