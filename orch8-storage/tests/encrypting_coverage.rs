#![allow(clippy::too_many_lines)]
//! `EncryptingStorage` decorator coverage.
//!
//! The decorator wraps a backend and transparently encrypts
//! `TaskInstance.context.data` on writes (`create_instance`,
//! `create_instances_batch`, `update_instance_context`) and decrypts on reads
//! (`get_instance`, `list_instances`, `claim_due_instances`). Pinning these
//! round-trip and at-rest invariants here protects against regressions where
//! a new `StorageBackend` method is added but the decorator forgets to
//! encrypt/decrypt, silently storing plaintext.

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use orch8_storage::encrypting::EncryptingStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::SequenceDefinition;

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ALT_KEY: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

fn mk_instance(tenant: &str, data: serde_json::Value) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId(tenant.into()),
        namespace: Namespace("default".into()),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext {
            data,
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    }
}

async fn seed_sequence(storage: &dyn StorageBackend, seq_id: SequenceId, tenant: &str) {
    let seq = SequenceDefinition {
        id: seq_id,
        tenant_id: TenantId(tenant.into()),
        namespace: Namespace("default".into()),
        name: "s".into(),
        version: 1,
        deprecated: false,
        blocks: vec![],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();
}

/// create → get round-trip: the caller sees the original plaintext, and the
/// inner (unwrapped) backend sees opaque ciphertext.
#[tokio::test]
async fn create_instance_encrypts_at_rest_and_decrypts_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let plaintext = json!({"ssn": "123-45-6789", "nested": {"k": "v"}});
    let instance = mk_instance("T1", plaintext.clone());
    seed_sequence(&storage, instance.sequence_id, "T1").await;
    storage.create_instance(&instance).await.unwrap();

    // Round-trip through the wrapper: plaintext restored.
    let got = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(got.context.data, plaintext);

    // At-rest: the inner backend sees an `enc:v1:...` sentinel, NOT plaintext.
    let raw = inner.get_instance(instance.id).await.unwrap().unwrap();
    assert!(
        FieldEncryptor::is_encrypted(&raw.context.data),
        "inner storage must see ciphertext marker, got: {}",
        raw.context.data
    );
    assert_ne!(raw.context.data, plaintext);
}

/// `update_instance_context` must re-encrypt the new context and keep reads
/// transparent. A missing encryption hook here would silently persist
/// plaintext on every update.
#[tokio::test]
async fn update_instance_context_re_encrypts() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("T1", json!({"v": 1}));
    seed_sequence(&storage, instance.sequence_id, "T1").await;
    storage.create_instance(&instance).await.unwrap();

    let new_ctx = ExecutionContext {
        data: json!({"updated": true, "secret": "shh"}),
        ..instance.context.clone()
    };
    storage
        .update_instance_context(instance.id, &new_ctx)
        .await
        .unwrap();

    let got = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(got.context.data["updated"], json!(true));
    assert_eq!(got.context.data["secret"], json!("shh"));

    let raw = inner.get_instance(instance.id).await.unwrap().unwrap();
    assert!(FieldEncryptor::is_encrypted(&raw.context.data));
}

/// Batch create must encrypt every instance in the batch. A regression where
/// the decorator forgot one element in a loop would surface here.
#[tokio::test]
async fn create_instances_batch_encrypts_every_element() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let a = mk_instance("T1", json!({"which": "a"}));
    let b = mk_instance("T1", json!({"which": "b", "more": [1, 2, 3]}));
    seed_sequence(&storage, a.sequence_id, "T1").await;
    seed_sequence(&storage, b.sequence_id, "T1").await;
    storage
        .create_instances_batch(&[a.clone(), b.clone()])
        .await
        .unwrap();

    for inst in [&a, &b] {
        let got = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(got.context.data, inst.context.data);
        let raw = inner.get_instance(inst.id).await.unwrap().unwrap();
        assert!(
            FieldEncryptor::is_encrypted(&raw.context.data),
            "batch element {:?} must be encrypted at rest",
            inst.id
        );
    }
}

/// `list_instances` decrypts every row it returns. The failure mode without
/// this hook is: clients receive unreadable `enc:v1:...` strings as data.
#[tokio::test]
async fn list_instances_decrypts_every_row() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let a = mk_instance("T1", json!({"n": 1}));
    let b = mk_instance("T1", json!({"n": 2}));
    seed_sequence(&storage, a.sequence_id, "T1").await;
    seed_sequence(&storage, b.sequence_id, "T1").await;
    storage.create_instance(&a).await.unwrap();
    storage.create_instance(&b).await.unwrap();

    let filter = InstanceFilter {
        tenant_id: Some(TenantId("T1".into())),
        ..Default::default()
    };
    let pagination = Pagination::default();
    let rows = storage.list_instances(&filter, &pagination).await.unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        // Every row must be decrypted — no ciphertext prefix should leak.
        assert!(
            !FieldEncryptor::is_encrypted(&row.context.data),
            "list returned ciphertext: {}",
            row.context.data
        );
        let n = row.context.data["n"].as_i64().unwrap();
        assert!(n == 1 || n == 2);
    }
}

/// `claim_due_instances` decrypts claimed rows. Without this, the scheduler
/// would see ciphertext when dispatching handlers.
#[tokio::test]
async fn claim_due_instances_decrypts_rows() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let mut a = mk_instance("T1", json!({"claim": "me"}));
    a.state = InstanceState::Scheduled;
    a.next_fire_at = Some(Utc::now() - chrono::Duration::seconds(1));
    seed_sequence(&storage, a.sequence_id, "T1").await;
    storage.create_instance(&a).await.unwrap();

    let claimed = storage
        .claim_due_instances(Utc::now(), 10, 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let row = &claimed[0];
    assert!(!FieldEncryptor::is_encrypted(&row.context.data));
    assert_eq!(row.context.data["claim"], json!("me"));
}

/// Key rotation: an encryptor configured with the old key as the rotation
/// fallback can still decrypt rows encrypted under that old key, while new
/// writes go through the new primary key.
#[tokio::test]
async fn key_rotation_reads_old_rows_writes_new_rows() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // Phase 1: write with OLD key.
    let old = FieldEncryptor::from_hex_key(ALT_KEY).unwrap();
    let old_storage = EncryptingStorage::new(inner.clone(), old);
    let instance = mk_instance("T1", json!({"era": "old"}));
    seed_sequence(&old_storage, instance.sequence_id, "T1").await;
    old_storage.create_instance(&instance).await.unwrap();

    // Phase 2: switch to NEW key with ALT as rotation fallback.
    let rotated = FieldEncryptor::from_hex_key(TEST_KEY)
        .unwrap()
        .with_old_key(ALT_KEY)
        .unwrap();
    let rotated_storage = EncryptingStorage::new(inner.clone(), rotated);

    // Read of old-encrypted row must succeed via fallback.
    let got = rotated_storage
        .get_instance(instance.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.context.data["era"], json!("old"));

    // New write goes through the NEW primary key: re-save triggers encryption
    // under the primary key.
    let new_ctx = ExecutionContext {
        data: json!({"era": "new"}),
        ..instance.context.clone()
    };
    rotated_storage
        .update_instance_context(instance.id, &new_ctx)
        .await
        .unwrap();

    // A fresh encryptor with ONLY the new key must be able to read it back —
    // proving the new write is in the new key's ciphertext namespace.
    let new_only = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let new_only_storage = EncryptingStorage::new(inner.clone(), new_only);
    let got = new_only_storage
        .get_instance(instance.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.context.data["era"], json!("new"));
}

/// Reading a non-existent instance returns `None` without any decrypt
/// attempt. Guards against a future regression where the decorator might
/// call `decrypt_value` on a placeholder and raise a spurious error.
#[tokio::test]
async fn get_missing_instance_returns_none() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    let got = storage.get_instance(InstanceId::new()).await.unwrap();
    assert!(got.is_none());
}

/// Unencrypted plaintext rows written directly to the inner backend must be
/// passed through untouched on read (via `FieldEncryptor::is_encrypted` guard).
/// This matters during a rollout where encryption is turned on: pre-existing
/// rows must remain readable without migration.
#[tokio::test]
async fn decrypt_skips_plaintext_rows() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance = mk_instance("T1", json!({"plain": true}));
    seed_sequence(inner.as_ref(), instance.sequence_id, "T1").await;
    // Write WITHOUT the decorator — simulates pre-encryption rollout data.
    inner.create_instance(&instance).await.unwrap();

    // Now wrap and read. The guard `is_encrypted` must be false for plaintext,
    // so the decorator returns the value unchanged.
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);
    let got = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(got.context.data["plain"], json!(true));
}
