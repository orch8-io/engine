//! Encryption-at-rest end-to-end tests.

use std::sync::Arc;

use orch8_storage::encrypting::EncryptingStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::encryption::FieldEncryptor;
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};

const TEST_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const TEST_KEY_2: &str = "ffeeffeeffeeffeeffeeffeeffeeffeeffeeffeeffeeffeeffeeffeeffeeffee";

fn make_sequence() -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("test".into()),
        namespace: Namespace("default".into()),
        name: "enc_seq".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: orch8_types::ids::BlockId("s1".into()),
            handler: "noop".into(),
            params: serde_json::json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        }))],
        interceptors: None,
        created_at: chrono::Utc::now(),
    }
}

fn make_instance(seq_id: SequenceId) -> TaskInstance {
    let now = chrono::Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId("test".into()),
        namespace: Namespace("default".into()),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: orch8_types::context::ExecutionContext {
            data: serde_json::json!({"secret": "password123", "nested": {"key": "value"}}),
            config: serde_json::json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
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

#[tokio::test]
async fn encrypted_context_round_trip() {
    let inner = SqliteStorage::in_memory().await.unwrap();
    let encryptor = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(Arc::new(inner), encryptor);

    let seq = make_sequence();
    storage.create_sequence(&seq).await.unwrap();

    let inst = make_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    // Read back — decrypt should restore plaintext.
    let got = storage
        .get_instance(inst.id)
        .await
        .unwrap()
        .expect("instance exists");
    assert_eq!(got.context.data, inst.context.data);
    assert_eq!(got.context.data["secret"], "password123");
}

#[tokio::test]
async fn encryption_key_rotation() {
    // Keep a handle to the inner storage so we can re-wrap it with a new key.
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // Write with key 1.
    let encryptor1 = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage1 = EncryptingStorage::new(Arc::clone(&inner), encryptor1);

    let seq = make_sequence();
    storage1.create_sequence(&seq).await.unwrap();

    let inst = make_instance(seq.id);
    storage1.create_instance(&inst).await.unwrap();

    // Read back with key 2 + old key 1 fallback.
    let encryptor2 = FieldEncryptor::from_hex_key(TEST_KEY_2)
        .unwrap()
        .with_old_key(TEST_KEY)
        .unwrap();
    let storage2 = EncryptingStorage::new(Arc::clone(&inner), encryptor2);

    let got = storage2
        .get_instance(inst.id)
        .await
        .unwrap()
        .expect("instance exists");
    assert_eq!(got.context.data["secret"], "password123");
}

#[tokio::test]
async fn wrong_key_fails_with_clear_error() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let encryptor = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(Arc::clone(&inner), encryptor);

    let seq = make_sequence();
    storage.create_sequence(&seq).await.unwrap();

    let inst = make_instance(seq.id);
    storage.create_instance(&inst).await.unwrap();

    // Attempt to read with a different key (no fallback).
    let bad_encryptor = FieldEncryptor::from_hex_key(TEST_KEY_2).unwrap();
    let bad_storage = EncryptingStorage::new(Arc::clone(&inner), bad_encryptor);

    let result = bad_storage.get_instance(inst.id).await;
    assert!(result.is_err(), "wrong key should fail decryption");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("encryption") || err_msg.contains("decrypt"),
        "error should mention encryption: {err_msg}"
    );
}
