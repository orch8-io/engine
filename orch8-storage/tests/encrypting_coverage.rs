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
use uuid::Uuid;

use orch8_storage::encrypting::EncryptingStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    AdminStore, InstanceStore, OutputStore, ResourceStore, SequenceStore, SignalStore,
    StorageBackend, WorkerStore,
};
use orch8_types::checkpoint::Checkpoint;
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use orch8_types::signal::{Signal, SignalType};
use orch8_types::step_log::StepLogEntry;
use orch8_types::worker::{WorkerTask, WorkerTaskState};

const TEST_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ALT_KEY: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

fn mk_instance(tenant: &str, data: serde_json::Value) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
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
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

async fn seed_sequence(storage: &dyn StorageBackend, seq_id: SequenceId, tenant: &str) {
    let seq = SequenceDefinition {
        id: seq_id,
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        name: "s".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
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
        tenant_id: Some(TenantId::unchecked("T1")),
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

// ==========================================================================
// Credential encryption coverage
// ==========================================================================

use orch8_types::config::SecretString;
use orch8_types::credential::{CredentialDef, CredentialKind};

fn mk_credential(id: &str, tenant: &str, value: &str) -> CredentialDef {
    let now = Utc::now();
    CredentialDef {
        id: id.into(),
        tenant_id: tenant.into(),
        name: format!("Test {id}"),
        kind: CredentialKind::ApiKey,
        value: SecretString::new(value.into()),
        expires_at: None,
        refresh_url: None,
        refresh_token: None,
        enabled: true,
        description: None,
        created_at: now,
        updated_at: now,
    }
}

/// create → get round-trip: credential value is encrypted at rest but
/// decrypted transparently on read.
#[tokio::test]
async fn credential_value_encrypted_at_rest_and_decrypted_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let secret = r#"{"token":"sk_live_super_secret_123"}"#;
    let cred = mk_credential("stripe-prod", "T1", secret);
    storage.create_credential(&cred).await.unwrap();

    // Round-trip through the wrapper: plaintext restored.
    let got = storage
        .get_credential(None, "stripe-prod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.value.expose(), secret);

    // At-rest: the inner backend sees encrypted ciphertext.
    let raw = inner
        .get_credential(None, "stripe-prod")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        raw.value.expose(),
        secret,
        "raw DB must not contain plaintext"
    );
    assert!(
        raw.value.expose().starts_with("enc:v1:"),
        "raw value must carry encryption prefix, got: {}",
        &raw.value.expose()[..raw.value.expose().len().min(30)]
    );
}

/// `update_credential` re-encrypts the new value.
#[tokio::test]
async fn credential_update_re_encrypts_value() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let cred = mk_credential("key1", "T1", "original-secret");
    storage.create_credential(&cred).await.unwrap();

    let mut updated = cred.clone();
    updated.value = SecretString::new("new-secret-value".into());
    storage.update_credential(&updated).await.unwrap();

    // Decrypted read returns the new value.
    let got = storage.get_credential(None, "key1").await.unwrap().unwrap();
    assert_eq!(got.value.expose(), "new-secret-value");

    // Raw still encrypted.
    let raw = inner.get_credential(None, "key1").await.unwrap().unwrap();
    assert!(raw.value.expose().starts_with("enc:v1:"));
    assert!(!raw.value.expose().contains("new-secret-value"));
}

/// `refresh_token` field is also encrypted at rest.
#[tokio::test]
async fn credential_refresh_token_encrypted_at_rest() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let now = Utc::now();
    let cred = CredentialDef {
        id: "google-oauth".into(),
        tenant_id: "T1".into(),
        name: "Google".into(),
        kind: CredentialKind::Oauth2,
        value: SecretString::new(r#"{"access_token":"at_123"}"#.into()),
        expires_at: Some(now + chrono::Duration::hours(1)),
        refresh_url: Some("https://oauth2.googleapis.com/token".into()),
        refresh_token: Some(SecretString::new("rt_super_secret".into())),
        enabled: true,
        description: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_credential(&cred).await.unwrap();

    // Round-trip: both value and refresh_token decrypted.
    let got = storage
        .get_credential(None, "google-oauth")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.value.expose(), r#"{"access_token":"at_123"}"#);
    assert_eq!(
        got.refresh_token.as_ref().unwrap().expose(),
        "rt_super_secret"
    );

    // At-rest: both fields encrypted.
    let raw = inner
        .get_credential(None, "google-oauth")
        .await
        .unwrap()
        .unwrap();
    assert!(raw.value.expose().starts_with("enc:v1:"));
    assert!(
        raw.refresh_token
            .as_ref()
            .unwrap()
            .expose()
            .starts_with("enc:v1:")
    );
}

/// `list_credentials` decrypts all returned credentials.
#[tokio::test]
async fn list_credentials_decrypts_all() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    storage
        .create_credential(&mk_credential("c1", "T1", "secret-1"))
        .await
        .unwrap();
    storage
        .create_credential(&mk_credential("c2", "T1", "secret-2"))
        .await
        .unwrap();

    let creds = storage
        .list_credentials(Some(&TenantId::unchecked("T1")), 1000)
        .await
        .unwrap();
    assert_eq!(creds.len(), 2);
    for c in &creds {
        assert!(
            !c.value.expose().starts_with("enc:v1:"),
            "list should return decrypted values"
        );
    }
    let values: Vec<&str> = creds.iter().map(|c| c.value.expose()).collect();
    assert!(values.contains(&"secret-1"));
    assert!(values.contains(&"secret-2"));
}

/// Pre-existing plaintext credentials (written before encryption was enabled)
/// are returned unchanged without a decryption error.
#[tokio::test]
async fn credential_plaintext_passthrough_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // Write directly to inner (simulates pre-encryption data).
    let cred = mk_credential("legacy", "T1", "plaintext-token");
    inner.create_credential(&cred).await.unwrap();

    // Now wrap with encryption and read.
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);
    let got = storage
        .get_credential(None, "legacy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.value.expose(), "plaintext-token");
}

/// Key rotation: credentials encrypted with the old key can still be read
/// after switching to a new primary key with the old key as fallback.
#[tokio::test]
async fn credential_key_rotation() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // Phase 1: encrypt with ALT_KEY.
    let old_enc = FieldEncryptor::from_hex_key(ALT_KEY).unwrap();
    let old_storage = EncryptingStorage::new(inner.clone(), old_enc);
    old_storage
        .create_credential(&mk_credential("rotated", "T1", "old-era-secret"))
        .await
        .unwrap();

    // Phase 2: switch to TEST_KEY with ALT_KEY as fallback.
    let rotated = FieldEncryptor::from_hex_key(TEST_KEY)
        .unwrap()
        .with_old_key(ALT_KEY)
        .unwrap();
    let rotated_storage = EncryptingStorage::new(inner.clone(), rotated);

    // Read old-encrypted credential via fallback.
    let got = rotated_storage
        .get_credential(None, "rotated")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.value.expose(), "old-era-secret");

    // New write uses the new primary key.
    let mut updated = got.clone();
    updated.value = SecretString::new("new-era-secret".into());
    rotated_storage.update_credential(&updated).await.unwrap();

    // Verify the new write is under the new key (read with new key alone).
    let new_only = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let new_only_storage = EncryptingStorage::new(inner, new_only);
    let got = new_only_storage
        .get_credential(None, "rotated")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.value.expose(), "new-era-secret");
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

// ---------------------------------------------------------------------------
// Issue #23 regression: merge_context_data on encrypted rows must preserve
// prior context data.
// ---------------------------------------------------------------------------
//
// Before the fix the decorator encrypted only the new value and then
// delegated to the inner `merge_context_data`, which runs a SQL-side
// `jsonb_set(context, ARRAY['data', key], value)` with a CASE guard that
// replaces non-object `data` with `{}`. On encrypted rows `data` is the
// ciphertext string `"enc:v1:..."` — not an object — so the guard wiped
// the entire existing (encrypted) context. That is irreversible data loss:
// the ciphertext is gone and no decrypt can recover it.

/// Merging a new key into an encrypted instance must not clobber prior
/// keys already present in `context.data`. Seeds an encrypted row with
/// two keys, merges a third, and asserts all three are readable afterwards.
#[tokio::test]
async fn merge_context_data_preserves_prior_keys_when_encrypted() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("T1", json!({"alpha": 1, "beta": "two"}));
    seed_sequence(inner.as_ref(), instance.sequence_id, "T1").await;
    storage.create_instance(&instance).await.unwrap();

    // Merge a third key.
    storage
        .merge_context_data(instance.id, "gamma", &json!(3))
        .await
        .unwrap();

    // All three keys must survive — the old merge path would have left only
    // `gamma` because it wiped the ciphertext first.
    let got = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(got.context.data["alpha"], json!(1));
    assert_eq!(got.context.data["beta"], json!("two"));
    assert_eq!(got.context.data["gamma"], json!(3));
}

/// At-rest check: after a merge, the inner backend must still see a single
/// ciphertext string (not a JSONB object and not plaintext). Guards against
/// a future refactor that forgets to re-encrypt the merged payload.
#[tokio::test]
async fn merge_context_data_re_encrypts_at_rest() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("T1", json!({"alpha": 1}));
    seed_sequence(inner.as_ref(), instance.sequence_id, "T1").await;
    storage.create_instance(&instance).await.unwrap();

    storage
        .merge_context_data(instance.id, "beta", &json!("added"))
        .await
        .unwrap();

    // Read via the unwrapped backend — data must be a ciphertext string
    // carrying the `enc:v1:` prefix, not a JSON object.
    let raw = inner.get_instance(instance.id).await.unwrap().unwrap();
    assert!(
        FieldEncryptor::is_encrypted(&raw.context.data),
        "at-rest data must remain encrypted after merge; got {:?}",
        raw.context.data
    );
}

/// Merging into a row whose `context.data` is not an object (e.g. seeded as
/// `null` or a scalar via a direct inner write) must normalise to `{}` and
/// insert the key. Mirrors the SQL CASE-guard in the plaintext path so
/// encrypted and non-encrypted modes produce identical observable results.
#[tokio::test]
async fn merge_context_data_normalises_non_object_when_encrypted() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    // Seed with `data: null` via the decorator (gets encrypted as null).
    let instance = mk_instance("T1", serde_json::Value::Null);
    seed_sequence(inner.as_ref(), instance.sequence_id, "T1").await;
    storage.create_instance(&instance).await.unwrap();

    storage
        .merge_context_data(instance.id, "k", &json!("v"))
        .await
        .unwrap();

    let got = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert!(got.context.data.is_object());
    assert_eq!(got.context.data["k"], json!("v"));
}

/// Missing-instance no-op parity: merging into a non-existent id must return
/// Ok without raising, matching the base backend's
/// `merge_context_data_on_missing_instance_is_noop` contract.
#[tokio::test]
async fn merge_context_data_missing_instance_is_noop_when_encrypted() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    let res = storage
        .merge_context_data(InstanceId::new(), "k", &json!("v"))
        .await;
    assert!(res.is_ok(), "merge on missing id must be a no-op");
}

// ------------------------------------------------------------------
// Delegation macro smoke tests
// ------------------------------------------------------------------

/// Verifies that macro-generated delegation methods forward correctly
/// through `EncryptingStorage` without interfering with the inner backend.
#[tokio::test]
async fn delegated_sequence_crud_passes_through_encryption_layer() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t1"),
        namespace: Namespace::new("ns"),
        name: "delegated-test".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: chrono::Utc::now(),
    };

    // create_sequence is a delegated method (macro-generated)
    storage.create_sequence(&seq).await.unwrap();

    // get_sequence is also delegated
    let got = storage.get_sequence(seq.id).await.unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().name, "delegated-test");

    // list_sequences is also delegated
    let list = storage
        .list_sequences(Some(&TenantId::unchecked("t1")), None, 10, 0)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);

    // deprecate_sequence is delegated
    storage.deprecate_sequence(seq.id).await.unwrap();
    let after = storage.get_sequence(seq.id).await.unwrap().unwrap();
    assert!(after.deprecated);
}

/// Verifies that `batch_reschedule_instances` delegation works through
/// the encryption layer.
#[tokio::test]
async fn delegated_batch_reschedule_passes_through_encryption_layer() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    let seq_id = SequenceId::new();
    seed_sequence(&storage, seq_id, "t1").await;

    let inst = mk_instance("t1", json!({"secret": "data"}));
    let mut inst2 = inst.clone();
    inst2.id = InstanceId::new();
    inst2.sequence_id = seq_id;
    let mut first = inst.clone();
    first.sequence_id = seq_id;
    first.state = InstanceState::Running;
    let mut second = inst2.clone();
    second.state = InstanceState::Running;
    storage.create_instance(&first).await.unwrap();
    storage.create_instance(&second).await.unwrap();

    let fire_at = chrono::Utc::now() + chrono::Duration::seconds(60);
    storage
        .batch_reschedule_instances(&[first.id, second.id], fire_at)
        .await
        .unwrap();

    let got1 = storage.get_instance(first.id).await.unwrap().unwrap();
    let got2 = storage.get_instance(second.id).await.unwrap().unwrap();
    assert_eq!(got1.state, InstanceState::Scheduled);
    assert_eq!(got2.state, InstanceState::Scheduled);
}

// ============================================================================
// Regression coverage for the 2026-07 deep storage review:
//   #1  EncryptingStorage silently reverted defaulted trait methods to no-ops
//   #2  Encryption coverage gaps (block_outputs, worker_tasks, signals,
//       externalized_state, checkpoints, step_logs, instance_kv)
//   #3  Encryption disabled context externalization
//   #10 Encrypted merge_context_data CAS loop could hard-fail under load
// ============================================================================

/// #1: `update_sequence_status` has a no-op default body in the trait
/// (`Ok(())`). Before the fix, `EncryptingStorage` inherited that default
/// instead of forwarding to `inner`, so the call silently did nothing.
#[tokio::test]
async fn update_sequence_status_forwards_to_inner() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    let seq_id = SequenceId::new();
    seed_sequence(&storage, seq_id, "t1").await;

    storage
        .update_sequence_status(seq_id, "staging")
        .await
        .unwrap();

    let seq = storage.get_sequence(seq_id).await.unwrap().unwrap();
    assert_eq!(seq.status.to_string(), "staging");
}

/// #1: the rollback-policy family (`create`/`get`/`list`/`delete_rollback_policy`,
/// `record_rollback`, `query_error_rate`, `list_rollback_history`) all have
/// no-op defaults. Before the fix, wrapping a backend in `EncryptingStorage`
/// meant policies were "created" into the void and `get_rollback_policy`
/// always returned `None` -- auto-rollback silently never fired.
#[tokio::test]
async fn rollback_policy_crud_forwards_to_inner() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    storage
        .create_rollback_policy("t1", "seq-a", 0.5, 60, Some(30), None, None)
        .await
        .unwrap();

    let policy = storage
        .get_rollback_policy("t1", "seq-a")
        .await
        .unwrap()
        .expect("rollback policy must actually be persisted, not dropped");
    assert_eq!(policy.tenant_id, "t1");
    assert_eq!(policy.sequence_name, "seq-a");
    assert!((policy.error_rate_threshold - 0.5).abs() < f64::EPSILON);

    let policies = storage
        .list_rollback_policies(Some("t1"), 10)
        .await
        .unwrap();
    assert_eq!(policies.len(), 1);

    storage
        .record_rollback("t1", "seq-a", 0.9, 0.5, "error spike")
        .await
        .unwrap();
    let history = storage
        .list_rollback_history(Some("t1"), Some("seq-a"), 10)
        .await
        .unwrap();
    assert_eq!(history.len(), 1, "rollback event must actually be recorded");

    storage.delete_rollback_policy("t1", "seq-a").await.unwrap();
    assert!(
        storage
            .get_rollback_policy("t1", "seq-a")
            .await
            .unwrap()
            .is_none()
    );
}

/// #1: `update_instance_context_cas`'s default falls back to an unconditional
/// `update_instance_context` (always returns `true`, dropping the CAS guard).
/// Before the fix, `EncryptingStorage` inherited that default instead of
/// forwarding the CAS check to `inner`, turning the scheduler's
/// optimistic-concurrency write into last-write-wins.
#[tokio::test]
async fn update_instance_context_cas_rejects_stale_snapshot() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    let instance = mk_instance("t1", json!({"v": 1}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();

    let stale_ts = instance.updated_at;

    // A concurrent writer updates the context first, advancing `updated_at`.
    let mut concurrent_ctx = instance.context.clone();
    concurrent_ctx.data = json!({"v": 2});
    storage
        .update_instance_context(instance.id, &concurrent_ctx)
        .await
        .unwrap();

    // Our CAS write, still using the now-stale snapshot timestamp, must be
    // rejected rather than silently overwriting the concurrent write.
    let mut our_ctx = instance.context.clone();
    our_ctx.data = json!({"v": "ours"});
    let applied = storage
        .update_instance_context_cas(instance.id, &our_ctx, stale_ts)
        .await
        .unwrap();
    assert!(!applied, "stale CAS write must be rejected, not applied");

    let got = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(
        got.context.data,
        json!({"v": 2}),
        "concurrent writer's value must survive a rejected CAS"
    );
}

/// #3: `create_instance_externalized` must externalize large fields *before*
/// encrypting `context.data`. Before the fix, the whole context was encrypted
/// into a single string first, so the inner backend's `externalize_fields`
/// (which requires a JSON object) silently no-opped -- oversized contexts
/// landed inline regardless of `threshold_bytes`.
#[tokio::test]
async fn create_instance_externalized_still_externalizes_under_encryption() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let big_value = "x".repeat(4096);
    let instance = mk_instance("t1", json!({"small": "tiny", "big": big_value.clone()}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;

    storage
        .create_instance_externalized(&instance, 1024)
        .await
        .unwrap();

    // Round-trip through the wrapper: plaintext restored, including the
    // externalized field re-hydrated by the caller's normal marker handling.
    let raw_stored = inner.get_instance(instance.id).await.unwrap().unwrap();
    assert!(
        FieldEncryptor::is_encrypted(&raw_stored.context.data),
        "residual context must still be encrypted at rest"
    );

    // The residual context stored inline must be small -- the big field was
    // pulled out, not inlined into the encrypted blob.
    let residual_len = serde_json::to_string(&raw_stored.context.data)
        .unwrap()
        .len();
    assert!(
        residual_len < big_value.len(),
        "big field must have been externalized, not encrypted inline (residual={residual_len} bytes)"
    );

    // The externalized payload itself must be encrypted at rest too.
    let ref_key = orch8_storage::externalizing::context_data_ref_key(
        &instance.id.into_uuid().to_string(),
        "big",
    );
    let raw_ref = inner
        .get_externalized_state(&ref_key)
        .await
        .unwrap()
        .expect("big field must have been persisted to externalized_state");
    assert!(
        FieldEncryptor::is_encrypted(&raw_ref),
        "externalized payload must be encrypted at rest, got: {raw_ref}"
    );

    // Through the wrapper, the externalized payload decrypts transparently.
    let decrypted_ref = storage
        .get_externalized_state(&ref_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decrypted_ref, json!(big_value));
}

/// #2: `BlockOutput.output` (handler results -- LLM responses, HTTP bodies)
/// must be encrypted at rest, not left as the same data class as `context`
/// but stored in plaintext.
#[tokio::test]
async fn block_output_encrypted_at_rest_and_decrypted_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("t1", json!({}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();

    let block_id = BlockId::new("step_1");
    let output = BlockOutput {
        id: Uuid::new_v4(),
        instance_id: instance.id,
        block_id: block_id.clone(),
        output: json!({"llm_response": "the secret answer is 42"}),
        output_ref: None,
        output_size: 0,
        attempt: 1,
        created_at: Utc::now(),
    };
    storage.save_block_output(&output).await.unwrap();

    let raw = inner
        .get_block_output(instance.id, &block_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        FieldEncryptor::is_encrypted(&raw.output),
        "block output must be encrypted at rest, got: {}",
        raw.output
    );

    let got = storage
        .get_block_output(instance.id, &block_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.output, output.output);
}

/// #2: `Signal.payload` (e.g. a full `ExecutionContext` snapshot for
/// `update_context` signals) must be encrypted at rest.
#[tokio::test]
async fn signal_payload_encrypted_at_rest_and_decrypted_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("t1", json!({}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();

    let signal = Signal {
        id: Uuid::new_v4(),
        instance_id: instance.id,
        signal_type: SignalType::UpdateContext,
        payload: json!({"data": {"secret": "shh"}}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&signal).await.unwrap();

    let raw = inner.get_pending_signals(instance.id).await.unwrap();
    assert_eq!(raw.len(), 1);
    assert!(
        FieldEncryptor::is_encrypted(&raw[0].payload),
        "signal payload must be encrypted at rest, got: {}",
        raw[0].payload
    );

    let got = storage.get_pending_signals(instance.id).await.unwrap();
    assert_eq!(got[0].payload, signal.payload);
}

/// #2: `WorkerTask.params`/`context`/`output` are the same data class as
/// `context.data` (a serialized `ExecutionContext` snapshot dispatched to
/// external workers) and must be encrypted at rest.
#[tokio::test]
async fn worker_task_params_context_output_encrypted_at_rest() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("t1", json!({}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();

    let task_id = Uuid::new_v4();
    let task = WorkerTask {
        id: task_id,
        instance_id: instance.id,
        block_id: BlockId::new("step_1"),
        handler_name: "http_call".into(),
        queue_name: None,
        params: json!({"url": "https://example.com", "api_key": "sekrit"}),
        context: json!({"data": {"secret": "shh"}}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    storage.create_worker_task(&task).await.unwrap();

    let raw = inner.get_worker_task(task_id).await.unwrap().unwrap();
    assert!(
        FieldEncryptor::is_encrypted(&raw.params),
        "worker task params must be encrypted at rest"
    );
    assert!(
        FieldEncryptor::is_encrypted(&raw.context),
        "worker task context must be encrypted at rest"
    );

    let got = storage.get_worker_task(task_id).await.unwrap().unwrap();
    assert_eq!(got.params, task.params);
    assert_eq!(got.context, task.context);

    // `complete_worker_task` requires the task to be claimed first.
    let claimed = storage
        .claim_worker_tasks("http_call", "worker-1", 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);

    // complete_worker_task's bare `output` argument must be encrypted too.
    storage
        .complete_worker_task(task_id, "worker-1", &json!({"result": "done"}))
        .await
        .unwrap();
    let raw_completed = inner.get_worker_task(task_id).await.unwrap().unwrap();
    let raw_output = raw_completed.output.expect("output must be set");
    assert!(
        FieldEncryptor::is_encrypted(&raw_output),
        "worker task output must be encrypted at rest"
    );
    let got_completed = storage.get_worker_task(task_id).await.unwrap().unwrap();
    assert_eq!(got_completed.output, Some(json!({"result": "done"})));
}

/// #2: `Checkpoint.checkpoint_data` (agent conversation history, completed
/// block IDs, context) must be encrypted at rest.
#[tokio::test]
async fn checkpoint_data_encrypted_at_rest_and_decrypted_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("t1", json!({}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();

    let checkpoint = Checkpoint {
        id: Uuid::new_v4(),
        instance_id: instance.id,
        checkpoint_data: json!({"messages": ["hello", "secret reply"]}),
        created_at: Utc::now(),
    };
    storage.save_checkpoint(&checkpoint).await.unwrap();

    let raw = inner
        .get_latest_checkpoint(instance.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        FieldEncryptor::is_encrypted(&raw.checkpoint_data),
        "checkpoint data must be encrypted at rest"
    );

    let got = storage
        .get_latest_checkpoint(instance.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.checkpoint_data, checkpoint.checkpoint_data);
}

/// #2: step log messages can echo step input/output and must be encrypted
/// at rest.
#[tokio::test]
async fn step_log_message_encrypted_at_rest_and_decrypted_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("t1", json!({}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();

    let block_id = BlockId::new("step_1");
    let entry = StepLogEntry {
        ts: Utc::now(),
        level: "error".into(),
        message: "handler failed with secret token abc123".into(),
    };
    storage
        .append_step_logs(instance.id, &block_id, std::slice::from_ref(&entry))
        .await
        .unwrap();

    let raw = inner.list_step_logs(instance.id).await.unwrap();
    assert_eq!(raw.len(), 1);
    assert!(
        FieldEncryptor::is_encrypted(&json!(raw[0].message)),
        "step log message must be encrypted at rest, got: {}",
        raw[0].message
    );

    let got = storage.list_step_logs(instance.id).await.unwrap();
    assert_eq!(got[0].message, entry.message);
}

/// #2: instance KV state (including the agent step's
/// `__agent__:{block_id}` conversation-history checkpoint) must be encrypted
/// at rest.
#[tokio::test]
async fn instance_kv_encrypted_at_rest_and_decrypted_on_read() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner.clone(), enc);

    let instance = mk_instance("t1", json!({}));
    seed_sequence(&storage, instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();

    let key = "__agent__:step_1";
    let value = json!({"messages": [{"role": "user", "content": "secret prompt"}]});
    storage
        .set_instance_kv(instance.id, key, &value)
        .await
        .unwrap();

    let raw = inner
        .get_instance_kv(instance.id, key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        FieldEncryptor::is_encrypted(&raw),
        "instance kv value must be encrypted at rest, got: {raw}"
    );

    let got = storage
        .get_instance_kv(instance.id, key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, value);
}

/// #10: under concurrent writers to the same instance, `merge_context_data`
/// must retry through CAS contention rather than hard-failing after a small
/// fixed number of attempts. Before the fix, 5 immediate (non-backoff)
/// retries could plausibly all lose the race under real concurrency; this
/// pins the outcome (all writes eventually land) rather than the exact
/// retry count, since the point of jittered backoff is to make contention
/// survivable, not to guarantee any particular attempt succeeds first.
#[tokio::test]
async fn merge_context_data_survives_concurrent_writers() {
    const WRITERS: usize = 8;

    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage: Arc<EncryptingStorage> = Arc::new(EncryptingStorage::new(inner, enc));

    let instance = mk_instance("t1", json!({}));
    seed_sequence(storage.as_ref(), instance.sequence_id, "t1").await;
    storage.create_instance(&instance).await.unwrap();
    let mut handles = Vec::with_capacity(WRITERS);
    for i in 0..WRITERS {
        let storage = Arc::clone(&storage);
        let id = instance.id;
        handles.push(tokio::spawn(async move {
            storage
                .merge_context_data(id, &format!("k{i}"), &json!(i))
                .await
        }));
    }
    for h in handles {
        h.await
            .unwrap()
            .expect("concurrent merge must not hard-fail");
    }

    let got = storage.get_instance(instance.id).await.unwrap().unwrap();
    for i in 0..WRITERS {
        assert_eq!(
            got.context.data[format!("k{i}")],
            json!(i),
            "every concurrent writer's key must have landed"
        );
    }
}

/// #11: an `enc:v2:` `context.data` ciphertext is bound to its owning
/// instance via AAD. Copying the raw ciphertext column to a *different*
/// instance's row (as an attacker with DB write access, or a buggy
/// copy/clone operation, might do) must fail to decrypt there instead of
/// silently succeeding -- the exact scenario the deep storage review's
/// AES-GCM ciphertext-binding finding warns about.
#[tokio::test]
async fn context_ciphertext_is_bound_to_its_instance_and_rejects_transplant() {
    let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let inner: Arc<dyn StorageBackend> = sqlite.clone();
    let enc = FieldEncryptor::from_hex_key(TEST_KEY).unwrap();
    let storage = EncryptingStorage::new(inner, enc);

    let a = mk_instance("t1", json!({"secret": "belongs-to-a"}));
    let b = mk_instance("t1", json!({"secret": "belongs-to-b"}));
    seed_sequence(&storage, a.sequence_id, "t1").await;
    seed_sequence(&storage, b.sequence_id, "t1").await;
    storage.create_instance(&a).await.unwrap();
    storage.create_instance(&b).await.unwrap();

    // Confirm the new format is in play before testing the failure mode.
    let raw_a = sqlite.get_instance(a.id).await.unwrap().unwrap();
    assert!(
        raw_a
            .context
            .data
            .as_str()
            .is_some_and(|s| s.starts_with("enc:v2:")),
        "context.data must use the AAD-bound v2 format, got: {}",
        raw_a.context.data
    );

    // Transplant instance A's ciphertext onto instance B's row directly at
    // the storage layer (bypassing the wrapper, as an attacker with DB
    // write access would).
    sqlx::query("UPDATE task_instances SET context = ?1 WHERE id = ?2")
        .bind(raw_a.context.data.as_str().unwrap())
        .bind(b.id.into_uuid().to_string())
        .execute(sqlite.pool())
        .await
        .unwrap();

    // Reading instance B back through the wrapper must fail to decrypt --
    // the ciphertext was sealed under A's AAD, not B's.
    let result = storage.get_instance(b.id).await;
    assert!(
        result.is_err(),
        "a ciphertext transplanted from a different instance must not decrypt"
    );
}
