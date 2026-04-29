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
        .get_credential("stripe-prod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.value.expose(), secret);

    // At-rest: the inner backend sees encrypted ciphertext.
    let raw = inner.get_credential("stripe-prod").await.unwrap().unwrap();
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
    let got = storage.get_credential("key1").await.unwrap().unwrap();
    assert_eq!(got.value.expose(), "new-secret-value");

    // Raw still encrypted.
    let raw = inner.get_credential("key1").await.unwrap().unwrap();
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
        .get_credential("google-oauth")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.value.expose(), r#"{"access_token":"at_123"}"#);
    assert_eq!(
        got.refresh_token.as_ref().unwrap().expose(),
        "rt_super_secret"
    );

    // At-rest: both fields encrypted.
    let raw = inner.get_credential("google-oauth").await.unwrap().unwrap();
    assert!(raw.value.expose().starts_with("enc:v1:"));
    assert!(raw
        .refresh_token
        .as_ref()
        .unwrap()
        .expose()
        .starts_with("enc:v1:"));
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
        .list_credentials(Some(&TenantId("T1".into())))
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
    let got = storage.get_credential("legacy").await.unwrap().unwrap();
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
        .get_credential("rotated")
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
        .get_credential("rotated")
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
        tenant_id: TenantId("t1".into()),
        namespace: Namespace("ns".into()),
        name: "delegated-test".into(),
        version: 1,
        deprecated: false,
        blocks: vec![],
        interceptors: None,
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
        .list_sequences(Some(&TenantId("t1".into())), None, 10, 0)
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
    let mut inst_mut = inst.clone();
    inst_mut.sequence_id = seq_id;
    inst_mut.state = InstanceState::Running;
    let mut inst2_mut = inst2.clone();
    inst2_mut.state = InstanceState::Running;
    storage.create_instance(&inst_mut).await.unwrap();
    storage.create_instance(&inst2_mut).await.unwrap();

    let fire_at = chrono::Utc::now() + chrono::Duration::seconds(60);
    storage
        .batch_reschedule_instances(&[inst_mut.id, inst2_mut.id], fire_at)
        .await
        .unwrap();

    let got1 = storage.get_instance(inst_mut.id).await.unwrap().unwrap();
    let got2 = storage.get_instance(inst2_mut.id).await.unwrap().unwrap();
    assert_eq!(got1.state, InstanceState::Scheduled);
    assert_eq!(got2.state, InstanceState::Scheduled);
}
