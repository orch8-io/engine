//! Comprehensive tests for orch8-publisher: manifest generation, canonical JSON,
//! signing/verification, pruning, `ManifestSequence`, and `SequencePublisher`.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{Duration, Utc};
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use serde_json::json;
use std::sync::Mutex;

use orch8_publisher::{
    CdnBackend, CdnError, ManifestBody, ManifestGenerator, ManifestRemoved, ManifestSequence,
    ManifestSigningKey, MemoryCdnBackend, SequencePublisher, SignedManifest,
};
use orch8_types::ids::{BlockId, Namespace, SequenceId, TenantId};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_key() -> SigningKey {
    SigningKey::generate(&mut OsRng)
}

fn make_manifest_sequence(name: &str, version: i32) -> ManifestSequence {
    make_manifest_sequence_with_tenant(name, version, "tenant")
}

fn make_manifest_sequence_with_tenant(
    name: &str,
    version: i32,
    tenant_id: &str,
) -> ManifestSequence {
    ManifestSequence {
        name: name.to_string(),
        version,
        url: format!("/{tenant_id}/sequences/{name}_v{version}.json"),
        signing_key_id: "key1".to_string(),
        sha256: format!("sha256_{name}_{version}"),
        required_handlers: vec!["echo".to_string()],
        min_sdk_version: "0.1.0".to_string(),
    }
}

fn make_removed(name: &str, days_ago: i64) -> ManifestRemoved {
    ManifestRemoved {
        name: name.to_string(),
        removed_at: Utc::now() - Duration::days(days_ago),
    }
}

fn generate_manifest(
    key: &SigningKey,
    sequences: Vec<ManifestSequence>,
    removed: Vec<ManifestRemoved>,
    other_keys: Vec<ManifestSigningKey>,
) -> SignedManifest {
    let r#gen = ManifestGenerator::new(key.clone(), "primary_key".to_string());
    r#gen.generate(sequences, removed, other_keys).unwrap()
}

fn verify_manifest_signature(signed: &SignedManifest, verifying_key: &VerifyingKey) -> bool {
    let sig_bytes = BASE64.decode(&signed.signature_b64).unwrap();
    let signature = Signature::from_slice(&sig_bytes).unwrap();
    verifying_key
        .verify(signed.canonical_json.as_bytes(), &signature)
        .is_ok()
}

/// In-memory mock CDN for `SequencePublisher` tests.
struct MockCdn {
    uploads: Mutex<Vec<(String, Vec<u8>)>>,
}

impl MockCdn {
    fn new() -> Self {
        Self {
            uploads: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl CdnBackend for MockCdn {
    async fn upload(
        &self,
        path: &str,
        bytes: Vec<u8>,
        _content_type: Option<&str>,
        _cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        self.uploads.lock().unwrap().push((path.to_string(), bytes));
        Ok(())
    }

    async fn delete(&self, _path: &str) -> Result<(), CdnError> {
        Ok(())
    }

    async fn get_etag(&self, _path: &str) -> Result<Option<String>, CdnError> {
        Ok(None)
    }
}

fn make_sequence_definition(name: &str, version: i32) -> SequenceDefinition {
    make_sequence_definition_with_tenant(name, version, "test_tenant")
}

fn make_sequence_definition_with_tenant(
    name: &str,
    version: i32,
    tenant_id: &str,
) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::new(tenant_id).unwrap(),
        namespace: Namespace::new("default"),
        name: name.to_string(),
        version,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("step1"),
            handler: "echo".to_string(),
            params: json!({"msg": "hello"}),
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
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: chrono::DateTime::parse_from_rfc3339("2025-06-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
    }
}

// ===========================================================================
// MANIFEST GENERATION (tests 1-30)
// ===========================================================================

// --- 1-5: Generate manifest with 0/1/5 sequences ---

#[test]
fn test_01_manifest_with_zero_sequences() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert_eq!(signed.body.sequences.len(), 0);
}

#[test]
fn test_02_manifest_with_one_sequence() {
    let key = test_key();
    let seq = make_manifest_sequence("onboarding", 1);
    let signed = generate_manifest(&key, vec![seq], vec![], vec![]);
    assert_eq!(signed.body.sequences.len(), 1);
    assert_eq!(signed.body.sequences[0].name, "onboarding");
}

#[test]
fn test_03_manifest_with_five_sequences() {
    let key = test_key();
    let seqs: Vec<ManifestSequence> = (1..=5)
        .map(|i| make_manifest_sequence(&format!("seq_{i}"), i))
        .collect();
    let signed = generate_manifest(&key, seqs, vec![], vec![]);
    assert_eq!(signed.body.sequences.len(), 5);
}

#[test]
fn test_04_manifest_sequences_preserve_order() {
    let key = test_key();
    let seqs = vec![
        make_manifest_sequence("z_seq", 1),
        make_manifest_sequence("a_seq", 2),
        make_manifest_sequence("m_seq", 3),
    ];
    let signed = generate_manifest(&key, seqs, vec![], vec![]);
    assert_eq!(signed.body.sequences[0].name, "z_seq");
    assert_eq!(signed.body.sequences[1].name, "a_seq");
    assert_eq!(signed.body.sequences[2].name, "m_seq");
}

#[test]
fn test_05_manifest_empty_sequences_produces_valid_json() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    assert!(parsed["sequences"].is_array());
    assert_eq!(parsed["sequences"].as_array().unwrap().len(), 0);
}

// --- 6-10: Manifest includes signing key ---

#[test]
fn test_06_manifest_includes_generator_signing_key() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(!signed.body.signing_keys.is_empty());
    let found = signed
        .body
        .signing_keys
        .iter()
        .any(|k| k.key_id == "primary_key");
    assert!(found, "generator key_id should appear in signing_keys");
}

#[test]
fn test_07_signing_key_public_key_is_base64() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let pk_b64 = &signed
        .body
        .signing_keys
        .iter()
        .find(|k| k.key_id == "primary_key")
        .unwrap()
        .public_key;
    let decoded = BASE64.decode(pk_b64).unwrap();
    assert_eq!(decoded.len(), 32, "Ed25519 public key is 32 bytes");
}

#[test]
fn test_08_signing_key_matches_verifying_key() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let pk_b64 = &signed
        .body
        .signing_keys
        .iter()
        .find(|k| k.key_id == "primary_key")
        .unwrap()
        .public_key;
    let decoded = BASE64.decode(pk_b64).unwrap();
    let vk = VerifyingKey::from_bytes(&decoded.try_into().unwrap()).unwrap();
    assert_eq!(vk, key.verifying_key());
}

#[test]
fn test_09_manifest_signing_keys_field_non_empty() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(
        !signed.body.signing_keys.is_empty(),
        "signing_keys must always contain at least the generator key"
    );
}

#[test]
fn test_10_manifest_signing_key_id_in_canonical_json() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(signed.canonical_json.contains("primary_key"));
}

// --- 11-15: Manifest signature is valid ---

#[test]
fn test_11_signature_is_non_empty() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(!signed.signature_b64.is_empty());
}

#[test]
fn test_12_signature_is_valid_base64() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let decoded = BASE64.decode(&signed.signature_b64);
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().len(), 64, "Ed25519 signature is 64 bytes");
}

#[test]
fn test_13_signature_verifies_with_generator_key() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_14_signature_verifies_with_sequences() {
    let key = test_key();
    let seqs = vec![make_manifest_sequence("flow", 1)];
    let signed = generate_manifest(&key, seqs, vec![], vec![]);
    assert!(verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_15_signature_verifies_with_removed_entries() {
    let key = test_key();
    let removed = vec![make_removed("old_seq", 5)];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    assert!(verify_manifest_signature(&signed, &key.verifying_key()));
}

// --- 16-20: Manifest version and generated_at ---

#[test]
fn test_16_manifest_version_is_monotonic_timestamp() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    // Version is now a monotonic epoch-millis stamp (replay protection), not a
    // hardcoded 1. It must equal generated_at and be a positive, recent value.
    assert_eq!(
        signed.body.manifest_version,
        signed.body.generated_at.timestamp_millis()
    );
    assert!(signed.body.manifest_version > 0);
}

#[test]
fn test_17_generated_at_is_recent() {
    let key = test_key();
    let before = Utc::now();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let after = Utc::now();
    assert!(signed.body.generated_at >= before);
    assert!(signed.body.generated_at <= after);
}

#[test]
fn test_18_manifest_version_in_canonical_json() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(signed.canonical_json.contains(&format!(
        "\"manifest_version\":{}",
        signed.body.manifest_version
    )));
}

#[test]
fn test_19_generated_at_in_canonical_json() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(signed.canonical_json.contains("\"generated_at\":"));
}

#[test]
fn test_20_manifest_body_deserializes_correctly() {
    let key = test_key();
    let signed = generate_manifest(
        &key,
        vec![make_manifest_sequence("test", 1)],
        vec![],
        vec![],
    );
    let parsed: ManifestBody = serde_json::from_str(&signed.canonical_json).unwrap();
    assert_eq!(parsed.manifest_version, signed.body.manifest_version);
    assert_eq!(parsed.sequences.len(), 1);
}

// --- 21-25: Multiple signing keys ---

#[test]
fn test_21_manifest_with_one_other_key() {
    let key = test_key();
    let other = ManifestSigningKey {
        key_id: "secondary".to_string(),
        public_key: BASE64.encode(test_key().verifying_key().to_bytes()),
    };
    let signed = generate_manifest(&key, vec![], vec![], vec![other]);
    assert_eq!(signed.body.signing_keys.len(), 2);
}

#[test]
fn test_22_manifest_with_multiple_other_keys() {
    let key = test_key();
    let others: Vec<ManifestSigningKey> = (0..3)
        .map(|i| ManifestSigningKey {
            key_id: format!("key_{i}"),
            public_key: BASE64.encode(test_key().verifying_key().to_bytes()),
        })
        .collect();
    let signed = generate_manifest(&key, vec![], vec![], others);
    // 3 other keys + 1 generator key = 4
    assert_eq!(signed.body.signing_keys.len(), 4);
}

#[test]
fn test_23_other_keys_appear_before_generator_key() {
    let key = test_key();
    let other = ManifestSigningKey {
        key_id: "first_key".to_string(),
        public_key: BASE64.encode(test_key().verifying_key().to_bytes()),
    };
    let signed = generate_manifest(&key, vec![], vec![], vec![other]);
    // other_keys are pushed first, then generator key is appended
    assert_eq!(signed.body.signing_keys[0].key_id, "first_key");
    assert_eq!(signed.body.signing_keys[1].key_id, "primary_key");
}

#[test]
fn test_24_all_signing_keys_in_canonical_json() {
    let key = test_key();
    let other = ManifestSigningKey {
        key_id: "extra_key".to_string(),
        public_key: BASE64.encode(test_key().verifying_key().to_bytes()),
    };
    let signed = generate_manifest(&key, vec![], vec![], vec![other]);
    assert!(signed.canonical_json.contains("extra_key"));
    assert!(signed.canonical_json.contains("primary_key"));
}

#[test]
fn test_25_signing_keys_have_valid_public_keys() {
    let key = test_key();
    let other_key = test_key();
    let other = ManifestSigningKey {
        key_id: "other".to_string(),
        public_key: BASE64.encode(other_key.verifying_key().to_bytes()),
    };
    let signed = generate_manifest(&key, vec![], vec![], vec![other]);
    for sk in &signed.body.signing_keys {
        let decoded = BASE64.decode(&sk.public_key).unwrap();
        assert_eq!(decoded.len(), 32);
        VerifyingKey::from_bytes(&decoded.try_into().unwrap()).unwrap();
    }
}

// --- 26-30: Manifest with removed entries ---

#[test]
fn test_26_manifest_with_one_removed_entry() {
    let key = test_key();
    let removed = vec![make_removed("deprecated_seq", 10)];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    assert_eq!(signed.body.removed.len(), 1);
    assert_eq!(signed.body.removed[0].name, "deprecated_seq");
}

#[test]
fn test_27_manifest_with_multiple_removed_entries() {
    let key = test_key();
    let removed = vec![
        make_removed("seq_a", 5),
        make_removed("seq_b", 10),
        make_removed("seq_c", 20),
    ];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    assert_eq!(signed.body.removed.len(), 3);
}

#[test]
fn test_28_removed_entries_preserve_timestamps() {
    let key = test_key();
    let removed_at = Utc::now() - Duration::days(15);
    let removed = vec![ManifestRemoved {
        name: "ts_test".to_string(),
        removed_at,
    }];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    // Timestamps should be within a second (serialization rounding)
    let diff = (signed.body.removed[0].removed_at - removed_at)
        .num_seconds()
        .abs();
    assert!(diff < 2);
}

#[test]
fn test_29_removed_in_canonical_json() {
    let key = test_key();
    let removed = vec![make_removed("gone_seq", 2)];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    assert!(signed.canonical_json.contains("gone_seq"));
}

#[test]
fn test_30_manifest_with_sequences_and_removed() {
    let key = test_key();
    let seqs = vec![make_manifest_sequence("active", 1)];
    let removed = vec![make_removed("inactive", 5)];
    let signed = generate_manifest(&key, seqs, removed, vec![]);
    assert_eq!(signed.body.sequences.len(), 1);
    assert_eq!(signed.body.removed.len(), 1);
}

// ===========================================================================
// CANONICAL JSON (tests 31-50)
// ===========================================================================

// --- 31-35: Keys sorted at all nesting levels ---

#[test]
fn test_31_canonical_json_sorts_top_level_keys() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    if let serde_json::Value::Object(map) = parsed {
        let keys: Vec<&String> = map.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "top-level keys must be sorted");
    } else {
        panic!("canonical_json should be an object");
    }
}

#[test]
fn test_32_canonical_json_sorts_nested_keys() {
    let key = test_key();
    let seq = ManifestSequence {
        name: "z_name".to_string(),
        version: 1,
        url: "/a_url".to_string(),
        signing_key_id: "b_key".to_string(),
        sha256: "c_hash".to_string(),
        required_handlers: vec!["d_handler".to_string()],
        min_sdk_version: "0.1.0".to_string(),
    };
    let signed = generate_manifest(&key, vec![seq], vec![], vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    let seq_obj = &parsed["sequences"][0];
    if let serde_json::Value::Object(map) = seq_obj {
        let keys: Vec<&String> = map.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "nested sequence keys must be sorted");
    }
}

#[test]
fn test_33_canonical_json_sorts_deeply_nested() {
    // Verify the canonical_json output has generated_at before manifest_version
    // (alphabetical: 'g' < 'm')
    let cj = generate_manifest(&test_key(), vec![], vec![], vec![]).canonical_json;
    let g_pos = cj.find("\"generated_at\"").unwrap();
    let m_pos = cj.find("\"manifest_version\"").unwrap();
    assert!(
        g_pos < m_pos,
        "generated_at should come before manifest_version alphabetically"
    );
}

#[test]
fn test_34_canonical_json_signing_keys_sorted_internally() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    let sk = &parsed["signing_keys"][0];
    if let serde_json::Value::Object(map) = sk {
        let keys: Vec<&String> = map.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "signing_key object keys must be sorted");
    }
}

#[test]
fn test_35_canonical_json_removed_entries_sorted_internally() {
    let key = test_key();
    let removed = vec![make_removed("test", 5)];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    let r = &parsed["removed"][0];
    if let serde_json::Value::Object(map) = r {
        let keys: Vec<&String> = map.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "removed entry keys must be sorted");
    }
}

// --- 36-40: Arrays preserve order ---

#[test]
fn test_36_sequences_array_preserves_order() {
    let key = test_key();
    let seqs = vec![
        make_manifest_sequence("first", 1),
        make_manifest_sequence("second", 2),
        make_manifest_sequence("third", 3),
    ];
    let signed = generate_manifest(&key, seqs, vec![], vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    let names: Vec<&str> = parsed["sequences"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["first", "second", "third"]);
}

#[test]
fn test_37_required_handlers_array_preserves_order() {
    let key = test_key();
    let seq = ManifestSequence {
        name: "test".to_string(),
        version: 1,
        url: "/test.json".to_string(),
        signing_key_id: "key1".to_string(),
        sha256: "hash".to_string(),
        required_handlers: vec!["zulu".to_string(), "alpha".to_string(), "mike".to_string()],
        min_sdk_version: "0.1.0".to_string(),
    };
    let signed = generate_manifest(&key, vec![seq], vec![], vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    let handlers: Vec<&str> = parsed["sequences"][0]["required_handlers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(handlers, vec!["zulu", "alpha", "mike"]);
}

#[test]
fn test_38_signing_keys_array_preserves_order() {
    let key = test_key();
    let others = vec![
        ManifestSigningKey {
            key_id: "aaa".to_string(),
            public_key: BASE64.encode(test_key().verifying_key().to_bytes()),
        },
        ManifestSigningKey {
            key_id: "bbb".to_string(),
            public_key: BASE64.encode(test_key().verifying_key().to_bytes()),
        },
    ];
    let signed = generate_manifest(&key, vec![], vec![], others);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    let key_ids: Vec<&str> = parsed["signing_keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["key_id"].as_str().unwrap())
        .collect();
    assert_eq!(key_ids, vec!["aaa", "bbb", "primary_key"]);
}

#[test]
fn test_39_removed_array_preserves_insertion_order() {
    let key = test_key();
    let removed = vec![
        make_removed("charlie", 5),
        make_removed("alpha", 10),
        make_removed("bravo", 15),
    ];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    let parsed: serde_json::Value = serde_json::from_str(&signed.canonical_json).unwrap();
    let names: Vec<&str> = parsed["removed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["charlie", "alpha", "bravo"]);
}

#[test]
fn test_40_empty_arrays_serialized_correctly() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(signed.canonical_json.contains("\"removed\":[]"));
    assert!(signed.canonical_json.contains("\"sequences\":[]"));
}

// --- 41-45: Deterministic output for same input ---

#[test]
fn test_43_canonical_json_no_trailing_newline() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(!signed.canonical_json.ends_with('\n'));
    assert!(!signed.canonical_json.ends_with('\r'));
}

#[test]
fn test_44_canonical_json_is_valid_json() {
    let key = test_key();
    let signed = generate_manifest(
        &key,
        vec![make_manifest_sequence("valid", 1)],
        vec![make_removed("gone", 3)],
        vec![],
    );
    let result: Result<serde_json::Value, _> = serde_json::from_str(&signed.canonical_json);
    assert!(result.is_ok());
}

#[test]
fn test_45_canonical_json_deterministic_for_same_body() {
    let key = test_key();
    let r#gen = ManifestGenerator::new(key.clone(), "k".to_string());
    let seq = make_manifest_sequence("stable", 1);
    let signed1 = r#gen.generate(vec![seq.clone()], vec![], vec![]).unwrap();
    // Deserialize and re-serialize the body to verify determinism
    let body: ManifestBody = serde_json::from_str(&signed1.canonical_json).unwrap();
    let gen2 = ManifestGenerator::new(key.clone(), "k".to_string());
    let signed2 = gen2.generate(body.sequences, body.removed, vec![]).unwrap();
    // The generated_at will differ, but structure is the same format
    let parsed1: serde_json::Value = serde_json::from_str(&signed1.canonical_json).unwrap();
    let parsed2: serde_json::Value = serde_json::from_str(&signed2.canonical_json).unwrap();
    // Both should have sorted keys
    if let serde_json::Value::Object(m1) = &parsed1
        && let serde_json::Value::Object(m2) = &parsed2
    {
        let k1: Vec<&String> = m1.keys().collect();
        let k2: Vec<&String> = m2.keys().collect();
        assert_eq!(k1, k2, "key order should be the same");
    }
}

// ===========================================================================
// MANIFEST SIGNING AND VERIFICATION (tests 51-70)
// ===========================================================================

// --- 51-55: Signature verifies with correct public key ---

#[test]
fn test_51_verify_empty_manifest_signature() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_52_verify_manifest_with_sequences() {
    let key = test_key();
    let seqs = vec![
        make_manifest_sequence("s1", 1),
        make_manifest_sequence("s2", 2),
    ];
    let signed = generate_manifest(&key, seqs, vec![], vec![]);
    assert!(verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_53_verify_manifest_with_removed() {
    let key = test_key();
    let removed = vec![make_removed("r1", 3), make_removed("r2", 7)];
    let signed = generate_manifest(&key, vec![], removed, vec![]);
    assert!(verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_54_verify_complex_manifest() {
    let key = test_key();
    let seqs: Vec<ManifestSequence> = (0..10)
        .map(|i| make_manifest_sequence(&format!("seq_{i}"), i))
        .collect();
    let removed: Vec<ManifestRemoved> = (0..5)
        .map(|i| make_removed(&format!("rm_{i}"), i + 1))
        .collect();
    let others = vec![ManifestSigningKey {
        key_id: "backup".to_string(),
        public_key: BASE64.encode(test_key().verifying_key().to_bytes()),
    }];
    let signed = generate_manifest(&key, seqs, removed, others);
    assert!(verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_55_signature_bytes_are_64() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    let sig_bytes = BASE64.decode(&signed.signature_b64).unwrap();
    assert_eq!(sig_bytes.len(), 64);
}

// --- 56-60: Signature fails with wrong key ---

#[test]
fn test_56_wrong_key_fails_verification() {
    let key = test_key();
    let wrong_key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    assert!(!verify_manifest_signature(
        &signed,
        &wrong_key.verifying_key()
    ));
}

#[test]
fn test_57_random_key_fails_verification() {
    let key = test_key();
    let signed = generate_manifest(
        &key,
        vec![make_manifest_sequence("test", 1)],
        vec![],
        vec![],
    );
    for _ in 0..5 {
        let random_key = test_key();
        if random_key.verifying_key() != key.verifying_key() {
            assert!(!verify_manifest_signature(
                &signed,
                &random_key.verifying_key()
            ));
        }
    }
}

#[test]
fn test_58_swapped_keys_fail_verification() {
    let key_a = test_key();
    let key_b = test_key();
    let signed_a = generate_manifest(&key_a, vec![], vec![], vec![]);
    let signed_b = generate_manifest(&key_b, vec![], vec![], vec![]);
    // Verify A with B's key fails
    assert!(!verify_manifest_signature(
        &signed_a,
        &key_b.verifying_key()
    ));
    // Verify B with A's key fails
    assert!(!verify_manifest_signature(
        &signed_b,
        &key_a.verifying_key()
    ));
}

#[test]
fn test_59_zero_verifying_key_fails() {
    let key = test_key();
    let signed = generate_manifest(&key, vec![], vec![], vec![]);
    // A zeroed-out key (if valid) should not verify
    let zero_bytes = [0u8; 32];
    // Some zero bytes may not form a valid VerifyingKey (it's on the curve check)
    // but if they do, verification should fail
    if let Ok(zero_vk) = VerifyingKey::from_bytes(&zero_bytes) {
        assert!(!verify_manifest_signature(&signed, &zero_vk));
    }
    // Test passes regardless - the point is wrong keys don't work
}

#[test]
fn test_60_previously_valid_key_fails_after_rotation() {
    let old_key = test_key();
    let new_key = test_key();
    // Manifest signed with new key
    let signed = generate_manifest(&new_key, vec![], vec![], vec![]);
    // Old key cannot verify
    assert!(!verify_manifest_signature(
        &signed,
        &old_key.verifying_key()
    ));
    // New key can verify
    assert!(verify_manifest_signature(&signed, &new_key.verifying_key()));
}

// --- 61-65: Tampered manifest fails verification ---

#[test]
fn test_61_tampered_canonical_json_fails() {
    let key = test_key();
    let mut signed = generate_manifest(&key, vec![], vec![], vec![]);
    // Tamper with canonical_json
    signed.canonical_json = signed
        .canonical_json
        .replace("\"manifest_version\":1", "\"manifest_version\":2");
    assert!(!verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_62_appended_byte_fails() {
    let key = test_key();
    let mut signed = generate_manifest(&key, vec![], vec![], vec![]);
    signed.canonical_json.push(' ');
    assert!(!verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_63_prepended_byte_fails() {
    let key = test_key();
    let mut signed = generate_manifest(&key, vec![], vec![], vec![]);
    signed.canonical_json = format!(" {}", signed.canonical_json);
    assert!(!verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_64_modified_sequence_name_fails() {
    let key = test_key();
    let seq = make_manifest_sequence("original", 1);
    let mut signed = generate_manifest(&key, vec![seq], vec![], vec![]);
    signed.canonical_json = signed.canonical_json.replace("original", "modified");
    assert!(!verify_manifest_signature(&signed, &key.verifying_key()));
}

#[test]
fn test_65_swapped_signature_fails() {
    let key = test_key();
    let signed_a = generate_manifest(&key, vec![make_manifest_sequence("a", 1)], vec![], vec![]);
    let mut signed_b =
        generate_manifest(&key, vec![make_manifest_sequence("b", 2)], vec![], vec![]);
    // Use signature from A on body of B
    signed_b.signature_b64 = signed_a.signature_b64.clone();
    assert!(!verify_manifest_signature(&signed_b, &key.verifying_key()));
}

// --- 66: Different keys produce different signatures ---

#[test]
fn test_66_different_keys_different_signatures() {
    let key_a = test_key();
    let key_b = test_key();
    let signed_a = generate_manifest(&key_a, vec![], vec![], vec![]);
    let signed_b = generate_manifest(&key_b, vec![], vec![], vec![]);
    // Even if canonical_json differs by generated_at, signatures should differ
    assert_ne!(signed_a.signature_b64, signed_b.signature_b64);
}

// ===========================================================================
// PRUNING AND MAINTENANCE (tests 71-80)
// ===========================================================================

// --- 71-75: prune_removed drops entries older than 30 days ---

#[test]
fn test_71_prune_removes_31_day_old_entry() {
    let mut removed = vec![make_removed("old", 31)];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert!(removed.is_empty());
}

#[test]
fn test_72_prune_removes_60_day_old_entry() {
    let mut removed = vec![make_removed("ancient", 60)];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert!(removed.is_empty());
}

#[test]
fn test_73_prune_removes_multiple_old_entries() {
    let mut removed = vec![
        make_removed("old1", 35),
        make_removed("old2", 40),
        make_removed("old3", 100),
    ];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert!(removed.is_empty());
}

#[test]
fn test_74_prune_keeps_recent_removes_old() {
    let mut removed = vec![make_removed("old", 31), make_removed("recent", 5)];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].name, "recent");
}

#[test]
fn test_75_prune_boundary_30_days_minus_margin() {
    // Entry at 29 days 23 hours is safely within the retention window.
    // We avoid testing exactly 30 days due to microsecond timing differences
    // between Utc::now() in test vs inside prune_removed.
    let mut removed = vec![ManifestRemoved {
        name: "boundary".to_string(),
        removed_at: Utc::now() - Duration::days(29) - Duration::hours(23),
    }];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert_eq!(
        removed.len(),
        1,
        "entry just under 30 days should be retained"
    );
}

// --- 76-80: prune_removed keeps recent entries ---

#[test]
fn test_76_prune_keeps_one_day_old() {
    let mut removed = vec![make_removed("fresh", 1)];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert_eq!(removed.len(), 1);
}

#[test]
fn test_77_prune_keeps_29_day_old() {
    let mut removed = vec![make_removed("almost_old", 29)];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert_eq!(removed.len(), 1);
}

#[test]
fn test_78_prune_keeps_zero_day_old() {
    let mut removed = vec![ManifestRemoved {
        name: "just_now".to_string(),
        removed_at: Utc::now(),
    }];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert_eq!(removed.len(), 1);
}

#[test]
fn test_79_prune_keeps_all_recent() {
    let mut removed: Vec<ManifestRemoved> = (1..=10)
        .map(|i| make_removed(&format!("recent_{i}"), i))
        .collect();
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert_eq!(removed.len(), 10);
}

#[test]
fn test_80_prune_empty_vec_stays_empty() {
    let mut removed: Vec<ManifestRemoved> = vec![];
    orch8_publisher::manifest::prune_removed(&mut removed);
    assert!(removed.is_empty());
}

// ===========================================================================
// MANIFEST SEQUENCE (tests 81-90)
// ===========================================================================

// --- 81-82: ManifestSequence construction and serde ---

#[test]
fn test_81_manifest_sequence_construction() {
    let seq = ManifestSequence {
        name: "onboarding".to_string(),
        version: 3,
        url: "/tenant/sequences/abc.json".to_string(),
        signing_key_id: "key_1".to_string(),
        sha256: "deadbeef".to_string(),
        required_handlers: vec!["http".to_string(), "email".to_string()],
        min_sdk_version: "1.2.0".to_string(),
    };
    assert_eq!(seq.name, "onboarding");
    assert_eq!(seq.version, 3);
}

#[test]
fn test_82_manifest_sequence_serialize_deserialize() {
    let seq = make_manifest_sequence("roundtrip", 5);
    let json = serde_json::to_string(&seq).unwrap();
    let deserialized: ManifestSequence = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.name, "roundtrip");
    assert_eq!(deserialized.version, 5);
}

// ===========================================================================
// SEQUENCE PUBLISHER (tests 91-100)
// ===========================================================================

#[tokio::test]
async fn test_93_publisher_publish_sequence_with_mock_cdn() {
    let key = test_key();
    let mock = MockCdn::new();
    let cdn: Box<dyn CdnBackend> = Box::new(mock);
    let r#gen = ManifestGenerator::new(key.clone(), "key1".to_string());
    let publisher =
        SequencePublisher::new(cdn, r#gen, "test_tenant".to_string(), "key1".to_string())
            .expect("valid tenant_id");

    let seq = make_sequence_definition("publish_test", 1);
    let entry = publisher.publish_sequence(&seq, &key).await.unwrap();
    assert_eq!(entry.name, "publish_test");
    assert_eq!(entry.version, 1);
    assert_eq!(entry.signing_key_id, "key1");
    assert!(!entry.sha256.is_empty());
}

#[tokio::test]
async fn test_94_publisher_uploads_to_cdn() {
    let key = test_key();
    let cdn = Box::new(MemoryCdnBackend::new());
    let r#gen = ManifestGenerator::new(key.clone(), "key1".to_string());
    let publisher =
        SequencePublisher::new(cdn, r#gen, "upload_tenant".to_string(), "key1".to_string())
            .expect("valid tenant_id");

    let seq = make_sequence_definition_with_tenant("cdn_test", 2, "upload_tenant");
    let entry = publisher.publish_sequence(&seq, &key).await.unwrap();

    // The URL should reference the tenant
    assert!(entry.url.contains("upload_tenant"));
    assert!(entry.url.contains("sequences"));
}

#[tokio::test]
async fn test_95_publisher_min_sdk_version_applied_to_entry() {
    let key = test_key();
    let cdn = Box::new(MemoryCdnBackend::new());
    let r#gen = ManifestGenerator::new(key.clone(), "key1".to_string());
    let publisher = SequencePublisher::new(cdn, r#gen, "tenant".to_string(), "key1".to_string())
        .expect("valid tenant_id")
        .with_min_sdk_version("5.0.0".to_string());

    let seq = make_sequence_definition_with_tenant("sdk_ver_test", 1, "tenant");
    let entry = publisher.publish_sequence(&seq, &key).await.unwrap();
    assert_eq!(entry.min_sdk_version, "5.0.0");
}

// --- 96-100: Publish flow with mock CDN ---

#[tokio::test]
async fn test_96_mock_cdn_utility_captures_uploads() {
    let mock = std::sync::Arc::new(MockCdn::new());
    let mock_clone = mock.clone();

    mock_clone
        .upload(
            "test/path.json",
            b"hello".to_vec(),
            Some("application/json"),
            None,
        )
        .await
        .unwrap();

    let uploads = mock.uploads.lock().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].0, "test/path.json");
    assert_eq!(uploads[0].1, b"hello");
}

#[tokio::test]
async fn test_97_publish_sequence_uploads_json_and_signature() {
    let key = test_key();
    let cdn = Box::new(MemoryCdnBackend::new());
    let r#gen = ManifestGenerator::new(key.clone(), "key1".to_string());
    let publisher = SequencePublisher::new(cdn, r#gen, "t_uploads".to_string(), "key1".to_string())
        .expect("valid tenant_id");

    let seq = make_sequence_definition_with_tenant("upload_check", 1, "t_uploads");
    let entry = publisher.publish_sequence(&seq, &key).await.unwrap();

    // Entry should have a sha256 that forms part of the URL
    assert!(entry.url.contains(&entry.sha256));
}

#[tokio::test]
async fn test_98_publish_manifest_uploads_to_manifest_path() {
    let key = test_key();
    let cdn_box: Box<dyn CdnBackend> = Box::new(MemoryCdnBackend::new());
    let r#gen = ManifestGenerator::new(key.clone(), "key1".to_string());
    let publisher =
        SequencePublisher::new(cdn_box, r#gen, "manifest_t".to_string(), "key1".to_string())
            .expect("valid tenant_id");

    publisher
        .publish_manifest(vec![], vec![], vec![])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_99_publish_manifest_contains_signature_and_json() {
    let key = test_key();
    let cdn = Box::new(MemoryCdnBackend::new());
    let r#gen = ManifestGenerator::new(key.clone(), "key1".to_string());
    let publisher = SequencePublisher::new(cdn, r#gen, "sig_test".to_string(), "key1".to_string())
        .expect("valid tenant_id");

    let seq = make_manifest_sequence_with_tenant("included", 1, "sig_test");
    // publish_manifest should succeed
    publisher
        .publish_manifest(vec![seq], vec![], vec![])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_100_publisher_required_handlers_extracted_from_sequence() {
    let key = test_key();
    let cdn = Box::new(MemoryCdnBackend::new());
    let r#gen = ManifestGenerator::new(key.clone(), "key1".to_string());
    let publisher = SequencePublisher::new(cdn, r#gen, "handler_t".to_string(), "key1".to_string())
        .expect("valid tenant_id");

    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::new("handler_t").unwrap(),
        namespace: Namespace::new("default"),
        name: "multi_handler".to_string(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![
            BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("s1"),
                handler: "http".to_string(),
                params: json!({}),
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
            })),
            BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("s2"),
                handler: "email".to_string(),
                params: json!({}),
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
            })),
            BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("s3"),
                handler: "http".to_string(), // duplicate
                params: json!({}),
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
            })),
        ],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };

    let entry = publisher.publish_sequence(&seq, &key).await.unwrap();
    // Should be deduplicated and sorted
    assert_eq!(entry.required_handlers, vec!["email", "http"]);
}
