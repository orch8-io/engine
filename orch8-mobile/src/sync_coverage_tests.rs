//! Sync download/verify coverage: URL redaction, hex encoding, SDK version
//! gating, and manifest signature framing.
//!
//! Count contract: 14 independently named unit tests.

use super::*;
use ed25519_dalek::Signer;
use orch8_storage::sqlite::SqliteStorage;

async fn orchestrator() -> SyncOrchestrator {
    let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
    SyncOrchestrator::new(
        mobile_storage,
        sqlite,
        RootKey {
            pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
        },
        "0.4.0".to_string(),
        50,
    )
}

fn signed_manifest_bytes(signing_key: &ed25519_dalek::SigningKey, body: &[u8]) -> Vec<u8> {
    let signature = signing_key.sign(body);
    let mut bytes = BASE64.encode(signature.to_bytes()).into_bytes();
    bytes.push(b'\n');
    bytes.extend_from_slice(body);
    bytes
}

fn minimal_manifest_json() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "signing_keys": [],
        "sequences": [],
        "removed": [],
        "manifest_version": 7,
        "generated_at": Utc::now(),
    }))
    .unwrap()
}

macro_rules! redaction_case {
    ($name:ident, $url:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(redacted_url($url), $expected);
        }
    };
}

redaction_case!(
    coverage_sync_001_redacted_url_strips_query_token,
    "https://cdn.example.com/manifest?token=secret",
    "https://cdn.example.com/manifest"
);
redaction_case!(
    coverage_sync_002_redacted_url_keeps_queryless_url,
    "https://cdn.example.com/manifest",
    "https://cdn.example.com/manifest"
);
redaction_case!(
    coverage_sync_003_redacted_url_stops_at_first_question_mark,
    "https://cdn.example.com/m?a=1?b=2",
    "https://cdn.example.com/m"
);

macro_rules! hex_case {
    ($name:ident, $bytes:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(to_hex($bytes), $expected);
        }
    };
}

hex_case!(coverage_sync_004_hex_encodes_empty_input, &[], "");
hex_case!(
    coverage_sync_005_hex_encodes_lowercase_byte_pairs,
    &[0xde, 0xad, 0x00, 0xff],
    "dead00ff"
);

#[test]
fn coverage_sync_006_sync_response_cap_is_five_mib() {
    assert_eq!(MAX_SYNC_RESPONSE_BYTES, 5 * 1024 * 1024);
}

macro_rules! version_case {
    ($name:ident, $sdk:expr, $min:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            let orch = orchestrator().await;
            assert_eq!(orch.version_meets_min($sdk, $min), $expected);
        }
    };
}

version_case!(
    coverage_sync_007_version_compare_is_numeric_not_lexicographic,
    "0.4.10",
    "0.4.9",
    true
);
version_case!(
    coverage_sync_008_version_compare_treats_missing_components_as_zero,
    "1.0.0.0",
    "1.0",
    true
);
version_case!(
    coverage_sync_009_version_compare_rejects_empty_component,
    "1..0",
    "0.1",
    false
);
version_case!(
    coverage_sync_010_version_compare_rejects_overflowing_component,
    "4294967296.0",
    "0.1",
    false
);

#[tokio::test]
async fn coverage_sync_011_manifest_round_trips_with_valid_root_signature() {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
    let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
    let orch = SyncOrchestrator::new(
        mobile_storage,
        sqlite,
        RootKey {
            pubkey: signing_key.verifying_key(),
        },
        "0.4.0".to_string(),
        50,
    );

    let body = minimal_manifest_json();
    let bytes = signed_manifest_bytes(&signing_key, &body);

    let manifest = orch.verify_and_parse_manifest(&bytes).unwrap();
    assert_eq!(manifest.manifest_version, 7);
    assert!(manifest.sequences.is_empty());
}

#[tokio::test]
async fn coverage_sync_012_manifest_rejects_signature_from_wrong_key() {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
    let body = minimal_manifest_json();
    let bytes = signed_manifest_bytes(&signing_key, &body);

    let orch = orchestrator().await;
    let result = orch.verify_and_parse_manifest(&bytes);
    assert!(matches!(result, Err(MobileError::Engine { .. })));
}

#[tokio::test]
async fn coverage_sync_013_manifest_rejects_malformed_signature_base64() {
    let orch = orchestrator().await;
    let result = orch.verify_and_parse_manifest(b"!!!not-base64!!!\n{}");
    assert!(matches!(result, Err(MobileError::Engine { .. })));
}

#[test]
fn coverage_sync_014_sync_result_default_is_all_zeros() {
    let result = SyncResult::default();
    assert_eq!(result.added, 0);
    assert_eq!(result.updated, 0);
    assert_eq!(result.removed, 0);
    assert_eq!(result.skipped, 0);
    assert_eq!(result.signature_failures, 0);
}
