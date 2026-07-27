//! Coverage tests for capability-scoped API key principals.
//!
//! Count contract: 23 independently named unit tests.

use super::*;
use std::collections::HashSet;

macro_rules! capability_round_trip {
    ($name:ident, $variant:expr, $wire:literal) => {
        #[test]
        fn $name() {
            let encoded = serde_json::to_string(&$variant).unwrap();
            assert_eq!(encoded, $wire);
            let decoded: ApiCapability = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, $variant);
        }
    };
}

capability_round_trip!(
    coverage_api_key_001_operator_serializes_snake_case,
    ApiCapability::Operator,
    "\"operator\""
);
capability_round_trip!(
    coverage_api_key_002_worker_serializes_snake_case,
    ApiCapability::Worker,
    "\"worker\""
);
capability_round_trip!(
    coverage_api_key_003_device_serializes_snake_case,
    ApiCapability::Device,
    "\"device\""
);
capability_round_trip!(
    coverage_api_key_004_publisher_serializes_snake_case,
    ApiCapability::Publisher,
    "\"publisher\""
);
capability_round_trip!(
    coverage_api_key_005_approver_serializes_snake_case,
    ApiCapability::Approver,
    "\"approver\""
);
capability_round_trip!(
    coverage_api_key_006_auditor_serializes_snake_case,
    ApiCapability::Auditor,
    "\"auditor\""
);

#[test]
fn coverage_api_key_007_unknown_capability_string_is_rejected() {
    assert!(serde_json::from_str::<ApiCapability>("\"root\"").is_err());
    assert!(serde_json::from_str::<ApiCapability>("\"admin\"").is_err());
    assert!(serde_json::from_str::<ApiCapability>("\"\"").is_err());
}

#[test]
fn coverage_api_key_008_capability_deserialization_is_case_sensitive() {
    assert!(serde_json::from_str::<ApiCapability>("\"Operator\"").is_err());
    assert!(serde_json::from_str::<ApiCapability>("\"WORKER\"").is_err());
}

#[test]
fn coverage_api_key_009_all_returns_exactly_six_capabilities() {
    assert_eq!(ApiCapability::all().len(), 6);
}

#[test]
fn coverage_api_key_010_all_contains_no_duplicate_capabilities() {
    let unique: HashSet<ApiCapability> = ApiCapability::all().into_iter().collect();
    assert_eq!(unique.len(), 6);
}

#[test]
fn coverage_api_key_011_all_covers_every_declared_variant() {
    let variants = [
        ApiCapability::Operator,
        ApiCapability::Worker,
        ApiCapability::Device,
        ApiCapability::Publisher,
        ApiCapability::Approver,
        ApiCapability::Auditor,
    ];
    let all = ApiCapability::all();
    for variant in variants {
        assert!(all.contains(&variant), "missing capability: {variant:?}");
    }
}

#[test]
fn coverage_api_key_012_all_returns_an_independent_vec_each_call() {
    let mut first = ApiCapability::all();
    first.clear();
    assert_eq!(ApiCapability::all().len(), 6);
}

#[test]
fn coverage_api_key_013_mint_grants_the_full_compatibility_set() {
    let minted = mint("acme", "legacy", None);
    assert_eq!(minted.record.capabilities, ApiCapability::all());
}

#[test]
fn coverage_api_key_014_mint_scoped_preserves_a_single_capability() {
    let minted = mint_scoped("acme", "worker", None, vec![ApiCapability::Worker]);
    assert_eq!(minted.record.capabilities, vec![ApiCapability::Worker]);
}

#[test]
fn coverage_api_key_015_mint_scoped_preserves_capability_order() {
    let grant = vec![ApiCapability::Auditor, ApiCapability::Operator];
    let minted = mint_scoped("acme", "audit-ops", None, grant.clone());
    assert_eq!(minted.record.capabilities, grant);
}

#[test]
fn coverage_api_key_016_mint_scoped_does_not_deduplicate_the_grant() {
    // Storage enforces persistence invariants; minting is a pure pass-through.
    let minted = mint_scoped(
        "acme",
        "dup",
        None,
        vec![ApiCapability::Worker, ApiCapability::Worker],
    );
    assert_eq!(minted.record.capabilities.len(), 2);
}

#[test]
fn coverage_api_key_017_mint_scoped_passes_an_empty_grant_through() {
    // "Empty is never persisted" is a storage-layer contract; the pure mint
    // does not silently upgrade an empty grant to `all()`.
    let minted = mint_scoped("acme", "empty", None, Vec::new());
    assert!(minted.record.capabilities.is_empty());
}

#[test]
fn coverage_api_key_018_mint_scoped_binds_tenant_name_and_expiry() {
    let expiry = Utc::now() + chrono::Duration::hours(1);
    let minted = mint_scoped("tenant-9", "ci", Some(expiry), vec![ApiCapability::Device]);
    assert_eq!(minted.record.tenant_id, "tenant-9");
    assert_eq!(minted.record.name, "ci");
    assert_eq!(minted.record.expires_at, Some(expiry));
}

#[test]
fn coverage_api_key_019_mint_scoped_hash_matches_the_one_time_secret() {
    let minted = mint_scoped("acme", "k", None, vec![ApiCapability::Approver]);
    // The documented secret shape: `sk_` plus two simple UUIDv4 (32 hex each).
    assert!(minted.secret.starts_with("sk_"));
    assert_eq!(minted.secret.len(), 3 + 64);
    assert_eq!(minted.record.key_hash, hash_api_key(&minted.secret));
    // Only the hash is persisted: 64 lowercase hex chars, never the plaintext.
    assert_eq!(minted.record.key_hash.len(), 64);
    assert!(
        minted
            .record
            .key_hash
            .chars()
            .all(|c| c.is_ascii_hexdigit())
    );
    assert_ne!(minted.record.key_hash, minted.secret);
}

#[test]
fn coverage_api_key_020_scoped_mints_generate_unique_ids_and_secrets() {
    let a = mint_scoped("acme", "a", None, vec![ApiCapability::Worker]);
    let b = mint_scoped("acme", "b", None, vec![ApiCapability::Worker]);
    assert_ne!(a.record.id, b.record.id);
    assert_ne!(a.secret, b.secret);
    assert_ne!(a.record.key_hash, b.record.key_hash);
}

#[test]
fn coverage_api_key_021_key_is_inactive_at_the_exact_expiry_instant() {
    // The active window is strict: `expires_at > now`.
    let now = Utc::now();
    let mut record = mint("t", "k", Some(now)).record;
    assert!(!record.is_active(now));
    record.expires_at = Some(now + chrono::Duration::milliseconds(1));
    assert!(record.is_active(now));
}

#[test]
fn coverage_api_key_022_fresh_scoped_record_is_unrevoked_and_never_used() {
    let minted = mint_scoped("acme", "k", None, vec![ApiCapability::Auditor]);
    assert!(!minted.record.revoked);
    assert_eq!(minted.record.last_used_at, None);
    assert!(minted.record.is_active(Utc::now()));
}

#[test]
fn coverage_api_key_023_mint_scoped_id_carries_the_ak_prefix() {
    // The record id is the public handle for listing/revocation: `ak_` plus a
    // 32-char simple UUID.
    let minted = mint_scoped("acme", "k", None, vec![ApiCapability::Worker]);
    assert!(minted.record.id.starts_with("ak_"));
    assert_eq!(minted.record.id.len(), 3 + 32);
}
