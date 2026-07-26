//! Governance coverage beyond the boundary suite: nonce-cache eviction and
//! purge semantics, signature envelope tampering, shape limit boundaries,
//! router fan-out, collapse-key encoding, and lifecycle persistence.
//!
//! Count contract: 28 independently named unit tests.

use chrono::TimeZone as _;

use super::*;

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 26, 12, 0, 0).unwrap()
}

fn signed_wake() -> SignedWakeMetadata {
    SignedWakeMetadata::sign(
        "tenant",
        "device",
        "command",
        "key-v1",
        &SigningKey::from_bytes(&[7; 32]),
        t0(),
        t0() + Duration::minutes(5),
    )
    .unwrap()
}

#[test]
fn coverage_governance_001_nonce_cache_capacity_clamps_to_one() {
    let mut cache = WakeNonceCache::new(0);
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    cache
        .consume(first, t0() + Duration::seconds(100), t0())
        .unwrap();
    // Capacity of one: inserting the second nonce evicts the first.
    cache
        .consume(second, t0() + Duration::seconds(50), t0())
        .unwrap();
    assert!(
        cache
            .consume(first, t0() + Duration::seconds(200), t0())
            .is_ok()
    );
}

#[test]
fn coverage_governance_002_nonce_cache_capacity_clamps_to_hundred_thousand() {
    assert_eq!(WakeNonceCache::new(usize::MAX).max_entries, 100_000);
}

#[test]
fn coverage_governance_003_consumed_nonce_replays_as_replay_error() {
    let mut cache = WakeNonceCache::new(8);
    let nonce = Uuid::new_v4();
    cache
        .consume(nonce, t0() + Duration::seconds(60), t0())
        .unwrap();
    assert_eq!(
        cache.consume(nonce, t0() + Duration::seconds(60), t0()),
        Err(PushGovernanceError::Replay)
    );
}

#[test]
fn coverage_governance_004_expired_nonce_can_be_consumed_again() {
    let mut cache = WakeNonceCache::new(8);
    let nonce = Uuid::new_v4();
    let later = t0() + Duration::seconds(20);
    cache
        .consume(nonce, t0() + Duration::seconds(10), t0())
        .unwrap();
    // After the original expiry passes, the nonce is purged before the
    // replay check, so a fresh wake may reuse it.
    assert!(
        cache
            .consume(nonce, later + Duration::seconds(10), later)
            .is_ok()
    );
}

#[test]
fn coverage_governance_005_eviction_removes_earliest_expiring_entry() {
    let mut cache = WakeNonceCache::new(2);
    let late = Uuid::new_v4();
    let early = Uuid::new_v4();
    let fresh = Uuid::new_v4();
    cache
        .consume(late, t0() + Duration::seconds(100), t0())
        .unwrap();
    cache
        .consume(early, t0() + Duration::seconds(50), t0())
        .unwrap();
    // Full: the earliest-expiring entry (early) is evicted, not the oldest insert.
    cache
        .consume(fresh, t0() + Duration::seconds(200), t0())
        .unwrap();
    assert_eq!(
        cache.consume(late, t0() + Duration::seconds(100), t0()),
        Err(PushGovernanceError::Replay)
    );
    assert!(
        cache
            .consume(early, t0() + Duration::seconds(50), t0())
            .is_ok()
    );
    assert_eq!(
        cache.consume(fresh, t0() + Duration::seconds(200), t0()),
        Err(PushGovernanceError::Replay)
    );
}

#[test]
fn coverage_governance_006_below_capacity_nothing_is_evicted() {
    let mut cache = WakeNonceCache::new(4);
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    cache
        .consume(first, t0() + Duration::seconds(100), t0())
        .unwrap();
    cache
        .consume(second, t0() + Duration::seconds(50), t0())
        .unwrap();
    // Both entries survive: re-consuming either is a replay.
    assert_eq!(
        cache.consume(first, t0() + Duration::seconds(100), t0()),
        Err(PushGovernanceError::Replay)
    );
    assert_eq!(
        cache.consume(second, t0() + Duration::seconds(50), t0()),
        Err(PushGovernanceError::Replay)
    );
}

#[test]
fn coverage_governance_007_entry_expiring_exactly_at_now_is_purged() {
    let mut cache = WakeNonceCache::new(8);
    let nonce = Uuid::new_v4();
    let expiry = t0() + Duration::seconds(10);
    cache.consume(nonce, expiry, t0()).unwrap();
    // The purge uses `expiry > now`, so at exactly `expiry` the entry is gone.
    assert!(
        cache
            .consume(nonce, expiry + Duration::seconds(10), expiry)
            .is_ok()
    );
}

#[test]
fn coverage_governance_008_sign_issues_unique_version_four_nonces() {
    let first = signed_wake();
    let second = signed_wake();
    assert_ne!(first.nonce, second.nonce);
    assert_eq!(first.nonce.get_version_num(), 4);
}

#[test]
fn coverage_governance_009_signature_is_url_safe_base64_of_64_bytes() {
    let wake = signed_wake();
    let decoded = URL_SAFE_NO_PAD.decode(&wake.signature).unwrap();
    assert_eq!(decoded.len(), 64);
    assert!(!wake.signature.contains(['+', '/', '=']));
}

#[test]
fn coverage_governance_010_tampered_nonce_is_rejected() {
    let mut wake = signed_wake();
    wake.nonce = Uuid::new_v4();
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut cache = WakeNonceCache::new(8);
    assert_eq!(
        wake.verify(
            "tenant",
            "device",
            t0() + Duration::seconds(1),
            &key.verifying_key(),
            &mut cache
        ),
        Err(PushGovernanceError::InvalidSignature)
    );
}

#[test]
fn coverage_governance_011_tampered_issued_at_is_rejected() {
    let mut wake = signed_wake();
    wake.issued_at -= Duration::seconds(60);
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut cache = WakeNonceCache::new(8);
    assert_eq!(
        wake.verify(
            "tenant",
            "device",
            t0() + Duration::seconds(1),
            &key.verifying_key(),
            &mut cache
        ),
        Err(PushGovernanceError::InvalidSignature)
    );
}

#[test]
fn coverage_governance_012_tampered_expires_at_is_rejected() {
    let mut wake = signed_wake();
    wake.expires_at += Duration::minutes(5);
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut cache = WakeNonceCache::new(8);
    assert_eq!(
        wake.verify(
            "tenant",
            "device",
            t0() + Duration::seconds(1),
            &key.verifying_key(),
            &mut cache
        ),
        Err(PushGovernanceError::InvalidSignature)
    );
}

#[test]
fn coverage_governance_013_non_base64_signature_is_rejected() {
    let mut wake = signed_wake();
    wake.signature = "!!!not-base64!!!".into();
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut cache = WakeNonceCache::new(8);
    assert_eq!(
        wake.verify(
            "tenant",
            "device",
            t0() + Duration::seconds(1),
            &key.verifying_key(),
            &mut cache
        ),
        Err(PushGovernanceError::InvalidSignature)
    );
}

#[test]
fn coverage_governance_014_wrong_length_signature_is_rejected() {
    let mut wake = signed_wake();
    wake.signature = URL_SAFE_NO_PAD.encode([0_u8; 32]);
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut cache = WakeNonceCache::new(8);
    assert_eq!(
        wake.verify(
            "tenant",
            "device",
            t0() + Duration::seconds(1),
            &key.verifying_key(),
            &mut cache
        ),
        Err(PushGovernanceError::InvalidSignature)
    );
}

#[test]
fn coverage_governance_015_field_length_boundary_is_256_bytes() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let at_limit = "t".repeat(256);
    assert!(
        SignedWakeMetadata::sign(
            &at_limit,
            "device",
            "command",
            "key",
            &key,
            t0(),
            t0() + Duration::minutes(5),
        )
        .is_ok()
    );
    let over_limit = "t".repeat(257);
    assert_eq!(
        SignedWakeMetadata::sign(
            &over_limit,
            "device",
            "command",
            "key",
            &key,
            t0(),
            t0() + Duration::minutes(5),
        ),
        Err(PushGovernanceError::InvalidExpiry)
    );
}

#[test]
fn coverage_governance_016_exactly_fifteen_minute_ttl_is_accepted() {
    let key = SigningKey::from_bytes(&[7; 32]);
    assert!(
        SignedWakeMetadata::sign(
            "tenant",
            "device",
            "command",
            "key",
            &key,
            t0(),
            t0() + Duration::minutes(15),
        )
        .is_ok()
    );
}

#[test]
fn coverage_governance_017_identity_mismatch_takes_precedence_over_expiry() {
    let wake = signed_wake();
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut cache = WakeNonceCache::new(8);
    // The wake is expired AND addressed to another tenant: the binding
    // failure must surface as a signature error, not an expiry error.
    assert_eq!(
        wake.verify(
            "other",
            "device",
            t0() + Duration::hours(1),
            &key.verifying_key(),
            &mut cache
        ),
        Err(PushGovernanceError::InvalidSignature)
    );
}

#[test]
fn coverage_governance_018_default_router_resolves_nothing() {
    let router = CredentialRouter::default();
    assert_eq!(
        router.resolve("tenant", "app", "topic"),
        Err(PushGovernanceError::MissingCredential)
    );
}

#[test]
fn coverage_governance_019_multiple_routes_resolve_independently() {
    let router = CredentialRouter::new([
        PushCredentialRoute {
            tenant_id: "tenant-a".into(),
            application_id: "field".into(),
            topic: "resume".into(),
            encrypted_credential_id: "cred-1".into(),
        },
        PushCredentialRoute {
            tenant_id: "tenant-a".into(),
            application_id: "field".into(),
            topic: "alert".into(),
            encrypted_credential_id: "cred-2".into(),
        },
        PushCredentialRoute {
            tenant_id: "tenant-b".into(),
            application_id: "field".into(),
            topic: "resume".into(),
            encrypted_credential_id: "cred-3".into(),
        },
    ])
    .unwrap();
    assert_eq!(
        router.resolve("tenant-a", "field", "resume").unwrap(),
        "cred-1"
    );
    assert_eq!(
        router.resolve("tenant-a", "field", "alert").unwrap(),
        "cred-2"
    );
    assert_eq!(
        router.resolve("tenant-b", "field", "resume").unwrap(),
        "cred-3"
    );
    assert_eq!(
        router.resolve("tenant-b", "field", "alert"),
        Err(PushGovernanceError::MissingCredential)
    );
}

struct FailingCredentialSource(std::sync::Mutex<u32>);

#[async_trait]
impl EncryptedPushCredentialSource for FailingCredentialSource {
    async fn load_provider(
        &self,
        _encrypted_credential_id: &str,
    ) -> Result<Arc<dyn crate::PushProvider>, PushGovernanceError> {
        *self.0.lock().unwrap() += 1;
        Err(PushGovernanceError::Invalid("vault offline".into()))
    }
}

#[tokio::test]
async fn coverage_governance_020_provider_for_missing_route_skips_the_source() {
    let router = CredentialRouter::default();
    let source = FailingCredentialSource(std::sync::Mutex::new(0));
    let result = router.provider_for(&source, "tenant", "app", "topic").await;
    assert_eq!(
        result.map(|_| ()),
        Err(PushGovernanceError::MissingCredential)
    );
    assert_eq!(*source.0.lock().unwrap(), 0);
}

#[tokio::test]
async fn coverage_governance_021_provider_for_propagates_source_failure() {
    let router = CredentialRouter::new([PushCredentialRoute {
        tenant_id: "tenant".into(),
        application_id: "app".into(),
        topic: "topic".into(),
        encrypted_credential_id: "cred".into(),
    }])
    .unwrap();
    let source = FailingCredentialSource(std::sync::Mutex::new(0));
    let result = router.provider_for(&source, "tenant", "app", "topic").await;
    assert_eq!(
        result.map(|_| ()),
        Err(PushGovernanceError::Invalid("vault offline".into()))
    );
    assert_eq!(*source.0.lock().unwrap(), 1);
}

fn wake(
    tenant: &str,
    device: &str,
    execution: &str,
    topic: &str,
    command: &str,
    second: i64,
) -> CollapsibleWake {
    CollapsibleWake {
        tenant_id: tenant.into(),
        device_id: device.into(),
        execution_id: execution.into(),
        topic: topic.into(),
        command_id: command.into(),
        created_at: t0() + Duration::seconds(second),
    }
}

#[test]
fn coverage_governance_022_empty_input_collapses_to_empty_output() {
    assert!(collapse_wakes(Vec::new()).is_empty());
}

#[test]
fn coverage_governance_023_length_prefixing_prevents_concatenation_ambiguity() {
    // Without length prefixes, ("ab", "c") and ("a", "bc") would hash the
    // same concatenated bytes and collapse onto each other.
    let left = wake("ab", "c", "e", "t", "cmd", 0).collapse_key();
    let right = wake("a", "bc", "e", "t", "cmd", 0).collapse_key();
    assert_ne!(left, right);
}

#[test]
fn coverage_governance_024_collapse_key_is_64_lowercase_hex_chars() {
    let key = wake("tenant", "device", "exec", "topic", "cmd", 0).collapse_key();
    assert_eq!(key.len(), 64);
    assert!(
        key.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
}

#[test]
fn coverage_governance_025_many_same_key_wakes_keep_single_newest() {
    let collapsed = collapse_wakes([
        wake("a", "d", "e", "t", "c1", 0),
        wake("a", "d", "e", "t", "c2", 3),
        wake("a", "d", "e", "t", "c3", 1),
        wake("a", "d", "e", "t", "c4", 2),
        wake("a", "d", "e", "t", "c5", 4),
    ]);
    assert_eq!(collapsed.len(), 1);
    assert_eq!(collapsed[0].command_id, "c5");
}

#[test]
fn coverage_governance_026_requarantine_refreshes_timestamp_and_keeps_reason() {
    let mut state = TokenLifecycleState {
        tenant_id: "tenant".into(),
        device_id: "device".into(),
        active: true,
        quarantined_at: None,
        quarantine_reason: None,
    };
    state.quarantine_invalid_token(t0());
    state.quarantine_invalid_token(t0() + Duration::seconds(30));
    assert!(!state.active);
    assert_eq!(state.quarantined_at, Some(t0() + Duration::seconds(30)));
    assert_eq!(
        state.quarantine_reason.as_deref(),
        Some("provider_invalid_token")
    );
}

#[test]
fn coverage_governance_027_token_lifecycle_state_survives_serde_round_trip() {
    let mut state = TokenLifecycleState {
        tenant_id: "tenant".into(),
        device_id: "device".into(),
        active: true,
        quarantined_at: None,
        quarantine_reason: None,
    };
    state.quarantine_invalid_token(t0());
    let encoded = serde_json::to_string(&state).unwrap();
    let decoded: TokenLifecycleState = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, state);
}

#[test]
fn coverage_governance_028_verified_wake_nonce_replays_on_second_verify() {
    // The nonce cache is only exercised end-to-end here: a wake that passes
    // verification once consumes its nonce, so the identical second verify
    // must surface Replay rather than succeeding again.
    let wake = signed_wake();
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut cache = WakeNonceCache::new(8);
    let now = t0() + Duration::seconds(1);
    assert_eq!(
        wake.verify("tenant", "device", now, &key.verifying_key(), &mut cache),
        Ok(())
    );
    assert_eq!(
        wake.verify("tenant", "device", now, &key.verifying_key(), &mut cache),
        Err(PushGovernanceError::Replay)
    );
}
