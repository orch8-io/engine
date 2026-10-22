//! Push governance rules: credential routes match only the exact
//! (tenant, application, topic) triple; a signed wake verifies only for its
//! bound tenant/device, inside `[issued_at, expires_at)`, with every signed
//! field intact and a nonce that has never been consumed; the replay cache
//! never forgets unexpired evidence to make room; and wake collapse keeps the
//! newest command per (tenant, device, execution, topic).

use chrono::TimeZone as _;

use super::*;

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 26, 12, 0, 0).unwrap()
}

fn key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

fn route(tenant: &str, application: &str, topic: &str, credential: &str) -> PushCredentialRoute {
    PushCredentialRoute {
        tenant_id: tenant.into(),
        application_id: application.into(),
        topic: topic.into(),
        encrypted_credential_id: credential.into(),
    }
}

/// A wake for tenant/device valid for five minutes from `t0`.
fn signed_wake() -> SignedWakeMetadata {
    SignedWakeMetadata::sign(
        "tenant",
        "device",
        "command",
        "key-v1",
        &key(),
        t0(),
        t0() + Duration::minutes(5),
    )
    .unwrap()
}

fn verify_at(
    wake: &SignedWakeMetadata,
    tenant: &str,
    device: &str,
    now: DateTime<Utc>,
    signing_key: &SigningKey,
) -> Result<(), PushGovernanceError> {
    let mut cache = WakeNonceCache::new(8);
    wake.verify(
        tenant,
        device,
        now,
        &signing_key.verifying_key(),
        &mut cache,
    )
}

fn collapsible(
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

// ---------------------------------------------------------------- routing

#[test]
fn credential_routes_resolve_only_the_exact_tenant_application_topic() {
    let router = CredentialRouter::new([
        route("tenant", "app", "topic", "cred-1"),
        route("tenant", "app", "alert", "cred-2"),
        route("other", "app", "topic", "vault/push/3"),
        route("locatário", "aplicativo", "tópico", "cred-4"),
        route("tenant ", " app", "topic ", "cred-5"),
    ])
    .unwrap();

    let hits = [
        (("tenant", "app", "topic"), "cred-1"),
        (("tenant", "app", "alert"), "cred-2"),
        (("other", "app", "topic"), "vault/push/3"),
        (("locatário", "aplicativo", "tópico"), "cred-4"),
        (("tenant ", " app", "topic "), "cred-5"),
    ];
    for ((tenant, application, topic), credential) in hits {
        assert_eq!(
            router.resolve(tenant, application, topic),
            Ok(credential),
            "hit {tenant}/{application}/{topic}"
        );
    }

    let misses = [
        ("other tenant's topic", ("other", "app", "alert")),
        ("unknown application", ("tenant", "app-b", "topic")),
        ("tenant case differs", ("Tenant", "app", "topic")),
        ("application case differs", ("tenant", "App", "topic")),
        ("topic case differs", ("tenant", "app", "Topic")),
        ("tenant prefix", ("tenant-child", "app", "topic")),
        ("application prefix", ("tenant", "app-child", "topic")),
        ("topic prefix", ("tenant", "app", "topic-child")),
        ("whitespace is not trimmed", ("tenant", "app", "topic ")),
        ("empty lookup", ("", "", "")),
    ];
    for (case, (tenant, application, topic)) in misses {
        assert_eq!(
            router.resolve(tenant, application, topic),
            Err(PushGovernanceError::MissingCredential),
            "{case}"
        );
    }
}

#[test]
fn credential_routes_reject_empty_fields_and_duplicate_triples() {
    let cases = [
        ("empty tenant", vec![route("", "app", "topic", "cred")]),
        (
            "empty application",
            vec![route("tenant", "", "topic", "cred")],
        ),
        ("empty topic", vec![route("tenant", "app", "", "cred")]),
        (
            "empty credential",
            vec![route("tenant", "app", "topic", "")],
        ),
        (
            "duplicate triple",
            vec![
                route("tenant", "app", "topic", "cred-a"),
                route("tenant", "app", "topic", "cred-b"),
            ],
        ),
    ];
    for (case, routes) in cases {
        assert!(
            matches!(
                CredentialRouter::new(routes),
                Err(PushGovernanceError::Invalid(_))
            ),
            "{case}"
        );
    }
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
async fn provider_lookup_for_an_unrouted_triple_never_touches_the_credential_source() {
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
async fn provider_lookup_propagates_a_credential_source_failure() {
    let router = CredentialRouter::new([route("tenant", "app", "topic", "cred")]).unwrap();
    let source = FailingCredentialSource(std::sync::Mutex::new(0));

    let result = router.provider_for(&source, "tenant", "app", "topic").await;

    assert_eq!(
        result.map(|_| ()),
        Err(PushGovernanceError::Invalid("vault offline".into()))
    );
    assert_eq!(*source.0.lock().unwrap(), 1);
}

// ------------------------------------------------------------ signed wake

#[test]
fn wake_verifies_only_from_issued_at_inclusive_to_expires_at_exclusive() {
    let wake = signed_wake();
    let cases = [
        (-1, Err(PushGovernanceError::InvalidExpiry)),
        (0, Ok(())),
        (1, Ok(())),
        (299, Ok(())),
        (300, Err(PushGovernanceError::InvalidExpiry)),
        (301, Err(PushGovernanceError::InvalidExpiry)),
    ];
    for (offset, expected) in cases {
        let now = t0() + Duration::seconds(offset);
        assert_eq!(
            verify_at(&wake, "tenant", "device", now, &key()),
            expected,
            "offset {offset}s"
        );
    }
}

#[test]
fn wake_with_a_mismatched_binding_or_any_tampered_signed_field_is_rejected() {
    type Mutation = fn(&mut SignedWakeMetadata);
    let cases: [(&str, Mutation, &str, &str, u8); 12] = [
        ("expected tenant differs", |_| {}, "other", "device", 7),
        ("expected device differs", |_| {}, "tenant", "other", 7),
        ("different verification key", |_| {}, "tenant", "device", 8),
        (
            "tenant tampered",
            |w| w.tenant_id = "other".into(),
            "other",
            "device",
            7,
        ),
        (
            "device tampered",
            |w| w.device_id = "other".into(),
            "tenant",
            "other",
            7,
        ),
        (
            "command tampered",
            |w| w.command_id = "other".into(),
            "tenant",
            "device",
            7,
        ),
        (
            "key id tampered",
            |w| w.key_id = "key-v2".into(),
            "tenant",
            "device",
            7,
        ),
        (
            "nonce tampered",
            |w| w.nonce = Uuid::new_v4(),
            "tenant",
            "device",
            7,
        ),
        (
            "issued_at tampered",
            |w| w.issued_at -= Duration::seconds(60),
            "tenant",
            "device",
            7,
        ),
        (
            "expires_at tampered",
            |w| w.expires_at += Duration::minutes(5),
            "tenant",
            "device",
            7,
        ),
        (
            "signature not base64",
            |w| w.signature = "!!!not-base64!!!".into(),
            "tenant",
            "device",
            7,
        ),
        (
            "signature wrong length",
            |w| w.signature = URL_SAFE_NO_PAD.encode([0_u8; 32]),
            "tenant",
            "device",
            7,
        ),
    ];
    for (case, mutate, tenant, device, key_byte) in cases {
        let mut wake = signed_wake();
        mutate(&mut wake);
        let signing_key = SigningKey::from_bytes(&[key_byte; 32]);
        assert_eq!(
            verify_at(
                &wake,
                tenant,
                device,
                t0() + Duration::seconds(1),
                &signing_key
            ),
            Err(PushGovernanceError::InvalidSignature),
            "{case}"
        );
    }
}

#[test]
fn identity_mismatch_is_reported_before_expiry() {
    // Expired AND addressed to another tenant: the binding failure wins.
    let wake = signed_wake();

    let result = verify_at(&wake, "other", "device", t0() + Duration::hours(1), &key());

    assert_eq!(result, Err(PushGovernanceError::InvalidSignature));
}

#[test]
fn wake_ttl_must_be_positive_and_at_most_fifteen_minutes() {
    let cases = [
        (
            "zero ttl",
            Duration::zero(),
            Err(PushGovernanceError::InvalidExpiry),
        ),
        (
            "negative ttl",
            Duration::seconds(-1),
            Err(PushGovernanceError::InvalidExpiry),
        ),
        ("exactly fifteen minutes", Duration::minutes(15), Ok(())),
        (
            "one second over fifteen minutes",
            Duration::minutes(15) + Duration::seconds(1),
            Err(PushGovernanceError::InvalidExpiry),
        ),
    ];
    for (case, ttl, expected) in cases {
        let result = SignedWakeMetadata::sign(
            "tenant",
            "device",
            "command",
            "key",
            &key(),
            t0(),
            t0() + ttl,
        );
        assert_eq!(result.map(|_| ()), expected, "{case}");
    }
}

/// Shape rows shared by the two malformed-shape tests below.
fn malformed_shapes() -> Vec<(&'static str, Result<(), PushGovernanceError>)> {
    let mutated = |mutate: fn(&mut SignedWakeMetadata)| {
        let mut wake = signed_wake();
        mutate(&mut wake);
        verify_at(
            &wake,
            &wake.tenant_id.clone(),
            &wake.device_id.clone(),
            t0(),
            &key(),
        )
    };
    let oversized_tenant = SignedWakeMetadata::sign(
        "t".repeat(257),
        "device",
        "command",
        "key",
        &key(),
        t0(),
        t0() + Duration::minutes(5),
    )
    .map(|_| ());
    vec![
        ("unknown schema version", mutated(|w| w.schema_version = 2)),
        ("empty tenant", mutated(|w| w.tenant_id.clear())),
        ("empty device", mutated(|w| w.device_id.clear())),
        ("empty command", mutated(|w| w.command_id.clear())),
        ("empty key id", mutated(|w| w.key_id.clear())),
        ("257-byte tenant", oversized_tenant),
    ]
}

#[test]
fn wake_with_a_malformed_shape_is_never_accepted() {
    // The exact error variant is asserted by the regression below.
    for (case, result) in malformed_shapes() {
        assert!(result.is_err(), "{case} must be rejected");
    }
    let at_limit = SignedWakeMetadata::sign(
        "t".repeat(256),
        "device",
        "command",
        "key",
        &key(),
        t0(),
        t0() + Duration::minutes(5),
    );
    assert!(at_limit.is_ok(), "256-byte fields are within the limit");
}

#[test]
fn wake_with_a_malformed_shape_is_reported_as_invalid_not_expired() {
    for (case, result) in malformed_shapes() {
        assert!(
            matches!(result, Err(PushGovernanceError::Invalid(_))),
            "{case}: {result:?}"
        );
    }
}

#[test]
fn each_signed_wake_carries_a_fresh_nonce() {
    assert_ne!(signed_wake().nonce, signed_wake().nonce);
}

#[test]
fn wake_signature_is_unpadded_url_safe_base64_of_an_ed25519_signature() {
    let wake = signed_wake();

    let decoded = URL_SAFE_NO_PAD.decode(&wake.signature).unwrap();

    assert_eq!(decoded.len(), 64);
    assert!(!wake.signature.contains(['+', '/', '=']));
}

// ------------------------------------------------------------ replay cache

#[test]
fn replay_cache_of_capacity_zero_still_holds_one_nonce() {
    let mut cache = WakeNonceCache::new(0);
    let first = Uuid::new_v4();
    cache
        .consume(first, t0() + Duration::seconds(100), t0())
        .unwrap();

    assert_eq!(
        cache.consume(Uuid::new_v4(), t0() + Duration::seconds(50), t0()),
        Err(PushGovernanceError::ReplayCacheFull)
    );
    assert_eq!(
        cache.consume(first, t0() + Duration::seconds(200), t0()),
        Err(PushGovernanceError::Replay)
    );
}

#[test]
fn nonce_is_reusable_once_its_wake_has_expired() {
    let cases = [
        ("well after expiry", Duration::seconds(20)),
        ("exactly at expiry", Duration::seconds(10)),
    ];
    for (case, later) in cases {
        let mut cache = WakeNonceCache::new(8);
        let nonce = Uuid::new_v4();
        cache
            .consume(nonce, t0() + Duration::seconds(10), t0())
            .unwrap();
        let now = t0() + later;

        assert_eq!(
            cache.consume(nonce, now + Duration::seconds(10), now),
            Ok(()),
            "{case}"
        );
    }
}

#[test]
fn full_replay_cache_keeps_unexpired_evidence_and_admits_new_wakes_only_after_expiry() {
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

    assert_eq!(
        cache.consume(fresh, t0() + Duration::seconds(200), t0()),
        Err(PushGovernanceError::ReplayCacheFull)
    );
    assert_eq!(
        cache.consume(late, t0() + Duration::seconds(100), t0()),
        Err(PushGovernanceError::Replay)
    );
    assert_eq!(
        cache.consume(early, t0() + Duration::seconds(50), t0()),
        Err(PushGovernanceError::Replay)
    );

    let after_early_expiry = t0() + Duration::seconds(50);
    assert_eq!(
        cache.consume(fresh, t0() + Duration::seconds(200), after_early_expiry),
        Ok(())
    );
    assert_eq!(
        cache.consume(late, t0() + Duration::seconds(100), after_early_expiry),
        Err(PushGovernanceError::Replay)
    );
}

#[test]
fn full_replay_cache_rejects_a_valid_signed_wake_until_capacity_expires() {
    let sign = |command: &str, ttl_secs: i64| {
        SignedWakeMetadata::sign(
            "tenant",
            "device",
            command,
            "key-v1",
            &key(),
            t0(),
            t0() + Duration::seconds(ttl_secs),
        )
        .unwrap()
    };
    let first = sign("first-command", 30);
    let second = sign("second-command", 60);
    let verifying_key = key().verifying_key();
    let mut cache = WakeNonceCache::new(1);
    let now = t0() + Duration::seconds(1);
    first
        .verify("tenant", "device", now, &verifying_key, &mut cache)
        .unwrap();

    assert_eq!(
        second.verify("tenant", "device", now, &verifying_key, &mut cache),
        Err(PushGovernanceError::ReplayCacheFull)
    );
    assert_eq!(
        first.verify("tenant", "device", now, &verifying_key, &mut cache),
        Err(PushGovernanceError::Replay)
    );
    assert_eq!(
        second.verify(
            "tenant",
            "device",
            t0() + Duration::seconds(30),
            &verifying_key,
            &mut cache
        ),
        Ok(())
    );
}

// ---------------------------------------------------------------- collapse

#[test]
fn collapse_key_separates_every_routing_component() {
    let base = collapsible("a", "d", "e", "t", "c", 0);
    let cases = [
        ("tenant", collapsible("b", "d", "e", "t", "c", 0)),
        ("device", collapsible("a", "d2", "e", "t", "c", 0)),
        ("execution", collapsible("a", "d", "e2", "t", "c", 0)),
        ("topic", collapsible("a", "d", "e", "t2", "c", 0)),
    ];
    for (component, other) in cases {
        assert_ne!(
            base.collapse_key(),
            other.collapse_key(),
            "{component} must change the collapse key"
        );
    }
    // Length prefixes keep ("ab","c") and ("a","bc") apart.
    assert_ne!(
        collapsible("ab", "c", "e", "t", "cmd", 0).collapse_key(),
        collapsible("a", "bc", "e", "t", "cmd", 0).collapse_key()
    );
}

#[test]
fn collapse_key_ignores_command_and_creation_time() {
    let base = collapsible("a", "d", "e", "t", "c1", 0).collapse_key();

    assert_eq!(
        base,
        collapsible("a", "d", "e", "t", "c2", 0).collapse_key()
    );
    assert_eq!(
        base,
        collapsible("a", "d", "e", "t", "c1", 9).collapse_key()
    );
}

#[test]
fn collapse_keeps_the_newest_wake_regardless_of_arrival_order() {
    let same = |command: &str, second: i64| collapsible("a", "d", "e", "t", command, second);
    let cases = [
        (
            "newer arrives last",
            vec![same("old", 0), same("new", 1)],
            "new",
        ),
        (
            "newer arrives first",
            vec![same("new", 1), same("old", 0)],
            "new",
        ),
        (
            "equal times tie-break on the larger command id",
            vec![same("b", 0), same("a", 0)],
            "b",
        ),
        (
            "shuffled run",
            vec![
                same("c1", 0),
                same("c2", 3),
                same("c3", 1),
                same("c5", 4),
                same("c4", 2),
            ],
            "c5",
        ),
    ];
    for (case, wakes, newest) in cases {
        let collapsed = collapse_wakes(wakes);
        assert_eq!(collapsed.len(), 1, "{case}");
        assert_eq!(collapsed[0].command_id, newest, "{case}");
    }
}

// --------------------------------------------------------- token lifecycle

#[test]
fn requarantine_refreshes_the_timestamp_and_keeps_the_reason() {
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
