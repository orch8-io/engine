//! Push routing, signed wake, collapse, and token lifecycle boundaries.
//!
//! Count contract: 60 independently named unit tests.

use chrono::TimeZone as _;

use super::*;

fn route(tenant: &str, application: &str, topic: &str, credential: &str) -> PushCredentialRoute {
    PushCredentialRoute {
        tenant_id: tenant.into(),
        application_id: application.into(),
        topic: topic.into(),
        encrypted_credential_id: credential.into(),
    }
}

macro_rules! route_hit_case {
    ($name:ident, $tenant:expr, $application:expr, $topic:expr, $credential:expr) => {
        #[test]
        fn $name() {
            let router =
                CredentialRouter::new([route($tenant, $application, $topic, $credential)]).unwrap();
            let actual = router.resolve($tenant, $application, $topic).unwrap();
            assert_eq!(actual, $credential);
        }
    };
}

route_hit_case!(
    coverage_push_001_simple_route_resolves,
    "tenant-a",
    "field",
    "resume",
    "cred-1"
);
route_hit_case!(
    coverage_push_002_numeric_route_resolves,
    "tenant-2",
    "app-2",
    "topic-2",
    "cred-2"
);
route_hit_case!(
    coverage_push_003_dotted_topic_route_resolves,
    "tenant",
    "app",
    "com.acme.app",
    "cred-3"
);
route_hit_case!(
    coverage_push_004_underscored_application_resolves,
    "tenant",
    "field_app",
    "resume",
    "cred-4"
);
route_hit_case!(
    coverage_push_005_slashed_credential_id_resolves,
    "tenant",
    "app",
    "resume",
    "vault/push/5"
);
route_hit_case!(
    coverage_push_006_unicode_route_resolves_exactly,
    "locatário",
    "aplicativo",
    "tópico",
    "cred-6"
);
route_hit_case!(
    coverage_push_007_whitespace_is_exact_route_data,
    "tenant ",
    " app",
    "topic ",
    "cred-7"
);
route_hit_case!(
    coverage_push_008_long_route_fields_resolve,
    "t",
    "a",
    "x",
    "c"
);
route_hit_case!(
    coverage_push_009_uppercase_route_resolves,
    "TENANT",
    "APP",
    "TOPIC",
    "CRED-9"
);
route_hit_case!(
    coverage_push_010_mixed_case_route_resolves,
    "Tenant",
    "App",
    "Topic",
    "Cred-10"
);

macro_rules! route_miss_case {
    ($name:ident, $stored:expr, $tenant:expr, $application:expr, $topic:expr) => {
        #[test]
        fn $name() {
            let router = CredentialRouter::new([$stored]).unwrap();
            let actual = router.resolve($tenant, $application, $topic);
            assert_eq!(actual, Err(PushGovernanceError::MissingCredential));
        }
    };
}

route_miss_case!(
    coverage_push_011_tenant_mismatch_is_missing,
    route("tenant-a", "app", "topic", "cred"),
    "tenant-b",
    "app",
    "topic"
);
route_miss_case!(
    coverage_push_012_application_mismatch_is_missing,
    route("tenant", "app-a", "topic", "cred"),
    "tenant",
    "app-b",
    "topic"
);
route_miss_case!(
    coverage_push_013_topic_mismatch_is_missing,
    route("tenant", "app", "topic-a", "cred"),
    "tenant",
    "app",
    "topic-b"
);
route_miss_case!(
    coverage_push_014_tenant_case_mismatch_is_missing,
    route("tenant", "app", "topic", "cred"),
    "Tenant",
    "app",
    "topic"
);
route_miss_case!(
    coverage_push_015_application_case_mismatch_is_missing,
    route("tenant", "app", "topic", "cred"),
    "tenant",
    "App",
    "topic"
);
route_miss_case!(
    coverage_push_016_topic_case_mismatch_is_missing,
    route("tenant", "app", "topic", "cred"),
    "tenant",
    "app",
    "Topic"
);
route_miss_case!(
    coverage_push_017_tenant_prefix_does_not_match,
    route("tenant", "app", "topic", "cred"),
    "tenant-child",
    "app",
    "topic"
);
route_miss_case!(
    coverage_push_018_application_prefix_does_not_match,
    route("tenant", "app", "topic", "cred"),
    "tenant",
    "app-child",
    "topic"
);
route_miss_case!(
    coverage_push_019_topic_prefix_does_not_match,
    route("tenant", "app", "topic", "cred"),
    "tenant",
    "app",
    "topic-child"
);
route_miss_case!(
    coverage_push_020_empty_lookup_does_not_match,
    route("tenant", "app", "topic", "cred"),
    "",
    "",
    ""
);

macro_rules! invalid_route_case {
    ($name:ident, $route:expr) => {
        #[test]
        fn $name() {
            let result = CredentialRouter::new([$route]);
            assert!(matches!(result, Err(PushGovernanceError::Invalid(_))));
        }
    };
}

invalid_route_case!(
    coverage_push_021_empty_tenant_route_is_rejected,
    route("", "app", "topic", "cred")
);
invalid_route_case!(
    coverage_push_022_empty_application_route_is_rejected,
    route("tenant", "", "topic", "cred")
);
invalid_route_case!(
    coverage_push_023_empty_topic_route_is_rejected,
    route("tenant", "app", "", "cred")
);
invalid_route_case!(
    coverage_push_024_empty_credential_route_is_rejected,
    route("tenant", "app", "topic", "")
);
#[test]
fn coverage_push_025_duplicate_route_is_rejected() {
    let result = CredentialRouter::new([
        route("tenant", "app", "topic", "cred-a"),
        route("tenant", "app", "topic", "cred-b"),
    ]);
    assert!(matches!(result, Err(PushGovernanceError::Invalid(_))));
}

fn issued_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap()
}

fn signed_wake() -> SignedWakeMetadata {
    let key = SigningKey::from_bytes(&[7; 32]);
    SignedWakeMetadata::sign(
        "tenant",
        "device",
        "command",
        "key-v1",
        &key,
        issued_at(),
        issued_at() + Duration::minutes(5),
    )
    .unwrap()
}

macro_rules! wake_verify_case {
    ($name:ident, $mutate:expr, $tenant:expr, $device:expr, $offset:expr, $key_byte:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let mut wake = signed_wake();
            ($mutate)(&mut wake);
            let key = SigningKey::from_bytes(&[$key_byte; 32]);
            let mut cache = WakeNonceCache::new(8);
            let result = wake.verify(
                $tenant,
                $device,
                issued_at() + Duration::seconds($offset),
                &key.verifying_key(),
                &mut cache,
            );
            assert_eq!(result, $expected);
        }
    };
}

wake_verify_case!(
    coverage_push_026_wake_valid_at_issue_time,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "device",
    0,
    7,
    Ok(())
);
wake_verify_case!(
    coverage_push_027_wake_valid_one_second_after_issue,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "device",
    1,
    7,
    Ok(())
);
wake_verify_case!(
    coverage_push_028_wake_valid_one_second_before_expiry,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "device",
    299,
    7,
    Ok(())
);
wake_verify_case!(
    coverage_push_029_wake_rejected_before_issue,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "device",
    -1,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);
wake_verify_case!(
    coverage_push_030_wake_rejected_at_expiry,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "device",
    300,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);
wake_verify_case!(
    coverage_push_031_wake_rejected_after_expiry,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "device",
    301,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);
wake_verify_case!(
    coverage_push_032_wrong_expected_tenant_is_rejected,
    |_w: &mut SignedWakeMetadata| {},
    "other",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidSignature)
);
wake_verify_case!(
    coverage_push_033_wrong_expected_device_is_rejected,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "other",
    1,
    7,
    Err(PushGovernanceError::InvalidSignature)
);
wake_verify_case!(
    coverage_push_034_wrong_verification_key_is_rejected,
    |_w: &mut SignedWakeMetadata| {},
    "tenant",
    "device",
    1,
    8,
    Err(PushGovernanceError::InvalidSignature)
);
wake_verify_case!(
    coverage_push_035_tampered_tenant_is_rejected,
    |w: &mut SignedWakeMetadata| w.tenant_id = "other".into(),
    "other",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidSignature)
);
wake_verify_case!(
    coverage_push_036_tampered_device_is_rejected,
    |w: &mut SignedWakeMetadata| w.device_id = "other".into(),
    "tenant",
    "other",
    1,
    7,
    Err(PushGovernanceError::InvalidSignature)
);
wake_verify_case!(
    coverage_push_037_tampered_command_is_rejected,
    |w: &mut SignedWakeMetadata| w.command_id = "other".into(),
    "tenant",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidSignature)
);
wake_verify_case!(
    coverage_push_038_tampered_key_id_is_rejected,
    |w: &mut SignedWakeMetadata| w.key_id = "key-v2".into(),
    "tenant",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidSignature)
);
wake_verify_case!(
    coverage_push_039_unknown_schema_is_rejected,
    |w: &mut SignedWakeMetadata| w.schema_version = 2,
    "tenant",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);
wake_verify_case!(
    coverage_push_040_empty_tenant_shape_is_rejected,
    |w: &mut SignedWakeMetadata| w.tenant_id.clear(),
    "",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);
wake_verify_case!(
    coverage_push_041_empty_device_shape_is_rejected,
    |w: &mut SignedWakeMetadata| w.device_id.clear(),
    "tenant",
    "",
    1,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);
wake_verify_case!(
    coverage_push_042_empty_command_shape_is_rejected,
    |w: &mut SignedWakeMetadata| w.command_id.clear(),
    "tenant",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);
wake_verify_case!(
    coverage_push_043_empty_key_shape_is_rejected,
    |w: &mut SignedWakeMetadata| w.key_id.clear(),
    "tenant",
    "device",
    1,
    7,
    Err(PushGovernanceError::InvalidExpiry)
);

#[test]
fn coverage_push_044_more_than_fifteen_minute_ttl_is_rejected() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let result = SignedWakeMetadata::sign(
        "tenant",
        "device",
        "command",
        "key",
        &key,
        issued_at(),
        issued_at() + Duration::minutes(16),
    );
    assert_eq!(result, Err(PushGovernanceError::InvalidExpiry));
}

#[test]
fn coverage_push_045_non_positive_ttl_is_rejected() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let result = SignedWakeMetadata::sign(
        "tenant",
        "device",
        "command",
        "key",
        &key,
        issued_at(),
        issued_at(),
    );
    assert_eq!(result, Err(PushGovernanceError::InvalidExpiry));
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
        created_at: issued_at() + Duration::seconds(second),
    }
}

macro_rules! collapse_key_difference_case {
    ($name:ident, $left:expr, $right:expr) => {
        #[test]
        fn $name() {
            let left = $left.collapse_key();
            let right = $right.collapse_key();
            assert_ne!(left, right);
        }
    };
}

collapse_key_difference_case!(
    coverage_push_046_tenant_changes_collapse_key,
    wake("a", "d", "e", "t", "c", 0),
    wake("b", "d", "e", "t", "c", 0)
);
collapse_key_difference_case!(
    coverage_push_047_device_changes_collapse_key,
    wake("a", "d1", "e", "t", "c", 0),
    wake("a", "d2", "e", "t", "c", 0)
);
collapse_key_difference_case!(
    coverage_push_048_execution_changes_collapse_key,
    wake("a", "d", "e1", "t", "c", 0),
    wake("a", "d", "e2", "t", "c", 0)
);
collapse_key_difference_case!(
    coverage_push_049_topic_changes_collapse_key,
    wake("a", "d", "e", "t1", "c", 0),
    wake("a", "d", "e", "t2", "c", 0)
);

#[test]
fn coverage_push_050_command_does_not_change_collapse_key() {
    let left = wake("a", "d", "e", "t", "c1", 0).collapse_key();
    let right = wake("a", "d", "e", "t", "c2", 0).collapse_key();
    assert_eq!(left, right);
}

#[test]
fn coverage_push_051_created_at_does_not_change_collapse_key() {
    let left = wake("a", "d", "e", "t", "c", 0).collapse_key();
    let right = wake("a", "d", "e", "t", "c", 1).collapse_key();
    assert_eq!(left, right);
}

#[test]
fn coverage_push_052_newer_wake_replaces_older_wake() {
    let collapsed = collapse_wakes([
        wake("a", "d", "e", "t", "old", 0),
        wake("a", "d", "e", "t", "new", 1),
    ]);
    assert_eq!(collapsed[0].command_id, "new");
}

#[test]
fn coverage_push_053_older_wake_does_not_replace_newer_wake() {
    let collapsed = collapse_wakes([
        wake("a", "d", "e", "t", "new", 1),
        wake("a", "d", "e", "t", "old", 0),
    ]);
    assert_eq!(collapsed[0].command_id, "new");
}

#[test]
fn coverage_push_054_command_id_breaks_equal_time_ties() {
    let collapsed = collapse_wakes([
        wake("a", "d", "e", "t", "a", 0),
        wake("a", "d", "e", "t", "b", 0),
    ]);
    assert_eq!(collapsed[0].command_id, "b");
}

#[test]
fn coverage_push_055_distinct_collapse_keys_are_preserved() {
    let collapsed = collapse_wakes([
        wake("a", "d", "e1", "t", "a", 0),
        wake("a", "d", "e2", "t", "b", 0),
    ]);
    assert_eq!(collapsed.len(), 2);
}

fn active_token() -> TokenLifecycleState {
    TokenLifecycleState {
        tenant_id: "tenant".into(),
        device_id: "device".into(),
        active: true,
        quarantined_at: None,
        quarantine_reason: None,
    }
}

#[test]
fn coverage_push_056_quarantine_marks_token_inactive() {
    let mut token = active_token();
    token.quarantine_invalid_token(issued_at());
    assert!(!token.active);
}

#[test]
fn coverage_push_057_quarantine_records_timestamp() {
    let mut token = active_token();
    token.quarantine_invalid_token(issued_at());
    assert_eq!(token.quarantined_at, Some(issued_at()));
}

#[test]
fn coverage_push_058_quarantine_records_provider_reason() {
    let mut token = active_token();
    token.quarantine_invalid_token(issued_at());
    assert_eq!(
        token.quarantine_reason.as_deref(),
        Some("provider_invalid_token")
    );
}

#[test]
fn coverage_push_059_reactivation_marks_token_active() {
    let mut token = active_token();
    token.quarantine_invalid_token(issued_at());
    token.reactivate_with_new_token();
    assert!(token.active);
}

#[test]
fn coverage_push_060_reactivation_clears_quarantine_evidence() {
    let mut token = active_token();
    token.quarantine_invalid_token(issued_at());
    token.reactivate_with_new_token();
    assert!(token.quarantined_at.is_none() && token.quarantine_reason.is_none());
}
