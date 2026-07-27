//! Coverage tests for portable capsule export validation and identity.
//!
//! Count contract: 18 independently named unit tests.

use super::*;
use orch8_types::ids::TenantId;

fn options() -> CapsuleExportOptions {
    CapsuleExportOptions {
        source_runtime_id: RuntimeId::new(),
        destination_runtime_id: Some(RuntimeId::new()),
        requirements: CapsuleRequirements::default(),
        expires_in_seconds: 300,
        signing_key_id: "signing".into(),
        encryption_key_id: "encryption".into(),
    }
}

fn execution() -> ContinuityExecution {
    ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: TenantId::unchecked("tenant_1"),
        current_instance_id: InstanceId::new(),
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    }
}

macro_rules! expiry_case {
    ($name:ident, $seconds:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let mut value = options();
            value.expires_in_seconds = $seconds;
            let result = validate_export_options(&value);
            assert_eq!(result.is_ok(), $valid);
            if !$valid {
                // Rejection is a configuration error, not an unrelated failure.
                assert!(matches!(result, Err(Error::Config(_))), "{result:?}");
            }
        }
    };
}

expiry_case!(coverage_portable_001_one_second_expiry_is_allowed, 1, true);
expiry_case!(
    coverage_portable_002_one_hour_expiry_is_allowed,
    3_600,
    true
);
expiry_case!(
    coverage_portable_003_zero_second_expiry_is_rejected,
    0,
    false
);
expiry_case!(
    coverage_portable_004_expiry_above_one_hour_is_rejected,
    3_601,
    false
);
expiry_case!(
    coverage_portable_005_unbounded_expiry_is_rejected,
    u32::MAX,
    false
);

#[test]
fn coverage_portable_006_expiry_error_names_the_bound() {
    let mut value = options();
    value.expires_in_seconds = 0;
    let error = validate_export_options(&value).unwrap_err().to_string();
    assert!(error.contains("between 1 and 3600 seconds"), "{error}");
}

#[test]
fn coverage_portable_007_single_byte_key_ids_are_allowed() {
    let mut value = options();
    value.signing_key_id = "s".into();
    value.encryption_key_id = "e".into();
    assert!(validate_export_options(&value).is_ok());
}

#[test]
fn coverage_portable_008_128_byte_key_ids_are_allowed() {
    let mut value = options();
    value.signing_key_id = "s".repeat(128);
    value.encryption_key_id = "e".repeat(128);
    assert!(validate_export_options(&value).is_ok());
}

#[test]
fn coverage_portable_009_oversized_signing_key_id_is_named_in_the_error() {
    let mut value = options();
    value.signing_key_id = "s".repeat(129);
    let error = validate_export_options(&value).unwrap_err().to_string();
    assert!(error.contains("signing key id"), "{error}");
}

#[test]
fn coverage_portable_010_empty_encryption_key_id_is_named_in_the_error() {
    let mut value = options();
    value.encryption_key_id.clear();
    let error = validate_export_options(&value).unwrap_err().to_string();
    assert!(error.contains("encryption key id"), "{error}");
}

#[test]
fn coverage_portable_011_identical_executions_share_an_identity() {
    let base = execution();
    assert!(same_continuity_identity(&base, &base.clone()));
}

macro_rules! identity_mismatch {
    ($name:ident, $mutate:expr) => {
        #[test]
        fn $name() {
            let base = execution();
            let mut other = base.clone();
            let mutate: fn(&mut ContinuityExecution) = $mutate;
            mutate(&mut other);
            assert!(!same_continuity_identity(&base, &other));
        }
    };
}

identity_mismatch!(
    coverage_portable_012_continuity_id_breaks_identity,
    |value: &mut ContinuityExecution| value.continuity_id = ContinuityId::new()
);
identity_mismatch!(
    coverage_portable_013_tenant_breaks_identity,
    |value: &mut ContinuityExecution| value.tenant_id = TenantId::unchecked("tenant_2")
);
identity_mismatch!(
    coverage_portable_014_instance_breaks_identity,
    |value: &mut ContinuityExecution| value.current_instance_id = InstanceId::new()
);
identity_mismatch!(
    coverage_portable_015_epoch_breaks_identity,
    |value: &mut ContinuityExecution| value.epoch = ExecutionEpoch::from_u64(9)
);
identity_mismatch!(
    coverage_portable_016_state_breaks_identity,
    |value: &mut ContinuityExecution| value.state = OwnershipState::Transferring
);
identity_mismatch!(
    coverage_portable_017_owner_runtime_breaks_identity,
    |value: &mut ContinuityExecution| value.owner_runtime_id = RuntimeId::new()
);

#[test]
fn coverage_portable_018_updated_at_does_not_break_identity() {
    // `updated_at` tracks freshness, not identity: two records describing the
    // same continuity execution must compare identical regardless of it.
    let base = execution();
    let mut other = base.clone();
    other.updated_at += Duration::hours(1);
    assert!(same_continuity_identity(&base, &other));
}
