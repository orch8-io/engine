//! Protected-field redaction and handoff boundaries.
//!
//! Count contract: 35 independently named unit tests.

use super::*;

fn protected() -> BTreeSet<String> {
    ["ssn".to_string(), "secret".to_string(), "photo".to_string()]
        .into_iter()
        .collect()
}

fn boundary() -> ProtectedFieldBoundary {
    ProtectedFieldBoundary::new(FieldEncryptor::from_bytes(&[11; 32]), protected()).unwrap()
}

macro_rules! sanitize_case {
    ($name:ident, $input:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let actual = sanitize_value(&$input, &protected());
            assert_eq!(actual, $expected);
        }
    };
}

sanitize_case!(
    coverage_privacy_001_null_is_unchanged,
    serde_json::Value::Null,
    serde_json::Value::Null
);
sanitize_case!(
    coverage_privacy_002_boolean_is_unchanged,
    serde_json::json!(true),
    serde_json::json!(true)
);
sanitize_case!(
    coverage_privacy_003_number_is_unchanged,
    serde_json::json!(42),
    serde_json::json!(42)
);
sanitize_case!(
    coverage_privacy_004_string_is_unchanged,
    serde_json::json!("public"),
    serde_json::json!("public")
);
sanitize_case!(
    coverage_privacy_005_empty_array_is_unchanged,
    serde_json::json!([]),
    serde_json::json!([])
);
sanitize_case!(
    coverage_privacy_006_empty_object_is_unchanged,
    serde_json::json!({}),
    serde_json::json!({})
);
sanitize_case!(
    coverage_privacy_007_ssn_is_redacted,
    serde_json::json!({"ssn":"123"}),
    serde_json::json!({"ssn":"[PROTECTED]"})
);
sanitize_case!(
    coverage_privacy_008_secret_is_redacted,
    serde_json::json!({"secret":"value"}),
    serde_json::json!({"secret":"[PROTECTED]"})
);
sanitize_case!(
    coverage_privacy_009_photo_is_redacted,
    serde_json::json!({"photo":"bytes"}),
    serde_json::json!({"photo":"[PROTECTED]"})
);
sanitize_case!(
    coverage_privacy_010_nested_ssn_is_redacted,
    serde_json::json!({"user":{"ssn":"123"}}),
    serde_json::json!({"user":{"ssn":"[PROTECTED]"}})
);
sanitize_case!(
    coverage_privacy_011_deep_secret_is_redacted,
    serde_json::json!({"a":{"b":{"secret":"x"}}}),
    serde_json::json!({"a":{"b":{"secret":"[PROTECTED]"}}})
);
sanitize_case!(
    coverage_privacy_012_array_object_secret_is_redacted,
    serde_json::json!([{"secret":"x"}]),
    serde_json::json!([{"secret":"[PROTECTED]"}])
);
sanitize_case!(
    coverage_privacy_013_multiple_array_secrets_are_redacted,
    serde_json::json!([{"secret":"x"},{"secret":"y"}]),
    serde_json::json!([{"secret":"[PROTECTED]"},{"secret":"[PROTECTED]"}])
);
sanitize_case!(
    coverage_privacy_014_nested_array_photo_is_redacted,
    serde_json::json!({"items":[{"photo":"x"}]}),
    serde_json::json!({"items":[{"photo":"[PROTECTED]"}]})
);
sanitize_case!(
    coverage_privacy_015_protected_object_is_replaced_wholly,
    serde_json::json!({"secret":{"nested":"x"}}),
    serde_json::json!({"secret":"[PROTECTED]"})
);
sanitize_case!(
    coverage_privacy_016_protected_array_is_replaced_wholly,
    serde_json::json!({"photo":["x","y"]}),
    serde_json::json!({"photo":"[PROTECTED]"})
);
sanitize_case!(
    coverage_privacy_017_protected_null_is_replaced,
    serde_json::json!({"ssn":null}),
    serde_json::json!({"ssn":"[PROTECTED]"})
);
sanitize_case!(
    coverage_privacy_018_public_sibling_is_preserved,
    serde_json::json!({"ssn":"x","case":"A"}),
    serde_json::json!({"ssn":"[PROTECTED]","case":"A"})
);
sanitize_case!(
    coverage_privacy_019_each_protected_key_is_redacted,
    serde_json::json!({"ssn":"x","secret":"y","photo":"z"}),
    serde_json::json!({"ssn":"[PROTECTED]","secret":"[PROTECTED]","photo":"[PROTECTED]"})
);
sanitize_case!(
    coverage_privacy_020_case_changed_key_is_not_redacted,
    serde_json::json!({"SSN":"x"}),
    serde_json::json!({"SSN":"x"})
);
sanitize_case!(
    coverage_privacy_021_prefixed_key_is_not_redacted,
    serde_json::json!({"user_ssn":"x"}),
    serde_json::json!({"user_ssn":"x"})
);
sanitize_case!(
    coverage_privacy_022_suffixed_key_is_not_redacted,
    serde_json::json!({"secret_value":"x"}),
    serde_json::json!({"secret_value":"x"})
);
sanitize_case!(
    coverage_privacy_023_empty_key_is_preserved,
    serde_json::json!({"":"x"}),
    serde_json::json!({"":"x"})
);
sanitize_case!(
    coverage_privacy_024_unicode_public_key_is_preserved,
    serde_json::json!({"segredo":"x"}),
    serde_json::json!({"segredo":"x"})
);
sanitize_case!(
    coverage_privacy_025_array_primitives_are_preserved,
    serde_json::json!([1, true, "x", null]),
    serde_json::json!([1, true, "x", null])
);

#[test]
fn coverage_privacy_026_empty_policy_is_rejected() {
    let result = ProtectedFieldBoundary::new(FieldEncryptor::from_bytes(&[1; 32]), Vec::new());
    assert!(matches!(result, Err(PrivacyError::InvalidPolicy)));
}

#[test]
fn coverage_privacy_027_empty_protected_field_is_rejected() {
    let result = ProtectedFieldBoundary::new(FieldEncryptor::from_bytes(&[1; 32]), [String::new()]);
    assert!(matches!(result, Err(PrivacyError::InvalidPolicy)));
}

#[test]
fn coverage_privacy_028_empty_tenant_cannot_seal() {
    let result = boundary().seal_for_handoff("", "instance", &serde_json::json!({"ssn":"x"}));
    assert!(matches!(result, Err(PrivacyError::InvalidPolicy)));
}

#[test]
fn coverage_privacy_029_empty_instance_cannot_seal() {
    let result = boundary().seal_for_handoff("tenant", "", &serde_json::json!({"ssn":"x"}));
    assert!(matches!(result, Err(PrivacyError::InvalidPolicy)));
}

#[test]
fn coverage_privacy_030_matching_aad_round_trips() {
    let sealed = boundary()
        .seal_for_handoff("tenant", "instance", &serde_json::json!({"ssn":"x"}))
        .unwrap();
    let opened = boundary()
        .open_in_trusted_runtime("tenant", "instance", &sealed)
        .unwrap();
    assert_eq!(opened, serde_json::json!({"ssn":"x"}));
}

#[test]
fn coverage_privacy_031_tenant_mismatch_fails_decryption() {
    let sealed = boundary()
        .seal_for_handoff("tenant-a", "instance", &serde_json::json!({"ssn":"x"}))
        .unwrap();
    let result = boundary().open_in_trusted_runtime("tenant-b", "instance", &sealed);
    assert!(result.is_err());
}

#[test]
fn coverage_privacy_032_instance_mismatch_fails_decryption() {
    let sealed = boundary()
        .seal_for_handoff("tenant", "instance-a", &serde_json::json!({"ssn":"x"}))
        .unwrap();
    let result = boundary().open_in_trusted_runtime("tenant", "instance-b", &sealed);
    assert!(result.is_err());
}

#[test]
fn coverage_privacy_033_empty_raw_leak_probe_is_rejected() {
    let result = boundary().assert_no_raw_value("", &[("log", serde_json::json!({}))]);
    assert!(matches!(result, Err(PrivacyError::InvalidPolicy)));
}

#[test]
fn coverage_privacy_034_raw_value_in_nested_output_is_detected() {
    let result = boundary().assert_no_raw_value(
        "needle",
        &[("trace", serde_json::json!({"nested":{"value":"needle"}}))],
    );
    assert!(matches!(result, Err(PrivacyError::Leak(surface)) if surface == "trace"));
}

#[test]
fn coverage_privacy_035_sanitized_surfaces_pass_leak_assertion() {
    let value = boundary().sanitize(
        DisclosureSurface::Sync,
        &serde_json::json!({"ssn":"needle","public":"ok"}),
    );
    let result = boundary().assert_no_raw_value("needle", &[("sync", value)]);
    assert!(result.is_ok());
}
