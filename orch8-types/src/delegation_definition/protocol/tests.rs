use super::*;
use serde_json::json;

fn reference() -> DefinitionBundleReference {
    DefinitionBundleReference {
        version: 1,
        sequence_sha256: "a".repeat(64),
        bundle_sha256: "b".repeat(64),
        payload_sha256: "c".repeat(64),
        payload_bytes: 128,
        definition_count: 1,
        selector_count: 0,
    }
}
fn usage() -> DelegationResourceUsage {
    DelegationResourceUsage {
        definition_expanded_bytes: 512,
        input_wire_version: 1,
        input_payload_bytes: 64,
        input_expanded_bytes: 256,
    }
}
fn root_only() -> DelegationProtocolCapabilities {
    DelegationProtocolCapabilities::v1(
        DefinitionGraphLimits {
            definitions: 1,
            selectors: 0,
            serialized_bytes: 128,
            expanded_bytes: 512,
        },
        64,
        256,
    )
    .unwrap()
}

#[test]
fn root_only_limits_accept_the_boundary_and_reject_each_excess_dimension() {
    let capabilities = root_only();
    assert!(capabilities.accepts(&reference(), usage()));
    for variant in 0..3 {
        let mut required = reference();
        match variant {
            0 => required.payload_bytes += 1,
            1 => required.definition_count += 1,
            _ => required.selector_count += 1,
        }
        assert!(!capabilities.accepts(&required, usage()));
    }
    for variant in 0..3 {
        let mut required = usage();
        match variant {
            0 => required.definition_expanded_bytes += 1,
            1 => required.input_payload_bytes += 1,
            _ => required.input_expanded_bytes += 1,
        }
        assert!(!capabilities.accepts(&reference(), required));
    }
}

#[test]
fn unknown_or_missing_usage_and_incompatible_codecs_never_admit() {
    for variant in 0..5 {
        let mut required = usage();
        match variant {
            0 => required.definition_expanded_bytes = 0,
            1 => required.input_payload_bytes = 0,
            2 => required.input_expanded_bytes = 0,
            3 => required.input_wire_version = 0,
            _ => required.input_wire_version = 2,
        }
        assert!(!root_only().accepts(&reference(), required));
    }
    for variant in 0..4 {
        let mut required = reference();
        match variant {
            0 => required.version = 2,
            1 => required.payload_bytes = 0,
            2 => required.definition_count = 0,
            _ => required.payload_sha256 = "not-a-hash".into(),
        }
        assert!(!root_only().accepts(&required, usage()));
    }
}

#[test]
fn invalid_capability_versions_or_budgets_cannot_widen_admission() {
    let valid = DelegationProtocolCapabilities::v1(
        DefinitionGraphLimits::default(),
        MAX_INPUT_PAYLOAD_BYTES,
        MAX_INPUT_EXPANDED_BYTES,
    )
    .unwrap();
    for variant in 0..15 {
        let mut capability = valid;
        match variant {
            0 => capability.version = 0,
            1 => capability.version = 2,
            2 => capability.definition_wire_version = 2,
            3 => capability.input_wire_version = 2,
            4 => capability.max_definitions = 0,
            5 => capability.max_definitions = 257,
            6 => capability.max_selectors = 1025,
            7 => capability.max_definition_payload_bytes = 0,
            8 => capability.max_definition_payload_bytes += 1,
            9 => capability.max_definition_expanded_bytes = 0,
            10 => capability.max_definition_expanded_bytes += 1,
            11 => capability.max_input_payload_bytes = 0,
            12 => capability.max_input_payload_bytes += 1,
            13 => capability.max_input_expanded_bytes = 0,
            _ => capability.max_input_expanded_bytes += 1,
        }
        assert!(capability.validate().is_err());
        assert!(!capability.accepts(&reference(), usage()));
    }
}

#[test]
fn descriptor_wire_shape_rejects_partial_unknown_negative_fractional_and_oversized_fields() {
    let valid = serde_json::to_value(root_only()).unwrap();
    let mut unknown = valid.clone();
    unknown["future_field"] = json!(1);
    assert!(serde_json::from_value::<DelegationProtocolCapabilities>(unknown).is_err());
    for field in valid.as_object().unwrap().keys() {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<DelegationProtocolCapabilities>(missing).is_err());
        for invalid in [json!(-1), json!(1.5), json!(u64::MAX), json!("128")] {
            let mut changed = valid.clone();
            changed[field] = invalid;
            assert!(serde_json::from_value::<DelegationProtocolCapabilities>(changed).is_err());
        }
    }
}

#[test]
fn limits_roundtrip_preserves_zero_selectors_and_independent_input_budgets() {
    let capability = root_only();
    let decoded: DelegationProtocolCapabilities =
        serde_json::from_value(serde_json::to_value(capability).unwrap()).unwrap();
    assert_eq!(decoded, capability);
    let limits = decoded.definition_limits().unwrap();
    assert_eq!(
        (
            limits.definitions,
            limits.selectors,
            limits.serialized_bytes,
            limits.expanded_bytes
        ),
        (1, 0, 128, 512)
    );
    let mut input = usage();
    input.input_payload_bytes = 65;
    assert!(!decoded.accepts(&reference(), input));
}
