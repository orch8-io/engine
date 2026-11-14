use super::*;
use crate::{delegation_definition::wire::DefinitionReadScope, ids::TenantId};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use uuid::Uuid;

const REQUEST: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
fn scope() -> DefinitionReadScope {
    DefinitionReadScope {
        tenant_id: TenantId::unchecked("tenant"),
        task_id: Uuid::from_u128(1),
        worker_id: "worker".into(),
        claim_epoch: u64::MAX,
    }
}
fn envelope(value: &serde_json::Value) -> (EncodedDelegationInput, DelegationInputEnvelope) {
    let encoded = EncodedDelegationInput::from_value(value, InputLimits::default()).unwrap();
    let envelope = DelegationInputEnvelope::new(scope(), REQUEST.into(), &encoded).unwrap();
    (encoded, envelope)
}
fn raw(payload: &[u8], expanded_bytes: u32) -> (DelegationInputReference, DelegationInputEnvelope) {
    let input = EncodedDelegationInput {
        reference: DelegationInputReference {
            version: 1,
            payload_sha256: bytes_sha256(payload),
            payload_bytes: u32::try_from(payload.len()).unwrap(),
            expanded_bytes,
        },
        payload: payload.to_vec(),
    };
    (
        input.reference.clone(),
        DelegationInputEnvelope::new(scope(), REQUEST.into(), &input).unwrap(),
    )
}

#[test]
fn exact_input_preserves_unicode_nul_numeric_edges_and_full_width_scope() {
    let value = json!({"private":"INPUT_PRIVATE_CANARY 😺\0","large":u64::MAX,"above_js_safe":9_007_199_254_740_993_u64,"negative_zero":-0.0,"tiny":f64::from_bits(1)});
    let (encoded, envelope) = envelope(&value);
    let wire = serde_json::to_value(&envelope).unwrap();
    assert_eq!(wire["scope"]["claim_epoch"], json!(u64::MAX.to_string()));
    let verified = envelope
        .verify(
            &scope(),
            REQUEST,
            encoded.reference(),
            InputLimits::default(),
        )
        .unwrap();
    assert_eq!(verified.value(), &value);
    assert_eq!(verified.value()["large"].as_u64(), Some(u64::MAX));
    assert!(
        verified.value()["negative_zero"]
            .as_f64()
            .unwrap()
            .is_sign_negative()
    );
    assert_eq!(verified.scope(), &scope());
    assert_eq!(verified.request_sha256(), REQUEST);
    assert_eq!(verified.reference(), encoded.reference());
    for debug in [
        format!("{encoded:?}"),
        format!("{envelope:?}"),
        format!("{verified:?}"),
    ] {
        assert!(!debug.contains("INPUT_PRIVATE_CANARY"));
        assert!(!debug.contains(&envelope.payload_base64));
    }
}

#[test]
fn changed_scope_request_or_reference_is_not_authenticated_by_a_body_digest() {
    let (input, wire) = envelope(&json!({"work":"private"}));
    for variant in 0..8 {
        let mut changed = wire.clone();
        match variant {
            0 => changed.scope.tenant_id = TenantId::unchecked("other"),
            1 => changed.scope.task_id = Uuid::new_v4(),
            2 => changed.scope.worker_id = "other".into(),
            3 => changed.scope.claim_epoch -= 1,
            4 => changed.request_sha256 = "a".repeat(64),
            5 => changed.version = 2,
            6 => changed.reference.payload_sha256 = "0".repeat(64),
            _ => changed.payload_base64 = STANDARD.encode(b"null"),
        }
        assert!(
            changed
                .verify(&scope(), REQUEST, input.reference(), InputLimits::default())
                .is_err()
        );
    }
    let mut changed = wire;
    changed.payload_base64 = STANDARD.encode(b"null");
    changed.reference.payload_sha256 = bytes_sha256(b"null");
    changed.reference.payload_bytes = 4;
    changed.reference.expanded_bytes = 64;
    assert!(
        changed
            .verify(&scope(), REQUEST, input.reference(), InputLimits::default())
            .is_err(),
        "an untrusted response cannot choose its own expected reference"
    );
}

#[test]
fn input_byte_and_expansion_bounds_are_independent_exact_and_utf8_based() {
    let value = json!({"ä":"😺\0"});
    let (input, wire) = envelope(&value);
    assert_eq!(input.reference().expanded_bytes, 199);
    let exact = InputLimits {
        serialized_bytes: input.payload.len(),
        expanded_bytes: 199,
    };
    assert!(EncodedDelegationInput::from_value(&value, exact).is_ok());
    assert!(
        wire.verify(&scope(), REQUEST, input.reference(), exact)
            .is_ok()
    );
    for limit in [
        InputLimits {
            serialized_bytes: exact.serialized_bytes - 1,
            ..exact
        },
        InputLimits {
            expanded_bytes: 198,
            ..exact
        },
    ] {
        assert!(EncodedDelegationInput::from_value(&value, limit).is_err());
        assert!(
            wire.verify(&scope(), REQUEST, input.reference(), limit)
                .is_err()
        );
    }
    for expanded in [198, 200] {
        let (reference, wire) = raw(input.payload(), expanded);
        assert!(
            wire.verify(&scope(), REQUEST, &reference, InputLimits::default())
                .is_err(),
            "the claimed expanded size must match the decoded value"
        );
    }
}

#[test]
fn dense_small_json_and_escape_expansion_cannot_exceed_their_budgets() {
    let value = json!(vec![0; 1024]);
    let limits = InputLimits {
        serialized_bytes: 4096,
        expanded_bytes: 4096,
    };
    assert!(EncodedDelegationInput::from_value(&value, limits).is_err());
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(bytes.len() < limits.serialized_bytes);
    let (reference, wire) = raw(&bytes, 4096);
    assert!(wire.verify(&scope(), REQUEST, &reference, limits).is_err());
    let escaped = json!("\0".repeat(1000));
    assert!(
        EncodedDelegationInput::from_value(
            &escaped,
            InputLimits {
                serialized_bytes: 1024,
                expanded_bytes: 2048
            }
        )
        .is_err()
    );
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit: 10,
    };
    writer.write_all(b"12345678").unwrap();
    assert!(writer.write_all(b"9012").is_err());
    assert_eq!(writer.bytes, b"12345678");
    assert!(writer.bytes.capacity() <= 10);
}

#[test]
fn malformed_hashed_bodies_trailing_values_and_bad_base64_fail_without_private_diagnostics() {
    for body in [
        b"{\"private\":\"INPUT_PRIVATE_CANARY\",\"bad\":".as_slice(),
        b"null true",
        &[0xff],
    ] {
        let (reference, wire) = raw(body, 1024);
        let error = wire
            .verify(&scope(), REQUEST, &reference, InputLimits::default())
            .unwrap_err();
        assert!(!format!("{error:?} {error}").contains("INPUT_PRIVATE_CANARY"));
    }
    let (input, wire) = envelope(&json!(null));
    for payload in [
        "!".repeat(wire.payload_base64.len()),
        String::new(),
        format!("{}=", wire.payload_base64),
    ] {
        let mut changed = wire.clone();
        changed.payload_base64 = payload;
        assert!(
            changed
                .verify(&scope(), REQUEST, input.reference(), InputLimits::default())
                .is_err()
        );
    }
    let mut changed = wire;
    changed.reference.payload_sha256 = "0".repeat(64);
    assert!(
        changed
            .verify(
                &scope(),
                REQUEST,
                &changed.reference,
                InputLimits::default()
            )
            .is_err()
    );
}

#[test]
fn unsupported_versions_invalid_hashes_and_oversized_references_fail_before_decode() {
    let (input, wire) = envelope(&json!(null));
    for variant in 0..6 {
        let mut reference = input.reference().clone();
        match variant {
            0 => reference.version = 2,
            1 => reference.payload_bytes = 0,
            2 => reference.payload_bytes = MAX_INPUT_PAYLOAD_BYTES + 1,
            3 => reference.expanded_bytes = 0,
            4 => reference.expanded_bytes = MAX_INPUT_EXPANDED_BYTES + 1,
            _ => reference.payload_sha256 = "A".repeat(64),
        }
        assert!(
            wire.verify(&scope(), REQUEST, &reference, InputLimits::default())
                .is_err()
        );
    }
    for limit in [
        InputLimits {
            serialized_bytes: 0,
            ..InputLimits::default()
        },
        InputLimits {
            expanded_bytes: 0,
            ..InputLimits::default()
        },
        InputLimits {
            serialized_bytes: 4 * 1024 * 1024 + 1,
            ..InputLimits::default()
        },
        InputLimits {
            expanded_bytes: 16 * 1024 * 1024 + 1,
            ..InputLimits::default()
        },
    ] {
        assert!(EncodedDelegationInput::from_value(&json!(null), limit).is_err());
    }
    assert!(DelegationInputEnvelope::new(scope(), "private".into(), &input).is_err());
    assert!(
        wire.verify(
            &scope(),
            "private",
            input.reference(),
            InputLimits::default()
        )
        .is_err()
    );
}

#[test]
fn constructed_and_decoded_inputs_share_the_recursion_boundary() {
    let mut value = json!(0);
    for _ in 0..127 {
        value = serde_json::Value::Array(vec![value]);
    }
    let (input, wire) = envelope(&value);
    assert!(
        wire.verify(&scope(), REQUEST, input.reference(), InputLimits::default())
            .is_ok()
    );
    value = serde_json::Value::Array(vec![value]);
    assert!(EncodedDelegationInput::from_value(&value, InputLimits::default()).is_err());
}

#[test]
fn wrapper_and_reference_reject_unknown_fields_and_non_text_claim_epochs() {
    let (input, wire) = envelope(&json!(null));
    let mut unknown = serde_json::to_value(&wire).unwrap();
    unknown["future_field"] = json!(true);
    assert!(serde_json::from_value::<DelegationInputEnvelope>(unknown).is_err());
    let mut unknown = serde_json::to_value(input.reference()).unwrap();
    unknown["future_field"] = json!(true);
    assert!(serde_json::from_value::<DelegationInputReference>(unknown).is_err());
    for epoch in [json!(1), json!("01"), json!("18446744073709551616")] {
        let mut changed = serde_json::to_value(&wire).unwrap();
        changed["scope"]["claim_epoch"] = epoch;
        assert!(serde_json::from_value::<DelegationInputEnvelope>(changed).is_err());
    }
    let mut zero = scope();
    zero.claim_epoch = 0;
    assert!(DelegationInputEnvelope::new(zero.clone(), REQUEST.into(), &input).is_err());
    assert!(
        wire.verify(&zero, REQUEST, input.reference(), InputLimits::default())
            .is_err()
    );
}

#[test]
fn captured_input_vector_preserves_independently_hashed_bytes_and_numeric_values() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../contracts/vectors/delegation-input-v1.json"
    ))
    .unwrap();
    let reference: DelegationInputReference =
        serde_json::from_value(fixture["reference"].clone()).unwrap();
    let envelope: DelegationInputEnvelope =
        serde_json::from_value(fixture["envelope"].clone()).unwrap();
    let payload = STANDARD.decode(&envelope.payload_base64).unwrap();
    assert_eq!(bytes_sha256(&payload), reference.payload_sha256);
    let verified = envelope
        .verify(&scope(), REQUEST, &reference, InputLimits::default())
        .unwrap();
    assert_eq!(verified.value()["large"].as_u64(), Some(u64::MAX));
    assert_eq!(
        verified.value()["above_js_safe"].as_u64(),
        Some(9_007_199_254_740_993)
    );
    assert!(
        verified.value()["negative_zero"]
            .as_f64()
            .unwrap()
            .is_sign_negative()
    );
    assert_eq!(
        verified.value()["private"],
        json!("INPUT_PRIVATE_CANARY 😺\0")
    );
}

#[test]
fn allocation_free_preflight_matches_encoder_boundaries() {
    for value in [
        json!(null),
        json!("\0".repeat(80)),
        json!({"λ":"😺","values":[u64::MAX,-0.0]}),
        json!([]),
    ] {
        let encoded = EncodedDelegationInput::from_value(&value, InputLimits::default()).unwrap();
        for bytes in [
            encoded.payload.len() - 1,
            encoded.payload.len(),
            encoded.payload.len() + 1,
        ] {
            for expanded in [
                encoded.reference.expanded_bytes - 1,
                encoded.reference.expanded_bytes,
                encoded.reference.expanded_bytes + 1,
            ] {
                let limits = InputLimits {
                    serialized_bytes: bytes,
                    expanded_bytes: usize::try_from(expanded).unwrap(),
                };
                assert_eq!(
                    EncodedDelegationInput::validate_value(&value, limits).is_ok(),
                    EncodedDelegationInput::from_value(&value, limits).is_ok()
                );
            }
        }
    }
}

#[test]
fn retained_input_uses_the_same_bounded_exact_byte_decoder() {
    let (encoded, _) = envelope(&json!({"private":"RETAINED_INPUT_CANARY\0😺","wide":u64::MAX}));
    let retained = EncodedDelegationInput::from_retained_base64(
        encoded.reference().clone(),
        &encoded.payload_base64(),
        InputLimits::default(),
    )
    .unwrap();
    assert_eq!(retained.payload(), encoded.payload());
    for bad in [
        String::new(),
        "!".repeat(encoded.payload_base64().len()),
        format!("{}AAAA", encoded.payload_base64()),
    ] {
        let error = EncodedDelegationInput::from_retained_base64(
            encoded.reference().clone(),
            &bad,
            InputLimits::default(),
        )
        .unwrap_err();
        assert!(!format!("{error:?}").contains("RETAINED_INPUT_CANARY"));
    }
    let mut wrong = encoded.reference().clone();
    wrong.expanded_bytes += 1;
    assert!(
        EncodedDelegationInput::from_retained_base64(
            wrong,
            &encoded.payload_base64(),
            InputLimits::default()
        )
        .is_err()
    );
    let mut wrong = encoded.reference().clone();
    wrong.payload_sha256 = "a".repeat(64);
    assert!(
        EncodedDelegationInput::from_retained_base64(
            wrong,
            &encoded.payload_base64(),
            InputLimits::default()
        )
        .is_err()
    );
}
