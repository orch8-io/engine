use super::*;
use serde_json::{Value, json};

fn vector() -> Value {
    serde_json::from_str(include_str!(
        "../../../../contracts/vectors/delegation-definition-v1.json"
    ))
    .unwrap()
}
fn fixture() -> (
    DefinitionWireEnvelope,
    DefinitionReadScope,
    DefinitionBundleReference,
) {
    let wire: DefinitionWireEnvelope =
        serde_json::from_value(vector()["envelope"].clone()).unwrap();
    let scope = wire.scope.clone();
    let reference = wire.reference.clone();
    (wire, scope, reference)
}
fn cost(value: &Value) -> u32 {
    64 + match value {
        Value::String(s) => u32::try_from(s.len()).unwrap(),
        Value::Array(v) => v.iter().map(cost).sum(),
        Value::Object(v) => v
            .iter()
            .map(|(k, v)| 64 + u32::try_from(k.len()).unwrap() + cost(v))
            .sum(),
        _ => 0,
    }
}

#[test]
fn exact_wire_vector_preserves_legacy_hash_numbers_unicode_and_full_epoch() {
    let (wire, scope, reference) = fixture();
    assert_eq!(scope.claim_epoch, u64::MAX);
    let verified = wire
        .verify(&scope, &reference, DefinitionGraphLimits::default())
        .unwrap();
    let definition = &verified.root().definition;
    assert_eq!(
        super::super::sequence_sha256(definition).unwrap(),
        vector()["legacy_semantic_sha256"].as_str().unwrap()
    );
    let crate::sequence::BlockDefinition::Step(step) = &definition.blocks[0] else {
        panic!("step")
    };
    assert_eq!(step.params["max_u64"].as_u64(), Some(u64::MAX));
    assert_eq!(step.params["over53"].as_u64(), Some(9_007_199_254_740_993));
    assert!(step.params["float"].is_f64());
    assert!(
        step.params["negative_zero"]
            .as_f64()
            .unwrap()
            .is_sign_negative()
    );
    assert_eq!(step.params["subnormal"].as_f64().unwrap().to_bits(), 1);
    assert_eq!(step.params["nul"], "a\0b");
    assert_eq!(step.params["\u{e000}"], "BMP");
    assert_eq!(step.params["\u{10000}"], "non-BMP");
    assert!(!format!("{wire:?}").contains(&wire.payload_base64));
    assert!(!format!("{verified:?}").contains("max_u64"));
    assert!(
        wire.verify(
            &scope,
            &reference,
            DefinitionGraphLimits {
                definitions: 1,
                selectors: 0,
                ..DefinitionGraphLimits::default()
            }
        )
        .is_ok()
    );
}

#[test]
fn retained_document_identity_does_not_rehash_newly_defaulted_struct_fields() {
    let fixture = vector();
    let mut body: Value =
        serde_json::from_str(fixture["retained_definition_json"].as_str().unwrap()).unwrap();
    body["blocks"][0]
        .as_object_mut()
        .unwrap()
        .remove("cancellable");
    let mut semantic = body.clone();
    semantic.as_object_mut().unwrap().remove("created_at");
    let hash = super::super::semantic_json_sha256(&semantic);
    let manifest = DefinitionManifest {
        version: 1,
        root_sha256: hash.clone(),
        bindings: vec![],
    };
    let expected_expanded = cost(&body);
    let record = serde_json::to_string(&body).unwrap();
    let retained = super::super::decode_retained_definition(&record).unwrap();
    assert_eq!(retained.sequence_sha256, hash);
    let (wire, verified) = EncodedDefinitionBundle::from_retained(
        &serde_json::to_string(&manifest).unwrap(),
        vec![RetainedDefinitionDocument {
            sequence_sha256: hash.clone(),
            definition_json: record,
        }],
        DefinitionGraphLimits::default(),
    )
    .unwrap();
    let crate::sequence::BlockDefinition::Step(step) = &verified.root().definition.blocks[0] else {
        panic!("step")
    };
    assert_eq!(wire.expanded_bytes(), expected_expanded);
    assert_eq!(verified.expanded_bytes(), expected_expanded);
    assert!(
        cost(&serde_json::to_value(&verified.root().definition).unwrap()) > expected_expanded,
        "defaulted typed fields must not change the retained resource charge"
    );
    assert!(
        step.cancellable,
        "older document still receives the compatible execution default"
    );
    assert_ne!(
        super::super::sequence_sha256(&verified.root().definition).unwrap(),
        hash,
        "this fixture must distinguish retained shape from current struct reserialization"
    );
    let scope: DefinitionReadScope =
        serde_json::from_value(fixture["envelope"]["scope"].clone()).unwrap();
    assert!(
        DefinitionWireEnvelope::new(scope.clone(), &wire)
            .unwrap()
            .verify(&scope, wire.reference(), DefinitionGraphLimits::default())
            .is_ok()
    );
}

#[test]
fn retained_codec_matches_independently_captured_golden_bytes_at_exact_limit() {
    let fixture = vector();
    let expected: DefinitionWireEnvelope =
        serde_json::from_value(fixture["envelope"].clone()).unwrap();
    let limit = DefinitionGraphLimits {
        serialized_bytes: expected.reference.payload_bytes as usize,
        ..DefinitionGraphLimits::default()
    };
    let record = || RetainedDefinitionDocument {
        sequence_sha256: fixture["legacy_semantic_sha256"].as_str().unwrap().into(),
        definition_json: fixture["retained_definition_json"].as_str().unwrap().into(),
    };
    let (encoded, _) = EncodedDefinitionBundle::from_retained(
        fixture["retained_manifest_json"].as_str().unwrap(),
        vec![record()],
        limit,
    )
    .unwrap();
    assert_eq!(encoded.reference(), &expected.reference);
    assert_eq!(
        DefinitionWireEnvelope::new(expected.scope.clone(), &encoded)
            .unwrap()
            .payload_base64,
        expected.payload_base64
    );
    assert!(
        EncodedDefinitionBundle::from_retained(
            fixture["retained_manifest_json"].as_str().unwrap(),
            vec![record()],
            DefinitionGraphLimits {
                serialized_bytes: limit.serialized_bytes - 1,
                ..limit
            }
        )
        .is_err()
    );
}

#[test]
fn wire_verification_requires_independent_scope_and_reference_and_exact_bytes() {
    let (wire, scope, reference) = fixture();
    for mutation in 0..10 {
        let mut damaged = wire.clone();
        match mutation {
            0 => damaged.scope.tenant_id = TenantId::unchecked("another-account"),
            1 => damaged.scope.task_id = Uuid::from_u128(42),
            2 => damaged.scope.worker_id = "another-worker".into(),
            3 => damaged.scope.claim_epoch -= 1,
            4 => damaged.reference.bundle_sha256 = "0".repeat(64),
            5 => damaged.reference.payload_sha256 = "0".repeat(64),
            6 => damaged.reference.definition_count += 1,
            7 => damaged.version = 2,
            8 => damaged.payload_base64.push('='),
            _ => {
                let mut payload = STANDARD.decode(&damaged.payload_base64).unwrap();
                payload[20] ^= 1;
                damaged.payload_base64 = STANDARD.encode(payload);
            }
        }
        assert!(
            damaged
                .verify(&scope, &reference, DefinitionGraphLimits::default())
                .is_err(),
            "mutation {mutation}"
        );
    }
    let mut wrong = scope.clone();
    wrong.tenant_id = TenantId::unchecked("another-account");
    let mut forged = wire.clone();
    forged.scope = wrong.clone();
    assert!(
        forged
            .verify(&wrong, &reference, DefinitionGraphLimits::default())
            .is_err(),
        "even matching outer scope cannot override the root tenant"
    );
}

#[test]
fn byte_integrity_does_not_certify_an_incomplete_or_rewritten_closure() {
    let (wire, scope, reference) = fixture();
    for mutation in 0..4 {
        let mut raw: Value =
            serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
        match mutation {
            0 => raw["definitions"][0]["definition"]["name"] = json!("rewritten"),
            1 => {
                raw["manifest"]["bindings"] = json!([{"selector":{"name":"missing","version":null},"sequence_sha256":"0".repeat(64)}]);
            }
            2 => raw["definitions"][0]["version"] = json!(2),
            _ => raw["definitions"].as_array_mut().unwrap().clear(),
        }
        let payload = serde_json::to_vec(&raw).unwrap();
        let mut wrong = wire.clone();
        let mut expectation = reference.clone();
        expectation.payload_sha256 = bytes_sha256(&payload);
        expectation.payload_bytes = u32::try_from(payload.len()).unwrap();
        wrong.reference = expectation.clone();
        wrong.payload_base64 = STANDARD.encode(payload);
        assert!(
            wrong
                .verify(&scope, &expectation, DefinitionGraphLimits::default())
                .is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn epoch_text_and_transport_resource_limits_fail_closed() {
    let (wire, scope, reference) = fixture();
    for invalid_epoch in [
        json!(1),
        json!("01"),
        json!("+1"),
        json!("-1"),
        json!(" 1"),
        json!("18446744073709551616"),
    ] {
        let mut value = serde_json::to_value(&scope).unwrap();
        value["claim_epoch"] = invalid_epoch;
        assert!(serde_json::from_value::<DefinitionReadScope>(value).is_err());
    }
    for limit in [
        DefinitionGraphLimits {
            expanded_bytes: 0,
            ..DefinitionGraphLimits::default()
        },
        DefinitionGraphLimits {
            expanded_bytes: super::super::MAX_DEFINITION_EXPANDED_BYTES + 1,
            ..DefinitionGraphLimits::default()
        },
        DefinitionGraphLimits {
            serialized_bytes: reference.payload_bytes as usize - 1,
            ..DefinitionGraphLimits::default()
        },
        DefinitionGraphLimits {
            definitions: 0,
            ..DefinitionGraphLimits::default()
        },
    ] {
        assert!(wire.verify(&scope, &reference, limit).is_err());
    }
    let mut bad = reference.clone();
    bad.payload_bytes = u32::MAX;
    assert!(
        wire.verify(&scope, &bad, DefinitionGraphLimits::default())
            .is_err()
    );
    let mut record = RetainedDefinitionDocument {
        sequence_sha256: reference.sequence_sha256.clone(),
        definition_json: vector()["retained_definition_json"]
            .as_str()
            .unwrap()
            .into(),
    };
    record.definition_json.push_str(" null");
    assert!(
        EncodedDefinitionBundle::from_retained(
            vector()["retained_manifest_json"].as_str().unwrap(),
            vec![record],
            DefinitionGraphLimits::default()
        )
        .is_err()
    );
}

fn published_invalid_payload(
    payload: &[u8],
) -> (
    DefinitionWireEnvelope,
    DefinitionReadScope,
    DefinitionBundleReference,
) {
    let (mut response, scope, mut expected) = fixture();
    expected.payload_sha256 = bytes_sha256(payload);
    expected.payload_bytes = u32::try_from(payload.len()).unwrap();
    response.reference = expected.clone();
    response.payload_base64 = STANDARD.encode(payload);
    (response, scope, expected)
}

// Test internal parse ordering separately from public, redacted diagnostics.
fn parser_failure(payload: &[u8], limits: DefinitionGraphLimits) -> StorageError {
    let (response, scope, expected) = published_invalid_payload(payload);
    assert!(response.verify(&scope, &expected, limits).is_err());
    decode_payload(payload, &expected.bundle_sha256, limits).unwrap_err()
}

#[test]
fn collection_limits_stop_before_deserializing_an_excess_element() {
    let (wire, _, _) = fixture();
    for limits in [
        DefinitionGraphLimits::default(),
        DefinitionGraphLimits {
            definitions: 1,
            selectors: 1,
            ..DefinitionGraphLimits::default()
        },
        DefinitionGraphLimits {
            definitions: 1,
            selectors: 0,
            ..DefinitionGraphLimits::default()
        },
    ] {
        for definitions in [true, false] {
            let mut raw: Value =
                serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
            if definitions {
                let mut docs = vec![raw["definitions"][0].clone(); limits.definitions];
                docs.push(json!("this must not be deserialized as a document"));
                raw["definitions"] = json!(docs);
            } else {
                let binding = json!({"selector":{"name":"child","version":null},"sequence_sha256":"0".repeat(64)});
                let mut bindings = vec![binding; limits.selectors];
                bindings.push(json!("this must not be deserialized as a binding"));
                raw["manifest"]["bindings"] = json!(bindings);
            }
            let payload = serde_json::to_vec(&raw).unwrap();
            let error = parser_failure(&payload, limits);
            assert!(
                error
                    .to_string()
                    .contains("collection exceeds supported limit"),
                "{error}"
            );
        }
    }
}

#[test]
fn streaming_decode_preserves_strict_object_fields_and_complete_json_input() {
    let (wire, scope, expected) = fixture();
    let payload = STANDARD.decode(&wire.payload_base64).unwrap();
    let raw: Value = serde_json::from_slice(&payload).unwrap();
    let manifest = &raw["manifest"];
    let definitions = &raw["definitions"];
    for (payload, diagnostic) in [
        (
            format!(
                r#"{{"manifest":{manifest},"manifest":{manifest},"definitions":{definitions}}}"#
            ),
            "duplicate field",
        ),
        (
            format!(
                r#"{{"manifest":{manifest},"definitions":{definitions},"definitions":{definitions}}}"#
            ),
            "duplicate field",
        ),
        (
            format!(r#"{{"manifest":{manifest},"definitions":{definitions},"unknown":true}}"#),
            "unknown field",
        ),
        (format!(r#"{{"manifest":{manifest}}}"#), "missing field"),
        (
            format!(r#"{{"definitions":{definitions}}}"#),
            "missing field",
        ),
        (
            format!(
                r#"{{"manifest":{{"version":1,"root_sha256":"{}","bindings":[],"bindings":[]}},"definitions":{definitions}}}"#,
                manifest["root_sha256"].as_str().unwrap()
            ),
            "duplicate field",
        ),
        (
            format!(
                r#"{{"manifest":{{"version":1,"root_sha256":"{}","unknown":true,"bindings":[]}},"definitions":{definitions}}}"#,
                manifest["root_sha256"].as_str().unwrap()
            ),
            "unknown field",
        ),
        (
            format!(
                r#"{{"manifest":{{"version":1,"root_sha256":"{}"}},"definitions":{definitions}}}"#,
                manifest["root_sha256"].as_str().unwrap()
            ),
            "missing field",
        ),
        (format!("[{manifest},{definitions}]"), "invalid type"),
        (
            format!("{} null", std::str::from_utf8(&payload).unwrap()),
            "trailing characters",
        ),
    ] {
        let error = parser_failure(payload.as_bytes(), DefinitionGraphLimits::default());
        assert!(
            error.to_string().contains(diagnostic),
            "{diagnostic}: {error}"
        );
    }
    // Zero selectors and a single definition are valid exact collection boundaries.
    wire.verify(
        &scope,
        &expected,
        DefinitionGraphLimits {
            definitions: 1,
            selectors: 0,
            ..DefinitionGraphLimits::default()
        },
    )
    .unwrap();
}

#[test]
fn standalone_models_reject_excess_collections_before_decoding_their_elements() {
    let (wire, _, _) = fixture();
    let mut raw: Value =
        serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
    let mut definitions =
        vec![raw["definitions"][0].clone(); DefinitionGraphLimits::default().definitions];
    definitions.push(json!("excess document"));
    raw["definitions"] = json!(definitions);
    let error = serde_json::from_str::<FrozenDefinitionGraph>(&raw.to_string()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("collection exceeds supported limit"),
        "{error}"
    );
    let binding =
        json!({"selector":{"name":"child","version":null},"sequence_sha256":"0".repeat(64)});
    let mut bindings = vec![binding; DefinitionGraphLimits::default().selectors];
    bindings.push(json!("excess binding"));
    raw["manifest"]["bindings"] = json!(bindings);
    let error =
        serde_json::from_str::<DefinitionManifest>(&raw["manifest"].to_string()).unwrap_err();
    assert!(DefinitionManifest::decode(&raw["manifest"].to_string()).is_err());
    assert!(
        error
            .to_string()
            .contains("collection exceeds supported limit"),
        "{error}"
    );
}

#[test]
fn compact_json_cannot_expand_past_the_decoded_definition_budget() {
    let (wire, _, _) = fixture();
    for (entries, bytes, expanded) in [
        (
            300_000,
            super::super::MAX_DEFINITION_BYTES,
            super::super::MAX_DEFINITION_EXPANDED_BYTES,
        ),
        (30_000, 128 * 1024, 512 * 1024),
    ] {
        let mut raw: Value =
            serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
        raw["definitions"][0]["definition"]["blocks"][0]["params"]["dense"] =
            json!(vec![0; entries]);
        let payload = serde_json::to_vec(&raw).unwrap();
        assert!(
            payload.len() < bytes,
            "fixture must pass the byte-size fence"
        );
        let error = parser_failure(
            &payload,
            DefinitionGraphLimits {
                serialized_bytes: bytes,
                expanded_bytes: expanded,
                ..DefinitionGraphLimits::default()
            },
        );
        assert!(
            error
                .to_string()
                .contains("definition exceeds decoded resource budget"),
            "{error}"
        );
    }
}

fn linked_dense_documents(entries: usize) -> (String, Vec<RetainedDefinitionDocument>) {
    let (wire, _, _) = fixture();
    let raw: Value =
        serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
    let mut root = raw["definitions"][0]["definition"].clone();
    root["blocks"][0]["params"]["dense"] = json!(vec![0; entries]);
    let mut child = root.clone();
    child["id"] = json!(Uuid::from_u128(12345));
    child["name"] = json!("child");
    child["version"] = json!(1);
    root["blocks"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"sub_sequence","id":"call-child","sequence_name":"child","version":1}));
    let root_text = root.to_string();
    let child_text = child.to_string();
    let root = super::super::decode_retained_definition(&root_text).unwrap();
    let child = super::super::decode_retained_definition(&child_text).unwrap();
    let manifest = DefinitionManifest {
        version: 1,
        root_sha256: root.sequence_sha256.clone(),
        bindings: vec![super::super::DefinitionBinding {
            selector: super::super::DefinitionSelector {
                name: "child".into(),
                version: Some(1),
            },
            sequence_sha256: child.sequence_sha256.clone(),
        }],
    };
    (
        serde_json::to_string(&manifest).unwrap(),
        vec![
            RetainedDefinitionDocument {
                sequence_sha256: root.sequence_sha256,
                definition_json: root_text,
            },
            RetainedDefinitionDocument {
                sequence_sha256: child.sequence_sha256,
                definition_json: child_text,
            },
        ],
    )
}

#[test]
fn decoded_budget_is_shared_across_distinct_reachable_documents() {
    let (manifest, documents) = linked_dense_documents(0);
    EncodedDefinitionBundle::from_retained(&manifest, documents, DefinitionGraphLimits::default())
        .unwrap();
    // Each body fits on its own, but their aggregate decoded values do not.
    let (manifest, documents) = linked_dense_documents(150_000);
    EncodedDefinitionBundle::from_retained(&manifest, documents, DefinitionGraphLimits::default())
        .unwrap_err();
}

#[test]
fn accepted_dense_documents_preserve_independent_exact_byte_limits() {
    let (wire, scope, _) = fixture();
    let raw: Value =
        serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
    let mut body = raw["definitions"][0]["definition"].clone();
    body["blocks"][0]["params"]["dense"] = json!(vec![0; 150_000]);
    let text = body.to_string();
    let definition = super::super::decode_retained_definition(&text).unwrap();
    let manifest = DefinitionManifest {
        version: 1,
        root_sha256: definition.sequence_sha256.clone(),
        bindings: vec![],
    };
    let (encoded, _) = EncodedDefinitionBundle::from_retained(
        &serde_json::to_string(&manifest).unwrap(),
        vec![RetainedDefinitionDocument {
            sequence_sha256: definition.sequence_sha256,
            definition_json: text,
        }],
        DefinitionGraphLimits::default(),
    )
    .unwrap();
    DefinitionWireEnvelope::new(scope.clone(), &encoded)
        .unwrap()
        .verify(
            &scope,
            encoded.reference(),
            DefinitionGraphLimits {
                serialized_bytes: encoded.payload().len(),
                ..DefinitionGraphLimits::default()
            },
        )
        .unwrap();
}

#[test]
fn typed_graph_verification_observes_local_decoded_limits() {
    let (wire, scope, reference) = fixture();
    let verified = wire
        .verify(&scope, &reference, DefinitionGraphLimits::default())
        .unwrap();
    let document = FrozenDelegationDefinition::new(verified.root().definition.clone()).unwrap();
    let graph = FrozenDefinitionGraph {
        manifest: DefinitionManifest {
            version: 1,
            root_sha256: document.sequence_sha256.clone(),
            bindings: vec![],
        },
        definitions: vec![document],
    };
    let expected = graph.sha256().unwrap();
    graph
        .verify(&expected, DefinitionGraphLimits::default())
        .unwrap();
    let error = graph
        .verify(
            &expected,
            DefinitionGraphLimits {
                expanded_bytes: 1,
                ..DefinitionGraphLimits::default()
            },
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("definition exceeds decoded resource budget"),
        "{error}"
    );
}

#[test]
fn retained_document_parser_has_the_same_hard_decoded_budget() {
    let (wire, _, _) = fixture();
    let mut raw: Value =
        serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
    raw["definitions"][0]["definition"]["blocks"][0]["params"]["dense"] = json!(vec![0; 300_000]);
    let body = serde_json::to_string(&raw["definitions"][0]["definition"]).unwrap();
    assert!(body.len() < super::super::MAX_DEFINITION_BYTES);
    super::super::decode_retained_definition(&body).unwrap_err();
}

#[test]
fn frozen_document_fields_remain_strict_under_budgeted_decoding() {
    let (wire, _, _) = fixture();
    let raw: Value =
        serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
    let manifest = &raw["manifest"];
    let document = &raw["definitions"][0];
    let hash = &document["sequence_sha256"];
    let definition = &document["definition"];
    for (document, diagnostic) in [
        (
            format!(
                r#"{{"version":1,"version":1,"sequence_sha256":{hash},"definition":{definition}}}"#
            ),
            "duplicate field",
        ),
        (
            format!(
                r#"{{"version":1,"sequence_sha256":{hash},"sequence_sha256":{hash},"definition":{definition}}}"#
            ),
            "duplicate field",
        ),
        (
            format!(
                r#"{{"version":1,"sequence_sha256":{hash},"definition":{definition},"definition":{definition}}}"#
            ),
            "duplicate field",
        ),
        (
            format!(
                r#"{{"version":1,"sequence_sha256":{hash},"definition":{definition},"unknown":true}}"#
            ),
            "unknown field",
        ),
        (
            format!(r#"{{"version":1,"sequence_sha256":{hash}}}"#),
            "missing field",
        ),
        (format!("[1,{hash},{definition}]"), "invalid type"),
    ] {
        let payload = format!(r#"{{"manifest":{manifest},"definitions":[{document}]}}"#);
        let error = parser_failure(payload.as_bytes(), DefinitionGraphLimits::default());
        assert!(
            error.to_string().contains(diagnostic),
            "{diagnostic}: {error}"
        );
    }
}

#[test]
fn public_definition_errors_do_not_echo_retained_values() {
    const SECRET: &str = "definition-secret-must-not-appear-in-diagnostics";
    let (wire, _, _) = fixture();
    let raw: Value =
        serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
    for wrapper in [true, false] {
        let mut bad = raw.clone();
        if wrapper {
            bad["definitions"][0]["version"] = json!(SECRET);
        } else {
            bad["definitions"][0]["definition"]["version"] = json!(SECRET);
        }
        let payload = serde_json::to_vec(&bad).unwrap();
        let (response, scope, expected) = published_invalid_payload(&payload);
        let error = response
            .verify(&scope, &expected, DefinitionGraphLimits::default())
            .unwrap_err();
        assert!(!format!("{error} {error:?}").contains(SECRET));
    }
    let mut body = raw["definitions"][0]["definition"].clone();
    body["version"] = json!(SECRET);
    let text = body.to_string();
    let error = super::super::decode_retained_definition(&text).unwrap_err();
    assert!(!format!("{error} {error:?}").contains(SECRET));
    let error = EncodedDefinitionBundle::from_retained(
        &raw["manifest"].to_string(),
        vec![RetainedDefinitionDocument {
            sequence_sha256: raw["manifest"]["root_sha256"].as_str().unwrap().into(),
            definition_json: text,
        }],
        DefinitionGraphLimits::default(),
    )
    .unwrap_err();
    assert!(!format!("{error} {error:?}").contains(SECRET));
    let mut manifest = raw["manifest"].clone();
    manifest["version"] = json!(SECRET);
    let error = DefinitionManifest::decode(&manifest.to_string()).unwrap_err();
    assert!(!format!("{error} {error:?}").contains(SECRET));
}

#[test]
fn definition_debug_metadata_omits_valid_private_parameters() {
    const SECRET: &str = "private-static-definition-parameter";
    let (wire, _, _) = fixture();
    let raw: Value =
        serde_json::from_slice(&STANDARD.decode(&wire.payload_base64).unwrap()).unwrap();
    let mut body = raw["definitions"][0]["definition"].clone();
    body["blocks"][0]["params"]["secret"] = json!(SECRET);
    let text = body.to_string();
    let frozen = super::super::decode_retained_definition(&text).unwrap();
    assert!(
        serde_json::to_string(&frozen.definition)
            .unwrap()
            .contains(SECRET)
    );
    assert!(!format!("{frozen:?}").contains(SECRET));
    let graph = FrozenDefinitionGraph {
        manifest: DefinitionManifest {
            version: 1,
            root_sha256: frozen.sequence_sha256.clone(),
            bindings: vec![],
        },
        definitions: vec![frozen],
    };
    assert!(!format!("{graph:?}").contains(SECRET));
}

#[test]
fn expanded_usage_matches_retained_bytes_and_input_metadata() {
    let (_, scope, reference) = fixture();
    let envelope: DefinitionWireEnvelope =
        serde_json::from_value(vector()["envelope"].clone()).unwrap();
    let payload = base64::engine::general_purpose::STANDARD
        .decode(&envelope.payload_base64)
        .unwrap();
    let raw: Value = serde_json::from_slice(&payload).unwrap();
    let expected: u32 = raw["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|document| cost(&document["definition"]))
        .sum();
    let verified = envelope
        .verify(&scope, &reference, DefinitionGraphLimits::default())
        .unwrap();
    assert_eq!(verified.expanded_bytes(), expected);
    let records = raw["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| RetainedDefinitionDocument {
            sequence_sha256: d["sequence_sha256"].as_str().unwrap().into(),
            definition_json: serde_json::to_string(&d["definition"]).unwrap(),
        })
        .collect();
    let (encoded, verified) = EncodedDefinitionBundle::from_retained(
        &serde_json::to_string(&raw["manifest"]).unwrap(),
        records,
        DefinitionGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(encoded.expanded_bytes(), expected);
    assert_eq!(verified.expanded_bytes(), expected);
    let input = crate::delegation_definition::input::EncodedDelegationInput::from_value(
        &serde_json::json!(null),
        crate::delegation_definition::input::InputLimits::default(),
    )
    .unwrap();
    let usage = crate::delegation_definition::protocol::DelegationResourceUsage::from_encoded(
        &encoded, &input,
    );
    assert_eq!(usage.definition_expanded_bytes, expected);
    assert_eq!(usage.input_payload_bytes, 4);
    assert_eq!(usage.input_expanded_bytes, 64);
}
