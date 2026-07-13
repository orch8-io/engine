//! Extensive black-box tests for DLQ root-cause fingerprinting.
//!
//! Covers the public surface of `orch8_types::failure` (fingerprint
//! stability/difference matrices, component explainability, message
//! normalization, legacy-message envelope derivation) and
//! `orch8_types::dlq` (wire types for group listing and guided retry).
//!
//! These complement — and deliberately do not duplicate — the inline unit
//! tests in `src/failure.rs` and `src/dlq.rs`.

use chrono::{DateTime, TimeZone, Utc};
use orch8_types::dlq::{DlqGroup, DlqGroupRetryRequest, DlqGroupRetryResponse, DlqRetryMode};
use orch8_types::failure::{
    ErrorClass, FailureEnvelope, FingerprintScope, envelope_from_message, fingerprint,
    normalize_message,
};
use orch8_types::finding::ResourceRef;
use serde_json::json;
use uuid::Uuid;

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 12, 8, 30, 0).unwrap()
}

fn t1() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 12, 9, 45, 0).unwrap()
}

fn scope() -> FingerprintScope {
    FingerprintScope {
        sequence_id: "3f1c2b6a-0000-7000-8000-0000000000aa".into(),
        sequence_version: 7,
    }
}

fn scope2() -> FingerprintScope {
    FingerprintScope {
        sequence_id: "3f1c2b6a-0000-7000-8000-0000000000bb".into(),
        sequence_version: 7,
    }
}

/// Fully-populated envelope used as the difference-matrix baseline.
fn full_envelope() -> FailureEnvelope {
    FailureEnvelope::new(
        "HTTP_STATUS",
        ErrorClass::ExternalDependency,
        "upstream said no",
        false,
        t0(),
    )
    .with_block("charge")
    .with_handler("http_request")
    .with_external_status("503")
    .with_resource(ResourceRef::new("credential", "billing_key"))
}

fn minimal_envelope(code: &str) -> FailureEnvelope {
    FailureEnvelope::new(code, ErrorClass::Application, "boom", false, t0())
}

fn all_classes() -> [ErrorClass; 9] {
    [
        ErrorClass::Application,
        ErrorClass::Configuration,
        ErrorClass::Worker,
        ErrorClass::Credential,
        ErrorClass::Policy,
        ErrorClass::ExternalDependency,
        ErrorClass::Timeout,
        ErrorClass::Cancelled,
        ErrorClass::Internal,
    ]
}

fn is_lower_hex(s: &str) -> bool {
    s.chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

// =====================================================================
// ErrorClass
// =====================================================================

#[test]
fn error_class_as_str_application() {
    assert_eq!(ErrorClass::Application.as_str(), "application");
}

#[test]
fn error_class_as_str_configuration() {
    assert_eq!(ErrorClass::Configuration.as_str(), "configuration");
}

#[test]
fn error_class_as_str_worker() {
    assert_eq!(ErrorClass::Worker.as_str(), "worker");
}

#[test]
fn error_class_as_str_credential() {
    assert_eq!(ErrorClass::Credential.as_str(), "credential");
}

#[test]
fn error_class_as_str_policy() {
    assert_eq!(ErrorClass::Policy.as_str(), "policy");
}

#[test]
fn error_class_as_str_external_dependency() {
    assert_eq!(
        ErrorClass::ExternalDependency.as_str(),
        "external_dependency"
    );
}

#[test]
fn error_class_as_str_timeout() {
    assert_eq!(ErrorClass::Timeout.as_str(), "timeout");
}

#[test]
fn error_class_as_str_cancelled() {
    assert_eq!(ErrorClass::Cancelled.as_str(), "cancelled");
}

#[test]
fn error_class_as_str_internal() {
    assert_eq!(ErrorClass::Internal.as_str(), "internal");
}

#[test]
fn error_class_as_str_values_are_pairwise_distinct() {
    let classes = all_classes();
    for (i, a) in classes.iter().enumerate() {
        for b in &classes[i + 1..] {
            assert_ne!(a.as_str(), b.as_str());
        }
    }
}

#[test]
fn error_class_deserializes_every_snake_case_string() {
    for class in all_classes() {
        let s = format!("\"{}\"", class.as_str());
        let back: ErrorClass = serde_json::from_str(&s).unwrap();
        assert_eq!(back, class, "{s}");
    }
}

#[test]
fn error_class_serde_round_trips_every_variant() {
    for class in all_classes() {
        let json = serde_json::to_string(&class).unwrap();
        let back: ErrorClass = serde_json::from_str(&json).unwrap();
        assert_eq!(back, class);
    }
}

#[test]
fn error_class_rejects_unknown_variant() {
    let r: Result<ErrorClass, _> = serde_json::from_str("\"cosmic_rays\"");
    assert!(r.is_err());
}

#[test]
fn error_class_rejects_pascal_case() {
    let r: Result<ErrorClass, _> = serde_json::from_str("\"ExternalDependency\"");
    assert!(r.is_err());
}

#[test]
fn error_class_rejects_empty_string() {
    let r: Result<ErrorClass, _> = serde_json::from_str("\"\"");
    assert!(r.is_err());
}

// =====================================================================
// FailureEnvelope construction + serde
// =====================================================================

#[test]
fn envelope_new_sets_required_fields() {
    let e = FailureEnvelope::new("CODE", ErrorClass::Worker, "msg", true, t0());
    assert_eq!(e.error_code, "CODE");
    assert_eq!(e.error_class, ErrorClass::Worker);
    assert_eq!(e.message, "msg");
    assert!(e.retryable);
    assert_eq!(e.occurred_at, t0());
}

#[test]
fn envelope_new_leaves_optionals_none() {
    let e = minimal_envelope("X");
    assert!(e.block_id.is_none());
    assert!(e.handler.is_none());
    assert!(e.external_status.is_none());
    assert!(e.resource.is_none());
    assert!(e.details.is_none());
}

#[test]
fn envelope_with_block_sets_block() {
    let e = minimal_envelope("X").with_block("charge");
    assert_eq!(e.block_id.as_deref(), Some("charge"));
}

#[test]
fn envelope_with_handler_sets_handler() {
    let e = minimal_envelope("X").with_handler("http_request");
    assert_eq!(e.handler.as_deref(), Some("http_request"));
}

#[test]
fn envelope_with_external_status_sets_status() {
    let e = minimal_envelope("X").with_external_status("503");
    assert_eq!(e.external_status.as_deref(), Some("503"));
}

#[test]
fn envelope_with_resource_sets_resource() {
    let e = minimal_envelope("X").with_resource(ResourceRef::new("queue", "orders"));
    let r = e.resource.unwrap();
    assert_eq!(r.kind, "queue");
    assert_eq!(r.id, "orders");
}

#[test]
fn envelope_with_details_sets_details() {
    let e = minimal_envelope("X").with_details(json!({"attempt": 5}));
    assert_eq!(e.details.unwrap()["attempt"], 5);
}

#[test]
fn envelope_builders_chain_and_last_write_wins() {
    let e = minimal_envelope("X").with_block("a").with_block("b");
    assert_eq!(e.block_id.as_deref(), Some("b"));
}

#[test]
fn envelope_serde_omits_block_id_when_absent() {
    let v = serde_json::to_value(minimal_envelope("X")).unwrap();
    assert!(!v.as_object().unwrap().contains_key("block_id"));
}

#[test]
fn envelope_serde_omits_handler_when_absent() {
    let v = serde_json::to_value(minimal_envelope("X")).unwrap();
    assert!(!v.as_object().unwrap().contains_key("handler"));
}

#[test]
fn envelope_serde_omits_external_status_when_absent() {
    let v = serde_json::to_value(minimal_envelope("X")).unwrap();
    assert!(!v.as_object().unwrap().contains_key("external_status"));
}

#[test]
fn envelope_serde_omits_resource_when_absent() {
    let v = serde_json::to_value(minimal_envelope("X")).unwrap();
    assert!(!v.as_object().unwrap().contains_key("resource"));
}

#[test]
fn envelope_serde_omits_details_when_absent() {
    let v = serde_json::to_value(minimal_envelope("X")).unwrap();
    assert!(!v.as_object().unwrap().contains_key("details"));
}

#[test]
fn envelope_serde_includes_optionals_when_present() {
    let v = serde_json::to_value(full_envelope().with_details(json!({"k": 1}))).unwrap();
    let obj = v.as_object().unwrap();
    for key in [
        "block_id",
        "handler",
        "external_status",
        "resource",
        "details",
    ] {
        assert!(obj.contains_key(key), "{key} should be present");
    }
}

#[test]
fn envelope_serde_error_class_is_snake_case_in_json() {
    let v = serde_json::to_value(full_envelope()).unwrap();
    assert_eq!(v["error_class"], "external_dependency");
}

#[test]
fn envelope_deserializes_without_optional_keys() {
    let e: FailureEnvelope = serde_json::from_value(json!({
        "error_code": "TIMEOUT",
        "error_class": "timeout",
        "message": "took too long",
        "retryable": true,
        "occurred_at": "2026-07-12T08:30:00Z",
    }))
    .unwrap();
    assert_eq!(e.error_code, "TIMEOUT");
    assert!(e.block_id.is_none());
    assert!(e.resource.is_none());
}

#[test]
fn envelope_full_round_trip_preserves_everything() {
    let e = full_envelope().with_details(json!({"attempt": 2, "note": "x"}));
    let back: FailureEnvelope = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
    assert_eq!(back, e);
}

#[test]
fn envelope_occurred_at_round_trips_exactly() {
    let e = minimal_envelope("X");
    let back: FailureEnvelope = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
    assert_eq!(back.occurred_at, t0());
}

// =====================================================================
// Fingerprint: stability
// =====================================================================

#[test]
fn fingerprint_is_deterministic_across_calls() {
    let e = full_envelope();
    let first = fingerprint(&scope(), &e);
    for _ in 0..10 {
        let again = fingerprint(&scope(), &e);
        assert_eq!(again.hash, first.hash);
        assert_eq!(again.components, first.components);
    }
}

#[test]
fn fingerprint_stable_across_construction_paths() {
    let built = full_envelope();
    let literal = FailureEnvelope {
        error_code: "HTTP_STATUS".into(),
        error_class: ErrorClass::ExternalDependency,
        message: "upstream said no".into(),
        block_id: Some("charge".into()),
        handler: Some("http_request".into()),
        external_status: Some("503".into()),
        resource: Some(ResourceRef::new("credential", "billing_key")),
        retryable: false,
        details: None,
        occurred_at: t0(),
    };
    assert_eq!(
        fingerprint(&scope(), &built).hash,
        fingerprint(&scope(), &literal).hash
    );
}

#[test]
fn fingerprint_hash_is_16_lowercase_hex_chars() {
    let fp = fingerprint(&scope(), &full_envelope());
    assert_eq!(fp.hash.len(), 16);
    assert!(is_lower_hex(&fp.hash), "{}", fp.hash);
}

#[test]
fn fingerprint_ignores_retryable_flag() {
    let mut a = full_envelope();
    a.retryable = true;
    let mut b = full_envelope();
    b.retryable = false;
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_ignores_details() {
    let a = full_envelope().with_details(json!({"attempt": 1}));
    let b = full_envelope().with_details(json!({"attempt": 99, "other": true}));
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_ignores_occurred_at() {
    let mut a = full_envelope();
    a.occurred_at = t0();
    let mut b = full_envelope();
    b.occurred_at = t1();
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_ignores_resource_display_name() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.resource = Some(ResourceRef::named(
        "credential",
        "billing_key",
        "Billing key",
    ));
    // The human-readable name is presentation-only; kind+id are identity.
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_with_specific_code_ignores_message_entirely() {
    let mut a = full_envelope();
    a.message = "attempt 12 failed for 3f1c2b6a-0000-7000-8000-0000000000cc".into();
    let mut b = full_envelope();
    b.message = "completely different words".into();
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

// =====================================================================
// Fingerprint: difference matrix (one axis at a time)
// =====================================================================

#[test]
fn fingerprint_differs_by_sequence_id() {
    let e = full_envelope();
    assert_ne!(
        fingerprint(&scope(), &e).hash,
        fingerprint(&scope2(), &e).hash
    );
}

#[test]
fn fingerprint_differs_by_sequence_version() {
    let mut s = scope();
    s.sequence_version = 8;
    let e = full_envelope();
    assert_ne!(fingerprint(&scope(), &e).hash, fingerprint(&s, &e).hash);
}

#[test]
fn fingerprint_handles_zero_and_negative_versions_distinctly() {
    let mut zero = scope();
    zero.sequence_version = 0;
    let mut neg = scope();
    neg.sequence_version = -1;
    let e = full_envelope();
    assert_ne!(fingerprint(&zero, &e).hash, fingerprint(&neg, &e).hash);
}

#[test]
fn fingerprint_differs_by_block_value() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.block_id = Some("refund".into());
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_block_presence() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.block_id = None;
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_distinguishes_empty_block_from_absent_block() {
    let mut empty = full_envelope();
    empty.block_id = Some(String::new());
    let mut absent = full_envelope();
    absent.block_id = None;
    assert_ne!(
        fingerprint(&scope(), &empty).hash,
        fingerprint(&scope(), &absent).hash
    );
}

#[test]
fn fingerprint_differs_by_handler_value() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.handler = Some("graphql_request".into());
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_handler_presence() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.handler = None;
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_error_class_for_every_pair() {
    let classes = all_classes();
    let mut hashes = Vec::new();
    for class in classes {
        let mut e = full_envelope();
        e.error_class = class;
        hashes.push(fingerprint(&scope(), &e).hash);
    }
    for (i, a) in hashes.iter().enumerate() {
        for b in &hashes[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn fingerprint_differs_by_error_code() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.error_code = "HTTP_TRANSPORT".into();
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_error_code_is_case_sensitive() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.error_code = "http_status".into();
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_external_status_value() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.external_status = Some("502".into());
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_external_status_presence() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.external_status = None;
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_resource_kind() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.resource = Some(ResourceRef::new("queue", "billing_key"));
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_resource_id() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.resource = Some(ResourceRef::new("credential", "other_key"));
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_by_resource_presence() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.resource = None;
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn fingerprint_differs_when_multiple_axes_change_together() {
    let a = full_envelope();
    let mut b = full_envelope();
    b.block_id = Some("refund".into());
    b.error_class = ErrorClass::Timeout;
    b.error_code = "TIMEOUT".into();
    b.external_status = None;
    assert_ne!(
        fingerprint(&scope2(), &b).hash,
        fingerprint(&scope(), &a).hash
    );
}

#[test]
fn fingerprint_axis_swap_does_not_collide() {
    // Swapping values across two axes must not cancel out.
    let mut a = minimal_envelope("X");
    a.block_id = Some("alpha".into());
    a.handler = Some("beta".into());
    let mut b = minimal_envelope("X");
    b.block_id = Some("beta".into());
    b.handler = Some("alpha".into());
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

// =====================================================================
// Fingerprint: component boundary unambiguity
// =====================================================================

#[test]
fn boundary_sequence_id_vs_version_is_unambiguous() {
    // "seq:a" + "v:11" must differ from "seq:a1" + "v:1".
    let a = FingerprintScope {
        sequence_id: "a".into(),
        sequence_version: 11,
    };
    let b = FingerprintScope {
        sequence_id: "a1".into(),
        sequence_version: 1,
    };
    let e = minimal_envelope("X");
    assert_ne!(fingerprint(&a, &e).hash, fingerprint(&b, &e).hash);
}

#[test]
fn boundary_block_vs_handler_is_unambiguous() {
    let mut a = minimal_envelope("X");
    a.block_id = Some("chargec".into());
    a.handler = Some("ard".into());
    let mut b = minimal_envelope("X");
    b.block_id = Some("charge".into());
    b.handler = Some("card".into());
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn boundary_code_vs_status_is_unambiguous() {
    let a = minimal_envelope("HTTP5").with_external_status("03");
    let b = minimal_envelope("HTTP").with_external_status("503");
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn boundary_status_vs_resource_is_unambiguous() {
    let a = minimal_envelope("X")
        .with_external_status("503q")
        .with_resource(ResourceRef::new("ueue", "orders"));
    let b = minimal_envelope("X")
        .with_external_status("503")
        .with_resource(ResourceRef::new("queue", "orders"));
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn boundary_resource_kind_vs_id_is_unambiguous() {
    let a = minimal_envelope("X").with_resource(ResourceRef::new("queueor", "ders"));
    let b = minimal_envelope("X").with_resource(ResourceRef::new("queue", "orders"));
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

// =====================================================================
// Fingerprint: explainable components
// =====================================================================

#[test]
fn components_full_envelope_in_documented_order() {
    let fp = fingerprint(&scope(), &full_envelope());
    assert_eq!(
        fp.components,
        vec![
            "seq:3f1c2b6a-0000-7000-8000-0000000000aa".to_string(),
            "v:7".to_string(),
            "block:charge".to_string(),
            "handler:http_request".to_string(),
            "class:external_dependency".to_string(),
            "code:HTTP_STATUS".to_string(),
            "status:503".to_string(),
            "resource:credential:billing_key".to_string(),
        ]
    );
}

#[test]
fn components_minimal_envelope_has_only_required_parts() {
    let fp = fingerprint(&scope(), &minimal_envelope("SOME_CODE"));
    assert_eq!(
        fp.components,
        vec![
            "seq:3f1c2b6a-0000-7000-8000-0000000000aa".to_string(),
            "v:7".to_string(),
            "class:application".to_string(),
            "code:SOME_CODE".to_string(),
        ]
    );
}

#[test]
fn components_skip_block_when_absent() {
    let fp = fingerprint(&scope(), &minimal_envelope("X"));
    assert!(!fp.components.iter().any(|c| c.starts_with("block:")));
}

#[test]
fn components_skip_handler_when_absent() {
    let fp = fingerprint(&scope(), &minimal_envelope("X"));
    assert!(!fp.components.iter().any(|c| c.starts_with("handler:")));
}

#[test]
fn components_skip_status_when_absent() {
    let fp = fingerprint(&scope(), &minimal_envelope("X"));
    assert!(!fp.components.iter().any(|c| c.starts_with("status:")));
}

#[test]
fn components_skip_resource_when_absent() {
    let fp = fingerprint(&scope(), &minimal_envelope("X"));
    assert!(!fp.components.iter().any(|c| c.starts_with("resource:")));
}

#[test]
fn components_start_with_sequence_then_version() {
    let fp = fingerprint(&scope(), &full_envelope());
    assert!(fp.components[0].starts_with("seq:"));
    assert!(fp.components[1].starts_with("v:"));
}

#[test]
fn components_negative_version_renders_signed() {
    let mut s = scope();
    s.sequence_version = -3;
    let fp = fingerprint(&s, &minimal_envelope("X"));
    assert_eq!(fp.components[1], "v:-3");
}

#[test]
fn components_never_contain_message_for_specific_code() {
    let mut e = full_envelope();
    e.message = "super secret operational text".into();
    let fp = fingerprint(&scope(), &e);
    assert!(!fp.components.iter().any(|c| c.starts_with("msg:")));
    assert!(!fp.components.iter().any(|c| c.contains("secret")));
}

// =====================================================================
// Fingerprint: unknown / empty code fallback
// =====================================================================

#[test]
fn unknown_code_appends_msg_component() {
    let mut e = minimal_envelope("unknown");
    e.message = "widget frobnication failed".into();
    let fp = fingerprint(&scope(), &e);
    assert!(fp.components.last().unwrap().starts_with("msg:"));
}

#[test]
fn empty_code_appends_msg_component() {
    let mut e = minimal_envelope("");
    e.message = "widget frobnication failed".into();
    let fp = fingerprint(&scope(), &e);
    assert!(fp.components.last().unwrap().starts_with("msg:"));
}

#[test]
fn uppercase_unknown_code_also_falls_back() {
    let mut e = minimal_envelope("UNKNOWN");
    e.message = "widget frobnication failed".into();
    let fp = fingerprint(&scope(), &e);
    assert!(fp.components.last().unwrap().starts_with("msg:"));
}

#[test]
fn mixed_case_unknown_code_also_falls_back() {
    let mut e = minimal_envelope("Unknown");
    e.message = "widget frobnication failed".into();
    let fp = fingerprint(&scope(), &e);
    assert!(fp.components.last().unwrap().starts_with("msg:"));
}

#[test]
fn code_merely_containing_unknown_does_not_fall_back() {
    let mut e = minimal_envelope("unknown_handler");
    e.message = "whatever".into();
    let fp = fingerprint(&scope(), &e);
    assert!(!fp.components.iter().any(|c| c.starts_with("msg:")));
}

#[test]
fn msg_component_is_a_bounded_hash_not_the_text() {
    let mut e = minimal_envelope("unknown");
    e.message = "the message body must never appear verbatim".into();
    let fp = fingerprint(&scope(), &e);
    let msg = fp.components.last().unwrap();
    let hex = msg.strip_prefix("msg:").unwrap();
    assert_eq!(hex.len(), 16);
    assert!(is_lower_hex(hex), "{hex}");
    assert!(!msg.contains("verbatim"));
}

#[test]
fn unknown_code_different_messages_split_groups() {
    let mut a = minimal_envelope("unknown");
    a.message = "the widget melted".into();
    let mut b = minimal_envelope("unknown");
    b.message = "the gasket blew".into();
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn unknown_code_volatile_uuids_do_not_split_groups() {
    let mut a = minimal_envelope("unknown");
    a.message = "lookup failed for 0198f0a2-1111-7000-8000-00000000aaaa".into();
    let mut b = minimal_envelope("unknown");
    b.message = "lookup failed for 0198f0a2-2222-7000-8000-00000000bbbb".into();
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn unknown_code_volatile_numbers_do_not_split_groups() {
    let mut a = minimal_envelope("unknown");
    a.message = "gave up after 17 attempts and 2500 ms".into();
    let mut b = minimal_envelope("unknown");
    b.message = "gave up after 90 attempts and 123456 ms".into();
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn unknown_code_case_and_whitespace_do_not_split_groups() {
    let mut a = minimal_envelope("unknown");
    a.message = "Connection   REFUSED by\tpeer".into();
    let mut b = minimal_envelope("unknown");
    b.message = "connection refused by peer".into();
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn unknown_code_messages_differing_past_256_chars_group_together() {
    let base = "word ".repeat(60); // normalizes far past the 256-char bound
    let mut a = minimal_envelope("unknown");
    a.message = format!("{base}alpha");
    let mut b = minimal_envelope("unknown");
    b.message = format!("{base}omega");
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn unknown_code_still_split_by_structural_axes() {
    // Same unknown message in two different blocks: still two incidents.
    let mut a = minimal_envelope("unknown").with_block("charge");
    a.message = "melted".into();
    let mut b = minimal_envelope("unknown").with_block("refund");
    b.message = "melted".into();
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

#[test]
fn unknown_vs_uppercase_unknown_codes_are_distinct_groups() {
    // Both fall back to the message hash, but the code component itself
    // stays case-sensitive.
    let mut a = minimal_envelope("unknown");
    a.message = "melted".into();
    let mut b = minimal_envelope("UNKNOWN");
    b.message = "melted".into();
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

// =====================================================================
// normalize_message: UUIDs and hex
// =====================================================================

#[test]
fn normalize_uuid_in_middle() {
    assert_eq!(
        normalize_message("instance 0198f0a2-1111-7000-8000-00000000abcd vanished"),
        "instance # vanished"
    );
}

#[test]
fn normalize_uuid_at_start() {
    assert_eq!(
        normalize_message("0198f0a2-1111-7000-8000-00000000abcd vanished"),
        "# vanished"
    );
}

#[test]
fn normalize_uuid_at_end() {
    assert_eq!(
        normalize_message("lost 0198f0a2-1111-7000-8000-00000000abcd"),
        "lost #"
    );
}

#[test]
fn normalize_multiple_uuids_separated_by_words() {
    assert_eq!(
        normalize_message(
            "moved 0198f0a2-1111-7000-8000-00000000aaaa to 0198f0a2-2222-7000-8000-00000000bbbb ok"
        ),
        "moved # to # ok"
    );
}

#[test]
fn normalize_adjacent_uuids_collapse_to_one_placeholder() {
    assert_eq!(
        normalize_message(
            "pair 0198f0a2-1111-7000-8000-00000000aaaa 0198f0a2-2222-7000-8000-00000000bbbb done"
        ),
        "pair # done"
    );
}

#[test]
fn normalize_uppercase_uuid_is_volatile() {
    assert_eq!(
        normalize_message("saw 0198F0A2-1111-7000-8000-00000000ABCD here"),
        "saw # here"
    );
}

#[test]
fn normalize_undashed_32_hex_uuid_is_volatile() {
    assert_eq!(
        normalize_message("trace 0198f0a21111700080000000abcd0000 end"),
        "trace # end"
    );
}

#[test]
fn normalize_hex_run_8_chars_is_volatile() {
    assert_eq!(normalize_message("frame deadbeef top"), "frame # top");
}

#[test]
fn normalize_hex_run_16_chars_is_volatile() {
    assert_eq!(normalize_message("addr deadbeefcafe1234 bad"), "addr # bad");
}

#[test]
fn normalize_hex_run_32_chars_is_volatile() {
    assert_eq!(
        normalize_message("sum deadbeefcafe1234deadbeefcafe1234 end"),
        "sum # end"
    );
}

#[test]
fn normalize_hex_run_7_chars_survives() {
    assert_eq!(normalize_message("word deadbee kept"), "word deadbee kept");
}

#[test]
fn normalize_short_hex_word_survives() {
    // "cafe", "bad", "face" are all-hex but too short to be volatile.
    assert_eq!(normalize_message("a bad cafe face"), "a bad cafe face");
}

// =====================================================================
// normalize_message: numbers and generated ids
// =====================================================================

#[test]
fn normalize_single_digit_is_volatile() {
    assert_eq!(normalize_message("attempt 7 failed"), "attempt # failed");
}

#[test]
fn normalize_long_pure_number_is_volatile() {
    assert_eq!(normalize_message("row 1234567890 gone"), "row # gone");
}

#[test]
fn normalize_epoch_timestamp_is_volatile() {
    assert_eq!(normalize_message("at 1720699200 ms"), "at # ms");
}

#[test]
fn normalize_long_mixed_alnum_id_is_volatile() {
    // 20 chars, 10 digits, not hex ('g'..'j') — a generated id.
    assert_eq!(
        normalize_message("job a1b2c3d4e5g6h7i8j9k0 died"),
        "job # died"
    );
}

#[test]
fn normalize_16_char_mixed_id_with_4_digits_is_volatile() {
    assert_eq!(normalize_message("key ghijklmnopqr1234 lost"), "key # lost");
}

#[test]
fn normalize_15_char_mixed_token_with_digits_survives() {
    // One char short of the generated-id length threshold.
    assert_eq!(
        normalize_message("key ghjklmnopqr1234 lost"),
        "key ghjklmnopqr1234 lost"
    );
}

#[test]
fn normalize_16_char_token_with_3_digits_survives() {
    assert_eq!(
        normalize_message("key ghijklmnopqrs123 lost"),
        "key ghijklmnopqrs123 lost"
    );
}

#[test]
fn normalize_short_request_id_survives() {
    // req-8f2a9b17: 12 chars, non-hex prefix — below both thresholds.
    // Short prefixed ids are NOT treated as volatile.
    assert_eq!(
        normalize_message("request req-8f2a9b17 failed"),
        "request req-8f2a9b17 failed"
    );
}

#[test]
fn normalize_long_request_id_is_volatile() {
    // 19 chars with 14 digits crosses the generated-id threshold.
    assert_eq!(
        normalize_message("request req-20260711-123456 failed"),
        "request # failed"
    );
}

#[test]
fn normalize_tokens_with_digits_and_letters_short_survive() {
    assert_eq!(normalize_message("2fast 2furious"), "2fast 2furious");
}

#[test]
fn normalize_s3_and_utf8_style_tokens_survive() {
    assert_eq!(
        normalize_message("s3 PUT failed: utf8 decode error"),
        "s3 put failed: utf8 decode error"
    );
}

#[test]
fn normalize_dashed_word_survives() {
    assert_eq!(
        normalize_message("re-try the well-known path"),
        "re-try the well-known path"
    );
}

#[test]
fn normalize_short_dashed_number_survives() {
    // "123-456" is hex-ish but only 7 chars.
    assert_eq!(normalize_message("code 123-456 ok"), "code 123-456 ok");
}

#[test]
fn normalize_longer_dashed_number_is_volatile() {
    // "1234-5678" is 9 chars of digits+dash → volatile.
    assert_eq!(normalize_message("code 1234-5678 ok"), "code # ok");
}

// =====================================================================
// normalize_message: timestamps
// =====================================================================

#[test]
fn normalize_date_only_is_volatile() {
    assert_eq!(normalize_message("on 2026-07-11 it broke"), "on # it broke");
}

#[test]
fn normalize_iso_timestamp_partially_survives() {
    // Actual behavior: the "2026-07-11T12" run contains a non-hex letter
    // so only the pure-digit segments become placeholders.
    assert_eq!(
        normalize_message("at 2026-07-11T12:00:00Z boom"),
        "at 2026-07-11t12:#:00z boom"
    );
}

// =====================================================================
// normalize_message: adjacency, separators, punctuation
// =====================================================================

#[test]
fn normalize_many_adjacent_volatile_tokens_collapse() {
    assert_eq!(normalize_message("ids 1 2 3 4 5 end"), "ids # end");
}

#[test]
fn normalize_volatile_split_by_colon_does_not_collapse() {
    // Placeholder collapsing only spans whitespace, not punctuation.
    assert_eq!(normalize_message("span 123:456 x"), "span #:# x");
}

#[test]
fn normalize_volatile_split_by_dot_does_not_collapse() {
    assert_eq!(normalize_message("took 12.34 s"), "took #.# s");
}

#[test]
fn normalize_ipv4_becomes_four_placeholders() {
    assert_eq!(
        normalize_message("peer 192.168.0.1 unreachable"),
        "peer #.#.#.# unreachable"
    );
}

#[test]
fn normalize_preserves_punctuation() {
    assert_eq!(
        normalize_message("error: frobnicate (stage two)!"),
        "error: frobnicate (stage two)!"
    );
}

#[test]
fn normalize_literal_hash_merges_with_placeholder() {
    // A literal '#' followed by a volatile token collapses into one.
    assert_eq!(normalize_message("x # 123"), "x #");
}

#[test]
fn normalize_volatile_only_message_is_single_placeholder() {
    assert_eq!(normalize_message("12345"), "#");
}

#[test]
fn normalize_message_of_only_uuids_is_single_placeholder() {
    assert_eq!(
        normalize_message(
            "0198f0a2-1111-7000-8000-00000000aaaa 0198f0a2-2222-7000-8000-00000000bbbb"
        ),
        "#"
    );
}

// =====================================================================
// normalize_message: case, whitespace, unicode, bounds
// =====================================================================

#[test]
fn normalize_lowercases_ascii() {
    assert_eq!(
        normalize_message("ERROR Failed BADLY"),
        "error failed badly"
    );
}

#[test]
fn normalize_collapses_tabs_and_newlines() {
    assert_eq!(normalize_message("a\tb\nc\r\nd"), "a b c d");
}

#[test]
fn normalize_trims_leading_and_trailing_whitespace() {
    assert_eq!(normalize_message("   tight   "), "tight");
}

#[test]
fn normalize_preserves_unicode_text() {
    // Non-ASCII passes through untouched (including case).
    assert_eq!(normalize_message("Ошибка Сети"), "Ошибка Сети");
}

#[test]
fn normalize_unicode_with_volatile_number() {
    assert_eq!(normalize_message("Ошибка 404"), "Ошибка #");
}

#[test]
fn normalize_bounds_output_at_256() {
    let long = "lengthy diagnostic words ".repeat(50);
    assert!(normalize_message(&long).len() <= 256);
}

#[test]
fn normalize_short_input_not_padded_or_truncated() {
    assert_eq!(normalize_message("ok"), "ok");
}

#[test]
fn normalize_empty_is_empty() {
    assert_eq!(normalize_message(""), "");
}

#[test]
fn normalize_whitespace_only_is_empty() {
    assert_eq!(normalize_message(" \t\n "), "");
}

#[test]
fn normalize_symbols_only_pass_through() {
    assert_eq!(normalize_message("?!@%"), "?!@%");
}

#[test]
fn normalize_is_idempotent_on_typical_messages() {
    for msg in [
        "instance 0198f0a2-1111-7000-8000-00000000abcd vanished",
        "retry 17 of 30 failed",
        "Connection REFUSED",
        "error: frobnicate (stage two)!",
    ] {
        let once = normalize_message(msg);
        assert_eq!(normalize_message(&once), once, "{msg}");
    }
}

// =====================================================================
// envelope_from_message: HTTP status extraction
// =====================================================================

fn efm(msg: &str) -> FailureEnvelope {
    envelope_from_message(msg, false, t0())
}

#[test]
fn efm_http_marker_extracts_status() {
    let e = efm("upstream http 503 unavailable");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.external_status.as_deref(), Some("503"));
    assert_eq!(e.error_class, ErrorClass::ExternalDependency);
}

#[test]
fn efm_status_marker_extracts_status() {
    let e = efm("got status 404 from api");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.external_status.as_deref(), Some("404"));
}

#[test]
fn efm_returned_marker_extracts_status() {
    let e = efm("endpoint returned 502 bad gateway");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.external_status.as_deref(), Some("502"));
}

#[test]
fn efm_status_401_is_credential_class() {
    assert_eq!(efm("status 401").error_class, ErrorClass::Credential);
}

#[test]
fn efm_status_403_is_credential_class() {
    assert_eq!(efm("status 403").error_class, ErrorClass::Credential);
}

#[test]
fn efm_status_408_is_external_dependency_class() {
    assert_eq!(
        efm("status 408").error_class,
        ErrorClass::ExternalDependency
    );
}

#[test]
fn efm_status_429_is_external_dependency_class() {
    assert_eq!(
        efm("status 429").error_class,
        ErrorClass::ExternalDependency
    );
}

#[test]
fn efm_status_400_is_application_class() {
    assert_eq!(efm("status 400").error_class, ErrorClass::Application);
}

#[test]
fn efm_status_404_is_application_class() {
    assert_eq!(efm("status 404").error_class, ErrorClass::Application);
}

#[test]
fn efm_status_422_is_application_class() {
    assert_eq!(efm("status 422").error_class, ErrorClass::Application);
}

#[test]
fn efm_status_499_is_application_class() {
    assert_eq!(efm("status 499").error_class, ErrorClass::Application);
}

#[test]
fn efm_status_500_is_external_dependency_class() {
    assert_eq!(
        efm("status 500").error_class,
        ErrorClass::ExternalDependency
    );
}

#[test]
fn efm_status_503_is_external_dependency_class() {
    assert_eq!(
        efm("status 503").error_class,
        ErrorClass::ExternalDependency
    );
}

#[test]
fn efm_status_599_is_external_dependency_class() {
    assert_eq!(
        efm("status 599").error_class,
        ErrorClass::ExternalDependency
    );
}

#[test]
fn efm_status_302_actual_behavior_external_dependency() {
    // Non-4xx, non-5xx statuses fall into the catch-all class.
    let e = efm("upstream returned 302 redirect");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.error_class, ErrorClass::ExternalDependency);
}

#[test]
fn efm_status_200_actual_behavior_external_dependency() {
    let e = efm("weird: status 200 but body was garbage");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.external_status.as_deref(), Some("200"));
    assert_eq!(e.error_class, ErrorClass::ExternalDependency);
}

#[test]
fn efm_five_digit_number_is_not_a_status() {
    let e = efm("returned 12345 rows then died");
    assert_eq!(e.error_code, "unknown");
    assert!(e.external_status.is_none());
}

#[test]
fn efm_two_digit_number_is_not_a_status() {
    let e = efm("http 42 is not a real status");
    assert_eq!(e.error_code, "unknown");
}

#[test]
fn efm_600_is_out_of_status_range() {
    let e = efm("http 600 impossible");
    assert_eq!(e.error_code, "unknown");
    assert!(e.external_status.is_none());
}

#[test]
fn efm_099_is_out_of_status_range() {
    let e = efm("http 099 impossible");
    assert_eq!(e.error_code, "unknown");
}

#[test]
fn efm_marker_requires_space_before_digits() {
    // "http503" has no "http " marker occurrence with digits after it.
    let e = efm("saw http503 in the log");
    assert_eq!(e.error_code, "unknown");
}

#[test]
fn efm_later_marker_still_found_when_first_has_no_digits() {
    // "http " is present but followed by a word; "status 503" still hits.
    let e = efm("http call blew up with status 503");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.external_status.as_deref(), Some("503"));
}

#[test]
fn efm_status_extraction_beats_timeout_keyword() {
    let e = efm("timeout waiting: upstream returned 504");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.external_status.as_deref(), Some("504"));
}

#[test]
fn efm_status_extraction_is_case_insensitive() {
    let e = efm("Upstream HTTP 503 Service Unavailable");
    assert_eq!(e.error_code, "HTTP_STATUS");
    assert_eq!(e.external_status.as_deref(), Some("503"));
}

// =====================================================================
// envelope_from_message: keyword classification
// =====================================================================

#[test]
fn efm_timed_out_is_timeout() {
    let e = efm("request timed out after 30s");
    assert_eq!(e.error_code, "TIMEOUT");
    assert_eq!(e.error_class, ErrorClass::Timeout);
}

#[test]
fn efm_timeout_word_is_timeout() {
    let e = efm("write timeout on socket");
    assert_eq!(e.error_code, "TIMEOUT");
}

#[test]
fn efm_deadline_is_timeout() {
    let e = efm("deadline exceeded");
    assert_eq!(e.error_code, "TIMEOUT");
    assert_eq!(e.error_class, ErrorClass::Timeout);
}

#[test]
fn efm_credential_keyword() {
    let e = efm("credential 'billing' has been revoked");
    assert_eq!(e.error_code, "CREDENTIAL");
    assert_eq!(e.error_class, ErrorClass::Credential);
}

#[test]
fn efm_unauthorized_keyword() {
    let e = efm("unauthorized: bad signature");
    assert_eq!(e.error_code, "CREDENTIAL");
    assert_eq!(e.error_class, ErrorClass::Credential);
}

#[test]
fn efm_no_compatible_worker() {
    let e = efm("no compatible worker for queue default");
    assert_eq!(e.error_code, "NO_WORKER");
    assert_eq!(e.error_class, ErrorClass::Worker);
}

#[test]
fn efm_no_worker() {
    let e = efm("no worker picked up the task");
    assert_eq!(e.error_code, "NO_WORKER");
    assert_eq!(e.error_class, ErrorClass::Worker);
}

#[test]
fn efm_circuit_breaker() {
    let e = efm("circuit breaker tripped for charge_card");
    assert_eq!(e.error_code, "CIRCUIT_OPEN");
    assert_eq!(e.error_class, ErrorClass::Policy);
}

#[test]
fn efm_breaker_open() {
    let e = efm("breaker open; refusing to dispatch");
    assert_eq!(e.error_code, "CIRCUIT_OPEN");
}

#[test]
fn efm_budget() {
    let e = efm("budget exhausted: max_wall_clock");
    assert_eq!(e.error_code, "BUDGET_EXCEEDED");
    assert_eq!(e.error_class, ErrorClass::Policy);
}

#[test]
fn efm_rate_limit_spaced() {
    let e = efm("rate limit hit for mailbox:inbox");
    assert_eq!(e.error_code, "RATE_LIMITED");
    assert_eq!(e.error_class, ErrorClass::Policy);
}

#[test]
fn efm_rate_limit_hyphenated() {
    let e = efm("rate-limit window closed");
    assert_eq!(e.error_code, "RATE_LIMITED");
}

#[test]
fn efm_blocked_colon_is_url_policy() {
    let e = efm("blocked: destination not on allowlist");
    assert_eq!(e.error_code, "URL_POLICY");
    assert_eq!(e.error_class, ErrorClass::Policy);
}

#[test]
fn efm_url_policy_keyword() {
    let e = efm("request denied by url policy");
    assert_eq!(e.error_code, "URL_POLICY");
}

#[test]
fn efm_unknown_handler_is_configuration() {
    let e = efm("unknown handler: charge_card_v9");
    assert_eq!(e.error_code, "CONFIGURATION");
    assert_eq!(e.error_class, ErrorClass::Configuration);
}

#[test]
fn efm_unknown_template_root_is_configuration() {
    let e = efm("unknown template root: bogus");
    assert_eq!(e.error_code, "CONFIGURATION");
}

#[test]
fn efm_template_alone_is_configuration() {
    let e = efm("template rendering blew up");
    assert_eq!(e.error_code, "CONFIGURATION");
}

#[test]
fn efm_invalid_params_is_configuration() {
    let e = efm("invalid params for step charge");
    assert_eq!(e.error_code, "CONFIGURATION");
}

#[test]
fn efm_schema_is_configuration() {
    let e = efm("output does not match schema");
    assert_eq!(e.error_code, "CONFIGURATION");
}

#[test]
fn efm_cancelled() {
    let e = efm("cancelled by operator");
    assert_eq!(e.error_code, "CANCELLED");
    assert_eq!(e.error_class, ErrorClass::Cancelled);
}

#[test]
fn efm_cancellation_variant() {
    let e = efm("cancellation requested upstream");
    assert_eq!(e.error_code, "CANCELLED");
}

#[test]
fn efm_dns_is_transport() {
    let e = efm("dns lookup for api.example.dev did not resolve");
    assert_eq!(e.error_code, "TRANSPORT");
    assert_eq!(e.error_class, ErrorClass::ExternalDependency);
}

#[test]
fn efm_connection_refused_is_transport() {
    let e = efm("connection refused by peer");
    assert_eq!(e.error_code, "TRANSPORT");
}

#[test]
fn efm_tls_is_transport() {
    let e = efm("tls handshake with upstream broke");
    assert_eq!(e.error_code, "TRANSPORT");
}

#[test]
fn efm_fallthrough_is_unknown_application() {
    let e = efm("the widget frobnicated itself");
    assert_eq!(e.error_code, "unknown");
    assert_eq!(e.error_class, ErrorClass::Application);
}

#[test]
fn efm_empty_message_is_unknown() {
    let e = efm("");
    assert_eq!(e.error_code, "unknown");
    assert_eq!(e.error_class, ErrorClass::Application);
}

#[test]
fn efm_keywords_are_case_insensitive() {
    let e = efm("REQUEST TIMED OUT");
    assert_eq!(e.error_code, "TIMEOUT");
}

// =====================================================================
// envelope_from_message: precedence + passthrough
// =====================================================================

#[test]
fn efm_timeout_beats_credential() {
    let e = efm("credential check timed out");
    assert_eq!(e.error_code, "TIMEOUT");
}

#[test]
fn efm_credential_beats_no_worker() {
    let e = efm("no worker holds this credential");
    assert_eq!(e.error_code, "CREDENTIAL");
}

#[test]
fn efm_budget_beats_rate_limit() {
    let e = efm("budget gone; also rate limit hit");
    assert_eq!(e.error_code, "BUDGET_EXCEEDED");
}

#[test]
fn efm_configuration_beats_cancel() {
    // "schema" is matched before "cancel" in the heuristic chain.
    let e = efm("cancelled while validating schema");
    assert_eq!(e.error_code, "CONFIGURATION");
}

#[test]
fn efm_cancel_beats_transport() {
    let e = efm("cancelled: connect was reset");
    assert_eq!(e.error_code, "CANCELLED");
}

#[test]
fn efm_message_kept_verbatim() {
    let msg = "Weird MIXED case with 12345 and  double  spaces";
    assert_eq!(efm(msg).message, msg);
}

#[test]
fn efm_retryable_true_passthrough() {
    assert!(envelope_from_message("timeout", true, t0()).retryable);
}

#[test]
fn efm_retryable_false_passthrough() {
    assert!(!envelope_from_message("timeout", false, t0()).retryable);
}

#[test]
fn efm_occurred_at_passthrough() {
    assert_eq!(
        envelope_from_message("timeout", false, t1()).occurred_at,
        t1()
    );
}

#[test]
fn efm_keyword_branch_leaves_optionals_none() {
    let e = efm("rate limit hit");
    assert!(e.block_id.is_none());
    assert!(e.handler.is_none());
    assert!(e.external_status.is_none());
    assert!(e.resource.is_none());
    assert!(e.details.is_none());
}

#[test]
fn efm_http_branch_sets_only_external_status() {
    let e = efm("http 503");
    assert!(e.external_status.is_some());
    assert!(e.block_id.is_none());
    assert!(e.handler.is_none());
    assert!(e.resource.is_none());
}

#[test]
fn efm_unknown_envelopes_fingerprint_by_normalized_message() {
    let a = efm("mystery failure in zone 12");
    let b = efm("mystery failure in zone 99");
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
    let c = efm("entirely different mystery");
    assert_ne!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &c).hash
    );
}

#[test]
fn efm_http_status_envelopes_group_regardless_of_request_id() {
    let a = efm("http 503 (req-11111111)");
    let b = efm("http 503 (req-22222222)");
    assert_eq!(
        fingerprint(&scope(), &a).hash,
        fingerprint(&scope(), &b).hash
    );
}

// =====================================================================
// DlqRetryMode serde
// =====================================================================

#[test]
fn retry_mode_deserializes_sample() {
    let m: DlqRetryMode = serde_json::from_str("\"sample\"").unwrap();
    assert_eq!(m, DlqRetryMode::Sample);
}

#[test]
fn retry_mode_deserializes_bulk() {
    let m: DlqRetryMode = serde_json::from_str("\"bulk\"").unwrap();
    assert_eq!(m, DlqRetryMode::Bulk);
}

#[test]
fn retry_mode_round_trips() {
    for m in [DlqRetryMode::Sample, DlqRetryMode::Bulk] {
        let back: DlqRetryMode = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(back, m);
    }
}

#[test]
fn retry_mode_rejects_unknown_value() {
    let r: Result<DlqRetryMode, _> = serde_json::from_str("\"yolo\"");
    assert!(r.is_err());
}

#[test]
fn retry_mode_rejects_pascal_case() {
    let r: Result<DlqRetryMode, _> = serde_json::from_str("\"Sample\"");
    assert!(r.is_err());
}

// =====================================================================
// DlqGroupRetryRequest
// =====================================================================

#[test]
fn retry_request_bulk_defaults() {
    let req: DlqGroupRetryRequest = serde_json::from_value(json!({"mode": "bulk"})).unwrap();
    assert_eq!(req.mode, DlqRetryMode::Bulk);
    assert!(!req.force);
    assert!(req.sample_verified_instance_id.is_none());
    assert!(req.limit.is_none());
}

#[test]
fn retry_request_parses_all_fields() {
    let id = Uuid::now_v7();
    let req: DlqGroupRetryRequest = serde_json::from_value(json!({
        "mode": "bulk",
        "sample_verified_instance_id": id,
        "force": true,
        "limit": 250,
    }))
    .unwrap();
    assert_eq!(req.mode, DlqRetryMode::Bulk);
    assert_eq!(req.sample_verified_instance_id, Some(id));
    assert!(req.force);
    assert_eq!(req.limit, Some(250));
}

#[test]
fn retry_request_requires_mode() {
    let r: Result<DlqGroupRetryRequest, _> = serde_json::from_value(json!({"force": true}));
    assert!(r.is_err());
}

#[test]
fn retry_request_rejects_negative_limit() {
    let r: Result<DlqGroupRetryRequest, _> =
        serde_json::from_value(json!({"mode": "bulk", "limit": -1}));
    assert!(r.is_err());
}

#[test]
fn retry_request_accepts_limit_zero() {
    let req: DlqGroupRetryRequest =
        serde_json::from_value(json!({"mode": "bulk", "limit": 0})).unwrap();
    assert_eq!(req.limit, Some(0));
}

#[test]
fn retry_request_rejects_malformed_sample_id() {
    let r: Result<DlqGroupRetryRequest, _> = serde_json::from_value(json!({
        "mode": "bulk",
        "sample_verified_instance_id": "not-a-uuid",
    }));
    assert!(r.is_err());
}

// =====================================================================
// DlqGroupRetryResponse + DlqGroup serde
// =====================================================================

#[test]
fn retry_response_round_trips() {
    let resp = DlqGroupRetryResponse {
        fingerprint: "00ff00ff00ff00ff".into(),
        mode: DlqRetryMode::Bulk,
        retried: vec![Uuid::now_v7(), Uuid::now_v7()],
        skipped: 3,
    };
    let back: DlqGroupRetryResponse =
        serde_json::from_str(&serde_json::to_string(&resp).unwrap()).unwrap();
    assert_eq!(back.fingerprint, resp.fingerprint);
    assert_eq!(back.mode, resp.mode);
    assert_eq!(back.retried, resp.retried);
    assert_eq!(back.skipped, resp.skipped);
}

#[test]
fn retry_response_mode_serialized_snake_case() {
    let resp = DlqGroupRetryResponse {
        fingerprint: "x".into(),
        mode: DlqRetryMode::Sample,
        retried: vec![],
        skipped: 0,
    };
    let v = serde_json::to_value(&resp).unwrap();
    assert_eq!(v["mode"], "sample");
}

fn sample_group() -> DlqGroup {
    DlqGroup {
        fingerprint: "deadbeefcafef00d".into(),
        components: vec![
            "seq:s".into(),
            "v:1".into(),
            "class:timeout".into(),
            "code:TIMEOUT".into(),
        ],
        error_class: ErrorClass::Timeout,
        error_code: "TIMEOUT".into(),
        sample_message: "request timed out".into(),
        count: 42,
        first_occurrence: t0(),
        last_occurrence: t1(),
        sequences: vec!["3f1c".into()],
        blocks: vec!["charge".into()],
        handlers: vec!["http_request".into()],
        sample_instance_ids: vec![Uuid::now_v7()],
    }
}

#[test]
fn dlq_group_round_trips() {
    let g = sample_group();
    let back: DlqGroup = serde_json::from_str(&serde_json::to_string(&g).unwrap()).unwrap();
    assert_eq!(back, g);
}

#[test]
fn dlq_group_round_trips_with_empty_lists() {
    let mut g = sample_group();
    g.sequences = vec![];
    g.blocks = vec![];
    g.handlers = vec![];
    g.sample_instance_ids = vec![];
    let back: DlqGroup = serde_json::from_str(&serde_json::to_string(&g).unwrap()).unwrap();
    assert_eq!(back, g);
}

#[test]
fn dlq_group_json_field_names_are_stable() {
    let v = serde_json::to_value(sample_group()).unwrap();
    let obj = v.as_object().unwrap();
    for key in [
        "fingerprint",
        "components",
        "error_class",
        "error_code",
        "sample_message",
        "count",
        "first_occurrence",
        "last_occurrence",
        "sequences",
        "blocks",
        "handlers",
        "sample_instance_ids",
    ] {
        assert!(obj.contains_key(key), "missing key {key}");
    }
}

#[test]
fn dlq_group_error_class_snake_case_in_json() {
    let v = serde_json::to_value(sample_group()).unwrap();
    assert_eq!(v["error_class"], "timeout");
}

#[test]
fn dlq_group_occurrences_round_trip_exactly() {
    let g = sample_group();
    let back: DlqGroup = serde_json::from_str(&serde_json::to_string(&g).unwrap()).unwrap();
    assert_eq!(back.first_occurrence, t0());
    assert_eq!(back.last_occurrence, t1());
}
