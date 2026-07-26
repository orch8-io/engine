//! Coverage tests for the redacted support-bundle exporter.
//!
//! Count contract: 51 independently named unit tests.

use super::*;

macro_rules! sensitive_key_case {
    ($name:ident, $key:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(sensitive_key($key), $expected);
        }
    };
}

// Every needle in the redaction list must match as a bare key.
sensitive_key_case!(
    coverage_bundle_001_context_key_is_sensitive,
    "context",
    true
);
sensitive_key_case!(
    coverage_bundle_002_payload_key_is_sensitive,
    "payload",
    true
);
sensitive_key_case!(coverage_bundle_003_output_key_is_sensitive, "output", true);
sensitive_key_case!(coverage_bundle_004_params_key_is_sensitive, "params", true);
sensitive_key_case!(
    coverage_bundle_005_credential_key_is_sensitive,
    "credential",
    true
);
sensitive_key_case!(coverage_bundle_006_secret_key_is_sensitive, "secret", true);
sensitive_key_case!(
    coverage_bundle_007_password_key_is_sensitive,
    "password",
    true
);
sensitive_key_case!(coverage_bundle_008_token_key_is_sensitive, "token", true);
sensitive_key_case!(coverage_bundle_009_api_key_is_sensitive, "api_key", true);
sensitive_key_case!(
    coverage_bundle_010_encryption_key_is_sensitive,
    "encryption_key",
    true
);
sensitive_key_case!(
    coverage_bundle_011_private_key_is_sensitive,
    "private_key",
    true
);
sensitive_key_case!(
    coverage_bundle_012_database_url_is_sensitive,
    "database_url",
    true
);

// Matching is case-insensitive and substring-based, like real API payloads.
sensitive_key_case!(
    coverage_bundle_013_uppercase_api_key_is_sensitive,
    "API_KEY",
    true
);
sensitive_key_case!(
    coverage_bundle_014_mixed_case_password_is_sensitive,
    "Password",
    true
);
sensitive_key_case!(
    coverage_bundle_015_payload_json_is_sensitive,
    "payload_json",
    true
);
sensitive_key_case!(
    coverage_bundle_016_bearer_token_is_sensitive,
    "bearer_token",
    true
);
sensitive_key_case!(
    coverage_bundle_017_prefixed_database_url_is_sensitive,
    "db_database_url",
    true
);
sensitive_key_case!(
    coverage_bundle_018_step_output_json_is_sensitive,
    "step_output_json",
    true
);
sensitive_key_case!(
    coverage_bundle_019_aws_secret_access_key_is_sensitive,
    "aws_secret_access_key",
    true
);

// Operational metadata keys must survive redaction.
sensitive_key_case!(coverage_bundle_020_id_key_is_not_sensitive, "id", false);
sensitive_key_case!(
    coverage_bundle_021_state_key_is_not_sensitive,
    "state",
    false
);
sensitive_key_case!(
    coverage_bundle_022_priority_key_is_not_sensitive,
    "priority",
    false
);
sensitive_key_case!(coverage_bundle_023_name_key_is_not_sensitive, "name", false);
sensitive_key_case!(
    coverage_bundle_024_version_key_is_not_sensitive,
    "version",
    false
);
sensitive_key_case!(
    coverage_bundle_025_queue_key_is_not_sensitive,
    "queue",
    false
);
// Bare "key" is not in the needle list — only compound secret keys are.
sensitive_key_case!(coverage_bundle_026_bare_key_is_not_sensitive, "key", false);
sensitive_key_case!(
    coverage_bundle_027_tenant_id_is_not_sensitive,
    "tenant_id",
    false
);
sensitive_key_case!(coverage_bundle_028_bare_url_is_not_sensitive, "url", false);
// "parameters" does not contain the "params" substring.
sensitive_key_case!(
    coverage_bundle_029_parameters_is_not_sensitive,
    "parameters",
    false
);
// "monkey" ends in "key" but contains no needle.
sensitive_key_case!(coverage_bundle_030_monkey_is_not_sensitive, "monkey", false);

#[test]
fn coverage_bundle_031_sensitive_scalar_is_replaced_with_redacted_marker() {
    let sanitized = sanitize(json!({"api_key": "sk-live-123"}));
    assert_eq!(sanitized["api_key"], Value::String("[REDACTED]".into()));
}

#[test]
fn coverage_bundle_032_sensitive_subtree_is_redacted_wholesale() {
    let sanitized = sanitize(json!({
        "context": {"customer": "alice", "deep": {"ssn": "000-00-0000"}}
    }));
    assert_eq!(sanitized["context"], Value::String("[REDACTED]".into()));
    let rendered = sanitized.to_string();
    assert!(!rendered.contains("alice"));
    assert!(!rendered.contains("000-00-0000"));
}

#[test]
fn coverage_bundle_033_arrays_recurse_and_sanitize_elements() {
    let sanitized = sanitize(json!({
        "events": [
            {"token": "abc", "state": "ok"},
            {"state": "done"}
        ]
    }));
    assert_eq!(sanitized["events"][0]["token"], json!("[REDACTED]"));
    assert_eq!(sanitized["events"][0]["state"], json!("ok"));
    assert_eq!(sanitized["events"][1]["state"], json!("done"));
}

#[test]
fn coverage_bundle_034_non_sensitive_scalars_are_preserved() {
    let sanitized = sanitize(json!({
        "name": "billing",
        "count": 42,
        "ratio": 1.5,
        "active": true,
        "missing": null
    }));
    assert_eq!(sanitized["name"], json!("billing"));
    assert_eq!(sanitized["count"], json!(42));
    assert_eq!(sanitized["ratio"], json!(1.5));
    assert_eq!(sanitized["active"], json!(true));
    assert_eq!(sanitized["missing"], Value::Null);
}

#[test]
fn coverage_bundle_035_uppercase_secret_keys_are_redacted() {
    let sanitized = sanitize(json!({"PASSWORD": "hunter2", "TOKEN": "tok"}));
    assert_eq!(sanitized["PASSWORD"], json!("[REDACTED]"));
    assert_eq!(sanitized["TOKEN"], json!("[REDACTED]"));
    assert!(!sanitized.to_string().contains("hunter2"));
}

#[test]
fn coverage_bundle_036_sibling_structure_is_preserved() {
    let sanitized = sanitize(json!({
        "instance": {
            "id": "i1",
            "secret": "x",
            "nested": {"safe": 1, "credential": "y"}
        }
    }));
    assert_eq!(sanitized["instance"]["id"], json!("i1"));
    assert_eq!(sanitized["instance"]["secret"], json!("[REDACTED]"));
    assert_eq!(sanitized["instance"]["nested"]["safe"], json!(1));
    assert_eq!(
        sanitized["instance"]["nested"]["credential"],
        json!("[REDACTED]")
    );
}

#[test]
fn coverage_bundle_037_deeply_nested_arrays_and_objects_are_reached() {
    let sanitized = sanitize(json!({
        "a": [{"b": [{"private_key": "pem", "v": 3}]}]
    }));
    assert_eq!(
        sanitized["a"][0]["b"][0]["private_key"],
        json!("[REDACTED]")
    );
    assert_eq!(sanitized["a"][0]["b"][0]["v"], json!(3));
}

#[test]
fn coverage_bundle_038_empty_containers_pass_through() {
    assert_eq!(sanitize(json!({})), json!({}));
    assert_eq!(sanitize(json!([])), json!([]));
}

#[test]
fn coverage_bundle_039_top_level_scalars_pass_through() {
    assert_eq!(sanitize(json!("plain")), json!("plain"));
    assert_eq!(sanitize(json!(7)), json!(7));
    assert_eq!(sanitize(Value::Null), Value::Null);
}

#[test]
fn coverage_bundle_040_realistic_instance_payload_leaks_nothing() {
    let sanitized = sanitize(json!({
        "id": "inst-1",
        "state": "failed",
        "context": {"data": {"email": "user@example.com"}},
        "steps": [{"id": "charge", "params": {"card": "4242"}, "output": {"receipt": "r1"}}],
        "error": "card declined"
    }));
    let rendered = sanitized.to_string();
    assert!(rendered.contains("inst-1"));
    assert!(rendered.contains("failed"));
    assert!(rendered.contains("card declined"));
    assert!(!rendered.contains("user@example.com"));
    assert!(!rendered.contains("4242"));
    assert!(!rendered.contains("receipt"));
}

#[test]
fn coverage_bundle_041_items_array_is_the_preferred_source() {
    let source = json!({"items": [{"id": "i1", "state": "running"}], "total": 99});
    let summaries = workload_summaries(&source, 10);
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["id"], json!("i1"));
}

#[test]
fn coverage_bundle_042_top_level_array_is_a_fallback_source() {
    let source = json!([{"id": "i1", "state": "paused"}]);
    let summaries = workload_summaries(&source, 10);
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["state"], json!("paused"));
}

#[test]
fn coverage_bundle_043_non_array_items_and_object_body_yield_nothing() {
    let source = json!({"items": "not-an-array"});
    assert!(workload_summaries(&source, 10).is_empty());
}

#[test]
fn coverage_bundle_044_non_object_entries_are_filtered_out() {
    let source = json!([{"id": "i1"}, "junk", 7, null, {"id": "i2"}]);
    let summaries = workload_summaries(&source, 10);
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0]["id"], json!("i1"));
    assert_eq!(summaries[1]["id"], json!("i2"));
}

#[test]
fn coverage_bundle_045_zero_limit_yields_nothing() {
    let source = json!([{"id": "i1"}]);
    assert!(workload_summaries(&source, 0).is_empty());
}

#[test]
fn coverage_bundle_046_limit_truncates_in_order() {
    let source = json!([{"id": "i1"}, {"id": "i2"}, {"id": "i3"}]);
    let summaries = workload_summaries(&source, 2);
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0]["id"], json!("i1"));
    assert_eq!(summaries[1]["id"], json!("i2"));
}

#[test]
fn coverage_bundle_047_every_allowlisted_key_is_preserved() {
    let source = json!([{
        "id": "i1",
        "sequence_id": "s1",
        "state": "running",
        "priority": 3,
        "created_at": "2026-07-25T00:00:00Z",
        "updated_at": "2026-07-25T01:00:00Z",
        "next_fire_at": "2026-07-25T02:00:00Z"
    }]);
    let summary = &workload_summaries(&source, 10)[0];
    for key in [
        "id",
        "sequence_id",
        "state",
        "priority",
        "created_at",
        "updated_at",
        "next_fire_at",
    ] {
        assert!(summary.get(key).is_some(), "allowlisted key {key} missing");
    }
    assert_eq!(summary["priority"], json!(3));
}

#[test]
fn coverage_bundle_048_non_allowlisted_keys_are_dropped() {
    let source = json!([{
        "id": "i1",
        "context": {"secret": true},
        "metadata": {"pii": 1},
        "payload": "x",
        "error": "boom"
    }]);
    let summary = &workload_summaries(&source, 10)[0];
    for key in ["context", "metadata", "payload", "error"] {
        assert!(summary.get(key).is_none(), "key {key} must be dropped");
    }
    assert_eq!(summary["id"], json!("i1"));
}

#[test]
fn coverage_bundle_049_missing_allowlist_keys_are_absent_not_null() {
    let source = json!([{"id": "i1", "state": "running"}]);
    let summary = &workload_summaries(&source, 10)[0];
    assert!(summary.get("priority").is_none());
    assert!(summary.get("next_fire_at").is_none());
}

#[test]
fn coverage_bundle_050_empty_input_yields_no_summaries() {
    assert!(workload_summaries(&json!({"items": []}), 10).is_empty());
    assert!(workload_summaries(&Value::Null, 10).is_empty());
    assert!(workload_summaries(&json!({"other": 1}), 10).is_empty());
}

#[test]
fn coverage_bundle_051_empty_object_entry_yields_empty_summary() {
    // An object entry passes the filter even when it carries no allowlisted
    // keys — it must surface as an empty summary, not be dropped or nulled.
    let source = json!([{}, {"id": "i1"}]);
    let summaries = workload_summaries(&source, 10);
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0], json!({}));
    assert_eq!(summaries[1], json!({"id": "i1"}));
}
