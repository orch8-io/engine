//! E2E: Template Debugger (Feature #25)
//!
//! Exercises the `debug_template` function end-to-end with various
//! template shapes, fallback chains, pipe filters, and error conditions.

use serde_json::json;

use orch8_engine::template_trace::debug_template;
use orch8_types::context::ExecutionContext;
use orch8_types::redaction::RedactionPolicy;
use orch8_types::template_trace::ResolutionStatus;

fn ctx(data: serde_json::Value, config: serde_json::Value) -> ExecutionContext {
    ExecutionContext {
        data,
        config,
        ..Default::default()
    }
}

fn dbg(
    template: &str,
    data: serde_json::Value,
    outputs: &serde_json::Value,
) -> orch8_types::template_trace::DebugTemplateResponse {
    debug_template(
        template,
        &ctx(data, json!({})),
        outputs,
        &RedactionPolicy::default(),
    )
}

// ------------------------------------------------------------------
// Basic resolution
// ------------------------------------------------------------------

#[test]
fn resolves_context_data_string() {
    let r = dbg(
        "{{ context.data.name }}",
        json!({"name": "Alice"}),
        &json!({}),
    );
    assert!(r.error.is_none());
    assert_eq!(r.result, Some(json!("Alice")));
    assert_eq!(r.result_type.as_deref(), Some("string"));
    assert_eq!(r.entries.len(), 1);
    assert_eq!(r.entries[0].status, ResolutionStatus::Ok);
    assert_eq!(r.entries[0].source.as_deref(), Some("context.data.name"));
}

#[test]
fn resolves_numeric_value() {
    let r = dbg(
        "{{ outputs.calc.total }}",
        json!({}),
        &json!({"calc": {"total": 42}}),
    );
    assert_eq!(r.result, Some(json!(42)));
    assert_eq!(r.result_type.as_deref(), Some("number"));
}

#[test]
fn resolves_boolean_value() {
    let r = dbg(
        "{{ context.data.active }}",
        json!({"active": true}),
        &json!({}),
    );
    assert_eq!(r.result, Some(json!(true)));
    assert_eq!(r.result_type.as_deref(), Some("boolean"));
}

#[test]
fn resolves_object_value() {
    let r = dbg(
        "{{ context.data.user }}",
        json!({"user": {"id": 1, "name": "X"}}),
        &json!({}),
    );
    assert_eq!(r.result, Some(json!({"id": 1, "name": "X"})));
    assert_eq!(r.result_type.as_deref(), Some("object"));
}

#[test]
fn resolves_array_value() {
    let r = dbg(
        "{{ context.data.tags }}",
        json!({"tags": ["a", "b"]}),
        &json!({}),
    );
    assert_eq!(r.result, Some(json!(["a", "b"])));
    assert_eq!(r.result_type.as_deref(), Some("array"));
}

// ------------------------------------------------------------------
// Inline interpolation
// ------------------------------------------------------------------

#[test]
fn inline_interpolation_produces_string() {
    let r = dbg(
        "Hello {{ context.data.name }}, you have {{ outputs.count.n }} items",
        json!({"name": "Bob"}),
        &json!({"count": {"n": 5}}),
    );
    assert_eq!(r.result, Some(json!("Hello Bob, you have 5 items")));
    assert_eq!(r.result_type.as_deref(), Some("string"));
    assert_eq!(r.entries.len(), 2);
}

// ------------------------------------------------------------------
// Fallback chain
// ------------------------------------------------------------------

#[test]
fn fallback_fires_when_primary_missing() {
    let r = dbg(
        "{{ context.data.primary | context.data.secondary }}",
        json!({"secondary": "backup"}),
        &json!({}),
    );
    assert_eq!(r.result, Some(json!("backup")));
    let e = &r.entries[0];
    assert!(e.fallback_used);
    assert_eq!(e.source.as_deref(), Some("context.data.secondary"));
}

#[test]
fn literal_fallback() {
    let r = dbg(
        "{{ context.data.absent | default-val }}",
        json!({}),
        &json!({}),
    );
    assert_eq!(r.result, Some(json!("default-val")));
    let e = &r.entries[0];
    assert!(e.fallback_used);
    assert_eq!(e.source.as_deref(), Some("literal"));
}

// ------------------------------------------------------------------
// Missing / null distinction
// ------------------------------------------------------------------

#[test]
fn missing_path_is_missing_status() {
    let r = dbg("{{ context.data.nope }}", json!({}), &json!({}));
    assert_eq!(r.entries[0].status, ResolutionStatus::Missing);
}

#[test]
fn explicit_null_is_null_status() {
    let r = dbg("{{ context.data.val }}", json!({"val": null}), &json!({}));
    assert_eq!(r.entries[0].status, ResolutionStatus::Null);
}

// ------------------------------------------------------------------
// Pipe filters
// ------------------------------------------------------------------

#[test]
fn upper_filter() {
    let r = dbg(
        "{{ context.data.name | upper }}",
        json!({"name": "alice"}),
        &json!({}),
    );
    assert_eq!(r.result, Some(json!("ALICE")));
}

#[test]
fn lower_filter() {
    let r = dbg(
        "{{ context.data.name | lower }}",
        json!({"name": "ALICE"}),
        &json!({}),
    );
    assert_eq!(r.result, Some(json!("alice")));
}

// ------------------------------------------------------------------
// Config references
// ------------------------------------------------------------------

#[test]
fn resolves_config_reference() {
    let r = debug_template(
        "{{ context.config.region }}",
        &ctx(json!({}), json!({"region": "eu-west"})),
        &json!({}),
        &RedactionPolicy::default(),
    );
    assert_eq!(r.result, Some(json!("eu-west")));
}

// ------------------------------------------------------------------
// Error cases
// ------------------------------------------------------------------

#[test]
fn unknown_root_returns_error() {
    let r = dbg("{{ bogus.path }}", json!({}), &json!({}));
    assert!(r.error.is_some());
    assert!(r.result.is_none());
    assert!(r.result_type.is_none());
}

// ------------------------------------------------------------------
// Plain text (no template)
// ------------------------------------------------------------------

#[test]
fn plain_text_returns_literal() {
    let r = dbg("just plain text", json!({}), &json!({}));
    assert_eq!(r.result, Some(json!("just plain text")));
    assert!(r.entries.is_empty());
    assert!(r.error.is_none());
}

// ------------------------------------------------------------------
// Redaction
// ------------------------------------------------------------------

#[test]
fn secret_shaped_values_are_redacted() {
    let r = dbg(
        "{{ context.data.tok }}",
        json!({"tok": "sk_live_abc123"}),
        &json!({}),
    );
    assert_eq!(r.entries[0].status, ResolutionStatus::Redacted);
    assert_eq!(r.result, Some(json!("[REDACTED]")));
}

// ------------------------------------------------------------------
// Output references
// ------------------------------------------------------------------

#[test]
fn resolves_output_reference() {
    let r = dbg(
        "{{ outputs.charge.amount }}",
        json!({}),
        &json!({"charge": {"amount": 100}}),
    );
    assert_eq!(r.result, Some(json!(100)));
    assert_eq!(
        r.entries[0].source.as_deref(),
        Some("outputs.charge.amount")
    );
}

// ------------------------------------------------------------------
// Multiple expressions
// ------------------------------------------------------------------

#[test]
fn multiple_expressions_each_traced() {
    let r = dbg(
        "{{ context.data.a }} and {{ context.data.b }}",
        json!({"a": 1, "b": 2}),
        &json!({}),
    );
    assert_eq!(r.entries.len(), 2);
    assert_eq!(r.entries[0].expression, "context.data.a");
    assert_eq!(r.entries[1].expression, "context.data.b");
}

// ------------------------------------------------------------------
// Empty inputs
// ------------------------------------------------------------------

#[test]
fn empty_string_template() {
    let r = dbg("", json!({}), &json!({}));
    assert_eq!(r.result, Some(json!("")));
    assert!(r.entries.is_empty());
}
