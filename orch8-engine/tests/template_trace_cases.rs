//! Extensive unit tests for the Template Resolution Inspector's trace
//! evaluation: `orch8_engine::template_trace::trace_params` plus the
//! `orch8_types::template_trace` types it produces.
//!
//! Organised by concern:
//! 1. status per root (ok / null / missing / redacted / error)
//! 2. fallback-chain provenance (source segment, `fallback_used`)
//! 3. string coercion flags for inline interpolation
//! 4. `result_type` for every JSON type
//! 5. nested param paths
//! 6. multiple expressions per string / unbalanced braces
//! 7. redaction (by leaf name, by value shape, aggregate)
//! 8. state-root resolution
//! 9. pipe filters visible in traced values
//! 10. error entries

use orch8_engine::template_trace::trace_params;
use orch8_types::context::ExecutionContext;
use orch8_types::redaction::{REDACTED, RedactionPolicy};
use orch8_types::template_trace::{ResolutionEntry, ResolutionStatus, ResolutionTrace};
use serde_json::{Value, json};

// ---------------------------------------------------------------- helpers

fn ctx(data: Value) -> ExecutionContext {
    ExecutionContext {
        data,
        ..Default::default()
    }
}

fn ctx_cfg(data: Value, config: Value) -> ExecutionContext {
    ExecutionContext {
        data,
        config,
        ..Default::default()
    }
}

/// Trace `params` against `context.data` + `outputs` (no config, no state).
#[allow(clippy::needless_pass_by_value)]
fn trace(params: Value, data: Value, outputs: Value) -> ResolutionTrace {
    trace_params(
        "blk",
        &params,
        &ctx(data),
        &outputs,
        None,
        &RedactionPolicy::default(),
    )
}

/// Trace against a config fixture only.
#[allow(clippy::needless_pass_by_value)]
fn trace_cfg(params: Value, config: Value) -> ResolutionTrace {
    trace_params(
        "blk",
        &params,
        &ctx_cfg(json!({}), config),
        &json!({}),
        None,
        &RedactionPolicy::default(),
    )
}

/// Trace against a state fixture (`None` = no state map at all).
#[allow(clippy::needless_pass_by_value)]
fn trace_state(params: Value, data: Value, state: Option<&Value>) -> ResolutionTrace {
    trace_params(
        "blk",
        &params,
        &ctx(data),
        &json!({}),
        state,
        &RedactionPolicy::default(),
    )
}

fn entry<'a>(t: &'a ResolutionTrace, path: &str) -> &'a ResolutionEntry {
    t.entries
        .iter()
        .find(|e| e.param_path == path)
        .unwrap_or_else(|| panic!("no entry for {path}: {:#?}", t.entries))
}

/// Trace a single param `v` and return its (single) entry.
fn one(template: &str, data: Value, outputs: Value) -> ResolutionEntry {
    let t = trace(json!({ "v": template }), data, outputs);
    entry(&t, "v").clone()
}

// =====================================================================
// 1. Status per root
// =====================================================================

// ---- context.data ----

#[test]
fn context_data_ok() {
    let e = one("{{ context.data.name }}", json!({"name": "ada"}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("ada")));
    assert_eq!(e.source.as_deref(), Some("context.data.name"));
    assert!(!e.fallback_used);
}

#[test]
fn context_data_explicit_null() {
    let e = one("{{ context.data.name }}", json!({"name": null}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Null);
    assert_eq!(e.value, Some(Value::Null));
    assert_eq!(e.result_type.as_deref(), Some("null"));
}

#[test]
fn context_data_missing() {
    let e = one("{{ context.data.nope }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Missing);
    assert_eq!(e.value, Some(Value::Null));
    assert_eq!(e.source, None);
}

#[test]
fn context_data_deep_missing_intermediate() {
    let e = one(
        "{{ context.data.a.b.c }}",
        json!({"a": {"x": 1}}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Missing);
}

#[test]
fn context_data_path_through_scalar_is_missing() {
    let e = one("{{ context.data.n.field }}", json!({"n": 5}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Missing);
}

// ---- context.config ----

#[test]
fn context_config_ok() {
    let t = trace_cfg(
        json!({"v": "{{ context.config.region }}"}),
        json!({"region": "eu"}),
    );
    let e = entry(&t, "v");
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("eu")));
    assert_eq!(e.source.as_deref(), Some("context.config.region"));
}

#[test]
fn context_config_explicit_null() {
    let t = trace_cfg(
        json!({"v": "{{ context.config.region }}"}),
        json!({"region": null}),
    );
    assert_eq!(entry(&t, "v").status, ResolutionStatus::Null);
}

#[test]
fn context_config_missing() {
    let t = trace_cfg(json!({"v": "{{ context.config.absent }}"}), json!({}));
    assert_eq!(entry(&t, "v").status, ResolutionStatus::Missing);
}

#[test]
fn context_config_redacted_by_leaf_name() {
    let t = trace_cfg(
        json!({"token": "{{ context.config.value }}"}),
        json!({"value": "plain-but-lands-in-token"}),
    );
    let e = entry(&t, "token");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

// ---- outputs ----

#[test]
fn outputs_ok() {
    let e = one(
        "{{ outputs.charge.amount }}",
        json!({}),
        json!({"charge": {"amount": 42}}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!(42)));
    assert_eq!(e.source.as_deref(), Some("outputs.charge.amount"));
}

#[test]
fn outputs_explicit_null_field() {
    let e = one(
        "{{ outputs.charge.receipt }}",
        json!({}),
        json!({"charge": {"receipt": null}}),
    );
    assert_eq!(e.status, ResolutionStatus::Null);
}

#[test]
fn outputs_missing_block() {
    let e = one("{{ outputs.ghost.amount }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Missing);
    assert_eq!(e.value, Some(Value::Null));
}

#[test]
fn outputs_missing_field_on_existing_block() {
    let e = one(
        "{{ outputs.charge.nope }}",
        json!({}),
        json!({"charge": {"amount": 42}}),
    );
    assert_eq!(e.status, ResolutionStatus::Missing);
}

#[test]
fn outputs_whole_block_object() {
    let e = one(
        "{{ outputs.charge }}",
        json!({}),
        json!({"charge": {"amount": 42, "ok": true}}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!({"amount": 42, "ok": true})));
    assert_eq!(e.result_type.as_deref(), Some("object"));
}

#[test]
fn outputs_redacted_by_leaf_name() {
    let t = trace(
        json!({"authorization": "{{ outputs.login.header }}"}),
        json!({}),
        json!({"login": {"header": "Basic abc"}}),
    );
    let e = entry(&t, "authorization");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

#[test]
fn steps_alias_resolves_like_outputs() {
    let e = one(
        "{{ steps.charge.amount }}",
        json!({}),
        json!({"charge": {"amount": 7}}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!(7)));
}

// ---- state ----

#[test]
fn state_ok() {
    let state = json!({"cursor": "abc"});
    let t = trace_state(json!({"v": "{{ state.cursor }}"}), json!({}), Some(&state));
    let e = entry(&t, "v");
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("abc")));
    assert_eq!(e.source.as_deref(), Some("state.cursor"));
}

#[test]
fn state_explicit_null() {
    let state = json!({"cursor": null});
    let t = trace_state(json!({"v": "{{ state.cursor }}"}), json!({}), Some(&state));
    assert_eq!(entry(&t, "v").status, ResolutionStatus::Null);
}

#[test]
fn state_missing_key() {
    let state = json!({"other": 1});
    let t = trace_state(json!({"v": "{{ state.cursor }}"}), json!({}), Some(&state));
    assert_eq!(entry(&t, "v").status, ResolutionStatus::Missing);
}

#[test]
fn state_key_with_none_state_is_missing() {
    let t = trace_state(json!({"v": "{{ state.cursor }}"}), json!({}), None);
    assert_eq!(entry(&t, "v").status, ResolutionStatus::Missing);
}

#[test]
fn whole_state_root_with_some_state() {
    let state = json!({"a": 1});
    let t = trace_state(json!({"v": "{{ state }}"}), json!({}), Some(&state));
    let e = entry(&t, "v");
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!({"a": 1})));
    assert_eq!(e.source.as_deref(), Some("state"));
}

#[test]
fn whole_state_root_with_none_state_is_empty_object() {
    // The resolver substitutes an empty map when no state was fetched, so
    // `{{ state }}` is a present (non-null) empty object, not missing.
    let t = trace_state(json!({"v": "{{ state }}"}), json!({}), None);
    let e = entry(&t, "v");
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!({})));
    assert_eq!(e.result_type.as_deref(), Some("object"));
}

#[test]
fn state_deep_path() {
    let state = json!({"pagination": {"pages": [10, 20, 30]}});
    let t = trace_state(
        json!({"v": "{{ state.pagination.pages.2 }}"}),
        json!({}),
        Some(&state),
    );
    assert_eq!(entry(&t, "v").value, Some(json!(30)));
}

#[test]
fn state_redacted_by_leaf_name() {
    let state = json!({"k": "opaque"});
    let t = trace_state(
        json!({"password": "{{ state.k }}"}),
        json!({}),
        Some(&state),
    );
    let e = entry(&t, "password");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

// ---- other roots ----

#[test]
fn bare_data_root_resolves_context_data() {
    let e = one("{{ data.x }}", json!({"x": "y"}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("y")));
}

#[test]
fn bare_config_root_resolves_context_config() {
    let t = trace_cfg(json!({"v": "{{ config.mode }}"}), json!({"mode": "fast"}));
    assert_eq!(entry(&t, "v").value, Some(json!("fast")));
}

#[test]
fn input_root_resolves_under_context_data_input() {
    let e = one("{{ input.q }}", json!({"input": {"q": "hi"}}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("hi")));
}

#[test]
fn runtime_attempt_resolves() {
    let e = one("{{ runtime.attempt }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!(0)));
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

#[test]
fn instance_id_root_resolves_to_empty_string_without_runtime_id() {
    let e = one("{{ instance_id }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("")));
}

// ---- errors ----

#[test]
fn unknown_root_is_error_with_message() {
    let e = one("{{ bogus.path }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(
        e.error
            .as_deref()
            .unwrap()
            .contains("unknown template root")
    );
}

#[test]
fn unknown_context_section_is_error() {
    let e = one("{{ context.wat.x }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(
        e.error
            .as_deref()
            .unwrap()
            .contains("unknown context section")
    );
}

#[test]
fn error_entries_carry_no_value_and_no_result_type() {
    let e = one("{{ bogus.path }}", json!({}), json!({}));
    assert_eq!(e.value, None);
    assert_eq!(e.result_type, None);
    assert!(e.error.is_some());
}

#[test]
fn unknown_template_function_is_error() {
    let e = one("{{ nope(context.data.x) }}", json!({"x": 1}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(
        e.error
            .as_deref()
            .unwrap()
            .contains("unknown template function")
    );
    assert_eq!(e.value, None);
}

#[test]
fn error_expression_field_preserves_original_expression() {
    let e = one("{{ bogus.path }}", json!({}), json!({}));
    assert_eq!(e.expression, "bogus.path");
    assert_eq!(e.param_path, "v");
}

// =====================================================================
// 2. Fallback-chain provenance, chains of length 1-4
// =====================================================================

#[test]
fn chain_len1_present_source_is_first_segment() {
    let e = one("{{ context.data.a }}", json!({"a": 1}), json!({}));
    assert_eq!(e.source.as_deref(), Some("context.data.a"));
    assert!(!e.fallback_used);
}

#[test]
fn chain_len1_missing_has_no_source() {
    let e = one("{{ context.data.a }}", json!({}), json!({}));
    assert_eq!(e.source, None);
    assert!(!e.fallback_used);
}

#[test]
fn chain_len2_first_present_wins_no_fallback() {
    let e = one(
        "{{ context.data.a | context.data.b }}",
        json!({"a": "first", "b": "second"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("first")));
    assert_eq!(e.source.as_deref(), Some("context.data.a"));
    assert!(!e.fallback_used);
}

#[test]
fn chain_len2_second_fires_when_first_missing() {
    let e = one(
        "{{ context.data.a | context.data.b }}",
        json!({"b": "backup"}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("backup")));
    assert_eq!(e.source.as_deref(), Some("context.data.b"));
    assert!(e.fallback_used);
}

#[test]
fn chain_len2_null_first_falls_through_to_second() {
    // Production semantics: explicit null DOES fall through the chain.
    let e = one(
        "{{ context.data.a | context.data.b }}",
        json!({"a": null, "b": "used"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("used")));
    assert_eq!(e.source.as_deref(), Some("context.data.b"));
    assert!(e.fallback_used);
}

#[test]
fn chain_len2_literal_fires() {
    let e = one("{{ context.data.a | fallback-word }}", json!({}), json!({}));
    assert_eq!(e.value, Some(json!("fallback-word")));
    assert_eq!(e.source.as_deref(), Some("literal"));
    assert!(e.fallback_used);
}

#[test]
fn chain_len3_middle_fires() {
    let e = one(
        "{{ context.data.a | outputs.b.v | context.data.c }}",
        json!({"c": "last"}),
        json!({"b": {"v": "middle"}}),
    );
    assert_eq!(e.value, Some(json!("middle")));
    assert_eq!(e.source.as_deref(), Some("outputs.b.v"));
    assert!(e.fallback_used);
}

#[test]
fn chain_len3_all_paths_missing_literal_fires() {
    let e = one(
        "{{ context.data.a | context.data.b | last-resort }}",
        json!({}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("last-resort")));
    assert_eq!(e.source.as_deref(), Some("literal"));
    assert!(e.fallback_used);
}

#[test]
fn chain_len4_last_path_fires() {
    let e = one(
        "{{ context.data.a | context.data.b | context.data.c | context.data.d }}",
        json!({"d": "deep"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("deep")));
    assert_eq!(e.source.as_deref(), Some("context.data.d"));
    assert!(e.fallback_used);
}

#[test]
fn chain_len4_null_missing_null_present_mix() {
    let e = one(
        "{{ context.data.a | context.data.b | context.data.c | context.data.d }}",
        json!({"a": null, "c": null, "d": true}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!(true)));
    assert_eq!(e.source.as_deref(), Some("context.data.d"));
    assert!(e.fallback_used);
}

#[test]
fn chain_all_missing_no_literal_is_missing_without_source() {
    let e = one(
        "{{ context.data.a | context.data.b | context.data.c }}",
        json!({}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Missing);
    assert_eq!(e.source, None);
    assert!(!e.fallback_used);
    assert_eq!(e.value, Some(Value::Null));
}

#[test]
fn chain_exhausted_first_null_reports_null_status() {
    // First segment present-but-null, rest missing → status reflects the
    // first segment: explicit null, not missing.
    let e = one(
        "{{ context.data.a | context.data.b }}",
        json!({"a": null}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Null);
}

#[test]
fn chain_exhausted_first_missing_second_null_reports_missing() {
    let e = one(
        "{{ context.data.a | context.data.b }}",
        json!({"b": null}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Missing);
}

#[test]
fn chain_mixed_roots_context_outputs_state() {
    let state = json!({"cursor": "from-state"});
    let t = trace_params(
        "blk",
        &json!({"v": "{{ context.data.a | outputs.b.v | state.cursor }}"}),
        &ctx(json!({})),
        &json!({}),
        Some(&state),
        &RedactionPolicy::default(),
    );
    let e = entry(&t, "v");
    assert_eq!(e.value, Some(json!("from-state")));
    assert_eq!(e.source.as_deref(), Some("state.cursor"));
    assert!(e.fallback_used);
}

#[test]
fn literal_in_middle_short_circuits_later_paths() {
    let e = one(
        "{{ context.data.a | stop-here | context.data.c }}",
        json!({"c": "never"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("stop-here")));
    assert_eq!(e.source.as_deref(), Some("literal"));
}

#[test]
fn numeric_bare_literal_fallback_is_a_string() {
    // Phase-1 literal fallbacks are taken verbatim as strings.
    let e = one("{{ context.data.a | 42 }}", json!({}), json!({}));
    assert_eq!(e.value, Some(json!("42")));
    assert_eq!(e.result_type.as_deref(), Some("string"));
    assert_eq!(e.source.as_deref(), Some("literal"));
}

#[test]
fn boolean_bare_literal_fallback_is_a_string() {
    let e = one("{{ context.data.a | true }}", json!({}), json!({}));
    assert_eq!(e.value, Some(json!("true")));
    assert_eq!(e.result_type.as_deref(), Some("string"));
}

#[test]
fn fallback_not_used_when_first_present_despite_literal_tail() {
    let e = one(
        "{{ context.data.a | never-used }}",
        json!({"a": "hit"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("hit")));
    assert!(!e.fallback_used);
    assert_eq!(e.source.as_deref(), Some("context.data.a"));
}

#[test]
fn fallback_provenance_survives_pipe_filters() {
    let e = one(
        "{{ context.data.a | context.data.b | upper }}",
        json!({"b": "low"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("LOW")));
    assert_eq!(e.source.as_deref(), Some("context.data.b"));
    assert!(e.fallback_used);
}

#[test]
fn fallback_object_value_from_second_segment() {
    let e = one(
        "{{ context.data.a | outputs.b }}",
        json!({}),
        json!({"b": {"deep": [1, 2]}}),
    );
    assert_eq!(e.value, Some(json!({"deep": [1, 2]})));
    assert_eq!(e.source.as_deref(), Some("outputs.b"));
    assert_eq!(e.result_type.as_deref(), Some("object"));
}

// =====================================================================
// 3. coerced_to_string
// =====================================================================

#[test]
fn whole_string_number_is_not_coerced() {
    let e = one("{{ context.data.n }}", json!({"n": 5}), json!({}));
    assert!(!e.coerced_to_string);
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

#[test]
fn whole_string_object_is_not_coerced() {
    let e = one("{{ context.data.o }}", json!({"o": {"a": 1}}), json!({}));
    assert!(!e.coerced_to_string);
}

#[test]
fn inline_number_is_coerced() {
    let e = one("count: {{ context.data.n }}", json!({"n": 5}), json!({}));
    assert!(e.coerced_to_string);
    assert_eq!(e.value, Some(json!(5)));
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

#[test]
fn inline_bool_is_coerced() {
    let e = one("flag={{ context.data.b }}", json!({"b": true}), json!({}));
    assert!(e.coerced_to_string);
    assert_eq!(e.result_type.as_deref(), Some("boolean"));
}

#[test]
fn inline_object_is_coerced() {
    let e = one(
        "obj: {{ context.data.o }}",
        json!({"o": {"a": 1}}),
        json!({}),
    );
    assert!(e.coerced_to_string);
    assert_eq!(e.result_type.as_deref(), Some("object"));
}

#[test]
fn inline_array_is_coerced() {
    let e = one("arr: {{ context.data.a }}", json!({"a": [1, 2]}), json!({}));
    assert!(e.coerced_to_string);
    assert_eq!(e.result_type.as_deref(), Some("array"));
}

#[test]
fn inline_string_is_not_coerced() {
    let e = one("hi {{ context.data.s }}", json!({"s": "there"}), json!({}));
    assert!(!e.coerced_to_string);
}

#[test]
fn trailing_text_after_expression_counts_as_inline() {
    let e = one("{{ context.data.n }} units", json!({"n": 3}), json!({}));
    assert!(e.coerced_to_string);
}

#[test]
fn two_expressions_in_braces_only_string_are_both_inline() {
    // "{{a}}{{b}}" starts with {{ and ends with }} but is NOT a single
    // expression, so both sub-expressions are inline coercions.
    let t = trace(
        json!({"v": "{{ context.data.a }}{{ context.data.b }}"}),
        json!({"a": 1, "b": 2}),
        json!({}),
    );
    let es: Vec<&ResolutionEntry> = t.entries.iter().filter(|e| e.param_path == "v").collect();
    assert_eq!(es.len(), 2);
    assert!(es[0].coerced_to_string);
    assert!(es[1].coerced_to_string);
}

#[test]
fn resolved_params_show_the_interpolated_string() {
    let t = trace(
        json!({"msg": "total: {{ context.data.n }} cents"}),
        json!({"n": 42}),
        json!({}),
    );
    assert_eq!(t.resolved_params, Some(json!({"msg": "total: 42 cents"})));
}

// =====================================================================
// 4. result_type for all six JSON types
// =====================================================================

#[test]
fn result_type_string() {
    let e = one("{{ context.data.v }}", json!({"v": "s"}), json!({}));
    assert_eq!(e.result_type.as_deref(), Some("string"));
}

#[test]
fn result_type_number() {
    let e = one("{{ context.data.v }}", json!({"v": 1.5}), json!({}));
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

#[test]
fn result_type_boolean() {
    let e = one("{{ context.data.v }}", json!({"v": false}), json!({}));
    assert_eq!(e.result_type.as_deref(), Some("boolean"));
    assert_eq!(e.value, Some(json!(false)));
}

#[test]
fn result_type_array() {
    let e = one("{{ context.data.v }}", json!({"v": [1, "a"]}), json!({}));
    assert_eq!(e.result_type.as_deref(), Some("array"));
    assert_eq!(e.value, Some(json!([1, "a"])));
}

#[test]
fn result_type_object() {
    let e = one("{{ context.data.v }}", json!({"v": {"k": 1}}), json!({}));
    assert_eq!(e.result_type.as_deref(), Some("object"));
}

#[test]
fn result_type_null_for_missing_path() {
    let e = one("{{ context.data.v }}", json!({}), json!({}));
    assert_eq!(e.result_type.as_deref(), Some("null"));
}

#[test]
fn result_type_null_for_explicit_null() {
    let e = one("{{ context.data.v }}", json!({"v": null}), json!({}));
    assert_eq!(e.result_type.as_deref(), Some("null"));
}

// =====================================================================
// 5. Nested param paths
// =====================================================================

#[test]
fn nested_object_param_paths_are_dotted() {
    let t = trace(
        json!({"body": {"customer": {"email": "{{ context.data.email }}"}}}),
        json!({"email": "a@b.c"}),
        json!({}),
    );
    let e = entry(&t, "body.customer.email");
    assert_eq!(e.value, Some(json!("a@b.c")));
}

#[test]
fn array_param_paths_use_indices() {
    let t = trace(
        json!({"headers": ["{{ context.data.h0 }}", "{{ context.data.h1 }}"]}),
        json!({"h0": "a", "h1": "b"}),
        json!({}),
    );
    assert_eq!(entry(&t, "headers.0").value, Some(json!("a")));
    assert_eq!(entry(&t, "headers.1").value, Some(json!("b")));
}

#[test]
fn objects_inside_arrays_inside_objects() {
    let t = trace(
        json!({"req": {"headers": [{"name": "x", "value": "{{ context.data.v }}"}]}}),
        json!({"v": "traced"}),
        json!({}),
    );
    assert_eq!(
        entry(&t, "req.headers.0.value").value,
        Some(json!("traced"))
    );
}

#[test]
fn deep_five_level_mixed_nesting() {
    let t = trace(
        json!({"a": [{"b": {"c": [{"d": "{{ context.data.x }}"}]}}]}),
        json!({"x": 9}),
        json!({}),
    );
    let e = entry(&t, "a.0.b.c.0.d");
    assert_eq!(e.value, Some(json!(9)));
}

#[test]
fn numeric_object_keys_in_params() {
    // A JSON object whose keys look numeric still produces dotted paths.
    let t = trace(
        json!({"0": {"1": "{{ context.data.x }}"}}),
        json!({"x": "n"}),
        json!({}),
    );
    assert_eq!(entry(&t, "0.1").value, Some(json!("n")));
}

#[test]
fn top_level_array_params() {
    let t = trace(
        json!(["{{ context.data.a }}", {"k": "{{ context.data.b }}"}]),
        json!({"a": 1, "b": 2}),
        json!({}),
    );
    assert_eq!(entry(&t, "0").value, Some(json!(1)));
    assert_eq!(entry(&t, "1.k").value, Some(json!(2)));
}

#[test]
fn each_nested_param_gets_its_own_status() {
    let t = trace(
        json!({"a": {"ok": "{{ context.data.x }}", "miss": "{{ context.data.y }}"}}),
        json!({"x": 1}),
        json!({}),
    );
    assert_eq!(entry(&t, "a.ok").status, ResolutionStatus::Ok);
    assert_eq!(entry(&t, "a.miss").status, ResolutionStatus::Missing);
}

#[test]
fn non_string_leaves_are_skipped_but_sibling_templates_traced() {
    let t = trace(
        json!({"n": 5, "b": true, "z": null, "t": "{{ context.data.x }}"}),
        json!({"x": "v"}),
        json!({}),
    );
    assert_eq!(t.entries.len(), 1);
    assert_eq!(entry(&t, "t").value, Some(json!("v")));
}

// =====================================================================
// 6. Multiple expressions per string, unbalanced braces
// =====================================================================

#[test]
fn two_expressions_in_one_string_yield_two_entries_in_order() {
    let t = trace(
        json!({"msg": "{{ context.data.a }} then {{ context.data.b }}"}),
        json!({"a": 1, "b": 2}),
        json!({}),
    );
    let es: Vec<&ResolutionEntry> = t.entries.iter().filter(|e| e.param_path == "msg").collect();
    assert_eq!(es.len(), 2);
    assert_eq!(es[0].expression, "context.data.a");
    assert_eq!(es[1].expression, "context.data.b");
}

#[test]
fn three_expressions_with_mixed_statuses() {
    let t = trace(
        json!({"msg": "{{ context.data.a }}/{{ context.data.b }}/{{ context.data.c }}"}),
        json!({"a": "x", "b": null}),
        json!({}),
    );
    let es: Vec<&ResolutionEntry> = t.entries.iter().filter(|e| e.param_path == "msg").collect();
    assert_eq!(es.len(), 3);
    assert_eq!(es[0].status, ResolutionStatus::Ok);
    assert_eq!(es[1].status, ResolutionStatus::Null);
    assert_eq!(es[2].status, ResolutionStatus::Missing);
}

#[test]
fn repeated_identical_expression_gets_one_entry_per_occurrence() {
    let t = trace(
        json!({"msg": "{{ context.data.a }} and {{ context.data.a }}"}),
        json!({"a": "dup"}),
        json!({}),
    );
    let es: Vec<&ResolutionEntry> = t.entries.iter().filter(|e| e.param_path == "msg").collect();
    assert_eq!(es.len(), 2);
    assert_eq!(es[0].expression, es[1].expression);
}

#[test]
fn expressions_across_multiple_params_all_traced() {
    let t = trace(
        json!({"p1": "{{ context.data.a }}", "p2": "{{ context.data.b }}", "p3": "{{ context.data.c }}"}),
        json!({"a": 1, "b": 2, "c": 3}),
        json!({}),
    );
    assert_eq!(t.entries.len(), 3);
}

#[test]
fn unbalanced_open_braces_produce_no_entries() {
    let t = trace(
        json!({"v": "{{ context.data.a"}),
        json!({"a": 1}),
        json!({}),
    );
    assert!(t.entries.is_empty());
}

#[test]
fn unbalanced_open_braces_kept_literal_in_resolved_params() {
    let t = trace(
        json!({"v": "{{ context.data.a"}),
        json!({"a": 1}),
        json!({}),
    );
    assert_eq!(t.resolved_params, Some(json!({"v": "{{ context.data.a"})));
}

#[test]
fn balanced_expression_before_unbalanced_tail_is_still_traced() {
    let t = trace(
        json!({"v": "{{ context.data.a }} and {{ tail"}),
        json!({"a": "yes"}),
        json!({}),
    );
    let es: Vec<&ResolutionEntry> = t.entries.iter().filter(|e| e.param_path == "v").collect();
    assert_eq!(es.len(), 1);
    assert_eq!(es[0].value, Some(json!("yes")));
}

#[test]
fn single_braces_are_not_templates() {
    let t = trace(json!({"v": "{context.data.a}"}), json!({"a": 1}), json!({}));
    assert!(t.entries.is_empty());
    assert_eq!(t.resolved_params, Some(json!({"v": "{context.data.a}"})));
}

#[test]
fn closing_braces_alone_are_not_templates() {
    let t = trace(json!({"v": "weird }} text"}), json!({}), json!({}));
    assert!(t.entries.is_empty());
}

#[test]
fn expression_inner_whitespace_is_trimmed_in_entry() {
    let e = one("{{   context.data.a   }}", json!({"a": 1}), json!({}));
    assert_eq!(e.expression, "context.data.a");
}

#[test]
fn no_space_expression_form_also_traced() {
    let e = one("{{context.data.a}}", json!({"a": 1}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!(1)));
}

// =====================================================================
// 7. Redaction
// =====================================================================

#[test]
fn api_key_leaf_redacted() {
    let t = trace(
        json!({"api_key": "{{ context.data.k }}"}),
        json!({"k": "super-secret"}),
        json!({}),
    );
    let e = entry(&t, "api_key");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

#[test]
fn password_leaf_redacted() {
    let t = trace(
        json!({"password": "{{ context.data.k }}"}),
        json!({"k": "hunter2"}),
        json!({}),
    );
    assert_eq!(entry(&t, "password").value, Some(json!(REDACTED)));
}

#[test]
fn token_leaf_redacted() {
    let t = trace(
        json!({"token": "{{ context.data.k }}"}),
        json!({"k": "tok"}),
        json!({}),
    );
    assert_eq!(entry(&t, "token").status, ResolutionStatus::Redacted);
}

#[test]
fn authorization_leaf_redacted() {
    let t = trace(
        json!({"authorization": "{{ context.data.k }}"}),
        json!({"k": "Basic x"}),
        json!({}),
    );
    assert_eq!(
        entry(&t, "authorization").status,
        ResolutionStatus::Redacted
    );
}

#[test]
fn client_secret_leaf_redacted() {
    let t = trace(
        json!({"client_secret": "{{ context.data.k }}"}),
        json!({"k": "s"}),
        json!({}),
    );
    assert_eq!(
        entry(&t, "client_secret").status,
        ResolutionStatus::Redacted
    );
}

#[test]
fn private_key_leaf_redacted() {
    let t = trace(
        json!({"private_key": "{{ context.data.k }}"}),
        json!({"k": "-----BEGIN"}),
        json!({}),
    );
    assert_eq!(entry(&t, "private_key").status, ResolutionStatus::Redacted);
}

#[test]
fn exact_key_auth_redacted_but_author_is_not() {
    let t = trace(
        json!({"auth": "{{ context.data.k }}", "author": "{{ context.data.k }}"}),
        json!({"k": "plain"}),
        json!({}),
    );
    assert_eq!(entry(&t, "auth").status, ResolutionStatus::Redacted);
    assert_eq!(entry(&t, "author").status, ResolutionStatus::Ok);
    assert_eq!(entry(&t, "author").value, Some(json!("plain")));
}

#[test]
fn nested_sensitive_leaf_redacted() {
    let t = trace(
        json!({"http": {"headers": {"authorization": "{{ context.data.k }}"}}}),
        json!({"k": "Basic zzz"}),
        json!({}),
    );
    let e = entry(&t, "http.headers.authorization");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

#[test]
fn camel_case_sensitive_leaf_redacted() {
    let t = trace(
        json!({"apiKey": "{{ context.data.k }}"}),
        json!({"k": "v"}),
        json!({}),
    );
    assert_eq!(entry(&t, "apiKey").status, ResolutionStatus::Redacted);
}

#[test]
fn sk_live_value_redacted_under_innocent_name() {
    let t = trace(
        json!({"note": "{{ context.data.t }}"}),
        json!({"t": "sk_live_abc123"}),
        json!({}),
    );
    let e = entry(&t, "note");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

#[test]
fn sk_test_value_redacted() {
    let t = trace(
        json!({"note": "{{ context.data.t }}"}),
        json!({"t": "sk_test_xyz"}),
        json!({}),
    );
    assert_eq!(entry(&t, "note").status, ResolutionStatus::Redacted);
}

#[test]
fn github_pat_value_redacted() {
    let t = trace(
        json!({"remark": "{{ context.data.t }}"}),
        json!({"t": "ghp_16charslong0000"}),
        json!({}),
    );
    assert_eq!(entry(&t, "remark").status, ResolutionStatus::Redacted);
}

#[test]
fn bearer_value_redacted() {
    let t = trace(
        json!({"header_value": "{{ context.data.t }}"}),
        json!({"t": "Bearer abc.def"}),
        json!({}),
    );
    assert_eq!(entry(&t, "header_value").status, ResolutionStatus::Redacted);
}

#[test]
fn jwt_shaped_value_redacted() {
    let t = trace(
        json!({"blob": "{{ context.data.t }}"}),
        json!({"t": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig"}),
        json!({}),
    );
    let e = entry(&t, "blob");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

#[test]
fn eyj_prefix_without_three_segments_is_not_a_jwt() {
    let t = trace(
        json!({"blob": "{{ context.data.t }}"}),
        json!({"t": "eyJ but not a jwt"}),
        json!({}),
    );
    assert_eq!(entry(&t, "blob").status, ResolutionStatus::Ok);
    assert_eq!(entry(&t, "blob").value, Some(json!("eyJ but not a jwt")));
}

#[test]
fn aws_access_key_id_value_redacted() {
    let t = trace(
        json!({"comment": "{{ context.data.t }}"}),
        json!({"t": "AKIAIOSFODNN7EXAMPLE"}),
        json!({}),
    );
    assert_eq!(entry(&t, "comment").status, ResolutionStatus::Redacted);
}

#[test]
fn slack_token_value_redacted() {
    let t = trace(
        json!({"comment": "{{ context.data.t }}"}),
        json!({"t": "xoxb-1234-abcd"}),
        json!({}),
    );
    assert_eq!(entry(&t, "comment").status, ResolutionStatus::Redacted);
}

#[test]
fn innocent_leaf_and_value_not_redacted() {
    let t = trace(
        json!({"email": "{{ context.data.e }}", "amount": "{{ context.data.a }}"}),
        json!({"e": "a@b.c", "a": 100}),
        json!({}),
    );
    assert_eq!(entry(&t, "email").value, Some(json!("a@b.c")));
    assert_eq!(entry(&t, "amount").value, Some(json!(100)));
}

#[test]
fn non_string_value_under_sensitive_leaf_redacted_keeps_original_type() {
    // result_type reflects the pre-redaction value; the value itself is the
    // redaction placeholder string.
    let t = trace(
        json!({"token": "{{ context.data.k }}"}),
        json!({"k": 12345}),
        json!({}),
    );
    let e = entry(&t, "token");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

#[test]
fn object_value_with_sensitive_inner_keys_is_partially_redacted_status_ok() {
    // Only whole-value redaction flips the status; inner-key redaction
    // keeps status ok while scrubbing the nested secret.
    let t = trace(
        json!({"payload": "{{ context.data.o }}"}),
        json!({"o": {"password": "hunter2", "user": "ada"}}),
        json!({}),
    );
    let e = entry(&t, "payload");
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!({"password": REDACTED, "user": "ada"})));
}

#[test]
fn secret_shaped_string_inside_object_value_scrubbed() {
    let t = trace(
        json!({"payload": "{{ context.data.o }}"}),
        json!({"o": {"note": "sk_live_inner", "n": 1}}),
        json!({}),
    );
    let e = entry(&t, "payload");
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!({"note": REDACTED, "n": 1})));
}

#[test]
fn resolved_params_aggregate_redacts_sensitive_keys() {
    let t = trace(
        json!({"api_key": "{{ context.data.k }}", "safe": "{{ context.data.s }}"}),
        json!({"k": "topsecret", "s": "visible"}),
        json!({}),
    );
    let rp = t.resolved_params.as_ref().unwrap();
    assert_eq!(rp["api_key"], json!(REDACTED));
    assert_eq!(rp["safe"], json!("visible"));
    // The raw secret never appears anywhere in the trace.
    let serialized = serde_json::to_string(&t).unwrap();
    assert!(!serialized.contains("topsecret"));
}

#[test]
fn resolved_params_redact_static_non_template_secrets_too() {
    // Even params without templates are redacted in the aggregate.
    let t = trace(
        json!({"password": "static-secret", "note": "plain"}),
        json!({}),
        json!({}),
    );
    assert!(t.entries.is_empty());
    let rp = t.resolved_params.as_ref().unwrap();
    assert_eq!(rp["password"], json!(REDACTED));
    assert_eq!(rp["note"], json!("plain"));
}

#[test]
fn resolved_params_redact_secret_shaped_interpolation_results() {
    let t = trace(
        json!({"memo": "{{ context.data.t }}"}),
        json!({"t": "ghp_leakedtoken000"}),
        json!({}),
    );
    let rp = t.resolved_params.as_ref().unwrap();
    assert_eq!(rp["memo"], json!(REDACTED));
    let serialized = serde_json::to_string(&t).unwrap();
    assert!(!serialized.contains("ghp_leakedtoken000"));
}

#[test]
fn sensitive_leaf_with_null_value_reports_null_not_redacted() {
    // Nothing to hide when the value is null.
    let t = trace(
        json!({"api_key": "{{ context.data.k }}"}),
        json!({"k": null}),
        json!({}),
    );
    assert_eq!(entry(&t, "api_key").status, ResolutionStatus::Null);
}

#[test]
fn sensitive_leaf_missing_reports_missing_not_redacted() {
    let t = trace(
        json!({"api_key": "{{ context.data.k }}"}),
        json!({}),
        json!({}),
    );
    assert_eq!(entry(&t, "api_key").status, ResolutionStatus::Missing);
}

// =====================================================================
// 8. Aggregate trace shape
// =====================================================================

#[test]
fn block_id_is_echoed() {
    let t = trace_params(
        "my-block",
        &json!({}),
        &ctx(json!({})),
        &json!({}),
        None,
        &RedactionPolicy::default(),
    );
    assert_eq!(t.block_id, "my-block");
}

#[test]
fn empty_params_object_yields_empty_entries_and_empty_resolved() {
    let t = trace(json!({}), json!({"a": 1}), json!({}));
    assert!(t.entries.is_empty());
    assert_eq!(t.resolved_params, Some(json!({})));
}

#[test]
fn params_without_templates_yield_zero_entries_and_identity_resolution() {
    let t = trace(
        json!({"a": "text", "b": 1, "c": [true, null], "d": {"e": "x"}}),
        json!({}),
        json!({}),
    );
    assert!(t.entries.is_empty());
    assert_eq!(
        t.resolved_params,
        Some(json!({"a": "text", "b": 1, "c": [true, null], "d": {"e": "x"}}))
    );
}

#[test]
fn resolved_params_none_when_whole_resolution_errors() {
    let t = trace(json!({"v": "{{ bogus.path }}"}), json!({}), json!({}));
    assert_eq!(t.resolved_params, None);
    assert_eq!(entry(&t, "v").status, ResolutionStatus::Error);
}

#[test]
fn one_error_still_leaves_other_entries_traced() {
    let t = trace(
        json!({"bad": "{{ bogus.path }}", "good": "{{ context.data.a }}"}),
        json!({"a": 1}),
        json!({}),
    );
    assert_eq!(entry(&t, "bad").status, ResolutionStatus::Error);
    assert_eq!(entry(&t, "good").status, ResolutionStatus::Ok);
    // ... but the aggregate resolution failed.
    assert_eq!(t.resolved_params, None);
}

#[test]
fn resolved_params_resolve_all_roots_together() {
    let state = json!({"k": "sv"});
    let t = trace_params(
        "blk",
        &json!({
            "d": "{{ context.data.a }}",
            "o": "{{ outputs.b.v }}",
            "s": "{{ state.k }}"
        }),
        &ctx(json!({"a": 1})),
        &json!({"b": {"v": 2}}),
        Some(&state),
        &RedactionPolicy::default(),
    );
    assert_eq!(t.resolved_params, Some(json!({"d": 1, "o": 2, "s": "sv"})));
}

#[test]
fn trace_serializes_with_snake_case_statuses() {
    let t = trace(
        json!({"m": "{{ context.data.x }}", "ok": "{{ context.data.a }}"}),
        json!({"a": 1}),
        json!({}),
    );
    let v = serde_json::to_value(&t).unwrap();
    let statuses: Vec<&str> = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["status"].as_str().unwrap())
        .collect();
    assert!(statuses.contains(&"missing"));
    assert!(statuses.contains(&"ok"));
}

#[test]
fn error_entry_serializes_without_value_key() {
    let t = trace(json!({"v": "{{ bogus.path }}"}), json!({}), json!({}));
    let v = serde_json::to_value(&t).unwrap();
    let e = &v["entries"][0];
    assert!(e.get("value").is_none());
    assert!(e.get("result_type").is_none());
    assert!(e["error"].is_string());
}

// =====================================================================
// 9. Array index navigation
// =====================================================================

#[test]
fn array_index_navigation() {
    let e = one(
        "{{ context.data.items.1 }}",
        json!({"items": ["zero", "one", "two"]}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("one")));
}

#[test]
fn array_index_zero() {
    let e = one(
        "{{ context.data.items.0 }}",
        json!({"items": [7]}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(7)));
}

#[test]
fn array_index_out_of_range_is_missing() {
    let e = one(
        "{{ context.data.items.5 }}",
        json!({"items": [1, 2]}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Missing);
}

#[test]
fn non_numeric_key_into_array_is_missing() {
    let e = one(
        "{{ context.data.items.first }}",
        json!({"items": [1]}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Missing);
}

#[test]
fn nested_array_of_arrays_navigation() {
    let e = one(
        "{{ context.data.grid.1.0 }}",
        json!({"grid": [[1, 2], [3, 4]]}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(3)));
}

#[test]
fn array_index_into_outputs() {
    let e = one(
        "{{ outputs.list.items.2.id }}",
        json!({}),
        json!({"list": {"items": [{"id": "a"}, {"id": "b"}, {"id": "c"}]}}),
    );
    assert_eq!(e.value, Some(json!("c")));
}

// =====================================================================
// 10. Pipe filters visible in traced values
// =====================================================================

#[test]
fn filter_upper() {
    let e = one(
        "{{ context.data.s | upper }}",
        json!({"s": "bob"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("BOB")));
    assert_eq!(e.source.as_deref(), Some("context.data.s"));
}

#[test]
fn filter_lower() {
    let e = one(
        "{{ context.data.s | lower }}",
        json!({"s": "LOUD"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("loud")));
}

#[test]
fn filter_trim() {
    let e = one(
        "{{ context.data.s | trim }}",
        json!({"s": "  pad  "}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("pad")));
}

#[test]
fn filter_abs_returns_float_number() {
    let e = one("{{ context.data.n | abs }}", json!({"n": -5}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!(5.0)));
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

#[test]
fn filter_url_encode() {
    let e = one(
        "{{ context.data.s | url_encode }}",
        json!({"s": "a b/c&d"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("a%20b%2Fc%26d")));
}

#[test]
fn filter_base64() {
    let e = one(
        "{{ context.data.s | base64 }}",
        json!({"s": "hello"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("aGVsbG8=")));
}

#[test]
fn filter_base64_decode() {
    let e = one(
        "{{ context.data.s | base64_decode }}",
        json!({"s": "aGVsbG8="}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("hello")));
}

#[test]
fn filter_base64_decode_invalid_input_is_error() {
    let e = one(
        "{{ context.data.s | base64_decode }}",
        json!({"s": "%%%not-base64%%%"}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(e.error.as_deref().unwrap().contains("base64_decode"));
    assert_eq!(e.value, None);
}

#[test]
fn filter_replace_with_literals() {
    let e = one(
        "{{ context.data.s | replace('l', 'L') }}",
        json!({"s": "hello"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("heLLo")));
}

#[test]
fn filter_replace_with_path_replacement() {
    let e = one(
        "{{ context.data.msg | replace('NAME', context.data.name) }}",
        json!({"msg": "hi NAME!", "name": "ada"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("hi ada!")));
}

#[test]
fn filter_replace_missing_second_arg_is_error() {
    let e = one(
        "{{ context.data.s | replace('x') }}",
        json!({"s": "x"}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(e.error.as_deref().unwrap().contains("replace()"));
}

#[test]
fn filter_default_fires_on_missing_with_no_source() {
    // The base path is missing; the value is manufactured by the filter,
    // so no fallback segment is reported and fallback_used stays false.
    let e = one(
        "{{ context.data.absent | default('fallback') }}",
        json!({}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("fallback")));
    assert_eq!(e.source, None);
    assert!(!e.fallback_used);
}

#[test]
fn filter_default_fires_on_empty_string() {
    let e = one(
        "{{ context.data.s | default('filled') }}",
        json!({"s": ""}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("filled")));
    // The empty string DID resolve, so provenance points at the path.
    assert_eq!(e.source.as_deref(), Some("context.data.s"));
}

#[test]
fn filter_default_not_applied_when_value_present() {
    let e = one(
        "{{ context.data.s | default('unused') }}",
        json!({"s": "real"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("real")));
}

#[test]
fn filter_default_with_numeric_literal_is_typed() {
    let e = one(
        "{{ context.data.absent | default(42) }}",
        json!({}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(42.0)));
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

#[test]
fn filter_default_with_boolean_literal_is_typed() {
    let e = one(
        "{{ context.data.absent | default(true) }}",
        json!({}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(true)));
    assert_eq!(e.result_type.as_deref(), Some("boolean"));
}

#[test]
fn filter_truncate() {
    let e = one(
        "{{ context.data.s | truncate(5) }}",
        json!({"s": "hello world"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("hello")));
}

#[test]
fn filter_truncate_with_suffix() {
    let e = one(
        "{{ context.data.s | truncate(5, '...') }}",
        json!({"s": "hello world"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("hello...")));
}

#[test]
fn filter_truncate_shorter_than_limit_untouched() {
    let e = one(
        "{{ context.data.s | truncate(50) }}",
        json!({"s": "short"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("short")));
}

#[test]
fn filter_truncate_without_length_is_error() {
    let e = one(
        "{{ context.data.s | truncate(x) }}",
        json!({"s": "abc"}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(e.error.as_deref().unwrap().contains("truncate()"));
}

#[test]
fn filter_join() {
    let e = one(
        "{{ context.data.items | join(', ') }}",
        json!({"items": ["a", "b", "c"]}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("a, b, c")));
    assert_eq!(e.result_type.as_deref(), Some("string"));
}

#[test]
fn filter_join_stringifies_non_string_elements() {
    let e = one(
        "{{ context.data.items | join('-') }}",
        json!({"items": [1, true, "x"]}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("1-true-x")));
}

#[test]
fn filter_split() {
    let e = one(
        "{{ context.data.csv | split(',') }}",
        json!({"csv": "a,b,c"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(["a", "b", "c"])));
    assert_eq!(e.result_type.as_deref(), Some("array"));
}

#[test]
fn filter_round() {
    // An arbitrary decimal (not a math constant) exercising round(2).
    let e = one(
        "{{ context.data.n | round(2) }}",
        json!({"n": 12.3456}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(12.35)));
}

#[test]
fn filter_round_to_integer() {
    let e = one(
        "{{ context.data.n | round(0) }}",
        json!({"n": 2.5}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(3.0)));
}

#[test]
fn filter_round_bad_arg_is_error() {
    let e = one(
        "{{ context.data.n | round(two) }}",
        json!({"n": 1.5}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(e.error.as_deref().unwrap().contains("round()"));
}

#[test]
fn filter_chain_upper_then_replace() {
    let e = one(
        "{{ context.data.s | upper | replace('B', 'X') }}",
        json!({"s": "bob"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("XOX")));
}

#[test]
fn filter_split_then_join_roundtrip() {
    let e = one(
        "{{ context.data.csv | split(',') | join(';') }}",
        json!({"csv": "a,b"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("a;b")));
}

#[test]
fn filter_on_missing_base_upper_yields_empty_string_ok() {
    // Quirk pinned: string filters coerce a missing (null) base to "" and
    // report ok — the filter manufactured a value.
    let e = one("{{ context.data.absent | upper }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("")));
    assert_eq!(e.source, None);
    assert!(!e.fallback_used);
}

#[test]
fn filter_hash_sha256() {
    let e = one(
        "{{ context.data.s | hash('sha256') }}",
        json!({"s": "abc"}),
        json!({}),
    );
    assert_eq!(
        e.value,
        Some(json!(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        ))
    );
}

#[test]
fn filter_hash_unknown_algo_is_error() {
    let e = one(
        "{{ context.data.s | hash('md5') }}",
        json!({"s": "abc"}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Error);
    assert!(e.error.as_deref().unwrap().contains("hash"));
}

#[test]
fn unknown_pipe_word_acts_as_literal_fallback_when_base_missing() {
    // "shout" is not a known filter, so it participates in the fallback
    // chain as a bare literal.
    let e = one("{{ context.data.absent | shout }}", json!({}), json!({}));
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("shout")));
    assert_eq!(e.source.as_deref(), Some("literal"));
}

#[test]
fn unknown_pipe_word_ignored_when_base_present() {
    let e = one(
        "{{ context.data.s | shout }}",
        json!({"s": "quiet"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!("quiet")));
    assert_eq!(e.source.as_deref(), Some("context.data.s"));
}

#[test]
fn json_function_stringifies() {
    let e = one(
        "{{ json(context.data.o) }}",
        json!({"o": {"a": 1}}),
        json!({}),
    );
    assert_eq!(e.status, ResolutionStatus::Ok);
    assert_eq!(e.value, Some(json!("{\"a\":1}")));
    assert_eq!(e.result_type.as_deref(), Some("string"));
}

#[test]
fn len_function_on_array_and_string() {
    let e = one(
        "{{ len(context.data.items) }}",
        json!({"items": [1, 2, 3]}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(3)));
    let e = one(
        "{{ len(context.data.s) }}",
        json!({"s": "hello"}),
        json!({}),
    );
    assert_eq!(e.value, Some(json!(5)));
}

#[test]
fn filtered_value_is_redacted_when_it_lands_in_sensitive_param() {
    let t = trace(
        json!({"api_key": "{{ context.data.k | upper }}"}),
        json!({"k": "secret"}),
        json!({}),
    );
    let e = entry(&t, "api_key");
    assert_eq!(e.status, ResolutionStatus::Redacted);
    assert_eq!(e.value, Some(json!(REDACTED)));
}

#[test]
fn inline_filtered_expression_flags_coercion_by_result_type() {
    let e = one(
        "rounded: {{ context.data.n | round(1) }}",
        json!({"n": 2.55}),
        json!({}),
    );
    assert!(e.coerced_to_string);
    assert_eq!(e.result_type.as_deref(), Some("number"));
}

// =====================================================================
// 11. ResolutionStatus / entry type behaviors (orch8-types)
// =====================================================================

#[test]
fn all_statuses_serialize_snake_case() {
    for (s, expect) in [
        (ResolutionStatus::Ok, "\"ok\""),
        (ResolutionStatus::Null, "\"null\""),
        (ResolutionStatus::Missing, "\"missing\""),
        (ResolutionStatus::Redacted, "\"redacted\""),
        (ResolutionStatus::Error, "\"error\""),
    ] {
        assert_eq!(serde_json::to_string(&s).unwrap(), expect);
    }
}

#[test]
fn all_statuses_deserialize_from_snake_case() {
    for (raw, expect) in [
        ("\"ok\"", ResolutionStatus::Ok),
        ("\"null\"", ResolutionStatus::Null),
        ("\"missing\"", ResolutionStatus::Missing),
        ("\"redacted\"", ResolutionStatus::Redacted),
        ("\"error\"", ResolutionStatus::Error),
    ] {
        let s: ResolutionStatus = serde_json::from_str(raw).unwrap();
        assert_eq!(s, expect);
    }
}

#[test]
fn full_trace_round_trips_through_json() {
    let t = trace(
        json!({"a": "{{ context.data.x }}", "b": "inline {{ context.data.n }}"}),
        json!({"x": "v", "n": 2}),
        json!({}),
    );
    let ser = serde_json::to_string(&t).unwrap();
    let back: ResolutionTrace = serde_json::from_str(&ser).unwrap();
    assert_eq!(back, t);
}

#[test]
fn entry_defaults_for_flags_when_absent_in_json() {
    let raw = json!({
        "param_path": "p",
        "expression": "context.data.x",
        "status": "missing"
    });
    let e: ResolutionEntry = serde_json::from_value(raw).unwrap();
    assert!(!e.fallback_used);
    assert!(!e.coerced_to_string);
    assert_eq!(e.value, None);
    assert_eq!(e.source, None);
    assert_eq!(e.error, None);
}

#[test]
fn trace_without_resolved_params_round_trips() {
    let t = ResolutionTrace {
        block_id: "b".into(),
        resolved_params: None,
        entries: vec![],
    };
    let v = serde_json::to_value(&t).unwrap();
    assert!(v.get("resolved_params").is_none());
    let back: ResolutionTrace = serde_json::from_value(v).unwrap();
    assert_eq!(back, t);
}
