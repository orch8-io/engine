//! Inspector-only template trace evaluation.
//!
//! Resolves a block's `params` exactly like production execution
//! (delegating to [`crate::template`]) while additionally recording, per
//! template expression: the original expression, the resolved value, the
//! fallback-chain segment that supplied it, the resulting JSON type, and
//! a status distinguishing ok / explicit-null / missing / redacted /
//! error.
//!
//! This is a separate evaluation mode: normal execution never records
//! provenance and pays no cost for this module. Resolution semantics stay
//! aligned with production because the final value of every expression is
//! computed by the production resolver itself; only the *provenance*
//! (which fallback segment fired) is re-derived here with the same
//! helpers the resolver uses.
//!
//! Handlers are never invoked; this module only reads context/outputs.

use serde_json::Value;

use orch8_types::context::ExecutionContext;
use orch8_types::redaction::{REDACTED, RedactionPolicy};
use orch8_types::template_trace::{
    DebugTemplateResponse, ResolutionEntry, ResolutionStatus, ResolutionTrace,
};

use crate::template;

/// Trace-resolve a block's params.
#[must_use]
pub fn trace_params(
    block_id: &str,
    params: &Value,
    context: &ExecutionContext,
    outputs: &Value,
    state: Option<&Value>,
    redaction: &RedactionPolicy,
) -> ResolutionTrace {
    let mut entries = Vec::new();
    walk("", params, context, outputs, state, redaction, &mut entries);

    let resolved_params = template::resolve_with_state(params, context, outputs, state)
        .ok()
        .map(|v| redaction.redacted(&v));

    ResolutionTrace {
        block_id: block_id.to_string(),
        resolved_params,
        entries,
    }
}

/// Debug-resolve a raw template string.
///
/// Unlike [`trace_params`] which expects a block's params object, this
/// takes a single template expression such as
/// `"{{ context.data.name | upper }}"` and returns the resolved value
/// with per-expression provenance entries.
#[must_use]
pub fn debug_template(
    raw_template: &str,
    context: &ExecutionContext,
    outputs: &Value,
    redaction: &RedactionPolicy,
) -> DebugTemplateResponse {
    let wrapped = Value::String(raw_template.to_string());
    let mut entries = Vec::new();
    walk(
        "_",
        &wrapped,
        context,
        outputs,
        None,
        redaction,
        &mut entries,
    );

    match template::resolve(&wrapped, context, outputs) {
        Ok(resolved) => {
            let redacted = redaction.redacted(&resolved);
            let result_type = Some(json_type_name(&redacted).to_string());
            DebugTemplateResponse {
                result: Some(redacted),
                result_type,
                entries,
                error: None,
            }
        }
        Err(e) => DebugTemplateResponse {
            result: None,
            result_type: None,
            entries,
            error: Some(e.to_string()),
        },
    }
}

fn walk(
    path: &str,
    value: &Value,
    context: &ExecutionContext,
    outputs: &Value,
    state: Option<&Value>,
    redaction: &RedactionPolicy,
    entries: &mut Vec<ResolutionEntry>,
) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                walk(&child, v, context, outputs, state, redaction, entries);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                let child = if path.is_empty() {
                    i.to_string()
                } else {
                    format!("{path}.{i}")
                };
                walk(&child, v, context, outputs, state, redaction, entries);
            }
        }
        Value::String(s) if s.contains("{{") => {
            trace_string(path, s, context, outputs, state, redaction, entries);
        }
        _ => {}
    }
}

/// Trace every balanced `{{ ... }}` expression inside one string param.
#[allow(clippy::too_many_arguments)]
fn trace_string(
    param_path: &str,
    raw: &str,
    context: &ExecutionContext,
    outputs: &Value,
    state: Option<&Value>,
    redaction: &RedactionPolicy,
    entries: &mut Vec<ResolutionEntry>,
) {
    // Whole-string single expression: the resolved value keeps its type.
    let whole = raw.starts_with("{{")
        && raw.ends_with("}}")
        && template::is_single_template_expr(raw[2..raw.len() - 2].trim());

    let mut remaining = raw;
    while let Some(open) = remaining.find("{{") {
        let after_open = &remaining[open + 2..];
        let Some(close) = template::find_closing_braces(after_open) else {
            break; // unbalanced — production treats it as literal text
        };
        let expression = after_open[..close].trim().to_string();
        entries.push(trace_expression(
            param_path,
            &expression,
            !whole,
            context,
            outputs,
            state,
            redaction,
        ));
        remaining = &after_open[close + 2..];
    }
}

/// Trace a single expression: production-resolve it for the value, then
/// re-run the fallback chain with the resolver's own helpers to find the
/// segment that supplied the value.
#[allow(clippy::too_many_arguments)]
fn trace_expression(
    param_path: &str,
    expression: &str,
    inline: bool,
    context: &ExecutionContext,
    outputs: &Value,
    state: Option<&Value>,
    redaction: &RedactionPolicy,
) -> ResolutionEntry {
    let mut entry = ResolutionEntry {
        param_path: param_path.to_string(),
        expression: expression.to_string(),
        status: ResolutionStatus::Error,
        value: None,
        result_type: None,
        source: None,
        fallback_used: false,
        coerced_to_string: false,
        error: None,
    };

    // The production resolver computes the final value (fallbacks + pipe
    // filters included).
    let single = format!("{{{{ {expression} }}}}");
    let resolved = template::resolve_with_state(&Value::String(single), context, outputs, state);

    // Provenance: which fallback segment fires first?
    let (source, fallback_used, base_missing) =
        analyze_fallback_chain(expression, context, outputs, state);
    entry.source = source;
    entry.fallback_used = fallback_used;

    match resolved {
        Ok(value) => {
            entry.result_type = Some(json_type_name(&value).to_string());
            entry.coerced_to_string = inline && !value.is_string();
            if value.is_null() {
                entry.status = if base_missing {
                    ResolutionStatus::Missing
                } else {
                    // Explicit null (or a filter produced null): either
                    // way the caller sees a genuine null value.
                    ResolutionStatus::Null
                };
                entry.value = Some(Value::Null);
            } else {
                let redacted = redact_entry_value(param_path, &value, redaction);
                entry.status = if redacted == Value::String(REDACTED.to_string()) {
                    ResolutionStatus::Redacted
                } else {
                    ResolutionStatus::Ok
                };
                entry.value = Some(redacted);
            }
        }
        Err(e) => {
            entry.status = ResolutionStatus::Error;
            entry.error = Some(e.to_string());
        }
    }
    entry
}

/// Walk the fallback chain the way `resolve_path` does and report:
/// (source segment that supplied the value, whether it was a fallback,
/// whether the first path segment was missing).
fn analyze_fallback_chain(
    expression: &str,
    context: &ExecutionContext,
    outputs: &Value,
    state: Option<&Value>,
) -> (Option<String>, bool, bool) {
    let segments = template::split_pipe_segments(expression);
    let mut first_missing = false;

    for (i, segment) in segments.iter().enumerate() {
        let seg = segment.trim();
        if template::is_pipe_filter(seg) {
            break; // filters transform; they don't supply the base value
        }
        if template::is_template_path(seg) {
            match template::try_resolve_single(seg, context, outputs, state) {
                Ok(Some(v)) if !v.is_null() => {
                    return (Some(seg.to_string()), i > 0, first_missing);
                }
                Ok(Some(_)) => {
                    // Present but null: the fallback chain moves on, but a
                    // null first segment is not "missing".
                }
                Ok(None) | Err(_) => {
                    if i == 0 {
                        first_missing = true;
                    }
                }
            }
        } else {
            // A quoted/bare literal default.
            return (Some("literal".to_string()), i > 0, first_missing);
        }
    }
    (None, false, first_missing)
}

/// Redact a single resolved value: by the param path it lands in (ANY
/// segment being sensitive redacts the entry — a value under
/// `credentials.note` must not leak just because `note` is innocent), or
/// by its own shape.
fn redact_entry_value(param_path: &str, value: &Value, redaction: &RedactionPolicy) -> Value {
    if param_path
        .split('.')
        .any(|segment| redaction.is_sensitive_key(segment))
    {
        return Value::String(REDACTED.to_string());
    }
    if let Value::String(s) = value
        && redaction.is_secret_shaped(s)
    {
        return Value::String(REDACTED.to_string());
    }
    redaction.redacted(value)
}

const fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(data: Value) -> ExecutionContext {
        ExecutionContext {
            data,
            ..Default::default()
        }
    }

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

    fn entry<'a>(t: &'a ResolutionTrace, path: &str) -> &'a ResolutionEntry {
        t.entries
            .iter()
            .find(|e| e.param_path == path)
            .unwrap_or_else(|| panic!("no entry for {path}: {:#?}", t.entries))
    }

    #[test]
    fn resolves_context_reference_with_provenance() {
        let t = trace(
            json!({"email": "{{ context.data.customer.email }}"}),
            json!({"customer": {"email": "a@b.c"}}),
            json!({}),
        );
        let e = entry(&t, "email");
        assert_eq!(e.status, ResolutionStatus::Ok);
        assert_eq!(e.value, Some(json!("a@b.c")));
        assert_eq!(e.result_type.as_deref(), Some("string"));
        assert_eq!(e.source.as_deref(), Some("context.data.customer.email"));
        assert!(!e.fallback_used);
        assert_eq!(t.resolved_params, Some(json!({"email": "a@b.c"})));
    }

    #[test]
    fn missing_path_is_distinguished_from_explicit_null() {
        let t = trace(
            json!({
                "missing": "{{ context.data.nope }}",
                "null_field": "{{ context.data.email }}"
            }),
            json!({"email": null}),
            json!({}),
        );
        assert_eq!(entry(&t, "missing").status, ResolutionStatus::Missing);
        assert_eq!(entry(&t, "null_field").status, ResolutionStatus::Null);
    }

    #[test]
    fn fallback_chain_reports_the_segment_that_fired() {
        let t = trace(
            json!({"v": "{{ context.data.primary | context.data.secondary }}"}),
            json!({"secondary": "backup"}),
            json!({}),
        );
        let e = entry(&t, "v");
        assert_eq!(e.status, ResolutionStatus::Ok);
        assert_eq!(e.source.as_deref(), Some("context.data.secondary"));
        assert!(e.fallback_used);
    }

    #[test]
    fn literal_fallback_is_reported_as_literal() {
        let t = trace(
            json!({"v": "{{ context.data.absent | default-val }}"}),
            json!({}),
            json!({}),
        );
        let e = entry(&t, "v");
        assert_eq!(e.source.as_deref(), Some("literal"));
        assert!(e.fallback_used);
        assert_eq!(e.value, Some(json!("default-val")));
    }

    #[test]
    fn explicit_null_does_not_fire_fallback_semantics_are_traced() {
        // The engine's fallback chain skips null values — the trace must
        // show where the value actually came from.
        let t = trace(
            json!({"v": "{{ context.data.maybe_null | fell-back }}"}),
            json!({"maybe_null": null}),
            json!({}),
        );
        let e = entry(&t, "v");
        // Production semantics: null DOES fall through to the fallback.
        assert_eq!(e.value, Some(json!("fell-back")));
        assert_eq!(e.source.as_deref(), Some("literal"));
        assert!(e.fallback_used);
    }

    #[test]
    fn output_references_resolve() {
        let t = trace(
            json!({"amount": "{{ outputs.charge.amount }}"}),
            json!({}),
            json!({"charge": {"amount": 42}}),
        );
        let e = entry(&t, "amount");
        assert_eq!(e.status, ResolutionStatus::Ok);
        assert_eq!(e.value, Some(json!(42)));
        assert_eq!(e.result_type.as_deref(), Some("number"));
    }

    #[test]
    fn unknown_root_is_an_error() {
        let t = trace(json!({"v": "{{ bogus.path }}"}), json!({}), json!({}));
        let e = entry(&t, "v");
        assert_eq!(e.status, ResolutionStatus::Error);
        assert!(
            e.error
                .as_deref()
                .unwrap()
                .contains("unknown template root")
        );
        assert!(e.value.is_none());
    }

    #[test]
    fn inline_interpolation_flags_string_coercion() {
        let t = trace(
            json!({"msg": "total: {{ outputs.charge.amount }} cents"}),
            json!({}),
            json!({"charge": {"amount": 42}}),
        );
        let e = entry(&t, "msg");
        assert!(e.coerced_to_string);
        assert_eq!(e.expression, "outputs.charge.amount");
    }

    #[test]
    fn multiple_expressions_in_one_string_each_get_an_entry() {
        let t = trace(
            json!({"msg": "{{ context.data.a }} and {{ context.data.b }}"}),
            json!({"a": 1, "b": 2}),
            json!({}),
        );
        let for_msg: Vec<&ResolutionEntry> =
            t.entries.iter().filter(|e| e.param_path == "msg").collect();
        assert_eq!(for_msg.len(), 2);
        assert_eq!(for_msg[0].expression, "context.data.a");
        assert_eq!(for_msg[1].expression, "context.data.b");
    }

    #[test]
    fn nested_params_get_dotted_paths() {
        let t = trace(
            json!({"body": {"customer": {"email": "{{ context.data.email }}"}},
                   "headers": [{"value": "{{ context.data.h }}"}]}),
            json!({"email": "x@y.z", "h": "v"}),
            json!({}),
        );
        assert_eq!(entry(&t, "body.customer.email").value, Some(json!("x@y.z")));
        assert_eq!(entry(&t, "headers.0.value").value, Some(json!("v")));
    }

    #[test]
    fn sensitive_param_names_are_redacted() {
        let t = trace(
            json!({"api_key": "{{ context.data.key_value }}"}),
            json!({"key_value": "super-secret"}),
            json!({}),
        );
        let e = entry(&t, "api_key");
        assert_eq!(e.status, ResolutionStatus::Redacted);
        assert_eq!(e.value, Some(json!(REDACTED)));
        // The aggregated resolved params are redacted too.
        assert_eq!(
            t.resolved_params.as_ref().unwrap()["api_key"],
            json!(REDACTED)
        );
    }

    #[test]
    fn secret_shaped_values_are_redacted_under_innocent_names() {
        let t = trace(
            json!({"note": "{{ context.data.tok }}"}),
            json!({"tok": "sk_live_abc123"}),
            json!({}),
        );
        let e = entry(&t, "note");
        assert_eq!(e.status, ResolutionStatus::Redacted);
        assert_eq!(e.value, Some(json!(REDACTED)));
    }

    #[test]
    fn pipe_filters_apply_to_traced_value() {
        let t = trace(
            json!({"v": "{{ context.data.name | upper }}"}),
            json!({"name": "bob"}),
            json!({}),
        );
        let e = entry(&t, "v");
        assert_eq!(e.value, Some(json!("BOB")));
        assert_eq!(e.source.as_deref(), Some("context.data.name"));
    }

    #[test]
    fn non_template_params_produce_no_entries() {
        let t = trace(
            json!({"static": "hello", "n": 5, "flag": true}),
            json!({}),
            json!({}),
        );
        assert!(t.entries.is_empty());
        assert_eq!(
            t.resolved_params,
            Some(json!({"static": "hello", "n": 5, "flag": true}))
        );
    }

    #[test]
    fn object_valued_resolution_keeps_type() {
        let t = trace(
            json!({"customer": "{{ context.data.customer }}"}),
            json!({"customer": {"id": 7, "name": "Ada"}}),
            json!({}),
        );
        let e = entry(&t, "customer");
        assert_eq!(e.result_type.as_deref(), Some("object"));
        assert!(!e.coerced_to_string);
        assert_eq!(e.value, Some(json!({"id": 7, "name": "Ada"})));
    }

    // ─── debug_template tests ───

    fn dbg(template: &str, data: Value, outputs: &Value) -> DebugTemplateResponse {
        debug_template(template, &ctx(data), outputs, &RedactionPolicy::default())
    }

    fn dbg_entry<'a>(r: &'a DebugTemplateResponse, expr: &str) -> &'a ResolutionEntry {
        r.entries
            .iter()
            .find(|e| e.expression == expr)
            .unwrap_or_else(|| panic!("no entry for expr {expr}: {:#?}", r.entries))
    }

    #[test]
    fn debug_template_resolves_simple_expression() {
        let r = dbg(
            "{{ context.data.name }}",
            json!({"name": "Alice"}),
            &json!({}),
        );
        assert!(r.error.is_none());
        assert_eq!(r.result, Some(json!("Alice")));
        assert_eq!(r.result_type.as_deref(), Some("string"));
        assert_eq!(r.entries.len(), 1);
        let e = &r.entries[0];
        assert_eq!(e.status, ResolutionStatus::Ok);
        assert_eq!(e.source.as_deref(), Some("context.data.name"));
    }

    #[test]
    fn debug_template_numeric_value_preserves_type() {
        let r = dbg(
            "{{ outputs.calc.total }}",
            json!({}),
            &json!({"calc": {"total": 99}}),
        );
        assert_eq!(r.result, Some(json!(99)));
        assert_eq!(r.result_type.as_deref(), Some("number"));
    }

    #[test]
    fn debug_template_missing_path_returns_null_status() {
        let r = dbg("{{ context.data.nope }}", json!({}), &json!({}));
        assert!(r.error.is_none());
        let e = dbg_entry(&r, "context.data.nope");
        assert_eq!(e.status, ResolutionStatus::Missing);
    }

    #[test]
    fn debug_template_explicit_null_returns_null_status() {
        let r = dbg("{{ context.data.val }}", json!({"val": null}), &json!({}));
        let e = dbg_entry(&r, "context.data.val");
        assert_eq!(e.status, ResolutionStatus::Null);
    }

    #[test]
    fn debug_template_fallback_chain() {
        let r = dbg(
            "{{ context.data.primary | context.data.secondary }}",
            json!({"secondary": "backup"}),
            &json!({}),
        );
        assert_eq!(r.result, Some(json!("backup")));
        let e = dbg_entry(&r, "context.data.primary | context.data.secondary");
        assert!(e.fallback_used);
        assert_eq!(e.source.as_deref(), Some("context.data.secondary"));
    }

    #[test]
    fn debug_template_pipe_filter() {
        let r = dbg(
            "{{ context.data.name | upper }}",
            json!({"name": "alice"}),
            &json!({}),
        );
        assert_eq!(r.result, Some(json!("ALICE")));
    }

    #[test]
    fn debug_template_unknown_root_is_error() {
        let r = dbg("{{ bogus.path }}", json!({}), &json!({}));
        assert!(r.error.is_some());
        assert!(r.result.is_none());
        assert!(r.result_type.is_none());
    }

    #[test]
    fn debug_template_inline_interpolation() {
        let r = dbg(
            "Hello {{ context.data.name }}, welcome!",
            json!({"name": "Bob"}),
            &json!({}),
        );
        assert_eq!(r.result, Some(json!("Hello Bob, welcome!")));
        assert_eq!(r.result_type.as_deref(), Some("string"));
    }

    #[test]
    fn debug_template_object_result() {
        let r = dbg(
            "{{ context.data.user }}",
            json!({"user": {"id": 1, "name": "X"}}),
            &json!({}),
        );
        assert_eq!(r.result, Some(json!({"id": 1, "name": "X"})));
        assert_eq!(r.result_type.as_deref(), Some("object"));
    }

    #[test]
    fn debug_template_redacts_sensitive_values() {
        let r = dbg(
            "{{ context.data.tok }}",
            json!({"tok": "sk_live_abc123"}),
            &json!({}),
        );
        assert_eq!(r.result, Some(json!(REDACTED)));
        let e = dbg_entry(&r, "context.data.tok");
        assert_eq!(e.status, ResolutionStatus::Redacted);
    }

    #[test]
    fn debug_template_no_template_returns_literal() {
        let r = dbg("plain text", json!({}), &json!({}));
        assert_eq!(r.result, Some(json!("plain text")));
        assert!(r.entries.is_empty());
    }

    #[test]
    fn debug_template_config_reference() {
        let r = debug_template(
            "{{ context.config.region }}",
            &ExecutionContext {
                config: json!({"region": "us-east"}),
                ..Default::default()
            },
            &json!({}),
            &RedactionPolicy::default(),
        );
        assert_eq!(r.result, Some(json!("us-east")));
    }
}
