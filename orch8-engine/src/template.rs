use orch8_types::asl_intrinsic::Intrinsic;
use orch8_types::context::ExecutionContext;
use tracing::warn;

use crate::error::EngineError;

pub(crate) use crate::template_filters::{
    find_closing_braces, is_pipe_filter, is_single_template_expr, is_template_path,
    split_pipe_segments,
};
use crate::template_filters::{parse_function_call, resolve_asl_intrinsic};

pub fn contains_template(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => s.contains("{{"),
        serde_json::Value::Object(map) => map.values().any(contains_template),
        serde_json::Value::Array(arr) => arr.iter().any(contains_template),
        _ => false,
    }
}

/// `true` if any template in `value` could reference the `state` root
/// (`{{ state.* }}`).
///
/// Conservative by design: a string merely *containing* `state` inside a
/// template triggers a hit, so the instance KV map is never skipped when it is
/// actually needed (no false negatives — at worst an unrelated `state`
/// substring causes a fetch that would have happened anyway). This lets callers
/// avoid an instance-KV round-trip for the common outputs-/context-only case.
#[must_use]
pub fn references_state(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => s.contains("{{") && s.contains("state"),
        serde_json::Value::Object(map) => map.values().any(references_state),
        serde_json::Value::Array(arr) => arr.iter().any(references_state),
        _ => false,
    }
}

/// Resolve `{{path}}` placeholders in a JSON value.
///
/// Supported path prefixes:
/// - `context.data.X` → `ExecutionContext.data["X"]`
/// - `context.config.X` → `ExecutionContext.config["X"]`
/// - `outputs.BLOCK_ID.field` → block output value
///
/// Returns the resolved value. Non-string values pass through unchanged.
pub fn resolve(
    value: &serde_json::Value,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    resolve_with_state(value, context, outputs, None)
}

/// Like [`resolve`] but with an optional pre-fetched instance KV state map.
/// When `state` is `Some`, paths like `{{ state.my_key }}` resolve from it.
pub fn resolve_with_state(
    value: &serde_json::Value,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<serde_json::Value, EngineError> {
    if !contains_template(value) {
        return Ok(value.clone());
    }
    resolve_inner(value, context, outputs, state)
}

/// Recursive worker for [`resolve_with_state`]. The `contains_template`
/// pre-check runs once at the top level: strings without `{{` resolve to
/// themselves anyway, so re-scanning every child's subtree would make
/// deeply nested params quadratic.
fn resolve_inner(
    value: &serde_json::Value,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<serde_json::Value, EngineError> {
    match value {
        serde_json::Value::String(s) => resolve_string(s, context, outputs, state),
        serde_json::Value::Object(map) => {
            let mut resolved = serde_json::Map::new();
            for (k, v) in map {
                resolved.insert(k.clone(), resolve_inner(v, context, outputs, state)?);
            }
            Ok(serde_json::Value::Object(resolved))
        }
        serde_json::Value::Array(arr) => {
            let resolved: Result<Vec<_>, _> = arr
                .iter()
                .map(|v| resolve_inner(v, context, outputs, state))
                .collect();
            Ok(serde_json::Value::Array(resolved?))
        }
        other => Ok(other.clone()),
    }
}

fn resolve_string(
    s: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<serde_json::Value, EngineError> {
    if let Some(result) = resolve_asl_intrinsic(s, context, outputs, state) {
        return result.map_err(|error| EngineError::TemplateError(error.0));
    }
    // Check if entire string is a single template expression (may contain
    // nested `{{`/`}}` inside quoted filter arguments like replace('{{x}}', ...)).
    if s.starts_with("{{") && s.ends_with("}}") {
        let inner = s[2..s.len() - 2].trim();
        if is_single_template_expr(inner) {
            return resolve_path(inner, context, outputs, state);
        }
    }

    // Inline templates: find balanced `{{ ... }}` expressions.
    let mut result = String::with_capacity(s.len());
    let mut remaining = s;
    while let Some(open) = remaining.find("{{") {
        result.push_str(&remaining[..open]);
        let after_open = &remaining[open + 2..];
        if let Some(close) = find_closing_braces(after_open) {
            let path = after_open[..close].trim();
            let resolved = resolve_path(path, context, outputs, state)?;
            match &resolved {
                serde_json::Value::String(s) => result.push_str(s),
                other => {
                    if other.is_object() || other.is_array() {
                        warn!(
                            template = %path,
                            value_type = if other.is_object() { "object" } else { "array" },
                            "template: interpolating complex value into string — did you mean to access a specific field?"
                        );
                    }
                    result.push_str(&other.to_string());
                }
            }
            remaining = &after_open[close + 2..];
        } else {
            result.push_str("{{");
            remaining = after_open;
        }
    }
    result.push_str(remaining);

    Ok(serde_json::Value::String(result))
}

fn resolve_path(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<serde_json::Value, EngineError> {
    crate::template_filters::resolve_path(path, context, outputs, state)
        .map_err(|error| EngineError::TemplateError(error.0))
}

pub(crate) fn try_resolve_single(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<Option<serde_json::Value>, EngineError> {
    crate::template_filters::try_resolve_single(path, context, outputs, state)
        .map_err(|error| EngineError::TemplateError(error.0))
}

// ---------------------------------------------------------------------------
// Static template validation
// ---------------------------------------------------------------------------

use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

#[derive(Debug, Clone)]
pub struct TemplateWarning {
    pub block_id: String,
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for TemplateWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}: {}", self.block_id, self.field, self.message)
    }
}

pub fn validate_sequence_templates(seq: &SequenceDefinition) -> Vec<TemplateWarning> {
    let mut warnings = Vec::new();
    for block in &seq.blocks {
        validate_block_templates(block, &mut warnings);
    }
    warnings
}

fn validate_block_templates(block: &BlockDefinition, warnings: &mut Vec<TemplateWarning>) {
    match block {
        BlockDefinition::Step(s) => {
            validate_json_templates(s.id.as_str(), "params", &s.params, warnings);
        }
        BlockDefinition::Parallel(p) => {
            for branch in &p.branches {
                for b in branch {
                    validate_block_templates(b, warnings);
                }
            }
        }
        BlockDefinition::Race(r) => {
            for branch in &r.branches {
                for b in branch {
                    validate_block_templates(b, warnings);
                }
            }
        }
        BlockDefinition::Loop(l) => {
            validate_template_string(l.id.as_str(), "condition", &l.condition, warnings);
            if let Some(ref break_on) = l.break_on {
                validate_template_string(l.id.as_str(), "break_on", break_on, warnings);
            }
            for b in &l.body {
                validate_block_templates(b, warnings);
            }
        }
        BlockDefinition::ForEach(fe) => {
            validate_template_string(fe.id.as_str(), "collection", &fe.collection, warnings);
            for b in &fe.body {
                validate_block_templates(b, warnings);
            }
        }
        BlockDefinition::Router(r) => {
            for route in &r.routes {
                validate_template_string(
                    r.id.as_str(),
                    "route.condition",
                    &route.condition,
                    warnings,
                );
                for b in &route.blocks {
                    validate_block_templates(b, warnings);
                }
            }
            if let Some(default) = &r.default {
                for b in default {
                    validate_block_templates(b, warnings);
                }
            }
        }
        BlockDefinition::TryCatch(tc) => {
            for b in &tc.try_block {
                validate_block_templates(b, warnings);
            }
            for b in &tc.catch_block {
                validate_block_templates(b, warnings);
            }
            if let Some(finally) = &tc.finally_block {
                for b in finally {
                    validate_block_templates(b, warnings);
                }
            }
        }
        BlockDefinition::SubSequence(_) => {}
        BlockDefinition::ABSplit(ab) => {
            for v in &ab.variants {
                for b in &v.blocks {
                    validate_block_templates(b, warnings);
                }
            }
        }
        BlockDefinition::CancellationScope(cs) => {
            for b in &cs.blocks {
                validate_block_templates(b, warnings);
            }
        }
        BlockDefinition::Saga(saga) => {
            for step in &saga.steps {
                validate_block_templates(&step.action, warnings);
                if let Some(comp) = &step.compensation {
                    validate_block_templates(comp, warnings);
                }
            }
        }
    }
}

fn validate_json_templates(
    block_id: &str,
    field: &str,
    value: &serde_json::Value,
    warnings: &mut Vec<TemplateWarning>,
) {
    match value {
        serde_json::Value::String(s) => {
            validate_template_string(block_id, field, s, warnings);
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child_field = format!("{field}.{k}");
                validate_json_templates(block_id, &child_field, v, warnings);
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let child_field = format!("{field}[{i}]");
                validate_json_templates(block_id, &child_field, v, warnings);
            }
        }
        _ => {}
    }
}

fn validate_template_string(
    block_id: &str,
    field: &str,
    s: &str,
    warnings: &mut Vec<TemplateWarning>,
) {
    if let Some(intrinsic) =
        Intrinsic::from_template(s).or_else(|| Intrinsic::from_callback_template(s))
    {
        if let Err(error) = intrinsic {
            warnings.push(TemplateWarning {
                block_id: block_id.to_string(),
                field: field.to_string(),
                message: error.to_string(),
            });
        }
        return;
    }
    let mut remainder = s;
    while let Some(start) = remainder.find("{{") {
        let after_open = &remainder[start + 2..];
        if let Some(end) = find_closing_braces(after_open) {
            let inner = after_open[..end].trim();
            validate_template_expr(block_id, field, inner, warnings);
            remainder = &after_open[end + 2..];
        } else {
            warnings.push(TemplateWarning {
                block_id: block_id.to_string(),
                field: field.to_string(),
                message: "unclosed template expression `{{` without matching `}}`".into(),
            });
            break;
        }
    }
}

fn validate_template_expr(
    block_id: &str,
    field: &str,
    inner: &str,
    warnings: &mut Vec<TemplateWarning>,
) {
    // `split_pipe_segments` always yields at least one segment.
    let segments = split_pipe_segments(inner);

    let first = segments[0].trim();
    if first.is_empty() {
        warnings.push(TemplateWarning {
            block_id: block_id.to_string(),
            field: field.to_string(),
            message: "empty template expression".into(),
        });
        return;
    }

    for (i, segment) in segments.iter().enumerate() {
        let seg = segment.trim();
        if seg.is_empty() {
            continue;
        }
        if i == 0 {
            if !is_template_path(seg) && !is_literal(seg) {
                warnings.push(TemplateWarning {
                    block_id: block_id.to_string(),
                    field: field.to_string(),
                    message: format!("unknown template root `{seg}`"),
                });
            }
            if let Some((func, _)) = parse_function_call(seg)
                && !is_known_template_function(func)
            {
                warnings.push(TemplateWarning {
                    block_id: block_id.to_string(),
                    field: field.to_string(),
                    message: format!("unknown template function `{func}()`"),
                });
            }
        } else if is_pipe_filter(seg) {
            // valid filter — nothing to warn about
        } else if is_template_path(seg) {
            // fallback-chain path — valid
        } else if is_literal(seg) {
            // literal fallback — valid
        } else {
            warnings.push(TemplateWarning {
                block_id: block_id.to_string(),
                field: field.to_string(),
                message: format!("unknown pipe filter or path `{seg}`"),
            });
        }
    }
}

fn is_known_template_function(name: &str) -> bool {
    matches!(name, "json" | "len")
}

fn is_literal(s: &str) -> bool {
    (s.starts_with('\'') && s.ends_with('\''))
        || (s.starts_with('"') && s.ends_with('"'))
        || s.parse::<f64>().is_ok()
        || s == "true"
        || s == "false"
        || s == "null"
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::ids::BlockId;
    use orch8_types::sequence::SequenceStatus;
    use serde_json::{Value, json};

    #[test]
    fn template_filter_fixtures_match_the_preview_contract() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/template_filter_parity.json"
        ))
        .unwrap();
        let context = ExecutionContext {
            data: fixture["input"].clone(),
            ..Default::default()
        };
        let outputs = json!({});
        assert_eq!(
            resolve(&fixture["templates"], &context, &outputs).unwrap(),
            fixture["expected"]
        );
        for invalid in fixture["invalid"].as_array().unwrap() {
            let error = resolve(&invalid["template"], &context, &outputs).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(invalid["message"].as_str().unwrap()),
                "{error}"
            );
        }
    }

    fn test_context() -> ExecutionContext {
        ExecutionContext {
            data: json!({"user": {"name": "Alice", "age": 30}}),
            config: json!({"api_key": "secret"}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        }
    }

    fn test_outputs() -> serde_json::Value {
        json!({
            "step_1": {"result": "ok", "count": 42}
        })
    }

    #[test]
    fn contains_template_detects_string() {
        assert!(contains_template(&json!("{{context.data.x}}")));
        assert!(contains_template(&json!("prefix {{x}} suffix")));
        assert!(!contains_template(&json!("no templates here")));
        assert!(!contains_template(&json!("")));
    }

    #[test]
    fn contains_template_detects_in_object_values() {
        assert!(contains_template(&json!({"a": "{{x}}"})));
        assert!(contains_template(&json!({"a": {"b": "{{x}}"}})));
        assert!(!contains_template(&json!({"a": "plain"})));
    }

    #[test]
    fn contains_template_detects_in_array() {
        assert!(contains_template(&json!(["a", "{{x}}"])));
        assert!(!contains_template(&json!(["a", "b"])));
    }

    #[test]
    fn contains_template_returns_false_for_non_string_leaves() {
        assert!(!contains_template(&json!(42)));
        assert!(!contains_template(&json!(true)));
        assert!(!contains_template(&json!(null)));
        assert!(!contains_template(&json!(2.72)));
    }

    #[test]
    fn references_state_detects_state_templates_no_false_negatives() {
        // Must fire whenever `{{ state.* }}` appears anywhere (nested too).
        assert!(references_state(&json!("{{ state.cursor }}")));
        assert!(references_state(&json!("{{state.x}}")));
        assert!(references_state(&json!({"a": {"b": "{{ state.k }}"}})));
        assert!(references_state(&json!(["x", "{{state.y}}"])));
    }

    #[test]
    fn references_state_false_for_non_state_templates() {
        assert!(!references_state(&json!("{{ outputs.step.result }}")));
        assert!(!references_state(&json!("{{ context.data.user }}")));
        assert!(!references_state(&json!("plain string, no template")));
        // `state` outside a template is not a reference.
        assert!(!references_state(&json!("state of the art")));
        assert!(!references_state(&json!(42)));
    }

    #[test]
    fn resolve_simple_path() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("{{context.data.user.name}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn resolve_output_path() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("{{outputs.step_1.count}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn resolve_inline_templates() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("Hello {{context.data.user.name}}, count is {{outputs.step_1.count}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Hello Alice, count is 42"));
    }

    #[test]
    fn resolve_nested_object() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!({
            "greeting": "Hi {{context.data.user.name}}",
            "key": "{{context.config.api_key}}"
        });
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!({"greeting": "Hi Alice", "key": "secret"}));
    }

    #[test]
    fn resolve_missing_path_returns_null() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("{{context.data.nonexistent}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, serde_json::Value::Null);
    }

    #[test]
    fn non_template_string_passes_through() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("plain string");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("plain string"));
    }

    #[test]
    fn numbers_pass_through() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!(42);
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn resolve_with_fallback_default() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("{{context.data.nonexistent | fallback_value}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("fallback_value"));
    }

    #[test]
    fn resolve_existing_value_ignores_fallback() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("{{context.data.user.name | default_name}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn resolve_inline_with_fallback() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("Hello {{context.data.missing|stranger}}!");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Hello stranger!"));
    }

    #[test]
    fn resolve_array_index_navigation() {
        let ctx = ExecutionContext {
            data: json!({"items": ["a", "b", "c"]}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let input = json!("{{context.data.items.1}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("b"));
    }

    #[test]
    fn resolve_inside_array_elements() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!(["literal", "{{context.data.user.name}}", 1]);
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(["literal", "Alice", 1]));
    }

    #[test]
    fn resolve_unknown_context_section_errors() {
        let ctx = test_context();
        let input = json!("{{context.bogus.x}}");
        let err = resolve(&input, &ctx, &json!({})).unwrap_err();
        assert!(
            matches!(err, EngineError::TemplateError(ref m) if m.contains("bogus")),
            "got: {err}"
        );
    }

    #[test]
    fn resolve_unknown_root_errors() {
        let ctx = test_context();
        let input = json!("{{something_random.x}}");
        let err = resolve(&input, &ctx, &json!({})).unwrap_err();
        assert!(matches!(err, EngineError::TemplateError(_)));
    }

    #[test]
    fn resolve_null_value_with_fallback_uses_fallback() {
        let ctx = ExecutionContext {
            data: json!({"maybe": null}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let input = json!("{{context.data.maybe | default}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("default"));
    }

    #[test]
    fn resolve_preserves_booleans_and_floats_in_single_expression() {
        let ctx = ExecutionContext {
            data: json!({"flag": true, "ratio": 2.5}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        assert_eq!(
            resolve(&json!("{{context.data.flag}}"), &ctx, &json!({})).unwrap(),
            json!(true)
        );
        assert_eq!(
            resolve(&json!("{{context.data.ratio}}"), &ctx, &json!({})).unwrap(),
            json!(2.5)
        );
    }

    #[test]
    fn resolve_preserves_number_type_whole_string() {
        let ctx = ExecutionContext {
            data: json!({"n": 7}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let result = resolve(&json!("{{context.data.n}}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!(7));
        assert!(result.is_number());
    }

    #[test]
    fn resolve_coerces_to_string_when_inlined() {
        let ctx = ExecutionContext {
            data: json!({"n": 7}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let result = resolve(&json!("count={{context.data.n}}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("count=7"));
    }

    #[test]
    fn resolve_preserves_object_type_whole_string() {
        let ctx = ExecutionContext {
            data: json!({"obj": {"x": 1, "y": 2}}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let result = resolve(&json!("{{context.data.obj}}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!({"x": 1, "y": 2}));
        assert!(result.is_object());
    }

    #[test]
    fn resolve_deeply_nested_template() {
        let ctx = ExecutionContext {
            data: json!({"a": {"b": {"c": "deep"}}}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let result = resolve(&json!("{{context.data.a.b.c}}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("deep"));
    }

    #[test]
    fn resolve_multiple_templates_in_single_string() {
        let ctx = ExecutionContext {
            data: json!({"first": "Alice", "last": "Smith"}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let input = json!("{{context.data.first}} {{context.data.last}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("Alice Smith"));
    }

    #[test]
    fn resolve_pipe_chain_uses_first_available() {
        let ctx = ExecutionContext {
            data: json!({"b": "from_b"}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let input = json!("{{context.data.a|context.data.b|literal_c}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("from_b"));
    }

    #[test]
    fn resolve_unicode_characters_preserved() {
        let ctx = ExecutionContext {
            data: json!({"greeting": "héllo 日本語 🦀"}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let result = resolve(&json!("{{context.data.greeting}}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("héllo 日本語 🦀"));
    }

    #[test]
    fn resolve_special_chars_preserved() {
        let ctx = ExecutionContext {
            data: json!({"s": "he said \"hi\" and \\path\\"}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let result = resolve(&json!("{{context.data.s}}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("he said \"hi\" and \\path\\"));
    }

    #[test]
    fn resolve_with_empty_context_returns_fallback() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let input = json!("{{context.data.anything|fallback}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("fallback"));

        let no_fallback = resolve(&json!("{{context.data.x}}"), &ctx, &json!({})).unwrap();
        assert_eq!(no_fallback, serde_json::Value::Null);
    }

    #[test]
    fn resolve_context_config_key() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({"api_key": "sk_123"}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let result = resolve(&json!("{{context.config.api_key}}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("sk_123"));
    }

    #[test]
    fn resolve_missing_root_returns_error() {
        let ctx = test_context();
        let err = resolve(&json!("{{foo.bar}}"), &ctx, &json!({})).unwrap_err();
        assert!(matches!(err, EngineError::TemplateError(_)));
    }

    #[test]
    fn resolve_missing_step_output_uses_fallback() {
        let ctx = test_context();
        let input = json!("{{outputs.missing_step.x|default}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("default"));
    }

    #[test]
    fn resolve_malformed_unclosed_template_preserved_literal() {
        let ctx = test_context();
        // No closing "}}" — leftover "{{" should be preserved as literal.
        let input = json!("before {{ unclosed");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("before {{ unclosed"));
    }

    // --- steps alias ---

    #[test]
    fn resolve_steps_alias_for_outputs() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("{{steps.step_1.count}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn resolve_steps_inline() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("Result: {{steps.step_1.result}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Result: ok"));
    }

    #[test]
    fn resolve_steps_missing_with_fallback() {
        let ctx = test_context();
        let outputs = test_outputs();
        let input = json!("{{steps.missing.x|default_val}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("default_val"));
    }

    // --- json() function ---

    #[test]
    fn resolve_json_function_object() {
        let ctx = test_context();
        let outputs = json!({"step_1": {"data": {"a": 1, "b": 2}}});
        let input = json!("{{json(outputs.step_1.data)}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("{\"a\":1,\"b\":2}"));
    }

    #[test]
    fn resolve_json_function_array() {
        let ctx = test_context();
        let outputs = json!({"step_1": {"items": [1, 2, 3]}});
        let input = json!("{{json(steps.step_1.items)}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("[1,2,3]"));
    }

    #[test]
    fn resolve_json_function_null() {
        let ctx = test_context();
        let input = json!("{{json(outputs.missing)}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("null"));
    }

    // --- len() function ---

    #[test]
    fn resolve_len_function_array() {
        let ctx = test_context();
        let outputs = json!({"step_1": {"items": [1, 2, 3, 4, 5]}});
        let input = json!("{{len(outputs.step_1.items)}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(5));
    }

    #[test]
    fn resolve_len_function_object() {
        let ctx = test_context();
        let outputs = json!({"step_1": {"data": {"a": 1, "b": 2}}});
        let input = json!("{{len(outputs.step_1.data)}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(2));
    }

    #[test]
    fn resolve_len_function_string() {
        let ctx = test_context();
        let outputs = json!({"step_1": {"name": "hello"}});
        let input = json!("{{len(steps.step_1.name)}}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(5));
    }

    #[test]
    fn resolve_len_function_null() {
        let ctx = test_context();
        let input = json!("{{len(outputs.missing)}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!(0));
    }

    #[test]
    fn resolve_len_inline() {
        let ctx = test_context();
        let outputs = json!({"step_1": {"items": [1, 2, 3]}});
        let input = json!("Found {{len(outputs.step_1.items)}} items");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Found 3 items"));
    }

    // --- instance_id ---

    #[test]
    fn resolve_instance_id() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext {
                instance_id: Some("abc-123".to_string()),
                ..Default::default()
            },
        };
        let input = json!("{{instance_id}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("abc-123"));
    }

    #[test]
    fn resolve_instance_id_inline() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext {
                instance_id: Some("xyz-789".to_string()),
                ..Default::default()
            },
        };
        let input = json!("Instance: {{instance_id}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("Instance: xyz-789"));
    }

    // --- input alias ---

    #[test]
    fn resolve_input_path() {
        let ctx = ExecutionContext {
            data: json!({"input": {"question": "Will X happen?", "market_id": "m123"}}),
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
        };
        let input = json!("{{input.question}}");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("Will X happen?"));
    }

    // --- unknown function ---

    #[test]
    fn resolve_unknown_function_errors() {
        let ctx = test_context();
        let input = json!("{{foo(outputs.step_1)}}");
        let err = resolve(&input, &ctx, &json!({})).unwrap_err();
        assert!(
            matches!(err, EngineError::TemplateError(ref m) if m.contains("unknown template function"))
        );
    }

    // --- replace() pipe filter ---

    #[test]
    fn resolve_replace_with_literal_search_and_path_replacement() {
        let ctx = test_context();
        let outputs = json!({
            "load_prompts": {"template": "Hello {{name}}, welcome!"},
            "data_step": {"user_name": "Alice"}
        });
        let input = json!(
            "{{ steps.load_prompts.template | replace('{{name}}', steps.data_step.user_name) }}"
        );
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Hello Alice, welcome!"));
    }

    #[test]
    fn resolve_replace_chained() {
        let ctx = test_context();
        let outputs = json!({
            "prompts": {"tpl": "Q: {{question}} P: {{price}}"},
            "market": {"q": "Will X happen?", "p": "0.65"}
        });
        let input = json!(
            "{{ steps.prompts.tpl | replace('{{question}}', steps.market.q) | replace('{{price}}', steps.market.p) }}"
        );
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Q: Will X happen? P: 0.65"));
    }

    #[test]
    fn resolve_replace_with_literal_replacement() {
        let ctx = test_context();
        let outputs = json!({"s": {"text": "hello world"}});
        let input = json!("{{ steps.s.text | replace('world', 'universe') }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("hello universe"));
    }

    #[test]
    fn resolve_replace_preserves_non_matching_text() {
        let ctx = test_context();
        let outputs = json!({"s": {"text": "no match here"}});
        let input = json!("{{ steps.s.text | replace('missing', 'found') }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("no match here"));
    }

    #[test]
    fn resolve_replace_rejects_empty_search() {
        let ctx = test_context();
        for input in [
            json!("{{ context.data.user.name | replace('', 'x') }}"),
            json!("{{ context.data.user.name | replace(, 'x') }}"),
        ] {
            let err = resolve(&input, &ctx, &json!({})).unwrap_err();
            assert!(
                matches!(err, EngineError::TemplateError(ref message) if message.contains("search string cannot be empty"))
            );
        }
    }

    #[test]
    fn resolve_replace_rejects_exponential_filter_expansion() {
        let ctx = ExecutionContext {
            data: json!({"text": "a"}),
            ..Default::default()
        };
        let filters = " | replace('a', 'aa')".repeat(23);
        let input = json!(format!("{{{{ context.data.text{filters} }}}}"));

        let err = resolve(&input, &ctx, &json!({})).unwrap_err();
        assert!(
            matches!(err, EngineError::TemplateError(ref message) if message.contains("filter result exceeds"))
        );
    }

    // --- upper filter ---

    #[test]
    fn filter_upper() {
        let ctx = test_context();
        let result = resolve(
            &json!("{{ context.data.user.name | upper }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("ALICE"));
    }

    #[test]
    fn filter_upper_non_string() {
        let ctx = ExecutionContext {
            data: json!({"n": 42}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(&json!("{{ context.data.n | upper }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!(""));
    }

    // --- lower filter ---

    #[test]
    fn filter_lower() {
        let ctx = ExecutionContext {
            data: json!({"country": "US"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.country | lower }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("us"));
    }

    // --- trim filter ---

    #[test]
    fn filter_trim() {
        let ctx = ExecutionContext {
            data: json!({"s": "  hello  "}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(&json!("{{ context.data.s | trim }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("hello"));
    }

    // --- abs filter ---

    #[test]
    fn filter_abs() {
        let ctx = ExecutionContext {
            data: json!({"delta": -5.3}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(&json!("{{ context.data.delta | abs }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!(5.3));
    }

    // --- default filter ---

    #[test]
    fn filter_default_on_null() {
        let ctx = ExecutionContext {
            data: json!({"currency": null}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.currency | default('USD') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("USD"));
    }

    #[test]
    fn filter_default_on_missing() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.missing | default('fallback') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("fallback"));
    }

    #[test]
    fn filter_default_with_lone_unmatched_quote_does_not_panic() {
        // A single unmatched quote char as the default() arg used to panic
        // in parse_literal_value's slice (s.len() - 1 underflow).
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            ..Default::default()
        };
        // Only asserting this doesn't panic; the exact Ok/Err outcome isn't load-bearing.
        let _ = resolve(
            &json!("{{ data.missing | default(') | zzz' }}"),
            &ctx,
            &json!({}),
        );
    }

    #[test]
    fn filter_default_on_existing() {
        let ctx = ExecutionContext {
            data: json!({"currency": "BRL"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.currency | default('USD') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("BRL"));
    }

    #[test]
    fn filter_default_on_empty_string() {
        let ctx = ExecutionContext {
            data: json!({"name": ""}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.name | default('Anonymous') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("Anonymous"));
    }

    // --- truncate filter ---

    #[test]
    fn filter_truncate() {
        let ctx = ExecutionContext {
            data: json!({"body": "Hello World, this is a long string"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.body | truncate(5, '...') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("Hello..."));
    }

    #[test]
    fn filter_truncate_short_string() {
        let ctx = ExecutionContext {
            data: json!({"s": "hi"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.s | truncate(10, '...') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("hi"));
    }

    // --- join filter ---

    #[test]
    fn filter_join() {
        let ctx = ExecutionContext {
            data: json!({"tags": ["a", "b", "c"]}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.tags | join(', ') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("a, b, c"));
    }

    // --- split filter ---

    #[test]
    fn filter_split() {
        let ctx = ExecutionContext {
            data: json!({"csv": "a,b,c"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.csv | split(',') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!(["a", "b", "c"]));
    }

    // --- url_encode filter ---

    #[test]
    fn filter_url_encode() {
        let ctx = ExecutionContext {
            data: json!({"q": "hello world&foo=bar"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.q | url_encode }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("hello%20world%26foo%3Dbar"));
    }

    // --- base64 filter ---

    #[test]
    fn filter_base64() {
        let ctx = ExecutionContext {
            data: json!({"key": "hello"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(&json!("{{ context.data.key | base64 }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("aGVsbG8="));
    }

    // --- base64_decode filter ---

    #[test]
    fn filter_base64_decode() {
        let ctx = ExecutionContext {
            data: json!({"token": "aGVsbG8="}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.token | base64_decode }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!("hello"));
    }

    // --- hash filter ---

    #[test]
    fn filter_hash_sha256() {
        let ctx = ExecutionContext {
            data: json!({"email": "test@example.com"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.email | hash('sha256') }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap().len(), 64);
    }

    // --- round filter ---

    #[test]
    fn filter_round() {
        let ctx = ExecutionContext {
            data: json!({"price": 3.14567}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.price | round(2) }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!(3.15));
    }

    // --- chained filters ---

    #[test]
    fn filter_chain_lower_then_default() {
        let ctx = ExecutionContext {
            data: json!({"email": "Alice@Example.COM"}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(&json!("{{ context.data.email | lower }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("alice@example.com"));
    }

    // --- root variable shortcuts ---

    #[test]
    fn resolve_config_shortcut() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({"api_key": "sk_123"}),
            ..Default::default()
        };
        let result = resolve(&json!("{{ config.api_key }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("sk_123"));
    }

    #[test]
    fn resolve_data_shortcut() {
        let ctx = ExecutionContext {
            data: json!({"user": {"name": "Bob"}}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(&json!("{{ data.user.name }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("Bob"));
    }

    #[test]
    fn resolve_runtime_attempt() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            runtime: orch8_types::context::RuntimeContext {
                attempt: 3,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = resolve(&json!("{{ runtime.attempt }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!(3));
    }

    #[test]
    fn resolve_runtime_instance_id() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            runtime: orch8_types::context::RuntimeContext {
                instance_id: Some("inst-42".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let result = resolve(&json!("{{ runtime.instance_id }}"), &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("inst-42"));
    }

    #[test]
    fn resolve_runtime_fast_path_scalar_fields() {
        let ctx = ExecutionContext {
            runtime: orch8_types::context::RuntimeContext {
                attempt: 7,
                total_steps_executed: 13,
                dry_run: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let out = json!({});
        assert_eq!(
            resolve(&json!("{{ runtime.total_steps_executed }}"), &ctx, &out).unwrap(),
            json!(13)
        );
        assert_eq!(
            resolve(&json!("{{ runtime.dry_run }}"), &ctx, &out).unwrap(),
            json!(true)
        );
        // Unknown field still routes through the fallback (no panic, no field).
        assert!(resolve(&json!("{{ runtime.attempt }}"), &ctx, &out).is_ok());
        assert_eq!(
            resolve(&json!("{{ runtime.attempt }}"), &ctx, &out).unwrap(),
            json!(7)
        );
    }

    // --- static template validation ---

    fn mk_step_block(id: &str, params: serde_json::Value) -> BlockDefinition {
        use orch8_types::sequence::StepDef;
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new(id),
            handler: "noop".into(),
            params,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            rate_budget: None,
            placement: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
            output_schema: None,
            when: None,
            compensation: None,
        }))
    }

    fn mk_seq(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
        SequenceDefinition {
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: orch8_types::ids::SequenceId::from_uuid(uuid::Uuid::nil()),
            tenant_id: orch8_types::ids::TenantId::unchecked("t"),
            namespace: orch8_types::ids::Namespace::new("ns"),
            name: "test".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks,
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            placement: None,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn validate_valid_templates_no_warnings() {
        let seq = mk_seq(vec![mk_step_block(
            "s1",
            json!({"url": "https://api.com/{{ context.data.id }}"}),
        )]);
        let warnings = validate_sequence_templates(&seq);
        assert!(
            warnings.is_empty(),
            "expected no warnings, got: {warnings:?}"
        );
    }

    #[test]
    fn validate_unknown_root_warns() {
        let seq = mk_seq(vec![mk_step_block(
            "s1",
            json!({"url": "{{ foobar.baz }}"}),
        )]);
        let warnings = validate_sequence_templates(&seq);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("unknown template root"));
    }

    #[test]
    fn validate_unknown_pipe_filter_warns() {
        let seq = mk_seq(vec![mk_step_block(
            "s1",
            json!({"v": "{{ context.data.x | bogus_filter }}"}),
        )]);
        let warnings = validate_sequence_templates(&seq);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("unknown pipe filter"));
    }

    #[test]
    fn validate_unclosed_template_warns() {
        let seq = mk_seq(vec![mk_step_block("s1", json!({"v": "hello {{ broken"}))]);
        let warnings = validate_sequence_templates(&seq);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("unclosed"));
    }

    #[test]
    fn validate_known_filters_no_warnings() {
        let seq = mk_seq(vec![mk_step_block(
            "s1",
            json!({"v": "{{ context.data.x | upper | trim }}"}),
        )]);
        let warnings = validate_sequence_templates(&seq);
        assert!(warnings.is_empty());
    }

    #[test]
    fn validate_loop_condition() {
        use orch8_types::sequence::LoopDef;
        let lp = BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("lp"),
            condition: "{{ unknown_root.x }}".into(),
            body: vec![],
            max_iterations: 5,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }));
        let seq = mk_seq(vec![lp]);
        let warnings = validate_sequence_templates(&seq);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].field, "condition");
    }

    // --- state.* template resolution tests ---

    #[test]
    fn resolve_state_top_level_key() {
        let ctx = test_context();
        let outputs = json!({});
        let state = json!({"color": "blue", "count": 42});
        let tmpl = json!("{{ state.color }}");
        let result = resolve_with_state(&tmpl, &ctx, &outputs, Some(&state)).unwrap();
        assert_eq!(result, json!("blue"));
    }

    #[test]
    fn resolve_state_nested_key() {
        let ctx = test_context();
        let outputs = json!({});
        let state = json!({"user": {"name": "Alice", "score": 7}});
        let tmpl = json!("{{ state.user.name }}");
        let result = resolve_with_state(&tmpl, &ctx, &outputs, Some(&state)).unwrap();
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn resolve_state_missing_key_returns_null() {
        let ctx = test_context();
        let outputs = json!({});
        let state = json!({"a": 1});
        let tmpl = json!("{{ state.nonexistent }}");
        let result = resolve_with_state(&tmpl, &ctx, &outputs, Some(&state)).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn resolve_state_none_returns_null() {
        let ctx = test_context();
        let outputs = json!({});
        let tmpl = json!("{{ state.anything }}");
        let result = resolve_with_state(&tmpl, &ctx, &outputs, None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn resolve_state_entire_object() {
        let ctx = test_context();
        let outputs = json!({});
        let state = json!({"x": 1, "y": 2});
        let tmpl = json!("{{ state }}");
        let result = resolve_with_state(&tmpl, &ctx, &outputs, Some(&state)).unwrap();
        assert_eq!(result, json!({"x": 1, "y": 2}));
    }

    #[test]
    fn resolve_state_in_object_params() {
        let ctx = test_context();
        let outputs = json!({});
        let state = json!({"greeting": "hello"});
        let tmpl = json!({"msg": "{{ state.greeting }}", "static": 42});
        let result = resolve_with_state(&tmpl, &ctx, &outputs, Some(&state)).unwrap();
        assert_eq!(result["msg"], json!("hello"));
        assert_eq!(result["static"], json!(42));
    }

    #[test]
    fn resolve_state_mixed_with_context() {
        let ctx = test_context();
        let outputs = json!({});
        let state = json!({"lang": "pt"});
        let tmpl = json!("{{ data.user.name }}-{{ state.lang }}");
        let result = resolve_with_state(&tmpl, &ctx, &outputs, Some(&state)).unwrap();
        assert_eq!(result, json!("Alice-pt"));
    }

    // --- object/array interpolation into strings (warning cases) ---

    #[test]
    fn inline_object_interpolation_produces_json_string() {
        let ctx = test_context();
        let outputs = json!({"get_price": {"price": 0.65, "ts": "2024-01-01"}});
        let input = json!("price: {{ steps.get_price }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        // Should still work — just serializes the object
        let s = result.as_str().unwrap();
        assert!(s.starts_with("price: {"));
        assert!(s.contains("\"price\""));
        assert!(s.contains("0.65"));
    }

    #[test]
    fn inline_array_interpolation_produces_json_string() {
        let ctx = test_context();
        let outputs = json!({"step": {"items": [1, 2, 3]}});
        let input = json!("items: {{ steps.step.items }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        let s = result.as_str().unwrap();
        assert!(s.starts_with("items: ["));
    }

    #[test]
    fn single_expr_object_returns_object_not_string() {
        let ctx = test_context();
        let outputs = json!({"get_price": {"price": 0.65, "ts": "2024-01-01"}});
        let input = json!("{{ steps.get_price }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        // Single expression should return the object directly, not a string
        assert!(result.is_object());
        assert_eq!(result["price"], json!(0.65));
    }

    #[test]
    fn single_expr_array_returns_array_not_string() {
        let ctx = test_context();
        let outputs = json!({"step": {"items": [1, 2, 3]}});
        let input = json!("{{ steps.step.items }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert!(result.is_array());
        assert_eq!(result, json!([1, 2, 3]));
    }

    #[test]
    fn inline_null_interpolation() {
        let ctx = test_context();
        let outputs = json!({"step": {"val": null}});
        let input = json!("value: {{ steps.step.val }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("value: null"));
    }

    #[test]
    fn inline_number_interpolation() {
        let ctx = test_context();
        let outputs = json!({"step": {"price": 0.65}});
        let input = json!("price: {{ steps.step.price }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("price: 0.65"));
    }

    #[test]
    fn inline_bool_interpolation() {
        let ctx = test_context();
        let outputs = json!({"step": {"active": true}});
        let input = json!("active: {{ steps.step.active }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("active: true"));
    }

    // --- edge cases for template resolution ---

    #[test]
    fn missing_step_output_in_arithmetic_context() {
        let ctx = test_context();
        let outputs = json!({"step": {"price": 0.5}});
        // Missing step output should resolve to null
        let input = json!("{{ steps.missing_step.value }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(null));
    }

    #[test]
    fn nested_field_access_on_null_returns_null() {
        let ctx = test_context();
        let outputs = json!({"step": {"data": null}});
        let input = json!("{{ steps.step.data.nested.field }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!(null));
    }

    #[test]
    fn multiple_inline_templates_with_mixed_types() {
        let ctx = test_context();
        let outputs = json!({
            "a": {"name": "test"},
            "b": {"count": 42}
        });
        let input = json!("Name: {{ steps.a.name }}, Count: {{ steps.b.count }}");
        let result = resolve(&input, &ctx, &outputs).unwrap();
        assert_eq!(result, json!("Name: test, Count: 42"));
    }

    // -----------------------------------------------------------------------
    // Warning-capture test infrastructure
    // -----------------------------------------------------------------------

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    struct CapturedWarning {
        message: String,
        fields: HashMap<String, String>,
    }

    struct WarningCapture {
        warnings: Arc<Mutex<Vec<CapturedWarning>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarningCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() == tracing::Level::WARN {
                let mut collector = FieldCollector::default();
                event.record(&mut collector);
                self.warnings.lock().unwrap().push(CapturedWarning {
                    message: collector.fields.remove("message").unwrap_or_default(),
                    fields: collector.fields,
                });
            }
        }
    }

    #[derive(Default)]
    struct FieldCollector {
        fields: HashMap<String, String>,
    }

    impl tracing::field::Visit for FieldCollector {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn capture_warnings<F: FnOnce()>(f: F) -> Vec<CapturedWarning> {
        use tracing_subscriber::layer::SubscriberExt;
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let layer = WarningCapture {
            warnings: Arc::clone(&warnings),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, f);
        let guard = warnings.lock().unwrap();
        guard.clone()
    }

    // -----------------------------------------------------------------------
    // Warning emission for complex value interpolation
    // -----------------------------------------------------------------------

    #[test]
    fn warn_on_object_interpolated_inline_with_fields() {
        let ctx = test_context();
        let outputs = json!({"get_price": {"price": 0.65, "ts": "2024-01-01"}});
        let input = json!("price: {{ steps.get_price }}");
        let warnings = capture_warnings(|| {
            let result = resolve(&input, &ctx, &outputs).unwrap();
            assert!(result.as_str().unwrap().starts_with("price: {"));
        });
        assert_eq!(warnings.len(), 1, "expected 1 warning, got: {warnings:?}");
        let w = &warnings[0];
        assert!(w.message.contains("interpolating complex value"));
        assert_eq!(
            w.fields.get("value_type").map(String::as_str),
            Some("object")
        );
    }

    #[test]
    fn warn_on_array_interpolated_inline_with_fields() {
        let ctx = test_context();
        let outputs = json!({"step": {"items": [1, 2, 3]}});
        let input = json!("items: {{ steps.step.items }}");
        let warnings = capture_warnings(|| {
            let result = resolve(&input, &ctx, &outputs).unwrap();
            assert!(result.as_str().unwrap().starts_with("items: ["));
        });
        assert_eq!(warnings.len(), 1);
        let w = &warnings[0];
        assert!(w.message.contains("interpolating complex value"));
        assert_eq!(
            w.fields.get("value_type").map(String::as_str),
            Some("array")
        );
    }

    #[test]
    fn warn_on_nested_object_interpolated_inline() {
        let ctx = test_context();
        let outputs = json!({"step": {"deep": {"a": {"b": 1}}}});
        let input = json!("val: {{ steps.step.deep }}");
        let warnings = capture_warnings(|| {
            resolve(&input, &ctx, &outputs).unwrap();
        });
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].fields.get("value_type").map(String::as_str),
            Some("object")
        );
    }

    #[test]
    fn warn_on_multiple_complex_interpolations_in_one_template() {
        let ctx = test_context();
        let outputs = json!({"s": {"obj": {"x": 1}, "arr": [1, 2]}});
        let input = json!("a={{ steps.s.obj }} b={{ steps.s.arr }}");
        let warnings = capture_warnings(|| {
            resolve(&input, &ctx, &outputs).unwrap();
        });
        assert_eq!(
            warnings.len(),
            2,
            "expected 2 warnings for two complex interpolations, got {}: {warnings:?}",
            warnings.len()
        );
        let types: Vec<&str> = warnings
            .iter()
            .filter_map(|w| w.fields.get("value_type").map(String::as_str))
            .collect();
        assert!(types.contains(&"object"));
        assert!(types.contains(&"array"));
    }

    // --- no false positives ---

    #[test]
    fn no_warn_when_object_is_single_expression() {
        let ctx = test_context();
        let outputs = json!({"get_price": {"price": 0.65}});
        let input = json!("{{ steps.get_price }}");
        let warnings = capture_warnings(|| {
            let result = resolve(&input, &ctx, &outputs).unwrap();
            assert!(result.is_object());
        });
        assert!(
            warnings.is_empty(),
            "single-expression object should not warn, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_when_array_is_single_expression() {
        let ctx = test_context();
        let outputs = json!({"step": {"items": [1, 2, 3]}});
        let input = json!("{{ steps.step.items }}");
        let warnings = capture_warnings(|| {
            let result = resolve(&input, &ctx, &outputs).unwrap();
            assert!(result.is_array());
        });
        assert!(
            warnings.is_empty(),
            "single-expression array should not warn, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_when_scalar_interpolated_inline() {
        let ctx = test_context();
        let outputs = json!({"step": {"price": 0.65}});
        let input = json!("price: {{ steps.step.price }}");
        let warnings = capture_warnings(|| {
            let result = resolve(&input, &ctx, &outputs).unwrap();
            assert_eq!(result, json!("price: 0.65"));
        });
        assert!(
            warnings.is_empty(),
            "scalar interpolation should not warn, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_when_string_interpolated_inline() {
        let ctx = test_context();
        let outputs = json!({"step": {"name": "test"}});
        let input = json!("name: {{ steps.step.name }}");
        let warnings = capture_warnings(|| {
            resolve(&input, &ctx, &outputs).unwrap();
        });
        assert!(
            warnings.is_empty(),
            "string interpolation should not warn, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_when_null_interpolated_inline() {
        let ctx = test_context();
        let outputs = json!({"step": {"val": null}});
        let input = json!("val: {{ steps.step.val }}");
        let warnings = capture_warnings(|| {
            resolve(&input, &ctx, &outputs).unwrap();
        });
        assert!(
            warnings.is_empty(),
            "null interpolation should not warn, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_when_bool_interpolated_inline() {
        let ctx = test_context();
        let outputs = json!({"step": {"active": true}});
        let input = json!("active: {{ steps.step.active }}");
        let warnings = capture_warnings(|| {
            resolve(&input, &ctx, &outputs).unwrap();
        });
        assert!(
            warnings.is_empty(),
            "bool interpolation should not warn, got: {warnings:?}"
        );
    }
}
