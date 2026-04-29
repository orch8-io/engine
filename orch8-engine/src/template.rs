use orch8_types::context::ExecutionContext;

use crate::error::EngineError;

pub fn contains_template(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => s.contains("{{"),
        serde_json::Value::Object(map) => map.values().any(contains_template),
        serde_json::Value::Array(arr) => arr.iter().any(contains_template),
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
    if !contains_template(value) {
        return Ok(value.clone());
    }
    match value {
        serde_json::Value::String(s) => resolve_string(s, context, outputs),
        serde_json::Value::Object(map) => {
            let mut resolved = serde_json::Map::new();
            for (k, v) in map {
                resolved.insert(k.clone(), resolve(v, context, outputs)?);
            }
            Ok(serde_json::Value::Object(resolved))
        }
        serde_json::Value::Array(arr) => {
            let resolved: Result<Vec<_>, _> =
                arr.iter().map(|v| resolve(v, context, outputs)).collect();
            Ok(serde_json::Value::Array(resolved?))
        }
        other => Ok(other.clone()),
    }
}

fn resolve_string(
    s: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    // Check if entire string is a single template expression.
    if s.starts_with("{{") && s.ends_with("}}") && s.matches("{{").count() == 1 {
        let path = s[2..s.len() - 2].trim();
        return resolve_path(path, context, outputs);
    }

    // Inline templates: build result string from segments to avoid O(n*m) reallocations.
    let mut result = String::with_capacity(s.len());
    let mut remaining = s;
    while let Some(open) = remaining.find("{{") {
        result.push_str(&remaining[..open]);
        let after_open = &remaining[open + 2..];
        if let Some(close) = after_open.find("}}") {
            let path = after_open[..close].trim();
            let resolved = resolve_path(path, context, outputs)?;
            match &resolved {
                serde_json::Value::String(s) => result.push_str(s),
                other => result.push_str(&other.to_string()),
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
) -> Result<serde_json::Value, EngineError> {
    // Support fallback chain: {{path|next|...|literal}}.
    // Segments that look like template paths (rooted in "context." or "outputs.")
    // are tried as path lookups; the first non-null wins. Any segment that is
    // not a known path root is treated as a literal string default.
    let segments: Vec<&str> = path.split('|').map(str::trim).collect();
    let first = segments[0];

    // Validate first segment is a known root (required — surfaces typos).
    if !is_template_path(first) {
        return Err(EngineError::TemplateError(format!(
            "unknown template root: {first}"
        )));
    }

    for segment in &segments {
        if is_template_path(segment) {
            if let Some(v) = try_resolve_single(segment, context, outputs)? {
                if !v.is_null() {
                    return Ok(v);
                }
            }
        } else {
            return Ok(serde_json::Value::String((*segment).to_string()));
        }
    }

    Ok(serde_json::Value::Null)
}

fn is_template_path(s: &str) -> bool {
    let root = s.split('.').next().unwrap_or("");
    root == "context" || root == "outputs"
}

fn try_resolve_single(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<Option<serde_json::Value>, EngineError> {
    let mut parts = path.split('.');
    let resolved = match parts.next() {
        Some("context") => {
            let section = parts.next().unwrap_or("");
            let source = match section {
                "data" => &context.data,
                "config" => &context.config,
                _ => {
                    return Err(EngineError::TemplateError(format!(
                        "unknown context section: {section}"
                    )));
                }
            };
            navigate_json(source, parts)
        }
        Some("outputs") => navigate_json(outputs, parts),
        _ => return Ok(None),
    };
    Ok(resolved.cloned())
}

/// Walk a JSON value by path segments, returning a borrow of the terminal
/// node. Returning `&Value` (rather than an owned clone) keeps traversal
/// zero-allocation; the single clone happens at the `resolve_path` boundary,
/// and only when the value is actually needed by the caller.
fn navigate_json<'a, 'v, I>(value: &'v serde_json::Value, path: I) -> Option<&'v serde_json::Value>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut current = value;
    for segment in path {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(segment)?;
            }
            serde_json::Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        assert!(!contains_template(&json!(3.14)));
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
}
