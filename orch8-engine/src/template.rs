use orch8_types::context::ExecutionContext;

use crate::error::EngineError;

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
    // Support fallback defaults: {{path|default_value}}
    let (path, fallback) = if let Some(idx) = path.find('|') {
        (path[..idx].trim(), Some(path[idx + 1..].trim()))
    } else {
        (path, None)
    };

    // Walk the path as an iterator — avoids allocating a Vec<&str> per call,
    // which matters on hot templating paths with many {{...}} expressions.
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
        _ => {
            return Err(EngineError::TemplateError(format!(
                "unknown template root: {path}"
            )));
        }
    };

    match resolved {
        Some(v) if !v.is_null() => Ok(v.clone()),
        _ => {
            if let Some(default) = fallback {
                Ok(serde_json::Value::String(default.to_string()))
            } else {
                Ok(serde_json::Value::Null)
            }
        }
    }
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
    fn resolve_malformed_unclosed_template_preserved_literal() {
        let ctx = test_context();
        // No closing "}}" — leftover "{{" should be preserved as literal.
        let input = json!("before {{ unclosed");
        let result = resolve(&input, &ctx, &json!({})).unwrap();
        assert_eq!(result, json!("before {{ unclosed"));
    }
}
