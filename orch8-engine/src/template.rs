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

    // Inline templates: replace each {{path}} with its string representation.
    let mut result = s.to_string();
    let mut start = 0;
    while let Some(open) = result[start..].find("{{") {
        let abs_open = start + open;
        if let Some(close) = result[abs_open..].find("}}") {
            let abs_close = abs_open + close;
            let path = result[abs_open + 2..abs_close].trim();
            let resolved = resolve_path(path, context, outputs)?;
            let replacement = match &resolved {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            result.replace_range(abs_open..abs_close + 2, &replacement);
            start = abs_open + replacement.len();
        } else {
            break;
        }
    }

    Ok(serde_json::Value::String(result))
}

fn resolve_path(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    let parts: Vec<&str> = path.split('.').collect();

    let resolved = match parts.first().copied() {
        Some("context") => {
            let section = parts.get(1).copied().unwrap_or("");
            let source = match section {
                "data" => &context.data,
                "config" => &context.config,
                _ => {
                    return Err(EngineError::TemplateError(format!(
                        "unknown context section: {section}"
                    )));
                }
            };
            navigate_json(source, &parts[2..])
        }
        Some("outputs") => navigate_json(outputs, &parts[1..]),
        _ => {
            return Err(EngineError::TemplateError(format!(
                "unknown template root: {path}"
            )));
        }
    };

    Ok(resolved.unwrap_or(serde_json::Value::Null))
}

fn navigate_json(value: &serde_json::Value, path: &[&str]) -> Option<serde_json::Value> {
    let mut current = value;
    for &segment in path {
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
    Some(current.clone())
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
            runtime: Default::default(),
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
}
