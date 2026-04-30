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
    // Check if entire string is a single template expression (may contain
    // nested `{{`/`}}` inside quoted filter arguments like replace('{{x}}', ...)).
    if s.starts_with("{{") && s.ends_with("}}") {
        let inner = s[2..s.len() - 2].trim();
        if is_single_template_expr(inner) {
            return resolve_path(inner, context, outputs);
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

/// Check if the inner content of `{{ ... }}` is a single template expression
/// (possibly with pipe filters) rather than containing multiple separate
/// template expressions. We check that the content between the outer `{{ }}`
/// doesn't contain unquoted `{{` which would indicate multiple expressions.
fn is_single_template_expr(inner: &str) -> bool {
    let mut in_quote = false;
    let mut quote_char = ' ';
    let bytes = inner.as_bytes();
    let mut i = 0;
    while i < bytes.len().saturating_sub(1) {
        let c = bytes[i] as char;
        if in_quote {
            if c == quote_char {
                in_quote = false;
            }
        } else if c == '\'' || c == '"' {
            in_quote = true;
            quote_char = c;
        } else if c == '{' && bytes[i + 1] == b'{' {
            return false;
        }
        i += 1;
    }
    true
}

/// Find the position of `}}` that closes a template expression, skipping
/// any `}}` that appear inside quoted strings (e.g. `replace('{{x}}', ...)`).
fn find_closing_braces(s: &str) -> Option<usize> {
    let mut in_quote = false;
    let mut quote_char = ' ';
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len().saturating_sub(1) {
        let c = bytes[i] as char;
        if in_quote {
            if c == quote_char {
                in_quote = false;
            }
        } else if c == '\'' || c == '"' {
            in_quote = true;
            quote_char = c;
        } else if c == '}' && bytes[i + 1] == b'}' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn resolve_path(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    // Split on `|` into segments. Segments starting with a known filter
    // function (e.g. `replace(...)`) are applied as transformations;
    // all others participate in fallback-chain resolution.
    let segments = split_pipe_segments(path);
    let first = segments[0].trim();

    if !is_template_path(first) {
        return Err(EngineError::TemplateError(format!(
            "unknown template root: {first}"
        )));
    }

    // Phase 1: resolve base value from fallback chain.
    let mut base_value = serde_json::Value::Null;
    let mut filter_start = segments.len();
    for (i, segment) in segments.iter().enumerate() {
        let seg = segment.trim();
        if is_pipe_filter(seg) {
            filter_start = i;
            break;
        }
        if is_template_path(seg) {
            if let Some(v) = try_resolve_single(seg, context, outputs)? {
                if !v.is_null() {
                    base_value = v;
                    filter_start = i + 1;
                    break;
                }
            }
        } else {
            base_value = serde_json::Value::String(seg.to_string());
            filter_start = i + 1;
            break;
        }
    }

    // Phase 2: apply pipe filters.
    let mut result = base_value;
    for segment in &segments[filter_start..] {
        let seg = segment.trim();
        if is_pipe_filter(seg) {
            result = apply_pipe_filter(seg, &result, context, outputs)?;
        }
    }

    Ok(result)
}

fn is_pipe_filter(s: &str) -> bool {
    s.starts_with("replace(")
}

fn split_pipe_segments(s: &str) -> Vec<&str> {
    let mut segments = Vec::new();
    let mut depth = 0u32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '|' if depth == 0 => {
                segments.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    segments.push(&s[start..]);
    segments
}

fn apply_pipe_filter(
    filter: &str,
    value: &serde_json::Value,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    if let Some(inner) = filter
        .strip_prefix("replace(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (search, replacement) = parse_replace_args(inner, context, outputs)?;
        let text = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        return Ok(serde_json::Value::String(
            text.replace(&search, &replacement),
        ));
    }
    Err(EngineError::TemplateError(format!(
        "unknown pipe filter: {filter}"
    )))
}

fn parse_replace_args(
    args: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<(String, String), EngineError> {
    // args looks like: 'search_string', replacement_path_or_literal
    // Find the comma that separates the two args (outside of quotes).
    let mut in_quote = false;
    let mut quote_char = ' ';
    let mut comma_pos = None;
    for (i, c) in args.char_indices() {
        if in_quote {
            if c == quote_char {
                in_quote = false;
            }
        } else if c == '\'' || c == '"' {
            in_quote = true;
            quote_char = c;
        } else if c == ',' {
            comma_pos = Some(i);
            break;
        }
    }
    let comma = comma_pos.ok_or_else(|| {
        EngineError::TemplateError(format!("replace() requires two arguments: {args}"))
    })?;
    let search_raw = args[..comma].trim();
    let replacement_raw = args[comma + 1..].trim();

    let search = unquote(search_raw);

    let replacement = if replacement_raw.starts_with('\'') || replacement_raw.starts_with('"') {
        unquote(replacement_raw)
    } else if is_template_path(replacement_raw) {
        match try_resolve_single(replacement_raw, context, outputs)? {
            Some(serde_json::Value::String(s)) => s,
            Some(other) => other.to_string(),
            None => String::new(),
        }
    } else {
        replacement_raw.to_string()
    };

    Ok((search, replacement))
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn is_template_path(s: &str) -> bool {
    // Strip leading function call for paths like "json(outputs.x.y)"
    let inner = strip_function_call(s).unwrap_or(s);
    let root = inner.split('.').next().unwrap_or("");
    root == "context"
        || root == "outputs"
        || root == "steps"
        || root == "input"
        || root == "instance_id"
}

fn try_resolve_single(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<Option<serde_json::Value>, EngineError> {
    // Check for function calls: json(path), len(path)
    if let Some((func, inner)) = parse_function_call(path) {
        let inner_val =
            try_resolve_single(inner, context, outputs)?.unwrap_or(serde_json::Value::Null);
        return Ok(Some(apply_template_function(func, &inner_val)?));
    }

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
        Some("outputs" | "steps") => navigate_json(outputs, parts),
        Some("input") => navigate_json(&context.data, std::iter::once("input").chain(parts)),
        Some("instance_id") => {
            let id_str = context.runtime.instance_id.as_deref().unwrap_or("");
            return Ok(Some(serde_json::Value::String(id_str.to_string())));
        }
        _ => return Ok(None),
    };
    Ok(resolved.cloned())
}

fn strip_function_call(s: &str) -> Option<&str> {
    let open = s.find('(')?;
    let close = s.rfind(')')?;
    if close > open {
        Some(&s[open + 1..close])
    } else {
        None
    }
}

fn parse_function_call(s: &str) -> Option<(&str, &str)> {
    let open = s.find('(')?;
    let close = s.rfind(')')?;
    if close <= open {
        return None;
    }
    let func = s[..open].trim();
    if func.is_empty() || !func.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    let inner = s[open + 1..close].trim();
    Some((func, inner))
}

fn apply_template_function(
    func: &str,
    value: &serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    match func {
        "json" => Ok(serde_json::Value::String(value.to_string())),
        "len" => {
            let n = match value {
                serde_json::Value::Array(a) => a.len(),
                serde_json::Value::Object(m) => m.len(),
                serde_json::Value::String(s) => s.len(),
                _ => 0,
            };
            Ok(serde_json::json!(n))
        }
        _ => Err(EngineError::TemplateError(format!(
            "unknown template function: {func}"
        ))),
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
        let input = json!("{{ steps.prompts.tpl | replace('{{question}}', steps.market.q) | replace('{{price}}', steps.market.p) }}");
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
}
