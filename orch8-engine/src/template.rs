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
    match value {
        serde_json::Value::String(s) => resolve_string(s, context, outputs, state),
        serde_json::Value::Object(map) => {
            let mut resolved = serde_json::Map::new();
            for (k, v) in map {
                resolved.insert(k.clone(), resolve_with_state(v, context, outputs, state)?);
            }
            Ok(serde_json::Value::Object(resolved))
        }
        serde_json::Value::Array(arr) => {
            let resolved: Result<Vec<_>, _> = arr
                .iter()
                .map(|v| resolve_with_state(v, context, outputs, state))
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
    state: Option<&serde_json::Value>,
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
            if let Some(v) = try_resolve_single(seg, context, outputs, state)? {
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
            result = apply_pipe_filter(seg, &result, context, outputs, state)?;
        }
    }

    Ok(result)
}

fn is_pipe_filter(s: &str) -> bool {
    matches!(
        s,
        "upper" | "lower" | "trim" | "abs" | "url_encode" | "base64" | "base64_decode"
    ) || s.starts_with("replace(")
        || s.starts_with("default(")
        || s.starts_with("truncate(")
        || s.starts_with("join(")
        || s.starts_with("split(")
        || s.starts_with("hash(")
        || s.starts_with("round(")
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
    state: Option<&serde_json::Value>,
) -> Result<serde_json::Value, EngineError> {
    if let Some(inner) = filter
        .strip_prefix("replace(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (search, replacement) = parse_replace_args(inner, context, outputs, state)?;
        let text = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        return Ok(serde_json::Value::String(
            text.replace(&search, &replacement),
        ));
    }
    match filter {
        "upper" => Ok(serde_json::json!(value
            .as_str()
            .unwrap_or("")
            .to_uppercase())),
        "lower" => Ok(serde_json::json!(value
            .as_str()
            .unwrap_or("")
            .to_lowercase())),
        "trim" => Ok(serde_json::json!(value.as_str().unwrap_or("").trim())),
        "abs" => Ok(serde_json::json!(value.as_f64().unwrap_or(0.0).abs())),
        "url_encode" => Ok(serde_json::json!(url_encode_str(
            value.as_str().unwrap_or("")
        ))),
        "base64" => {
            use base64::Engine;
            let s = value.as_str().unwrap_or("");
            Ok(serde_json::json!(
                base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
            ))
        }
        "base64_decode" => {
            use base64::Engine;
            let s = value.as_str().unwrap_or("");
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|e| EngineError::TemplateError(format!("base64_decode: {e}")))?;
            Ok(serde_json::json!(String::from_utf8(decoded)
                .unwrap_or_else(
                    |e| String::from_utf8_lossy(e.as_bytes()).into_owned()
                )))
        }
        _ => apply_pipe_filter_with_args(filter, value),
    }
}

fn apply_pipe_filter_with_args(
    filter: &str,
    value: &serde_json::Value,
) -> Result<serde_json::Value, EngineError> {
    if let Some(inner) = filter
        .strip_prefix("default(")
        .and_then(|s| s.strip_suffix(')'))
    {
        if value.is_null() || (value.as_str().is_some_and(str::is_empty)) {
            return Ok(parse_literal_value(inner.trim()));
        }
        return Ok(value.clone());
    }
    if let Some(inner) = filter
        .strip_prefix("truncate(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (n, suffix) = parse_truncate_args(inner)?;
        let s = value.as_str().unwrap_or("");
        return Ok(serde_json::json!(if s.chars().count() > n {
            format!("{}{suffix}", s.chars().take(n).collect::<String>())
        } else {
            s.to_string()
        }));
    }
    if let Some(inner) = filter
        .strip_prefix("join(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let sep = unquote(inner.trim());
        let arr = value.as_array().cloned().unwrap_or_default();
        let joined: String = arr
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(&sep);
        return Ok(serde_json::json!(joined));
    }
    if let Some(inner) = filter
        .strip_prefix("split(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let sep = unquote(inner.trim());
        let s = value.as_str().unwrap_or("");
        let parts: Vec<serde_json::Value> = s
            .split(&sep)
            .map(|p| serde_json::Value::String(p.to_string()))
            .collect();
        return Ok(serde_json::Value::Array(parts));
    }
    if let Some(inner) = filter
        .strip_prefix("hash(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let algo = unquote(inner.trim());
        let s = value.as_str().unwrap_or("");
        return Ok(serde_json::json!(hash_string(s, &algo)?));
    }
    if let Some(inner) = filter
        .strip_prefix("round(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let decimals: u32 = inner
            .trim()
            .parse()
            .map_err(|_| EngineError::TemplateError("round() requires integer arg".into()))?;
        let n = value.as_f64().unwrap_or(0.0);
        let factor = 10_f64.powi(decimals as i32);
        return Ok(serde_json::json!((n * factor).round() / factor));
    }
    Err(EngineError::TemplateError(format!(
        "unknown pipe filter: {filter}"
    )))
}

fn parse_literal_value(s: &str) -> serde_json::Value {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        return serde_json::Value::String(s[1..s.len() - 1].to_string());
    }
    if let Ok(n) = s.parse::<f64>() {
        return serde_json::json!(n);
    }
    match s {
        "true" => serde_json::Value::Bool(true),
        "false" => serde_json::Value::Bool(false),
        "null" => serde_json::Value::Null,
        _ => serde_json::Value::String(s.to_string()),
    }
}

fn parse_truncate_args(args: &str) -> Result<(usize, String), EngineError> {
    let parts = split_filter_args(args);
    let n: usize = parts
        .first()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| EngineError::TemplateError("truncate() requires length".into()))?;
    let suffix = parts.get(1).map(|s| unquote(s.trim())).unwrap_or_default();
    Ok((n, suffix))
}

fn split_filter_args(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut in_quote = false;
    let mut quote_char = ' ';
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if in_quote {
            if c == quote_char {
                in_quote = false;
            }
        } else if c == '\'' || c == '"' {
            in_quote = true;
            quote_char = c;
        } else if c == ',' {
            parts.push(&s[start..i]);
            start = i + 1;
        }
    }
    parts.push(&s[start..]);
    parts
}

fn hash_string(s: &str, algo: &str) -> Result<String, EngineError> {
    use sha2::Digest;
    match algo {
        "sha256" => {
            let result = sha2::Sha256::digest(s.as_bytes());
            Ok(format!("{result:x}"))
        }
        _ => Err(EngineError::TemplateError(format!(
            "unsupported hash algorithm: {algo}"
        ))),
    }
}

fn url_encode_str(s: &str) -> String {
    use std::fmt::Write;
    let mut encoded = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                let _ = write!(encoded, "%{byte:02X}");
            }
        }
    }
    encoded
}

fn parse_replace_args(
    args: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
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
        match try_resolve_single(replacement_raw, context, outputs, state)? {
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
    matches!(
        root,
        "context"
            | "outputs"
            | "steps"
            | "input"
            | "instance_id"
            | "config"
            | "data"
            | "runtime"
            | "state"
    )
}

fn try_resolve_single(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<Option<serde_json::Value>, EngineError> {
    // Check for function calls: json(path), len(path)
    if let Some((func, inner)) = parse_function_call(path) {
        let inner_val =
            try_resolve_single(inner, context, outputs, state)?.unwrap_or(serde_json::Value::Null);
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
        Some("config") => navigate_json(&context.config, parts),
        Some("data") => navigate_json(&context.data, parts),
        Some("runtime") => {
            let rt_json = serde_json::to_value(&context.runtime).unwrap_or(serde_json::Value::Null);
            let remaining: Vec<&str> = parts.collect();
            if remaining.is_empty() {
                return Ok(Some(rt_json));
            }
            return Ok(navigate_json_owned(&rt_json, &remaining));
        }
        Some("state") => {
            let empty = serde_json::Value::Object(serde_json::Map::new());
            let source = state.unwrap_or(&empty);
            let remaining: Vec<&str> = parts.collect();
            if remaining.is_empty() {
                return Ok(Some(source.clone()));
            }
            return Ok(navigate_json_owned(source, &remaining));
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

fn navigate_json_owned(value: &serde_json::Value, path: &[&str]) -> Option<serde_json::Value> {
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
            validate_json_templates(&s.id.0, "params", &s.params, warnings);
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
            validate_expr_string(&l.id.0, "condition", &l.condition, warnings);
            if let Some(ref break_on) = l.break_on {
                validate_expr_string(&l.id.0, "break_on", break_on, warnings);
            }
            for b in &l.body {
                validate_block_templates(b, warnings);
            }
        }
        BlockDefinition::ForEach(fe) => {
            validate_template_string(&fe.id.0, "collection", &fe.collection, warnings);
            for b in &fe.body {
                validate_block_templates(b, warnings);
            }
        }
        BlockDefinition::Router(r) => {
            for route in &r.routes {
                validate_expr_string(&r.id.0, "route.condition", &route.condition, warnings);
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
    let mut remainder = s;
    while let Some(start) = remainder.find("{{") {
        let after_open = &remainder[start + 2..];
        match find_closing_braces(after_open) {
            Some(end) => {
                let inner = after_open[..end].trim();
                validate_template_expr(block_id, field, inner, warnings);
                remainder = &after_open[end + 2..];
            }
            None => {
                warnings.push(TemplateWarning {
                    block_id: block_id.to_string(),
                    field: field.to_string(),
                    message: "unclosed template expression `{{` without matching `}}`".into(),
                });
                break;
            }
        }
    }
}

fn validate_expr_string(block_id: &str, field: &str, s: &str, warnings: &mut Vec<TemplateWarning>) {
    validate_template_string(block_id, field, s, warnings);
}

fn validate_template_expr(
    block_id: &str,
    field: &str,
    inner: &str,
    warnings: &mut Vec<TemplateWarning>,
) {
    let segments = split_pipe_segments(inner);
    if segments.is_empty() {
        return;
    }

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
            if let Some((func, _)) = parse_function_call(seg) {
                if !is_known_template_function(func) {
                    warnings.push(TemplateWarning {
                        block_id: block_id.to_string(),
                        field: field.to_string(),
                        message: format!("unknown template function `{func}()`"),
                    });
                }
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
    use serde_json::{json, Value};

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
        assert!(result.as_str().unwrap().len() == 64);
    }

    // --- round filter ---

    #[test]
    fn filter_round() {
        let ctx = ExecutionContext {
            data: json!({"price": 3.14159}),
            config: json!({}),
            ..Default::default()
        };
        let result = resolve(
            &json!("{{ context.data.price | round(2) }}"),
            &ctx,
            &json!({}),
        )
        .unwrap();
        assert_eq!(result, json!(3.14));
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

    // --- static template validation ---

    fn mk_step_block(id: &str, params: serde_json::Value) -> BlockDefinition {
        use orch8_types::sequence::StepDef;
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId(id.into()),
            handler: "noop".into(),
            params,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        }))
    }

    fn mk_seq(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
        SequenceDefinition {
            id: orch8_types::ids::SequenceId(uuid::Uuid::nil()),
            tenant_id: orch8_types::ids::TenantId("t".into()),
            namespace: orch8_types::ids::Namespace("ns".into()),
            name: "test".into(),
            version: 1,
            deprecated: false,
            blocks,
            interceptors: None,
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
            id: BlockId("lp".into()),
            condition: "{{ unknown_root.x }}".into(),
            body: vec![],
            max_iterations: 5,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
        }));
        let seq = mk_seq(vec![lp]);
        let warnings = validate_sequence_templates(&seq);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].field == "condition");
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
}
