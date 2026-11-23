//! Pure template filters and path resolution used by both execution runtimes.

use orch8_types::asl_intrinsic::Intrinsic;
use orch8_types::context::ExecutionContext;

#[derive(Debug)]
pub(crate) struct TemplateFailure(pub(crate) String);

/// Hard ceiling for the value produced by any individual template filter.
///
/// Filters operate on workflow-controlled input and several of them can expand
/// it. Keeping every intermediate value bounded prevents a short filter chain
/// from exhausting the worker's memory before downstream payload limits run.
const MAX_TEMPLATE_FILTER_RESULT_BYTES: usize = 4 * 1024 * 1024;

pub(crate) fn resolve_asl_intrinsic(
    source: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Option<Result<serde_json::Value, TemplateFailure>> {
    let read = |path: &str| {
        try_resolve_single(path, context, outputs, state)
            .ok()
            .flatten()
    };
    if let Some(expression) = Intrinsic::from_callback_template(source) {
        return Some(
            expression
                .and_then(|expression| expression.bind_callback(read))
                .map_err(|error| TemplateFailure(error.to_string()))
                .and_then(|bound| {
                    serde_json::to_value(bound).map_err(|error| TemplateFailure(error.to_string()))
                }),
        );
    }
    Intrinsic::from_template(source).map(|expression| {
        expression
            .and_then(|expression| expression.evaluate(read))
            .map_err(|error| TemplateFailure(error.to_string()))
    })
}

/// Quote-aware scan shared by the template scanners below. Iterates `s` with
/// byte indices while tracking single/double-quote state, calling `f` for each
/// character with its byte index and whether it sits inside a quoted span.
/// Returning `false` from `f` stops the scan early. Note: like the original
/// hand-rolled loops this has no escape handling — a `\'` inside a
/// single-quoted span still toggles quote state. That historical behavior is
/// preserved deliberately.
pub(crate) fn scan_quote_aware(s: &str, mut f: impl FnMut(usize, char, bool) -> bool) {
    let mut in_quote = false;
    let mut quote_char = ' ';
    for (i, c) in s.char_indices() {
        if !f(i, c, in_quote) {
            return;
        }
        if in_quote {
            if c == quote_char {
                in_quote = false;
            }
        } else if c == '\'' || c == '"' {
            in_quote = true;
            quote_char = c;
        }
    }
}

/// Check if the inner content of `{{ ... }}` is a single template expression
/// (possibly with pipe filters) rather than containing multiple separate
/// template expressions. We check that the content between the outer `{{ }}`
/// doesn't contain unquoted `{{` which would indicate multiple expressions.
pub(crate) fn is_single_template_expr(inner: &str) -> bool {
    let mut single = true;
    scan_quote_aware(inner, |i, c, in_quote| {
        if !in_quote && c == '{' && inner[i..].starts_with("{{") {
            single = false;
            return false;
        }
        true
    });
    single
}

/// Find the position of `}}` that closes a template expression, skipping
/// any `}}` that appear inside quoted strings (e.g. `replace('{{x}}', ...)`).
pub(crate) fn find_closing_braces(s: &str) -> Option<usize> {
    let mut pos = None;
    scan_quote_aware(s, |i, c, in_quote| {
        if !in_quote && c == '}' && s[i..].starts_with("}}") {
            pos = Some(i);
            return false;
        }
        true
    });
    pos
}

pub(crate) fn resolve_path(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<serde_json::Value, TemplateFailure> {
    // Split on `|` into segments. Segments starting with a known filter
    // function (e.g. `replace(...)`) are applied as transformations;
    // all others participate in fallback-chain resolution.
    let segments = split_pipe_segments(path);
    let first = segments[0].trim();

    if !is_template_path(first) {
        return Err(TemplateFailure(format!("unknown template root: {first}")));
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
            if let Some(v) = try_resolve_single(seg, context, outputs, state)?
                && !v.is_null()
            {
                base_value = v;
                filter_start = i + 1;
                break;
            }
        } else {
            // Parse quoted-string/number literals the same way the `default()`
            // filter and the static validator (`is_literal`) treat them, so
            // `{{ data.missing | 'USD' }}` yields `USD` (no quotes) and
            // `{{ data.missing | 42 }}` yields a number. Bare words stay
            // plain strings (`| fallback_value` → "fallback_value").
            base_value = parse_literal_value(seg);
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
            ensure_filter_result_size(&result)?;
        }
    }

    Ok(result)
}

pub(crate) fn is_pipe_filter(s: &str) -> bool {
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

pub(crate) fn split_pipe_segments(s: &str) -> Vec<&str> {
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
) -> Result<serde_json::Value, TemplateFailure> {
    if let Some(inner) = filter
        .strip_prefix("replace(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (search, replacement) = parse_replace_args(inner, context, outputs, state)?;
        let text = match value {
            serde_json::Value::String(s) => std::borrow::Cow::Borrowed(s.as_str()),
            other => std::borrow::Cow::Owned(other.to_string()),
        };
        return Ok(serde_json::Value::String(bounded_replace(
            text.as_ref(),
            &search,
            &replacement,
        )?));
    }
    match filter {
        "upper" => Ok(serde_json::json!(
            value.as_str().unwrap_or("").to_uppercase()
        )),
        "lower" => Ok(serde_json::json!(
            value.as_str().unwrap_or("").to_lowercase()
        )),
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
                .map_err(|e| TemplateFailure(format!("base64_decode: {e}")))?;
            Ok(serde_json::json!(
                String::from_utf8(decoded)
                    .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
            ))
        }
        _ => apply_pipe_filter_with_args(filter, value),
    }
}

fn bounded_replace(text: &str, search: &str, replacement: &str) -> Result<String, TemplateFailure> {
    if search.is_empty() {
        return Err(TemplateFailure(
            "replace() search string cannot be empty".into(),
        ));
    }

    let matches = text.matches(search).count();
    let removed = matches.checked_mul(search.len());
    let added = matches.checked_mul(replacement.len());
    let output_len = removed
        .and_then(|removed| text.len().checked_sub(removed))
        .and_then(|retained| added.and_then(|added| retained.checked_add(added)))
        .ok_or_else(filter_result_too_large)?;

    if output_len > MAX_TEMPLATE_FILTER_RESULT_BYTES {
        return Err(filter_result_too_large());
    }

    Ok(text.replace(search, replacement))
}

fn ensure_filter_result_size(value: &serde_json::Value) -> Result<(), TemplateFailure> {
    let exceeds_limit = match value {
        serde_json::Value::String(s) => s.len() > MAX_TEMPLATE_FILTER_RESULT_BYTES,
        // Collection-producing filters are uncommon. Counting their encoded
        // size avoids allocating a second copy merely to enforce the limit.
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            struct ByteCounter(usize);

            impl std::io::Write for ByteCounter {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    self.0 = self.0.saturating_add(bytes.len());
                    Ok(bytes.len())
                }

                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }

            let mut counter = ByteCounter(0);
            serde_json::to_writer(&mut counter, value).is_err()
                || counter.0 > MAX_TEMPLATE_FILTER_RESULT_BYTES
        }
        _ => false,
    };

    if exceeds_limit {
        return Err(filter_result_too_large());
    }
    Ok(())
}

fn filter_result_too_large() -> TemplateFailure {
    TemplateFailure(format!(
        "template filter result exceeds {MAX_TEMPLATE_FILTER_RESULT_BYTES} byte limit"
    ))
}

fn apply_pipe_filter_with_args(
    filter: &str,
    value: &serde_json::Value,
) -> Result<serde_json::Value, TemplateFailure> {
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
        // Single pass: stop after n chars and check whether anything remains.
        let mut chars = s.chars();
        let head: String = chars.by_ref().take(n).collect();
        return Ok(serde_json::json!(if chars.next().is_some() {
            format!("{head}{suffix}")
        } else {
            s.to_string()
        }));
    }
    if let Some(inner) = filter
        .strip_prefix("join(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let sep = unquote(inner.trim());
        let arr: &[serde_json::Value] = value.as_array().map_or(&[], Vec::as_slice);
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
            .map_err(|_| TemplateFailure("round() requires integer arg".into()))?;
        let n = value.as_f64().unwrap_or(0.0);
        #[allow(clippy::cast_possible_wrap)]
        let factor = 10_f64.powi(decimals as i32);
        return Ok(serde_json::json!((n * factor).round() / factor));
    }
    Err(TemplateFailure(format!("unknown pipe filter: {filter}")))
}

fn parse_literal_value(s: &str) -> serde_json::Value {
    let s = s.trim();
    if s.len() >= 2
        && ((s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')))
    {
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

fn parse_truncate_args(args: &str) -> Result<(usize, String), TemplateFailure> {
    let parts = split_filter_args(args);
    let n: usize = parts
        .first()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| TemplateFailure("truncate() requires length".into()))?;
    let suffix = parts.get(1).map(|s| unquote(s.trim())).unwrap_or_default();
    Ok((n, suffix))
}

fn split_filter_args(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    scan_quote_aware(s, |i, c, in_quote| {
        if !in_quote && c == ',' {
            parts.push(&s[start..i]);
            start = i + 1;
        }
        true
    });
    parts.push(&s[start..]);
    parts
}

/// Lowercase hex-encode bytes without pulling in the `hex` crate.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hash_string(s: &str, algo: &str) -> Result<String, TemplateFailure> {
    use sha2::Digest;
    match algo {
        "sha256" => {
            let result = sha2::Sha256::digest(s.as_bytes());
            Ok(to_hex(&result))
        }
        _ => Err(TemplateFailure(format!(
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
) -> Result<(String, String), TemplateFailure> {
    // args looks like: 'search_string', replacement_path_or_literal
    // Find the comma that separates the two args (outside of quotes).
    let mut comma_pos = None;
    scan_quote_aware(args, |i, c, in_quote| {
        if !in_quote && c == ',' {
            comma_pos = Some(i);
            return false;
        }
        true
    });
    let comma = comma_pos
        .ok_or_else(|| TemplateFailure(format!("replace() requires two arguments: {args}")))?;
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
    if s.len() >= 2
        && ((s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')))
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

pub(crate) fn is_template_path(s: &str) -> bool {
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

pub(crate) fn try_resolve_single(
    path: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
    state: Option<&serde_json::Value>,
) -> Result<Option<serde_json::Value>, TemplateFailure> {
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
                    return Err(TemplateFailure(format!(
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
            let rt = &context.runtime;
            let remaining: Vec<&str> = parts.collect();
            // Fast-path single scalar fields straight from the typed struct,
            // avoiding a full `serde_json::to_value(RuntimeContext)` on every
            // `{{ runtime.* }}` access. Anything else falls back to the
            // serialize-then-navigate path so semantics stay identical —
            // including `instance_id`, which is `skip_serializing_if None`, so
            // its absent case must resolve to "not found" (None), not Null.
            if remaining.len() == 1 {
                let fast = match remaining[0] {
                    "attempt" => Some(serde_json::Value::from(rt.attempt)),
                    "total_steps_executed" => {
                        Some(serde_json::Value::from(rt.total_steps_executed))
                    }
                    "dry_run" => Some(serde_json::Value::from(rt.dry_run)),
                    "dry_run_auto_approve" => {
                        Some(serde_json::Value::from(rt.dry_run_auto_approve))
                    }
                    "instance_id" => rt.instance_id.as_deref().map(serde_json::Value::from),
                    _ => None,
                };
                if let Some(v) = fast {
                    return Ok(Some(v));
                }
            }
            let rt_json = serde_json::to_value(rt).unwrap_or(serde_json::Value::Null);
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

pub(crate) fn parse_function_call(s: &str) -> Option<(&str, &str)> {
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
) -> Result<serde_json::Value, TemplateFailure> {
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
        _ => Err(TemplateFailure(format!(
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
    navigate_json(value, path.iter().copied()).cloned()
}
