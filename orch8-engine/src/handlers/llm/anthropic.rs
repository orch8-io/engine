use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::common::{
    classify_api_error, classify_reqwest_error, extract_system_message, is_json_object_format,
    merge_json_response_fields, permanent, retryable, safe_truncate,
};
use super::sse::{SseParser, charge_stream_bytes, next_chunk, stream_idle_timeout};
use super::{DeltaSink, http_client};

/// Messages API version header. Still the current (and only) version.
const ANTHROPIC_VERSION: &str = "2023-06-01";
/// Default endpoint (fallbacks are a first-party Claude API feature).
const DEFAULT_ANTHROPIC_BASE: &str = "https://api.anthropic.com/v1";
/// Beta enabling `fallbacks: "default"` (server-side refusal fallbacks).
const SERVER_SIDE_FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";
/// Default output cap: generous enough not to truncate adaptive thinking plus
/// a normal reply, while keeping non-streaming requests under HTTP timeouts.
const DEFAULT_MAX_TOKENS: u64 = 16_000;

/// Models that reject non-default sampling params (`temperature`, `top_p`,
/// `top_k`) with a 400: Opus 4.7+, Sonnet 5, and the Fable/Mythos tier.
fn rejects_sampling(model: &str) -> bool {
    [
        "claude-opus-5",
        "claude-opus-4-7",
        "claude-opus-4-8",
        "claude-sonnet-5",
        "claude-fable",
        "claude-mythos",
    ]
    .iter()
    .any(|prefix| model.starts_with(prefix))
}

/// Models on which server-side refusal fallbacks are enabled by default.
fn fallbacks_by_default(model: &str) -> bool {
    matches!(model, "claude-opus-5" | "claude-fable-5-1")
}

/// `OpenAI`-shaped function tools → Messages API tools. Native Anthropic tool
/// definitions (and server tools) pass through unchanged.
fn to_anthropic_tools(tools: &Value) -> Value {
    let Some(arr) = tools.as_array() else {
        return tools.clone();
    };
    Value::Array(
        arr.iter()
            .map(|t| {
                let Some(f) = t
                    .get("function")
                    .filter(|_| t.get("type").and_then(Value::as_str) == Some("function"))
                else {
                    return t.clone();
                };
                let mut tool = Map::new();
                tool.insert("name".into(), f.get("name").cloned().unwrap_or_default());
                if let Some(desc) = f.get("description") {
                    tool.insert("description".into(), desc.clone());
                }
                tool.insert(
                    "input_schema".into(),
                    f.get("parameters")
                        .cloned()
                        .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                );
                if let Some(strict) = f.get("strict") {
                    tool.insert("strict".into(), strict.clone());
                }
                Value::Object(tool)
            })
            .collect(),
    )
}

/// `OpenAI` `tool_choice` → Messages API `tool_choice`.
fn to_anthropic_tool_choice(choice: &Value) -> Value {
    match choice {
        Value::String(s) => match s.as_str() {
            "required" | "any" => json!({"type": "any"}),
            "none" => json!({"type": "none"}),
            _ => json!({"type": "auto"}),
        },
        Value::Object(o) if o.get("type").and_then(Value::as_str) == Some("function") => {
            json!({"type": "tool", "name": o.get("function").and_then(|f| f.get("name"))})
        }
        other => other.clone(),
    }
}

/// Convert one conversation message to Messages API shape. Handles the
/// `OpenAI`-shaped tool loop produced by the `agent` handler: an assistant
/// turn replays its raw `anthropic_content` blocks when present (so thinking
/// blocks go back unchanged, as the API requires) or is rebuilt from
/// `content` + `tool_calls`; a `role: tool` result becomes a `tool_result`
/// block. Everything else goes through the multimodal image conversion.
fn to_anthropic_message(msg: &Value) -> Value {
    match msg.get("role").and_then(Value::as_str) {
        Some("assistant") => {
            if let Some(raw) = msg.get("anthropic_content").filter(|c| c.is_array()) {
                return json!({"role": "assistant", "content": raw});
            }
            let Some(calls) = msg.get("tool_calls").and_then(Value::as_array) else {
                return super::multimodal::to_anthropic_message(msg);
            };
            let mut blocks = Vec::new();
            if let Some(text) = msg
                .get("content")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
            {
                blocks.push(json!({"type": "text", "text": text}));
            }
            for call in calls {
                let f = call.get("function");
                let input = match f.and_then(|f| f.get("arguments")) {
                    Some(Value::String(s)) => serde_json::from_str(s).unwrap_or_else(|_| json!({})),
                    Some(v) if v.is_object() => v.clone(),
                    _ => json!({}),
                };
                blocks.push(json!({
                    "type": "tool_use",
                    "id": call.get("id"),
                    "name": f.and_then(|f| f.get("name")),
                    "input": input,
                }));
            }
            json!({"role": "assistant", "content": blocks})
        }
        Some("tool") => json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": msg.get("tool_call_id"),
                "content": msg.get("content").cloned().unwrap_or_else(|| json!("")),
            }],
        }),
        _ => super::multimodal::to_anthropic_message(msg),
    }
}

/// Convert the conversation, merging consecutive tool results into one user
/// message (parallel tool results must come back together).
fn to_anthropic_messages(messages: &Value) -> Value {
    let Some(arr) = messages.as_array() else {
        return messages.clone();
    };
    let mut out: Vec<Value> = Vec::with_capacity(arr.len());
    for msg in arr {
        let converted = to_anthropic_message(msg);
        let is_tool_result = msg.get("role").and_then(Value::as_str) == Some("tool");
        if is_tool_result
            && let Some(prev) = out.last_mut()
            && prev.get("role").and_then(Value::as_str) == Some("user")
            && prev
                .get("content")
                .and_then(Value::as_array)
                .and_then(|c| c.first())
                .and_then(|b| b.get("type"))
                .and_then(Value::as_str)
                == Some("tool_result")
            && let (Some(prev_blocks), Some(new_blocks)) = (
                prev.get_mut("content").and_then(Value::as_array_mut),
                converted.get("content").and_then(Value::as_array),
            )
        {
            prev_blocks.extend(new_blocks.iter().cloned());
            continue;
        }
        out.push(converted);
    }
    Value::Array(out)
}

/// Build the `/messages` request body shared by the streaming and
/// non-streaming paths (so message conversion behaves identically).
fn build_body(params: &Value, model: &str) -> Map<String, Value> {
    let messages_raw = params.get("messages").cloned().unwrap_or(json!([]));
    let (system_from_msgs, messages) = extract_system_message(&messages_raw);
    let messages = to_anthropic_messages(&messages);

    let max_tokens = params
        .get("max_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_TOKENS);

    let mut body = serde_json::Map::new();
    body.insert("model".into(), json!(model));
    body.insert("messages".into(), messages);
    body.insert("max_tokens".into(), json!(max_tokens));

    if let Some(sys) = params.get("system") {
        body.insert("system".into(), sys.clone());
    } else if let Some(sys) = system_from_msgs {
        body.insert("system".into(), Value::String(sys));
    }

    for &key in &[
        "temperature",
        "top_p",
        "top_k",
        "stop_sequences",
        "metadata",
        "thinking",
        "output_config",
    ] {
        if let Some(val) = params.get(key) {
            body.insert(key.into(), val.clone());
        }
    }
    if let Some(tools) = params.get("tools") {
        body.insert("tools".into(), to_anthropic_tools(tools));
    }
    if let Some(choice) = params.get("tool_choice") {
        body.insert("tool_choice".into(), to_anthropic_tool_choice(choice));
    }
    // `effort` shorthand → `output_config.effort` (explicit output_config wins).
    if let Some(effort) = params.get("effort").filter(|e| e.is_string())
        && let Some(cfg) = body
            .entry("output_config")
            .or_insert_with(|| json!({}))
            .as_object_mut()
    {
        cfg.entry("effort").or_insert_with(|| effort.clone());
    }
    if rejects_sampling(model) {
        let dropped: Vec<&str> = ["temperature", "top_p", "top_k"]
            .into_iter()
            .filter(|k| body.remove(*k).is_some())
            .collect();
        if !dropped.is_empty() {
            warn!(
                model,
                ?dropped,
                "llm_call: model rejects sampling params; dropped"
            );
        }
    }
    body
}

/// Server-side refusal fallbacks: on by default for the models that need
/// them at the first-party endpoint; `fallbacks: false` opts out, any other
/// explicit `fallbacks` value passes through.
fn apply_fallbacks(
    body: &mut Map<String, Value>,
    params: &Value,
    model: &str,
    base_url: &str,
) -> bool {
    match params.get("fallbacks") {
        Some(Value::Bool(false)) => false,
        Some(explicit) if !explicit.is_null() && !explicit.is_boolean() => {
            body.insert("fallbacks".into(), explicit.clone());
            true
        }
        _ if fallbacks_by_default(model)
            && base_url.trim_end_matches('/') == DEFAULT_ANTHROPIC_BASE =>
        {
            body.insert("fallbacks".into(), json!("default"));
            true
        }
        _ => false,
    }
}

pub(super) async fn call_anthropic(
    params: &Value,
    api_key: &str,
    base_url: &str,
    deltas: Option<&DeltaSink>,
) -> Result<Value, StepError> {
    let url = format!("{base_url}/messages");

    let model = super::resolve_model(params, "anthropic")?;
    let model = model.as_str();

    let mut body = build_body(params, model);
    if deltas.is_some() {
        body.insert("stream".into(), json!(true));
    }
    let fallbacks = apply_fallbacks(&mut body, params, model, base_url);
    let body = Value::Object(body);

    debug!(url = %url, model = %model, streaming = deltas.is_some(), fallbacks, "llm_call: Anthropic");

    let mut req = http_client()
        .post(&url)
        .header("x-api-key", api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("Content-Type", "application/json");
    if fallbacks {
        req = req.header("anthropic-beta", SERVER_SIDE_FALLBACK_BETA);
    }
    let resp = req
        .json(&body)
        .send()
        .await
        .map_err(|e| classify_reqwest_error(&e))?;

    if let Some(sink) = deltas {
        return consume_anthropic_stream(resp, params, sink).await;
    }

    let status = resp.status().as_u16();
    let resp_body: Value = super::read_json_capped(resp).await?;

    if status >= 400 {
        return Err(classify_api_error(status, &resp_body));
    }

    refusal_error(&resp_body)?;
    let mut output = normalize_anthropic_response(&resp_body);

    if is_json_object_format(params)
        && let Some(content_owned) = output
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
            .map(String::from)
    {
        merge_json_response_fields(&content_owned, &mut output);
    }

    Ok(output)
}

/// Accumulator for the Anthropic streaming event protocol. Rebuilds the
/// complete (non-streaming-shaped) `/messages` response body so the final
/// output goes through the exact same [`normalize_anthropic_response`] path.
#[derive(Default)]
struct AnthropicStreamAcc {
    model: Value,
    /// Content blocks keyed by stream `index`.
    blocks: BTreeMap<u64, Value>,
    /// Accumulated `input_json_delta` fragments per `tool_use` block index.
    partial_tool_json: BTreeMap<u64, String>,
    /// Merged usage: `message_start` provides `input_tokens`, the final
    /// `message_delta` overlays the authoritative `output_tokens`.
    usage: Map<String, Value>,
    stop_reason: Value,
    done: bool,
}

impl AnthropicStreamAcc {
    /// Ingest one event payload, publishing text deltas to `sink`.
    /// Returns an error for explicit `error` events from the provider.
    fn ingest(&mut self, data: &str, sink: &DeltaSink) -> Result<(), StepError> {
        let Ok(event) = serde_json::from_str::<Value>(data) else {
            warn!(
                data_preview = %safe_truncate(data, 200),
                "llm_call: skipping unparseable Anthropic streaming event"
            );
            return Ok(());
        };
        match event.get("type").and_then(Value::as_str).unwrap_or("") {
            "message_start" => {
                if let Some(message) = event.get("message") {
                    if let Some(model) = message.get("model").filter(|m| m.is_string()) {
                        self.model = model.clone();
                    }
                    self.merge_usage(message.get("usage"));
                }
            }
            "content_block_start" => {
                if let (Some(index), Some(block)) = (
                    event.get("index").and_then(Value::as_u64),
                    event.get("content_block"),
                ) {
                    self.blocks.insert(index, block.clone());
                }
            }
            "content_block_delta" => self.ingest_block_delta(&event, sink),
            "content_block_stop" => {
                if let Some(index) = event.get("index").and_then(Value::as_u64) {
                    self.finalize_tool_input(index);
                }
            }
            "message_delta" => {
                if let Some(reason) = event
                    .get("delta")
                    .and_then(|d| d.get("stop_reason"))
                    .filter(|r| !r.is_null())
                {
                    self.stop_reason = reason.clone();
                }
                self.merge_usage(event.get("usage"));
            }
            "message_stop" => self.done = true,
            "error" => return Err(classify_stream_error(event.get("error"))),
            // `ping` and unknown / future event types are ignored.
            _ => {}
        }
        Ok(())
    }

    fn ingest_block_delta(&mut self, event: &Value, sink: &DeltaSink) {
        let Some(index) = event.get("index").and_then(Value::as_u64) else {
            return;
        };
        let Some(delta) = event.get("delta") else {
            return;
        };
        match delta.get("type").and_then(Value::as_str).unwrap_or("") {
            "text_delta" => {
                if let Some(text) = delta.get("text").and_then(Value::as_str) {
                    if let Some(Value::String(existing)) =
                        self.blocks.get_mut(&index).and_then(|b| b.get_mut("text"))
                    {
                        existing.push_str(text);
                    }
                    if !text.is_empty() {
                        sink.publish(text);
                    }
                }
            }
            "input_json_delta" => {
                if let Some(fragment) = delta.get("partial_json").and_then(Value::as_str) {
                    self.partial_tool_json
                        .entry(index)
                        .or_default()
                        .push_str(fragment);
                }
            }
            // Thinking blocks must be replayable byte-for-byte in tool loops,
            // so their text and signature are accumulated (not published —
            // they are not user-visible output).
            kind @ ("thinking_delta" | "signature_delta") => {
                let field = if kind == "thinking_delta" {
                    "thinking"
                } else {
                    "signature"
                };
                if let (Some(piece), Some(block)) = (
                    delta.get(field).and_then(Value::as_str),
                    self.blocks.get_mut(&index).and_then(Value::as_object_mut),
                ) {
                    let slot = block
                        .entry(field)
                        .or_insert_with(|| Value::String(String::new()));
                    if let Value::String(existing) = slot {
                        existing.push_str(piece);
                    } else {
                        *slot = Value::String(piece.to_owned());
                    }
                }
            }
            _ => {}
        }
    }

    /// Parse the accumulated `input_json_delta` fragments of a `tool_use`
    /// block into its `input` field.
    fn finalize_tool_input(&mut self, index: u64) {
        let Some(fragments) = self.partial_tool_json.remove(&index) else {
            return;
        };
        if fragments.is_empty() {
            return;
        }
        match serde_json::from_str::<Value>(&fragments) {
            Ok(input) => {
                if let Some(block) = self.blocks.get_mut(&index) {
                    block["input"] = input;
                }
            }
            Err(e) => warn!(
                error = %e,
                "llm_call: accumulated tool_use input is not valid JSON"
            ),
        }
    }

    fn merge_usage(&mut self, usage: Option<&Value>) {
        if let Some(map) = usage.and_then(Value::as_object) {
            for (k, v) in map {
                self.usage.insert(k.clone(), v.clone());
            }
        }
    }

    /// Rebuild the non-streaming response body and normalize it.
    fn into_output(self, params: &Value) -> Value {
        let content: Vec<Value> = self.blocks.into_values().collect();
        let rebuilt = json!({
            "content": content,
            "model": self.model,
            "stop_reason": self.stop_reason,
            "usage": Value::Object(self.usage),
        });
        let mut output = normalize_anthropic_response(&rebuilt);
        if is_json_object_format(params)
            && let Some(content_owned) = output
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(Value::as_str)
                .map(String::from)
        {
            merge_json_response_fields(&content_owned, &mut output);
        }
        output
    }
}

/// Map a mid-stream `error` event to the retry taxonomy: capacity/server
/// conditions are retryable (and eligible for provider failover); request,
/// auth and permission errors are permanent.
fn classify_stream_error(error: Option<&Value>) -> StepError {
    let error_type = error
        .and_then(|e| e.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let message = error
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    match error_type {
        "invalid_request_error"
        | "authentication_error"
        | "permission_error"
        | "not_found_error"
        | "request_too_large" => permanent(format!("stream error ({error_type}): {message}")),
        // overloaded_error, api_error, rate_limit_error, timeout_error, …
        _ => retryable(format!("stream error ({error_type}): {message}")),
    }
}

/// Consume an Anthropic SSE event stream, publishing text deltas and
/// rebuilding the full response.
///
/// Termination contract: the provider must send `message_stop`. A stream
/// that ends before that is incomplete and fails **retryable**; a chunk gap
/// longer than the idle timeout fails retryable via [`next_chunk`].
async fn consume_anthropic_stream(
    mut resp: reqwest::Response,
    params: &Value,
    sink: &DeltaSink,
) -> Result<Value, StepError> {
    let status = resp.status().as_u16();
    if status >= 400 {
        // Error responses are plain JSON, not SSE.
        let resp_body: Value = super::read_json_capped(resp).await?;
        return Err(classify_api_error(status, &resp_body));
    }

    let idle_timeout = stream_idle_timeout(params);
    let mut parser = SseParser::default();
    let mut acc = AnthropicStreamAcc::default();

    let mut received = 0usize;
    while let Some(chunk) = next_chunk(&mut resp, idle_timeout).await? {
        charge_stream_bytes(&mut received, chunk.len())?;
        for event in parser.push(&chunk) {
            acc.ingest(&event.data, sink)?;
        }
        if acc.done {
            break;
        }
    }

    if !acc.done {
        return Err(retryable(
            "provider stream ended before message_stop — response is incomplete".to_string(),
        ));
    }

    if acc.stop_reason.as_str() == Some("refusal") {
        return Err(StepError::Permanent {
            message: "model declined the request (refusal)".into(),
            details: None,
        });
    }
    Ok(acc.into_output(params))
}

/// A `refusal` stop (safety classifiers declined; HTTP 200) carries no usable
/// content — fail permanently with the category instead of returning an
/// empty "success".
fn refusal_error(resp_body: &Value) -> Result<(), StepError> {
    if resp_body.get("stop_reason").and_then(Value::as_str) != Some("refusal") {
        return Ok(());
    }
    let details = resp_body.get("stop_details").cloned();
    let category = details
        .as_ref()
        .and_then(|d| d.get("category"))
        .and_then(Value::as_str)
        .unwrap_or("unspecified");
    Err(StepError::Permanent {
        message: format!("model declined the request (refusal, category: {category})"),
        details,
    })
}

fn normalize_anthropic_response(resp_body: &Value) -> Value {
    let content = resp_body.get("content").cloned().unwrap_or_default();

    let text = content
        .as_array()
        .and_then(|arr| {
            arr.iter()
                .find(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        })
        .and_then(|b| b.get("text"))
        .cloned()
        .unwrap_or_default();

    let tool_calls: Vec<Value> = content
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("tool_use"))
                .map(|b| {
                    json!({
                        "id": b.get("id"),
                        "type": "function",
                        "function": {
                            "name": b.get("name"),
                            "arguments": serde_json::to_string(
                                b.get("input").unwrap_or(&Value::Null)
                            ).unwrap_or_else(|_| "null".to_string()),
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let mut message = serde_json::Map::new();
    message.insert("role".into(), json!("assistant"));
    message.insert("content".into(), json!(text));
    if !tool_calls.is_empty() {
        message.insert("tool_calls".into(), json!(tool_calls));
    }
    // Raw blocks (thinking, tool_use, …) so a tool loop can replay this turn
    // unchanged — the API rejects dropped or edited thinking blocks.
    if content.as_array().is_some_and(|blocks| {
        blocks
            .iter()
            .any(|b| b.get("type").and_then(Value::as_str) != Some("text"))
    }) {
        message.insert("anthropic_content".into(), content.clone());
    }

    json!({
        "provider": "anthropic",
        "model": resp_body.get("model").cloned().unwrap_or_default(),
        "message": message,
        "finish_reason": resp_body.get("stop_reason").cloned().unwrap_or_default(),
        "usage": resp_body.get("usage").cloned().unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_anthropic_text_response() {
        let resp = json!({
            "content": [{"type": "text", "text": "Hello"}],
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5},
        });
        let output = normalize_anthropic_response(&resp);
        assert_eq!(output["provider"], "anthropic");
        assert_eq!(output["message"]["content"], "Hello");
        assert_eq!(output["message"]["role"], "assistant");
        assert!(output["message"].get("tool_calls").is_none());
    }

    #[test]
    fn normalize_anthropic_tool_use_response() {
        let resp = json!({
            "content": [
                {"type": "text", "text": "I'll search for that."},
                {"type": "tool_use", "id": "tc_1", "name": "search", "input": {"q": "rust"}},
            ],
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 20, "output_tokens": 15},
        });
        let output = normalize_anthropic_response(&resp);
        let tool_calls = output["message"]["tool_calls"].as_array().unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0]["function"]["name"], "search");
        assert_eq!(tool_calls[0]["type"], "function");
    }

    #[test]
    fn normalize_anthropic_empty_content() {
        let resp = json!({"model": "claude-sonnet-4-20250514"});
        let output = normalize_anthropic_response(&resp);
        assert_eq!(output["provider"], "anthropic");
    }

    #[test]
    fn classify_stream_error_taxonomy() {
        let overloaded = json!({"type": "overloaded_error", "message": "busy"});
        assert!(matches!(
            classify_stream_error(Some(&overloaded)),
            StepError::Retryable { .. }
        ));
        let invalid = json!({"type": "invalid_request_error", "message": "bad"});
        assert!(matches!(
            classify_stream_error(Some(&invalid)),
            StepError::Permanent { .. }
        ));
        assert!(matches!(
            classify_stream_error(None),
            StepError::Retryable { .. }
        ));
    }

    #[test]
    fn sampling_params_dropped_only_for_models_that_reject_them() {
        let params = json!({"temperature": 0.2, "top_p": 0.9, "top_k": 5, "messages": []});
        let body = build_body(&params, "claude-opus-5");
        assert!(body.get("temperature").is_none() && body.get("top_k").is_none());
        let body = build_body(&params, "claude-haiku-4-5");
        assert_eq!(body["temperature"], 0.2);
    }

    #[test]
    fn default_max_tokens_and_effort_shorthand() {
        let body = build_body(&json!({"effort": "low", "messages": []}), "claude-opus-5");
        assert_eq!(body["max_tokens"], DEFAULT_MAX_TOKENS);
        assert_eq!(body["output_config"]["effort"], "low");
        // An explicit output_config wins over the shorthand.
        let body = build_body(
            &json!({"effort": "low", "output_config": {"effort": "high"}, "messages": []}),
            "claude-opus-5",
        );
        assert_eq!(body["output_config"]["effort"], "high");
    }

    #[test]
    fn openai_shaped_tools_and_tool_choice_are_translated() {
        let params = json!({
            "messages": [],
            "tools": [
                {"type": "function", "function": {"name": "search", "description": "d",
                    "parameters": {"type": "object", "properties": {"q": {"type": "string"}}}}},
                {"name": "native", "input_schema": {"type": "object"}}
            ],
            "tool_choice": "required",
        });
        let body = build_body(&params, "claude-opus-5");
        assert_eq!(body["tools"][0]["name"], "search");
        assert_eq!(
            body["tools"][0]["input_schema"]["properties"]["q"]["type"],
            "string"
        );
        assert!(body["tools"][0].get("function").is_none());
        assert_eq!(body["tools"][1]["name"], "native");
        assert_eq!(body["tool_choice"], json!({"type": "any"}));
        assert_eq!(
            to_anthropic_tool_choice(&json!({"type": "function", "function": {"name": "search"}})),
            json!({"type": "tool", "name": "search"})
        );
    }

    #[test]
    fn tool_loop_conversation_round_trips_to_messages_api() {
        let raw = json!([
            {"type": "thinking", "thinking": "", "signature": "sig"},
            {"type": "tool_use", "id": "t1", "name": "search", "input": {"q": "x"}}
        ]);
        let messages = json!([
            {"role": "user", "content": "find x"},
            // A turn produced by this adapter: replayed verbatim (thinking kept).
            {"role": "assistant", "content": "", "tool_calls": [], "anthropic_content": raw},
            {"role": "tool", "tool_call_id": "t1", "content": "r1"},
            // A turn from an OpenAI-compatible provider (failover): rebuilt.
            {"role": "assistant", "content": "checking", "tool_calls": [
                {"id": "t2", "type": "function", "function": {"name": "search", "arguments": "{\"q\":\"y\"}"}},
                {"id": "t3", "type": "function", "function": {"name": "search", "arguments": "{}"}}
            ]},
            {"role": "tool", "tool_call_id": "t2", "content": "r2"},
            {"role": "tool", "tool_call_id": "t3", "content": "r3"}
        ]);
        let out = to_anthropic_messages(&messages);
        let out = out.as_array().unwrap();
        assert_eq!(
            out.len(),
            5,
            "parallel tool results merge into one user turn"
        );
        assert_eq!(out[1]["content"], raw);
        assert_eq!(out[2]["content"][0]["type"], "tool_result");
        assert_eq!(out[2]["content"][0]["tool_use_id"], "t1");
        assert_eq!(
            out[3]["content"][0],
            json!({"type": "text", "text": "checking"})
        );
        assert_eq!(out[3]["content"][1]["input"], json!({"q": "y"}));
        let merged = out[4]["content"].as_array().unwrap();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[1]["tool_use_id"], "t3");
    }

    #[test]
    fn normalized_output_keeps_raw_blocks_for_replay() {
        let resp = json!({
            "content": [
                {"type": "thinking", "thinking": "", "signature": "s"},
                {"type": "text", "text": "hi"}
            ],
            "model": "claude-opus-5", "stop_reason": "end_turn", "usage": {}
        });
        let out = normalize_anthropic_response(&resp);
        assert_eq!(out["message"]["content"], "hi");
        assert_eq!(out["message"]["anthropic_content"][0]["type"], "thinking");
        // Text-only responses stay lean.
        let plain = json!({"content": [{"type": "text", "text": "x"}], "stop_reason": "end_turn"});
        assert!(
            normalize_anthropic_response(&plain)["message"]
                .get("anthropic_content")
                .is_none()
        );
    }

    #[test]
    fn fallbacks_default_on_for_new_models_at_first_party_endpoint_only() {
        let mut body = Map::new();
        assert!(apply_fallbacks(
            &mut body,
            &json!({}),
            "claude-opus-5",
            DEFAULT_ANTHROPIC_BASE
        ));
        assert_eq!(body["fallbacks"], "default");

        let mut body = Map::new();
        assert!(!apply_fallbacks(
            &mut body,
            &json!({"fallbacks": false}),
            "claude-opus-5",
            DEFAULT_ANTHROPIC_BASE
        ));
        assert!(!apply_fallbacks(
            &mut Map::new(),
            &json!({}),
            "claude-opus-5",
            "https://proxy.example/v1"
        ));
        assert!(!apply_fallbacks(
            &mut Map::new(),
            &json!({}),
            "claude-haiku-4-5",
            DEFAULT_ANTHROPIC_BASE
        ));

        let mut body = Map::new();
        assert!(apply_fallbacks(
            &mut body,
            &json!({"fallbacks": [{"model": "claude-opus-4-8"}]}),
            "claude-sonnet-5",
            DEFAULT_ANTHROPIC_BASE
        ));
        assert_eq!(body["fallbacks"][0]["model"], "claude-opus-4-8");
    }

    #[test]
    fn refusal_is_a_permanent_error_with_category() {
        let resp = json!({"stop_reason": "refusal", "content": [],
                          "stop_details": {"type": "refusal", "category": "cyber"}});
        let err = refusal_error(&resp).unwrap_err();
        assert!(matches!(&err, StepError::Permanent { message, .. } if message.contains("cyber")));
        assert!(refusal_error(&json!({"stop_reason": "end_turn"})).is_ok());
    }

    #[test]
    fn streamed_thinking_blocks_accumulate_text_and_signature() {
        let sink = DeltaSink::for_test();
        let mut acc = AnthropicStreamAcc::default();
        for event in [
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "thinking", "thinking": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "thinking_delta", "thinking": "plan"}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "signature_delta", "signature": "abc"}}),
        ] {
            acc.ingest(&event.to_string(), &sink).unwrap();
        }
        assert_eq!(acc.blocks[&0]["thinking"], "plan");
        assert_eq!(acc.blocks[&0]["signature"], "abc");
    }
}
