use std::collections::BTreeMap;

use serde_json::{json, Map, Value};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::common::{
    classify_api_error, classify_reqwest_error, extract_system_message, is_json_object_format,
    merge_json_response_fields, permanent, retryable, safe_truncate,
};
use super::sse::{next_chunk, stream_idle_timeout, SseParser};
use super::{anthropic_default_model, http_client, DeltaSink};

/// Build the `/messages` request body shared by the streaming and
/// non-streaming paths (so multimodal message conversion behaves identically).
fn build_body(params: &Value, model: &str) -> Map<String, Value> {
    let messages_raw = params.get("messages").cloned().unwrap_or(json!([]));
    let (system_from_msgs, messages) = extract_system_message(&messages_raw);
    // Plain-string content is cloned unchanged; normalized image blocks
    // become Anthropic base64 `source` blocks at request-build time.
    let messages = match messages.as_array() {
        Some(arr) => Value::Array(
            arr.iter()
                .map(super::multimodal::to_anthropic_message)
                .collect(),
        ),
        None => messages,
    };

    let max_tokens = params
        .get("max_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(4096);

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
        "tools",
        "tool_choice",
        "metadata",
    ] {
        if let Some(val) = params.get(key) {
            body.insert(key.into(), val.clone());
        }
    }
    body
}

pub(super) async fn call_anthropic(
    params: &Value,
    api_key: &str,
    base_url: &str,
    deltas: Option<&DeltaSink>,
) -> Result<Value, StepError> {
    let url = format!("{base_url}/messages");

    let model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(anthropic_default_model());

    let mut body = build_body(params, model);
    if deltas.is_some() {
        body.insert("stream".into(), json!(true));
    }
    let body = Value::Object(body);

    debug!(url = %url, model = %model, streaming = deltas.is_some(), "llm_call: Anthropic");

    let resp = http_client()
        .post(&url)
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| classify_reqwest_error(&e))?;

    if let Some(sink) = deltas {
        return consume_anthropic_stream(resp, params, sink).await;
    }

    let status = resp.status().as_u16();
    let resp_body: Value = resp
        .json()
        .await
        .map_err(|e| retryable(format!("response parse error: {e}")))?;

    if status >= 400 {
        return Err(classify_api_error(status, &resp_body));
    }

    let mut output = normalize_anthropic_response(&resp_body);

    if is_json_object_format(params) {
        if let Some(content_owned) = output
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
            .map(String::from)
        {
            merge_json_response_fields(&content_owned, &mut output);
        }
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
        if is_json_object_format(params) {
            if let Some(content_owned) = output
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(Value::as_str)
                .map(String::from)
            {
                merge_json_response_fields(&content_owned, &mut output);
            }
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
        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| retryable(format!("response parse error: {e}")))?;
        return Err(classify_api_error(status, &resp_body));
    }

    let idle_timeout = stream_idle_timeout(params);
    let mut parser = SseParser::default();
    let mut acc = AnthropicStreamAcc::default();

    while let Some(chunk) = next_chunk(&mut resp, idle_timeout).await? {
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

    Ok(acc.into_output(params))
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
}
