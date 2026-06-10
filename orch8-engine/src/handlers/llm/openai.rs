use std::collections::BTreeMap;

use serde_json::{json, Map, Value};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::common::{
    classify_api_error, classify_reqwest_error, is_json_object_format, merge_json_response_fields,
    retryable, safe_truncate,
};
use super::sse::{next_chunk, stream_idle_timeout, SseParser};
use super::{http_client, openai_default_model, DeltaSink};

/// Build the `/chat/completions` request body shared by the streaming and
/// non-streaming paths (so multimodal message conversion behaves identically).
fn build_body(params: &Value, model: &str) -> Map<String, Value> {
    let messages = {
        let mut msgs = Vec::new();
        if let Some(sys) = params.get("system").and_then(Value::as_str) {
            msgs.push(json!({"role": "system", "content": sys}));
        }
        if let Some(Value::Array(arr)) = params.get("messages") {
            // Plain-string content is cloned unchanged; normalized image
            // blocks become `image_url` data URLs at request-build time.
            msgs.extend(arr.iter().map(super::multimodal::to_openai_message));
        }
        Value::Array(msgs)
    };

    let mut body = serde_json::Map::new();
    body.insert("model".into(), json!(model));
    body.insert("messages".into(), messages);

    for &key in &[
        "temperature",
        "max_tokens",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "stop",
        "tools",
        "tool_choice",
        "response_format",
        "seed",
        "n",
    ] {
        if let Some(val) = params.get(key) {
            body.insert(key.into(), val.clone());
        }
    }
    body
}

pub(super) async fn call_openai_compat(
    params: &Value,
    api_key: &str,
    base_url: &str,
    provider: &str,
    deltas: Option<&DeltaSink>,
) -> Result<Value, StepError> {
    let url = format!("{base_url}/chat/completions");

    let model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(openai_default_model());

    let mut body = build_body(params, model);
    if deltas.is_some() {
        body.insert("stream".into(), json!(true));
        // Without this the final usage chunk is omitted and the streamed
        // output would lose token accounting relative to non-streaming.
        body.insert("stream_options".into(), json!({"include_usage": true}));
    }
    let body = Value::Object(body);

    debug!(url = %url, model = %model, provider = %provider, streaming = deltas.is_some(), "llm_call: OpenAI-compatible");

    let resp = http_client()
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| classify_reqwest_error(&e))?;

    if let Some(sink) = deltas {
        return consume_openai_stream(resp, params, provider, sink).await;
    }

    let status = resp.status().as_u16();
    let resp_body: Value = resp
        .json()
        .await
        .map_err(|e| retryable(format!("response parse error: {e}")))?;

    if status >= 400 {
        return Err(classify_api_error(status, &resp_body));
    }

    let choice = resp_body
        .get("choices")
        .and_then(|c| c.get(0))
        .cloned()
        .unwrap_or_default();

    let mut output = json!({
        "provider": provider,
        "model": resp_body.get("model").cloned().unwrap_or_default(),
        "message": choice.get("message").cloned().unwrap_or_default(),
        "finish_reason": choice.get("finish_reason").cloned().unwrap_or_default(),
        "usage": resp_body.get("usage").cloned().unwrap_or_default(),
    });

    if is_json_object_format(params) {
        if let Some(content_str) = choice
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
        {
            merge_json_response_fields(content_str, &mut output);
        }
    }

    Ok(output)
}

/// Partially-accumulated tool call, keyed by the chunk `index` field.
#[derive(Default)]
struct ToolCallAcc {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

/// Accumulator for `OpenAI` streaming chunks. Produces an output identical in
/// shape to the non-streaming path once the stream terminates with `[DONE]`.
#[derive(Default)]
struct OpenAiStreamAcc {
    model: Value,
    role: Option<String>,
    content: String,
    saw_content: bool,
    tool_calls: BTreeMap<u64, ToolCallAcc>,
    finish_reason: Value,
    usage: Value,
    done: bool,
}

impl OpenAiStreamAcc {
    /// Ingest one `data:` payload, publishing text deltas to `sink`.
    fn ingest(&mut self, data: &str, sink: &DeltaSink) {
        if data.trim() == "[DONE]" {
            self.done = true;
            return;
        }
        let Ok(chunk) = serde_json::from_str::<Value>(data) else {
            warn!(
                data_preview = %safe_truncate(data, 200),
                "llm_call: skipping unparseable streaming chunk"
            );
            return;
        };
        if self.model.is_null() {
            if let Some(model) = chunk.get("model").filter(|m| m.is_string()) {
                self.model = model.clone();
            }
        }
        // The final chunk (stream_options.include_usage) carries usage with
        // an empty choices array.
        if let Some(usage) = chunk.get("usage").filter(|u| u.is_object()) {
            self.usage = usage.clone();
        }
        let Some(choice) = chunk.get("choices").and_then(|c| c.get(0)) else {
            return;
        };
        if let Some(fr) = choice.get("finish_reason").filter(|fr| !fr.is_null()) {
            self.finish_reason = fr.clone();
        }
        let Some(delta) = choice.get("delta") else {
            return;
        };
        if let Some(role) = delta.get("role").and_then(Value::as_str) {
            self.role.get_or_insert_with(|| role.to_string());
        }
        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            self.saw_content = true;
            if !text.is_empty() {
                self.content.push_str(text);
                sink.publish(text);
            }
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                let index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
                let acc = self.tool_calls.entry(index).or_default();
                if let Some(id) = call.get("id").and_then(Value::as_str) {
                    acc.id.get_or_insert_with(|| id.to_string());
                }
                if let Some(function) = call.get("function") {
                    if let Some(name) = function.get("name").and_then(Value::as_str) {
                        acc.name.get_or_insert_with(|| name.to_string());
                    }
                    if let Some(args) = function.get("arguments").and_then(Value::as_str) {
                        acc.arguments.push_str(args);
                    }
                }
            }
        }
    }

    /// Assemble the final output in the exact shape of the non-streaming path.
    fn into_output(self, provider: &str, params: &Value) -> Value {
        let mut message = serde_json::Map::new();
        message.insert(
            "role".into(),
            json!(self.role.as_deref().unwrap_or("assistant")),
        );
        // Tool-call-only responses report `content: null` (matching the
        // non-streaming response shape); otherwise the accumulated text.
        let content = if !self.saw_content && !self.tool_calls.is_empty() {
            Value::Null
        } else {
            json!(self.content)
        };
        message.insert("content".into(), content);
        if !self.tool_calls.is_empty() {
            let calls: Vec<Value> = self
                .tool_calls
                .into_values()
                .map(|tc| {
                    json!({
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.name, "arguments": tc.arguments},
                    })
                })
                .collect();
            message.insert("tool_calls".into(), json!(calls));
        }

        let mut output = json!({
            "provider": provider,
            "model": self.model,
            "message": Value::Object(message),
            "finish_reason": self.finish_reason,
            "usage": self.usage,
        });

        if is_json_object_format(params) && self.saw_content {
            merge_json_response_fields(&self.content, &mut output);
        }
        output
    }
}

/// Consume an OpenAI-compatible SSE stream, publishing text deltas and
/// accumulating the full response.
///
/// Termination contract: the provider must send `data: [DONE]`. A stream
/// that ends (EOF / connection drop) before that is incomplete and fails
/// **retryable**; a chunk gap longer than the idle timeout fails retryable
/// via [`next_chunk`].
async fn consume_openai_stream(
    mut resp: reqwest::Response,
    params: &Value,
    provider: &str,
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
    let mut acc = OpenAiStreamAcc::default();

    while let Some(chunk) = next_chunk(&mut resp, idle_timeout).await? {
        for event in parser.push(&chunk) {
            acc.ingest(&event.data, sink);
        }
        if acc.done {
            break;
        }
    }

    if !acc.done {
        return Err(retryable(
            "provider stream ended before [DONE] — response is incomplete".to_string(),
        ));
    }

    Ok(acc.into_output(provider, params))
}
