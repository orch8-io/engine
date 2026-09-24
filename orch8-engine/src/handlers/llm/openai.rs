use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::common::{
    classify_api_error, classify_reqwest_error, is_json_object_format, merge_json_response_fields,
    retryable, safe_truncate,
};
use super::sse::{SseParser, charge_stream_bytes, next_chunk, stream_idle_timeout};
use super::{DeltaSink, http_client};

/// `OpenAI` reasoning models (o-series, GPT-5/6) reject sampling params.
fn openai_rejects_sampling(model: &str) -> bool {
    let m = model.rsplit('/').next().unwrap_or(model);
    m.starts_with("gpt-5")
        || m.starts_with("gpt-6")
        || m.starts_with("o1")
        || m.starts_with("o3")
        || m.starts_with("o4")
}

/// Build the `/chat/completions` request body shared by the streaming and
/// non-streaming paths (so multimodal message conversion behaves identically).
/// Provider-specific field names are mapped here (see the module docs of
/// `llm` for the table).
fn build_body(params: &Value, model: &str, provider: &str) -> Map<String, Value> {
    let messages = {
        let mut msgs = Vec::new();
        if let Some(sys) = params.get("system").and_then(Value::as_str) {
            msgs.push(json!({"role": "system", "content": sys}));
        }
        if let Some(Value::Array(arr)) = params.get("messages") {
            // Plain-string content is cloned unchanged; normalized image
            // blocks become `image_url` data URLs at request-build time.
            msgs.extend(arr.iter().map(|m| {
                let mut m = super::multimodal::to_openai_message(m);
                // Anthropic-native blocks kept for round-tripping (a failover
                // from `anthropic`) are meaningless to this protocol.
                if let Some(obj) = m.as_object_mut() {
                    obj.remove("anthropic_content");
                }
                m
            }));
        }
        Value::Array(msgs)
    };

    let mut body = serde_json::Map::new();
    body.insert("model".into(), json!(model));
    body.insert("messages".into(), messages);

    for &key in &[
        "temperature",
        "max_tokens",
        "max_completion_tokens",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "stop",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "response_format",
        "reasoning_effort",
        "seed",
        "n",
    ] {
        if let Some(val) = params.get(key) {
            body.insert(key.into(), val.clone());
        }
    }

    match provider {
        "openai" => {
            // `max_tokens` is deprecated on Chat Completions and rejected by
            // the reasoning models; `max_completion_tokens` works everywhere.
            if let Some(v) = body.remove("max_tokens") {
                body.entry("max_completion_tokens").or_insert(v);
            }
            if openai_rejects_sampling(model) {
                let dropped: Vec<&str> = ["temperature", "top_p"]
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
        }
        "mistral" => {
            if let Some(v) = body.remove("seed") {
                body.entry("random_seed").or_insert(v);
            }
            for &key in &["random_seed", "safe_prompt", "prompt_mode"] {
                if let Some(val) = params.get(key) {
                    body.insert(key.into(), val.clone());
                }
            }
        }
        _ => {}
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

    let model = super::resolve_model(params, provider)?;
    let model = model.as_str();

    let mut body = build_body(params, model, provider);
    if deltas.is_some() {
        body.insert("stream".into(), json!(true));
        // Without this the final usage chunk is omitted and the streamed
        // output would lose token accounting relative to non-streaming.
        // Mistral always reports usage on the final chunk and does not
        // document `stream_options`.
        if provider != "mistral" {
            body.insert("stream_options".into(), json!({"include_usage": true}));
        }
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
    let resp_body: Value = super::read_json_capped(resp).await?;

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

    if is_json_object_format(params)
        && let Some(content_str) = choice
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
    {
        merge_json_response_fields(content_str, &mut output);
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
        if self.model.is_null()
            && let Some(model) = chunk.get("model").filter(|m| m.is_string())
        {
            self.model = model.clone();
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
        let resp_body: Value = super::read_json_capped(resp).await?;
        return Err(classify_api_error(status, &resp_body));
    }

    let idle_timeout = stream_idle_timeout(params);
    let mut parser = SseParser::default();
    let mut acc = OpenAiStreamAcc::default();

    let mut received = 0usize;
    while let Some(chunk) = next_chunk(&mut resp, idle_timeout).await? {
        charge_stream_bytes(&mut received, chunk.len())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_uses_max_completion_tokens_and_drops_sampling_on_reasoning_models() {
        let params = json!({"max_tokens": 500, "temperature": 0.3, "reasoning_effort": "high",
                            "messages": [{"role": "user", "content": "hi"}]});
        let body = build_body(&params, "gpt-6-astra", "openai");
        assert!(body.get("max_tokens").is_none());
        assert_eq!(body["max_completion_tokens"], 500);
        assert!(body.get("temperature").is_none());
        assert_eq!(body["reasoning_effort"], "high");

        let body = build_body(&params, "gpt-4o", "openai");
        assert_eq!(
            body["temperature"], 0.3,
            "non-reasoning models keep sampling"
        );
    }

    #[test]
    fn mistral_maps_seed_and_passes_its_own_fields() {
        let params = json!({"seed": 7, "max_tokens": 64, "safe_prompt": true,
                            "prompt_mode": "reasoning", "messages": []});
        let body = build_body(&params, "mistral-medium-latest", "mistral");
        assert!(body.get("seed").is_none());
        assert_eq!(body["random_seed"], 7);
        assert_eq!(body["max_tokens"], 64, "Mistral still takes max_tokens");
        assert_eq!(body["safe_prompt"], true);
        assert_eq!(body["prompt_mode"], "reasoning");
    }

    #[test]
    fn other_providers_are_passed_through_unchanged() {
        let params = json!({"seed": 7, "max_tokens": 64, "temperature": 0.1,
                            "safe_prompt": true, "messages": []});
        let body = build_body(&params, "deepseek-v4-pro", "deepseek");
        assert_eq!(body["seed"], 7);
        assert_eq!(body["max_tokens"], 64);
        assert!(body.get("safe_prompt").is_none());
    }

    #[test]
    fn anthropic_replay_blocks_are_not_sent_to_openai_protocol() {
        let params = json!({"messages": [
            {"role": "assistant", "content": "x", "anthropic_content": [{"type": "thinking"}]}
        ]});
        let body = build_body(&params, "gpt-6-astra", "openai");
        assert!(body["messages"][0].get("anthropic_content").is_none());
    }
}
