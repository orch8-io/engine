use serde_json::{json, Value};
use tracing::debug;

use orch8_types::error::StepError;

use super::common::{
    classify_api_error, classify_reqwest_error, extract_system_message, is_json_object_format,
    merge_json_response_fields, retryable,
};
use super::{anthropic_default_model, http_client};

pub(super) async fn call_anthropic(
    params: &Value,
    api_key: &str,
    base_url: &str,
) -> Result<Value, StepError> {
    let url = format!("{base_url}/messages");

    let model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(anthropic_default_model());

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
    let body = Value::Object(body);

    debug!(url = %url, model = %model, "llm_call: Anthropic");

    let resp = http_client()
        .post(&url)
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| classify_reqwest_error(&e))?;

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
}
