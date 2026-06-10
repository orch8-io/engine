use serde_json::{json, Value};
use tracing::debug;

use orch8_types::error::StepError;

use super::common::{
    classify_api_error, classify_reqwest_error, is_json_object_format, merge_json_response_fields,
    retryable,
};
use super::{http_client, openai_default_model};

pub(super) async fn call_openai_compat(
    params: &Value,
    api_key: &str,
    base_url: &str,
    provider: &str,
) -> Result<Value, StepError> {
    let url = format!("{base_url}/chat/completions");

    let model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(openai_default_model());

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
    let body = Value::Object(body);

    debug!(url = %url, model = %model, provider = %provider, "llm_call: OpenAI-compatible");

    let resp = http_client()
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
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
