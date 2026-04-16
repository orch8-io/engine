//! Built-in `llm_call` handler — universal LLM integration.
//!
//! Supports all major providers through two API formats:
//! - **OpenAI-compatible**: `OpenAI`, `Deepseek`, `Qwen`, `Perplexity`, `Gemini`, `Groq`, `Together`, `Mistral`
//! - **Anthropic**: Claude (uses the `Messages` API)
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `provider` | string | `"openai"` | Provider name (see below) |
//! | `base_url` | string | per-provider | Override API base URL |
//! | `api_key` | string | — | API key (direct) |
//! | `api_key_env` | string | — | Env var name containing API key |
//! | `model` | string | per-provider | Model identifier |
//! | `messages` | array | `[]` | Chat messages (`{role, content}`) |
//! | `system` | string | — | System prompt (Anthropic shorthand) |
//! | `temperature` | number | — | Sampling temperature |
//! | `max_tokens` | number | `4096` | Max output tokens |
//! | `tools` | array | — | Tool/function definitions |
//! | `tool_choice` | string/object | — | Tool selection strategy |
//!
//! ## Providers
//!
//! | Name | Base URL | Format |
//! |------|----------|--------|
//! | `openai` | `api.openai.com/v1` | OpenAI |
//! | `anthropic` | `api.anthropic.com/v1` | Anthropic |
//! | `gemini` | `generativelanguage.googleapis.com/v1beta/openai` | OpenAI |
//! | `deepseek` | `api.deepseek.com` | OpenAI |
//! | `qwen` | `dashscope.aliyuncs.com/compatible-mode/v1` | OpenAI |
//! | `perplexity` | `api.perplexity.ai` | OpenAI |
//! | `groq` | `api.groq.com/openai/v1` | OpenAI |
//! | `together` | `api.together.xyz/v1` | OpenAI |
//! | `mistral` | `api.mistral.ai/v1` | OpenAI |
//! | `openrouter` | `openrouter.ai/api/v1` | OpenAI |

use std::sync::OnceLock;
use std::time::Duration;

use serde_json::{json, Value};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::StepContext;

/// Shared HTTP client for all LLM calls (connection pooling, TLS, keep-alive).
pub(crate) fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .timeout(Duration::from_secs(300))
            .build()
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "failed to build optimized HTTP client, using default");
                reqwest::Client::new()
            })
    })
}

/// Main handler: routes to the correct provider API.
pub async fn handle_llm_call(ctx: StepContext) -> Result<Value, StepError> {
    let provider = ctx
        .params
        .get("provider")
        .and_then(Value::as_str)
        .unwrap_or("openai");

    let api_key = resolve_api_key(&ctx.params, provider)?;
    let base = resolve_base_url(&ctx.params, provider);

    if provider == "anthropic" {
        call_anthropic(&ctx.params, &api_key, &base).await
    } else {
        call_openai_compat(&ctx.params, &api_key, &base, provider).await
    }
}

// ---------------------------------------------------------------------------
// OpenAI-compatible path (covers most providers)
// ---------------------------------------------------------------------------

async fn call_openai_compat(
    params: &Value,
    api_key: &str,
    base_url: &str,
    provider: &str,
) -> Result<Value, StepError> {
    let url = format!("{base_url}/chat/completions");

    let model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("gpt-4o");

    let mut body = json!({
        "model": model,
        "messages": params.get("messages").cloned().unwrap_or(json!([])),
    });

    // Forward optional fields as-is.
    let obj = body.as_object_mut().expect("body is object");
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
            obj.insert(key.into(), val.clone());
        }
    }

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

    // Normalize to common output format.
    let choice = resp_body
        .get("choices")
        .and_then(|c| c.get(0))
        .cloned()
        .unwrap_or_default();

    Ok(json!({
        "provider": provider,
        "model": resp_body.get("model").cloned().unwrap_or_default(),
        "message": choice.get("message").cloned().unwrap_or_default(),
        "finish_reason": choice.get("finish_reason").cloned().unwrap_or_default(),
        "usage": resp_body.get("usage").cloned().unwrap_or_default(),
    }))
}

// ---------------------------------------------------------------------------
// Anthropic Messages API
// ---------------------------------------------------------------------------

async fn call_anthropic(
    params: &Value,
    api_key: &str,
    base_url: &str,
) -> Result<Value, StepError> {
    let url = format!("{base_url}/messages");

    let model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("claude-sonnet-4-20250514");

    let messages_raw = params.get("messages").cloned().unwrap_or(json!([]));
    let (system_from_msgs, messages) = extract_system_message(&messages_raw);

    let max_tokens = params
        .get("max_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(4096);

    let mut body = json!({
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
    });

    let obj = body.as_object_mut().expect("body is object");

    // System prompt: explicit param takes precedence, then extracted from messages.
    if let Some(sys) = params.get("system") {
        obj.insert("system".into(), sys.clone());
    } else if let Some(sys) = system_from_msgs {
        obj.insert("system".into(), Value::String(sys));
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
            obj.insert(key.into(), val.clone());
        }
    }

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

    Ok(normalize_anthropic_response(&resp_body))
}

/// Normalize an Anthropic response body into the OpenAI-like shape we return.
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

    // Convert tool_use blocks to OpenAI tool_calls format.
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

    let mut message = json!({
        "role": "assistant",
        "content": text,
    });
    if !tool_calls.is_empty() {
        message
            .as_object_mut()
            .expect("msg is object")
            .insert("tool_calls".into(), json!(tool_calls));
    }

    json!({
        "provider": "anthropic",
        "model": resp_body.get("model").cloned().unwrap_or_default(),
        "message": message,
        "finish_reason": resp_body.get("stop_reason").cloned().unwrap_or_default(),
        "usage": resp_body.get("usage").cloned().unwrap_or_default(),
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the first `system` message from a messages array and return the
/// remaining messages (Anthropic requires system as a top-level field).
fn extract_system_message(messages: &Value) -> (Option<String>, Value) {
    let Some(arr) = messages.as_array() else {
        return (None, messages.clone());
    };

    let mut system = None;
    let mut filtered = Vec::with_capacity(arr.len());

    for msg in arr {
        if msg.get("role").and_then(Value::as_str) == Some("system") {
            if system.is_none() {
                system = msg
                    .get("content")
                    .and_then(Value::as_str)
                    .map(String::from);
            }
        } else {
            filtered.push(msg.clone());
        }
    }

    (system, json!(filtered))
}

/// Resolve API key: direct param → env var param → provider default env var.
fn resolve_api_key(params: &Value, provider: &str) -> Result<String, StepError> {
    if let Some(key) = params.get("api_key").and_then(Value::as_str) {
        return Ok(key.to_string());
    }

    if let Some(env_var) = params.get("api_key_env").and_then(Value::as_str) {
        return std::env::var(env_var).map_err(|_| {
            permanent(format!("env var '{env_var}' not set"))
        });
    }

    let env_var = default_env_var(provider);
    std::env::var(env_var).map_err(|_| {
        permanent(format!(
            "no API key: set 'api_key', 'api_key_env', or env var '{env_var}'"
        ))
    })
}

/// Default env var name per provider.
fn default_env_var(provider: &str) -> &'static str {
    match provider {
        "anthropic" => "ANTHROPIC_API_KEY",
        "gemini" => "GEMINI_API_KEY",
        "deepseek" => "DEEPSEEK_API_KEY",
        "qwen" => "DASHSCOPE_API_KEY",
        "perplexity" => "PERPLEXITY_API_KEY",
        "groq" => "GROQ_API_KEY",
        "together" => "TOGETHER_API_KEY",
        "mistral" => "MISTRAL_API_KEY",
        "openrouter" => "OPENROUTER_API_KEY",
        _ => "OPENAI_API_KEY",
    }
}

/// Resolve base URL: explicit param → provider default.
fn resolve_base_url(params: &Value, provider: &str) -> String {
    if let Some(url) = params.get("base_url").and_then(Value::as_str) {
        return url.trim_end_matches('/').to_string();
    }

    match provider {
        "anthropic" => "https://api.anthropic.com/v1",
        "gemini" => "https://generativelanguage.googleapis.com/v1beta/openai",
        "deepseek" => "https://api.deepseek.com",
        "qwen" => "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "perplexity" => "https://api.perplexity.ai",
        "groq" => "https://api.groq.com/openai/v1",
        "together" => "https://api.together.xyz/v1",
        "mistral" => "https://api.mistral.ai/v1",
        "openrouter" => "https://openrouter.ai/api/v1",
        _ => "https://api.openai.com/v1",
    }
    .to_string()
}

fn classify_reqwest_error(e: &reqwest::Error) -> StepError {
    if e.is_timeout() || e.is_connect() {
        retryable(format!("network error: {e}"))
    } else {
        permanent(format!("request error: {e}"))
    }
}

fn classify_api_error(status: u16, body: &Value) -> StepError {
    let msg = body
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");

    match status {
        429 => {
            warn!(status, msg, "LLM rate limited");
            retryable(format!("rate limited: {msg}"))
        }
        500..=599 => retryable(format!("server error ({status}): {msg}")),
        401 | 403 => permanent(format!("auth error ({status}): {msg}")),
        _ => permanent(format!("API error ({status}): {msg}")),
    }
}

fn retryable(message: String) -> StepError {
    StepError::Retryable {
        message,
        details: None,
    }
}

fn permanent(message: String) -> StepError {
    StepError::Permanent {
        message,
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_system_from_messages() {
        let msgs = json!([
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hello"},
        ]);
        let (sys, filtered) = extract_system_message(&msgs);
        assert_eq!(sys, Some("You are helpful.".into()));
        assert_eq!(filtered.as_array().unwrap().len(), 1);
        assert_eq!(filtered[0]["role"], "user");
    }

    #[test]
    fn extract_system_no_system() {
        let msgs = json!([{"role": "user", "content": "Hi"}]);
        let (sys, filtered) = extract_system_message(&msgs);
        assert!(sys.is_none());
        assert_eq!(filtered.as_array().unwrap().len(), 1);
    }

    #[test]
    fn resolve_base_urls() {
        let empty = json!({});
        assert!(resolve_base_url(&empty, "openai").contains("openai.com"));
        assert!(resolve_base_url(&empty, "anthropic").contains("anthropic.com"));
        assert!(resolve_base_url(&empty, "deepseek").contains("deepseek.com"));
        assert!(resolve_base_url(&empty, "qwen").contains("dashscope"));
        assert!(resolve_base_url(&empty, "perplexity").contains("perplexity"));
        assert!(resolve_base_url(&empty, "gemini").contains("googleapis"));
        assert!(resolve_base_url(&empty, "groq").contains("groq.com"));
        assert!(resolve_base_url(&empty, "together").contains("together"));
        assert!(resolve_base_url(&empty, "mistral").contains("mistral"));
        assert!(resolve_base_url(&empty, "openrouter").contains("openrouter"));
    }

    #[test]
    fn custom_base_url_overrides() {
        let params = json!({"base_url": "http://localhost:11434/v1"});
        assert_eq!(
            resolve_base_url(&params, "openai"),
            "http://localhost:11434/v1"
        );
    }

    #[test]
    fn default_env_vars() {
        assert_eq!(default_env_var("openai"), "OPENAI_API_KEY");
        assert_eq!(default_env_var("anthropic"), "ANTHROPIC_API_KEY");
        assert_eq!(default_env_var("gemini"), "GEMINI_API_KEY");
        assert_eq!(default_env_var("deepseek"), "DEEPSEEK_API_KEY");
        assert_eq!(default_env_var("unknown"), "OPENAI_API_KEY");
    }

    #[test]
    fn classify_errors() {
        let body_429 = json!({"error": {"message": "too many requests"}});
        assert!(matches!(
            classify_api_error(429, &body_429),
            StepError::Retryable { .. }
        ));

        let body_500 = json!({"error": {"message": "internal"}});
        assert!(matches!(
            classify_api_error(500, &body_500),
            StepError::Retryable { .. }
        ));

        let body_401 = json!({"error": {"message": "invalid key"}});
        assert!(matches!(
            classify_api_error(401, &body_401),
            StepError::Permanent { .. }
        ));
    }
}
