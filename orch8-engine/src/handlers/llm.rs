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
//! | `providers` | array | — | Failover list of `{provider, api_key, model}` objects |
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
            .timeout(Duration::from_mins(5))
            .build()
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "failed to build optimized HTTP client, using default");
                reqwest::Client::new()
            })
    })
}

/// Main handler: routes to the correct provider API.
///
/// If the params contain a `providers` array, iterates through each provider
/// in order, attempting the call. On failure, tries the next provider.
/// The output includes a `tried` array listing providers attempted in order.
pub async fn handle_llm_call(ctx: StepContext) -> Result<Value, StepError> {
    // Check for multi-provider failover mode.
    if let Some(providers) = ctx.params.get("providers").and_then(Value::as_array) {
        return handle_llm_call_failover(&ctx.params, providers).await;
    }

    // Single-provider mode (existing behavior).
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

/// Default per-provider timeout in failover mode. Without this cap a single
/// hung provider would burn the full 5-minute client timeout before failing
/// over — multiplying latency across every remaining provider.
const DEFAULT_PER_PROVIDER_TIMEOUT: Duration = Duration::from_mins(1);

/// Failover logic: iterate through providers, try each one, stop on first success.
async fn handle_llm_call_failover(params: &Value, providers: &[Value]) -> Result<Value, StepError> {
    if providers.is_empty() {
        return Err(permanent("providers array is empty".to_string()));
    }

    // Perf#9: apply a short per-provider timeout in failover mode so a single
    // slow provider can't eat the whole budget. Callers can override with
    // `per_provider_timeout_secs`; 0 disables the cap (back to the client-level
    // 5-minute ceiling).
    let per_attempt_timeout = params
        .get("per_provider_timeout_secs")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_PER_PROVIDER_TIMEOUT, Duration::from_secs);

    let mut tried: Vec<String> = Vec::new();
    let mut last_error: Option<StepError> = None;

    for provider_config in providers {
        let provider_name = provider_config
            .get("provider")
            .and_then(Value::as_str)
            .unwrap_or("openai");

        tried.push(provider_name.to_string());

        // Build merged params: base params overlaid with per-provider overrides.
        let merged = merge_provider_params(params, provider_config);

        let api_key = match resolve_api_key(&merged, provider_name) {
            Ok(key) => key,
            Err(e) => {
                warn!(provider = %provider_name, error = %e, "llm_call failover: skipping provider (key resolution failed)");
                last_error = Some(e);
                continue;
            }
        };

        let base = resolve_base_url(&merged, provider_name);
        if !super::builtin::is_url_safe(&base).await {
            warn!(provider = %provider_name, base_url = %base, "llm_call: rejected unsafe base_url");
            last_error = Some(permanent(format!(
                "provider {provider_name}: base_url '{base}' targets an internal or non-public address"
            )));
            continue;
        }

        let attempt = async {
            if provider_name == "anthropic" {
                call_anthropic(&merged, &api_key, &base).await
            } else {
                call_openai_compat(&merged, &api_key, &base, provider_name).await
            }
        };

        let result = if per_attempt_timeout.is_zero() {
            attempt.await
        } else {
            match tokio::time::timeout(per_attempt_timeout, attempt).await {
                Ok(r) => r,
                Err(_) => Err(retryable(format!(
                    "provider {provider_name} timed out after {per_attempt_timeout:?}"
                ))),
            }
        };

        match result {
            Ok(mut output) => {
                // Inject the `tried` list into the successful response.
                if let Some(obj) = output.as_object_mut() {
                    obj.insert("tried".into(), json!(tried));
                }
                return Ok(output);
            }
            Err(e) => {
                warn!(provider = %provider_name, error = %e, "llm_call failover: provider failed, trying next");
                last_error = Some(e);
            }
        }
    }

    // All providers failed.
    let error_msg = match last_error.as_ref() {
        Some(e) => format!("all providers failed, last error: {e}"),
        None => "all providers failed".to_string(),
    };

    Err(permanent(error_msg))
}

/// Merge base params with per-provider overrides.
/// Provider-specific fields (`provider`, `api_key`, `api_key_env`, `model`, `base_url`)
/// from the provider config override the top-level params.
fn merge_provider_params(base_params: &Value, provider_config: &Value) -> Value {
    // Build incrementally: skip the `providers` key during construction
    // instead of cloning the entire tree then removing it.
    let mut merged = if let Some(base_obj) = base_params.as_object() {
        let mut obj = serde_json::Map::with_capacity(base_obj.len());
        for (k, v) in base_obj {
            if k == "providers" {
                continue;
            }
            obj.insert(k.clone(), v.clone());
        }
        serde_json::Value::Object(obj)
    } else {
        base_params.clone()
    };

    // Overlay provider-specific fields.
    if let (Some(merged_obj), Some(config_obj)) =
        (merged.as_object_mut(), provider_config.as_object())
    {
        for (key, value) in config_obj {
            merged_obj.insert(key.clone(), value.clone());
        }
    }

    merged
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

    let messages = {
        let mut msgs = Vec::new();
        if let Some(sys) = params.get("system").and_then(Value::as_str) {
            msgs.push(json!({"role": "system", "content": sys}));
        }
        if let Some(Value::Array(arr)) = params.get("messages") {
            msgs.extend(arr.iter().cloned());
        }
        Value::Array(msgs)
    };

    let mut body = json!({
        "model": model,
        "messages": messages,
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

    let mut output = json!({
        "provider": provider,
        "model": resp_body.get("model").cloned().unwrap_or_default(),
        "message": choice.get("message").cloned().unwrap_or_default(),
        "finish_reason": choice.get("finish_reason").cloned().unwrap_or_default(),
        "usage": resp_body.get("usage").cloned().unwrap_or_default(),
    });

    // When response_format is json_object, parse message.content and merge
    // parsed fields into the top-level output so downstream templates can
    // reference e.g. `steps.score_markets.markets` directly.
    if params
        .get("response_format")
        .and_then(|rf| rf.get("type"))
        .and_then(Value::as_str)
        == Some("json_object")
    {
        if let Some(content_str) = choice
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
        {
            if let Ok(parsed) = serde_json::from_str::<Value>(content_str) {
                if let (Some(out_obj), Some(parsed_obj)) =
                    (output.as_object_mut(), parsed.as_object())
                {
                    for (k, v) in parsed_obj {
                        out_obj.entry(k).or_insert_with(|| v.clone());
                    }
                }
            }
        }
    }

    Ok(output)
}

// ---------------------------------------------------------------------------
// Anthropic Messages API
// ---------------------------------------------------------------------------

async fn call_anthropic(params: &Value, api_key: &str, base_url: &str) -> Result<Value, StepError> {
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
                system = msg.get("content").and_then(Value::as_str).map(String::from);
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
        return std::env::var(env_var)
            .map_err(|_| permanent(format!("env var '{env_var}' not set")));
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

const fn retryable(message: String) -> StepError {
    StepError::Retryable {
        message,
        details: None,
    }
}

const fn permanent(message: String) -> StepError {
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

    #[test]
    fn merge_provider_params_overlays_fields() {
        let base = json!({
            "messages": [{"role": "user", "content": "hello"}],
            "provider": "openai",
            "api_key": "base-key",
            "model": "gpt-4o",
            "providers": [{"provider": "anthropic"}],
        });
        let config = json!({
            "provider": "anthropic",
            "api_key": "override-key",
            "model": "claude-sonnet-4-20250514",
        });

        let merged = merge_provider_params(&base, &config);
        assert_eq!(merged.get("provider").unwrap(), "anthropic");
        assert_eq!(merged.get("api_key").unwrap(), "override-key");
        assert_eq!(merged.get("model").unwrap(), "claude-sonnet-4-20250514");
        // messages preserved from base
        assert!(merged.get("messages").is_some());
        // providers key removed
        assert!(merged.get("providers").is_none());
    }

    #[test]
    fn resolve_api_key_prefers_direct_param_over_env() {
        // #298/#185 — if `api_key` is set directly on params, it wins. We
        // don't want to accidentally read the process env when the caller
        // has already provided a key (avoids ambient credential leaks).
        let params = json!({"api_key": "direct-param-key"});
        // Even if OPENAI_API_KEY is set, direct param is returned.
        std::env::set_var("OPENAI_API_KEY", "env-sourced-key");
        let key = resolve_api_key(&params, "openai").expect("direct param must resolve");
        assert_eq!(key, "direct-param-key");
    }

    #[test]
    fn resolve_api_key_from_explicit_env_var_param() {
        // #185 — if `api_key_env` names an env var, we read that var.
        // Use a unique var name to avoid clashing with other tests.
        let var = "ORCH8_TEST_LLM_KEY_EXPLICIT";
        std::env::set_var(var, "from-explicit-env");
        let params = json!({"api_key_env": var});
        let key = resolve_api_key(&params, "openai").unwrap();
        assert_eq!(key, "from-explicit-env");
        std::env::remove_var(var);
    }

    #[test]
    fn resolve_api_key_returns_permanent_error_when_nothing_set() {
        // #299 — exhaustive miss (no param, no api_key_env, no default env
        // var set) must be a `Permanent` step error. Retry would just spin.
        // Use a made-up provider so the default env var is OPENAI_API_KEY
        // which we blank out for this test.
        let prev = std::env::var("ORCH8_TEST_LLM_KEY_NONE").ok();
        std::env::set_var("OPENAI_API_KEY_UNSET_MARKER", "ignored");
        let params = json!({"api_key_env": "ORCH8_TEST_LLM_KEY_NONE_UNSET_VAR"});
        let err = resolve_api_key(&params, "openai").expect_err("missing env var must error out");
        assert!(matches!(err, StepError::Permanent { .. }));
        if let Some(v) = prev {
            std::env::set_var("ORCH8_TEST_LLM_KEY_NONE", v);
        }
    }

    #[test]
    fn classify_api_error_403_is_permanent() {
        // #304 — 403 Forbidden must match 401 semantics: permanent auth
        // failure. Retrying won't help; re-queueing is wasted work.
        let body = json!({"error": {"message": "forbidden"}});
        assert!(matches!(
            classify_api_error(403, &body),
            StepError::Permanent { .. }
        ));
    }

    #[test]
    fn classify_api_error_503_is_retryable() {
        // #305 (non-500 5xx) — service unavailable is transient.
        let body = json!({"error": {"message": "unavailable"}});
        assert!(matches!(
            classify_api_error(503, &body),
            StepError::Retryable { .. }
        ));
    }

    #[test]
    fn classify_api_error_4xx_other_is_permanent() {
        // 4xx (other than 401/403/429) is a client error — permanent.
        let body = json!({"error": {"message": "bad request"}});
        assert!(matches!(
            classify_api_error(400, &body),
            StepError::Permanent { .. }
        ));
    }

    #[test]
    fn merge_provider_params_preserves_base_when_empty_override() {
        // #183 — overlay with an empty config should be a pure pass-through
        // of the base (minus the `providers` array, which is always stripped).
        let base = json!({
            "messages": [{"role": "user", "content": "hi"}],
            "model": "gpt-4",
            "providers": [{"provider": "anthropic"}],
        });
        let config = json!({});
        let merged = merge_provider_params(&base, &config);
        assert_eq!(merged["model"], "gpt-4");
        assert!(merged.get("providers").is_none());
        assert!(merged.get("messages").is_some());
    }

    #[test]
    fn extract_system_first_system_wins_others_dropped() {
        // Edge case: multiple system messages — only the first is surfaced.
        let msgs = json!([
            {"role": "system", "content": "first"},
            {"role": "user", "content": "hello"},
            {"role": "system", "content": "second"},
        ]);
        let (sys, filtered) = extract_system_message(&msgs);
        assert_eq!(sys, Some("first".into()));
        // Both system messages are filtered out of the returned array.
        let arr = filtered.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["role"], "user");
    }

    #[test]
    fn empty_providers_returns_error() {
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_llm_call_failover(&json!({}), &[]));
        assert!(result.is_err());
    }
}
