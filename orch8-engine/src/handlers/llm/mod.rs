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
//! | `model` | string | per-provider | Model identifier (see env var overrides below) |
//! | `messages` | array | `[]` | Chat messages (`{role, content}`) |
//! | `system` | string | — | System prompt (Anthropic shorthand) |
//! | `temperature` | number | — | Sampling temperature |
//! | `max_tokens` | number | `4096` | Max output tokens |
//! | `tools` | array | — | Tool/function definitions |
//! | `tool_choice` | string/object | — | Tool selection strategy |
//! | `total_timeout_secs` | number | `120` | Cumulative timeout across all failover attempts (0 disables) |
//! | `per_provider_timeout_secs` | number | `60` | Per-provider timeout in failover mode |
//!
//! ## Providers
//!
//! | Name | Base URL | Format |
//! |------|----------|--------|
//! | `openai` | `api.openai.com/v1` | `OpenAI` |
//! | `anthropic` | `api.anthropic.com/v1` | `Anthropic` |
//! | `gemini` | `generativelanguage.googleapis.com/v1beta/openai` | `OpenAI` |
//! | `deepseek` | `api.deepseek.com` | `OpenAI` |
//! | `qwen` | `dashscope.aliyuncs.com/compatible-mode/v1` | `OpenAI` |
//! | `perplexity` | `api.perplexity.ai` | `OpenAI` |
//! | `groq` | `api.groq.com/openai/v1` | `OpenAI` |
//! | `together` | `api.together.xyz/v1` | `OpenAI` |
//! | `mistral` | `api.mistral.ai/v1` | `OpenAI` |
//! | `openrouter` | `openrouter.ai/api/v1` | `OpenAI` |
//!
//! ## Default model env vars
//!
//! When no `model` is specified in step params, the handler reads from these
//! env vars (checked once at first use):
//!
//! | Env var | Applies to | Fallback |
//! |---------|-----------|----------|
//! | `ORCH8_LLM_DEFAULT_MODEL_OPENAI` | All `OpenAI`-compatible providers | `gpt-4o` |
//! | `ORCH8_LLM_DEFAULT_MODEL_ANTHROPIC` | `Anthropic` | `claude-sonnet-4-6` |

mod anthropic;
mod common;
mod openai;

use std::sync::OnceLock;
use std::time::Duration;

use serde_json::{json, Value};

/// Default model for the OpenAI-compatible provider. Override at runtime
/// with `ORCH8_LLM_DEFAULT_MODEL_OPENAI`.
pub(crate) fn openai_default_model() -> &'static str {
    static MODEL: OnceLock<String> = OnceLock::new();
    MODEL.get_or_init(|| {
        std::env::var("ORCH8_LLM_DEFAULT_MODEL_OPENAI").unwrap_or_else(|_| "gpt-4o".to_string())
    })
}

/// Default model for the Anthropic provider. Override at runtime
/// with `ORCH8_LLM_DEFAULT_MODEL_ANTHROPIC`.
pub(crate) fn anthropic_default_model() -> &'static str {
    static MODEL: OnceLock<String> = OnceLock::new();
    MODEL.get_or_init(|| {
        std::env::var("ORCH8_LLM_DEFAULT_MODEL_ANTHROPIC")
            .unwrap_or_else(|_| "claude-sonnet-4-6".to_string())
    })
}
use tracing::warn;

use orch8_types::error::StepError;

use self::common::{permanent, resolve_api_key, resolve_base_url, retryable};
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
///
/// If the params contain a `providers` array, iterates through each provider
/// in order, attempting the call. On failure, tries the next provider.
/// The output includes a `tried` array listing providers attempted in order.
pub async fn handle_llm_call(ctx: StepContext) -> Result<Value, StepError> {
    let dry = ctx.is_dry_run();

    if let Some(providers) = ctx.params.get("providers").and_then(Value::as_array) {
        if dry {
            // Validate before skipping: an empty providers array is a config
            // error a dry-run should surface.
            if providers.is_empty() {
                return Err(permanent("providers array is empty".to_string()));
            }
            return Ok(llm_dry_run_stub(&ctx.params));
        }
        return handle_llm_call_failover(&ctx.params, providers).await;
    }

    let provider = ctx
        .params
        .get("provider")
        .and_then(Value::as_str)
        .unwrap_or("openai");

    // Resolve + validate (api key present, base URL allowed) BEFORE the
    // dry-run skip, so a dry-run still catches missing keys / blocked URLs.
    let api_key = resolve_api_key(&ctx.params, provider)?;
    let base = resolve_base_url(&ctx.params, provider);

    if !super::builtin::is_url_safe(&base).await {
        return Err(permanent(format!("base_url is not allowed: {base}")));
    }

    // Dry-run: validation passed; skip only the model call. Mirror the output
    // shape (empty assistant message) so downstream templates resolve.
    if dry {
        return Ok(llm_dry_run_stub(&ctx.params));
    }

    let out = if provider == "anthropic" {
        anthropic::call_anthropic(&ctx.params, &api_key, &base).await?
    } else {
        openai::call_openai_compat(&ctx.params, &api_key, &base, provider).await?
    };
    // Capture token usage for cost aggregation (best-effort — never fails the call).
    record_llm_usage(&ctx, &out).await;
    Ok(out)
}

/// Record LLM token usage as a `usage_event`. Normalizes the two provider usage
/// shapes (`OpenAI` `prompt_tokens`/`completion_tokens`, `Anthropic`
/// `input_tokens`/`output_tokens`).
/// Best-effort: a recording failure is logged, never propagated.
async fn record_llm_usage(ctx: &StepContext, out: &Value) {
    let Some(usage) = out.get("usage") else {
        return;
    };
    let pick = |a: &str, b: &str| {
        usage
            .get(a)
            .or_else(|| usage.get(b))
            .and_then(Value::as_i64)
            .unwrap_or(0)
    };
    let input = pick("input_tokens", "prompt_tokens");
    let output = pick("output_tokens", "completion_tokens");
    if input == 0 && output == 0 {
        return; // provider returned no usage — nothing to attribute.
    }
    let event = orch8_storage::UsageEvent {
        tenant_id: ctx.tenant_id.as_str().to_string(),
        instance_id: Some(ctx.instance_id),
        block_id: Some(ctx.block_id.as_str().to_string()),
        kind: "llm_tokens".to_string(),
        model: out
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        input_tokens: input,
        output_tokens: output,
        created_at: chrono::Utc::now(),
    };
    if let Err(e) = ctx.storage.record_usage_event(&event).await {
        warn!(error = %e, "llm_call: failed to record token usage");
    }
}

/// Canonical dry-run stub for `llm_call`, echoing the requested model.
fn llm_dry_run_stub(params: &Value) -> Value {
    let model = params.get("model").cloned().unwrap_or(Value::Null);
    super::util::dry_run_stub(
        "llm_call",
        Value::Null,
        json!({
            "message": { "role": "assistant", "content": "" },
            "model": model,
            "finish_reason": "dry_run",
            "usage": {},
        }),
    )
}

/// Default per-provider timeout in failover mode.
const DEFAULT_PER_PROVIDER_TIMEOUT: Duration = Duration::from_secs(60);

/// Default cumulative timeout across all failover attempts.
const DEFAULT_TOTAL_TIMEOUT: Duration = Duration::from_secs(120);

/// Failover logic: iterate through providers, try each one, stop on first success.
///
/// Applies both a per-provider timeout and a cumulative timeout across all
/// attempts. Override via `total_timeout_secs` (0 disables the cumulative cap).
async fn handle_llm_call_failover(params: &Value, providers: &[Value]) -> Result<Value, StepError> {
    if providers.is_empty() {
        return Err(permanent("providers array is empty".to_string()));
    }

    let total_timeout = params
        .get("total_timeout_secs")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_TOTAL_TIMEOUT, Duration::from_secs);

    if total_timeout.is_zero() {
        return failover_inner(params, providers).await;
    }

    match tokio::time::timeout(total_timeout, failover_inner(params, providers)).await {
        Ok(result) => result,
        Err(_) => Err(retryable(format!(
            "all providers exceeded total timeout of {total_timeout:?}"
        ))),
    }
}

async fn failover_inner(params: &Value, providers: &[Value]) -> Result<Value, StepError> {
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
                anthropic::call_anthropic(&merged, &api_key, &base).await
            } else {
                openai::call_openai_compat(&merged, &api_key, &base, provider_name).await
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

    let error_msg = match last_error.as_ref() {
        Some(e) => format!("all providers failed, last error: {e}"),
        None => "all providers failed".to_string(),
    };

    Err(permanent(error_msg))
}

fn merge_provider_params(base_params: &Value, provider_config: &Value) -> Value {
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

    if let (Some(merged_obj), Some(config_obj)) =
        (merged.as_object_mut(), provider_config.as_object())
    {
        for (key, value) in config_obj {
            merged_obj.insert(key.clone(), value.clone());
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_providers_returns_error() {
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_llm_call_failover(&json!({}), &[]));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn dry_run_skips_model_call() {
        use std::sync::Arc;
        // validate-then-skip resolves api_key + checks base_url safety BEFORE
        // skipping, so the dry-run must supply a valid (safe) config. Seed the
        // SSRF allowlist for the loopback base so no real DNS/call happens.
        let base = "http://127.0.0.1:1";
        super::super::builtin::mark_url_safe_for_test(base).await;
        let storage: Arc<dyn orch8_storage::StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let mut ctx = StepContext {
            instance_id: orch8_types::ids::InstanceId::new(),
            tenant_id: orch8_types::ids::TenantId::unchecked("t"),
            block_id: orch8_types::ids::BlockId::new("b"),
            params: json!({ "provider": "openai", "model": "gpt-4o", "api_key": "k", "base_url": base }),
            context: orch8_types::context::ExecutionContext::default(),
            attempt: 0,
            storage,
            wait_for_input: None,
        };
        ctx.context.runtime.dry_run = true;
        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["handler"], "llm_call");
        assert_eq!(out["model"], "gpt-4o");
        assert_eq!(out["finish_reason"], "dry_run");
        assert_eq!(out["message"]["content"], "");
    }

    #[tokio::test]
    async fn record_llm_usage_normalizes_both_provider_shapes() {
        use std::sync::Arc;
        let storage: Arc<dyn orch8_storage::StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let ctx = StepContext {
            instance_id: orch8_types::ids::InstanceId::new(),
            tenant_id: orch8_types::ids::TenantId::unchecked("t1"),
            block_id: orch8_types::ids::BlockId::new("b"),
            params: json!({}),
            context: orch8_types::context::ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        // OpenAI shape (prompt_/completion_tokens) and Anthropic shape
        // (input_/output_tokens) both normalize; a response with no usage records nothing.
        record_llm_usage(
            &ctx,
            &json!({ "model": "gpt-4o", "usage": { "prompt_tokens": 12, "completion_tokens": 7 } }),
        )
        .await;
        record_llm_usage(
            &ctx,
            &json!({ "model": "claude", "usage": { "input_tokens": 3, "output_tokens": 4 } }),
        )
        .await;
        record_llm_usage(&ctx, &json!({ "model": "x" })).await; // no usage → skipped

        let now = chrono::Utc::now();
        let agg = storage
            .query_usage(
                "t1",
                now - chrono::Duration::hours(1),
                now + chrono::Duration::hours(1),
            )
            .await
            .unwrap();
        assert_eq!(agg.len(), 2, "two models, no-usage call skipped");
        let gpt = agg.iter().find(|a| a.model == "gpt-4o").unwrap();
        assert_eq!((gpt.input_tokens, gpt.output_tokens), (12, 7));
        let claude = agg.iter().find(|a| a.model == "claude").unwrap();
        assert_eq!((claude.input_tokens, claude.output_tokens), (3, 4));
    }

    #[test]
    fn default_total_timeout_is_reasonable() {
        assert_eq!(DEFAULT_TOTAL_TIMEOUT, Duration::from_secs(120));
        assert!(DEFAULT_TOTAL_TIMEOUT > DEFAULT_PER_PROVIDER_TIMEOUT);
    }

    #[test]
    fn cumulative_timeout_returns_error_on_empty_providers_before_timeout() {
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_llm_call_failover(&json!({}), &[]));
        assert!(matches!(result, Err(StepError::Permanent { .. })));
    }

    #[test]
    fn cumulative_timeout_can_be_disabled_via_zero() {
        let params = json!({"total_timeout_secs": 0});
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_llm_call_failover(&params, &[]));
        assert!(matches!(result, Err(StepError::Permanent { .. })));
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
        assert!(merged.get("messages").is_some());
        assert!(merged.get("providers").is_none());
    }

    #[test]
    fn merge_provider_params_preserves_base_when_empty_override() {
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
}
