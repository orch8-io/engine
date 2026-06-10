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
//! | `response_schema` | object | — | JSON Schema the response must satisfy (validated, auto-repaired) |
//! | `max_repair_attempts` | number | `2` | Schema-repair re-calls per provider attempt (hard cap 5) |
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
pub(crate) mod common;
mod openai;
mod schema;

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

use self::common::{
    merge_json_response_fields, permanent, resolve_api_key, resolve_base_url, retryable,
    set_usage_totals, usage_tokens,
};
use super::StepContext;

/// Shared HTTP client for all LLM calls (connection pooling, TLS, keep-alive).
pub(crate) fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .timeout(Duration::from_secs(300))
            // SSRF: the initial URL is validated by `is_url_safe`, but reqwest
            // follows redirects by default without re-checking. Re-validate
            // every hop and refuse redirects to internal/metadata targets.
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 10 {
                    return attempt.error("too many redirects");
                }
                if crate::handlers::builtin::redirect_target_allowed(attempt.url()) {
                    attempt.follow()
                } else {
                    attempt.error("blocked: redirect targets a private/internal network address")
                }
            }))
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

    // Compile `response_schema` once per step, BEFORE any provider call (and
    // before the dry-run skip), so an invalid schema is surfaced as a config
    // error without burning tokens.
    let response_schema = schema::compile_response_schema(&ctx.params)?;

    if let Some(providers) = ctx.params.get("providers").and_then(Value::as_array) {
        if dry {
            // Validate before skipping: an empty providers array is a config
            // error a dry-run should surface.
            if providers.is_empty() {
                return Err(permanent("providers array is empty".to_string()));
            }
            return Ok(llm_dry_run_stub(&ctx.params));
        }
        return handle_llm_call_failover(&ctx.params, providers, response_schema.as_ref()).await;
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

    let out = match response_schema.as_ref() {
        Some(compiled) => {
            call_provider_with_schema(&ctx.params, &api_key, &base, provider, compiled)
                .await
                .map_err(SchemaCallFailure::into_permanent)?
        }
        None => dispatch_provider(&ctx.params, &api_key, &base, provider).await?,
    };
    // Capture token usage for cost aggregation (best-effort — never fails the call).
    record_llm_usage(&ctx, &out).await;
    Ok(out)
}

/// Route a single call to the correct provider API.
async fn dispatch_provider(
    params: &Value,
    api_key: &str,
    base: &str,
    provider: &str,
) -> Result<Value, StepError> {
    if provider == "anthropic" {
        anthropic::call_anthropic(params, api_key, base).await
    } else {
        openai::call_openai_compat(params, api_key, base, provider).await
    }
}

/// How a schema-validated provider attempt failed.
enum SchemaCallFailure {
    /// The underlying provider call failed (network, auth, API error, …).
    Provider(StepError),
    /// The provider responded, but every response failed schema validation
    /// within the repair budget.
    Exhausted { message: String, details: Value },
}

impl SchemaCallFailure {
    /// Error for the single-provider path: validation exhaustion is permanent
    /// (a step retry would just re-burn the same repair budget).
    fn into_permanent(self) -> StepError {
        match self {
            Self::Provider(e) => e,
            Self::Exhausted { message, details } => StepError::Permanent {
                message,
                details: Some(details),
            },
        }
    }

    /// Error for the failover loop: validation exhaustion is retryable so the
    /// loop proceeds to the next provider. If ALL providers fail, the failover
    /// aggregation converts the last error into a permanent one.
    fn into_retryable_for_failover(self) -> StepError {
        match self {
            Self::Provider(e) => e,
            Self::Exhausted { message, details } => StepError::Retryable {
                message,
                details: Some(details),
            },
        }
    }
}

/// Call one provider with `response_schema` enforcement and bounded repair.
///
/// The assistant text is JSON-extracted (fences/prose tolerated) and validated
/// against the compiled schema. On failure, a repair turn (raw response +
/// corrective user message) is appended and the SAME provider is re-called, up
/// to `max_repair_attempts` times. On success the validated object's top-level
/// fields are merged into the output (existing keys win) and `schema_valid` /
/// `repair_attempts` are added. Token usage is summed across repair calls into
/// the final output's `usage` fields.
async fn call_provider_with_schema(
    params: &Value,
    api_key: &str,
    base: &str,
    provider: &str,
    compiled: &schema::CompiledSchema,
) -> Result<Value, SchemaCallFailure> {
    let mut work = params.clone();
    // Nudge OpenAI-compatible providers toward raw JSON output, unless the
    // step already chose a response_format. Anthropic has no equivalent —
    // prompt + repair handle it.
    if provider != "anthropic" {
        if let Some(obj) = work.as_object_mut() {
            obj.entry("response_format")
                .or_insert_with(|| json!({"type": "json_object"}));
        }
    }

    let max = compiled.max_repair_attempts;
    let (mut total_input, mut total_output) = (0i64, 0i64);
    let mut last_errors: Vec<String> = Vec::new();
    let mut last_response = String::new();

    for attempt in 0..=max {
        let mut out = dispatch_provider(&work, api_key, base, provider)
            .await
            .map_err(SchemaCallFailure::Provider)?;
        let (input, output) = usage_tokens(&out);
        total_input += input;
        total_output += output;

        let content = out
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        match compiled.validate_text(schema::extract_candidate_json(&content)) {
            Ok(valid) => {
                if attempt > 0 {
                    // Account for the tokens burned by repair calls too.
                    set_usage_totals(&mut out, total_input, total_output);
                }
                if let Ok(serialized) = serde_json::to_string(&valid) {
                    merge_json_response_fields(&serialized, &mut out);
                }
                if let Some(obj) = out.as_object_mut() {
                    obj.insert("schema_valid".into(), json!(true));
                    obj.insert("repair_attempts".into(), json!(attempt));
                }
                return Ok(out);
            }
            Err(errors) => {
                warn!(
                    provider = %provider,
                    attempt,
                    errors = ?errors,
                    "llm_call: response failed response_schema validation"
                );
                if attempt < max {
                    schema::append_repair_turn(&mut work, &content, &errors);
                }
                last_errors = errors;
                last_response = content;
            }
        }
    }

    Err(SchemaCallFailure::Exhausted {
        message: format!(
            "response failed response_schema validation after {max} repair attempt(s)"
        ),
        details: schema::exhaustion_details(&last_errors, &last_response),
    })
}

/// Record LLM token usage as a `usage_event`. Normalizes the two provider usage
/// shapes (`OpenAI` `prompt_tokens`/`completion_tokens`, `Anthropic`
/// `input_tokens`/`output_tokens`).
/// Best-effort: a recording failure is logged, never propagated.
async fn record_llm_usage(ctx: &StepContext, out: &Value) {
    let (input, output) = usage_tokens(out);
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
///
/// When `response_schema` is set, validation + repair run per provider
/// attempt (within that provider's timeout); a provider that exhausts its
/// repair budget counts as a failed provider and failover proceeds.
async fn handle_llm_call_failover(
    params: &Value,
    providers: &[Value],
    response_schema: Option<&schema::CompiledSchema>,
) -> Result<Value, StepError> {
    if providers.is_empty() {
        return Err(permanent("providers array is empty".to_string()));
    }

    let total_timeout = params
        .get("total_timeout_secs")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_TOTAL_TIMEOUT, Duration::from_secs);

    if total_timeout.is_zero() {
        return failover_inner(params, providers, response_schema).await;
    }

    match tokio::time::timeout(
        total_timeout,
        failover_inner(params, providers, response_schema),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(retryable(format!(
            "all providers exceeded total timeout of {total_timeout:?}"
        ))),
    }
}

async fn failover_inner(
    params: &Value,
    providers: &[Value],
    response_schema: Option<&schema::CompiledSchema>,
) -> Result<Value, StepError> {
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
            match response_schema {
                Some(compiled) => {
                    call_provider_with_schema(&merged, &api_key, &base, provider_name, compiled)
                        .await
                        .map_err(SchemaCallFailure::into_retryable_for_failover)
                }
                None => dispatch_provider(&merged, &api_key, &base, provider_name).await,
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

    // Aggregate into a permanent error, carrying forward the last error's
    // details (e.g. schema validation_errors / last_response) so they aren't
    // lost when all providers fail.
    let (message, details) = match last_error {
        Some(e) => {
            let message = format!("all providers failed, last error: {e}");
            let (StepError::Retryable { details, .. } | StepError::Permanent { details, .. }) = e;
            (message, details)
        }
        None => ("all providers failed".to_string(), None),
    };

    Err(StepError::Permanent { message, details })
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
            .block_on(handle_llm_call_failover(&json!({}), &[], None));
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
            .block_on(handle_llm_call_failover(&json!({}), &[], None));
        assert!(matches!(result, Err(StepError::Permanent { .. })));
    }

    #[test]
    fn cumulative_timeout_can_be_disabled_via_zero() {
        let params = json!({"total_timeout_secs": 0});
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_llm_call_failover(&params, &[], None));
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

    // --- response_schema validate + repair (mock OpenAI-compatible server) --
    //
    // Same dep-free pattern as the webhook delivery tests: a tiny TCP
    // listener that answers each request with the next queued JSON body and
    // records the parsed request bodies.

    use std::collections::VecDeque;
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Read one HTTP/1.1 request (headers + Content-Length body bytes).
    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        let mut tmp = [0u8; 1024];
        let mut header_end = None;
        loop {
            let n = stream.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if header_end.is_none() {
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    header_end = Some(pos + 4);
                }
            }
            if let Some(end) = header_end {
                let headers = std::str::from_utf8(&buf[..end]).unwrap_or("");
                let cl: usize = headers
                    .lines()
                    .find_map(|l| {
                        let l = l.to_ascii_lowercase();
                        l.strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if buf.len() >= end + cl {
                    break;
                }
            }
        }
        buf
    }

    /// Start a mock OpenAI-compatible server that answers each request with
    /// the next queued response body (HTTP 200). Returns the base URL (already
    /// marked SSRF-safe) and the parsed JSON request bodies received.
    async fn start_openai_mock(
        responses: Vec<Value>,
    ) -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let base = format!("http://127.0.0.1:{port}");
        let requests = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
        let requests_srv = Arc::clone(&requests);
        let queue = Arc::new(tokio::sync::Mutex::new(VecDeque::from(responses)));

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let raw = read_http_request(&mut stream).await;
                if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                    if let Ok(v) = serde_json::from_slice::<Value>(&raw[pos + 4..]) {
                        requests_srv.lock().await.push(v);
                    }
                }
                let body = queue.lock().await.pop_front().unwrap_or_else(
                    || json!({"error": {"message": "mock response queue exhausted"}}),
                );
                let body = serde_json::to_string(&body).unwrap();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        super::super::builtin::mark_url_safe_for_test(&base).await;
        (base, requests)
    }

    /// `OpenAI` chat-completions shaped response with the given assistant text.
    fn openai_resp(content: &str, prompt: i64, completion: i64) -> Value {
        json!({
            "model": "gpt-test",
            "choices": [
                {"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": prompt, "completion_tokens": completion},
        })
    }

    /// JSON Schema used by the validate+repair tests: `{"name": <string>}`.
    fn name_schema() -> Value {
        json!({
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string"}},
        })
    }

    async fn test_ctx(params: Value) -> StepContext {
        let storage: Arc<dyn orch8_storage::StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        StepContext {
            instance_id: orch8_types::ids::InstanceId::new(),
            tenant_id: orch8_types::ids::TenantId::unchecked("t"),
            block_id: orch8_types::ids::BlockId::new("b"),
            params,
            context: orch8_types::context::ExecutionContext::default(),
            attempt: 0,
            storage,
            wait_for_input: None,
        }
    }

    #[tokio::test]
    async fn response_schema_valid_on_first_try() {
        let (base, requests) =
            start_openai_mock(vec![openai_resp(r#"{"name": "ok"}"#, 10, 5)]).await;
        let ctx = test_ctx(json!({
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "k",
            "base_url": base,
            "messages": [{"role": "user", "content": "emit json"}],
            "response_schema": name_schema(),
        }))
        .await;

        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["schema_valid"], true);
        assert_eq!(out["repair_attempts"], 0);
        assert_eq!(out["name"], "ok", "validated fields merged into output");
        assert_eq!(out["usage"]["prompt_tokens"], 10);
        assert_eq!(out["usage"]["completion_tokens"], 5);

        let reqs = requests.lock().await;
        assert_eq!(reqs.len(), 1, "exactly one provider call");
        assert_eq!(
            reqs[0]["response_format"]["type"], "json_object",
            "json_object nudge injected for OpenAI-compatible provider"
        );
    }

    #[tokio::test]
    async fn response_schema_repaired_on_second_attempt() {
        // First response: fenced + wrong type for `name`. Second: valid.
        let (base, requests) = start_openai_mock(vec![
            openai_resp("```json\n{\"name\": 42}\n```", 10, 5),
            openai_resp(r#"{"name": "fixed"}"#, 20, 7),
        ])
        .await;
        let ctx = test_ctx(json!({
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "k",
            "base_url": base,
            "messages": [{"role": "user", "content": "emit json"}],
            "response_schema": name_schema(),
        }))
        .await;

        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["schema_valid"], true);
        assert_eq!(out["repair_attempts"], 1);
        assert_eq!(out["name"], "fixed");
        // Usage accumulated across the initial call + one repair call.
        assert_eq!(out["usage"]["prompt_tokens"], 30);
        assert_eq!(out["usage"]["completion_tokens"], 12);

        let reqs = requests.lock().await;
        assert_eq!(reqs.len(), 2);
        // The repair call carries the raw failing response as an assistant
        // message plus a corrective user message.
        let msgs = reqs[1]["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[1]["role"], "assistant");
        assert_eq!(msgs[1]["content"], "```json\n{\"name\": 42}\n```");
        assert_eq!(msgs[2]["role"], "user");
        let repair = msgs[2]["content"].as_str().unwrap();
        assert!(repair.contains("Validation errors"), "{repair}");
        assert!(
            repair.contains("Return ONLY the corrected JSON"),
            "{repair}"
        );
    }

    #[tokio::test]
    async fn response_schema_exhausted_repairs_is_permanent_with_details() {
        let (base, requests) = start_openai_mock(vec![
            openai_resp(r#"{"wrong": 1}"#, 10, 5),
            openai_resp(r#"{"still": "wrong"}"#, 10, 5),
        ])
        .await;
        let ctx = test_ctx(json!({
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "k",
            "base_url": base,
            "messages": [{"role": "user", "content": "emit json"}],
            "response_schema": name_schema(),
            "max_repair_attempts": 1,
        }))
        .await;

        let err = handle_llm_call(ctx)
            .await
            .expect_err("must exhaust repairs");
        match err {
            StepError::Permanent { message, details } => {
                assert!(
                    message.contains("response_schema validation after 1 repair attempt"),
                    "{message}"
                );
                let details = details.expect("details must be present");
                let errors = details["validation_errors"].as_array().unwrap();
                assert!(!errors.is_empty());
                assert_eq!(details["last_response"], r#"{"still": "wrong"}"#);
            }
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other}"),
        }
        assert_eq!(requests.lock().await.len(), 2, "initial call + 1 repair");
    }

    #[tokio::test]
    async fn response_schema_failover_moves_to_next_provider() {
        // Provider A keeps returning schema-invalid output; provider B is valid.
        let (base_a, requests_a) =
            start_openai_mock(vec![openai_resp(r#"{"nope": true}"#, 1, 1)]).await;
        let (base_b, requests_b) =
            start_openai_mock(vec![openai_resp(r#"{"name": "from-b"}"#, 2, 2)]).await;
        let ctx = test_ctx(json!({
            "messages": [{"role": "user", "content": "emit json"}],
            "response_schema": name_schema(),
            "max_repair_attempts": 0,
            "providers": [
                {"provider": "openai", "api_key": "k", "model": "m", "base_url": base_a},
                {"provider": "openai", "api_key": "k", "model": "m", "base_url": base_b},
            ],
        }))
        .await;

        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["schema_valid"], true);
        assert_eq!(out["repair_attempts"], 0);
        assert_eq!(out["name"], "from-b");
        assert_eq!(out["tried"], json!(["openai", "openai"]));
        assert_eq!(requests_a.lock().await.len(), 1);
        assert_eq!(requests_b.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn response_schema_all_providers_fail_validation_is_permanent() {
        let (base_a, _) = start_openai_mock(vec![openai_resp(r#"{"a": 1}"#, 1, 1)]).await;
        let (base_b, _) = start_openai_mock(vec![openai_resp("not json at all", 1, 1)]).await;
        let ctx = test_ctx(json!({
            "messages": [{"role": "user", "content": "emit json"}],
            "response_schema": name_schema(),
            "max_repair_attempts": 0,
            "providers": [
                {"provider": "openai", "api_key": "k", "model": "m", "base_url": base_a},
                {"provider": "openai", "api_key": "k", "model": "m", "base_url": base_b},
            ],
        }))
        .await;

        let err = handle_llm_call(ctx)
            .await
            .expect_err("all providers invalid");
        match err {
            StepError::Permanent { message, details } => {
                assert!(message.contains("all providers failed"), "{message}");
                let details = details.expect("last validation details carried forward");
                assert!(!details["validation_errors"].as_array().unwrap().is_empty());
                assert_eq!(details["last_response"], "not json at all");
            }
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other}"),
        }
    }

    #[tokio::test]
    async fn invalid_response_schema_fails_without_provider_call() {
        let (base, requests) = start_openai_mock(vec![]).await;
        let ctx = test_ctx(json!({
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "k",
            "base_url": base,
            "response_schema": {"type": "not-a-real-type"},
        }))
        .await;

        let err = handle_llm_call(ctx)
            .await
            .expect_err("bad schema must fail");
        assert!(matches!(err, StepError::Permanent { .. }), "{err}");
        assert!(err.to_string().contains("not a valid JSON Schema"), "{err}");
        assert_eq!(requests.lock().await.len(), 0, "no provider call made");
    }

    #[tokio::test]
    async fn dry_run_with_response_schema_skips_validation() {
        let base = "http://127.0.0.1:2";
        super::super::builtin::mark_url_safe_for_test(base).await;
        let mut ctx = test_ctx(json!({
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "k",
            "base_url": base,
            "response_schema": name_schema(),
        }))
        .await;
        ctx.context.runtime.dry_run = true;

        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["finish_reason"], "dry_run");
        assert!(
            out.get("schema_valid").is_none(),
            "no validation in dry-run"
        );
    }
}
