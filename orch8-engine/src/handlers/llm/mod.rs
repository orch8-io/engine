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
//! | `messages` | array | `[]` | Chat messages (`{role, content}`); `content` is a string or an array of content blocks (see Multimodal content) |
//! | `system` | string | — | System prompt (Anthropic shorthand) |
//! | `temperature` | number | — | Sampling temperature |
//! | `max_tokens` | number | `4096` | Max output tokens |
//! | `tools` | array | — | Tool/function definitions |
//! | `tool_choice` | string/object | — | Tool selection strategy |
//! | `total_timeout_secs` | number | `120` | Cumulative timeout across all failover attempts (0 disables) |
//! | `per_provider_timeout_secs` | number | `60` | Per-provider timeout in failover mode |
//! | `response_schema` | object | — | JSON Schema the response must satisfy (validated, auto-repaired) |
//! | `max_repair_attempts` | number | `2` | Schema-repair re-calls per provider attempt (hard cap 5) |
//! | `max_image_bytes` | number | 20 MiB | Per-image size cap, pre-encoding (can only lower the default) |
//! | `stream` | bool | `false` | Consume the provider's SSE stream (see Streaming) |
//! | `stream_idle_timeout_secs` | number | `30` | Max gap between streamed chunks before failing retryable |
//!
//! ## Streaming
//!
//! With `stream: true` the provider call uses the streaming wire protocol
//! (`OpenAI` `/chat/completions` SSE with `stream_options.include_usage`;
//! Anthropic `/messages` event stream). Incremental text deltas are published
//! to the in-process [`crate::stream_bus`] as `llm_delta` events — clients
//! watching `GET /instances/{id}/stream` receive them live. The step's
//! **durable output is unchanged**: deltas are accumulated and the completed
//! output has exactly the non-streaming shape (message, `finish_reason`,
//! `usage`, tool calls), so downstream blocks are unaffected.
//!
//! Failure taxonomy: a mid-stream connection drop, a stream that ends before
//! the provider's terminal event (`[DONE]` / `message_stop`), or a chunk gap
//! exceeding `stream_idle_timeout_secs` all fail **retryable** (and are
//! eligible for provider failover). Provider `error` events map to the usual
//! retryable/permanent split.
//!
//! Interactions: `stream` combined with `response_schema` falls back to
//! non-streaming (logged) — the validate/repair loop requires complete
//! responses, and the durable output is identical either way. `dry_run`
//! skips the provider call entirely, exactly as without streaming. Failover
//! and multimodal content work unchanged (request building is shared).
//!
//! ## Multimodal content
//!
//! A message's `content` may be a plain string (unchanged behavior) or an
//! array of content blocks:
//!
//! - `{"type": "text", "text": "…"}`
//! - `{"type": "image", "artifact": "<key>", "media_type": "image/png"}` —
//!   image bytes fetched from the artifact store before provider dispatch.
//!   `artifact` accepts the same shapes as `blob_get`'s `ref` (bare key,
//!   `{"key"}`, `{"artifact": {"key"}}`). `media_type` defaults to `image/png`.
//! - `{"type": "image", "data": "<base64>", "media_type": "image/png"}` —
//!   inline base64 passthrough.
//!
//! Images are resolved once (before failover, so every provider attempt
//! reuses the fetched bytes); each provider adapter converts to its wire
//! shape — OpenAI-compatible `image_url` data URLs, Anthropic base64
//! `source` blocks. See the `multimodal` submodule.
//!
//! ## Telemetry
//!
//! Each successful call emits a `gen_ai.client.inference` `tracing` event
//! carrying `OTel` `GenAI` semantic-convention fields (`gen_ai.operation.name`,
//! `gen_ai.system`, `gen_ai.request.model`, `gen_ai.response.model`,
//! `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`) — structured
//! telemetry consumable by any subscriber, no OTLP exporter required.
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
mod multimodal;
mod openai;
mod schema;
mod sse;

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

/// Live-delta publisher for a streaming `llm_call` step: forwards incremental
/// text fragments to the per-instance [`crate::stream_bus`] channel so clients
/// watching the instance's SSE stream see tokens as they arrive. Publishing is
/// best-effort and a no-op when nobody is subscribed; the durable step output
/// is always the full accumulated response regardless.
pub(crate) struct DeltaSink {
    instance_id: orch8_types::ids::InstanceId,
    block_id: String,
}

impl DeltaSink {
    /// Publish one text delta for this step.
    fn publish(&self, delta: &str) {
        let bus = crate::stream_bus::stream_bus();
        if !bus.has_subscribers(self.instance_id) {
            return; // skip the event allocation when nobody is watching
        }
        bus.publish(
            self.instance_id,
            crate::stream_bus::StreamEvent::LlmDelta {
                block_id: self.block_id.clone(),
                delta: delta.to_string(),
            },
        );
    }
}

/// Main handler: routes to the correct provider API.
///
/// If the params contain a `providers` array, iterates through each provider
/// in order, attempting the call. On failure, tries the next provider.
/// The output includes a `tried` array listing providers attempted in order.
pub async fn handle_llm_call(mut ctx: StepContext) -> Result<Value, StepError> {
    let dry = ctx.is_dry_run();

    // Compile `response_schema` once per step, BEFORE any provider call (and
    // before the dry-run skip), so an invalid schema is surfaced as a config
    // error without burning tokens.
    let response_schema = schema::compile_response_schema(&ctx.params)?;

    // Streaming applies only without `response_schema`: the validate/repair
    // loop needs complete responses, so `stream` + `response_schema` falls
    // back to a regular (non-streaming) call with identical durable output.
    let stream_requested = ctx
        .params
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let delta_sink = if stream_requested && !dry {
        if response_schema.is_some() {
            warn!(
                block_id = %ctx.block_id.as_str(),
                "llm_call: stream=true combined with response_schema — falling back to non-streaming"
            );
            None
        } else {
            Some(DeltaSink {
                instance_id: ctx.instance_id,
                block_id: ctx.block_id.as_str().to_string(),
            })
        }
    } else {
        None
    };

    if let Some(providers) = ctx.params.get("providers").and_then(Value::as_array) {
        if dry {
            // Validate before skipping: an empty providers array is a config
            // error a dry-run should surface.
            if providers.is_empty() {
                return Err(permanent("providers array is empty".to_string()));
            }
            return Ok(llm_dry_run_stub(&ctx.params));
        }
        let providers = providers.clone();
        // Resolve artifact-backed image blocks ONCE, before the failover loop
        // and schema paths, so every provider attempt reuses the fetched bytes.
        multimodal::resolve_message_images(&ctx.storage, ctx.instance_id, &mut ctx.params).await?;
        return handle_llm_call_failover(
            &ctx.params,
            &providers,
            response_schema.as_ref(),
            delta_sink.as_ref(),
        )
        .await;
    }

    let provider = ctx
        .params
        .get("provider")
        .and_then(Value::as_str)
        .unwrap_or("openai")
        .to_string();

    // Resolve + validate (api key present, base URL allowed) BEFORE the
    // dry-run skip, so a dry-run still catches missing keys / blocked URLs.
    let api_key = resolve_api_key(&ctx.params, &provider)?;
    let base = resolve_base_url(&ctx.params, &provider);

    if !super::builtin::is_url_safe(&base).await {
        return Err(permanent(format!("base_url is not allowed: {base}")));
    }

    // Dry-run: validation passed; skip only the model call. Mirror the output
    // shape (empty assistant message) so downstream templates resolve. Image
    // resolution stays AFTER this skip — a dry-run never reads artifacts.
    if dry {
        return Ok(llm_dry_run_stub(&ctx.params));
    }

    multimodal::resolve_message_images(&ctx.storage, ctx.instance_id, &mut ctx.params).await?;

    let out = match response_schema.as_ref() {
        Some(compiled) => {
            call_provider_with_schema(&ctx.params, &api_key, &base, &provider, compiled)
                .await
                .map_err(SchemaCallFailure::into_permanent)?
        }
        None => {
            dispatch_provider(&ctx.params, &api_key, &base, &provider, delta_sink.as_ref()).await?
        }
    };
    emit_gen_ai_telemetry(&ctx.params, &provider, &out);
    // Capture token usage for cost aggregation (best-effort — never fails the call).
    record_llm_usage(&ctx, &out).await;
    Ok(out)
}

/// Emit `OTel` `GenAI` semantic-convention telemetry for a completed LLM call.
///
/// A single structured `tracing::info!` event with the message
/// `gen_ai.client.inference`, consumable by any subscriber — convention-named
/// structured telemetry only, no OTLP exporter involved. Fields (greppable):
///
/// - `gen_ai.operation.name` — always `"chat"`.
/// - `gen_ai.system` — provider name (`"openai"`, `"anthropic"`, …).
/// - `gen_ai.request.model` — model requested in params (or the provider default).
/// - `gen_ai.response.model` — model reported by the provider response, when present.
/// - `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens` — token usage
///   as reported by the provider (0 when absent).
fn emit_gen_ai_telemetry(params: &Value, provider: &str, out: &Value) {
    let request_model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            if provider == "anthropic" {
                anthropic_default_model()
            } else {
                openai_default_model()
            }
        });
    let response_model = out
        .get("model")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty());
    let (input_tokens, output_tokens) = usage_tokens(out);
    tracing::info!(
        gen_ai.operation.name = "chat",
        gen_ai.system = provider,
        gen_ai.request.model = request_model,
        gen_ai.response.model = response_model,
        gen_ai.usage.input_tokens = input_tokens,
        gen_ai.usage.output_tokens = output_tokens,
        "gen_ai.client.inference"
    );
}

/// Route a single call to the correct provider API. `deltas: Some(_)` selects
/// the streaming wire protocol (SSE consumption + delta publication); the
/// returned output has the same shape either way.
async fn dispatch_provider(
    params: &Value,
    api_key: &str,
    base: &str,
    provider: &str,
    deltas: Option<&DeltaSink>,
) -> Result<Value, StepError> {
    if provider == "anthropic" {
        anthropic::call_anthropic(params, api_key, base, deltas).await
    } else {
        openai::call_openai_compat(params, api_key, base, provider, deltas).await
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
        // Always non-streaming: schema validation + repair needs the complete
        // response (the `stream` fallback is decided in `handle_llm_call`).
        let mut out = dispatch_provider(&work, api_key, base, provider, None)
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
    delta_sink: Option<&DeltaSink>,
) -> Result<Value, StepError> {
    if providers.is_empty() {
        return Err(permanent("providers array is empty".to_string()));
    }

    let total_timeout = params
        .get("total_timeout_secs")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_TOTAL_TIMEOUT, Duration::from_secs);

    if total_timeout.is_zero() {
        return failover_inner(params, providers, response_schema, delta_sink).await;
    }

    match tokio::time::timeout(
        total_timeout,
        failover_inner(params, providers, response_schema, delta_sink),
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
    delta_sink: Option<&DeltaSink>,
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
                None => {
                    dispatch_provider(&merged, &api_key, &base, provider_name, delta_sink).await
                }
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
                emit_gen_ai_telemetry(&merged, provider_name, &output);
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
            .block_on(handle_llm_call_failover(&json!({}), &[], None, None));
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
            .block_on(handle_llm_call_failover(&json!({}), &[], None, None));
        assert!(matches!(result, Err(StepError::Permanent { .. })));
    }

    #[test]
    fn cumulative_timeout_can_be_disabled_via_zero() {
        let params = json!({"total_timeout_secs": 0});
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_llm_call_failover(&params, &[], None, None));
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

    // --- multimodal image content (artifact store → provider wire shape) ---

    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use orch8_storage::artifacts::ObjectArtifactStore;

    /// Storage with an in-memory artifact backend, for image-resolution tests.
    async fn storage_with_artifacts() -> Arc<dyn orch8_storage::StorageBackend> {
        Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        )
    }

    fn ctx_with(
        storage: &Arc<dyn orch8_storage::StorageBackend>,
        instance_id: orch8_types::ids::InstanceId,
        params: Value,
    ) -> StepContext {
        StepContext {
            instance_id,
            tenant_id: orch8_types::ids::TenantId::unchecked("t"),
            block_id: orch8_types::ids::BlockId::new("b"),
            params,
            context: orch8_types::context::ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(storage),
            wait_for_input: None,
        }
    }

    #[tokio::test]
    async fn openai_artifact_image_sent_as_data_url() {
        let (base, requests) = start_openai_mock(vec![openai_resp("a red square", 5, 3)]).await;
        let storage = storage_with_artifacts().await;
        let id = orch8_types::ids::InstanceId::new();
        let raw: &[u8] = b"\x89PNG\r\n fake image";
        let aref = storage
            .put_artifact(id, "image/png", bytes::Bytes::from_static(raw))
            .await
            .unwrap();

        let ctx = ctx_with(
            &storage,
            id,
            json!({
                "provider": "openai",
                "model": "gpt-test",
                "api_key": "k",
                "base_url": base,
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "what is this?"},
                    {"type": "image", "artifact": {"artifact": {"key": aref.key}}},
                ]}],
            }),
        );
        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["message"]["content"], "a red square");

        let reqs = requests.lock().await;
        assert_eq!(reqs.len(), 1);
        let blocks = reqs[0]["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks[0], json!({"type": "text", "text": "what is this?"}));
        assert_eq!(blocks[1]["type"], "image_url");
        let url = blocks[1]["image_url"]["url"].as_str().unwrap();
        assert_eq!(
            url,
            format!("data:image/png;base64,{}", STANDARD.encode(raw)),
            "outgoing OpenAI body carries the artifact bytes as a data URL"
        );
    }

    #[tokio::test]
    async fn anthropic_artifact_image_sent_as_base64_source_block() {
        // The mock answers any path, so it serves /messages too; queue an
        // Anthropic-shaped response body.
        let (base, requests) = start_openai_mock(vec![json!({
            "content": [{"type": "text", "text": "a cat"}],
            "model": "claude-test",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 4, "output_tokens": 2},
        })])
        .await;
        let storage = storage_with_artifacts().await;
        let id = orch8_types::ids::InstanceId::new();
        let raw: &[u8] = b"\xff\xd8\xff fake jpeg";
        let aref = storage
            .put_artifact(id, "image/jpeg", bytes::Bytes::from_static(raw))
            .await
            .unwrap();

        let ctx = ctx_with(
            &storage,
            id,
            json!({
                "provider": "anthropic",
                "model": "claude-test",
                "api_key": "k",
                "base_url": base,
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "describe"},
                    {"type": "image", "artifact": aref.key, "media_type": "image/jpeg"},
                ]}],
            }),
        );
        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["message"]["content"], "a cat");

        let reqs = requests.lock().await;
        assert_eq!(reqs.len(), 1);
        let blocks = reqs[0]["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks[0], json!({"type": "text", "text": "describe"}));
        assert_eq!(
            blocks[1],
            json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": STANDARD.encode(raw),
                },
            }),
            "outgoing Anthropic body carries the base64 source block"
        );
    }

    #[tokio::test]
    async fn missing_image_artifact_is_permanent_without_provider_call() {
        let (base, requests) = start_openai_mock(vec![]).await;
        let storage = storage_with_artifacts().await;
        let id = orch8_types::ids::InstanceId::new();
        // Owned by this instance (passes the ownership guard) but nonexistent.
        let missing = format!("{}/nonexistent", id.into_uuid());

        let ctx = ctx_with(
            &storage,
            id,
            json!({
                "provider": "openai",
                "model": "gpt-test",
                "api_key": "k",
                "base_url": base,
                "messages": [{"role": "user", "content": [
                    {"type": "image", "artifact": missing},
                ]}],
            }),
        );
        let err = handle_llm_call(ctx)
            .await
            .expect_err("missing artifact must fail the step");
        let StepError::Permanent { message, .. } = err else {
            panic!("expected Permanent");
        };
        assert!(message.contains("not found"), "{message}");
        assert_eq!(requests.lock().await.len(), 0, "no provider call made");
    }

    #[tokio::test]
    async fn failover_resolves_images_once_and_reuses_them() {
        // Provider A fails key resolution (unset env var), so the loop moves
        // on; provider B must still receive the already-resolved data URL —
        // resolution happened once, before the failover loop.
        let (base_b, requests_b) = start_openai_mock(vec![openai_resp("ok", 1, 1)]).await;
        let storage = storage_with_artifacts().await;
        let id = orch8_types::ids::InstanceId::new();
        let raw: &[u8] = b"png-bytes";
        let aref = storage
            .put_artifact(id, "image/png", bytes::Bytes::from_static(raw))
            .await
            .unwrap();

        let ctx = ctx_with(
            &storage,
            id,
            json!({
                "messages": [{"role": "user", "content": [
                    {"type": "image", "artifact": aref.key},
                ]}],
                "providers": [
                    {"provider": "openai", "api_key_env": "LLM_TEST_UNSET_KEY_VAR", "model": "m", "base_url": base_b},
                    {"provider": "openai", "api_key": "k", "model": "m", "base_url": base_b},
                ],
            }),
        );
        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["tried"], json!(["openai", "openai"]));

        let reqs = requests_b.lock().await;
        assert_eq!(reqs.len(), 1, "only the second provider was called");
        let url = reqs[0]["messages"][0]["content"][0]["image_url"]["url"]
            .as_str()
            .unwrap();
        assert_eq!(
            url,
            format!("data:image/png;base64,{}", STANDARD.encode(raw))
        );
    }

    // --- gen_ai.* telemetry (OTel GenAI semantic conventions) --------------
    //
    // Same Layer-based capture pattern as the warning-capture tests in
    // expression.rs / template.rs: a subscriber layer collects events whose
    // message is `gen_ai.client.inference` into a map of field → rendered value.

    #[derive(Default)]
    struct FieldCollector {
        fields: std::collections::HashMap<String, String>,
    }

    impl tracing::field::Visit for FieldCollector {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    struct GenAiCapture {
        events: Arc<std::sync::Mutex<Vec<std::collections::HashMap<String, String>>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for GenAiCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut collector = FieldCollector::default();
            event.record(&mut collector);
            if collector.fields.get("message").map(String::as_str)
                == Some("gen_ai.client.inference")
            {
                self.events.lock().unwrap().push(collector.fields);
            }
        }
    }

    #[tokio::test]
    async fn gen_ai_telemetry_event_emitted_on_success() {
        use tracing_subscriber::layer::SubscriberExt;
        let (base, _requests) = start_openai_mock(vec![openai_resp("hi", 11, 4)]).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(GenAiCapture {
            events: Arc::clone(&events),
        });
        // Thread-local default: the #[tokio::test] current-thread runtime runs
        // the whole call on this thread, so every event is captured.
        let _guard = tracing::subscriber::set_default(subscriber);

        let ctx = test_ctx(json!({
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "k",
            "base_url": base,
            "messages": [{"role": "user", "content": "hello"}],
        }))
        .await;
        handle_llm_call(ctx).await.unwrap();

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1, "exactly one gen_ai.client.inference event");
        let e = &events[0];
        assert_eq!(e["gen_ai.operation.name"], "chat");
        assert_eq!(e["gen_ai.system"], "openai");
        assert_eq!(e["gen_ai.request.model"], "gpt-test");
        assert_eq!(e["gen_ai.response.model"], "gpt-test");
        assert_eq!(e["gen_ai.usage.input_tokens"], "11");
        assert_eq!(e["gen_ai.usage.output_tokens"], "4");
    }

    #[tokio::test]
    async fn gen_ai_telemetry_emitted_from_failover_path() {
        use tracing_subscriber::layer::SubscriberExt;
        let (base, _requests) = start_openai_mock(vec![openai_resp("hi", 2, 1)]).await;
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(GenAiCapture {
            events: Arc::clone(&events),
        });
        let _guard = tracing::subscriber::set_default(subscriber);

        let ctx = test_ctx(json!({
            "messages": [{"role": "user", "content": "hello"}],
            "providers": [
                {"provider": "openai", "api_key": "k", "model": "m-failover", "base_url": base},
            ],
        }))
        .await;
        handle_llm_call(ctx).await.unwrap();

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["gen_ai.request.model"], "m-failover");
        assert_eq!(events[0]["gen_ai.system"], "openai");
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

    // --- streaming (mock SSE servers) ---------------------------------------
    //
    // Same dep-free TCP pattern as `start_openai_mock`, but the response is a
    // Server-Sent Events body (no Content-Length; body ends when the
    // connection closes). `hold_open` keeps the socket open after the body to
    // exercise the inter-chunk idle timeout.

    async fn start_sse_mock(
        sse_body: String,
        hold_open: bool,
    ) -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let base = format!("http://127.0.0.1:{port}");
        let requests = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
        let requests_srv = Arc::clone(&requests);

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
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                     Cache-Control: no-cache\r\nConnection: close\r\n\r\n{sse_body}"
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                if hold_open {
                    // Leave the connection open with no further data so the
                    // client's idle timeout (not EOF) decides the outcome.
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
                let _ = stream.shutdown().await;
            }
        });

        super::super::builtin::mark_url_safe_for_test(&base).await;
        (base, requests)
    }

    /// Render JSON chunks as `OpenAI`-style `data:` SSE events.
    fn openai_sse(chunks: &[Value], with_done: bool) -> String {
        use std::fmt::Write as _;
        let mut body = String::new();
        for chunk in chunks {
            let _ = write!(body, "data: {chunk}\n\n");
        }
        if with_done {
            body.push_str("data: [DONE]\n\n");
        }
        body
    }

    /// Render `(event, data)` pairs as Anthropic-style named SSE events.
    fn anthropic_sse(events: &[(&str, Value)]) -> String {
        use std::fmt::Write as _;
        let mut body = String::new();
        for (name, data) in events {
            let _ = write!(body, "event: {name}\ndata: {data}\n\n");
        }
        body
    }

    /// A complete `OpenAI` SSE stream producing "Hello world" with usage 10/5.
    fn openai_hello_stream() -> String {
        openai_sse(
            &[
                json!({"id": "c1", "model": "gpt-test", "choices": [
                    {"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}]}),
                json!({"choices": [{"index": 0, "delta": {"content": "Hello "}, "finish_reason": null}]}),
                json!({"choices": [{"index": 0, "delta": {"content": "world"}, "finish_reason": null}]}),
                json!({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
                json!({"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 5}}),
            ],
            true,
        )
    }

    fn stream_params(base: &str) -> Value {
        json!({
            "provider": "openai",
            "model": "gpt-test",
            "api_key": "k",
            "base_url": base,
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true,
        })
    }

    #[tokio::test]
    async fn openai_streaming_output_matches_non_streaming_shape() {
        // Non-streaming reference call.
        let (base_ref, _) = start_openai_mock(vec![openai_resp("Hello world", 10, 5)]).await;
        let mut ref_params = stream_params(&base_ref);
        ref_params["stream"] = json!(false);
        let reference = handle_llm_call(test_ctx(ref_params).await).await.unwrap();

        // Streaming call over the canned SSE stream.
        let (base, requests) = start_sse_mock(openai_hello_stream(), false).await;
        let out = handle_llm_call(test_ctx(stream_params(&base)).await)
            .await
            .unwrap();

        assert_eq!(
            out, reference,
            "accumulated streaming output must equal the non-streaming shape"
        );
        assert_eq!(out["message"]["content"], "Hello world");
        assert_eq!(out["usage"]["prompt_tokens"], 10);
        assert_eq!(out["usage"]["completion_tokens"], 5);
        assert_eq!(out["finish_reason"], "stop");

        let reqs = requests.lock().await;
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0]["stream"], true, "provider asked to stream");
        assert_eq!(
            reqs[0]["stream_options"]["include_usage"], true,
            "usage requested so the streamed output keeps token accounting"
        );
    }

    #[tokio::test]
    async fn openai_streaming_accumulates_tool_call_deltas() {
        let body = openai_sse(
            &[
                json!({"model": "gpt-test", "choices": [{"index": 0, "delta": {"role": "assistant", "tool_calls": [
                    {"index": 0, "id": "call_1", "type": "function", "function": {"name": "search", "arguments": ""}}
                ]}, "finish_reason": null}]}),
                json!({"choices": [{"index": 0, "delta": {"tool_calls": [
                    {"index": 0, "function": {"arguments": "{\"q\":"}}
                ]}, "finish_reason": null}]}),
                json!({"choices": [{"index": 0, "delta": {"tool_calls": [
                    {"index": 0, "function": {"arguments": "\"rust\"}"}}
                ]}, "finish_reason": null}]}),
                json!({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
                json!({"choices": [], "usage": {"prompt_tokens": 7, "completion_tokens": 3}}),
            ],
            true,
        );
        let (base, _requests) = start_sse_mock(body, false).await;
        let out = handle_llm_call(test_ctx(stream_params(&base)).await)
            .await
            .unwrap();

        assert_eq!(out["finish_reason"], "tool_calls");
        assert_eq!(
            out["message"]["content"],
            Value::Null,
            "tool-call-only response keeps content null like non-streaming"
        );
        let calls = out["message"]["tool_calls"].as_array().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["id"], "call_1");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "search");
        assert_eq!(calls[0]["function"]["arguments"], "{\"q\":\"rust\"}");
        assert_eq!(out["usage"]["prompt_tokens"], 7);
    }

    #[tokio::test]
    async fn openai_stream_dropped_before_done_is_retryable() {
        // Chunks but no `[DONE]`: the connection closes mid-response.
        let body = openai_sse(
            &[json!({"model": "gpt-test", "choices": [
                {"index": 0, "delta": {"role": "assistant", "content": "Hel"}, "finish_reason": null}]})],
            false,
        );
        let (base, _) = start_sse_mock(body, false).await;
        let err = handle_llm_call(test_ctx(stream_params(&base)).await)
            .await
            .expect_err("incomplete stream must fail");
        match err {
            StepError::Retryable { message, .. } => {
                assert!(message.contains("[DONE]"), "{message}");
            }
            other @ StepError::Permanent { .. } => panic!("expected Retryable, got {other}"),
        }
    }

    #[tokio::test]
    async fn openai_stream_idle_timeout_is_retryable() {
        // One chunk, then the socket stays open and silent.
        let body = openai_sse(
            &[json!({"model": "gpt-test", "choices": [
                {"index": 0, "delta": {"role": "assistant", "content": "He"}, "finish_reason": null}]})],
            false,
        );
        let (base, _) = start_sse_mock(body, true).await;
        let mut params = stream_params(&base);
        params["stream_idle_timeout_secs"] = json!(1);
        let err = handle_llm_call(test_ctx(params).await)
            .await
            .expect_err("stalled stream must fail");
        match err {
            StepError::Retryable { message, .. } => {
                assert!(message.contains("stalled"), "{message}");
            }
            other @ StepError::Permanent { .. } => panic!("expected Retryable, got {other}"),
        }
    }

    /// A complete Anthropic event stream producing "a cat" with usage 4/2.
    fn anthropic_cat_stream() -> String {
        anthropic_sse(&[
            (
                "message_start",
                json!({"type": "message_start", "message": {
                "id": "m1", "model": "claude-test", "role": "assistant",
                "usage": {"input_tokens": 4, "output_tokens": 1}}}),
            ),
            (
                "content_block_start",
                json!({"type": "content_block_start", "index": 0,
                "content_block": {"type": "text", "text": ""}}),
            ),
            ("ping", json!({"type": "ping"})),
            (
                "content_block_delta",
                json!({"type": "content_block_delta", "index": 0,
                "delta": {"type": "text_delta", "text": "a "}}),
            ),
            (
                "content_block_delta",
                json!({"type": "content_block_delta", "index": 0,
                "delta": {"type": "text_delta", "text": "cat"}}),
            ),
            (
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 0}),
            ),
            (
                "message_delta",
                json!({"type": "message_delta",
                "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 2}}),
            ),
            ("message_stop", json!({"type": "message_stop"})),
        ])
    }

    fn anthropic_stream_params(base: &str) -> Value {
        json!({
            "provider": "anthropic",
            "model": "claude-test",
            "api_key": "k",
            "base_url": base,
            "messages": [{"role": "user", "content": "describe"}],
            "stream": true,
        })
    }

    #[tokio::test]
    async fn anthropic_streaming_output_matches_non_streaming_shape() {
        // Non-streaming reference call against the canned full response.
        let (base_ref, _) = start_openai_mock(vec![json!({
            "content": [{"type": "text", "text": "a cat"}],
            "model": "claude-test",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 4, "output_tokens": 2},
        })])
        .await;
        let mut ref_params = anthropic_stream_params(&base_ref);
        ref_params["stream"] = json!(false);
        let reference = handle_llm_call(test_ctx(ref_params).await).await.unwrap();

        let (base, requests) = start_sse_mock(anthropic_cat_stream(), false).await;
        let out = handle_llm_call(test_ctx(anthropic_stream_params(&base)).await)
            .await
            .unwrap();

        assert_eq!(
            out, reference,
            "accumulated streaming output must equal the non-streaming shape"
        );
        assert_eq!(out["message"]["content"], "a cat");
        assert_eq!(out["usage"], json!({"input_tokens": 4, "output_tokens": 2}));
        assert_eq!(out["finish_reason"], "end_turn");
        assert_eq!(out["model"], "claude-test");

        let reqs = requests.lock().await;
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0]["stream"], true);
    }

    #[tokio::test]
    async fn anthropic_streaming_accumulates_tool_use_input_json() {
        let body = anthropic_sse(&[
            (
                "message_start",
                json!({"type": "message_start", "message": {
                "id": "m1", "model": "claude-test", "role": "assistant",
                "usage": {"input_tokens": 9, "output_tokens": 1}}}),
            ),
            (
                "content_block_start",
                json!({"type": "content_block_start", "index": 0,
                "content_block": {"type": "tool_use", "id": "tc_1", "name": "search", "input": {}}}),
            ),
            (
                "content_block_delta",
                json!({"type": "content_block_delta", "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": "{\"q\":"}}),
            ),
            (
                "content_block_delta",
                json!({"type": "content_block_delta", "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": "\"rust\"}"}}),
            ),
            (
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 0}),
            ),
            (
                "message_delta",
                json!({"type": "message_delta",
                "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 6}}),
            ),
            ("message_stop", json!({"type": "message_stop"})),
        ]);
        let (base, _) = start_sse_mock(body, false).await;
        let out = handle_llm_call(test_ctx(anthropic_stream_params(&base)).await)
            .await
            .unwrap();

        assert_eq!(out["finish_reason"], "tool_use");
        let calls = out["message"]["tool_calls"].as_array().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["id"], "tc_1");
        assert_eq!(calls[0]["function"]["name"], "search");
        assert_eq!(calls[0]["function"]["arguments"], "{\"q\":\"rust\"}");
        assert_eq!(out["usage"]["output_tokens"], 6);
    }

    #[tokio::test]
    async fn anthropic_stream_overloaded_error_event_is_retryable() {
        let body = anthropic_sse(&[
            (
                "message_start",
                json!({"type": "message_start", "message": {
                "id": "m1", "model": "claude-test", "role": "assistant",
                "usage": {"input_tokens": 1, "output_tokens": 1}}}),
            ),
            (
                "error",
                json!({"type": "error",
                "error": {"type": "overloaded_error", "message": "Overloaded"}}),
            ),
        ]);
        let (base, _) = start_sse_mock(body, false).await;
        let err = handle_llm_call(test_ctx(anthropic_stream_params(&base)).await)
            .await
            .expect_err("error event must fail the call");
        match err {
            StepError::Retryable { message, .. } => {
                assert!(message.contains("overloaded_error"), "{message}");
            }
            other @ StepError::Permanent { .. } => panic!("expected Retryable, got {other}"),
        }
    }

    #[tokio::test]
    async fn anthropic_stream_dropped_before_message_stop_is_retryable() {
        let body = anthropic_sse(&[
            (
                "message_start",
                json!({"type": "message_start", "message": {
                "id": "m1", "model": "claude-test", "role": "assistant",
                "usage": {"input_tokens": 1, "output_tokens": 1}}}),
            ),
            (
                "content_block_start",
                json!({"type": "content_block_start", "index": 0,
                "content_block": {"type": "text", "text": ""}}),
            ),
        ]);
        let (base, _) = start_sse_mock(body, false).await;
        let err = handle_llm_call(test_ctx(anthropic_stream_params(&base)).await)
            .await
            .expect_err("incomplete stream must fail");
        match err {
            StepError::Retryable { message, .. } => {
                assert!(message.contains("message_stop"), "{message}");
            }
            other @ StepError::Permanent { .. } => panic!("expected Retryable, got {other}"),
        }
    }

    #[tokio::test]
    async fn stream_with_response_schema_falls_back_to_non_streaming() {
        // The mock serves a plain (non-SSE) JSON response — proving the call
        // went out as a regular completion despite `stream: true`.
        let (base, requests) =
            start_openai_mock(vec![openai_resp(r#"{"name": "ok"}"#, 10, 5)]).await;
        let mut params = stream_params(&base);
        params["response_schema"] = name_schema();
        let out = handle_llm_call(test_ctx(params).await).await.unwrap();
        assert_eq!(out["schema_valid"], true);
        assert_eq!(out["name"], "ok");

        let reqs = requests.lock().await;
        assert_eq!(reqs.len(), 1);
        assert!(
            reqs[0].get("stream").is_none(),
            "request body must not ask the provider to stream"
        );
    }

    #[tokio::test]
    async fn streaming_publishes_llm_deltas_to_bus() {
        use crate::stream_bus::{stream_bus, StreamEvent};

        let (base, _) = start_sse_mock(openai_hello_stream(), false).await;
        let ctx = test_ctx(stream_params(&base)).await;
        let instance_id = ctx.instance_id;
        let mut rx = stream_bus().subscribe(instance_id);

        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["message"]["content"], "Hello world");

        let mut deltas = Vec::new();
        while let Ok(event) = rx.try_recv() {
            let StreamEvent::LlmDelta { block_id, delta } = event;
            assert_eq!(block_id, "b");
            deltas.push(delta);
        }
        assert_eq!(deltas, vec!["Hello ".to_string(), "world".to_string()]);
    }

    #[tokio::test]
    async fn failover_streams_from_second_provider_after_first_fails() {
        // Provider A's key env var is unset → skipped; provider B streams.
        let (base_b, requests_b) = start_sse_mock(openai_hello_stream(), false).await;
        let ctx = test_ctx(json!({
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true,
            "providers": [
                {"provider": "openai", "api_key_env": "LLM_TEST_UNSET_STREAM_KEY", "model": "m", "base_url": base_b},
                {"provider": "openai", "api_key": "k", "model": "gpt-test", "base_url": base_b},
            ],
        }))
        .await;
        let out = handle_llm_call(ctx).await.unwrap();
        assert_eq!(out["tried"], json!(["openai", "openai"]));
        assert_eq!(out["message"]["content"], "Hello world");
        assert_eq!(out["usage"]["completion_tokens"], 5);
        assert_eq!(requests_b.lock().await.len(), 1);
    }
}
