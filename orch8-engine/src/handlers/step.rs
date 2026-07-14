use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{Instrument, info, warn};
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::error::StepError;
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::output::BlockOutput;

use crate::error::EngineError;
use crate::handlers::{HandlerRegistry, StepContext};

/// Parameters for step execution, bundled to avoid too many function arguments.
pub struct StepExecParams {
    pub instance_id: InstanceId,
    pub tenant_id: TenantId,
    pub block_id: BlockId,
    pub handler_name: String,
    pub params: serde_json::Value,
    pub context: orch8_types::context::ExecutionContext,
    pub attempt: u32,
    pub timeout: Option<Duration>,
    /// If > 0, outputs exceeding this byte size are externalized.
    pub externalize_threshold: u32,
    /// Cloned from `StepDef::wait_for_input` so the `human_review` handler
    /// can surface the resolved choice list in its notification payload.
    pub wait_for_input: Option<orch8_types::sequence::HumanInputDef>,
    /// Resolved cache key from `StepDef::cache_key`. If set, step output is
    /// cached in instance KV state under this key. On subsequent executions,
    /// the cached value is returned without running the handler.
    pub cache_key: Option<String>,
    /// Optional JSON Schema to validate handler output against.
    pub output_schema: Option<serde_json::Value>,
}

/// Execute a step without persisting the output — returns the `BlockOutput` for
/// the caller to save (typically combined with a state transition in one transaction).
/// Count JSON-serialized bytes without allocating a throw-away buffer.
fn json_byte_size(value: &serde_json::Value) -> Result<usize, serde_json::Error> {
    use std::io::{self, Write};
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

fn validate_output_schema(
    output_schema: Option<&serde_json::Value>,
    output: &serde_json::Value,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<(), EngineError> {
    let Some(schema) = output_schema else {
        return Ok(());
    };
    let validator = match jsonschema::validator_for(schema) {
        Ok(v) => v,
        Err(e) => {
            return Err(EngineError::StepFailed {
                instance_id,
                block_id: block_id.clone(),
                message: format!("output_schema is not a valid JSON Schema: {e}"),
                retryable: false,
                details: None,
            });
        }
    };
    let errors: Vec<String> = validator
        .iter_errors(output)
        .map(|e| {
            let path = e.instance_path().to_string();
            if path.is_empty() {
                e.to_string()
            } else {
                format!("at {path}: {e}")
            }
        })
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(EngineError::StepFailed {
            instance_id,
            block_id: block_id.clone(),
            message: format!(
                "output failed output_schema validation: {}",
                errors.join("; ")
            ),
            retryable: false,
            details: None,
        })
    }
}

#[allow(clippy::too_many_lines)]
pub async fn execute_step_dry(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    exec: StepExecParams,
) -> Result<BlockOutput, EngineError> {
    // Memoization: if output already exists for this block+attempt, return it.
    if exec.attempt > 0
        && let Some(existing) = storage
            .get_block_output(exec.instance_id, &exec.block_id)
            .await?
        // The in-progress sentinel now carries the real attempt number, so it
        // could falsely match the memoization check below — but it is NOT a
        // real output. Skip it so a crashed-mid-step block re-executes rather
        // than returning the sentinel as a cached result.
        && existing.output_ref.as_deref()
            != Some(crate::handlers::param_resolve::IN_PROGRESS_SENTINEL)
    {
        // Ref#4: attempts past i16::MAX can no longer be represented in
        // the block_outputs.attempt column. Refuse to memoize rather than
        // clamping — a clamp would make every retry past 32 767 collide
        // against the same row and silently replay a stale output.
        let matches_memoized = u16::try_from(exec.attempt)
            .ok()
            .is_some_and(|a| existing.attempt == a);
        if matches_memoized {
            info!(
                instance_id = %exec.instance_id,
                block_id = %exec.block_id,
                "step already executed (memoized), returning cached output"
            );
            return Ok(existing);
        }
    }

    // Step output caching: if a cache_key is set and a cached value exists
    // in instance KV state, return it without running the handler.
    if let Some(ref cache_key) = exec.cache_key {
        let prefixed_key = format!("_cache:{cache_key}");
        if let Ok(Some(cached)) = storage
            .get_instance_kv(exec.instance_id, &prefixed_key)
            .await
        {
            info!(
                instance_id = %exec.instance_id,
                block_id = %exec.block_id,
                cache_key = %cache_key,
                "step output served from cache"
            );
            let output_size =
                u32::try_from(json_byte_size(&cached).unwrap_or(0)).unwrap_or(u32::MAX);
            return Ok(BlockOutput {
                id: Uuid::now_v7(),
                instance_id: exec.instance_id,
                block_id: exec.block_id,
                output: cached,
                output_ref: None,
                output_size,
                attempt: u16::try_from(exec.attempt).unwrap_or(u16::MAX),
                created_at: Utc::now(),
            });
        }
    }

    let handler = handlers.get(&exec.handler_name).ok_or_else(|| {
        let names = handlers.handler_names();
        let suggestion = orch8_types::suggest::did_you_mean(&exec.handler_name, &names);
        match suggestion {
            Some(s) => {
                EngineError::HandlerNotFound(format!("{} (did you mean: {s}?)", exec.handler_name))
            }
            None => EngineError::HandlerNotFound(exec.handler_name.clone()),
        }
    })?;

    let mut effect_guard = if exec.context.runtime.dry_run {
        None
    } else {
        crate::effect_guard::EffectGuard::begin(
            storage.as_ref(),
            &exec.tenant_id,
            exec.instance_id,
            &exec.block_id,
            &exec.handler_name,
            &exec.params,
            exec.attempt,
        )
        .await?
    };

    let instance_id = exec.instance_id;
    let block_id = exec.block_id.clone();
    let attempt = exec.attempt;
    let timeout = exec.timeout;
    let cache_key = exec.cache_key;
    let output_schema = exec.output_schema;

    // Span around the handler invocation, exported via OTLP when the server
    // is configured with an endpoint. Structured handler events (e.g. the
    // `gen_ai.client.inference` event from `llm_call`) ride inside it.
    // Cardinality stays sane: identity fields only — no params, no outputs.
    let step_span = tracing::info_span!(
        "orch8.step",
        instance_id = %instance_id,
        block_id = %block_id,
        handler = %exec.handler_name,
        tenant_id = %exec.tenant_id,
        attempt = attempt,
    );

    let step_ctx = StepContext {
        instance_id,
        tenant_id: exec.tenant_id,
        block_id: exec.block_id,
        params: exec.params,
        context: Arc::new(exec.context),
        attempt,
        storage: Arc::clone(storage),
        wait_for_input: exec.wait_for_input,
    };

    let handler_fut = handler(step_ctx).instrument(step_span);
    let result = if let Some(dur) = timeout {
        let Ok(result) = tokio::time::timeout(dur, handler_fut).await else {
            if let Some(guard) = effect_guard.take() {
                guard.mark_unknown().await?;
            }
            return Err(EngineError::StepTimeout {
                block_id,
                timeout: dur,
            });
        };
        result
    } else {
        handler_fut.await
    };

    match result {
        Ok(output) => {
            if let Some(guard) = effect_guard.take() {
                guard.commit(&output).await?;
            }
            validate_output_schema(output_schema.as_ref(), &output, instance_id, &block_id)?;

            // Save to cache if cache_key is set.
            if let Some(ref ck) = cache_key {
                let prefixed_key = format!("_cache:{ck}");
                if let Err(e) = storage
                    .set_instance_kv(instance_id, &prefixed_key, &output)
                    .await
                {
                    warn!(
                        instance_id = %instance_id,
                        cache_key = %ck,
                        error = %e,
                        "failed to save step output to cache"
                    );
                }
            }

            let output_size =
                u32::try_from(json_byte_size(&output).unwrap_or(0)).unwrap_or(u32::MAX);

            let block_output = maybe_externalize(
                storage.as_ref(),
                instance_id,
                block_id,
                output,
                output_size,
                u16::try_from(attempt).unwrap_or(u16::MAX),
                exec.externalize_threshold,
            )
            .await?;

            info!(
                instance_id = %instance_id,
                output_size = output_size,
                externalized = block_output.output_ref.is_some(),
                "step completed successfully"
            );

            Ok(block_output)
        }
        Err(step_err) => {
            if let Some(guard) = effect_guard.take() {
                guard.mark_unknown().await?;
            }
            warn!(
                instance_id = %instance_id,
                attempt = attempt,
                error = %step_err,
                "step execution failed"
            );
            Err(map_step_error(step_err, instance_id, &block_id))
        }
    }
}

/// Execute a step with memoization: check if output already exists (idempotency),
/// invoke the handler if not, persist the result.
#[allow(clippy::too_many_lines)]
pub async fn execute_step(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    exec: StepExecParams,
) -> Result<serde_json::Value, EngineError> {
    // Memoization: if output already exists for this block+attempt, return it.
    // Skip the lookup on attempt 0 — no prior output can exist for a fresh block.
    if exec.attempt > 0
        && let Some(existing) = storage
            .get_block_output(exec.instance_id, &exec.block_id)
            .await?
        // The in-progress sentinel now carries the real attempt number, so it
        // could falsely match the memoization check below — but it is NOT a
        // real output. Skip it so a crashed-mid-step block re-executes rather
        // than returning the sentinel as a cached result.
        && existing.output_ref.as_deref()
            != Some(crate::handlers::param_resolve::IN_PROGRESS_SENTINEL)
    {
        // Ref#4: attempts past i16::MAX can no longer be represented in
        // the block_outputs.attempt column. Refuse to memoize rather than
        // clamping — a clamp would make every retry past 32 767 collide
        // against the same row and silently replay a stale output.
        let matches_memoized = u16::try_from(exec.attempt)
            .ok()
            .is_some_and(|a| existing.attempt == a);
        if matches_memoized {
            info!(
                instance_id = %exec.instance_id,
                block_id = %exec.block_id,
                "step already executed (memoized), returning cached output"
            );
            return Ok(existing.output);
        }
    }

    // Step output caching for the tree-evaluator path.
    if let Some(ref cache_key) = exec.cache_key {
        let prefixed_key = format!("_cache:{cache_key}");
        if let Ok(Some(cached)) = storage
            .get_instance_kv(exec.instance_id, &prefixed_key)
            .await
        {
            info!(
                instance_id = %exec.instance_id,
                block_id = %exec.block_id,
                cache_key = %cache_key,
                "step output served from cache"
            );
            let output_size =
                u32::try_from(json_byte_size(&cached).unwrap_or(0)).unwrap_or(u32::MAX);
            let block_output = BlockOutput {
                id: Uuid::now_v7(),
                instance_id: exec.instance_id,
                block_id: exec.block_id,
                output: cached.clone(),
                output_ref: None,
                output_size,
                attempt: u16::try_from(exec.attempt).unwrap_or(u16::MAX),
                created_at: Utc::now(),
            };
            storage.save_block_output(&block_output).await?;
            return Ok(cached);
        }
    }

    let handler = handlers.get(&exec.handler_name).ok_or_else(|| {
        let names = handlers.handler_names();
        let suggestion = orch8_types::suggest::did_you_mean(&exec.handler_name, &names);
        match suggestion {
            Some(s) => {
                EngineError::HandlerNotFound(format!("{} (did you mean: {s}?)", exec.handler_name))
            }
            None => EngineError::HandlerNotFound(exec.handler_name.clone()),
        }
    })?;

    let mut effect_guard = if exec.context.runtime.dry_run {
        None
    } else {
        crate::effect_guard::EffectGuard::begin(
            storage.as_ref(),
            &exec.tenant_id,
            exec.instance_id,
            &exec.block_id,
            &exec.handler_name,
            &exec.params,
            exec.attempt,
        )
        .await?
    };

    // Save fields needed after move into step_ctx.
    let instance_id = exec.instance_id;
    let block_id = exec.block_id.clone();
    let attempt = exec.attempt;
    let timeout = exec.timeout;
    let cache_key = exec.cache_key;
    let output_schema = exec.output_schema;

    // Same `orch8.step` span as `execute_step_dry` — the tree-evaluator path
    // must export identically-shaped spans as the flat scheduler path.
    let step_span = tracing::info_span!(
        "orch8.step",
        instance_id = %instance_id,
        block_id = %block_id,
        handler = %exec.handler_name,
        tenant_id = %exec.tenant_id,
        attempt = attempt,
    );

    let step_ctx = StepContext {
        instance_id,
        tenant_id: exec.tenant_id,
        block_id: exec.block_id,
        params: exec.params,
        context: Arc::new(exec.context),
        attempt,
        storage: Arc::clone(storage),
        wait_for_input: exec.wait_for_input,
    };

    // Execute with optional timeout.
    let handler_fut = handler(step_ctx).instrument(step_span);
    let result = if let Some(dur) = timeout {
        let Ok(result) = tokio::time::timeout(dur, handler_fut).await else {
            if let Some(guard) = effect_guard.take() {
                guard.mark_unknown().await?;
            }
            return Err(EngineError::StepTimeout {
                block_id,
                timeout: dur,
            });
        };
        result
    } else {
        handler_fut.await
    };

    match result {
        Ok(output) => {
            if let Some(guard) = effect_guard.take() {
                guard.commit(&output).await?;
            }
            validate_output_schema(output_schema.as_ref(), &output, instance_id, &block_id)?;

            if let Some(ref ck) = cache_key {
                let prefixed_key = format!("_cache:{ck}");
                if let Err(e) = storage
                    .set_instance_kv(instance_id, &prefixed_key, &output)
                    .await
                {
                    warn!(
                        instance_id = %instance_id,
                        cache_key = %ck,
                        error = %e,
                        "failed to save step output to cache"
                    );
                }
            }

            let output_size =
                u32::try_from(json_byte_size(&output).unwrap_or(0)).unwrap_or(u32::MAX);

            let block_output = maybe_externalize(
                storage.as_ref(),
                instance_id,
                block_id,
                output,
                output_size,
                u16::try_from(attempt).unwrap_or(u16::MAX),
                exec.externalize_threshold,
            )
            .await?;

            storage.save_block_output(&block_output).await?;

            info!(
                instance_id = %instance_id,
                output_size = output_size,
                externalized = block_output.output_ref.is_some(),
                "step completed successfully"
            );

            Ok(block_output.output)
        }
        Err(step_err) => {
            if let Some(guard) = effect_guard.take() {
                guard.mark_unknown().await?;
            }
            warn!(
                instance_id = %instance_id,
                attempt = attempt,
                error = %step_err,
                "step execution failed"
            );
            Err(map_step_error(step_err, instance_id, &block_id))
        }
    }
}

/// If output exceeds the externalization threshold, store the payload in
/// `externalized_state` and replace the output with a reference marker.
async fn maybe_externalize(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    block_id: BlockId,
    output: serde_json::Value,
    output_size: u32,
    attempt: u16,
    threshold: u32,
) -> Result<BlockOutput, EngineError> {
    let should_externalize = threshold > 0 && output_size > threshold;

    if should_externalize {
        let ref_key = format!("{}:{}", instance_id, block_id.as_str());
        storage
            .save_externalized_state(instance_id, &ref_key, &output)
            .await?;
        Ok(BlockOutput {
            id: Uuid::now_v7(),
            instance_id,
            block_id,
            output: serde_json::json!({"_externalized": true, "_ref": ref_key}),
            output_ref: Some(ref_key),
            output_size,
            attempt,
            created_at: Utc::now(),
        })
    } else {
        Ok(BlockOutput {
            id: Uuid::now_v7(),
            instance_id,
            block_id,
            output,
            output_ref: None,
            output_size,
            attempt,
            created_at: Utc::now(),
        })
    }
}

fn map_step_error(err: StepError, instance_id: InstanceId, block_id: &BlockId) -> EngineError {
    match err {
        StepError::Retryable { message, details } => EngineError::StepFailed {
            instance_id,
            block_id: block_id.clone(),
            message,
            retryable: true,
            details,
        },
        StepError::Permanent { message, details } => EngineError::StepFailed {
            instance_id,
            block_id: block_id.clone(),
            message,
            retryable: false,
            details,
        },
    }
}

/// Evaluate the conditional retry policy (`retry_if` / `non_retryable_codes`)
/// against the current error. Returns `true` if the retry should proceed,
/// `false` if the error should be treated as permanent.
pub fn should_retry(
    retry: &orch8_types::sequence::RetryPolicy,
    error_message: &str,
    error_details: Option<&serde_json::Value>,
    context: &orch8_types::context::ExecutionContext,
    outputs: &serde_json::Value,
) -> bool {
    if let Some(codes) = &retry.non_retryable_codes
        && let Some(details) = error_details
    {
        // Handlers vary in what they call the discriminator: HTTP-based
        // handlers (`http_request`, ActivePieces sidecar) set a numeric
        // `status`, while others may set a string `error_code`/`code`.
        // Check all three so `non_retryable_codes: ["404", "429"]`
        // matches an HTTP handler's `details.status` without requiring
        // every handler to also duplicate it under `error_code`.
        let candidates = [
            details.get("error_code"),
            details.get("code"),
            details.get("status"),
        ];
        let matched = candidates.into_iter().flatten().any(|v| {
            if let Some(s) = v.as_str() {
                codes.iter().any(|c| c == s)
            } else {
                let as_str = v.to_string();
                codes.iter().any(|c| c == &as_str)
            }
        });
        if matched {
            return false;
        }
    }

    if let Some(expr) = &retry.retry_if {
        let mut eval_ctx = context.clone();
        let error_obj = match error_details {
            Some(d) => serde_json::json!({
                "message": error_message,
                "details": d,
            }),
            None => serde_json::json!({
                "message": error_message,
            }),
        };
        // `context.data` defaults to `Value::Null` and is not guaranteed to be
        // an object (a caller can supply any JSON value as instance context),
        // so coerce non-object data into an empty object before merging in
        // `_error` rather than risking a panic on `.as_object_mut()`.
        if !eval_ctx.data.is_object() {
            eval_ctx.data = serde_json::json!({});
        }
        eval_ctx
            .data
            .as_object_mut()
            .expect("just coerced to an object above")
            .insert("_error".into(), error_obj);
        return crate::expression::evaluate_condition(expr, &eval_ctx, outputs);
    }

    true
}

/// Calculate the backoff duration for a given retry attempt.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn calculate_backoff(
    attempt: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    multiplier: f64,
) -> Duration {
    if attempt == 0 {
        return initial_backoff;
    }
    let initial_ms = initial_backoff.as_millis() as f64;
    let max_ms = max_backoff.as_millis() as f64;
    #[allow(clippy::cast_possible_wrap)]
    let exponent = attempt.min(63) as i32;
    let backoff_ms = (initial_ms * multiplier.powi(exponent)).min(max_ms);
    Duration::from_millis(backoff_ms.max(0.0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_exponential() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(60);
        let multiplier = 2.0;

        assert_eq!(
            calculate_backoff(0, initial, max, multiplier),
            Duration::from_secs(1)
        );
        assert_eq!(
            calculate_backoff(1, initial, max, multiplier),
            Duration::from_secs(2)
        );
        assert_eq!(
            calculate_backoff(2, initial, max, multiplier),
            Duration::from_secs(4)
        );
        assert_eq!(
            calculate_backoff(3, initial, max, multiplier),
            Duration::from_secs(8)
        );
    }

    #[test]
    fn validate_output_schema_none_always_passes() {
        let output = serde_json::json!({"any": "thing"});
        let result = validate_output_schema(None, &output, InstanceId::new(), &BlockId::new("s1"));
        assert!(result.is_ok());
    }

    #[test]
    fn validate_output_schema_conforming_output_passes() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"score": {"type": "number"}},
            "required": ["score"]
        });
        let output = serde_json::json!({"score": 42});
        let result = validate_output_schema(
            Some(&schema),
            &output,
            InstanceId::new(),
            &BlockId::new("s1"),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn validate_output_schema_non_conforming_output_returns_permanent_error() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"score": {"type": "number"}},
            "required": ["score"]
        });
        let output = serde_json::json!({"score": "not a number"});
        let result = validate_output_schema(
            Some(&schema),
            &output,
            InstanceId::new(),
            &BlockId::new("s1"),
        );
        let err = result.unwrap_err();
        match err {
            EngineError::StepFailed {
                retryable, message, ..
            } => {
                assert!(!retryable, "schema violation must be permanent");
                assert!(message.contains("output_schema validation"), "{message}");
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }

    #[test]
    fn validate_output_schema_missing_required_field() {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["name", "age"]
        });
        let output = serde_json::json!({"name": "Alice"});
        let result = validate_output_schema(
            Some(&schema),
            &output,
            InstanceId::new(),
            &BlockId::new("s1"),
        );
        let err = result.unwrap_err();
        match err {
            EngineError::StepFailed { message, .. } => {
                assert!(
                    message.contains("age") || message.contains("required"),
                    "{message}"
                );
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }

    #[test]
    fn validate_output_schema_malformed_schema_returns_error() {
        let schema = serde_json::json!({"type": "not_a_real_type"});
        let output = serde_json::json!({"x": 1});
        let result = validate_output_schema(
            Some(&schema),
            &output,
            InstanceId::new(),
            &BlockId::new("s1"),
        );
        // jsonschema may either reject the schema at compile time (StepFailed with "not a valid JSON Schema")
        // or validate successfully depending on the implementation. Either way should not panic.
        // With "not_a_real_type", jsonschema::validator_for will fail.
        let err = result.unwrap_err();
        match err {
            EngineError::StepFailed {
                retryable, message, ..
            } => {
                assert!(!retryable);
                assert!(message.contains("not a valid JSON Schema"), "{message}");
            }
            other => panic!("expected StepFailed, got {other:?}"),
        }
    }

    #[test]
    fn validate_output_schema_additional_properties_allowed_by_default() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"a": {"type": "number"}}
        });
        let output = serde_json::json!({"a": 1, "b": "extra"});
        let result = validate_output_schema(
            Some(&schema),
            &output,
            InstanceId::new(),
            &BlockId::new("s1"),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn validate_output_schema_strict_additional_properties_rejects_extras() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"a": {"type": "number"}},
            "additionalProperties": false
        });
        let output = serde_json::json!({"a": 1, "b": "extra"});
        let result = validate_output_schema(
            Some(&schema),
            &output,
            InstanceId::new(),
            &BlockId::new("s1"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn backoff_caps_at_max() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(10);
        let multiplier = 2.0;

        assert_eq!(
            calculate_backoff(10, initial, max, multiplier),
            Duration::from_secs(10)
        );
    }

    /// Asserts the `orch8.step` span (exported via OTLP when the server has an
    /// endpoint configured) wraps handler invocation and carries the expected
    /// identity fields. Span emission is asserted via a tracing test layer
    /// rather than `opentelemetry_sdk`'s in-memory exporter — wiring the OpenTelemetry
    /// bridge into orch8-engine's dev-deps just for this would drag the whole
    /// opentelemetry stack into the engine's test build for no extra signal:
    /// the tracing span IS the unit the OTLP layer exports.
    #[tokio::test]
    async fn execute_step_dry_emits_orch8_step_span_around_handler() {
        use std::collections::HashMap;
        use std::sync::Mutex;

        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Default)]
        struct SpanCapture {
            spans: Arc<Mutex<Vec<HashMap<String, String>>>>,
        }

        struct FieldVisitor(HashMap<String, String>);
        impl tracing::field::Visit for FieldVisitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().to_string(), format!("{value:?}"));
            }
        }

        impl<S> tracing_subscriber::Layer<S> for SpanCapture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _id: &tracing::span::Id,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if attrs.metadata().name() == "orch8.step" {
                    let mut visitor = FieldVisitor(HashMap::new());
                    attrs.record(&mut visitor);
                    self.spans.lock().unwrap().push(visitor.0);
                }
            }
        }

        let capture = SpanCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());

        let storage: Arc<dyn orch8_storage::StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let mut handlers = HandlerRegistry::new();
        handlers.register("mock_step", |_ctx| async {
            Ok(serde_json::json!({"ok": true}))
        });

        let instance_id = InstanceId::new();
        let exec = StepExecParams {
            instance_id,
            tenant_id: TenantId::unchecked("tenant-a"),
            block_id: BlockId::new("step-1"),
            handler_name: "mock_step".into(),
            params: serde_json::json!({}),
            context: orch8_types::context::ExecutionContext::default(),
            attempt: 0,
            timeout: None,
            externalize_threshold: 0,
            wait_for_input: None,
            cache_key: None,
            output_schema: None,
        };

        let output = execute_step_dry(&storage, &handlers, exec)
            .with_subscriber(subscriber)
            .await
            .unwrap();
        assert_eq!(output.output["ok"], true);

        let spans = capture.spans.lock().unwrap();
        assert_eq!(spans.len(), 1, "expected exactly one orch8.step span");
        let fields = &spans[0];
        assert_eq!(fields.get("handler").map(String::as_str), Some("mock_step"));
        assert_eq!(
            fields.get("tenant_id").map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(fields.get("block_id").map(String::as_str), Some("step-1"));
        assert_eq!(fields.get("attempt").map(String::as_str), Some("0"));
        assert_eq!(
            fields.get("instance_id").cloned(),
            Some(instance_id.to_string())
        );
    }

    // --- should_retry (conditional retry policies) ---

    fn base_retry() -> orch8_types::sequence::RetryPolicy {
        orch8_types::sequence::RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 2.0,
            retry_if: None,
            non_retryable_codes: None,
        }
    }

    fn empty_context() -> orch8_types::context::ExecutionContext {
        orch8_types::context::ExecutionContext::default()
    }

    #[test]
    fn should_retry_true_when_no_conditions_configured() {
        let retry = base_retry();
        assert!(should_retry(
            &retry,
            "boom",
            None,
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_true_when_retry_if_evaluates_truthy() {
        let mut retry = base_retry();
        retry.retry_if = Some("contains(_error.message, \"timeout\")".into());
        assert!(should_retry(
            &retry,
            "connection timeout",
            None,
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_false_when_retry_if_evaluates_falsy() {
        let mut retry = base_retry();
        retry.retry_if = Some("contains(_error.message, \"timeout\")".into());
        assert!(!should_retry(
            &retry,
            "permission denied",
            None,
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_retry_if_sees_error_details() {
        let mut retry = base_retry();
        retry.retry_if = Some("_error.details.status == 503".into());
        let details = serde_json::json!({"status": 503});
        assert!(should_retry(
            &retry,
            "service unavailable",
            Some(&details),
            &empty_context(),
            &serde_json::Value::Null,
        ));

        let details_ok = serde_json::json!({"status": 400});
        assert!(!should_retry(
            &retry,
            "bad request",
            Some(&details_ok),
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_retry_if_sees_instance_data() {
        let mut retry = base_retry();
        retry.retry_if = Some("data.allow_retry == true".into());
        let mut ctx = empty_context();
        ctx.data = serde_json::json!({"allow_retry": true});
        assert!(should_retry(
            &retry,
            "boom",
            None,
            &ctx,
            &serde_json::Value::Null,
        ));

        ctx.data = serde_json::json!({"allow_retry": false});
        assert!(!should_retry(
            &retry,
            "boom",
            None,
            &ctx,
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_non_object_context_data_does_not_panic() {
        let mut retry = base_retry();
        retry.retry_if = Some("contains(_error.message, \"x\")".into());
        let mut ctx = empty_context();
        ctx.data = serde_json::Value::String("not an object".into());
        // Must not panic even though context.data isn't an object.
        let _ = should_retry(&retry, "xyz", None, &ctx, &serde_json::Value::Null);
    }

    #[test]
    fn should_retry_false_when_error_code_in_non_retryable_list() {
        let mut retry = base_retry();
        retry.non_retryable_codes = Some(vec!["INVALID_INPUT".into(), "AUTH_FAILED".into()]);
        let details = serde_json::json!({"error_code": "AUTH_FAILED"});
        assert!(!should_retry(
            &retry,
            "unauthorized",
            Some(&details),
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_true_when_error_code_not_in_non_retryable_list() {
        let mut retry = base_retry();
        retry.non_retryable_codes = Some(vec!["INVALID_INPUT".into()]);
        let details = serde_json::json!({"error_code": "RATE_LIMITED"});
        assert!(should_retry(
            &retry,
            "too many requests",
            Some(&details),
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_non_retryable_codes_checks_code_alias() {
        let mut retry = base_retry();
        retry.non_retryable_codes = Some(vec!["FATAL".into()]);
        let details = serde_json::json!({"code": "FATAL"});
        assert!(!should_retry(
            &retry,
            "unrecoverable",
            Some(&details),
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_non_retryable_codes_with_no_details_falls_through() {
        let mut retry = base_retry();
        retry.non_retryable_codes = Some(vec!["FATAL".into()]);
        // No details supplied — the denylist can't match, so retry proceeds.
        assert!(should_retry(
            &retry,
            "unrecoverable",
            None,
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }

    #[test]
    fn should_retry_non_retryable_codes_wins_over_retry_if() {
        let mut retry = base_retry();
        retry.non_retryable_codes = Some(vec!["FATAL".into()]);
        // retry_if alone would say "retry", but the denylist takes priority.
        retry.retry_if = Some("true".into());
        let details = serde_json::json!({"error_code": "FATAL"});
        assert!(!should_retry(
            &retry,
            "unrecoverable",
            Some(&details),
            &empty_context(),
            &serde_json::Value::Null,
        ));
    }
}
