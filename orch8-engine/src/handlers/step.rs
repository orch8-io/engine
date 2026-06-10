use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn, Instrument};
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

#[allow(clippy::too_many_lines)]
pub async fn execute_step_dry(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    exec: StepExecParams,
) -> Result<BlockOutput, EngineError> {
    // Memoization: if output already exists for this block+attempt, return it.
    if exec.attempt > 0 {
        if let Some(existing) = storage
            .get_block_output(exec.instance_id, &exec.block_id)
            .await?
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

    let instance_id = exec.instance_id;
    let block_id = exec.block_id.clone();
    let attempt = exec.attempt;
    let timeout = exec.timeout;
    let cache_key = exec.cache_key;

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
        context: exec.context,
        attempt,
        storage: Arc::clone(storage),
        wait_for_input: exec.wait_for_input,
    };

    let handler_fut = handler(step_ctx).instrument(step_span);
    let result = if let Some(dur) = timeout {
        match tokio::time::timeout(dur, handler_fut).await {
            Ok(res) => res,
            Err(_) => {
                return Err(EngineError::StepTimeout {
                    block_id,
                    timeout: dur,
                });
            }
        }
    } else {
        handler_fut.await
    };

    match result {
        Ok(output) => {
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
    if exec.attempt > 0 {
        if let Some(existing) = storage
            .get_block_output(exec.instance_id, &exec.block_id)
            .await?
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

    // Save fields needed after move into step_ctx.
    let instance_id = exec.instance_id;
    let block_id = exec.block_id.clone();
    let attempt = exec.attempt;
    let timeout = exec.timeout;
    let cache_key = exec.cache_key;

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
        context: exec.context,
        attempt,
        storage: Arc::clone(storage),
        wait_for_input: exec.wait_for_input,
    };

    // Execute with optional timeout.
    let handler_fut = handler(step_ctx).instrument(step_span);
    let result = if let Some(dur) = timeout {
        match tokio::time::timeout(dur, handler_fut).await {
            Ok(res) => res,
            Err(_) => {
                return Err(EngineError::StepTimeout {
                    block_id,
                    timeout: dur,
                });
            }
        }
    } else {
        handler_fut.await
    };

    match result {
        Ok(output) => {
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
        StepError::Retryable { message, .. } => EngineError::StepFailed {
            instance_id,
            block_id: block_id.clone(),
            message,
            retryable: true,
        },
        StepError::Permanent { message, .. } => EngineError::StepFailed {
            instance_id,
            block_id: block_id.clone(),
            message,
            retryable: false,
        },
    }
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
}
