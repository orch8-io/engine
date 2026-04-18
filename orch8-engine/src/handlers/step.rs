use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn};
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
}

/// Execute a step without persisting the output — returns the `BlockOutput` for
/// the caller to save (typically combined with a state transition in one transaction).
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
            if existing.attempt == i16::try_from(exec.attempt).unwrap_or(i16::MAX) {
                info!(
                    instance_id = %exec.instance_id,
                    block_id = %exec.block_id,
                    "step already executed (memoized), returning cached output"
                );
                return Ok(existing);
            }
        }
    }

    let handler = handlers
        .get(&exec.handler_name)
        .ok_or_else(|| EngineError::HandlerNotFound(exec.handler_name.clone()))?;

    let instance_id = exec.instance_id;
    let block_id = exec.block_id.clone();
    let attempt = exec.attempt;
    let timeout = exec.timeout;

    let step_ctx = StepContext {
        instance_id,
        tenant_id: exec.tenant_id,
        block_id: exec.block_id,
        params: exec.params,
        context: exec.context,
        attempt,
        storage: Arc::clone(storage),
    };

    let result = if let Some(dur) = timeout {
        match tokio::time::timeout(dur, handler(step_ctx)).await {
            Ok(res) => res,
            Err(_) => {
                return Err(EngineError::StepTimeout {
                    block_id,
                    timeout: dur,
                });
            }
        }
    } else {
        handler(step_ctx).await
    };

    match result {
        Ok(output) => {
            let output_size = serde_json::to_vec(&output)
                .map_or(0, |v| i32::try_from(v.len()).unwrap_or(i32::MAX));

            let block_output = maybe_externalize(
                storage.as_ref(),
                instance_id,
                block_id,
                output,
                output_size,
                i16::try_from(attempt).unwrap_or(i16::MAX),
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
            if existing.attempt == i16::try_from(exec.attempt).unwrap_or(i16::MAX) {
                info!(
                    instance_id = %exec.instance_id,
                    block_id = %exec.block_id,
                    "step already executed (memoized), returning cached output"
                );
                return Ok(existing.output);
            }
        }
    }

    let handler = handlers
        .get(&exec.handler_name)
        .ok_or_else(|| EngineError::HandlerNotFound(exec.handler_name.clone()))?;

    // Save fields needed after move into step_ctx.
    let instance_id = exec.instance_id;
    let block_id = exec.block_id.clone();
    let attempt = exec.attempt;
    let timeout = exec.timeout;

    let step_ctx = StepContext {
        instance_id,
        tenant_id: exec.tenant_id,
        block_id: exec.block_id,
        params: exec.params,
        context: exec.context,
        attempt,
        storage: Arc::clone(storage),
    };

    // Execute with optional timeout.
    let result = if let Some(dur) = timeout {
        match tokio::time::timeout(dur, handler(step_ctx)).await {
            Ok(res) => res,
            Err(_) => {
                return Err(EngineError::StepTimeout {
                    block_id,
                    timeout: dur,
                });
            }
        }
    } else {
        handler(step_ctx).await
    };

    match result {
        Ok(output) => {
            let output_size = serde_json::to_vec(&output)
                .map_or(0, |v| i32::try_from(v.len()).unwrap_or(i32::MAX));

            let block_output = maybe_externalize(
                storage.as_ref(),
                instance_id,
                block_id,
                output,
                output_size,
                i16::try_from(attempt).unwrap_or(i16::MAX),
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
    output_size: i32,
    attempt: i16,
    threshold: u32,
) -> Result<BlockOutput, EngineError> {
    #[allow(clippy::cast_sign_loss)]
    let should_externalize = threshold > 0 && output_size > 0 && (output_size as u32) > threshold;

    if should_externalize {
        let ref_key = format!("{}:{}", instance_id, block_id.0);
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
        let max = Duration::from_mins(1);
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
}
