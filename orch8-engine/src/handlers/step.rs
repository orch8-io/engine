use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn};
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::error::StepError;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::output::BlockOutput;

use crate::error::EngineError;
use crate::handlers::{HandlerRegistry, StepContext};

/// Parameters for step execution, bundled to avoid too many function arguments.
pub struct StepExecParams {
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub handler_name: String,
    pub params: serde_json::Value,
    pub context: orch8_types::context::ExecutionContext,
    pub attempt: u32,
    pub timeout: Option<Duration>,
}

/// Execute a step with memoization: check if output already exists (idempotency),
/// invoke the handler if not, persist the result.
pub async fn execute_step(
    storage: &dyn StorageBackend,
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
        block_id: exec.block_id,
        params: exec.params,
        context: exec.context,
        attempt,
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
            // Estimate output size from the serialized form without a separate
            // to_string() allocation — serde_json::to_vec is used by the DB layer anyway.
            let output_size = serde_json::to_vec(&output)
                .map(|v| i32::try_from(v.len()).unwrap_or(i32::MAX))
                .unwrap_or(0);

            let block_output = BlockOutput {
                id: Uuid::new_v4(),
                instance_id,
                block_id,
                output,
                output_ref: None,
                output_size,
                attempt: i16::try_from(attempt).unwrap_or(i16::MAX),
                created_at: Utc::now(),
            };

            storage.save_block_output(&block_output).await?;

            info!(
                instance_id = %instance_id,
                output_size = output_size,
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
    let exponent = i32::try_from(attempt).unwrap_or(i32::MAX);
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
}
