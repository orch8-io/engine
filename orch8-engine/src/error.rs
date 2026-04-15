use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use std::time::Duration;

/// Engine-level errors.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("invalid state transition: {from} -> {to} for instance {instance_id}")]
    InvalidTransition {
        instance_id: InstanceId,
        from: InstanceState,
        to: InstanceState,
    },

    #[error("handler not found: {0}")]
    HandlerNotFound(String),

    #[error("step failed: {block_id} on instance {instance_id}: {message}")]
    StepFailed {
        instance_id: InstanceId,
        block_id: BlockId,
        message: String,
        retryable: bool,
    },

    #[error("step timed out: {block_id} after {timeout:?}")]
    StepTimeout {
        block_id: BlockId,
        timeout: Duration,
    },

    #[error("max iterations exceeded in loop {block_id}")]
    MaxIterationsExceeded { block_id: BlockId },

    #[error("template resolution failed: {0}")]
    TemplateError(String),

    #[error("shutdown in progress")]
    ShuttingDown,
}
