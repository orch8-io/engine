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

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("shutdown in progress")]
    ShuttingDown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::ids::{BlockId, InstanceId};

    #[test]
    fn storage_error_wraps_and_displays() {
        let src = StorageError::Connection("db unreachable".into());
        let err: EngineError = src.into();
        let msg = err.to_string();
        assert!(msg.starts_with("storage error:"), "got: {msg}");
        assert!(msg.contains("db unreachable"), "got: {msg}");
        assert!(matches!(err, EngineError::Storage(_)));
    }

    #[test]
    fn storage_error_from_not_found_preserves_entity() {
        let src = StorageError::NotFound {
            entity: "instance",
            id: "xyz".into(),
        };
        let err: EngineError = src.into();
        let msg = err.to_string();
        assert!(msg.contains("instance"));
        assert!(msg.contains("xyz"));
    }

    #[test]
    fn invalid_transition_display_includes_both_states_and_id() {
        let id = InstanceId::new();
        let err = EngineError::InvalidTransition {
            instance_id: id,
            from: InstanceState::Completed,
            to: InstanceState::Running,
        };
        let msg = err.to_string();
        assert!(msg.contains("completed"), "got: {msg}");
        assert!(msg.contains("running"), "got: {msg}");
        assert!(msg.contains(&id.0.to_string()), "got: {msg}");
    }

    #[test]
    fn handler_not_found_display_names_handler() {
        let err = EngineError::HandlerNotFound("send_email".into());
        assert_eq!(err.to_string(), "handler not found: send_email");
    }

    #[test]
    fn step_failed_display_includes_message_and_ids() {
        let inst = InstanceId::new();
        let err = EngineError::StepFailed {
            instance_id: inst,
            block_id: BlockId("b1".into()),
            message: "boom".into(),
            retryable: true,
        };
        let msg = err.to_string();
        assert!(msg.contains("b1"));
        assert!(msg.contains(&inst.0.to_string()));
        assert!(msg.contains("boom"));
    }

    #[test]
    fn step_timeout_display_includes_block_and_duration() {
        let err = EngineError::StepTimeout {
            block_id: BlockId("slow".into()),
            timeout: Duration::from_secs(5),
        };
        let msg = err.to_string();
        assert!(msg.contains("slow"));
        assert!(msg.contains('5'));
    }

    #[test]
    fn max_iterations_and_template_and_shutdown_displays() {
        assert_eq!(
            EngineError::MaxIterationsExceeded {
                block_id: BlockId("lp".into()),
            }
            .to_string(),
            "max iterations exceeded in loop lp",
        );
        assert_eq!(
            EngineError::TemplateError("bad expr".into()).to_string(),
            "template resolution failed: bad expr",
        );
        assert_eq!(EngineError::ShuttingDown.to_string(), "shutdown in progress");
    }

    #[test]
    fn not_found_and_invalid_config_displays() {
        assert_eq!(
            EngineError::NotFound("sequence/42".into()).to_string(),
            "not found: sequence/42",
        );
        assert_eq!(
            EngineError::InvalidConfig("missing field".into()).to_string(),
            "invalid configuration: missing field",
        );
    }
}
