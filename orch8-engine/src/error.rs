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
        details: Option<serde_json::Value>,
    },

    #[error("step timed out: {block_id} after {timeout:?}")]
    StepTimeout {
        block_id: BlockId,
        timeout: Duration,
    },

    #[error(
        "effect {effect_id} for block {block_id} is {state:?}; automatic redispatch is blocked"
    )]
    EffectBlocked {
        effect_id: orch8_types::continuity::EffectId,
        block_id: BlockId,
        state: orch8_types::continuity::EffectState,
    },

    #[error("effect dispatch for block {block_id} violates invariant {invariant_id}")]
    InvariantViolation {
        invariant_id: orch8_types::continuity_advanced::InvariantId,
        block_id: BlockId,
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

impl EngineError {
    /// `true` when this error is a *transient* storage failure — connection
    /// loss, connection-pool exhaustion, or a transient external-backend blip
    /// (e.g. a database failover or object-store throttle).
    ///
    /// The scheduler's per-instance safety net uses this to distinguish an
    /// infrastructure hiccup from a genuine processing failure: a transient
    /// storage error must **reschedule** the instance (it stays healthy and
    /// retries on a later tick) rather than transition it to terminal
    /// `Failed`, which would DLQ an entire claimed batch on a momentary
    /// database blip.
    ///
    /// Delegates to [`StorageError::is_transient`] — the single source of
    /// truth shared with the step-level mapper — so both classifiers always
    /// agree: logic errors (`Query`, `Serialization`, `Conflict`,
    /// `Unsupported`, …) are NOT transient; retrying them loops forever.
    #[must_use]
    pub fn is_transient_storage(&self) -> bool {
        matches!(self, Self::Storage(e) if e.is_transient())
    }

    /// Normalize `StepTimeout` into a retryable `StepFailed`, leaving every
    /// other variant untouched.
    ///
    /// A step with `timeout` + `retry` configured used to fail the instance
    /// permanently on its first timeout: both dispatch paths (the fast-path
    /// `scheduler::step_exec` and the tree-evaluator's `handlers::step_block`)
    /// let `StepTimeout` fall through to their generic (non-retryable) error
    /// arm, bypassing both the retry policy and any surrounding `TryCatch`.
    /// Calling this right after dispatch means a timeout flows through the
    /// exact same retryable-failure handling as `StepFailed { retryable: true, .. }` —
    /// the retry-policy check still decides whether attempts remain, so this
    /// makes a timeout *eligible* for retry rather than forcing one.
    #[must_use]
    pub fn normalize_timeout_as_retryable(self, instance_id: InstanceId) -> Self {
        match self {
            Self::StepTimeout { block_id, timeout } => Self::StepFailed {
                instance_id,
                message: format!("step timed out after {timeout:?}"),
                block_id,
                retryable: true,
                details: None,
            },
            other => other,
        }
    }
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

    /// The scheduler safety net (`is_transient_storage`) and the step-level
    /// mapper (`map_storage_err`) used to classify `Query` in opposite
    /// directions — permanent in one place, retryable in the other. Both now
    /// delegate to `StorageError::is_transient`; pin that they agree on every
    /// variant. Keep the table in sync when adding variants.
    #[test]
    fn storage_classifiers_agree_on_every_variant() {
        use crate::handlers::util::map_storage_err;
        use orch8_types::error::StepError;

        let serde_err = serde_json::from_str::<serde_json::Value>("{bad").unwrap_err();
        let variants: Vec<(&str, StorageError)> = vec![
            ("Connection", StorageError::Connection("x".into())),
            ("Query", StorageError::Query("x".into())),
            (
                "NotFound",
                StorageError::NotFound {
                    entity: "instance",
                    id: "x".into(),
                },
            ),
            ("Conflict", StorageError::Conflict("x".into())),
            (
                "TerminalTarget",
                StorageError::TerminalTarget {
                    entity: "instance".into(),
                    id: "x".into(),
                },
            ),
            ("Migration", StorageError::Migration("x".into())),
            ("Serialization", StorageError::Serialization(serde_err)),
            ("PoolExhausted", StorageError::PoolExhausted),
            ("Unsupported", StorageError::Unsupported("x".into())),
            ("Backend", StorageError::Backend("x".into())),
        ];
        assert_eq!(
            variants.len(),
            10,
            "table must cover every StorageError variant"
        );
        for (name, err) in variants {
            let step_retryable = matches!(map_storage_err(&err), StepError::Retryable { .. });
            let source_of_truth = err.is_transient();
            let scheduler_transient = EngineError::Storage(err).is_transient_storage();
            assert_eq!(
                scheduler_transient, source_of_truth,
                "{name}: scheduler classifier disagrees"
            );
            assert_eq!(
                step_retryable, source_of_truth,
                "{name}: step-level mapper disagrees"
            );
        }
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
        assert!(msg.contains(&id.to_string()), "got: {msg}");
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
            block_id: BlockId::new("b1"),
            message: "boom".into(),
            retryable: true,
            details: None,
        };
        let msg = err.to_string();
        assert!(msg.contains("b1"));
        assert!(msg.contains(&inst.into_uuid().to_string()));
        assert!(msg.contains("boom"));
    }

    #[test]
    fn step_timeout_display_includes_block_and_duration() {
        let err = EngineError::StepTimeout {
            block_id: BlockId::new("slow"),
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
                block_id: BlockId::new("lp"),
            }
            .to_string(),
            "max iterations exceeded in loop lp",
        );
        assert_eq!(
            EngineError::TemplateError("bad expr".into()).to_string(),
            "template resolution failed: bad expr",
        );
        assert_eq!(
            EngineError::ShuttingDown.to_string(),
            "shutdown in progress"
        );
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

    /// H-7: `StepTimeout` used to bypass retry policy and `TryCatch` entirely
    /// (it fell through to the generic, non-retryable error arm at both
    /// dispatch sites). It must normalize into a retryable `StepFailed` so
    /// the existing retry-policy machinery gets a chance to decide.
    #[test]
    fn normalize_timeout_as_retryable_converts_step_timeout() {
        let instance_id = InstanceId::new();
        let err = EngineError::StepTimeout {
            block_id: BlockId::new("slow"),
            timeout: Duration::from_secs(5),
        };
        let normalized = err.normalize_timeout_as_retryable(instance_id);
        match normalized {
            EngineError::StepFailed {
                instance_id: got_id,
                block_id,
                retryable,
                message,
                ..
            } => {
                assert_eq!(got_id, instance_id);
                assert_eq!(block_id.as_str(), "slow");
                assert!(retryable, "a timeout must be retry-eligible");
                assert!(message.contains("timed out"));
            }
            other => panic!("expected StepFailed, got: {other:?}"),
        }
    }

    /// Every other variant must pass through `normalize_timeout_as_retryable`
    /// unchanged.
    #[test]
    fn normalize_timeout_as_retryable_leaves_other_variants_untouched() {
        let instance_id = InstanceId::new();
        let err = EngineError::StepFailed {
            instance_id,
            block_id: BlockId::new("b"),
            message: "permanent".into(),
            retryable: false,
            details: None,
        };
        let normalized = err.normalize_timeout_as_retryable(instance_id);
        assert!(matches!(
            normalized,
            EngineError::StepFailed {
                retryable: false,
                ..
            }
        ));
    }
}
