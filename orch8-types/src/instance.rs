use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::context::ExecutionContext;
use crate::ids::{InstanceId, Namespace, SequenceId, TenantId};

/// Instance states form a strict state machine.
/// Transitions are validated at runtime via `can_transition_to`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[sqlx(type_name = "instance_state", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InstanceState {
    Scheduled,
    Running,
    Waiting,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

/// All valid instance state names, used for fuzzy matching in error messages.
pub const INSTANCE_STATE_NAMES: &[&str] = &[
    "scheduled",
    "running",
    "waiting",
    "paused",
    "completed",
    "failed",
    "cancelled",
];

impl FromStr for InstanceState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scheduled" => Ok(Self::Scheduled),
            "running" => Ok(Self::Running),
            "waiting" => Ok(Self::Waiting),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => {
                let suggestion = crate::suggest::did_you_mean(other, INSTANCE_STATE_NAMES);
                match suggestion {
                    Some(s) => Err(format!(
                        "unknown instance state: {other} (did you mean: {s}?)"
                    )),
                    None => Err(format!("unknown instance state: {other}")),
                }
            }
        }
    }
}

impl InstanceState {
    /// Returns true if the transition is legal.
    #[must_use]
    pub const fn can_transition_to(self, target: Self) -> bool {
        matches!(
            (self, target),
            (
                Self::Scheduled,
                Self::Running | Self::Paused | Self::Cancelled
            ) | (
                Self::Running,
                Self::Scheduled
                    | Self::Waiting
                    | Self::Completed
                    | Self::Failed
                    | Self::Paused
                    | Self::Cancelled
            ) | (
                Self::Waiting,
                Self::Running | Self::Scheduled | Self::Cancelled | Self::Failed
            ) | (Self::Paused, Self::Scheduled | Self::Cancelled)
                | (Self::Failed, Self::Scheduled) // retry from DLQ
        )
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl std::fmt::Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Scheduled => "scheduled",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        };
        f.write_str(s)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
    ToSchema,
)]
#[repr(i16)]
#[non_exhaustive]
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl TryFrom<i16> for Priority {
    type Error = String;

    fn try_from(v: i16) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Low),
            1 => Ok(Self::Normal),
            2 => Ok(Self::High),
            3 => Ok(Self::Critical),
            other => Err(format!("unknown priority value: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TaskInstance {
    pub id: InstanceId,
    pub sequence_id: SequenceId,
    pub tenant_id: TenantId,
    pub namespace: Namespace,
    pub state: InstanceState,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub priority: Priority,
    pub timezone: String,
    pub metadata: serde_json::Value,
    pub context: ExecutionContext,
    /// Optional concurrency key for limiting parallel executions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_key: Option<String>,
    /// Max concurrent running instances with the same concurrency key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<i32>,
    /// Optional idempotency key for deduplication at creation time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Session ID for cross-instance shared state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<uuid::Uuid>,
    /// Parent instance ID (set when this instance is a sub-sequence child).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_instance_id: Option<InstanceId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_transitions() {
        assert!(InstanceState::Scheduled.can_transition_to(InstanceState::Running));
        assert!(InstanceState::Running.can_transition_to(InstanceState::Completed));
        assert!(InstanceState::Running.can_transition_to(InstanceState::Failed));
        assert!(InstanceState::Running.can_transition_to(InstanceState::Scheduled));
        assert!(InstanceState::Failed.can_transition_to(InstanceState::Scheduled));
    }

    #[test]
    fn invalid_transitions() {
        assert!(!InstanceState::Completed.can_transition_to(InstanceState::Running));
        assert!(!InstanceState::Cancelled.can_transition_to(InstanceState::Scheduled));
        assert!(!InstanceState::Failed.can_transition_to(InstanceState::Completed));
    }

    #[test]
    fn terminal_states() {
        assert!(InstanceState::Completed.is_terminal());
        assert!(InstanceState::Failed.is_terminal());
        assert!(InstanceState::Cancelled.is_terminal());
        assert!(!InstanceState::Running.is_terminal());
        assert!(!InstanceState::Scheduled.is_terminal());
    }

    // --- TEST_PLAN items 63-74 — state machine coverage per transition. ---

    #[test]
    fn scheduled_to_running_is_valid() {
        assert!(InstanceState::Scheduled.can_transition_to(InstanceState::Running));
    }

    #[test]
    fn scheduled_to_completed_is_invalid() {
        assert!(!InstanceState::Scheduled.can_transition_to(InstanceState::Completed));
    }

    #[test]
    fn running_to_completed_is_valid() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Completed));
    }

    #[test]
    fn running_to_failed_is_valid() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Failed));
    }

    #[test]
    fn running_to_paused_is_valid() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Paused));
    }

    #[test]
    fn running_to_cancelled_is_valid() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Cancelled));
    }

    #[test]
    fn paused_to_scheduled_is_valid_resume() {
        assert!(InstanceState::Paused.can_transition_to(InstanceState::Scheduled));
    }

    #[test]
    fn paused_to_completed_is_invalid() {
        assert!(!InstanceState::Paused.can_transition_to(InstanceState::Completed));
    }

    #[test]
    fn waiting_to_running_is_valid() {
        assert!(InstanceState::Waiting.can_transition_to(InstanceState::Running));
    }

    #[test]
    fn completed_is_terminal_rejects_all_targets() {
        for target in [
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
            InstanceState::Failed,
            InstanceState::Cancelled,
            InstanceState::Completed,
        ] {
            assert!(
                !InstanceState::Completed.can_transition_to(target),
                "Completed -> {target:?} must be invalid"
            );
        }
    }

    #[test]
    fn failed_to_scheduled_is_valid_retry() {
        assert!(InstanceState::Failed.can_transition_to(InstanceState::Scheduled));
    }

    #[test]
    fn from_str_typo_suggests_correction() {
        let err = "runing".parse::<InstanceState>().unwrap_err();
        assert!(err.contains("did you mean: running?"), "got: {err}");
    }

    #[test]
    fn from_str_garbage_no_suggestion() {
        let err = "foobar".parse::<InstanceState>().unwrap_err();
        assert!(!err.contains("did you mean"), "got: {err}");
        assert!(err.contains("unknown instance state: foobar"), "got: {err}");
    }

    #[test]
    fn from_str_valid_states_all_parse() {
        for name in INSTANCE_STATE_NAMES {
            assert!(
                name.parse::<InstanceState>().is_ok(),
                "failed to parse: {name}"
            );
        }
    }

    #[test]
    fn cancelled_is_terminal_rejects_all_targets() {
        for target in [
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
            InstanceState::Failed,
            InstanceState::Completed,
            InstanceState::Cancelled,
        ] {
            assert!(
                !InstanceState::Cancelled.can_transition_to(target),
                "Cancelled -> {target:?} must be invalid"
            );
        }
    }
}
