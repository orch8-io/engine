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
pub enum InstanceState {
    Scheduled,
    Running,
    Waiting,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl InstanceState {
    /// Returns true if the transition is legal.
    pub fn can_transition_to(self, target: Self) -> bool {
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

    pub fn is_terminal(self) -> bool {
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
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
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
}
