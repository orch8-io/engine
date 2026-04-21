use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{BlockId, InstanceId};

/// State of a worker task in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkerTaskState {
    Pending,
    Claimed,
    Completed,
    Failed,
}

impl FromStr for WorkerTaskState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown worker task state: {other}")),
        }
    }
}

impl std::fmt::Display for WorkerTaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Claimed => f.write_str("claimed"),
            Self::Completed => f.write_str("completed"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_task_state_from_str_pending() {
        assert_eq!(
            "pending".parse::<WorkerTaskState>().unwrap(),
            WorkerTaskState::Pending
        );
    }

    #[test]
    fn worker_task_state_from_str_claimed() {
        assert_eq!(
            "claimed".parse::<WorkerTaskState>().unwrap(),
            WorkerTaskState::Claimed
        );
    }

    #[test]
    fn worker_task_state_from_str_completed() {
        assert_eq!(
            "completed".parse::<WorkerTaskState>().unwrap(),
            WorkerTaskState::Completed
        );
    }

    #[test]
    fn worker_task_state_from_str_failed() {
        assert_eq!(
            "failed".parse::<WorkerTaskState>().unwrap(),
            WorkerTaskState::Failed
        );
    }

    #[test]
    fn worker_task_state_from_str_unknown() {
        let err = "bogus".parse::<WorkerTaskState>().unwrap_err();
        assert!(err.contains("unknown worker task state: bogus"));
    }

    #[test]
    fn worker_task_state_display_roundtrip() {
        let states = [
            WorkerTaskState::Pending,
            WorkerTaskState::Claimed,
            WorkerTaskState::Completed,
            WorkerTaskState::Failed,
        ];
        for state in states {
            let s = state.to_string();
            let parsed: WorkerTaskState = s.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn worker_task_state_serde_roundtrip() {
        let state = WorkerTaskState::Claimed;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"claimed\"");
        let back: WorkerTaskState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
    }
}

/// A task dispatched to an external worker for execution.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkerTask {
    pub id: Uuid,
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub handler_name: String,
    /// Named task queue for routing to dedicated worker pools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    pub params: serde_json::Value,
    /// Serialized `ExecutionContext` — kept as raw JSON to avoid coupling workers to Rust types.
    pub context: serde_json::Value,
    pub attempt: i16,
    pub timeout_ms: Option<i64>,
    pub state: WorkerTaskState,
    pub worker_id: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub output: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub error_retryable: Option<bool>,
    pub created_at: DateTime<Utc>,
}
