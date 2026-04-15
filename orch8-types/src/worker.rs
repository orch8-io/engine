use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{BlockId, InstanceId};

/// State of a worker task in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerTaskState {
    Pending,
    Claimed,
    Completed,
    Failed,
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
