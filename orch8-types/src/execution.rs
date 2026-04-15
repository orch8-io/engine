use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{BlockId, ExecutionNodeId, InstanceId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "node_state", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    Pending,
    Running,
    /// Dispatched to an external worker; awaiting completion callback.
    Waiting,
    Completed,
    Failed,
    Cancelled,
    Skipped,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "block_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum BlockType {
    Step,
    Parallel,
    Race,
    Loop,
    ForEach,
    Router,
    TryCatch,
}

impl std::fmt::Display for BlockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Step => "step",
            Self::Parallel => "parallel",
            Self::Race => "race",
            Self::Loop => "loop",
            Self::ForEach => "for_each",
            Self::Router => "router",
            Self::TryCatch => "try_catch",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionNode {
    pub id: ExecutionNodeId,
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub parent_id: Option<ExecutionNodeId>,
    pub block_type: BlockType,
    pub branch_index: Option<i16>,
    pub state: NodeState,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}
