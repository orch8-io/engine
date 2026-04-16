use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{BlockId, ExecutionNodeId, InstanceId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
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
    SubSequence,
    ABSplit,
    CancellationScope,
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
            Self::SubSequence => "sub_sequence",
            Self::ABSplit => "ab_split",
            Self::CancellationScope => "cancellation_scope",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_state_display() {
        assert_eq!(NodeState::Pending.to_string(), "pending");
        assert_eq!(NodeState::Running.to_string(), "running");
        assert_eq!(NodeState::Waiting.to_string(), "waiting");
        assert_eq!(NodeState::Completed.to_string(), "completed");
        assert_eq!(NodeState::Failed.to_string(), "failed");
        assert_eq!(NodeState::Cancelled.to_string(), "cancelled");
        assert_eq!(NodeState::Skipped.to_string(), "skipped");
    }

    #[test]
    fn block_type_display() {
        assert_eq!(BlockType::Step.to_string(), "step");
        assert_eq!(BlockType::Parallel.to_string(), "parallel");
        assert_eq!(BlockType::Race.to_string(), "race");
        assert_eq!(BlockType::Loop.to_string(), "loop");
        assert_eq!(BlockType::ForEach.to_string(), "for_each");
        assert_eq!(BlockType::Router.to_string(), "router");
        assert_eq!(BlockType::TryCatch.to_string(), "try_catch");
        assert_eq!(BlockType::SubSequence.to_string(), "sub_sequence");
        assert_eq!(BlockType::ABSplit.to_string(), "ab_split");
        assert_eq!(
            BlockType::CancellationScope.to_string(),
            "cancellation_scope"
        );
    }

    #[test]
    fn node_state_serde_round_trip() {
        for state in [
            NodeState::Pending,
            NodeState::Running,
            NodeState::Waiting,
            NodeState::Completed,
            NodeState::Failed,
            NodeState::Cancelled,
            NodeState::Skipped,
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let back: NodeState = serde_json::from_str(&json).unwrap();
            assert_eq!(back, state);
        }
    }

    #[test]
    fn block_type_serde_round_trip() {
        for bt in [
            BlockType::Step,
            BlockType::Parallel,
            BlockType::Race,
            BlockType::Loop,
            BlockType::ForEach,
            BlockType::Router,
            BlockType::TryCatch,
            BlockType::SubSequence,
            BlockType::ABSplit,
            BlockType::CancellationScope,
        ] {
            let json = serde_json::to_string(&bt).unwrap();
            let back: BlockType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, bt);
        }
    }

    #[test]
    fn node_state_serde_rejects_invalid() {
        assert!(serde_json::from_str::<NodeState>(r#""paused""#).is_err());
    }

    #[test]
    fn block_type_serde_rejects_invalid() {
        assert!(serde_json::from_str::<BlockType>(r#""unknown""#).is_err());
    }
}
