use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Represents a running engine node in a multi-node cluster.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClusterNode {
    /// Unique node ID (generated at startup).
    pub id: Uuid,
    /// Human-readable node name (e.g. hostname or pod name).
    pub name: String,
    /// Node status.
    pub status: NodeStatus,
    /// When this node first registered.
    pub registered_at: DateTime<Utc>,
    /// Last heartbeat timestamp.
    pub last_heartbeat_at: DateTime<Utc>,
    /// If true, the node should stop accepting new work and drain.
    pub drain: bool,
}

/// Status of a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Actively processing work.
    Active,
    /// Draining — finishing in-flight work, not claiming new instances.
    Draining,
    /// Node has shut down gracefully.
    Stopped,
}

impl FromStr for NodeStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "draining" => Ok(Self::Draining),
            "stopped" => Ok(Self::Stopped),
            other => Err(format!("unknown node status: {other}")),
        }
    }
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Draining => f.write_str("draining"),
            Self::Stopped => f.write_str("stopped"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_status_from_str_all_variants() {
        assert_eq!("active".parse::<NodeStatus>().unwrap(), NodeStatus::Active);
        assert_eq!(
            "draining".parse::<NodeStatus>().unwrap(),
            NodeStatus::Draining
        );
        assert_eq!(
            "stopped".parse::<NodeStatus>().unwrap(),
            NodeStatus::Stopped
        );
    }

    #[test]
    fn node_status_from_str_unknown() {
        let err = "crashed".parse::<NodeStatus>().unwrap_err();
        assert!(err.contains("unknown node status: crashed"));
    }

    #[test]
    fn node_status_display_roundtrip() {
        let statuses = [
            NodeStatus::Active,
            NodeStatus::Draining,
            NodeStatus::Stopped,
        ];
        for status in statuses {
            let s = status.to_string();
            let parsed: NodeStatus = s.parse().unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn node_status_serde_roundtrip() {
        let status = NodeStatus::Draining;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"draining\"");
        let back: NodeStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }
}
