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
    pub attempt: u16,
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

/// A worker's self-reported registration, refreshed on every poll.
///
/// One row per `(worker_id, handler_name)` pair — a worker that serves three
/// handlers appears three times. `last_seen_at` is bumped on every poll, so
/// liveness is "polled recently", independent of whether tasks were claimed.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkerRegistration {
    pub worker_id: String,
    pub handler_name: String,
    /// Named queue the worker polled, when using queue-scoped polling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    /// Optional worker-reported build/deploy version string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Tenant scope of the polling credential, when tenant-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub last_seen_at: DateTime<Utc>,
}

/// A control command queued for a specific worker, delivered via the worker
/// control channel (`GET /workers/{id}/commands`). The worker acts on pending
/// commands and acks them (`DELETE /workers/commands/{id}`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkerCommand {
    pub id: uuid::Uuid,
    /// The worker this command targets.
    pub worker_id: String,
    /// `drain` (stop claiming new tasks, finish in-flight), `reload`
    /// (re-read config / re-register handlers), or `ping` (liveness probe).
    pub command: WorkerCommandKind,
    /// Optional command parameters (e.g. a drain deadline).
    #[serde(default)]
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// The kind of a [`WorkerCommand`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerCommandKind {
    Drain,
    Reload,
    Ping,
}

impl std::fmt::Display for WorkerCommandKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Drain => f.write_str("drain"),
            Self::Reload => f.write_str("reload"),
            Self::Ping => f.write_str("ping"),
        }
    }
}

impl std::str::FromStr for WorkerCommandKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "drain" => Ok(Self::Drain),
            "reload" => Ok(Self::Reload),
            "ping" => Ok(Self::Ping),
            other => Err(format!("unknown worker command: {other}")),
        }
    }
}

/// A minimum-worker-version pin for a `(tenant, handler)` pair. A worker
/// reporting a version below `min_version` is not given tasks for that handler
/// at poll time — used to roll a fixed worker build out before old workers can
/// pick up affected work.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkerVersionPin {
    pub tenant_id: String,
    pub handler_name: String,
    pub min_version: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Does a worker's reported `version` satisfy a `min_version` pin?
///
/// Both are parsed as dot-separated numeric components (`"1.10.2"`) and
/// compared numerically (so `1.10.0 >= 1.9.0`, unlike a string compare).
/// Missing components default to 0 (`"2" == "2.0.0"`). If either side has a
/// non-numeric component the comparison falls back to a plain string `>=`.
/// A worker that reports no version never satisfies a pin (it's treated as too
/// old to trust).
#[must_use]
pub fn version_satisfies(worker: Option<&str>, min_version: &str) -> bool {
    let Some(worker) = worker else {
        return false;
    };
    match (parse_version(worker), parse_version(min_version)) {
        (Some(mut w), Some(mut m)) => {
            // Pad to equal length so "2" compares equal to "2.0.0".
            let len = w.len().max(m.len());
            w.resize(len, 0);
            m.resize(len, 0);
            w >= m
        }
        // Non-numeric versions: lexical fallback.
        _ => worker >= min_version,
    }
}

#[cfg(test)]
mod version_tests {
    use super::version_satisfies;

    #[test]
    fn numeric_versions_compare_numerically() {
        assert!(version_satisfies(Some("1.10.0"), "1.9.0"));
        assert!(!version_satisfies(Some("1.9.0"), "1.10.0"));
        assert!(version_satisfies(Some("2"), "2.0.0"));
        assert!(version_satisfies(Some("v2.1"), "2.0.0"));
        assert!(version_satisfies(Some("2.0.0"), "2.0.0"));
    }

    #[test]
    fn missing_worker_version_never_satisfies() {
        assert!(!version_satisfies(None, "1.0.0"));
    }

    #[test]
    fn non_numeric_falls_back_to_lexical() {
        assert!(version_satisfies(Some("2024-06-01"), "2024-05-01"));
        assert!(!version_satisfies(Some("2024-04-01"), "2024-05-01"));
    }
}

/// Parse `"1.10.2"` → `[1, 10, 2]`. Returns `None` if any component is not a
/// non-negative integer. Trailing/leading whitespace is trimmed.
fn parse_version(v: &str) -> Option<Vec<u64>> {
    let v = v.trim().trim_start_matches('v');
    if v.is_empty() {
        return None;
    }
    v.split('.').map(|c| c.parse::<u64>().ok()).collect()
}
