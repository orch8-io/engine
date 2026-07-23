//! Shared request/response types and small helpers for the instances module.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::error::ApiError;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority};
use orch8_types::signal::SignalType;

#[derive(Deserialize, ToSchema)]
pub struct CreateInstanceRequest {
    pub(crate) sequence_id: SequenceId,
    pub(crate) tenant_id: TenantId,
    pub(crate) namespace: Namespace,
    #[serde(default)]
    pub(crate) priority: Priority,
    #[serde(default = "default_timezone")]
    pub(crate) timezone: String,
    #[serde(default)]
    pub(crate) metadata: serde_json::Value,
    #[serde(default)]
    pub(crate) context: ExecutionContext,
    /// Run this instance in dry-run mode: side-effecting steps (HTTP/LLM/MCP
    /// calls, agent loops, event/signal emission, memory writes, block
    /// injection, external plugins) skip their real effect and return a stub;
    /// control flow and templating run normally. Any sequence can be run dry.
    /// Default false.
    #[serde(default)]
    pub(crate) dry_run: bool,
    /// When dry-run is on, auto-approve `human_review` gates with the default
    /// choice so the simulation flows past them instead of pausing. Ignored
    /// unless `dry_run` is true. Default false.
    #[serde(default)]
    pub(crate) dry_run_auto_approve: bool,
    pub(crate) next_fire_at: Option<DateTime<Utc>>,
    pub(crate) concurrency_key: Option<String>,
    pub(crate) max_concurrency: Option<u32>,
    pub(crate) idempotency_key: Option<String>,
    /// Optional resource budget (token/step caps) enforced by the scheduler.
    /// When any configured limit is exceeded the instance is paused with
    /// `metadata.paused_reason = "budget_exceeded"`. Default: no budget.
    #[serde(default)]
    pub(crate) budget: Option<orch8_types::instance::Budget>,
}

pub fn default_timezone() -> String {
    "UTC".to_string()
}

#[derive(Deserialize, ToSchema)]
pub struct BatchCreateRequest {
    pub(crate) instances: Vec<CreateInstanceRequest>,
}

#[derive(Deserialize, ToSchema)]
pub struct UpdateStateRequest {
    pub(crate) state: InstanceState,
    pub(crate) next_fire_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, ToSchema)]
pub struct UpdateContextRequest {
    pub(crate) context: ExecutionContext,
}

/// A signal enqueued atomically with a resume-from or fork, so it is already
/// pending when the instance becomes claimable. Closes the race window of the
/// separate "reset, then signal" two-call pattern.
#[derive(Deserialize, ToSchema)]
pub struct InjectedSignal {
    pub(crate) signal_type: SignalType,
    #[serde(default)]
    pub(crate) payload: serde_json::Value,
}

/// Body for `POST /instances/{id}/resume-from/{block_id}`. The whole body is
/// optional; when present, `context` must be a JSON object whose top-level
/// keys are shallow-merged into `context.data` (same per-key semantics as
/// `StorageBackend::merge_context_data`) before the instance is re-scheduled.
/// `signals` are enqueued before the wake transition — they are pending by
/// the time the instance can run again.
#[derive(Default, Deserialize, ToSchema)]
pub struct ResumeFromRequest {
    #[serde(default)]
    pub(crate) context: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) signals: Vec<InjectedSignal>,
}

/// Body for `POST /instances/{id}/fork`. `from_block_id` must be a top-level
/// block of the source's sequence; `context` (optional) is shallow-merged
/// into the fork's `context.data` with the same per-key semantics as
/// resume-from; `dry_run` defaults to **true** so a forked production
/// workflow does not re-fire side effects unless explicitly asked to.
/// `signals` are enqueued on the fork before it becomes claimable.
#[derive(Deserialize, ToSchema)]
pub struct ForkRequest {
    pub(crate) from_block_id: String,
    #[serde(default)]
    pub(crate) context: Option<serde_json::Value>,
    #[serde(default = "default_true")]
    pub(crate) dry_run: bool,
    #[serde(default)]
    pub(crate) signals: Vec<InjectedSignal>,
}

pub const fn default_true() -> bool {
    true
}

#[derive(Deserialize, ToSchema)]
pub struct SendSignalRequest {
    pub(crate) signal_type: SignalType,
    #[serde(default)]
    pub(crate) payload: serde_json::Value,
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub(crate) tenant_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) sequence_id: Option<Uuid>,
    pub(crate) state: Option<String>,
    #[serde(default)]
    pub(crate) offset: u64,
    #[serde(default = "default_limit")]
    pub(crate) limit: u32,
}

pub const fn default_limit() -> u32 {
    100
}

#[derive(Deserialize, ToSchema)]
pub struct BulkFilter {
    pub(crate) tenant_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) sequence_id: Option<Uuid>,
    pub(crate) states: Option<Vec<InstanceState>>,
    /// Top-level `metadata` equality filters (key → value), combined with AND.
    /// Served by the same indexed path as `GET /instances?metadata.k=v`.
    #[serde(default)]
    pub(crate) metadata: Option<std::collections::HashMap<String, String>>,
}

/// The operation a batch action applies to every matched instance.
#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BatchAction {
    /// Re-run a `Failed` instance (clears the stale tree + sentinel outputs,
    /// resets the run identity and step counters, then re-schedules).
    Retry,
    /// Enqueue a `pause` control signal.
    Pause,
    /// Enqueue a `resume` control signal.
    Resume,
    /// Enqueue a `cancel` control signal.
    Cancel,
    /// Enqueue a custom signal named by `signal_type`, carrying `payload`.
    Signal,
}

#[derive(Deserialize, ToSchema)]
pub struct BatchActionRequest {
    pub(crate) filter: BulkFilter,
    pub(crate) action: BatchAction,
    /// Custom signal name for the `signal` action (no `custom:` prefix). Ignored
    /// for the other actions.
    #[serde(default)]
    pub(crate) signal_type: Option<String>,
    /// Payload for the `signal` action.
    #[serde(default)]
    pub(crate) payload: serde_json::Value,
    /// When true, count matching instances and apply nothing.
    #[serde(default)]
    pub(crate) dry_run: bool,
    /// Hard cap on how many instances are acted on. Default 1000, max 10000.
    #[serde(default = "default_batch_action_limit")]
    pub(crate) limit: u32,
}

fn default_batch_action_limit() -> u32 {
    1000
}

#[derive(Serialize, ToSchema)]
pub struct BatchActionResponse {
    /// Instances matched by the filter (capped by `limit`).
    pub(crate) matched: u64,
    /// Instances the action was successfully applied to.
    pub(crate) applied: u64,
    /// Instances the action did not apply to (e.g. retry of a non-Failed
    /// instance, or a signal to a terminal instance).
    pub(crate) skipped: u64,
    /// Instances where applying the action errored.
    pub(crate) failed: u64,
    pub(crate) dry_run: bool,
}

#[derive(Deserialize, ToSchema)]
pub struct BulkUpdateStateRequest {
    pub(crate) filter: BulkFilter,
    pub(crate) state: InstanceState,
}

#[derive(Deserialize, ToSchema)]
pub struct BulkRescheduleRequest {
    pub(crate) filter: BulkFilter,
    /// Shift `next_fire_at` by this many seconds (positive = later, negative = earlier).
    pub(crate) offset_secs: i64,
}

#[derive(Serialize, ToSchema)]
pub struct CountResponse {
    pub(crate) count: u64,
}

/// Parse comma-separated state values.
pub fn parse_states(s: &str) -> Result<Vec<InstanceState>, ApiError> {
    if s.trim().is_empty() {
        return Ok(Vec::new());
    }
    s.split(',')
        .map(|part| {
            let trimmed = part.trim();
            match trimmed {
                "scheduled" => Ok(InstanceState::Scheduled),
                "running" => Ok(InstanceState::Running),
                "waiting" => Ok(InstanceState::Waiting),
                "paused" => Ok(InstanceState::Paused),
                "completed" => Ok(InstanceState::Completed),
                "failed" => Ok(InstanceState::Failed),
                "cancelled" => Ok(InstanceState::Cancelled),
                other => Err(ApiError::InvalidArgument(format!(
                    "unknown instance state: {other}"
                ))),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_state() {
        assert_eq!(
            parse_states("running").unwrap(),
            vec![InstanceState::Running]
        );
    }

    #[test]
    fn parse_multiple_states() {
        assert_eq!(
            parse_states("scheduled,running,completed").unwrap(),
            vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Completed
            ]
        );
    }

    #[test]
    fn parse_states_with_whitespace() {
        assert_eq!(
            parse_states(" scheduled , running ").unwrap(),
            vec![InstanceState::Scheduled, InstanceState::Running]
        );
    }

    #[test]
    fn parse_all_states() {
        let all = "scheduled,running,waiting,paused,completed,failed,cancelled";
        assert_eq!(
            parse_states(all).unwrap(),
            vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
                InstanceState::Paused,
                InstanceState::Completed,
                InstanceState::Failed,
                InstanceState::Cancelled,
            ]
        );
    }

    #[test]
    fn parse_empty_string_returns_empty() {
        assert_eq!(parse_states("").unwrap(), Vec::<InstanceState>::new());
    }

    #[test]
    fn parse_unknown_state_errors() {
        let err = parse_states("running,unknown").unwrap_err();
        assert!(err.to_string().contains("unknown instance state: unknown"));
    }

    #[test]
    fn default_limit_is_100() {
        assert_eq!(default_limit(), 100);
    }

    #[test]
    fn default_timezone_is_utc() {
        assert_eq!(default_timezone(), "UTC");
    }
}
