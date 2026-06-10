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
                // Failed -> Scheduled is retry from the DLQ.
                // Completed / Cancelled -> Scheduled is resume-from-block
                // surgery: an operator may re-run a finished instance from an
                // arbitrary block (wiping that block's and all later outputs
                // first), which requires re-scheduling instances that already
                // reached a terminal state. All other exits from the terminal
                // states remain invalid.
                | (
                    Self::Failed | Self::Completed | Self::Cancelled,
                    Self::Scheduled
                )
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

/// A single budget breach detected by [`Budget::first_breach`].
///
/// `limit` names the breached field (e.g. `"max_total_tokens"`), `limit_value`
/// is the configured cap and `actual` the observed consumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetBreach {
    /// Name of the breached limit field (e.g. `"max_input_tokens"`).
    pub limit: &'static str,
    /// The configured limit value.
    pub limit_value: i64,
    /// The actual observed value that exceeded the limit.
    pub actual: i64,
}

/// Optional per-instance resource budget enforced by the scheduler.
///
/// When any configured limit is exceeded (recorded LLM token usage or
/// executed step count), the scheduler pauses the instance with
/// `metadata.paused_reason = "budget_exceeded"` instead of letting it keep
/// spending. Resume via the normal `Paused -> Scheduled` transition.
///
/// v1 semantics: the budget is checked when the scheduler claims the
/// instance, *before* executing work for that tick. A resumed instance
/// therefore gets one more tick of work before the check fires again; if the
/// budget is still exceeded it pauses again on the next claim. A single
/// tick's work may also overshoot the limit slightly (the check is
/// pre-flight, not mid-step).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Budget {
    /// Max total LLM input (prompt) tokens recorded for this instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens: Option<i64>,
    /// Max total LLM output (completion) tokens recorded for this instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<i64>,
    /// Max combined (input + output) LLM tokens recorded for this instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<i64>,
    /// Max executed steps (`context.runtime.total_steps_executed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<i64>,
}

impl Budget {
    /// True if any token-based limit is configured (callers can skip the
    /// usage-totals query entirely when this is false).
    #[must_use]
    pub const fn has_token_limits(&self) -> bool {
        self.max_input_tokens.is_some()
            || self.max_output_tokens.is_some()
            || self.max_total_tokens.is_some()
    }

    /// Return the first configured limit exceeded by the observed usage, or
    /// `None` when the instance is within budget. A limit is breached when
    /// the actual value is strictly greater than the configured cap.
    #[must_use]
    pub fn first_breach(
        &self,
        input_tokens: i64,
        output_tokens: i64,
        steps: i64,
    ) -> Option<BudgetBreach> {
        let checks = [
            ("max_input_tokens", self.max_input_tokens, input_tokens),
            ("max_output_tokens", self.max_output_tokens, output_tokens),
            (
                "max_total_tokens",
                self.max_total_tokens,
                input_tokens.saturating_add(output_tokens),
            ),
            ("max_steps", self.max_steps, steps),
        ];
        checks.into_iter().find_map(|(limit, cap, actual)| {
            cap.filter(|&limit_value| actual > limit_value)
                .map(|limit_value| BudgetBreach {
                    limit,
                    limit_value,
                    actual,
                })
        })
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
    pub max_concurrency: Option<u32>,
    /// Optional idempotency key for deduplication at creation time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Session ID for cross-instance shared state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<uuid::Uuid>,
    /// Parent instance ID (set when this instance is a sub-sequence child).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_instance_id: Option<InstanceId>,
    /// Optional resource budget enforced by the scheduler (see [`Budget`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<Budget>,
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
        assert!(!InstanceState::Cancelled.can_transition_to(InstanceState::Running));
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
    fn completed_rejects_all_targets_except_scheduled() {
        // `Completed -> Scheduled` is the resume-from-block escape hatch;
        // every other exit from Completed remains invalid.
        for target in [
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
        assert!(InstanceState::Completed.can_transition_to(InstanceState::Scheduled));
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
    fn cancelled_rejects_all_targets_except_scheduled() {
        // `Cancelled -> Scheduled` is the resume-from-block escape hatch;
        // every other exit from Cancelled remains invalid.
        for target in [
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
        assert!(InstanceState::Cancelled.can_transition_to(InstanceState::Scheduled));
    }

    // --- Budget ---

    #[test]
    fn budget_serde_round_trip() {
        let budget = Budget {
            max_input_tokens: Some(1_000),
            max_output_tokens: None,
            max_total_tokens: Some(5_000),
            max_steps: Some(10),
        };
        let json = serde_json::to_string(&budget).unwrap();
        let back: Budget = serde_json::from_str(&json).unwrap();
        assert_eq!(budget, back);
        // Unset fields are skipped entirely, not serialized as null.
        assert!(!json.contains("max_output_tokens"), "got: {json}");
    }

    #[test]
    fn budget_deserializes_from_empty_object() {
        let budget: Budget = serde_json::from_str("{}").unwrap();
        assert_eq!(budget, Budget::default());
        assert!(!budget.has_token_limits());
    }

    #[test]
    fn task_instance_json_without_budget_field_deserializes() {
        // JSON produced before the budget field existed must still load.
        let json = serde_json::json!({
            "id": uuid::Uuid::now_v7(),
            "sequence_id": uuid::Uuid::now_v7(),
            "tenant_id": "t1",
            "namespace": "ns",
            "state": "scheduled",
            "next_fire_at": null,
            "priority": "Normal",
            "timezone": "UTC",
            "metadata": {},
            "context": { "data": {}, "config": {}, "audit": [] },
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        });
        let inst: TaskInstance = serde_json::from_value(json).unwrap();
        assert!(inst.budget.is_none());
    }

    #[test]
    fn task_instance_budget_round_trips_through_json() {
        let json = serde_json::json!({
            "id": uuid::Uuid::now_v7(),
            "sequence_id": uuid::Uuid::now_v7(),
            "tenant_id": "t1",
            "namespace": "ns",
            "state": "scheduled",
            "next_fire_at": null,
            "priority": "Normal",
            "timezone": "UTC",
            "metadata": {},
            "context": { "data": {}, "config": {}, "audit": [] },
            "budget": { "max_total_tokens": 100 },
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        });
        let inst: TaskInstance = serde_json::from_value(json).unwrap();
        let budget = inst.budget.as_ref().expect("budget present");
        assert_eq!(budget.max_total_tokens, Some(100));
        let back = serde_json::to_value(&inst).unwrap();
        assert_eq!(back["budget"]["max_total_tokens"], 100);
    }

    #[test]
    fn budget_first_breach_within_limits_is_none() {
        let budget = Budget {
            max_input_tokens: Some(100),
            max_output_tokens: Some(100),
            max_total_tokens: Some(200),
            max_steps: Some(5),
        };
        assert_eq!(budget.first_breach(100, 100, 5), None);
    }

    #[test]
    fn budget_first_breach_reports_each_limit() {
        let input_breach = Budget {
            max_input_tokens: Some(10),
            ..Budget::default()
        }
        .first_breach(11, 0, 0)
        .unwrap();
        assert_eq!(input_breach.limit, "max_input_tokens");
        assert_eq!(input_breach.limit_value, 10);
        assert_eq!(input_breach.actual, 11);

        let output_breach = Budget {
            max_output_tokens: Some(10),
            ..Budget::default()
        }
        .first_breach(0, 11, 0)
        .unwrap();
        assert_eq!(output_breach.limit, "max_output_tokens");

        let total_breach = Budget {
            max_total_tokens: Some(10),
            ..Budget::default()
        }
        .first_breach(6, 6, 0)
        .unwrap();
        assert_eq!(total_breach.limit, "max_total_tokens");
        assert_eq!(total_breach.actual, 12);

        let steps_breach = Budget {
            max_steps: Some(3),
            ..Budget::default()
        }
        .first_breach(0, 0, 4)
        .unwrap();
        assert_eq!(steps_breach.limit, "max_steps");
    }

    #[test]
    fn budget_with_no_limits_never_breaches() {
        assert_eq!(
            Budget::default().first_breach(i64::MAX, i64::MAX, i64::MAX),
            None
        );
    }

    #[test]
    fn display_round_trip() {
        for state in [
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
            InstanceState::Completed,
            InstanceState::Failed,
            InstanceState::Cancelled,
        ] {
            let s = state.to_string();
            let parsed = InstanceState::from_str(&s).unwrap();
            assert_eq!(state, parsed);
        }
    }
}
