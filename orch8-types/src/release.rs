//! Safe workflow releases: domain model for the release control plane.
//!
//! A release ties an exact baseline sequence version to an exact
//! candidate version (stored by id — never inferred later from mutable
//! names), moves through a conditional, audited state machine, and
//! routes a deterministic cohort of new instances to the candidate
//! during canary.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{Namespace, SequenceId, TenantId};

/// Release lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseState {
    /// Created; nothing verified yet.
    Draft,
    /// Historical validation in progress.
    Validating,
    /// Validation passed (or was explicitly skipped); canary may start.
    Ready,
    /// A percentage of new instances routes to the candidate.
    Canary,
    /// All new instances route to the candidate.
    Promoted,
    /// Canary halted; new instances route to the baseline. Resumable.
    Paused,
    /// Canary aborted; all traffic returned to the baseline. Terminal.
    RolledBack,
    /// Validation failed. Terminal.
    Failed,
}

impl ReleaseState {
    /// Legal transitions. Every transition must go through a
    /// compare-and-swap so concurrent promote/rollback cannot both win.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Draft, Self::Validating | Self::Ready | Self::Failed)
                | (Self::Validating, Self::Ready | Self::Failed)
                | (Self::Ready, Self::Canary | Self::Failed)
                | (Self::Canary, Self::Promoted | Self::Paused | Self::RolledBack)
                | (Self::Paused, Self::Canary | Self::RolledBack)
        )
    }

    /// Terminal states accept no further transitions.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Promoted | Self::RolledBack | Self::Failed)
    }

    /// States in which instance creation consults the release for
    /// routing.
    #[must_use]
    pub const fn routes_traffic(self) -> bool {
        matches!(self, Self::Canary | Self::Promoted)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Validating => "validating",
            Self::Ready => "ready",
            Self::Canary => "canary",
            Self::Promoted => "promoted",
            Self::Paused => "paused",
            Self::RolledBack => "rolled_back",
            Self::Failed => "failed",
        }
    }
}

impl std::str::FromStr for ReleaseState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "draft" => Ok(Self::Draft),
            "validating" => Ok(Self::Validating),
            "ready" => Ok(Self::Ready),
            "canary" => Ok(Self::Canary),
            "promoted" => Ok(Self::Promoted),
            "paused" => Ok(Self::Paused),
            "rolled_back" => Ok(Self::RolledBack),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown release state: {other}")),
        }
    }
}

/// Gate metrics evaluated over canary observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GateMetric {
    /// Fraction of instances ending `failed` (0.0–1.0).
    ErrorRate,
    /// Fraction of instances ending `cancelled` (0.0–1.0).
    CancelRate,
}

/// One release gate: the candidate may regress at most
/// `max_regression` (absolute difference, e.g. `0.05` = five
/// percentage points) versus the baseline, evaluated only once both
/// variants have at least `min_sample` terminal observations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ReleaseGate {
    pub metric: GateMetric,
    /// Maximum allowed absolute regression of the candidate vs. the
    /// baseline (candidate − baseline).
    pub max_regression: f64,
    /// Minimum terminal instances per variant before the gate is
    /// conclusive.
    pub min_sample: u32,
}

/// How in-flight instances are treated when a release promotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InFlightPolicy {
    /// In-flight instances stay pinned to the version they started with
    /// (default — versions are immutable, so this is always safe).
    #[default]
    Pin,
    /// The operator migrates in-flight instances explicitly via the
    /// existing migration endpoint; promotion does not touch them.
    OperatorDecision,
}

/// A release candidate under evaluation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowRelease {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub namespace: Namespace,
    /// Display name (the sequence name at creation) — informational
    /// only; routing always uses the exact ids below.
    pub sequence_name: String,
    pub baseline_sequence_id: SequenceId,
    pub baseline_version: i32,
    pub candidate_sequence_id: SequenceId,
    pub candidate_version: i32,
    pub state: ReleaseState,
    /// Percentage of new instances routed to the candidate during
    /// canary (0–100).
    pub canary_percent: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gates: Vec<ReleaseGate>,
    #[serde(default)]
    pub in_flight_policy: InFlightPolicy,
    /// Result summary of the last historical validation run, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_summary: Option<serde_json::Value>,
    /// When the canary started (observation window lower bound).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary_started_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Immutable audit record of one state transition (or attempted one).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ReleaseDecision {
    pub id: Uuid,
    pub release_id: Uuid,
    pub from_state: ReleaseState,
    pub to_state: ReleaseState,
    /// Who/what decided: `operator`, `gate:error_rate`, `validation`.
    pub actor: String,
    /// Human-readable justification.
    pub reason: String,
    pub decided_at: DateTime<Utc>,
}

/// Which variant of a release an instance was routed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseVariant {
    Baseline,
    Candidate,
}

/// Deterministic cohort assignment: the same `(release, cohort_key)`
/// always lands on the same variant, and raising `canary_percent`
/// only *adds* cohorts to the candidate (monotone) — cohorts never
/// flap back and forth as the percentage grows.
#[must_use]
pub fn assign_variant(release_id: Uuid, cohort_key: &str, canary_percent: u8) -> ReleaseVariant {
    if canary_percent >= 100 {
        return ReleaseVariant::Candidate;
    }
    if canary_percent == 0 {
        return ReleaseVariant::Baseline;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for chunk in [release_id.as_bytes().as_slice(), cohort_key.as_bytes()] {
        for &b in chunk {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    #[allow(clippy::cast_possible_truncation)]
    let bucket = (hash % 100) as u8;
    if bucket < canary_percent {
        ReleaseVariant::Candidate
    } else {
        ReleaseVariant::Baseline
    }
}

// ---------------------------------------------------------------------------
// Semantic diff report types (logic lives in orch8-engine::release_diff)
// ---------------------------------------------------------------------------

/// How much a semantic-diff change matters, ordered.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DiffSeverity {
    /// Cosmetic / metadata only.
    Informational,
    /// Execution behavior changes (paths, timing, retries).
    Behavioral,
    /// An external side effect may fire again or differently.
    SideEffectRisk,
    /// The candidate will break: dangling references, narrowed inputs.
    Incompatible,
}

/// One semantic difference between two sequence versions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DiffEntry {
    /// Stable category code, snake_case (`block_added`,
    /// `handler_changed`, `input_schema_narrowed`, ...).
    pub category: String,
    pub severity: DiffSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    pub summary: String,
}

/// The full semantic-diff report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SemanticDiff {
    pub entries: Vec<DiffEntry>,
    /// Worst severity present (absent for an empty diff).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_severity: Option<DiffSeverity>,
    /// Lint findings on the candidate, included so the release report is
    /// one artifact.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_lint: Vec<String>,
}

/// Aggregated terminal counts for one variant during canary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct VariantStats {
    pub total: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
}

impl VariantStats {
    #[must_use]
    pub fn rate(&self, metric: GateMetric) -> Option<f64> {
        if self.total == 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        let total = self.total as f64;
        #[allow(clippy::cast_precision_loss)]
        Some(match metric {
            GateMetric::ErrorRate => self.failed as f64 / total,
            GateMetric::CancelRate => self.cancelled as f64 / total,
        })
    }
}

/// Outcome of evaluating one gate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct GateEvaluation {
    pub gate: ReleaseGate,
    /// `None` while either variant lacks `min_sample` observations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_rate: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_rate: Option<f64>,
    pub verdict: GateVerdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GateVerdict {
    /// Not enough data yet — never treated as pass.
    Inconclusive,
    Pass,
    Fail,
}

/// Evaluate every gate against the two variants' stats. Insufficient
/// samples yield `Inconclusive`, never `Pass`.
#[must_use]
pub fn evaluate_gates(
    gates: &[ReleaseGate],
    baseline: VariantStats,
    candidate: VariantStats,
) -> Vec<GateEvaluation> {
    gates
        .iter()
        .map(|gate| {
            let enough = baseline.total >= u64::from(gate.min_sample)
                && candidate.total >= u64::from(gate.min_sample);
            let baseline_rate = baseline.rate(gate.metric);
            let candidate_rate = candidate.rate(gate.metric);
            let verdict = if !enough {
                GateVerdict::Inconclusive
            } else {
                match (baseline_rate, candidate_rate) {
                    (Some(b), Some(c)) => {
                        if c - b > gate.max_regression {
                            GateVerdict::Fail
                        } else {
                            GateVerdict::Pass
                        }
                    }
                    _ => GateVerdict::Inconclusive,
                }
            };
            GateEvaluation {
                gate: gate.clone(),
                baseline_rate,
                candidate_rate,
                verdict,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- state machine ---

    #[test]
    fn legal_transition_paths() {
        use ReleaseState as S;
        assert!(S::Draft.can_transition_to(S::Validating));
        assert!(S::Draft.can_transition_to(S::Ready)); // explicit skip
        assert!(S::Validating.can_transition_to(S::Ready));
        assert!(S::Validating.can_transition_to(S::Failed));
        assert!(S::Ready.can_transition_to(S::Canary));
        assert!(S::Canary.can_transition_to(S::Promoted));
        assert!(S::Canary.can_transition_to(S::Paused));
        assert!(S::Canary.can_transition_to(S::RolledBack));
        assert!(S::Paused.can_transition_to(S::Canary));
        assert!(S::Paused.can_transition_to(S::RolledBack));
    }

    #[test]
    fn illegal_transitions_are_rejected() {
        use ReleaseState as S;
        // Terminal states are frozen.
        for terminal in [S::Promoted, S::RolledBack, S::Failed] {
            for next in [
                S::Draft,
                S::Validating,
                S::Ready,
                S::Canary,
                S::Promoted,
                S::Paused,
                S::RolledBack,
                S::Failed,
            ] {
                assert!(!terminal.can_transition_to(next), "{terminal:?} -> {next:?}");
            }
        }
        // No skipping validation outcome straight to canary.
        assert!(!S::Draft.can_transition_to(S::Canary));
        assert!(!S::Validating.can_transition_to(S::Canary));
        // Promote requires an active canary.
        assert!(!S::Ready.can_transition_to(S::Promoted));
        assert!(!S::Paused.can_transition_to(S::Promoted));
    }

    #[test]
    fn state_round_trips_through_str() {
        use std::str::FromStr;
        for s in [
            ReleaseState::Draft,
            ReleaseState::Validating,
            ReleaseState::Ready,
            ReleaseState::Canary,
            ReleaseState::Promoted,
            ReleaseState::Paused,
            ReleaseState::RolledBack,
            ReleaseState::Failed,
        ] {
            assert_eq!(ReleaseState::from_str(s.as_str()).unwrap(), s);
            // serde repr matches as_str
            assert_eq!(
                serde_json::to_string(&s).unwrap(),
                format!("\"{}\"", s.as_str())
            );
        }
    }

    #[test]
    fn only_canary_and_promoted_route_traffic() {
        assert!(ReleaseState::Canary.routes_traffic());
        assert!(ReleaseState::Promoted.routes_traffic());
        for s in [
            ReleaseState::Draft,
            ReleaseState::Validating,
            ReleaseState::Ready,
            ReleaseState::Paused,
            ReleaseState::RolledBack,
            ReleaseState::Failed,
        ] {
            assert!(!s.routes_traffic(), "{s:?}");
        }
    }

    // --- cohort assignment ---

    #[test]
    fn assignment_is_deterministic() {
        let release = Uuid::now_v7();
        for key in ["tenant-a", "customer-42", "device-x"] {
            let first = assign_variant(release, key, 30);
            for _ in 0..100 {
                assert_eq!(assign_variant(release, key, 30), first, "{key}");
            }
        }
    }

    #[test]
    fn assignment_is_monotone_in_percent() {
        // A cohort on the candidate at p% must stay on the candidate for
        // every higher percentage — raising the rollout never reshuffles.
        let release = Uuid::now_v7();
        for i in 0..200 {
            let key = format!("cohort-{i}");
            let mut was_candidate = false;
            for pct in 0..=100u8 {
                let v = assign_variant(release, &key, pct);
                if was_candidate {
                    assert_eq!(
                        v,
                        ReleaseVariant::Candidate,
                        "cohort {key} flapped back at {pct}%"
                    );
                }
                if v == ReleaseVariant::Candidate {
                    was_candidate = true;
                }
            }
            assert!(was_candidate, "at 100% everyone is on the candidate");
        }
    }

    #[test]
    fn assignment_extremes() {
        let release = Uuid::now_v7();
        for i in 0..50 {
            let key = format!("k{i}");
            assert_eq!(assign_variant(release, &key, 0), ReleaseVariant::Baseline);
            assert_eq!(assign_variant(release, &key, 100), ReleaseVariant::Candidate);
        }
    }

    #[test]
    fn assignment_distribution_is_roughly_proportional() {
        let release = Uuid::now_v7();
        let n = 10_000;
        let candidates = (0..n)
            .filter(|i| {
                assign_variant(release, &format!("cohort-{i}"), 30) == ReleaseVariant::Candidate
            })
            .count();
        let share = candidates as f64 / f64::from(n);
        assert!((0.25..=0.35).contains(&share), "share was {share}");
    }

    #[test]
    fn different_releases_shuffle_cohorts_independently() {
        // The same cohort key must not always land on the same side across
        // releases (no global bias by key).
        let keys: Vec<String> = (0..100).map(|i| format!("k{i}")).collect();
        let r1 = Uuid::now_v7();
        let r2 = Uuid::now_v7();
        let same = keys
            .iter()
            .filter(|k| assign_variant(r1, k, 50) == assign_variant(r2, k, 50))
            .count();
        assert!((20..=80).contains(&same), "suspicious correlation: {same}/100");
    }

    // --- gates ---

    fn stats(total: u64, failed: u64) -> VariantStats {
        VariantStats {
            total,
            completed: total - failed,
            failed,
            cancelled: 0,
        }
    }

    #[test]
    fn gate_passes_within_regression_budget() {
        let gates = vec![ReleaseGate {
            metric: GateMetric::ErrorRate,
            max_regression: 0.05,
            min_sample: 10,
        }];
        let evals = evaluate_gates(&gates, stats(100, 10), stats(100, 12));
        assert_eq!(evals[0].verdict, GateVerdict::Pass); // 0.12-0.10 = 0.02 <= 0.05
    }

    #[test]
    fn gate_fails_beyond_regression_budget() {
        let gates = vec![ReleaseGate {
            metric: GateMetric::ErrorRate,
            max_regression: 0.05,
            min_sample: 10,
        }];
        let evals = evaluate_gates(&gates, stats(100, 10), stats(100, 20));
        assert_eq!(evals[0].verdict, GateVerdict::Fail); // +0.10 > 0.05
        assert_eq!(evals[0].baseline_rate, Some(0.10));
        assert_eq!(evals[0].candidate_rate, Some(0.20));
    }

    #[test]
    fn insufficient_sample_is_inconclusive_never_pass() {
        let gates = vec![ReleaseGate {
            metric: GateMetric::ErrorRate,
            max_regression: 0.05,
            min_sample: 50,
        }];
        // Candidate looks perfect but only 5 observations: inconclusive.
        let evals = evaluate_gates(&gates, stats(100, 10), stats(5, 0));
        assert_eq!(evals[0].verdict, GateVerdict::Inconclusive);
    }

    #[test]
    fn candidate_improvement_passes() {
        let gates = vec![ReleaseGate {
            metric: GateMetric::ErrorRate,
            max_regression: 0.0,
            min_sample: 10,
        }];
        let evals = evaluate_gates(&gates, stats(100, 20), stats(100, 5));
        assert_eq!(evals[0].verdict, GateVerdict::Pass);
    }

    #[test]
    fn cancel_rate_gate_reads_cancelled() {
        let gates = vec![ReleaseGate {
            metric: GateMetric::CancelRate,
            max_regression: 0.01,
            min_sample: 10,
        }];
        let baseline = VariantStats { total: 100, completed: 95, failed: 0, cancelled: 5 };
        let candidate = VariantStats { total: 100, completed: 80, failed: 0, cancelled: 20 };
        let evals = evaluate_gates(&gates, baseline, candidate);
        assert_eq!(evals[0].verdict, GateVerdict::Fail);
    }

    #[test]
    fn release_round_trips() {
        let r = WorkflowRelease {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            sequence_name: "checkout".into(),
            baseline_sequence_id: SequenceId::new(),
            baseline_version: 3,
            candidate_sequence_id: SequenceId::new(),
            candidate_version: 4,
            state: ReleaseState::Canary,
            canary_percent: 5,
            gates: vec![ReleaseGate {
                metric: GateMetric::ErrorRate,
                max_regression: 0.05,
                min_sample: 20,
            }],
            in_flight_policy: InFlightPolicy::Pin,
            validation_summary: Some(serde_json::json!({"replayed": 20})),
            canary_started_at: Some(Utc::now()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let back: WorkflowRelease =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
    }
}
