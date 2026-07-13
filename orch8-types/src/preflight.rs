//! Sequence preflight: one readiness report before publishing or starting
//! a workflow.
//!
//! A preflight run answers "will this sequence actually execute?" by
//! combining static validation with a snapshot of the runtime inventory
//! (workers, credentials, plugins, queues, sub-sequences). Every check
//! reports `pass`, `warning`, `fail`, or `unknown` — uncertainty is never
//! turned into success.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::finding::Finding;

/// Outcome of a single preflight check.
///
/// Ordering is by increasing severity for aggregation: `Pass < Warning <
/// Unknown < Fail`. `Unknown` outranks `Warning` because unproven
/// readiness is riskier than a known cosmetic issue.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PreflightStatus {
    /// The check proved readiness.
    Pass,
    /// Something looks off but execution can proceed.
    Warning,
    /// Readiness could not be proven (inventory unavailable). NOT success.
    Unknown,
    /// The sequence will not work until this is fixed.
    Fail,
}

/// One named readiness check with its findings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PreflightCheck {
    /// Stable check identifier, `snake_case`: `definition_valid`,
    /// `handlers_have_workers`, `credentials_present`, ...
    pub id: String,
    pub status: PreflightStatus,
    /// Human-readable one-liner of the result.
    pub summary: String,
    /// Detailed findings (empty for a clean pass).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<Finding>,
}

impl PreflightCheck {
    #[must_use]
    pub fn pass(id: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            status: PreflightStatus::Pass,
            summary: summary.into(),
            findings: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_status(
        id: impl Into<String>,
        status: PreflightStatus,
        summary: impl Into<String>,
        findings: Vec<Finding>,
    ) -> Self {
        Self {
            id: id.into(),
            status,
            summary: summary.into(),
            findings,
        }
    }
}

/// The combined preflight report for one sequence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PreflightReport {
    pub sequence_name: String,
    pub sequence_version: i64,
    /// Worst status across all checks.
    pub overall: PreflightStatus,
    pub checks: Vec<PreflightCheck>,
    pub generated_at: DateTime<Utc>,
}

impl PreflightReport {
    /// Assemble a report; `overall` is the worst check status (or `Pass`
    /// for an empty check list).
    #[must_use]
    pub fn new(
        sequence_name: impl Into<String>,
        sequence_version: i64,
        checks: Vec<PreflightCheck>,
        generated_at: DateTime<Utc>,
    ) -> Self {
        let overall = checks
            .iter()
            .map(|c| c.status)
            .max()
            .unwrap_or(PreflightStatus::Pass);
        Self {
            sequence_name: sequence_name.into(),
            sequence_version,
            overall,
            checks,
            generated_at,
        }
    }

    /// True when nothing blocks execution (warnings allowed).
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(
            self.overall,
            PreflightStatus::Pass | PreflightStatus::Warning
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finding::{Confidence, FindingSeverity};
    use chrono::TimeZone;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 11, 0, 0, 0).unwrap()
    }

    #[test]
    fn status_orders_pass_warning_unknown_fail() {
        assert!(PreflightStatus::Pass < PreflightStatus::Warning);
        assert!(PreflightStatus::Warning < PreflightStatus::Unknown);
        assert!(PreflightStatus::Unknown < PreflightStatus::Fail);
    }

    #[test]
    fn status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&PreflightStatus::Unknown).unwrap(),
            "\"unknown\""
        );
    }

    #[test]
    fn overall_is_worst_check_status() {
        let report = PreflightReport::new(
            "seq",
            1,
            vec![
                PreflightCheck::pass("a", "ok"),
                PreflightCheck::with_status("b", PreflightStatus::Warning, "meh", vec![]),
                PreflightCheck::with_status("c", PreflightStatus::Unknown, "?", vec![]),
            ],
            t0(),
        );
        assert_eq!(report.overall, PreflightStatus::Unknown);
        // Unknown means NOT ready — uncertainty is not success.
        assert!(!report.is_ready());
    }

    #[test]
    fn overall_fail_dominates() {
        let report = PreflightReport::new(
            "seq",
            1,
            vec![
                PreflightCheck::pass("a", "ok"),
                PreflightCheck::with_status("b", PreflightStatus::Fail, "broken", vec![]),
            ],
            t0(),
        );
        assert_eq!(report.overall, PreflightStatus::Fail);
        assert!(!report.is_ready());
    }

    #[test]
    fn empty_checks_pass_and_warnings_stay_ready() {
        assert_eq!(
            PreflightReport::new("s", 1, vec![], t0()).overall,
            PreflightStatus::Pass
        );
        let warn = PreflightReport::new(
            "s",
            1,
            vec![PreflightCheck::with_status(
                "w",
                PreflightStatus::Warning,
                "w",
                vec![],
            )],
            t0(),
        );
        assert!(warn.is_ready());
    }

    #[test]
    fn report_round_trips_with_findings() {
        let finding = Finding::new(
            "NO_COMPATIBLE_WORKER",
            FindingSeverity::Error,
            "no worker for charge_card",
            Confidence::High,
            t0(),
        );
        let report = PreflightReport::new(
            "checkout",
            2,
            vec![PreflightCheck::with_status(
                "handlers_have_workers",
                PreflightStatus::Fail,
                "1 handler without workers",
                vec![finding],
            )],
            t0(),
        );
        let json = serde_json::to_string(&report).unwrap();
        let back: PreflightReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back, report);
    }
}
