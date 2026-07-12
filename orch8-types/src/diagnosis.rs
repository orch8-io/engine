//! Stuck-instance diagnosis: "why is this workflow not moving?"
//!
//! A diagnosis is read-only. It ranks possible explanations by how
//! directly the evidence supports them and never claims certainty while
//! multiple explanations remain possible. Recovery actions are described
//! as remediations; they are never executed from here.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::finding::Finding;

/// Whether the observed situation is normal, degraded, or self-contradictory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosisHealth {
    /// The instance is doing exactly what its definition asks (e.g. a
    /// future timer).
    Expected,
    /// Something external is unhealthy (no worker, stale claim, open
    /// breaker) — the workflow will not progress until it is fixed.
    Degraded,
    /// Stored state contradicts itself; likely requires operator repair.
    Inconsistent,
}

/// How strongly the evidence points at this explanation. Used as the
/// primary ranking key.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosisCategory {
    /// Direct evidence of the current blocker.
    DirectEvidence,
    /// A plausible cause consistent with the evidence.
    ProbableCause,
    /// A general health observation that may or may not be the blocker.
    HealthWarning,
}

/// One ranked explanation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Diagnosis {
    pub category: DiagnosisCategory,
    pub health: DiagnosisHealth,
    /// The explanation itself: code, summary, evidence, remediations,
    /// confidence.
    #[serde(flatten)]
    pub finding: Finding,
}

/// The full report for one instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InstanceDiagnosisReport {
    pub instance_id: Uuid,
    /// Instance state at diagnosis time (engine state name).
    pub state: String,
    /// Ranked: direct evidence first, then probable causes, then health
    /// warnings; higher confidence first within a category.
    pub diagnoses: Vec<Diagnosis>,
    pub generated_at: DateTime<Utc>,
}

/// Rank diagnoses in presentation order: category, then confidence
/// (descending), then code for determinism.
pub fn rank_diagnoses(diagnoses: &mut [Diagnosis]) {
    diagnoses.sort_by(|a, b| {
        a.category
            .cmp(&b.category)
            .then(b.finding.confidence.cmp(&a.finding.confidence))
            .then(a.finding.code.cmp(&b.finding.code))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finding::{Confidence, FindingSeverity};
    use chrono::TimeZone;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 11, 0, 0, 0).unwrap()
    }

    fn diag(code: &str, category: DiagnosisCategory, confidence: Confidence) -> Diagnosis {
        Diagnosis {
            category,
            health: DiagnosisHealth::Expected,
            finding: Finding::new(code, FindingSeverity::Info, "s", confidence, t0()),
        }
    }

    #[test]
    fn categories_order_direct_probable_health() {
        assert!(DiagnosisCategory::DirectEvidence < DiagnosisCategory::ProbableCause);
        assert!(DiagnosisCategory::ProbableCause < DiagnosisCategory::HealthWarning);
    }

    #[test]
    fn ranking_puts_direct_evidence_first_then_confidence() {
        let mut ds = vec![
            diag("HEALTH", DiagnosisCategory::HealthWarning, Confidence::Certain),
            diag("PROBABLE_HI", DiagnosisCategory::ProbableCause, Confidence::High),
            diag("DIRECT", DiagnosisCategory::DirectEvidence, Confidence::Medium),
            diag("PROBABLE_LO", DiagnosisCategory::ProbableCause, Confidence::Low),
        ];
        rank_diagnoses(&mut ds);
        let codes: Vec<&str> = ds.iter().map(|d| d.finding.code.as_str()).collect();
        assert_eq!(codes, vec!["DIRECT", "PROBABLE_HI", "PROBABLE_LO", "HEALTH"]);
    }

    #[test]
    fn diagnosis_serializes_finding_fields_flattened() {
        let d = diag("WAITING_UNTIL", DiagnosisCategory::DirectEvidence, Confidence::Certain);
        let v = serde_json::to_value(&d).unwrap();
        // Finding fields appear at the top level of the diagnosis object.
        assert_eq!(v["code"], "WAITING_UNTIL");
        assert_eq!(v["category"], "direct_evidence");
        assert_eq!(v["confidence"], "certain");
        let back: Diagnosis = serde_json::from_value(v).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn report_round_trips() {
        let report = InstanceDiagnosisReport {
            instance_id: Uuid::now_v7(),
            state: "waiting".into(),
            diagnoses: vec![diag("X", DiagnosisCategory::ProbableCause, Confidence::Low)],
            generated_at: t0(),
        };
        let json = serde_json::to_string(&report).unwrap();
        let back: InstanceDiagnosisReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back, report);
    }
}
