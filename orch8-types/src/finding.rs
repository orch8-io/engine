//! Shared evidence vocabulary for operator-facing diagnostics.
//!
//! Preflight, release validation, stuck-instance diagnosis, delivery
//! inspection, and DLQ triage all report through this one model so the
//! dashboard, CLI, API responses, and SDKs stay consistent.
//!
//! A [`Finding`] is read-only: it describes what was observed and what an
//! operator *could* do about it. A [`Remediation`] is a description of a
//! separately authorized action — surfacing one here never executes it.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// How severe a finding is for the operator.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    /// Purely informational; no action expected.
    Info,
    /// Something looks off but the workflow can still run.
    Warning,
    /// The subject will not work correctly until this is addressed.
    Error,
    /// Data loss, security exposure, or production outage risk.
    Critical,
}

/// How confident the producer of a finding is in its diagnosis.
///
/// Diagnosis surfaces must never claim certainty while multiple
/// explanations remain possible — pick the honest level.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// One of several plausible explanations.
    Low,
    /// The most likely explanation, but not directly proven.
    Medium,
    /// Backed by direct evidence.
    High,
    /// Proven by construction (e.g. the timer genuinely is in the future).
    Certain,
}

/// A reference to the resource a finding is about, precise enough for the
/// dashboard/CLI to deep-link to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResourceRef {
    /// Resource kind, e.g. `sequence`, `instance`, `block`, `worker`,
    /// `credential`, `queue`, `plugin`, `webhook_endpoint`, `release`.
    pub kind: String,
    /// The resource identifier (UUID or name, whichever is canonical for
    /// the kind).
    pub id: String,
    /// Optional human-readable name when the id alone is opaque.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl ResourceRef {
    #[must_use]
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            name: None,
        }
    }

    #[must_use]
    pub fn named(kind: impl Into<String>, id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            name: Some(name.into()),
        }
    }
}

/// One piece of evidence backing a finding: a fact, where it was observed,
/// and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Evidence {
    /// Short machine-friendly label, e.g. `next_fire_at`, `worker_last_seen`.
    pub label: String,
    /// Human-readable statement of the fact.
    pub summary: String,
    /// Optional structured payload (already redacted by the producer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// When this fact was observed (not when the finding was assembled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
}

impl Evidence {
    #[must_use]
    pub fn new(label: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            summary: summary.into(),
            data: None,
            observed_at: None,
        }
    }

    #[must_use]
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    #[must_use]
    pub fn observed_at(mut self, at: DateTime<Utc>) -> Self {
        self.observed_at = Some(at);
        self
    }
}

/// A suggested, separately authorized action. Describing a remediation
/// never performs it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Remediation {
    /// Human-readable description of the action.
    pub summary: String,
    /// Optional CLI command the operator can run verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Optional API route or dashboard path for the action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    /// True when performing this action may repeat external side effects
    /// (re-running a block, redelivering a webhook). UIs must require
    /// explicit confirmation for these.
    #[serde(default)]
    pub side_effect_risk: bool,
}

impl Remediation {
    #[must_use]
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            command: None,
            link: None,
            side_effect_risk: false,
        }
    }

    #[must_use]
    pub fn with_command(mut self, command: impl Into<String>) -> Self {
        self.command = Some(command.into());
        self
    }

    #[must_use]
    pub fn with_link(mut self, link: impl Into<String>) -> Self {
        self.link = Some(link.into());
        self
    }

    #[must_use]
    pub fn with_side_effect_risk(mut self) -> Self {
        self.side_effect_risk = true;
        self
    }
}

/// One read-only diagnostic statement with its evidence and suggested
/// remediations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Finding {
    /// Stable machine code, SCREAMING_SNAKE_CASE, e.g. `NO_COMPATIBLE_WORKER`.
    pub code: String,
    pub severity: FindingSeverity,
    /// One-sentence human-readable statement.
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<Evidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affected_resource: Option<ResourceRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remediation: Vec<Remediation>,
    pub confidence: Confidence,
    pub observed_at: DateTime<Utc>,
}

impl Finding {
    /// Start building a finding. `observed_at` must come from the caller's
    /// clock so virtual-time consumers (contract tests, replay) stay
    /// deterministic.
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        severity: FindingSeverity,
        summary: impl Into<String>,
        confidence: Confidence,
        observed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            code: code.into(),
            severity,
            summary: summary.into(),
            evidence: Vec::new(),
            affected_resource: None,
            remediation: Vec::new(),
            confidence,
            observed_at,
        }
    }

    #[must_use]
    pub fn with_evidence(mut self, evidence: Evidence) -> Self {
        self.evidence.push(evidence);
        self
    }

    #[must_use]
    pub fn with_resource(mut self, resource: ResourceRef) -> Self {
        self.affected_resource = Some(resource);
        self
    }

    #[must_use]
    pub fn with_remediation(mut self, remediation: Remediation) -> Self {
        self.remediation.push(remediation);
        self
    }
}

/// Sort findings for presentation: most severe first, then highest
/// confidence, then stable by code so equal findings render in a
/// deterministic order.
pub fn rank_findings(findings: &mut [Finding]) {
    findings.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then(b.confidence.cmp(&a.confidence))
            .then(a.code.cmp(&b.code))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap()
    }

    #[test]
    fn severity_orders_from_info_to_critical() {
        assert!(FindingSeverity::Info < FindingSeverity::Warning);
        assert!(FindingSeverity::Warning < FindingSeverity::Error);
        assert!(FindingSeverity::Error < FindingSeverity::Critical);
    }

    #[test]
    fn confidence_orders_from_low_to_certain() {
        assert!(Confidence::Low < Confidence::Medium);
        assert!(Confidence::Medium < Confidence::High);
        assert!(Confidence::High < Confidence::Certain);
    }

    #[test]
    fn severity_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&FindingSeverity::Warning).unwrap(),
            "\"warning\""
        );
        assert_eq!(
            serde_json::to_string(&FindingSeverity::Critical).unwrap(),
            "\"critical\""
        );
    }

    #[test]
    fn confidence_serializes_snake_case() {
        assert_eq!(serde_json::to_string(&Confidence::High).unwrap(), "\"high\"");
    }

    #[test]
    fn finding_builder_assembles_all_fields() {
        let f = Finding::new(
            "NO_COMPATIBLE_WORKER",
            FindingSeverity::Error,
            "handler charge_card has no compatible worker",
            Confidence::High,
            t0(),
        )
        .with_resource(ResourceRef::named("handler", "charge_card", "Charge card"))
        .with_evidence(
            Evidence::new("last_worker_seen", "worker w-1 last seen 2h ago")
                .with_data(serde_json::json!({"worker_id": "w-1"}))
                .observed_at(t0()),
        )
        .with_remediation(
            Remediation::new("upgrade worker w-1 or relax the version pin")
                .with_command("orch8 worker pins set charge_card --min-version 1.2.0"),
        );

        assert_eq!(f.code, "NO_COMPATIBLE_WORKER");
        assert_eq!(f.severity, FindingSeverity::Error);
        assert_eq!(f.evidence.len(), 1);
        assert_eq!(f.remediation.len(), 1);
        let res = f.affected_resource.as_ref().unwrap();
        assert_eq!(res.kind, "handler");
        assert_eq!(res.name.as_deref(), Some("Charge card"));
        assert!(!f.remediation[0].side_effect_risk);
    }

    #[test]
    fn finding_round_trips_through_json() {
        let f = Finding::new(
            "WAITING_UNTIL",
            FindingSeverity::Info,
            "instance waits for a future timer",
            Confidence::Certain,
            t0(),
        )
        .with_evidence(Evidence::new("next_fire_at", "fires at 2026-07-12T00:00:00Z"));

        let json = serde_json::to_string(&f).unwrap();
        let back: Finding = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f);
    }

    #[test]
    fn empty_collections_are_omitted_from_json() {
        let f = Finding::new(
            "X",
            FindingSeverity::Info,
            "s",
            Confidence::Low,
            t0(),
        );
        let v = serde_json::to_value(&f).unwrap();
        let obj = v.as_object().unwrap();
        assert!(!obj.contains_key("evidence"));
        assert!(!obj.contains_key("remediation"));
        assert!(!obj.contains_key("affected_resource"));
    }

    #[test]
    fn missing_collections_deserialize_to_empty() {
        let f: Finding = serde_json::from_value(serde_json::json!({
            "code": "X",
            "severity": "info",
            "summary": "s",
            "confidence": "low",
            "observed_at": "2026-07-11T12:00:00Z",
        }))
        .unwrap();
        assert!(f.evidence.is_empty());
        assert!(f.remediation.is_empty());
        assert!(f.affected_resource.is_none());
    }

    #[test]
    fn remediation_side_effect_risk_defaults_false_and_round_trips() {
        let r: Remediation =
            serde_json::from_value(serde_json::json!({"summary": "retry the block"})).unwrap();
        assert!(!r.side_effect_risk);

        let risky = Remediation::new("re-run the HTTP block").with_side_effect_risk();
        let v = serde_json::to_value(&risky).unwrap();
        assert_eq!(v["side_effect_risk"], serde_json::json!(true));
    }

    #[test]
    fn rank_findings_orders_by_severity_then_confidence_then_code() {
        let mk = |code: &str, sev, conf| Finding::new(code, sev, "s", conf, t0());
        let mut findings = vec![
            mk("B_INFO", FindingSeverity::Info, Confidence::Certain),
            mk("A_ERR_LOW", FindingSeverity::Error, Confidence::Low),
            mk("C_CRIT", FindingSeverity::Critical, Confidence::Low),
            mk("A_ERR_HIGH", FindingSeverity::Error, Confidence::High),
            mk("A_ERR_HIGH2", FindingSeverity::Error, Confidence::High),
        ];
        rank_findings(&mut findings);
        let codes: Vec<&str> = findings.iter().map(|f| f.code.as_str()).collect();
        assert_eq!(
            codes,
            vec!["C_CRIT", "A_ERR_HIGH", "A_ERR_HIGH2", "A_ERR_LOW", "B_INFO"]
        );
    }

    #[test]
    fn rank_findings_is_deterministic_for_equal_rank() {
        let mk = |code: &str| {
            Finding::new(code, FindingSeverity::Warning, "s", Confidence::Medium, t0())
        };
        let mut a = vec![mk("Z"), mk("A"), mk("M")];
        let mut b = vec![mk("M"), mk("Z"), mk("A")];
        rank_findings(&mut a);
        rank_findings(&mut b);
        let ca: Vec<&str> = a.iter().map(|f| f.code.as_str()).collect();
        let cb: Vec<&str> = b.iter().map(|f| f.code.as_str()).collect();
        assert_eq!(ca, cb);
        assert_eq!(ca, vec!["A", "M", "Z"]);
    }
}
