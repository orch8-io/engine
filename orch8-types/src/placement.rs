//! Placement policies: data residency, capability labels, sticky affinity,
//! priority lanes, and global rate budgets (see `docs/PLACEMENT.md`).
//!
//! Placement compiles into the existing capability-placement mechanism: the
//! resolved facts are merged into the step's [`CapsuleRequirements`]
//! (`$runtime`), so the one claim predicate
//! ([`crate::worker::claim_allowed`]) enforces them for every backend and
//! every poll shape. There is no second scheduler.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::continuity::{CapsuleRequirements, RuntimeCapabilities};
use crate::instance::Priority;

/// Visible reason recorded on an instance whose placed step has no live
/// runtime that satisfies its hard placement constraints. The step's task
/// stays `pending` (it is never handed to a non-matching runtime).
pub const PLACEMENT_UNSATISFIED: &str = "placement_unsatisfied";
/// Runtime label that carries a runtime's data-residency zone. `residency`
/// constraints match only runtimes that advertise this label explicitly.
pub const RESIDENCY_LABEL: &str = "residency";
/// Instance metadata key holding the latest placement status.
pub const PLACEMENT_METADATA_KEY: &str = "placement";
/// Maximum labels in one placement / policy / advertisement.
pub const MAX_LABELS: usize = 32;
/// Maximum length of a label key, label value, region, or residency zone.
pub const MAX_FACT_LEN: usize = 128;
/// Maximum tenant placement policies.
pub const MAX_POLICIES: usize = 256;
/// Default bounded wait for sticky affinity and soft label preferences.
pub const DEFAULT_PREFERENCE_WAIT_MS: u64 = 15_000;
/// Upper bound for `affinity_wait_ms`.
pub const MAX_PREFERENCE_WAIT_MS: u64 = 600_000;
/// Maximum length of a rate-budget key.
pub const MAX_RATE_BUDGET_KEY_LEN: usize = 128;

/// Sticky-affinity mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Affinity {
    /// Prefer the runtime that ran this instance's previous worker step.
    Instance,
    /// No preference (default).
    #[default]
    None,
}

/// Plan/tenant priority lane. Lanes map onto the engine's existing instance
/// [`Priority`], which drives claim order and cooperative preemption.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PriorityLane {
    Premium,
    Standard,
    Batch,
}

impl PriorityLane {
    /// Instance priority a lane runs at.
    #[must_use]
    pub const fn priority(self) -> Priority {
        match self {
            Self::Premium => Priority::High,
            Self::Standard => Priority::Normal,
            Self::Batch => Priority::Low,
        }
    }

    /// Lane an instance priority belongs to (for metrics labels).
    #[must_use]
    pub const fn for_priority(priority: Priority) -> Self {
        match priority {
            Priority::Critical | Priority::High => Self::Premium,
            Priority::Low => Self::Batch,
            _ => Self::Standard,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Premium => "premium",
            Self::Standard => "standard",
            Self::Batch => "batch",
        }
    }
}

impl std::str::FromStr for PriorityLane {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "premium" => Ok(Self::Premium),
            "standard" => Ok(Self::Standard),
            "batch" => Ok(Self::Batch),
            other => Err(format!(
                "unknown priority lane `{other}` (expected premium, standard, or batch)"
            )),
        }
    }
}

/// Step- or sequence-level `placement`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Placement {
    /// Only runtimes advertising this region may claim the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Every label must be advertised with exactly this value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Data-residency zone: only runtimes advertising the label
    /// `residency=<zone>` may claim. Never relaxed; the step waits
    /// (`placement_unsatisfied`) instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<String>,
    /// `instance`: prefer the runtime that ran the previous worker step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,
    /// Bounded wait for `affinity` before any eligible runtime may claim.
    /// Default 15000, maximum 600000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_wait_ms: Option<u64>,
    /// Priority lane. Sequence-level (sets the instance priority at
    /// creation); rejected on steps because priority is per instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_lane: Option<PriorityLane>,
}

fn validate_fact(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("placement.{field} must not be empty"));
    }
    if value.len() > MAX_FACT_LEN {
        return Err(format!(
            "placement.{field} must be at most {MAX_FACT_LEN} bytes"
        ));
    }
    Ok(())
}

fn validate_labels(field: &str, labels: &BTreeMap<String, String>) -> Result<(), String> {
    if labels.len() > MAX_LABELS {
        return Err(format!("{field} must have at most {MAX_LABELS} labels"));
    }
    for (key, value) in labels {
        validate_fact(&format!("{field} key"), key)?;
        validate_fact(&format!("{field}.{key}"), value)?;
    }
    Ok(())
}

impl Placement {
    /// Whether the placement constrains *where* the step may run (region,
    /// labels, residency). Such steps always go to the worker queue.
    #[must_use]
    pub fn has_hard_constraints(&self) -> bool {
        self.region.is_some() || !self.labels.is_empty() || self.residency.is_some()
    }

    /// Structural validation (bounds, non-empty facts).
    pub fn validate(&self) -> Result<(), String> {
        if let Some(region) = &self.region {
            validate_fact("region", region)?;
        }
        if let Some(residency) = &self.residency {
            validate_fact("residency", residency)?;
        }
        validate_labels("placement.labels", &self.labels)?;
        if let Some(wait) = self.affinity_wait_ms
            && (wait == 0 || wait > MAX_PREFERENCE_WAIT_MS)
        {
            return Err(format!(
                "placement.affinity_wait_ms must be within 1..={MAX_PREFERENCE_WAIT_MS}"
            ));
        }
        Ok(())
    }

    /// Combine a step placement (`self`) with the sequence-level default.
    /// Hard facts must agree (a step cannot escape the sequence's region or
    /// residency, or redefine one of its labels); `affinity` and its wait
    /// are overridden by the step.
    pub fn combine(&self, sequence: &Self) -> Result<Self, String> {
        let region = merge_fact("region", self.region.as_ref(), sequence.region.as_ref())?;
        let residency = merge_fact(
            "residency",
            self.residency.as_ref(),
            sequence.residency.as_ref(),
        )?;
        let labels = merge_labels(&self.labels, &sequence.labels)?;
        Ok(Self {
            region,
            labels,
            residency,
            affinity: self.affinity.or(sequence.affinity),
            affinity_wait_ms: self.affinity_wait_ms.or(sequence.affinity_wait_ms),
            priority_lane: self.priority_lane.or(sequence.priority_lane),
        })
    }
}

fn merge_fact(
    field: &str,
    specific: Option<&String>,
    base: Option<&String>,
) -> Result<Option<String>, String> {
    match (specific, base) {
        (Some(a), Some(b)) if a != b => Err(format!(
            "placement conflict: {field} `{a}` contradicts `{b}`"
        )),
        (Some(a), _) => Ok(Some(a.clone())),
        (None, b) => Ok(b.cloned()),
    }
}

fn merge_labels(
    specific: &BTreeMap<String, String>,
    base: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, String> {
    let mut merged = base.clone();
    for (key, value) in specific {
        if let Some(existing) = merged.get(key)
            && existing != value
        {
            return Err(format!(
                "placement conflict: label `{key}={value}` contradicts `{key}={existing}`"
            ));
        }
        merged.insert(key.clone(), value.clone());
    }
    Ok(merged)
}

/// Selector of a tenant placement policy. Every present field must match;
/// an empty selector matches every worker-dispatched step of the tenant.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PolicyMatch {
    /// Sequence name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<String>,
    /// Step handler name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    /// Instance tag: an entry of the instance's `metadata.tags` array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
}

impl PolicyMatch {
    #[must_use]
    pub fn matches(&self, target: &PlacementTarget<'_>) -> bool {
        self.sequence
            .as_deref()
            .is_none_or(|sequence| sequence == target.sequence_name)
            && self
                .handler
                .as_deref()
                .is_none_or(|handler| handler == target.handler)
            && self
                .tag
                .as_deref()
                .is_none_or(|tag| target.tags.contains(&tag))
    }
}

/// Hard constraints a matching policy adds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PolicyRequirement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<String>,
}

/// Soft preference a matching policy adds: runtimes with these labels get
/// the task first for a bounded wait, then any eligible runtime may claim.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PolicyPreference {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// One tenant placement policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PlacementPolicy {
    pub name: String,
    #[serde(rename = "match", default)]
    pub matcher: PolicyMatch,
    #[serde(default)]
    pub require: PolicyRequirement,
    #[serde(default)]
    pub prefer: Option<PolicyPreference>,
}

/// `GET|PUT /api/v1/placement/policies` body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PlacementPolicies {
    pub items: Vec<PlacementPolicy>,
}

impl PlacementPolicies {
    /// Structural validation: bounded, uniquely and non-emptily named, with
    /// well-formed facts.
    pub fn validate(&self) -> Result<(), String> {
        if self.items.len() > MAX_POLICIES {
            return Err(format!("at most {MAX_POLICIES} placement policies"));
        }
        let mut names = std::collections::BTreeSet::new();
        for policy in &self.items {
            if policy.name.trim().is_empty() || policy.name.len() > MAX_FACT_LEN {
                return Err(format!("policy name must be 1..={MAX_FACT_LEN} bytes"));
            }
            if !names.insert(policy.name.as_str()) {
                return Err(format!("duplicate policy name `{}`", policy.name));
            }
            let context = |error: String| format!("policy `{}`: {error}", policy.name);
            for (field, value) in [
                ("match.sequence", &policy.matcher.sequence),
                ("match.handler", &policy.matcher.handler),
                ("match.tag", &policy.matcher.tag),
                ("require.region", &policy.require.region),
                ("require.residency", &policy.require.residency),
            ] {
                if let Some(value) = value {
                    validate_fact(field, value).map_err(context)?;
                }
            }
            validate_labels("require.labels", &policy.require.labels).map_err(context)?;
            if let Some(prefer) = &policy.prefer {
                validate_labels("prefer.labels", &prefer.labels).map_err(context)?;
            }
        }
        Ok(())
    }
}

/// What a policy selector is evaluated against.
#[derive(Debug, Clone, Copy)]
pub struct PlacementTarget<'a> {
    pub sequence_name: &'a str,
    pub handler: &'a str,
    pub tags: &'a [&'a str],
}

/// Soft preference window compiled into [`CapsuleRequirements::prefer`].
/// Until `until_ms` (unix epoch milliseconds) only a preferred claimant may
/// take the task; afterwards any claimant satisfying the hard requirements
/// may. A preference never widens eligibility.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PlacementPreference {
    /// Sticky affinity: the worker id (== runtime id for capability polls)
    /// that ran the instance's previous worker step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Preferred labels (from tenant policies).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    pub until_ms: i64,
}

impl PlacementPreference {
    /// Whether `worker_id` / `labels` may claim under this preference at
    /// `now`.
    #[must_use]
    pub fn admits(
        &self,
        worker_id: &str,
        labels: &BTreeMap<String, String>,
        now: DateTime<Utc>,
    ) -> bool {
        if now.timestamp_millis() >= self.until_ms {
            return true;
        }
        let by_worker = self
            .worker_id
            .as_deref()
            .is_some_and(|preferred| preferred == worker_id);
        let by_labels = !self.labels.is_empty() && labels_match(&self.labels, labels);
        by_worker || by_labels || (self.worker_id.is_none() && self.labels.is_empty())
    }
}

/// Bounds for labels a runtime advertises (at most 32; keys and values
/// 1..=128 bytes).
pub fn validate_runtime_labels(labels: &BTreeMap<String, String>) -> Result<(), String> {
    validate_labels("capabilities.labels", labels)
}

/// Whether every required label is advertised with the same value.
#[must_use]
pub fn labels_match(
    required: &BTreeMap<String, String>,
    offered: &BTreeMap<String, String>,
) -> bool {
    required
        .iter()
        .all(|(key, value)| offered.get(key) == Some(value))
}

/// Effective placement of one step dispatch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedPlacement {
    pub region: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub residency: Option<String>,
    pub prefer_labels: BTreeMap<String, String>,
    pub affinity: Affinity,
    pub affinity_wait_ms: u64,
    /// Names of the tenant policies that matched (evidence).
    pub policies: Vec<String>,
}

impl ResolvedPlacement {
    #[must_use]
    pub fn has_hard_constraints(&self) -> bool {
        self.region.is_some() || !self.labels.is_empty() || self.residency.is_some()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.has_hard_constraints()
            && self.prefer_labels.is_empty()
            && self.affinity == Affinity::None
    }
}

/// Resolve step + sequence placement and the matching tenant policies.
/// Policy requirements are hard: a contradiction with the step/sequence
/// placement (or between two policies) is an error, never a silent
/// override — residency cannot be weakened by a more specific rule.
pub fn resolve(
    step: Option<&Placement>,
    sequence: Option<&Placement>,
    policies: &[PlacementPolicy],
    target: &PlacementTarget<'_>,
) -> Result<ResolvedPlacement, String> {
    let default = Placement::default();
    let combined = step
        .unwrap_or(&default)
        .combine(sequence.unwrap_or(&default))?;
    let mut region = combined.region;
    let mut residency = combined.residency;
    let mut labels = combined.labels;
    let mut prefer_labels = BTreeMap::new();
    let mut matched = Vec::new();
    for policy in policies.iter().filter(|p| p.matcher.matches(target)) {
        let context = |error: String| format!("policy `{}`: {error}", policy.name);
        region = merge_fact("region", policy.require.region.as_ref(), region.as_ref())
            .map_err(context)?;
        residency = merge_fact(
            "residency",
            policy.require.residency.as_ref(),
            residency.as_ref(),
        )
        .map_err(context)?;
        labels = merge_labels(&policy.require.labels, &labels).map_err(context)?;
        if let Some(prefer) = &policy.prefer {
            for (key, value) in &prefer.labels {
                prefer_labels
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        matched.push(policy.name.clone());
    }
    Ok(ResolvedPlacement {
        region,
        labels,
        residency,
        prefer_labels,
        affinity: combined.affinity.unwrap_or_default(),
        affinity_wait_ms: combined
            .affinity_wait_ms
            .unwrap_or(DEFAULT_PREFERENCE_WAIT_MS),
        policies: matched,
    })
}

/// Merge a resolved placement's hard facts into `$runtime` requirements.
/// `region` narrows an existing `$runtime.regions` list (conflict if the
/// region is not in it); labels and residency must agree.
pub fn apply_hard_constraints(
    placement: &ResolvedPlacement,
    requirements: &mut CapsuleRequirements,
) -> Result<(), String> {
    if let Some(region) = &placement.region {
        if !requirements.regions.is_empty() && !requirements.regions.contains(region) {
            return Err(format!(
                "placement conflict: region `{region}` is outside $runtime.regions {:?}",
                requirements.regions
            ));
        }
        requirements.regions = vec![region.clone()];
    }
    requirements.residency = merge_fact(
        "residency",
        placement.residency.as_ref(),
        requirements.residency.as_ref(),
    )?;
    requirements.labels = merge_labels(&placement.labels, &requirements.labels)?;
    Ok(())
}

/// Whether `capabilities` satisfy the hard placement facts (labels and
/// residency) of `requirements`. Regions are checked by
/// [`CapsuleRequirements::is_satisfied_by`] itself.
#[must_use]
pub fn placement_facts_satisfied(
    requirements: &CapsuleRequirements,
    capabilities: &RuntimeCapabilities,
) -> bool {
    if !labels_match(&requirements.labels, &capabilities.labels) {
        return false;
    }
    requirements
        .residency
        .as_ref()
        .is_none_or(|zone| capabilities.labels.get(RESIDENCY_LABEL) == Some(zone))
}

/// Stable `region` metric label for queued work.
#[must_use]
pub fn region_label(requirements: &CapsuleRequirements) -> String {
    if requirements.regions.is_empty() {
        "any".to_string()
    } else {
        let mut regions = requirements.regions.clone();
        regions.sort();
        regions.join(",")
    }
}

/// Pending worker-task backlog grouped for autoscaling metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDepthRow {
    pub tenant_id: String,
    pub handler_name: String,
    pub requirements: CapsuleRequirements,
    pub priority: Priority,
    pub count: u64,
}

// ---------------------------------------------------------------------------
// Global rate budgets (durable token buckets shared by every node)
// ---------------------------------------------------------------------------

/// A durable token bucket per `(tenant, key)`. Steps declaring
/// `"rate_budget": "<key>"` take one token before dispatch; with no token
/// the instance is deferred (never failed) until one refills.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RateBudget {
    pub tenant_id: String,
    pub key: String,
    /// Bucket size (maximum burst).
    pub capacity: u32,
    /// Tokens added per second.
    pub refill_per_sec: f64,
    /// Tokens currently available (as of `updated_at`).
    pub tokens: f64,
    pub updated_at: DateTime<Utc>,
}

/// Outcome of taking one token.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RateBudgetCheck {
    /// Token taken.
    Allowed,
    /// Bucket empty: retry at `retry_after`.
    Deferred { retry_after: DateTime<Utc> },
    /// No budget configured for the key: not gated.
    Unconfigured,
}

/// Validate a budget key: 1..=128 bytes of `[A-Za-z0-9._:-]`.
pub fn validate_rate_budget_key(key: &str) -> Result<(), String> {
    if key.is_empty() || key.len() > MAX_RATE_BUDGET_KEY_LEN {
        return Err(format!(
            "rate_budget key must be 1..={MAX_RATE_BUDGET_KEY_LEN} bytes"
        ));
    }
    if !key
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
    {
        return Err("rate_budget key may only contain [A-Za-z0-9._:-]".into());
    }
    Ok(())
}

/// Validate bucket parameters.
pub fn validate_rate_budget(capacity: u32, refill_per_sec: f64) -> Result<(), String> {
    if capacity == 0 || capacity > 1_000_000 {
        return Err("capacity must be within 1..=1000000".into());
    }
    if !refill_per_sec.is_finite() || refill_per_sec <= 0.0 || refill_per_sec > 1_000_000.0 {
        return Err("refill_per_sec must be finite and within (0, 1000000]".into());
    }
    Ok(())
}

/// Token-bucket step shared by every storage backend: refill for the time
/// elapsed since `updated_at`, then take one token if available. Returns the
/// new token count and the decision.
#[must_use]
pub fn take_token(
    capacity: u32,
    refill_per_sec: f64,
    tokens: f64,
    updated_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> (f64, RateBudgetCheck) {
    #[allow(clippy::cast_precision_loss)]
    let elapsed = (now - updated_at).num_milliseconds().max(0) as f64 / 1000.0;
    let available = (tokens + elapsed * refill_per_sec).min(f64::from(capacity));
    if available >= 1.0 {
        return (available - 1.0, RateBudgetCheck::Allowed);
    }
    let wait_secs = (1.0 - available) / refill_per_sec;
    #[allow(clippy::cast_possible_truncation)]
    let wait_ms = (wait_secs * 1000.0).ceil().max(1.0) as i64;
    (
        available,
        RateBudgetCheck::Deferred {
            retry_after: now + chrono::Duration::milliseconds(wait_ms),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn target<'a>(tags: &'a [&'a str]) -> PlacementTarget<'a> {
        PlacementTarget {
            sequence_name: "billing",
            handler: "charge",
            tags,
        }
    }

    #[test]
    fn contract_shape_round_trips() {
        let json = serde_json::json!({
            "region": "eu-west-1",
            "labels": {"gpu": "a100"},
            "residency": "eu",
            "affinity": "instance",
            "priority_lane": "premium"
        });
        let placement: Placement = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(placement.affinity, Some(Affinity::Instance));
        assert_eq!(placement.priority_lane, Some(PriorityLane::Premium));
        assert_eq!(serde_json::to_value(&placement).unwrap(), json);
        let policies: PlacementPolicies = serde_json::from_value(serde_json::json!({
            "items": [{"name": "eu", "match": {"sequence": "billing"},
                       "require": {"residency": "eu"}, "prefer": {"labels": {"tier": "fast"}}}]
        }))
        .unwrap();
        assert_eq!(
            policies.items[0].matcher.sequence.as_deref(),
            Some("billing")
        );
        assert!(policies.validate().is_ok());
    }

    #[test]
    fn step_cannot_escape_sequence_residency() {
        let step = Placement {
            residency: Some("us".into()),
            ..Placement::default()
        };
        let sequence = Placement {
            residency: Some("eu".into()),
            ..Placement::default()
        };
        assert!(step.combine(&sequence).unwrap_err().contains("residency"));
    }

    #[test]
    fn policies_add_hard_facts_and_preferences() {
        let policies = vec![
            PlacementPolicy {
                name: "eu-billing".into(),
                matcher: PolicyMatch {
                    sequence: Some("billing".into()),
                    ..PolicyMatch::default()
                },
                require: PolicyRequirement {
                    residency: Some("eu".into()),
                    ..PolicyRequirement::default()
                },
                prefer: Some(PolicyPreference {
                    labels: labels(&[("tier", "fast")]),
                }),
            },
            PlacementPolicy {
                name: "tagged".into(),
                matcher: PolicyMatch {
                    tag: Some("vip".into()),
                    ..PolicyMatch::default()
                },
                require: PolicyRequirement {
                    labels: labels(&[("pool", "vip")]),
                    ..PolicyRequirement::default()
                },
                prefer: None,
            },
        ];
        let resolved = resolve(None, None, &policies, &target(&[])).unwrap();
        assert_eq!(resolved.residency.as_deref(), Some("eu"));
        assert!(resolved.labels.is_empty());
        assert_eq!(resolved.prefer_labels, labels(&[("tier", "fast")]));
        assert_eq!(resolved.policies, vec!["eu-billing".to_string()]);

        let resolved = resolve(None, None, &policies, &target(&["vip"])).unwrap();
        assert_eq!(resolved.labels, labels(&[("pool", "vip")]));
    }

    #[test]
    fn policy_contradicting_step_residency_is_an_error() {
        let step = Placement {
            residency: Some("us".into()),
            ..Placement::default()
        };
        let policies = vec![PlacementPolicy {
            name: "eu".into(),
            matcher: PolicyMatch::default(),
            require: PolicyRequirement {
                residency: Some("eu".into()),
                ..PolicyRequirement::default()
            },
            prefer: None,
        }];
        let error = resolve(Some(&step), None, &policies, &target(&[])).unwrap_err();
        assert!(error.contains("policy `eu`"), "{error}");
    }

    #[test]
    fn region_narrows_runtime_regions() {
        let mut requirements = CapsuleRequirements {
            regions: vec!["eu-west-1".into(), "eu-central-1".into()],
            ..CapsuleRequirements::default()
        };
        let placement = ResolvedPlacement {
            region: Some("eu-west-1".into()),
            ..ResolvedPlacement::default()
        };
        apply_hard_constraints(&placement, &mut requirements).unwrap();
        assert_eq!(requirements.regions, vec!["eu-west-1".to_string()]);
        let outside = ResolvedPlacement {
            region: Some("us-east-1".into()),
            ..ResolvedPlacement::default()
        };
        assert!(apply_hard_constraints(&outside, &mut requirements).is_err());
    }

    #[test]
    fn preference_expires_into_fallback() {
        let now = Utc::now();
        let preference = PlacementPreference {
            worker_id: Some("w1".into()),
            labels: BTreeMap::new(),
            until_ms: (now + chrono::Duration::seconds(10)).timestamp_millis(),
        };
        assert!(preference.admits("w1", &BTreeMap::new(), now));
        assert!(!preference.admits("w2", &BTreeMap::new(), now));
        assert!(preference.admits("w2", &BTreeMap::new(), now + chrono::Duration::seconds(11)));
        let by_label = PlacementPreference {
            worker_id: None,
            labels: labels(&[("tier", "fast")]),
            until_ms: preference.until_ms,
        };
        assert!(by_label.admits("w9", &labels(&[("tier", "fast")]), now));
        assert!(!by_label.admits("w9", &labels(&[("tier", "slow")]), now));
    }

    #[test]
    fn token_bucket_refills_and_defers() {
        let t0 = Utc::now();
        let (tokens, check) = take_token(2, 1.0, 2.0, t0, t0);
        assert_eq!(check, RateBudgetCheck::Allowed);
        let (tokens, check) = take_token(2, 1.0, tokens, t0, t0);
        assert_eq!(check, RateBudgetCheck::Allowed);
        let (tokens, check) = take_token(2, 1.0, tokens, t0, t0);
        let RateBudgetCheck::Deferred { retry_after } = check else {
            panic!("expected deferral");
        };
        assert_eq!(retry_after, t0 + chrono::Duration::seconds(1));
        let later = t0 + chrono::Duration::milliseconds(1500);
        let (tokens, check) = take_token(2, 1.0, tokens, t0, later);
        assert_eq!(check, RateBudgetCheck::Allowed);
        assert!((tokens - 0.5).abs() < 1e-9);
        // Refill is capped at capacity.
        let (tokens, _) = take_token(2, 1.0, 0.0, t0, t0 + chrono::Duration::hours(1));
        assert!((tokens - 1.0).abs() < 1e-9);
    }

    #[test]
    fn lanes_map_onto_priorities() {
        assert_eq!(PriorityLane::Premium.priority(), Priority::High);
        assert_eq!(PriorityLane::Batch.priority(), Priority::Low);
        assert_eq!(
            PriorityLane::for_priority(Priority::Critical),
            PriorityLane::Premium
        );
        assert_eq!("batch".parse::<PriorityLane>(), Ok(PriorityLane::Batch));
    }

    #[test]
    fn budget_key_validation() {
        assert!(validate_rate_budget_key("stripe-api").is_ok());
        assert!(validate_rate_budget_key("a b").is_err());
        assert!(validate_rate_budget_key("").is_err());
        assert!(validate_rate_budget(0, 1.0).is_err());
        assert!(validate_rate_budget(10, f64::NAN).is_err());
    }
}
