use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{BlockId, InstanceId};

/// Reserved step-param key containing capability/location requirements for an
/// external worker. The scheduler removes this key before exposing params to
/// the handler and persists the parsed value on the durable task offer.
pub const RUNTIME_REQUIREMENTS_PARAM: &str = "$runtime";

/// Split routing requirements from handler input without widening every step
/// definition. `params.$runtime` uses the `CapsuleRequirements` wire shape.
pub fn take_runtime_requirements(
    mut params: serde_json::Value,
) -> Result<(crate::continuity::CapsuleRequirements, serde_json::Value), String> {
    let Some(object) = params.as_object_mut() else {
        return Ok((crate::continuity::CapsuleRequirements::default(), params));
    };
    let Some(raw) = object.remove(RUNTIME_REQUIREMENTS_PARAM) else {
        return Ok((crate::continuity::CapsuleRequirements::default(), params));
    };
    let requirements = serde_json::from_value(raw)
        .map_err(|error| format!("invalid {RUNTIME_REQUIREMENTS_PARAM} requirements: {error}"))?;
    Ok((requirements, params))
}

/// Parse `params.$runtime` without removing it (dispatch routing decision).
pub fn peek_runtime_requirements(
    params: &serde_json::Value,
) -> Result<crate::continuity::CapsuleRequirements, String> {
    match params.get(RUNTIME_REQUIREMENTS_PARAM) {
        None => Ok(crate::continuity::CapsuleRequirements::default()),
        Some(raw) => serde_json::from_value(raw.clone())
            .map_err(|error| format!("invalid {RUNTIME_REQUIREMENTS_PARAM} requirements: {error}")),
    }
}

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
    fn runtime_requirements_are_removed_from_handler_params() {
        let input = serde_json::json!({
            "$runtime": {
                "hardware": ["cuda"],
                "regions": ["norway"],
                "requires_network": true
            },
            "report_id": "q3"
        });

        let (requirements, params) = take_runtime_requirements(input).unwrap();

        assert_eq!(requirements.hardware, ["cuda"]);
        assert_eq!(requirements.regions, ["norway"]);
        assert!(requirements.requires_network);
        assert_eq!(params, serde_json::json!({"report_id": "q3"}));
    }

    #[test]
    fn absent_runtime_requirements_leave_params_unchanged() {
        let input = serde_json::json!({"report_id": "q3"});

        let (requirements, params) = take_runtime_requirements(input.clone()).unwrap();

        assert_eq!(
            requirements,
            crate::continuity::CapsuleRequirements::default()
        );
        assert_eq!(params, input);
    }

    #[test]
    fn non_object_params_have_default_runtime_requirements() {
        let input = serde_json::json!(["one", "two"]);

        let (requirements, params) = take_runtime_requirements(input.clone()).unwrap();

        assert_eq!(
            requirements,
            crate::continuity::CapsuleRequirements::default()
        );
        assert_eq!(params, input);
    }

    #[test]
    fn malformed_runtime_requirements_are_rejected() {
        let error = take_runtime_requirements(serde_json::json!({
            "$runtime": {"requires_network": "yes"}
        }))
        .unwrap_err();

        assert!(error.contains("invalid $runtime requirements"));
        assert!(error.contains("boolean"));
    }

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
    /// Capability, locality, and trust facts a runtime must satisfy before it
    /// can atomically claim this task.
    #[serde(default, skip_serializing_if = "is_default_requirements")]
    pub requirements: crate::continuity::CapsuleRequirements,
    pub params: serde_json::Value,
    /// Serialized `ExecutionContext` — kept as raw JSON to avoid coupling workers to Rust types.
    pub context: serde_json::Value,
    pub attempt: u16,
    pub timeout_ms: Option<i64>,
    pub state: WorkerTaskState,
    pub worker_id: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    /// Monotonic ownership generation. Every successful claim increments this
    /// value. Workers must echo it on every lease mutation so a process from an
    /// older claim cannot act after the task has been reclaimed, even when the
    /// same stable `worker_id` is reused.
    #[serde(default)]
    pub claim_epoch: u64,
    /// Latest durable progress snapshot supplied by the activity worker.
    /// A replacement worker receives this value when it claims the task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_checkpoint: Option<serde_json::Value>,
    /// Monotonic compare-and-swap version for `resume_checkpoint`.
    #[serde(default)]
    pub checkpoint_seq: u64,
    pub completed_at: Option<DateTime<Utc>>,
    pub output: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub error_retryable: Option<bool>,
    pub created_at: DateTime<Utc>,
    /// Deterministic idempotency key of this attempt's effect receipt, fixed
    /// at dispatch. Nodes pass it to handlers and downstream APIs as an
    /// idempotency key; the server settles the receipt by this stored id
    /// (never a recomputed one, which would miss after an ownership handoff).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_id: Option<crate::continuity::EffectId>,
    /// Continuity ownership epoch of the instance when the task was
    /// dispatched. Lease mutations are fenced on it: a task dispatched under
    /// an older owner cannot complete after a handoff. `None` = the instance
    /// was not enrolled in continuity at dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_epoch: Option<u64>,
    /// Lease (seconds between heartbeats) for the current claim, chosen from
    /// the claimant's runtime kind. `None` = the server-wide default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_secs: Option<u32>,
    /// The step's params referenced `credentials://` material. Such a task is
    /// never claimable by a `browser` runtime.
    #[serde(default, skip_serializing_if = "is_false")]
    pub carries_credentials: bool,
    /// Runtime kind of the current claimant (from its capability
    /// advertisement), recorded for lease sizing, output bounds, and
    /// provenance. `None` for legacy capability-less claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_runtime_kind: Option<crate::continuity::RuntimeKind>,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde skip predicate requires `&T`.
const fn is_false(value: &bool) -> bool {
    !*value
}

impl WorkerTask {
    /// Whether a runtime advertising `capabilities` may claim this task:
    /// its placement requirements are satisfied, and a browser never
    /// receives a task whose params carried credentials.
    #[must_use]
    pub fn claimable_by(
        &self,
        capabilities: &crate::continuity::RuntimeCapabilities,
        now: DateTime<Utc>,
    ) -> bool {
        claim_allowed(
            &self.requirements,
            self.carries_credentials,
            capabilities,
            now,
        )
    }
}

/// Claim predicate shared by every storage backend (see
/// [`WorkerTask::claimable_by`]).
#[must_use]
pub fn claim_allowed(
    requirements: &crate::continuity::CapsuleRequirements,
    carries_credentials: bool,
    capabilities: &crate::continuity::RuntimeCapabilities,
    now: DateTime<Utc>,
) -> bool {
    if carries_credentials && capabilities.kind == crate::continuity::RuntimeKind::Browser {
        return false;
    }
    requirements.is_satisfied_by(capabilities, now)
}

/// Whether any string in `value` is a `credentials://` reference.
#[must_use]
pub fn contains_credential_reference(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => text.contains("credentials://"),
        serde_json::Value::Array(items) => items.iter().any(contains_credential_reference),
        serde_json::Value::Object(map) => map.values().any(contains_credential_reference),
        _ => false,
    }
}

/// Context delivered to a `browser` claimant. A browser never receives
/// secrets: the read-only `config` section and the audit trail (both routinely
/// carry endpoints, keys, and operator data) are dropped, any top-level `data`
/// entry holding a `credentials://` reference is removed, and the platform
/// [`crate::redaction::RedactionPolicy`] is applied to what remains
/// (secret-shaped keys/values and URL-embedded credentials are redacted).
#[must_use]
pub fn browser_safe_context(context: &serde_json::Value) -> serde_json::Value {
    let policy = crate::redaction::RedactionPolicy::default();
    let Some(object) = context.as_object() else {
        return serde_json::Value::Object(serde_json::Map::new());
    };
    let mut safe = serde_json::Map::new();
    if let Some(data) = object.get("data") {
        let mut data = data.clone();
        if let Some(entries) = data.as_object_mut() {
            entries.retain(|_, value| !contains_credential_reference(value));
        }
        policy.redact_value(&mut data);
        safe.insert("data".into(), data);
    }
    if let Some(runtime) = object.get("runtime") {
        let mut runtime = runtime.clone();
        policy.redact_value(&mut runtime);
        safe.insert("runtime".into(), runtime);
    }
    serde_json::Value::Object(safe)
}

fn is_default_requirements(value: &crate::continuity::CapsuleRequirements) -> bool {
    value == &crate::continuity::CapsuleRequirements::default()
}

/// Proof that a caller owns one specific claim generation of a worker task.
/// A stable worker identifier alone is insufficient because a restarted
/// process may reuse it while an older process is still running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerClaim {
    pub worker_id: String,
    pub claim_epoch: u64,
}

impl WorkerClaim {
    #[must_use]
    pub fn new(worker_id: impl Into<String>, claim_epoch: u64) -> Self {
        Self {
            worker_id: worker_id.into(),
            claim_epoch,
        }
    }
}

/// Durable reason for a worker-task attempt transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerAttemptEventKind {
    Claimed,
    Reclaimed,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    StaleMutationRejected,
}

impl WorkerAttemptEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Reclaimed => "reclaimed",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::StaleMutationRejected => "stale_mutation_rejected",
        }
    }
}

impl std::str::FromStr for WorkerAttemptEventKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "claimed" => Ok(Self::Claimed),
            "reclaimed" => Ok(Self::Reclaimed),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "timed_out" => Ok(Self::TimedOut),
            "cancelled" => Ok(Self::Cancelled),
            "stale_mutation_rejected" => Ok(Self::StaleMutationRejected),
            other => Err(format!("unknown worker attempt event kind: {other}")),
        }
    }
}

#[cfg(test)]
mod attempt_tests {
    use super::*;

    #[test]
    fn event_kind_has_stable_storage_roundtrip() {
        for kind in [
            WorkerAttemptEventKind::Claimed,
            WorkerAttemptEventKind::Reclaimed,
            WorkerAttemptEventKind::Completed,
            WorkerAttemptEventKind::Failed,
            WorkerAttemptEventKind::TimedOut,
            WorkerAttemptEventKind::Cancelled,
            WorkerAttemptEventKind::StaleMutationRejected,
        ] {
            assert_eq!(
                kind.as_str().parse::<WorkerAttemptEventKind>().unwrap(),
                kind
            );
        }
        assert!("unknown".parse::<WorkerAttemptEventKind>().is_err());
    }

    #[test]
    fn claim_serializes_as_protocol_contract() {
        let claim = WorkerClaim::new("worker-a", 7);
        assert_eq!(
            serde_json::to_value(claim).unwrap(),
            serde_json::json!({
                "worker_id": "worker-a", "claim_epoch": 7
            })
        );
    }
}

/// What happens to an instance when a worker task is resolved without a
/// worker-reported outcome (lease expiry, timeout, voluntary release).
#[derive(Debug, Clone)]
pub enum WorkerTaskResolutionAction {
    /// Give the task back: state `pending`, lease cleared, receipt untouched.
    /// Only valid when no side effect can have started (pure step, or a
    /// release before the handler started).
    Requeue,
    /// Replace the task with `retry_task` (next attempt), reset the tree node
    /// to `pending` (when present), and schedule the instance at `fire_at`.
    Retry {
        retry_task: Box<WorkerTask>,
        node_id: Option<crate::ids::ExecutionNodeId>,
        fire_at: DateTime<Utc>,
    },
    /// Mark the task failed, fail the tree node, and schedule the instance so
    /// the evaluator propagates the failure.
    FailNode {
        node_id: Option<crate::ids::ExecutionNodeId>,
        fire_at: DateTime<Utc>,
    },
    /// Flat (tree-less) execution: mark the task and the instance failed.
    FailInstance,
    /// Mark only the task failed; the caller integrates the outcome into the
    /// instance itself (device-mesh delegation results).
    FailTaskOnly,
}

/// One fenced, atomic worker-task resolution. Storage applies `action` only
/// when the task is still in `expected_state` at `expected_claim_epoch` (and,
/// when set, still held by `expected_worker_id`); otherwise nothing changes
/// and the call returns `false`, so a racing completion always wins cleanly.
#[derive(Debug, Clone)]
pub struct WorkerTaskResolution {
    pub task_id: Uuid,
    pub instance_id: InstanceId,
    pub expected_state: WorkerTaskState,
    pub expected_claim_epoch: u64,
    pub expected_worker_id: Option<String>,
    pub event: WorkerAttemptEventKind,
    pub reason: String,
    pub retryable: bool,
    pub action: WorkerTaskResolutionAction,
}

/// Append-only evidence for one transition of one worker claim generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerTaskAttemptEvent {
    pub id: Uuid,
    pub task_id: Uuid,
    pub claim_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    pub event: WorkerAttemptEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
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
    /// Tenant that owns the target worker. A tenant-scoped worker session
    /// only receives (and can only ack) commands of its own tenant, since
    /// `worker_id` is client-chosen. Empty = deliverable only to unscoped
    /// (root / insecure-mode) sessions.
    #[serde(default)]
    pub tenant_id: String,
    /// `drain` (stop claiming new tasks, finish in-flight), `reload`
    /// (re-read config / re-register handlers), `ping` (liveness probe), or
    /// `place` (accept a placement payload for runtime-local execution).
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
    Place,
}

impl std::fmt::Display for WorkerCommandKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Drain => f.write_str("drain"),
            Self::Reload => f.write_str("reload"),
            Self::Ping => f.write_str("ping"),
            Self::Place => f.write_str("place"),
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
            "place" => Ok(Self::Place),
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

    #[test]
    fn numeric_compare_beats_lexical_on_padded_minor() {
        // String compare would rank "1.9.5" >= "1.10" (since '9' > '1'); numeric
        // padding must correctly rank 1.9.5 < 1.10.
        assert!(!version_satisfies(Some("1.9.5"), "1.10"));
        assert!(version_satisfies(Some("1.10"), "1.9.5"));
    }

    #[test]
    fn mixed_numeric_and_non_numeric_uses_lexical() {
        // One side parses, the other does not → lexical `>=` over the raw strings
        // (so the numeric ordering is NOT honored — "9.0" beats "10.x" lexically).
        assert!(version_satisfies(Some("9.0"), "10.x"));
        assert!(!version_satisfies(Some("abc"), "abd"));
        assert!(version_satisfies(Some("abd"), "abc"));
    }

    #[test]
    fn empty_and_malformed_components_fall_back() {
        // Empty worker string parses to None → lexical. Internal empty components
        // ("1..2", "1.2.") fail u64 parse → lexical, never panic.
        assert!(version_satisfies(Some(""), "")); // "" >= ""
        assert!(!version_satisfies(Some(""), "1.0.0")); // "" < "1.0.0"
        assert!(!version_satisfies(Some("1..2"), "1.9.9")); // lexical: "1..2" < "1.9.9"
        assert!(!version_satisfies(Some("-1"), "0")); // negative fails parse → "-1" < "0"
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

#[cfg(test)]
mod distribution_tests {
    use super::*;
    use crate::continuity::{
        CapsuleRequirements, RuntimeCapabilities, RuntimeConnectivity, RuntimeId, RuntimeKind,
        RuntimeTrustLevel,
    };

    fn caps(kind: RuntimeKind) -> RuntimeCapabilities {
        let now = Utc::now();
        RuntimeCapabilities {
            runtime_id: RuntimeId::new(),
            kind,
            trust: RuntimeTrustLevel::Registered,
            handlers: vec!["h".into()],
            plugins: Vec::new(),
            credentials: Vec::new(),
            regions: Vec::new(),
            hardware: Vec::new(),
            offline_capable: false,
            connectivity: Some(RuntimeConnectivity::Wifi),
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            observed_at: now,
            expires_at: now + chrono::Duration::minutes(4),
        }
    }

    #[test]
    fn browser_never_claims_credential_bearing_tasks() {
        let now = Utc::now();
        let requirements = CapsuleRequirements::default();
        assert!(!claim_allowed(
            &requirements,
            true,
            &caps(RuntimeKind::Browser),
            now
        ));
        assert!(claim_allowed(
            &requirements,
            false,
            &caps(RuntimeKind::Browser),
            now
        ));
        assert!(claim_allowed(
            &requirements,
            true,
            &caps(RuntimeKind::Mobile),
            now
        ));
        assert!(claim_allowed(
            &requirements,
            true,
            &caps(RuntimeKind::Server),
            now
        ));
    }

    #[test]
    fn credential_references_are_detected_anywhere() {
        assert!(contains_credential_reference(&serde_json::json!({
            "a": [{"auth": "credentials://stripe/key"}]
        })));
        assert!(!contains_credential_reference(
            &serde_json::json!({"a": "plain"})
        ));
    }

    #[test]
    fn browser_context_drops_secret_sections_and_redacts() {
        let context = serde_json::json!({
            "data": {
                "order": {"id": 7, "api_key": "k"},
                "token_ref": "credentials://vault/token",
                "note": "Bearer abc.def",
                "db": "postgres://user:hunter2@db/app"
            },
            "config": {"stripe_key": "sk_live_x"},
            "audit": [{"event": "x"}],
            "runtime": {"current_step": "s1"}
        });
        let safe = browser_safe_context(&context);
        assert!(safe.get("config").is_none());
        assert!(safe.get("audit").is_none());
        assert!(safe["data"].get("token_ref").is_none());
        assert_eq!(safe["data"]["order"]["id"], 7);
        assert_ne!(safe["data"]["order"]["api_key"], "k");
        assert_ne!(safe["data"]["note"], "Bearer abc.def");
        assert!(!safe["data"]["db"].as_str().unwrap().contains("hunter2"));
        assert_eq!(safe["runtime"]["current_step"], "s1");
    }

    #[test]
    fn distribution_fields_are_optional_on_the_wire() {
        let json = serde_json::json!({
            "id": uuid::Uuid::now_v7(),
            "instance_id": uuid::Uuid::now_v7(),
            "block_id": "b",
            "handler_name": "h",
            "params": {},
            "context": {},
            "attempt": 0,
            "timeout_ms": null,
            "state": "pending",
            "worker_id": null,
            "claimed_at": null,
            "heartbeat_at": null,
            "completed_at": null,
            "output": null,
            "error_message": null,
            "error_retryable": null,
            "created_at": Utc::now(),
        });
        let task: WorkerTask = serde_json::from_value(json).unwrap();
        assert!(task.effect_id.is_none());
        assert!(!task.carries_credentials);
        let back = serde_json::to_value(&task).unwrap();
        assert!(back.get("effect_id").is_none());
        assert!(back.get("carries_credentials").is_none());
    }
}
