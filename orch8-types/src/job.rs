//! Background jobs: fire-and-forget handler invocations without authoring a
//! sequence.
//!
//! A job is a thin facade over the existing instance machinery. Enqueueing a
//! job ensures an auto-managed single-step *system sequence* exists for the
//! `(tenant, namespace, handler, queue, retry)` combination and creates one
//! ordinary [`crate::instance::TaskInstance`] against it. The job id **is**
//! the instance id, so retries, the DLQ, pull/push worker dispatch,
//! idempotency keys, tenant enforcement, entitlements and every instance view
//! in the dashboard apply unchanged.
//!
//! The job's bookkeeping lives under the reserved `_job` key of the
//! instance's `metadata` (see [`JOB_METADATA_KEY`]) so jobs can be listed with
//! the indexed metadata-containment filter.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

use crate::instance::{InstanceState, Priority};

/// Prefix of every auto-managed job sequence name.
pub const JOB_SEQUENCE_PREFIX: &str = "_job.";
/// Block id of the single step inside a job sequence.
pub const JOB_BLOCK_ID: &str = "run";
/// Reserved top-level metadata key holding the job's bookkeeping object.
pub const JOB_METADATA_KEY: &str = "_job";
/// Prefix applied to caller-supplied idempotency keys so job keys never
/// collide with instance keys created through `POST /instances`.
pub const JOB_IDEMPOTENCY_PREFIX: &str = "job:";
/// Maximum accepted handler-name length.
pub const MAX_HANDLER_LEN: usize = 200;
/// Maximum accepted queue-name length.
pub const MAX_QUEUE_LEN: usize = 200;
/// Upper bound on `retry.max_attempts` (total attempts).
pub const MAX_JOB_ATTEMPTS: u32 = 1000;

/// Retry policy for a job.
///
/// `max_attempts` counts **total** executions including the first one
/// (BullMQ/Sidekiq semantics): `1` means "never retry", `3` means "run at
/// most three times". Backoff is exponential (x2) starting at
/// `initial_backoff_ms`, capped at `max_backoff_ms` (default 60 000).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobRetry {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_backoff_ms: Option<u64>,
}

impl JobRetry {
    /// Validate bounds. Returns a human-readable error message on failure.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_attempts == 0 {
            return Err("retry.max_attempts must be >= 1".into());
        }
        if self.max_attempts > MAX_JOB_ATTEMPTS {
            return Err(format!("retry.max_attempts must be <= {MAX_JOB_ATTEMPTS}"));
        }
        if let Some(max) = self.max_backoff_ms
            && max < self.initial_backoff_ms
        {
            return Err("retry.max_backoff_ms must be >= retry.initial_backoff_ms".into());
        }
        Ok(())
    }

    /// Whether this policy ever re-runs a failed attempt.
    #[must_use]
    pub const fn retries(&self) -> bool {
        self.max_attempts > 1
    }
}

/// Body of `POST /jobs`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EnqueueJobRequest {
    /// Handler name. Built-in handlers run in-process; any other name is
    /// dispatched to external workers (pull via `/workers/tasks/poll` or push
    /// via a queue dispatch config).
    pub handler: String,
    /// JSON object passed to the handler as its `params` (and stored as the
    /// instance's `context.data`).
    #[serde(default = "empty_object")]
    pub payload: serde_json::Value,
    /// Named worker queue. Omitted = default queue.
    #[serde(default)]
    pub queue: Option<String>,
    /// Priority level name: `low`, `normal` (default), `high`, `critical`
    /// (case-insensitive).
    #[serde(default, deserialize_with = "deserialize_priority_opt")]
    #[schema(value_type = Option<String>)]
    pub priority: Option<Priority>,
    #[serde(default)]
    pub retry: Option<JobRetry>,
    /// Run no earlier than `now + delay_ms`. Mutually exclusive with `run_at`.
    #[serde(default)]
    pub delay_ms: Option<u64>,
    /// Run no earlier than this instant. Mutually exclusive with `delay_ms`.
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    /// Tenant-scoped idempotency key. Re-posting the same key returns the
    /// existing job with HTTP 200 instead of creating a duplicate.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Free-form caller metadata (JSON object) stored on the instance.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    /// Optional tenant (the `X-Tenant-Id` header is authoritative when set).
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Optional namespace. Default `default`.
    #[serde(default)]
    pub namespace: Option<String>,
}

fn empty_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Parse a priority level name case-insensitively.
#[must_use]
pub fn parse_priority(s: &str) -> Option<Priority> {
    match s.to_ascii_lowercase().as_str() {
        "low" => Some(Priority::Low),
        "normal" => Some(Priority::Normal),
        "high" => Some(Priority::High),
        "critical" => Some(Priority::Critical),
        _ => None,
    }
}

fn deserialize_priority_opt<'de, D>(deserializer: D) -> Result<Option<Priority>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    raw.map(|s| {
        parse_priority(&s).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown priority '{s}' (expected low, normal, high or critical)"
            ))
        })
    })
    .transpose()
}

impl EnqueueJobRequest {
    /// Validate request-level invariants that do not need storage.
    pub fn validate(&self) -> Result<(), String> {
        let handler = self.handler.trim();
        if handler.is_empty() {
            return Err("handler must not be empty".into());
        }
        if handler.len() > MAX_HANDLER_LEN {
            return Err(format!("handler must be at most {MAX_HANDLER_LEN} bytes"));
        }
        if handler != self.handler || handler.chars().any(char::is_control) {
            return Err(
                "handler must not contain surrounding whitespace or control characters".into(),
            );
        }
        if let Some(queue) = &self.queue {
            if queue.trim().is_empty() {
                return Err("queue must not be empty when provided".into());
            }
            if queue.len() > MAX_QUEUE_LEN {
                return Err(format!("queue must be at most {MAX_QUEUE_LEN} bytes"));
            }
        }
        if !self.payload.is_object() {
            return Err("payload must be a JSON object".into());
        }
        if let Some(meta) = &self.metadata
            && !meta.is_object()
            && !meta.is_null()
        {
            return Err("metadata must be a JSON object".into());
        }
        if self.delay_ms.is_some() && self.run_at.is_some() {
            return Err("delay_ms and run_at are mutually exclusive".into());
        }
        if let Some(key) = &self.idempotency_key
            && key.is_empty()
        {
            return Err("idempotency_key must not be empty when provided".into());
        }
        if let Some(retry) = &self.retry {
            retry.validate()?;
        }
        Ok(())
    }

    /// When the job should first become runnable.
    #[must_use]
    pub fn first_run_at(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        if let Some(at) = self.run_at {
            return at;
        }
        match self.delay_ms {
            Some(ms) => {
                let ms = i64::try_from(ms).unwrap_or(i64::MAX);
                now.checked_add_signed(chrono::Duration::milliseconds(ms))
                    .unwrap_or(DateTime::<Utc>::MAX_UTC)
            }
            None => now,
        }
    }
}

/// Externally visible job status, derived from the instance state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Waiting for its run time, a retry backoff, or a free worker.
    Scheduled,
    /// Currently executing (in-process or claimed by an external worker).
    Running,
    Completed,
    /// Failed with no retry policy (single attempt).
    Failed,
    Cancelled,
    /// Failed although a retry policy was configured (attempts exhausted or
    /// the error was non-retryable). Retry it via `POST /instances/{id}/retry`
    /// or the DLQ group endpoints.
    DeadLettered,
}

impl JobStatus {
    /// Parse the wire name.
    #[must_use]
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s {
            "scheduled" => Some(Self::Scheduled),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "dead_lettered" => Some(Self::DeadLettered),
            _ => None,
        }
    }

    /// Derive the job status from an instance state plus whether the job had
    /// a retry policy.
    #[must_use]
    pub const fn from_instance_state(state: InstanceState, retries: bool) -> Self {
        match state {
            InstanceState::Scheduled | InstanceState::Paused => Self::Scheduled,
            InstanceState::Running | InstanceState::Waiting => Self::Running,
            InstanceState::Completed => Self::Completed,
            InstanceState::Failed if retries => Self::DeadLettered,
            InstanceState::Failed => Self::Failed,
            InstanceState::Cancelled => Self::Cancelled,
        }
    }

    /// Instance states that map onto this status.
    #[must_use]
    pub fn instance_states(self) -> Vec<InstanceState> {
        match self {
            Self::Scheduled => vec![InstanceState::Scheduled, InstanceState::Paused],
            Self::Running => vec![InstanceState::Running, InstanceState::Waiting],
            Self::Completed => vec![InstanceState::Completed],
            Self::Failed | Self::DeadLettered => vec![InstanceState::Failed],
            Self::Cancelled => vec![InstanceState::Cancelled],
        }
    }
}

/// Job representation returned by every `/jobs` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Job {
    /// Job id (identical to `instance_id`).
    pub id: uuid::Uuid,
    /// Backing instance id — usable with every `/instances/{id}` endpoint.
    pub instance_id: uuid::Uuid,
    pub handler: String,
    pub status: JobStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    pub created_at: DateTime<Utc>,
    /// When the job is (or was first) due to run.
    pub run_at: DateTime<Utc>,
    /// Executions started so far (0 before the first run).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    /// Handler output once completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    /// Last error message when failed / dead-lettered (or while retrying).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `GET /jobs` response page (keyset-paginated, newest first).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobPage {
    pub items: Vec<Job>,
    /// Opaque cursor for the next (older) page; absent on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(v: serde_json::Value) -> EnqueueJobRequest {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn minimal_request_defaults() {
        let r = req(json!({"handler": "send_email"}));
        assert_eq!(r.payload, json!({}));
        assert!(r.priority.is_none());
        assert!(r.validate().is_ok());
    }

    #[test]
    fn priority_is_case_insensitive() {
        let r = req(json!({"handler": "h", "priority": "HIGH"}));
        assert_eq!(r.priority, Some(Priority::High));
        let r = req(json!({"handler": "h", "priority": "critical"}));
        assert_eq!(r.priority, Some(Priority::Critical));
        assert!(
            serde_json::from_value::<EnqueueJobRequest>(
                json!({"handler": "h", "priority": "urgent"})
            )
            .is_err()
        );
    }

    #[test]
    fn unknown_fields_rejected() {
        assert!(
            serde_json::from_value::<EnqueueJobRequest>(json!({"handler": "h", "bogus": 1}))
                .is_err()
        );
    }

    #[test]
    fn validation_rules() {
        assert!(req(json!({"handler": ""})).validate().is_err());
        assert!(req(json!({"handler": " h"})).validate().is_err());
        assert!(
            req(json!({"handler": "h", "payload": [1]}))
                .validate()
                .is_err()
        );
        assert!(
            req(json!({"handler": "h", "queue": " "}))
                .validate()
                .is_err()
        );
        assert!(
            req(json!({"handler": "h", "delay_ms": 5, "run_at": "2030-01-01T00:00:00Z"}))
                .validate()
                .is_err()
        );
        assert!(
            req(json!({"handler": "h", "retry": {"max_attempts": 0, "initial_backoff_ms": 1}}))
                .validate()
                .is_err()
        );
        assert!(
            req(json!({"handler": "h", "retry": {"max_attempts": 3, "initial_backoff_ms": 500, "max_backoff_ms": 100}}))
                .validate()
                .is_err()
        );
        assert!(
            req(json!({"handler": "h", "idempotency_key": ""}))
                .validate()
                .is_err()
        );
        assert!(
            req(json!({"handler": "h", "retry": {"max_attempts": 3, "initial_backoff_ms": 100}}))
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn first_run_at_honours_delay_and_run_at() {
        let now = Utc::now();
        assert_eq!(req(json!({"handler": "h"})).first_run_at(now), now);
        let d = req(json!({"handler": "h", "delay_ms": 1500})).first_run_at(now);
        assert_eq!((d - now).num_milliseconds(), 1500);
        let at = req(json!({"handler": "h", "run_at": "2030-01-01T00:00:00Z"})).first_run_at(now);
        assert_eq!(at.to_rfc3339(), "2030-01-01T00:00:00+00:00");
        // Absurd delays saturate instead of panicking.
        let far = req(json!({"handler": "h", "delay_ms": u64::MAX})).first_run_at(now);
        assert!(far > now);
    }

    #[test]
    fn status_mapping() {
        use InstanceState as S;
        assert_eq!(
            JobStatus::from_instance_state(S::Scheduled, false),
            JobStatus::Scheduled
        );
        assert_eq!(
            JobStatus::from_instance_state(S::Paused, false),
            JobStatus::Scheduled
        );
        assert_eq!(
            JobStatus::from_instance_state(S::Waiting, false),
            JobStatus::Running
        );
        assert_eq!(
            JobStatus::from_instance_state(S::Failed, false),
            JobStatus::Failed
        );
        assert_eq!(
            JobStatus::from_instance_state(S::Failed, true),
            JobStatus::DeadLettered
        );
        assert_eq!(
            JobStatus::from_instance_state(S::Completed, true),
            JobStatus::Completed
        );
        for status in [
            JobStatus::Scheduled,
            JobStatus::Running,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Cancelled,
            JobStatus::DeadLettered,
        ] {
            let wire = serde_json::to_value(status).unwrap();
            assert_eq!(
                JobStatus::from_str_loose(wire.as_str().unwrap()),
                Some(status)
            );
            for st in status.instance_states() {
                let retries = status == JobStatus::DeadLettered;
                assert_eq!(JobStatus::from_instance_state(st, retries), status);
            }
        }
    }
}
