//! Background jobs: enqueue a handler invocation without authoring a sequence.
//!
//! A job is an ordinary [`TaskInstance`] of an auto-managed, single-step
//! *system sequence*. One such sequence exists per
//! `(tenant, namespace, handler, queue, retry policy)`; it is created lazily
//! on first enqueue and reused afterwards. Because the job is just an
//! instance, every existing mechanism applies unchanged: the step retry
//! policy and the DLQ, pull and push worker dispatch (any handler that is not
//! a built-in is routed to external workers on the step's queue), tenant
//! enforcement, entitlements, idempotency keys and all instance views.
//!
//! Wire types live in [`orch8_types::job`].

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::job::{
    JOB_BLOCK_ID, JOB_METADATA_KEY, JOB_SEQUENCE_PREFIX, Job, JobRetry, JobStatus,
};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{
    BlockDefinition, RetryPolicy, SequenceDefinition, SequenceStatus, StepDef,
};

use crate::error::EngineError;

/// Default cap on retry backoff when the caller does not set one.
const DEFAULT_MAX_BACKOFF_MS: u64 = 60_000;

/// What distinguishes one job system sequence from another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSpec {
    pub handler: String,
    pub queue: Option<String>,
    pub retry: Option<JobRetry>,
}

impl JobSpec {
    /// Whether a failed run of this job is retried.
    #[must_use]
    pub fn retries(&self) -> bool {
        self.retry.as_ref().is_some_and(JobRetry::retries)
    }

    /// Deterministic system-sequence name. Jobs with only a handler map onto
    /// `_job.<handler>`; a queue and/or retry policy adds a short content
    /// hash so each distinct execution policy gets its own immutable
    /// sequence (step policy is part of the sequence, not the instance).
    #[must_use]
    pub fn sequence_name(&self) -> String {
        if self.queue.is_none() && self.retry.is_none() {
            return format!("{JOB_SEQUENCE_PREFIX}{}", self.handler);
        }
        let canonical = serde_json::json!({
            "queue": self.queue,
            "retry": self.retry,
        });
        let digest = Sha256::digest(canonical.to_string().as_bytes());
        let mut hex = String::with_capacity(12);
        for byte in digest.iter().take(6) {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
        }
        format!("{JOB_SEQUENCE_PREFIX}{}~{hex}", self.handler)
    }

    /// Engine retry policy for this job. Job `max_attempts` counts total
    /// executions; the engine's `max_attempts` counts *re*-tries after the
    /// first attempt, hence the `- 1`.
    #[must_use]
    pub fn retry_policy(&self) -> Option<RetryPolicy> {
        let retry = self.retry.as_ref().filter(|r| r.retries())?;
        let initial = retry.initial_backoff_ms;
        let max = retry
            .max_backoff_ms
            .unwrap_or_else(|| DEFAULT_MAX_BACKOFF_MS.max(initial));
        Some(RetryPolicy {
            max_attempts: retry.max_attempts - 1,
            initial_backoff: std::time::Duration::from_millis(initial),
            max_backoff: std::time::Duration::from_millis(max),
            backoff_multiplier: 2.0,
            retry_if: None,
            non_retryable_codes: None,
        })
    }

    /// Build the single-step system sequence for this spec.
    #[must_use]
    pub fn build_sequence(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
    ) -> SequenceDefinition {
        let step = StepDef {
            id: BlockId::new(JOB_BLOCK_ID),
            handler: self.handler.clone(),
            // Whole-string template → the payload object is passed through
            // with its JSON type intact as the handler's params.
            params: serde_json::Value::String("{{ data }}".into()),
            delay: None,
            retry: self.retry_policy(),
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: self.queue.clone(),
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
            output_schema: None,
            when: None,
            compensation: None,
        };
        SequenceDefinition {
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: tenant_id.clone(),
            namespace: namespace.clone(),
            name: self.sequence_name(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![BlockDefinition::Step(Box::new(step))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: Utc::now(),
        }
    }

    /// The `metadata._job` bookkeeping object stored on each job instance.
    #[must_use]
    pub fn metadata_marker(&self) -> serde_json::Value {
        let mut marker = serde_json::json!({
            "v": 1,
            "handler": self.handler,
            "retries": self.retries(),
            "max_attempts": self.retry.as_ref().map_or(1, |r| r.max_attempts),
        });
        if let Some(queue) = &self.queue {
            marker["queue"] = serde_json::Value::String(queue.clone());
        }
        marker
    }
}

/// Fetch (or lazily create) the system sequence for `spec`. Safe under
/// concurrent enqueues: the loser of the unique `(tenant, namespace, name,
/// version)` insert race re-reads the winner's row.
pub async fn ensure_job_sequence(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    namespace: &Namespace,
    spec: &JobSpec,
) -> Result<SequenceDefinition, EngineError> {
    let name = spec.sequence_name();
    if let Some(existing) = storage
        .get_sequence_by_name(tenant_id, namespace, &name, None)
        .await?
    {
        return Ok(existing);
    }
    let seq = spec.build_sequence(tenant_id, namespace);
    match storage.create_sequence(&seq).await {
        Ok(()) => Ok(seq),
        Err(StorageError::Conflict(_)) => storage
            .get_sequence_by_name(tenant_id, namespace, &name, None)
            .await?
            .ok_or_else(|| {
                EngineError::NotFound(format!("job sequence '{name}' vanished after conflict"))
            }),
        Err(e) => Err(e.into()),
    }
}

/// Everything needed to materialise one job instance.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub tenant_id: TenantId,
    pub namespace: Namespace,
    pub spec: JobSpec,
    pub payload: serde_json::Value,
    pub priority: Priority,
    pub run_at: DateTime<Utc>,
    /// Already prefixed with [`orch8_types::job::JOB_IDEMPOTENCY_PREFIX`].
    pub idempotency_key: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// Build (without persisting) the instance backing a job.
#[must_use]
pub fn build_job_instance(job: NewJob, sequence_id: SequenceId) -> TaskInstance {
    let now = Utc::now();
    let mut metadata = match job.metadata {
        Some(serde_json::Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    let mut marker = job.spec.metadata_marker();
    marker["run_at"] = serde_json::Value::String(job.run_at.to_rfc3339());
    metadata.insert(JOB_METADATA_KEY.into(), marker);
    TaskInstance {
        id: InstanceId::new(),
        sequence_id,
        tenant_id: job.tenant_id,
        namespace: job.namespace,
        state: InstanceState::Scheduled,
        next_fire_at: Some(job.run_at),
        priority: job.priority,
        timezone: "UTC".into(),
        metadata: serde_json::Value::Object(metadata),
        context: ExecutionContext {
            data: job.payload,
            ..Default::default()
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: job.idempotency_key,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

/// The `metadata._job` marker of an instance, if it is a job.
#[must_use]
pub fn job_marker(instance: &TaskInstance) -> Option<&serde_json::Value> {
    instance
        .metadata
        .get(JOB_METADATA_KEY)
        .filter(|m| m.get("handler").is_some_and(serde_json::Value::is_string))
}

/// Metadata containment filter selecting job instances, optionally narrowed
/// by handler and by whether a retry policy was configured.
#[must_use]
pub fn job_metadata_filter(handler: Option<&str>, retries: Option<bool>) -> serde_json::Value {
    let mut marker = serde_json::json!({ "v": 1 });
    if let Some(h) = handler {
        marker["handler"] = serde_json::Value::String(h.to_string());
    }
    if let Some(r) = retries {
        marker["retries"] = serde_json::Value::Bool(r);
    }
    serde_json::json!({ JOB_METADATA_KEY: marker })
}

/// Project an instance (plus the latest output row of its `run` step) onto
/// the job wire shape. Returns `None` when the instance is not a job.
#[must_use]
pub fn job_from_instance(instance: &TaskInstance, latest: Option<&BlockOutput>) -> Option<Job> {
    let marker = job_marker(instance)?;
    let handler = marker.get("handler")?.as_str()?.to_string();
    let retries = marker
        .get("retries")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let queue = marker
        .get("queue")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let status = JobStatus::from_instance_state(instance.state, retries);

    let mut attempts = 0u32;
    let mut output = None;
    let mut error = None;
    if let Some(row) = latest {
        attempts = u32::from(row.attempt).saturating_add(1);
        match row.output_ref.as_deref() {
            // In-flight sentinel: the attempt has started but produced nothing yet.
            Some(r) if r == crate::handlers::param_resolve::IN_PROGRESS_SENTINEL => {}
            Some("__retry__" | "__error__") => {
                error = row
                    .output
                    .get("message")
                    .or_else(|| row.output.get("error"))
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string);
            }
            _ => {
                if status == JobStatus::Completed {
                    output = Some(row.output.clone());
                }
            }
        }
    }
    // The originally requested run time is recorded in the marker; fall
    // back to the live schedule for instances written without it.
    let run_at = marker
        .get("run_at")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
        .or(instance.next_fire_at)
        .unwrap_or(instance.created_at);
    Some(Job {
        id: instance.id.into_uuid(),
        instance_id: instance.id.into_uuid(),
        handler,
        status,
        queue,
        created_at: instance.created_at,
        run_at,
        attempts: Some(attempts),
        output,
        error,
    })
}

/// Load the job view for one instance (fetches the step's latest output).
pub async fn load_job(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
) -> Result<Option<Job>, EngineError> {
    if job_marker(instance).is_none() {
        return Ok(None);
    }
    let latest = storage
        .get_block_output(instance.id, &BlockId::new(JOB_BLOCK_ID))
        .await?;
    Ok(job_from_instance(instance, latest.as_ref()))
}

/// Load job views for a page of instances with one batched output query.
pub async fn load_jobs(
    storage: &dyn StorageBackend,
    instances: &[TaskInstance],
) -> Result<Vec<Job>, EngineError> {
    let block = BlockId::new(JOB_BLOCK_ID);
    let keys: Vec<(InstanceId, &BlockId)> = instances
        .iter()
        .filter(|i| job_marker(i).is_some())
        .map(|i| (i.id, &block))
        .collect();
    let outputs = if keys.is_empty() {
        std::collections::HashMap::new()
    } else {
        storage.get_block_outputs_batch(&keys).await?
    };
    Ok(instances
        .iter()
        .filter_map(|i| job_from_instance(i, outputs.get(&(i.id, block.clone()))))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(handler: &str, queue: Option<&str>, retry: Option<(u32, u64)>) -> JobSpec {
        JobSpec {
            handler: handler.into(),
            queue: queue.map(Into::into),
            retry: retry.map(|(n, b)| JobRetry {
                max_attempts: n,
                initial_backoff_ms: b,
                max_backoff_ms: None,
            }),
        }
    }

    #[test]
    fn sequence_name_is_deterministic_and_policy_scoped() {
        assert_eq!(spec("email", None, None).sequence_name(), "_job.email");
        let a = spec("email", Some("q"), None).sequence_name();
        let b = spec("email", Some("q"), None).sequence_name();
        let c = spec("email", Some("q2"), None).sequence_name();
        let d = spec("email", Some("q"), Some((3, 100))).sequence_name();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert!(a.starts_with("_job.email~"));
        assert_eq!(a.len(), "_job.email~".len() + 12);
    }

    #[test]
    fn retry_policy_maps_total_attempts_to_engine_retries() {
        assert!(spec("h", None, None).retry_policy().is_none());
        assert!(spec("h", None, Some((1, 100))).retry_policy().is_none());
        let p = spec("h", None, Some((3, 250))).retry_policy().unwrap();
        assert_eq!(p.max_attempts, 2);
        assert_eq!(p.initial_backoff.as_millis(), 250);
        assert_eq!(p.max_backoff.as_millis(), 60_000);
        // Default cap never undercuts the initial backoff.
        let p = spec("h", None, Some((2, 120_000))).retry_policy().unwrap();
        assert_eq!(p.max_backoff.as_millis(), 120_000);
    }

    #[test]
    fn build_sequence_has_single_run_step() {
        let s = spec("worker.thing", Some("gpu"), Some((4, 10)));
        let seq = s.build_sequence(&TenantId::unchecked("t"), &Namespace::new("default"));
        assert_eq!(seq.blocks.len(), 1);
        let BlockDefinition::Step(step) = &seq.blocks[0] else {
            panic!("expected step");
        };
        assert_eq!(step.id.as_str(), JOB_BLOCK_ID);
        assert_eq!(step.handler, "worker.thing");
        assert_eq!(step.queue_name.as_deref(), Some("gpu"));
        assert_eq!(step.retry.as_ref().unwrap().max_attempts, 3);
    }

    #[test]
    fn metadata_marker_and_filter_agree() {
        let s = spec("h", Some("q"), Some((2, 1)));
        let m = s.metadata_marker();
        assert_eq!(m["handler"], "h");
        assert_eq!(m["queue"], "q");
        assert_eq!(m["retries"], true);
        assert_eq!(m["max_attempts"], 2);
        let f = job_metadata_filter(Some("h"), Some(true));
        assert_eq!(f["_job"]["handler"], "h");
        assert_eq!(f["_job"]["retries"], true);
        assert_eq!(
            job_metadata_filter(None, None),
            serde_json::json!({"_job": {"v": 1}})
        );
    }

    fn job_instance(state: InstanceState, retries: bool) -> TaskInstance {
        let mut inst = build_job_instance(
            NewJob {
                tenant_id: TenantId::unchecked("t"),
                namespace: Namespace::new("default"),
                spec: spec("h", None, retries.then_some((3, 1))),
                payload: serde_json::json!({"a": 1}),
                priority: Priority::Normal,
                run_at: Utc::now(),
                idempotency_key: None,
                metadata: Some(serde_json::json!({"user": "x", "_job": "spoofed"})),
            },
            SequenceId::new(),
        );
        inst.state = state;
        inst
    }

    #[test]
    fn build_job_instance_overrides_reserved_metadata() {
        let inst = job_instance(InstanceState::Scheduled, false);
        assert_eq!(inst.metadata["user"], "x");
        assert_eq!(inst.metadata["_job"]["handler"], "h");
        assert_eq!(inst.context.data, serde_json::json!({"a": 1}));
    }

    fn out(attempt: u16, output: serde_json::Value, r: Option<&str>) -> BlockOutput {
        BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: InstanceId::new(),
            block_id: BlockId::new(JOB_BLOCK_ID),
            output,
            output_ref: r.map(Into::into),
            output_size: 0,
            attempt,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn job_view_projection() {
        let inst = job_instance(InstanceState::Scheduled, false);
        let j = job_from_instance(&inst, None).unwrap();
        assert_eq!(j.status, JobStatus::Scheduled);
        assert_eq!(j.attempts, Some(0));
        assert_eq!(j.id, j.instance_id);

        let inst = job_instance(InstanceState::Completed, false);
        let row = out(0, serde_json::json!({"ok": true}), None);
        let j = job_from_instance(&inst, Some(&row)).unwrap();
        assert_eq!(j.status, JobStatus::Completed);
        assert_eq!(j.output, Some(serde_json::json!({"ok": true})));
        assert_eq!(j.attempts, Some(1));

        let inst = job_instance(InstanceState::Failed, true);
        let row = out(
            2,
            serde_json::json!({"_retry_marker": true, "error": "boom"}),
            Some("__retry__"),
        );
        let j = job_from_instance(&inst, Some(&row)).unwrap();
        assert_eq!(j.status, JobStatus::DeadLettered);
        assert_eq!(j.error.as_deref(), Some("boom"));
        assert_eq!(j.attempts, Some(3));
        assert!(j.output.is_none());

        let mut plain = inst.clone();
        plain.metadata = serde_json::json!({});
        assert!(job_from_instance(&plain, None).is_none());
    }
}
