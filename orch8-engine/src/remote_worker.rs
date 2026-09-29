//! Client side of the external worker lease protocol, shared by every
//! remote runtime node built on this engine: the phone
//! (`orch8-mobile`), and the hybrid remote executor
//! ([`crate::remote_executor`]).
//!
//! A remote node claims leased tasks from a control plane
//! (`POST /workers/tasks/poll` with a capability advertisement), keeps each
//! lease alive with heartbeats, and settles it with `complete` / `fail`, or
//! gives it back with `release`. Every mutation echoes the task's
//! `claim_epoch`, so a process that lost its lease can never overwrite the
//! outcome of the attempt that replaced it. This module owns the wire shapes
//! and status classification so the nodes cannot drift apart; each node keeps
//! its own scheduling policy (the phone's journal and power states, the
//! executor's drain).

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::RuntimeCapabilities;
use orch8_types::ids::{BlockId, InstanceId, TenantId};

use crate::handlers::StepContext;

/// Default lease when neither the task nor the poll response carries one.
pub const DEFAULT_LEASE_SECS: u64 = 120;
/// Cap on any control-plane response body a node buffers.
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Outcome class of a lease mutation (heartbeat/complete/fail/release).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseResponse {
    /// 2xx — accepted.
    Accepted,
    /// 404 / 409 / 410 — the task is gone or no longer ours (stale epoch,
    /// reclaimed, reaped). Stop acting on it and forget the claim.
    LostOwnership,
    /// 404/405 on an endpoint an older server may not have (release).
    Unsupported,
    /// Network error, 408/429/5xx — try again later.
    Retry,
    /// Any other 4xx — the request itself is wrong; retrying cannot help.
    Rejected,
}

/// Classify an HTTP status of a lease mutation.
#[must_use]
pub fn classify_lease_status(status: u16, missing_route_is_unsupported: bool) -> LeaseResponse {
    match status {
        200..=299 => LeaseResponse::Accepted,
        404 | 405 if missing_route_is_unsupported => LeaseResponse::Unsupported,
        404 | 409 | 410 => LeaseResponse::LostOwnership,
        408 | 425 | 429 | 500..=599 => LeaseResponse::Retry,
        _ => LeaseResponse::Rejected,
    }
}

/// `POST /workers/tasks/poll` response.
#[derive(Debug, Default, Deserialize)]
pub struct PollResponse {
    #[serde(default)]
    pub tasks: Vec<RemoteTask>,
    #[serde(default)]
    pub lease_secs: Option<u64>,
    #[serde(default)]
    pub heartbeat_interval_secs: Option<u64>,
    #[serde(default)]
    pub poll_after_ms: Option<u64>,
}

/// A claimed worker task, parsed leniently: only the fields a node needs
/// are required, and every distributed-execution field is optional so nodes
/// work against servers with and without the v1 contract additions.
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteTask {
    pub id: uuid::Uuid,
    pub instance_id: uuid::Uuid,
    pub block_id: String,
    pub handler_name: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub context: Value,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default)]
    pub timeout_ms: Option<i64>,
    #[serde(default)]
    pub claim_epoch: u64,
    #[serde(default)]
    pub effect_id: Option<String>,
    #[serde(default)]
    pub continuity_epoch: Option<u64>,
    #[serde(default)]
    pub lease_secs: Option<u32>,
    #[serde(default)]
    pub resume_checkpoint: Option<Value>,
}

#[derive(Serialize)]
struct PollBody<'a> {
    handler_name: &'a str,
    worker_id: &'a str,
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a str>,
    capabilities: &'a RuntimeCapabilities,
}

/// HTTP client for the lease protocol, bound to one API base, credential,
/// and worker identity. Transport hardening (TLS roots, timeouts) is the
/// caller's `reqwest::Client`.
#[derive(Clone)]
pub struct HttpLeaseClient {
    http: reqwest::Client,
    api_base: String,
    headers: Vec<(&'static str, String)>,
    worker_id: String,
}

impl std::fmt::Debug for HttpLeaseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpLeaseClient")
            .field("api_base", &self.api_base)
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

impl HttpLeaseClient {
    /// `api_base` is the versioned API root (e.g. `https://host/api/v1`).
    /// `headers` are sent on every request (`x-api-key`, `x-tenant-id`,
    /// `x-device-id`, …).
    #[must_use]
    pub fn new(
        http: reqwest::Client,
        api_base: &str,
        headers: Vec<(&'static str, String)>,
        worker_id: String,
    ) -> Self {
        Self {
            http,
            api_base: api_base.trim_end_matches('/').to_string(),
            headers,
            worker_id,
        }
    }

    #[must_use]
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base, path.trim_start_matches('/'))
    }

    /// A `POST` to `path` (relative to the API base) with the node headers.
    pub fn post(&self, path: &str) -> reqwest::RequestBuilder {
        let mut request = self.http.post(self.url(path));
        for (name, value) in &self.headers {
            request = request.header(*name, value);
        }
        request
    }

    /// Claim up to `limit` tasks of `handler` as `capabilities`.
    ///
    /// # Errors
    /// Network failures, non-2xx statuses, and malformed bodies, as a
    /// message with the URL's secrets redacted.
    pub async fn poll(
        &self,
        handler: &str,
        limit: u32,
        version: Option<&str>,
        capabilities: &RuntimeCapabilities,
    ) -> Result<PollResponse, String> {
        let body = PollBody {
            handler_name: handler,
            worker_id: &self.worker_id,
            limit: limit.max(1),
            version,
            capabilities,
        };
        let resp = self
            .post("workers/tasks/poll")
            .json(&body)
            .send()
            .await
            .map_err(|e| network_err("poll tasks", &e))?;
        let resp = expect_success("poll tasks", resp).await?;
        let bytes = read_capped(resp).await?;
        serde_json::from_slice(&bytes).map_err(|e| format!("poll tasks: invalid response: {e}"))
    }

    /// `POST /runtimes/register`: refresh this node's capability lease
    /// between polls (e.g. while every slot is busy, or to advertise
    /// `draining`).
    ///
    /// # Errors
    /// Network failures and non-2xx statuses.
    pub async fn register_runtime(
        &self,
        tenant_id: &str,
        capabilities: &RuntimeCapabilities,
    ) -> Result<(), String> {
        let resp = self
            .post("runtimes/register")
            .json(&serde_json::json!({ "tenant_id": tenant_id, "capabilities": capabilities }))
            .send()
            .await
            .map_err(|e| network_err("register runtime", &e))?;
        expect_success("register runtime", resp).await.map(|_| ())
    }

    pub async fn heartbeat(&self, task_id: uuid::Uuid, claim_epoch: u64) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/heartbeat"),
            &serde_json::json!({ "worker_id": self.worker_id, "claim_epoch": claim_epoch }),
            false,
        )
        .await
    }

    pub async fn complete(
        &self,
        task_id: uuid::Uuid,
        claim_epoch: u64,
        output: &Value,
    ) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/complete"),
            &serde_json::json!({
                "worker_id": self.worker_id,
                "claim_epoch": claim_epoch,
                "output": output,
            }),
            false,
        )
        .await
    }

    pub async fn fail(
        &self,
        task_id: uuid::Uuid,
        claim_epoch: u64,
        message: &str,
        retryable: bool,
    ) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/fail"),
            &serde_json::json!({
                "worker_id": self.worker_id,
                "claim_epoch": claim_epoch,
                "message": message,
                "retryable": retryable,
            }),
            false,
        )
        .await
    }

    /// `POST /workers/tasks/{id}/release`. Returns
    /// [`LeaseResponse::Unsupported`] on 404/405 so the caller can fall back
    /// to a retryable `fail` against a server that predates the endpoint.
    pub async fn release(
        &self,
        task_id: uuid::Uuid,
        claim_epoch: u64,
        started: bool,
    ) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/release"),
            &serde_json::json!({
                "worker_id": self.worker_id,
                "claim_epoch": claim_epoch,
                "started": started,
            }),
            true,
        )
        .await
    }

    async fn lease_call(
        &self,
        path: &str,
        body: &Value,
        missing_route_is_unsupported: bool,
    ) -> LeaseResponse {
        match self.post(path).json(body).send().await {
            Ok(resp) => classify_lease_status(resp.status().as_u16(), missing_route_is_unsupported),
            Err(e) => {
                debug!(path, error = %crate::outbound::redact_error(&e), "lease call failed");
                LeaseResponse::Retry
            }
        }
    }
}

fn network_err(what: &str, e: &reqwest::Error) -> String {
    format!("{what}: {}", crate::outbound::redact_error(e))
}

async fn expect_success(what: &str, resp: reqwest::Response) -> Result<reqwest::Response, String> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = read_capped(resp).await.unwrap_or_default();
    let snippet = String::from_utf8_lossy(&body[..body.len().min(512)]).into_owned();
    Err(format!("{what}: HTTP {status}: {snippet}"))
}

async fn read_capped(resp: reqwest::Response) -> Result<Vec<u8>, String> {
    crate::handlers::builtin::read_body_capped(resp, MAX_RESPONSE_BYTES)
        .await
        .map_err(|e| match e {
            crate::handlers::builtin::BodyReadError::TooLarge(_) => {
                format!("response exceeds {MAX_RESPONSE_BYTES} bytes")
            }
            crate::handlers::builtin::BodyReadError::Io(message) => message,
        })
}

/// Heartbeat cadence: the server's explicit interval, else a third of the
/// tightest lease on offer, clamped to at least one second.
#[must_use]
pub fn heartbeat_interval(
    server_interval: Option<u64>,
    response_lease: Option<u64>,
    task_lease: Option<u32>,
) -> Duration {
    let lease = task_lease
        .map(u64::from)
        .or(response_lease)
        .filter(|l| *l > 0)
        .unwrap_or(DEFAULT_LEASE_SECS);
    let from_lease = (lease / 3).max(1);
    let secs = server_interval
        .filter(|s| *s > 0)
        .map_or(from_lease, |s| s.min(from_lease));
    Duration::from_secs(secs.max(1))
}

/// Add the reserved `__orch8` member to object params: the task's effect
/// id (the server's deterministic idempotency key), identity, attempt, and
/// resume checkpoint. Given to host-native handlers only; built-ins never
/// see it.
pub fn inject_task_metadata(params: &mut Value, task: &RemoteTask, runtime_id: &str) {
    if let Value::Object(map) = params {
        map.insert(
            "__orch8".into(),
            serde_json::json!({
                "effect_id": task.effect_id,
                "task_id": task.id,
                "instance_id": task.instance_id,
                "block_id": task.block_id,
                "attempt": task.attempt,
                "runtime_id": runtime_id,
                "continuity_epoch": task.continuity_epoch,
                "resume_checkpoint": task.resume_checkpoint,
            }),
        );
    }
}

/// The [`StepContext`] a remote node hands to a handler for `task`.
/// `storage` is the node's own (local or scratch) store, never the control
/// plane's database.
#[must_use]
pub fn step_context(
    task: &RemoteTask,
    tenant_id: TenantId,
    params: Value,
    storage: Arc<dyn StorageBackend>,
) -> StepContext {
    let context: ExecutionContext =
        serde_json::from_value(task.context.clone()).unwrap_or_default();
    StepContext {
        instance_id: InstanceId::from_uuid(task.instance_id),
        tenant_id,
        block_id: BlockId::new(task.block_id.clone()),
        params,
        context: Arc::new(context),
        attempt: task.attempt.max(1),
        storage,
        wait_for_input: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_status_classification() {
        assert_eq!(classify_lease_status(200, false), LeaseResponse::Accepted);
        assert_eq!(
            classify_lease_status(409, false),
            LeaseResponse::LostOwnership
        );
        assert_eq!(
            classify_lease_status(404, false),
            LeaseResponse::LostOwnership
        );
        assert_eq!(classify_lease_status(404, true), LeaseResponse::Unsupported);
        assert_eq!(classify_lease_status(405, true), LeaseResponse::Unsupported);
        assert_eq!(classify_lease_status(503, false), LeaseResponse::Retry);
        assert_eq!(classify_lease_status(429, false), LeaseResponse::Retry);
        assert_eq!(classify_lease_status(400, false), LeaseResponse::Rejected);
    }

    #[test]
    fn remote_task_parses_with_and_without_contract_fields() {
        let old: RemoteTask = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(), "instance_id": uuid::Uuid::new_v4(),
            "block_id": "b", "handler_name": "scan", "params": {}, "context": {},
            "attempt": 1, "claim_epoch": 3, "state": "claimed",
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .unwrap();
        assert!(old.effect_id.is_none() && old.lease_secs.is_none());
        let new: RemoteTask = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(), "instance_id": uuid::Uuid::new_v4(),
            "block_id": "b", "handler_name": "scan", "claim_epoch": 1,
            "effect_id": "eff-1", "continuity_epoch": 4, "lease_secs": 120,
            "target_runtime_id": "x", "runtime_kinds": ["mobile"]
        }))
        .unwrap();
        assert_eq!(new.effect_id.as_deref(), Some("eff-1"));
        assert_eq!(new.lease_secs, Some(120));
        assert_eq!(new.continuity_epoch, Some(4));
    }

    #[test]
    fn heartbeat_cadence_follows_the_tightest_lease() {
        assert_eq!(
            heartbeat_interval(None, None, None),
            Duration::from_secs(40)
        );
        assert_eq!(
            heartbeat_interval(Some(15), Some(60), None),
            Duration::from_secs(15)
        );
        assert_eq!(
            heartbeat_interval(Some(15), Some(60), Some(9)),
            Duration::from_secs(3)
        );
        assert_eq!(
            heartbeat_interval(None, Some(1), None),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn metadata_is_injected_into_object_params_only() {
        let task: RemoteTask = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(), "instance_id": uuid::Uuid::new_v4(),
            "block_id": "b", "handler_name": "h", "effect_id": "e-1", "attempt": 2
        }))
        .unwrap();
        let mut params = serde_json::json!({"a": 1});
        inject_task_metadata(&mut params, &task, "rt");
        assert_eq!(params["__orch8"]["effect_id"], "e-1");
        assert_eq!(params["__orch8"]["runtime_id"], "rt");
        let mut scalar = serde_json::json!(5);
        inject_task_metadata(&mut scalar, &task, "rt");
        assert_eq!(scalar, serde_json::json!(5));
    }
}
