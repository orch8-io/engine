use axum::extract::{Path, Query, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::filter::Pagination;
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use orch8_types::worker::{
    WorkerAttemptEventKind, WorkerClaim, WorkerTaskAttemptEvent, WorkerTaskState,
};
use orch8_types::worker_filter::WorkerTaskFilter;

use orch8_types::execution::NodeState;

use crate::AppState;
use crate::auth::OptionalAdmin;
use crate::error::ApiError;

const MAX_WORKER_ARTIFACT_UPLOAD_BYTES: usize = 10 * 1024 * 1024;

#[derive(serde::Serialize, ToSchema)]
pub(crate) struct PollTasksResponse {
    tasks: Vec<ClaimedWorkerTask>,
    /// Server-wide default lease; each task's own `lease_secs` wins.
    lease_secs: u64,
    heartbeat_interval_secs: u64,
    poll_after_ms: u64,
}

/// A claimed task as delivered to a runtime node: the task (with its
/// `effect_id`, `continuity_epoch`, and effective `lease_secs`) plus its
/// placement echoed at the top level for debugging.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct ClaimedWorkerTask {
    #[serde(flatten)]
    task: orch8_types::worker::WorkerTask,
    /// `$runtime.runtime_id`: the only node allowed to claim (mailbox).
    #[serde(skip_serializing_if = "Option::is_none")]
    target_runtime_id: Option<orch8_types::continuity::RuntimeId>,
    /// `$runtime.runtime_kinds`: kinds allowed to claim (empty = any).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    runtime_kinds: Vec<orch8_types::continuity::RuntimeKind>,
}

/// Shape claimed tasks for the wire: every task reports its effective lease,
/// and a browser claimant never receives secrets — its context is filtered
/// (`config`/audit dropped, credential-bearing entries removed, redaction
/// policy applied). The stored row keeps the full context for other kinds.
fn prepare_claimed_tasks(
    state: &AppState,
    mut tasks: Vec<orch8_types::worker::WorkerTask>,
) -> Vec<orch8_types::worker::WorkerTask> {
    for task in &mut tasks {
        if task.lease_secs.is_none() {
            task.lease_secs = Some(u32::try_from(state.worker_lease_secs).unwrap_or(u32::MAX));
        }
        if task.claimed_runtime_kind == Some(orch8_types::continuity::RuntimeKind::Browser) {
            task.context = orch8_types::worker::browser_safe_context(&task.context);
        }
    }
    tasks
}

/// Record `placement.status = placed` on instances whose placed tasks were
/// just claimed (clears a previous `placement_unsatisfied`).
async fn note_placed_claims(state: &AppState, tasks: &[orch8_types::worker::WorkerTask]) {
    for task in tasks {
        orch8_engine::step_placement::record_placement_claimed(state.storage.as_ref(), task).await;
    }
}

fn poll_response(state: &AppState, tasks: Vec<orch8_types::worker::WorkerTask>) -> Response {
    let tasks: Vec<_> = prepare_claimed_tasks(state, tasks)
        .into_iter()
        .map(|task| ClaimedWorkerTask {
            target_runtime_id: task.requirements.runtime_id,
            runtime_kinds: task.requirements.runtime_kinds.clone(),
            task,
        })
        .collect();
    let poll_after_ms = if tasks.is_empty() { 1_000 } else { 0 };
    let mut response = Json(PollTasksResponse {
        tasks,
        lease_secs: state.worker_lease_secs,
        heartbeat_interval_secs: state.worker_heartbeat_interval_secs,
        poll_after_ms,
    })
    .into_response();
    if poll_after_ms > 0 {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
    }
    response
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/workers", get(list_workers))
        .route("/handlers", get(list_handlers))
        .route("/workers/tasks", get(list_tasks))
        .route("/workers/tasks/stats", get(task_stats))
        .route("/workers/tasks/{id}/attempts", get(list_task_attempts))
        .route("/workers/tasks/poll", post(poll_tasks))
        .route("/workers/tasks/poll/queue", post(poll_tasks_from_queue))
        .route("/workers/tasks/{id}/complete", post(complete_task))
        .route(
            "/workers/tasks/{id}/artifacts/{upload_id}",
            post(upload_task_artifact),
        )
        .route("/workers/tasks/{id}/fail", post(fail_task))
        .route("/workers/tasks/{id}/heartbeat", post(heartbeat_task))
        .route("/workers/tasks/{id}/release", post(release_task))
        .route("/workers/commands", post(enqueue_command))
        .route("/workers/commands/{id}", axum::routing::delete(ack_command))
        .route("/workers/{worker_id}/commands", get(list_commands))
        .route(
            "/workers/version-pins",
            post(set_version_pin).get(list_version_pins),
        )
        .route(
            "/workers/version-pins/{tenant_id}/{handler_name}",
            axum::routing::delete(delete_version_pin),
        )
}

#[derive(Deserialize)]
pub(crate) struct AttemptQuery {
    #[serde(default = "default_attempt_limit")]
    limit: u32,
}

const fn default_attempt_limit() -> u32 {
    100
}

#[utoipa::path(get, path = "/workers/tasks/{id}/attempts", tag = "workers",
    params(("id" = Uuid, Path), ("limit" = Option<u32>, Query)),
    responses((status = 200, body = Vec<WorkerTaskAttemptEvent>), (status = 404))
)]
pub(crate) async fn list_task_attempts(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(task_id): Path<Uuid>,
    Query(query): Query<AttemptQuery>,
) -> Result<Json<Vec<WorkerTaskAttemptEvent>>, ApiError> {
    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    let instance = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("worker_task {task_id}"),
    )?;
    let events = state
        .storage
        .list_worker_task_attempt_events(task_id, query.limit.min(1000))
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task_attempt"))?;
    Ok(Json(events))
}

async fn record_stale_rejection(
    state: &AppState,
    task_id: Uuid,
    claim: &WorkerClaim,
    reason: &str,
) {
    let event = WorkerTaskAttemptEvent {
        id: Uuid::now_v7(),
        task_id,
        claim_epoch: claim.claim_epoch,
        worker_id: Some(claim.worker_id.clone()),
        event: WorkerAttemptEventKind::StaleMutationRejected,
        reason: Some(reason.to_string()),
        created_at: chrono::Utc::now(),
    };
    if let Err(error) = state.storage.record_worker_task_attempt_event(&event).await {
        tracing::warn!(%task_id, %error, "failed to record stale worker mutation");
    }
}

/// Ownership half of the lease fence: a task dispatched under an older
/// continuity owner (or whose execution is mid-export) cannot mutate its
/// lease, even with a current `claim_epoch`. 409, like a lost lease.
async fn enforce_ownership_fence(
    state: &AppState,
    tenant_id: &orch8_types::ids::TenantId,
    task: &orch8_types::worker::WorkerTask,
    claim: &WorkerClaim,
    operation: &str,
) -> Result<(), ApiError> {
    let current = orch8_engine::ownership::worker_task_ownership_current(
        state.storage.as_ref(),
        tenant_id,
        task,
    )
    .await
    .map_err(|error| ApiError::Internal(format!("ownership fence: {error}")))?;
    if current {
        return Ok(());
    }
    record_stale_rejection(
        state,
        task.id,
        claim,
        &format!("{operation} rejected: continuity ownership changed"),
    )
    .await;
    Err(ApiError::Conflict(
        "worker task ownership changed (continuity handoff); the lease is stale".into(),
    ))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct SetVersionPinRequest {
    tenant_id: String,
    handler_name: String,
    min_version: String,
}

#[derive(Deserialize)]
pub(crate) struct ListPinsQuery {
    tenant_id: Option<String>,
}

/// Create-or-update a minimum-worker-version pin for a `(tenant, handler)`.
#[utoipa::path(post, path = "/workers/version-pins", tag = "workers",
    request_body = SetVersionPinRequest,
    responses((status = 200, description = "Pin set", body = orch8_types::worker::WorkerVersionPin))
)]
pub(crate) async fn set_version_pin(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<SetVersionPinRequest>,
) -> Result<impl IntoResponse, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    let tenant_id = crate::auth::enforce_tenant_create(
        &tenant_ctx,
        &orch8_types::ids::TenantId::unchecked(req.tenant_id),
    )?;
    if req.handler_name.trim().is_empty() || req.min_version.trim().is_empty() {
        return Err(ApiError::InvalidArgument(
            "handler_name and min_version are required".into(),
        ));
    }
    let now = chrono::Utc::now();
    let pin = orch8_types::worker::WorkerVersionPin {
        tenant_id: tenant_id.into_string(),
        handler_name: req.handler_name,
        min_version: req.min_version,
        created_at: now,
        updated_at: now,
    };
    state
        .storage
        .upsert_worker_version_pin(&pin)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_version_pin"))?;
    Ok((StatusCode::OK, Json(pin)))
}

/// List worker version pins, optionally filtered by tenant.
#[utoipa::path(get, path = "/workers/version-pins", tag = "workers",
    params(("tenant_id" = Option<String>, Query, description = "Filter by tenant")),
    responses((status = 200, description = "Pins", body = Vec<orch8_types::worker::WorkerVersionPin>))
)]
pub(crate) async fn list_version_pins(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListPinsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let pins = state
        .storage
        .list_worker_version_pins(scoped.as_ref().map(orch8_types::ids::TenantId::as_str))
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_version_pin"))?;
    Ok(Json(pins))
}

/// Delete a worker version pin.
#[utoipa::path(delete, path = "/workers/version-pins/{tenant_id}/{handler_name}", tag = "workers",
    params(
        ("tenant_id" = String, Path, description = "Tenant id"),
        ("handler_name" = String, Path, description = "Handler name"),
    ),
    responses((status = 204, description = "Deleted"))
)]
pub(crate) async fn delete_version_pin(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((tenant_id, handler_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &orch8_types::ids::TenantId::unchecked(&tenant_id),
        "worker_version_pin",
    )?;
    state
        .storage
        .delete_worker_version_pin(&tenant_id, &handler_name)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_version_pin"))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct EnqueueCommandRequest {
    worker_id: String,
    /// Tenant owning the target worker. Tenant-scoped (per-tenant key) worker
    /// streams only receive commands of their own tenant; leave empty to
    /// address workers connected with the root key.
    #[serde(default)]
    tenant_id: String,
    /// `drain`, `reload`, `ping`, or `place`.
    command: orch8_types::worker::WorkerCommandKind,
    #[serde(default)]
    payload: serde_json::Value,
}

/// Queue a control command for a worker. The worker picks it up via
/// `GET /workers/{worker_id}/commands` and acks it after acting.
///
/// This is an operator/admin endpoint: only callers with an admin context may
/// enqueue commands to the worker fleet.
#[utoipa::path(post, path = "/workers/commands", tag = "workers",
    request_body = EnqueueCommandRequest,
    responses(
        (status = 201, description = "Command queued", body = orch8_types::worker::WorkerCommand),
        (status = 403, description = "Admin context required"),
    )
)]
pub(crate) async fn enqueue_command(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Json(req): Json<EnqueueCommandRequest>,
) -> Result<impl IntoResponse, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    if req.worker_id.trim().is_empty() {
        return Err(ApiError::InvalidArgument("worker_id is required".into()));
    }
    let cmd = orch8_types::worker::WorkerCommand {
        id: Uuid::now_v7(),
        worker_id: req.worker_id,
        tenant_id: req.tenant_id,
        command: req.command,
        payload: req.payload,
        created_at: chrono::Utc::now(),
    };
    state
        .storage
        .enqueue_worker_command(&cmd)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_command"))?;
    Ok((StatusCode::CREATED, Json(cmd)))
}

/// List a worker's pending control commands (the worker control channel).
#[utoipa::path(get, path = "/workers/{worker_id}/commands", tag = "workers",
    params(("worker_id" = String, Path, description = "Worker id")),
    responses((status = 200, description = "Pending commands", body = Vec<orch8_types::worker::WorkerCommand>))
)]
pub(crate) async fn list_commands(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(worker_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    let commands = state
        .storage
        .list_worker_commands(&worker_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_command"))?;
    Ok(Json(commands))
}

/// Acknowledge (delete) a delivered command.
#[utoipa::path(delete, path = "/workers/commands/{id}", tag = "workers",
    params(("id" = Uuid, Path, description = "Command id")),
    responses((status = 204, description = "Acknowledged"))
)]
pub(crate) async fn ack_command(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    state
        .storage
        .delete_worker_command(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_command"))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct PollRequest {
    handler_name: String,
    worker_id: String,
    #[serde(default = "default_poll_limit")]
    limit: u32,
    /// Optional worker build/deploy version, recorded on the worker registry.
    #[serde(default)]
    version: Option<String>,
    /// Short-lived, fail-closed capability advertisement used for atomic
    /// hardware, region, browser/mobile UI, network, and trust matching.
    #[serde(default)]
    capabilities: Option<orch8_types::continuity::RuntimeCapabilities>,
}

const fn default_poll_limit() -> u32 {
    1
}

const SHA256_HEX: &[u8; 16] = b"0123456789abcdef";

/// Record a worker registration from a poll. Best-effort: a registry write
/// failure must never fail the poll itself, so errors are logged and dropped.
async fn record_registration(
    state: &AppState,
    worker_id: &str,
    handler_name: &str,
    queue_name: Option<&str>,
    version: Option<&str>,
    tenant_id: Option<&orch8_types::ids::TenantId>,
) {
    let registration = orch8_types::worker::WorkerRegistration {
        worker_id: worker_id.to_string(),
        handler_name: handler_name.to_string(),
        queue_name: queue_name.map(ToString::to_string),
        version: version.map(ToString::to_string),
        tenant_id: tenant_id.map(|t| t.as_str().to_string()),
        last_seen_at: chrono::Utc::now(),
    };
    if let Err(e) = state
        .storage
        .upsert_worker_registration(&registration)
        .await
    {
        tracing::warn!(
            error = %e,
            worker_id,
            handler_name,
            "failed to record worker registration"
        );
    }
}

/// Is this worker blocked by a `(tenant, handler)` version pin? Returns `true`
/// only when a pin exists and the worker's reported version doesn't satisfy it.
/// Best-effort: a lookup error never blocks the poll (returns `false`).
async fn version_pin_blocks(
    state: &AppState,
    tenant_id: Option<&orch8_types::ids::TenantId>,
    handler_name: &str,
    worker_version: Option<&str>,
) -> bool {
    let tenant = tenant_id.map_or("", orch8_types::ids::TenantId::as_str);
    match state
        .storage
        .get_worker_version_pin(tenant, handler_name)
        .await
    {
        Ok(Some(pin)) => !orch8_types::worker::version_satisfies(worker_version, &pin.min_version),
        Ok(None) => false,
        Err(e) => {
            tracing::warn!(error = %e, handler_name, "version pin lookup failed; allowing poll");
            false
        }
    }
}

async fn validate_and_record_capabilities(
    state: &AppState,
    scoped: Option<&orch8_types::ids::TenantId>,
    worker_id: &str,
    handler_name: &str,
    capabilities: Option<&orch8_types::continuity::RuntimeCapabilities>,
) -> Result<(), ApiError> {
    let Some(capabilities) = capabilities else {
        return Ok(());
    };
    crate::continuity::validate_runtime_registration(capabilities, chrono::Utc::now())?;
    if capabilities.runtime_id.to_string() != worker_id {
        return Err(ApiError::InvalidArgument(
            "worker_id must equal capabilities.runtime_id".into(),
        ));
    }
    if !capabilities
        .handlers
        .iter()
        .any(|handler| handler == handler_name)
    {
        return Err(ApiError::InvalidArgument(
            "capabilities.handlers must include handler_name".into(),
        ));
    }
    if let Some(tenant_id) = scoped {
        state
            .storage
            .upsert_runtime_capabilities(tenant_id, capabilities)
            .await
            .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?;
    }
    Ok(())
}

/// Bind a poll to the caller's credential. A browser-session principal may
/// only poll as its own `runtime_id`, with `kind = browser`, for handlers and
/// queues its token grants; a conflicting self-assertion is refused (403).
/// Its capability advertisement is clamped (trust at most `registered`,
/// handlers limited to the allowlist, no credential bindings, expiry at most
/// the token's) and synthesized when absent, so browser claims always go
/// through capability matching (and the browser no-secrets claim filter).
fn bind_poll_identity(
    binding: &crate::browser_sessions::OptionalBinding,
    worker_id: &str,
    handler_name: &str,
    queue_name: Option<&str>,
    capabilities: Option<orch8_types::continuity::RuntimeCapabilities>,
) -> Result<Option<orch8_types::continuity::RuntimeCapabilities>, ApiError> {
    use orch8_types::continuity::{RuntimeCapabilities, RuntimeTrustLevel};

    let Some(axum::Extension(binding)) = binding else {
        return Ok(capabilities);
    };
    crate::browser_sessions::enforce_bound_worker(
        &Some(axum::Extension(binding.clone())),
        worker_id,
    )?;
    if !binding.allows_handler(handler_name) {
        return Err(ApiError::Forbidden(format!(
            "handler {handler_name} is not granted to this browser session"
        )));
    }
    if let Some(queue) = queue_name
        && !binding.allows_queue(queue)
    {
        return Err(ApiError::Forbidden(format!(
            "queue {queue} is not granted to this browser session"
        )));
    }
    let now = chrono::Utc::now();
    let mut capabilities = match capabilities {
        Some(capabilities) => {
            if capabilities.kind != binding.kind || capabilities.runtime_id != binding.runtime_id {
                return Err(ApiError::Forbidden(
                    "capabilities kind/runtime_id conflict with the browser session binding".into(),
                ));
            }
            capabilities
        }
        None => RuntimeCapabilities {
            runtime_id: binding.runtime_id,
            kind: binding.kind,
            trust: RuntimeTrustLevel::Registered,
            handlers: Vec::new(),
            plugins: Vec::new(),
            credentials: Vec::new(),
            regions: Vec::new(),
            hardware: Vec::new(),
            offline_capable: false,
            connectivity: None,
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            labels: std::collections::BTreeMap::new(),
            observed_at: now,
            expires_at: now + chrono::Duration::minutes(4),
        },
    };
    capabilities.trust = capabilities.trust.min(RuntimeTrustLevel::Registered);
    capabilities
        .handlers
        .retain(|handler| binding.allows_handler(handler));
    if !capabilities
        .handlers
        .iter()
        .any(|handler| handler == handler_name)
    {
        capabilities.handlers.push(handler_name.to_owned());
    }
    capabilities.credentials.clear();
    capabilities.capsule_signing_public_key = None;
    capabilities.expires_at = capabilities.expires_at.min(binding.expires_at);
    Ok(Some(capabilities))
}

#[utoipa::path(post, path = "/workers/tasks/poll", tag = "workers",
    request_body = PollRequest,
    responses((status = 200, description = "Claimed worker tasks and lease hints", body = PollTasksResponse))
)]
pub(crate) async fn poll_tasks(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    binding: crate::browser_sessions::OptionalBinding,
    Json(mut req): Json<PollRequest>,
) -> Result<impl IntoResponse, ApiError> {
    req.capabilities = bind_poll_identity(
        &binding,
        &req.worker_id,
        &req.handler_name,
        None,
        req.capabilities.take(),
    )?;
    let limit = req.limit.min(1000);
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    validate_and_record_capabilities(
        &state,
        scoped.as_ref(),
        &req.worker_id,
        &req.handler_name,
        req.capabilities.as_ref(),
    )
    .await?;

    // Version pin: a worker below the (tenant, handler) min version is given no
    // tasks for that handler. Still record the registration so the operator can
    // see the stale-version worker is polling.
    if version_pin_blocks(
        &state,
        scoped.as_ref(),
        &req.handler_name,
        req.version.as_deref(),
    )
    .await
    {
        record_registration(
            &state,
            &req.worker_id,
            &req.handler_name,
            None,
            req.version.as_deref(),
            scoped.as_ref(),
        )
        .await;
        return Ok(poll_response(&state, Vec::new()));
    }

    // When a tenant is scoped, route through the tenant-aware claim so
    // the predicate is enforced INSIDE the lock window. The previous
    // "claim then filter" path would mark a foreign tenant's row claimed
    // with this worker_id, drop it from the response, and leave a ghost
    // `claimed` row invisible to its owning tenant until the stale-task
    // reaper reset it.
    let tasks = if let Some(capabilities) = req.capabilities.as_ref() {
        state
            .storage
            .claim_worker_tasks_matching(
                &req.handler_name,
                &req.worker_id,
                scoped.as_ref(),
                None,
                capabilities,
                limit,
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    } else if let Some(ref tid) = scoped {
        state
            .storage
            .claim_worker_tasks_for_tenant(&req.handler_name, &req.worker_id, tid, limit)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    } else {
        state
            .storage
            .claim_worker_tasks(&req.handler_name, &req.worker_id, limit)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    };

    record_registration(
        &state,
        &req.worker_id,
        &req.handler_name,
        None,
        req.version.as_deref(),
        scoped.as_ref(),
    )
    .await;
    note_placed_claims(&state, &tasks).await;

    Ok(poll_response(&state, tasks))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct QueuePollRequest {
    queue_name: String,
    handler_name: String,
    worker_id: String,
    #[serde(default = "default_poll_limit")]
    limit: u32,
    /// Optional worker build/deploy version, recorded on the worker registry.
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    capabilities: Option<orch8_types::continuity::RuntimeCapabilities>,
}

#[utoipa::path(post, path = "/workers/tasks/poll/queue", tag = "workers",
    request_body = QueuePollRequest,
    responses((status = 200, description = "Claimed worker tasks from queue and lease hints", body = PollTasksResponse))
)]
pub(crate) async fn poll_tasks_from_queue(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    binding: crate::browser_sessions::OptionalBinding,
    Json(mut req): Json<QueuePollRequest>,
) -> Result<impl IntoResponse, ApiError> {
    req.capabilities = bind_poll_identity(
        &binding,
        &req.worker_id,
        &req.handler_name,
        Some(&req.queue_name),
        req.capabilities.take(),
    )?;
    let limit = req.limit.min(1000);
    // Tenant-scoped claim path — see `poll_tasks` comment for the rationale.
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    validate_and_record_capabilities(
        &state,
        scoped.as_ref(),
        &req.worker_id,
        &req.handler_name,
        req.capabilities.as_ref(),
    )
    .await?;

    // Version pin (same as the default poll path).
    if version_pin_blocks(
        &state,
        scoped.as_ref(),
        &req.handler_name,
        req.version.as_deref(),
    )
    .await
    {
        record_registration(
            &state,
            &req.worker_id,
            &req.handler_name,
            Some(&req.queue_name),
            req.version.as_deref(),
            scoped.as_ref(),
        )
        .await;
        return Ok(poll_response(&state, Vec::new()));
    }

    let tasks = if let Some(capabilities) = req.capabilities.as_ref() {
        state
            .storage
            .claim_worker_tasks_matching(
                &req.handler_name,
                &req.worker_id,
                scoped.as_ref(),
                Some(&req.queue_name),
                capabilities,
                limit,
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    } else if let Some(ref tid) = scoped {
        state
            .storage
            .claim_worker_tasks_from_queue_for_tenant(
                &req.queue_name,
                &req.handler_name,
                &req.worker_id,
                tid,
                limit,
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    } else {
        state
            .storage
            .claim_worker_tasks_from_queue(
                &req.queue_name,
                &req.handler_name,
                &req.worker_id,
                limit,
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    };

    record_registration(
        &state,
        &req.worker_id,
        &req.handler_name,
        Some(&req.queue_name),
        req.version.as_deref(),
        scoped.as_ref(),
    )
    .await;
    note_placed_claims(&state, &tasks).await;

    Ok(poll_response(&state, tasks))
}

/// Aggregated view of one worker on the fleet, grouped from its
/// per-handler registrations.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct WorkerInfo {
    pub worker_id: String,
    /// Handler names this worker has polled for.
    pub handlers: Vec<String>,
    /// Named queues this worker has polled (empty when default queue only).
    pub queues: Vec<String>,
    /// Most recently reported version string, if any.
    pub version: Option<String>,
    pub last_seen_at: chrono::DateTime<chrono::Utc>,
    /// True when the worker polled within the liveness window.
    pub alive: bool,
    /// Number of tasks currently claimed by this worker.
    pub in_flight: i64,
}

#[derive(Deserialize)]
pub(crate) struct ListWorkersQuery {
    /// Liveness window in seconds (default 60).
    #[serde(default = "default_alive_within_secs")]
    alive_within_secs: i64,
    /// Include workers whose last poll is older than the liveness window.
    #[serde(default)]
    include_stale: bool,
}

const fn default_alive_within_secs() -> i64 {
    60
}

#[utoipa::path(get, path = "/workers", tag = "workers",
    params(
        ("alive_within_secs" = Option<i64>, Query, description = "Liveness window in seconds (default 60)"),
        ("include_stale" = Option<bool>, Query, description = "Include workers not seen within the window"),
    ),
    responses((status = 200, description = "Worker fleet with liveness", body = Vec<WorkerInfo>))
)]
pub(crate) async fn list_workers(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<ListWorkersQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let registrations = state
        .storage
        .list_worker_registrations(None)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_registration"))?;
    let in_flight: std::collections::HashMap<String, i64> = state
        .storage
        .claimed_task_counts_by_worker(scoped.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .into_iter()
        .collect();

    let mut by_worker: std::collections::BTreeMap<String, WorkerInfo> =
        std::collections::BTreeMap::new();
    for reg in registrations {
        // Tenant-scoped callers see their own workers plus unscoped ones.
        if let Some(ref tid) = scoped
            && reg.tenant_id.as_deref().is_some_and(|t| t != tid.as_str())
        {
            continue;
        }
        let entry = by_worker
            .entry(reg.worker_id.clone())
            .or_insert_with(|| WorkerInfo {
                worker_id: reg.worker_id.clone(),
                handlers: Vec::new(),
                queues: Vec::new(),
                version: None,
                last_seen_at: reg.last_seen_at,
                alive: false,
                in_flight: 0,
            });
        if !entry.handlers.contains(&reg.handler_name) {
            entry.handlers.push(reg.handler_name);
        }
        if let Some(queue) = reg.queue_name
            && !entry.queues.contains(&queue)
        {
            entry.queues.push(queue);
        }
        // Registrations arrive newest-first, so the first version wins.
        if entry.version.is_none() {
            entry.version = reg.version;
        }
        if reg.last_seen_at > entry.last_seen_at {
            entry.last_seen_at = reg.last_seen_at;
        }
    }

    let alive_cutoff =
        chrono::Utc::now() - chrono::Duration::seconds(query.alive_within_secs.max(1));
    let mut workers: Vec<WorkerInfo> = by_worker
        .into_values()
        .map(|mut w| {
            w.alive = w.last_seen_at >= alive_cutoff;
            w.in_flight = in_flight.get(&w.worker_id).copied().unwrap_or(0);
            w.handlers.sort_unstable();
            w.queues.sort_unstable();
            w
        })
        .filter(|w| query.include_stale || w.alive)
        .collect();
    workers.sort_by_key(|w| std::cmp::Reverse(w.last_seen_at));

    Ok(Json(workers))
}

/// Catalog of every handler name the engine can serve.
#[derive(serde::Serialize, ToSchema)]
pub(crate) struct HandlerCatalog {
    /// Handlers executed in-process by the engine.
    pub builtin: Vec<String>,
    /// Handler names served by external workers (from the worker registry).
    pub external: Vec<String>,
}

#[utoipa::path(get, path = "/handlers", tag = "workers",
    responses((status = 200, description = "Handler catalog", body = HandlerCatalog))
)]
pub(crate) async fn list_handlers(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
) -> Result<impl IntoResponse, ApiError> {
    let registrations = state
        .storage
        .list_worker_registrations(None)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_registration"))?;
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let mut external: Vec<String> = registrations
        .into_iter()
        .filter(|reg| {
            scoped
                .as_ref()
                .is_none_or(|tid| reg.tenant_id.as_deref().is_none_or(|t| t == tid.as_str()))
        })
        .map(|reg| reg.handler_name)
        .collect();
    external.sort_unstable();
    external.dedup();

    Ok(Json(HandlerCatalog {
        builtin: state.builtin_handlers.as_ref().clone(),
        external,
    }))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CompleteRequest {
    worker_id: String,
    claim_epoch: u64,
    output: serde_json::Value,
    /// Optional log lines the worker captured while running this task.
    #[serde(default)]
    logs: Vec<orch8_types::step_log::StepLogEntry>,
    /// W3C `traceparent` of the worker's span (alternatively sent as the
    /// `traceparent` HTTP header) so the completion joins the dispatch trace.
    #[serde(default)]
    traceparent: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactUploadQuery {
    worker_id: String,
    claim_epoch: u64,
    #[serde(default)]
    file_name: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
}

#[derive(Debug, serde::Serialize, ToSchema)]
pub(crate) struct WorkerArtifactReceipt {
    artifact: orch8_types::artifact::ArtifactRef,
    upload_id: Uuid,
    file_name: Option<String>,
    sha256: String,
    size: u64,
}

/// Upload a task-owned file before completion. The client chooses `upload_id`,
/// so a phone can safely retry after suspension or a network transition.
#[utoipa::path(
    post,
    path = "/workers/tasks/{id}/artifacts/{upload_id}",
    tag = "workers",
    params(
        ("id" = Uuid, Path, description = "Worker task ID"),
        ("upload_id" = Uuid, Path, description = "Stable client idempotency key"),
        ("worker_id" = String, Query),
        ("claim_epoch" = u64, Query),
        ("file_name" = Option<String>, Query),
        ("sha256" = Option<String>, Query),
    ),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses(
        (status = 201, description = "Artifact stored", body = WorkerArtifactReceipt),
        (status = 409, description = "Lease changed or upload ID reused with other bytes"),
    )
)]
#[allow(clippy::too_many_lines)] // lease + ownership fence + bounded upload in one handler
pub(crate) async fn upload_task_artifact(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((task_id, upload_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<ArtifactUploadQuery>,
    request: Request,
) -> Result<impl IntoResponse, ApiError> {
    use sha2::{Digest, Sha256};

    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    let instance = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("worker_task {task_id}"),
    )?;
    let claim = WorkerClaim::new(query.worker_id, query.claim_epoch);
    if task.state != WorkerTaskState::Claimed
        || task.worker_id.as_deref() != Some(claim.worker_id.as_str())
        || task.claim_epoch != claim.claim_epoch
    {
        record_stale_rejection(
            &state,
            task_id,
            &claim,
            "artifact upload rejected: lease changed",
        )
        .await;
        return Err(ApiError::Conflict("worker task lease changed".into()));
    }
    enforce_ownership_fence(
        &state,
        &instance.tenant_id,
        &task,
        &claim,
        "artifact upload",
    )
    .await?;
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("application/octet-stream")
        .to_string();
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_WORKER_ARTIFACT_UPLOAD_BYTES)
    {
        return Err(ApiError::PayloadTooLarge(
            "artifact exceeds the 10 MiB request limit".into(),
        ));
    }
    let body = axum::body::to_bytes(request.into_body(), MAX_WORKER_ARTIFACT_UPLOAD_BYTES)
        .await
        .map_err(|_| {
            ApiError::PayloadTooLarge("artifact exceeds the 10 MiB request limit".into())
        })?;
    let hash = Sha256::digest(&body);
    let mut digest = String::with_capacity(64);
    for byte in hash {
        digest.push(char::from(SHA256_HEX[usize::from(byte >> 4)]));
        digest.push(char::from(SHA256_HEX[usize::from(byte & 0x0f)]));
    }
    if query
        .sha256
        .as_deref()
        .is_some_and(|expected| !expected.eq_ignore_ascii_case(&digest))
    {
        return Err(ApiError::InvalidArgument("artifact sha256 mismatch".into()));
    }
    let artifact = state
        .storage
        .put_artifact_with_id(task.instance_id, upload_id, &content_type, body)
        .await
        .map_err(|error| ApiError::from_storage(error, "artifact"))?;
    if !state
        .storage
        .heartbeat_worker_task(task_id, &claim)
        .await
        .map_err(|error| ApiError::from_storage(error, "worker_task"))?
    {
        record_stale_rejection(
            &state,
            task_id,
            &claim,
            "artifact upload completed after lease changed",
        )
        .await;
        return Err(ApiError::Conflict("worker task lease changed".into()));
    }
    Ok((
        StatusCode::CREATED,
        Json(WorkerArtifactReceipt {
            size: artifact.size,
            artifact,
            upload_id,
            file_name: query.file_name,
            sha256: digest,
        }),
    ))
}

/// Persist worker-reported step logs (best-effort — a write failure must never
/// fail the completion/failure path).
async fn persist_reported_logs(
    state: &AppState,
    instance_id: orch8_types::ids::InstanceId,
    block_id: &orch8_types::ids::BlockId,
    logs: &[orch8_types::step_log::StepLogEntry],
) {
    if logs.is_empty() {
        return;
    }
    if let Err(e) = state
        .storage
        .append_step_logs(instance_id, block_id, logs)
        .await
    {
        tracing::warn!(error = %e, "failed to persist worker-reported step logs");
    }
}

#[utoipa::path(post, path = "/workers/tasks/{id}/complete", tag = "workers",
    params(("id" = Uuid, Path, description = "Worker task ID")),
    request_body = CompleteRequest,
    responses(
        (status = 200, description = "Task completed"),
        (status = 404, description = "Worker task not found"),
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn complete_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    binding: crate::browser_sessions::OptionalBinding,
    Path(task_id): Path<Uuid>,
    headers: axum::http::HeaderMap,
    Json(req): Json<CompleteRequest>,
) -> Result<impl IntoResponse, ApiError> {
    crate::browser_sessions::enforce_bound_worker(&binding, &req.worker_id)?;
    // Fetch task first to verify tenant access via its instance.
    let pre_task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    // Verify tenant access via the task's owning instance. If the instance is
    // missing we cannot confirm ownership — treat as NotFound so a tenant-scoped
    // caller cannot operate on orphaned tasks from another tenant.
    let inst = state
        .storage
        .get_instance(pre_task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", pre_task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &inst.tenant_id,
        &format!("worker_task {task_id}"),
    )?;
    // W3C trace context echoed by the worker continues the dispatch trace.
    let traceparent = req.traceparent.clone().or_else(|| {
        headers
            .get("traceparent")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    });
    orch8_engine::trace_context::completion_span(
        task_id,
        pre_task.instance_id.into_uuid(),
        traceparent.as_deref(),
    )
    .in_scope(|| tracing::info!(block_id = %pre_task.block_id, "worker completion received"));
    let claim = WorkerClaim::new(req.worker_id.clone(), req.claim_epoch);
    let same_lease = pre_task.worker_id.as_deref() == Some(claim.worker_id.as_str())
        && pre_task.claim_epoch == claim.claim_epoch;
    // Idempotent retry: the task row is marked completed BEFORE the output
    // save + instance transition below (separate writes). If the process
    // died in between, the instance sat in Waiting forever and the worker's
    // retry got a 409. A retry by the SAME lease holder of an already
    // completed task now re-runs only the (guarded) transition half.
    let completion_retry = pre_task.state == WorkerTaskState::Completed && same_lease;
    if !completion_retry && (pre_task.state != WorkerTaskState::Claimed || !same_lease) {
        record_stale_rejection(&state, task_id, &claim, "complete rejected: lease changed").await;
        return Err(ApiError::Conflict("worker task lease changed".into()));
    }
    if !completion_retry {
        enforce_ownership_fence(&state, &inst.tenant_id, &pre_task, &claim, "complete").await?;
    }
    // Browser output is untrusted page data (DOM, forms, user input): bound
    // its size before anything is committed.
    let from_browser = binding.is_some()
        || pre_task.claimed_runtime_kind == Some(orch8_types::continuity::RuntimeKind::Browser);
    if from_browser && !completion_retry {
        let bytes = serde_json::to_vec(&req.output)
            .map_err(|error| ApiError::InvalidArgument(error.to_string()))?
            .len();
        if bytes > state.browser_output_max_bytes {
            return Err(ApiError::PayloadTooLarge(format!(
                "browser step output is {bytes} bytes; the maximum is {}",
                state.browser_output_max_bytes
            )));
        }
    }
    // The committed output wins on a retry (the worker may resend a
    // regenerated payload); a first completion uses the request's.
    let mut req = req;
    if completion_retry && let Some(stored) = pre_task.output.clone() {
        req.output = stored;
    }

    // Enforce `max_context_bytes` on the post-merge context BEFORE anything
    // is committed. Every other context writer (create, patch, fork) checks
    // it; the worker merge bypassed it, so one oversized worker output could
    // grow `context.data` without bound.
    if let Some(obj) = req.output.as_object() {
        let mut projected = inst.context.clone();
        if !projected.data.is_object() {
            projected.data = serde_json::Value::Object(serde_json::Map::new());
        }
        if let Some(data_obj) = projected.data.as_object_mut() {
            for (k, v) in obj {
                data_obj.insert(k.clone(), v.clone());
            }
        }
        projected.check_size(state.max_context_bytes)?;
    }

    let tenant_id = inst.tenant_id.clone();
    let tenant_for_cb = Some(inst.tenant_id);

    if !completion_retry {
        orch8_engine::effect_guard::commit_external_worker_effect(
            state.storage.as_ref(),
            &tenant_id,
            &pre_task,
            &req.output,
        )
        .await
        .map_err(|error| ApiError::Conflict(error.to_string()))?;

        let updated = state
            .storage
            .complete_worker_task(task_id, &claim, &req.output)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

        if !updated {
            record_stale_rejection(
                &state,
                task_id,
                &claim,
                "complete rejected: lease changed during commit",
            )
            .await;
            return Err(ApiError::Conflict("worker task lease changed".into()));
        }
    }

    // Persist any worker-reported logs for this step (once — a completion
    // retry already persisted them on the first attempt).
    if !completion_retry {
        persist_reported_logs(&state, pre_task.instance_id, &pre_task.block_id, &req.logs).await;
        record_output_provenance(&state, &tenant_id, &pre_task, &req.output).await;
    }

    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;

    // Device-mesh delegation: integrate the result into the parent (block
    // output + `context.data.delegations.<id>` + wake) instead of treating
    // the mailbox task as one of the parent's own steps.
    if orch8_engine::delegation::is_delegation_task(&task) {
        // A completion retry (the first response was lost) integrates only
        // when the first attempt did not get that far: the result block
        // output is the integration's first write.
        let integrated = completion_retry
            && state
                .storage
                .get_block_output(task.instance_id, &task.block_id)
                .await
                .map_err(|e| ApiError::from_storage(e, "block_output"))?
                .is_some();
        if !integrated {
            orch8_engine::delegation::integrate_delegation_outcome(
                state.storage.as_ref(),
                &task,
                Ok(&req.output),
            )
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
        }
        return Ok(StatusCode::OK);
    }

    let output_json = serde_json::to_string(&req.output).map_err(|e| {
        ApiError::InvalidArgument(format!("failed to serialize worker output: {e}"))
    })?;
    let task_block_id = task.block_id.clone();
    let block_output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: task.instance_id,
        block_id: task.block_id,
        output: req.output,
        output_ref: None,
        output_size: u32::try_from(output_json.len()).unwrap_or(u32::MAX),
        attempt: task.attempt,
        created_at: chrono::Utc::now(),
    };

    // Re-read instance state before propagating. Between task-claim and
    // completion a cancel/admin-fail could have terminated the instance.
    let Some(mut instance) = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
    else {
        return Ok(StatusCode::OK);
    };
    if instance.state.is_terminal() || instance.state == InstanceState::Paused {
        tracing::info!(
            instance_id = %task.instance_id,
            state = %instance.state,
            block_id = %task_block_id,
            "external worker completion arrived for terminal/paused instance — task accepted, transition skipped"
        );
        // Still roll the external work into the breaker — the handler's
        // dependency did succeed, and withholding the signal would leave
        // a spurious failure in the breaker window.
        if let (Some(cb), Some(tenant)) = (state.circuit_breakers.as_ref(), tenant_for_cb.as_ref())
            && orch8_engine::circuit_breaker::is_breaker_tracked(&task.handler_name)
        {
            cb.record_success(tenant, &task.handler_name);
        }
        return Ok(StatusCode::OK);
    }

    // Merge step output into context.data BEFORE the atomic write.
    let merged_context = if let Some(obj) = block_output.output.as_object() {
        if instance.context.data.is_null() || !instance.context.data.is_object() {
            instance.context.data = serde_json::Value::Object(serde_json::Map::new());
        }
        if let Some(data_obj) = instance.context.data.as_object_mut() {
            for (k, v) in obj {
                data_obj.insert(k.clone(), v.clone());
            }
            true
        } else {
            false
        }
    } else {
        false
    };

    // Find the execution node that this worker task corresponds to.
    // We do this BEFORE the atomic write so we can include the node
    // completion in the same transaction, closing the race where the
    // scheduler claims the instance between output-save and node-complete.
    let tree = state
        .storage
        .get_execution_tree(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    let node = tree.iter().find(|n| {
        n.block_id == task_block_id && matches!(n.state, NodeState::Running | NodeState::Waiting)
    });

    // On a completion retry, only finish a transition that demonstrably did
    // not happen yet: the step's node is still live (tree path), or — flat
    // path, no tree — the instance is still Waiting on this very step.
    // Otherwise the first attempt already transitioned; re-running the
    // non-atomic fallback would save a duplicate output and re-schedule.
    if completion_retry {
        let pending = if tree.is_empty() {
            instance.state == InstanceState::Waiting
                && instance
                    .context
                    .runtime
                    .current_step
                    .as_ref()
                    .is_none_or(|step| *step == task_block_id)
        } else {
            node.is_some()
        };
        if !pending {
            return Ok(StatusCode::OK);
        }
        tracing::info!(
            task_id = %task_id,
            instance_id = %task.instance_id,
            block_id = %task_block_id,
            "worker completion retry: task already completed, finishing the instance transition"
        );
    }

    let cas_err = if let Some(node) = node {
        let result = if merged_context {
            state
                .storage
                .save_output_complete_node_merge_context_and_transition(
                    &block_output,
                    node.id,
                    task.instance_id,
                    &instance.context,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        } else {
            state
                .storage
                .save_output_complete_node_and_transition(
                    &block_output,
                    node.id,
                    task.instance_id,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        };
        match result {
            Ok(()) => false,
            Err(orch8_types::error::StorageError::TerminalTarget { .. }) => true,
            Err(e) => return Err(ApiError::from_storage(e, "worker_task")),
        }
    } else {
        // No live node. Flat (step-only) sequences run on the scheduler's
        // fast path and never build an execution tree: saving the output and
        // re-scheduling the instance *is* their completion path. Only a tree
        // instance without a live node for this step is unexpected (the node
        // was cancelled or already settled concurrently).
        if tree.is_empty() {
            tracing::debug!(
                instance_id = %task.instance_id,
                block_id = %task_block_id,
                "worker completion for a flat (tree-less) instance"
            );
        } else {
            tracing::warn!(
                instance_id = %task.instance_id,
                block_id = %task_block_id,
                "worker completion: execution node not in Running/Waiting state — falling back to non-atomic transition"
            );
        }
        let result = if merged_context {
            state
                .storage
                .save_output_merge_context_and_transition(
                    &block_output,
                    task.instance_id,
                    &instance.context,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        } else {
            state
                .storage
                .save_output_and_transition(
                    &block_output,
                    task.instance_id,
                    InstanceState::Scheduled,
                    Some(chrono::Utc::now()),
                )
                .await
        };
        match result {
            Ok(()) => false,
            Err(orch8_types::error::StorageError::TerminalTarget { .. }) => true,
            Err(e) => return Err(ApiError::from_storage(e, "worker_task")),
        }
    };

    if cas_err {
        tracing::info!(
            instance_id = %task.instance_id,
            block_id = %task_block_id,
            "worker completion CAS failed — instance became terminal/paused after read"
        );
    }

    // Roll external-worker success into the same breaker registry the
    // in-process step-exec path uses. Skip-listed handlers (pure control-flow
    // built-ins) bypass bookkeeping — they have no external dep to break.
    if let (Some(cb), Some(tenant)) = (state.circuit_breakers.as_ref(), tenant_for_cb.as_ref())
        && orch8_engine::circuit_breaker::is_breaker_tracked(&task.handler_name)
    {
        cb.record_success(tenant, &task.handler_name);
    }

    Ok(StatusCode::OK)
}

/// Record which runtime produced a step output (kind + id) in the audit
/// trail and, for continuity-enrolled instances, the provenance chain — as
/// evidence alongside the output, never by mutating the output JSON.
/// Best-effort: provenance failures are logged, never fail the completion.
async fn record_output_provenance(
    state: &AppState,
    tenant_id: &orch8_types::ids::TenantId,
    task: &orch8_types::worker::WorkerTask,
    output: &serde_json::Value,
) {
    let Some(kind) = task.claimed_runtime_kind else {
        return;
    };
    let encoded = serde_json::to_vec(output).unwrap_or_default();
    let output_sha256 = crate::continuity::hex_sha256(&encoded);
    let runtime_id = task.worker_id.clone().unwrap_or_default();
    let details = serde_json::json!({
        "task_id": task.id,
        "runtime_kind": kind,
        "runtime_id": runtime_id,
        "claim_epoch": task.claim_epoch,
        "effect_id": task.effect_id,
        "output_sha256": output_sha256,
        "output_bytes": encoded.len(),
        "untrusted_page_data": kind == orch8_types::continuity::RuntimeKind::Browser,
    });
    let entry = orch8_types::audit::AuditLogEntry {
        id: Uuid::now_v7(),
        instance_id: task.instance_id,
        tenant_id: tenant_id.clone(),
        event_type: "worker_output_provenance".into(),
        from_state: None,
        to_state: None,
        block_id: Some(task.block_id.as_str().to_owned()),
        details: details.clone(),
        created_at: chrono::Utc::now(),
    };
    if let Err(error) = state.storage.append_audit_log(&entry).await {
        tracing::warn!(task_id = %task.id, %error, "failed to record worker output provenance");
    }
    match state
        .storage
        .get_continuity_execution_by_instance(tenant_id, task.instance_id)
        .await
    {
        Ok(Some(execution)) => {
            let digest = crate::continuity::hex_sha256(details.to_string().as_bytes());
            if let Err(error) = crate::continuity::append_provenance_digest(
                state,
                &execution,
                "remote_step_output",
                &format!(
                    "step {} output from {} runtime {runtime_id}",
                    task.block_id,
                    kind.as_str()
                ),
                &digest,
            )
            .await
            {
                tracing::warn!(task_id = %task.id, ?error, "failed to append output provenance");
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(task_id = %task.id, %error, "provenance lookup failed");
        }
    }
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct FailRequest {
    worker_id: String,
    claim_epoch: u64,
    message: String,
    #[serde(default)]
    retryable: bool,
    /// Optional log lines the worker captured while running this task.
    #[serde(default)]
    logs: Vec<orch8_types::step_log::StepLogEntry>,
}

#[utoipa::path(post, path = "/workers/tasks/{id}/fail", tag = "workers",
    params(("id" = Uuid, Path, description = "Worker task ID")),
    request_body = FailRequest,
    responses(
        (status = 200, description = "Task failed"),
        (status = 404, description = "Worker task not found"),
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn fail_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    binding: crate::browser_sessions::OptionalBinding,
    Path(task_id): Path<Uuid>,
    Json(req): Json<FailRequest>,
) -> Result<impl IntoResponse, ApiError> {
    crate::browser_sessions::enforce_bound_worker(&binding, &req.worker_id)?;
    // Fetch task first to verify tenant access via its instance.
    let pre_task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    // Verify tenant access via the task's owning instance. If the instance is
    // missing we cannot confirm ownership — treat as NotFound so a tenant-scoped
    // caller cannot operate on orphaned tasks from another tenant.
    let inst = state
        .storage
        .get_instance(pre_task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", pre_task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &inst.tenant_id,
        &format!("worker_task {task_id}"),
    )?;
    let claim = WorkerClaim::new(req.worker_id.clone(), req.claim_epoch);
    let same_lease = pre_task.worker_id.as_deref() == Some(claim.worker_id.as_str())
        && pre_task.claim_epoch == claim.claim_epoch;
    // Idempotent re-report (e.g. a device resending recorded outcomes after
    // a restart): the same lease holder failing an already-failed task is a
    // no-op success, mirroring the completion retry path.
    if pre_task.state == WorkerTaskState::Failed && same_lease {
        return Ok(StatusCode::OK);
    }
    if pre_task.state != WorkerTaskState::Claimed || !same_lease {
        record_stale_rejection(&state, task_id, &claim, "fail rejected: lease changed").await;
        return Err(ApiError::Conflict("worker task lease changed".into()));
    }
    enforce_ownership_fence(&state, &inst.tenant_id, &pre_task, &claim, "fail").await?;

    // One fenced transaction (shared with the reaper, timeouts and release):
    // receipt → `unknown`, then the task fails and the instance advances per
    // the step's retry policy — or only the task fails for a delegation or a
    // terminal/paused instance. A racing lease change applies nothing.
    let failed = orch8_engine::worker_lease::fail_worker_task(
        state.storage.as_ref(),
        &inst,
        &pre_task,
        &claim,
        &req.message,
        req.retryable,
    )
    .await
    .map_err(|error| match error {
        orch8_engine::error::EngineError::Storage(error) => {
            ApiError::from_storage(error, "worker_task")
        }
        other => ApiError::Conflict(other.to_string()),
    })?;

    // Persist any worker-reported logs for this step (best-effort).
    persist_reported_logs(&state, pre_task.instance_id, &pre_task.block_id, &req.logs).await;

    if !failed {
        record_stale_rejection(
            &state,
            task_id,
            &claim,
            "fail rejected: lease changed during commit",
        )
        .await;
        return Err(ApiError::Conflict("worker task lease changed".into()));
    }

    // Roll external-worker failure into the breaker. Done unconditionally (for
    // both retryable and non-retryable) to mirror in-process step-exec, which
    // records a failure on every Err arm. Control-flow built-ins stay
    // skip-listed.
    if let Some(cb) = state.circuit_breakers.as_ref()
        && !orch8_engine::delegation::is_delegation_task(&pre_task)
        && orch8_engine::circuit_breaker::is_breaker_tracked(&pre_task.handler_name)
    {
        cb.record_failure(&inst.tenant_id, &pre_task.handler_name);
    }

    Ok(StatusCode::OK)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct HeartbeatRequest {
    worker_id: String,
    claim_epoch: u64,
    #[serde(default)]
    checkpoint: Option<serde_json::Value>,
    #[serde(default)]
    checkpoint_seq: Option<u64>,
}

#[utoipa::path(post, path = "/workers/tasks/{id}/heartbeat", tag = "workers",
    params(("id" = Uuid, Path, description = "Worker task ID")),
    request_body = HeartbeatRequest,
    responses(
        (status = 200, description = "Heartbeat/checkpoint updated; response contains checkpoint_seq"),
        (status = 404, description = "Worker task is not currently owned by this worker"),
        (status = 409, description = "Worker lost ownership or checkpoint sequence is stale"),
    )
)]
pub(crate) async fn heartbeat_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    binding: crate::browser_sessions::OptionalBinding,
    Path(task_id): Path<Uuid>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<impl IntoResponse, ApiError> {
    crate::browser_sessions::enforce_bound_worker(&binding, &req.worker_id)?;
    // Fetch task first to verify tenant access via its instance.
    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    let inst = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "task_instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("task_instance {}", task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &inst.tenant_id,
        &format!("worker_task {task_id}"),
    )?;

    let claim = WorkerClaim::new(req.worker_id.clone(), req.claim_epoch);
    enforce_ownership_fence(&state, &inst.tenant_id, &task, &claim, "heartbeat").await?;

    let next_checkpoint_seq = if let Some(checkpoint) = req.checkpoint {
        const MAX_ACTIVITY_CHECKPOINT_BYTES: usize = 256 * 1024;
        let checkpoint_bytes = serde_json::to_vec(&checkpoint)
            .map_err(|error| ApiError::InvalidArgument(error.to_string()))?
            .len();
        if checkpoint_bytes > MAX_ACTIVITY_CHECKPOINT_BYTES {
            return Err(ApiError::PayloadTooLarge(format!(
                "activity checkpoint is {checkpoint_bytes} bytes; maximum is {MAX_ACTIVITY_CHECKPOINT_BYTES}"
            )));
        }
        let expected_seq = req.checkpoint_seq.ok_or_else(|| {
            ApiError::InvalidArgument("checkpoint_seq is required with checkpoint".into())
        })?;
        state
            .storage
            .checkpoint_worker_task(task_id, &claim, expected_seq, &checkpoint)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?
    } else {
        let updated = state
            .storage
            .heartbeat_worker_task(task_id, &claim)
            .await
            .map_err(|e| ApiError::from_storage(e, "worker_task"))?;
        updated.then_some(task.checkpoint_seq)
    };

    let Some(checkpoint_seq) = next_checkpoint_seq else {
        record_stale_rejection(
            &state,
            task_id,
            &claim,
            "heartbeat/checkpoint rejected: lease or sequence changed",
        )
        .await;
        return Err(ApiError::Conflict(
            "worker task ownership or checkpoint sequence changed".into(),
        ));
    };

    Ok(Json(
        serde_json::json!({ "checkpoint_seq": checkpoint_seq }),
    ))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct ReleaseRequest {
    worker_id: String,
    claim_epoch: u64,
    /// Whether the handler already started. `false`: the task goes straight
    /// back to `pending` and its effect receipt is untouched. `true`: treated
    /// like a lease expiry after start (a side-effecting step's receipt
    /// becomes `unknown` and the step's retry policy decides).
    #[serde(default)]
    started: bool,
}

/// Voluntarily give a claimed task back (tab closing, app backgrounding).
/// Safe to call from `fetch(url, {keepalive: true})` during `pagehide`.
#[utoipa::path(post, path = "/workers/tasks/{id}/release", tag = "workers",
    params(("id" = Uuid, Path, description = "Worker task ID")),
    request_body = ReleaseRequest,
    responses(
        (status = 204, description = "Task released"),
        (status = 404, description = "Worker task not found"),
        (status = 409, description = "Caller no longer holds this lease (or ownership changed)"),
    )
)]
pub(crate) async fn release_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    binding: crate::browser_sessions::OptionalBinding,
    Path(task_id): Path<Uuid>,
    Json(req): Json<ReleaseRequest>,
) -> Result<StatusCode, ApiError> {
    crate::browser_sessions::enforce_bound_worker(&binding, &req.worker_id)?;
    let task = state
        .storage
        .get_worker_task(task_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?
        .ok_or_else(|| ApiError::NotFound(format!("worker_task {task_id}")))?;
    let inst = state
        .storage
        .get_instance(task.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", task.instance_id)))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &inst.tenant_id,
        &format!("worker_task {task_id}"),
    )?;
    let claim = WorkerClaim::new(req.worker_id, req.claim_epoch);
    if task.state != WorkerTaskState::Claimed
        || task.worker_id.as_deref() != Some(claim.worker_id.as_str())
        || task.claim_epoch != claim.claim_epoch
    {
        record_stale_rejection(&state, task_id, &claim, "release rejected: lease changed").await;
        return Err(ApiError::Conflict("worker task lease changed".into()));
    }
    enforce_ownership_fence(&state, &inst.tenant_id, &task, &claim, "release").await?;
    let released = orch8_engine::worker_lease::release_worker_task(
        state.storage.as_ref(),
        &inst,
        &task,
        &claim,
        req.started,
    )
    .await
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    if !released {
        record_stale_rejection(
            &state,
            task_id,
            &claim,
            "release rejected: lease changed during commit",
        )
        .await;
        return Err(ApiError::Conflict("worker task lease changed".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub(crate) struct ListTasksQuery {
    tenant_id: Option<String>,
    state: Option<String>,
    handler_name: Option<String>,
    worker_id: Option<String>,
    queue_name: Option<String>,
    #[serde(default = "default_list_limit")]
    limit: u32,
    #[serde(default)]
    offset: u64,
}

const fn default_list_limit() -> u32 {
    50
}

fn parse_states(raw: &str) -> Result<Vec<WorkerTaskState>, ApiError> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(|s| match s.trim() {
            "pending" => Ok(WorkerTaskState::Pending),
            "claimed" => Ok(WorkerTaskState::Claimed),
            "completed" => Ok(WorkerTaskState::Completed),
            "failed" => Ok(WorkerTaskState::Failed),
            other => Err(ApiError::InvalidArgument(format!(
                "unknown worker task state: {other}"
            ))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_worker_state() {
        assert_eq!(
            parse_states("claimed").unwrap(),
            vec![WorkerTaskState::Claimed]
        );
    }

    #[test]
    fn parse_multiple_worker_states() {
        assert_eq!(
            parse_states("pending,claimed,completed").unwrap(),
            vec![
                WorkerTaskState::Pending,
                WorkerTaskState::Claimed,
                WorkerTaskState::Completed
            ]
        );
    }

    #[test]
    fn parse_worker_states_with_whitespace() {
        assert_eq!(
            parse_states(" pending , failed ").unwrap(),
            vec![WorkerTaskState::Pending, WorkerTaskState::Failed]
        );
    }

    #[test]
    fn parse_all_worker_states() {
        let all = "pending,claimed,completed,failed";
        assert_eq!(
            parse_states(all).unwrap(),
            vec![
                WorkerTaskState::Pending,
                WorkerTaskState::Claimed,
                WorkerTaskState::Completed,
                WorkerTaskState::Failed,
            ]
        );
    }

    #[test]
    fn parse_empty_worker_string_returns_empty() {
        assert_eq!(parse_states("").unwrap(), Vec::<WorkerTaskState>::new());
    }

    #[test]
    fn parse_unknown_worker_state_errors() {
        let err = parse_states("claimed,bogus").unwrap_err();
        assert!(err.to_string().contains("unknown worker task state: bogus"));
    }

    #[test]
    fn default_list_limit_is_50() {
        assert_eq!(default_list_limit(), 50);
    }
}

#[utoipa::path(get, path = "/workers/tasks", tag = "workers",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("state" = Option<String>, Query, description = "Comma-separated worker task states"),
        ("handler_name" = Option<String>, Query, description = "Filter by handler name"),
        ("worker_id" = Option<String>, Query, description = "Filter by claiming worker id"),
        ("queue_name" = Option<String>, Query, description = "Filter by queue name"),
        ("limit" = u32, Query, description = "Max rows to return (≤ 1000)"),
        ("offset" = u32, Query, description = "Skip N rows"),
    ),
    responses((status = 200, description = "Worker tasks", body = Vec<orch8_types::worker::WorkerTask>))
)]
pub(crate) async fn list_tasks(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<ListTasksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let states = query.state.as_deref().map(parse_states).transpose()?;
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref());

    let filter = WorkerTaskFilter {
        tenant_id: scoped_tenant,
        states,
        handler_name: query.handler_name,
        worker_id: query.worker_id,
        queue_name: query.queue_name,
        instance_id: None,
    };

    let pagination = Pagination {
        limit: query.limit.min(1000),
        offset: query.offset,
        sort_ascending: false,
    };

    let tasks = state
        .storage
        .list_worker_tasks(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    Ok(Json(tasks))
}

#[utoipa::path(get, path = "/workers/tasks/stats", tag = "workers",
    responses((status = 200, description = "Aggregate worker task stats", body = orch8_types::worker_filter::WorkerTaskStats))
)]
pub(crate) async fn task_stats(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let result = state
        .storage
        .worker_task_stats(scoped_tenant.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_task"))?;

    Ok(Json(result))
}
