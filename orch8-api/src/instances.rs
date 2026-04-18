use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::context::ExecutionContext;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::signal::{Signal, SignalType};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/instances", post(create_instance).get(list_instances))
        .route("/instances/batch", post(create_instances_batch))
        .route("/instances/{id}", get(get_instance))
        .route("/instances/{id}/state", patch(update_state))
        .route("/instances/{id}/context", patch(update_context))
        .route("/instances/{id}/signals", post(send_signal))
        .route("/instances/{id}/outputs", get(get_outputs))
        .route("/instances/{id}/tree", get(get_execution_tree))
        .route("/instances/{id}/retry", post(retry_instance))
        .route(
            "/instances/{id}/checkpoints",
            get(list_checkpoints).post(save_checkpoint),
        )
        .route(
            "/instances/{id}/checkpoints/latest",
            get(get_latest_checkpoint),
        )
        .route("/instances/{id}/checkpoints/prune", post(prune_checkpoints))
        .route("/instances/{id}/audit", get(list_audit_log))
        .route("/instances/{id}/inject-blocks", post(inject_blocks))
        .route(
            "/instances/{id}/stream",
            get(crate::streaming::stream_instance),
        )
        .route("/instances/bulk/state", patch(bulk_update_state))
        .route("/instances/bulk/reschedule", patch(bulk_reschedule))
        .route("/instances/dlq", get(list_dlq))
}

// === Request/Response types ===

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateInstanceRequest {
    sequence_id: SequenceId,
    tenant_id: TenantId,
    namespace: Namespace,
    #[serde(default)]
    priority: Priority,
    #[serde(default = "default_timezone")]
    timezone: String,
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default)]
    context: ExecutionContext,
    next_fire_at: Option<DateTime<Utc>>,
    concurrency_key: Option<String>,
    max_concurrency: Option<i32>,
    idempotency_key: Option<String>,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BatchCreateRequest {
    instances: Vec<CreateInstanceRequest>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateStateRequest {
    state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateContextRequest {
    context: ExecutionContext,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct SendSignalRequest {
    signal_type: SignalType,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Deserialize)]
pub(crate) struct ListQuery {
    tenant_id: Option<String>,
    namespace: Option<String>,
    sequence_id: Option<Uuid>,
    state: Option<String>,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    100
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BulkUpdateStateRequest {
    filter: BulkFilter,
    state: InstanceState,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BulkFilter {
    tenant_id: Option<String>,
    namespace: Option<String>,
    sequence_id: Option<Uuid>,
    states: Option<Vec<InstanceState>>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BulkRescheduleRequest {
    filter: BulkFilter,
    /// Shift `next_fire_at` by this many seconds (positive = later, negative = earlier).
    offset_secs: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CountResponse {
    count: u64,
}

// === Handlers ===

#[utoipa::path(post, path = "/instances", tag = "instances",
    request_body = CreateInstanceRequest,
    responses(
        (status = 201, description = "Instance created", body = serde_json::Value),
        (status = 200, description = "Deduplicated (idempotency key match)", body = serde_json::Value),
    )
)]
pub(crate) async fn create_instance(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<crate::auth::TenantContext>>,
    Json(req): Json<CreateInstanceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let now = Utc::now();

    // Enforce tenant isolation: if X-Tenant-Id header was provided, use it
    // and reject requests that try to write to a different tenant.
    let tenant_id = if let Some(axum::Extension(ctx)) = &tenant_ctx {
        if !req.tenant_id.0.is_empty() && req.tenant_id != ctx.tenant_id {
            return Err(ApiError::Forbidden(
                "tenant_id in body does not match X-Tenant-Id header".into(),
            ));
        }
        ctx.tenant_id.clone()
    } else {
        req.tenant_id.clone()
    };

    // Reject empty tenant / namespace up-front. Without this the DB happily
    // accepts the blank strings and the resulting row leaks into the default
    // tenant's view — tenant isolation requires non-empty scoping values.
    if tenant_id.0.trim().is_empty() {
        return Err(ApiError::InvalidArgument(
            "tenant_id must not be empty".into(),
        ));
    }
    if req.namespace.0.trim().is_empty() {
        return Err(ApiError::InvalidArgument(
            "namespace must not be empty".into(),
        ));
    }

    // Resolve the target sequence up-front and surface "not found" as 404 —
    // otherwise the insert would bottom out on a Postgres FK violation and
    // bubble up as a generic 500.
    let _sequence = state
        .storage
        .get_sequence(req.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", req.sequence_id.0)))?;

    // Reject oversized contexts before they hit the DB.
    req.context.check_size(state.max_context_bytes)?;

    // Idempotency check: if key exists, return existing instance id.
    if let Some(ref idem_key) = req.idempotency_key {
        if !idem_key.is_empty() {
            if let Some(existing) = state
                .storage
                .find_by_idempotency_key(&tenant_id, idem_key)
                .await
                .map_err(|e| ApiError::from_storage(e, "instance"))?
            {
                return Ok((
                    StatusCode::OK,
                    Json(serde_json::json!({ "id": existing.id, "deduplicated": true })),
                ));
            }
        }
    }

    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: req.sequence_id,
        tenant_id,
        namespace: req.namespace,
        state: InstanceState::Scheduled,
        next_fire_at: Some(req.next_fire_at.unwrap_or(now)),
        priority: req.priority,
        timezone: req.timezone,
        metadata: req.metadata,
        context: req.context,
        concurrency_key: req.concurrency_key,
        max_concurrency: req.max_concurrency,
        idempotency_key: req.idempotency_key,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    };

    state
        .storage
        .create_instance(&instance)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "id": instance.id })),
    ))
}

#[utoipa::path(post, path = "/instances/batch", tag = "instances",
    request_body = BatchCreateRequest,
    responses(
        (status = 201, description = "Batch created", body = CountResponse),
        (status = 400, description = "Empty batch"),
    )
)]
pub(crate) async fn create_instances_batch(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<BatchCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Empty batch is a no-op — return success with count=0 so callers
    // can pass through dynamically-computed batches without special-casing
    // the zero-length path.
    if req.instances.is_empty() {
        return Ok((StatusCode::OK, Json(CountResponse { count: 0 })));
    }
    if req.instances.len() > 10_000 {
        return Err(ApiError::InvalidArgument(
            "batch size must not exceed 10,000".into(),
        ));
    }

    // Enforce tenant isolation and context size for each item in the batch.
    for (i, r) in req.instances.iter().enumerate() {
        crate::auth::enforce_tenant_create(&tenant_ctx, &r.tenant_id)?;
        r.context
            .check_size(state.max_context_bytes)
            .map_err(|e| ApiError::PayloadTooLarge(format!("instances[{i}]: {e}")))?;
    }

    let now = Utc::now();
    let instances: Vec<TaskInstance> = req
        .instances
        .into_iter()
        .map(|r| TaskInstance {
            id: InstanceId::new(),
            sequence_id: r.sequence_id,
            tenant_id: r.tenant_id,
            namespace: r.namespace,
            state: InstanceState::Scheduled,
            next_fire_at: Some(r.next_fire_at.unwrap_or(now)),
            priority: r.priority,
            timezone: r.timezone,
            metadata: r.metadata,
            context: r.context,
            concurrency_key: r.concurrency_key,
            max_concurrency: r.max_concurrency,
            idempotency_key: r.idempotency_key,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        })
        .collect();

    let count = state
        .storage
        .create_instances_batch(&instances)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok((StatusCode::CREATED, Json(CountResponse { count })))
}

#[utoipa::path(get, path = "/instances/{id}", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Instance found", body = TaskInstance),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn get_instance(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<crate::auth::TenantContext>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    // Enforce tenant isolation: if header-based tenant is set, reject cross-tenant reads.
    if let Some(axum::Extension(ctx)) = &tenant_ctx {
        if instance.tenant_id != ctx.tenant_id {
            return Err(ApiError::NotFound(format!("instance {id}")));
        }
    }

    Ok(Json(instance))
}

#[utoipa::path(get, path = "/instances", tag = "instances",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("namespace" = Option<String>, Query, description = "Filter by namespace"),
        ("sequence_id" = Option<Uuid>, Query, description = "Filter by sequence"),
        ("state" = Option<String>, Query, description = "Comma-separated states"),
        ("offset" = u64, Query, description = "Pagination offset"),
        ("limit" = u32, Query, description = "Pagination limit (max 1000)"),
    ),
    responses((status = 200, description = "List of instances", body = Vec<TaskInstance>))
)]
pub(crate) async fn list_instances(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<crate::auth::TenantContext>>,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Enforce tenant isolation: header-based tenant overrides query param.
    let tenant_id = if let Some(axum::Extension(ctx)) = &tenant_ctx {
        Some(ctx.tenant_id.clone())
    } else {
        q.tenant_id.map(TenantId)
    };
    let filter = InstanceFilter {
        tenant_id,
        namespace: q.namespace.map(Namespace),
        sequence_id: q.sequence_id.map(SequenceId),
        states: q.state.map(|s| parse_states(&s)),
        metadata_filter: None,
        priority: None,
    };

    let pagination = Pagination {
        offset: q.offset,
        limit: q.limit,
    }
    .capped();

    let instances = state
        .storage
        .list_instances(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok(Json(instances))
}

#[utoipa::path(patch, path = "/instances/{id}/state", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = UpdateStateRequest,
    responses(
        (status = 200, description = "State updated"),
        (status = 400, description = "Invalid state transition"),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn update_state(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateStateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    if !instance.state.can_transition_to(req.state) {
        return Err(ApiError::InvalidArgument(format!(
            "cannot transition from {} to {}",
            instance.state, req.state
        )));
    }

    state
        .storage
        .update_instance_state(instance_id, req.state, req.next_fire_at)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(patch, path = "/instances/{id}/context", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = UpdateContextRequest,
    responses(
        (status = 200, description = "Context updated"),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn update_context(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateContextRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    req.context.check_size(state.max_context_bytes)?;

    state
        .storage
        .update_instance_context(instance_id, &req.context)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(post, path = "/instances/{id}/signals", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = SendSignalRequest,
    responses(
        (status = 201, description = "Signal enqueued", body = serde_json::Value),
        (status = 400, description = "Instance is in terminal state"),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn send_signal(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<SendSignalRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    if instance.state.is_terminal() {
        return Err(ApiError::InvalidArgument(format!(
            "cannot send signal to instance in {} state",
            instance.state
        )));
    }

    let signal = Signal {
        id: Uuid::new_v4(),
        instance_id,
        signal_type: req.signal_type,
        payload: req.payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };

    state
        .storage
        .enqueue_signal(&signal)
        .await
        .map_err(|e| ApiError::from_storage(e, "signal"))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "signal_id": signal.id })),
    ))
}

#[utoipa::path(get, path = "/instances/{id}/outputs", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, description = "Block outputs", body = Vec<orch8_types::output::BlockOutput>))
)]
pub(crate) async fn get_outputs(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let mut outputs = state
        .storage
        .get_all_outputs(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "outputs"))?;

    // Inflate externalization markers in-place so API consumers see real data.
    // See `docs/CONTEXT_MANAGEMENT.md` §8.5.
    for out in &mut outputs {
        if let Some(ref_key) = orch8_engine::externalized::extract_ref_key(&out.output) {
            if let Some(resolved) = state
                .storage
                .get_externalized_state(ref_key)
                .await
                .map_err(|e| ApiError::from_storage(e, "externalized_state"))?
            {
                out.output = resolved;
            }
            // Missing payload: leave marker visible so callers can detect.
        }
    }

    Ok(Json(outputs))
}

#[utoipa::path(get, path = "/instances/{id}/tree", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Execution tree", body = Vec<orch8_types::execution::ExecutionNode>),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn get_execution_tree(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let tree = state
        .storage
        .get_execution_tree(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;

    Ok(Json(tree))
}

#[utoipa::path(patch, path = "/instances/bulk/state", tag = "instances",
    request_body = BulkUpdateStateRequest,
    responses((status = 200, description = "Bulk state update result", body = CountResponse))
)]
pub(crate) async fn bulk_update_state(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<BulkUpdateStateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, req.filter.tenant_id.as_deref());
    let filter = InstanceFilter {
        tenant_id: scoped_tenant,
        namespace: req.filter.namespace.map(Namespace),
        sequence_id: req.filter.sequence_id.map(SequenceId),
        states: req.filter.states,
        metadata_filter: None,
        priority: None,
    };

    let count = state
        .storage
        .bulk_update_state(&filter, req.state)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok(Json(CountResponse { count }))
}

#[utoipa::path(patch, path = "/instances/bulk/reschedule", tag = "instances",
    request_body = BulkRescheduleRequest,
    responses((status = 200, description = "Bulk reschedule result", body = CountResponse))
)]
pub(crate) async fn bulk_reschedule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<BulkRescheduleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, req.filter.tenant_id.as_deref());
    let filter = InstanceFilter {
        tenant_id: scoped_tenant,
        namespace: req.filter.namespace.map(Namespace),
        sequence_id: req.filter.sequence_id.map(SequenceId),
        states: req.filter.states,
        metadata_filter: None,
        priority: None,
    };

    let count = state
        .storage
        .bulk_reschedule(&filter, req.offset_secs)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok(Json(CountResponse { count }))
}

/// DLQ: list failed instances.
#[utoipa::path(get, path = "/instances/dlq", tag = "instances",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("namespace" = Option<String>, Query, description = "Filter by namespace"),
        ("offset" = u64, Query, description = "Pagination offset"),
        ("limit" = u32, Query, description = "Pagination limit"),
    ),
    responses((status = 200, description = "Failed instances (DLQ)", body = Vec<TaskInstance>))
)]
pub(crate) async fn list_dlq(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let filter = InstanceFilter {
        tenant_id: scoped_tenant,
        namespace: q.namespace.map(Namespace),
        sequence_id: q.sequence_id.map(SequenceId),
        states: Some(vec![InstanceState::Failed]),
        metadata_filter: None,
        priority: None,
    };

    let pagination = Pagination {
        offset: q.offset,
        limit: q.limit,
    }
    .capped();

    let instances = state
        .storage
        .list_instances(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok(Json(instances))
}

/// Retry a failed instance: reset to scheduled with immediate fire time.
#[utoipa::path(post, path = "/instances/{id}/retry", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Instance retried", body = serde_json::Value),
        (status = 400, description = "Instance is not in failed state"),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn retry_instance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    if instance.state != InstanceState::Failed {
        return Err(ApiError::InvalidArgument(format!(
            "can only retry failed instances, current state: {}",
            instance.state
        )));
    }

    state
        .storage
        .update_instance_state(instance_id, InstanceState::Scheduled, Some(Utc::now()))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "id": instance_id, "state": "scheduled" })),
    ))
}

// === Checkpoints ===

#[derive(Deserialize, ToSchema)]
pub struct SaveCheckpointRequest {
    checkpoint_data: serde_json::Value,
}

#[derive(Deserialize, ToSchema)]
pub struct PruneCheckpointsRequest {
    /// Number of most recent checkpoints to keep.
    keep: u32,
}

#[utoipa::path(post, path = "/instances/{id}/checkpoints", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = SaveCheckpointRequest,
    responses(
        (status = 201, description = "Checkpoint saved", body = serde_json::Value),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn save_checkpoint(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<SaveCheckpointRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let checkpoint = orch8_types::checkpoint::Checkpoint {
        id: Uuid::new_v4(),
        instance_id,
        checkpoint_data: req.checkpoint_data,
        created_at: Utc::now(),
    };

    state
        .storage
        .save_checkpoint(&checkpoint)
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoint"))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "id": checkpoint.id })),
    ))
}

#[utoipa::path(get, path = "/instances/{id}/checkpoints", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, description = "List of checkpoints", body = Vec<orch8_types::checkpoint::Checkpoint>))
)]
pub(crate) async fn list_checkpoints(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let checkpoints = state
        .storage
        .list_checkpoints(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoints"))?;

    Ok(Json(checkpoints))
}

#[utoipa::path(get, path = "/instances/{id}/checkpoints/latest", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Latest checkpoint", body = orch8_types::checkpoint::Checkpoint),
        (status = 404, description = "No checkpoint found"),
    )
)]
pub(crate) async fn get_latest_checkpoint(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let checkpoint = state
        .storage
        .get_latest_checkpoint(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoint"))?
        .ok_or_else(|| ApiError::NotFound(format!("checkpoint for instance {id}")))?;

    Ok(Json(checkpoint))
}

#[utoipa::path(post, path = "/instances/{id}/checkpoints/prune", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = PruneCheckpointsRequest,
    responses((status = 200, description = "Pruned checkpoints", body = CountResponse))
)]
pub(crate) async fn prune_checkpoints(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<PruneCheckpointsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let count = state
        .storage
        .prune_checkpoints(InstanceId(id), req.keep)
        .await
        .map_err(|e| ApiError::from_storage(e, "checkpoints"))?;

    Ok(Json(CountResponse { count }))
}

/// Parse comma-separated state values.
// === Audit Log ===

#[utoipa::path(
    get,
    path = "/instances/{id}/audit",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, body = Vec<orch8_types::audit::AuditLogEntry>))
)]
async fn list_audit_log(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let entries = state
        .storage
        .list_audit_log(InstanceId(id), 200)
        .await
        .map_err(|e| ApiError::from_storage(e, "audit_log"))?;
    Ok(Json(entries))
}

// === Dynamic Step Injection ===

#[derive(Deserialize, ToSchema)]
pub(crate) struct InjectBlocksRequest {
    /// Blocks must be a valid JSON array of `BlockDefinition` objects.
    pub blocks: serde_json::Value,
    /// Position to inject at (0-indexed). If omitted, blocks are appended.
    #[serde(default)]
    pub position: Option<usize>,
}

/// Validate injected blocks and return their IDs.
fn validate_injected_blocks(blocks: &serde_json::Value) -> Result<Vec<String>, ApiError> {
    let arr = blocks
        .as_array()
        .ok_or_else(|| ApiError::InvalidArgument("blocks must be a JSON array".into()))?;
    if arr.is_empty() {
        return Err(ApiError::InvalidArgument(
            "blocks array must not be empty".into(),
        ));
    }
    let mut ids = Vec::with_capacity(arr.len());
    for (i, block) in arr.iter().enumerate() {
        let def = serde_json::from_value::<orch8_types::sequence::BlockDefinition>(block.clone())
            .map_err(|_| {
            ApiError::InvalidArgument(format!("blocks[{i}] is not a valid BlockDefinition"))
        })?;
        ids.push(block_def_id(&def));
    }
    Ok(ids)
}

/// Extract the ID from any block definition variant.
fn block_def_id(def: &orch8_types::sequence::BlockDefinition) -> String {
    use orch8_types::sequence::BlockDefinition;
    match def {
        BlockDefinition::Step(s) => s.id.0.clone(),
        BlockDefinition::Parallel(p) => p.id.0.clone(),
        BlockDefinition::Race(r) => r.id.0.clone(),
        BlockDefinition::Loop(l) => l.id.0.clone(),
        BlockDefinition::ForEach(f) => f.id.0.clone(),
        BlockDefinition::Router(r) => r.id.0.clone(),
        BlockDefinition::TryCatch(t) => t.id.0.clone(),
        BlockDefinition::SubSequence(s) => s.id.0.clone(),
        BlockDefinition::ABSplit(a) => a.id.0.clone(),
        BlockDefinition::CancellationScope(cs) => cs.id.0.clone(),
    }
}

#[utoipa::path(
    post,
    path = "/instances/{id}/inject-blocks",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = InjectBlocksRequest,
    responses((status = 200, description = "Blocks injected"))
)]
async fn inject_blocks(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(body): Json<InjectBlocksRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify instance belongs to caller's tenant
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let block_ids = validate_injected_blocks(&body.blocks)?;

    // If position is specified, merge with existing injected blocks at that index.
    let final_blocks = if let Some(pos) = body.position {
        let existing = state
            .storage
            .get_injected_blocks(InstanceId(id))
            .await
            .map_err(|e| ApiError::from_storage(e, "instance"))?;

        let mut arr = existing
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();

        let new_blocks = body.blocks.as_array().cloned().unwrap_or_default();
        let insert_at = pos.min(arr.len());
        for (i, block) in new_blocks.into_iter().enumerate() {
            arr.insert(insert_at + i, block);
        }
        serde_json::Value::Array(arr)
    } else {
        body.blocks
    };

    state
        .storage
        .inject_blocks(InstanceId(id), &final_blocks)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok(Json(serde_json::json!({
        "injected_block_ids": block_ids,
        "position": body.position,
        "total_injected": final_blocks.as_array().map_or(0, Vec::len),
    })))
}

fn parse_states(s: &str) -> Vec<InstanceState> {
    s.split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            match trimmed {
                "scheduled" => Some(InstanceState::Scheduled),
                "running" => Some(InstanceState::Running),
                "waiting" => Some(InstanceState::Waiting),
                "paused" => Some(InstanceState::Paused),
                "completed" => Some(InstanceState::Completed),
                "failed" => Some(InstanceState::Failed),
                "cancelled" => Some(InstanceState::Cancelled),
                _ => None,
            }
        })
        .collect()
}
