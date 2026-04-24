//! Instance lifecycle: create, list, get, update state/context, retry, batch create.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use uuid::Uuid;

use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};

use super::types::{
    parse_states, BatchCreateRequest, CountResponse, CreateInstanceRequest, ListQuery,
    UpdateContextRequest, UpdateStateRequest,
};
use crate::error::ApiError;
use crate::AppState;

#[utoipa::path(post, path = "/instances", tag = "instances",
    request_body = CreateInstanceRequest,
    responses(
        (status = 201, description = "Instance created", body = serde_json::Value),
        (status = 200, description = "Deduplicated (idempotency key match)", body = serde_json::Value),
    )
)]
pub async fn create_instance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<CreateInstanceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let now = Utc::now();

    // Ref#5: delegate header/body tenant reconciliation to the shared helper
    // instead of re-implementing the rule inline (the inline copy drifted from
    // `enforce_tenant_create` once already).
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &req.tenant_id)?;

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
pub async fn create_instances_batch(
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
    // Also collect unique sequence_ids to validate they exist before inserting.
    //
    // Mirrors the single-instance create checks — batch must not be a loophole
    // that lets blank tenant/namespace rows leak into the default tenant's
    // view (tenant isolation requires non-empty scoping values).
    let mut sequence_ids = std::collections::HashSet::new();
    for (i, r) in req.instances.iter().enumerate() {
        crate::auth::enforce_tenant_create(&tenant_ctx, &r.tenant_id)?;
        if r.tenant_id.0.trim().is_empty() {
            return Err(ApiError::InvalidArgument(format!(
                "instances[{i}]: tenant_id must not be empty"
            )));
        }
        if r.namespace.0.trim().is_empty() {
            return Err(ApiError::InvalidArgument(format!(
                "instances[{i}]: namespace must not be empty"
            )));
        }
        r.context
            .check_size(state.max_context_bytes)
            .map_err(|e| ApiError::PayloadTooLarge(format!("instances[{i}]: {e}")))?;
        sequence_ids.insert(r.sequence_id);
    }

    // Validate all referenced sequences exist.
    for seq_id in &sequence_ids {
        state
            .storage
            .get_sequence(*seq_id)
            .await
            .map_err(|e| ApiError::from_storage(e, "sequence"))?
            .ok_or_else(|| ApiError::NotFound(format!("sequence {}", seq_id.0)))?;
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
pub async fn get_instance(
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
pub async fn list_instances(
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
        states: q.state.as_deref().map(parse_states).transpose()?,
        metadata_filter: None,
        priority: None,
    };

    let pagination = Pagination {
        offset: q.offset,
        limit: q.limit,
        sort_ascending: false,
    }
    .capped();

    let limit = pagination.limit;
    let instances = state
        .storage
        .list_instances(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok(Json(crate::PaginatedResponse::from_vec(instances, limit)))
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
pub async fn update_state(
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

    // If this instance is a SubSequence child and the external transition
    // drove it to a terminal state, wake the parent so its SubSequence node
    // can observe the child's final state on the next tick. Without this,
    // cancelling a child via the API leaves the parent stuck in Waiting
    // indefinitely — the scheduler's `wake_parent_if_child` is only invoked
    // on engine-driven transitions.
    if matches!(
        req.state,
        InstanceState::Failed | InstanceState::Cancelled | InstanceState::Completed
    ) {
        if let Some(parent_id) = instance.parent_instance_id {
            if let Ok(Some(parent)) = state.storage.get_instance(parent_id).await {
                if parent.state == InstanceState::Waiting {
                    if let Err(e) = state
                        .storage
                        .update_instance_state(
                            parent_id,
                            InstanceState::Scheduled,
                            Some(Utc::now()),
                        )
                        .await
                    {
                        tracing::warn!(
                            parent_id = %parent_id.0,
                            error = %e,
                            "failed to wake parent instance after child terminal transition"
                        );
                    }
                }
            }
        }
    }

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
pub async fn update_context(
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

/// Retry a failed instance: reset to scheduled with immediate fire time.
#[utoipa::path(post, path = "/instances/{id}/retry", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Instance retried", body = serde_json::Value),
        (status = 400, description = "Instance is not in failed state"),
        (status = 404, description = "Instance not found"),
    )
)]
pub async fn retry_instance(
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

    // Delete the stale execution tree so the evaluator rebuilds it from
    // scratch. Without this, the old Failed/Running nodes would cause the
    // retried instance to immediately re-fail or get stuck.
    state
        .storage
        .delete_execution_tree(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;

    // Clear only sentinel outputs (in-progress markers from permanently
    // failed steps) so those steps can re-execute on retry. Real outputs
    // from successfully completed steps are preserved — the fast path's
    // `completed_blocks` check will skip them, preventing double execution
    // of side-effectful handlers (email, HTTP POST, etc.).
    state
        .storage
        .delete_sentinel_block_outputs(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?;

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
