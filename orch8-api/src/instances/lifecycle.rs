//! Instance lifecycle: create, list, get, update state/context, retry, batch create.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::BlockDefinition;

use super::types::{
    BatchCreateRequest, CountResponse, CreateInstanceRequest, ListQuery, ResumeFromRequest,
    UpdateContextRequest, UpdateStateRequest, parse_states,
};
use crate::AppState;
use crate::error::ApiError;

/// Scope an idempotency key by execution mode. Non-empty keys for dry-runs are
/// prefixed so a real run reusing the same key isn't deduplicated to a prior
/// simulation (see E1). Empty / absent keys are preserved verbatim so the
/// existing empty-key unique-constraint semantics are unchanged.
///
/// Edge case (accepted): a *real* run whose key is literally `dryrun:<x>` shares
/// the dedup namespace of a dry-run with key `<x>`. Harmless in practice — caller
/// keys are opaque and this collision only affects at-most-once dedup, never
/// correctness of execution.
fn mode_scoped_idempotency_key(key: Option<&str>, dry_run: bool) -> Option<String> {
    key.map(|k| {
        if !k.is_empty() && dry_run {
            format!("dryrun:{k}")
        } else {
            k.to_string()
        }
    })
}

/// Resolve the instance that won a concurrent create with the same
/// idempotency key. `None` means the conflict was not eligible for idempotent
/// recovery (for example, an omitted or empty key).
async fn concurrent_idempotency_winner(
    state: &AppState,
    tenant_id: &TenantId,
    idempotency_key: Option<&str>,
) -> Result<Option<InstanceId>, ApiError> {
    let Some(key) = idempotency_key.filter(|key| !key.is_empty()) else {
        return Ok(None);
    };
    let instance = state
        .storage
        .find_by_idempotency_key(tenant_id, key)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::Conflict("idempotency key conflict without an instance".into()))?;
    Ok(Some(instance.id))
}

/// Collect `metadata.<key>=<value>` query params into a flat JSON object used
/// as the metadata equality filter. Returns `None` when no such params are
/// present (so the common unfiltered list path stays a no-op). Keys are the
/// part after the `metadata.` prefix; values are kept as JSON strings —
/// matching is string-equality on top-level metadata keys.
fn build_metadata_filter(
    raw: &std::collections::HashMap<String, String>,
) -> Option<serde_json::Value> {
    let mut obj = serde_json::Map::new();
    for (k, v) in raw {
        if let Some(key) = k.strip_prefix("metadata.")
            && !key.is_empty()
        {
            obj.insert(key.to_string(), serde_json::Value::String(v.clone()));
        }
    }
    if obj.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(obj))
    }
}

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
    // Reject an explicitly empty tenant_id when the caller has not supplied a
    // header. If a tenant header is present it remains authoritative even when
    // the body field is blank.
    if req.tenant_id.as_str().trim().is_empty() && tenant_ctx.is_none() {
        return Err(ApiError::InvalidArgument(
            "tenant_id must not be empty".into(),
        ));
    }

    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &req.tenant_id)?;

    // Defensive: the shared helper falls back to "default" when both the body
    // and header are absent, so the tenant can never be empty here.
    if tenant_id.as_str().trim().is_empty() {
        return Err(ApiError::InvalidArgument(
            "tenant_id must not be empty".into(),
        ));
    }
    if req.namespace.as_str().trim().is_empty() {
        return Err(ApiError::InvalidArgument(
            "namespace must not be empty".into(),
        ));
    }

    // Resolve the target sequence up-front and surface "not found" as 404 —
    // otherwise the insert would bottom out on a Postgres FK violation and
    // bubble up as a generic 500.
    let sequence = state
        .storage
        .get_sequence(req.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", req.sequence_id.into_uuid())))?;

    // Cross-tenant sequence IDOR guard: a caller must not instantiate a
    // sequence that belongs to another tenant.
    if sequence.tenant_id != tenant_id {
        return Err(ApiError::Forbidden(
            "sequence does not belong to the caller's tenant".into(),
        ));
    }

    // Validate input against the sequence's optional `input_schema` (422 on
    // failure) before anything is persisted.
    crate::input_schema::validate_input(sequence.input_schema.as_ref(), &req.context.data)?;

    // Reject oversized contexts before they hit the DB.
    req.context.check_size(state.max_context_bytes)?;

    // Dry-run is requested per run; stamp it onto the instance's runtime
    // context so every handler sees it (and it survives checkpoint/replay).
    let mut context = req.context;
    context.runtime.dry_run = req.dry_run || context.runtime.dry_run;
    context.runtime.dry_run_auto_approve =
        req.dry_run_auto_approve || context.runtime.dry_run_auto_approve;
    let dry_run = context.runtime.dry_run;

    // Idempotency identity must include the execution mode: otherwise a real
    // run reusing a dry-run's key would be silently deduplicated to the
    // simulation (the real workflow never executes). Namespace non-empty keys
    // by mode; an empty key is preserved verbatim so the existing empty-key
    // unique-constraint behavior is unchanged.
    let effective_idem = mode_scoped_idempotency_key(req.idempotency_key.as_deref(), dry_run);

    // Idempotency check: only for non-empty keys (empty keys are not a dedup
    // lookup — they rely on the DB unique constraint).
    if let Some(ref idem_key) = effective_idem
        && !idem_key.is_empty()
        && let Some(existing) = state
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
        context,
        concurrency_key: req.concurrency_key,
        max_concurrency: req.max_concurrency,
        idempotency_key: effective_idem.clone(),
        session_id: None,
        parent_instance_id: None,
        budget: req.budget,
        created_at: now,
        updated_at: now,
    };

    // The preflight lookup above gives the common retry path a fast read, but
    // it cannot make the operation atomic: a simultaneous request can insert
    // the same key after this request observes no row. The database's unique
    // index remains the authority; on that expected conflict, read the winner
    // and return the same idempotent success response instead of leaking a
    // spurious 409 to one of the callers.
    match state.storage.create_instance(&instance).await {
        Ok(()) => {}
        Err(err @ StorageError::Conflict(_)) => {
            if let Some(id) = concurrent_idempotency_winner(
                &state,
                &instance.tenant_id,
                effective_idem.as_deref(),
            )
            .await?
            {
                return Ok((
                    StatusCode::OK,
                    Json(serde_json::json!({ "id": id, "deduplicated": true })),
                ));
            }
            return Err(ApiError::from_storage(err, "instance"));
        }
        Err(e) => return Err(ApiError::from_storage(e, "instance")),
    }

    // Surface dry-run mode in telemetry so simulated runs are distinguishable
    // from real ones (the flag is also queryable on the instance's
    // `context.runtime.dry_run`).
    if instance.context.runtime.dry_run {
        tracing::info!(instance_id = %instance.id, dry_run = true, "instance created (dry-run)");
    }

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
#[allow(clippy::too_many_lines)]
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
    let mut authoritative_tenants = std::collections::HashMap::new();
    for (i, r) in req.instances.iter().enumerate() {
        if r.tenant_id.as_str().trim().is_empty() && tenant_ctx.is_none() {
            return Err(ApiError::InvalidArgument(format!(
                "instances[{i}]: tenant_id must not be empty"
            )));
        }
        let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &r.tenant_id)?;
        if tenant_id.as_str().trim().is_empty() {
            return Err(ApiError::InvalidArgument(format!(
                "instances[{i}]: tenant_id must not be empty"
            )));
        }
        authoritative_tenants.insert(i, tenant_id);
        if r.namespace.as_str().trim().is_empty() {
            return Err(ApiError::InvalidArgument(format!(
                "instances[{i}]: namespace must not be empty"
            )));
        }
        r.context
            .check_size(state.max_context_bytes)
            .map_err(|e| ApiError::PayloadTooLarge(format!("instances[{i}]: {e}")))?;
        sequence_ids.insert(r.sequence_id);
    }

    // Fetch every referenced sequence in one storage query instead of one
    // round-trip per distinct ID. A batch may contain 10,000 instances, so
    // the previous loop could otherwise turn validation alone into 10,000
    // sequential database queries.
    let mut input_schemas: std::collections::HashMap<_, Option<serde_json::Value>> =
        std::collections::HashMap::new();
    let mut sequence_tenants: std::collections::HashMap<_, TenantId> =
        std::collections::HashMap::new();
    let sequence_ids: Vec<_> = sequence_ids.into_iter().collect();
    for seq in state
        .storage
        .get_sequences(&sequence_ids)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
    {
        sequence_tenants.insert(seq.id, seq.tenant_id);
        input_schemas.insert(seq.id, seq.input_schema);
    }

    // Validate each item's data against its sequence's input_schema (422) and
    // enforce that the sequence belongs to the item's tenant.
    for (i, r) in req.instances.iter().enumerate() {
        let item_tenant = authoritative_tenants
            .get(&i)
            .cloned()
            .ok_or_else(|| ApiError::Internal(format!("instances[{i}]: tenant not resolved")))?;
        let seq_tenant = sequence_tenants
            .get(&r.sequence_id)
            .cloned()
            .ok_or_else(|| {
                ApiError::Internal(format!("instances[{i}]: sequence tenant missing"))
            })?;
        if seq_tenant != item_tenant {
            return Err(ApiError::Forbidden(format!(
                "instances[{i}]: sequence does not belong to the caller's tenant"
            )));
        }
        let schema = input_schemas.get(&r.sequence_id).and_then(Option::as_ref);
        crate::input_schema::validate_input(schema, &r.context.data).map_err(|e| match e {
            ApiError::UnprocessableEntity(msg) => {
                ApiError::UnprocessableEntity(format!("instances[{i}]: {msg}"))
            }
            other => other,
        })?;
    }

    let now = Utc::now();
    let instances: Vec<TaskInstance> = req
        .instances
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let mut context = r.context;
            context.runtime.dry_run = r.dry_run || context.runtime.dry_run;
            context.runtime.dry_run_auto_approve =
                r.dry_run_auto_approve || context.runtime.dry_run_auto_approve;
            // Same mode-namespacing as the single-create path (see E1).
            let idempotency_key =
                mode_scoped_idempotency_key(r.idempotency_key.as_deref(), context.runtime.dry_run);
            let tenant_id = authoritative_tenants.remove(&i).ok_or_else(|| {
                ApiError::Internal(format!("instances[{i}]: tenant not resolved"))
            })?;
            Ok::<_, ApiError>(TaskInstance {
                id: InstanceId::new(),
                sequence_id: r.sequence_id,
                tenant_id,
                namespace: r.namespace,
                state: InstanceState::Scheduled,
                next_fire_at: Some(r.next_fire_at.unwrap_or(now)),
                priority: r.priority,
                timezone: r.timezone,
                metadata: r.metadata,
                context,
                concurrency_key: r.concurrency_key,
                max_concurrency: r.max_concurrency,
                idempotency_key,
                session_id: None,
                parent_instance_id: None,
                budget: r.budget,
                created_at: now,
                updated_at: now,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

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
        .get_instance(InstanceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    // Enforce tenant isolation: if header-based tenant is set, reject cross-tenant reads.
    if let Some(axum::Extension(ctx)) = &tenant_ctx
        && instance.tenant_id != ctx.tenant_id
    {
        return Err(ApiError::NotFound(format!("instance {id}")));
    }

    Ok(Json(instance))
}

#[utoipa::path(get, path = "/instances/{id}/children", tag = "instances",
    params(("id" = Uuid, Path, description = "Parent instance ID")),
    responses(
        (status = 200, description = "Child instances spawned by this instance", body = Vec<TaskInstance>),
        (status = 404, description = "Parent instance not found"),
    )
)]
pub async fn get_instance_children(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<crate::auth::TenantContext>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let parent_id = InstanceId::from_uuid(id);

    // Resolve the parent first so a missing parent is a 404 and tenant
    // isolation is enforced against the parent's owner.
    let parent = state
        .storage
        .get_instance(parent_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    if let Some(axum::Extension(ctx)) = &tenant_ctx
        && parent.tenant_id != ctx.tenant_id
    {
        return Err(ApiError::NotFound(format!("instance {id}")));
    }

    let children = state
        .storage
        .get_child_instances(parent_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok(Json(children))
}

#[utoipa::path(get, path = "/instances/{id}/logs", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Step logs (oldest first)", body = Vec<orch8_types::step_log::StepLog>),
        (status = 404, description = "Instance not found"),
    )
)]
pub async fn get_instance_logs(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<crate::auth::TenantContext>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId::from_uuid(id);
    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    if let Some(axum::Extension(ctx)) = &tenant_ctx
        && instance.tenant_id != ctx.tenant_id
    {
        return Err(ApiError::NotFound(format!("instance {id}")));
    }

    let logs = state
        .storage
        .list_step_logs(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "step_logs"))?;

    Ok(Json(logs))
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
    Query(raw): Query<std::collections::HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    // Enforce tenant isolation: header-based tenant overrides query param.
    let tenant_id = if let Some(axum::Extension(ctx)) = &tenant_ctx {
        Some(ctx.tenant_id.clone())
    } else {
        q.tenant_id.map(TenantId::unchecked)
    };

    // Top-level metadata equality filters: every `?metadata.<key>=<value>`
    // query param becomes one entry in a JSON object matched against the
    // instance's `metadata` (Postgres `@>` containment over the existing GIN
    // index; SQLite `json_extract` text-equality). String-equality only — the
    // indexed substitute for Temporal's search attributes.
    let metadata_filter = build_metadata_filter(&raw);

    let filter = InstanceFilter {
        tenant_id,
        namespace: q.namespace.map(Namespace::new),
        sequence_id: q.sequence_id.map(SequenceId::from_uuid),
        states: q.state.as_deref().map(parse_states).transpose()?,
        metadata_filter,
        priority: None,
    };

    let mut pagination = Pagination {
        offset: q.offset,
        limit: q.limit,
        sort_ascending: false,
    }
    .capped();

    let limit = pagination.limit;
    pagination.limit = limit.saturating_add(1);
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
    let instance_id = InstanceId::from_uuid(id);

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
    ) && let Some(parent_id) = instance.parent_instance_id
        && let Ok(Some(parent)) = state.storage.get_instance(parent_id).await
        && parent.state == InstanceState::Waiting
        && let Err(e) = state
            .storage
            .update_instance_state(parent_id, InstanceState::Scheduled, Some(Utc::now()))
            .await
    {
        tracing::warn!(
            parent_id = %parent_id.into_uuid(),
            error = %e,
            "failed to wake parent instance after child terminal transition"
        );
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
    let instance_id = InstanceId::from_uuid(id);

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
    let instance_id = InstanceId::from_uuid(id);

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

/// Recursively collect the IDs of `block` and every block nested inside it
/// (composite branches, loop bodies, router routes, try/catch/finally arms,
/// A/B variants, cancellation scopes).
///
/// Used by resume-from-block to wipe descendant outputs together with the
/// composite that owns them — leaving a nested loop's iteration-counter
/// marker or a nested step's output in place would make the re-run skip or
/// short-circuit those blocks. Fork-from reuses it to gather the copy set
/// for blocks *before* the fork point.
pub(super) fn collect_block_ids(block: &BlockDefinition, out: &mut Vec<BlockId>) {
    fn collect_list(blocks: &[BlockDefinition], out: &mut Vec<BlockId>) {
        for b in blocks {
            collect_block_ids(b, out);
        }
    }
    match block {
        BlockDefinition::Step(s) => out.push(s.id.clone()),
        BlockDefinition::Parallel(p) => {
            out.push(p.id.clone());
            for branch in &p.branches {
                collect_list(branch, out);
            }
        }
        BlockDefinition::Race(r) => {
            out.push(r.id.clone());
            for branch in &r.branches {
                collect_list(branch, out);
            }
        }
        BlockDefinition::Loop(l) => {
            out.push(l.id.clone());
            collect_list(&l.body, out);
        }
        BlockDefinition::ForEach(fe) => {
            out.push(fe.id.clone());
            collect_list(&fe.body, out);
        }
        BlockDefinition::Router(r) => {
            out.push(r.id.clone());
            for route in &r.routes {
                collect_list(&route.blocks, out);
            }
            if let Some(default) = &r.default {
                collect_list(default, out);
            }
        }
        BlockDefinition::TryCatch(tc) => {
            out.push(tc.id.clone());
            collect_list(&tc.try_block, out);
            collect_list(&tc.catch_block, out);
            if let Some(finally) = &tc.finally_block {
                collect_list(finally, out);
            }
        }
        BlockDefinition::SubSequence(s) => out.push(s.id.clone()),
        BlockDefinition::ABSplit(ab) => {
            out.push(ab.id.clone());
            for variant in &ab.variants {
                collect_list(&variant.blocks, out);
            }
        }
        BlockDefinition::CancellationScope(cs) => {
            out.push(cs.id.clone());
            collect_list(&cs.blocks, out);
        }
    }
}

/// The ID of a top-level block, regardless of variant.
pub(super) fn top_level_block_id(block: &BlockDefinition) -> &BlockId {
    match block {
        BlockDefinition::Step(s) => &s.id,
        BlockDefinition::Parallel(p) => &p.id,
        BlockDefinition::Race(r) => &r.id,
        BlockDefinition::Loop(l) => &l.id,
        BlockDefinition::ForEach(fe) => &fe.id,
        BlockDefinition::Router(r) => &r.id,
        BlockDefinition::TryCatch(tc) => &tc.id,
        BlockDefinition::SubSequence(s) => &s.id,
        BlockDefinition::ABSplit(ab) => &ab.id,
        BlockDefinition::CancellationScope(cs) => &cs.id,
    }
}

/// DLQ surgery: re-run an instance from an arbitrary block.
///
/// Wipes the target block's outputs and the outputs of every block at or
/// after it in the sequence's top-level `blocks` array (including blocks
/// nested inside wiped composites), optionally shallow-merges a context
/// patch into `context.data`, and re-schedules the instance immediately.
/// Earlier blocks keep their outputs: on the flat all-step execution path
/// the completed-blocks check skips them, so side-effectful handlers do not
/// run twice. On the tree path the execution tree is rebuilt from scratch —
/// the exact same semantics as `POST /instances/{id}/retry`, which also
/// deletes the tree while preserving real outputs.
///
/// Linear assumption: "after" is defined by position in the top-level
/// `blocks` array, which is the order both execution paths (flat fast path
/// and tree evaluator) run top-level blocks in. The target must itself be a
/// top-level block — resuming from a block nested inside a composite is not
/// supported.
#[utoipa::path(post, path = "/instances/{id}/resume-from/{block_id}", tag = "instances",
    params(
        ("id" = Uuid, Path, description = "Instance ID"),
        ("block_id" = String, Path, description = "Top-level block to resume from"),
    ),
    request_body = Option<ResumeFromRequest>,
    responses(
        (status = 200, description = "Instance re-scheduled from the given block", body = serde_json::Value),
        (status = 400, description = "Unknown block, or instance is in a state that cannot be resumed"),
        (status = 404, description = "Instance or sequence not found"),
    )
)]
#[allow(clippy::too_many_lines)] // sequential surgery steps — splitting hurts readability
pub async fn resume_from_block(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((id, block_id)): Path<(Uuid, String)>,
    req: Option<Json<ResumeFromRequest>>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId::from_uuid(id);
    let req = req.map(|Json(r)| r).unwrap_or_default();

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

    // Only quiescent instances can be operated on. Running/Scheduled/Waiting
    // instances must be paused or cancelled first — wiping outputs under a
    // live execution would race the scheduler.
    if !matches!(
        instance.state,
        InstanceState::Failed
            | InstanceState::Paused
            | InstanceState::Completed
            | InstanceState::Cancelled
    ) {
        return Err(ApiError::InvalidArgument(format!(
            "cannot resume-from in state {}: pause or cancel the instance first",
            instance.state
        )));
    }

    // Validate the context patch up-front, before any destructive write.
    let patch = match req.context {
        None => None,
        Some(serde_json::Value::Object(map)) => Some(map),
        Some(_) => {
            return Err(ApiError::InvalidArgument(
                "context patch must be a JSON object".into(),
            ));
        }
    };

    let sequence = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| {
            ApiError::NotFound(format!("sequence {}", instance.sequence_id.into_uuid()))
        })?;

    let target_idx = sequence
        .blocks
        .iter()
        .position(|b| top_level_block_id(b).as_str() == block_id)
        .ok_or_else(|| {
            ApiError::InvalidArgument(format!(
                "block '{block_id}' is not a top-level block of sequence {}",
                instance.sequence_id.into_uuid()
            ))
        })?;

    // Wipe set: the target block and everything after it in top-level order,
    // plus all blocks nested inside those (composite iteration markers, retry
    // markers and in-progress sentinels all live in `block_outputs` under the
    // block's own ID, so one batched delete clears them all).
    let mut wipe_ids: Vec<BlockId> = Vec::new();
    for block in &sequence.blocks[target_idx..] {
        collect_block_ids(block, &mut wipe_ids);
    }

    // Delete the stale execution tree so the evaluator rebuilds it from
    // scratch (same mechanics as `retry_instance`). Earlier blocks keep
    // their outputs and are memoized on the re-run.
    state
        .storage
        .delete_execution_tree(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;

    let outputs_deleted = state
        .storage
        .delete_block_outputs_batch(instance_id, &wipe_ids)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?;

    // Purge stale worker_tasks rows for the wiped blocks — the
    // `UNIQUE(instance_id, block_id)` constraint would otherwise swallow the
    // re-run's dispatch via `ON CONFLICT DO NOTHING` and strand the instance
    // in Waiting (same purge the loop/for_each iteration reset performs).
    let wipe_strs: Vec<String> = wipe_ids.iter().map(|b| b.as_str().to_owned()).collect();
    state
        .storage
        .cancel_worker_tasks_for_blocks(instance_id.into_uuid(), &wipe_strs)
        .await
        .map_err(|e| ApiError::from_storage(e, "worker_tasks"))?;

    // Apply the context patch: shallow per-key merge into `context.data`
    // (the same per-key semantics as `StorageBackend::merge_context_data`,
    // which the engine uses for handler-driven context writes).
    if let Some(patch) = patch {
        let mut context = instance.context.clone();
        if !context.data.is_object() {
            context.data = serde_json::json!({});
        }
        if let Some(data) = context.data.as_object_mut() {
            for (key, value) in patch {
                data.insert(key, value);
            }
        }
        context.check_size(state.max_context_bytes)?;
        state
            .storage
            .update_instance_context(instance_id, &context)
            .await
            .map_err(|e| ApiError::from_storage(e, "instance"))?;
    }

    // Enqueue injected signals BEFORE the wake transition so they are
    // already pending when the instance becomes claimable — the atomic
    // alternative to the racy "resume, then signal" two-call pattern.
    // Plain `enqueue_signal` (not `_if_active`): the instance may still be
    // in a terminal state here; it is about to be revived.
    let signals_enqueued = req.signals.len();
    for injected in req.signals {
        let signal = orch8_types::signal::Signal {
            id: Uuid::now_v7(),
            instance_id,
            signal_type: injected.signal_type,
            payload: injected.payload,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        };
        state
            .storage
            .enqueue_signal(&signal)
            .await
            .map_err(|e| ApiError::from_storage(e, "signal"))?;
    }

    state
        .storage
        .update_instance_state(instance_id, InstanceState::Scheduled, Some(Utc::now()))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    tracing::info!(
        instance_id = %instance_id,
        resumed_from = %block_id,
        outputs_deleted = outputs_deleted,
        signals_enqueued = signals_enqueued,
        "instance resumed from block"
    );

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "id": instance_id,
            "state": "scheduled",
            "resumed_from": block_id,
            "outputs_deleted": outputs_deleted,
            "signals_enqueued": signals_enqueued,
        })),
    ))
}
