//! Background jobs API: enqueue a handler invocation without authoring a
//! sequence (`POST /jobs`), inspect it (`GET /jobs/{id}`, `GET /jobs`) and
//! cancel it (`DELETE /jobs/{id}`).
//!
//! Jobs are instances of auto-managed single-step system sequences (see
//! [`orch8_engine::jobs`]); the job id is the instance id, so every
//! `/instances/{id}` endpoint (retry, timeline, outputs, stream) works on a
//! job too. Tenant isolation, entitlements and idempotency follow
//! `POST /instances` exactly.

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use orch8_engine::jobs::{JobSpec, NewJob};
use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::filter::InstanceFilter;
use orch8_types::ids::{InstanceId, Namespace, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::job::{EnqueueJobRequest, JOB_IDEMPOTENCY_PREFIX, Job, JobPage, JobStatus};
use orch8_types::signal::{Signal, SignalType};

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/jobs", get(list_jobs).post(enqueue_job))
        .route("/jobs/{id}", get(get_job).delete(cancel_job))
}

fn job_view(instance: &TaskInstance, job: Option<Job>) -> Result<Job, ApiError> {
    job.ok_or_else(|| ApiError::NotFound(format!("job {}", instance.id.into_uuid())))
}

async fn load_job_view(state: &AppState, instance: &TaskInstance) -> Result<Job, ApiError> {
    let job = orch8_engine::jobs::load_job(state.storage.as_ref(), instance).await?;
    job_view(instance, job)
}

/// Fetch a job's backing instance, enforcing tenant isolation (404 on
/// cross-tenant access and for instances that are not jobs).
async fn fetch_job_instance(
    state: &AppState,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<TaskInstance, ApiError> {
    let label = format!("job {id}");
    let instance = state
        .storage
        .get_instance(InstanceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "job"))?
        .ok_or_else(|| ApiError::NotFound(label.clone()))?;
    crate::auth::enforce_tenant_access(tenant_ctx, &instance.tenant_id, &label)?;
    if orch8_engine::jobs::job_marker(&instance).is_none() {
        return Err(ApiError::NotFound(label));
    }
    Ok(instance)
}

/// Enqueue a background job.
#[utoipa::path(post, path = "/jobs", tag = "jobs",
    request_body = EnqueueJobRequest,
    responses(
        (status = 201, description = "Job enqueued", body = Job),
        (status = 200, description = "Idempotent replay: existing job returned", body = Job),
        (status = 400, description = "Invalid request"),
        (status = 413, description = "Payload exceeds the context size limit"),
    )
)]
pub async fn enqueue_job(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<EnqueueJobRequest>,
) -> Result<impl IntoResponse, ApiError> {
    req.validate().map_err(ApiError::InvalidArgument)?;

    let body_tenant = TenantId::unchecked(req.tenant_id.clone().unwrap_or_default());
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body_tenant)?;
    let namespace = Namespace::new(
        req.namespace
            .clone()
            .filter(|ns| !ns.trim().is_empty())
            .unwrap_or_else(|| "default".into()),
    );

    // Size check on exactly what will be persisted as the context.
    let context = ExecutionContext {
        data: req.payload.clone(),
        ..Default::default()
    };
    context.check_size(state.max_context_bytes)?;

    let entitlement_plan = crate::entitlements::admit_instances(
        &state,
        &tenant_id,
        std::slice::from_ref(&namespace),
        1,
        context.serialized_size(),
    )?;

    let idempotency_key = req
        .idempotency_key
        .as_deref()
        .map(|k| format!("{JOB_IDEMPOTENCY_PREFIX}{k}"));
    if let Some(key) = &idempotency_key
        && let Some(existing) = state
            .storage
            .find_by_idempotency_key(&tenant_id, key)
            .await
            .map_err(|e| ApiError::from_storage(e, "job"))?
    {
        return Ok((
            StatusCode::OK,
            Json(load_job_view(&state, &existing).await?),
        ));
    }

    let spec = JobSpec {
        handler: req.handler.clone(),
        queue: req.queue.clone(),
        retry: req.retry.clone(),
    };
    let sequence = orch8_engine::jobs::ensure_job_sequence(
        state.storage.as_ref(),
        &tenant_id,
        &namespace,
        &spec,
    )
    .await?;

    let now = Utc::now();
    let instance = orch8_engine::jobs::build_job_instance(
        NewJob {
            tenant_id: tenant_id.clone(),
            namespace,
            spec,
            payload: req.payload.clone(),
            priority: req.priority.unwrap_or(Priority::Normal),
            run_at: req.first_run_at(now),
            idempotency_key: idempotency_key.clone(),
            metadata: req.metadata.clone(),
        },
        sequence.id,
    );

    match state
        .storage
        .create_instance_admitted(&instance, entitlement_plan.max_active_instances)
        .await
    {
        Ok(()) => {}
        Err(err @ StorageError::Conflict(_)) => {
            // Lost a concurrent race on the same idempotency key: return the
            // winner exactly like `POST /instances` does.
            if let Some(key) = &idempotency_key
                && let Some(existing) = state
                    .storage
                    .find_by_idempotency_key(&tenant_id, key)
                    .await
                    .map_err(|e| ApiError::from_storage(e, "job"))?
            {
                return Ok((
                    StatusCode::OK,
                    Json(load_job_view(&state, &existing).await?),
                ));
            }
            return Err(ApiError::from_storage(err, "job"));
        }
        Err(e) => return Err(ApiError::from_storage(e, "job")),
    }

    let job = job_view(
        &instance,
        orch8_engine::jobs::job_from_instance(&instance, None),
    )?;
    Ok((StatusCode::CREATED, Json(job)))
}

/// Fetch one job.
#[utoipa::path(get, path = "/jobs/{id}", tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id (= instance id)")),
    responses(
        (status = 200, description = "Job", body = Job),
        (status = 404, description = "Job not found"),
    )
)]
pub async fn get_job(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance = fetch_job_instance(&state, &tenant_ctx, id).await?;
    Ok(Json(load_job_view(&state, &instance).await?))
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub handler: Option<String>,
    pub status: Option<String>,
    #[serde(default = "default_list_limit")]
    pub limit: u32,
    pub cursor: Option<String>,
    pub tenant_id: Option<String>,
    pub namespace: Option<String>,
}

const fn default_list_limit() -> u32 {
    50
}

/// List jobs, newest first, with keyset pagination.
#[utoipa::path(get, path = "/jobs", tag = "jobs",
    params(
        ("handler" = Option<String>, Query, description = "Only jobs for this handler"),
        ("status" = Option<String>, Query, description = "scheduled | running | completed | failed | cancelled | dead_lettered"),
        ("limit" = Option<u32>, Query, description = "Page size (default 50, max 500)"),
        ("cursor" = Option<String>, Query, description = "Opaque `next_cursor` from the previous page"),
        ("tenant_id" = Option<String>, Query, description = "Tenant filter (admin callers without X-Tenant-Id only)"),
        ("namespace" = Option<String>, Query, description = "Namespace filter"),
    ),
    responses(
        (status = 200, description = "Page of jobs", body = JobPage),
        (status = 400, description = "Invalid status or cursor"),
    )
)]
pub async fn list_jobs(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    admin_ctx: crate::auth::OptionalAdmin,
    Query(q): Query<ListJobsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Same isolation rule as `GET /instances`: the header scopes the list;
    // without it only an admin may list (optionally narrowed by tenant_id).
    let tenant_id = if let Some(axum::Extension(ctx)) = &tenant_ctx {
        Some(ctx.tenant_id.clone())
    } else {
        crate::api_keys::require_admin(&admin_ctx)?;
        q.tenant_id
            .filter(|t| !t.is_empty())
            .map(TenantId::unchecked)
    };

    let status = q
        .status
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| {
            JobStatus::from_str_loose(s)
                .ok_or_else(|| ApiError::InvalidArgument(format!("unknown job status: {s}")))
        })
        .transpose()?;
    let retries = match status {
        Some(JobStatus::Failed) => Some(false),
        Some(JobStatus::DeadLettered) => Some(true),
        _ => None,
    };
    let before = q
        .cursor
        .as_deref()
        .filter(|c| !c.is_empty())
        .map(|c| {
            Uuid::parse_str(c)
                .map(InstanceId::from_uuid)
                .map_err(|_| ApiError::InvalidArgument("invalid cursor".into()))
        })
        .transpose()?;
    let limit = q.limit.clamp(1, 500);

    let filter = InstanceFilter {
        tenant_id,
        namespace: q.namespace.filter(|n| !n.is_empty()).map(Namespace::new),
        sequence_id: None,
        states: status.map(JobStatus::instance_states),
        metadata_filter: Some(orch8_engine::jobs::job_metadata_filter(
            q.handler.as_deref().filter(|h| !h.is_empty()),
            retries,
        )),
        priority: None,
    };

    let mut instances = state
        .storage
        .list_instances_keyset(&filter, before, limit + 1)
        .await
        .map_err(|e| ApiError::from_storage(e, "jobs"))?;
    let has_more = instances.len() > limit as usize;
    instances.truncate(limit as usize);
    let next_cursor = if has_more {
        instances.last().map(|i| i.id.into_uuid().to_string())
    } else {
        None
    };
    let items = orch8_engine::jobs::load_jobs(state.storage.as_ref(), &instances).await?;
    Ok(Json(JobPage {
        items,
        next_cursor,
        has_more,
    }))
}

/// Cancel a job. A job that has not started yet is cancelled immediately
/// (200, status `cancelled`); a running job receives a cancel signal that
/// the engine applies at its next step boundary (202). Cancelling a job that
/// already finished is a 409.
#[utoipa::path(delete, path = "/jobs/{id}", tag = "jobs",
    params(("id" = Uuid, Path, description = "Job id (= instance id)")),
    responses(
        (status = 200, description = "Job cancelled", body = Job),
        (status = 202, description = "Cancellation requested for a running job", body = Job),
        (status = 404, description = "Job not found"),
        (status = 409, description = "Job already finished"),
    )
)]
pub async fn cancel_job(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance = fetch_job_instance(&state, &tenant_ctx, id).await?;
    if instance.state.is_terminal() {
        return Err(ApiError::Conflict(format!(
            "job {id} already finished ({})",
            instance.state
        )));
    }

    // Not yet started (or waiting out a retry backoff): cancel directly so a
    // far-future job does not linger until its fire time. CAS on the state
    // we read — losing the race to a claim falls through to the signal path.
    if matches!(
        instance.state,
        InstanceState::Scheduled | InstanceState::Paused
    ) && state
        .storage
        .conditional_update_instance_state(
            instance.id,
            instance.state,
            InstanceState::Cancelled,
            None,
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "job"))?
    {
        let refreshed = state
            .storage
            .get_instance(instance.id)
            .await
            .map_err(|e| ApiError::from_storage(e, "job"))?
            .unwrap_or(instance);
        return Ok((
            StatusCode::OK,
            Json(load_job_view(&state, &refreshed).await?),
        ));
    }

    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id: instance.id,
        signal_type: SignalType::Cancel,
        payload: serde_json::Value::Null,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match state.storage.enqueue_signal_if_active(&signal).await {
        Ok(()) => {}
        Err(StorageError::TerminalTarget { .. }) => {
            return Err(ApiError::Conflict(format!("job {id} already finished")));
        }
        Err(e) => return Err(ApiError::from_storage(e, "job")),
    }
    let refreshed = state
        .storage
        .get_instance(instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "job"))?
        .unwrap_or(instance);
    Ok((
        StatusCode::ACCEPTED,
        Json(load_job_view(&state, &refreshed).await?),
    ))
}
