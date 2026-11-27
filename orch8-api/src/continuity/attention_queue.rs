use axum::{
    Extension, Json,
    extract::{Path, Query, State},
};
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{ContinuityId, DataClassification},
    continuity_advanced::{AttentionState, AttentionTask, AttentionTaskId},
    ids::TenantId,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AppState,
    auth::{OptionalAdmin, OptionalTenant, PrincipalContext, principal_is_operator},
    error::ApiError,
};

#[derive(Deserialize)]
pub(crate) struct AttentionQueueQuery {
    state: Option<AttentionState>,
    limit: Option<u32>,
    after_deadline: Option<DateTime<Utc>>,
    after_id: Option<AttentionTaskId>,
}

#[derive(Serialize, ToSchema)]
struct AttentionQueuePosition {
    deadline: DateTime<Utc>,
    id: AttentionTaskId,
}

#[derive(Serialize, ToSchema)]
struct AttentionQueueRow {
    id: AttentionTaskId,
    continuity_id: ContinuityId,
    state: AttentionState,
    priority: u8,
    deadline: DateTime<Utc>,
    lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct AttentionQueue {
    tenant_id: TenantId,
    state: AttentionState,
    items: Vec<AttentionQueueRow>,
    next: Option<AttentionQueuePosition>,
}

#[utoipa::path(get, path = "/continuity/attention", tag = "continuity",
    params(
        ("state" = Option<AttentionState>, Query, description = "Task state (default pending)"),
        ("limit" = Option<u32>, Query, description = "Maximum tasks (1–100, default 50)"),
        ("after_deadline" = Option<String>, Query, description = "Deadline of the previous page's last task"),
        ("after_id" = Option<String>, Query, description = "UUID of the previous page's last task"),
    ),
    responses(
        (status = 200, description = "Scoped metadata page", body = AttentionQueue),
        (status = 400, description = "Missing tenant or invalid pagination"),
        (status = 403, description = "Operator capability required"),
    )
)]
pub(crate) async fn list_attention_tasks(
    State(app): State<AppState>,
    tenant: OptionalTenant,
    admin: OptionalAdmin,
    principal: Option<Extension<PrincipalContext>>,
    Query(query): Query<AttentionQueueQuery>,
) -> Result<Json<AttentionQueue>, ApiError> {
    let tenant_id = operator_tenant(tenant, &admin, principal.as_ref().map(|actor| &actor.0))?;
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) || query.after_deadline.is_some() != query.after_id.is_some() {
        return Err(ApiError::InvalidArgument(
            "invalid attention queue pagination".into(),
        ));
    }
    let state = query.state.unwrap_or(AttentionState::Pending);
    let mut rows = app
        .storage
        .list_attention_tasks(
            &tenant_id,
            state,
            query.after_deadline.zip(query.after_id),
            limit + 1,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "attention queue"))?;
    if rows.len() > limit as usize + 1
        || rows
            .iter()
            .any(|task| task.tenant_id != tenant_id || task.state != state)
    {
        return Err(ApiError::Internal(
            "attention queue scope could not be verified".into(),
        ));
    }
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next = rows
        .last()
        .filter(|_| has_more)
        .map(|last| AttentionQueuePosition {
            deadline: last.deadline,
            id: last.id,
        });
    let items = rows.into_iter().map(AttentionQueueRow::from).collect();
    Ok(Json(AttentionQueue {
        tenant_id,
        state,
        items,
        next,
    }))
}

impl From<AttentionTask> for AttentionQueueRow {
    fn from(row: AttentionTask) -> Self {
        Self {
            id: row.id,
            continuity_id: row.continuity_id,
            state: row.state,
            priority: row.priority,
            deadline: row.deadline,
            lease_expires_at: row.lease_expires_at,
        }
    }
}

fn operator_tenant(
    tenant: OptionalTenant,
    admin: &OptionalAdmin,
    principal: Option<&PrincipalContext>,
) -> Result<TenantId, ApiError> {
    if admin.is_none() && !principal_is_operator(principal) {
        return Err(ApiError::Forbidden(
            "attention inspection requires Operator access".into(),
        ));
    }
    tenant
        .map(|Extension(tenant)| tenant.tenant_id)
        .ok_or_else(|| ApiError::InvalidArgument("attention inspection requires a tenant".into()))
}

#[derive(Serialize, ToSchema)]
pub(crate) struct AttentionTaskDetail {
    tenant_id: TenantId,
    task: AttentionQueueRow,
    classification: DataClassification,
    required_skills: Vec<String>,
    allowed_regions: Vec<String>,
    assignee: Option<String>,
    decision_recorded: bool,
    decided_at: Option<DateTime<Utc>>,
}

#[utoipa::path(get, path = "/continuity/attention/{id}", tag = "continuity",
    params(("id" = String, Path, description = "Exact attention task UUID")),
    responses(
        (status = 200, description = "Scoped task metadata", body = AttentionTaskDetail),
        (status = 403, description = "Operator capability required"),
        (status = 404, description = "Exact task not found in the selected tenant"),
    )
)]
pub(crate) async fn inspect_attention_task(
    State(app): State<AppState>,
    tenant: OptionalTenant,
    admin: OptionalAdmin,
    principal: Option<Extension<PrincipalContext>>,
    Path(id): Path<AttentionTaskId>,
) -> Result<Json<AttentionTaskDetail>, ApiError> {
    let tenant_id = operator_tenant(tenant, &admin, principal.as_ref().map(|actor| &actor.0))?;
    let task = app
        .storage
        .get_attention_task(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "attention task"))?
        .filter(|task| task.tenant_id == tenant_id && task.id == id)
        .ok_or_else(|| ApiError::NotFound("attention task".into()))?;
    Ok(Json(AttentionTaskDetail {
        tenant_id,
        classification: task.classification,
        required_skills: task.required_skills.clone(),
        allowed_regions: task.allowed_regions.clone(),
        assignee: task.assignee.clone(),
        decision_recorded: task.decision_sha256.is_some(),
        decided_at: task.decided_at,
        task: task.into(),
    }))
}
