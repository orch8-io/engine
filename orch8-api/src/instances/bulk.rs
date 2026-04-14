//! Bulk operations and DLQ listing.

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Json;

use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{Namespace, SequenceId};
use orch8_types::instance::InstanceState;

use super::types::{BulkRescheduleRequest, BulkUpdateStateRequest, CountResponse, ListQuery};
use crate::error::ApiError;
use crate::AppState;

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
    responses((status = 200, description = "Failed instances (DLQ)", body = Vec<orch8_types::instance::TaskInstance>))
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
