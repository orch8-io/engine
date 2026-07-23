//! Bulk operations and DLQ listing.

use axum::Json;
use axum::extract::{Query, State};
use axum::response::IntoResponse;

use chrono::Utc;
use uuid::Uuid;

use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{Namespace, SequenceId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::signal::{Signal, SignalType};

use super::types::{
    BatchAction, BatchActionRequest, BatchActionResponse, BulkFilter, BulkRescheduleRequest,
    BulkUpdateStateRequest, CountResponse, ListQuery,
};
use crate::AppState;
use crate::error::ApiError;

/// Top-level metadata equality filters (reuses the indexed metadata path) —
/// the same construction `batch_action` uses, shared by every bulk endpoint
/// so a caller-supplied `metadata` filter is never silently ignored.
fn metadata_filter(filter: &BulkFilter) -> Option<serde_json::Value> {
    filter.metadata.as_ref().filter(|m| !m.is_empty()).map(|m| {
        serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect(),
        )
    })
}

#[utoipa::path(patch, path = "/instances/bulk/state", tag = "instances",
    request_body = BulkUpdateStateRequest,
    responses((status = 200, description = "Bulk state update result", body = CountResponse))
)]
pub async fn bulk_update_state(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<BulkUpdateStateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, req.filter.tenant_id.as_deref());
    if scoped_tenant.is_none() {
        return Err(ApiError::InvalidArgument(
            "bulk operations require a tenant_id".into(),
        ));
    }
    let metadata = metadata_filter(&req.filter);
    let filter = InstanceFilter {
        tenant_id: scoped_tenant,
        namespace: req.filter.namespace.map(Namespace::new),
        sequence_id: req.filter.sequence_id.map(SequenceId::from_uuid),
        states: req.filter.states,
        metadata_filter: metadata,
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
pub async fn bulk_reschedule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<BulkRescheduleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, req.filter.tenant_id.as_deref());
    if scoped_tenant.is_none() {
        return Err(ApiError::InvalidArgument(
            "bulk operations require a tenant_id".into(),
        ));
    }
    let metadata = metadata_filter(&req.filter);
    let filter = InstanceFilter {
        tenant_id: scoped_tenant,
        namespace: req.filter.namespace.map(Namespace::new),
        sequence_id: req.filter.sequence_id.map(SequenceId::from_uuid),
        states: req.filter.states,
        metadata_filter: metadata,
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
pub async fn list_dlq(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let filter = InstanceFilter {
        tenant_id: scoped_tenant,
        namespace: q.namespace.map(Namespace::new),
        sequence_id: q.sequence_id.map(SequenceId::from_uuid),
        states: Some(vec![InstanceState::Failed]),
        metadata_filter: None,
        priority: None,
    };

    let pagination = Pagination {
        offset: q.offset,
        limit: q.limit,
        sort_ascending: false,
    }
    .capped();

    let instances = state
        .storage
        .list_instances(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    Ok(Json(instances))
}

/// Apply one control action to every instance matching a filter. Capped,
/// audited, and dry-run-able. Tenant-scoped — a tenant can only act on its own
/// instances.
#[utoipa::path(post, path = "/instances/batch-action", tag = "instances",
    request_body = BatchActionRequest,
    responses(
        (status = 200, description = "Batch action result", body = BatchActionResponse),
        (status = 400, description = "Missing tenant or invalid action params"),
    )
)]
pub async fn batch_action(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<BatchActionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped_tenant = crate::auth::scoped_tenant_id(&tenant_ctx, req.filter.tenant_id.as_deref());
    let Some(tenant_id) = scoped_tenant else {
        return Err(ApiError::InvalidArgument(
            "batch actions require a tenant_id".into(),
        ));
    };

    // The `signal` action needs an explicit signal name.
    if matches!(req.action, BatchAction::Signal) && req.signal_type.is_none() {
        return Err(ApiError::InvalidArgument(
            "the `signal` action requires `signal_type`".into(),
        ));
    }

    // Top-level metadata equality filters (reuses the indexed metadata path).
    let metadata = metadata_filter(&req.filter);

    let filter = InstanceFilter {
        tenant_id: Some(tenant_id.clone()),
        namespace: req.filter.namespace.clone().map(Namespace::new),
        sequence_id: req.filter.sequence_id.map(SequenceId::from_uuid),
        states: req.filter.states.clone(),
        metadata_filter: metadata,
        priority: None,
    };

    let limit = req.limit.clamp(1, 10_000);
    let pagination = Pagination {
        offset: 0,
        limit,
        sort_ascending: true,
    };
    let instances = state
        .storage
        .list_instances(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    let matched = instances.len() as u64;
    if req.dry_run {
        return Ok(Json(BatchActionResponse {
            matched,
            applied: 0,
            skipped: 0,
            failed: 0,
            dry_run: true,
        }));
    }

    let mut applied = 0u64;
    let mut skipped = 0u64;
    let mut failed = 0u64;
    for inst in &instances {
        match apply_batch_action(&state, inst, &req).await {
            Ok(true) => {
                applied += 1;
                audit_batch_action(&state, inst, &req).await;
            }
            Ok(false) => skipped += 1,
            Err(()) => failed += 1,
        }
    }

    Ok(Json(BatchActionResponse {
        matched,
        applied,
        skipped,
        failed,
        dry_run: false,
    }))
}

/// Apply the action to a single instance. `Ok(true)` = applied, `Ok(false)` =
/// not applicable (skipped), `Err(())` = a storage error occurred.
async fn apply_batch_action(
    state: &AppState,
    inst: &TaskInstance,
    req: &BatchActionRequest,
) -> Result<bool, ()> {
    match req.action {
        BatchAction::Retry => {
            if inst.state != InstanceState::Failed {
                return Ok(false);
            }
            // Mirror the single-instance retry: clear stale tree + sentinel
            // outputs, reset the run identity (run_id, step counters — a
            // stale step budget would instantly re-exhaust), then re-schedule.
            let s = &state.storage;
            if s.delete_execution_tree(inst.id).await.is_err()
                || s.delete_sentinel_block_outputs(inst.id).await.is_err()
                || s.reset_instance_run(inst.id, &Uuid::now_v7().to_string())
                    .await
                    .is_err()
                || s.update_instance_state(inst.id, InstanceState::Scheduled, Some(Utc::now()))
                    .await
                    .is_err()
            {
                return Err(());
            }
            Ok(true)
        }
        BatchAction::Pause | BatchAction::Resume | BatchAction::Cancel | BatchAction::Signal => {
            let signal_type = match req.action {
                BatchAction::Pause => SignalType::Pause,
                BatchAction::Resume => SignalType::Resume,
                BatchAction::Cancel => SignalType::Cancel,
                // Validated non-None above.
                BatchAction::Signal => {
                    SignalType::Custom(req.signal_type.clone().unwrap_or_default())
                }
                BatchAction::Retry => {
                    tracing::error!("Retry reached non-retry batch arm");
                    return Err(());
                }
            };
            let signal = Signal {
                id: Uuid::now_v7(),
                instance_id: inst.id,
                signal_type,
                payload: req.payload.clone(),
                delivered: false,
                created_at: Utc::now(),
                delivered_at: None,
            };
            match state.storage.enqueue_signal_if_active(&signal).await {
                Ok(()) => Ok(true),
                // Terminal target: the signal doesn't apply — skip, not fail.
                Err(
                    orch8_types::error::StorageError::TerminalTarget { .. }
                    | orch8_types::error::StorageError::NotFound { .. },
                ) => Ok(false),
                Err(_) => Err(()),
            }
        }
    }
}

/// Record a best-effort audit-log entry for a successfully applied action.
async fn audit_batch_action(state: &AppState, inst: &TaskInstance, req: &BatchActionRequest) {
    let action = match req.action {
        BatchAction::Retry => "retry",
        BatchAction::Pause => "pause",
        BatchAction::Resume => "resume",
        BatchAction::Cancel => "cancel",
        BatchAction::Signal => "signal",
    };
    let entry = orch8_types::audit::AuditLogEntry {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        tenant_id: inst.tenant_id.clone(),
        event_type: "batch_action".into(),
        from_state: Some(inst.state.to_string()),
        to_state: None,
        block_id: None,
        details: serde_json::json!({ "action": action }),
        created_at: Utc::now(),
    };
    if let Err(e) = state.storage.append_audit_log(&entry).await {
        tracing::warn!(instance_id = %inst.id, error = %e, "failed to write batch-action audit entry");
    }
}
