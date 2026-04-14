//! Audit log listing for an instance.

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use uuid::Uuid;

use orch8_types::ids::InstanceId;

use crate::error::ApiError;
use crate::AppState;

#[utoipa::path(
    get,
    path = "/instances/{id}/audit",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, body = Vec<orch8_types::audit::AuditLogEntry>))
)]
pub(crate) async fn list_audit_log(
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
