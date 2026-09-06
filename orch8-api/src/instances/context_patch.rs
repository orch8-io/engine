//! Targeted data updates preserve runtime/config/audit and concurrent context writes.
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Map, Value};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;
use orch8_types::ids::InstanceId;

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchContextDataRequest {
    /// Shallow merge into context.data; omitted keys and all other sections remain unchanged.
    pub patch: Map<String, Value>,
    /// Remove these data keys before applying the patch; other sections remain intact.
    #[serde(default)]
    pub remove_keys: Vec<String>,
}

#[utoipa::path(patch, path = "/instances/{id}/context/data", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = PatchContextDataRequest,
    responses(
        (status = 200, description = "Data patch applied without replacing other context sections"),
        (status = 400, description = "Existing data is not an object"),
        (status = 413, description = "Merged context exceeds its size limit"),
        (status = 404, description = "Instance not found"),
        (status = 409, description = "Concurrent context updates; retry the request"),
    )
)]
pub async fn patch_context_data(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<PatchContextDataRequest>,
) -> Result<StatusCode, ApiError> {
    let instance_id = InstanceId::from_uuid(id);
    for _ in 0..5 {
        let instance = state
            .storage
            .get_instance(instance_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "instance"))?
            .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
        crate::auth::enforce_tenant_access(
            &tenant_ctx,
            &instance.tenant_id,
            &format!("instance {id}"),
        )?;
        let expected = instance.updated_at;
        let mut context = instance.context;
        let data = context.data.as_object_mut().ok_or_else(|| {
            ApiError::InvalidArgument("context.data must be an object for a data patch".into())
        })?;
        for key in &req.remove_keys {
            data.remove(key);
        }
        data.extend(req.patch.clone());
        context.check_size(state.max_context_bytes)?;
        if state
            .storage
            .update_instance_context_cas(instance_id, &context, expected)
            .await
            .map_err(|error| ApiError::from_storage(error, "instance"))?
        {
            return Ok(StatusCode::OK);
        }
        tokio::task::yield_now().await;
    }
    Err(ApiError::Conflict(
        "context changed concurrently; retry the data patch".into(),
    ))
}
