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
use orch8_storage::externalizing::{context_pointer_mut, escape_literals, externalize_paths};
use orch8_types::context::ExecutionContext;
use orch8_types::ids::InstanceId;
use orch8_types::sequence::{SequenceDefinition, is_context_pointer};

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchContextDataRequest {
    /// Shallow merge into context.data; omitted keys and all other sections remain unchanged.
    #[serde(default)]
    pub patch: Map<String, Value>,
    /// Remove these data keys before applying the patch; other sections remain intact.
    #[serde(default)]
    pub remove_keys: Vec<String>,
    /// Replace existing values by JSON Pointer relative to context.data.
    #[serde(default)]
    pub pointer_patch: Map<String, Value>,
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
        for (key, mut value) in req.patch.clone() {
            escape_literals(&mut value);
            data.insert(key, value);
        }
        for (path, mut value) in req.pointer_patch.clone() {
            let target = is_context_pointer(&path)
                .then(|| context_pointer_mut(&mut context.data, &path))
                .flatten()
                .ok_or_else(|| {
                    ApiError::InvalidArgument(format!(
                        "pointer_patch path '{path}' must name an existing inline slot"
                    ))
                })?;
            escape_literals(&mut value);
            *target = value;
        }
        if persist_context(
            state.storage.as_ref(),
            state.max_context_bytes,
            instance_id,
            instance.sequence_id,
            expected,
            &mut context,
        )
        .await?
        {
            return Ok(StatusCode::OK);
        }
        tokio::task::yield_now().await;
    }
    Err(ApiError::Conflict(
        "context changed concurrently; retry the data patch".into(),
    ))
}

pub(super) async fn persist_context(
    storage: &dyn orch8_storage::StorageBackend,
    max_context_bytes: u32,
    id: InstanceId,
    sequence_id: orch8_types::ids::SequenceId,
    expected: chrono::DateTime<chrono::Utc>,
    context: &mut ExecutionContext,
) -> Result<bool, ApiError> {
    let sequence = storage
        .get_sequence(sequence_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?;
    let payloads = externalize_paths(
        &mut context.data,
        &id.to_string(),
        sequence
            .iter()
            .flat_map(SequenceDefinition::externalized_context_paths),
    );
    context.check_size(max_context_bytes)?;
    if payloads.is_empty() {
        return storage
            .update_instance_context_cas(id, context, expected)
            .await
            .map_err(|error| ApiError::from_storage(error, "instance"));
    }
    storage
        .update_prepared_context_cas(id, context, &payloads, expected, None)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))
}
