//! Artifact read endpoints.
//!
//! Exposes the durable-artifact primitive over HTTP so a control plane / viewer
//! can list an instance's artifacts and fetch their bytes. Both are tenant-
//! scoped: the instance the artifact belongs to must be owned by the caller
//! (when a tenant header is present), mirroring `get_instance`.

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use uuid::Uuid;

use orch8_types::ids::InstanceId;

use crate::auth::TenantContext;
use crate::error::ApiError;
use crate::AppState;

/// Verify the instance exists and (when a tenant header is present) is owned by
/// the caller. Returns the same opaque `NotFound` for both "missing" and
/// "cross-tenant" so an artifact's existence can't be probed across tenants.
async fn require_owned_instance(
    state: &AppState,
    tenant: Option<&TenantContext>,
    instance_id: InstanceId,
    not_found: impl Fn() -> ApiError,
) -> Result<(), ApiError> {
    let inst = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(&not_found)?;
    if let Some(ctx) = tenant {
        if inst.tenant_id != ctx.tenant_id {
            return Err(not_found());
        }
    }
    Ok(())
}

#[utoipa::path(
    get, path = "/instances/{id}/artifacts", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses(
        (status = 200, description = "Artifact metadata for the instance", body = serde_json::Value),
        (status = 404, description = "Instance not found"),
    )
)]
pub async fn list_instance_artifacts(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<TenantContext>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId::from_uuid(id);
    require_owned_instance(
        &state,
        tenant_ctx.as_ref().map(|e| &e.0),
        instance_id,
        || ApiError::NotFound(format!("instance {id}")),
    )
    .await?;

    let metas = state
        .storage
        .list_artifacts(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "artifacts"))?;

    let items: Vec<_> = metas
        .into_iter()
        .map(|m| serde_json::json!({ "key": m.key, "size": m.size, "uri": format!("artifact://{}", m.key) }))
        .collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

/// Optional query params for [`get_artifact_bytes`].
#[derive(Debug, Deserialize)]
pub struct ArtifactBytesQuery {
    /// Override the response `Content-Type` (the value isn't stored alongside
    /// the bytes; callers that know the type — e.g. from the producing step's
    /// `ArtifactRef` — can pass it for inline rendering).
    pub content_type: Option<String>,
}

#[utoipa::path(
    get, path = "/artifacts/{key}", tag = "instances",
    params(
        ("key" = String, Path, description = "Artifact object key (`<instance_id>/<artifact_id>`)"),
        ("content_type" = Option<String>, Query, description = "Override response Content-Type"),
    ),
    responses(
        (status = 200, description = "Raw artifact bytes"),
        (status = 404, description = "Artifact not found"),
    )
)]
pub async fn get_artifact_bytes(
    State(state): State<AppState>,
    tenant_ctx: Option<axum::Extension<TenantContext>>,
    Path(key): Path<String>,
    Query(q): Query<ArtifactBytesQuery>,
) -> Result<Response, ApiError> {
    let not_found = || ApiError::NotFound(format!("artifact {key}"));

    // The key is `<instance_id>/<artifact_id>` — tenant-scope by the owning
    // instance parsed from the prefix.
    let instance_id = key
        .split('/')
        .next()
        .and_then(|s| Uuid::parse_str(s).ok())
        .map(InstanceId::from_uuid)
        .ok_or_else(not_found)?;
    require_owned_instance(
        &state,
        tenant_ctx.as_ref().map(|e| &e.0),
        instance_id,
        not_found,
    )
    .await?;

    let bytes = state
        .storage
        .get_artifact(&key)
        .await
        .map_err(|e| ApiError::from_storage(e, "artifact"))?
        .ok_or_else(not_found)?;

    let content_type = q
        .content_type
        .unwrap_or_else(|| "application/octet-stream".to_string());
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response())
}
