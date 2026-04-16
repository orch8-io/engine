use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::ids::{InstanceId, SequenceId};
use orch8_types::sequence::SequenceDefinition;

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sequences", post(create_sequence))
        .route("/sequences/{id}", get(get_sequence))
        .route("/sequences/{id}/deprecate", post(deprecate_sequence))
        .route("/sequences/by-name", get(get_sequence_by_name))
        .route("/sequences/versions", get(list_sequence_versions))
        .route("/sequences/migrate-instance", post(migrate_instance))
}

#[utoipa::path(post, path = "/sequences", tag = "sequences",
    request_body = SequenceDefinition,
    responses(
        (status = 201, description = "Sequence created", body = serde_json::Value),
        (status = 409, description = "Sequence already exists"),
    )
)]
pub(crate) async fn create_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(mut seq): Json<SequenceDefinition>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &seq.tenant_id)?;
    seq.tenant_id = tenant_id;

    state
        .storage
        .create_sequence(&seq)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "id": seq.id })),
    ))
}

#[utoipa::path(get, path = "/sequences/{id}", tag = "sequences",
    params(("id" = Uuid, Path, description = "Sequence ID")),
    responses(
        (status = 200, description = "Sequence found", body = SequenceDefinition),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn get_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;

    crate::auth::enforce_tenant_access(&tenant_ctx, &seq.tenant_id, &format!("sequence {id}"))?;

    Ok(Json(seq))
}

#[derive(Deserialize)]
pub(crate) struct ByNameQuery {
    tenant_id: String,
    namespace: String,
    name: String,
    version: Option<i32>,
}

#[utoipa::path(get, path = "/sequences/by-name", tag = "sequences",
    params(
        ("tenant_id" = String, Query, description = "Tenant ID"),
        ("namespace" = String, Query, description = "Namespace"),
        ("name" = String, Query, description = "Sequence name"),
        ("version" = Option<i32>, Query, description = "Optional version"),
    ),
    responses(
        (status = 200, description = "Sequence found", body = SequenceDefinition),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn get_sequence_by_name(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ByNameQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::scoped_tenant_id(&tenant_ctx, Some(&q.tenant_id))
        .unwrap_or_else(|| orch8_types::ids::TenantId(q.tenant_id.clone()));
    let namespace = orch8_types::ids::Namespace(q.namespace);

    let seq = state
        .storage
        .get_sequence_by_name(&tenant_id, &namespace, &q.name, q.version)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", q.name)))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &seq.tenant_id,
        &format!("sequence {}", q.name),
    )?;

    Ok(Json(seq))
}

#[utoipa::path(post, path = "/sequences/{id}/deprecate", tag = "sequences",
    params(("id" = Uuid, Path, description = "Sequence ID to deprecate")),
    responses(
        (status = 204, description = "Sequence deprecated"),
    )
)]
pub(crate) async fn deprecate_sequence(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;

    crate::auth::enforce_tenant_access(&tenant_ctx, &seq.tenant_id, &format!("sequence {id}"))?;

    state
        .storage
        .deprecate_sequence(SequenceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path = "/sequences/versions", tag = "sequences",
    params(
        ("tenant_id" = String, Query, description = "Tenant ID"),
        ("namespace" = String, Query, description = "Namespace"),
        ("name" = String, Query, description = "Sequence name"),
    ),
    responses(
        (status = 200, description = "All versions", body = Vec<SequenceDefinition>),
    )
)]
pub(crate) async fn list_sequence_versions(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ByNameQuery>,
) -> Result<Json<Vec<SequenceDefinition>>, ApiError> {
    let tenant_id = crate::auth::scoped_tenant_id(&tenant_ctx, Some(&q.tenant_id))
        .unwrap_or_else(|| orch8_types::ids::TenantId(q.tenant_id.clone()));
    let namespace = orch8_types::ids::Namespace(q.namespace);

    let versions = state
        .storage
        .list_sequence_versions(&tenant_id, &namespace, &q.name)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    Ok(Json(versions))
}

/// Hot migration: rebind a running instance to a different sequence version.
/// The instance will pick up the new version's block definitions on its next tick.
/// Only non-terminal instances can be migrated.
#[derive(Deserialize, ToSchema)]
pub(crate) struct MigrateInstanceRequest {
    /// The instance to migrate.
    instance_id: Uuid,
    /// The new sequence version to bind the instance to.
    target_sequence_id: Uuid,
}

#[utoipa::path(post, path = "/sequences/migrate-instance", tag = "sequences",
    request_body = MigrateInstanceRequest,
    responses(
        (status = 200, description = "Instance migrated to new sequence version"),
        (status = 400, description = "Instance in terminal state"),
        (status = 404, description = "Instance or sequence not found"),
    )
)]
pub(crate) async fn migrate_instance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<MigrateInstanceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate the instance exists and is not terminal.
    let instance = state
        .storage
        .get_instance(InstanceId(req.instance_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", req.instance_id)))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {}", req.instance_id),
    )?;

    if instance.state.is_terminal() {
        return Err(ApiError::InvalidArgument(format!(
            "instance {} is in terminal state {}",
            req.instance_id, instance.state
        )));
    }

    // Validate the target sequence exists.
    let _target_seq = state
        .storage
        .get_sequence(SequenceId(req.target_sequence_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", req.target_sequence_id)))?;

    // Rebind: update the instance's sequence_id to the new version.
    state
        .storage
        .update_instance_sequence(
            InstanceId(req.instance_id),
            SequenceId(req.target_sequence_id),
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    Ok(Json(serde_json::json!({
        "migrated": true,
        "instance_id": req.instance_id,
        "from_sequence_id": instance.sequence_id,
        "to_sequence_id": req.target_sequence_id,
    })))
}
