use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use orch8_types::ids::SequenceId;
use orch8_types::sequence::SequenceDefinition;

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sequences", post(create_sequence))
        .route("/sequences/{id}", get(get_sequence))
        .route("/sequences/by-name", get(get_sequence_by_name))
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
    Json(seq): Json<SequenceDefinition>,
) -> Result<impl IntoResponse, ApiError> {
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
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;

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
    Query(q): Query<ByNameQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = orch8_types::ids::TenantId(q.tenant_id);
    let namespace = orch8_types::ids::Namespace(q.namespace);

    let seq = state
        .storage
        .get_sequence_by_name(&tenant_id, &namespace, &q.name, q.version)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", q.name)))?;

    Ok(Json(seq))
}
