use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::ids::TenantId;
use orch8_types::session::{Session, SessionState};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sessions", post(create_session))
        .route("/sessions/{id}", get(get_session))
        .route("/sessions/by-key/{tenant_id}/{key}", get(get_session_by_key))
        .route("/sessions/{id}/data", patch(update_session_data))
        .route("/sessions/{id}/state", patch(update_session_state))
        .route("/sessions/{id}/instances", get(list_session_instances))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateSessionRequest {
    tenant_id: TenantId,
    session_key: String,
    #[serde(default)]
    data: serde_json::Value,
    #[serde(default)]
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[utoipa::path(
    post,
    path = "/sessions",
    request_body = CreateSessionRequest,
    responses((status = 201, body = Session))
)]
async fn create_session(
    State(state): State<AppState>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let now = chrono::Utc::now();
    let session = Session {
        id: Uuid::new_v4(),
        tenant_id: body.tenant_id,
        session_key: body.session_key,
        data: body.data,
        state: SessionState::Active,
        created_at: now,
        updated_at: now,
        expires_at: body.expires_at,
    };
    state
        .storage
        .create_session(&session)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?;
    Ok((StatusCode::CREATED, Json(session)))
}

#[utoipa::path(
    get,
    path = "/sessions/{id}",
    params(("id" = Uuid, Path, description = "Session ID")),
    responses((status = 200, body = Session))
)]
async fn get_session(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let session = state
        .storage
        .get_session(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?
        .ok_or_else(|| ApiError::NotFound("session not found".into()))?;
    Ok(Json(session))
}

#[utoipa::path(
    get,
    path = "/sessions/by-key/{tenant_id}/{key}",
    params(
        ("tenant_id" = String, Path, description = "Tenant ID"),
        ("key" = String, Path, description = "Session key"),
    ),
    responses((status = 200, body = Session))
)]
async fn get_session_by_key(
    State(state): State<AppState>,
    Path((tenant_id, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let session = state
        .storage
        .get_session_by_key(&TenantId(tenant_id), &key)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?
        .ok_or_else(|| ApiError::NotFound("session not found".into()))?;
    Ok(Json(session))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateSessionDataRequest {
    data: serde_json::Value,
}

#[utoipa::path(
    patch,
    path = "/sessions/{id}/data",
    params(("id" = Uuid, Path, description = "Session ID")),
    request_body = UpdateSessionDataRequest,
    responses((status = 200, description = "Session data updated"))
)]
async fn update_session_data(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSessionDataRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .storage
        .update_session_data(id, &body.data)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateSessionStateRequest {
    state: SessionState,
}

#[utoipa::path(
    patch,
    path = "/sessions/{id}/state",
    params(("id" = Uuid, Path, description = "Session ID")),
    request_body = UpdateSessionStateRequest,
    responses((status = 200, description = "Session state updated"))
)]
async fn update_session_state(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSessionStateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .storage
        .update_session_state(id, body.state)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    get,
    path = "/sessions/{id}/instances",
    params(("id" = Uuid, Path, description = "Session ID")),
    responses((status = 200, body = Vec<orch8_types::instance::TaskInstance>))
)]
async fn list_session_instances(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instances = state
        .storage
        .list_session_instances(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?;
    Ok(Json(instances))
}
