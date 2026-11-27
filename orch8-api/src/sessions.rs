use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::session::{
    Session, SessionExecutionFilter, SessionExecutionSummary, SessionState, SessionSummary,
};

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sessions", get(list_sessions).post(create_session))
        .route("/sessions/{id}", get(get_session))
        .route(
            "/sessions/by-key/{tenant_id}/{key}",
            get(get_session_by_key),
        )
        .route("/sessions/{id}/data", patch(update_session_data))
        .route("/sessions/{id}/state", patch(update_session_state))
        .route("/sessions/{id}/instances", get(list_session_instances))
}

const SESSION_PAGE_SIZE: usize = 40;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionInventoryQuery {
    cursor: Option<Uuid>,
    search: Option<String>,
    state: Option<SessionState>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SessionInventoryPage {
    tenant_id: TenantId,
    cursor: Option<Uuid>,
    search: Option<String>,
    state: Option<SessionState>,
    items: Vec<SessionSummary>,
    next_cursor: Option<Uuid>,
}

#[utoipa::path(get, path = "/sessions",
    params(
        ("cursor" = Option<Uuid>, Query, description = "Exclusive cursor from the previous page"),
        ("search" = Option<String>, Query, description = "Literal case-sensitive substring of the session key"),
        ("state" = Option<SessionState>, Query, description = "Exact stored session state"),
    ),
    responses((status = 200, body = SessionInventoryPage), (status = 400, description = "Invalid inventory filters"), (status = 403, description = "Tenant scope required"))
)]
async fn list_sessions(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<SessionInventoryQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = tenant_ctx
        .ok_or_else(|| ApiError::Forbidden("session inventory requires a tenant scope".into()))?;
    if query
        .search
        .as_ref()
        .is_some_and(|search| search.is_empty() || search.len() > 512 || search.contains('\0'))
    {
        return Err(ApiError::InvalidArgument(
            "session key search must be 1–512 UTF-8 bytes without NUL".into(),
        ));
    }
    let mut items = state
        .storage
        .list_session_summaries(
            &tenant.tenant_id,
            query.cursor,
            query.search.as_deref(),
            query.state,
            (SESSION_PAGE_SIZE + 1) as u32,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "session inventory"))?;
    let has_more = items.len() > SESSION_PAGE_SIZE;
    items.truncate(SESSION_PAGE_SIZE);
    let next_cursor = items.last().filter(|_| has_more).map(|item| item.id);
    Ok(Json(SessionInventoryPage {
        tenant_id: tenant.tenant_id.clone(),
        cursor: query.cursor,
        search: query.search,
        state: query.state,
        items,
        next_cursor,
    }))
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
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if body.session_key.trim().is_empty() || body.session_key.len() > 512 {
        return Err(ApiError::InvalidArgument(
            "session_key must be non-blank and at most 512 bytes".into(),
        ));
    }
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    validate_session_data_size(&body.data, state.max_context_bytes)?;
    let now = chrono::Utc::now();
    let session = Session {
        id: Uuid::now_v7(),
        tenant_id,
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
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let session = state
        .storage
        .get_session(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?
        .ok_or_else(|| ApiError::NotFound("session not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &session.tenant_id, &format!("session {id}"))?;
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
    tenant_ctx: crate::auth::OptionalTenant,
    Path((tenant_id, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    if tenant_id.len() > 128 {
        return Err(ApiError::InvalidArgument(
            "tenant_id exceeds maximum length of 128".into(),
        ));
    }
    if key.len() > 512 {
        return Err(ApiError::InvalidArgument(
            "session key exceeds maximum length of 512".into(),
        ));
    }
    let session = state
        .storage
        .get_session_by_key(&TenantId::unchecked(tenant_id), &key)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?
        .ok_or_else(|| ApiError::NotFound("session not found".into()))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &session.tenant_id,
        &format!("session by key {}", session.id),
    )?;
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
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSessionDataRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let session = state
        .storage
        .get_session(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?
        .ok_or_else(|| ApiError::NotFound("session not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &session.tenant_id, &format!("session {id}"))?;
    validate_session_data_size(&body.data, state.max_context_bytes)?;
    state
        .storage
        .update_session_data(id, &body.data)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?;
    Ok(StatusCode::OK)
}

/// Session data uses the same serialized-size ceiling on create and update.
/// `max_context_bytes == 0` disables the check.
fn validate_session_data_size(
    data: &serde_json::Value,
    max_context_bytes: u32,
) -> Result<(), ApiError> {
    if max_context_bytes > 0 {
        // Fail-safe: an unserializable value counts as over the limit.
        let actual = serde_json::to_vec(data).map_or(usize::MAX, |bytes| bytes.len());
        if actual > max_context_bytes as usize {
            return Err(ApiError::PayloadTooLarge(format!(
                "session data is {actual} bytes, max is {max_context_bytes}"
            )));
        }
    }
    Ok(())
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
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSessionStateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let session = state
        .storage
        .get_session(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?
        .ok_or_else(|| ApiError::NotFound("session not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &session.tenant_id, &format!("session {id}"))?;
    state
        .storage
        .update_session_state(id, body.state)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct SessionRunsQuery {
    view: Option<String>,
    cursor: Option<String>,
    search: Option<String>,
    state: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SessionRunsPage {
    session_id: Uuid,
    tenant_id: TenantId,
    cursor: Option<Uuid>,
    search: Option<String>,
    state: Option<InstanceState>,
    next_cursor: Option<InstanceId>,
    items: Vec<SessionExecutionSummary>,
}

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum SessionRunsResponse {
    Page(SessionRunsPage),
    Full(Vec<orch8_types::instance::TaskInstance>),
}

const SESSION_RUNS_PAGE_SIZE: usize = 50;

#[utoipa::path(
    get,
    path = "/sessions/{id}/instances",
    params(("id" = Uuid, Path, description = "Session ID"),
        ("view" = Option<String>, Query, description = "page returns metadata in descending execution UUID order"),
        ("cursor" = Option<Uuid>, Query, description = "Exclusive cursor for the page view"),
        ("search" = Option<String>, Query, description = "ASCII-case-insensitive terms in execution UUID, sequence UUID or namespace"),
        ("state" = Option<InstanceState>, Query, description = "Exact stored execution state for the page view")),
    responses((status = 200, body = SessionRunsResponse))
)]
async fn list_session_instances(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(query): Query<SessionRunsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let session = state
        .storage
        .get_session(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?
        .ok_or_else(|| ApiError::NotFound("session not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &session.tenant_id, &format!("session {id}"))?;
    if query.view.as_deref() == Some("page") {
        let cursor = query
            .cursor
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|_| ApiError::InvalidArgument("invalid associated execution cursor".into()))?;
        let execution_state = query
            .state
            .as_deref()
            .map(str::parse::<InstanceState>)
            .transpose()
            .map_err(ApiError::InvalidArgument)?;
        let mut terms = Vec::new();
        if let Some(search) = &query.search {
            if search.len() > 512 || search.contains('\0') {
                return Err(ApiError::InvalidArgument(
                    "invalid associated execution search".into(),
                ));
            }
            terms.extend(search.split_ascii_whitespace().map(str::to_ascii_lowercase));
            if terms.is_empty() || terms.len() > 12 {
                return Err(ApiError::InvalidArgument(
                    "use 1–12 associated execution search terms".into(),
                ));
            }
        }
        let filter = SessionExecutionFilter {
            tenant_id: &session.tenant_id,
            session_id: id,
            before: cursor.map(InstanceId::from_uuid),
            terms: &terms,
            state: execution_state,
            limit: (SESSION_RUNS_PAGE_SIZE + 1) as u32,
        };
        match state
            .storage
            .list_session_execution_summaries(&filter)
            .await
        {
            Ok(mut items) => {
                let has_more = items.len() > SESSION_RUNS_PAGE_SIZE;
                items.truncate(SESSION_RUNS_PAGE_SIZE);
                let next_cursor = items.last().filter(|_| has_more).map(|row| row.id);
                return Ok(Json(SessionRunsResponse::Page(SessionRunsPage {
                    session_id: id,
                    tenant_id: session.tenant_id,
                    cursor,
                    search: query.search,
                    state: execution_state,
                    next_cursor,
                    items,
                })));
            }
            Err(StorageError::Unsupported(_)) => {}
            Err(error) => return Err(ApiError::from_storage(error, "associated executions")),
        }
    }
    let instances = state
        .storage
        .list_session_instances(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "session"))?;
    Ok(Json(SessionRunsResponse::Full(instances)))
}
