//! Per-queue dispatch mode management (poll vs push).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use utoipa::ToSchema;

use orch8_types::ids::TenantId;
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};

use crate::auth::{enforce_tenant_access, enforce_tenant_create, OptionalTenant};
use crate::error::ApiError;
use crate::security::validate_public_url;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/queues/dispatch", post(set_dispatch).get(list_dispatch))
        .route(
            "/queues/dispatch/{tenant_id}/{queue_name}",
            axum::routing::delete(delete_dispatch),
        )
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct SetDispatchRequest {
    tenant_id: String,
    queue_name: String,
    mode: DispatchMode,
    #[serde(default)]
    push_url: Option<String>,
    /// Optional HMAC secret for signing pushed envelopes.
    #[serde(default)]
    secret: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct ListDispatchQuery {
    tenant_id: Option<String>,
}

#[utoipa::path(post, path = "/queues/dispatch", tag = "routing",
    request_body = SetDispatchRequest,
    responses(
        (status = 200, description = "Dispatch config set", body = QueueDispatchConfig),
        (status = 400, description = "push mode requires push_url"),
    )
)]
pub(crate) async fn set_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(req): Json<SetDispatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(req.tenant_id))?;

    if req.queue_name.trim().is_empty() {
        return Err(ApiError::InvalidArgument("queue_name is required".into()));
    }
    if req.mode == DispatchMode::Push {
        let url = req
            .push_url
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| ApiError::InvalidArgument("push mode requires a push_url".into()))?;
        validate_public_url(url).map_err(|e| ApiError::InvalidArgument(e.to_string()))?;
    }

    let now = Utc::now();
    let cfg = QueueDispatchConfig {
        tenant_id: tenant_id.as_str().to_string(),
        queue_name: req.queue_name,
        mode: req.mode,
        push_url: req.push_url,
        secret: req.secret,
        created_at: now,
        updated_at: now,
    };
    state
        .storage
        .upsert_queue_dispatch(&cfg)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_dispatch"))?;
    // Never echo the secret back.
    let mut out = cfg;
    out.secret = None;
    Ok((StatusCode::OK, Json(out)))
}

#[utoipa::path(get, path = "/queues/dispatch", tag = "routing",
    params(("tenant_id" = Option<String>, Query, description = "Filter by tenant")),
    responses((status = 200, description = "Dispatch configs", body = Vec<QueueDispatchConfig>))
)]
pub(crate) async fn list_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(q): Query<ListDispatchQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref())
        .ok_or_else(|| ApiError::InvalidArgument("tenant_id is required".into()))?;
    let mut configs = state
        .storage
        .list_queue_dispatch(Some(scoped.as_str()))
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_dispatch"))?;
    // Secrets are write-only: never return them in list responses.
    for cfg in &mut configs {
        cfg.secret = None;
    }
    Ok(Json(configs))
}

#[utoipa::path(delete, path = "/queues/dispatch/{tenant_id}/{queue_name}", tag = "routing",
    params(
        ("tenant_id" = String, Path, description = "Tenant id"),
        ("queue_name" = String, Path, description = "Queue name"),
    ),
    responses((status = 204, description = "Deleted"))
)]
pub(crate) async fn delete_dispatch(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path((tenant_id, queue_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    enforce_tenant_access(
        &tenant_ctx,
        &TenantId::unchecked(tenant_id.clone()),
        "queue_dispatch",
    )?;
    state
        .storage
        .delete_queue_dispatch(&tenant_id, &queue_name)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_dispatch"))?;
    Ok(StatusCode::NO_CONTENT)
}
