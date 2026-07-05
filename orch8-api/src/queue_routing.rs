//! Dynamic task-queue routing rule management.
//!
//! A rule overrides the queue an external-worker task lands on, keyed by
//! `(tenant_id, handler_name)` with an optional `match_queue`. Evaluated at
//! enqueue (highest `priority` wins). Tenant-scoped CRUD.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::ids::TenantId;
use orch8_types::queue_routing::QueueRoutingRule;

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/routing-rules", post(create_rule).get(list_rules))
        .route("/routing-rules/{id}", get(get_rule).delete(delete_rule))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateRoutingRuleRequest {
    tenant_id: String,
    handler_name: String,
    #[serde(default)]
    match_queue: Option<String>,
    queue_override: String,
    #[serde(default)]
    priority: i32,
    #[serde(default = "default_true")]
    enabled: bool,
}

const fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
pub(crate) struct ListRoutingQuery {
    tenant_id: Option<String>,
    handler_name: Option<String>,
}

#[utoipa::path(post, path = "/routing-rules", tag = "routing",
    request_body = CreateRoutingRuleRequest,
    responses(
        (status = 201, description = "Rule created", body = QueueRoutingRule),
        (status = 400, description = "Missing/empty fields"),
    )
)]
pub(crate) async fn create_rule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<CreateRoutingRuleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(&req.tenant_id))?;
    if req.handler_name.trim().is_empty() || req.queue_override.trim().is_empty() {
        return Err(ApiError::InvalidArgument(
            "handler_name and queue_override are required".into(),
        ));
    }
    let now = Utc::now();
    let rule = QueueRoutingRule {
        id: Uuid::now_v7(),
        tenant_id: tenant_id.as_str().to_string(),
        handler_name: req.handler_name,
        match_queue: req.match_queue,
        queue_override: req.queue_override,
        priority: req.priority,
        enabled: req.enabled,
        created_at: now,
        updated_at: now,
    };
    state
        .storage
        .create_queue_routing_rule(&rule)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_routing_rule"))?;
    Ok((StatusCode::CREATED, Json(rule)))
}

#[utoipa::path(get, path = "/routing-rules", tag = "routing",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("handler_name" = Option<String>, Query, description = "Filter by handler"),
    ),
    responses((status = 200, description = "Routing rules", body = Vec<QueueRoutingRule>))
)]
pub(crate) async fn list_rules(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListRoutingQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let rules = state
        .storage
        .list_queue_routing_rules(scoped.as_ref(), q.handler_name.as_deref())
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_routing_rule"))?;
    Ok(Json(rules))
}

#[utoipa::path(get, path = "/routing-rules/{id}", tag = "routing",
    params(("id" = Uuid, Path, description = "Rule id")),
    responses(
        (status = 200, description = "Rule", body = QueueRoutingRule),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn get_rule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let rule = state
        .storage
        .get_queue_routing_rule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_routing_rule"))?
        .ok_or_else(|| ApiError::NotFound(format!("queue_routing_rule {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId::unchecked(&rule.tenant_id),
        &format!("queue_routing_rule {id}"),
    )?;
    Ok(Json(rule))
}

#[utoipa::path(delete, path = "/routing-rules/{id}", tag = "routing",
    params(("id" = Uuid, Path, description = "Rule id")),
    responses((status = 204, description = "Deleted"), (status = 404, description = "Not found"))
)]
pub(crate) async fn delete_rule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let rule = state
        .storage
        .get_queue_routing_rule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_routing_rule"))?
        .ok_or_else(|| ApiError::NotFound(format!("queue_routing_rule {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId::unchecked(&rule.tenant_id),
        &format!("queue_routing_rule {id}"),
    )?;
    state
        .storage
        .delete_queue_routing_rule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "queue_routing_rule"))?;
    Ok(StatusCode::NO_CONTENT)
}
