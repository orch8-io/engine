//! Built-in alert rule management (tenant-scoped CRUD).
//!
//! Rules are evaluated by every engine node (`orch8_engine::alerts`);
//! transitions are delivered durably through the webhook outbox to Slack,
//! `PagerDuty` Events API v2, or a signed generic webhook. Destinations hold
//! only `credentials://` references — secrets stay in the credential store.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::alert::{AlertCondition, AlertDestination, AlertRule, AlertRuleState};
use orch8_types::ids::TenantId;

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/alerts/rules", get(list_rules).post(create_rule))
        .route(
            "/alerts/rules/{id}",
            get(get_rule).put(update_rule).delete(delete_rule),
        )
}

const fn default_true() -> bool {
    true
}

const fn default_cooldown() -> u64 {
    orch8_types::alert::DEFAULT_COOLDOWN_SECS
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AlertRuleRequest {
    /// Tenant (defaults to the `X-Tenant-Id` header).
    #[serde(default)]
    pub tenant_id: String,
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub condition: AlertCondition,
    pub destination: AlertDestination,
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AlertRuleView {
    #[serde(flatten)]
    pub rule: AlertRule,
    /// Evaluator state (absent until first evaluation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<AlertRuleState>,
}

#[derive(Deserialize)]
pub(crate) struct ListQuery {
    tenant_id: Option<String>,
}

#[utoipa::path(post, path = "/alerts/rules", tag = "alerts",
    request_body = AlertRuleRequest,
    responses(
        (status = 201, description = "Rule created", body = AlertRule),
        (status = 400, description = "Invalid rule (e.g. literal secret instead of credentials:// reference)"),
    )
)]
pub(crate) async fn create_rule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<AlertRuleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(&req.tenant_id))?;
    let now = Utc::now();
    let rule = AlertRule {
        id: Uuid::now_v7(),
        tenant_id,
        name: req.name,
        enabled: req.enabled,
        condition: req.condition,
        destination: req.destination,
        cooldown_secs: req.cooldown_secs,
        created_at: now,
        updated_at: now,
    };
    rule.validate().map_err(ApiError::InvalidArgument)?;
    state
        .storage
        .create_alert_rule(&rule)
        .await
        .map_err(|e| ApiError::from_storage(e, "alert rule"))?;
    Ok((StatusCode::CREATED, Json(rule)))
}

#[utoipa::path(get, path = "/alerts/rules", tag = "alerts",
    params(("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers only)")),
    responses((status = 200, description = "Alert rules", body = [AlertRule]))
)]
pub(crate) async fn list_rules(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let rules = state
        .storage
        .list_alert_rules(tenant.as_ref(), 1000)
        .await
        .map_err(|e| ApiError::from_storage(e, "alert rule"))?;
    Ok(Json(rules))
}

async fn load(
    state: &AppState,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<AlertRule, ApiError> {
    let scoped = tenant_ctx
        .as_ref()
        .map(|axum::Extension(c)| c.tenant_id.clone());
    state
        .storage
        .get_alert_rule(scoped.as_ref(), id)
        .await
        .map_err(|e| ApiError::from_storage(e, "alert rule"))?
        .ok_or_else(|| ApiError::NotFound(format!("alert rule {id}")))
}

#[utoipa::path(get, path = "/alerts/rules/{id}", tag = "alerts",
    params(("id" = Uuid, Path, description = "Rule ID")),
    responses(
        (status = 200, description = "Rule with evaluator state", body = AlertRuleView),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn get_rule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let rule = load(&state, &tenant_ctx, id).await?;
    let st = state
        .storage
        .get_alert_rule_state(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "alert rule state"))?;
    Ok(Json(AlertRuleView { rule, state: st }))
}

#[utoipa::path(put, path = "/alerts/rules/{id}", tag = "alerts",
    params(("id" = Uuid, Path, description = "Rule ID")),
    request_body = AlertRuleRequest,
    responses(
        (status = 200, description = "Rule replaced", body = AlertRule),
        (status = 400, description = "Invalid rule"),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn update_rule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<AlertRuleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let existing = load(&state, &tenant_ctx, id).await?;
    if !req.tenant_id.is_empty() && req.tenant_id != existing.tenant_id.as_str() {
        return Err(ApiError::InvalidArgument(
            "tenant_id cannot be changed".into(),
        ));
    }
    let rule = AlertRule {
        name: req.name,
        enabled: req.enabled,
        condition: req.condition,
        destination: req.destination,
        cooldown_secs: req.cooldown_secs,
        updated_at: Utc::now(),
        ..existing
    };
    rule.validate().map_err(ApiError::InvalidArgument)?;
    let updated = state
        .storage
        .update_alert_rule(&rule)
        .await
        .map_err(|e| ApiError::from_storage(e, "alert rule"))?;
    if !updated {
        return Err(ApiError::NotFound(format!("alert rule {id}")));
    }
    Ok(Json(rule))
}

#[utoipa::path(delete, path = "/alerts/rules/{id}", tag = "alerts",
    params(("id" = Uuid, Path, description = "Rule ID")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn delete_rule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let rule = load(&state, &tenant_ctx, id).await?;
    let deleted = state
        .storage
        .delete_alert_rule(&rule.tenant_id, id)
        .await
        .map_err(|e| ApiError::from_storage(e, "alert rule"))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("alert rule {id}")))
    }
}
