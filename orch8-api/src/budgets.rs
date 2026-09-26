//! Tenant spend budgets admin API (see `docs/BUDGETS.md`).
//!
//! Writes (create/update/delete) require the root/admin API key — a budget
//! is a platform control over a tenant, so a tenant key cannot lift its own
//! cap. Reads (statuses, alerts) are open to the tenant itself.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_engine::tenant_budgets;
use orch8_types::ai::{
    BudgetAlert, BudgetPeriod, BudgetStatus, TenantBudget, default_budget_thresholds,
};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use crate::AppState;
use crate::api_keys::require_admin;
use crate::auth::{OptionalAdmin, OptionalTenant};
use crate::error::ApiError;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/budgets", get(list_budgets).post(create_budget))
        .route("/budgets/alerts", get(list_alerts))
        .route("/budgets/{id}", put(update_budget).delete(delete_budget))
}

/// Create/replace body. `limit_usd` is the per-period estimated spend cap.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct BudgetRequest {
    pub tenant_id: String,
    /// Model prefix filter (e.g. `gpt-5`, `claude-opus`); omit for all models.
    #[serde(default)]
    pub model: Option<String>,
    pub period: BudgetPeriod,
    pub limit_usd: f64,
    /// Soft alert thresholds in percent. Default `[50, 80, 100]`.
    #[serde(default)]
    pub thresholds: Option<Vec<u8>>,
    /// Fail new `llm_call` dispatches closed at 100%. Default `true`.
    #[serde(default)]
    pub hard_cap: Option<bool>,
}

fn map_storage(e: StorageError) -> ApiError {
    match e {
        StorageError::Constraint(m) => ApiError::InvalidArgument(m),
        other => ApiError::from_storage(other, "budget"),
    }
}

fn build(
    req: BudgetRequest,
    tenant: &TenantId,
    id: Uuid,
    created_at: chrono::DateTime<Utc>,
) -> TenantBudget {
    let mut thresholds = req.thresholds.unwrap_or_else(default_budget_thresholds);
    thresholds.sort_unstable();
    thresholds.dedup();
    TenantBudget {
        id,
        tenant_id: tenant.as_str().to_string(),
        model: req.model.filter(|m| !m.trim().is_empty()),
        period: req.period,
        limit_usd: req.limit_usd,
        thresholds,
        hard_cap: req.hard_cap.unwrap_or(true),
        created_at,
        updated_at: Utc::now(),
    }
}

#[utoipa::path(
    post, path = "/budgets", tag = "budgets",
    request_body = BudgetRequest,
    responses(
        (status = 201, body = TenantBudget),
        (status = 400, description = "Invalid budget"),
        (status = 403, description = "Requires the root/admin API key"),
    )
)]
pub(crate) async fn create_budget(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Json(req): Json<BudgetRequest>,
) -> Result<(StatusCode, Json<TenantBudget>), ApiError> {
    require_admin(&admin)?;
    let tenant = crate::auth::enforce_tenant_create(
        &tenant_ctx,
        &TenantId::unchecked(req.tenant_id.clone()),
    )?;
    let budget = build(req, &tenant, Uuid::now_v7(), Utc::now());
    tenant_budgets::save_budget(state.storage.as_ref(), &budget)
        .await
        .map_err(map_storage)?;
    Ok((StatusCode::CREATED, Json(budget)))
}

#[utoipa::path(
    put, path = "/budgets/{id}", tag = "budgets",
    request_body = BudgetRequest,
    params(("id" = Uuid, Path, description = "Budget id")),
    responses(
        (status = 200, body = TenantBudget),
        (status = 403, description = "Requires the root/admin API key"),
        (status = 404),
    )
)]
pub(crate) async fn update_budget(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<BudgetRequest>,
) -> Result<Json<TenantBudget>, ApiError> {
    require_admin(&admin)?;
    let tenant = crate::auth::enforce_tenant_create(
        &tenant_ctx,
        &TenantId::unchecked(req.tenant_id.clone()),
    )?;
    let existing = state
        .storage
        .list_tenant_budgets(tenant.as_str())
        .await?
        .into_iter()
        .find(|b| b.id == id)
        .ok_or_else(|| ApiError::NotFound(format!("budget {id}")))?;
    let budget = build(req, &tenant, id, existing.created_at);
    tenant_budgets::save_budget(state.storage.as_ref(), &budget)
        .await
        .map_err(map_storage)?;
    Ok(Json(budget))
}

#[derive(Debug, Deserialize)]
pub(crate) struct TenantQuery {
    tenant_id: Option<String>,
    limit: Option<u32>,
}

fn read_tenant(ctx: &OptionalTenant, query_tenant: Option<&str>) -> Result<String, ApiError> {
    crate::auth::scoped_tenant_id(ctx, query_tenant)
        .map(|t| t.as_str().to_string())
        .ok_or_else(|| {
            ApiError::InvalidArgument(
                "budget routes require a tenant (X-Tenant-Id header or ?tenant_id=)".into(),
            )
        })
}

#[utoipa::path(
    delete, path = "/budgets/{id}", tag = "budgets",
    params(
        ("id" = Uuid, Path, description = "Budget id"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)"),
    ),
    responses((status = 204), (status = 403), (status = 404))
)]
pub(crate) async fn delete_budget(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Path(id): Path<Uuid>,
    Query(q): Query<TenantQuery>,
) -> Result<StatusCode, ApiError> {
    require_admin(&admin)?;
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    if tenant_budgets::delete_budget(state.storage.as_ref(), &tenant, id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("budget {id}")))
    }
}

#[utoipa::path(
    get, path = "/budgets", tag = "budgets",
    params(("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)")),
    responses((status = 200, description = "Budgets with current-period spend and state", body = Vec<BudgetStatus>))
)]
pub(crate) async fn list_budgets(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(q): Query<TenantQuery>,
) -> Result<Json<Vec<BudgetStatus>>, ApiError> {
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    let statuses =
        tenant_budgets::budget_statuses(state.storage.as_ref(), &tenant, Utc::now()).await?;
    Ok(Json(statuses))
}

#[utoipa::path(
    get, path = "/budgets/alerts", tag = "budgets",
    params(
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers)"),
        ("limit" = Option<u32>, Query, description = "Max alerts (default 100, max 1000)"),
    ),
    responses((status = 200, description = "`budget.threshold_crossed` records, newest first", body = Vec<BudgetAlert>))
)]
pub(crate) async fn list_alerts(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(q): Query<TenantQuery>,
) -> Result<Json<Vec<BudgetAlert>>, ApiError> {
    let tenant = read_tenant(&tenant_ctx, q.tenant_id.as_deref())?;
    let limit = q.limit.unwrap_or(100).clamp(1, 1_000);
    Ok(Json(
        state.storage.list_budget_alerts(&tenant, limit).await?,
    ))
}
