//! Error budget / auto-rollback policy endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::auth::{enforce_tenant_access, enforce_tenant_create, scoped_tenant_id, OptionalTenant};
use crate::error::ApiError;
use crate::security::validate_public_url;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/rollback-policies", post(create_policy))
        .route("/rollback-policies", get(list_policies))
        .route("/rollback-policies/{name}", get(get_policy))
        .route("/rollback-policies/{name}", delete(delete_policy))
}

// ── Requests / Responses ──

#[derive(Debug, Deserialize)]
pub struct CreatePolicyRequest {
    pub tenant_id: Option<String>,
    pub sequence_name: String,
    pub error_rate_threshold: f64,
    pub time_window_secs: i32,
    pub cooldown_secs: Option<i32>,
    pub confirmation_window_secs: Option<i32>,
    pub webhook_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PolicyResponse {
    pub id: i64,
    pub tenant_id: String,
    pub sequence_name: String,
    pub error_rate_threshold: f64,
    pub time_window_secs: i32,
    pub enabled: bool,
    pub cooldown_secs: i32,
    pub confirmation_window_secs: i32,
    pub webhook_url: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<orch8_types::rollback::RollbackPolicy> for PolicyResponse {
    fn from(p: orch8_types::rollback::RollbackPolicy) -> Self {
        Self {
            id: p.id,
            tenant_id: p.tenant_id,
            sequence_name: p.sequence_name,
            error_rate_threshold: p.error_rate_threshold,
            time_window_secs: p.time_window_secs,
            enabled: p.enabled,
            cooldown_secs: p.cooldown_secs,
            confirmation_window_secs: p.confirmation_window_secs,
            webhook_url: p.webhook_url,
            created_at: p.created_at.to_rfc3339(),
            updated_at: p.updated_at.to_rfc3339(),
        }
    }
}

// ── Handlers ──

pub(crate) async fn create_policy(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Json(req): Json<CreatePolicyRequest>,
) -> Result<(StatusCode, Json<PolicyResponse>), ApiError> {
    if req.sequence_name.is_empty() {
        return Err(ApiError::InvalidArgument(
            "sequence_name is required".into(),
        ));
    }
    if req.error_rate_threshold < 0.0 || req.error_rate_threshold > 1.0 {
        return Err(ApiError::InvalidArgument(
            "error_rate_threshold must be in [0.0, 1.0]".into(),
        ));
    }
    if req.time_window_secs <= 0 {
        return Err(ApiError::InvalidArgument(
            "time_window_secs must be positive".into(),
        ));
    }

    if let Some(ref url) = req.webhook_url {
        validate_public_url(url).map_err(|e| ApiError::InvalidArgument(e.to_string()))?;
    }

    let tenant_id = enforce_tenant_create(
        &tenant_ctx,
        &orch8_types::ids::TenantId::unchecked(req.tenant_id.as_deref().unwrap_or("")),
    )?;
    let tenant = tenant_id.as_str().to_string();

    state
        .storage
        .create_rollback_policy(
            &tenant,
            &req.sequence_name,
            req.error_rate_threshold,
            req.time_window_secs,
            req.cooldown_secs,
            req.confirmation_window_secs,
            req.webhook_url.as_deref(),
        )
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;

    let policy = state
        .storage
        .get_rollback_policy(&tenant, &req.sequence_name)
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?
        .ok_or_else(|| ApiError::Internal("Policy not found after creation".into()))?;

    Ok((StatusCode::CREATED, Json(policy.into())))
}

pub(crate) async fn get_policy(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    axum::extract::Path(sequence_name): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<PolicyResponse>, ApiError> {
    let tenant = scoped_tenant_id(&tenant_ctx, params.get("tenant_id").map(String::as_str))
        .map_or_else(|| "default".to_string(), |t| t.as_str().to_string());

    let policy = state
        .storage
        .get_rollback_policy(&tenant, &sequence_name)
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?
        .ok_or_else(|| ApiError::NotFound(format!("rollback_policy {sequence_name}")))?;

    enforce_tenant_access(
        &tenant_ctx,
        &orch8_types::ids::TenantId::unchecked(policy.tenant_id.clone()),
        "rollback_policy",
    )?;

    Ok(Json(policy.into()))
}

pub(crate) async fn list_policies(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<PolicyResponse>>, ApiError> {
    let tenant = scoped_tenant_id(&tenant_ctx, params.get("tenant_id").map(String::as_str));
    let tenant = tenant.as_ref().map(orch8_types::ids::TenantId::as_str);

    let policies = state
        .storage
        .list_rollback_policies(tenant, 100)
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;

    Ok(Json(
        policies.into_iter().map(PolicyResponse::from).collect(),
    ))
}

pub(crate) async fn delete_policy(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    axum::extract::Path(sequence_name): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode, ApiError> {
    let tenant = scoped_tenant_id(&tenant_ctx, params.get("tenant_id").map(String::as_str))
        .map_or_else(|| "default".to_string(), |t| t.as_str().to_string());

    // Verify the caller may touch this tenant before mutating.
    enforce_tenant_access(
        &tenant_ctx,
        &orch8_types::ids::TenantId::unchecked(tenant.clone()),
        "rollback_policy",
    )?;

    state
        .storage
        .delete_rollback_policy(&tenant, &sequence_name)
        .await
        .map_err(|e| ApiError::Internal(format!("DB error: {e}")))?;

    Ok(StatusCode::NO_CONTENT)
}
