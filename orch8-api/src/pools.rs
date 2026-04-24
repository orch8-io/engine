use axum::extract::{Path, State};
use axum::routing::{get, put};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::ids::{ResourceKey, TenantId};
use orch8_types::pool::{PoolResource, ResourcePool, RotationStrategy};

use crate::error::ApiError;
use crate::AppState;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/pools", get(list_pools).post(create_pool))
        .route("/pools/{id}", get(get_pool).delete(delete_pool))
        .route(
            "/pools/{pool_id}/resources",
            get(list_resources).post(add_resource),
        )
        .route(
            "/pools/{pool_id}/resources/{resource_id}",
            put(update_resource).delete(delete_resource),
        )
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreatePoolRequest {
    pub tenant_id: String,
    pub name: String,
    #[serde(default = "default_strategy")]
    pub strategy: RotationStrategy,
}

const fn default_strategy() -> RotationStrategy {
    RotationStrategy::RoundRobin
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct AddResourceRequest {
    pub resource_key: String,
    pub name: String,
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default)]
    pub daily_cap: u32,
    #[serde(default)]
    pub warmup_start: Option<String>,
    #[serde(default)]
    pub warmup_days: u32,
    #[serde(default)]
    pub warmup_start_cap: u32,
}

const fn default_weight() -> u32 {
    1
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateResourceRequest {
    pub name: Option<String>,
    pub weight: Option<u32>,
    pub enabled: Option<bool>,
    pub daily_cap: Option<u32>,
    pub warmup_start: Option<String>,
    pub warmup_days: Option<u32>,
    pub warmup_start_cap: Option<u32>,
}

#[utoipa::path(
    post, path = "/pools",
    request_body = CreatePoolRequest,
    responses((status = 201, body = ResourcePool)),
    tag = "pools"
)]
pub(crate) async fn create_pool(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<CreatePoolRequest>,
) -> Result<(axum::http::StatusCode, Json<ResourcePool>), ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId(req.tenant_id))?;
    let now = Utc::now();
    let pool = ResourcePool {
        id: Uuid::now_v7(),
        tenant_id,
        name: req.name,
        strategy: req.strategy,
        round_robin_index: 0,
        created_at: now,
        updated_at: now,
    };
    state.storage.create_resource_pool(&pool).await?;
    Ok((axum::http::StatusCode::CREATED, Json(pool)))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListPoolsQuery {
    tenant_id: Option<String>,
}

#[utoipa::path(
    get, path = "/pools",
    params(("tenant_id" = String, Query, description = "Tenant ID")),
    responses((status = 200, body = Vec<ResourcePool>)),
    tag = "pools"
)]
pub(crate) async fn list_pools(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    axum::extract::Query(query): axum::extract::Query<ListPoolsQuery>,
) -> Result<Json<Vec<ResourcePool>>, ApiError> {
    let tenant_id = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref())
        .unwrap_or_else(|| TenantId(String::new()));
    let pools = state.storage.list_resource_pools(&tenant_id).await?;
    Ok(Json(pools))
}

#[utoipa::path(
    get, path = "/pools/{id}",
    responses((status = 200, body = ResourcePool)),
    tag = "pools"
)]
pub(crate) async fn get_pool(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<Json<ResourcePool>, ApiError> {
    let pool = state
        .storage
        .get_resource_pool(id)
        .await?
        .ok_or_else(|| ApiError::NotFound("pool not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &pool.tenant_id, &format!("pool {id}"))?;
    Ok(Json(pool))
}

#[utoipa::path(
    delete, path = "/pools/{id}",
    responses((status = 204)),
    tag = "pools"
)]
pub(crate) async fn delete_pool(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<axum::http::StatusCode, ApiError> {
    let pool = state
        .storage
        .get_resource_pool(id)
        .await?
        .ok_or_else(|| ApiError::NotFound("pool not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &pool.tenant_id, &format!("pool {id}"))?;
    state.storage.delete_resource_pool(id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get, path = "/pools/{pool_id}/resources",
    responses((status = 200, body = Vec<PoolResource>)),
    tag = "pools"
)]
pub(crate) async fn list_resources(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(pool_id): Path<Uuid>,
) -> Result<Json<Vec<PoolResource>>, ApiError> {
    let pool = state
        .storage
        .get_resource_pool(pool_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("pool not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &pool.tenant_id, &format!("pool {pool_id}"))?;
    let resources = state.storage.list_pool_resources(pool_id).await?;
    Ok(Json(resources))
}

#[utoipa::path(
    post, path = "/pools/{pool_id}/resources",
    request_body = AddResourceRequest,
    responses((status = 201, body = PoolResource)),
    tag = "pools"
)]
pub(crate) async fn add_resource(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(pool_id): Path<Uuid>,
    Json(req): Json<AddResourceRequest>,
) -> Result<(axum::http::StatusCode, Json<PoolResource>), ApiError> {
    if req.resource_key.is_empty() || req.resource_key.len() > 255 {
        return Err(ApiError::InvalidArgument(
            "resource_key must be 1-255 characters".into(),
        ));
    }
    if req.name.is_empty() || req.name.len() > 255 {
        return Err(ApiError::InvalidArgument(
            "name must be 1-255 characters".into(),
        ));
    }
    if req.weight == 0 {
        return Err(ApiError::InvalidArgument(
            "weight must be at least 1".into(),
        ));
    }
    let pool = state
        .storage
        .get_resource_pool(pool_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("pool not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &pool.tenant_id, &format!("pool {pool_id}"))?;
    let warmup_start = req
        .warmup_start
        .and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok());

    let resource = PoolResource {
        id: Uuid::now_v7(),
        pool_id,
        resource_key: ResourceKey(req.resource_key),
        name: req.name,
        weight: req.weight,
        enabled: true,
        daily_cap: req.daily_cap,
        daily_usage: 0,
        daily_usage_date: None,
        warmup_start,
        warmup_days: req.warmup_days,
        warmup_start_cap: req.warmup_start_cap,
        created_at: Utc::now(),
    };
    state.storage.add_pool_resource(&resource).await?;
    Ok((axum::http::StatusCode::CREATED, Json(resource)))
}

#[utoipa::path(
    put, path = "/pools/{pool_id}/resources/{resource_id}",
    request_body = UpdateResourceRequest,
    responses((status = 200, body = PoolResource)),
    tag = "pools"
)]
pub(crate) async fn update_resource(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((pool_id, resource_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateResourceRequest>,
) -> Result<Json<PoolResource>, ApiError> {
    let pool = state
        .storage
        .get_resource_pool(pool_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("pool not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &pool.tenant_id, &format!("pool {pool_id}"))?;
    let resources = state.storage.list_pool_resources(pool_id).await?;
    let mut resource = resources
        .into_iter()
        .find(|r| r.id == resource_id)
        .ok_or_else(|| ApiError::NotFound("resource not found".into()))?;

    if let Some(name) = req.name {
        resource.name = name;
    }
    if let Some(weight) = req.weight {
        resource.weight = weight;
    }
    if let Some(enabled) = req.enabled {
        resource.enabled = enabled;
    }
    if let Some(daily_cap) = req.daily_cap {
        resource.daily_cap = daily_cap;
    }
    if let Some(warmup_start) = req.warmup_start {
        resource.warmup_start = chrono::NaiveDate::parse_from_str(&warmup_start, "%Y-%m-%d").ok();
    }
    if let Some(warmup_days) = req.warmup_days {
        resource.warmup_days = warmup_days;
    }
    if let Some(warmup_start_cap) = req.warmup_start_cap {
        resource.warmup_start_cap = warmup_start_cap;
    }

    state.storage.update_pool_resource(&resource).await?;
    Ok(Json(resource))
}

#[utoipa::path(
    delete, path = "/pools/{pool_id}/resources/{resource_id}",
    responses((status = 204)),
    tag = "pools"
)]
pub(crate) async fn delete_resource(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((pool_id, resource_id)): Path<(Uuid, Uuid)>,
) -> Result<axum::http::StatusCode, ApiError> {
    let pool = state
        .storage
        .get_resource_pool(pool_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("pool not found".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &pool.tenant_id, &format!("pool {pool_id}"))?;
    state.storage.delete_pool_resource(resource_id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}
