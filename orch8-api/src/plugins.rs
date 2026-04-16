//! Plugin registry: CRUD management for WASM and gRPC plugins.
//!
//! Plugins map handler names to external implementations.
//! A step with `handler: "wasm://my-plugin"` resolves via the plugin
//! registry to find the `.wasm` module path. Similarly, `grpc://` plugins
//! resolve to a host:port/Service.Method endpoint.

use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::ids::TenantId;
use orch8_types::plugin::{PluginDef, PluginType};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/plugins", get(list_plugins).post(create_plugin))
        .route(
            "/plugins/{name}",
            get(get_plugin).delete(delete_plugin).patch(update_plugin),
        )
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreatePluginRequest {
    pub name: String,
    pub plugin_type: PluginType,
    pub source: String,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePluginRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PluginQuery {
    #[serde(default)]
    pub tenant_id: Option<String>,
}

async fn create_plugin(
    State(state): State<AppState>,
    Json(body): Json<CreatePluginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if body.name.is_empty() || body.source.is_empty() {
        return Err(ApiError::InvalidArgument(
            "name and source are required".into(),
        ));
    }

    let now = chrono::Utc::now();
    let plugin = PluginDef {
        name: body.name,
        plugin_type: body.plugin_type,
        source: body.source,
        tenant_id: body.tenant_id,
        enabled: true,
        config: body.config,
        description: body.description,
        created_at: now,
        updated_at: now,
    };

    state
        .storage
        .create_plugin(&plugin)
        .await
        .map_err(|e| ApiError::from_storage(e, "plugin"))?;

    Ok((StatusCode::CREATED, Json(plugin)))
}

async fn list_plugins(
    State(state): State<AppState>,
    Query(query): Query<PluginQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_ref = query.tenant_id.as_ref().map(|t| TenantId(t.clone()));
    let plugins = state
        .storage
        .list_plugins(tenant_ref.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "plugin"))?;
    Ok(Json(plugins))
}

async fn get_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let plugin = state
        .storage
        .get_plugin(&name)
        .await
        .map_err(|e| ApiError::from_storage(e, "plugin"))?
        .ok_or_else(|| ApiError::NotFound(format!("plugin '{name}'")))?;
    Ok(Json(plugin))
}

async fn update_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<UpdatePluginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut plugin = state
        .storage
        .get_plugin(&name)
        .await
        .map_err(|e| ApiError::from_storage(e, "plugin"))?
        .ok_or_else(|| ApiError::NotFound(format!("plugin '{name}'")))?;

    if let Some(source) = body.source {
        plugin.source = source;
    }
    if let Some(enabled) = body.enabled {
        plugin.enabled = enabled;
    }
    if let Some(config) = body.config {
        plugin.config = config;
    }
    if let Some(description) = body.description {
        plugin.description = Some(description);
    }
    plugin.updated_at = chrono::Utc::now();

    state
        .storage
        .update_plugin(&plugin)
        .await
        .map_err(|e| ApiError::from_storage(e, "plugin"))?;

    Ok(Json(plugin))
}

async fn delete_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .storage
        .get_plugin(&name)
        .await
        .map_err(|e| ApiError::from_storage(e, "plugin"))?
        .ok_or_else(|| ApiError::NotFound(format!("plugin '{name}'")))?;

    state
        .storage
        .delete_plugin(&name)
        .await
        .map_err(|e| ApiError::from_storage(e, "plugin"))?;

    Ok(StatusCode::NO_CONTENT)
}
