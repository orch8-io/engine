//! Event-driven triggers: inbound webhooks that create instances on POST.
//!
//! Each trigger maps a URL path + sequence name. When an HTTP POST hits the
//! trigger endpoint, a new instance is created with the request body as
//! initial context data.
//!
//! Trigger management: CRUD via `/triggers` endpoints.
//! Trigger invocation: `POST /triggers/{slug}/fire`

use axum::extract::{Json, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use utoipa::ToSchema;

use orch8_types::ids::TenantId;
use orch8_types::trigger::{TriggerDef, TriggerType};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/triggers", get(list_triggers).post(create_trigger))
        .route("/triggers/{slug}", get(get_trigger).delete(delete_trigger))
        .route("/triggers/{slug}/fire", post(fire_trigger))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTriggerRequest {
    pub slug: String,
    pub sequence_name: String,
    #[serde(default)]
    pub version: Option<i32>,
    pub tenant_id: String,
    #[serde(default = "orch8_types::serde_defaults::default_namespace")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(default)]
    pub trigger_type: TriggerType,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct TriggerQuery {
    #[serde(default)]
    pub tenant_id: Option<String>,
}

async fn create_trigger(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateTriggerRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if body.slug.is_empty() || body.sequence_name.is_empty() {
        return Err(ApiError::InvalidArgument(
            "slug and sequence_name are required".into(),
        ));
    }
    if body.slug.len() > 255 {
        return Err(ApiError::InvalidArgument(
            "slug must not exceed 255 characters".into(),
        ));
    }
    if body.sequence_name.len() > 255 {
        return Err(ApiError::InvalidArgument(
            "sequence_name must not exceed 255 characters".into(),
        ));
    }

    let tenant_id =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId(body.tenant_id.clone()))?;

    let now = chrono::Utc::now();
    let trigger = TriggerDef {
        slug: body.slug,
        sequence_name: body.sequence_name,
        version: body.version,
        tenant_id: tenant_id.0,
        namespace: body.namespace,
        enabled: true,
        secret: body.secret.map(orch8_types::config::SecretString::new),
        trigger_type: body.trigger_type,
        config: body.config,
        created_at: now,
        updated_at: now,
    };

    state
        .storage
        .create_trigger(&trigger)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?;

    Ok((StatusCode::CREATED, Json(trigger)))
}

async fn list_triggers(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<TriggerQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_ref = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref());
    let triggers = state
        .storage
        .list_triggers(tenant_ref.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?;
    Ok(Json(triggers))
}

async fn get_trigger(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(slug): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let trigger = state
        .storage
        .get_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?
        .ok_or_else(|| ApiError::NotFound(format!("trigger '{slug}'")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId(trigger.tenant_id.clone()),
        &format!("trigger '{slug}'"),
    )?;
    Ok(Json(trigger))
}

async fn delete_trigger(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(slug): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify it exists first.
    let trigger = state
        .storage
        .get_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?
        .ok_or_else(|| ApiError::NotFound(format!("trigger '{slug}'")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId(trigger.tenant_id.clone()),
        &format!("trigger '{slug}'"),
    )?;

    state
        .storage
        .delete_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Fire a trigger: create a new instance from the trigger's sequence.
///
/// The request body becomes the initial `context.data` for the instance.
async fn fire_trigger(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(slug): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let trigger = state
        .storage
        .get_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?
        .ok_or_else(|| ApiError::NotFound(format!("trigger '{slug}'")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId(trigger.tenant_id.clone()),
        &format!("trigger '{slug}'"),
    )?;

    if !trigger.enabled {
        return Err(ApiError::InvalidArgument(format!(
            "trigger '{slug}' is disabled"
        )));
    }

    // Validate trigger secret if configured.
    if let Some(ref secret) = trigger.secret {
        let provided = headers
            .get("x-trigger-secret")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let expected = secret.expose();
        if provided.len() != expected.len()
            || !bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
        {
            return Err(ApiError::Unauthorized);
        }
    }

    let meta = serde_json::json!({ "source": "http_fire" });
    let instance_id = orch8_engine::triggers::create_trigger_instance(
        &*state.storage,
        &trigger,
        body,
        meta,
        None,
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "instance_id": instance_id,
            "trigger": slug,
            "sequence_name": trigger.sequence_name,
        })),
    ))
}
