//! Event-driven triggers: inbound webhooks that create instances on POST.
//!
//! Each trigger maps a URL path + sequence name. When an HTTP POST hits the
//! trigger endpoint, a new instance is created with the request body as
//! initial context data.
//!
//! Trigger management: CRUD via `/triggers` endpoints.
//! Trigger invocation: `POST /triggers/{slug}/fire`

use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
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
    Json(body): Json<CreateTriggerRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if body.slug.is_empty() || body.sequence_name.is_empty() {
        return Err(ApiError::InvalidArgument(
            "slug and sequence_name are required".into(),
        ));
    }

    let now = chrono::Utc::now();
    let trigger = TriggerDef {
        slug: body.slug,
        sequence_name: body.sequence_name,
        version: body.version,
        tenant_id: body.tenant_id,
        namespace: body.namespace,
        enabled: true,
        secret: body.secret,
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
    Query(query): Query<TriggerQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_ref = query.tenant_id.as_ref().map(|t| TenantId(t.clone()));
    let triggers = state
        .storage
        .list_triggers(tenant_ref.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?;
    Ok(Json(triggers))
}

async fn get_trigger(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let trigger = state
        .storage
        .get_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?
        .ok_or_else(|| ApiError::NotFound(format!("trigger '{slug}'")))?;
    Ok(Json(trigger))
}

async fn delete_trigger(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify it exists first.
    state
        .storage
        .get_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?
        .ok_or_else(|| ApiError::NotFound(format!("trigger '{slug}'")))?;

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
    Path(slug): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let trigger = state
        .storage
        .get_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?
        .ok_or_else(|| ApiError::NotFound(format!("trigger '{slug}'")))?;

    if !trigger.enabled {
        return Err(ApiError::InvalidArgument(format!(
            "trigger '{slug}' is disabled"
        )));
    }

    let meta = serde_json::json!({ "source": "http_fire" });
    let instance_id = orch8_engine::triggers::create_trigger_instance(
        &*state.storage,
        &trigger,
        body,
        meta,
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
