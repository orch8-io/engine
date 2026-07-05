//! Event-driven triggers: inbound webhooks that create instances on POST.
//!
//! Each trigger maps a URL path + sequence name. When an HTTP POST hits the
//! trigger endpoint, a new instance is created with the request body as
//! initial context data.
//!
//! Trigger management: CRUD via `/triggers` endpoints.
//! Trigger invocation: `POST /triggers/{slug}/fire`
//!
//! Polling triggers (`trigger_type: "activepieces_poll"`) are fired by the
//! engine's poll loop instead of inbound HTTP. Their `config` is validated
//! at creation (see [`orch8_engine::ap_poll::parse_config`]) and
//! `GET /triggers/{slug}` additionally returns a `poll_state` field with the
//! cursor, `last_error`, and `consecutive_failures` bookkeeping.

use axum::Router;
use axum::extract::{Json, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::ids::{Namespace, TenantId};
use orch8_types::trigger::{TriggerDef, TriggerType};

use crate::AppState;
use crate::error::ApiError;

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
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    100
}

#[utoipa::path(post, path = "/triggers", tag = "triggers",
    request_body = CreateTriggerRequest,
    responses(
        (status = 201, description = "Trigger created", body = orch8_types::trigger::TriggerDef),
        (status = 400, description = "Invalid input"),
    )
)]
pub(crate) async fn create_trigger(
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
    if body.secret.as_ref().is_some_and(String::is_empty) {
        return Err(ApiError::InvalidArgument(
            "trigger secret cannot be empty; omit the field to leave the trigger unauthenticated"
                .into(),
        ));
    }
    // Polling triggers carry a structured config (piece, trigger, schedule);
    // reject malformed configs here so the engine's poll loop never sees one.
    if body.trigger_type == TriggerType::ActivepiecesPoll {
        orch8_engine::ap_poll::parse_config(&body.config).map_err(|e| {
            ApiError::InvalidArgument(format!("invalid activepieces_poll config: {e}"))
        })?;
    }

    let tenant_id = crate::auth::enforce_tenant_create(
        &tenant_ctx,
        &TenantId::unchecked(body.tenant_id.clone()),
    )?;

    // Verify the target sequence exists and belongs to the caller's tenant.
    let namespace = Namespace::new(body.namespace.clone());
    let sequence = state
        .storage
        .get_sequence_by_name(&tenant_id, &namespace, &body.sequence_name, body.version)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "sequence {} in namespace {}",
                body.sequence_name, body.namespace
            ))
        })?;
    if sequence.tenant_id != tenant_id {
        return Err(ApiError::Forbidden(
            "sequence does not belong to the caller's tenant".into(),
        ));
    }

    let now = chrono::Utc::now();
    let trigger = TriggerDef {
        slug: body.slug,
        sequence_name: body.sequence_name,
        version: body.version,
        tenant_id,
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

#[utoipa::path(get, path = "/triggers", tag = "triggers",
    params(("tenant_id" = Option<String>, Query, description = "Filter by tenant")),
    responses((status = 200, description = "Triggers", body = Vec<orch8_types::trigger::TriggerDef>))
)]
pub(crate) async fn list_triggers(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<TriggerQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_ref = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref());
    let triggers = state
        .storage
        .list_triggers(tenant_ref.as_ref(), query.limit)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?;
    Ok(Json(triggers))
}

#[utoipa::path(get, path = "/triggers/{slug}", tag = "triggers",
    params(("slug" = String, Path, description = "Trigger slug")),
    responses(
        (status = 200, description = "Trigger. For activepieces_poll triggers the \
            response additionally carries a `poll_state` field (cursor, last_error, \
            consecutive_failures, last_poll_at).",
            body = orch8_types::trigger::TriggerDef),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn get_trigger(
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
        &trigger.tenant_id,
        &format!("trigger '{slug}'"),
    )?;

    // Polling triggers surface their runtime bookkeeping (dedupe cursor,
    // last_error, consecutive_failures) alongside the definition.
    if trigger.trigger_type == TriggerType::ActivepiecesPoll {
        let poll_state = state
            .storage
            .get_trigger_poll_state(&slug)
            .await
            .map_err(|e| ApiError::from_storage(e, "trigger_poll_state"))?;
        let mut body = serde_json::to_value(&trigger)
            .map_err(|e| ApiError::Internal(format!("trigger serialization failed: {e}")))?;
        if let serde_json::Value::Object(map) = &mut body {
            map.insert(
                "poll_state".into(),
                poll_state.map_or(serde_json::Value::Null, |s| {
                    serde_json::to_value(s).unwrap_or(serde_json::Value::Null)
                }),
            );
        }
        return Ok(Json(body).into_response());
    }
    Ok(Json(trigger).into_response())
}

#[utoipa::path(delete, path = "/triggers/{slug}", tag = "triggers",
    params(("slug" = String, Path, description = "Trigger slug")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn delete_trigger(
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
        &trigger.tenant_id,
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
#[utoipa::path(post, path = "/triggers/{slug}/fire", tag = "triggers",
    params(("slug" = String, Path, description = "Trigger slug")),
    request_body = serde_json::Value,
    responses(
        (status = 201, description = "Instance created"),
        (status = 400, description = "Trigger disabled or bad secret"),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn fire_trigger(
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
        &trigger.tenant_id,
        &format!("trigger '{slug}'"),
    )?;

    if !trigger.enabled {
        return Err(ApiError::InvalidArgument(format!(
            "trigger '{slug}' is disabled"
        )));
    }

    // Validate trigger secret if configured. Empty configured secrets are
    // treated as unauthenticated (defence in depth — creation should reject them).
    if let Some(ref secret) = trigger.secret {
        if secret.is_empty() {
            tracing::warn!(slug = %slug, "trigger has an empty secret — rejecting fire request");
            return Err(ApiError::Unauthorized);
        }
        let provided = headers
            .get("x-trigger-secret")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !orch8_types::auth::verify_secret_constant_time(provided, secret.expose()) {
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
