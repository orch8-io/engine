//! Public (unauthenticated) webhook ingestion.
//!
//! `POST /webhooks/{slug}` is the inbound endpoint third parties (GitHub,
//! Stripe, Shopify, ...) POST events to. It deliberately bypasses the tenant
//! header + API key middlewares because the caller is external and won't
//! have credentials — authentication is enforced instead by the trigger's own
//! HMAC secret (`x-trigger-secret` header), validated with constant-time
//! comparison.
//!
//! The server wires this router in *after* the auth middleware layers so its
//! routes are not subject to them. Do not merge this module through
//! [`crate::build_router`] — that would re-introduce the middleware.
//!
//! Only triggers with `trigger_type = "webhook"` are accepted. `event` and
//! `nats` types are rejected with 404 (they're never meant for public entry).

use axum::extract::{Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use subtle::ConstantTimeEq;

use orch8_types::trigger::TriggerType;

use crate::error::ApiError;
use crate::AppState;

/// Public router — merged into the server *after* auth middleware so these
/// routes are reachable without a tenant header or API key.
pub fn public_routes() -> Router<AppState> {
    Router::new().route("/webhooks/{slug}", post(public_webhook))
}

/// Accept an inbound webhook POST and create a new instance from its body.
///
/// Rejects with 404 if:
/// - The slug doesn't exist.
/// - The trigger is disabled.
/// - The trigger is not of type `webhook` (events and nats are internal only).
///
/// Rejects with 401 if:
/// - The trigger has a `secret` configured but no `x-trigger-secret` header
///   was provided, or the provided secret doesn't match.
async fn public_webhook(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let trigger = state
        .storage
        .get_trigger(&slug)
        .await
        .map_err(|e| ApiError::from_storage(e, "trigger"))?
        .ok_or_else(|| ApiError::NotFound(format!("webhook '{slug}'")))?;

    // Only publicly-typed triggers can be reached through this route.
    if !matches!(trigger.trigger_type, TriggerType::Webhook) {
        // 404 (not 403) — we don't confirm existence of non-webhook triggers
        // to anonymous callers.
        return Err(ApiError::NotFound(format!("webhook '{slug}'")));
    }
    if !trigger.enabled {
        return Err(ApiError::NotFound(format!("webhook '{slug}'")));
    }

    // If a secret is configured, require it. A missing secret on a public
    // endpoint would let anyone fire the trigger — deliberately refuse to
    // accept such triggers here.
    let Some(ref secret) = trigger.secret else {
        tracing::warn!(
            slug = %slug,
            "webhook '{slug}' has no secret configured — rejecting public POST"
        );
        return Err(ApiError::Unauthorized);
    };

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

    let meta = serde_json::json!({
        "source": "public_webhook",
        "user_agent": headers.get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or(""),
    });
    let instance_id =
        orch8_engine::triggers::create_trigger_instance(&*state.storage, &trigger, body, meta)
            .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "instance_id": instance_id,
            "trigger": slug,
        })),
    ))
}
