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

use std::sync::LazyLock;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use moka::future::Cache;

use orch8_types::trigger::TriggerType;

use crate::error::ApiError;
use crate::AppState;

/// Maximum skew permitted between the caller's `x-trigger-timestamp` and
/// the server clock. Matches the default window most `SaaS` webhook senders
/// (Stripe, GitHub) use — large enough to tolerate NTP drift, small enough
/// that a captured payload can't be replayed the next day.
const REPLAY_WINDOW_SECS: i64 = 300;

/// Global nonce cache with composite key `slug:nonce`. Using a single cache
/// instead of a per-slug `DashMap` prevents unbounded growth when many unique
/// slugs are encountered (e.g., scanning probes or UUID-based slugs).
static SEEN_NONCES: LazyLock<Cache<String, ()>> = LazyLock::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs((REPLAY_WINDOW_SECS as u64) + 60))
        .max_capacity(100_000)
        .build()
});

/// Maximum body size for public webhooks. Webhook payloads are small event
/// notifications (GitHub, Stripe, etc.) — anything larger is suspicious and
/// should be rejected to prevent memory exhaustion / JSON parse `DoS`.
const MAX_WEBHOOK_BODY_SIZE: usize = 1024 * 1024; // 1 MB

/// Public router — merged into the server *after* auth middleware so these
/// routes are reachable without a tenant header or API key.
pub fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/webhooks/{slug}", post(public_webhook))
        .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_SIZE))
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
#[utoipa::path(post, path = "/webhooks/{slug}", tag = "webhooks",
    params(("slug" = String, Path, description = "Webhook slug (the trigger slug)")),
    request_body = serde_json::Value,
    responses(
        (status = 201, description = "Instance created from webhook payload"),
        (status = 401, description = "Missing or invalid `x-trigger-secret`"),
        (status = 404, description = "Unknown slug, disabled trigger, or non-webhook trigger type"),
    )
)]
pub(crate) async fn public_webhook(
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
    if !orch8_types::auth::verify_secret_constant_time(provided, secret.expose()) {
        return Err(ApiError::Unauthorized);
    }

    // Replay protection: require `x-trigger-timestamp` within the window
    // AND a unique `x-trigger-nonce` that we haven't seen recently. The
    // timestamp bounds the replay window; the nonce prevents repeated
    // delivery inside the window (e.g. an attacker capturing a single
    // request and replaying it milliseconds later).
    //
    // Headers are optional when the trigger has no secret — but we already
    // rejected secret-less triggers above, so here a configured secret
    // implies replay protection is active. Any third-party sender that
    // adopted orch8 webhooks before this change will need to send these
    // headers; we surface a clear 401 so the failure mode is obvious rather
    // than silent success.
    let ts_hdr = headers
        .get("x-trigger-timestamp")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());
    let Some(ts) = ts_hdr else {
        tracing::warn!(slug = %slug, "webhook rejected: missing/invalid x-trigger-timestamp");
        return Err(ApiError::Unauthorized);
    };
    let now = chrono::Utc::now().timestamp();
    if now.abs_diff(ts) > REPLAY_WINDOW_SECS as u64 {
        tracing::warn!(
            slug = %slug,
            skew = now - ts,
            "webhook rejected: timestamp outside replay window"
        );
        return Err(ApiError::Unauthorized);
    }

    let nonce = headers
        .get("x-trigger-nonce")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if nonce.is_empty() {
        tracing::warn!(slug = %slug, "webhook rejected: missing x-trigger-nonce");
        return Err(ApiError::Unauthorized);
    }
    let composite_key = format!("{slug}:{nonce}");
    if SEEN_NONCES.get(&composite_key).await.is_some() {
        tracing::warn!(slug = %slug, "webhook rejected: nonce reuse detected");
        return Err(ApiError::Unauthorized);
    }
    SEEN_NONCES.insert(composite_key, ()).await;

    let meta = serde_json::json!({
        "source": "public_webhook",
        "user_agent": headers.get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or(""),
    });
    let instance_id = orch8_engine::triggers::create_trigger_instance(
        &*state.storage,
        &trigger,
        body,
        meta,
        None,
    )
    .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "instance_id": instance_id,
            "trigger": slug,
        })),
    ))
}
