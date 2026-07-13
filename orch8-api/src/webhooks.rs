//! Public (unauthenticated) webhook ingestion.
//!
//! `POST /webhooks/{slug}` is the inbound endpoint third parties (GitHub,
//! Stripe, Shopify, ...) POST events to. It deliberately bypasses the tenant
//! header + API key middlewares because the caller is external and won't
//! have credentials — authentication is enforced instead by the trigger's own
//! an HMAC-SHA256 signature over the timestamp, nonce, and exact request body.
//!
//! The server wires this router in *after* the auth middleware layers so its
//! routes are not subject to them. Do not merge this module through
//! [`crate::build_router`] — that would re-introduce the middleware.
//!
//! Only triggers with `trigger_type = "webhook"` are accepted. `event` and
//! `nats` types are rejected with 404 (they're never meant for public entry).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use base64::Engine as _;
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use moka::sync::Cache;
use sha2::Sha256;

use orch8_types::trigger::TriggerType;

use crate::AppState;
use crate::error::ApiError;

/// Maximum skew permitted between the caller's `x-trigger-timestamp` and
/// the server clock. Matches the default window most `SaaS` webhook senders
/// (Stripe, GitHub) use — large enough to tolerate NTP drift, small enough
/// that a captured payload can't be replayed the next day.
const REPLAY_WINDOW_SECS: i64 = 300;

/// Maximum a caller's timestamp may be in the *future* relative to the server
/// clock. Future-dated timestamps only need to tolerate modest clock skew;
/// allowing the full [`REPLAY_WINDOW_SECS`] into the future would let an
/// attacker pre-date a captured payload to extend its validity well past the
/// nonce cache's reach. The past window stays at [`REPLAY_WINDOW_SECS`] for
/// NTP drift + network delay.
const MAX_FUTURE_SKEW_SECS: i64 = 60;

/// `true` if a caller's unix `ts` falls within the accepted window around the
/// server clock `now`: up to [`REPLAY_WINDOW_SECS`] in the past and
/// [`MAX_FUTURE_SKEW_SECS`] in the future.
fn timestamp_within_window(now: i64, ts: i64) -> bool {
    let age = now - ts; // positive = past, negative = future
    (-MAX_FUTURE_SKEW_SECS..=REPLAY_WINDOW_SECS).contains(&age)
}

/// Maximum body size for public webhooks. Webhook payloads are small event
/// notifications (GitHub, Stripe, etc.) — anything larger is suspicious and
/// should be rejected to prevent memory exhaustion / JSON parse `DoS`.
const MAX_WEBHOOK_BODY_SIZE: usize = 1024 * 1024; // 1 MB

const SIGNATURE_PREFIX: &str = "v1=";
const MIN_NONCE_LEN: usize = 16;
const MAX_NONCE_LEN: usize = 128;
const MAX_WEBHOOKS_PER_SECOND: u64 = 300;

static WEBHOOK_RATE_COUNTERS: LazyLock<Cache<String, Arc<AtomicU64>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(100_000)
        .time_to_live(Duration::from_secs(1))
        .build()
});

fn check_rate_limit(peer: Option<SocketAddr>, slug: &str) -> Result<(), ApiError> {
    let peer = peer.map_or_else(|| "unknown".to_owned(), |address| address.ip().to_string());
    let key = format!("{peer}:{slug}");
    let counter = WEBHOOK_RATE_COUNTERS
        .entry(key)
        .or_insert_with(|| Arc::new(AtomicU64::new(0)))
        .into_value();
    if counter.fetch_add(1, Ordering::Relaxed) >= MAX_WEBHOOKS_PER_SECOND {
        return Err(ApiError::RateLimited(
            "public webhook request rate exceeded".into(),
        ));
    }
    Ok(())
}

/// Verify `v1=base64url(HMAC-SHA256(secret, timestamp.nonce.body))`.
fn verify_signature(
    signature: &str,
    secret: &[u8],
    timestamp: &str,
    nonce: &str,
    body: &[u8],
) -> bool {
    let Some(encoded) = signature.strip_prefix(SIGNATURE_PREFIX) else {
        return false;
    };
    let Ok(provided) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(nonce.as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.verify_slice(&provided).is_ok()
}

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
/// - The trigger has no secret, or the request signature is missing/invalid.
#[utoipa::path(post, path = "/webhooks/{slug}", tag = "webhooks",
    params(("slug" = String, Path, description = "Webhook slug (the trigger slug)")),
    request_body = serde_json::Value,
    responses(
        (status = 202, description = "Webhook accepted; instance created and queued for execution"),
        (status = 401, description = "Missing or invalid webhook signature"),
        (status = 404, description = "Unknown slug, disabled trigger, or non-webhook trigger type"),
    )
)]
pub(crate) async fn public_webhook(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    peer: Result<ConnectInfo<SocketAddr>, axum::extract::rejection::ExtensionRejection>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    check_rate_limit(peer.ok().map(|ConnectInfo(address)| address), &slug)?;

    // Unscoped: this is the public, unauthenticated webhook endpoint --
    // resolving *which* tenant owns `slug` is this lookup's job, since the
    // caller has no tenant context to scope by. The request signature and
    // trigger-type checks below are the real authorization boundary here.
    let trigger = state
        .storage
        .get_trigger(None, &slug)
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
    // accept such triggers here. Empty secrets are treated as unconfigured
    // (defence in depth — creation should reject them).
    let Some(ref secret) = trigger.secret else {
        tracing::warn!(
            slug = %slug,
            "webhook '{slug}' has no secret configured — rejecting public POST"
        );
        return Err(ApiError::Unauthorized);
    };
    if secret.is_empty() {
        tracing::warn!(slug = %slug, "webhook '{slug}' has an empty secret — rejecting public POST");
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
    let timestamp = headers
        .get("x-trigger-timestamp")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Ok(ts) = timestamp.parse::<i64>() else {
        tracing::warn!(slug = %slug, "webhook rejected: missing/invalid x-trigger-timestamp");
        return Err(ApiError::Unauthorized);
    };
    let now = chrono::Utc::now().timestamp();
    if !timestamp_within_window(now, ts) {
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
    if !(MIN_NONCE_LEN..=MAX_NONCE_LEN).contains(&nonce.len())
        || !nonce.bytes().all(|byte| byte.is_ascii_graphic())
    {
        tracing::warn!(slug = %slug, "webhook rejected: missing x-trigger-nonce");
        return Err(ApiError::Unauthorized);
    }

    let signature = headers
        .get("x-orch8-signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !verify_signature(
        signature,
        secret.expose().as_bytes(),
        timestamp,
        nonce,
        &body,
    ) {
        return Err(ApiError::Unauthorized);
    }

    let expires_at =
        chrono::Utc::now() + chrono::Duration::seconds(REPLAY_WINDOW_SECS + MAX_FUTURE_SKEW_SECS);
    let claimed = state
        .storage
        .claim_webhook_nonce(&slug, nonce, expires_at)
        .await
        .map_err(|error| ApiError::from_storage(error, "webhook nonce"))?;
    if !claimed {
        tracing::warn!(slug = %slug, "webhook rejected: nonce reuse detected");
        return Err(ApiError::Unauthorized);
    }

    let body: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|error| ApiError::InvalidArgument(format!("invalid webhook JSON: {error}")))?;

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

#[cfg(test)]
mod tests {
    use super::{
        MAX_FUTURE_SKEW_SECS, MAX_WEBHOOKS_PER_SECOND, REPLAY_WINDOW_SECS, check_rate_limit,
        timestamp_within_window, verify_signature,
    };
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    use orch8_types::config::SecretString;
    use sha2::Sha256;

    #[test]
    fn empty_configured_secret_is_treated_as_unconfigured() {
        let secret = SecretString::new(String::new());
        assert!(secret.is_empty());
    }

    #[test]
    fn timestamp_window_accepts_recent_past_and_small_future() {
        let now = 1_000_000;
        assert!(timestamp_within_window(now, now), "exact now");
        assert!(timestamp_within_window(now, now - REPLAY_WINDOW_SECS)); // edge of past window
        assert!(timestamp_within_window(now, now - REPLAY_WINDOW_SECS / 2));
        assert!(timestamp_within_window(now, now + MAX_FUTURE_SKEW_SECS)); // edge of future skew
    }

    #[test]
    fn timestamp_window_rejects_stale_and_far_future() {
        let now = 1_000_000;
        // Too far in the past (beyond replay window).
        assert!(!timestamp_within_window(now, now - REPLAY_WINDOW_SECS - 1));
        // Far-future-dated payload — previously accepted up to +300s, now
        // rejected beyond the tight skew allowance.
        assert!(!timestamp_within_window(
            now,
            now + MAX_FUTURE_SKEW_SECS + 1
        ));
        assert!(!timestamp_within_window(now, now + REPLAY_WINDOW_SECS));
    }

    #[test]
    fn signature_binds_timestamp_nonce_and_body() {
        let secret = b"a sufficiently long webhook secret";
        let timestamp = "1000000";
        let nonce = "unique-request-nonce";
        let body = br#"{"event":"created"}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(nonce.as_bytes());
        mac.update(b".");
        mac.update(body);
        let encoded =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let signature = format!("v1={encoded}");

        assert!(verify_signature(&signature, secret, timestamp, nonce, body));
        assert!(!verify_signature(
            &signature, secret, timestamp, nonce, b"{}"
        ));
        assert!(!verify_signature(
            &signature,
            secret,
            timestamp,
            "another-nonce",
            body
        ));
    }

    #[test]
    fn webhook_rate_limit_is_bounded_per_peer_and_slug() {
        let slug = "rate-limit-unit-test";
        for _ in 0..MAX_WEBHOOKS_PER_SECOND {
            assert!(check_rate_limit(None, slug).is_ok());
        }
        assert!(matches!(
            check_rate_limit(None, slug),
            Err(crate::error::ApiError::RateLimited(_))
        ));
    }
}
