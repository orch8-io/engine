//! Public (unauthenticated) endpoints that resolve `human_review`
//! interactive approvals — Slack button clicks, Teams card actions, and
//! email magic links.
//!
//! Authentication is the single-use approval token (256-bit random, stored
//! only as SHA-256, expiring), plus the Slack signing secret for Slack
//! interactions. A decision is recorded exactly like an Approver-capability
//! API call: a `human_input:<gate>` signal enqueued with
//! `enqueue_signal_if_active` (tenant-checked, refused for terminal
//! instances), plus an `approval_decision` audit-log entry naming the
//! channel and actor.
//!
//! Routes (merged by the server outside the API-key/tenant middleware):
//! - `POST /approvals/slack/interactions` — Slack interactivity Request URL.
//! - `GET  /approvals/act/{token}` — confirm page; **never** changes state,
//!   so link prefetchers / mail scanners cannot approve anything.
//! - `POST /approvals/act/{token}` — records the decision (HTML form post,
//!   or JSON for Teams `Action.Http` / scripted callers).

use std::net::SocketAddr;

use axum::Router;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use bytes::Bytes;
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use uuid::Uuid;

use orch8_types::approval_link::{
    ApprovalActionToken, ApprovalChannel, hash_token, token_looks_valid,
};
use orch8_types::instance::InstanceState;
use orch8_types::signal::{Signal, SignalType};

use crate::AppState;
use crate::error::ApiError;
use crate::public_http::{check_rate, html_escape, html_page, json_response};

/// Slack's documented replay window for `X-Slack-Request-Timestamp`.
const SLACK_TOLERANCE_SECS: i64 = 300;
const MAX_BODY: usize = 64 * 1024;
const MAX_COMMENT_CHARS: usize = 2_000;
const RATE_PER_SECOND: u64 = 30;

pub fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/approvals/slack/interactions", post(slack_interaction))
        .route("/approvals/act/{token}", get(confirm_page).post(act))
        .layer(DefaultBodyLimit::max(MAX_BODY))
}

type Peer = Result<ConnectInfo<SocketAddr>, axum::extract::rejection::ExtensionRejection>;

fn peer_addr(peer: Peer) -> Option<SocketAddr> {
    peer.ok().map(|ConnectInfo(a)| a)
}

/// Enqueue the decision through the same path as `POST /instances/{id}/signals`
/// and write the audit entry. The token has already been consumed.
async fn record_decision(
    state: &AppState,
    token: &ApprovalActionToken,
    actor: serde_json::Value,
    comment: Option<String>,
) -> Result<(), ApiError> {
    let instance = state
        .storage
        .get_instance(token.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound("approval target".into()))?;
    // Fail closed: the token's tenant must own the instance.
    if instance.tenant_id != token.tenant_id {
        tracing::error!(instance_id = %token.instance_id, "approval token tenant does not match instance tenant");
        return Err(ApiError::NotFound("approval target".into()));
    }
    let mut payload = serde_json::json!({
        "value": token.choice,
        "decided_by": actor,
    });
    if let Some(c) = comment.filter(|c| !c.trim().is_empty()) {
        payload["comment"] = serde_json::Value::String(c.chars().take(MAX_COMMENT_CHARS).collect());
    }
    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id: token.instance_id,
        signal_type: SignalType::Custom(format!("human_input:{}", token.block_id.as_str())),
        payload: payload.clone(),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    state
        .storage
        .enqueue_signal_if_active(&signal)
        .await
        .map_err(|e| match e {
            orch8_types::error::StorageError::TerminalTarget { .. } => {
                ApiError::Conflict("the workflow has already finished".into())
            }
            orch8_types::error::StorageError::NotFound { .. } => {
                ApiError::NotFound("approval target".into())
            }
            other => ApiError::from_storage(other, "signal"),
        })?;
    // Same wake-up as the signals endpoint for Scheduled instances.
    if let Ok(Some(fresh)) = state.storage.get_instance(token.instance_id).await
        && fresh.state == InstanceState::Scheduled
    {
        let _ = state
            .storage
            .conditional_update_instance_state(
                token.instance_id,
                InstanceState::Scheduled,
                InstanceState::Scheduled,
                Some(Utc::now()),
            )
            .await;
    }
    let entry = orch8_types::audit::AuditLogEntry {
        id: Uuid::now_v7(),
        instance_id: token.instance_id,
        tenant_id: token.tenant_id.clone(),
        event_type: "approval_decision".into(),
        from_state: Some(instance.state.to_string()),
        to_state: None,
        block_id: Some(token.block_id.as_str().to_owned()),
        details: serde_json::json!({
            "channel": token.channel.as_str(),
            "choice": token.choice,
            "decided_by": payload["decided_by"],
            "recipient": token.recipient,
            "signal_id": signal.id,
        }),
        created_at: Utc::now(),
    };
    if let Err(e) = state.storage.append_audit_log(&entry).await {
        tracing::warn!(instance_id = %token.instance_id, error = %e, "failed to write approval audit entry");
    }
    Ok(())
}

/// Human-readable prompt + choice label for the confirm page.
async fn describe(state: &AppState, token: &ApprovalActionToken) -> (String, String) {
    let fallback = (String::new(), token.choice.clone());
    let Ok(Some(instance)) = state.storage.get_instance(token.instance_id).await else {
        return fallback;
    };
    if instance.tenant_id != token.tenant_id {
        return fallback;
    }
    let Ok(Some(seq)) = state.storage.get_sequence(instance.sequence_id).await else {
        return fallback;
    };
    let Some(step) = crate::approvals::find_step_by_id(&seq, &token.block_id) else {
        return fallback;
    };
    let Some(h) = step.wait_for_input.as_ref() else {
        return fallback;
    };
    let label = h
        .effective_choices()
        .into_iter()
        .find(|c| c.value == token.choice)
        .map_or_else(|| token.choice.clone(), |c| c.label);
    (h.prompt.clone(), label)
}

fn invalid_link_page() -> Response {
    html_page(
        StatusCode::GONE,
        "Link unavailable",
        "<h1>This link is no longer valid</h1><p>It may have expired, or a decision for this request was already recorded.</p>",
    )
}

/// Look up a presented token without consuming it. Unknown, used, or
/// expired tokens all look the same to the caller.
async fn live_token(state: &AppState, raw: &str) -> Result<Option<ApprovalActionToken>, ApiError> {
    if !token_looks_valid(raw) {
        return Ok(None);
    }
    let token = state
        .storage
        .get_approval_token(&hash_token(raw))
        .await
        .map_err(|e| ApiError::from_storage(e, "approval token"))?;
    Ok(token.filter(|t| t.used_at.is_none() && t.expires_at > Utc::now()))
}

#[utoipa::path(get, path = "/approvals/act/{token}", tag = "approvals",
    params(("token" = String, Path, description = "Approval magic-link token")),
    responses(
        (status = 200, description = "Confirmation page (no state change)", content_type = "text/html"),
        (status = 410, description = "Unknown, used, or expired link", content_type = "text/html"),
    )
)]
pub(crate) async fn confirm_page(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    peer: Peer,
) -> Response {
    if let Err(e) = check_rate("approval-act", peer_addr(peer), RATE_PER_SECOND) {
        return axum::response::IntoResponse::into_response(e);
    }
    let token = match live_token(&state, &raw).await {
        Ok(Some(t)) => t,
        Ok(None) => return invalid_link_page(),
        Err(e) => return axum::response::IntoResponse::into_response(e),
    };
    if token.channel == ApprovalChannel::Slack {
        // Slack tokens are only valid through the signed interaction endpoint.
        return invalid_link_page();
    }
    let (prompt, label) = describe(&state, &token).await;
    let body = format!(
        "<h1>Confirm your decision</h1>{}<p>You are about to record: <strong>{}</strong></p>\
         <form method=\"post\"><label for=\"c\" class=\"muted\">Comment (optional)</label>\
         <textarea id=\"c\" name=\"comment\" maxlength=\"{MAX_COMMENT_CHARS}\"></textarea>\
         <button type=\"submit\">Confirm: {}</button></form>\
         <p class=\"muted\">This link is single-use and expires {}.</p>",
        if prompt.is_empty() {
            String::new()
        } else {
            format!("<p>{}</p>", html_escape(&prompt))
        },
        html_escape(&label),
        html_escape(&label),
        html_escape(&token.expires_at.format("%Y-%m-%d %H:%M UTC").to_string()),
    );
    html_page(StatusCode::OK, "Confirm decision", &body)
}

fn wants_json(headers: &HeaderMap) -> bool {
    let has = |name: axum::http::HeaderName| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("application/json"))
    };
    has(axum::http::header::CONTENT_TYPE) || has(axum::http::header::ACCEPT)
}

fn parse_comment(headers: &HeaderMap, body: &[u8]) -> Option<String> {
    if wants_json(headers) {
        serde_json::from_slice::<serde_json::Value>(body)
            .ok()?
            .get("comment")?
            .as_str()
            .map(str::to_string)
    } else {
        url::form_urlencoded::parse(body)
            .find(|(k, _)| k == "comment")
            .map(|(_, v)| v.into_owned())
    }
}

#[utoipa::path(post, path = "/approvals/act/{token}", tag = "approvals",
    params(("token" = String, Path, description = "Approval magic-link token")),
    request_body(content = String, content_type = "application/x-www-form-urlencoded", description = "Optional `comment` (form field or JSON `{\"comment\"}`)"),
    responses(
        (status = 200, description = "Decision recorded"),
        (status = 409, description = "Workflow already finished"),
        (status = 410, description = "Unknown, used, or expired link"),
    )
)]
pub(crate) async fn act(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    peer: Peer,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    use axum::response::IntoResponse as _;
    let json = wants_json(&headers);
    if let Err(e) = check_rate("approval-act", peer_addr(peer), RATE_PER_SECOND) {
        return e.into_response();
    }
    let gone = || {
        if json {
            json_response(
                StatusCode::GONE,
                &serde_json::json!({"error": "link is no longer valid"}),
            )
        } else {
            invalid_link_page()
        }
    };
    if !token_looks_valid(&raw) {
        return gone();
    }
    // Slack tokens must come through the signed interaction endpoint.
    match state.storage.get_approval_token(&hash_token(&raw)).await {
        Ok(Some(t)) if t.channel == ApprovalChannel::Slack => return gone(),
        Ok(_) => {}
        Err(e) => return ApiError::from_storage(e, "approval token").into_response(),
    }
    let token = match state
        .storage
        .consume_approval_token(&hash_token(&raw), Utc::now())
        .await
    {
        Ok(Some(t)) => t,
        Ok(None) => return gone(),
        Err(e) => return ApiError::from_storage(e, "approval token").into_response(),
    };
    let comment = parse_comment(&headers, &body);
    let actor = serde_json::json!({
        "channel": token.channel.as_str(),
        "recipient": token.recipient,
    });
    match record_decision(&state, &token, actor, comment).await {
        Ok(()) => {
            let (_, label) = describe(&state, &token).await;
            if json {
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"status": "recorded", "choice": token.choice}),
                )
            } else {
                html_page(
                    StatusCode::OK,
                    "Decision recorded",
                    &format!(
                        "<h1>Thanks — decision recorded</h1><p>You chose <strong>{}</strong>. You can close this page.</p>",
                        html_escape(&label)
                    ),
                )
            }
        }
        Err(ApiError::Conflict(_)) if !json => html_page(
            StatusCode::CONFLICT,
            "Already finished",
            "<h1>This workflow has already finished</h1><p>No decision was recorded.</p>",
        ),
        Err(e) => {
            tracing::error!(instance_id = %token.instance_id, error = %e, "approval token consumed but decision not recorded");
            e.into_response()
        }
    }
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) || s.len() > 128 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Verify Slack's `v0` request signature over the raw body.
pub(crate) fn verify_slack_signature(
    secret: &[u8],
    timestamp: &str,
    signature: &str,
    body: &[u8],
    now: i64,
) -> bool {
    let Ok(ts) = timestamp.parse::<i64>() else {
        return false;
    };
    if (now - ts).abs() > SLACK_TOLERANCE_SECS {
        return false;
    }
    let Some(provided) = signature.strip_prefix("v0=").and_then(decode_hex) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    mac.update(b"v0:");
    mac.update(timestamp.as_bytes());
    mac.update(b":");
    mac.update(body);
    mac.verify_slice(&provided).is_ok()
}

async fn resolve_slack_secret(
    state: &AppState,
    token: &ApprovalActionToken,
) -> Option<zeroize::Zeroizing<String>> {
    let reference = token.verify_secret_ref.as_ref()?;
    let mut value = serde_json::Value::String(format!("credentials://{reference}"));
    orch8_engine::credentials::resolve_in_value(
        &*state.storage,
        token.tenant_id.as_str(),
        &mut value,
    )
    .await
    .ok()?;
    match value {
        serde_json::Value::String(s) if !s.is_empty() => Some(zeroize::Zeroizing::new(s)),
        _ => None,
    }
}

#[utoipa::path(post, path = "/approvals/slack/interactions", tag = "approvals",
    request_body(content = String, content_type = "application/x-www-form-urlencoded", description = "Slack interactivity payload"),
    responses(
        (status = 200, description = "Interaction processed (Slack requires 200)"),
        (status = 401, description = "Bad Slack signature, stale timestamp, or unknown token"),
    )
)]
pub(crate) async fn slack_interaction(
    State(state): State<AppState>,
    peer: Peer,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    use axum::response::IntoResponse as _;
    if let Err(e) = check_rate("approval-slack", peer_addr(peer), RATE_PER_SECOND) {
        return e.into_response();
    }
    let header = |n: &str| headers.get(n).and_then(|v| v.to_str().ok()).unwrap_or("");
    let timestamp = header("x-slack-request-timestamp").to_string();
    let signature = header("x-slack-signature").to_string();

    let Some(payload) = url::form_urlencoded::parse(&body)
        .find(|(k, _)| k == "payload")
        .and_then(|(_, v)| serde_json::from_str::<serde_json::Value>(&v).ok())
    else {
        return ApiError::Unauthorized.into_response();
    };
    let Some(raw) = payload
        .pointer("/actions/0/value")
        .and_then(serde_json::Value::as_str)
        .filter(|r| token_looks_valid(r))
    else {
        return ApiError::Unauthorized.into_response();
    };
    let token = match state.storage.get_approval_token(&hash_token(raw)).await {
        Ok(Some(t)) if t.channel == ApprovalChannel::Slack => t,
        Ok(_) => return ApiError::Unauthorized.into_response(),
        Err(e) => return ApiError::from_storage(e, "approval token").into_response(),
    };
    let Some(secret) = resolve_slack_secret(&state, &token).await else {
        tracing::warn!(instance_id = %token.instance_id, "slack interaction rejected: signing secret credential unavailable");
        return ApiError::Unauthorized.into_response();
    };
    if !verify_slack_signature(
        secret.as_bytes(),
        &timestamp,
        &signature,
        &body,
        Utc::now().timestamp(),
    ) {
        tracing::warn!(instance_id = %token.instance_id, "slack interaction rejected: bad signature or stale timestamp");
        return ApiError::Unauthorized.into_response();
    }
    drop(secret);

    let consumed = match state
        .storage
        .consume_approval_token(&token.token_hash, Utc::now())
        .await
    {
        Ok(c) => c,
        Err(e) => return ApiError::from_storage(e, "approval token").into_response(),
    };
    let Some(token) = consumed else {
        return json_response(
            StatusCode::OK,
            &serde_json::json!({"response_type": "ephemeral", "replace_original": false,
                "text": "This approval was already recorded or has expired."}),
        );
    };
    let actor = serde_json::json!({
        "channel": "slack",
        "user_id": payload.pointer("/user/id"),
        "username": payload.pointer("/user/username").or_else(|| payload.pointer("/user/name")),
        "team_id": payload.pointer("/team/id"),
    });
    match record_decision(&state, &token, actor, None).await {
        Ok(()) => json_response(
            StatusCode::OK,
            &serde_json::json!({"response_type": "ephemeral", "replace_original": false,
                "text": format!("Recorded: {}", token.choice)}),
        ),
        Err(e) => {
            tracing::error!(instance_id = %token.instance_id, error = %e, "slack approval consumed but decision not recorded");
            json_response(
                StatusCode::OK,
                &serde_json::json!({"response_type": "ephemeral", "replace_original": false,
                    "text": "Could not record the decision (the workflow may have finished)."}),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &[u8], ts: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(b"v0:");
        mac.update(ts.as_bytes());
        mac.update(b":");
        mac.update(body);
        let bytes = mac.finalize().into_bytes();
        bytes.iter().fold(String::from("v0="), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
    }

    #[test]
    fn slack_signature_verification() {
        // Slack's documented example.
        let secret = b"8f742231b10e8888abcd99yyyzzz85a5";
        let body = b"token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c";
        let ts = "1531420618";
        assert_eq!(
            sign(secret, ts, body),
            "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503"
        );
        let now = 1_531_420_618;
        let sig = sign(secret, ts, body);
        assert!(verify_slack_signature(secret, ts, &sig, body, now));
        assert!(
            !verify_slack_signature(secret, ts, &sig, body, now + 301),
            "stale"
        );
        assert!(!verify_slack_signature(b"other", ts, &sig, body, now));
        assert!(!verify_slack_signature(secret, ts, &sig, b"tampered", now));
        assert!(!verify_slack_signature(secret, ts, "v1=00", body, now));
    }
}
