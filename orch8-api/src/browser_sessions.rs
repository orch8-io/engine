//! Short-lived, scoped credentials for browser runtime nodes.
//!
//! A customer's app backend (holding an Operator key) calls
//! `POST /runtimes/browser-sessions`; the browser tab then uses the returned
//! token as its `x-api-key` (or `Authorization: Bearer`). The token is a
//! stateless, HMAC-signed claim set:
//!
//! * bound to one tenant, `kind = browser`, one `runtime_id`, a handler
//!   allowlist and (optionally) a queue allowlist;
//! * usable only for poll / complete / fail / heartbeat / release under
//!   `/workers/tasks*` (never task listing, commands, pins, or anything else);
//! * expires after `ttl_secs` (default 900, max 3600) — there is no refresh:
//!   the backend mints a new token.
//!
//! The signing key is derived from the root API key digest, so every replica
//! sharing the root key verifies every token. In `--insecure` mode a
//! process-random key is used (tokens then only verify on that process).

use std::sync::{Arc, OnceLock};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use orch8_types::continuity::{RuntimeId, RuntimeKind};
use orch8_types::ids::TenantId;

use crate::AppState;
use crate::auth::{OptionalAdmin, OptionalTenant, PrincipalContext};
use crate::error::ApiError;

/// Prefix identifying a browser-session token among API credentials.
pub const TOKEN_PREFIX: &str = "bst_";
pub const DEFAULT_TTL_SECS: u32 = 900;
pub const MAX_TTL_SECS: u32 = 3_600;
const MAX_HANDLERS: usize = 64;
const MAX_QUEUES: usize = 16;
const MAX_NAME_BYTES: usize = 256;

/// Verified identity a browser-session token binds a request to. Inserted
/// into request extensions by the auth middleware; worker handlers reject any
/// runtime identity, kind, handler, or queue that conflicts with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeBinding {
    pub tenant_id: TenantId,
    pub kind: RuntimeKind,
    pub runtime_id: RuntimeId,
    pub handlers: Vec<String>,
    pub queues: Vec<String>,
    pub expires_at: DateTime<Utc>,
}

impl RuntimeBinding {
    #[must_use]
    pub fn allows_handler(&self, handler: &str) -> bool {
        self.handlers.iter().any(|allowed| allowed == handler)
    }

    #[must_use]
    pub fn allows_queue(&self, queue: &str) -> bool {
        self.queues.iter().any(|allowed| allowed == queue)
    }

    /// Whether `worker_id` names this binding's runtime.
    #[must_use]
    pub fn is_runtime(&self, worker_id: &str) -> bool {
        worker_id == self.runtime_id.to_string()
    }
}

pub type OptionalBinding = Option<Extension<RuntimeBinding>>;

/// Reject a lease mutation made by a bound browser runtime on behalf of any
/// other worker identity.
pub fn enforce_bound_worker(binding: &OptionalBinding, worker_id: &str) -> Result<(), ApiError> {
    if let Some(Extension(binding)) = binding
        && !binding.is_runtime(worker_id)
    {
        return Err(ApiError::Forbidden(
            "worker_id conflicts with the browser session's runtime_id".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Claims {
    v: u8,
    jti: uuid::Uuid,
    tenant_id: String,
    runtime_id: RuntimeId,
    handlers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    queues: Vec<String>,
    iat: i64,
    exp: i64,
}

/// HMAC-SHA256 signer/verifier for browser-session tokens.
#[derive(Clone)]
pub struct BrowserSessionSigner {
    key: [u8; 32],
}

impl std::fmt::Debug for BrowserSessionSigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BrowserSessionSigner(<redacted>)")
    }
}

impl BrowserSessionSigner {
    /// The signer every replica derives from the same root API key digest;
    /// `None` (insecure mode) uses a process-random key.
    #[must_use]
    pub fn for_root(root_key_digest: Option<[u8; 32]>) -> Self {
        let Some(digest) = root_key_digest else {
            static EPHEMERAL: OnceLock<[u8; 32]> = OnceLock::new();
            return Self {
                key: *EPHEMERAL.get_or_init(rand::random),
            };
        };
        let mut hasher = Sha256::new();
        hasher.update(b"orch8-browser-session-v1\0");
        hasher.update(digest);
        Self {
            key: hasher.finalize().into(),
        }
    }

    fn mac(&self, payload: &[u8]) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.key)
            .expect("HMAC accepts a 32-byte key");
        mac.update(payload);
        mac
    }

    fn mint(&self, claims: &Claims) -> Result<String, ApiError> {
        let payload = serde_json::to_vec(claims)
            .map_err(|error| ApiError::Internal(format!("encode browser session: {error}")))?;
        let signature = self.mac(&payload).finalize().into_bytes();
        Ok(format!(
            "{TOKEN_PREFIX}{}.{}",
            URL_SAFE_NO_PAD.encode(&payload),
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    /// Verify a token's signature and expiry. `None` for anything invalid.
    #[must_use]
    pub fn verify(&self, token: &str, now: DateTime<Utc>) -> Option<RuntimeBinding> {
        let body = token.strip_prefix(TOKEN_PREFIX)?;
        let (payload_b64, signature_b64) = body.split_once('.')?;
        let payload = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
        let signature = URL_SAFE_NO_PAD.decode(signature_b64).ok()?;
        self.mac(&payload).verify_slice(&signature).ok()?;
        let claims: Claims = serde_json::from_slice(&payload).ok()?;
        if claims.v != 1 || claims.exp <= now.timestamp() {
            return None;
        }
        Some(RuntimeBinding {
            tenant_id: TenantId::new(claims.tenant_id).ok()?,
            kind: RuntimeKind::Browser,
            runtime_id: claims.runtime_id,
            handlers: claims.handlers,
            queues: claims.queues,
            expires_at: DateTime::from_timestamp(claims.exp, 0)?,
        })
    }
}

/// Routes a browser-session principal may call: the lease protocol only.
#[must_use]
pub fn route_allowed(method: &axum::http::Method, path: &str) -> bool {
    if method != axum::http::Method::POST {
        return false;
    }
    let path = path.strip_prefix(crate::API_V1_PREFIX).unwrap_or(path);
    if path == "/workers/tasks/poll" || path == "/workers/tasks/poll/queue" {
        return true;
    }
    let Some(rest) = path.strip_prefix("/workers/tasks/") else {
        return false;
    };
    let mut segments = rest.split('/');
    let (Some(id), Some(action), None) = (segments.next(), segments.next(), segments.next()) else {
        return false;
    };
    uuid::Uuid::parse_str(id).is_ok()
        && matches!(action, "complete" | "fail" | "heartbeat" | "release")
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/runtimes/browser-sessions", post(create_browser_session))
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateBrowserSessionRequest {
    /// Runtime identity to bind (a fresh one is minted when absent).
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    runtime_id: Option<RuntimeId>,
    /// Handler allowlist: polls for any other handler are refused.
    handlers: Vec<String>,
    /// Token lifetime in seconds (default 900, max 3600).
    #[serde(default)]
    ttl_secs: Option<u32>,
    /// Named queues the tab may poll with `/workers/tasks/poll/queue`.
    #[serde(default)]
    queues: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CreateBrowserSessionResponse {
    token: String,
    #[schema(value_type = String)]
    runtime_id: RuntimeId,
    expires_at: DateTime<Utc>,
    handlers: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    queues: Vec<String>,
}

fn validate_names(label: &str, names: &[String], max: usize) -> Result<(), ApiError> {
    if names.len() > max {
        return Err(ApiError::InvalidArgument(format!(
            "at most {max} {label} may be granted"
        )));
    }
    if names
        .iter()
        .any(|name| name.trim().is_empty() || name.len() > MAX_NAME_BYTES)
    {
        return Err(ApiError::InvalidArgument(format!(
            "{label} must be non-empty and at most {MAX_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

/// Mint a browser-session token. Operator/Admin only — called by the
/// customer's app backend, never by the browser itself.
#[utoipa::path(post, path = "/runtimes/browser-sessions", tag = "workers",
    request_body = CreateBrowserSessionRequest,
    responses(
        (status = 201, description = "Scoped browser-session token", body = CreateBrowserSessionResponse),
        (status = 400, description = "Invalid handlers, queues, or ttl"),
        (status = 403, description = "Caller is not an operator"),
    )
)]
pub(crate) async fn create_browser_session(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    principal: Option<Extension<PrincipalContext>>,
    tenant_ctx: OptionalTenant,
    Json(body): Json<CreateBrowserSessionRequest>,
) -> Result<(StatusCode, Json<CreateBrowserSessionResponse>), ApiError> {
    let principal = principal.map(|Extension(principal)| principal);
    if admin.is_none() && !crate::auth::principal_is_operator(principal.as_ref()) {
        return Err(ApiError::Forbidden(
            "minting browser sessions requires an operator or admin key".into(),
        ));
    }
    if principal.as_ref().is_none() && admin.is_none() {
        return Err(ApiError::Unauthorized);
    }
    if body.handlers.is_empty() {
        return Err(ApiError::InvalidArgument(
            "a browser session needs at least one handler".into(),
        ));
    }
    validate_names("handlers", &body.handlers, MAX_HANDLERS)?;
    validate_names("queues", &body.queues, MAX_QUEUES)?;
    let ttl = body.ttl_secs.unwrap_or(DEFAULT_TTL_SECS);
    if ttl == 0 || ttl > MAX_TTL_SECS {
        return Err(ApiError::InvalidArgument(format!(
            "ttl_secs must be between 1 and {MAX_TTL_SECS}"
        )));
    }
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(""))?;
    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(i64::from(ttl));
    let runtime_id = body.runtime_id.unwrap_or_default();
    let mut handlers = body.handlers;
    handlers.sort();
    handlers.dedup();
    let mut queues = body.queues;
    queues.sort();
    queues.dedup();
    let claims = Claims {
        v: 1,
        jti: uuid::Uuid::now_v7(),
        tenant_id: tenant_id.as_str().to_owned(),
        runtime_id,
        handlers: handlers.clone(),
        queues: queues.clone(),
        iat: now.timestamp(),
        exp: expires_at.timestamp(),
    };
    let token = state.browser_sessions.mint(&claims)?;
    tracing::info!(
        tenant_id = %tenant_id,
        %runtime_id,
        ttl_secs = ttl,
        handlers = handlers.len(),
        "minted browser session"
    );
    Ok((
        StatusCode::CREATED,
        Json(CreateBrowserSessionResponse {
            token,
            runtime_id,
            expires_at: DateTime::from_timestamp(claims.exp, 0).unwrap_or(expires_at),
            handlers,
            queues,
        }),
    ))
}

/// Shared signer handle stored on [`AppState`].
pub type SharedSigner = Arc<BrowserSessionSigner>;

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(exp: i64) -> Claims {
        Claims {
            v: 1,
            jti: uuid::Uuid::now_v7(),
            tenant_id: "acme".into(),
            runtime_id: RuntimeId::new(),
            handlers: vec!["read_dom".into()],
            queues: Vec::new(),
            iat: 0,
            exp,
        }
    }

    #[test]
    fn tokens_verify_bind_and_expire() {
        let signer = BrowserSessionSigner::for_root(Some([3; 32]));
        let now = Utc::now();
        let issued = claims(now.timestamp() + 60);
        let token = signer.mint(&issued).unwrap();
        assert!(token.starts_with(TOKEN_PREFIX));
        let binding = signer.verify(&token, now).unwrap();
        assert_eq!(binding.kind, RuntimeKind::Browser);
        assert_eq!(binding.runtime_id, issued.runtime_id);
        assert!(binding.allows_handler("read_dom"));
        assert!(!binding.allows_handler("charge_card"));
        assert!(
            signer
                .verify(&token, now + chrono::Duration::seconds(61))
                .is_none()
        );
    }

    #[test]
    fn forged_or_foreign_tokens_are_rejected() {
        let signer = BrowserSessionSigner::for_root(Some([3; 32]));
        let other = BrowserSessionSigner::for_root(Some([4; 32]));
        let now = Utc::now();
        let token = signer.mint(&claims(now.timestamp() + 60)).unwrap();
        assert!(other.verify(&token, now).is_none(), "different root key");
        let (payload, signature) = token[TOKEN_PREFIX.len()..].split_once('.').unwrap();
        let mut forged: Claims =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        forged.handlers.push("charge_card".into());
        let tampered = format!(
            "{TOKEN_PREFIX}{}.{signature}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&forged).unwrap())
        );
        assert!(signer.verify(&tampered, now).is_none(), "claims are signed");
        assert!(signer.verify("sk_live_x", now).is_none());
    }

    #[test]
    fn browser_sessions_reach_only_the_lease_protocol() {
        let post = axum::http::Method::POST;
        let id = uuid::Uuid::now_v7();
        for allowed in [
            "/api/v1/workers/tasks/poll".to_owned(),
            "/workers/tasks/poll/queue".to_owned(),
            format!("/api/v1/workers/tasks/{id}/complete"),
            format!("/api/v1/workers/tasks/{id}/fail"),
            format!("/api/v1/workers/tasks/{id}/heartbeat"),
            format!("/api/v1/workers/tasks/{id}/release"),
        ] {
            assert!(route_allowed(&post, &allowed), "{allowed}");
        }
        for denied in [
            "/api/v1/workers/tasks".to_owned(),
            "/api/v1/workers/tasks/stats".to_owned(),
            format!("/api/v1/workers/tasks/{id}/attempts"),
            format!("/api/v1/workers/tasks/{id}/artifacts/{id}"),
            "/api/v1/workers/commands".to_owned(),
            "/api/v1/workers/version-pins".to_owned(),
            "/api/v1/instances".to_owned(),
            "/api/v1/runtimes/browser-sessions".to_owned(),
        ] {
            assert!(!route_allowed(&post, &denied), "{denied}");
        }
        assert!(!route_allowed(
            &axum::http::Method::GET,
            "/api/v1/workers/tasks/poll"
        ));
    }
}
