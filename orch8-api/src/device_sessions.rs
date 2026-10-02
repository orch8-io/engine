//! Short-lived, scoped credentials for phone runtime nodes.
//!
//! A phone app must never ship an operator (or any stored) API key: anyone
//! who extracts it from the app binary would hold it. Instead the customer's
//! app backend (holding an Operator key) calls `POST /runtimes/device-sessions`
//! for one device and the phone's persisted runtime id, and hands the returned
//! `dst_…` token to the SDK (through its `TokenProvider` callback, which also
//! refreshes it on `401`). The token is a stateless claim set signed by the
//! same signer as browser sessions (`ORCH8_BROWSER_SESSION_SECRET`, else the
//! root-key derivation):
//!
//! * bound to one tenant, `kind = mobile`, one `device_id`, one `runtime_id`,
//!   and a handler allowlist;
//! * usable only on the routes [`route_allowed`] lists (denied by default):
//!   the device's own mobile register / sync / runtime advertisement, the
//!   worker lease protocol, and the continuity delegation calls — each
//!   handler additionally enforces that the device, runtime, execution, or
//!   delegation it acts on is the bound one's;
//! * expires after `ttl_secs` (default 3600, max 86400); the backend mints a
//!   new one when the SDK asks for a refresh.

use axum::extract::State;
use axum::http::{Method, StatusCode};
use axum::routing::post;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::continuity::{RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel};
use orch8_types::ids::TenantId;

use crate::AppState;
use crate::auth::{OptionalAdmin, OptionalTenant, PrincipalContext};
use crate::browser_sessions::{
    Claims, MAX_HANDLERS, MAX_NAME_BYTES, OptionalBinding, RuntimeBinding, device_binding,
    validate_names,
};
use crate::error::ApiError;

/// Prefix identifying a device-session token among API credentials.
pub const TOKEN_PREFIX: &str = "dst_";
pub const DEFAULT_TTL_SECS: u32 = 3_600;
pub const MAX_TTL_SECS: u32 = 86_400;

/// Routes a device-session principal may call. Everything else — task
/// listing, worker commands, browser/device session minting, sequences,
/// credentials, instances, handoffs, other devices' data — is refused
/// before routing.
#[must_use]
pub fn route_allowed(method: &Method, path: &str) -> bool {
    let path = path.strip_prefix(crate::API_V1_PREFIX).unwrap_or(path);
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if *method == Method::GET {
        return match segments.as_slice() {
            ["runtimes"] => true,
            ["continuity", "delegations", id] => uuid::Uuid::parse_str(id).is_ok(),
            _ => false,
        };
    }
    if *method != Method::POST {
        return false;
    }
    match segments.as_slice() {
        ["mobile", "sync"]
        | ["mobile", "devices", "register"]
        | ["workers", "tasks", "poll"]
        | ["continuity", "executions" | "grants"]
        | ["continuity", "delegations", "claim"] => true,
        ["mobile", "devices", device_id, "runtime"] => !device_id.is_empty(),
        ["workers", "tasks", id, action] => {
            uuid::Uuid::parse_str(id).is_ok()
                && matches!(*action, "complete" | "fail" | "heartbeat" | "release")
        }
        _ => false,
    }
}

/// A device session may only act for its own device.
pub fn enforce_bound_device(binding: &OptionalBinding, device_id: &str) -> Result<(), ApiError> {
    if let Some(binding) = device_binding(binding)
        && binding.device_id.as_deref() != Some(device_id)
    {
        return Err(ApiError::Forbidden(
            "device_id conflicts with the device session's device".into(),
        ));
    }
    Ok(())
}

/// A device session may only act as its own runtime.
pub fn enforce_bound_runtime(
    binding: &OptionalBinding,
    runtime_id: RuntimeId,
) -> Result<(), ApiError> {
    if let Some(binding) = device_binding(binding)
        && binding.runtime_id != runtime_id
    {
        return Err(ApiError::Forbidden(
            "runtime_id conflicts with the device session's runtime".into(),
        ));
    }
    Ok(())
}

/// Clamp a device-session runtime advertisement: the runtime must be the
/// bound one, it may only advertise handlers its token grants (they are
/// the handlers delegations are routed to it for), and it carries no
/// placement labels.
pub fn clamp_advertisement(
    binding: &RuntimeBinding,
    capabilities: &mut orch8_types::continuity::RuntimeCapabilities,
) -> Result<(), ApiError> {
    if capabilities.runtime_id != binding.runtime_id || capabilities.kind != binding.kind {
        return Err(ApiError::Forbidden(
            "capabilities kind/runtime_id conflict with the device session".into(),
        ));
    }
    capabilities
        .handlers
        .retain(|handler| binding.allows_handler(handler));
    // Placement labels (`residency=…`) are operator-vouched facts a phone
    // cannot assert for itself.
    capabilities.labels.clear();
    capabilities.expires_at = capabilities.expires_at.min(binding.expires_at);
    Ok(())
}

/// What a device session sees of `GET /runtimes`: only the runtimes it
/// could delegate to right now, reduced to the facts destination selection
/// matches on. The full list would hand any phone the tenant's runtime
/// inventory (regions, hardware, credential references, network and battery
/// state, capsule keys); a device only needs to pick a live, delegation-
/// capable destination that advertises the handler it delegates.
///
/// Kept: `runtime_id`, `kind`, `handlers`, `observed_at`/`expires_at`
/// (liveness), and `trust` normalized to `registered` (selection only
/// requires *at least* registered). Dropped: the caller's own runtime,
/// draining or expired runtimes, runtimes below registered trust or not
/// accepting delegations, and every other fact.
#[must_use]
pub fn destination_view(
    runtimes: Vec<RuntimeCapabilities>,
    caller: RuntimeId,
    now: DateTime<Utc>,
) -> Vec<RuntimeCapabilities> {
    runtimes
        .into_iter()
        .filter(|runtime| {
            runtime.runtime_id != caller
                && !runtime.draining
                && runtime.expires_at > now
                && runtime.trust >= RuntimeTrustLevel::Registered
                && runtime
                    .handlers
                    .iter()
                    .any(|handler| handler == orch8_engine::delegation::DELEGATION_HANDLER)
        })
        .map(|runtime| RuntimeCapabilities {
            runtime_id: runtime.runtime_id,
            kind: runtime.kind,
            trust: RuntimeTrustLevel::Registered,
            handlers: runtime.handlers,
            plugins: Vec::new(),
            credentials: Vec::new(),
            regions: Vec::new(),
            hardware: Vec::new(),
            offline_capable: false,
            connectivity: None,
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            // Destination selection matches kind and handlers only; labels
            // (residency, hardware classes, …) are inventory, not routing.
            labels: std::collections::BTreeMap::new(),
            observed_at: runtime.observed_at,
            expires_at: runtime.expires_at,
        })
        .collect()
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/runtimes/device-sessions", post(create_device_session))
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateDeviceSessionRequest {
    /// The mobile device (`MobileEngineConfig.device_id`) the token acts for.
    device_id: String,
    /// The phone's persisted runtime id (`MobileEngine.node_runtime_id()`):
    /// its `worker_id` for leases and the owner of executions it hosts.
    #[schema(value_type = String)]
    runtime_id: RuntimeId,
    /// Handlers the phone may poll and advertise. Empty = the phone only
    /// delegates (it never claims tasks).
    #[serde(default)]
    handlers: Vec<String>,
    /// Token lifetime in seconds (default 3600, max 86400).
    #[serde(default)]
    ttl_secs: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CreateDeviceSessionResponse {
    token: String,
    device_id: String,
    #[schema(value_type = String)]
    runtime_id: RuntimeId,
    expires_at: DateTime<Utc>,
    handlers: Vec<String>,
}

/// Mint a device-session token. Operator/Admin only — called by the
/// customer's app backend, never by the phone itself.
#[utoipa::path(post, path = "/runtimes/device-sessions", tag = "workers",
    request_body = CreateDeviceSessionRequest,
    responses(
        (status = 201, description = "Scoped device-session token", body = CreateDeviceSessionResponse),
        (status = 400, description = "Invalid device_id, handlers, or ttl"),
        (status = 403, description = "Caller is not an operator"),
        (status = 409, description = "The device is registered to another tenant"),
    )
)]
pub(crate) async fn create_device_session(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    principal: Option<Extension<PrincipalContext>>,
    tenant_ctx: OptionalTenant,
    Json(body): Json<CreateDeviceSessionRequest>,
) -> Result<(StatusCode, Json<CreateDeviceSessionResponse>), ApiError> {
    let principal = principal.map(|Extension(principal)| principal);
    if admin.is_none() && !crate::auth::principal_is_operator(principal.as_ref()) {
        return Err(ApiError::Forbidden(
            "minting device sessions requires an operator or admin key".into(),
        ));
    }
    if principal.is_none() && admin.is_none() {
        return Err(ApiError::Unauthorized);
    }
    if body.device_id.trim().is_empty() || body.device_id.len() > MAX_NAME_BYTES {
        return Err(ApiError::InvalidArgument(format!(
            "device_id must be non-empty and at most {MAX_NAME_BYTES} bytes"
        )));
    }
    validate_names("handlers", &body.handlers, MAX_HANDLERS)?;
    let ttl = body.ttl_secs.unwrap_or(DEFAULT_TTL_SECS);
    if ttl == 0 || ttl > MAX_TTL_SECS {
        return Err(ApiError::InvalidArgument(format!(
            "ttl_secs must be between 1 and {MAX_TTL_SECS}"
        )));
    }
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(""))?;
    // A device already registered to another tenant cannot be bound here.
    if let Some(device) = state
        .storage
        .get_mobile_device(&body.device_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "mobile_devices"))?
        && device.tenant_id != tenant_id.as_str()
    {
        return Err(ApiError::Conflict(format!(
            "device {} is already registered to another tenant",
            body.device_id
        )));
    }
    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(i64::from(ttl));
    let mut handlers = body.handlers;
    handlers.sort();
    handlers.dedup();
    let claims = Claims {
        v: 1,
        jti: uuid::Uuid::now_v7(),
        tenant_id: tenant_id.as_str().to_owned(),
        runtime_id: body.runtime_id,
        handlers: handlers.clone(),
        queues: Vec::new(),
        kind: Some(RuntimeKind::Mobile),
        device_id: Some(body.device_id.clone()),
        iat: now.timestamp(),
        exp: expires_at.timestamp(),
    };
    let token = state.browser_sessions.mint(&claims)?;
    tracing::info!(
        tenant_id = %tenant_id,
        runtime_id = %body.runtime_id,
        device_id = %body.device_id,
        ttl_secs = ttl,
        handlers = handlers.len(),
        "minted device session"
    );
    Ok((
        StatusCode::CREATED,
        Json(CreateDeviceSessionResponse {
            token,
            device_id: body.device_id,
            runtime_id: body.runtime_id,
            expires_at: DateTime::from_timestamp(claims.exp, 0).unwrap_or(expires_at),
            handlers,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser_sessions::BrowserSessionSigner;

    fn claims(kind: Option<RuntimeKind>, device_id: Option<&str>, exp: i64) -> Claims {
        Claims {
            v: 1,
            jti: uuid::Uuid::now_v7(),
            tenant_id: "acme".into(),
            runtime_id: RuntimeId::new(),
            handlers: vec!["scan".into()],
            queues: Vec::new(),
            kind,
            device_id: device_id.map(str::to_owned),
            iat: 0,
            exp,
        }
    }

    #[test]
    fn device_tokens_bind_device_and_runtime_and_never_cross_kinds() {
        let signer = BrowserSessionSigner::for_root(Some([5; 32]));
        let now = Utc::now();
        let issued = claims(
            Some(RuntimeKind::Mobile),
            Some("phone-1"),
            now.timestamp() + 60,
        );
        let token = signer.mint(&issued).unwrap();
        assert!(token.starts_with(TOKEN_PREFIX));
        let binding = signer.verify(&token, now).unwrap();
        assert_eq!(binding.kind, RuntimeKind::Mobile);
        assert_eq!(binding.device_id.as_deref(), Some("phone-1"));
        assert_eq!(binding.runtime_id, issued.runtime_id);
        assert!(binding.is_device());

        // Re-prefixing a device token as a browser token (or the reverse)
        // fails: the signed kind must match the prefix.
        let body = &token[TOKEN_PREFIX.len()..];
        assert!(signer.verify(&format!("bst_{body}"), now).is_none());
        let browser = signer
            .mint(&claims(None, None, now.timestamp() + 60))
            .unwrap();
        let browser_body = &browser[crate::browser_sessions::TOKEN_PREFIX.len()..];
        assert!(
            signer
                .verify(&format!("{TOKEN_PREFIX}{browser_body}"), now)
                .is_none()
        );
        assert_eq!(signer.verify(&browser, now).unwrap().device_id, None);

        // A mobile claim set without a device is never valid.
        let unbound = signer
            .mint(&claims(
                Some(RuntimeKind::Mobile),
                None,
                now.timestamp() + 60,
            ))
            .unwrap();
        assert!(signer.verify(&unbound, now).is_none());
        // Expiry.
        assert!(
            signer
                .verify(&token, now + chrono::Duration::seconds(61))
                .is_none()
        );
    }

    #[test]
    fn device_advertisements_are_clamped_to_the_binding() {
        let now = Utc::now();
        let binding = RuntimeBinding {
            tenant_id: TenantId::unchecked("acme"),
            kind: RuntimeKind::Mobile,
            runtime_id: RuntimeId::new(),
            device_id: Some("phone-1".into()),
            handlers: vec!["scan".into()],
            queues: Vec::new(),
            expires_at: now + chrono::Duration::seconds(60),
        };
        let mut caps = RuntimeCapabilities {
            runtime_id: binding.runtime_id,
            kind: RuntimeKind::Mobile,
            trust: RuntimeTrustLevel::Registered,
            handlers: vec!["scan".into(), "charge".into()],
            plugins: Vec::new(),
            credentials: Vec::new(),
            regions: Vec::new(),
            hardware: Vec::new(),
            offline_capable: true,
            connectivity: None,
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            labels: [("residency".to_owned(), "eu".to_owned())].into(),
            observed_at: now,
            expires_at: now + chrono::Duration::minutes(5),
        };
        clamp_advertisement(&binding, &mut caps).unwrap();
        assert_eq!(caps.handlers, vec!["scan".to_string()]);
        assert!(caps.labels.is_empty(), "a phone cannot claim residency");
        assert_eq!(caps.expires_at, binding.expires_at);
        caps.kind = RuntimeKind::Browser;
        assert!(clamp_advertisement(&binding, &mut caps).is_err());
    }

    #[test]
    fn device_sessions_reach_only_their_routes() {
        let post = Method::POST;
        let get = Method::GET;
        let id = uuid::Uuid::now_v7();
        for (method, allowed) in [
            (&post, "/api/v1/mobile/sync".to_owned()),
            (&post, "/api/v1/mobile/devices/register".to_owned()),
            (&post, "/api/v1/mobile/devices/phone-1/runtime".to_owned()),
            (&post, "/api/v1/workers/tasks/poll".to_owned()),
            (&post, format!("/api/v1/workers/tasks/{id}/complete")),
            (&post, format!("/api/v1/workers/tasks/{id}/fail")),
            (&post, format!("/api/v1/workers/tasks/{id}/heartbeat")),
            (&post, format!("/api/v1/workers/tasks/{id}/release")),
            (&post, "/api/v1/continuity/executions".to_owned()),
            (&post, "/api/v1/continuity/grants".to_owned()),
            (&post, "/api/v1/continuity/delegations/claim".to_owned()),
            (&get, format!("/api/v1/continuity/delegations/{id}")),
            (&get, "/api/v1/runtimes".to_owned()),
        ] {
            assert!(route_allowed(method, &allowed), "{method} {allowed}");
        }
        for (method, denied) in [
            (&get, "/api/v1/workers/tasks".to_owned()),
            (&get, "/api/v1/workers/tasks/stats".to_owned()),
            (&post, "/api/v1/workers/tasks/poll/queue".to_owned()),
            (&post, format!("/api/v1/workers/tasks/{id}/artifacts/{id}")),
            (&post, "/api/v1/workers/commands".to_owned()),
            (&post, "/api/v1/runtimes/browser-sessions".to_owned()),
            (&post, "/api/v1/runtimes/device-sessions".to_owned()),
            (&post, "/api/v1/runtimes/register".to_owned()),
            (&post, "/api/v1/sequences".to_owned()),
            (&get, format!("/api/v1/sequences/{id}")),
            (&post, "/api/v1/credentials".to_owned()),
            (&post, "/api/v1/instances".to_owned()),
            (&get, "/api/v1/mobile/devices".to_owned()),
            (&get, "/api/v1/mobile/approvals".to_owned()),
            (&post, format!("/api/v1/mobile/approvals/{id}/resolve")),
            (&post, "/api/v1/mobile/commands".to_owned()),
            (&get, "/api/v1/mobile/status".to_owned()),
            (&post, "/api/v1/continuity/grants/consume".to_owned()),
            (&post, "/api/v1/continuity/handoffs".to_owned()),
            (&get, format!("/api/v1/continuity/executions/{id}")),
            (&get, "/api/v1/continuity/delegations/not-a-uuid".to_owned()),
            (&post, format!("/api/v1/continuity/delegations/{id}")),
            (&post, "/api/v1/api-keys".to_owned()),
            // Sub-tenant, embed, federation, and migration surfaces.
            (&get, "/api/v1/sub-tenants/acme/limits".to_owned()),
            (&get, "/api/v1/usage/sub-tenants".to_owned()),
            (&post, "/api/v1/embed/tokens".to_owned()),
            (&post, "/api/v1/embed/runs".to_owned()),
            (&post, "/api/v1/federation/peers".to_owned()),
            (&post, "/api/v1/federation/inbound".to_owned()),
            (&post, "/api/v1/continuity/federation/sign".to_owned()),
            (&post, "/api/v1/migrations/import".to_owned()),
            (&get, "/api/v1/receipts/export".to_owned()),
            (
                &Method::DELETE,
                format!("/api/v1/workers/tasks/{id}/complete"),
            ),
        ] {
            assert!(!route_allowed(method, &denied), "{method} {denied}");
        }
    }
}
