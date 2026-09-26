//! Public progress links.
//!
//! Authenticated (tenant-scoped, Operator capability):
//! - `POST   /instances/{id}/share` — mint a token (returned once; only its
//!   SHA-256 is stored) with an expiry and optional `context.data` allowlist.
//! - `GET    /instances/{id}/shares` — list shares (never tokens).
//! - `DELETE /instances/{id}/share/{share_id}` — revoke.
//!
//! Public (no auth, rate limited, fail-closed — every failure is a uniform
//! 404):
//! - `GET /public/progress/{token}` — redacted JSON: step labels, statuses,
//!   completed/total, timestamps. **No context data** unless allowlisted.
//! - `GET /public/progress/{token}/embed` — self-contained HTML progress bar
//!   (inline CSS/JS pinned by CSP hashes, no external requests except
//!   polling its own JSON).
//! - `GET /public/progress/embed.js` — loader for
//!   `<script src=".../public/progress/embed.js" data-token="…" async></script>`
//!   that injects the embed iframe.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::approval_link::{hash_token, token_looks_valid};
use orch8_types::execution::NodeState;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::progress_share::{MAX_ALLOWED_FIELDS, ProgressShare, allowed_field_is_valid};
use orch8_types::sequence::BlockDefinition;

use crate::AppState;
use crate::error::ApiError;
use crate::public_http::{check_rate, csp_hash, html_page, json_response, with_csp};

const DEFAULT_TTL_SECS: i64 = 7 * 86_400;
const MAX_TTL_SECS: i64 = 90 * 86_400;
const MIN_TTL_SECS: i64 = 60;
/// Allowlisted values larger than this (serialized) are omitted.
const MAX_FIELD_BYTES: usize = 4_096;
const PUBLIC_RATE_PER_SECOND: u64 = 20;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/instances/{id}/share", post(create_share))
        .route("/instances/{id}/shares", get(list_shares))
        .route("/instances/{id}/share/{share_id}", delete(revoke_share))
}

pub fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/public/progress/embed.js", get(embed_loader))
        .route("/public/progress/{token}", get(public_progress))
        .route("/public/progress/{token}/embed", get(embed_page))
}

#[derive(Debug, Default, Deserialize, ToSchema)]
pub struct CreateShareRequest {
    /// Lifetime in seconds (60..=7776000). Default 7 days.
    #[serde(default)]
    pub expires_in_secs: Option<i64>,
    /// Top-level `context.data` keys to expose publicly. Default: none.
    #[serde(default)]
    pub allowed_fields: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CreateShareResponse {
    pub id: Uuid,
    /// The bearer token. Returned only once; store it securely.
    pub token: String,
    /// Public JSON URL (absolute when the server has `api.public_url`).
    pub url: String,
    /// Embeddable HTML page URL.
    pub embed_url: String,
    /// Paste-in `<script>` snippet.
    pub embed_snippet: String,
    pub expires_at: DateTime<Utc>,
    pub allowed_fields: Vec<String>,
}

async fn load_owned_instance(
    state: &AppState,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<orch8_types::instance::TaskInstance, ApiError> {
    let instance = state
        .storage
        .get_instance(InstanceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(tenant_ctx, &instance.tenant_id, &format!("instance {id}"))?;
    Ok(instance)
}

fn public_base() -> String {
    orch8_engine::handlers::approval_links::public_base_url()
        .unwrap_or("")
        .to_string()
}

#[utoipa::path(post, path = "/instances/{id}/share", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    request_body = CreateShareRequest,
    responses(
        (status = 201, description = "Share created; token returned once", body = CreateShareResponse),
        (status = 400, description = "Invalid expiry or field allowlist"),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn create_share(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    body: Option<Json<CreateShareRequest>>,
) -> Result<impl IntoResponse, ApiError> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let ttl = req.expires_in_secs.unwrap_or(DEFAULT_TTL_SECS);
    if !(MIN_TTL_SECS..=MAX_TTL_SECS).contains(&ttl) {
        return Err(ApiError::InvalidArgument(format!(
            "expires_in_secs must be {MIN_TTL_SECS}..={MAX_TTL_SECS}"
        )));
    }
    if req.allowed_fields.len() > MAX_ALLOWED_FIELDS
        || !req.allowed_fields.iter().all(|f| allowed_field_is_valid(f))
    {
        return Err(ApiError::InvalidArgument(format!(
            "allowed_fields: at most {MAX_ALLOWED_FIELDS} plain top-level keys ([A-Za-z0-9_-], ≤64 chars)"
        )));
    }
    let instance = load_owned_instance(&state, &tenant_ctx, id).await?;
    let bytes: [u8; 32] = rand::random();
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let now = Utc::now();
    let mut fields = req.allowed_fields;
    fields.sort();
    fields.dedup();
    let share = ProgressShare {
        id: Uuid::now_v7(),
        token_hash: hash_token(&token),
        tenant_id: instance.tenant_id.clone(),
        instance_id: instance.id,
        allowed_fields: fields.clone(),
        created_at: now,
        expires_at: now + chrono::Duration::seconds(ttl),
        revoked_at: None,
    };
    state
        .storage
        .create_progress_share(&share)
        .await
        .map_err(|e| ApiError::from_storage(e, "progress share"))?;
    let base = public_base();
    let url = format!("{base}/public/progress/{token}");
    let embed_url = format!("{url}/embed");
    let embed_snippet = format!(
        "<script src=\"{base}/public/progress/embed.js\" data-token=\"{token}\" async></script>"
    );
    Ok((
        StatusCode::CREATED,
        Json(CreateShareResponse {
            id: share.id,
            token,
            url,
            embed_url,
            embed_snippet,
            expires_at: share.expires_at,
            allowed_fields: fields,
        }),
    ))
}

#[utoipa::path(get, path = "/instances/{id}/shares", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID")),
    responses((status = 200, description = "Shares for the instance (tokens are never returned)", body = [ProgressShare]))
)]
pub(crate) async fn list_shares(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance = load_owned_instance(&state, &tenant_ctx, id).await?;
    let shares = state
        .storage
        .list_progress_shares(&instance.tenant_id, instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "progress share"))?;
    Ok(Json(shares))
}

#[utoipa::path(delete, path = "/instances/{id}/share/{share_id}", tag = "instances",
    params(("id" = Uuid, Path, description = "Instance ID"), ("share_id" = Uuid, Path, description = "Share ID")),
    responses(
        (status = 204, description = "Share revoked"),
        (status = 404, description = "Unknown, foreign, or already-revoked share"),
    )
)]
pub(crate) async fn revoke_share(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((id, share_id)): Path<(Uuid, Uuid)>,
) -> Result<impl IntoResponse, ApiError> {
    let instance = load_owned_instance(&state, &tenant_ctx, id).await?;
    let revoked = state
        .storage
        .revoke_progress_share(&instance.tenant_id, instance.id, share_id, Utc::now())
        .await
        .map_err(|e| ApiError::from_storage(e, "progress share"))?;
    if revoked {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("share {share_id}")))
    }
}

// ---------------------------------------------------------------------------
// Public view
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, ToSchema)]
pub struct PublicStep {
    pub label: String,
    /// `pending` | `running` | `waiting` | `completed` | `failed` | `skipped` | `cancelled`
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PublicProgress {
    /// Instance status: `scheduled` | `running` | `waiting` | `paused` | `completed` | `failed` | `cancelled`.
    pub status: String,
    pub completed: usize,
    pub total: usize,
    /// 0..=100.
    pub percent: u8,
    pub steps: Vec<PublicStep>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub link_expires_at: DateTime<Utc>,
    /// Only the share's allowlisted `context.data` keys; absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub data: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Step ids in definition order (DFS), including nested blocks.
fn collect_steps(blocks: &[BlockDefinition], out: &mut Vec<String>) {
    for block in blocks {
        match block {
            BlockDefinition::Step(s) => out.push(s.id.as_str().to_owned()),
            BlockDefinition::Parallel(p) => p.branches.iter().for_each(|b| collect_steps(b, out)),
            BlockDefinition::Race(r) => r.branches.iter().for_each(|b| collect_steps(b, out)),
            BlockDefinition::Loop(l) => collect_steps(&l.body, out),
            BlockDefinition::ForEach(f) => collect_steps(&f.body, out),
            BlockDefinition::Router(r) => {
                r.routes
                    .iter()
                    .for_each(|route| collect_steps(&route.blocks, out));
                if let Some(d) = r.default.as_ref() {
                    collect_steps(d, out);
                }
            }
            BlockDefinition::TryCatch(t) => {
                collect_steps(&t.try_block, out);
                collect_steps(&t.catch_block, out);
                if let Some(f) = t.finally_block.as_ref() {
                    collect_steps(f, out);
                }
            }
            BlockDefinition::SubSequence(_) => {}
            BlockDefinition::ABSplit(ab) => ab
                .variants
                .iter()
                .for_each(|v| collect_steps(&v.blocks, out)),
            BlockDefinition::CancellationScope(cs) => collect_steps(&cs.blocks, out),
            BlockDefinition::Saga(saga) => {
                for step in &saga.steps {
                    collect_steps(std::slice::from_ref(step.action.as_ref()), out);
                }
            }
        }
    }
}

fn humanize(id: &str) -> String {
    let spaced: String = id
        .chars()
        .map(|c| if c == '_' || c == '-' { ' ' } else { c })
        .collect();
    let mut chars = spaced.chars();
    chars.next().map_or_else(String::new, |f| {
        f.to_uppercase().collect::<String>() + chars.as_str()
    })
}

const fn node_status(s: NodeState) -> &'static str {
    match s {
        NodeState::Running => "running",
        NodeState::Waiting => "waiting",
        NodeState::Completed => "completed",
        NodeState::Failed => "failed",
        NodeState::Cancelled => "cancelled",
        NodeState::Skipped => "skipped",
        // `Pending` and any future (non-exhaustive) state.
        _ => "pending",
    }
}

/// Resolve a presented token to a live share + owned instance. Any failure
/// is `None` (uniform 404 to the caller).
async fn resolve_share(
    state: &AppState,
    raw: &str,
) -> Result<Option<(ProgressShare, orch8_types::instance::TaskInstance)>, ApiError> {
    if !token_looks_valid(raw) {
        return Ok(None);
    }
    let Some(share) = state
        .storage
        .get_progress_share_by_hash(&hash_token(raw))
        .await
        .map_err(|e| ApiError::from_storage(e, "progress share"))?
    else {
        return Ok(None);
    };
    if !share.is_live(Utc::now()) {
        return Ok(None);
    }
    let Some(instance) = state
        .storage
        .get_instance(share.instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
    else {
        return Ok(None);
    };
    // Tenant isolation: the share must belong to the instance's tenant.
    if instance.tenant_id != share.tenant_id {
        tracing::error!(share_id = %share.id, "progress share tenant mismatch — refusing");
        return Ok(None);
    }
    Ok(Some((share, instance)))
}

#[allow(clippy::too_many_lines)]
async fn build_progress(
    state: &AppState,
    share: &ProgressShare,
    instance: &orch8_types::instance::TaskInstance,
) -> Result<PublicProgress, ApiError> {
    let sequence = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;
    let mut ids = Vec::new();
    if let Some(seq) = &sequence {
        collect_steps(&seq.blocks, &mut ids);
    }
    let tree = state
        .storage
        .get_execution_tree(instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution tree"))?;
    let terminal_done = instance.state == InstanceState::Completed;
    let steps: Vec<PublicStep> = if tree.is_empty() {
        let completed = state
            .storage
            .get_completed_block_ids(instance.id)
            .await
            .map_err(|e| ApiError::from_storage(e, "outputs"))?;
        let current = instance.context.runtime.current_step.as_ref();
        ids.iter()
            .map(|id| {
                let done = completed.iter().any(|b| b.as_str() == id);
                let status = if done {
                    "completed"
                } else if current.is_some_and(|c| c.as_str() == id) {
                    match instance.state {
                        InstanceState::Waiting => "waiting",
                        InstanceState::Failed => "failed",
                        InstanceState::Cancelled => "cancelled",
                        _ => "running",
                    }
                } else if terminal_done {
                    "skipped"
                } else {
                    "pending"
                };
                PublicStep {
                    label: humanize(id),
                    status,
                    started_at: None,
                    completed_at: None,
                }
            })
            .collect()
    } else {
        ids.iter()
            .map(|id| {
                // Latest node for this block (loops create several).
                let node = tree
                    .iter()
                    .filter(|n| n.block_id.as_str() == id)
                    .max_by_key(|n| n.started_at);
                PublicStep {
                    label: humanize(id),
                    status: node.map_or(if terminal_done { "skipped" } else { "pending" }, |n| {
                        node_status(n.state)
                    }),
                    started_at: node.and_then(|n| n.started_at),
                    completed_at: node.and_then(|n| n.completed_at),
                }
            })
            .collect()
    };
    let total = steps.len();
    let completed = steps
        .iter()
        .filter(|s| matches!(s.status, "completed" | "skipped"))
        .count();
    let percent = if terminal_done {
        100
    } else {
        (completed * 100)
            .checked_div(total)
            .map_or(0, |p| u8::try_from(p).unwrap_or(100))
    };
    let data = if share.allowed_fields.is_empty() {
        None
    } else {
        let mut out = serde_json::Map::new();
        if let Some(obj) = instance.context.data.as_object() {
            for f in &share.allowed_fields {
                if let Some(v) = obj.get(f)
                    && serde_json::to_vec(v).is_ok_and(|b| b.len() <= MAX_FIELD_BYTES)
                {
                    out.insert(f.clone(), v.clone());
                }
            }
        }
        Some(out)
    };
    Ok(PublicProgress {
        status: instance.state.to_string(),
        completed,
        total,
        percent,
        steps,
        created_at: instance.created_at,
        updated_at: instance.updated_at,
        link_expires_at: share.expires_at,
        data,
    })
}

type Peer = Result<ConnectInfo<SocketAddr>, axum::extract::rejection::ExtensionRejection>;

fn not_found_json() -> Response {
    json_response(
        StatusCode::NOT_FOUND,
        &serde_json::json!({"error": "not found"}),
    )
}

#[utoipa::path(get, path = "/public/progress/{token}", tag = "public",
    params(("token" = String, Path, description = "Share token")),
    responses(
        (status = 200, description = "Redacted progress", body = PublicProgress),
        (status = 404, description = "Unknown, expired, or revoked link"),
        (status = 429, description = "Rate limited"),
    )
)]
pub(crate) async fn public_progress(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    peer: Peer,
) -> Response {
    if let Err(e) = check_rate(
        "public-progress",
        peer.ok().map(|ConnectInfo(a)| a),
        PUBLIC_RATE_PER_SECOND,
    ) {
        return e.into_response();
    }
    let resolved = match resolve_share(&state, &raw).await {
        Ok(Some(r)) => r,
        Ok(None) => return not_found_json(),
        Err(e) => {
            tracing::warn!(error = %e, "public progress lookup failed");
            return not_found_json();
        }
    };
    match build_progress(&state, &resolved.0, &resolved.1).await {
        Ok(p) => json_response(
            StatusCode::OK,
            &serde_json::to_value(&p).unwrap_or_default(),
        ),
        Err(e) => {
            tracing::warn!(error = %e, "public progress build failed");
            not_found_json()
        }
    }
}

const EMBED_CSS: &str = "html,body{margin:0;background:transparent;font:14px system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#111827}.w{padding:8px 4px}.t{display:flex;justify-content:space-between;margin-bottom:6px}.b{height:10px;background:#e5e7eb;border-radius:6px;overflow:hidden}.f{height:100%;width:0;background:#2563eb;transition:width .4s}.f.done{background:#16a34a}.f.bad{background:#dc2626}.s{color:#6b7280;font-size:12px;margin-top:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}";

/// Static (token-free) script: derives the JSON URL from its own path and
/// renders with `textContent` only.
const EMBED_JS: &str = "(function(){var u=location.pathname.replace(/\\/embed$/,'');var f=document.getElementById('f'),p=document.getElementById('p'),l=document.getElementById('l'),s=document.getElementById('s');var T={completed:1,failed:1,cancelled:1};function r(d){var pc=Math.max(0,Math.min(100,d.percent|0));f.style.width=pc+'%';f.className='f'+(d.status==='completed'?' done':(d.status==='failed'||d.status==='cancelled')?' bad':'');p.textContent=pc+'%';l.textContent=d.status==='completed'?'Complete':(d.completed+' of '+d.total+' steps');var cur=null;(d.steps||[]).forEach(function(x){if(!cur&&(x.status==='running'||x.status==='waiting'))cur=x;});s.textContent=cur?(cur.label+(cur.status==='waiting'?' (waiting)':'')):(T[d.status]?d.status:'');return !T[d.status];}function tick(){fetch(u,{cache:'no-store',credentials:'omit',referrerPolicy:'no-referrer'}).then(function(x){if(!x.ok)throw 0;return x.json();}).then(function(d){if(r(d))setTimeout(tick,5000);}).catch(function(){s.textContent='Progress unavailable';});}tick();})();";

fn embed_html() -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"referrer\" content=\"no-referrer\"><title>Progress</title><style>{EMBED_CSS}</style></head><body><div class=\"w\" role=\"progressbar\" aria-label=\"Workflow progress\"><div class=\"t\"><span id=\"l\">Loading…</span><span id=\"p\"></span></div><div class=\"b\"><div id=\"f\" class=\"f\"></div></div><div id=\"s\" class=\"s\"></div></div><script>{EMBED_JS}</script></body></html>"
    )
}

#[utoipa::path(get, path = "/public/progress/{token}/embed", tag = "public",
    params(("token" = String, Path, description = "Share token")),
    responses(
        (status = 200, description = "Self-contained progress-bar page", content_type = "text/html"),
        (status = 404, description = "Unknown, expired, or revoked link"),
    )
)]
pub(crate) async fn embed_page(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    peer: Peer,
) -> Response {
    if let Err(e) = check_rate(
        "public-progress",
        peer.ok().map(|ConnectInfo(a)| a),
        PUBLIC_RATE_PER_SECOND,
    ) {
        return e.into_response();
    }
    match resolve_share(&state, &raw).await {
        Ok(Some(_)) => {}
        _ => {
            return html_page(
                StatusCode::NOT_FOUND,
                "Not found",
                "<h1>Link not available</h1>",
            );
        }
    }
    // Embeddable by design (frame-ancestors *), but scripts/styles are pinned
    // to the exact inline bodies and the only network access is same-origin.
    let csp = format!(
        "default-src 'none'; style-src {}; script-src {}; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors *",
        csp_hash(EMBED_CSS),
        csp_hash(EMBED_JS)
    );
    with_csp(
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            embed_html(),
        )
            .into_response(),
        &csp,
    )
}

/// Loader for the paste-in snippet. Reads `data-token` from its own tag and
/// inserts a sandboxed, no-referrer iframe pointing at the embed page.
const EMBED_LOADER_JS: &str = "(function(){var s=document.currentScript;if(!s)return;var t=s.getAttribute('data-token')||'';if(!/^[A-Za-z0-9_-]{32,128}$/.test(t))return;var o=new URL(s.src).origin;var f=document.createElement('iframe');f.src=o+'/public/progress/'+t+'/embed';f.title='Workflow progress';f.setAttribute('referrerpolicy','no-referrer');f.setAttribute('sandbox','allow-scripts allow-same-origin');f.setAttribute('loading','lazy');f.style.cssText='width:100%;height:'+(s.getAttribute('data-height')||'72')+'px;border:0;background:transparent';s.parentNode.insertBefore(f,s.nextSibling);})();";

#[utoipa::path(get, path = "/public/progress/embed.js", tag = "public",
    responses((status = 200, description = "Embed loader script", content_type = "application/javascript"))
)]
pub(crate) async fn embed_loader() -> Response {
    with_csp(
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            )],
            EMBED_LOADER_JS,
        )
            .into_response(),
        "default-src 'none'",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humanize_ids() {
        assert_eq!(humanize("send_receipt"), "Send receipt");
        assert_eq!(humanize("a-b"), "A b");
        assert_eq!(humanize(""), "");
    }

    #[test]
    fn embed_page_is_self_contained() {
        let html = embed_html();
        assert!(
            !html.contains("http://") && !html.contains("https://"),
            "no external refs"
        );
        assert!(
            !EMBED_JS.contains("innerHTML"),
            "render via textContent only"
        );
    }
}
