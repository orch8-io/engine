//! Signed effect-receipt export: portable at-most-once dispatch evidence.
//!
//! Bundles are JSON Lines signed with the engine's continuity signing key
//! (derived from the master encryption key). See
//! [`orch8_engine::receipt_bundle`] for the format and what it proves.

use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use orch8_engine::receipt_bundle::{BundleScope, build_bundle};
use orch8_types::continuity::EffectReceipt;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, TenantId};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

/// Instances scanned per window export before the bundle is marked truncated.
const WINDOW_INSTANCE_CAP: usize = 10_000;
const PAGE: u32 = 500;
const RECEIPTS_PER_INSTANCE: u32 = 10_000;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/instances/{id}/receipts/export",
            get(export_instance_receipts),
        )
        .route("/receipts/export", get(export_window_receipts))
        .route("/receipts/signing-key", get(signing_key))
}

#[derive(Debug, Deserialize)]
pub struct TenantParam {
    pub tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WindowParams {
    pub tenant_id: Option<String>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SigningKeyResponse {
    /// Stable identifier of the engine's continuity signing key.
    pub signing_key_id: String,
    /// Always `ed25519`.
    pub algorithm: String,
    /// Base64 of the raw 32-byte Ed25519 public key. Pin it when verifying.
    pub public_key: String,
}

fn resolve_tenant(tenant_ctx: &OptionalTenant, query: Option<&str>) -> Result<TenantId, ApiError> {
    if let Some(axum::Extension(ctx)) = tenant_ctx {
        if let Some(q) = query.filter(|q| !q.is_empty())
            && q != ctx.tenant_id.as_str()
        {
            return Err(ApiError::Forbidden(
                "tenant_id does not match X-Tenant-Id header".into(),
            ));
        }
        return Ok(ctx.tenant_id.clone());
    }
    let raw = query
        .filter(|q| !q.is_empty())
        .ok_or_else(|| ApiError::InvalidArgument("tenant_id is required".into()))?;
    TenantId::new(raw).map_err(ApiError::InvalidArgument)
}

fn crypto(state: &AppState) -> Result<&crate::ContinuityCrypto, ApiError> {
    state.continuity_crypto.as_deref().ok_or_else(|| {
        ApiError::Unavailable(
            "receipt signing requires the engine master key (ORCH8_ENCRYPTION_KEY)".into(),
        )
    })
}

fn bundle_response(body: String, filename: &str) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/x-ndjson".to_owned()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        body,
    )
        .into_response()
}

/// Export the effect-receipt ledger of one instance as a signed JSONL bundle.
#[utoipa::path(
    get, path = "/instances/{id}/receipts/export", tag = "receipts",
    params(
        ("id" = String, Path, description = "Instance ID"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers only)"),
    ),
    responses(
        (status = 200, description = "Signed JSONL bundle: header, one line per effect receipt, signature trailer. At-most-once dispatch evidence, not an exactly-once claim.", content_type = "application/x-ndjson", body = String),
        (status = 404, description = "Instance not found"),
        (status = 503, description = "Signing key not configured"),
    )
)]
pub async fn export_instance_receipts(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(id): Path<InstanceId>,
    Query(query): Query<TenantParam>,
) -> Result<Response, ApiError> {
    let crypto = crypto(&state)?;
    let instance = state
        .storage
        .get_instance(id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &instance.tenant_id, "instance")?;
    let tenant_id = match query.tenant_id.as_deref().filter(|t| !t.is_empty()) {
        Some(requested) if requested != instance.tenant_id.as_str() => {
            return Err(ApiError::NotFound("instance".into()));
        }
        _ => instance.tenant_id.clone(),
    };
    let receipts = state
        .storage
        .list_instance_effect_receipts(&tenant_id, id, RECEIPTS_PER_INSTANCE)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipts"))?;
    let body = build_bundle(
        tenant_id.as_str(),
        BundleScope::Instance {
            instance_id: id.to_string(),
        },
        receipts,
        &crypto.signing_key,
        &crypto.signing_key_id,
        Utc::now(),
    )
    .map_err(|error| ApiError::Internal(format!("serialize receipts: {error}")))?;
    Ok(bundle_response(body, &format!("receipts-{id}.jsonl")))
}

/// Export every effect receipt created in `[from, to)` for a tenant.
#[utoipa::path(
    get, path = "/receipts/export", tag = "receipts",
    params(
        ("from" = String, Query, description = "Window start (RFC 3339, inclusive)"),
        ("to" = String, Query, description = "Window end (RFC 3339, exclusive)"),
        ("tenant_id" = Option<String>, Query, description = "Tenant (unscoped callers only)"),
    ),
    responses(
        (status = 200, description = "Signed JSONL bundle for the window. `scope.truncated=true` when more than 10000 instances were touched in the window.", content_type = "application/x-ndjson", body = String),
        (status = 400, description = "Invalid window or missing tenant"),
        (status = 503, description = "Signing key not configured"),
    )
)]
pub async fn export_window_receipts(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Query(query): Query<WindowParams>,
) -> Result<Response, ApiError> {
    let crypto = crypto(&state)?;
    let tenant_id = resolve_tenant(&tenant_ctx, query.tenant_id.as_deref())?;
    if query.from >= query.to {
        return Err(ApiError::InvalidArgument(
            "`from` must be before `to`".into(),
        ));
    }
    let (receipts, truncated) =
        receipts_in_window(&state, &tenant_id, query.from, query.to).await?;
    let body = build_bundle(
        tenant_id.as_str(),
        BundleScope::Window {
            from: query.from,
            to: query.to,
            truncated,
        },
        receipts,
        &crypto.signing_key,
        &crypto.signing_key_id,
        Utc::now(),
    )
    .map_err(|error| ApiError::Internal(format!("serialize receipts: {error}")))?;
    Ok(bundle_response(
        body,
        &format!(
            "receipts-{}-{}.jsonl",
            query.from.format("%Y%m%dT%H%M%SZ"),
            query.to.format("%Y%m%dT%H%M%SZ")
        ),
    ))
}

/// A receipt created at `t` belongs to an instance whose `updated_at >= t`,
/// so scanning instances newest-updated first and stopping below `from` is
/// complete.
async fn receipts_in_window(
    state: &AppState,
    tenant_id: &TenantId,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<(Vec<EffectReceipt>, bool), ApiError> {
    let filter = InstanceFilter {
        tenant_id: Some(tenant_id.clone()),
        ..InstanceFilter::default()
    };
    let mut receipts = Vec::new();
    let mut scanned = 0_usize;
    let mut offset = 0_u64;
    loop {
        let page = state
            .storage
            .list_instances(
                &filter,
                &Pagination {
                    offset,
                    limit: PAGE,
                    sort_ascending: false,
                },
            )
            .await
            .map_err(|error| ApiError::from_storage(error, "instances"))?;
        let n = page.len();
        for instance in page {
            if instance.updated_at < from {
                return Ok((receipts, false));
            }
            if scanned >= WINDOW_INSTANCE_CAP {
                return Ok((receipts, true));
            }
            scanned += 1;
            let ledger = state
                .storage
                .list_instance_effect_receipts(tenant_id, instance.id, RECEIPTS_PER_INSTANCE)
                .await
                .map_err(|error| ApiError::from_storage(error, "effect receipts"))?;
            receipts.extend(
                ledger
                    .into_iter()
                    .filter(|r| r.created_at >= from && r.created_at < to),
            );
        }
        if n < PAGE as usize {
            return Ok((receipts, false));
        }
        offset += u64::from(PAGE);
    }
}

/// The public half of the key that signs receipt bundles.
#[utoipa::path(
    get, path = "/receipts/signing-key", tag = "receipts",
    responses(
        (status = 200, description = "Engine receipt-signing public key", body = SigningKeyResponse),
        (status = 503, description = "Signing key not configured"),
    )
)]
pub async fn signing_key(
    State(state): State<AppState>,
) -> Result<Json<SigningKeyResponse>, ApiError> {
    let crypto = crypto(&state)?;
    Ok(Json(SigningKeyResponse {
        signing_key_id: crypto.signing_key_id.clone(),
        algorithm: "ed25519".into(),
        public_key: base64::engine::general_purpose::STANDARD
            .encode(crypto.signing_key.verifying_key().to_bytes()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_resolution_prefers_header_and_rejects_mismatch() {
        let ctx: OptionalTenant = Some(axum::Extension(crate::auth::TenantContext {
            tenant_id: TenantId::new("acme").unwrap(),
        }));
        assert_eq!(resolve_tenant(&ctx, None).unwrap().as_str(), "acme");
        assert!(resolve_tenant(&ctx, Some("other")).is_err());
        assert!(resolve_tenant(&None, None).is_err());
        assert_eq!(resolve_tenant(&None, Some("t1")).unwrap().as_str(), "t1");
    }
}
