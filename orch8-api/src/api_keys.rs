//! Per-tenant API key management.
//!
//! These endpoints mint, list, and revoke the keys consumed by
//! [`crate::auth::api_key_middleware`]. They are **admin-only**: every handler
//! requires the request to have authenticated with the unscoped root key (the
//! [`AdminContext`](crate::auth::AdminContext) marker). A per-tenant key can
//! never mint or list other keys — that would let a tenant escalate.
//!
//! The plaintext secret is returned **once**, in the create response. It is
//! never stored (only its SHA-256 hash is) and is unrecoverable afterwards.

use axum::Router;
use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, post};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::ids::TenantId;

use crate::AppState;
use crate::auth::OptionalAdmin;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api-keys", post(create_api_key).get(list_api_keys))
        .route("/api-keys/{id}", delete(revoke_api_key))
}

/// Require that the caller authenticated with the root/admin key.
pub(crate) fn require_admin(admin: &OptionalAdmin) -> Result<(), ApiError> {
    if admin.is_some() {
        Ok(())
    } else {
        // A per-tenant key (or no auth context) may not manage keys.
        Err(ApiError::Forbidden(
            "API key management requires the root API key".into(),
        ))
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateApiKeyRequest {
    /// Tenant the new key authenticates as.
    pub tenant_id: String,
    /// Human-readable label.
    #[serde(default)]
    pub name: String,
    /// Optional expiry. Omit for a non-expiring key.
    #[serde(default)]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Create response — the only place the plaintext `secret` is ever returned.
#[derive(Debug, Serialize, ToSchema)]
pub struct CreatedApiKey {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    /// One-time plaintext secret. Store it now — it is unrecoverable.
    pub secret: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// List/metadata view — never includes the secret or its hash.
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiKeyInfo {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub revoked: bool,
}

impl From<orch8_types::api_key::ApiKeyRecord> for ApiKeyInfo {
    fn from(r: orch8_types::api_key::ApiKeyRecord) -> Self {
        Self {
            id: r.id,
            tenant_id: r.tenant_id,
            name: r.name,
            created_at: r.created_at,
            last_used_at: r.last_used_at,
            expires_at: r.expires_at,
            revoked: r.revoked,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListApiKeysQuery {
    pub tenant_id: String,
}

async fn create_api_key(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Json(body): Json<CreateApiKeyRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    if body.tenant_id.trim().is_empty() {
        return Err(ApiError::InvalidArgument("tenant_id is required".into()));
    }

    let minted =
        orch8_types::api_key::mint(body.tenant_id.clone(), body.name.clone(), body.expires_at);
    state
        .storage
        .create_api_key(&minted.record)
        .await
        .map_err(|e| ApiError::from_storage(e, "api_key"))?;

    Ok((
        StatusCode::CREATED,
        Json(CreatedApiKey {
            id: minted.record.id,
            tenant_id: minted.record.tenant_id,
            name: minted.record.name,
            secret: minted.secret,
            created_at: minted.record.created_at,
            expires_at: minted.record.expires_at,
        }),
    ))
}

async fn list_api_keys(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Query(q): Query<ListApiKeysQuery>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    if q.tenant_id.trim().is_empty() {
        return Err(ApiError::InvalidArgument(
            "tenant_id query parameter is required".into(),
        ));
    }
    let keys = state
        .storage
        .list_api_keys(&TenantId::unchecked(q.tenant_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "api_key"))?;
    let infos: Vec<ApiKeyInfo> = keys.into_iter().map(ApiKeyInfo::from).collect();
    Ok(Json(infos))
}

async fn revoke_api_key(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    let revoked = state
        .storage
        .revoke_api_key(&id)
        .await
        .map_err(|e| ApiError::from_storage(e, "api_key"))?;
    if revoked {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound("api_key".into()))
    }
}
