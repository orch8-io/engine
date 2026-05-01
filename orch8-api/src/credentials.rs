//! Credentials registry: CRUD management for shared secrets.
//!
//! Secrets are referenced from step params via `credentials://<id>`.
//! Responses use [`CredentialResponse`] which completely strips secret
//! material (`value`, `refresh_token`) — credential values never leave the
//! server. They are only resolved internally by step handlers (LLM, HTTP,
//! etc.) via the `credentials://` scheme.

use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::config::SecretString;
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::ids::TenantId;

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/credentials",
            get(list_credentials).post(create_credential),
        )
        .route(
            "/credentials/{id}",
            get(get_credential)
                .delete(delete_credential)
                .patch(update_credential),
        )
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateCredentialRequest {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub kind: CredentialKind,
    /// Raw secret value (typically JSON) — will be stored and redacted on read.
    pub value: String,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateCredentialRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<CredentialKind>,
    /// When set, replaces the stored secret. Write-only — never returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// API response for credentials — secret material is completely stripped.
///
/// Credential values never leave the server; they are only resolved
/// internally by step handlers (LLM, HTTP, etc.) via `credentials://<id>`.
#[derive(Debug, Serialize, ToSchema)]
pub struct CredentialResponse {
    pub id: String,
    #[serde(default)]
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub kind: CredentialKind,
    /// Whether this credential can be resolved by step handlers.
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,
    /// Whether a refresh token is configured (true/false, never the value).
    pub has_refresh_token: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<CredentialDef> for CredentialResponse {
    fn from(c: CredentialDef) -> Self {
        Self {
            id: c.id,
            tenant_id: c.tenant_id,
            name: c.name,
            kind: c.kind,
            enabled: c.enabled,
            expires_at: c.expires_at,
            refresh_url: c.refresh_url,
            has_refresh_token: c.refresh_token.is_some(),
            description: c.description,
            created_at: c.created_at,
            updated_at: c.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CredentialQuery {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    100
}

async fn create_credential(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateCredentialRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if body.id.is_empty() || body.name.is_empty() || body.value.is_empty() {
        return Err(ApiError::InvalidArgument(
            "id, name, and value are required".into(),
        ));
    }
    if body.id.len() > 255 {
        return Err(ApiError::InvalidArgument(
            "id must not exceed 255 characters".into(),
        ));
    }
    // Ids flow straight into the `credentials://<id>` URI scheme; keep them
    // URL-safe so parsing never needs percent-decoding.
    if !body
        .id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(ApiError::InvalidArgument(
            "id may only contain alphanumerics, '-', '_', '.'".into(),
        ));
    }
    if matches!(body.kind, CredentialKind::Oauth2)
        && body.refresh_url.is_some()
        && body.refresh_token.is_none()
    {
        return Err(ApiError::InvalidArgument(
            "oauth2 credential with refresh_url requires refresh_token".into(),
        ));
    }

    let tenant_id =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId(body.tenant_id.clone()))?;

    let now = chrono::Utc::now();
    let credential = CredentialDef {
        id: body.id,
        tenant_id: tenant_id.0,
        name: body.name,
        kind: body.kind,
        value: SecretString::new(body.value),
        expires_at: body.expires_at,
        refresh_url: body.refresh_url,
        refresh_token: body.refresh_token.map(SecretString::new),
        enabled: true,
        description: body.description,
        created_at: now,
        updated_at: now,
    };

    state
        .storage
        .create_credential(&credential)
        .await
        .map_err(|e| ApiError::from_storage(e, "credential"))?;

    Ok((
        StatusCode::CREATED,
        Json(CredentialResponse::from(credential)),
    ))
}

async fn list_credentials(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<CredentialQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_ref = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref());
    let credentials = state
        .storage
        .list_credentials(tenant_ref.as_ref(), query.limit)
        .await
        .map_err(|e| ApiError::from_storage(e, "credential"))?;
    let response: Vec<CredentialResponse> = credentials.into_iter().map(Into::into).collect();
    Ok(Json(response))
}

async fn get_credential(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let credential = state
        .storage
        .get_credential(&id)
        .await
        .map_err(|e| ApiError::from_storage(e, "credential"))?
        .ok_or_else(|| ApiError::NotFound(format!("credential '{id}'")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId(credential.tenant_id.clone()),
        &format!("credential '{id}'"),
    )?;
    Ok(Json(CredentialResponse::from(credential)))
}

async fn update_credential(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<String>,
    Json(body): Json<UpdateCredentialRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut credential = state
        .storage
        .get_credential(&id)
        .await
        .map_err(|e| ApiError::from_storage(e, "credential"))?
        .ok_or_else(|| ApiError::NotFound(format!("credential '{id}'")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId(credential.tenant_id.clone()),
        &format!("credential '{id}'"),
    )?;

    if let Some(name) = body.name {
        credential.name = name;
    }
    if let Some(kind) = body.kind {
        credential.kind = kind;
    }
    if let Some(value) = body.value {
        credential.value = SecretString::new(value);
    }
    if let Some(expires_at) = body.expires_at {
        credential.expires_at = Some(expires_at);
    }
    if let Some(refresh_url) = body.refresh_url {
        credential.refresh_url = Some(refresh_url);
    }
    if let Some(refresh_token) = body.refresh_token {
        credential.refresh_token = Some(SecretString::new(refresh_token));
    }
    if let Some(enabled) = body.enabled {
        credential.enabled = enabled;
    }
    if let Some(description) = body.description {
        credential.description = Some(description);
    }
    credential.updated_at = chrono::Utc::now();

    state
        .storage
        .update_credential(&credential)
        .await
        .map_err(|e| ApiError::from_storage(e, "credential"))?;

    Ok(Json(CredentialResponse::from(credential)))
}

async fn delete_credential(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let credential = state
        .storage
        .get_credential(&id)
        .await
        .map_err(|e| ApiError::from_storage(e, "credential"))?
        .ok_or_else(|| ApiError::NotFound(format!("credential '{id}'")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &TenantId(credential.tenant_id.clone()),
        &format!("credential '{id}'"),
    )?;

    state
        .storage
        .delete_credential(&id)
        .await
        .map_err(|e| ApiError::from_storage(e, "credential"))?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_types::config::SecretString;
    use orch8_types::credential::{CredentialDef, CredentialKind};

    fn mk_credential() -> CredentialDef {
        CredentialDef {
            id: "cred_1".into(),
            tenant_id: "tenant_a".into(),
            name: "API Key".into(),
            kind: CredentialKind::ApiKey,
            value: SecretString::new("secret123".into()),
            expires_at: None,
            refresh_url: None,
            refresh_token: Some(SecretString::new("refresh456".into())),
            enabled: true,
            description: Some("test cred".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn credential_response_strips_secret_value() {
        let cred = mk_credential();
        let resp: CredentialResponse = cred.into();
        // Secret value must never appear in the response.
        // The struct has no `value` field, so compilation is the main guard.
        assert_eq!(resp.id, "cred_1");
        assert_eq!(resp.name, "API Key");
        assert_eq!(resp.tenant_id, "tenant_a");
    }

    #[test]
    fn credential_response_has_refresh_token_flag() {
        let mut cred = mk_credential();
        let resp: CredentialResponse = cred.clone().into();
        assert!(resp.has_refresh_token);

        cred.refresh_token = None;
        let resp2: CredentialResponse = cred.into();
        assert!(!resp2.has_refresh_token);
    }

    #[test]
    fn credential_response_preserves_optional_fields() {
        let mut cred = mk_credential();
        cred.expires_at = Some(Utc::now());
        cred.refresh_url = Some("https://example.com/refresh".into());
        let resp: CredentialResponse = cred.into();
        assert!(resp.expires_at.is_some());
        assert_eq!(
            resp.refresh_url.as_deref(),
            Some("https://example.com/refresh")
        );
    }

    #[test]
    fn credential_response_reflects_enabled() {
        let mut cred = mk_credential();
        cred.enabled = false;
        let resp: CredentialResponse = cred.into();
        assert!(!resp.enabled);
    }
}
