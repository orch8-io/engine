//! Embedded surface: scoped `o8e1` embed tokens and the `/embed/*` routes the
//! embed kit (`@orch8/embed`) calls from an end customer's browser.
//!
//! Authentication model:
//! * `POST /embed/tokens` and `PUT /embed/theme` are tenant-admin routes
//!   (API key, Operator capability).
//! * Every other `/embed/*` route authenticates **only** with
//!   `Authorization: Bearer o8e1…`. The API-key middleware passes such
//!   requests through without granting any tenant/admin context
//!   ([`is_token_route`]); each handler verifies the token itself through the
//!   [`token::EmbedPrincipal`] extractor, which binds the request to exactly
//!   one tenant and one sub-tenant. Nothing an embed token can reach returns
//!   raw instance context, metadata, or unlisted step outputs.
//! * All embed routes answer 404 while no `[embed] token_secret` is set.

pub mod runs;
pub mod sequences;
pub mod token;

use axum::extract::State;
use axum::http::Method;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::Utc;
use serde::Serialize;
use utoipa::ToSchema;

use orch8_types::sub_tenant::EmbedTheme;

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;
use crate::license::{License, SoftEnforcer};

pub use token::{EmbedPrincipal, EmbedScope, EmbedSigner};

/// Process-wide embedded/licensing runtime shared through [`AppState`].
#[derive(Debug, Default)]
pub struct EmbeddedRuntime {
    /// `None` = embedding disabled (all embed routes 404).
    pub signer: Option<EmbedSigner>,
    /// Browser origins allowed to call `/api/v1/embed/*` (CORS).
    pub allowed_origins: Vec<String>,
    pub license: License,
    pub enforcer: SoftEnforcer,
}

impl EmbeddedRuntime {
    /// Build from `[embed]` / `[license]` config (env already merged). An
    /// invalid embed secret is a startup error, never a silent disable.
    pub fn from_config(
        embed: &orch8_types::config::EmbedConfig,
        license: &orch8_types::config::LicenseConfig,
    ) -> Result<Self, String> {
        let secret = embed.token_secret.expose();
        let signer = if secret.trim().is_empty() {
            None
        } else {
            Some(EmbedSigner::from_hex(secret).map_err(|e| format!("[embed] token_secret: {e}"))?)
        };
        Ok(Self {
            signer,
            allowed_origins: parse_origins(&embed.allowed_origins),
            license: License::load(license.key.expose()),
            enforcer: SoftEnforcer::default(),
        })
    }

    /// Runtime with embedding enabled for tests.
    #[must_use]
    pub fn with_signer(signer: EmbedSigner, license: License) -> Self {
        Self {
            signer: Some(signer),
            allowed_origins: Vec::new(),
            license,
            enforcer: SoftEnforcer::default(),
        }
    }
}

/// Split a comma-separated origin list, dropping blanks.
#[must_use]
pub fn parse_origins(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|o| !o.is_empty())
        .map(|o| o.trim_end_matches('/').to_string())
        .collect()
}

/// Routes an `o8e1` bearer may call (method + path, with or without the
/// `/api/v1` prefix). Everything else requires an API key.
#[must_use]
pub fn is_token_route(method: &Method, path: &str) -> bool {
    let path = path.strip_prefix(crate::API_V1_PREFIX).unwrap_or(path);
    let Some(rest) = path.strip_prefix("/embed/") else {
        return false;
    };
    let segments: Vec<&str> = rest.split('/').collect();
    if segments.iter().any(|segment| segment.is_empty()) {
        return false;
    }
    let get = *method == Method::GET;
    let post = *method == Method::POST;
    let put = *method == Method::PUT;
    match segments.as_slice() {
        ["runs"] => get || post,
        ["runs", _] | ["approvals" | "sequences" | "theme"] => get,
        ["approvals", _] => post,
        ["sequences", _] => get || put,
        _ => false,
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/embed/tokens", post(token::issue_token))
        .route("/embed/runs", get(runs::list_runs).post(runs::start_run))
        .route("/embed/runs/{id}", get(runs::get_run))
        .route("/embed/approvals", get(runs::list_approvals))
        .route("/embed/approvals/{id}", post(runs::resolve_approval))
        .route("/embed/sequences", get(sequences::list_sequences))
        .route(
            "/embed/sequences/{name}",
            get(sequences::get_sequence).put(sequences::put_sequence),
        )
        .route("/embed/theme", get(get_theme).put(put_theme))
}

/// Theme as served to embed components.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ThemeResponse {
    pub css_vars: std::collections::BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logo_url: Option<String>,
    /// Effective: `true` only when requested *and* licensed for `white_label`.
    pub hide_badge: bool,
}

fn effective_theme(state: &AppState, theme: EmbedTheme) -> ThemeResponse {
    let white_label = state
        .embedded
        .license
        .has_feature("white_label", Utc::now());
    ThemeResponse {
        css_vars: theme
            .css_vars
            .into_iter()
            .map(|(name, value)| (short_var_name(&name).to_string(), value))
            .collect(),
        logo_url: theme.logo_url,
        hide_badge: theme.hide_badge && white_label,
    }
}

async fn load_theme(
    state: &AppState,
    tenant: &orch8_types::ids::TenantId,
) -> Result<EmbedTheme, ApiError> {
    Ok(state
        .storage
        .get_embed_theme(tenant)
        .await
        .map_err(|e| ApiError::from_storage(e, "embed_theme"))?
        .unwrap_or_default())
}

#[utoipa::path(get, path = "/embed/theme", tag = "embed", operation_id = "embed_get_theme",
    responses(
        (status = 200, description = "Tenant embed theme (embed token or tenant API key)", body = ThemeResponse),
        (status = 401, description = "Missing/invalid embed token"),
        (status = 404, description = "Embedding is disabled"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn get_theme(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    tenant_ctx: OptionalTenant,
    bearer: Option<Extension<crate::auth::EmbedBearer>>,
) -> Result<impl IntoResponse, ApiError> {
    let Some(signer) = state.embedded.signer.as_ref() else {
        return Err(ApiError::NotFound("embed routes are disabled".into()));
    };
    // An embed token binds the tenant (the API-key middleware let it through
    // context-free); otherwise the API-key middleware authenticated the
    // caller and bound its tenant.
    let tenant = if bearer.is_some() {
        let token = token::bearer_token(&headers).ok_or(ApiError::Unauthorized)?;
        signer
            .verify(token, Utc::now())
            .ok_or(ApiError::Unauthorized)?
            .tenant_id
    } else {
        crate::sub_tenants::require_tenant(&tenant_ctx)?
    };
    let theme = load_theme(&state, &tenant).await?;
    Ok(Json(effective_theme(&state, theme)))
}

const MAX_CSS_VARS: usize = 128;
const MAX_CSS_VALUE_BYTES: usize = 256;
const MAX_LOGO_URL_BYTES: usize = 2_048;

const CSS_VAR_PREFIX: &str = "--orch8-";

/// `css_vars` keys are accepted with or without the `--orch8-` prefix and
/// always served without it (the embed kit adds the prefix).
fn short_var_name(name: &str) -> &str {
    name.strip_prefix(CSS_VAR_PREFIX).unwrap_or(name)
}

/// Theme values end up inside a shadow-DOM stylesheet: names must be plain
/// identifiers (becoming `--orch8-<name>`) and values may not break out of a
/// declaration. Returns the theme with normalised (prefix-less) names.
fn normalize_theme(theme: EmbedTheme) -> Result<EmbedTheme, ApiError> {
    if theme.css_vars.len() > MAX_CSS_VARS {
        return Err(ApiError::InvalidArgument(format!(
            "css_vars may define at most {MAX_CSS_VARS} properties"
        )));
    }
    let mut css_vars = std::collections::BTreeMap::new();
    for (raw_name, value) in &theme.css_vars {
        let name = short_var_name(raw_name);
        let ident_ok = !name.is_empty()
            && name.len() <= 64
            && !name.starts_with('-')
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if !ident_ok {
            return Err(ApiError::InvalidArgument(format!(
                "css_vars: {raw_name:?} must be a custom property name ([A-Za-z0-9_-], optionally prefixed --orch8-)"
            )));
        }
        let name = name.to_string();
        if value.len() > MAX_CSS_VALUE_BYTES
            || value.chars().any(|c| {
                matches!(c, ';' | '{' | '}' | '<' | '>' | '\\' | '"' | '\'' | '`') || c.is_control()
            })
            || value.to_ascii_lowercase().contains("url(")
            || value.to_ascii_lowercase().contains("expression(")
        {
            return Err(ApiError::InvalidArgument(format!(
                "css_vars: value of {name} is not an allowed CSS value"
            )));
        }
        if css_vars.insert(name.clone(), value.clone()).is_some() {
            return Err(ApiError::InvalidArgument(format!(
                "css_vars: {name} is defined twice (with and without the prefix)"
            )));
        }
    }
    if let Some(url) = &theme.logo_url {
        let parsed = url::Url::parse(url)
            .map_err(|_| ApiError::InvalidArgument("logo_url must be an absolute URL".into()))?;
        if url.len() > MAX_LOGO_URL_BYTES || parsed.scheme() != "https" {
            return Err(ApiError::InvalidArgument(
                "logo_url must be an https URL of at most 2048 bytes".into(),
            ));
        }
    }
    Ok(EmbedTheme { css_vars, ..theme })
}

#[utoipa::path(put, path = "/embed/theme", tag = "embed", operation_id = "embed_put_theme",
    request_body = EmbedTheme,
    responses(
        (status = 200, description = "Stored; `hide_badge` reports the effective (license-gated) value",
            body = ThemeResponse),
        (status = 400, description = "Invalid theme"),
        (status = 404, description = "Embedding is disabled"),
    )
)]
pub(crate) async fn put_theme(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    principal: Option<Extension<crate::auth::PrincipalContext>>,
    Json(theme): Json<EmbedTheme>,
) -> Result<impl IntoResponse, ApiError> {
    if state.embedded.signer.is_none() {
        return Err(ApiError::NotFound("embed routes are disabled".into()));
    }
    if !crate::auth::principal_is_operator(principal.as_ref().map(|Extension(p)| p)) {
        return Err(ApiError::Forbidden(
            "updating the embed theme requires the operator capability".into(),
        ));
    }
    let tenant = crate::sub_tenants::require_tenant(&tenant_ctx)?;
    let theme = normalize_theme(theme)?;
    state
        .storage
        .put_embed_theme(&tenant, &theme)
        .await
        .map_err(|e| ApiError::from_storage(e, "embed_theme"))?;
    Ok(Json(effective_theme(&state, theme)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_routes_are_an_explicit_allowlist() {
        for (m, p) in [
            (Method::GET, "/api/v1/embed/runs"),
            (Method::GET, "/api/v1/embed/runs/abc"),
            (Method::POST, "/api/v1/embed/runs"),
            (Method::GET, "/api/v1/embed/approvals"),
            (Method::POST, "/api/v1/embed/approvals/xyz"),
            (Method::GET, "/api/v1/embed/sequences"),
            (Method::GET, "/api/v1/embed/sequences/onboarding"),
            (Method::PUT, "/api/v1/embed/sequences/onboarding"),
            (Method::GET, "/api/v1/embed/theme"),
            (Method::GET, "/embed/runs"),
        ] {
            assert!(is_token_route(&m, p), "{m} {p}");
        }
        for (m, p) in [
            (Method::POST, "/api/v1/embed/tokens"),
            (Method::PUT, "/api/v1/embed/theme"),
            (Method::DELETE, "/api/v1/embed/runs/abc"),
            (Method::GET, "/api/v1/embed/runs/abc/extra"),
            (Method::GET, "/api/v1/embed/runs/"),
            (Method::GET, "/api/v1/instances"),
            (Method::GET, "/api/v1/embedded/runs"),
            (Method::POST, "/api/v1/embed/sequences/x"),
        ] {
            assert!(!is_token_route(&m, p), "{m} {p}");
        }
    }

    #[test]
    fn theme_validation_blocks_css_injection() {
        let mut theme = EmbedTheme::default();
        theme
            .css_vars
            .insert("--orch8-accent".into(), "#0af".into());
        theme.css_vars.insert("radius".into(), "4px".into());
        theme.logo_url = Some("https://cdn.example.com/logo.svg".into());
        let normalized = normalize_theme(theme).unwrap();
        assert_eq!(
            normalized.css_vars.keys().collect::<Vec<_>>(),
            vec!["accent", "radius"]
        );

        let mut dup = EmbedTheme::default();
        dup.css_vars.insert("--orch8-accent".into(), "red".into());
        dup.css_vars.insert("accent".into(), "blue".into());
        assert!(normalize_theme(dup).is_err());

        for (name, value) in [
            ("--color", "red"),
            ("--orch8-", "red"),
            ("a b", "red"),
            ("--orch8-x", "red; } body { display:none"),
            ("--orch8-x", "url(https://evil)"),
            ("--orch8-x", "</style><script>"),
        ] {
            let mut bad = EmbedTheme::default();
            bad.css_vars.insert(name.into(), value.into());
            assert!(normalize_theme(bad).is_err(), "{name}: {value}");
        }
        let bad = EmbedTheme {
            logo_url: Some("javascript:alert(1)".into()),
            ..EmbedTheme::default()
        };
        assert!(normalize_theme(bad).is_err());
        let bad = EmbedTheme {
            logo_url: Some("http://insecure.example.com/logo.png".into()),
            ..EmbedTheme::default()
        };
        assert!(normalize_theme(bad).is_err());
    }
}
