//! Credentials registry: named, tenant-scoped secrets referenced by step params
//! via the `credentials://<id>` URI scheme.
//!
//! The sequence JSON stored in Postgres/SQLite never contains raw secrets —
//! authors write `"auth": { "access_token": "credentials://stripe-prod" }` and
//! the step handler resolves the reference at dispatch time through the
//! `StorageBackend::get_credential` call. This lets operators rotate secrets
//! without touching any sequence, and keeps a single encrypted blob per secret
//! regardless of how many workflows consume it.
//!
//! `OAuth2` credentials carry a `refresh_url` and `refresh_token`; a background
//! loop in `orch8-engine` refreshes them a few minutes before expiry.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The kind of secret held in a credential.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    /// A single opaque token (Bearer, API key, PAT).
    #[default]
    ApiKey,
    /// `OAuth2` access/refresh token pair with optional expiry.
    Oauth2,
    /// Basic auth username + password.
    Basic,
}

impl fmt::Display for CredentialKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApiKey => f.write_str("api_key"),
            Self::Oauth2 => f.write_str("oauth2"),
            Self::Basic => f.write_str("basic"),
        }
    }
}

impl CredentialKind {
    #[must_use]
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s {
            "api_key" => Some(Self::ApiKey),
            "oauth2" => Some(Self::Oauth2),
            "basic" => Some(Self::Basic),
            _ => None,
        }
    }
}

/// A persisted credential — the secret value is redacted in Debug/Serialize
/// output (via [`crate::config::SecretString`]) so it cannot leak through API
/// responses or logs.
///
/// The `value` field's shape depends on `kind`:
/// - `api_key`: `{"token": "..."}`
/// - `oauth2`: `{"access_token": "...", "refresh_token": "..."}` (`refresh_token`
///   is duplicated in the top-level [`Self::refresh_token`] column for indexed
///   lookup by the refresh loop)
/// - `basic`:  `{"username": "...", "password": "..."}`
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CredentialDef {
    /// Unique credential id (used in `credentials://<id>` references).
    pub id: String,
    /// Tenant that owns this credential (empty = global).
    #[serde(default)]
    pub tenant_id: String,
    /// Human-readable display name.
    pub name: String,
    /// What kind of secret this is.
    #[serde(default)]
    pub kind: CredentialKind,
    /// The secret material as a JSON-encoded string. Redacted in Debug/Serialize.
    /// Use `.value.expose()` to obtain the raw value; pass to `serde_json::from_str`
    /// to parse into a typed struct.
    #[schema(value_type = String)]
    pub value: crate::config::SecretString,
    /// When the current `value` becomes unusable. Used by the refresh loop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// `OAuth2` token endpoint for refresh. None for static credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,
    /// `OAuth2` refresh token. Redacted in Debug/Serialize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub refresh_token: Option<crate::config::SecretString>,
    /// Whether this credential can be resolved. Disabled credentials cause
    /// `credentials://<id>` refs to fail-fast with a permanent error.
    #[serde(default = "crate::serde_defaults::yes")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_kind_display() {
        assert_eq!(CredentialKind::ApiKey.to_string(), "api_key");
        assert_eq!(CredentialKind::Oauth2.to_string(), "oauth2");
        assert_eq!(CredentialKind::Basic.to_string(), "basic");
    }

    #[test]
    fn credential_kind_default_is_api_key() {
        assert_eq!(CredentialKind::default(), CredentialKind::ApiKey);
    }

    #[test]
    fn credential_kind_from_str_loose() {
        assert_eq!(
            CredentialKind::from_str_loose("api_key"),
            Some(CredentialKind::ApiKey)
        );
        assert_eq!(
            CredentialKind::from_str_loose("oauth2"),
            Some(CredentialKind::Oauth2)
        );
        assert_eq!(
            CredentialKind::from_str_loose("basic"),
            Some(CredentialKind::Basic)
        );
        assert_eq!(CredentialKind::from_str_loose("unknown"), None);
        assert_eq!(CredentialKind::from_str_loose(""), None);
    }

    #[test]
    fn credential_kind_serde_round_trip() {
        let oauth: CredentialKind = serde_json::from_str(r#""oauth2""#).unwrap();
        assert_eq!(oauth, CredentialKind::Oauth2);
        assert_eq!(serde_json::to_string(&oauth).unwrap(), r#""oauth2""#);
    }

    #[test]
    fn credential_def_redacts_secret_on_serialize() {
        let now = Utc::now();
        let def = CredentialDef {
            id: "stripe-prod".into(),
            tenant_id: "t1".into(),
            name: "Stripe Production".into(),
            kind: CredentialKind::ApiKey,
            value: crate::config::SecretString::new(r#"{"token":"sk_live_xxx"}"#.into()),
            expires_at: None,
            refresh_url: None,
            refresh_token: None,
            enabled: true,
            description: None,
            created_at: now,
            updated_at: now,
        };
        assert_eq!(def.value.expose(), r#"{"token":"sk_live_xxx"}"#);
        let json = serde_json::to_string(&def).unwrap();
        // Secret is redacted — sk_live_xxx MUST NOT appear in serialized output.
        assert!(!json.contains("sk_live_xxx"));
        assert!(json.contains("[REDACTED]"));
    }

    #[test]
    fn credential_def_defaults() {
        let json = r#"{
            "id": "github-bot",
            "name": "GitHub Bot",
            "value": "{\"token\":\"ghp_xxx\"}",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let def: CredentialDef = serde_json::from_str(json).unwrap();
        assert_eq!(def.tenant_id, "");
        assert_eq!(def.kind, CredentialKind::ApiKey);
        assert!(def.enabled);
        assert!(def.expires_at.is_none());
    }

    #[test]
    fn credential_def_oauth2_round_trip() {
        let now = Utc::now();
        let def = CredentialDef {
            id: "google".into(),
            tenant_id: "t1".into(),
            name: "Google OAuth".into(),
            kind: CredentialKind::Oauth2,
            value: crate::config::SecretString::new(
                r#"{"access_token":"a","refresh_token":"r"}"#.into(),
            ),
            expires_at: Some(now),
            refresh_url: Some("https://oauth2.googleapis.com/token".into()),
            refresh_token: Some(crate::config::SecretString::new("r".into())),
            enabled: true,
            description: Some("shared workspace access".into()),
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&def).unwrap();
        // Real refresh token must be redacted.
        assert!(!json.contains("\"r\""));
        let back: CredentialDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "google");
        assert_eq!(back.kind, CredentialKind::Oauth2);
        assert_eq!(
            back.refresh_url.as_deref(),
            Some("https://oauth2.googleapis.com/token")
        );
    }
}
