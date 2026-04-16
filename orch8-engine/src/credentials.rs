//! Credentials resolver + `OAuth2` refresh loop.
//!
//! # Resolving `credentials://` references
//!
//! Step params may contain `credentials://<id>` strings anywhere in the JSON
//! tree. [`resolve_in_value`] walks the tree and replaces every such string
//! with the corresponding credential's value. Credential values are stored as
//! JSON text: valid JSON (objects, strings, numbers) is parsed and inlined;
//! invalid JSON is inlined as a raw string. This lets the same credential
//! back either "the whole `auth` object" or "one `access_token` field":
//!
//! ```json
//! // credential "stripe-prod" holds: {"access_token": "sk_live_xxx"}
//!
//! // Whole-object reference:
//! "auth": "credentials://stripe-prod"
//! // → auth becomes {"access_token": "sk_live_xxx"}
//!
//! // Field-level reference:
//! "auth": {"access_token": "credentials://stripe-prod/access_token"}
//! // → auth becomes {"access_token": "sk_live_xxx"}
//! ```
//!
//! Missing, disabled, or malformed credentials produce
//! [`StepError::Permanent`] — the step fails fast on the first reference so
//! retries never silently dispatch with a null secret.
//!
//! # `OAuth2` refresh loop
//!
//! [`run_refresh_loop`] polls every 60s for `kind=oauth2` credentials whose
//! `expires_at` is within 5 minutes of now, and POSTs to `refresh_url` with
//! `{grant_type: "refresh_token", refresh_token: <token>}`. On success the
//! credential is updated in place; on failure a warning is logged and the
//! credential is left alone — steps referencing it will still attempt to use
//! the stale token until it actually expires.

use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::config::SecretString;
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::error::StepError;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const SCHEME: &str = "credentials://";

/// Subset of the RFC 6749 token response we care about on refresh.
#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
}

/// Shared HTTP client for `OAuth2` refresh calls.
static REFRESH_CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_else(|e| {
            warn!(error = %e, "failed to build credentials refresh client, using default");
            reqwest::Client::new()
        })
});

/// Recursively walk `value` and replace every `credentials://<id>[/<key>]`
/// string with the resolved credential material.
///
/// Tenant scoping: if `tenant_id` is non-empty, only credentials owned by
/// that tenant or by the global scope (empty `tenant_id`) are considered.
pub async fn resolve_in_value(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    value: &mut Value,
) -> Result<(), StepError> {
    match value {
        Value::String(s) => {
            if let Some(resolved) = resolve_string(storage, tenant_id, s).await? {
                *value = resolved;
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                Box::pin(resolve_in_value(storage, tenant_id, v)).await?;
            }
        }
        Value::Object(map) => {
            for (_k, v) in map.iter_mut() {
                Box::pin(resolve_in_value(storage, tenant_id, v)).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Resolve a single string. Returns `Ok(None)` if it isn't a `credentials://`
/// reference (caller should leave the value as-is).
async fn resolve_string(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    s: &str,
) -> Result<Option<Value>, StepError> {
    let Some(rest) = s.strip_prefix(SCHEME) else {
        return Ok(None);
    };
    let (id, key) = match rest.split_once('/') {
        Some((id, key)) => (id, Some(key)),
        None => (rest, None),
    };
    if id.is_empty() {
        return Err(StepError::Permanent {
            message: format!("credentials: missing id in reference '{s}'"),
            details: None,
        });
    }

    let credential = storage
        .get_credential(id)
        .await
        .map_err(|e| StepError::Permanent {
            message: format!("credentials: storage lookup for '{id}' failed: {e}"),
            details: None,
        })?;
    let credential = credential.ok_or_else(|| StepError::Permanent {
        message: format!("credentials: no credential registered with id '{id}'"),
        details: None,
    })?;

    // Tenant isolation — a tenant may only resolve its own credentials or
    // global ones. Refuse cross-tenant access at the resolver layer.
    if !tenant_id.is_empty()
        && !credential.tenant_id.is_empty()
        && credential.tenant_id != tenant_id
    {
        return Err(StepError::Permanent {
            message: format!(
                "credentials: credential '{id}' is not accessible from tenant '{tenant_id}'"
            ),
            details: None,
        });
    }
    if !credential.enabled {
        return Err(StepError::Permanent {
            message: format!("credentials: credential '{id}' is disabled"),
            details: None,
        });
    }

    // Parse the stored value as JSON. If parsing fails, treat it as a plain
    // string — this lets operators store either structured objects or raw
    // tokens without ceremony.
    let parsed: Value = serde_json::from_str(credential.value.expose())
        .unwrap_or_else(|_| Value::String(credential.value.expose().to_string()));

    match key {
        None => Ok(Some(parsed)),
        Some(k) => {
            let field = parsed.get(k).cloned().ok_or_else(|| StepError::Permanent {
                message: format!("credentials: credential '{id}' has no field '{k}'"),
                details: None,
            })?;
            Ok(Some(field))
        }
    }
}

/// Background loop that refreshes `OAuth2` credentials nearing expiry.
pub async fn run_refresh_loop(
    storage: Arc<dyn StorageBackend>,
    poll_interval: std::time::Duration,
    refresh_ahead: std::time::Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(poll_interval);
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!("credentials refresh loop cancelled");
                break;
            }
            _ = ticker.tick() => {
                match storage.list_credentials_due_for_refresh(refresh_ahead).await {
                    Ok(due) if due.is_empty() => {}
                    Ok(due) => {
                        debug!(count = due.len(), "refreshing oauth2 credentials");
                        for credential in due {
                            if let Err(e) = refresh_credential(storage.as_ref(), credential).await {
                                warn!(error = %e, "credential refresh failed");
                            }
                        }
                    }
                    Err(e) => warn!(error = %e, "failed to list credentials due for refresh"),
                }
            }
        }
    }
}

/// Refresh a single `OAuth2` credential by `POST`ing to its `refresh_url`.
/// The expected response shape is the standard RFC 6749 token response:
/// `{access_token, expires_in, refresh_token?}`.
async fn refresh_credential(
    storage: &dyn StorageBackend,
    mut credential: CredentialDef,
) -> Result<(), String> {
    if !matches!(credential.kind, CredentialKind::Oauth2) {
        return Err(format!(
            "credential '{}' is not oauth2 (got {:?})",
            credential.id, credential.kind
        ));
    }
    let refresh_url = credential
        .refresh_url
        .clone()
        .ok_or_else(|| format!("credential '{}' has no refresh_url", credential.id))?;
    let refresh_token = credential
        .refresh_token
        .as_ref()
        .ok_or_else(|| format!("credential '{}' has no refresh_token", credential.id))?
        .expose()
        .to_string();

    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token.as_str()),
    ];
    let response = REFRESH_CLIENT
        .post(&refresh_url)
        .form(&params)
        .send()
        .await
        .map_err(|e| format!("credential '{}': refresh POST failed: {e}", credential.id))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "credential '{}': refresh returned {status}: {body}",
            credential.id
        ));
    }

    let token: TokenResponse = response.json().await.map_err(|e| {
        format!(
            "credential '{}': malformed refresh response: {e}",
            credential.id
        )
    })?;

    // Merge the new access_token into the stored value JSON object so downstream
    // consumers still see the same shape (access_token, plus whatever else was
    // stored — userinfo, scope, etc.).
    let mut value_json: serde_json::Value =
        serde_json::from_str(credential.value.expose()).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = value_json.as_object_mut() {
        obj.insert(
            "access_token".into(),
            serde_json::Value::String(token.access_token.clone()),
        );
        if let Some(ref rt) = token.refresh_token {
            obj.insert(
                "refresh_token".into(),
                serde_json::Value::String(rt.clone()),
            );
        }
    } else {
        // Value wasn't an object — replace with a fresh one.
        value_json = serde_json::json!({ "access_token": token.access_token });
    }
    credential.value = SecretString::new(value_json.to_string());
    if let Some(rt) = token.refresh_token {
        credential.refresh_token = Some(SecretString::new(rt));
    }
    if let Some(expires_in) = token.expires_in {
        credential.expires_at = Some(chrono::Utc::now() + chrono::Duration::seconds(expires_in));
    }
    credential.updated_at = chrono::Utc::now();

    storage.update_credential(&credential).await.map_err(|e| {
        format!(
            "credential '{}': persisting refresh failed: {e}",
            credential.id
        )
    })?;

    info!(
        credential_id = %credential.id,
        expires_at = ?credential.expires_at,
        "oauth2 credential refreshed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_string_ignores_non_reference() {
        // A non-credentials string is left untouched.
        let s = "hello";
        let prefix_ok = !s.starts_with(SCHEME);
        assert!(prefix_ok);
    }

    #[tokio::test]
    async fn resolve_in_value_leaves_non_refs_untouched() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut val = serde_json::json!({
            "channel": "#general",
            "text": "hello world",
            "count": 42,
        });
        resolve_in_value(&storage, "t1", &mut val).await.unwrap();
        assert_eq!(val["channel"], "#general");
        assert_eq!(val["text"], "hello world");
        assert_eq!(val["count"], 42);
    }

    #[tokio::test]
    async fn resolve_substitutes_full_credential_value() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let now = chrono::Utc::now();
        storage
            .create_credential(&CredentialDef {
                id: "stripe-prod".into(),
                tenant_id: "t1".into(),
                name: "Stripe Production".into(),
                kind: CredentialKind::ApiKey,
                value: SecretString::new(r#"{"access_token":"sk_live_xxx"}"#.into()),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let mut val = serde_json::json!({
            "auth": "credentials://stripe-prod"
        });
        resolve_in_value(&storage, "t1", &mut val).await.unwrap();
        assert_eq!(val["auth"]["access_token"], "sk_live_xxx");
    }

    #[tokio::test]
    async fn resolve_substitutes_single_field() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let now = chrono::Utc::now();
        storage
            .create_credential(&CredentialDef {
                id: "stripe".into(),
                tenant_id: String::new(),
                name: "Stripe".into(),
                kind: CredentialKind::ApiKey,
                value: SecretString::new(
                    r#"{"access_token":"sk_live_xxx","account":"acct_1"}"#.into(),
                ),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let mut val = serde_json::json!({
            "access_token": "credentials://stripe/access_token",
            "account": "credentials://stripe/account",
        });
        resolve_in_value(&storage, "t1", &mut val).await.unwrap();
        assert_eq!(val["access_token"], "sk_live_xxx");
        assert_eq!(val["account"], "acct_1");
    }

    #[tokio::test]
    async fn resolve_rejects_missing_credential() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut val = serde_json::json!({
            "auth": "credentials://missing"
        });
        let err = resolve_in_value(&storage, "t1", &mut val)
            .await
            .unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn resolve_rejects_disabled_credential() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let now = chrono::Utc::now();
        storage
            .create_credential(&CredentialDef {
                id: "old".into(),
                tenant_id: String::new(),
                name: "Old".into(),
                kind: CredentialKind::ApiKey,
                value: SecretString::new(r#"{"token":"x"}"#.into()),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: false,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let mut val = serde_json::json!("credentials://old");
        let err = resolve_in_value(&storage, "t1", &mut val)
            .await
            .unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn resolve_rejects_cross_tenant_access() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let now = chrono::Utc::now();
        storage
            .create_credential(&CredentialDef {
                id: "acme-secret".into(),
                tenant_id: "acme".into(),
                name: "Acme Secret".into(),
                kind: CredentialKind::ApiKey,
                value: SecretString::new(r#"{"token":"x"}"#.into()),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let mut val = serde_json::json!("credentials://acme-secret");
        let err = resolve_in_value(&storage, "other-tenant", &mut val)
            .await
            .unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn resolve_falls_back_to_raw_string_for_non_json_values() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let now = chrono::Utc::now();
        storage
            .create_credential(&CredentialDef {
                id: "plain".into(),
                tenant_id: String::new(),
                name: "Plain".into(),
                kind: CredentialKind::ApiKey,
                value: SecretString::new("raw-token-xyz".into()),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let mut val = serde_json::json!("credentials://plain");
        resolve_in_value(&storage, "t1", &mut val).await.unwrap();
        assert_eq!(val, serde_json::Value::String("raw-token-xyz".into()));
    }

    #[tokio::test]
    async fn resolve_recurses_into_nested_objects() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let now = chrono::Utc::now();
        storage
            .create_credential(&CredentialDef {
                id: "k".into(),
                tenant_id: String::new(),
                name: "K".into(),
                kind: CredentialKind::ApiKey,
                value: SecretString::new(r#""deep-token""#.into()),
                expires_at: None,
                refresh_url: None,
                refresh_token: None,
                enabled: true,
                description: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        let mut val = serde_json::json!({
            "outer": { "inner": { "token": "credentials://k" } }
        });
        resolve_in_value(&storage, "t1", &mut val).await.unwrap();
        assert_eq!(val["outer"]["inner"]["token"], "deep-token");
    }
}
