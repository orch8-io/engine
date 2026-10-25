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
//!
//! Every engine node runs this loop, so each refresh first claims a short
//! lease on the credential row (`REFRESH_LEASE`) and re-reads it; only
//! the claimer calls the token endpoint. The new tokens are written with a
//! compare-and-swap on `updated_at`, re-applied onto the latest row if a
//! concurrent API edit landed in between — a rotated refresh token is never
//! overwritten by a stale read-modify-write. Residual risk: a crash between
//! the provider rotating the token and the CAS write still loses the new
//! token (inherent to rotating refresh tokens without provider-side
//! idempotency).

use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::clock::SharedClock;
use orch8_types::config::SecretString;
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::error::StepError;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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

/// Shared HTTP client for `OAuth2` refresh calls. `TokenEndpoint` profile:
/// SSRF resolver + no proxy, and never follows redirects (a 307/308 would
/// re-POST the `refresh_token` to wherever the endpoint points).
static REFRESH_CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    crate::outbound::build(
        crate::outbound::builder(crate::outbound::Profile::TokenEndpoint)
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30)),
    )
});

/// How long one node owns a credential's refresh. Must exceed the refresh
/// client's request timeout (30s) so a slow token endpoint cannot let a
/// second node start a concurrent refresh with the same refresh token.
const REFRESH_LEASE: chrono::TimeDelta = chrono::TimeDelta::seconds(120);

/// Cap on a token-endpoint response body.
const MAX_REFRESH_BODY_BYTES: usize = 64 * 1024;

/// Credentials refreshed concurrently in one pass.
const REFRESH_CONCURRENCY: usize = 4;

/// Consecutive refresh failures after which each further failure logs at
/// `error` (the credential most likely needs re-authorizing).
const REFRESH_FAILURES_WARN: u32 = 5;

/// Lifetime assumed for an access token whose response has no `expires_in`
/// (e.g. Salesforce). Without it `expires_at` stayed in the past and the
/// credential was refreshed, rotating its refresh token, on every pass.
const DEFAULT_TOKEN_LIFETIME: chrono::TimeDelta = chrono::TimeDelta::hours(1);

/// Back-off after the `failures`-th consecutive failed refresh: 2 min,
/// doubling, capped at 6 h.
fn refresh_failure_backoff(failures: u32) -> chrono::TimeDelta {
    let minutes = 1_i64 << failures.clamp(1, 9);
    chrono::TimeDelta::minutes(minutes).min(chrono::TimeDelta::hours(6))
}

/// Cap on the hold after a *transient* refresh failure (transport error, 5xx,
/// 429, storage hiccup). A provider blip must not park a healthy credential
/// for hours; only permanent errors (`invalid_grant`, 4xx, bad config)
/// escalate towards the 6 h cap (D-03).
const TRANSIENT_REFRESH_BACKOFF_CAP: chrono::TimeDelta = chrono::TimeDelta::minutes(15);

/// Back-off after the `failures`-th consecutive *transient* failure.
fn transient_refresh_backoff(failures: u32) -> chrono::TimeDelta {
    refresh_failure_backoff(failures).min(TRANSIENT_REFRESH_BACKOFF_CAP)
}

/// A failed token refresh, classified for back-off purposes.
#[derive(Debug)]
struct RefreshError {
    message: String,
    /// The same request will keep failing until a human fixes the credential
    /// (revoked grant, 4xx from the token endpoint, missing/blocked config).
    permanent: bool,
}

impl RefreshError {
    fn permanent(message: String) -> Self {
        Self {
            message,
            permanent: true,
        }
    }
}

/// Unclassified failures (network, storage, body read) are transient.
impl From<String> for RefreshError {
    fn from(message: String) -> Self {
        Self {
            message,
            permanent: false,
        }
    }
}

/// Whether a non-success token-endpoint status is permanent for this grant.
/// 4xx means the request itself was rejected (`invalid_grant`, `invalid_client`,
/// ...); 408/425/429 and all 5xx are the provider being unavailable.
fn refresh_status_is_permanent(status: reqwest::StatusCode) -> bool {
    status.is_client_error() && !matches!(status.as_u16(), 408 | 425 | 429)
}

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

/// Keep infrastructure and ownership failures distinct from reference failures so the
/// caller cannot convert a lost evaluation into a user step failure.
#[derive(Debug)]
pub(crate) enum ClaimedResolutionError {
    Infrastructure(crate::error::EngineError),
    Credential(StepError),
}

pub(crate) async fn resolve_claimed_in_value(
    storage: &dyn StorageBackend,
    tenant: &orch8_types::ids::TenantId,
    claim: crate::control::ExecutionClaim,
    value: &mut Value,
) -> Result<(), ClaimedResolutionError> {
    claim
        .verify(storage, tenant)
        .await
        .map_err(ClaimedResolutionError::Infrastructure)?;
    let mut resolved = value.clone();
    resolve_claimed_walk(storage, tenant, claim, &mut resolved).await?;
    claim
        .verify(storage, tenant)
        .await
        .map_err(ClaimedResolutionError::Infrastructure)?;
    *value = resolved;
    Ok(())
}

async fn resolve_claimed_walk(
    storage: &dyn StorageBackend,
    tenant: &orch8_types::ids::TenantId,
    claim: crate::control::ExecutionClaim,
    value: &mut Value,
) -> Result<(), ClaimedResolutionError> {
    match value {
        Value::String(reference) if reference.starts_with(SCHEME) => {
            claim
                .verify(storage, tenant)
                .await
                .map_err(ClaimedResolutionError::Infrastructure)?;
            let result = resolve_string_classified(storage, tenant.as_str(), reference).await;
            claim
                .verify(storage, tenant)
                .await
                .map_err(ClaimedResolutionError::Infrastructure)?;
            if let Some(resolved) = result.map_err(|error| match error {
                CredentialLookupError::Storage { source, .. } => {
                    ClaimedResolutionError::Infrastructure(crate::error::EngineError::Storage(
                        source,
                    ))
                }
                CredentialLookupError::Credential(error) => {
                    ClaimedResolutionError::Credential(error)
                }
            })? {
                *value = resolved;
            }
        }
        Value::Array(values) => {
            for value in values {
                Box::pin(resolve_claimed_walk(storage, tenant, claim, value)).await?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                Box::pin(resolve_claimed_walk(storage, tenant, claim, value)).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Preserve the storage cause until the caller selects its recovery policy.
/// Claimed preparation uses scheduler infrastructure recovery; trusted legacy
/// resolution exposes the existing `StepError` classification.
#[derive(Debug)]
enum CredentialLookupError {
    Storage {
        credential_id: String,
        source: orch8_types::error::StorageError,
    },
    Credential(StepError),
}

impl CredentialLookupError {
    fn into_step_error(self) -> StepError {
        match self {
            Self::Credential(error) => error,
            Self::Storage {
                credential_id,
                source,
            } => {
                let message =
                    format!("credentials: storage lookup for '{credential_id}' failed: {source}");
                if source.is_transient() {
                    StepError::Retryable {
                        message,
                        details: None,
                    }
                } else {
                    StepError::Permanent {
                        message,
                        details: None,
                    }
                }
            }
        }
    }
}

/// Resolve a single string. Returns `Ok(None)` for an ordinary string.
async fn resolve_string(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    s: &str,
) -> Result<Option<Value>, StepError> {
    resolve_string_classified(storage, tenant_id, s)
        .await
        .map_err(CredentialLookupError::into_step_error)
}

async fn resolve_string_classified(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    s: &str,
) -> Result<Option<Value>, CredentialLookupError> {
    let Some((id, key)) = parse_reference(s).map_err(CredentialLookupError::Credential)? else {
        return Ok(None);
    };

    // Storage selects this tenant or the existing global scope. Claimed
    // preparation changes error recovery, not credential lookup authority.
    let owned_tenant =
        (!tenant_id.is_empty()).then(|| orch8_types::ids::TenantId::unchecked(tenant_id));
    let credential = storage
        .get_credential(owned_tenant.as_ref(), id)
        .await
        .map_err(|source| CredentialLookupError::Storage {
            credential_id: id.to_owned(),
            source,
        })?;
    let credential = credential.ok_or_else(|| {
        CredentialLookupError::Credential(StepError::Permanent {
            message: format!("credentials: no credential registered with id '{id}'"),
            details: None,
        })
    })?;
    if !credential.enabled {
        return Err(CredentialLookupError::Credential(StepError::Permanent {
            message: format!("credentials: credential '{id}' is disabled"),
            details: None,
        }));
    }

    expand_material(id, key, credential.value.expose())
        .map(Some)
        .map_err(CredentialLookupError::Credential)
}

/// Turn stored credential text into the value a reference expands to.
///
/// The text is parsed as JSON; if parsing fails it is treated as a plain
/// string, so operators can store either structured objects or raw tokens
/// without ceremony. `key` selects one field of an object credential.
fn expand_material(id: &str, key: Option<&str>, material: &str) -> Result<Value, StepError> {
    let parsed: Value =
        serde_json::from_str(material).unwrap_or_else(|_| Value::String(material.to_string()));
    match key {
        None => Ok(parsed),
        Some(k) => parsed.get(k).cloned().ok_or_else(|| StepError::Permanent {
            message: format!("credentials: credential '{id}' has no field '{k}'"),
            details: None,
        }),
    }
}

/// Split `credentials://<id>[/<key>]`. `Ok(None)` for any other string.
fn parse_reference(s: &str) -> Result<Option<(&str, Option<&str>)>, StepError> {
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
    Ok(Some((id, key)))
}

// ---------------------------------------------------------------------------
// Executor-local credentials (hybrid mode)
// ---------------------------------------------------------------------------

/// Environment variable prefix of executor-local credentials. The rest of the
/// variable name is the credential id, verbatim (case-sensitive):
/// `ORCH8_CREDENTIAL_stripe_prod` backs `credentials://stripe_prod`.
pub const LOCAL_CREDENTIAL_ENV_PREFIX: &str = "ORCH8_CREDENTIAL_";

/// Upper bound on one local credential file.
const MAX_LOCAL_CREDENTIAL_BYTES: u64 = 64 * 1024;
/// Upper bound on credential names a runtime advertises.
const MAX_ADVERTISED_CREDENTIALS: usize = 64;

/// Credentials held by a remote executor, resolved **on the executor**.
///
/// In hybrid mode the engine never expands `credentials://` references of a
/// step placed on remote executors: the task carries the reference, the
/// executor looks the id up here, and the secret never reaches the control
/// plane. Sources, in order:
///
/// 1. `ORCH8_CREDENTIAL_<id>` environment variables;
/// 2. `<dir>/<id>` files (e.g. a mounted Kubernetes Secret), when a
///    directory is configured.
///
/// Values use the engine's format: JSON is parsed (so a field reference
/// `credentials://<id>/<field>` works), anything else is a plain string.
#[derive(Debug, Clone, Default)]
pub struct LocalCredentials {
    dir: Option<std::path::PathBuf>,
    env: std::collections::BTreeMap<String, SecretString>,
}

/// Credential ids usable as a file name and a capability fact.
fn is_local_credential_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && !id.starts_with('.')
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

impl LocalCredentials {
    /// Snapshot `ORCH8_CREDENTIAL_*` from the process environment and use
    /// `dir` (if any) for file-backed credentials.
    #[must_use]
    pub fn from_env(dir: Option<std::path::PathBuf>) -> Self {
        Self::from_vars(dir, std::env::vars())
    }

    /// Like [`Self::from_env`] with explicit variables (tests).
    #[must_use]
    pub fn from_vars(
        dir: Option<std::path::PathBuf>,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        let env = vars
            .into_iter()
            .filter_map(|(name, value)| {
                let id = name.strip_prefix(LOCAL_CREDENTIAL_ENV_PREFIX)?;
                is_local_credential_id(id).then(|| (id.to_owned(), SecretString::new(value)))
            })
            .collect();
        Self { dir, env }
    }

    /// The raw material for `id`, if this executor holds it.
    fn material(&self, id: &str) -> Result<Option<SecretString>, StepError> {
        if !is_local_credential_id(id) {
            return Ok(None);
        }
        if let Some(value) = self.env.get(id) {
            return Ok(Some(value.clone()));
        }
        let Some(dir) = &self.dir else {
            return Ok(None);
        };
        let path = dir.join(id);
        match std::fs::metadata(&path) {
            Ok(meta) if meta.is_file() && meta.len() <= MAX_LOCAL_CREDENTIAL_BYTES => {}
            Ok(meta) if meta.is_file() => {
                return Err(StepError::Permanent {
                    message: format!(
                        "credentials: local credential '{id}' exceeds {MAX_LOCAL_CREDENTIAL_BYTES} bytes"
                    ),
                    details: None,
                });
            }
            _ => return Ok(None),
        }
        let text = std::fs::read_to_string(&path).map_err(|e| StepError::Retryable {
            message: format!("credentials: cannot read local credential '{id}': {e}"),
            details: None,
        })?;
        Ok(Some(SecretString::new(
            text.trim_end_matches(['\n', '\r']).to_owned(),
        )))
    }

    /// Credential ids this executor can resolve (advertised to the control
    /// plane as capability facts; never values). Sorted, at most 64.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        let mut names: std::collections::BTreeSet<String> = self.env.keys().cloned().collect();
        if let Some(dir) = &self.dir
            && let Ok(entries) = std::fs::read_dir(dir)
        {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if is_local_credential_id(&name)
                    && std::fs::metadata(entry.path()).is_ok_and(|m| m.is_file())
                {
                    names.insert(name);
                }
            }
        }
        names.into_iter().take(MAX_ADVERTISED_CREDENTIALS).collect()
    }

    /// Replace every `credentials://<id>[/<key>]` string in `value` with the
    /// locally held material. A reference to a credential this executor does
    /// not hold fails the step permanently (it never falls back to asking the
    /// control plane).
    pub fn resolve_in_value(&self, value: &mut Value) -> Result<(), StepError> {
        match value {
            Value::String(s) => {
                if let Some((id, key)) = parse_reference(s)? {
                    let material = self.material(id)?.ok_or_else(|| StepError::Permanent {
                        message: format!(
                            "credentials: this executor holds no local credential '{id}' \
                             (set {LOCAL_CREDENTIAL_ENV_PREFIX}{id} or add it to the credentials directory)"
                        ),
                        details: None,
                    })?;
                    *value = expand_material(id, key, material.expose())?;
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.resolve_in_value(item)?;
                }
            }
            Value::Object(map) => {
                for item in map.values_mut() {
                    self.resolve_in_value(item)?;
                }
            }
            _ => {}
        }
        Ok(())
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
                tokio::select! {
                    () = cancel.cancelled() => {
                        info!("credentials refresh loop cancelled");
                        break;
                    }
                    () = refresh_pass(&storage, refresh_ahead) => {}
                }
            }
        }
    }
}

/// One refresh pass over the due credentials, a few at a time: one hanging
/// token endpoint (up to the 35 s client timeout) must not hold up the rest
/// until they expire. Dropping the future aborts the in-flight refreshes.
async fn refresh_pass(storage: &Arc<dyn StorageBackend>, refresh_ahead: std::time::Duration) {
    refresh_pass_at(storage, refresh_ahead, &SharedClock::default()).await;
}

/// The single-pass entry point of [`run_refresh_loop`], reading "now" (the
/// refresh lease, the due cutoff, the failure back-off) from `clock`. Which
/// credentials are *listed* as due is still decided by the database clock
/// (`list_credentials_due_for_refresh`), so tests back-date `expires_at`.
pub async fn refresh_pass_at(
    storage: &Arc<dyn StorageBackend>,
    refresh_ahead: std::time::Duration,
    clock: &SharedClock,
) {
    let due = match storage
        .list_credentials_due_for_refresh(refresh_ahead)
        .await
    {
        Ok(due) if due.is_empty() => return,
        Ok(due) => due,
        Err(e) => {
            warn!(error = %e, "failed to list credentials due for refresh");
            return;
        }
    };
    debug!(count = due.len(), "refreshing oauth2 credentials");
    let mut in_flight = tokio::task::JoinSet::new();
    for credential in due {
        if in_flight.len() >= REFRESH_CONCURRENCY {
            in_flight.join_next().await;
        }
        let storage = Arc::clone(storage);
        let clock = clock.clone();
        in_flight.spawn(async move {
            if let Err(e) =
                refresh_due_credential(storage.as_ref(), &credential, refresh_ahead, clock.now())
                    .await
            {
                warn!(error = %e, "credential refresh failed");
            }
        });
    }
    while in_flight.join_next().await.is_some() {}
}

/// Claim, re-read, and refresh one credential listed as due. Returns
/// `Ok(false)` when another node holds the refresh lease or the credential
/// is no longer due (someone refreshed it since the listing).
async fn refresh_due_credential(
    storage: &dyn StorageBackend,
    listed: &CredentialDef,
    refresh_ahead: std::time::Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, String> {
    let lease_until = now.checked_add_signed(REFRESH_LEASE).unwrap_or(now);
    let claimed = storage
        .claim_credential_refresh(&listed.id, now, lease_until)
        .await
        .map_err(|e| format!("credential '{}': refresh claim failed: {e}", listed.id))?;
    if !claimed {
        debug!(credential_id = %listed.id, "refresh lease held by another node, skipping");
        return Ok(false);
    }
    // Re-read under the lease: the listing may predate another node's
    // refresh, whose rotated refresh token must be the one we send.
    let Some(fresh) = storage
        .get_credential(None, &listed.id)
        .await
        .map_err(|e| format!("credential '{}': re-read failed: {e}", listed.id))?
    else {
        return Ok(false);
    };
    let cutoff =
        now + chrono::Duration::from_std(refresh_ahead).unwrap_or(chrono::Duration::seconds(300));
    if !fresh.enabled || fresh.expires_at.is_none_or(|t| t > cutoff) {
        return Ok(false);
    }
    if let Err(refresh_error) = refresh_credential(storage, fresh).await {
        // Back off: a dead credential (revoked app, `invalid_grant`) must not
        // be retried, and listed ahead of healthy ones, every pass. Transient
        // failures (provider outage) use a short capped hold instead (D-03).
        let backoff: fn(u32) -> chrono::TimeDelta = if refresh_error.permanent {
            refresh_failure_backoff
        } else {
            transient_refresh_backoff
        };
        let error = refresh_error.message;
        match storage
            .record_credential_refresh_failure(&listed.id, now, backoff)
            .await
        {
            Ok(failures) if failures >= REFRESH_FAILURES_WARN => {
                error!(credential_id = %listed.id, failures,
                    retry_in_secs = backoff(failures).num_seconds(),
                    "oauth2 credential keeps failing to refresh; check or re-authorize it");
            }
            Ok(_) => {}
            Err(e) => warn!(credential_id = %listed.id, error = %e,
                "failed to record the credential refresh failure"),
        }
        crate::metrics::inc(crate::metrics::CREDENTIAL_REFRESH_FAILURES);
        return Err(error);
    }
    if let Err(e) = storage.reset_credential_refresh_failures(&listed.id).await {
        warn!(credential_id = %listed.id, error = %e, "failed to reset the credential refresh back-off");
    }
    Ok(true)
}

/// Refresh a single `OAuth2` credential by `POST`ing to its `refresh_url`.
/// The expected response shape is the standard RFC 6749 token response:
/// `{access_token, expires_in, refresh_token?}`.
async fn refresh_credential(
    storage: &dyn StorageBackend,
    credential: CredentialDef,
) -> Result<(), RefreshError> {
    if !matches!(credential.kind, CredentialKind::Oauth2) {
        return Err(RefreshError::permanent(format!(
            "credential '{}' is not oauth2 (got {:?})",
            credential.id, credential.kind
        )));
    }
    let refresh_url = credential.refresh_url.clone().ok_or_else(|| {
        RefreshError::permanent(format!("credential '{}' has no refresh_url", credential.id))
    })?;
    let refresh_token = credential
        .refresh_token
        .as_ref()
        .ok_or_else(|| {
            RefreshError::permanent(format!(
                "credential '{}' has no refresh_token",
                credential.id
            ))
        })?
        .expose()
        .to_string();

    if !crate::handlers::builtin::is_url_safe(&refresh_url).await {
        return Err(RefreshError::permanent(format!(
            "credential '{}': refresh_url '{}' targets an internal or non-public address",
            credential.id,
            crate::outbound::redact_url(&refresh_url)
        )));
    }

    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token.as_str()),
    ];
    let response = REFRESH_CLIENT
        .post(&refresh_url)
        .form(&params)
        .send()
        .await
        .map_err(|e| {
            format!(
                "credential '{}': refresh POST failed: {}",
                credential.id,
                crate::outbound::redact_error(&e)
            )
        })?;
    let status = response.status();
    let body = crate::outbound::read_body_capped(response, MAX_REFRESH_BODY_BYTES)
        .await
        .map_err(|e| {
            format!(
                "credential '{}': refresh returned {status} and reading the body failed: {e:?}",
                credential.id
            )
        })?;
    if !status.is_success() {
        // Token endpoints may echo submitted form fields in error bodies.
        // Never propagate that body into the refresh loop's warning log: it
        // can contain the refresh token we just sent.
        return Err(RefreshError {
            message: format!("credential '{}': refresh returned {status}", credential.id),
            permanent: refresh_status_is_permanent(status),
        });
    }

    let token: TokenResponse = serde_json::from_slice(&body).map_err(|e| {
        RefreshError::permanent(format!(
            "credential '{}': malformed refresh response: {e}",
            credential.id
        ))
    })?;

    persist_refreshed_tokens(storage, &credential, &token).await?;
    Ok(())
}

/// Apply a token response onto `credential` in place: merge `access_token`
/// (and a rotated `refresh_token`) into the stored value JSON so downstream
/// consumers still see the same shape, and advance `expires_at`.
fn apply_token_response(credential: &mut CredentialDef, token: &TokenResponse) {
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
    if let Some(ref rt) = token.refresh_token {
        credential.refresh_token = Some(SecretString::new(rt.clone()));
    }
    // `DateTime + Duration` panics on overflow, and `expires_in` comes from
    // the token endpoint's response — never let a hostile or buggy value
    // kill the refresh loop. A missing (or unusable) `expires_in` assumes
    // `DEFAULT_TOKEN_LIFETIME` rather than keeping a past expiry.
    let lifetime = token
        .expires_in
        .and_then(chrono::Duration::try_seconds)
        .filter(|delta| *delta > chrono::TimeDelta::zero())
        .unwrap_or(DEFAULT_TOKEN_LIFETIME);
    credential.expires_at = chrono::Utc::now()
        .checked_add_signed(lifetime)
        .or(credential.expires_at);
}

/// Persist refreshed tokens with a CAS on `updated_at`. If a concurrent
/// writer (typically an API PATCH) changed the row since `read` was loaded,
/// re-apply only the token fields onto the latest row and CAS once more —
/// the provider has already rotated, so these tokens are the live ones and
/// must not be dropped, while the concurrent edit's other fields survive.
async fn persist_refreshed_tokens(
    storage: &dyn StorageBackend,
    read: &CredentialDef,
    token: &TokenResponse,
) -> Result<(), String> {
    let mut updated = read.clone();
    apply_token_response(&mut updated, token);
    let persisted = |e| format!("credential '{}': persisting refresh failed: {e}", read.id);
    if storage
        .update_credential_cas(&updated, read.updated_at)
        .await
        .map_err(persisted)?
    {
        info!(
            credential_id = %read.id,
            expires_at = ?updated.expires_at,
            "oauth2 credential refreshed"
        );
        return Ok(());
    }
    let Some(latest) = storage
        .get_credential(None, &read.id)
        .await
        .map_err(persisted)?
    else {
        return Err(format!(
            "credential '{}': deleted during refresh; new tokens discarded",
            read.id
        ));
    };
    let mut merged = latest.clone();
    apply_token_response(&mut merged, token);
    if storage
        .update_credential_cas(&merged, latest.updated_at)
        .await
        .map_err(persisted)?
    {
        info!(
            credential_id = %read.id,
            expires_at = ?merged.expires_at,
            "oauth2 credential refreshed (merged onto concurrent update)"
        );
        Ok(())
    } else {
        Err(format!(
            "credential '{}': persisting refresh lost two CAS races; new tokens discarded",
            read.id
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::AdminStore;

    #[test]
    fn local_credentials_resolve_from_env_and_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("db-pass"), "s3cr3t\n").unwrap();
        std::fs::write(dir.path().join("api"), r#"{"token":"tok-1","user":"u"}"#).unwrap();
        std::fs::write(dir.path().join(".hidden"), "x").unwrap();
        std::fs::create_dir(dir.path().join("..data")).unwrap_or_default();
        let local = LocalCredentials::from_vars(
            Some(dir.path().to_path_buf()),
            [
                (
                    "ORCH8_CREDENTIAL_stripe_prod".to_string(),
                    "sk_env".to_string(),
                ),
                ("ORCH8_CREDENTIAL_bad/id".to_string(), "x".to_string()),
                ("OTHER".to_string(), "y".to_string()),
            ],
        );
        assert_eq!(local.names(), vec!["api", "db-pass", "stripe_prod"]);

        let mut params = serde_json::json!({
            "password": "credentials://db-pass",
            "auth": {"bearer": "credentials://api/token"},
            "key": "credentials://stripe_prod",
            "plain": "credentials-free",
        });
        local.resolve_in_value(&mut params).unwrap();
        assert_eq!(params["password"], "s3cr3t");
        assert_eq!(params["auth"]["bearer"], "tok-1");
        assert_eq!(params["key"], "sk_env");
        assert_eq!(params["plain"], "credentials-free");

        let mut missing = serde_json::json!({"x": "credentials://nope"});
        let err = local
            .resolve_in_value(&mut missing)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no local credential 'nope'"), "{err}");
        let mut traversal = serde_json::json!("credentials://../etc/passwd");
        assert!(local.resolve_in_value(&mut traversal).is_err());
        let mut no_field = serde_json::json!("credentials://api/missing");
        assert!(local.resolve_in_value(&mut no_field).is_err());
    }

    fn credential(id: &str, kind: CredentialKind) -> CredentialDef {
        let now = chrono::Utc::now();
        CredentialDef {
            id: id.into(),
            tenant_id: String::new(),
            name: id.into(),
            kind,
            value: SecretString::new(r#"{"token":"secret"}"#.into()),
            expires_at: None,
            refresh_url: None,
            refresh_token: None,
            enabled: true,
            description: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn resolve_string_ignores_non_reference() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();

        assert!(matches!(
            resolve_string(&storage, "tenant", "hello").await,
            Ok(None)
        ));
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

    #[tokio::test]
    async fn resolve_recurses_into_arrays() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        storage
            .create_credential(&credential("array-token", CredentialKind::ApiKey))
            .await
            .unwrap();
        let mut value = serde_json::json!([
            "unchanged",
            "credentials://array-token/token",
            {"nested": ["credentials://array-token/token"]}
        ]);

        resolve_in_value(&storage, "tenant", &mut value)
            .await
            .unwrap();

        assert_eq!(value[0], "unchanged");
        assert_eq!(value[1], "secret");
        assert_eq!(value[2]["nested"][0], "secret");
    }

    #[tokio::test]
    async fn resolve_rejects_reference_without_an_id() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut value = serde_json::json!("credentials://");

        let error = resolve_in_value(&storage, "tenant", &mut value)
            .await
            .unwrap_err();

        let StepError::Permanent { message, .. } = error else {
            panic!("missing credential id should be a permanent error");
        };
        assert!(message.contains("missing id"));
        assert!(message.contains("credentials://"));
    }

    #[tokio::test]
    async fn resolve_rejects_missing_structured_field() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        storage
            .create_credential(&credential("structured", CredentialKind::ApiKey))
            .await
            .unwrap();
        let mut value = serde_json::json!("credentials://structured/missing");

        let error = resolve_in_value(&storage, "tenant", &mut value)
            .await
            .unwrap_err();

        let StepError::Permanent { message, .. } = error else {
            panic!("missing credential field should be a permanent error");
        };
        assert!(message.contains("credential 'structured'"));
        assert!(message.contains("no field 'missing'"));
    }

    #[tokio::test]
    async fn refresh_rejects_non_oauth_credentials_before_network_access() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let value = credential("api-key", CredentialKind::ApiKey);

        let error = refresh_credential(&storage, value)
            .await
            .unwrap_err()
            .message;

        assert!(error.contains("is not oauth2"));
    }

    #[tokio::test]
    async fn refresh_requires_refresh_url() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut value = credential("oauth", CredentialKind::Oauth2);
        value.refresh_token = Some(SecretString::new("refresh-token".into()));

        let error = refresh_credential(&storage, value)
            .await
            .unwrap_err()
            .message;

        assert_eq!(error, "credential 'oauth' has no refresh_url");
    }

    #[tokio::test]
    async fn refresh_requires_refresh_token() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut value = credential("oauth", CredentialKind::Oauth2);
        value.refresh_url = Some("https://example.com/token".into());

        let error = refresh_credential(&storage, value)
            .await
            .unwrap_err()
            .message;

        assert_eq!(error, "credential 'oauth' has no refresh_token");
    }

    #[tokio::test]
    async fn refresh_rejects_internal_token_endpoint() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut value = credential("oauth", CredentialKind::Oauth2);
        value.refresh_url = Some("http://127.0.0.1/token".into());
        value.refresh_token = Some(SecretString::new("refresh-token".into()));

        let error = refresh_credential(&storage, value)
            .await
            .unwrap_err()
            .message;

        assert!(error.contains("targets an internal or non-public address"));
    }

    #[tokio::test]
    async fn refresh_loop_stops_when_cancelled() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            run_refresh_loop(
                storage,
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(300),
                cancel,
            ),
        )
        .await
        .expect("cancelled refresh loop should stop promptly");
    }

    fn due_oauth(id: &str) -> CredentialDef {
        let mut c = credential(id, CredentialKind::Oauth2);
        c.refresh_url = Some("https://token.example.invalid/oauth".into());
        c.refresh_token = Some(SecretString::new("rt-1".into()));
        c.expires_at = Some(chrono::Utc::now());
        c
    }

    /// Only the lease holder refreshes: with another node's lease live, the
    /// refresh is skipped before any network access.
    #[tokio::test]
    async fn refresh_skips_when_another_node_holds_the_lease() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let cred = due_oauth("oa");
        storage.create_credential(&cred).await.unwrap();
        let now = chrono::Utc::now();
        assert!(
            storage
                .claim_credential_refresh("oa", now, now + REFRESH_LEASE)
                .await
                .unwrap(),
            "first claim wins"
        );
        let refreshed = refresh_due_credential(
            &storage,
            &cred,
            std::time::Duration::from_secs(300),
            chrono::Utc::now(),
        )
        .await
        .expect("a held lease is a skip, not an error");
        assert!(!refreshed);
    }

    /// A concurrent API edit between the refresh read and the token write
    /// must neither be clobbered nor clobber the freshly rotated tokens.
    #[tokio::test]
    async fn persist_refresh_merges_tokens_onto_concurrent_update() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        storage.create_credential(&due_oauth("oa")).await.unwrap();
        let read = storage.get_credential(None, "oa").await.unwrap().unwrap();

        // Concurrent PATCH lands first.
        let mut patched = read.clone();
        patched.description = Some("edited".into());
        assert!(
            storage
                .update_credential_cas(&patched, read.updated_at)
                .await
                .unwrap()
        );

        let token = TokenResponse {
            access_token: "at-2".into(),
            refresh_token: Some("rt-2".into()),
            expires_in: Some(3600),
        };
        persist_refreshed_tokens(&storage, &read, &token)
            .await
            .unwrap();

        let stored = storage.get_credential(None, "oa").await.unwrap().unwrap();
        assert_eq!(stored.description.as_deref(), Some("edited"));
        assert_eq!(stored.refresh_token.as_ref().unwrap().expose(), "rt-2");
        let value: Value = serde_json::from_str(stored.value.expose()).unwrap();
        assert_eq!(value["access_token"], "at-2");
    }

    /// L5-06: a credential whose refresh fails backs off and leaves the due
    /// listing, so it is not retried every pass ahead of healthy ones.
    #[tokio::test]
    async fn failed_refresh_backs_off_and_leaves_the_due_list() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut dead = due_oauth("dead");
        // Fails fast without network access (blocked internal endpoint).
        dead.refresh_url = Some("http://127.0.0.1/token".into());
        dead.expires_at = Some(chrono::Utc::now() - chrono::Duration::days(30));
        storage.create_credential(&dead).await.unwrap();
        storage
            .create_credential(&due_oauth("healthy"))
            .await
            .unwrap();
        let ahead = std::time::Duration::from_secs(300);

        let due = storage
            .list_credentials_due_for_refresh(ahead)
            .await
            .unwrap();
        assert_eq!(due.len(), 2);
        assert!(
            refresh_due_credential(&storage, &dead, ahead, chrono::Utc::now())
                .await
                .is_err()
        );

        let due = storage
            .list_credentials_due_for_refresh(ahead)
            .await
            .unwrap();
        let ids: Vec<_> = due.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["healthy"], "the failing credential is backed off");
        // The back-off holds the lease: no node may retry it yet.
        let now = chrono::Utc::now();
        assert!(
            !storage
                .claim_credential_refresh("dead", now, now + REFRESH_LEASE)
                .await
                .unwrap()
        );
        let failures = storage
            .record_credential_refresh_failure("dead", now, refresh_failure_backoff)
            .await
            .unwrap();
        assert_eq!(failures, 2, "failures accumulate");
        storage
            .reset_credential_refresh_failures("dead")
            .await
            .unwrap();
        assert_eq!(
            storage
                .list_credentials_due_for_refresh(ahead)
                .await
                .unwrap()
                .len(),
            2,
            "a reset makes it due again"
        );
    }

    #[test]
    fn refresh_backoff_doubles_and_is_capped() {
        assert_eq!(refresh_failure_backoff(1), chrono::TimeDelta::minutes(2));
        assert_eq!(refresh_failure_backoff(2), chrono::TimeDelta::minutes(4));
        assert_eq!(refresh_failure_backoff(50), chrono::TimeDelta::hours(6));
    }

    /// D-03: provider blips (transport, 5xx, 429) hold a credential for at
    /// most ~15 min; only 4xx rejections escalate to the multi-hour cap.
    #[test]
    fn transient_refresh_failures_do_not_escalate() {
        assert_eq!(transient_refresh_backoff(1), chrono::TimeDelta::minutes(2));
        assert_eq!(
            transient_refresh_backoff(50),
            chrono::TimeDelta::minutes(15)
        );
        for code in [500_u16, 502, 503, 408, 425, 429] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert!(!refresh_status_is_permanent(status), "{code} is transient");
        }
        for code in [400_u16, 401, 403, 404] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert!(refresh_status_is_permanent(status), "{code} is permanent");
        }
        assert!(!RefreshError::from("net down".to_string()).permanent);
    }

    /// A token response without `expires_in` must move `expires_at` into the
    /// future instead of leaving the credential due (and refreshed) forever.
    #[test]
    fn token_without_expires_in_gets_a_default_lifetime() {
        let mut cred = due_oauth("oa");
        cred.expires_at = Some(chrono::Utc::now() - chrono::Duration::hours(2));
        apply_token_response(
            &mut cred,
            &TokenResponse {
                access_token: "at".into(),
                refresh_token: None,
                expires_in: None,
            },
        );
        let left = cred.expires_at.unwrap() - chrono::Utc::now();
        assert!(left > chrono::Duration::minutes(55) && left <= DEFAULT_TOKEN_LIFETIME);
    }

    /// L5-07: a storage blip while resolving a reference is retryable.
    #[tokio::test]
    async fn transient_storage_error_while_resolving_is_retryable() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        storage.pool().close().await;
        let err = resolve_string(&storage, "", "credentials://api")
            .await
            .unwrap_err();
        assert!(matches!(err, StepError::Retryable { .. }), "{err:?}");
    }

    /// A stale full-row write (the old PATCH behavior) must be rejected.
    #[tokio::test]
    async fn update_credential_cas_rejects_stale_write() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        storage.create_credential(&due_oauth("oa")).await.unwrap();
        let read = storage.get_credential(None, "oa").await.unwrap().unwrap();
        let mut first = read.clone();
        first.refresh_token = Some(SecretString::new("rotated".into()));
        assert!(
            storage
                .update_credential_cas(&first, read.updated_at)
                .await
                .unwrap()
        );
        let mut stale = read.clone();
        stale.description = Some("stale".into());
        assert!(
            !storage
                .update_credential_cas(&stale, read.updated_at)
                .await
                .unwrap()
        );
        let stored = storage.get_credential(None, "oa").await.unwrap().unwrap();
        assert_eq!(stored.refresh_token.as_ref().unwrap().expose(), "rotated");
    }
}
