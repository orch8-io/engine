//! Offline license keys (ed25519) with soft enforcement.
//!
//! Format: `o8l1.<b64url(json)>.<b64url(ed25519 signature over "o8l1." + b64json)>`.
//! Orch8 Cloud signs keys with a private key that never leaves Cloud
//! (`ORCH8_LICENSE_SIGNING_KEY`); the engine verifies them offline against a
//! compiled-in public key ([`EMBEDDED_PUBLIC_KEY_B64`]), which tests and
//! private deployments may override with `ORCH8_LICENSE_PUBLIC_KEY`.
//!
//! Enforcement is **soft only**: a missing, invalid or expired license never
//! blocks an execution. Using more than [`UNLICENSED_SUB_TENANT_ALLOWANCE`]
//! active sub-tenants without a license that includes `sub_tenants` adds
//! `X-Orch8-License: unlicensed` to API responses and logs a rate-limited
//! warning; exceeding a license's `max_sub_tenants` adds
//! `X-Orch8-License: over_limit`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::error::ApiError;

pub const LICENSE_PREFIX: &str = "o8l1";
/// Env override for the verification key (base64 of the raw 32-byte key).
pub const PUBLIC_KEY_ENV: &str = "ORCH8_LICENSE_PUBLIC_KEY";
/// Env equivalent of `[license] key`.
pub const LICENSE_KEY_ENV: &str = "ORCH8_LICENSE_KEY";
/// Compiled-in license verification key (base64, raw 32 bytes).
///
/// Production key (rotated in 2026-10-02). The PKCS#8 private half lives only
/// in Orch8 Cloud's secret store as `ORCH8_LICENSE_SIGNING_KEY`. Rotation
/// steps: `docs/EMBEDDED.md#rotating-the-verification-key`. Never commit a
/// private key.
pub const EMBEDDED_PUBLIC_KEY_B64: &str = "90fS4aSuSHgES8cBI11eRdmZdAxUtntWaKLgfSkuN2Q=";
/// Active sub-tenants tolerated without a `sub_tenants` license.
pub const UNLICENSED_SUB_TENANT_ALLOWANCE: u64 = 3;
/// Response header carrying the soft-enforcement verdict.
pub const LICENSE_HEADER: &str = "x-orch8-license";
/// Rolling window defining an "active" sub-tenant for enforcement.
const ACTIVE_WINDOW_DAYS: i64 = 30;
const COUNT_CACHE_TTL: Duration = Duration::from_secs(60);
const WARN_INTERVAL: Duration = Duration::from_secs(3_600);
/// Upper bound on a license token (defensive parse bound).
const MAX_LICENSE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LicenseStatus {
    Licensed,
    Unlicensed,
    Expired,
    Invalid,
}

/// Signed license claims. Unknown fields are ignored so Cloud can add claims
/// without breaking older engines. Timestamps accept unix seconds or RFC 3339.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LicensePayload {
    pub v: u8,
    pub licensee: String,
    /// `oem`, `embedded` or `hybrid`.
    pub edition: String,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub max_sub_tenants: Option<u64>,
    #[serde(deserialize_with = "flexible_ts")]
    pub issued_at: DateTime<Utc>,
    #[serde(deserialize_with = "flexible_ts")]
    pub expires_at: DateTime<Utc>,
}

fn flexible_ts<'de, D: Deserializer<'de>>(deserializer: D) -> Result<DateTime<Utc>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Ts {
        Unix(i64),
        Text(String),
    }
    match Ts::deserialize(deserializer)? {
        Ts::Unix(secs) => DateTime::from_timestamp(secs, 0)
            .ok_or_else(|| serde::de::Error::custom("timestamp out of range")),
        Ts::Text(text) => DateTime::parse_from_rfc3339(&text)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom),
    }
}

/// A license as loaded at startup; expiry is re-evaluated on every read.
#[derive(Debug, Clone, Default)]
pub struct License {
    payload: Option<LicensePayload>,
    /// Set when a key was configured but failed verification.
    invalid: bool,
}

impl License {
    #[must_use]
    pub fn unlicensed() -> Self {
        Self::default()
    }

    /// Verify `key` against `public_key`. Never fails: a bad key yields an
    /// `invalid` license (soft enforcement).
    #[must_use]
    pub fn verify(key: &str, public_key: &VerifyingKey) -> Self {
        let key = key.trim();
        if key.is_empty() {
            return Self::unlicensed();
        }
        match decode_and_verify(key, public_key) {
            Some(payload) => Self {
                payload: Some(payload),
                invalid: false,
            },
            None => Self {
                payload: None,
                invalid: true,
            },
        }
    }

    /// Load from the configured key and the effective public key
    /// ([`PUBLIC_KEY_ENV`] override, else the compiled-in key). An
    /// unparseable override makes a configured license `invalid` (logged).
    #[must_use]
    pub fn load(key: &str) -> Self {
        if key.trim().is_empty() {
            return Self::unlicensed();
        }
        match effective_public_key() {
            Ok(public_key) => {
                let license = Self::verify(key, &public_key);
                if license.invalid {
                    tracing::warn!(
                        "license key failed verification; running unlicensed (soft enforcement)"
                    );
                }
                license
            }
            Err(error) => {
                tracing::error!(%error, "license verification key is unusable");
                Self {
                    payload: None,
                    invalid: true,
                }
            }
        }
    }

    #[must_use]
    pub fn status(&self, now: DateTime<Utc>) -> LicenseStatus {
        match &self.payload {
            None if self.invalid => LicenseStatus::Invalid,
            None => LicenseStatus::Unlicensed,
            Some(payload) if payload.expires_at <= now => LicenseStatus::Expired,
            Some(_) => LicenseStatus::Licensed,
        }
    }

    /// Whether a *currently valid* license grants `feature`.
    #[must_use]
    pub fn has_feature(&self, feature: &str, now: DateTime<Utc>) -> bool {
        self.status(now) == LicenseStatus::Licensed
            && self
                .payload
                .as_ref()
                .is_some_and(|p| p.features.iter().any(|f| f == feature))
    }

    #[must_use]
    pub fn info(&self, now: DateTime<Utc>) -> LicenseInfo {
        let status = self.status(now);
        let payload = self.payload.as_ref();
        LicenseInfo {
            status,
            edition: payload.map(|p| p.edition.clone()),
            licensee: payload.map(|p| p.licensee.clone()),
            expires_at: payload.map(|p| p.expires_at),
            features: if status == LicenseStatus::Licensed {
                payload.map(|p| p.features.clone()).unwrap_or_default()
            } else {
                Vec::new()
            },
            max_sub_tenants: payload.and_then(|p| p.max_sub_tenants),
        }
    }

    /// Soft-enforcement verdict for `active` sub-tenants, if any.
    #[must_use]
    pub fn verdict(&self, active: u64, now: DateTime<Utc>) -> Option<&'static str> {
        if self.has_feature("sub_tenants", now) {
            let max = self.payload.as_ref().and_then(|p| p.max_sub_tenants);
            return max.filter(|max| active > *max).map(|_| "over_limit");
        }
        (active > UNLICENSED_SUB_TENANT_ALLOWANCE).then_some("unlicensed")
    }
}

fn decode_and_verify(key: &str, public_key: &VerifyingKey) -> Option<LicensePayload> {
    if key.len() > MAX_LICENSE_BYTES {
        return None;
    }
    let mut parts = key.split('.');
    let (Some(prefix), Some(payload_b64), Some(signature_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if prefix != LICENSE_PREFIX {
        return None;
    }
    let signature = URL_SAFE_NO_PAD.decode(signature_b64).ok()?;
    let signature = Signature::from_slice(&signature).ok()?;
    let signed = format!("{LICENSE_PREFIX}.{payload_b64}");
    public_key
        .verify_strict(signed.as_bytes(), &signature)
        .ok()?;
    let payload = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let payload: LicensePayload = serde_json::from_slice(&payload).ok()?;
    (payload.v == 1).then_some(payload)
}

/// Parse a base64 (standard or URL-safe) raw 32-byte ed25519 public key.
pub fn parse_public_key(b64: &str) -> Result<VerifyingKey, String> {
    let b64 = b64.trim();
    let bytes = STANDARD
        .decode(b64)
        .or_else(|_| URL_SAFE_NO_PAD.decode(b64.trim_end_matches('=')))
        .map_err(|e| format!("license public key is not base64: {e}"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "license public key must be 32 raw bytes".to_string())?;
    VerifyingKey::from_bytes(&bytes).map_err(|e| format!("invalid ed25519 public key: {e}"))
}

fn effective_public_key() -> Result<VerifyingKey, String> {
    match std::env::var(PUBLIC_KEY_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        Some(override_key) => parse_public_key(&override_key),
        None => parse_public_key(EMBEDDED_PUBLIC_KEY_B64),
    }
}

/// `GET /license` body.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LicenseInfo {
    pub status: LicenseStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub licensee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    pub features: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_sub_tenants: Option<u64>,
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/license", get(get_license))
}

#[utoipa::path(get, path = "/license", tag = "license",
    responses((status = 200, description = "Offline-verified license status (soft enforcement only)",
        body = LicenseInfo))
)]
pub(crate) async fn get_license(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.embedded.license.info(Utc::now())))
}

/// Cached engine-wide active-sub-tenant count + warn throttle.
#[derive(Debug)]
pub struct SoftEnforcer {
    ttl: Duration,
    cache: tokio::sync::Mutex<Option<(Instant, u64)>>,
    last_warn: std::sync::Mutex<Option<Instant>>,
}

impl Default for SoftEnforcer {
    fn default() -> Self {
        Self::with_ttl(COUNT_CACHE_TTL)
    }
}

impl SoftEnforcer {
    /// Enforcer whose active-sub-tenant count is cached for `ttl` (tests use
    /// zero to observe changes immediately).
    #[must_use]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            cache: tokio::sync::Mutex::new(None),
            last_warn: std::sync::Mutex::new(None),
        }
    }

    async fn active_sub_tenants(&self, state: &AppState) -> Option<u64> {
        let mut cache = self.cache.lock().await;
        if let Some((at, count)) = *cache
            && at.elapsed() < self.ttl
        {
            return Some(count);
        }
        let since = Utc::now() - chrono::Duration::days(ACTIVE_WINDOW_DAYS);
        match state.storage.count_active_sub_tenants(since).await {
            Ok(count) => {
                *cache = Some((Instant::now(), count));
                Some(count)
            }
            Err(error) => {
                // Soft enforcement fails open: never error a request over it.
                tracing::debug!(%error, "license soft-enforcement count unavailable");
                *cache = Some((Instant::now(), 0));
                None
            }
        }
    }

    fn warn(&self, verdict: &str, active: u64) {
        let Ok(mut last) = self.last_warn.lock() else {
            return;
        };
        if last.is_some_and(|at| at.elapsed() < WARN_INTERVAL) {
            return;
        }
        *last = Some(Instant::now());
        tracing::warn!(
            verdict,
            active_sub_tenants = active,
            allowance = UNLICENSED_SUB_TENANT_ALLOWANCE,
            "sub-tenant usage exceeds the license (soft enforcement: executions are not blocked); \
             see https://orch8.io/docs/embedded#license-keys"
        );
    }
}

/// Middleware adding `X-Orch8-License` when sub-tenant use exceeds the
/// license. Never blocks or fails a request.
pub async fn soft_enforcement(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    let enforcer: Arc<crate::embed::EmbeddedRuntime> = Arc::clone(&state.embedded);
    if let Some(active) = enforcer.enforcer.active_sub_tenants(&state).await
        && let Some(verdict) = enforcer.license.verdict(active, Utc::now())
    {
        enforcer.enforcer.warn(verdict, active);
        response
            .headers_mut()
            .insert(LICENSE_HEADER, HeaderValue::from_static(verdict));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn keypair() -> (SigningKey, VerifyingKey) {
        let signing = SigningKey::from_bytes(&rand::random::<[u8; 32]>());
        let verifying = signing.verifying_key();
        (signing, verifying)
    }

    fn sign(signing: &SigningKey, payload: &serde_json::Value) -> String {
        let b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(payload).unwrap());
        let signed = format!("{LICENSE_PREFIX}.{b64}");
        let sig = signing.sign(signed.as_bytes());
        format!("{signed}.{}", URL_SAFE_NO_PAD.encode(sig.to_bytes()))
    }

    fn payload(expires_in_days: i64) -> serde_json::Value {
        let now = Utc::now();
        serde_json::json!({
            "v": 1,
            "licensee": "Acme",
            "edition": "embedded",
            "features": ["white_label", "sub_tenants"],
            "max_sub_tenants": 100,
            "issued_at": now.timestamp(),
            "expires_at": (now + chrono::Duration::days(expires_in_days)).to_rfc3339(),
        })
    }

    #[test]
    fn valid_license_verifies_offline() {
        let (signing, verifying) = keypair();
        let license = License::verify(&sign(&signing, &payload(30)), &verifying);
        let now = Utc::now();
        assert_eq!(license.status(now), LicenseStatus::Licensed);
        assert!(license.has_feature("white_label", now));
        let info = license.info(now);
        assert_eq!(info.edition.as_deref(), Some("embedded"));
        assert_eq!(info.licensee.as_deref(), Some("Acme"));
        assert_eq!(info.max_sub_tenants, Some(100));
    }

    #[test]
    fn wrong_key_tampering_and_garbage_are_invalid() {
        let (signing, _) = keypair();
        let (_, other) = keypair();
        let token = sign(&signing, &payload(30));
        let now = Utc::now();
        assert_eq!(
            License::verify(&token, &other).status(now),
            LicenseStatus::Invalid
        );

        let (signing2, verifying2) = keypair();
        let good = sign(&signing2, &payload(30));
        let mut parts: Vec<&str> = good.split('.').collect();
        let forged = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({"v":1,"licensee":"Evil","edition":"oem",
                "features":["white_label"],"issued_at":0,"expires_at":4_102_444_800_i64}))
            .unwrap(),
        );
        parts[1] = &forged;
        assert_eq!(
            License::verify(&parts.join("."), &verifying2).status(now),
            LicenseStatus::Invalid
        );
        for garbage in ["o8l1", "o8l1.a.b", "o8x1.a.b", "o8l1.a.b.c", "nonsense"] {
            assert_eq!(
                License::verify(garbage, &verifying2).status(now),
                LicenseStatus::Invalid,
                "{garbage}"
            );
        }
        assert_eq!(
            License::verify("  ", &verifying2).status(now),
            LicenseStatus::Unlicensed
        );
    }

    #[test]
    fn expired_license_grants_no_features() {
        let (signing, verifying) = keypair();
        let license = License::verify(&sign(&signing, &payload(-1)), &verifying);
        let now = Utc::now();
        assert_eq!(license.status(now), LicenseStatus::Expired);
        assert!(!license.has_feature("white_label", now));
        assert!(license.info(now).features.is_empty());
    }

    #[test]
    fn unsupported_version_is_invalid() {
        let (signing, verifying) = keypair();
        let mut p = payload(30);
        p["v"] = serde_json::json!(2);
        assert_eq!(
            License::verify(&sign(&signing, &p), &verifying).status(Utc::now()),
            LicenseStatus::Invalid
        );
    }

    #[test]
    fn soft_enforcement_verdicts() {
        let now = Utc::now();
        let unlicensed = License::unlicensed();
        assert_eq!(unlicensed.verdict(3, now), None);
        assert_eq!(unlicensed.verdict(4, now), Some("unlicensed"));

        let (signing, verifying) = keypair();
        let licensed = License::verify(&sign(&signing, &payload(30)), &verifying);
        assert_eq!(licensed.verdict(100, now), None);
        assert_eq!(licensed.verdict(101, now), Some("over_limit"));

        let mut no_sub = payload(30);
        no_sub["features"] = serde_json::json!(["white_label"]);
        let no_sub = License::verify(&sign(&signing, &no_sub), &verifying);
        assert_eq!(no_sub.verdict(4, now), Some("unlicensed"));
    }

    #[test]
    fn compiled_public_key_parses() {
        assert!(parse_public_key(EMBEDDED_PUBLIC_KEY_B64).is_ok());
        assert!(parse_public_key("not base64!").is_err());
        assert!(parse_public_key(&STANDARD.encode([1u8; 31])).is_err());
    }
}
