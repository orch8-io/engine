use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::{FcmConfig, PushError, PushProvider, SignedWakeMetadata};

/// Fallback access-token lifetime used when the token endpoint omits
/// `expires_in` (Google currently returns 3600s). The cached token is
/// refreshed at 90% of the lifetime so it never expires mid-send.
const DEFAULT_TOKEN_LIFETIME_SECS: u64 = 3600;
const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
/// Allowed OAuth token URI for FCM service accounts.
const FCM_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
/// Maximum push token length. Tokens longer than this are rejected before
/// they can bloat JSON bodies or memory.
const MAX_TOKEN_LEN: usize = 512;
/// Maximum error response body we will include in a `PushError::Delivery`.
const MAX_ERROR_BODY_LEN: usize = 4 * 1024;
/// M-21: total delivery attempts (the initial try plus retries) for a
/// transient failure (network error, 429/quota, 5xx) or an auth rejection
/// (401/403 — invalid/expired OAuth token) that a fresh token can resolve.
/// An unregistered device token never retries.
const MAX_DELIVERY_ATTEMPTS: u32 = 3;

/// Minimum spacing between forced access-token refreshes triggered by a
/// 401/403, process-wide. A rejection that persists across fresh tokens is a
/// permission/configuration problem; re-minting per wake would only hammer
/// the OAuth endpoint.
const MIN_FORCED_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

const fn retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(200_u64.saturating_mul(1 << attempt))
}

fn error_preview(body: &str) -> String {
    if body.len() > MAX_ERROR_BODY_LEN {
        format!(
            "{}… (truncated)",
            crate::safe_prefix(body, MAX_ERROR_BODY_LEN)
        )
    } else {
        body.to_owned()
    }
}

/// How `send_silent_push` should react to an FCM response. Pulled out as a
/// pure function (no network I/O) so the M-21 retry/invalidate decision is
/// directly unit-testable without a mock FCM server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FcmOutcome {
    Success,
    /// The structured per-token error (`error.details[].errorCode ==
    /// "UNREGISTERED"`): the device token itself is gone -- never retry.
    InvalidToken,
    /// 401/403: FCM rejected the OAuth access token (expired/revoked/wrong
    /// scope), not the device token -- invalidate the cached token and retry
    /// with a fresh one.
    AuthRejected,
    /// 429/quota exhaustion or 5xx: transient failure -- retry with backoff.
    Retryable,
    /// Any other non-2xx: not retryable.
    Permanent,
}

fn classify_fcm_response(status: reqwest::StatusCode, body: &str) -> FcmOutcome {
    if status.is_success() {
        return FcmOutcome::Success;
    }
    // Only the structured per-token error identifies a dead device token.
    // A bare 404/410 (e.g. a mistyped project id, which FCM also answers
    // with 404 "Requested entity was not found") is a project-level failure:
    // treating it as InvalidToken would let one config typo wipe every
    // registered device token.
    let unregistered = serde_json::from_str::<serde_json::Value>(body).is_ok_and(|v| {
        v.get("error")
            .and_then(|e| e.get("details"))
            .and_then(|d| d.as_array())
            .is_some_and(|details| {
                details
                    .iter()
                    .any(|d| d.get("errorCode").and_then(|c| c.as_str()) == Some("UNREGISTERED"))
            })
    });
    if unregistered {
        return FcmOutcome::InvalidToken;
    }
    // Other token-level rejections that FCM reports under 400/403 and that no
    // amount of OAuth refreshing can fix: a token minted for a different
    // sender (403 SENDER_ID_MISMATCH) or malformed (INVALID_ARGUMENT naming
    // the registration token). Quarantine the token instead of discarding the
    // process-wide access token and retrying.
    if fcm_token_level_rejection(body) {
        return FcmOutcome::InvalidToken;
    }
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return FcmOutcome::AuthRejected;
    }
    // 429 / quota exhaustion is transient at any real fan-out volume: back
    // off and retry rather than failing the push outright.
    if status.as_u16() == 429
        || body.contains("RESOURCE_EXHAUSTED")
        || body.contains("QUOTA_EXCEEDED")
    {
        return FcmOutcome::Retryable;
    }
    if status.is_server_error() {
        return FcmOutcome::Retryable;
    }
    FcmOutcome::Permanent
}

fn fcm_token_level_rejection(body: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let Some(error) = value.get("error") else {
        return false;
    };
    let detail_code = error
        .get("details")
        .and_then(|d| d.as_array())
        .is_some_and(|details| {
            details.iter().any(|d| {
                matches!(
                    d.get("errorCode").and_then(|c| c.as_str()),
                    Some("SENDER_ID_MISMATCH")
                )
            })
        });
    if detail_code {
        return true;
    }
    let status = error.get("status").and_then(|s| s.as_str());
    let message = error
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    status == Some("INVALID_ARGUMENT") && message.to_ascii_lowercase().contains("registration token")
}

#[derive(serde::Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    token_uri: String,
}

impl Drop for ServiceAccount {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.private_key.zeroize();
    }
}

#[derive(serde::Serialize)]
struct JwtClaims {
    iss: String,
    scope: String,
    aud: String,
    iat: i64,
    exp: i64,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Lifetime in seconds; absent from non-Google token endpoints.
    #[serde(default)]
    expires_in: Option<u64>,
}

struct CachedToken {
    token: String,
    created_at: Instant,
    /// How long the token may be served from cache (90% of its lifetime).
    ttl: Duration,
}

impl Drop for CachedToken {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.token.zeroize();
    }
}

pub struct FcmProvider {
    client: reqwest::Client,
    project_id: String,
    message_base_url: String,
    service_account: ServiceAccount,
    cached_token: Mutex<Option<CachedToken>>,
    /// When a 401/403 last forced the cached token to be discarded.
    last_forced_refresh: std::sync::Mutex<Option<Instant>>,
}

impl FcmProvider {
    pub fn new(config: FcmConfig) -> Result<Self, PushError> {
        let service_account: ServiceAccount =
            serde_json::from_str(config.service_account_json.expose())
                .map_err(|e| PushError::Config(format!("invalid FCM service account JSON: {e}")))?;
        if service_account.token_uri != FCM_TOKEN_URI {
            return Err(PushError::Config(format!(
                "FCM token_uri must be {FCM_TOKEN_URI}, got {}",
                service_account.token_uri
            )));
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| PushError::Config(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            client,
            project_id: config.project_id,
            message_base_url: "https://fcm.googleapis.com".into(),
            service_account,
            cached_token: Mutex::new(None),
            last_forced_refresh: std::sync::Mutex::new(None),
        })
    }

    /// Whether a rejected access token may be discarded now (and, if so,
    /// record the refresh).
    fn begin_forced_refresh(&self) -> bool {
        let mut last = self
            .last_forced_refresh
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if last.is_some_and(|at| at.elapsed() < MIN_FORCED_REFRESH_INTERVAL) {
            return false;
        }
        *last = Some(Instant::now());
        true
    }

    async fn get_or_refresh_token(&self) -> Result<String, PushError> {
        // Keep the refresh mutex through the exchange. Token refresh is rare,
        // and this coalesces a burst of cache misses into one OAuth request.
        let mut cached = self.cached_token.lock().await;
        if let Some(ref ct) = *cached
            && ct.created_at.elapsed() < ct.ttl
        {
            return Ok(ct.token.clone());
        }

        let now = chrono::Utc::now().timestamp();
        let claims = JwtClaims {
            iss: self.service_account.client_email.clone(),
            scope: FCM_SCOPE.to_string(),
            aud: self.service_account.token_uri.clone(),
            iat: now,
            exp: now + 3600,
        };

        let encoding_key =
            EncodingKey::from_rsa_pem(self.service_account.private_key.as_bytes())
                .map_err(|e| PushError::Config(format!("invalid FCM private key: {e}")))?;

        let jwt = encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
            .map_err(|e| PushError::Config(format!("JWT signing failed: {e}")))?;

        let resp = self
            .client
            .post(&self.service_account.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await
            .map_err(|e| PushError::Delivery(format!("FCM token exchange failed: {e}")))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            let preview = if body.len() > MAX_ERROR_BODY_LEN {
                format!(
                    "{}… (truncated)",
                    crate::safe_prefix(&body, MAX_ERROR_BODY_LEN)
                )
            } else {
                body
            };
            return Err(PushError::Delivery(format!(
                "FCM token exchange returned error: {preview}"
            )));
        }

        let token_resp: TokenResponse = resp
            .json()
            .await
            .map_err(|e| PushError::Delivery(format!("FCM token parse failed: {e}")))?;

        let access_token = token_resp.access_token;
        let ttl = Duration::from_secs(
            token_resp
                .expires_in
                .unwrap_or(DEFAULT_TOKEN_LIFETIME_SECS)
                .saturating_mul(9)
                / 10,
        );
        *cached = Some(CachedToken {
            token: access_token.clone(),
            created_at: Instant::now(),
            ttl,
        });
        Ok(access_token)
    }
}

impl FcmProvider {
    fn wake_payload(
        token: &str,
        metadata: Option<&SignedWakeMetadata>,
    ) -> Result<serde_json::Value, PushError> {
        let mut data =
            serde_json::Map::from_iter([("type".into(), serde_json::Value::String("sync".into()))]);
        if let Some(metadata) = metadata {
            data.insert(
                "orch8".into(),
                serde_json::Value::String(serde_json::to_string(metadata).map_err(|error| {
                    PushError::Permanent(format!("serialize signed wake: {error}"))
                })?),
            );
        }
        Ok(serde_json::json!({
            "message": {
                "token": token,
                "data": data,
                "android": {
                    "priority": "normal",
                    // Same wake semantics as APNs: one queued wake per device,
                    // dropped if the device cannot take it within the TTL.
                    "collapse_key": crate::WAKE_COLLAPSE_ID,
                    "ttl": format!("{}s", crate::WAKE_TTL_SECS)
                }
            }
        }))
    }

    async fn send(
        &self,
        token: &str,
        metadata: Option<&SignedWakeMetadata>,
    ) -> Result<(), PushError> {
        if token.len() > MAX_TOKEN_LEN {
            return Err(PushError::InvalidToken);
        }

        let url = format!(
            "{}/v1/projects/{}/messages:send",
            self.message_base_url, self.project_id
        );
        let payload = Self::wake_payload(token, metadata)?;

        let mut last_err = String::from("no attempts made");
        for attempt in 0..MAX_DELIVERY_ATTEMPTS {
            let access_token = match self.get_or_refresh_token().await {
                Ok(token) => token,
                // Misconfigured credentials will never succeed — fail fast.
                Err(e @ PushError::Config(_)) => return Err(e),
                // A transient exchange failure (network blip, Google 5xx) is
                // one delivery attempt: back off and retry instead of
                // aborting the push.
                Err(e) => {
                    last_err = e.to_string();
                    warn!(attempt, error = %last_err, "FCM token exchange failed, will retry");
                    if attempt + 1 < MAX_DELIVERY_ATTEMPTS {
                        tokio::time::sleep(retry_backoff(attempt)).await;
                        continue;
                    }
                    break;
                }
            };

            let resp = match self
                .client
                .post(&url)
                .header("authorization", format!("Bearer {access_token}"))
                .json(&payload)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("FCM request failed: {e}");
                    warn!(attempt, error = %last_err, "FCM request failed, will retry");
                    if attempt + 1 < MAX_DELIVERY_ATTEMPTS {
                        tokio::time::sleep(retry_backoff(attempt)).await;
                    }
                    continue;
                }
            };

            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();

            match classify_fcm_response(status, &body) {
                FcmOutcome::Success => {
                    debug!(token = crate::safe_prefix(token, 8), "FCM silent push sent");
                    return Ok(());
                }
                FcmOutcome::InvalidToken => {
                    warn!(token = crate::safe_prefix(token, 8), "FCM token invalid");
                    return Err(PushError::InvalidToken);
                }
                FcmOutcome::AuthRejected => {
                    // M-21: retrying with the SAME cached token would fail
                    // identically until it naturally expires from the cache
                    // up to TOKEN_REFRESH_INTERVAL later; invalidate it so
                    // the next attempt (in this loop, or a subsequent call)
                    // forces a fresh token exchange.
                    // Forced refreshes are rate-limited across the whole
                    // process: the first rejection in a window may re-mint the
                    // token (and the rest of this call keeps refreshing), but
                    // a later wake hitting the same rejection inside the
                    // window backs off through the outbox's retry schedule
                    // instead of hammering the OAuth endpoint per wake.
                    if attempt == 0 && !self.begin_forced_refresh() {
                        warn!(status = %status, "FCM rejected the access token; forced refresh rate-limited");
                        return Err(PushError::Retryable(format!(
                            "FCM returned {status} (access token rejected; refresh rate-limited): {}",
                            error_preview(&body)
                        )));
                    }
                    warn!(
                        attempt,
                        status = %status,
                        "FCM rejected access token — invalidating cached token"
                    );
                    *self.cached_token.lock().await = None;
                    last_err = format!("FCM returned {status} (access token rejected)");
                    if attempt + 1 < MAX_DELIVERY_ATTEMPTS {
                        continue;
                    }
                    break;
                }
                FcmOutcome::Retryable if attempt + 1 < MAX_DELIVERY_ATTEMPTS => {
                    last_err = format!("FCM returned {status}");
                    warn!(attempt, status = %status, "FCM server error, will retry");
                    tokio::time::sleep(retry_backoff(attempt)).await;
                }
                outcome @ (FcmOutcome::Retryable | FcmOutcome::Permanent) => {
                    let preview = error_preview(&body);
                    let message = format!("FCM returned {status}: {preview}");
                    return Err(if outcome == FcmOutcome::Retryable {
                        PushError::Retryable(message)
                    } else {
                        PushError::Permanent(message)
                    });
                }
            }
        }

        Err(PushError::Retryable(format!(
            "FCM delivery failed after {MAX_DELIVERY_ATTEMPTS} attempts: {last_err}"
        )))
    }
}

#[async_trait]
impl PushProvider for FcmProvider {
    async fn send_silent_push(&self, token: &str, _platform: &str) -> Result<(), PushError> {
        self.send(token, None).await
    }

    async fn send_signed_wake(
        &self,
        token: &str,
        _platform: &str,
        metadata: &SignedWakeMetadata,
    ) -> Result<(), PushError> {
        self.send(token, Some(metadata)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use ed25519_dalek::SigningKey;
    use reqwest::StatusCode;

    #[test]
    fn wake_payload_embeds_the_complete_signed_metadata() {
        let issued_at = Utc::now();
        let wake = SignedWakeMetadata::sign(
            "tenant-a",
            "device-a",
            "command-a",
            "wake-key-1",
            &SigningKey::from_bytes(&[7_u8; 32]),
            issued_at,
            issued_at + ChronoDuration::minutes(5),
        )
        .expect("valid signed wake");

        let payload = FcmProvider::wake_payload("token-a", Some(&wake)).expect("valid payload");
        let encoded = payload["message"]["data"]["orch8"]
            .as_str()
            .expect("signed wake string");
        let decoded: SignedWakeMetadata = serde_json::from_str(encoded).expect("signed wake JSON");

        assert_eq!(decoded, wake);
    }

    #[test]
    fn classify_fcm_response_success_on_2xx() {
        assert_eq!(
            classify_fcm_response(StatusCode::OK, ""),
            FcmOutcome::Success
        );
        assert_eq!(
            classify_fcm_response(StatusCode::CREATED, ""),
            FcmOutcome::Success
        );
        // A 2xx is never misread as retryable even if the body mentions quota.
        let quota_looking = r#"{"name":"projects/p/messages/1","status":"RESOURCE_EXHAUSTED"}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::OK, quota_looking),
            FcmOutcome::Success
        );
    }

    /// M-21: only the structured per-token error
    /// (`error.details[].errorCode == "UNREGISTERED"`) identifies a dead
    /// device token, so it never gets retried.
    #[test]
    fn classify_fcm_response_unregistered_body_is_invalid_token() {
        let body = r#"{"error":{"code":404,"message":"Requested entity was not found.","status":"NOT_FOUND","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::NOT_FOUND, body),
            FcmOutcome::InvalidToken
        );
    }

    /// A bare 404/410 without the structured UNREGISTERED detail is a
    /// project-level failure (e.g. mistyped project id, which FCM answers
    /// with exactly this body) — it must NOT be treated as an invalid token,
    /// or one config typo would wipe the whole device registry.
    #[test]
    fn classify_fcm_response_bare_404_is_permanent_not_invalid_token() {
        let body =
            r#"{"error":{"status":"NOT_FOUND","message":"Requested entity was not found."}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::NOT_FOUND, body),
            FcmOutcome::Permanent
        );
        let cases = [
            (
                "UNREGISTERED only in the message string",
                StatusCode::NOT_FOUND,
                r#"{"error":{"code":404,"message":"UNREGISTERED"}}"#,
            ),
            (
                "details is an object, not an array",
                StatusCode::NOT_FOUND,
                r#"{"error":{"details":{"errorCode":"UNREGISTERED"}}}"#,
            ),
            (
                "malformed JSON body",
                StatusCode::NOT_FOUND,
                "not json at all",
            ),
            ("bare 410", StatusCode::GONE, ""),
        ];
        for (case, status, body) in cases {
            assert_eq!(
                classify_fcm_response(status, body),
                FcmOutcome::Permanent,
                "{case}"
            );
        }
    }

    /// The structured dead-token signal takes precedence over 429/5xx retry
    /// handling: retrying a token FCM says is unregistered can never succeed.
    #[test]
    fn classify_fcm_response_unregistered_detail_wins_over_retryable_status() {
        let body = r#"{"error":{"code":429,"details":[{"errorCode":"UNREGISTERED"}]}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::TOO_MANY_REQUESTS, body),
            FcmOutcome::InvalidToken
        );
    }

    /// 429 / quota exhaustion is transient — the vendor rate limit lifts, so
    /// it must retry with backoff rather than fail the push outright.
    #[test]
    fn classify_fcm_response_429_and_quota_are_retryable() {
        assert_eq!(
            classify_fcm_response(StatusCode::TOO_MANY_REQUESTS, "{}"),
            FcmOutcome::Retryable
        );
        let body = r#"{"error":{"code":429,"message":"Quota exceeded for quota metric.","status":"RESOURCE_EXHAUSTED"}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::TOO_MANY_REQUESTS, body),
            FcmOutcome::Retryable
        );
        let body2 = r#"{"error":{"code":403,"message":"quota","status":"PERMISSION_DENIED","details":[{"errorCode":"QUOTA_EXCEEDED"}]}}"#;
        // Quota detail wins even on a 403 that would otherwise be auth.
        assert_eq!(
            classify_fcm_response(StatusCode::FORBIDDEN, body2),
            FcmOutcome::AuthRejected
        );
        let body3 = r#"{"error":{"code":500,"status":"QUOTA_EXCEEDED"}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::INTERNAL_SERVER_ERROR, body3),
            FcmOutcome::Retryable
        );
        // Quota exhaustion on a non-auth 4xx still backs off.
        let body4 = r#"{"error":{"code":400,"status":"RESOURCE_EXHAUSTED"}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::BAD_REQUEST, body4),
            FcmOutcome::Retryable
        );
    }

    /// M-21: 401/403 (OAuth access token rejected) must be classified as
    /// auth-rejected so the caller invalidates the cached token and retries,
    /// rather than treated as a generic permanent failure.
    #[test]
    fn classify_fcm_response_401_and_403_are_auth_rejected() {
        assert_eq!(
            classify_fcm_response(StatusCode::UNAUTHORIZED, "{}"),
            FcmOutcome::AuthRejected
        );
        assert_eq!(
            classify_fcm_response(StatusCode::FORBIDDEN, "{}"),
            FcmOutcome::AuthRejected
        );
    }

    /// M-21: 5xx must be retryable (transient).
    #[test]
    fn classify_fcm_response_5xx_is_retryable() {
        assert_eq!(
            classify_fcm_response(StatusCode::INTERNAL_SERVER_ERROR, "{}"),
            FcmOutcome::Retryable
        );
        assert_eq!(
            classify_fcm_response(StatusCode::SERVICE_UNAVAILABLE, "{}"),
            FcmOutcome::Retryable
        );
        assert_eq!(
            classify_fcm_response(StatusCode::BAD_GATEWAY, "{}"),
            FcmOutcome::Retryable
        );
    }

    /// Other 4xx (malformed request, etc.) are permanent — retrying an
    /// unchanged request would just fail identically.
    #[test]
    fn classify_fcm_response_other_4xx_is_permanent() {
        let body = r#"{"error":{"code":400,"message":"INVALID_ARGUMENT"}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::BAD_REQUEST, body),
            FcmOutcome::Permanent
        );
    }

    #[test]
    fn unsigned_wake_payload_is_a_data_only_sync_message() {
        let payload = FcmProvider::wake_payload("token-a", None).expect("valid payload");
        let message = &payload["message"];

        assert_eq!(message["token"], "token-a");
        assert_eq!(message["data"]["type"], "sync");
        assert!(message.get("notification").is_none(), "silent wake only");
        assert!(message["data"].get("orch8").is_none());
    }

    /// Error bodies are capped at `MAX_ERROR_BODY_LEN` bytes, cut on a char
    /// boundary, and marked as truncated; bodies within the cap pass through.
    #[test]
    fn error_preview_caps_bodies_on_a_char_boundary() {
        let at_limit = "a".repeat(MAX_ERROR_BODY_LEN);
        assert_eq!(error_preview(&at_limit), at_limit);

        let over = "a".repeat(MAX_ERROR_BODY_LEN + 1);
        assert_eq!(error_preview(&over), format!("{at_limit}… (truncated)"));

        // 'é' is two bytes, so a byte cut at the limit could split one.
        let multibyte = "é".repeat(MAX_ERROR_BODY_LEN);
        let preview = error_preview(&multibyte);
        let prefix = preview
            .strip_suffix("… (truncated)")
            .expect("oversized body is marked truncated");
        assert!(prefix.len() <= MAX_ERROR_BODY_LEN);
        assert!(prefix.chars().all(|c| c == 'é'));
    }

    #[test]
    fn retry_backoff_saturates_instead_of_overflowing() {
        assert_eq!(retry_backoff(63), Duration::from_millis(u64::MAX));
    }

    #[test]
    fn malformed_service_account_json_is_a_config_error() {
        let cases = [
            ("not JSON", "{not json"),
            (
                "missing token_uri",
                r#"{"client_email":"x@p.iam.gserviceaccount.com","private_key":"k"}"#,
            ),
        ];
        for (case, json) in cases {
            let result = FcmProvider::new(FcmConfig {
                project_id: "p".into(),
                service_account_json: json.to_owned().into(),
            });
            let Err(PushError::Config(message)) = result else {
                panic!("{case}: expected a config error");
            };
            assert!(
                message.contains("invalid FCM service account JSON"),
                "{case}: {message}"
            );
        }
    }
}

#[cfg(test)]
#[path = "fcm_protocol_tests.rs"]
mod fcm_protocol_tests;
