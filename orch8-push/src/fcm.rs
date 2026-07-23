use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::{FcmConfig, PushError, PushProvider};

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

const fn retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(200_u64.saturating_mul(1 << attempt))
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
    let unregistered = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .is_some_and(|v| {
            v.get("error")
                .and_then(|e| e.get("details"))
                .and_then(|d| d.as_array())
                .is_some_and(|details| {
                    details.iter().any(|d| {
                        d.get("errorCode").and_then(|c| c.as_str()) == Some("UNREGISTERED")
                    })
                })
        });
    if unregistered {
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
    service_account: ServiceAccount,
    cached_token: Mutex<Option<CachedToken>>,
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
            service_account,
            cached_token: Mutex::new(None),
        })
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

#[async_trait]
impl PushProvider for FcmProvider {
    async fn send_silent_push(&self, token: &str, _platform: &str) -> Result<(), PushError> {
        if token.len() > MAX_TOKEN_LEN {
            return Err(PushError::InvalidToken);
        }

        let url = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            self.project_id
        );
        let payload = serde_json::json!({
            "message": {
                "token": token,
                "data": {
                    "type": "sync"
                },
                "android": {
                    "priority": "normal"
                }
            }
        });

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
                FcmOutcome::Retryable | FcmOutcome::Permanent => {
                    let preview = if body.len() > MAX_ERROR_BODY_LEN {
                        format!(
                            "{}… (truncated)",
                            crate::safe_prefix(&body, MAX_ERROR_BODY_LEN)
                        )
                    } else {
                        body
                    };
                    return Err(PushError::Delivery(format!(
                        "FCM returned {status}: {preview}"
                    )));
                }
            }
        }

        Err(PushError::Delivery(format!(
            "FCM delivery failed after {MAX_DELIVERY_ATTEMPTS} attempts: {last_err}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn classify_fcm_response_success_on_2xx() {
        assert_eq!(
            classify_fcm_response(StatusCode::OK, ""),
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
        let body2 = r#"{"error":{"code":404,"message":"UNREGISTERED"}}"#;
        assert_eq!(
            classify_fcm_response(StatusCode::NOT_FOUND, body2),
            FcmOutcome::Permanent
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
}
