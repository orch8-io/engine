use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::{ApnsConfig, PushError, PushProvider};

const TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(50 * 60);
/// Maximum push token length. Tokens longer than this are rejected before
/// they can bloat URLs, JSON bodies, or memory.
const MAX_TOKEN_LEN: usize = 512;
/// Maximum error response body we will include in a `PushError::Delivery`.
const MAX_ERROR_BODY_LEN: usize = 4 * 1024;
/// M-21: retries beyond the first attempt for a transient failure (network
/// error, 5xx) or an auth rejection (403 — invalid/expired JWT) that a fresh
/// token can resolve. A permanently invalid device token (410) or any other
/// client error never retries.
const MAX_DELIVERY_ATTEMPTS: u32 = 3;

const fn retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(200_u64.saturating_mul(1 << attempt))
}

/// How `send_silent_push` should react to an APNs response status. Pulled out
/// as a pure function (no network I/O) so the M-21 retry/invalidate decision
/// is directly unit-testable without a mock APNs server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApnsOutcome {
    Success,
    /// 410: the device token itself is gone -- never retry.
    InvalidToken,
    /// 403: APNs rejected the JWT (expired/wrong key/revoked), not the
    /// device token -- invalidate the cached token and retry with a fresh one.
    AuthRejected,
    /// 5xx: transient server-side failure -- retry with backoff.
    Retryable,
    /// Any other non-2xx: not retryable.
    Permanent,
}

fn classify_apns_status(status: reqwest::StatusCode) -> ApnsOutcome {
    if status.is_success() {
        ApnsOutcome::Success
    } else if status.as_u16() == 410 {
        ApnsOutcome::InvalidToken
    } else if status.as_u16() == 403 {
        ApnsOutcome::AuthRejected
    } else if status.is_server_error() {
        ApnsOutcome::Retryable
    } else {
        ApnsOutcome::Permanent
    }
}

#[derive(serde::Serialize)]
struct Claims {
    iss: String,
    iat: i64,
}

struct CachedToken {
    token: String,
    created_at: Instant,
}

impl Drop for CachedToken {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.token.zeroize();
    }
}

pub struct ApnsProvider {
    client: reqwest::Client,
    encoding_key: EncodingKey,
    key_id: String,
    team_id: String,
    topic: String,
    base_url: &'static str,
    cached_token: Mutex<Option<CachedToken>>,
}

impl ApnsProvider {
    pub fn new(config: ApnsConfig) -> Result<Self, PushError> {
        let encoding_key = EncodingKey::from_ec_pem(config.key_pem.expose().as_bytes())
            .map_err(|e| PushError::Config(format!("invalid APNs key: {e}")))?;

        let base_url = if config.sandbox {
            "https://api.sandbox.push.apple.com"
        } else {
            "https://api.push.apple.com"
        };

        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| PushError::Config(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            client,
            encoding_key,
            key_id: config.key_id,
            team_id: config.team_id,
            topic: config.topic,
            base_url,
            cached_token: Mutex::new(None),
        })
    }

    async fn get_or_refresh_token(&self) -> Result<String, PushError> {
        let mut cached = self.cached_token.lock().await;
        if let Some(ref ct) = *cached
            && ct.created_at.elapsed() < TOKEN_REFRESH_INTERVAL
        {
            return Ok(ct.token.clone());
        }

        let now = chrono::Utc::now().timestamp();
        let claims = Claims {
            iss: self.team_id.clone(),
            iat: now,
        };
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());

        let token = encode(&header, &claims, &self.encoding_key)
            .map_err(|e| PushError::Config(format!("JWT signing failed: {e}")))?;

        *cached = Some(CachedToken {
            token: token.clone(),
            created_at: Instant::now(),
        });
        Ok(token)
    }
}

#[async_trait]
impl PushProvider for ApnsProvider {
    async fn send_silent_push(&self, token: &str, _platform: &str) -> Result<(), PushError> {
        if token.len() > MAX_TOKEN_LEN {
            return Err(PushError::InvalidToken);
        }

        let mut last_err = String::from("no attempts made");
        for attempt in 0..MAX_DELIVERY_ATTEMPTS {
            let jwt = self.get_or_refresh_token().await?;
            let encoded = urlencoding::encode(token);
            let url = format!("{}/3/device/{}", self.base_url, encoded);

            let payload = serde_json::json!({
                "aps": {
                    "content-available": 1
                }
            });

            let resp = match self
                .client
                .post(&url)
                .header("authorization", format!("bearer {jwt}"))
                .header("apns-push-type", "background")
                .header("apns-priority", "5")
                .header("apns-topic", &self.topic)
                .json(&payload)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("APNs request failed: {e}");
                    warn!(attempt, error = %last_err, "APNs request failed, will retry");
                    if attempt + 1 < MAX_DELIVERY_ATTEMPTS {
                        tokio::time::sleep(retry_backoff(attempt)).await;
                    }
                    continue;
                }
            };

            let status = resp.status();
            match classify_apns_status(status) {
                ApnsOutcome::Success => {
                    debug!(
                        token = crate::safe_prefix(token, 8),
                        "APNs silent push sent"
                    );
                    return Ok(());
                }
                ApnsOutcome::InvalidToken => {
                    warn!(
                        token = crate::safe_prefix(token, 8),
                        "APNs token invalid (410)"
                    );
                    return Err(PushError::InvalidToken);
                }
                ApnsOutcome::AuthRejected => {
                    // M-21: retrying with the SAME cached JWT would just
                    // fail identically until it naturally expires from the
                    // cache up to TOKEN_REFRESH_INTERVAL later; invalidate
                    // it so the next attempt (in this loop, or a subsequent
                    // call) forces a fresh sign.
                    warn!(
                        attempt,
                        "APNs rejected JWT (403) — invalidating cached token"
                    );
                    *self.cached_token.lock().await = None;
                    last_err = "APNs returned 403 (JWT rejected)".to_string();
                    if attempt + 1 < MAX_DELIVERY_ATTEMPTS {
                        continue;
                    }
                    break;
                }
                ApnsOutcome::Retryable if attempt + 1 < MAX_DELIVERY_ATTEMPTS => {
                    last_err = format!("APNs returned {status}");
                    warn!(attempt, status = %status, "APNs server error, will retry");
                    tokio::time::sleep(retry_backoff(attempt)).await;
                }
                ApnsOutcome::Retryable | ApnsOutcome::Permanent => {
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
                        "APNs returned {status}: {preview}"
                    )));
                }
            }
        }

        Err(PushError::Delivery(format!(
            "APNs delivery failed after {MAX_DELIVERY_ATTEMPTS} attempts: {last_err}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn classify_apns_status_success_on_2xx() {
        assert_eq!(classify_apns_status(StatusCode::OK), ApnsOutcome::Success);
        assert_eq!(
            classify_apns_status(StatusCode::NO_CONTENT),
            ApnsOutcome::Success
        );
    }

    /// M-21: 410 (device token gone) must never be retried.
    #[test]
    fn classify_apns_status_410_is_invalid_token() {
        assert_eq!(
            classify_apns_status(StatusCode::GONE),
            ApnsOutcome::InvalidToken
        );
    }

    /// M-21: 403 (JWT rejected) must be classified as auth-rejected so the
    /// caller invalidates the cached token and retries, rather than treated
    /// as a generic permanent failure.
    #[test]
    fn classify_apns_status_403_is_auth_rejected() {
        assert_eq!(
            classify_apns_status(StatusCode::FORBIDDEN),
            ApnsOutcome::AuthRejected
        );
    }

    /// M-21: 5xx must be retryable (transient).
    #[test]
    fn classify_apns_status_5xx_is_retryable() {
        assert_eq!(
            classify_apns_status(StatusCode::INTERNAL_SERVER_ERROR),
            ApnsOutcome::Retryable
        );
        assert_eq!(
            classify_apns_status(StatusCode::SERVICE_UNAVAILABLE),
            ApnsOutcome::Retryable
        );
    }

    /// Other 4xx (malformed request, unknown topic, etc.) are permanent —
    /// retrying an unchanged request would just fail identically.
    #[test]
    fn classify_apns_status_other_4xx_is_permanent() {
        assert_eq!(
            classify_apns_status(StatusCode::BAD_REQUEST),
            ApnsOutcome::Permanent
        );
        assert_eq!(
            classify_apns_status(StatusCode::UNAUTHORIZED),
            ApnsOutcome::Permanent
        );
    }
}
