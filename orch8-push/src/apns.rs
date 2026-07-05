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

#[derive(serde::Serialize)]
struct Claims {
    iss: String,
    iat: i64,
}

struct CachedToken {
    token: String,
    created_at: Instant,
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
        let encoding_key = EncodingKey::from_ec_pem(config.key_pem.as_bytes())
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
        let jwt = self.get_or_refresh_token().await?;
        let encoded = urlencoding::encode(token);
        let url = format!("{}/3/device/{}", self.base_url, encoded);

        let payload = serde_json::json!({
            "aps": {
                "content-available": 1
            }
        });

        let resp = self
            .client
            .post(&url)
            .header("authorization", format!("bearer {jwt}"))
            .header("apns-push-type", "background")
            .header("apns-priority", "5")
            .header("apns-topic", &self.topic)
            .json(&payload)
            .send()
            .await
            .map_err(|e| PushError::Delivery(format!("APNs request failed: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            debug!(
                token = &token[..8.min(token.len())],
                "APNs silent push sent"
            );
            return Ok(());
        }

        if status.as_u16() == 410 {
            warn!(
                token = &token[..8.min(token.len())],
                "APNs token invalid (410)"
            );
            return Err(PushError::InvalidToken);
        }

        let body = resp.text().await.unwrap_or_default();
        let preview = if body.len() > MAX_ERROR_BODY_LEN {
            format!("{}… (truncated)", &body[..MAX_ERROR_BODY_LEN])
        } else {
            body
        };
        Err(PushError::Delivery(format!(
            "APNs returned {status}: {preview}"
        )))
    }
}
