use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::{FcmConfig, PushError, PushProvider};

const TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(55 * 60);
const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
/// Allowed OAuth token URI for FCM service accounts.
const FCM_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
/// Maximum push token length. Tokens longer than this are rejected before
/// they can bloat JSON bodies or memory.
const MAX_TOKEN_LEN: usize = 512;
/// Maximum error response body we will include in a `PushError::Delivery`.
const MAX_ERROR_BODY_LEN: usize = 4 * 1024;

#[derive(serde::Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    token_uri: String,
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
}

struct CachedToken {
    token: String,
    created_at: Instant,
}

pub struct FcmProvider {
    client: reqwest::Client,
    project_id: String,
    service_account: ServiceAccount,
    cached_token: Mutex<Option<CachedToken>>,
}

impl FcmProvider {
    pub fn new(config: FcmConfig) -> Result<Self, PushError> {
        let service_account: ServiceAccount = serde_json::from_str(&config.service_account_json)
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
        {
            let cached = self.cached_token.lock().await;
            if let Some(ref ct) = *cached
                && ct.created_at.elapsed() < TOKEN_REFRESH_INTERVAL
            {
                return Ok(ct.token.clone());
            }
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
            return Err(PushError::Delivery(format!(
                "FCM token exchange returned error: {body}"
            )));
        }

        let token_resp: TokenResponse = resp
            .json()
            .await
            .map_err(|e| PushError::Delivery(format!("FCM token parse failed: {e}")))?;

        let access_token = token_resp.access_token.clone();
        let mut cached = self.cached_token.lock().await;
        *cached = Some(CachedToken {
            token: access_token.clone(),
            created_at: Instant::now(),
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
        let access_token = self.get_or_refresh_token().await?;
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

        let resp = self
            .client
            .post(&url)
            .header("authorization", format!("Bearer {access_token}"))
            .json(&payload)
            .send()
            .await
            .map_err(|e| PushError::Delivery(format!("FCM request failed: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            debug!(token = &token[..8.min(token.len())], "FCM silent push sent");
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();

        let is_invalid = body.contains("UNREGISTERED")
            || body.contains("NOT_FOUND")
            || serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.get("code")).cloned())
                .and_then(|c| c.as_i64())
                .is_some_and(|code| code == 404 || code == 410);
        if is_invalid {
            warn!(token = &token[..8.min(token.len())], "FCM token invalid");
            return Err(PushError::InvalidToken);
        }

        let preview = if body.len() > MAX_ERROR_BODY_LEN {
            format!("{}… (truncated)", &body[..MAX_ERROR_BODY_LEN])
        } else {
            body
        };
        Err(PushError::Delivery(format!(
            "FCM returned {status}: {preview}"
        )))
    }
}
