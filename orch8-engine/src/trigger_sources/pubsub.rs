//! GCP Pub/Sub trigger (`trigger_type: "pubsub"`, engine feature `pubsub`).
//!
//! Uses the Pub/Sub REST API (`subscriptions.pull` / `acknowledge` /
//! `modifyAckDeadline`) over the engine's existing `reqwest` client. Auth is
//! an `OAuth2` access token from, in order: an inline/`credentials://`
//! service-account JSON (`credentials_json`), the file named by
//! `GOOGLE_APPLICATION_CREDENTIALS`, or the GCE/GKE metadata server. Setting
//! `endpoint` to an emulator URL (or `PUBSUB_EMULATOR_HOST`) disables auth.
//!
//! Each message creates one instance (idempotency key = Pub/Sub
//! `messageId`) and is acked only after the create commits. A failed create
//! is nacked (`ackDeadlineSeconds: 0`) so Pub/Sub redelivers it (and applies
//! the subscription's dead-letter policy). Pub/Sub load-balances pulls, so
//! every engine node may consume.
//!
//! ```json
//! {
//!   "subscription": "projects/my-proj/subscriptions/orders-orch8",
//!   "credentials_json": "credentials://gcp-sa",   // optional
//!   "max_messages": 50,
//!   "endpoint": "http://localhost:8085"           // optional (emulator)
//! }
//! ```

use serde_json::{Value, json};

use super::{opt_str, opt_u64, req_str, require_object};

/// Validated `pubsub` trigger config.
#[derive(Clone, PartialEq, Eq)]
pub struct PubSubConfig {
    /// Full resource name `projects/{p}/subscriptions/{s}`.
    pub subscription: String,
    pub credentials_json: Option<String>,
    pub max_messages: u64,
    /// API base URL; defaults to `https://pubsub.googleapis.com`.
    pub endpoint: Option<String>,
}

impl std::fmt::Debug for PubSubConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubConfig")
            .field("subscription", &self.subscription)
            .field(
                "credentials_json",
                &self.credentials_json.as_ref().map(|_| "<redacted>"),
            )
            .field("max_messages", &self.max_messages)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~' | '+' | '%'))
}

impl PubSubConfig {
    pub fn parse(config: &Value) -> Result<Self, String> {
        require_object(config, "pubsub")?;
        let raw = req_str(config, "subscription")?;
        let subscription = if raw.starts_with("projects/") {
            raw
        } else {
            let project = req_str(config, "project").map_err(|_| {
                "'subscription' must be 'projects/{p}/subscriptions/{s}' or 'project' must be set"
                    .to_string()
            })?;
            format!("projects/{project}/subscriptions/{raw}")
        };
        let parts: Vec<&str> = subscription.split('/').collect();
        let valid = matches!(
            parts.as_slice(),
            ["projects", p, "subscriptions", s] if valid_segment(p) && valid_segment(s)
        );
        if !valid {
            return Err(format!("invalid subscription name '{subscription}'"));
        }
        let endpoint = match opt_str(config, "endpoint")? {
            Some(e) => {
                let url =
                    url::Url::parse(&e).map_err(|err| format!("'endpoint' invalid: {err}"))?;
                if !matches!(url.scheme(), "http" | "https") {
                    return Err("'endpoint' must be http(s)".into());
                }
                Some(e.trim_end_matches('/').to_string())
            }
            None => None,
        };
        Ok(Self {
            subscription,
            credentials_json: opt_str(config, "credentials_json")?,
            max_messages: opt_u64(config, "max_messages", 50, 1, 1000)?,
            endpoint,
        })
    }
}

/// Map one `receivedMessages[]` entry to `(data, meta, source_id, ack_id)`.
/// Returns `None` for entries missing `ackId` or `messageId`.
#[must_use]
pub fn map_received(
    subscription: &str,
    received: &Value,
) -> Option<(Value, Value, String, String)> {
    use base64::Engine as _;
    let ack_id = received.get("ackId")?.as_str()?.to_string();
    let message = received.get("message")?;
    let message_id = message.get("messageId")?.as_str()?.to_string();
    let bytes = message
        .get("data")
        .and_then(Value::as_str)
        .map(|d| {
            base64::engine::general_purpose::STANDARD
                .decode(d)
                .unwrap_or_default()
        })
        .unwrap_or_default();
    let data = if bytes.is_empty() {
        Value::Null
    } else {
        super::decode_payload(&bytes)
    };
    let meta = json!({
        "subscription": subscription,
        "message_id": message_id,
        "attributes": message.get("attributes").cloned().unwrap_or(Value::Null),
        "publish_time": message.get("publishTime").cloned().unwrap_or(Value::Null),
        "ordering_key": message.get("orderingKey").cloned().unwrap_or(Value::Null),
        "delivery_attempt": received.get("deliveryAttempt").cloned().unwrap_or(Value::Null),
    });
    Some((data, meta, message_id, ack_id))
}

#[cfg(feature = "pubsub")]
pub use listener::run;

#[cfg(feature = "pubsub")]
mod listener {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info, warn};

    use orch8_storage::StorageBackend;
    use orch8_types::trigger::TriggerDef;

    use super::PubSubConfig;
    use crate::error::EngineError;
    use crate::trigger_sources::{deliver, failure_backoff, resolved_config, sleep_or_cancel};

    const SCOPE: &str = "https://www.googleapis.com/auth/pubsub";
    const DEFAULT_ENDPOINT: &str = "https://pubsub.googleapis.com";
    const METADATA_TOKEN_URL: &str = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

    #[derive(serde::Deserialize)]
    struct ServiceAccount {
        client_email: String,
        private_key: String,
        #[serde(default = "default_token_uri")]
        token_uri: String,
    }

    fn default_token_uri() -> String {
        "https://oauth2.googleapis.com/token".into()
    }

    #[derive(serde::Serialize)]
    struct Claims<'a> {
        iss: &'a str,
        scope: &'a str,
        aud: &'a str,
        iat: i64,
        exp: i64,
    }

    #[derive(serde::Deserialize)]
    struct TokenResponse {
        access_token: String,
        #[serde(default)]
        expires_in: Option<u64>,
    }

    enum Auth {
        None,
        ServiceAccount(ServiceAccount),
        Metadata,
    }

    struct TokenSource {
        http: reqwest::Client,
        auth: Auth,
        cached: Option<(String, Instant, Duration)>,
    }

    impl TokenSource {
        async fn token(&mut self) -> Result<Option<String>, String> {
            if matches!(self.auth, Auth::None) {
                return Ok(None);
            }
            if let Some((t, at, ttl)) = &self.cached
                && at.elapsed() < *ttl
            {
                return Ok(Some(t.clone()));
            }
            let resp = match &self.auth {
                Auth::None => return Ok(None),
                Auth::ServiceAccount(sa) => {
                    let now = chrono::Utc::now().timestamp();
                    let claims = Claims {
                        iss: &sa.client_email,
                        scope: SCOPE,
                        aud: &sa.token_uri,
                        iat: now,
                        exp: now + 3600,
                    };
                    let key = jsonwebtoken::EncodingKey::from_rsa_pem(sa.private_key.as_bytes())
                        .map_err(|e| format!("invalid service account key: {e}"))?;
                    let jwt = jsonwebtoken::encode(
                        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
                        &claims,
                        &key,
                    )
                    .map_err(|e| format!("JWT signing failed: {e}"))?;
                    self.http
                        .post(&sa.token_uri)
                        .form(&[
                            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                            ("assertion", jwt.as_str()),
                        ])
                        .send()
                        .await
                }
                Auth::Metadata => {
                    self.http
                        .get(METADATA_TOKEN_URL)
                        .header("Metadata-Flavor", "Google")
                        .send()
                        .await
                }
            }
            .map_err(|e| format!("token request failed: {e}"))?;
            if !resp.status().is_success() {
                return Err(format!("token endpoint returned {}", resp.status()));
            }
            let body: TokenResponse = resp
                .json()
                .await
                .map_err(|e| format!("token response invalid: {e}"))?;
            let ttl = Duration::from_secs(body.expires_in.unwrap_or(3600).saturating_mul(9) / 10);
            self.cached = Some((body.access_token.clone(), Instant::now(), ttl));
            Ok(Some(body.access_token))
        }
    }

    fn auth_for(cfg: &PubSubConfig, emulator: bool) -> Result<Auth, EngineError> {
        if emulator {
            return Ok(Auth::None);
        }
        let json = match &cfg.credentials_json {
            Some(j) => Some(j.clone()),
            None => match std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
                Ok(path) => Some(std::fs::read_to_string(&path).map_err(|e| {
                    EngineError::InvalidConfig(format!(
                        "pubsub: cannot read GOOGLE_APPLICATION_CREDENTIALS: {e}"
                    ))
                })?),
                Err(_) => None,
            },
        };
        match json {
            Some(j) => serde_json::from_str::<ServiceAccount>(&j)
                .map(Auth::ServiceAccount)
                .map_err(|e| {
                    EngineError::InvalidConfig(format!(
                        "pubsub: credentials must be a service-account JSON key: {e}"
                    ))
                }),
            None => Ok(Auth::Metadata),
        }
    }

    async fn call(
        http: &reqwest::Client,
        tokens: &mut TokenSource,
        url: &str,
        body: &Value,
    ) -> Result<Value, String> {
        let mut req = http.post(url).json(body);
        if let Some(t) = tokens.token().await? {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            tokens.cached = None;
        }
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            let preview: String = text.chars().take(300).collect();
            return Err(format!("pubsub returned {status}: {preview}"));
        }
        resp.json().await.map_err(|e| e.to_string())
    }

    /// Run the Pub/Sub listener until cancelled.
    pub async fn run(
        storage: Arc<dyn StorageBackend>,
        trigger: TriggerDef,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let config = resolved_config(storage.as_ref(), &trigger).await?;
        let cfg = PubSubConfig::parse(&config)
            .map_err(|e| EngineError::InvalidConfig(format!("pubsub: {e}")))?;
        let emulator_env = std::env::var("PUBSUB_EMULATOR_HOST").ok();
        let emulator = cfg.endpoint.is_some() || emulator_env.is_some();
        let base = cfg
            .endpoint
            .clone()
            .or_else(|| emulator_env.map(|h| format!("http://{h}")))
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(90))
            .build()
            .map_err(|e| EngineError::InvalidConfig(format!("pubsub http client: {e}")))?;
        let mut tokens = TokenSource {
            http: http.clone(),
            auth: auth_for(&cfg, emulator && cfg.credentials_json.is_none())?,
            cached: None,
        };
        let pull_url = format!("{base}/v1/{}:pull", cfg.subscription);
        let ack_url = format!("{base}/v1/{}:acknowledge", cfg.subscription);
        let nack_url = format!("{base}/v1/{}:modifyAckDeadline", cfg.subscription);
        let slug = trigger.slug.clone();
        info!(slug, subscription = %cfg.subscription, "pubsub trigger listener active");

        let pull_body = json!({"maxMessages": cfg.max_messages});
        let mut failures = 0u32;
        loop {
            let pulled = tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                r = call(&http, &mut tokens, &pull_url, &pull_body) => r,
            };
            let received = match pulled {
                Ok(v) => {
                    failures = 0;
                    v.get("receivedMessages")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                }
                Err(e) => {
                    failures = failures.saturating_add(1);
                    warn!(slug, error = %e, "pubsub pull failed");
                    if sleep_or_cancel(&cancel, failure_backoff(failures)).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let mut acks = Vec::new();
            let mut nacks = Vec::new();
            for entry in &received {
                let Some((data, meta, message_id, ack_id)) =
                    super::map_received(&cfg.subscription, entry)
                else {
                    warn!(slug, "pubsub message without ackId/messageId, skipping");
                    continue;
                };
                match deliver(storage.as_ref(), &trigger, data, meta, &message_id).await {
                    Ok(_) => acks.push(ack_id),
                    Err(e) => {
                        error!(slug, message_id, error = %e, "pubsub message not delivered; nacking");
                        nacks.push(ack_id);
                    }
                }
            }
            if !acks.is_empty()
                && let Err(e) = call(&http, &mut tokens, &ack_url, &json!({"ackIds": acks})).await
            {
                warn!(slug, error = %e, "pubsub ack failed; redelivery will be deduplicated");
            }
            if !nacks.is_empty()
                && let Err(e) = call(
                    &http,
                    &mut tokens,
                    &nack_url,
                    &json!({"ackIds": nacks, "ackDeadlineSeconds": 0}),
                )
                .await
            {
                warn!(slug, error = %e, "pubsub nack failed; message redelivers after its ack deadline");
            }
            if received.is_empty() && sleep_or_cancel(&cancel, Duration::from_millis(200)).await {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_and_short_subscription_names() {
        let c =
            PubSubConfig::parse(&json!({"subscription": "projects/p1/subscriptions/s1"})).unwrap();
        assert_eq!(c.subscription, "projects/p1/subscriptions/s1");
        assert_eq!(c.max_messages, 50);
        let c = PubSubConfig::parse(&json!({
            "project": "p1", "subscription": "s1",
            "endpoint": "http://localhost:8085/", "max_messages": 5,
            "credentials_json": "{\"secret\": true}"
        }))
        .unwrap();
        assert_eq!(c.subscription, "projects/p1/subscriptions/s1");
        assert_eq!(c.endpoint.as_deref(), Some("http://localhost:8085"));
        assert!(!format!("{c:?}").contains("secret"));
    }

    #[test]
    fn rejects_invalid_configs() {
        for bad in [
            json!({}),
            json!({"subscription": "s1"}),
            json!({"subscription": "projects/p/topics/t"}),
            json!({"subscription": "projects//subscriptions/s"}),
            json!({"subscription": "projects/p/subscriptions/s/extra"}),
            json!({"subscription": "projects/p/subscriptions/s", "endpoint": "ftp://x"}),
            json!({"subscription": "projects/p/subscriptions/s", "max_messages": 0}),
        ] {
            assert!(PubSubConfig::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn maps_received_message() {
        let entry = json!({
            "ackId": "ack-1",
            "message": {
                "data": "eyJpZCI6IDF9", // {"id": 1}
                "attributes": {"source": "billing"},
                "messageId": "123",
                "publishTime": "2026-01-01T00:00:00Z",
                "orderingKey": "k"
            },
            "deliveryAttempt": 2
        });
        let (data, meta, id, ack) = map_received("projects/p/subscriptions/s", &entry).unwrap();
        assert_eq!(data, json!({"id": 1}));
        assert_eq!(id, "123");
        assert_eq!(ack, "ack-1");
        assert_eq!(meta["attributes"]["source"], "billing");
        assert_eq!(meta["delivery_attempt"], 2);
        assert_eq!(meta["ordering_key"], "k");

        let empty = json!({"ackId": "a", "message": {"messageId": "9"}});
        assert_eq!(map_received("s", &empty).unwrap().0, Value::Null);
        assert!(map_received("s", &json!({"message": {"messageId": "9"}})).is_none());
        assert!(map_received("s", &json!({"ackId": "a", "message": {}})).is_none());
    }
}
