//! Push notification fan-out: FCM topic messaging and APNs silent push.

use tracing::{info, warn};

/// Push notification service.
pub struct PushNotifier {
    fcm_token: Option<String>,
    apns_token: Option<String>,
    http: reqwest::Client,
}

impl PushNotifier {
    pub fn new(fcm_token: Option<String>, apns_token: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("reqwest client builds");
        Self {
            fcm_token,
            apns_token,
            http,
        }
    }

    /// Notify all devices for a tenant that a new manifest is available.
    /// Fires FCM topic message and APNs silent push.
    /// Failures are logged but not returned as errors (graceful degradation).
    pub async fn notify_manifest_update(&self, tenant_id: &str) {
        if let Some(ref token) = self.fcm_token {
            let topic = format!("orch8_{tenant_id}");
            let payload = serde_json::json!({
                "message": {
                    "topic": topic,
                    "data": {
                        "type": "manifest_update",
                        "tenant_id": tenant_id,
                    },
                    "android": {
                        "priority": "high"
                    }
                }
            });

            match self.send_fcm(token, &payload).await {
                Ok(()) => info!(tenant = %tenant_id, "FCM manifest update sent"),
                Err(e) => warn!(tenant = %tenant_id, error = %e, "FCM manifest update failed"),
            }
        }

        if let Some(ref token) = self.apns_token {
            let payload = serde_json::json!({
                "aps": {
                    "content-available": 1
                },
                "type": "manifest_update",
                "tenant_id": tenant_id,
            });

            match self.send_apns(token, &payload) {
                Ok(()) => info!(tenant = %tenant_id, "APNs manifest update sent"),
                Err(e) => warn!(tenant = %tenant_id, error = %e, "APNs manifest update failed"),
            }
        }
    }

    async fn send_fcm(&self, token: &str, payload: &serde_json::Value) -> Result<(), PushError> {
        let response = self
            .http
            .post("https://fcm.googleapis.com/v1/projects/_/messages:send")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .json(payload)
            .send()
            .await
            .map_err(|e| PushError::Network(e.to_string()))?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PushError::Fcm(body));
        }
        Ok(())
    }

    #[allow(clippy::unused_self)]
    fn send_apns(&self, _token: &str, _payload: &serde_json::Value) -> Result<(), PushError> {
        warn!("APNs push not implemented — requires JWT auth + HTTP/2 (use apns2 crate)");
        Err(PushError::Apns("APNs not implemented".to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("network error: {0}")]
    Network(String),
    #[error("FCM error: {0}")]
    Fcm(String),
    #[error("APNs error: {0}")]
    Apns(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notify_without_tokens_is_noop() {
        let notifier = PushNotifier::new(None, None);
        notifier.notify_manifest_update("tenant1").await;
    }
}
