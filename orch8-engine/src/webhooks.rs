use std::sync::OnceLock;
use std::time::Duration;

use chrono::Utc;
use serde::Serialize;
use tracing::{debug, error, warn};

use orch8_types::config::WebhookConfig;
use orch8_types::ids::InstanceId;

use crate::metrics;

/// Shared HTTP client (connection pooling, TLS, keep-alive).
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .expect("failed to build HTTP client")
    })
}

/// Webhook event payload sent to configured URLs.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookEvent {
    pub event_type: String,
    pub instance_id: Option<InstanceId>,
    pub timestamp: String,
    pub data: serde_json::Value,
}

/// Send a webhook event to all configured URLs.
/// Non-blocking: spawns a background task for each URL.
pub fn emit(config: &WebhookConfig, event: &WebhookEvent) {
    if config.urls.is_empty() {
        return;
    }

    debug!(
        event_type = %event.event_type,
        urls = config.urls.len(),
        "emitting webhook event"
    );

    for url in &config.urls {
        let url = url.clone();
        let event = event.clone();
        let timeout = Duration::from_secs(config.timeout_secs);
        let max_retries = config.max_retries;

        tokio::spawn(async move {
            send_with_retry(&url, &event, timeout, max_retries).await;
        });
    }
}

async fn send_with_retry(url: &str, event: &WebhookEvent, timeout: Duration, max_retries: u32) {
    let body = match serde_json::to_vec(event) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "failed to serialize webhook event");
            return;
        }
    };

    for attempt in 0..=max_retries {
        match send_request(url, &body, timeout).await {
            Ok(status) if status < 400 => {
                metrics::inc(metrics::WEBHOOKS_SENT);
                debug!(url = %url, event_type = %event.event_type, "webhook delivered");
                return;
            }
            Ok(status) => {
                warn!(
                    url = %url,
                    status = status,
                    attempt = attempt,
                    "webhook returned error status"
                );
            }
            Err(e) => {
                warn!(
                    url = %url,
                    error = %e,
                    attempt = attempt,
                    "webhook request failed"
                );
            }
        }

        if attempt < max_retries {
            tokio::time::sleep(backoff_duration(attempt)).await;
        }
    }

    metrics::inc(metrics::WEBHOOKS_FAILED);
    error!(
        url = %url,
        event_type = %event.event_type,
        "webhook delivery failed after all retries"
    );
}

/// Send an HTTP POST request via reqwest (TLS, connection pooling, proper HTTP).
async fn send_request(url: &str, body: &[u8], timeout: Duration) -> Result<u16, String> {
    let resp = http_client()
        .post(url)
        .header("Content-Type", "application/json")
        .timeout(timeout)
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    Ok(resp.status().as_u16())
}

/// Compute exponential backoff delay for a given attempt.
pub(crate) fn backoff_duration(attempt: u32) -> Duration {
    Duration::from_millis(500_u64.saturating_mul(2_u64.saturating_pow(attempt)))
}

/// Helper to create common webhook events.
pub fn instance_event(
    event_type: &str,
    instance_id: InstanceId,
    data: serde_json::Value,
) -> WebhookEvent {
    WebhookEvent {
        event_type: event_type.to_string(),
        instance_id: Some(instance_id),
        timestamp: Utc::now().to_rfc3339(),
        data,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_increases_exponentially() {
        assert_eq!(backoff_duration(0), Duration::from_millis(500));
        assert_eq!(backoff_duration(1), Duration::from_millis(1_000));
        assert_eq!(backoff_duration(2), Duration::from_millis(2_000));
        assert_eq!(backoff_duration(3), Duration::from_millis(4_000));
    }

    #[test]
    fn instance_event_sets_fields() {
        let id = InstanceId(uuid::Uuid::new_v4());
        let event = instance_event("instance.completed", id, serde_json::json!({"key": "val"}));
        assert_eq!(event.event_type, "instance.completed");
        assert_eq!(event.instance_id, Some(id));
        assert_eq!(event.data["key"], "val");
        assert!(!event.timestamp.is_empty());
    }

    #[test]
    fn webhook_event_serializes() {
        let event = WebhookEvent {
            event_type: "test".into(),
            instance_id: None,
            timestamp: "2024-01-01T00:00:00Z".into(),
            data: serde_json::json!({}),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event_type\":\"test\""));
    }

    #[test]
    fn emit_skips_empty_urls() {
        let config = WebhookConfig {
            urls: vec![],
            timeout_secs: 5,
            max_retries: 0,
        };
        let event = WebhookEvent {
            event_type: "test".into(),
            instance_id: None,
            timestamp: "now".into(),
            data: serde_json::json!({}),
        };
        // Should not panic or spawn tasks.
        emit(&config, &event);
    }
}
