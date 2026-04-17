use std::sync::OnceLock;
use std::time::Duration;

use chrono::Utc;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
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
            .unwrap_or_else(|e| {
                warn!(error = %e, "failed to build optimized HTTP client, using default");
                reqwest::Client::new()
            })
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
/// The `cancel` token allows graceful shutdown to abort in-flight webhook retries.
pub fn emit(config: &WebhookConfig, event: &WebhookEvent, cancel: &CancellationToken) {
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
        let cancel = cancel.clone();

        tokio::spawn(async move {
            send_with_retry(&url, &event, timeout, max_retries, &cancel).await;
        });
    }
}

async fn send_with_retry(
    url: &str,
    event: &WebhookEvent,
    timeout: Duration,
    max_retries: u32,
    cancel: &CancellationToken,
) {
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
            tokio::select! {
                () = cancel.cancelled() => {
                    warn!(url = %url, attempt, "webhook retry aborted by shutdown");
                    return;
                }
                () = tokio::time::sleep(backoff_duration(attempt)) => {}
            }
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
        assert_eq!(backoff_duration(1), Duration::from_secs(1));
        assert_eq!(backoff_duration(2), Duration::from_secs(2));
        assert_eq!(backoff_duration(3), Duration::from_secs(4));
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
        emit(&config, &event, &CancellationToken::new());
    }

    #[test]
    fn backoff_saturates_instead_of_overflowing() {
        // At attempt=63, 2^63 would overflow u64; saturating_pow must cap it.
        // The call must return *some* Duration and not panic.
        let d = backoff_duration(100);
        assert!(d.as_secs() > 0);
    }

    #[test]
    fn backoff_matches_documented_formula_low_attempts() {
        // Confirms contract: 500ms * 2^attempt for attempts 4 and 5.
        assert_eq!(backoff_duration(4), Duration::from_secs(8));
        assert_eq!(backoff_duration(5), Duration::from_secs(16));
    }

    #[test]
    fn webhook_event_with_instance_id_serializes_id_as_string() {
        let id = InstanceId(uuid::Uuid::new_v4());
        let event = WebhookEvent {
            event_type: "instance.running".into(),
            instance_id: Some(id),
            timestamp: "2024-01-01T00:00:00Z".into(),
            data: serde_json::json!({"n": 1}),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(
            json.contains(&id.0.to_string()),
            "serialized JSON must contain UUID: {json}"
        );
        assert!(json.contains("\"instance_id\""));
    }

    #[test]
    fn webhook_event_with_none_instance_id_serializes_as_null() {
        let event = WebhookEvent {
            event_type: "system.tick".into(),
            instance_id: None,
            timestamp: "t".into(),
            data: serde_json::json!({}),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"instance_id\":null"));
    }

    #[test]
    fn instance_event_timestamp_is_rfc3339_parseable() {
        let id = InstanceId(uuid::Uuid::new_v4());
        let event = instance_event("x", id, serde_json::json!({}));
        // Must parse as a valid RFC 3339 timestamp.
        let parsed = chrono::DateTime::parse_from_rfc3339(&event.timestamp);
        assert!(
            parsed.is_ok(),
            "timestamp must be RFC 3339: {}",
            event.timestamp
        );
    }

    #[test]
    fn instance_event_preserves_arbitrary_nested_data() {
        let id = InstanceId(uuid::Uuid::new_v4());
        let payload = serde_json::json!({
            "nested": {"a": [1, 2, 3], "b": null},
            "flag": true,
        });
        let event = instance_event("instance.completed", id, payload.clone());
        assert_eq!(event.data, payload);
    }

    #[test]
    fn webhook_event_is_cloneable() {
        // Clone is derived; this test locks the derive in place.
        let id = InstanceId(uuid::Uuid::new_v4());
        let a = WebhookEvent {
            event_type: "e".into(),
            instance_id: Some(id),
            timestamp: "t".into(),
            data: serde_json::json!({"k": "v"}),
        };
        let b = a.clone();
        assert_eq!(a.event_type, b.event_type);
        assert_eq!(a.instance_id, b.instance_id);
        assert_eq!(a.data, b.data);
    }

    #[tokio::test]
    async fn emit_with_cancelled_token_does_not_panic() {
        // Pre-cancelled token is a valid input — emit must not panic when urls are empty.
        let config = WebhookConfig {
            urls: vec![],
            timeout_secs: 1,
            max_retries: 0,
        };
        let event = instance_event(
            "test",
            InstanceId(uuid::Uuid::new_v4()),
            serde_json::json!({}),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        emit(&config, &event, &cancel);
    }
}
