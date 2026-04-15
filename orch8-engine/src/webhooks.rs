use std::time::Duration;

use chrono::Utc;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, warn};

use orch8_types::config::WebhookConfig;
use orch8_types::ids::InstanceId;

use crate::metrics;

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
            let backoff = Duration::from_millis(500 * u64::from(2_u32.pow(attempt)));
            tokio::time::sleep(backoff).await;
        }
    }

    metrics::inc(metrics::WEBHOOKS_FAILED);
    error!(
        url = %url,
        event_type = %event.event_type,
        "webhook delivery failed after all retries"
    );
}

/// Send an HTTP POST request. Uses a minimal TCP-based approach to avoid
/// pulling in a full HTTP client dependency. For production, replace with reqwest.
async fn send_request(url: &str, body: &[u8], timeout: Duration) -> Result<u16, String> {
    // Parse URL to extract host and path.
    let url_str = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"));
    let Some(rest) = url_str else {
        return Err("invalid URL scheme".into());
    };

    let (host_port, path) = rest.split_once('/').unwrap_or((rest, ""));
    let path = format!("/{path}");

    let stream = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(host_port))
        .await
        .map_err(|_| "connection timeout".to_string())?
        .map_err(|e| e.to_string())?;

    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        path,
        host_port,
        body.len()
    );

    let mut stream = stream;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    stream.write_all(body).await.map_err(|e| e.to_string())?;

    let mut response = vec![0u8; 1024];
    let n = stream
        .read(&mut response)
        .await
        .map_err(|e| e.to_string())?;
    let response_str = String::from_utf8_lossy(&response[..n]);

    // Parse status code from "HTTP/1.1 200 OK"
    let status = response_str
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(500);

    Ok(status)
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
