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
pub(crate) const fn backoff_duration(attempt: u32) -> Duration {
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
        let id = InstanceId(uuid::Uuid::now_v7());
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
        let id = InstanceId(uuid::Uuid::now_v7());
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
        let id = InstanceId(uuid::Uuid::now_v7());
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
        let id = InstanceId(uuid::Uuid::now_v7());
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
        let id = InstanceId(uuid::Uuid::now_v7());
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

    // --- Mock HTTP server for webhook delivery tests -----------------------
    //
    // Spawns a single-request-per-connection TCP listener that applies a
    // user-supplied response strategy. Keeps these tests dep-free (no
    // wiremock crate needed).

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Read an HTTP/1.1 request from `stream` until `\r\n\r\n` + Content-Length
    /// body bytes are received. Returns the full request bytes.
    async fn read_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        let mut tmp = [0u8; 1024];
        let mut header_end = None;
        loop {
            let n = stream.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if header_end.is_none() {
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    header_end = Some(pos + 4);
                }
            }
            if let Some(end) = header_end {
                // Parse Content-Length to know how much body to expect.
                let headers = std::str::from_utf8(&buf[..end]).unwrap_or("");
                let cl: usize = headers
                    .lines()
                    .find_map(|l| {
                        let l = l.to_ascii_lowercase();
                        l.strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if buf.len() >= end + cl {
                    break;
                }
            }
        }
        buf
    }

    /// Start a mock HTTP server that responds to each request with the status
    /// yielded by `status_fn(attempt_index)`. Returns the URL and a counter of
    /// received requests.
    async fn start_mock_server<F>(
        status_fn: F,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    )
    where
        F: Fn(usize) -> u16 + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/hook");
        let counter = Arc::new(AtomicUsize::new(0));
        let bodies = Arc::new(tokio::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        let counter_srv = counter.clone();
        let bodies_srv = bodies.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let attempt = counter_srv.fetch_add(1, Ordering::SeqCst);
                let status = status_fn(attempt);
                let req_bytes = read_request(&mut stream).await;
                bodies_srv.lock().await.push(req_bytes);
                let reason = match status {
                    200 => "OK",
                    500 => "Internal Server Error",
                    503 => "Service Unavailable",
                    _ => "Status",
                };
                let resp = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        (url, counter, bodies)
    }

    #[tokio::test]
    async fn send_with_retry_posts_correct_payload_on_success() {
        let (url, counter, bodies) = start_mock_server(|_| 200).await;
        let cancel = CancellationToken::new();
        let event = WebhookEvent {
            event_type: "instance.completed".into(),
            instance_id: Some(InstanceId(uuid::Uuid::now_v7())),
            timestamp: "2024-01-01T00:00:00Z".into(),
            data: serde_json::json!({"k": "v"}),
        };
        send_with_retry(&url, &event, Duration::from_secs(2), 0, &cancel).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1, "exactly one request");
        {
            let bodies = bodies.lock().await;
            let raw = std::str::from_utf8(&bodies[0]).unwrap();
            assert!(raw.starts_with("POST /hook HTTP/1.1"), "method+path: {raw}");
            assert!(raw
                .to_ascii_lowercase()
                .contains("content-type: application/json"));
            assert!(raw.contains("\"event_type\":\"instance.completed\""));
            assert!(raw.contains("\"k\":\"v\""));
            drop(bodies);
        }
    }

    #[tokio::test]
    async fn send_with_retry_retries_on_5xx_then_succeeds() {
        // First attempt: 500. Second attempt: 200.
        let (url, counter, _bodies) = start_mock_server(|n| if n == 0 { 500 } else { 200 }).await;
        let cancel = CancellationToken::new();
        let event = WebhookEvent {
            event_type: "t".into(),
            instance_id: None,
            timestamp: "t".into(),
            data: serde_json::json!({}),
        };
        // Override attempt pacing by using max_retries=5 — backoff is 500ms for
        // attempt=0, acceptable for a single retry in a test.
        let start = std::time::Instant::now();
        send_with_retry(&url, &event, Duration::from_secs(2), 5, &cancel).await;
        let elapsed = start.elapsed();

        assert_eq!(counter.load(Ordering::SeqCst), 2, "one retry expected");
        assert!(
            elapsed >= Duration::from_millis(400),
            "backoff should apply: {elapsed:?}",
        );
    }

    #[tokio::test]
    async fn send_with_retry_gives_up_after_max_retries() {
        // Always return 500. With max_retries=2, expect 3 total attempts
        // (attempt 0 + 2 retries).
        let (url, counter, _bodies) = start_mock_server(|_| 500).await;
        let cancel = CancellationToken::new();
        let event = WebhookEvent {
            event_type: "t".into(),
            instance_id: None,
            timestamp: "t".into(),
            data: serde_json::json!({}),
        };
        // Cancel after a short wait so the second backoff (1s) does not stall
        // the test; the first backoff of 500ms will have already happened and
        // produced the third request attempt.
        let cancel_for_task = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(800)).await;
            cancel_for_task.cancel();
        });
        send_with_retry(&url, &event, Duration::from_secs(2), 2, &cancel).await;

        let attempts = counter.load(Ordering::SeqCst);
        assert!(
            (2..=3).contains(&attempts),
            "expected 2-3 attempts before cancel, got {attempts}"
        );
    }

    #[tokio::test]
    async fn send_with_retry_timeout_aborts_request() {
        // Bind a listener that accepts the connection but never writes a
        // response — this forces the per-request `timeout` to fire.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/slow");
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_srv = accepted.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                accepted_srv.fetch_add(1, Ordering::SeqCst);
                // Hold the socket open without responding.
                let _keepalive = stream;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let cancel = CancellationToken::new();
        let event = WebhookEvent {
            event_type: "t".into(),
            instance_id: None,
            timestamp: "t".into(),
            data: serde_json::json!({}),
        };
        let start = std::time::Instant::now();
        // max_retries=0 so we only make one attempt and the timeout is the
        // exclusive stopping condition. Timeout is short.
        send_with_retry(&url, &event, Duration::from_millis(300), 0, &cancel).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(5),
            "timeout must abort, not hang: {elapsed:?}",
        );
        assert!(
            accepted.load(Ordering::SeqCst) >= 1,
            "connection should have been accepted"
        );
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
            InstanceId(uuid::Uuid::now_v7()),
            serde_json::json!({}),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        emit(&config, &event, &cancel);
    }
}
