use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::Semaphore;

use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

use orch8_storage::StorageBackend;
use orch8_types::config::WebhookConfig;
use orch8_types::ids::InstanceId;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};

use crate::metrics;

/// Process-global outbox sink + config, set once at engine startup. When set,
/// a delivery that exhausts its retries is parked here instead of dropped, and
/// the config is used to sign redeliveries.
static OUTBOX_STORAGE: OnceLock<Arc<dyn StorageBackend>> = OnceLock::new();
static OUTBOX_CONFIG: OnceLock<WebhookConfig> = OnceLock::new();

/// Bound the number of in-flight webhook dispatches so a burst of events cannot
/// exhaust the Tokio runtime or open an unbounded number of outbound sockets.
/// The limit is per-event fan-out: each `emit()` acquires one permit per URL.
static WEBHOOK_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

const DEFAULT_WEBHOOK_CONCURRENCY: usize = 64;

fn webhook_semaphore() -> &'static Arc<Semaphore> {
    WEBHOOK_SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(DEFAULT_WEBHOOK_CONCURRENCY)))
}

/// Tracks every in-flight webhook delivery task (M-7) so graceful shutdown
/// can wait for them to actually finish (or observe `cancel` and exit)
/// instead of `emit`'s bare `tokio::spawn` calls being silently detached and
/// then abruptly killed when the runtime is torn down mid-request.
static WEBHOOK_TASKS: OnceLock<TaskTracker> = OnceLock::new();

fn webhook_tasks() -> &'static TaskTracker {
    WEBHOOK_TASKS.get_or_init(TaskTracker::new)
}

/// Wait (up to `timeout`) for every webhook delivery task spawned via
/// [`emit`] to finish. Called during engine shutdown, after firing the
/// `cancel` token so in-flight retries observe it, and returns once every
/// task has completed or `timeout` elapses (whichever comes first) so a
/// single hung dispatch can't stall shutdown forever.
pub async fn wait_for_webhook_tasks(timeout: Duration) {
    let tracker = webhook_tasks();
    // `close()` makes `wait()` resolve once all *currently tracked* tasks
    // finish, without blocking new tasks from being tracked (harmless if a
    // step still emits one during the drain window).
    tracker.close();
    if tokio::time::timeout(timeout, tracker.wait()).await.is_err() {
        warn!(
            timeout_secs = timeout.as_secs(),
            "timed out waiting for in-flight webhook deliveries to finish"
        );
    }
}

/// Wire the webhook outbox. Exhausted deliveries park via `storage`; `config`
/// signs redeliveries. Idempotent — only the first call takes effect.
pub fn init_outbox(storage: Arc<dyn StorageBackend>, config: WebhookConfig) {
    let _ = OUTBOX_STORAGE.set(storage);
    let _ = OUTBOX_CONFIG.set(config);
}

/// Shared HTTP client (connection pooling, TLS, keep-alive).
///
/// Webhook target URLs are operator-configured, so the initial host is trusted
/// (an operator may legitimately point a webhook at an internal notifier). The
/// redirect policy, however, re-validates every hop: a trusted target that
/// returns `302 → http://169.254.169.254/…` must not be followed into the
/// cloud-metadata / internal network. Mirrors `llm::http_client`'s policy.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 10 {
                    return attempt.error("too many redirects");
                }
                if crate::handlers::builtin::redirect_target_allowed(attempt.url()) {
                    attempt.follow()
                } else {
                    attempt.error("blocked: redirect targets a private/internal network address")
                }
            }))
            .build()
            .unwrap_or_else(|e| {
                warn!(error = %e, "failed to build optimized HTTP client, using default");
                reqwest::Client::new()
            })
    })
}

/// Webhook event payload sent to configured URLs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub event_type: String,
    pub instance_id: Option<InstanceId>,
    pub timestamp: String,
    pub data: serde_json::Value,
}

/// Send a webhook event to all configured URLs.
/// Non-blocking: spawns a background task for each URL, tracked via an
/// internal `TaskTracker` (M-7) so [`wait_for_webhook_tasks`] can await them
/// at shutdown instead of the runtime abruptly killing an in-flight delivery.
/// The `cancel` token allows graceful shutdown to abort in-flight webhook retries.
pub async fn emit(config: &WebhookConfig, event: &WebhookEvent, cancel: &CancellationToken) {
    if config.urls.is_empty() {
        return;
    }

    debug!(
        event_type = %event.event_type,
        urls = config.urls.len(),
        "emitting webhook event"
    );

    let semaphore = webhook_semaphore().clone();
    let tasks = webhook_tasks();

    for url in &config.urls {
        let url = url.clone();
        let event = event.clone();
        let timeout = Duration::from_secs(config.timeout_secs);
        let max_retries = config.max_retries;
        // Expose the secret into an owned String for the spawned task; the
        // signing path takes `Option<&str>`.
        let secret = config.secret.as_ref().map(|s| s.expose().to_string());
        let cancel = cancel.clone();
        // Acquire before spawning. This bounds both active requests and queued
        // tasks: callers apply backpressure once all dispatch slots are busy.
        let permit = tokio::select! {
            result = semaphore.clone().acquire_owned() => result,
            () = cancel.cancelled() => return,
        };
        let Ok(permit) = permit else {
            warn!(url = %url, "webhook semaphore closed; dropping dispatch");
            return;
        };

        tasks.spawn(async move {
            let _permit = permit;
            if cancel.is_cancelled() {
                return;
            }
            send_with_retry(
                &url,
                &event,
                timeout,
                max_retries,
                secret.as_deref(),
                &cancel,
            )
            .await;
        });
    }
}

/// Build durable `pending` rows for one event. These rows are committed in the
/// same transaction as the terminal instance transition, then consumed by
/// [`run_outbox_loop`].
pub(crate) fn pending_entries(
    config: &WebhookConfig,
    event: &WebhookEvent,
) -> Vec<WebhookOutboxEntry> {
    let payload = serde_json::to_value(event).unwrap_or_else(|error| {
        error!(%error, "failed to serialize webhook event for durable outbox");
        serde_json::Value::Null
    });
    config
        .urls
        .iter()
        .map(|url| WebhookOutboxEntry {
            id: uuid::Uuid::now_v7(),
            url: url.clone(),
            event_type: event.event_type.clone(),
            instance_id: event.instance_id.map(InstanceId::into_uuid),
            payload: payload.clone(),
            attempts: 0,
            last_error: None,
            created_at: Utc::now(),
            delivery_id: Some(uuid::Uuid::now_v7()),
            status: WebhookOutboxStatus::Pending,
            next_attempt_at: None,
            claimed_at: None,
        })
        .collect()
}

async fn deliver_claimed(
    storage: &dyn StorageBackend,
    config: &WebhookConfig,
    entry: &WebhookOutboxEntry,
    cancel: &CancellationToken,
) {
    let event: WebhookEvent = match serde_json::from_value(entry.payload.clone()) {
        Ok(event) => event,
        Err(error) => {
            let message = format!("invalid durable webhook payload: {error}");
            if let Err(storage_error) = storage
                .fail_webhook_outbox_attempt(entry.id, &message, None)
                .await
            {
                warn!(error = %storage_error, outbox_id = %entry.id, "failed to park invalid webhook payload");
            }
            return;
        }
    };
    let delivery_id = entry.delivery_id.unwrap_or_else(uuid::Uuid::now_v7);
    let secret = config.secret.as_ref().map(|s| s.expose().to_string());
    match try_send(
        delivery_id,
        &entry.url,
        &event,
        Duration::from_secs(config.timeout_secs),
        config.max_retries,
        secret.as_deref(),
        cancel,
    )
    .await
    {
        Ok(()) => {
            if let Err(error) = storage.delete_webhook_outbox(entry.id).await {
                warn!(%error, outbox_id = %entry.id, "delivered webhook but failed to remove outbox row");
            }
        }
        Err(reason) => {
            // Shutdown is not delivery exhaustion. Return the row to pending
            // so the next engine process retries it automatically instead of
            // stranding it in the operator-only parked queue.
            let retry_at = cancel.is_cancelled().then(Utc::now);
            metrics::inc(metrics::WEBHOOKS_FAILED);
            if let Err(error) = storage
                .fail_webhook_outbox_attempt(entry.id, &reason, retry_at)
                .await
            {
                warn!(%error, outbox_id = %entry.id, "failed to reschedule durable webhook");
            } else if retry_at.is_none() {
                metrics::inc(metrics::WEBHOOKS_PARKED);
            }
        }
    }
}

/// Drain durable pending webhooks until shutdown. Claims are coordinated by
/// the storage backend (`SKIP LOCKED` on `Postgres`, `BEGIN IMMEDIATE` on
/// `SQLite`), so every engine node may run this loop safely.
pub async fn run_outbox_loop(
    storage: Arc<dyn StorageBackend>,
    config: WebhookConfig,
    cancel: CancellationToken,
) {
    let stale_after = max_retry_pass_duration(&config).saturating_add(Duration::from_secs(300));
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {}
        }

        let stale_before =
            Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or(chrono::TimeDelta::MAX);
        if let Err(error) = storage.recover_stale_webhook_claims(stale_before).await {
            warn!(%error, "failed to recover stale webhook outbox claims");
        }

        let claimed = match storage
            .claim_due_webhook_outbox(
                Utc::now(),
                u32::try_from(DEFAULT_WEBHOOK_CONCURRENCY).unwrap_or(u32::MAX),
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                warn!(%error, "failed to claim durable webhook deliveries");
                continue;
            }
        };
        let mut deliveries = tokio::task::JoinSet::new();
        for entry in claimed {
            let storage = Arc::clone(&storage);
            let config = config.clone();
            let cancel = cancel.clone();
            deliveries.spawn(async move {
                deliver_claimed(storage.as_ref(), &config, &entry, &cancel).await;
            });
        }
        while deliveries.join_next().await.is_some() {}
    }
}

/// Upper bound for one `try_send` pass: every request timeout plus every
/// exponential-backoff sleep. Used as the outbox claim lease so another node
/// cannot reclaim a row while its current dispatcher is legitimately sleeping
/// between retries.
fn max_retry_pass_duration(config: &WebhookConfig) -> Duration {
    let request_count = u64::from(config.max_retries).saturating_add(1);
    let mut total = Duration::from_secs(config.timeout_secs.saturating_mul(request_count));
    for attempt in 0..config.max_retries {
        total = total.saturating_add(backoff_duration(attempt));
        if total == Duration::MAX {
            break;
        }
    }
    total
}

/// Record one delivery attempt (best-effort — inspector data must never
/// block or fail a delivery).
async fn record_attempt(
    delivery_id: uuid::Uuid,
    url: &str,
    event: &WebhookEvent,
    attempt_number: i32,
    started: std::time::Instant,
    outcome: Result<u16, &str>,
    signed: bool,
) {
    let Some(storage) = OUTBOX_STORAGE.get() else {
        return;
    };
    let mut row = orch8_types::webhook_delivery::WebhookDeliveryAttempt {
        id: uuid::Uuid::now_v7(),
        delivery_id,
        url: url.to_string(),
        event_type: event.event_type.clone(),
        instance_id: event.instance_id.map(InstanceId::into_uuid),
        attempt_number,
        attempted_at: Utc::now(),
        duration_ms: i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
        success: false,
        status_code: None,
        error_class: None,
        error_excerpt: None,
        signed,
    };
    match outcome {
        Ok(status) if status < 400 => {
            row.success = true;
            row.status_code = Some(i32::from(status));
        }
        Ok(status) => {
            row.status_code = Some(i32::from(status));
            row.set_error(&format!("http {status}"), Some(status));
        }
        Err(e) => row.set_error(e, None),
    }
    if let Err(e) = storage.record_webhook_attempt(&row).await {
        debug!(error = %e, "failed to record webhook delivery attempt");
    }
}

/// One full retry pass. Returns `Ok(())` on a 2xx/3xx, or `Err(last_error)`
/// after exhausting `max_retries` (or on shutdown). Does NOT park — callers
/// decide what to do with a failure. Every attempt is recorded (bounded,
/// redacted metadata) under `delivery_id` for the delivery inspector.
async fn try_send(
    delivery_id: uuid::Uuid,
    url: &str,
    event: &WebhookEvent,
    timeout: Duration,
    max_retries: u32,
    secret: Option<&str>,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let body = serde_json::to_vec(event).map_err(|e| format!("serialize: {e}"))?;
    let signed = secret.is_some();

    let mut last_error = String::from("no attempts made");
    for attempt in 0..=max_retries {
        let attempt_number = i32::try_from(attempt + 1).unwrap_or(i32::MAX);
        let started = std::time::Instant::now();
        match send_request(url, &body, timeout, secret).await {
            Ok(status) if status < 400 => {
                record_attempt(
                    delivery_id,
                    url,
                    event,
                    attempt_number,
                    started,
                    Ok(status),
                    signed,
                )
                .await;
                metrics::inc(metrics::WEBHOOKS_SENT);
                debug!(url = %url, event_type = %event.event_type, "webhook delivered");
                return Ok(());
            }
            Ok(status) => {
                record_attempt(
                    delivery_id,
                    url,
                    event,
                    attempt_number,
                    started,
                    Ok(status),
                    signed,
                )
                .await;
                last_error = format!("http {status}");
                warn!(url = %url, status, attempt, "webhook returned error status");
            }
            Err(e) => {
                record_attempt(
                    delivery_id,
                    url,
                    event,
                    attempt_number,
                    started,
                    Err(e.as_str()),
                    signed,
                )
                .await;
                warn!(url = %url, error = %e, attempt, "webhook request failed");
                last_error = e;
            }
        }

        if attempt < max_retries {
            tokio::select! {
                () = cancel.cancelled() => {
                    warn!(url = %url, attempt, "webhook retry aborted by shutdown");
                    return Err("aborted by shutdown".into());
                }
                () = tokio::time::sleep(backoff_duration(attempt)) => {}
            }
        }
    }
    Err(last_error)
}

async fn send_with_retry(
    url: &str,
    event: &WebhookEvent,
    timeout: Duration,
    max_retries: u32,
    secret: Option<&str>,
    cancel: &CancellationToken,
) {
    let delivery_id = uuid::Uuid::now_v7();
    let last_error = match try_send(
        delivery_id,
        url,
        event,
        timeout,
        max_retries,
        secret,
        cancel,
    )
    .await
    {
        Ok(()) => return,
        Err(reason) => reason,
    };

    metrics::inc(metrics::WEBHOOKS_FAILED);
    error!(
        url = %url,
        event_type = %event.event_type,
        "webhook delivery failed after all retries"
    );

    // Park the exhausted delivery so it isn't silently lost.
    if let Some(storage) = OUTBOX_STORAGE.get() {
        let entry = WebhookOutboxEntry {
            id: uuid::Uuid::now_v7(),
            url: url.to_string(),
            event_type: event.event_type.clone(),
            instance_id: event.instance_id.map(InstanceId::into_uuid),
            payload: serde_json::to_value(event).unwrap_or(serde_json::Value::Null),
            attempts: i32::try_from(max_retries.saturating_add(1)).unwrap_or(i32::MAX),
            last_error: Some(last_error),
            created_at: Utc::now(),
            delivery_id: Some(delivery_id),
            status: orch8_types::webhook_outbox::WebhookOutboxStatus::Parked,
            next_attempt_at: None,
            claimed_at: None,
        };
        match storage.park_webhook(&entry).await {
            Ok(()) => metrics::inc(metrics::WEBHOOKS_PARKED),
            Err(e) => warn!(url = %url, error = %e, "failed to park exhausted webhook"),
        }
    }
}

/// Whether a redelivery would be HMAC-signed (the wired outbox config has
/// a secret). Used by the redelivery *preview* endpoint — the secret
/// itself never leaves the engine.
#[must_use]
pub fn redelivery_will_sign() -> bool {
    OUTBOX_CONFIG.get().is_some_and(|c| c.secret.is_some())
}

/// Redeliver a parked webhook to its original URL — one fresh retry pass using
/// the engine's webhook config for signing. On success (`Ok(delivery_id)`)
/// the caller should delete the outbox row; `Err(reason)` means it should
/// stay parked. The returned id names the new attempt group recorded for
/// the delivery inspector.
pub async fn redeliver(
    entry: &WebhookOutboxEntry,
    cancel: &CancellationToken,
) -> Result<uuid::Uuid, String> {
    let event: WebhookEvent =
        serde_json::from_value(entry.payload.clone()).map_err(|e| format!("bad payload: {e}"))?;
    let (timeout, max_retries, secret) = match OUTBOX_CONFIG.get() {
        Some(c) => (
            Duration::from_secs(c.timeout_secs),
            c.max_retries,
            c.secret.as_ref().map(|s| s.expose().to_string()),
        ),
        None => (Duration::from_secs(10), 3, None),
    };
    // A redelivery is its own attempt group.
    let delivery_id = uuid::Uuid::now_v7();
    try_send(
        delivery_id,
        &entry.url,
        &event,
        timeout,
        max_retries,
        secret.as_deref(),
        cancel,
    )
    .await
    .map(|()| delivery_id)
}

/// Send an HTTP POST request via reqwest (TLS, connection pooling, proper HTTP).
///
/// When `secret` is set, the request is signed (Stripe/GitHub-style) so the
/// receiver can verify authenticity and reject replays:
/// - `X-Orch8-Timestamp: <unix secs>`
/// - `X-Orch8-Signature: sha256=<hex HMAC-SHA256(secret, "{ts}.{body}")>`
async fn send_request(
    url: &str,
    body: &[u8],
    timeout: Duration,
    secret: Option<&str>,
) -> Result<u16, String> {
    let mut req = http_client()
        .post(url)
        .header("Content-Type", "application/json")
        .timeout(timeout);

    if let Some(secret) = secret {
        let ts = Utc::now().timestamp();
        let sig = sign(secret, ts, body);
        req = req
            .header("X-Orch8-Timestamp", ts.to_string())
            .header("X-Orch8-Signature", format!("sha256={sig}"));
    }

    let resp = req
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    Ok(resp.status().as_u16())
}

type HmacSha256 = Hmac<Sha256>;

/// Lowercase hex-encode bytes without pulling in the `hex` crate.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Compute the outbound signature: HMAC-SHA256 over `"{timestamp}.{body}"`,
/// hex-encoded. Binding the timestamp into the signed string means a captured
/// body can't be replayed under a different time.
pub(crate) fn sign(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .unwrap_or_else(|_| unreachable!("HmacSha256 accepts keys of any length"));
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    to_hex(&mac.finalize().into_bytes())
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
    use orch8_storage::WorkerStore;

    fn pending_test_entry(url: String, payload: serde_json::Value) -> WebhookOutboxEntry {
        WebhookOutboxEntry {
            id: uuid::Uuid::now_v7(),
            url,
            event_type: "instance.completed".into(),
            instance_id: Some(InstanceId::new().into_uuid()),
            payload,
            attempts: 0,
            last_error: None,
            created_at: Utc::now(),
            delivery_id: Some(uuid::Uuid::now_v7()),
            status: WebhookOutboxStatus::Pending,
            next_attempt_at: None,
            claimed_at: None,
        }
    }

    #[test]
    fn backoff_increases_exponentially() {
        assert_eq!(backoff_duration(0), Duration::from_millis(500));
        assert_eq!(backoff_duration(1), Duration::from_secs(1));
        assert_eq!(backoff_duration(2), Duration::from_secs(2));
        assert_eq!(backoff_duration(3), Duration::from_secs(4));
    }

    #[test]
    fn instance_event_sets_fields() {
        let id = InstanceId::new();
        let event = instance_event("instance.completed", id, serde_json::json!({"key": "val"}));
        assert_eq!(event.event_type, "instance.completed");
        assert_eq!(event.instance_id, Some(id));
        assert_eq!(event.data["key"], "val");
        assert!(!event.timestamp.is_empty());
    }

    #[test]
    fn pending_entries_create_one_durable_row_per_url() {
        let config = WebhookConfig {
            urls: vec![
                "https://hooks.example.com/a".into(),
                "https://hooks.example.com/b".into(),
            ],
            timeout_secs: 5,
            max_retries: 2,
            secret: None,
        };
        let instance_id = InstanceId::new();
        let event = instance_event(
            "instance.completed",
            instance_id,
            serde_json::json!({"run_id": "r1"}),
        );

        let entries = pending_entries(&config, &event);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| {
            e.status == WebhookOutboxStatus::Pending
                && e.instance_id == Some(instance_id.into_uuid())
                && e.attempts == 0
                && e.next_attempt_at.is_none()
                && e.claimed_at.is_none()
        }));
        assert_ne!(entries[0].id, entries[1].id);
        assert_ne!(entries[0].delivery_id, entries[1].delivery_id);
        let round_trip: WebhookEvent = serde_json::from_value(entries[0].payload.clone()).unwrap();
        assert_eq!(round_trip.event_type, "instance.completed");
        assert_eq!(round_trip.data["run_id"], "r1");
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

    #[tokio::test]
    async fn emit_skips_empty_urls() {
        let config = WebhookConfig {
            urls: vec![],
            timeout_secs: 5,
            max_retries: 0,
            secret: None,
        };
        let event = WebhookEvent {
            event_type: "test".into(),
            instance_id: None,
            timestamp: "now".into(),
            data: serde_json::json!({}),
        };
        // Should not panic or spawn tasks.
        emit(&config, &event, &CancellationToken::new()).await;
    }

    #[tokio::test]
    async fn emit_does_not_exhaust_runtime_under_large_fanout() {
        // Many URLs should still only acquire a bounded number of concurrent
        // permits; the test completes without timing out or spawning thousands
        // of simultaneous requests.
        let (url, counter, _bodies) = start_mock_server(|_| 200).await;
        let urls: Vec<String> = (0..500).map(|_| url.clone()).collect();
        let config = WebhookConfig {
            urls,
            timeout_secs: 2,
            max_retries: 0,
            secret: None,
        };
        let event = instance_event("test", InstanceId::new(), serde_json::json!({}));
        emit(&config, &event, &CancellationToken::new()).await;

        for _ in 0..200 {
            if counter.load(Ordering::SeqCst) >= 500 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            500,
            "all 500 dispatches should complete"
        );
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
    fn outbox_claim_lease_covers_request_time_and_all_backoffs() {
        let config = WebhookConfig {
            urls: vec![],
            timeout_secs: 10,
            max_retries: 10,
            secret: None,
        };
        // 11 requests * 10s + sleeps 0.5+1+...+256s = 621.5s.
        assert_eq!(
            max_retry_pass_duration(&config),
            Duration::from_millis(621_500)
        );
    }

    #[tokio::test]
    async fn invalid_durable_payload_is_parked_for_operator_inspection() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let entry = pending_test_entry(
            "https://hooks.example.com/a".into(),
            serde_json::json!({"not": "a WebhookEvent"}),
        );
        storage.park_webhook(&entry).await.unwrap();
        let claimed = storage
            .claim_due_webhook_outbox(Utc::now(), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();

        deliver_claimed(
            &storage,
            &WebhookConfig::default(),
            &claimed,
            &CancellationToken::new(),
        )
        .await;

        let parked = storage.get_webhook_outbox(entry.id).await.unwrap().unwrap();
        assert_eq!(parked.status, WebhookOutboxStatus::Parked);
        assert!(
            parked
                .last_error
                .as_deref()
                .is_some_and(|message| message.contains("invalid durable webhook payload"))
        );
    }

    #[test]
    fn webhook_event_with_instance_id_serializes_id_as_string() {
        let id = InstanceId::new();
        let event = WebhookEvent {
            event_type: "instance.running".into(),
            instance_id: Some(id),
            timestamp: "2024-01-01T00:00:00Z".into(),
            data: serde_json::json!({"n": 1}),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(
            json.contains(&id.to_string()),
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
        let id = InstanceId::new();
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
        let id = InstanceId::new();
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
        let id = InstanceId::new();
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

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
            if header_end.is_none()
                && let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n")
            {
                header_end = Some(pos + 4);
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
    async fn successful_durable_delivery_removes_outbox_row() {
        let (url, counter, _bodies) = start_mock_server(|_| 200).await;
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let event = instance_event(
            "instance.completed",
            InstanceId::new(),
            serde_json::json!({}),
        );
        let entry = pending_test_entry(url, serde_json::to_value(event).unwrap());
        storage.park_webhook(&entry).await.unwrap();
        let claimed = storage
            .claim_due_webhook_outbox(Utc::now(), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let config = WebhookConfig {
            urls: vec![],
            timeout_secs: 2,
            max_retries: 0,
            secret: None,
        };

        deliver_claimed(&storage, &config, &claimed, &CancellationToken::new()).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(
            storage
                .get_webhook_outbox(entry.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn send_with_retry_posts_correct_payload_on_success() {
        let (url, counter, bodies) = start_mock_server(|_| 200).await;
        let cancel = CancellationToken::new();
        let event = WebhookEvent {
            event_type: "instance.completed".into(),
            instance_id: Some(InstanceId::new()),
            timestamp: "2024-01-01T00:00:00Z".into(),
            data: serde_json::json!({"k": "v"}),
        };
        send_with_retry(&url, &event, Duration::from_secs(2), 0, None, &cancel).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1, "exactly one request");
        {
            let bodies = bodies.lock().await;
            let raw = std::str::from_utf8(&bodies[0]).unwrap();
            assert!(raw.starts_with("POST /hook HTTP/1.1"), "method+path: {raw}");
            assert!(
                raw.to_ascii_lowercase()
                    .contains("content-type: application/json")
            );
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
        send_with_retry(&url, &event, Duration::from_secs(2), 5, None, &cancel).await;
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
        send_with_retry(&url, &event, Duration::from_secs(2), 2, None, &cancel).await;

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
        send_with_retry(&url, &event, Duration::from_millis(300), 0, None, &cancel).await;
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
            secret: None,
        };
        let event = instance_event("test", InstanceId::new(), serde_json::json!({}));
        let cancel = CancellationToken::new();
        cancel.cancel();
        emit(&config, &event, &cancel).await;
    }

    // --- Outbound signing -------------------------------------------------

    /// Extract a header value from a raw HTTP/1.1 request (case-insensitive).
    fn header_value(raw: &str, name: &str) -> Option<String> {
        let head = raw.split("\r\n\r\n").next().unwrap_or("");
        let needle = format!("{}:", name.to_ascii_lowercase());
        head.lines().find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix(&needle)
                .map(|_| line[name.len() + 1..].trim().to_string())
        })
    }

    /// Return the body (everything after the header terminator).
    fn body_of(raw: &str) -> &str {
        raw.split_once("\r\n\r\n").map_or("", |(_, body)| body)
    }

    #[test]
    fn sign_is_deterministic_and_sensitive() {
        let a = sign("secret", 1000, b"body");
        assert_eq!(a.len(), 64, "hex SHA-256 is 64 chars");
        assert!(
            a.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "lowercase hex only: {a}"
        );
        // Deterministic for identical inputs.
        assert_eq!(a, sign("secret", 1000, b"body"));
        // Sensitive to every input: key, timestamp, body.
        assert_ne!(a, sign("secret2", 1000, b"body"));
        assert_ne!(a, sign("secret", 1001, b"body"));
        assert_ne!(a, sign("secret", 1000, b"body2"));
    }

    #[tokio::test]
    async fn signed_delivery_sends_verifiable_signature_headers() {
        let (url, counter, bodies) = start_mock_server(|_| 200).await;
        let cancel = CancellationToken::new();
        let event = WebhookEvent {
            event_type: "instance.completed".into(),
            instance_id: None,
            timestamp: "2024-01-01T00:00:00Z".into(),
            data: serde_json::json!({ "k": "v" }),
        };
        let secret = "whsec_test_123";
        send_with_retry(
            &url,
            &event,
            Duration::from_secs(2),
            0,
            Some(secret),
            &cancel,
        )
        .await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        let bodies = bodies.lock().await;
        let raw = std::str::from_utf8(&bodies[0]).unwrap();

        let ts: i64 = header_value(raw, "X-Orch8-Timestamp")
            .expect("timestamp header present")
            .parse()
            .expect("timestamp is an integer");
        let sig = header_value(raw, "X-Orch8-Signature").expect("signature header present");
        let hex = sig.strip_prefix("sha256=").expect("sha256= prefix");
        assert_eq!(hex.len(), 64);

        // A receiver recomputing over the exact body it received must match —
        // this is the property that makes the webhook verifiable.
        let body = body_of(raw);
        assert_eq!(hex, sign(secret, ts, body.as_bytes()));
    }

    #[tokio::test]
    async fn unsigned_delivery_omits_signature_headers() {
        let (url, _counter, bodies) = start_mock_server(|_| 200).await;
        let cancel = CancellationToken::new();
        let event = WebhookEvent {
            event_type: "t".into(),
            instance_id: None,
            timestamp: "t".into(),
            data: serde_json::json!({}),
        };
        send_with_retry(&url, &event, Duration::from_secs(2), 0, None, &cancel).await;
        let bodies = bodies.lock().await;
        let raw = std::str::from_utf8(&bodies[0]).unwrap();
        assert!(
            header_value(raw, "X-Orch8-Signature").is_none(),
            "no signature without a secret"
        );
        assert!(header_value(raw, "X-Orch8-Timestamp").is_none());
    }

    #[tokio::test]
    async fn emit_delivers_signed_request_to_configured_url() {
        let (url, counter, bodies) = start_mock_server(|_| 200).await;
        let config = WebhookConfig {
            urls: vec![url],
            timeout_secs: 2,
            max_retries: 0,
            secret: Some("whsec_emit".into()),
        };
        let event = instance_event(
            "instance.completed",
            InstanceId::new(),
            serde_json::json!({ "n": 1 }),
        );
        emit(&config, &event, &CancellationToken::new()).await;

        // emit spawns a background task — poll until it lands.
        for _ in 0..50 {
            if counter.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1, "emit delivered once");
        let bodies = bodies.lock().await;
        let raw = std::str::from_utf8(&bodies[0]).unwrap();
        assert!(
            header_value(raw, "X-Orch8-Signature").is_some(),
            "emit threaded the secret through to a signature"
        );
    }

    /// M-7: `emit`'s delivery task must be tracked so
    /// `wait_for_webhook_tasks` actually waits for it to finish rather than
    /// returning immediately while it's still in flight (which would let the
    /// runtime tear it down mid-request during shutdown).
    #[tokio::test]
    async fn wait_for_webhook_tasks_waits_for_in_flight_delivery() {
        use tokio::io::AsyncWriteExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/hook");
        let done = Arc::new(AtomicUsize::new(0));
        let done_srv = done.clone();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0u8; 4096];
            let _ = read_request_into(&mut stream, &mut buf).await;
            // Deliberately slow response — gives wait_for_webhook_tasks
            // something meaningful to actually wait for.
            tokio::time::sleep(Duration::from_millis(150)).await;
            done_srv.store(1, Ordering::SeqCst);
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });

        let config = WebhookConfig {
            urls: vec![url],
            timeout_secs: 5,
            max_retries: 0,
            secret: None,
        };
        let event = instance_event(
            "instance.completed",
            InstanceId::new(),
            serde_json::json!({}),
        );
        emit(&config, &event, &CancellationToken::new()).await;

        // The delivery is still in flight (server hasn't slept 150ms yet).
        assert_eq!(done.load(Ordering::SeqCst), 0);

        wait_for_webhook_tasks(Duration::from_secs(5)).await;

        // By the time wait_for_webhook_tasks returns, the tracked delivery
        // task (and therefore the mock server's slow-then-respond handler it
        // awaited) must have completed.
        assert_eq!(
            done.load(Ordering::SeqCst),
            1,
            "wait_for_webhook_tasks must wait for the in-flight delivery to finish"
        );
    }

    /// Minimal request reader for tests that need to hold the connection open
    /// afterward (unlike `read_request`, which is only used with mock servers
    /// that respond-then-close per request).
    async fn read_request_into(
        stream: &mut tokio::net::TcpStream,
        buf: &mut [u8],
    ) -> std::io::Result<usize> {
        use tokio::io::AsyncReadExt;
        stream.read(buf).await
    }
}
