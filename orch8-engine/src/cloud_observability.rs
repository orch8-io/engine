//! Run-metadata export to a managed cloud (`[cloud_observability]`).
//!
//! The engine records every instance state transition that goes through the
//! lifecycle funnel ([`crate::lifecycle::audit_transition`]) into a bounded,
//! process-global buffer. A background task drains it every `interval_ms`,
//! enriches each event with the sequence name/version, and POSTs batches of
//! at most [`MAX_BATCH`] events to `{endpoint}/api/ingest/v1/runs`.
//!
//! Guarantees:
//! - **Metadata only.** The wire event ([`ExportEvent`]) has no field that
//!   can hold context, inputs, outputs, params, or error messages.
//! - **Never blocks the engine.** Recording is an O(1) push under a short
//!   lock; when the buffer is full the *oldest* event is dropped and counted.
//! - **Retries with backoff.** A failed batch is put back at the front of the
//!   buffer (still subject to the bound) and the next attempt waits an
//!   exponentially growing delay capped at [`MAX_BACKOFF`].

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use orch8_storage::StorageBackend;
use orch8_types::config::CloudObservabilityConfig;
use orch8_types::ids::{InstanceId, SequenceId};
use orch8_types::instance::InstanceState;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

/// Contract §6: at most 500 events per request.
pub const MAX_BATCH: usize = 500;
/// Upper bound on the retry delay.
pub const MAX_BACKOFF: Duration = Duration::from_secs(60);
const MIN_INTERVAL_MS: u64 = 250;
/// Batches sent per tick before yielding back to the interval timer.
const MAX_BATCHES_PER_TICK: usize = 20;
const SEQUENCE_CACHE_LIMIT: usize = 4_096;

/// A transition as captured on the hot path. Holds identifiers only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionEvent {
    pub instance_id: InstanceId,
    pub state: InstanceState,
    pub at: DateTime<Utc>,
    pub step_id: Option<String>,
}

/// Wire event (contract §6). Adding a field here is a privacy review:
/// nothing that can carry workflow data belongs in this struct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExportEvent {
    pub instance_id: String,
    pub sequence_name: String,
    pub sequence_version: Option<i64>,
    pub state: String,
    pub at: DateTime<Utc>,
    pub step_id: Option<String>,
    pub duration_ms: Option<i64>,
    pub error_kind: Option<String>,
    pub sub_tenant: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportBatch {
    pub engine_id: String,
    pub engine_version: &'static str,
    pub sent_at: DateTime<Utc>,
    pub events: Vec<ExportEvent>,
}

// ---------------------------------------------------------------------------
// Bounded buffer
// ---------------------------------------------------------------------------

/// Bounded FIFO with drop-oldest semantics.
#[derive(Debug)]
pub struct ExportBuffer {
    queue: Mutex<VecDeque<TransitionEvent>>,
    capacity: usize,
    dropped: AtomicU64,
}

impl ExportBuffer {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity.min(1_024))),
            capacity,
            dropped: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<TransitionEvent>> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Append, evicting the oldest event when full. Never blocks on I/O.
    pub fn push(&self, event: TransitionEvent) {
        let evicted = {
            let mut queue = self.lock();
            let evicted = if queue.len() >= self.capacity {
                queue.pop_front().is_some()
            } else {
                false
            };
            queue.push_back(event);
            evicted
        };
        if evicted {
            self.note_dropped(1);
        }
    }

    /// Put a failed batch back in front (it is older than anything queued
    /// since). If that overflows the bound, the oldest events are dropped.
    pub fn requeue_front(&self, events: Vec<TransitionEvent>) {
        let overflow = {
            let mut queue = self.lock();
            for event in events.into_iter().rev() {
                queue.push_front(event);
            }
            let overflow = queue.len().saturating_sub(self.capacity);
            queue.drain(..overflow);
            overflow
        };
        if overflow > 0 {
            self.note_dropped(overflow as u64);
        }
    }

    /// Remove up to `max` oldest events.
    pub fn drain(&self, max: usize) -> Vec<TransitionEvent> {
        let mut queue = self.lock();
        let n = max.min(queue.len());
        queue.drain(..n).collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn note_dropped(&self, n: u64) {
        self.dropped.fetch_add(n, Ordering::Relaxed);
        metrics::counter!("orch8_cloud_observability_dropped_total").increment(n);
    }
}

static BUFFER: OnceLock<Arc<ExportBuffer>> = OnceLock::new();

/// Hot-path hook: record a transition if export is enabled. O(1), no I/O.
pub fn record_transition(instance_id: InstanceId, state: InstanceState, step_id: Option<&str>) {
    if let Some(buffer) = BUFFER.get() {
        buffer.push(TransitionEvent {
            instance_id,
            state,
            at: Utc::now(),
            step_id: step_id.map(ToOwned::to_owned),
        });
    }
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

/// Exponential backoff: `base * 2^failures`, capped at `max`.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    failures: u32,
}

impl Backoff {
    #[must_use]
    pub const fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            failures: 0,
        }
    }

    /// Record a failure and return the delay before the next attempt.
    pub fn next_delay(&mut self) -> Duration {
        let factor = 1_u32.checked_shl(self.failures.min(20)).unwrap_or(u32::MAX);
        self.failures = self.failures.saturating_add(1);
        self.base.saturating_mul(factor).min(self.max)
    }

    pub const fn reset(&mut self) {
        self.failures = 0;
    }

    #[must_use]
    pub const fn failures(&self) -> u32 {
        self.failures
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("ingest request failed: {0}")]
    Transport(String),
    #[error("ingest rejected the batch with HTTP {0}")]
    Status(u16),
}

pub type SendFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ExportError>> + Send + 'a>>;

/// Delivery of one serialized batch. Abstracted for tests.
pub trait ExportTransport: Send + Sync {
    fn send<'a>(&'a self, batch: &'a ExportBatch) -> SendFuture<'a>;
}

/// `POST {endpoint}/api/ingest/v1/runs` with a bearer token.
pub struct HttpTransport {
    client: reqwest::Client,
    url: String,
    api_key: orch8_types::SecretString,
}

impl HttpTransport {
    pub fn new(config: &CloudObservabilityConfig) -> Result<Self, String> {
        let endpoint = validate_endpoint(&config.endpoint)?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            url: format!("{endpoint}/api/ingest/v1/runs"),
            api_key: config.api_key.clone(),
        })
    }
}

impl ExportTransport for HttpTransport {
    fn send<'a>(&'a self, batch: &'a ExportBatch) -> SendFuture<'a> {
        Box::pin(async move {
            let response = self
                .client
                .post(&self.url)
                .bearer_auth(self.api_key.expose())
                .json(batch)
                .send()
                .await
                .map_err(|e| ExportError::Transport(e.without_url().to_string()))?;
            let status = response.status();
            if status.is_success() {
                Ok(())
            } else {
                Err(ExportError::Status(status.as_u16()))
            }
        })
    }
}

/// HTTPS required, except plain HTTP to a loopback host (local testing).
pub fn validate_endpoint(endpoint: &str) -> Result<String, String> {
    let endpoint = endpoint.trim().trim_end_matches('/');
    let url = url::Url::parse(endpoint)
        .map_err(|e| format!("cloud_observability.endpoint is not a URL: {e}"))?;
    let loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    match url.scheme() {
        "https" => Ok(endpoint.to_owned()),
        "http" if loopback => Ok(endpoint.to_owned()),
        _ => Err("cloud_observability.endpoint must use https:// (http only for loopback)".into()),
    }
}

// ---------------------------------------------------------------------------
// Exporter
// ---------------------------------------------------------------------------

type EnrichFuture<'a> = Pin<Box<dyn Future<Output = Vec<ExportEvent>> + Send + 'a>>;

/// Turns captured transitions into wire events.
trait Enrich: Send {
    fn enrich<'a>(&'a mut self, events: &'a [TransitionEvent]) -> EnrichFuture<'a>;
}

impl Enrich for Enricher {
    fn enrich<'a>(&'a mut self, events: &'a [TransitionEvent]) -> EnrichFuture<'a> {
        Box::pin(self.enrich_events(events))
    }
}

/// Resolves sequence identity for events; cached per sequence id.
struct Enricher {
    storage: Arc<dyn StorageBackend>,
    sequences: HashMap<SequenceId, (String, i64)>,
}

impl Enricher {
    async fn enrich_events(&mut self, events: &[TransitionEvent]) -> Vec<ExportEvent> {
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            let instance = self
                .storage
                .get_instance(event.instance_id)
                .await
                .ok()
                .flatten();
            let (sequence_name, sequence_version, created_at) = match &instance {
                Some(instance) => {
                    let identity = self.sequence(instance.sequence_id).await;
                    (
                        identity
                            .as_ref()
                            .map(|(n, _)| n.clone())
                            .unwrap_or_default(),
                        identity.map(|(_, v)| v),
                        Some(instance.created_at),
                    )
                }
                None => (String::new(), None, None),
            };
            let duration_ms = if event.state.is_terminal() {
                created_at.map(|created| (event.at - created).num_milliseconds().max(0))
            } else {
                None
            };
            out.push(ExportEvent {
                instance_id: event.instance_id.to_string(),
                sequence_name,
                sequence_version,
                state: event.state.to_string(),
                at: event.at,
                step_id: event.step_id.clone(),
                duration_ms,
                // Only a coarse, data-free classification is allowed here.
                error_kind: (event.state == InstanceState::Failed).then(|| "failed".to_owned()),
                sub_tenant: None,
            });
        }
        out
    }

    async fn sequence(&mut self, id: SequenceId) -> Option<(String, i64)> {
        if let Some(hit) = self.sequences.get(&id) {
            return Some(hit.clone());
        }
        let sequence = self.storage.get_sequence(id).await.ok().flatten()?;
        if self.sequences.len() >= SEQUENCE_CACHE_LIMIT {
            self.sequences.clear();
        }
        let identity = (sequence.name.clone(), i64::from(sequence.version));
        self.sequences.insert(id, identity.clone());
        Some(identity)
    }
}

/// Drain the buffer in batches of at most [`MAX_BATCH`] and deliver them.
/// Returns the number of events delivered and whether a failure occurred
/// (the failed batch is requeued at the front).
async fn flush(
    buffer: &ExportBuffer,
    transport: &dyn ExportTransport,
    enricher: &mut dyn Enrich,
    engine_id: &str,
) -> (usize, bool) {
    let mut delivered = 0;
    for _ in 0..MAX_BATCHES_PER_TICK {
        let raw = buffer.drain(MAX_BATCH);
        if raw.is_empty() {
            break;
        }
        let batch = ExportBatch {
            engine_id: engine_id.to_owned(),
            engine_version: env!("CARGO_PKG_VERSION"),
            sent_at: Utc::now(),
            events: enricher.enrich(&raw).await,
        };
        match transport.send(&batch).await {
            Ok(()) => {
                delivered += raw.len();
                metrics::counter!("orch8_cloud_observability_exported_total")
                    .increment(raw.len() as u64);
            }
            Err(error) => {
                tracing::warn!(%error, events = raw.len(), "cloud observability export failed; will retry");
                buffer.requeue_front(raw);
                return (delivered, true);
            }
        }
    }
    (delivered, false)
}

/// Background loop. Exits on `cancel` after one best-effort final flush.
pub async fn run_exporter(
    buffer: Arc<ExportBuffer>,
    transport: Arc<dyn ExportTransport>,
    storage: Arc<dyn StorageBackend>,
    engine_id: String,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut enricher = Enricher {
        storage,
        sequences: HashMap::new(),
    };
    let mut backoff = Backoff::new(Duration::from_secs(1), MAX_BACKOFF);
    let mut wait = interval;
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                let final_flush = flush(&buffer, transport.as_ref(), &mut enricher, &engine_id);
                let _ = tokio::time::timeout(Duration::from_secs(5), final_flush).await;
                break;
            }
            () = tokio::time::sleep(wait) => {}
        }
        let (_, failed) = flush(&buffer, transport.as_ref(), &mut enricher, &engine_id).await;
        wait = if failed {
            backoff.next_delay().max(interval)
        } else {
            backoff.reset();
            interval
        };
    }
}

/// Install the global buffer and spawn the exporter. `Ok(None)` when export
/// is not configured. Call once per process.
pub fn spawn(
    config: &CloudObservabilityConfig,
    storage: Arc<dyn StorageBackend>,
    cancel: CancellationToken,
) -> Result<Option<tokio::task::JoinHandle<()>>, String> {
    if !config.enabled() {
        return Ok(None);
    }
    if config.api_key.is_empty() {
        return Err("cloud_observability.api_key is required when endpoint is set".into());
    }
    if config.engine_id.trim().is_empty() {
        return Err("cloud_observability.engine_id is required when endpoint is set".into());
    }
    let transport: Arc<dyn ExportTransport> = Arc::new(HttpTransport::new(config)?);
    let buffer =
        Arc::clone(BUFFER.get_or_init(|| Arc::new(ExportBuffer::new(config.max_buffered_events))));
    let interval = Duration::from_millis(config.interval_ms.max(MIN_INTERVAL_MS));
    Ok(Some(tokio::spawn(run_exporter(
        buffer,
        transport,
        storage,
        config.engine_id.trim().to_owned(),
        interval,
        cancel,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(n: u128) -> TransitionEvent {
        TransitionEvent {
            instance_id: InstanceId::from_uuid(uuid::Uuid::from_u128(n)),
            state: InstanceState::Running,
            at: Utc::now(),
            step_id: None,
        }
    }

    #[derive(Default)]
    struct FakeTransport {
        fail_first: std::sync::atomic::AtomicUsize,
        sent: Mutex<Vec<Vec<String>>>,
    }

    impl ExportTransport for FakeTransport {
        fn send<'a>(&'a self, batch: &'a ExportBatch) -> SendFuture<'a> {
            Box::pin(async move {
                if self.fail_first.load(Ordering::SeqCst) > 0 {
                    self.fail_first.fetch_sub(1, Ordering::SeqCst);
                    return Err(ExportError::Status(503));
                }
                self.sent
                    .lock()
                    .unwrap()
                    .push(batch.events.iter().map(|e| e.instance_id.clone()).collect());
                Ok(())
            })
        }
    }

    struct Plain;

    impl Enrich for Plain {
        fn enrich<'a>(&'a mut self, events: &'a [TransitionEvent]) -> EnrichFuture<'a> {
            let out = plain(events);
            Box::pin(async move { out })
        }
    }

    fn plain(events: &[TransitionEvent]) -> Vec<ExportEvent> {
        events
            .iter()
            .map(|e| ExportEvent {
                instance_id: e.instance_id.to_string(),
                sequence_name: "s".into(),
                sequence_version: Some(1),
                state: e.state.to_string(),
                at: e.at,
                step_id: None,
                duration_ms: None,
                error_kind: None,
                sub_tenant: None,
            })
            .collect()
    }

    #[test]
    fn buffer_drops_oldest_when_full() {
        let buffer = ExportBuffer::new(3);
        for n in 1..=5 {
            buffer.push(event(n));
        }
        assert_eq!(buffer.dropped(), 2);
        let ids: Vec<_> = buffer
            .drain(10)
            .into_iter()
            .map(|e| e.instance_id)
            .collect();
        assert_eq!(
            ids,
            (3..=5)
                .map(|n| InstanceId::from_uuid(uuid::Uuid::from_u128(n)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn requeue_keeps_order_and_bound() {
        let buffer = ExportBuffer::new(4);
        buffer.push(event(3));
        buffer.push(event(4));
        buffer.requeue_front(vec![event(0), event(1), event(2)]);
        // 5 events for 4 slots: the oldest (0) is dropped.
        assert_eq!(buffer.dropped(), 1);
        let ids: Vec<_> = buffer
            .drain(10)
            .into_iter()
            .map(|e| e.instance_id)
            .collect();
        assert_eq!(
            ids,
            (1..=4)
                .map(|n| InstanceId::from_uuid(uuid::Uuid::from_u128(n)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn backoff_doubles_caps_and_resets() {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
        let delays: Vec<u64> = (0..8).map(|_| backoff.next_delay().as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60]);
        for _ in 0..100 {
            backoff.next_delay();
        }
        assert_eq!(backoff.next_delay(), Duration::from_secs(60));
        backoff.reset();
        assert_eq!(backoff.failures(), 0);
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn flush_batches_at_most_500_events() {
        let buffer = ExportBuffer::new(10_000);
        for n in 0..1_200 {
            buffer.push(event(n));
        }
        let transport = FakeTransport::default();
        let (delivered, failed) = flush(&buffer, &transport, &mut Plain, "engine-1").await;
        assert_eq!((delivered, failed), (1_200, false));
        let sizes: Vec<usize> = transport
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(Vec::len)
            .collect();
        assert_eq!(sizes, [500, 500, 200]);
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn failed_batch_is_retried_without_loss_or_reordering() {
        let buffer = ExportBuffer::new(10_000);
        for n in 0..10 {
            buffer.push(event(n));
        }
        let transport = FakeTransport::default();
        transport.fail_first.store(1, Ordering::SeqCst);
        let (delivered, failed) = flush(&buffer, &transport, &mut Plain, "e").await;
        assert_eq!((delivered, failed), (0, true));
        assert_eq!(buffer.len(), 10);
        let (delivered, failed) = flush(&buffer, &transport, &mut Plain, "e").await;
        assert_eq!((delivered, failed), (10, false));
        let sent = transport.sent.lock().unwrap();
        let expected: Vec<String> = (0..10)
            .map(|n| uuid::Uuid::from_u128(n).to_string())
            .collect();
        assert_eq!(sent[0], expected);
    }

    #[test]
    fn wire_event_carries_metadata_only() {
        let wire = serde_json::to_value(&plain(&[event(1)])[0]).unwrap();
        let mut keys: Vec<&str> = wire
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "at",
                "duration_ms",
                "error_kind",
                "instance_id",
                "sequence_name",
                "sequence_version",
                "state",
                "step_id",
                "sub_tenant",
            ]
        );
    }

    #[test]
    fn endpoint_requires_https_except_loopback() {
        assert!(validate_endpoint("https://cloud.orch8.io/").is_ok());
        assert!(validate_endpoint("http://127.0.0.1:9000").is_ok());
        assert!(validate_endpoint("http://cloud.orch8.io").is_err());
        assert!(validate_endpoint("not a url").is_err());
    }

    #[tokio::test]
    async fn enrichment_reads_sequence_identity_without_payloads() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let tenant = orch8_types::ids::TenantId::new("t").unwrap();
        let sequence: orch8_types::sequence::SequenceDefinition =
            serde_json::from_value(serde_json::json!({
                "id": uuid::Uuid::now_v7(), "tenant_id": "t", "namespace": "default",
                "name": "billing", "version": 3, "created_at": Utc::now(),
                "blocks": [{"type": "step", "id": "a", "handler": "noop", "params": {}}]
            }))
            .unwrap();
        storage.create_sequence(&sequence).await.unwrap();
        let now = Utc::now();
        let instance = orch8_types::instance::TaskInstance {
            id: InstanceId::new(),
            sequence_id: sequence.id,
            tenant_id: tenant,
            namespace: orch8_types::ids::Namespace::new("default"),
            state: InstanceState::Completed,
            next_fire_at: None,
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({"secret": "do-not-export"}),
            context: orch8_types::context::ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now - chrono::Duration::seconds(2),
            updated_at: now,
        };
        storage.create_instance(&instance).await.unwrap();
        let mut enricher = Enricher {
            storage,
            sequences: HashMap::new(),
        };
        let out = enricher
            .enrich_events(&[TransitionEvent {
                instance_id: instance.id,
                state: InstanceState::Completed,
                at: now,
                step_id: Some("a".into()),
            }])
            .await;
        assert_eq!(out[0].sequence_name, "billing");
        assert_eq!(out[0].sequence_version, Some(3));
        assert!(out[0].duration_ms.unwrap() >= 1_900);
        let json = serde_json::to_string(&out).unwrap();
        assert!(!json.contains("do-not-export"));
    }
}
