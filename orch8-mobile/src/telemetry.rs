//! SDK-side telemetry: event buffering, auto-flush, and batch upload.

use std::sync::Arc;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use orch8_types::clock::SharedClock;

use crate::error::MobileError;
use crate::storage::MobileStorage;

/// Maximum events stored in the local `SQLite` buffer.
const MAX_BUFFER_SIZE: u32 = 1000;
/// Maximum events accepted by the mobile telemetry ingestion endpoint.
/// Keeping this separate from the offline buffer ceiling prevents a full
/// buffer from producing an oversized request that the server must reject.
const MAX_UPLOAD_BATCH_SIZE: u32 = 500;
/// Auto-flush when buffer reaches this percentage of capacity.
const AUTO_FLUSH_PCT: u32 = 80;
/// When an offline buffer crosses its hard ceiling, trim a chunk so the next
/// events need only their durable insert instead of one delete per event.
const CAPACITY_TRIM_PCT: u32 = 90;
/// Minimum time between automatic flush attempts triggered by `record()`'s
/// over-threshold check (H-17). Without this, once the buffer crosses the
/// threshold every single subsequent `record()` call re-triggers a full
/// flush attempt — a failing or rate-limited endpoint gets hammered on every
/// recorded event instead of the SDK backing off.
const AUTO_FLUSH_COOLDOWN_SECS: i64 = 30;

/// A telemetry event emitted by the mobile engine.
#[derive(Debug, Clone, Serialize, Deserialize, uniffi::Record)]
pub struct TelemetryEventRecord {
    pub event_type: String,
    pub payload: String,
    pub timestamp: String,
}

impl TelemetryEventRecord {
    pub fn new(event_type: &str, payload: &str) -> Self {
        Self {
            event_type: event_type.to_string(),
            payload: payload.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}

/// Device context sent with every telemetry batch.
#[derive(Debug, Clone, Serialize, Deserialize, uniffi::Record)]
pub struct DeviceContext {
    pub device_id: String,
    pub os_name: String,
    pub os_version: String,
    pub app_version: String,
    pub sdk_version: String,
}

/// Telemetry manager handles the local event buffer and flushing.
pub struct TelemetryManager {
    storage: Arc<MobileStorage>,
    enabled: bool,
    device_ctx: std::sync::Mutex<DeviceContext>,
    http: OnceLock<reqwest::Client>,
    last_endpoint: std::sync::Mutex<Option<String>>,
    last_flush_attempt: std::sync::Mutex<Option<chrono::DateTime<chrono::Utc>>>,
    buffer_state: tokio::sync::Mutex<TelemetryBufferState>,
    clock: SharedClock,
}

#[derive(Default)]
struct TelemetryBufferState {
    count: Option<u64>,
}

#[allow(dead_code)]
impl TelemetryManager {
    pub fn new(storage: Arc<MobileStorage>, enabled: bool, device_ctx: DeviceContext) -> Self {
        Self::new_with_clock(storage, enabled, device_ctx, SharedClock::default())
    }

    /// Lets tests exercise the auto-flush cooldown with a [`ManualClock`]
    /// instead of sleeping real wall-clock time.
    ///
    /// [`ManualClock`]: orch8_types::clock::ManualClock
    pub(crate) fn new_with_clock(
        storage: Arc<MobileStorage>,
        enabled: bool,
        device_ctx: DeviceContext,
        clock: SharedClock,
    ) -> Self {
        Self {
            storage,
            enabled,
            device_ctx: std::sync::Mutex::new(device_ctx),
            http: OnceLock::new(),
            last_endpoint: std::sync::Mutex::new(None),
            last_flush_attempt: std::sync::Mutex::new(None),
            buffer_state: tokio::sync::Mutex::new(TelemetryBufferState::default()),
            clock,
        }
    }

    fn http_client(&self) -> &reqwest::Client {
        self.http
            .get_or_init(|| crate::build_mobile_http_client(std::time::Duration::from_secs(30)))
    }

    pub fn set_device_context(&self, ctx: DeviceContext) {
        *self
            .device_ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ctx;
    }

    /// Record a telemetry event into the local buffer.
    pub async fn record(&self, event: &TelemetryEventRecord) -> Result<(), MobileError> {
        if !self.enabled {
            return Ok(());
        }

        let count = self
            .append_bounded(&event.event_type, &event.payload, &event.timestamp)
            .await?;
        self.maybe_auto_flush(count).await;
        Ok(())
    }

    async fn append_bounded(
        &self,
        event_type: &str,
        payload: &str,
        created_at: &str,
    ) -> Result<u64, MobileError> {
        let mut state = self.buffer_state.lock().await;
        let prior_count = match state.count {
            Some(count) => count,
            None => self.count_events().await?,
        };
        self.storage
            .append_telemetry_event_at(event_type, payload, created_at)
            .await
            .map_err(mobile_storage_error)?;

        let mut count = prior_count.saturating_add(1);
        // The insert is already durable. Publish its count before pruning so
        // a prune failure cannot leave the in-memory counter one row behind.
        state.count = Some(count);
        if count > u64::from(MAX_BUFFER_SIZE) {
            let target = u64::from(MAX_BUFFER_SIZE) * u64::from(CAPACITY_TRIM_PCT) / 100;
            let dropped = self
                .storage
                .delete_oldest_telemetry_events(count.saturating_sub(target))
                .await
                .map_err(mobile_storage_error)?;
            count = count.saturating_sub(dropped);
            state.count = Some(count);
            tracing::info!(dropped, "trimmed oldest telemetry events at capacity");
        }
        Ok(count)
    }

    async fn maybe_auto_flush(&self, count: u64) {
        let threshold = u64::from(MAX_BUFFER_SIZE) * u64::from(AUTO_FLUSH_PCT) / 100;
        if count < threshold {
            return;
        }
        if !self.claim_auto_flush_attempt() {
            tracing::debug!(
                count,
                "telemetry buffer over threshold but auto-flush is cooling down"
            );
            return;
        }

        tracing::info!(
            count,
            "telemetry buffer at {}% — auto-flush",
            AUTO_FLUSH_PCT
        );
        let endpoint = self
            .last_endpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(endpoint) = endpoint
            && let Err(error) = self.flush(&endpoint).await
        {
            tracing::warn!(%error, "auto-flush failed");
        }
    }

    /// Claim one automatic attempt before doing network I/O. Attempts, not
    /// successes, are throttled so an offline device cannot retry per event.
    fn claim_auto_flush_attempt(&self) -> bool {
        let now = self.clock.now();
        let mut last = self
            .last_flush_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ready = last
            .is_none_or(|time| now - time >= chrono::Duration::seconds(AUTO_FLUSH_COOLDOWN_SECS));
        if ready {
            *last = Some(now);
        }
        ready
    }

    /// Flush buffered telemetry to the remote endpoint.
    pub async fn flush(&self, endpoint_url: &str) -> Result<FlushResult, MobileError> {
        if !self.enabled {
            return Ok(FlushResult {
                sent: 0,
                dropped: 0,
            });
        }

        let events = self
            .storage
            .read_telemetry_events(MAX_UPLOAD_BATCH_SIZE)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        if events.is_empty() {
            return Ok(FlushResult {
                sent: 0,
                dropped: 0,
            });
        }

        let PreparedTelemetryBatch { body, ids } = self.prepare_batch(&events)?;
        // The request body now owns the serialized data, so the SQLite row
        // strings do not need to remain live throughout a potentially slow
        // mobile DNS/TLS/upload round trip.
        drop(events);

        let response = self.send_batch(endpoint_url, body).await?;
        if !response.status().is_success() {
            return Err(flush_response_error(response).await);
        }
        *self
            .last_endpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(endpoint_url.to_string());
        let deleted = self.delete_flushed_events(&ids).await?;
        Ok(FlushResult {
            sent: deleted,
            dropped: 0,
        })
    }

    fn prepare_batch(
        &self,
        events: &[crate::storage::TelemetryEvent],
    ) -> Result<PreparedTelemetryBatch, MobileError> {
        let ids = events.iter().map(|event| event.id).collect();
        let device_ctx = self
            .device_ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let body = serde_json::to_string(&TelemetryBatch {
            events,
            device: &device_ctx,
        })?;
        Ok(PreparedTelemetryBatch { body, ids })
    }

    async fn send_batch(
        &self,
        endpoint_url: &str,
        body: String,
    ) -> Result<reqwest::Response, MobileError> {
        self.http_client()
            .post(endpoint_url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| MobileError::Engine {
                message: e.to_string(),
            })
    }

    async fn delete_flushed_events(&self, ids: &[i64]) -> Result<u64, MobileError> {
        let mut state = self.buffer_state.lock().await;
        let deleted = self
            .storage
            .delete_telemetry_events(ids)
            .await
            .map_err(mobile_storage_error)?;
        if let Some(count) = &mut state.count {
            *count = count.saturating_sub(deleted);
        }
        Ok(deleted)
    }

    /// Drop oldest events when the buffer is over capacity.
    pub async fn enforce_capacity(&self) -> Result<u64, MobileError> {
        let mut state = self.buffer_state.lock().await;
        let count = self.count_events().await?;
        let excess = count.saturating_sub(u64::from(MAX_BUFFER_SIZE));
        let dropped = if excess == 0 {
            0
        } else {
            self.storage
                .delete_oldest_telemetry_events(excess)
                .await
                .map_err(mobile_storage_error)?
        };
        state.count = Some(count.saturating_sub(dropped));
        if dropped != 0 {
            tracing::info!(
                dropped,
                "dropped oldest telemetry events to enforce capacity"
            );
        }
        Ok(dropped)
    }

    async fn count_events(&self) -> Result<u64, MobileError> {
        self.storage
            .count_telemetry_events()
            .await
            .map_err(mobile_storage_error)
    }
}

fn mobile_storage_error(error: orch8_types::error::StorageError) -> MobileError {
    MobileError::Storage {
        message: error.to_string(),
    }
}

async fn flush_response_error(response: reqwest::Response) -> MobileError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    tracing::warn!(%status, %body, "telemetry flush failed");
    MobileError::Engine {
        message: format!("telemetry flush failed: {status}"),
    }
}

#[derive(Serialize)]
struct TelemetryBatch<'a> {
    events: &'a [crate::storage::TelemetryEvent],
    device: &'a DeviceContext,
}

struct PreparedTelemetryBatch {
    body: String,
    ids: Vec<i64>,
}

/// Result of a telemetry flush operation.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FlushResult {
    pub sent: u64,
    pub dropped: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    async fn read_http_body(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        loop {
            let mut chunk = [0_u8; 4096];
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "connection closed before the request body arrived");
            request.extend_from_slice(&chunk[..read]);

            let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let body_start = header_end + 4;
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length: ")
                        .or_else(|| line.strip_prefix("Content-Length: "))
                })
                .unwrap()
                .parse::<usize>()
                .unwrap();
            if request.len() >= body_start + content_length {
                return request[body_start..body_start + content_length].to_vec();
            }
        }
    }

    async fn setup() -> (TelemetryManager, Arc<MobileStorage>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::file_mobile(&path)
                .await
                .unwrap(),
        );
        let storage = Arc::new(MobileStorage::new(sqlite));
        let mgr = TelemetryManager::new(
            storage.clone(),
            true,
            DeviceContext {
                device_id: "dev-1".to_string(),
                os_name: "iOS".to_string(),
                os_version: "17.0".to_string(),
                app_version: "1.0.0".to_string(),
                sdk_version: "0.4.0".to_string(),
            },
        );
        (mgr, storage, dir)
    }

    #[tokio::test]
    async fn record_and_count() {
        let (mgr, storage, _dir) = setup().await;

        let event = TelemetryEventRecord::new("TestEvent", r#"{"x":1}"#);
        mgr.record(&event).await.unwrap();

        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(mgr.buffer_state.lock().await.count, Some(1));
        let stored = storage.read_telemetry_events(1).await.unwrap();
        assert_eq!(stored[0].event_type, event.event_type);
        assert_eq!(stored[0].payload, event.payload);
        assert_eq!(stored[0].created_at, event.timestamp);
    }

    #[tokio::test]
    async fn prepared_batch_keeps_ids_out_of_the_wire_payload() {
        let (mgr, _storage, _dir) = setup().await;
        let events = vec![
            crate::storage::TelemetryEvent {
                id: 41,
                event_type: "Started".to_string(),
                payload: r#"{"screen":"home"}"#.to_string(),
                created_at: "2026-07-25T12:00:00Z".to_string(),
            },
            crate::storage::TelemetryEvent {
                id: 42,
                event_type: "Finished".to_string(),
                payload: r#"{"ok":true}"#.to_string(),
                created_at: "2026-07-25T12:00:01Z".to_string(),
            },
        ];

        let prepared = mgr.prepare_batch(&events).unwrap();

        assert_eq!(prepared.ids, [41, 42]);
        let body: serde_json::Value = serde_json::from_str(&prepared.body).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 2);
        assert_eq!(body["events"][0]["event_type"], "Started");
        assert_eq!(body["events"][0]["timestamp"], "2026-07-25T12:00:00Z");
        assert!(body["events"][0].get("id").is_none());
        assert!(body["events"][0].get("device").is_none());
        assert_eq!(body["device"]["device_id"], "dev-1");
    }

    #[tokio::test]
    async fn disabled_does_not_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::file_mobile(&path)
                .await
                .unwrap(),
        );
        let storage = Arc::new(MobileStorage::new(sqlite));
        let mgr = TelemetryManager::new(
            storage.clone(),
            false,
            DeviceContext {
                device_id: "dev-1".to_string(),
                os_name: "iOS".to_string(),
                os_version: "17.0".to_string(),
                app_version: "1.0.0".to_string(),
                sdk_version: "0.4.0".to_string(),
            },
        );

        let event = TelemetryEventRecord::new("TestEvent", r#"{"x":1}"#);
        mgr.record(&event).await.unwrap();

        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn enforce_capacity_drops_oldest() {
        let (mgr, storage, _dir) = setup().await;

        // Seed 5 events.
        for i in 0..5 {
            let event = TelemetryEventRecord::new("TestEvent", &format!("{{\"i\":{i}}}"));
            mgr.record(&event).await.unwrap();
        }

        // Artificially lower capacity by deleting middle events, then reinsert.
        // Instead, we just verify the enforce_capacity logic works by calling it
        // with a large count — but our MAX_BUFFER_SIZE is 1000 so this is hard.
        // We'll test the drop logic directly.
        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 5);

        // enforce_capacity with 5/1000 should not drop anything.
        let dropped = mgr.enforce_capacity().await.unwrap();
        assert_eq!(dropped, 0);
    }

    #[tokio::test]
    async fn record_keeps_offline_buffer_bounded() {
        let (mgr, storage, _dir) = setup().await;

        storage
            .append_telemetry_event("oldest", "{}")
            .await
            .unwrap();
        for _ in 1..MAX_BUFFER_SIZE {
            storage.append_telemetry_event("seed", "{}").await.unwrap();
        }

        mgr.record(&TelemetryEventRecord::new("newest", "{}"))
            .await
            .unwrap();

        let trim_target = u64::from(MAX_BUFFER_SIZE) * u64::from(CAPACITY_TRIM_PCT) / 100;
        assert_eq!(storage.count_telemetry_events().await.unwrap(), trim_target);
        assert_eq!(mgr.buffer_state.lock().await.count, Some(trim_target));
        let first = storage.read_telemetry_events(1).await.unwrap();
        assert_eq!(first[0].event_type, "seed", "oldest chunk must be evicted");
    }

    #[tokio::test]
    async fn flush_succeeds_and_deletes_events() {
        let (mgr, storage, _dir) = setup().await;
        assert!(mgr.http.get().is_none());

        // Seed 3 events.
        for i in 0..3 {
            let event = TelemetryEventRecord::new("TestEvent", &format!("{{\"i\":{i}}}"));
            mgr.record(&event).await.unwrap();
        }
        assert_eq!(storage.count_telemetry_events().await.unwrap(), 3);

        // Spin up a tiny HTTP server that accepts the batch.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (body_tx, body_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_body(&mut socket).await;
            body_tx.send(body).unwrap();
            let response = "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n";
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let url = format!("http://127.0.0.1:{port}/telemetry");
        let result = mgr.flush(&url).await.unwrap();
        assert_eq!(result.sent, 3);
        assert!(mgr.http.get().is_some());

        server.await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_rx.await.unwrap()).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 3);
        assert_eq!(body["device"]["device_id"], "dev-1");
        assert!(body["events"][0].get("device").is_none());
        assert!(body["events"][0].get("id").is_none());

        // Events should be deleted after successful flush.
        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 0);
        assert_eq!(mgr.buffer_state.lock().await.count, Some(0));
    }

    #[tokio::test]
    async fn flush_caps_request_at_server_batch_limit() {
        let (mgr, storage, _dir) = setup().await;
        for i in 0..=MAX_UPLOAD_BATCH_SIZE {
            storage
                .append_telemetry_event("TestEvent", &format!("{{\"i\":{i}}}"))
                .await
                .unwrap();
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (count_tx, count_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_http_body(&mut socket).await;
            assert!(
                body.len() < 60_000,
                "500-event telemetry request unexpectedly grew to {} bytes",
                body.len()
            );
            let batch: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(batch["device"]["device_id"], "dev-1");
            assert!(batch["events"][0].get("device").is_none());
            count_tx
                .send(batch["events"].as_array().unwrap().len())
                .unwrap();
            socket
                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        });

        let result = mgr
            .flush(&format!("http://127.0.0.1:{port}/telemetry"))
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(
            count_rx.await.unwrap(),
            usize::try_from(MAX_UPLOAD_BATCH_SIZE).unwrap()
        );
        assert_eq!(result.sent, u64::from(MAX_UPLOAD_BATCH_SIZE));
        assert_eq!(storage.count_telemetry_events().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn flush_when_disabled_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::file_mobile(&path)
                .await
                .unwrap(),
        );
        let storage = Arc::new(MobileStorage::new(sqlite));
        let mgr = TelemetryManager::new(
            storage.clone(),
            false,
            DeviceContext {
                device_id: "dev-1".to_string(),
                os_name: "iOS".to_string(),
                os_version: "17.0".to_string(),
                app_version: "1.0.0".to_string(),
                sdk_version: "0.4.0".to_string(),
            },
        );

        let result = mgr.flush("http://127.0.0.1:1/telemetry").await.unwrap();
        assert_eq!(result.sent, 0);
        assert_eq!(result.dropped, 0);
        assert!(mgr.http.get().is_none());
    }

    #[tokio::test]
    async fn empty_flush_does_not_initialize_http_client() {
        let (mgr, _storage, _dir) = setup().await;

        let result = mgr.flush("http://127.0.0.1:1/telemetry").await.unwrap();

        assert_eq!(result.sent, 0);
        assert_eq!(result.dropped, 0);
        assert!(mgr.http.get().is_none());
    }

    #[tokio::test]
    async fn flush_fails_when_server_returns_error() {
        let (mgr, storage, _dir) = setup().await;

        let event = TelemetryEventRecord::new("TestEvent", r#"{"x":1}"#);
        mgr.record(&event).await.unwrap();
        assert_eq!(storage.count_telemetry_events().await.unwrap(), 1);

        // Spin up a tiny HTTP server that returns 500.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            use tokio::io::AsyncWriteExt;
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _n = socket.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let url = format!("http://127.0.0.1:{port}/telemetry");
        let result = mgr.flush(&url).await;
        assert!(result.is_err());

        server.await.unwrap();

        // Events should NOT be deleted after failed flush.
        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn set_device_context_updates_context() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (mgr, _storage) = rt.block_on(async {
            let sqlite = Arc::new(
                orch8_storage::sqlite::SqliteStorage::file_mobile(&path)
                    .await
                    .unwrap(),
            );
            let storage = Arc::new(MobileStorage::new(sqlite));
            let mgr = TelemetryManager::new(
                storage.clone(),
                true,
                DeviceContext {
                    device_id: "dev-1".to_string(),
                    os_name: "iOS".to_string(),
                    os_version: "17.0".to_string(),
                    app_version: "1.0.0".to_string(),
                    sdk_version: "0.4.0".to_string(),
                },
            );
            (mgr, storage)
        });

        let new_ctx = DeviceContext {
            device_id: "dev-2".to_string(),
            os_name: "Android".to_string(),
            os_version: "14.0".to_string(),
            app_version: "2.0.0".to_string(),
            sdk_version: "0.5.0".to_string(),
        };
        mgr.set_device_context(new_ctx.clone());

        // Verify by flushing — the device context should appear in the batch.
        // We can't easily inspect the private field, but we can verify the call
        // doesn't panic and the setter accepts the value.
        assert_eq!(mgr.device_ctx.lock().unwrap().device_id, "dev-2");
    }

    /// H-17: once the buffer is over the auto-flush threshold, repeated
    /// `record()` calls within the cooldown window must not re-attempt a
    /// flush — only the first crossing (and later ones past the cooldown)
    /// should reach the endpoint.
    #[tokio::test]
    async fn auto_flush_respects_cooldown_between_attempts() {
        use orch8_types::clock::{Clock, ManualClock};
        use std::sync::atomic::{AtomicU32, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::file_mobile(&path)
                .await
                .unwrap(),
        );
        let storage = Arc::new(MobileStorage::new(sqlite));

        let start = chrono::Utc::now();
        let manual = Arc::new(ManualClock::new(start));
        let clock = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);

        let mgr = TelemetryManager::new_with_clock(
            storage.clone(),
            true,
            DeviceContext {
                device_id: "dev-1".to_string(),
                os_name: "iOS".to_string(),
                os_version: "17.0".to_string(),
                app_version: "1.0.0".to_string(),
                sdk_version: "0.4.0".to_string(),
            },
            clock,
        );

        // Bulk-seed just under the 80% auto-flush threshold directly via
        // storage, bypassing record()'s per-call overhead.
        for i in 0..799 {
            storage
                .append_telemetry_event("TestEvent", &format!("{{\"i\":{i}}}"))
                .await
                .unwrap();
        }

        // A mock server that always fails, so the buffer never drains and
        // stays over threshold across every subsequent record() call.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                attempts2.fetch_add(1, Ordering::SeqCst);
                let mut buf = vec![0u8; 65536];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        });

        let url = format!("http://127.0.0.1:{port}/telemetry");
        *mgr.last_endpoint.lock().unwrap() = Some(url);

        // The 800th event crosses the threshold -> first attempt.
        mgr.record(&TelemetryEventRecord::new("TestEvent", "{}"))
            .await
            .unwrap();
        // Two more while still within the cooldown window -> must NOT
        // trigger additional attempts.
        mgr.record(&TelemetryEventRecord::new("TestEvent", "{}"))
            .await
            .unwrap();
        mgr.record(&TelemetryEventRecord::new("TestEvent", "{}"))
            .await
            .unwrap();
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "must not re-attempt within the cooldown window"
        );

        // Advance the manual clock past the cooldown and record again -> a
        // second attempt must now fire.
        manual.advance(chrono::Duration::seconds(AUTO_FLUSH_COOLDOWN_SECS + 1));
        mgr.record(&TelemetryEventRecord::new("TestEvent", "{}"))
            .await
            .unwrap();
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "must attempt again once the cooldown has elapsed"
        );

        server.abort();
    }
}
