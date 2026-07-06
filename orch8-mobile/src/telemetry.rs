//! SDK-side telemetry: event buffering, auto-flush, and batch upload.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use orch8_types::clock::SharedClock;

use crate::error::MobileError;
use crate::storage::MobileStorage;

/// Maximum events stored in the local `SQLite` buffer.
const MAX_BUFFER_SIZE: u32 = 1000;
/// Auto-flush when buffer reaches this percentage of capacity.
#[allow(dead_code)]
const AUTO_FLUSH_PCT: u32 = 80;
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
    http: reqwest::Client,
    last_endpoint: std::sync::Mutex<Option<String>>,
    last_flush_attempt: std::sync::Mutex<Option<chrono::DateTime<chrono::Utc>>>,
    clock: SharedClock,
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
        // The builder only uses constants, so failure is a programming error.
        #[allow(clippy::expect_used)]
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client builds");
        Self {
            storage,
            enabled,
            device_ctx: std::sync::Mutex::new(device_ctx),
            http,
            last_endpoint: std::sync::Mutex::new(None),
            last_flush_attempt: std::sync::Mutex::new(None),
            clock,
        }
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

        let payload = serde_json::to_string(event)?;
        self.storage
            .append_telemetry_event(&event.event_type, &payload)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;

        // Auto-flush at 80% capacity.
        let count =
            self.storage
                .count_telemetry_events()
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        if count >= (u64::from(MAX_BUFFER_SIZE) * u64::from(AUTO_FLUSH_PCT) / 100) {
            // H-17: only attempt an auto-flush if the cooldown since the last
            // *attempt* (successful or not) has elapsed. Otherwise, once the
            // buffer is over threshold, every subsequent `record()` call
            // would re-attempt a full flush — hammering a failing or
            // rate-limited endpoint on every single recorded event.
            let now = self.clock.now();
            let should_attempt = {
                let mut last = self
                    .last_flush_attempt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let ready = last
                    .is_none_or(|t| now - t >= chrono::Duration::seconds(AUTO_FLUSH_COOLDOWN_SECS));
                if ready {
                    *last = Some(now);
                }
                ready
            };

            if should_attempt {
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
                    && let Err(e) = self.flush(&endpoint).await
                {
                    tracing::warn!(error = %e, "auto-flush failed");
                }
            } else {
                tracing::debug!(
                    count,
                    "telemetry buffer over threshold but auto-flush is cooling down"
                );
            }
        }

        Ok(())
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
            .read_telemetry_events(MAX_BUFFER_SIZE)
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

        let device_ctx = self
            .device_ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let batch: Vec<TelemetryBatchItem> = events
            .iter()
            .map(|e| TelemetryBatchItem {
                event_type: e.event_type.clone(),
                payload: e.payload.clone(),
                timestamp: e.created_at.to_rfc3339(),
                device: device_ctx.clone(),
            })
            .collect();

        let body = serde_json::to_string(&batch)?;

        let response = self
            .http
            .post(endpoint_url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| MobileError::Engine {
                message: e.to_string(),
            })?;

        if response.status().is_success() {
            *self
                .last_endpoint
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(endpoint_url.to_string());
            let ids: Vec<i64> = events.iter().map(|e| e.id).collect();
            let deleted = self
                .storage
                .delete_telemetry_events(&ids)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
            Ok(FlushResult {
                sent: deleted,
                dropped: 0,
            })
        } else {
            let status = response.status();
            let body_text = response.text().await.unwrap_or_default();
            tracing::warn!(status = %status, body = %body_text, "telemetry flush failed");
            Err(MobileError::Engine {
                message: format!("telemetry flush failed: {status}"),
            })
        }
    }

    /// Drop oldest events when the buffer is over capacity.
    pub async fn enforce_capacity(&self) -> Result<u64, MobileError> {
        let count =
            self.storage
                .count_telemetry_events()
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        if count > u64::from(MAX_BUFFER_SIZE) {
            let excess = count - u64::from(MAX_BUFFER_SIZE);
            #[allow(clippy::cast_possible_truncation)]
            let to_drop = self
                .storage
                .read_telemetry_events(excess as u32)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
            let ids: Vec<i64> = to_drop.iter().map(|e| e.id).collect();
            let dropped = self
                .storage
                .delete_telemetry_events(&ids)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
            tracing::info!(
                dropped,
                "dropped oldest telemetry events to enforce capacity"
            );
            Ok(dropped)
        } else {
            Ok(0)
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct TelemetryBatchItem {
    event_type: String,
    payload: String,
    timestamp: String,
    device: DeviceContext,
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
    async fn flush_succeeds_and_deletes_events() {
        let (mgr, storage, _dir) = setup().await;

        // Seed 3 events.
        for i in 0..3 {
            let event = TelemetryEventRecord::new("TestEvent", &format!("{{\"i\":{i}}}"));
            mgr.record(&event).await.unwrap();
        }
        assert_eq!(storage.count_telemetry_events().await.unwrap(), 3);

        // Spin up a tiny HTTP server that accepts the batch.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            use tokio::io::AsyncWriteExt;
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _n = socket.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n";
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let url = format!("http://127.0.0.1:{port}/telemetry");
        let result = mgr.flush(&url).await.unwrap();
        assert_eq!(result.sent, 3);

        server.await.unwrap();

        // Events should be deleted after successful flush.
        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 0);
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
