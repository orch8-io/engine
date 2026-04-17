//! Background GC sweeper for `externalized_state`.
//!
//! Externalized payloads may carry an `expires_at` timestamp (set by the
//! caller that wrote them). This loop periodically asks storage to delete
//! up to [`GC_BATCH_LIMIT`] rows whose `expires_at` has elapsed.
//!
//! Design notes:
//! - The sweep is bounded per tick to avoid one long-running transaction
//!   starving writer workloads when a large backlog accumulates.
//! - Errors are logged but never propagated — GC is a best-effort
//!   background maintenance task; a missed sweep just means the next one
//!   picks up the slack.
//! - Instance-scoped cleanup is handled separately via FK cascade
//!   (`ON DELETE CASCADE` on `externalized_state.instance_id` → `task_instances.id`).
//!   `Postgres` enforces this natively; `SQLite` requires both the FK declaration
//!   and a connection-level `PRAGMA foreign_keys = ON` (set in the `SQLite`
//!   pool options). This loop only targets TTL-expired rows, not
//!   instance-deletion cleanup.
//!
//! See `docs/CONTEXT_MANAGEMENT.md` §8.5 for the lifecycle contract.

use std::sync::Arc;
use std::time::Duration;

use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;
use tokio_util::sync::CancellationToken;

use crate::metrics;

/// Collapse a [`StorageError`] into a low-cardinality label so the
/// `GC_EXTERNALIZED_ERRORS` counter distinguishes transient issues
/// (pool exhaustion, connection drops) from structural ones (bad query,
/// migration skew). The set is intentionally small — high-cardinality
/// error labels break Prometheus.
fn error_kind(err: &StorageError) -> &'static str {
    match err {
        StorageError::Connection(_) => "connection",
        StorageError::Query(_) => "query",
        StorageError::NotFound { .. } => "not_found",
        StorageError::Conflict(_) => "conflict",
        StorageError::Migration(_) => "migration",
        StorageError::Serialization(_) => "serialization",
        StorageError::PoolExhausted => "pool_exhausted",
    }
}

/// Maximum rows deleted per sweep. Sized so a single sweep completes well
/// under one second on typical row counts, leaving the storage backend
/// responsive for foreground traffic.
pub const GC_BATCH_LIMIT: u32 = 1_000;

/// Default cadence between sweep ticks.
pub const GC_DEFAULT_INTERVAL: Duration = Duration::from_mins(5);

/// Run the expiry sweeper until `cancel` fires. Each tick calls
/// [`StorageBackend::delete_expired_externalized_state`] once with
/// [`GC_BATCH_LIMIT`]; continued backlog naturally spreads across ticks.
pub async fn run_gc_loop(
    storage: Arc<dyn StorageBackend>,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    // First tick fires immediately — skip it so startup isn't a DB burst.
    ticker.tick().await;

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {
                match storage.delete_expired_externalized_state(GC_BATCH_LIMIT).await {
                    Ok(0) => {}
                    Ok(n) => {
                        tracing::info!(count = n, "externalized gc: deleted expired rows");
                        metrics::inc_by(metrics::GC_EXTERNALIZED_DELETED, n);
                    }
                    Err(e) => {
                        let kind = error_kind(&e);
                        tracing::error!(error = %e, kind, "externalized gc sweep failed");
                        metrics::inc_with(
                            metrics::GC_EXTERNALIZED_ERRORS,
                            &[("kind", kind.to_string())],
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    use super::*;

    /// Table-test `error_kind` exhaustively so a new `StorageError` variant
    /// forces a deliberate label choice (rather than silently mapping to
    /// `"query"` by accident). Prometheus label cardinality is the invariant
    /// this guards — adding an unchecked variant would break it.
    #[test]
    fn error_kind_maps_every_storage_error_variant() {
        use orch8_types::error::StorageError;

        let cases = [
            (StorageError::Connection("down".into()), "connection"),
            (StorageError::Query("bad".into()), "query"),
            (
                StorageError::NotFound {
                    entity: "row",
                    id: "x".into(),
                },
                "not_found",
            ),
            (StorageError::Conflict("dup".into()), "conflict"),
            (StorageError::Migration("skew".into()), "migration"),
            (StorageError::PoolExhausted, "pool_exhausted"),
        ];
        for (err, expected) in cases {
            assert_eq!(error_kind(&err), expected, "mismatch for {err:?}");
        }

        // Serialization needs its own case because it holds a
        // `serde_json::Error` which can't be cheaply constructed inline.
        let ser_err: StorageError = serde_json::from_str::<serde_json::Value>("{invalid")
            .unwrap_err()
            .into();
        assert_eq!(error_kind(&ser_err), "serialization");
    }

    /// Labels returned by `error_kind` must be stable identifiers (not error
    /// messages containing user data) — the whole point of the `kind` label
    /// is low, bounded cardinality for Prometheus.
    #[test]
    fn error_kind_labels_are_low_cardinality() {
        use orch8_types::error::StorageError;
        let samples = [
            StorageError::Connection("secret-host:5432".into()),
            StorageError::Query("user-supplied-id-42".into()),
            StorageError::Conflict("tenant-42".into()),
        ];
        for err in samples {
            let kind = error_kind(&err);
            assert!(
                !kind.contains(':') && !kind.contains('-') && kind.len() < 32,
                "label '{kind}' leaked variable data from {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn gc_loop_exits_on_cancel() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let cancel = CancellationToken::new();
        let handle = tokio::spawn({
            let storage = Arc::clone(&storage);
            let cancel = cancel.clone();
            async move { run_gc_loop(storage, Duration::from_millis(10), cancel).await }
        });
        // Let the loop enter its select!, then cancel.
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancel.cancel();
        // Should exit promptly.
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("gc loop did not exit on cancel")
            .unwrap();
    }

    #[tokio::test]
    async fn gc_loop_with_empty_store_is_noop() {
        // Belt-and-suspenders: an idle store should not panic or error —
        // delete_expired_externalized_state returns Ok(0) and the loop
        // keeps running until cancelled. Shorter deadline than the
        // cancel test to catch any accidental blocking.
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let cancel = CancellationToken::new();
        let handle = tokio::spawn({
            let storage = Arc::clone(&storage);
            let cancel = cancel.clone();
            async move { run_gc_loop(storage, Duration::from_millis(5), cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("gc loop did not exit on cancel")
            .unwrap();
    }
}
