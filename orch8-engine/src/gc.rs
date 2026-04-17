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
//! - The FK between `externalized_state` and `task_instances` uses
//!   `ON DELETE CASCADE` (migration 024) so instance deletion handles its
//!   own payload cleanup atomically; this loop only targets TTL-expired
//!   rows.
//!
//! See `docs/CONTEXT_MANAGEMENT.md` §8.5 for the lifecycle contract.

use std::sync::Arc;
use std::time::Duration;

use orch8_storage::StorageBackend;
use tokio_util::sync::CancellationToken;

use crate::metrics;

/// Maximum rows deleted per sweep. Sized so a single sweep completes well
/// under one second on typical row counts, leaving the storage backend
/// responsive for foreground traffic.
pub const GC_BATCH_LIMIT: u32 = 1_000;

/// Default cadence between sweep ticks.
pub const GC_DEFAULT_INTERVAL: Duration = Duration::from_secs(300);

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
                        tracing::error!(error = %e, "externalized gc sweep failed");
                        metrics::inc(metrics::GC_EXTERNALIZED_ERRORS);
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
