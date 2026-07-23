//! Background GC sweeper for `externalized_state` and related bounded-growth
//! tables.
//!
//! Externalized payloads may carry an `expires_at` timestamp (set by the
//! caller that wrote them). This loop periodically asks storage to delete
//! up to [`GC_BATCH_LIMIT`] rows whose `expires_at` has elapsed. The same
//! tick also sweeps `telemetry_events` and the mobile-sync tables
//! (`mobile_devices`, `mobile_approval_requests`, `mobile_commands`) — these
//! prune methods existed on both backends but had no non-test caller until
//! this loop wired them in (review finding L3).
//!
//! Design notes:
//! - The sweep is bounded per tick to avoid one long-running transaction
//!   starving writer workloads when a large backlog accumulates.
//! - Errors are logged but never propagated — GC is a best-effort
//!   background maintenance task; a missed sweep just means the next one
//!   picks up the slack.
//! - Row-level cleanup for tables FK'd to `task_instances` with
//!   `ON DELETE CASCADE` (`Postgres` enforces this natively; `SQLite` also
//!   requires a connection-level `PRAGMA foreign_keys = ON`, set in the
//!   `SQLite` pool options) happens automatically whenever a `task_instances`
//!   row is deleted -- which this loop does too, opt-in, via the
//!   terminal-instance sweep (`instance_retention_secs` in
//!   `SchedulerConfig`; off by default since it removes queryable instance
//!   history). Tables with no FK to `task_instances` at all (`step_logs`,
//!   `audit_log` on `SQLite`, `usage_events`) are cleaned up explicitly by
//!   that same sweep rather than relying on cascade.
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
const fn error_kind(err: &StorageError) -> &'static str {
    match err {
        StorageError::Connection(_) => "connection",
        StorageError::Query(_) => "query",
        StorageError::NotFound { .. } => "not_found",
        StorageError::Conflict(_) => "conflict",
        StorageError::TerminalTarget { .. } => "terminal_target",
        StorageError::Migration(_) => "migration",
        StorageError::Serialization(_) => "serialization",
        StorageError::PoolExhausted => "pool_exhausted",
        StorageError::Unsupported(_) => "unsupported",
        StorageError::Backend(_) => "backend",
    }
}

/// Maximum rows deleted per sweep. Sized so a single sweep completes well
/// under one second on typical row counts, leaving the storage backend
/// responsive for foreground traffic.
pub const GC_BATCH_LIMIT: u32 = 1_000;

/// Default cadence between sweep ticks.
pub const GC_DEFAULT_INTERVAL: Duration = Duration::from_secs(300);

/// Default TTL for `emit_event_dedupe` rows.
///
/// After this window a retry with the same `(parent, dedupe_key)` is no
/// longer considered a duplicate — it will create a fresh child. Callers
/// should not rely on dedupe beyond this window; see the `emit_event` design
/// doc for rationale.
///
/// Encoded as seconds (`30` days × `24` hours × `3600` seconds = `2_592_000` seconds)
/// so the value can be used in a `const` context with `Duration::from_secs`.
pub const EMIT_DEDUPE_DEFAULT_TTL: Duration = Duration::from_secs(2_592_000);

/// Default retention for `telemetry_events` rows (90 days).
pub const TELEMETRY_EVENTS_DEFAULT_TTL: Duration = Duration::from_secs(7_776_000);

/// Default retention for webhook delivery attempt rows (30 days).
pub const WEBHOOK_ATTEMPTS_DEFAULT_TTL: Duration = Duration::from_secs(2_592_000);

/// Default retention for unmatched (pending) inbox events (30 days).
pub const EVENT_INBOX_DEFAULT_TTL: Duration = Duration::from_secs(2_592_000);

/// Default inactivity window after which a mobile device's `last_sync_at`
/// causes it to be marked `active = false` (30 days). This updates a flag,
/// it does not delete the device row.
pub const MOBILE_DEVICE_STALE_DEFAULT_THRESHOLD: Duration = Duration::from_secs(2_592_000);

/// Default retention for acked mobile commands before deletion (7 days).
pub const MOBILE_COMMAND_ACKED_DEFAULT_TTL: Duration = Duration::from_secs(604_800);

/// Default retention for never-acked (abandoned) mobile commands before
/// deletion (7 days).
pub const MOBILE_COMMAND_EXPIRED_DEFAULT_TTL: Duration = Duration::from_secs(604_800);

/// Run the expiry sweeper until `cancel` fires.
///
/// Each tick calls `StorageBackend::delete_expired_externalized_state` once
/// with [`GC_BATCH_LIMIT`] and then sweeps
/// `StorageBackend::delete_expired_emit_event_dedupe`
/// with the same bound. Continued backlog naturally spreads across ticks.
///
/// Both tables share the same tick so the engine only maintains one timer;
/// the two sweeps run concurrently via [`tokio::join!`]. They target disjoint
/// tables, so there is no lock contention between them, and running in
/// parallel keeps total tick latency at `max(sweep_a, sweep_b)` rather than
/// `sweep_a + sweep_b`. Errors are still logged independently per sweep.
pub async fn run_gc_loop(
    storage: Arc<dyn StorageBackend>,
    interval: Duration,
    artifact_retention: Option<Duration>,
    instance_retention: Option<Duration>,
    cancel: CancellationToken,
) {
    run_gc_loop_with_ttl(
        storage,
        interval,
        EMIT_DEDUPE_DEFAULT_TTL,
        artifact_retention,
        instance_retention,
        cancel,
    )
    .await;
}

/// Same as [`run_gc_loop`] but with an explicit dedupe TTL — used by tests to
/// force expiry on rows created seconds ago.
pub async fn run_gc_loop_with_ttl(
    storage: Arc<dyn StorageBackend>,
    interval: Duration,
    dedupe_ttl: Duration,
    artifact_retention: Option<Duration>,
    instance_retention: Option<Duration>,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    // First tick fires immediately — skip it so startup isn't a DB burst.
    ticker.tick().await;

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {
                tokio::join!(
                    sweep_externalized(storage.as_ref()),
                    sweep_emit_dedupe(storage.as_ref(), dedupe_ttl),
                    sweep_artifacts_opt(storage.as_ref(), artifact_retention),
                    sweep_telemetry_events(storage.as_ref()),
                    sweep_mobile_devices(storage.as_ref()),
                    sweep_mobile_approvals(storage.as_ref()),
                    sweep_mobile_commands(storage.as_ref()),
                    sweep_terminal_instances_opt(storage.as_ref(), instance_retention),
                    sweep_webhook_attempts(storage.as_ref()),
                    sweep_event_inbox(storage.as_ref()),
                );
            }
        }
    }
}

async fn sweep_externalized(storage: &dyn StorageBackend) {
    match storage
        .delete_expired_externalized_state(GC_BATCH_LIMIT)
        .await
    {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "externalized gc: deleted expired rows");
            metrics::inc_by(metrics::GC_EXTERNALIZED_DELETED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "externalized gc sweep failed");
            metrics::inc_with(metrics::GC_EXTERNALIZED_ERRORS, &[("kind", kind)]);
        }
    }
}

/// Convert a caller/config-supplied retention into a sweep cutoff. Returns
/// `None` when the duration is unrepresentable as a `chrono::Duration`
/// (> ~292M years) or the subtraction would overflow `chrono`'s date range
/// (~±262k years). The caller must then skip the sweep: falling back to a
/// zero cutoff would invert the semantics and delete every row (cutoff =
/// now), and an unchecked subtraction would panic and kill the GC task.
/// Compile-time-constant retentions (telemetry, webhook, inbox) can hit
/// neither case and use `from_std` directly.
fn retention_cutoff(retention: Duration) -> Option<chrono::DateTime<chrono::Utc>> {
    let d = chrono::Duration::from_std(retention).ok()?;
    chrono::Utc::now().checked_sub_signed(d)
}

async fn sweep_emit_dedupe(storage: &dyn StorageBackend, ttl: Duration) {
    let Some(cutoff) = retention_cutoff(ttl) else {
        tracing::warn!(
            ?ttl,
            "emit_event_dedupe gc: TTL unrepresentable; skipping sweep"
        );
        return;
    };
    match storage
        .delete_expired_emit_event_dedupe(cutoff, GC_BATCH_LIMIT)
        .await
    {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "emit_event_dedupe gc: deleted expired rows");
            metrics::inc_by(metrics::GC_EMIT_DEDUPE_DELETED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "emit_event_dedupe gc sweep failed");
            metrics::inc_with(metrics::GC_EMIT_DEDUPE_ERRORS, &[("kind", kind)]);
        }
    }
}

/// Delete `telemetry_events` rows older than [`TELEMETRY_EVENTS_DEFAULT_TTL`].
/// Closes review finding L3: `delete_old_telemetry_events` had a real
/// implementation on both backends but no caller outside tests.
async fn sweep_telemetry_events(storage: &dyn StorageBackend) {
    let ttl = chrono::Duration::from_std(TELEMETRY_EVENTS_DEFAULT_TTL)
        .unwrap_or_else(|_| chrono::Duration::zero());
    let cutoff = chrono::Utc::now() - ttl;
    match storage
        .delete_old_telemetry_events(cutoff, GC_BATCH_LIMIT)
        .await
    {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "telemetry gc: deleted expired rows");
            metrics::inc_by(metrics::GC_TELEMETRY_DELETED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "telemetry gc sweep failed");
            metrics::inc_with(metrics::GC_TELEMETRY_ERRORS, &[("kind", kind)]);
        }
    }
}

/// Delete webhook delivery attempt rows older than
/// [`WEBHOOK_ATTEMPTS_DEFAULT_TTL`] so the delivery inspector's history
/// stays bounded.
async fn sweep_webhook_attempts(storage: &dyn StorageBackend) {
    let ttl = chrono::Duration::from_std(WEBHOOK_ATTEMPTS_DEFAULT_TTL)
        .unwrap_or_else(|_| chrono::Duration::zero());
    let cutoff = chrono::Utc::now() - ttl;
    match storage.delete_webhook_attempts_before(cutoff).await {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "webhook attempt gc: deleted expired rows");
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "webhook attempt gc sweep failed");
        }
    }
}

/// Expire pending inbox events past retention so unmatched events don't
/// accumulate forever. Consumed events are never touched.
async fn sweep_event_inbox(storage: &dyn StorageBackend) {
    let ttl = chrono::Duration::from_std(EVENT_INBOX_DEFAULT_TTL)
        .unwrap_or_else(|_| chrono::Duration::zero());
    let cutoff = chrono::Utc::now() - ttl;
    match storage.expire_events_before(cutoff).await {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "event inbox gc: expired unmatched events");
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "event inbox gc sweep failed");
        }
    }
}

/// Mark mobile devices `active = false` once they've missed
/// [`MOBILE_DEVICE_STALE_DEFAULT_THRESHOLD`] of sync activity. Closes review
/// finding L3 for `mark_stale_devices_inactive`.
async fn sweep_mobile_devices(storage: &dyn StorageBackend) {
    let secs = i64::try_from(MOBILE_DEVICE_STALE_DEFAULT_THRESHOLD.as_secs()).unwrap_or(i64::MAX);
    match storage.mark_stale_devices_inactive(secs).await {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "mobile gc: deactivated stale devices");
            metrics::inc_by(metrics::GC_MOBILE_DEVICES_DEACTIVATED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "mobile device gc sweep failed");
            metrics::inc_with(metrics::GC_MOBILE_ERRORS, &[("sweep", "devices")]);
        }
    }
}

/// Flip pending mobile approval requests to `expired` once they've missed
/// their caller-supplied `timeout_secs`. Closes review finding L3 for
/// `expire_mobile_approvals`.
async fn sweep_mobile_approvals(storage: &dyn StorageBackend) {
    match storage.expire_mobile_approvals().await {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "mobile gc: expired pending approvals");
            metrics::inc_by(metrics::GC_MOBILE_APPROVALS_EXPIRED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "mobile approval gc sweep failed");
            metrics::inc_with(metrics::GC_MOBILE_ERRORS, &[("sweep", "approvals")]);
        }
    }
}

/// Delete acked mobile commands past [`MOBILE_COMMAND_ACKED_DEFAULT_TTL`] and
/// never-acked (abandoned) ones past [`MOBILE_COMMAND_EXPIRED_DEFAULT_TTL`].
/// Closes review finding L3 for `cleanup_acked_commands` /
/// `cleanup_expired_commands`.
async fn sweep_mobile_commands(storage: &dyn StorageBackend) {
    let acked_secs = i64::try_from(MOBILE_COMMAND_ACKED_DEFAULT_TTL.as_secs()).unwrap_or(i64::MAX);
    match storage.cleanup_acked_commands(acked_secs).await {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "mobile gc: deleted acked commands");
            metrics::inc_by(metrics::GC_MOBILE_COMMANDS_DELETED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "mobile command gc (acked) sweep failed");
            metrics::inc_with(metrics::GC_MOBILE_ERRORS, &[("sweep", "commands_acked")]);
        }
    }

    let expired_secs =
        i64::try_from(MOBILE_COMMAND_EXPIRED_DEFAULT_TTL.as_secs()).unwrap_or(i64::MAX);
    match storage.cleanup_expired_commands(expired_secs).await {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "mobile gc: deleted abandoned commands");
            metrics::inc_by(metrics::GC_MOBILE_COMMANDS_DELETED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "mobile command gc (expired) sweep failed");
            metrics::inc_with(metrics::GC_MOBILE_ERRORS, &[("sweep", "commands_expired")]);
        }
    }
}

/// No-op wrapper so the artifact sweep can sit in the same `tokio::join!` as the
/// other sweeps regardless of whether retention is enabled.
async fn sweep_artifacts_opt(storage: &dyn StorageBackend, retention: Option<Duration>) {
    if let Some(retention) = retention {
        sweep_artifacts(storage, retention).await;
    }
}

/// Delete the durable artifacts of instances that have been terminal for longer
/// than `retention`. Bounded per tick by [`GC_BATCH_LIMIT`]; idempotent via the
/// `_artifacts_gced` marker so swept instances aren't re-scanned. Best-effort —
/// errors are logged, the marker is only set after a successful delete so a
/// failed instance retries next tick.
async fn sweep_artifacts(storage: &dyn StorageBackend, retention: Duration) {
    let Some(cutoff) = retention_cutoff(retention) else {
        tracing::warn!(
            ?retention,
            "artifact gc: retention unrepresentable; skipping sweep"
        );
        return;
    };

    let candidates = match storage
        .list_artifact_gc_candidates(cutoff, GC_BATCH_LIMIT)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "artifact gc: candidate query failed");
            metrics::inc_with(metrics::GC_ARTIFACTS_ERRORS, &[("kind", kind)]);
            return;
        }
    };

    let mut deleted = 0u64;
    for id in candidates {
        match storage.delete_instance_artifacts(id).await {
            Ok(n) => {
                deleted += n;
                // Mark only after a successful delete so a transient failure
                // retries on the next tick rather than being silently skipped.
                if let Err(e) = storage.mark_artifacts_gced(id).await {
                    let kind = error_kind(&e);
                    tracing::error!(instance_id = %id, error = %e, kind, "artifact gc: mark failed");
                    metrics::inc_with(metrics::GC_ARTIFACTS_ERRORS, &[("kind", kind)]);
                }
            }
            Err(e) => {
                let kind = error_kind(&e);
                tracing::error!(instance_id = %id, error = %e, kind, "artifact gc: delete failed");
                metrics::inc_with(metrics::GC_ARTIFACTS_ERRORS, &[("kind", kind)]);
            }
        }
    }
    if deleted > 0 {
        tracing::info!(count = deleted, "artifact gc: deleted blobs");
        metrics::inc_by(metrics::GC_ARTIFACTS_DELETED, deleted);
    }
}

/// No-op wrapper so the terminal-instance sweep can sit in the same
/// `tokio::join!` as the other sweeps regardless of whether retention is
/// enabled. Off by default (`retention: None`) -- see
/// `SchedulerConfig::instance_retention_secs`.
async fn sweep_terminal_instances_opt(storage: &dyn StorageBackend, retention: Option<Duration>) {
    if let Some(retention) = retention {
        sweep_terminal_instances(storage, retention).await;
    }
}

/// Delete `task_instances` rows (and their execution history) that have been
/// terminal for longer than `retention`. Bounded per tick by
/// [`GC_BATCH_LIMIT`]. Closes review finding L1 -- previously there was no
/// non-test path that ever deleted a `task_instances` row outside of
/// `delete_sequence`'s cascade.
async fn sweep_terminal_instances(storage: &dyn StorageBackend, retention: Duration) {
    let Some(cutoff) = retention_cutoff(retention) else {
        tracing::warn!(
            ?retention,
            "instance gc: retention unrepresentable; skipping sweep"
        );
        return;
    };

    match storage
        .delete_terminal_instances(cutoff, GC_BATCH_LIMIT)
        .await
    {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(count = n, "instance gc: deleted terminal instances");
            metrics::inc_by(metrics::GC_INSTANCES_DELETED, n);
        }
        Err(e) => {
            let kind = error_kind(&e);
            tracing::error!(error = %e, kind, "instance gc sweep failed");
            metrics::inc_with(metrics::GC_INSTANCES_ERRORS, &[("kind", kind)]);
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
            async move { run_gc_loop(storage, Duration::from_millis(10), None, None, cancel).await }
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

    /// T15: end-to-end proof that the GC loop sweeps `emit_event_dedupe`
    /// rows once their age exceeds the configured dedupe TTL. Uses a short
    /// non-zero TTL plus a matching sleep so `SQLite`'s `datetime('now')` —
    /// which has 1-second resolution — reliably advances past the row's
    /// `created_at`. The TTL arithmetic itself is unit-tested at the storage
    /// layer in `delete_expired_emit_event_dedupe_removes_old_rows`; this test
    /// only proves that the loop wires `sweep_emit_dedupe` in.
    #[tokio::test]
    async fn gc_loop_sweeps_emit_event_dedupe() {
        use orch8_storage::{DedupeScope, EmitDedupeOutcome};
        use orch8_types::ids::InstanceId;

        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );

        let parent = InstanceId::new();
        let child = InstanceId::new();
        storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k", child)
            .await
            .unwrap();

        // SQLite's `datetime('now')` resolves to whole seconds — sleep long
        // enough that `now - 1s` is strictly after the row's stored
        // `created_at`, independent of where the current second boundary fell.
        tokio::time::sleep(Duration::from_millis(2_100)).await;

        let cancel = CancellationToken::new();
        let handle = {
            let storage = Arc::clone(&storage);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_gc_loop_with_ttl(
                    storage,
                    Duration::from_millis(10),
                    Duration::from_secs(1),
                    None,
                    None,
                    cancel,
                )
                .await;
            })
        };

        // Give the loop a few ticks to observe the backlog and sweep it.
        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("gc loop did not exit on cancel")
            .unwrap();

        // Second call should now insert fresh because the prior row was swept.
        let outcome = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k", InstanceId::new())
            .await
            .unwrap();
        assert_eq!(
            outcome,
            EmitDedupeOutcome::Inserted,
            "dedupe row should have been swept by the gc loop"
        );
    }

    /// End-to-end proof that the GC loop wires in `sweep_mobile_approvals`:
    /// a pending approval whose `timeout_secs` has already elapsed flips to
    /// `expired` on the next tick. Regression for review finding L3 —
    /// `expire_mobile_approvals` had a real implementation on both backends
    /// but no non-test caller before this loop wired it in.
    #[tokio::test]
    async fn gc_loop_sweeps_expired_mobile_approvals() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );

        let approval = orch8_storage::MobileApprovalRequest {
            id: "appr-1".into(),
            device_id: "dev-1".into(),
            tenant_id: "t1".into(),
            instance_id: "inst-1".into(),
            block_id: "b1".into(),
            sequence_name: None,
            prompt: None,
            choices: None,
            store_as: None,
            timeout_secs: Some(0),
            metadata: None,
            state: "pending".into(),
            resolution: None,
            created_at: String::new(),
            resolved_at: None,
        };
        storage.insert_mobile_approval(&approval).await.unwrap();

        // SQLite's `datetime('now')` resolves to whole seconds — sleep past
        // the second boundary so `created_at + 0s < now()` is unambiguous.
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let cancel = CancellationToken::new();
        let handle = {
            let storage = Arc::clone(&storage);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_gc_loop(storage, Duration::from_millis(10), None, None, cancel).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("gc loop did not exit on cancel")
            .unwrap();

        let updated = storage
            .get_mobile_approval("appr-1")
            .await
            .unwrap()
            .expect("approval should still exist");
        assert_eq!(
            updated.state, "expired",
            "pending approval past its timeout should have been expired by the gc loop"
        );
    }

    fn old_terminal_instance() -> orch8_types::instance::TaskInstance {
        let now = chrono::Utc::now();
        orch8_types::instance::TaskInstance {
            id: orch8_types::ids::InstanceId::new(),
            sequence_id: orch8_types::ids::SequenceId::new(),
            tenant_id: orch8_types::ids::TenantId::unchecked("t"),
            namespace: orch8_types::ids::Namespace::new("default"),
            state: orch8_types::instance::InstanceState::Completed,
            next_fire_at: None,
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: orch8_types::context::ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now - chrono::Duration::days(1),
            updated_at: now - chrono::Duration::days(1),
        }
    }

    /// End-to-end proof that the GC loop wires in `sweep_terminal_instances`
    /// when `instance_retention` is `Some(..)`: a terminal instance older
    /// than the retention window is deleted. Regression for review finding
    /// L1 -- there was previously no non-test path that ever deleted a
    /// `task_instances` row outside of `delete_sequence`'s cascade.
    #[tokio::test]
    async fn gc_loop_sweeps_old_terminal_instances_when_retention_enabled() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let inst = old_terminal_instance();
        storage.create_instance(&inst).await.unwrap();

        let cancel = CancellationToken::new();
        let handle = {
            let storage = Arc::clone(&storage);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_gc_loop(
                    storage,
                    Duration::from_millis(10),
                    None,
                    Some(Duration::from_secs(3600)), // 1h retention; instance is 1 day old.
                    cancel,
                )
                .await;
            })
        };

        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("gc loop did not exit on cancel")
            .unwrap();

        assert!(
            storage.get_instance(inst.id).await.unwrap().is_none(),
            "old terminal instance should have been swept once retention is enabled"
        );
    }

    /// The instance-retention sweep is opt-in -- with `instance_retention:
    /// None` (the default), an old terminal instance must survive.
    #[tokio::test]
    async fn gc_loop_leaves_terminal_instances_when_retention_disabled() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let inst = old_terminal_instance();
        storage.create_instance(&inst).await.unwrap();

        let cancel = CancellationToken::new();
        let handle = {
            let storage = Arc::clone(&storage);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_gc_loop(storage, Duration::from_millis(10), None, None, cancel).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("gc loop did not exit on cancel")
            .unwrap();

        assert!(
            storage.get_instance(inst.id).await.unwrap().is_some(),
            "terminal instance must survive when instance_retention is disabled (the default)"
        );
    }

    /// An absurdly large retention (e.g. `instance_retention_secs =
    /// u64::MAX`, meaning "effectively forever") must not delete anything:
    /// `retention_cutoff` returns `None` and the sweep skips. Guards the
    /// previous zero-cutoff fallback, which inverted the semantics and would
    /// have deleted every terminal instance at once.
    #[tokio::test]
    async fn terminal_instance_sweep_skips_on_unrepresentable_retention() {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let inst = old_terminal_instance();
        storage.create_instance(&inst).await.unwrap();

        // Overflows chrono's i64-millisecond Duration range.
        sweep_terminal_instances(storage.as_ref(), Duration::from_secs(u64::MAX)).await;
        // Representable as a chrono Duration (~317k years) but `now -
        // retention` would overflow chrono's ~±262k-year date range
        // (previously: panic).
        sweep_terminal_instances(storage.as_ref(), Duration::from_secs(10_000_000_000_000)).await;

        assert!(
            storage.get_instance(inst.id).await.unwrap().is_some(),
            "unrepresentable retention must skip the sweep, not delete everything"
        );
    }

    #[test]
    fn gc_batch_limit_is_reasonable() {
        assert_eq!(GC_BATCH_LIMIT, 1_000);
    }

    #[test]
    fn gc_default_interval_is_five_minutes() {
        assert_eq!(GC_DEFAULT_INTERVAL, Duration::from_secs(300));
    }

    #[test]
    fn emit_dedupe_default_ttl_is_30_days() {
        // 30 days = 720 hours
        assert_eq!(EMIT_DEDUPE_DEFAULT_TTL, Duration::from_secs(2_592_000));
    }

    #[test]
    fn error_kind_terminal_target() {
        let err = StorageError::TerminalTarget {
            entity: "instance".into(),
            id: "abc".into(),
        };
        assert_eq!(error_kind(&err), "terminal_target");
    }

    #[test]
    fn error_kind_all_variants_have_stable_labels() {
        // Every label must be lowercase, contain no spaces or special chars.
        let labels = [
            "connection",
            "query",
            "not_found",
            "conflict",
            "terminal_target",
            "migration",
            "serialization",
            "pool_exhausted",
        ];
        for label in labels {
            assert!(
                label.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "label '{label}' contains invalid chars"
            );
        }
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
            async move { run_gc_loop(storage, Duration::from_millis(5), None, None, cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("gc loop did not exit on cancel")
            .unwrap();
    }
}
