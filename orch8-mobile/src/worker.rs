//! On-device worker loop: the phone as a leased runtime node.
//!
//! The worker polls the control plane as a `mobile` runtime
//! (`POST /workers/tasks/poll` with its `runtime_id` + capabilities), runs
//! each claimed task through the engine's handler registry (app-native
//! `StepHandler`s and any opted-in builtins), heartbeats at the lease
//! cadence, and settles the task with `complete` / `fail` — or gives it back
//! with `release` when the app is backgrounded before the task started.
//!
//! ## Crash safety
//!
//! Every claim is written to the local `mobile_worker_claims` table *before*
//! the handler runs, then marked `started`, then updated with the handler's
//! outcome before it is reported. If the OS kills the app at any point, the
//! next process drains that table:
//!
//! | persisted state            | action on restart                              |
//! |----------------------------|------------------------------------------------|
//! | claimed, not started       | `release {started:false}` → task back to pending |
//! | started, no outcome        | `release {started:true}` → effect marked unknown |
//! | outcome recorded           | re-deliver `complete` / `fail`                  |
//!
//! A server without the `release` endpoint (404/405) gets a retryable `fail`
//! instead, which requeues the task under its retry policy.
//!
//! ## Device-side timeouts
//!
//! When a task's `timeout_ms` (or the engine's `handler_timeout_ms`) elapses
//! on the device, the handler may still be running — an app-native call
//! cannot be interrupted — so the outcome is *ambiguous*, not a failure. The
//! worker answers `release {started: true}`: the server marks the effect
//! receipt `unknown` and applies the step's retry policy (a new attempt gets
//! a new `effect_id`), exactly like a lease expiry after start. Until the
//! timed-out invocation actually returns, the worker claims no new task for
//! that handler (see [`crate::stragglers`]), so a retry never overlaps the
//! attempt it replaces on the same device.
//!
//! ## Handler input
//!
//! App-native handlers receive the task `params` JSON. When `params` is an
//! object, the worker adds a reserved `__orch8` member:
//!
//! ```json
//! { "...": "params", "__orch8": {
//!     "effect_id": "…", "task_id": "…", "instance_id": "…", "block_id": "…",
//!     "attempt": 1, "runtime_id": "…", "continuity_epoch": 3,
//!     "resume_checkpoint": null } }
//! ```
//!
//! `effect_id` is the server's deterministic idempotency key for the step's
//! effect: pass it to downstream APIs (e.g. as an `Idempotency-Key` header)
//! so a re-run after a crash cannot duplicate the side effect. It is `null`
//! against servers that predate the distributed contract. Built-in handlers
//! never see `__orch8`.

use orch8_engine::remote_worker::lease::{ConfirmedLease, LeaseEnd};

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::StorageBackend;
use orch8_types::error::StepError;

use crate::PowerState;
use crate::error::MobileError;
use crate::node::{LeaseResponse, NodeClient, RemoteTask};
use crate::stragglers::{Stragglers, device_timeout_error, is_device_timeout};

/// Delivery attempts for complete/fail before leaving the outcome to the
/// orphan drain.
const DELIVERY_ATTEMPTS: u32 = 3;
/// How often the loop re-drains undelivered outcomes of earlier tasks.
const ORPHAN_DRAIN_INTERVAL: Duration = Duration::from_secs(60);
/// Claims older than this are forgotten: the server reaped them long ago.
const STALE_CLAIM_AGE: chrono::Duration = chrono::Duration::days(7);
/// Back-off after a failed poll.
const POLL_ERROR_BACKOFF: Duration = Duration::from_secs(15);

/// Options for `start_worker`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct WorkerOptions {
    /// Remote tasks executed concurrently on the device (default 1).
    #[uniffi(default = 1)]
    pub max_concurrent_tasks: u32,
    /// Poll cadence while idle, before power-state scaling (default 15 s).
    /// Push wake-ups and `on_push_received` poll immediately regardless.
    #[uniffi(default = 15000)]
    pub idle_poll_interval_ms: u64,
    /// Worker build/version reported to the server's version pins.
    #[uniffi(default = None)]
    pub version: Option<String>,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 1,
            idle_poll_interval_ms: 15_000,
            version: None,
        }
    }
}

/// Counters exposed through `worker_stats`.
#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct WorkerStats {
    pub running: bool,
    pub in_flight: u32,
    pub claimed: u64,
    pub completed: u64,
    pub failed: u64,
    pub released: u64,
    /// Tasks whose lease was lost (reclaimed by the server) mid-execution.
    pub lost: u64,
}

/// Result of a bounded background window (`run_worker_window`).
#[derive(Debug, Clone, uniffi::Record)]
pub struct WorkerWindowResult {
    pub claimed: u64,
    pub completed: u64,
    pub failed: u64,
    /// Tasks still executing when the budget ran out. They keep running
    /// while the process lives; if it is suspended, the lease lapses and the
    /// next launch releases them.
    pub still_running: u32,
    pub budget_exhausted: bool,
}

mod admission;
mod claims;
use admission::{Admission, Decision, Execution};
use claims::ClaimKey;
pub(crate) use claims::{ClaimStore, Outcome};

/// Settle every persisted claim not currently executing in this process.
/// Returns how many claims were resolved (released, failed, or delivered).
pub(crate) async fn drain_orphans(
    client: &NodeClient,
    store: &ClaimStore,
    stats: Option<&Counters>,
) -> u32 {
    let rows = match store.list().await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "failed to read worker claim journal");
            return 0;
        }
    };
    let mut resolved = 0;
    for row in rows {
        if !row.key.belongs_to(&client.worker_id(), client.api_base()) {
            continue;
        }
        let Some(_reservation) = store.reserve(&row.key) else {
            // Another worker generation or recovery pass still owns this claim.
            continue;
        };
        // The list can precede a completed live invocation. Re-read after
        // reserving so recovery cannot release an outcome that was just saved.
        let row = match store.get(&row.key).await {
            Ok(Some(row)) => row,
            Ok(None) => continue,
            Err(error) => {
                warn!(%error, "failed to reload worker recovery claim");
                continue;
            }
        };
        if chrono::Utc::now() - row.claimed_at > STALE_CLAIM_AGE {
            store.delete(&row.key).await;
            continue;
        }
        // Registration and restart can launch independent recovery passes.
        // Share the same cleanup I/O budget as surplus deliveries.
        let Ok(_slot) = store.admission.release_slots.acquire().await else {
            break;
        };
        let settled = if let Some(outcome) = &row.outcome {
            let result = deliver_once(client, row.key.task_id, row.key.claim_epoch, outcome).await;
            if let Some(stats) = stats
                && result == LeaseResponse::Accepted
            {
                stats.record_outcome(outcome);
            }
            result != LeaseResponse::Retry
        } else {
            let released = release(client, row.key.task_id, row.key.claim_epoch, row.started).await;
            if released && let Some(stats) = stats {
                stats.released.fetch_add(1, Ordering::Relaxed);
            }
            released
        };
        if settled {
            info!(task_id = %row.key.task_id, started = row.started, "settled orphaned worker claim after restart");
            store.delete(&row.key).await;
            resolved += 1;
        }
    }
    resolved
}

/// Give a claim back. `true` once the server no longer considers it ours.
async fn release(
    client: &NodeClient,
    task_id: uuid::Uuid,
    claim_epoch: u64,
    started: bool,
) -> bool {
    match client.release(task_id, claim_epoch, started).await {
        LeaseResponse::Accepted | LeaseResponse::LostOwnership | LeaseResponse::Rejected => true,
        LeaseResponse::Retry => false,
        LeaseResponse::Unsupported => {
            // Pre-contract server: a retryable failure requeues the task.
            let message = if started {
                "mobile node stopped before finishing; outcome unknown"
            } else {
                "released by mobile node before start"
            };
            client.fail(task_id, claim_epoch, message, true).await != LeaseResponse::Retry
        }
    }
}

async fn deliver_once(
    client: &NodeClient,
    task_id: uuid::Uuid,
    epoch: u64,
    outcome: &Outcome,
) -> LeaseResponse {
    match outcome {
        Outcome::Complete { output } => client.complete(task_id, epoch, output).await,
        Outcome::Fail { message, retryable } => {
            client.fail(task_id, epoch, message, *retryable).await
        }
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct Counters {
    claimed: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    released: AtomicU64,
    lost: AtomicU64,
}

impl Counters {
    fn record_outcome(&self, outcome: &Outcome) {
        match outcome {
            Outcome::Complete { .. } => self.completed.fetch_add(1, Ordering::Relaxed),
            Outcome::Fail { .. } => self.failed.fetch_add(1, Ordering::Relaxed),
        };
    }
}

/// Engine-owned signals the worker consults before claiming work.
#[derive(Clone)]
pub(crate) struct HostSignals {
    /// False while the app is backgrounded (`pause`), true after `resume`.
    pub foreground: Arc<AtomicBool>,
    /// Shared with the tick controller (`report_power_state`).
    pub power_state: Arc<AtomicU8>,
}

pub(crate) struct WorkerDeps {
    pub client: Arc<NodeClient>,
    pub store: ClaimStore,
    pub registry: Arc<HandlerRegistry>,
    pub foreign_handlers: HashSet<String>,
    pub storage: Arc<dyn StorageBackend>,
    pub signals: HostSignals,
    /// Handler invocations still running after a device-side timeout.
    pub stragglers: Arc<Stragglers>,
}

pub(crate) struct Worker {
    deps: WorkerDeps,
    options: WorkerOptions,
    handlers: Vec<String>,
    wake: Arc<Notify>,
    admission: Arc<Admission>,
    counters: Counters,
    /// Independently owned OS background windows, released on cancellation.
    active_windows: AtomicU64,
    /// Bumped after every poll round that claimed nothing.
    idle_rounds: AtomicU64,
    cancel: CancellationToken,
}

/// Each window owns one permission; cancellation and unwinding revoke it too.
struct ClaimWindow<'a> {
    active: &'a AtomicU64,
    wake: &'a Notify,
}

impl Drop for ClaimWindow<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.wake.notify_one();
    }
}

impl Worker {
    pub fn new(deps: WorkerDeps, options: WorkerOptions) -> Result<Arc<Self>, MobileError> {
        let handlers = deps.client.handlers();
        if handlers.is_empty() {
            return Err(MobileError::InvalidInput {
                message: "the node advertises no handlers; register handlers before register_node"
                    .into(),
            });
        }
        let missing: Vec<&String> = handlers
            .iter()
            .filter(|h| !deps.registry.contains(h))
            .collect();
        if !missing.is_empty() {
            return Err(MobileError::InvalidInput {
                message: format!("advertised handlers are not registered: {missing:?}"),
            });
        }
        let admission = Arc::clone(&deps.store.admission);
        admission.configure(options.max_concurrent_tasks);
        Ok(Arc::new(Self {
            deps,
            options,
            handlers,
            wake: Arc::clone(&admission.wake),
            admission,
            counters: Counters::default(),
            active_windows: AtomicU64::new(0),
            idle_rounds: AtomicU64::new(0),
            cancel: CancellationToken::new(),
        }))
    }

    pub fn spawn(self: &Arc<Self>, handle: &tokio::runtime::Handle) {
        let this = Arc::clone(self);
        handle.spawn(async move { this.run().await });
    }

    pub fn stop(&self) {
        self.cancel.cancel();
        self.wake.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Push wake-up / host activity: poll now.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    pub fn stats(&self) -> WorkerStats {
        WorkerStats {
            running: !self.cancel.is_cancelled(),
            #[allow(clippy::cast_possible_truncation)]
            in_flight: self.in_flight_len() as u32,
            claimed: self.counters.claimed.load(Ordering::Relaxed),
            completed: self.counters.completed.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
            released: self.counters.released.load(Ordering::Relaxed),
            lost: self.counters.lost.load(Ordering::Relaxed),
        }
    }

    fn in_flight_len(&self) -> usize {
        self.admission.in_flight()
    }

    fn power(&self) -> PowerState {
        PowerState::from_atomic(self.deps.signals.power_state.load(Ordering::Acquire))
    }

    /// May the worker claim new tasks right now?
    fn may_claim(&self) -> bool {
        let allowed_window = self.deps.signals.foreground.load(Ordering::Acquire)
            || self.active_windows.load(Ordering::Acquire) > 0;
        allowed_window && !self.cancel.is_cancelled() && self.power() != PowerState::CriticalBattery
    }

    fn idle_interval(&self) -> Duration {
        Duration::from_millis(self.options.idle_poll_interval_ms.max(250))
            .saturating_mul(self.power().tick_multiplier())
    }

    async fn run(self: Arc<Self>) {
        info!(handlers = ?self.handlers, "mobile worker loop started");
        drain_orphans(&self.deps.client, &self.deps.store, Some(&self.counters)).await;
        let mut next_drain = tokio::time::Instant::now() + ORPHAN_DRAIN_INTERVAL;
        let mut cursor = 0usize;

        loop {
            if self.cancel.is_cancelled() {
                break;
            }
            let mut wait = self.idle_interval();
            if self.may_claim() && self.admission.available() > 0 {
                match self.poll_round(&mut cursor).await {
                    Ok((claimed, hint)) => {
                        if claimed > 0 {
                            wait = Duration::ZERO;
                        } else {
                            self.idle_rounds.fetch_add(1, Ordering::AcqRel);
                            if let Some(hint) = hint {
                                wait = wait.max(hint);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "mobile worker poll failed");
                        self.idle_rounds.fetch_add(1, Ordering::AcqRel);
                        wait = wait.max(POLL_ERROR_BACKOFF);
                    }
                }
            }

            if tokio::time::Instant::now() >= next_drain {
                drain_orphans(&self.deps.client, &self.deps.store, Some(&self.counters)).await;
                next_drain = tokio::time::Instant::now() + ORPHAN_DRAIN_INTERVAL;
            }

            if wait.is_zero() && self.admission.available() > 0 {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                () = self.cancel.cancelled() => break,
                () = self.wake.notified() => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
        info!("mobile worker loop stopped");
    }

    /// Poll each served handler once (round-robin start), spawning claimed
    /// tasks. Returns how many were claimed and the server's back-off hint.
    async fn poll_round(
        self: &Arc<Self>,
        cursor: &mut usize,
    ) -> Result<(usize, Option<Duration>), MobileError> {
        // A stopped generation may still be awaiting a poll response. Keep
        // at most one such response batch outstanding across engine workers.
        let _owner = tokio::select! {
            () = self.cancel.cancelled() => return Ok((0, None)),
            owner = self.admission.poll_owner.lock() => owner,
        };
        let mut claimed = 0usize;
        let mut hint: Option<Duration> = None;
        let count = self.handlers.len();
        for offset in 0..count {
            let free = self.admission.available();
            if free == 0 || !self.may_claim() || self.cancel.is_cancelled() {
                break;
            }
            let handler = &self.handlers[(*cursor + offset) % count];
            if self.deps.stragglers.blocks(handler) {
                // A timed-out invocation of this handler is still running:
                // never start a retry of it alongside.
                continue;
            }
            #[allow(clippy::cast_possible_truncation)]
            let response = self
                .deps
                .client
                .poll(handler, free as u32, self.options.version.as_deref())
                .await?;
            if let Some(ms) = response.poll_after_ms.filter(|ms| *ms > 0) {
                let h = Duration::from_millis(ms.min(60_000));
                hint = Some(hint.map_or(h, |cur| cur.min(h)));
            }
            for task in response.tasks {
                claimed += 1;
                self.counters.claimed.fetch_add(1, Ordering::Relaxed);
                let heartbeat = heartbeat_interval(
                    response.heartbeat_interval_secs,
                    response.lease_secs,
                    task.lease_secs,
                );
                self.spawn_task(task, heartbeat);
            }
        }
        *cursor = (*cursor + 1) % count.max(1);
        Ok((claimed, hint))
    }

    fn spawn_task(self: &Arc<Self>, task: RemoteTask, heartbeat: Duration) {
        if let Some(future) = self.task_future(task, heartbeat) {
            tokio::spawn(future);
        }
    }

    #[cfg(test)]
    async fn execute(self: &Arc<Self>, task: RemoteTask, heartbeat: Duration) {
        if let Some(future) = self.task_future(task, heartbeat) {
            future.await;
        }
    }

    /// Reserve synchronously, before spawning or polling any future. The
    /// reservation also owns capacity when cancellation precedes first poll.
    fn task_future(
        self: &Arc<Self>,
        task: RemoteTask,
        heartbeat: Duration,
    ) -> Option<impl std::future::Future<Output = ()> + Send + 'static> {
        let key = ClaimKey::for_task(
            &task,
            &self.deps.client.worker_id(),
            self.deps.client.api_base(),
        );
        let decision = self.admission.admit(&key);
        match &decision {
            Decision::Ignore => return None,
            Decision::Release {
                superseded: Some(cancel),
                ..
            } => cancel.cancel(),
            _ => {}
        }
        let this = Arc::clone(self);
        Some(async move {
            match decision {
                Decision::Execute(claim) => this.execute_reserved(task, heartbeat, claim).await,
                Decision::Release { guard, .. } => {
                    // Both queued and active releases retain their generation
                    // fence, and all worker generations share the I/O bound.
                    let Ok(_slot) = this.admission.release_slots.acquire().await else {
                        return;
                    };
                    this.release_unstarted(&task, &key).await;
                    drop(guard);
                }
                Decision::Ignore => unreachable!("ignored before creating the future"),
            }
        })
    }

    async fn release_unstarted(&self, task: &RemoteTask, key: &ClaimKey) {
        let client = &self.deps.client;
        let store = &self.deps.store;
        let Some(_reservation) = store.reserve(key) else {
            return;
        };
        match store
            .insert(task, &client.worker_id(), client.api_base())
            .await
        {
            Ok(true) => {}
            // Prior start/outcome evidence must be reconciled by recovery,
            // never replaced with an unstarted release due to capacity.
            Ok(false) => return,
            Err(error) => {
                warn!(%error, task_id = %task.id, "cannot journal unstarted release; leaving claim for expiry");
                return;
            }
        }
        if release(client, task.id, task.claim_epoch, false).await {
            self.counters.released.fetch_add(1, Ordering::Relaxed);
            store.delete(key).await;
        }
    }

    async fn execute_reserved(
        self: &Arc<Self>,
        task: RemoteTask,
        heartbeat: Duration,
        claim: Execution,
    ) {
        let client = &self.deps.client;
        let store = &self.deps.store;
        let task_id = task.id;
        let key = ClaimKey::for_task(&task, &client.worker_id(), client.api_base());
        let Some(_reservation) = store.reserve(&key) else {
            // A duplicate delivery cannot revoke or re-enter a live callback.
            return;
        };
        match store
            .insert(&task, &client.worker_id(), client.api_base())
            .await
        {
            Ok(true) => {}
            Ok(false) => return, // Existing evidence belongs to recovery, not a second invocation.
            Err(error) => {
                // A failed INSERT cannot distinguish a fresh claim from
                // existing started/outcome evidence. Do not assert that an
                // earlier callback never ran; recovery or lease expiry owns it.
                warn!(%error, %task_id, "cannot journal claim; leaving it for recovery or expiry");
                return;
            }
        }
        let result = self
            .execute_journaled(&task, &key, heartbeat, claim.cancellation())
            .await;
        if result {
            store.delete(&key).await;
        }
    }

    /// Runs a journaled claim to settlement. Returns `true` when the journal
    /// entry can be deleted (settled or lost); `false` leaves it for the
    /// orphan drain.
    async fn execute_journaled(
        self: &Arc<Self>,
        task: &RemoteTask,
        key: &ClaimKey,
        heartbeat: Duration,
        superseded: &CancellationToken,
    ) -> bool {
        let client = &self.deps.client;
        let store = &self.deps.store;

        if !self.may_claim() {
            // Backgrounded (or battery critical) between claim and start.
            let released = release(client, task.id, task.claim_epoch, false).await;
            if released {
                self.counters.released.fetch_add(1, Ordering::Relaxed);
            }
            return released;
        }
        let acquired = tokio::select! {
            biased;
            () = superseded.cancelled() => Some(Err(LeaseEnd::LostOwnership)),
            () = self.cancel.cancelled() => None,
            lease = ConfirmedLease::acquire(task.lease_secs, heartbeat, || client.heartbeat(task.id, task.claim_epoch)) => Some(lease),
        };
        let lease = match acquired {
            Some(Ok(lease)) => lease,
            Some(Err(LeaseEnd::LostOwnership)) => {
                self.counters.lost.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            _ => {
                let released = release(client, task.id, task.claim_epoch, false).await;
                if released {
                    self.counters.released.fetch_add(1, Ordering::Relaxed);
                }
                return released;
            }
        };
        let mut started = false;
        let mut outcome = None;
        let result = tokio::select! {
            biased;
            () = superseded.cancelled() => Err(LeaseEnd::LostOwnership),
            result = lease.run(
                heartbeat,
                self.execute_confirmed(task, key, &mut started, &mut outcome),
                || client.heartbeat(task.id, task.claim_epoch),
            ) => result,
        };
        // The owned invocation is dropped before any recovery I/O. A running
        // native callback is now quarantined until its physical return.
        match result {
            Ok(settled) => settled,
            Err(LeaseEnd::LostOwnership) => {
                self.counters.lost.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                if let Some(outcome) = outcome {
                    // Do not replace a known result with an ambiguous release.
                    // Persist it even if expiry interrupted the earlier write;
                    // the orphan drain can reconcile it with server authority.
                    if let Err(error) = store.record_outcome(key, &outcome).await {
                        warn!(%error, task_id = %task.id, "failed to retain outcome at lease expiry");
                    }
                    return false;
                }
                let released = release(client, task.id, task.claim_epoch, started).await;
                if released {
                    self.counters.released.fetch_add(1, Ordering::Relaxed);
                }
                released
            }
        }
    }

    async fn execute_confirmed(
        self: &Arc<Self>,
        task: &RemoteTask,
        key: &ClaimKey,
        started: &mut bool,
        saved_outcome: &mut Option<Outcome>,
    ) -> bool {
        let client = &self.deps.client;
        let store = &self.deps.store;
        // Confirmation may have waited through a network outage while the OS
        // window ended, power changed, or this worker was stopped.
        if !self.may_claim() {
            let released = release(client, task.id, task.claim_epoch, false).await;
            if released {
                self.counters.released.fetch_add(1, Ordering::Relaxed);
            }
            return released;
        }
        match store.mark_started(key).await {
            Ok(true) => {}
            Ok(false) => {
                self.counters.lost.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            Err(error) => {
                warn!(%error, task_id = %task.id, "cannot journal task start; releasing");
                let released = release(client, task.id, task.claim_epoch, false).await;
                if released {
                    self.counters.released.fetch_add(1, Ordering::Relaxed);
                }
                return released;
            }
        }

        *started = true;
        let result = self.invoke(task).await;

        if let Err(error) = &result
            && is_device_timeout(error)
        {
            // Ambiguous: the handler may still complete its side effect.
            // Give the lease back as "started" so the server marks the
            // receipt unknown and follows the retry policy. If the release
            // cannot be delivered now, the journal (started, no outcome)
            // makes the orphan drain send the same release later.
            warn!(task_id = %task.id, "device-side timeout; releasing the task as started (effect unknown)");
            let released = release(client, task.id, task.claim_epoch, true).await;
            if released {
                self.counters.released.fetch_add(1, Ordering::Relaxed);
            }
            return released;
        }

        let outcome = match result {
            Err(StepError::ClaimLost) => {
                self.counters.lost.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            Ok(output) => Outcome::Complete { output },
            Err(StepError::Retryable { message, .. }) => Outcome::Fail {
                message,
                retryable: true,
            },
            Err(StepError::Permanent { message, .. }) => Outcome::Fail {
                message,
                retryable: false,
            },
        };
        let outcome = saved_outcome.insert(outcome);
        self.settle_outcome(task, key, outcome).await
    }

    async fn settle_outcome(&self, task: &RemoteTask, key: &ClaimKey, outcome: &Outcome) -> bool {
        let client = &self.deps.client;
        let store = &self.deps.store;
        match store.record_outcome(key, outcome).await {
            Ok(true) => {}
            Ok(false) => {
                self.counters.lost.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            Err(error) => warn!(%error, task_id = %task.id, "failed to journal task outcome"),
        }

        let mut backoff = Duration::from_millis(500);
        for attempt in 1..=DELIVERY_ATTEMPTS {
            match deliver_once(client, task.id, task.claim_epoch, outcome).await {
                LeaseResponse::Accepted => {
                    self.counters.record_outcome(outcome);
                    return true;
                }
                LeaseResponse::LostOwnership => {
                    self.counters.lost.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                LeaseResponse::Rejected | LeaseResponse::Unsupported => {
                    warn!(task_id = %task.id, "server rejected task settlement");
                    return true;
                }
                LeaseResponse::Retry if attempt < DELIVERY_ATTEMPTS => {
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = self.cancel.cancelled() => return false,
                    }
                    backoff = backoff.saturating_mul(2);
                }
                LeaseResponse::Retry => {}
            }
        }
        debug!(task_id = %task.id, "settlement deferred to the orphan drain");
        false
    }

    async fn invoke(&self, task: &RemoteTask) -> Result<Value, StepError> {
        let Some(handler) = self.deps.registry.get(&task.handler_name) else {
            return Err(StepError::Permanent {
                message: format!(
                    "handler '{}' is not available on this device",
                    task.handler_name
                ),
                details: None,
            });
        };
        let mut params = task.params.clone();
        if self.deps.foreign_handlers.contains(&task.handler_name) {
            inject_task_metadata(&mut params, task, &self.deps.client.worker_id());
        }
        let ctx = orch8_engine::remote_worker::step_context(
            task,
            crate::mobile_tenant_id(),
            params,
            Arc::clone(&self.deps.storage),
        );
        let future = handler(ctx);
        let Some(ms) = task
            .timeout_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .filter(|ms| *ms > 0)
        else {
            return future.await;
        };
        // Keep ownership in this future. Losing the lease or timing out drops
        // the foreign wrapper immediately, which aborts a queued native call
        // or quarantines an already-running call until its physical completion.
        tokio::time::timeout(Duration::from_millis(ms), future)
            .await
            .unwrap_or_else(|_| {
                Err(device_timeout_error(format!(
                    "task timed out after {ms}ms on device"
                )))
            })
    }

    /// Bounded background window: claim and run tasks until the queue is
    /// idle and nothing is in flight, or the budget elapses.
    pub async fn run_window(&self, budget: Duration) -> WorkerWindowResult {
        let before = self.stats();
        let start_idle = self.idle_rounds.load(Ordering::Acquire);
        self.active_windows.fetch_add(1, Ordering::AcqRel);
        let _window = ClaimWindow {
            active: &self.active_windows,
            wake: &self.wake,
        };
        self.wake.notify_one();
        let start = tokio::time::Instant::now();
        let mut exhausted = true;
        while start.elapsed() < budget && !self.cancel.is_cancelled() {
            let idle_seen = self.idle_rounds.load(Ordering::Acquire) > start_idle;
            if idle_seen && self.in_flight_len() == 0 {
                exhausted = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let after = self.stats();
        WorkerWindowResult {
            claimed: after.claimed - before.claimed,
            completed: after.completed - before.completed,
            failed: after.failed - before.failed,
            still_running: after.in_flight,
            budget_exhausted: exhausted,
        }
    }
}

pub(crate) use orch8_engine::remote_worker::{heartbeat_interval, inject_task_metadata};

#[cfg(test)]
#[path = "worker_tests.rs"]
mod worker_tests;
