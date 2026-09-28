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

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::error::StepError;
use orch8_types::ids::{BlockId, InstanceId};

use crate::PowerState;
use crate::error::MobileError;
use crate::node::{LeaseResponse, NodeClient, RemoteTask};
use crate::stragglers::{Stragglers, device_timeout_error, is_device_timeout};

/// Default lease when neither the task nor the poll response carries one.
const DEFAULT_LEASE_SECS: u64 = 120;
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

// ---------------------------------------------------------------------------
// Durable claim journal
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Outcome {
    Complete { output: Value },
    Fail { message: String, retryable: bool },
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimRow {
    pub task_id: uuid::Uuid,
    pub claim_epoch: u64,
    pub api_base: String,
    pub started: bool,
    pub outcome: Option<Outcome>,
    pub claimed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone)]
pub(crate) struct ClaimStore {
    pool: SqlitePool,
}

impl ClaimStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn init_tables(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mobile_worker_claims (
                task_id      TEXT PRIMARY KEY,
                claim_epoch  INTEGER NOT NULL,
                worker_id    TEXT NOT NULL,
                handler_name TEXT NOT NULL,
                effect_id    TEXT,
                api_base     TEXT NOT NULL,
                started      INTEGER NOT NULL DEFAULT 0,
                outcome      TEXT,
                claimed_at   TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn insert(
        &self,
        task: &RemoteTask,
        worker_id: &str,
        api_base: &str,
    ) -> Result<(), sqlx::Error> {
        #[allow(clippy::cast_possible_wrap)]
        let epoch = task.claim_epoch as i64;
        sqlx::query(
            "INSERT OR REPLACE INTO mobile_worker_claims
                (task_id, claim_epoch, worker_id, handler_name, effect_id, api_base, started, outcome, claimed_at)
             VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?)",
        )
        .bind(task.id.to_string())
        .bind(epoch)
        .bind(worker_id)
        .bind(&task.handler_name)
        .bind(task.effect_id.as_deref())
        .bind(api_base)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn mark_started(&self, task_id: uuid::Uuid) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE mobile_worker_claims SET started = 1 WHERE task_id = ?")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn record_outcome(
        &self,
        task_id: uuid::Uuid,
        outcome: &Outcome,
    ) -> Result<(), sqlx::Error> {
        let json = serde_json::to_string(outcome).unwrap_or_else(|_| "null".into());
        sqlx::query("UPDATE mobile_worker_claims SET outcome = ? WHERE task_id = ?")
            .bind(json)
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete(&self, task_id: uuid::Uuid) {
        if let Err(e) = sqlx::query("DELETE FROM mobile_worker_claims WHERE task_id = ?")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
        {
            warn!(error = %e, %task_id, "failed to delete settled worker claim");
        }
    }

    pub async fn list(&self) -> Result<Vec<ClaimRow>, sqlx::Error> {
        let rows: Vec<(String, i64, String, i64, Option<String>, String)> = sqlx::query_as(
            "SELECT task_id, claim_epoch, api_base, started, outcome, claimed_at
             FROM mobile_worker_claims ORDER BY claimed_at",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(id, epoch, api_base, started, outcome, claimed_at)| {
                Some(ClaimRow {
                    task_id: uuid::Uuid::parse_str(&id).ok()?,
                    #[allow(clippy::cast_sign_loss)]
                    claim_epoch: epoch as u64,
                    api_base,
                    started: started != 0,
                    outcome: outcome.and_then(|o| serde_json::from_str(&o).ok()),
                    claimed_at: chrono::DateTime::parse_from_rfc3339(&claimed_at)
                        .map_or_else(|_| chrono::Utc::now(), |t| t.with_timezone(&chrono::Utc)),
                })
            })
            .collect())
    }

    pub async fn count(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COUNT(*) FROM mobile_worker_claims")
            .fetch_one(&self.pool)
            .await
    }
}

/// Settle every persisted claim not currently executing in this process.
/// Returns how many claims were resolved (released, failed, or delivered).
pub(crate) async fn drain_orphans(
    client: &NodeClient,
    store: &ClaimStore,
    in_flight: &StdMutex<HashSet<uuid::Uuid>>,
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
        let busy = in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&row.task_id);
        if busy {
            continue;
        }
        if chrono::Utc::now() - row.claimed_at > STALE_CLAIM_AGE {
            store.delete(row.task_id).await;
            continue;
        }
        if row.api_base.trim_end_matches('/') != client.api_base() {
            // Claimed against another control plane; only a client for that
            // base can settle it (it ages out above otherwise).
            continue;
        }
        let settled = if let Some(outcome) = &row.outcome {
            let result = deliver_once(client, row.task_id, row.claim_epoch, outcome).await;
            if let Some(stats) = stats
                && result == LeaseResponse::Accepted
            {
                stats.record_outcome(outcome);
            }
            result != LeaseResponse::Retry
        } else {
            let released = release(client, row.task_id, row.claim_epoch, row.started).await;
            if released && let Some(stats) = stats {
                stats.released.fetch_add(1, Ordering::Relaxed);
            }
            released
        };
        if settled {
            info!(task_id = %row.task_id, started = row.started, "settled orphaned worker claim after restart");
            store.delete(row.task_id).await;
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
    wake: Notify,
    slots: Arc<Semaphore>,
    in_flight: StdMutex<HashSet<uuid::Uuid>>,
    counters: Counters,
    /// Set while a bounded background window may claim work even though the
    /// app is not in the foreground.
    window_active: AtomicBool,
    /// Bumped after every poll round that claimed nothing.
    idle_rounds: AtomicU64,
    cancel: CancellationToken,
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
        let slots = options.max_concurrent_tasks.clamp(1, 64) as usize;
        Ok(Arc::new(Self {
            deps,
            options,
            handlers,
            wake: Notify::new(),
            slots: Arc::new(Semaphore::new(slots)),
            in_flight: StdMutex::new(HashSet::new()),
            counters: Counters::default(),
            window_active: AtomicBool::new(false),
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
        self.in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    fn power(&self) -> PowerState {
        PowerState::from_atomic(self.deps.signals.power_state.load(Ordering::Acquire))
    }

    /// May the worker claim new tasks right now?
    fn may_claim(&self) -> bool {
        let allowed_window = self.deps.signals.foreground.load(Ordering::Acquire)
            || self.window_active.load(Ordering::Acquire);
        allowed_window && self.power() != PowerState::CriticalBattery
    }

    fn idle_interval(&self) -> Duration {
        Duration::from_millis(self.options.idle_poll_interval_ms.max(250))
            .saturating_mul(self.power().tick_multiplier())
    }

    async fn run(self: Arc<Self>) {
        info!(handlers = ?self.handlers, "mobile worker loop started");
        drain_orphans(
            &self.deps.client,
            &self.deps.store,
            &self.in_flight,
            Some(&self.counters),
        )
        .await;
        let mut next_drain = tokio::time::Instant::now() + ORPHAN_DRAIN_INTERVAL;
        let mut cursor = 0usize;

        loop {
            if self.cancel.is_cancelled() {
                break;
            }
            let mut wait = self.idle_interval();
            if self.may_claim() && self.slots.available_permits() > 0 {
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
                drain_orphans(
                    &self.deps.client,
                    &self.deps.store,
                    &self.in_flight,
                    Some(&self.counters),
                )
                .await;
                next_drain = tokio::time::Instant::now() + ORPHAN_DRAIN_INTERVAL;
            }

            if wait.is_zero() && self.slots.available_permits() > 0 {
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
        let mut claimed = 0usize;
        let mut hint: Option<Duration> = None;
        let count = self.handlers.len();
        for offset in 0..count {
            let free = self.slots.available_permits();
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
        let permit = Arc::clone(&self.slots).try_acquire_owned();
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let Ok(permit) = permit else {
                // More tasks than free slots (server ignored `limit`): hand
                // the surplus back untouched.
                if release(&this.deps.client, task.id, task.claim_epoch, false).await {
                    this.counters.released.fetch_add(1, Ordering::Relaxed);
                }
                return;
            };
            this.execute(task, heartbeat).await;
            drop(permit);
            this.wake.notify_one();
        });
    }

    async fn execute(self: &Arc<Self>, task: RemoteTask, heartbeat: Duration) {
        let client = &self.deps.client;
        let store = &self.deps.store;
        let task_id = task.id;

        if let Err(e) = store
            .insert(&task, &client.worker_id(), client.api_base())
            .await
        {
            // Without a journal entry a crash could strand the lease; do not
            // start work we cannot recover.
            warn!(error = %e, %task_id, "cannot journal claim; releasing task");
            if release(client, task_id, task.claim_epoch, false).await {
                self.counters.released.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        self.in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(task_id);

        let result = self.execute_journaled(&task, heartbeat).await;

        self.in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&task_id);
        if result {
            store.delete(task_id).await;
        }
    }

    /// Runs a journaled claim to settlement. Returns `true` when the journal
    /// entry can be deleted (settled or lost); `false` leaves it for the
    /// orphan drain.
    async fn execute_journaled(self: &Arc<Self>, task: &RemoteTask, heartbeat: Duration) -> bool {
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
        if let Err(e) = store.mark_started(task.id).await {
            warn!(error = %e, task_id = %task.id, "cannot journal task start; releasing");
            let released = release(client, task.id, task.claim_epoch, false).await;
            if released {
                self.counters.released.fetch_add(1, Ordering::Relaxed);
            }
            return released;
        }

        let invocation = self.invoke(task);
        tokio::pin!(invocation);
        let mut ticker =
            tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
        let result = loop {
            tokio::select! {
                result = &mut invocation => break Some(result),
                _ = ticker.tick() => {
                    if client.heartbeat(task.id, task.claim_epoch).await == LeaseResponse::LostOwnership {
                        break None;
                    }
                }
            }
        };
        let Some(result) = result else {
            warn!(task_id = %task.id, "lease lost while executing; abandoning task");
            self.counters.lost.fetch_add(1, Ordering::Relaxed);
            return true;
        };

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
        if let Err(e) = store.record_outcome(task.id, &outcome).await {
            warn!(error = %e, task_id = %task.id, "failed to journal task outcome");
        }

        let mut backoff = Duration::from_millis(500);
        for attempt in 1..=DELIVERY_ATTEMPTS {
            match deliver_once(client, task.id, task.claim_epoch, &outcome).await {
                LeaseResponse::Accepted => {
                    self.counters.record_outcome(&outcome);
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
        let context: ExecutionContext =
            serde_json::from_value(task.context.clone()).unwrap_or_default();
        let ctx = StepContext {
            instance_id: InstanceId::from_uuid(task.instance_id),
            tenant_id: crate::mobile_tenant_id(),
            block_id: BlockId::new(task.block_id.clone()),
            params,
            context: Arc::new(context),
            attempt: task.attempt.max(1),
            storage: Arc::clone(&self.deps.storage),
            wait_for_input: None,
        };
        let future = handler(ctx);
        let Some(ms) = task
            .timeout_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .filter(|ms| *ms > 0)
        else {
            return future.await;
        };
        // Run the invocation as its own task so a timeout stops *waiting*
        // without pretending the handler stopped: it keeps running and is
        // tracked as a straggler until it returns.
        let mut running = tokio::spawn(future);
        match tokio::time::timeout(Duration::from_millis(ms), &mut running).await {
            Ok(Ok(result)) => result,
            Ok(Err(join_error)) => Err(StepError::Permanent {
                message: format!("handler task failed: {join_error}"),
                details: None,
            }),
            Err(_elapsed) => {
                let stragglers = Arc::clone(&self.deps.stragglers);
                let handler_name = task.handler_name.clone();
                stragglers.begin(&handler_name);
                tokio::spawn(async move {
                    let _ = running.await;
                    stragglers.end(&handler_name);
                });
                Err(device_timeout_error(format!(
                    "task timed out after {ms}ms on device"
                )))
            }
        }
    }

    /// Bounded background window: claim and run tasks until the queue is
    /// idle and nothing is in flight, or the budget elapses.
    pub async fn run_window(&self, budget: Duration) -> WorkerWindowResult {
        let before = self.stats();
        let start_idle = self.idle_rounds.load(Ordering::Acquire);
        self.window_active.store(true, Ordering::Release);
        self.wake.notify_one();
        let deadline = tokio::time::Instant::now() + budget;
        let mut exhausted = true;
        while tokio::time::Instant::now() < deadline && !self.cancel.is_cancelled() {
            let idle_seen = self.idle_rounds.load(Ordering::Acquire) > start_idle;
            if idle_seen && self.in_flight_len() == 0 {
                exhausted = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.window_active.store(false, Ordering::Release);
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

/// Add the reserved `__orch8` member (see module docs) to object params.
pub(crate) fn inject_task_metadata(params: &mut Value, task: &RemoteTask, runtime_id: &str) {
    if let Value::Object(map) = params {
        map.insert(
            "__orch8".into(),
            serde_json::json!({
                "effect_id": task.effect_id,
                "task_id": task.id,
                "instance_id": task.instance_id,
                "block_id": task.block_id,
                "attempt": task.attempt,
                "runtime_id": runtime_id,
                "continuity_epoch": task.continuity_epoch,
                "resume_checkpoint": task.resume_checkpoint,
            }),
        );
    }
}

/// Heartbeat cadence: the server's explicit interval, else a third of the
/// tightest lease on offer, clamped to at least one second.
pub(crate) fn heartbeat_interval(
    server_interval: Option<u64>,
    response_lease: Option<u64>,
    task_lease: Option<u32>,
) -> Duration {
    let lease = task_lease
        .map(u64::from)
        .or(response_lease)
        .filter(|l| *l > 0)
        .unwrap_or(DEFAULT_LEASE_SECS);
    let from_lease = (lease / 3).max(1);
    let secs = server_interval
        .filter(|s| *s > 0)
        .map_or(from_lease, |s| s.min(from_lease));
    Duration::from_secs(secs.max(1))
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod worker_tests;
