pub mod alerts;
pub mod ap_poll;
mod backend_identity;
pub mod capsule;
pub mod circuit_breaker;
pub mod cloud_observability;
pub mod compensation;
pub mod continuity;
pub mod continuity_advanced;
pub mod control;
pub mod credentials;
pub mod cron;
pub mod dataflow;
pub mod delegation;
pub mod receipt_bundle;
/// Virtual time for scheduling decisions — re-exported from `orch8-types` so
/// engine users can write `orch8_engine::clock::ManualClock`.
pub mod clock {
    pub use orch8_types::clock::{Clock, ManualClock, SharedClock, SystemClock};
}
pub mod doctor;
pub mod effect_guard;
pub mod embed_sandbox;
pub mod error;
pub mod evaluator;
pub mod event_correlation;
pub mod explain;
pub mod expression;
pub mod externalized;
pub mod failover;
pub mod federation;
pub mod gc;
pub mod handlers;
pub mod interceptors;
pub mod jobs;
pub mod lifecycle;
pub mod lint;
pub mod liveness;
pub mod memory_governance;
pub mod metrics;
pub mod model_pricing;
pub mod optimizer;
pub mod outbound;
pub mod ownership;
pub mod placement;
pub mod preflight;
pub mod preload;
pub mod prompt_registry;
pub mod provenance;
pub mod push;
pub mod queue_routing;
pub mod recovery;
pub mod release_diff;
pub mod remote_executor;
pub mod remote_worker;
pub mod required_fields;
pub mod scheduler;
pub mod scheduling;
pub mod sequence_cache;
pub mod signals;
pub mod step_logs;
pub mod step_placement;
pub mod stream_bus;
pub mod stream_windows;
pub mod template;
pub mod template_trace;
pub mod tenant_budgets;
// Test harness (scheduler-faithful driver, in-process engine); test-only.
#[cfg(any(test, feature = "test-util"))]
pub mod testing;
pub mod trace_context;
pub mod trigger_sources;
pub mod triggers;
pub mod webhooks;
pub mod worker_lease;

use std::sync::Arc;
use std::time::Duration;

use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::handlers::HandlerRegistry;

/// Upper bound for awaiting background tasks to drain after cancel. Tasks that
/// respect the cancellation token normally exit in well under this window; the
/// timeout exists so a stuck task can't block shutdown indefinitely.
const BACKGROUND_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// The core scheduling and orchestration engine.
pub struct Engine {
    storage: Arc<dyn StorageBackend>,
    config: SchedulerConfig,
    handlers: Arc<HandlerRegistry>,
    cancel: CancellationToken,
    /// Unique node ID for this engine instance in a multi-node cluster.
    node_id: Uuid,
    /// Human-readable node name.
    node_name: String,
    /// Tick heartbeat read by the health probes (I-H11).
    liveness: Arc<liveness::EngineLiveness>,
}

/// First delay between startup retries after a transient storage error.
const STARTUP_RETRY_INITIAL: Duration = Duration::from_secs(1);
/// Cap on the delay between startup retries.
const STARTUP_RETRY_MAX: Duration = Duration::from_secs(30);

impl Engine {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        config: SchedulerConfig,
        handlers: HandlerRegistry,
        cancel: CancellationToken,
    ) -> Self {
        let node_id = Uuid::now_v7();
        let node_name = hostname().unwrap_or_else(|| {
            // UUID hyphen-form is always ≥ 8 chars, so slicing is safe.
            let id_str = node_id.to_string();
            format!("node-{}", &id_str[..8])
        });
        let liveness = Arc::new(liveness::EngineLiveness::new(
            Duration::from_millis(config.tick_interval_ms),
            Duration::from_secs(config.liveness_stale_secs),
        ));
        Self {
            storage,
            config,
            handlers: Arc::new(handlers),
            cancel,
            node_id,
            node_name,
            liveness,
        }
    }

    /// Share a caller-owned liveness handle (the server builds it before the
    /// engine so the health routes can hold it).
    #[must_use]
    pub fn with_liveness(mut self, liveness: Arc<liveness::EngineLiveness>) -> Self {
        self.liveness = liveness;
        self
    }

    /// The tick heartbeat the health probes read.
    pub fn liveness(&self) -> Arc<liveness::EngineLiveness> {
        Arc::clone(&self.liveness)
    }

    pub fn storage(&self) -> &dyn StorageBackend {
        self.storage.as_ref()
    }

    pub const fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    pub const fn node_id(&self) -> Uuid {
        self.node_id
    }

    /// Start the engine: runs recovery, cron loop, and the main tick loop until cancelled.
    ///
    /// The startup steps (node registration, stale-instance recovery) retry
    /// transient storage errors with capped exponential backoff until they
    /// succeed or `cancel` fires (L2-04): a database restart during a rolling
    /// deploy used to stop the engine for good while the API kept serving.
    /// A permanent error still returns `Err`; the server treats any engine
    /// exit before shutdown as fatal and exits non-zero. While startup keeps
    /// retrying, the liveness heartbeat is armed but not stamped, so a startup
    /// that never succeeds eventually fails `/health/live`.
    pub async fn run(&self) -> Result<(), error::EngineError> {
        self.liveness.arm();
        let started = retry_transient_startup(&self.cancel, "engine startup", || async {
            // Register this node in the cluster.
            self.register_node().await?;
            // Recover stale instances from previous crash
            recovery::recover_stale_instances(
                self.storage.as_ref(),
                self.config.stale_instance_threshold_secs,
            )
            .await?;
            Ok(())
        })
        .await;
        match started {
            Ok(true) => {}
            Ok(false) => {
                self.liveness.disarm();
                return Ok(());
            }
            Err(e) => {
                self.liveness.disarm();
                return Err(e);
            }
        }

        let mut background = self.spawn_background_tasks();

        // Run the main tick loop
        let result = scheduler::run_tick_loop_with_liveness(
            Arc::clone(&self.storage),
            Arc::clone(&self.handlers),
            &self.config,
            self.cancel.clone(),
            Some(Arc::clone(&self.liveness)),
        )
        .await;
        self.liveness.disarm();

        // Drain background tasks. They all observe `self.cancel` (fired by
        // either scheduler exit, external caller, or drain signal) so this
        // is normally prompt. The bounded timeout is a safety net against
        // a stuck task blocking deregistration.
        Self::drain_background(&mut background).await;

        // Engine-owned durable webhooks drain with their outbox task above.
        // Legacy free-function emitters have an explicit process-wide tracker;
        // shutting down this engine must not wait for unrelated emitters.
        // Push wake-ups are best-effort hints: stop the in-flight ones.
        push::shutdown();

        // Deregister this node on shutdown.
        self.deregister_node().await;

        result
    }

    /// Await all spawned background tasks, bounded by [`BACKGROUND_SHUTDOWN_TIMEOUT`].
    /// After the timeout, remaining tasks are aborted — we accept that a misbehaving
    /// task may lose its final state rather than let it hold the engine from exiting.
    pub(crate) async fn drain_background(background: &mut JoinSet<()>) {
        let drain = async { while background.join_next().await.is_some() {} };
        if tokio::time::timeout(BACKGROUND_SHUTDOWN_TIMEOUT, drain)
            .await
            .is_ok()
        {
            tracing::info!("engine background tasks drained");
        } else {
            tracing::warn!(
                remaining = background.len(),
                timeout_secs = BACKGROUND_SHUTDOWN_TIMEOUT.as_secs(),
                "background tasks did not drain before timeout; aborting"
            );
            background.abort_all();
            // Collect the aborted tasks so the set is fully drained.
            while background.join_next().await.is_some() {}
        }
    }

    /// Spawn all background tasks (heartbeat, reapers, cron, triggers) into a
    /// [`JoinSet`] so `run()` can await graceful shutdown after cancel fires
    /// instead of detaching them. Each task listens on `self.cancel` and is
    /// expected to exit promptly once the token is cancelled.
    #[allow(clippy::too_many_lines)]
    fn spawn_background_tasks(&self) -> JoinSet<()> {
        let mut set: JoinSet<()> = JoinSet::new();
        // Every loop runs under `supervise` (L2-05): a loop that panics or
        // returns while the engine is still running is restarted with
        // backoff and counted in `orch8_background_loop_restarts_total{loop}`
        // instead of silently disappearing until the next process restart.
        let cancel = &self.cancel;

        // Cluster heartbeat + drain check (every 10 seconds).
        let hb_storage = Arc::clone(&self.storage);
        let hb_node_id = self.node_id;
        let drain_cancel = self.cancel.clone();
        set.spawn(supervise("node_heartbeat", cancel.clone(), move || {
            let hb_storage = Arc::clone(&hb_storage);
            let hb_cancel = drain_cancel.clone();
            let drain_cancel = drain_cancel.clone();
            async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    tokio::select! {
                        () = hb_cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            if let Err(e) = hb_storage.heartbeat_node(hb_node_id).await {
                                tracing::error!(error = %e, "cluster heartbeat failed");
                            }
                            match hb_storage.should_drain(hb_node_id).await {
                                Ok(true) => {
                                    tracing::info!(node_id = %hb_node_id, "drain signal received — initiating shutdown");
                                    drain_cancel.cancel();
                                }
                                Ok(false) => {}
                                Err(e) => tracing::error!(error = %e, "drain check failed"),
                            }
                            metrics::loop_pass("node_heartbeat");
                        }
                    }
                }
            }
        }));

        // Stale node reaper. Tick cadence + stale threshold come from
        // SchedulerConfig so tight-loop integration tests can shrink them.
        let node_reaper_storage = Arc::clone(&self.storage);
        let node_reaper_cancel = self.cancel.clone();
        let node_reaper_tick = Duration::from_secs(self.config.node_reaper_tick_secs);
        let node_reaper_stale = Duration::from_secs(self.config.node_reaper_stale_secs);
        set.spawn(supervise("node_reaper", cancel.clone(), move || {
            let node_reaper_storage = Arc::clone(&node_reaper_storage);
            let node_reaper_cancel = node_reaper_cancel.clone();
            async move {
                let mut ticker = tokio::time::interval(node_reaper_tick);
                loop {
                    tokio::select! {
                        () = node_reaper_cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            match node_reaper_storage
                                .reap_stale_nodes(node_reaper_stale)
                                .await
                            {
                                Ok(0) => {}
                                Ok(n) => tracing::info!(count = n, "reaped stale cluster nodes"),
                                Err(e) => tracing::error!(error = %e, "cluster node reaper error"),
                            }
                            metrics::loop_pass("node_reaper");
                        }
                    }
                }
            }
        }));

        // Worker task reaper (resets stale claimed tasks). Tick + threshold
        // come from SchedulerConfig for the same reason as the node reaper.
        let reaper_storage = Arc::clone(&self.storage);
        let reaper_cancel = self.cancel.clone();
        let worker_reaper_tick = Duration::from_secs(self.config.worker_reaper_tick_secs);
        let worker_reaper_stale = Duration::from_secs(self.config.worker_reaper_stale_secs);
        set.spawn(supervise("worker_reaper", cancel.clone(), move || {
            let reaper_storage = Arc::clone(&reaper_storage);
            let reaper_cancel = reaper_cancel.clone();
            async move {
                let mut ticker = tokio::time::interval(worker_reaper_tick);
                loop {
                    tokio::select! {
                        () = reaper_cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            // Expired leases (per-task lease wins over the
                            // default) and timed-out tasks. Side-effecting
                            // tasks are never blindly requeued: their receipt
                            // goes `unknown` and the step's retry policy
                            // decides; timed-out tasks always advance.
                            match crate::worker_lease::reap_worker_tasks(
                                reaper_storage.as_ref(),
                                worker_reaper_stale,
                            )
                            .await
                            {
                                Ok(report) if report.total() == 0 => {}
                                Ok(report) => tracing::info!(
                                    requeued = report.requeued,
                                    ambiguous = report.ambiguous,
                                    timed_out = report.timed_out,
                                    stranded_finalized = report.stranded_finalized,
                                    "resolved expired worker tasks"
                                ),
                                Err(e) => tracing::error!(error = %e, "worker task reaper error"),
                            }
                            metrics::loop_pass("worker_reaper");
                        }
                    }
                }
            }
        }));

        // Stale instance reaper. Resets instances stuck in Running
        // back to Scheduled so the scheduler can re-process them. This is the
        // runtime counterpart of the startup-only `recover_stale_instances` —
        // without it, an instance orphaned by a panic or cancelled task stays
        // stuck until the engine restarts. Waiting instances are not touched:
        // they never heartbeat, so the sweep would recycle every parked gate
        // on each pass; their liveness is the deadline/timeout sweep's job.
        let inst_reaper_storage = Arc::clone(&self.storage);
        let inst_reaper_cancel = self.cancel.clone();
        let inst_reaper_threshold = Duration::from_secs(self.config.stale_instance_threshold_secs);
        set.spawn(supervise("instance_reaper", cancel.clone(), move || {
            let inst_reaper_storage = Arc::clone(&inst_reaper_storage);
            let inst_reaper_cancel = inst_reaper_cancel.clone();
            async move {
                // Tick at half the threshold so detection latency is at most
                // threshold + threshold/2 in the worst case.
                let tick = std::cmp::max(inst_reaper_threshold / 2, Duration::from_secs(10));
                let mut ticker = tokio::time::interval(tick);
                loop {
                    tokio::select! {
                        () = inst_reaper_cancel.cancelled() => break,
                        _ = ticker.tick() => {
                            match inst_reaper_storage
                                .recover_stale_instances(inst_reaper_threshold)
                                .await
                            {
                                Ok(0) => {}
                                Ok(n) => tracing::warn!(
                                    count = n,
                                    threshold_secs = inst_reaper_threshold.as_secs(),
                                    "recovered stale instances (runtime reaper)"
                                ),
                                Err(e) => tracing::error!(error = %e, "stale instance reaper error"),
                            }
                            metrics::loop_pass("instance_reaper");
                        }
                    }
                }
            }
        }));

        // Cron loop.
        let cron_storage = Arc::clone(&self.storage);
        let cron_cancel = self.cancel.clone();
        let cron_tick = std::time::Duration::from_secs(self.config.cron_tick_secs);
        let cron_clock = self.config.clock.clone();
        set.spawn(supervise("cron", cancel.clone(), move || {
            let cron_storage = Arc::clone(&cron_storage);
            let cron_cancel = cron_cancel.clone();
            let cron_clock = cron_clock.clone();
            async move {
                if let Err(e) =
                    cron::run_cron_loop_with_clock(cron_storage, cron_tick, cron_clock, cron_cancel)
                        .await
                {
                    tracing::error!(error = %e, "cron loop exited with error");
                }
            }
        }));

        // Trigger processor loop (syncs trigger definitions every 15 seconds).
        let trigger_storage = Arc::clone(&self.storage);
        let trigger_cancel = self.cancel.clone();
        set.spawn(supervise("triggers", cancel.clone(), move || {
            let trigger_storage = Arc::clone(&trigger_storage);
            let trigger_cancel = trigger_cancel.clone();
            async move {
                triggers::run_trigger_loop(
                    trigger_storage,
                    std::time::Duration::from_secs(15),
                    trigger_cancel,
                )
                .await;
                tracing::info!("trigger processor loop exited");
            }
        }));

        // Durable webhook outbox. Terminal events are committed alongside the
        // instance state transition; this loop owns network delivery and
        // survives process restarts through pending/stale-claim recovery.
        let webhook_storage = Arc::clone(&self.storage);
        let webhook_config = self.config.webhooks.clone();
        let webhook_cancel = self.cancel.clone();
        set.spawn(supervise("webhook_outbox", cancel.clone(), move || {
            let webhook_storage = Arc::clone(&webhook_storage);
            let webhook_config = webhook_config.clone();
            let webhook_cancel = webhook_cancel.clone();
            async move {
                webhooks::run_outbox_loop(webhook_storage, webhook_config, webhook_cancel).await;
                tracing::info!("webhook outbox loop exited");
            }
        }));

        Self::spawn_credentials_refresh(&mut set, Arc::clone(&self.storage), self.cancel.clone());

        // Externalized-state GC sweeper (TTL-based cleanup, every 5 minutes).
        // Also sweeps terminal-instance artifacts when retention is configured
        // (`artifact_retention_secs > 0`); otherwise artifacts are kept. Same
        // for terminal `task_instances` rows themselves (`instance_retention_secs`).
        let gc_storage = Arc::clone(&self.storage);
        let gc_cancel = self.cancel.clone();
        // Only sweep when retention is configured AND an artifact backend is
        // actually wired in — otherwise the sweep would pointlessly mark every
        // terminal instance with no blobs to delete.
        let artifact_retention = (self.config.artifact_retention_secs > 0
            && self.storage.artifacts_enabled())
        .then(|| Duration::from_secs(self.config.artifact_retention_secs));
        // Off by default -- deleting task_instances rows removes queryable
        // instance history, so a deployment must opt in explicitly.
        let instance_retention = (self.config.instance_retention_secs > 0)
            .then(|| Duration::from_secs(self.config.instance_retention_secs));
        set.spawn(supervise("gc", cancel.clone(), move || {
            let gc_storage = Arc::clone(&gc_storage);
            let gc_cancel = gc_cancel.clone();
            async move {
                gc::run_gc_loop(
                    gc_storage,
                    gc::GC_DEFAULT_INTERVAL,
                    artifact_retention,
                    instance_retention,
                    gc_cancel,
                )
                .await;
                tracing::info!("externalized gc loop exited");
            }
        }));

        // Autoscaling backlog gauges (`orch8_queue_depth{capability,region,
        // priority_lane}`, `orch8_placement_unsatisfied`) for KEDA/HPA.
        let backlog_storage = Arc::clone(&self.storage);
        let backlog_cancel = self.cancel.clone();
        set.spawn(supervise("backlog_metrics", cancel.clone(), move || {
            let backlog_storage = Arc::clone(&backlog_storage);
            let backlog_cancel = backlog_cancel.clone();
            async move {
                step_placement::run_backlog_metrics_loop(
                    backlog_storage,
                    step_placement::BACKLOG_METRICS_INTERVAL,
                    backlog_cancel,
                )
                .await;
            }
        }));

        set
    }

    /// Spawn the `OAuth2` credential refresh loop into the engine's
    /// [`JoinSet`] so it participates in graceful shutdown.
    /// Polls every 60s and refreshes tokens expiring within the next 5 minutes.
    fn spawn_credentials_refresh(
        set: &mut JoinSet<()>,
        storage: Arc<dyn StorageBackend>,
        cancel: CancellationToken,
    ) {
        set.spawn(supervise(
            "credentials_refresh",
            cancel.clone(),
            move || {
                let storage = Arc::clone(&storage);
                let cancel = cancel.clone();
                async move {
                    credentials::run_refresh_loop(
                        storage,
                        std::time::Duration::from_secs(60),
                        std::time::Duration::from_secs(300),
                        cancel,
                    )
                    .await;
                    tracing::info!("credentials refresh loop exited");
                }
            },
        ));
    }

    /// Register this engine instance as a cluster node.
    async fn register_node(&self) -> Result<(), error::EngineError> {
        let now = chrono::Utc::now();
        let node = orch8_types::cluster::ClusterNode {
            id: self.node_id,
            name: self.node_name.clone(),
            status: orch8_types::cluster::NodeStatus::Active,
            registered_at: now,
            last_heartbeat_at: now,
            drain: false,
            drain_started_at: None,
            stopped_at: None,
            capabilities_withdrawn: false,
            execution_handoff_evidence: None,
        };
        self.storage.register_node(&node).await?;
        tracing::info!(
            node_id = %self.node_id,
            node_name = %self.node_name,
            "cluster node registered"
        );
        Ok(())
    }

    /// Mark this node as stopped in the cluster registry.
    async fn deregister_node(&self) {
        if let Err(e) = self.storage.deregister_node(self.node_id).await {
            tracing::error!(error = %e, "failed to deregister cluster node");
        } else {
            tracing::info!(node_id = %self.node_id, "cluster node deregistered");
        }
    }
}

/// First delay before restarting a background loop that died.
const LOOP_RESTART_INITIAL: Duration = Duration::from_secs(1);
/// Cap on the restart delay.
const LOOP_RESTART_MAX: Duration = Duration::from_secs(60);
/// A loop that ran at least this long before dying restarts from the
/// initial delay again (it was healthy; this is a fresh failure).
const LOOP_HEALTHY_RUN: Duration = Duration::from_secs(60);

/// Aborts the wrapped task when dropped, so aborting the supervisor (the
/// engine's bounded shutdown drain calls `abort_all`) also stops the loop it
/// runs instead of leaving it detached.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// +-20% jitter so several loops that crashed together (shared dependency
/// down) do not restart in lockstep.
fn jittered_restart_delay(delay: Duration) -> Duration {
    delay.mul_f64(rand::RngExt::random_range(&mut rand::rng(), 0.8..1.2))
}

/// Run a background loop built by `make` until `cancel` fires, restarting it
/// with capped exponential backoff whenever it panics or returns early
/// (L2-05). Each restart increments
/// `orch8_background_loop_restarts_total{loop=name}` and logs at `error!`.
pub async fn supervise<F, Fut>(name: &'static str, cancel: CancellationToken, mut make: F)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut delay = LOOP_RESTART_INITIAL;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let started = tokio::time::Instant::now();
        let mut task = AbortOnDrop(tokio::spawn(make()));
        let outcome = (&mut task.0).await;
        if cancel.is_cancelled() {
            return;
        }
        match outcome {
            Ok(()) => tracing::error!(
                background_loop = name,
                "background loop exited while the engine is running; restarting"
            ),
            Err(e) => tracing::error!(
                background_loop = name,
                error = %e,
                "background loop panicked; restarting"
            ),
        }
        metrics::inc_with(metrics::BACKGROUND_LOOP_RESTARTS, &[("loop", name)]);
        if started.elapsed() >= LOOP_HEALTHY_RUN {
            delay = LOOP_RESTART_INITIAL;
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(jittered_restart_delay(delay)) => {}
        }
        delay = (delay * 2).min(LOOP_RESTART_MAX);
    }
}

/// Run `op` until it succeeds, retrying transient storage errors with capped,
/// jittered exponential backoff. Returns `Ok(true)` on success, `Ok(false)`
/// when `cancel` fired while waiting, and `Err` on a non-transient error.
async fn retry_transient_startup<F, Fut>(
    cancel: &CancellationToken,
    what: &'static str,
    mut op: F,
) -> Result<bool, error::EngineError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), error::EngineError>>,
{
    let mut delay = STARTUP_RETRY_INITIAL;
    let mut attempt: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            return Ok(false);
        }
        match op().await {
            Ok(()) => return Ok(true),
            Err(e) if e.is_transient_storage() => {
                attempt = attempt.saturating_add(1);
                // ±20% jitter so a fleet restarting together does not
                // retry in lockstep against a recovering database.
                let jittered =
                    delay.mul_f64(rand::RngExt::random_range(&mut rand::rng(), 0.8..1.2));
                tracing::warn!(
                    error = %e,
                    attempt,
                    retry_in_ms = u64::try_from(jittered.as_millis()).unwrap_or(u64::MAX),
                    "{what} hit a transient storage error; retrying"
                );
                tokio::select! {
                    () = cancel.cancelled() => return Ok(false),
                    () = tokio::time::sleep(jittered) => {}
                }
                delay = (delay * 2).min(STARTUP_RETRY_MAX);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Best-effort hostname retrieval.
fn hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("POD_NAME"))
        .ok()
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use tokio::time::Instant;

    /// Tasks that honor cancellation exit promptly; `drain_background`
    /// should return well before the shutdown timeout and leave the set
    /// empty. This validates the common-case branch — no aborts needed.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn drain_background_returns_promptly_when_tasks_cooperate() {
        let cancel = CancellationToken::new();
        let mut set: JoinSet<()> = JoinSet::new();
        for _ in 0..3 {
            let token = cancel.clone();
            set.spawn(async move {
                token.cancelled().await;
            });
        }
        cancel.cancel();

        let started = Instant::now();
        Engine::drain_background(&mut set).await;
        // With start_paused, only awaits on `tokio::time` advance the clock.
        // Cooperative tasks complete on the runtime's ready queue, so virtual
        // time never reaches the 10s shutdown deadline.
        assert!(
            started.elapsed() < BACKGROUND_SHUTDOWN_TIMEOUT,
            "expected prompt drain, took {:?}",
            started.elapsed()
        );
        assert_eq!(set.len(), 0, "all tasks should be joined");
    }

    /// Stuck tasks (ignoring the cancel token) must be forcefully aborted
    /// so the timeout branch actually completes the drain. Without the
    /// `abort_all` fallback this test would hang past the shutdown deadline.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn drain_background_aborts_stuck_tasks_after_timeout() {
        let cancel = CancellationToken::new();
        let mut set: JoinSet<()> = JoinSet::new();
        // Two cooperative tasks and one stuck task. The stuck task runs a
        // forever-pending future so only `abort()` can terminate it.
        for _ in 0..2 {
            let token = cancel.clone();
            set.spawn(async move {
                token.cancelled().await;
            });
        }
        set.spawn(async {
            // Never completes, never checks cancel.
            std::future::pending::<()>().await;
        });
        cancel.cancel();

        Engine::drain_background(&mut set).await;
        // Whether prompt or via timeout+abort, the contract is the same:
        // `drain_background` always returns with an empty set.
        assert_eq!(
            set.len(),
            0,
            "drain_background must always leave the set empty, even after timeout"
        );
    }

    /// L2-05: a background loop that panics (or returns) while the engine is
    /// running is restarted; once cancel fires the supervisor exits.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn supervise_restarts_a_panicking_loop_until_cancelled() {
        let cancel = CancellationToken::new();
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let runs_in = Arc::clone(&runs);
        let token = cancel.clone();
        let sup = tokio::spawn(supervise("test_loop", cancel.clone(), move || {
            let runs = Arc::clone(&runs_in);
            let token = token.clone();
            async move {
                let n = runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                match n {
                    0 => panic!("loop bug"),
                    1 => {} // early return: also restarted
                    _ => token.cancelled().await,
                }
            }
        }));
        // Two restarts back off 1 s then 2 s (virtual time).
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 3);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), sup)
            .await
            .expect("supervisor exits on cancel")
            .unwrap();
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn supervise_restart_delay_is_jittered_within_bounds() {
        let delays: std::collections::BTreeSet<_> = (0..50)
            .map(|_| jittered_restart_delay(Duration::from_secs(10)))
            .collect();
        assert!(
            delays
                .iter()
                .all(|d| *d >= Duration::from_secs(8) && *d <= Duration::from_secs(12))
        );
        assert!(delays.len() > 5, "delays spread out");
    }

    /// L2-04: transient storage errors during startup are retried with
    /// backoff instead of permanently stopping the engine.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn startup_retries_transient_errors_until_success() {
        let cancel = CancellationToken::new();
        let calls = std::sync::atomic::AtomicU32::new(0);
        let started = retry_transient_startup(&cancel, "test", || async {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < 2 {
                Err(error::EngineError::Storage(
                    orch8_types::error::StorageError::Connection("db restarting".into()),
                ))
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();
        assert!(started);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    /// A permanent error is not retried: the engine exits and the server
    /// turns that into a non-zero process exit.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn startup_does_not_retry_permanent_errors() {
        let cancel = CancellationToken::new();
        let calls = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient_startup(&cancel, "test", || async {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(error::EngineError::Storage(
                orch8_types::error::StorageError::Query("bad schema".into()),
            ))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Cancellation while backing off ends the retry loop cleanly.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn startup_retry_stops_on_cancel() {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let started = retry_transient_startup(&cancel, "test", move || {
            token.cancel();
            async {
                Err(error::EngineError::Storage(
                    orch8_types::error::StorageError::PoolExhausted,
                ))
            }
        })
        .await
        .unwrap();
        assert!(!started);
    }

    /// An empty set must return immediately — no timeout, no spurious log.
    /// Guards against an off-by-one in the drain loop (e.g., `join_next`
    /// returning `None` on first call should exit cleanly).
    #[tokio::test]
    async fn drain_background_is_noop_for_empty_set() {
        let mut set: JoinSet<()> = JoinSet::new();
        let started = Instant::now();
        Engine::drain_background(&mut set).await;
        assert!(started.elapsed() < std::time::Duration::from_millis(50));
    }
}
