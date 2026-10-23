use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::clock::SharedClock;
use orch8_types::config::{SchedulerConfig, WebhookConfig};
use orch8_types::filter::InstanceFilter;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::Signal;

use crate::error::EngineError;
use crate::evaluator::{EvalOutcome, TerminalReason};
use crate::handlers::HandlerRegistry;
use crate::sequence_cache::SequenceCache;

mod step_exec;
#[cfg(test)]
mod tests;

pub(crate) use step_exec::check_human_input_at_with_claim;
pub(crate) use step_exec::{
    HUMAN_GATE_MARKER, auto_decide_marker_key, clamped_fire_at, delay_marker_key,
    park_tree_instance_until, step_preamble_deferral,
};
pub use step_exec::{check_human_input, check_human_input_at};

/// Result of a single tick execution, suitable for mobile/embedded callers
/// that drive the engine manually rather than via the continuous tick loop.
#[derive(Debug, Clone, Default)]
pub struct TickOnceResult {
    /// Number of instances that advanced during this tick.
    pub instances_advanced: u32,
    /// Number of individual steps executed during this tick.
    pub steps_executed: u32,
    /// True if there are still scheduled instances that could run on the next tick.
    pub has_pending_work: bool,
}

/// Thread-safe counters shared between the caller and spawned per-instance
/// tasks so `tick_once` can report accurate `instances_advanced` /
/// `steps_executed` values.
pub(crate) struct TickCounters {
    instances_advanced: AtomicU32,
    steps_executed: AtomicU32,
}

/// Bundles the static configuration parameters that remain constant for the
/// lifetime of a single tick. This replaces the long parameter lists on
/// `process_tick` and `process_instance`, making the call sites easier to
/// read and extend.
pub(crate) struct TickContext<'a> {
    pub storage: &'a Arc<dyn StorageBackend>,
    pub handlers: &'a Arc<HandlerRegistry>,
    pub semaphore: &'a Arc<Semaphore>,
    pub cancel: &'a CancellationToken,
    pub sequence_cache: &'a Arc<SequenceCache>,
    pub webhook_config: &'a Arc<WebhookConfig>,
    pub max_steps_per_instance: u32,
    pub batch_size: u32,
    pub max_per_tenant: u32,
    pub externalize_threshold: u32,
    pub counters: Option<Arc<TickCounters>>,
    /// Time source for all scheduling decisions made during this tick
    /// (claiming, deferrals, deadline/timeout checks). Defaults to the
    /// system clock via `SchedulerConfig::clock`.
    pub clock: &'a SharedClock,
    /// Same threshold the stale-instance reaper (`recover_stale_instances`)
    /// uses to reclaim wedged instances. Each claimed instance's processing
    /// task heartbeats at a fraction of this interval (C-1) so a step that is
    /// still genuinely executing never looks stale to the reaper.
    pub stale_instance_threshold_secs: u64,
}

/// Execute a single scheduling pass: claim due instances, process them, handle
/// signals, and return a summary. This is the mobile/embedded entry point —
/// the caller controls tick cadence rather than the engine running its own loop.
pub async fn tick_once(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    semaphore: &Arc<Semaphore>,
    config: &SchedulerConfig,
    sequence_cache: &Arc<SequenceCache>,
    cancel: &CancellationToken,
) -> Result<TickOnceResult, EngineError> {
    let webhook_config = Arc::new(config.webhooks.clone());

    let counters = Arc::new(TickCounters {
        instances_advanced: AtomicU32::new(0),
        steps_executed: AtomicU32::new(0),
    });

    let ctx = TickContext {
        storage,
        handlers,
        semaphore,
        cancel,
        sequence_cache,
        webhook_config: &webhook_config,
        max_steps_per_instance: config.max_steps_per_instance,
        batch_size: config.batch_size,
        max_per_tenant: config.max_instances_per_tenant,
        externalize_threshold: config.externalize_output_threshold,
        counters: Some(Arc::clone(&counters)),
        clock: &config.clock,
        stale_instance_threshold_secs: config.stale_instance_threshold_secs,
    };

    let join_handles = process_tick(&ctx).await?;

    // Await all spawned instance tasks so the counters are fully populated
    // before we read them.
    for handle in join_handles {
        // Errors here (task panic / cancellation) are already handled inside
        // the spawned task — we only need to ensure they finish.
        let _ = handle.await;
    }

    retry_deferred_parent_wakes(storage).await;

    let hooks = HookTasks::default();
    process_signalled_instances(
        storage,
        handlers,
        sequence_cache,
        config.batch_size,
        &config.clock,
        &hooks,
    )
    .await?;
    // After the signal sweep so a parent cancelled by a signal this pass
    // takes its children down in the same pass.
    cancel_orphans(storage, handlers, sequence_cache, &hooks).await;

    // Like the SLA sweep below, the single-shot path carries no cursor and
    // scans the whole Waiting set.
    process_waiting_deadlines(
        &SweepContext {
            storage,
            handlers,
            sequence_cache,
            webhook_config: &webhook_config,
            cancel,
            batch_size: config.batch_size,
            clock: &config.clock,
            hooks: &hooks,
        },
        &mut WaitingSweepCursor::default(),
        u32::MAX,
    )
    .await?;
    // Single-shot callers expect the cleanup/escalation hooks of this pass
    // to have run when the call returns.
    hooks.join_all().await;

    // `tick_once` is the single-shot mobile entry point — no cursor is
    // carried between calls, so each call scans the full active set
    // (u32::MAX pages). Bounding pages here would leave instances sorted
    // past the bound never swept on this path.
    let mut sla_cursor = SlaSweepCursor::default();
    process_sla_breaches(
        storage,
        sequence_cache,
        &webhook_config,
        cancel,
        config.batch_size,
        &config.clock,
        &mut sla_cursor,
        u32::MAX,
    )
    .await?;

    // A failed query does not prove the queue is idle. Embedded callers use
    // this flag to stop draining, so propagate storage failures for retry.
    // EXISTS probe, not a COUNT over every parked timer (L4-16).
    let has_pending = storage
        .any_instance_in_state(InstanceState::Scheduled)
        .await?;

    Ok(TickOnceResult {
        instances_advanced: counters.instances_advanced.load(Ordering::Relaxed),
        steps_executed: counters.steps_executed.load(Ordering::Relaxed),
        has_pending_work: has_pending,
    })
}

/// Pre-fetched data for an instance, gathered in batch before spawning tasks.
#[derive(Clone)]
struct PrefetchedData {
    signals: Vec<Signal>,
    completed_block_ids: Vec<BlockId>,
}

/// Run the scheduling tick loop until cancellation.
///
/// Each tick:
/// 1. Claims a batch of due instances (`FOR UPDATE SKIP LOCKED`)
/// 2. Spawns bounded concurrent tasks to process each instance
/// 3. Overlapping ticks are safe — `SKIP LOCKED` prevents double-claiming
pub async fn run_tick_loop(
    storage: Arc<dyn StorageBackend>,
    handlers: Arc<HandlerRegistry>,
    config: &SchedulerConfig,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    run_tick_loop_with_liveness(storage, handlers, config, cancel, None).await
}

/// [`run_tick_loop`] that also stamps `liveness` at the end of every loop
/// iteration (claim plus all sweeps), so the health probes can tell a
/// wedged scheduler from a healthy one (I-H11).
#[allow(clippy::too_many_lines)]
pub async fn run_tick_loop_with_liveness(
    storage: Arc<dyn StorageBackend>,
    handlers: Arc<HandlerRegistry>,
    config: &SchedulerConfig,
    cancel: CancellationToken,
    liveness: Option<Arc<crate::liveness::EngineLiveness>>,
) -> Result<(), EngineError> {
    let tick_duration = Duration::from_millis(config.tick_interval_ms);
    let batch_size = config.batch_size;
    let max_concurrent = config.max_concurrent_steps as usize;
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    // In-memory LRU cache for sequence definitions (avoids 1 DB query per instance).
    // Unified `SequenceCache` covers both by-id and by-name lookups so future
    // callers in evaluator/triggers can cut DB roundtrips on sub-sequence
    // resolution and trigger firing without reshaping this plumbing.
    let sequence_cache: Arc<SequenceCache> =
        Arc::new(SequenceCache::new(1_000, Duration::from_secs(300)));

    // Share WebhookConfig via Arc to avoid cloning per instance.
    let webhook_config = Arc::new(config.webhooks.clone());
    let externalize_threshold = config.externalize_output_threshold;

    let mut ticker = interval(tick_duration);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        tick_ms = config.tick_interval_ms,
        batch_size = batch_size,
        max_concurrent = max_concurrent,
        "starting tick loop"
    );

    let mut consecutive_failures: u32 = 0;
    // Rotating cursor for the SLA sweep — persists across ticks so the sweep
    // covers the whole active set over successive ticks, a bounded number of
    // pages at a time.
    let mut sla_cursor = SlaSweepCursor::default();
    // Rotating cursor for the Waiting deadline/timeout sweep (L6-04).
    let mut waiting_cursor = WaitingSweepCursor::default();
    // One hook pool for the loop's lifetime: the per-tenant / queue bounds
    // only mean something if they span iterations (D-05).
    let hooks = HookTasks::default();
    // Stamp liveness between phases, not only at the end of the iteration:
    // under DB slowness claim + marker preload + sweeps can together exceed
    // the readiness window although every phase is making progress (D-04).
    let stamp = || {
        if let Some(liveness) = &liveness {
            liveness.record_tick();
        }
    };

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!("tick loop cancelled, draining in-flight tasks");
                let grace = Duration::from_secs(config.shutdown_grace_period_secs);
                if tokio::time::timeout(grace, wait_for_drain(&semaphore, max_concurrent)).await.is_err() {
                    warn!("grace period expired, some tasks may not have completed");
                }
                // Cancel/escalation hooks are best-effort but owed: give them
                // a bounded window, then abort the stragglers.
                if !hooks.drain(grace).await {
                    warn!("grace period expired, some cancel/escalation hooks were aborted");
                }
                info!("tick loop stopped");
                return Ok(());
            }
            _ = ticker.tick() => {
                let ctx = TickContext {
                    storage: &storage,
                    handlers: &handlers,
                    semaphore: &semaphore,
                    cancel: &cancel,
                    sequence_cache: &sequence_cache,
                    webhook_config: &webhook_config,
                    max_steps_per_instance: config.max_steps_per_instance,
                    batch_size,
                    max_per_tenant: config.max_instances_per_tenant,
                    externalize_threshold,
                    // The tick loop does not need per-tick counters — pass None.
                    counters: None,
                    clock: &config.clock,
                    stale_instance_threshold_secs: config.stale_instance_threshold_secs,
                };
                match process_tick(&ctx).await {
                    Ok(_join_handles) => {
                        // Fire-and-forget: spawned tasks release semaphore
                        // permits on completion. We intentionally drop the
                        // handles here — `wait_for_drain` at shutdown is the
                        // only place that needs to await them.
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        let backoff_ms = 100u64
                            .saturating_mul(2u64.saturating_pow(consecutive_failures))
                            .min(5000);
                        error!(
                            error = %e,
                            consecutive_failures,
                            backoff_ms,
                            "tick processing failed, backing off"
                        );
                        // Inside a select so shutdown is not delayed by up
                        // to the 5 s backoff.
                        tokio::select! {
                            () = cancel.cancelled() => {}
                            () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                        }
                    }
                }
                stamp();
                retry_deferred_parent_wakes(&storage).await;
                // Hooks spawned by the sweeps run off the tick on the
                // loop-wide bounded pool.
                // Process signals for paused/waiting instances that won't be
                // picked up by claim_due_instances (which only claims
                // scheduled instances). Without this, resume/cancel signals
                // on paused instances would never be processed.
                if let Err(e) = process_signalled_instances(
                    &storage,
                    &handlers,
                    &sequence_cache,
                    batch_size,
                    &config.clock,
                    &hooks,
                )
                .await
                {
                    error!(error = %e, "signalled instance processing failed");
                }
                // After the signal sweep: a parent cancelled by a signal this
                // iteration takes its children down in the same iteration.
                cancel_orphans(&storage, &handlers, &sequence_cache, &hooks).await;
                stamp();
                // Check SLA deadlines for waiting instances (external worker
                // dispatch). These instances are not claimed by the normal
                // tick, but deadlines should still fire.
                if let Err(e) = process_waiting_deadlines(
                    &SweepContext {
                        storage: &storage,
                        handlers: &handlers,
                        sequence_cache: &sequence_cache,
                        webhook_config: &webhook_config,
                        cancel: &cancel,
                        batch_size,
                        clock: &config.clock,
                        hooks: &hooks,
                    },
                    &mut waiting_cursor,
                    MAX_SWEEP_PAGES,
                ).await {
                    error!(error = %e, "waiting deadline processing failed");
                }
                stamp();
                // Alert-only SLA sweep over active instances (max_runtime /
                // max_step_runtime). Emits webhook + metric on breach; never
                // changes instance state. Rotates through the active set via
                // `sla_cursor`, a bounded page count per tick.
                if let Err(e) = process_sla_breaches(
                    &storage,
                    &sequence_cache,
                    &webhook_config,
                    &cancel,
                    batch_size,
                    &config.clock,
                    &mut sla_cursor,
                    SLA_SWEEP_MAX_PAGES_PER_TICK,
                ).await {
                    error!(error = %e, "sla breach sweep failed");
                }
                stamp();
                crate::metrics::loop_pass("tick");
            }
        }
    }
}

/// Consecutive instance-heartbeat failures after which the lease is treated
/// as lost (the heartbeat ticks at a third of the staleness window).
const HEARTBEAT_LEASE_LOST_AFTER: u32 = 3;

/// Preserve subsecond cadence for short staleness windows. A zero threshold
/// has no positive safe heartbeat interval; retain a nonzero fallback cadence
/// for callers using zero for immediate recovery checks.
fn instance_heartbeat_interval(stale_threshold_secs: u64) -> Duration {
    if stale_threshold_secs == 0 {
        return Duration::from_secs(5);
    }
    Duration::from_secs(stale_threshold_secs) / 3
}

/// Wait until all semaphore permits are available (all in-flight tasks drained).
///
/// Uses `acquire_many` which blocks until `max_permits` are free — no polling.
async fn wait_for_drain(semaphore: &Semaphore, max_permits: usize) {
    // Saturating cast: `max_concurrent_steps` is `u32` at the config layer, so
    // values above `u32::MAX` cannot occur in practice. Clamp defensively.
    let permits = u32::try_from(max_permits).unwrap_or(u32::MAX);
    // If acquire_many errors, the semaphore was closed — drain is trivially complete.
    if let Ok(permit) = semaphore.acquire_many(permits).await {
        // Immediately release: we only care that all in-flight tasks returned their permits.
        drop(permit);
    }
}

/// Process a single tick: claim due instances and spawn processing tasks.
///
/// When `counters` is `Some`, each spawned task will atomically increment the
/// shared counters upon completion. The returned `Vec<JoinHandle>` allows the
/// caller to await all tasks before reading the counters (used by `tick_once`).
/// When `counters` is `None` (continuous tick loop), the caller may drop the
/// handles — tasks are fire-and-forget.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(skip_all, fields(batch_size = ctx.batch_size, claimed = tracing::field::Empty))]
async fn process_tick(ctx: &TickContext<'_>) -> Result<Vec<JoinHandle<()>>, EngineError> {
    let _tick_timer = crate::metrics::Timer::start(crate::metrics::TICK_DURATION);

    let available = ctx.semaphore.available_permits();
    if available == 0 {
        return Ok(Vec::new());
    }
    let fetch_limit = std::cmp::min(ctx.batch_size, u32::try_from(available).unwrap_or(u32::MAX));

    let now = ctx.clock.now();
    let instances = ctx
        .storage
        .claim_due_instances(now, fetch_limit, ctx.max_per_tenant)
        .await?;

    if instances.is_empty() {
        return Ok(Vec::new());
    }

    // Enforce concurrency limits BEFORE spawning tasks. `claim_due_instances`
    // sets all claimed instances to Running in a single batch — if multiple
    // instances share a concurrency_key, more than `max_concurrency` may be
    // Running simultaneously. We put the excess back to Scheduled here,
    // synchronously, so the observable Running count never exceeds the limit.
    let claimed_ids: Vec<_> = instances
        .iter()
        .map(|instance| (instance.id, instance.updated_at))
        .collect();
    let mut instances = match enforce_concurrency_limits(ctx.storage, instances, ctx.clock).await {
        Ok(instances) => instances,
        Err(error) => {
            reschedule_claimed_after_setup_error(
                ctx.storage,
                &claimed_ids,
                ctx.clock.now(),
                "concurrency-limit setup",
            )
            .await;
            return Err(error);
        }
    };

    let count = instances.len();
    tracing::Span::current().record("claimed", count);
    debug!(count = count, "claimed instances for processing");
    #[allow(clippy::cast_precision_loss)]
    crate::metrics::set_gauge(crate::metrics::QUEUE_DEPTH, count as f64);

    // Batch-fetch signals and completed block IDs for all claimed instances (2 queries total).
    let instance_ids: Vec<InstanceId> = instances.iter().map(|i| i.id).collect();
    let prefetch = tokio::try_join!(
        async {
            ctx.storage
                .get_pending_signals_batch(&instance_ids)
                .await
                .map_err(EngineError::from)
        },
        async {
            ctx.storage
                .get_completed_block_ids_batch(&instance_ids)
                .await
                .map_err(EngineError::from)
        },
    );
    let (signals_map, completed_map) = match prefetch {
        Ok(prefetched) => prefetched,
        Err(error) => {
            let ids: Vec<_> = instances
                .iter()
                .map(|instance| (instance.id, instance.updated_at))
                .collect();
            reschedule_claimed_after_setup_error(
                ctx.storage,
                &ids,
                ctx.clock.now(),
                "batch prefetch",
            )
            .await;
            return Err(error);
        }
    };

    // Hydrate externalized `context.data` markers for every claimed
    // instance in one batched fetch. This removes the per-step N+1 that
    // would otherwise occur inside `resolve_markers`. Storage errors are
    // non-fatal: the per-step resolver remains the correctness fallback.
    crate::preload::preload_externalized_markers(ctx.storage.as_ref(), &mut instances).await;

    // Wrap in Arc so we can share cheaply across spawned tasks.
    let prefetched = Arc::new(build_prefetch_map(signals_map, completed_map));

    let mut join_handles = Vec::with_capacity(instances.len());

    for instance in instances {
        // Reserve the permit synchronously BEFORE spawning so fast ticks
        // cannot claim more rows than available concurrency slots. The permit
        // is moved into the spawned task and released when processing completes.
        let Ok(permit) = ctx.semaphore.clone().try_acquire_owned() else {
            // All permits taken — defer this instance back to Scheduled so
            // a future tick picks it up.
            debug!(
                instance_id = %instance.id,
                "no semaphore permit available, deferring instance"
            );
            if let Err(e) = ctx
                .storage
                .conditional_update_claimed_instance_state(
                    instance.id,
                    instance.updated_at,
                    InstanceState::Running,
                    InstanceState::Scheduled,
                    Some(ctx.clock.now()),
                )
                .await
            {
                // If this write fails the row stays `Running` and is never
                // re-claimed — surface it rather than silently stranding it.
                warn!(
                    instance_id = %instance.id,
                    error = %e,
                    "failed to defer instance back to Scheduled; it may be stuck Running"
                );
            }
            continue;
        };

        let storage = Arc::clone(ctx.storage);
        let handlers = Arc::clone(ctx.handlers);
        let webhooks = Arc::clone(ctx.webhook_config);
        let seq_cache = Arc::clone(ctx.sequence_cache);
        let prefetched = Arc::clone(&prefetched);
        let cancel = ctx.cancel.clone();
        let counters = ctx.counters.clone();
        let externalize_threshold = ctx.externalize_threshold;
        let max_steps_per_instance = ctx.max_steps_per_instance;
        let clock = ctx.clock.clone();
        let stale_instance_threshold_secs = ctx.stale_instance_threshold_secs;

        crate::metrics::inc(crate::metrics::INSTANCES_CLAIMED);

        // Capture the initial step count before spawning so we can compute
        // the delta after the task completes.
        let initial_steps = instance.context.runtime.total_steps_executed;

        let handle = tokio::spawn(async move {
            // Permit was acquired before spawn — hold it for the duration.
            let _permit = permit;

            let _instance_timer = crate::metrics::Timer::start(crate::metrics::INSTANCE_DURATION);
            let instance_id = instance.id;
            // Claim token (L6-01): `claim_due_instances` returns the claim
            // time as `updated_at`; the heartbeat is fenced on it.
            let claim_token = instance.updated_at;

            // Extract pre-fetched data for this instance (or use empty defaults).
            let data = prefetched
                .get(&instance_id)
                .cloned()
                .unwrap_or_else(|| PrefetchedData {
                    signals: Vec::new(),
                    completed_block_ids: Vec::new(),
                });

            // Lease heartbeat (C-1): while this instance's step is genuinely
            // in flight, periodically touch `updated_at` so
            // `recover_stale_instances` can tell a slow-but-healthy step
            // apart from one whose worker actually died, and doesn't
            // re-dispatch it out from under itself mid-execution. Stopped as
            // soon as `process_instance` returns, below.
            let heartbeat_storage = Arc::clone(&storage);
            let heartbeat_stop = CancellationToken::new();
            // Set by the heartbeat task once the lease is presumed lost; the
            // flat step loop checks it between steps (its only safe
            // cancellation point) and stops starting new work.
            let lease_lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let heartbeat_handle = {
                let heartbeat_stop = heartbeat_stop.clone();
                let lease_lost = Arc::clone(&lease_lost);
                // Tick at a fraction of the reaper's own staleness window so
                // several heartbeats land comfortably before this instance
                // could ever look stale.
                let heartbeat_interval = instance_heartbeat_interval(stale_instance_threshold_secs);
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(heartbeat_interval);
                    ticker.tick().await; // first tick fires immediately; claiming already set updated_at
                    let mut consecutive_failures: u32 = 0;
                    loop {
                        tokio::select! {
                            () = heartbeat_stop.cancelled() => break,
                            _ = ticker.tick() => {
                                match heartbeat_storage.heartbeat_instance(instance_id, claim_token).await {
                                    Ok(orch8_storage::InstanceLease::Held) => consecutive_failures = 0,
                                    Ok(orch8_storage::InstanceLease::Lost) => {
                                        // Definitive: the reaper reset this claim or
                                        // another claim replaced it. Stop starting
                                        // steps; the new owner runs the instance.
                                        lease_lost.store(true, std::sync::atomic::Ordering::Release);
                                        error!(
                                            instance_id = %instance_id,
                                            "instance lease lost to another claim; not starting further steps"
                                        );
                                        break;
                                    }
                                    Err(e) => {
                                        consecutive_failures = consecutive_failures.saturating_add(1);
                                        // The interval is a third of the reaper's
                                        // staleness window, so this many misses in a row
                                        // means the lease has (almost certainly) lapsed
                                        // and another node may re-dispatch the step.
                                        // The in-flight step is not aborted (dropping it
                                        // mid-write is worse than the overlap), but no
                                        // further step is started — see `lease_lost`.
                                        if consecutive_failures == HEARTBEAT_LEASE_LOST_AFTER {
                                            lease_lost.store(true, std::sync::atomic::Ordering::Release);
                                            error!(
                                                instance_id = %instance_id,
                                                error = %e,
                                                consecutive_failures,
                                                "instance heartbeat failing repeatedly — lease likely lost; the step may be re-dispatched by the stale-instance reaper"
                                            );
                                        } else {
                                            warn!(instance_id = %instance_id, error = %e, consecutive_failures, "instance heartbeat failed");
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
            };

            // Spawn a nested task so we can `.await` its JoinHandle — this
            // catches panics (JoinError::is_panic) that would otherwise
            // silently kill the outer task and leave the instance stuck in
            // Running forever (leaking a semaphore permit).
            let s2 = Arc::clone(&storage);
            // `clock` is moved into the inner task below; keep a handle in the
            // outer task for the transient-error reschedule fire time.
            let clock_outer = clock.clone();
            let lease_lost_inner = Arc::clone(&lease_lost);
            let result = tokio::spawn(async move {
                let ctx = InstanceRunCtx {
                    storage: &s2,
                    handlers: &handlers,
                    webhook_config: &webhooks,
                    sequence_cache: &seq_cache,
                    externalize_threshold,
                    max_steps_per_instance,
                    cancel: &cancel,
                    clock: &clock,
                    lease_lost: &lease_lost_inner,
                    claim_token,
                };
                process_instance(&ctx, instance, data).await
            })
            .await;

            heartbeat_stop.cancel();
            let _ = heartbeat_handle.await;

            match result {
                Ok(Ok(())) => transient_retries::clear(instance_id),
                Ok(Err(EngineError::ClaimLost { .. })) => {
                    transient_retries::clear(instance_id);
                    tracing::debug!(%instance_id, "stopped processing after claimed write was refused");
                    if let Err(error) = storage
                        .conditional_update_claimed_instance_state(
                            instance_id,
                            claim_token,
                            InstanceState::Running,
                            InstanceState::Scheduled,
                            Some(clock_outer.now()),
                        )
                        .await
                    {
                        tracing::warn!(%instance_id, %error, "failed to hand back stopped claim");
                    }
                }
                Ok(Err(e)) if e.is_transient_storage() => {
                    // A transient storage failure (connection loss, pool
                    // exhaustion, DB failover) is NOT a processing failure —
                    // failing the instance here would DLQ otherwise-healthy
                    // work on a momentary infrastructure blip. Reschedule it
                    // back to Scheduled so a later tick retries; the CAS guard
                    // (Running -> Scheduled) is a no-op if a concurrent writer
                    // already moved it. Retries back off per instance and are
                    // bounded (L2-17): an error that is "transient" by type
                    // but recurs forever (a rotated artifact key, a deleted
                    // payload blob) used to re-claim the instance ~10x/s.
                    crate::metrics::inc(crate::metrics::INSTANCE_TRANSIENT_RETRIES);
                    match transient_retries::record(instance_id) {
                        transient_retries::Decision::Retry(delay) => {
                            warn!(
                                instance_id = %instance_id,
                                error = %e,
                                retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                                "instance processing hit a transient storage error -- rescheduling instead of failing"
                            );
                            let fire_at = clock_outer.now()
                                + chrono::Duration::from_std(delay)
                                    .unwrap_or(chrono::Duration::seconds(60));
                            if let Err(te) = storage
                                .conditional_update_claimed_instance_state(
                                    instance_id,
                                    claim_token,
                                    InstanceState::Running,
                                    InstanceState::Scheduled,
                                    Some(fire_at),
                                )
                                .await
                            {
                                error!(
                                    instance_id = %instance_id,
                                    error = %te,
                                    "failed to reschedule instance after transient storage error"
                                );
                            }
                        }
                        transient_retries::Decision::Exhausted { attempts } => {
                            crate::metrics::inc(crate::metrics::INSTANCE_TRANSIENT_RETRY_EXHAUSTED);
                            if mark_instance_failed(
                                &storage,
                                instance_id,
                                claim_token,
                                &e,
                                "instance kept failing with a transient-classified error; retries exhausted (transient_retry_exhausted)",
                                "failed to transition instance to Failed after transient retries were exhausted",
                            )
                            .await
                            {
                                record_transient_exhaustion(&storage, instance_id, attempts, &e).await;
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    mark_instance_failed(
                        &storage,
                        instance_id,
                        claim_token,
                        &e,
                        "instance processing failed",
                        "failed to transition instance to Failed after processing error",
                    )
                    .await;
                }
                Err(join_err) => {
                    mark_instance_failed(
                        &storage,
                        instance_id,
                        claim_token,
                        &join_err,
                        "instance processing panicked",
                        "failed to transition instance to Failed after panic",
                    )
                    .await;
                }
            }

            // Update tick counters if the caller requested them.
            if let Some(ref ctr) = counters {
                ctr.instances_advanced.fetch_add(1, Ordering::Relaxed);

                // Compute the number of steps executed by reading the final
                // `total_steps_executed` from storage and subtracting the
                // snapshot taken before processing.
                let final_steps = storage
                    .get_instance(instance_id)
                    .await
                    .ok()
                    .flatten()
                    .map_or(initial_steps, |i| i.context.runtime.total_steps_executed);
                let delta = final_steps.saturating_sub(initial_steps);
                if delta > 0 {
                    ctr.steps_executed.fetch_add(delta, Ordering::Relaxed);
                }
            }
        });

        join_handles.push(handle);
    }

    Ok(join_handles)
}

/// Per-instance backoff and cap for transient-error reschedules (L2-17).
///
/// In-process (per node, lost on restart): it bounds the hot loop on this
/// node and eventually lands a persistently failing instance in the DLQ,
/// without an extra storage write on a path that runs exactly when storage
/// is misbehaving.
mod transient_retries {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex, PoisonError};
    use std::time::{Duration, Instant};

    use orch8_types::ids::InstanceId;

    /// Delay before the first transient retry; doubles per consecutive failure.
    const BASE: Duration = Duration::from_millis(200);
    /// Cap on the delay between transient retries.
    const MAX: Duration = Duration::from_secs(60);
    /// Fail the instance once it has failed this many times in a row...
    pub(super) const MAX_ATTEMPTS: u32 = 30;
    /// ...and the first of those failures is at least this old, so a short
    /// database outage (where every instance fails transiently) never DLQs
    /// healthy work.
    pub(super) const MIN_AGE: Duration = Duration::from_secs(15 * 60);
    /// Entries older than this are dropped when the map grows large (the
    /// instance finished elsewhere or was cancelled).
    const STALE_AFTER: Duration = Duration::from_secs(2 * 3600);
    const PRUNE_AT: usize = 10_000;
    /// Hard bound: past this, the oldest entries are evicted even if younger
    /// than `STALE_AFTER` (a long outage fails every instance transiently).
    pub(super) const HARD_CAP: usize = 50_000;

    #[derive(Debug, PartialEq, Eq)]
    pub(super) enum Decision {
        Retry(Duration),
        Exhausted { attempts: u32 },
    }

    static STATE: LazyLock<Mutex<HashMap<InstanceId, (u32, Instant)>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// Pure policy: `attempts` consecutive transient failures, the first one
    /// `since_first` ago.
    pub(super) fn decide(attempts: u32, since_first: Duration) -> Decision {
        if attempts >= MAX_ATTEMPTS && since_first >= MIN_AGE {
            return Decision::Exhausted { attempts };
        }
        let exp = attempts.saturating_sub(1).min(16);
        let delay = BASE.saturating_mul(1u32 << exp).min(MAX);
        // 80-120% jitter so a batch that failed together does not retry in
        // lockstep.
        Decision::Retry(delay.mul_f64(rand::RngExt::random_range(&mut rand::rng(), 0.8..1.2)))
    }

    pub(super) fn record(id: InstanceId) -> Decision {
        let now = Instant::now();
        let mut map = STATE.lock().unwrap_or_else(PoisonError::into_inner);
        enforce_bounds(&mut map, now, PRUNE_AT, HARD_CAP);
        let entry = map.entry(id).or_insert((0, now));
        entry.0 = entry.0.saturating_add(1);
        let decision = decide(entry.0, now.duration_since(entry.1));
        if matches!(decision, Decision::Exhausted { .. }) {
            map.remove(&id);
        }
        decision
    }

    /// Prune stale entries at `prune_at`; if still at `hard_cap`, evict the
    /// oldest 10% (counted in a metric) so the map can never grow unbounded.
    pub(super) fn enforce_bounds(
        map: &mut HashMap<InstanceId, (u32, Instant)>,
        now: Instant,
        prune_at: usize,
        hard_cap: usize,
    ) {
        if map.len() >= prune_at {
            map.retain(|_, (_, first)| now.duration_since(*first) < STALE_AFTER);
        }
        if map.len() >= hard_cap {
            let mut by_age: Vec<(Instant, InstanceId)> =
                map.iter().map(|(id, (_, first))| (*first, *id)).collect();
            by_age.sort_unstable();
            let evict = (hard_cap / 10).max(1);
            for (_, id) in by_age.into_iter().take(evict) {
                map.remove(&id);
                crate::metrics::inc(crate::metrics::TRANSIENT_RETRY_ENTRIES_EVICTED);
            }
        }
    }

    pub(super) fn clear(id: InstanceId) {
        let mut map = STATE.lock().unwrap_or_else(PoisonError::into_inner);
        if !map.is_empty() {
            map.remove(&id);
        }
    }
}

/// Persist why the instance was failed after exhausting transient retries,
/// so the DLQ entry carries a distinct `transient_retry_exhausted` reason.
/// Best-effort: storage is, by definition, misbehaving here.
async fn record_transient_exhaustion(
    storage: &Arc<dyn StorageBackend>,
    instance_id: InstanceId,
    attempts: u32,
    error: &EngineError,
) {
    let output = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id,
        block_id: BlockId::new("_engine:transient_retry_exhausted"),
        output: serde_json::json!({
            "_error": "transient_retry_exhausted",
            "attempts": attempts,
            "last_error": error.to_string(),
        }),
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now(),
    };
    if let Err(e) = storage.save_block_output(&output).await {
        warn!(instance_id = %instance_id, error = %e, "failed to record transient-retry exhaustion");
    }
}

/// Return claimed instances to `Scheduled` without resurrecting a row that a
/// concurrent signal or operator already moved to another state.
async fn defer_running_instances(
    storage: &Arc<dyn StorageBackend>,
    claims: &[(InstanceId, chrono::DateTime<Utc>)],
    fire_at: chrono::DateTime<Utc>,
) -> Result<(), EngineError> {
    let mut first_error = None;
    for &(id, claim_token) in claims {
        if let Err(error) = storage
            .conditional_update_claimed_instance_state(
                id,
                claim_token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(fire_at),
            )
            .await
        {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), |error| Err(error.into()))
}

/// Best-effort rollback for failures that occur after a batch was claimed but
/// before per-instance processing tasks were spawned. Preserve the original
/// setup error while making every claimed row immediately eligible to retry.
async fn reschedule_claimed_after_setup_error(
    storage: &Arc<dyn StorageBackend>,
    claims: &[(InstanceId, chrono::DateTime<Utc>)],
    fire_at: chrono::DateTime<Utc>,
    phase: &'static str,
) {
    if let Err(error) = defer_running_instances(storage, claims, fire_at).await {
        error!(%error, phase, count = claims.len(), "failed to reschedule claimed instances after scheduler setup error");
    }
}

/// Shared failure path for the spawned-task failure arms in `process_tick`
/// (processing error and panic alike): bump the failure metric, log the
/// cause, and CAS the instance Running -> Failed as a safety net so it
/// doesn't stay in Running forever and get re-claimed every tick.
async fn mark_instance_failed(
    storage: &Arc<dyn StorageBackend>,
    instance_id: InstanceId,
    claim_token: chrono::DateTime<Utc>,
    cause: &(dyn std::fmt::Display + Send + Sync),
    cause_label: &'static str,
    transition_label: &'static str,
) -> bool {
    error!(
        instance_id = %instance_id,
        error = %cause,
        "{cause_label}"
    );
    match storage
        .conditional_update_claimed_instance_state(
            instance_id,
            claim_token,
            InstanceState::Running,
            InstanceState::Failed,
            None,
        )
        .await
    {
        Ok(true) => {
            crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
            true
        }
        Ok(false) => {
            debug!(instance_id = %instance_id, "failure transition skipped: claim no longer owns instance");
            false
        }
        Err(te) => {
            error!(instance_id = %instance_id, error = %te, "{transition_label}");
            false
        }
    }
}

/// Enforce per-concurrency-key limits on a batch of freshly claimed (Running)
/// instances. For each distinct `concurrency_key`, we count how many instances
/// with that key were *already* Running in the DB before this batch was claimed,
/// then keep only enough from the batch to stay within `max_concurrency`. Excess
/// instances are transitioned back to `Scheduled` with a short defer window so
/// they are retried on a future tick.
///
/// Returns the filtered list of instances that should proceed to processing.
async fn enforce_concurrency_limits(
    storage: &Arc<dyn StorageBackend>,
    instances: Vec<orch8_types::instance::TaskInstance>,
    clock: &SharedClock,
) -> Result<Vec<orch8_types::instance::TaskInstance>, EngineError> {
    // Collect concurrency keys present in the batch.
    // ⚡ Bolt: Use a flat Vec and `chunk_by` instead of a HashMap to avoid allocation
    // and hashing overhead on the hot scheduling path.
    // Keys are tenant-scoped: group by (tenant_id, concurrency_key).
    let mut key_instances: Vec<((&str, &str), usize)> = Vec::with_capacity(instances.len());
    for (idx, inst) in instances.iter().enumerate() {
        if let (Some(key), Some(_max)) = (&inst.concurrency_key, inst.max_concurrency) {
            key_instances.push(((inst.tenant_id.as_str(), key.as_str()), idx));
        }
    }

    if key_instances.is_empty() {
        return Ok(instances);
    }

    // Sort by key first, then by original index to preserve priority order.
    key_instances.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // Batch count running instances for all concurrency keys in a single query.
    let mut keys: Vec<(&str, &str)> = key_instances.iter().map(|(k, _)| *k).collect();
    keys.dedup();
    let running_counts = storage.count_running_by_concurrency_keys(&keys).await?;

    // For each concurrency key, determine how many slots are available.
    let mut deferred_indices = Vec::new();
    for chunk in key_instances.chunk_by(|a, b| a.0 == b.0) {
        let key = chunk[0].0;

        // All instances in the group share the same max_concurrency.
        let max = instances[chunk[0].1].max_concurrency.unwrap_or(u32::MAX);

        // Count how many instances with this key are currently Running in the
        // DB. This count includes the instances we just claimed (since
        // claim_due_instances already set them to Running). Subtract the batch
        // members to get the pre-existing running count.
        let total_running = running_counts
            .get(&(key.0.to_owned(), key.1.to_owned()))
            .copied()
            .unwrap_or(0);
        #[allow(clippy::cast_possible_wrap)]
        let batch_count = chunk.len() as i64;
        let already_running = total_running - batch_count;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let slots = i64::from(max).saturating_sub(already_running).max(0) as usize;

        // Keep the first `slots` instances (by batch order, which preserves
        // priority ordering from claim_due_instances), defer the rest.
        if slots < chunk.len() {
            for &(_, idx) in &chunk[slots..] {
                deferred_indices.push(idx);
            }
        }
    }

    if deferred_indices.is_empty() {
        return Ok(instances);
    }

    let defer_at = clock.now() + chrono::Duration::seconds(1);
    let mut deferred_claims = Vec::with_capacity(deferred_indices.len());
    for &idx in &deferred_indices {
        let inst = &instances[idx];
        debug!(
            instance_id = %inst.id,
            concurrency_key = %inst.concurrency_key.as_deref().unwrap_or(""),
            "concurrency limit exceeded at claim time, deferring"
        );
        deferred_claims.push((inst.id, inst.updated_at));
    }
    defer_running_instances(storage, &deferred_claims, defer_at).await?;

    // Order-preserving removal: `swap_remove` would pull tail elements into
    // the holes, scrambling the priority order established by
    // claim_due_instances (e.g. deferring indices [1,3] from [A,B,C,D,E]
    // would yield [A,E,C] instead of [A,C,E]).
    //
    // Performance: Sorting `deferred_indices` and using binary search avoids
    // the allocation and hashing overhead of building a HashSet for
    // exclusion checking on the execution hot path.
    deferred_indices.sort_unstable();
    deferred_indices.dedup();

    let mut i = 0;
    let mut instances = instances;
    instances.retain(|_| {
        let keep = deferred_indices.binary_search(&i).is_err();
        i += 1;
        keep
    });

    Ok(instances)
}

/// Signals that justify waking a `Scheduled` instance before its
/// `next_fire_at` (see [`process_signalled_instances`]).
fn wakes_scheduled_instance(signal_type: &orch8_types::signal::SignalType) -> bool {
    use orch8_types::signal::SignalType;
    matches!(signal_type, SignalType::Pause | SignalType::Cancel)
}

/// Process signals for instances in paused/waiting state.
///
/// `claim_due_instances` only picks up `scheduled` instances. Signals (resume,
/// cancel, `update_context`) enqueued against paused or waiting instances would
/// sit unprocessed indefinitely without this sweep. The function is cheap when
/// no such signals exist (single indexed query returning empty).
async fn process_signalled_instances(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    sequence_cache: &SequenceCache,
    batch_size: u32,
    clock: &SharedClock,
    hooks: &HookTasks,
) -> Result<(), EngineError> {
    let signalled = storage.get_signalled_instance_ids(batch_size).await?;
    if signalled.is_empty() {
        return Ok(());
    }

    debug!(
        count = signalled.len(),
        "processing signals for paused/waiting instances"
    );

    // Batch-fetch all pending signals in one query instead of N separate fetches.
    let instance_ids: Vec<_> = signalled.iter().map(|(id, _)| *id).collect();
    let mut all_signals = storage.get_pending_signals_batch(&instance_ids).await?;

    for (instance_id, current_state) in signalled {
        let signals = all_signals.remove(&instance_id).unwrap_or_default();
        if signals.is_empty() {
            continue;
        }
        // L2-11: one instance whose signals cannot be applied (undecodable
        // context or sequence, a failing write) used to abort the whole
        // sweep with `?` — every tick, at the head of the oldest-first batch
        // — so pauses, resumes, cancels and approvals of every instance
        // after it were never delivered. Isolate per-instance failures.
        if let Err(e) = process_one_signalled_instance(
            storage,
            handlers,
            sequence_cache,
            clock,
            hooks,
            instance_id,
            current_state,
            signals,
        )
        .await
        {
            warn!(
                instance_id = %instance_id,
                state = %current_state,
                error = %e,
                "signal sweep: failed to apply signals for instance; continuing sweep"
            );
        }
    }

    Ok(())
}

/// Apply the pending signals of one paused/waiting (or signalled scheduled)
/// instance for [`process_signalled_instances`].
#[allow(clippy::too_many_arguments)]
async fn process_one_signalled_instance(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    sequence_cache: &SequenceCache,
    clock: &SharedClock,
    hooks: &HookTasks,
    instance_id: InstanceId,
    current_state: InstanceState,
    signals: Vec<Signal>,
) -> Result<(), EngineError> {
    // Scheduled instances with a pending *control* signal just need a
    // wake-up so the normal tick processes it immediately. Skip the full
    // signal processor to avoid invalid state transitions.
    //
    // Only pause/cancel justify cutting a delay, send window, or retry
    // backoff short. Everything else (update_context, custom signals and
    // `human_input:<step>` responses for a step not reached yet) is
    // consumed when the instance next runs naturally — waking for those
    // re-armed a parked instance on every sweep, which together with a
    // step delay was a livelock (ENG-R-N1). Storage already filters
    // these rows; this is the backend-independent guard.
    if current_state == InstanceState::Scheduled {
        if !signals
            .iter()
            .any(|s| wakes_scheduled_instance(&s.signal_type))
        {
            return Ok(());
        }
        debug!(
            instance_id = %instance_id,
            "waking scheduled instance with pending signal"
        );
        if let Err(e) = storage
            .conditional_update_instance_state(
                instance_id,
                InstanceState::Scheduled,
                InstanceState::Scheduled,
                Some(clock.now()),
            )
            .await
        {
            warn!(
                instance_id = %instance_id,
                error = %e,
                "failed to re-arm signalled scheduled instance"
            );
        }
        return Ok(());
    }

    let abort = crate::signals::process_signals_prefetched(
        storage.as_ref(),
        instance_id,
        current_state,
        signals,
        None,
    )
    .await?;
    if abort {
        // Signal handling owns state transitions but deliberately has no
        // scheduler clock parameter. Normalize any wake-up here so a
        // virtual-time engine never inherits `Utc::now()` as its next
        // fire time and interprets an immediate signal as a months-long
        // delay.
        if let Some(inst) = storage.get_instance(instance_id).await?
            && inst.state == InstanceState::Scheduled
        {
            storage
                .conditional_update_instance_state(
                    instance_id,
                    InstanceState::Scheduled,
                    InstanceState::Scheduled,
                    Some(clock.now()),
                )
                .await?;
        }
        debug!(
            instance_id = %instance_id,
            from_state = %current_state,
            "signal processed for non-scheduled instance"
        );
        // A cancel signal may have just driven this instance terminal.
        // Run the sequence's best-effort `on_cancel` cleanup if so — this
        // covers the immediate (unscoped) cancel path that bypasses tree
        // evaluation. (Scoped cancellations complete through the evaluator,
        // whose terminal path runs the same hook.) The hooks run off the
        // tick (L2-06): a bulk cancel against a slow hook target used to
        // block scheduling for `instances × hooks × 30 s`.
        if current_state != InstanceState::Cancelled
            && let Ok(Some(inst)) = storage.get_instance(instance_id).await
            && inst.state == InstanceState::Cancelled
            && let Ok(seq) = sequence_cache
                .get_by_id(storage.as_ref(), inst.sequence_id)
                .await
        {
            // A sub-sequence parent parked on this child must observe the
            // cancel (the API no longer flips the state itself).
            wake_parent_if_child(storage.as_ref(), &inst).await;
            let storage = Arc::clone(storage);
            let handlers = Arc::clone(handlers);
            let tenant = inst.tenant_id.clone();
            hooks.spawn(tenant.as_str(), async move {
                // Sagas the cancel cut short owe their compensations (L3-11).
                crate::handlers::saga::compensate_aborted_sagas(
                    &storage,
                    &handlers,
                    &inst,
                    &seq,
                    "cancelled",
                )
                .await;
                if let Some(ref blocks) = seq.on_cancel {
                    run_cleanup_hooks(&storage, &handlers, &inst, blocks, "on_cancel").await;
                }
            });
        }
    }
    Ok(())
}

/// Max cleanup / escalation hooks the sweeps run concurrently off the tick
/// (L2-06), across all tenants. Each hook is still bounded by
/// [`HOOK_DISPATCH_TIMEOUT`].
const HOOK_CONCURRENCY: usize = 16;

/// Max hooks of one tenant running at once (D-05): a tenant whose hook
/// target hangs for [`HOOK_DISPATCH_TIMEOUT`] must not occupy every global
/// permit and starve the other tenants' cancel/escalation hooks.
const HOOK_PER_TENANT_CONCURRENCY: usize = 4;

/// Max hooks accepted but not yet finished (running + waiting for a permit).
/// Beyond this a new hook is dropped and counted in
/// `orch8_hooks_dropped_total` instead of queueing without bound (D-05).
const HOOK_QUEUE_CAPACITY: usize = 1024;

/// Tenant-semaphore map size above which idle entries are pruned.
const HOOK_TENANT_MAP_PRUNE_AT: usize = 1024;

struct HookPool {
    global: Arc<Semaphore>,
    tenants: std::sync::Mutex<HashMap<String, Arc<Semaphore>>>,
    /// Hooks spawned and not yet finished (queued or running).
    pending: Arc<std::sync::atomic::AtomicUsize>,
    tasks: std::sync::Mutex<tokio::task::JoinSet<()>>,
}

/// Decrements the pending count when the hook finishes or is aborted.
struct PendingGuard(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Cleanup / escalation hooks the sweeps spawn off the tick task.
///
/// One pool lives for the whole tick loop (cloning shares it). Hooks are
/// bounded three ways (D-05): at most [`HOOK_CONCURRENCY`] run at once,
/// at most [`HOOK_PER_TENANT_CONCURRENCY`] per tenant, and at most
/// [`HOOK_QUEUE_CAPACITY`] are queued or running (overflow is dropped and
/// counted). They are tracked in a `JoinSet` so shutdown can [`drain`]
/// them (bounded wait, then abort) and `tick_once` can [`join_all`].
///
/// [`drain`]: HookTasks::drain
/// [`join_all`]: HookTasks::join_all
#[derive(Clone)]
pub(crate) struct HookTasks(Arc<HookPool>);

impl Default for HookTasks {
    fn default() -> Self {
        Self(Arc::new(HookPool {
            global: Arc::new(Semaphore::new(HOOK_CONCURRENCY)),
            tenants: std::sync::Mutex::new(HashMap::new()),
            pending: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            tasks: std::sync::Mutex::new(tokio::task::JoinSet::new()),
        }))
    }
}

impl HookTasks {
    /// Spawn `hook` for `tenant`, or drop it when the queue is full.
    fn spawn<F>(&self, tenant: &str, hook: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let pool = &self.0;
        if pool.pending.fetch_add(1, Ordering::AcqRel) >= HOOK_QUEUE_CAPACITY {
            pool.pending.fetch_sub(1, Ordering::AcqRel);
            crate::metrics::inc(crate::metrics::HOOKS_DROPPED);
            warn!(
                tenant_id = tenant,
                capacity = HOOK_QUEUE_CAPACITY,
                "hook queue full; dropping cancel/escalation hook"
            );
            return;
        }
        let guard = PendingGuard(Arc::clone(&pool.pending));
        let tenant_sem = {
            let mut tenants = pool
                .tenants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if tenants.len() >= HOOK_TENANT_MAP_PRUNE_AT {
                tenants.retain(|_, sem| Arc::strong_count(sem) > 1);
            }
            Arc::clone(
                tenants
                    .entry(tenant.to_owned())
                    .or_insert_with(|| Arc::new(Semaphore::new(HOOK_PER_TENANT_CONCURRENCY))),
            )
        };
        let global = Arc::clone(&pool.global);
        let mut tasks = pool
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Reap finished hooks so the set does not grow with the process.
        while tasks.try_join_next().is_some() {}
        tasks.spawn(async move {
            let _guard = guard;
            // Tenant permit first: a tenant's excess hooks wait here without
            // holding a global permit. The semaphores are never closed.
            let _tenant_permit = tenant_sem.acquire_owned().await;
            let _permit = global.acquire_owned().await;
            hook.await;
        });
    }

    /// Hooks accepted and not yet finished.
    #[cfg(test)]
    pub(crate) fn pending(&self) -> usize {
        self.0.pending.load(Ordering::Acquire)
    }

    /// Wait up to `wait` for every spawned hook, then abort the rest.
    /// Returns `true` when all finished in time.
    async fn drain(&self, wait: Duration) -> bool {
        let mut set = std::mem::take(
            &mut *self
                .0
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let finished =
            tokio::time::timeout(wait, async { while set.join_next().await.is_some() {} })
                .await
                .is_ok();
        if !finished {
            set.abort_all();
        }
        finished
    }

    /// Wait for every spawned hook (single-shot callers).
    async fn join_all(self) {
        let mut set = std::mem::take(
            &mut *self
                .0
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        while set.join_next().await.is_some() {}
    }
}

/// Max pages of Waiting instances swept for deadlines/timeouts per tick. Bounds
/// per-tick work; the rotating [`WaitingSweepCursor`] carries the position
/// across ticks so a backlog larger than this still gets fully covered.
const MAX_SWEEP_PAGES: u32 = 64;

/// Upper bound on the cursor's sequence -> has-nested-timers memo.
const WAITING_SWEEP_MEMO_MAX: usize = 4096;

/// Rotating-scan state for the Waiting deadline/timeout sweep, carried across
/// ticks by the tick loop (`tick_once` uses a fresh one and scans everything).
///
/// - `offset`: where the next tick resumes paging the Waiting set. The sweep
///   used to restart at offset 0 every tick and stop after
///   [`MAX_SWEEP_PAGES`], so instances sorted past `64 × batch_size` never
///   had their deadlines or `wait_for_input` timeouts checked (L6-04).
/// - `nested_timers`: memo of `sequence_id` -> "has a step *nested* in a
///   composite that declares a `deadline` or `wait_for_input.timeout`"
///   (immutable per sequence id), so only those instances pay for an
///   execution-tree read (L3-02).
#[derive(Default)]
pub(crate) struct WaitingSweepCursor {
    offset: u64,
    nested_timers: HashMap<orch8_types::ids::SequenceId, bool>,
}

impl WaitingSweepCursor {
    fn has_nested_timers(&mut self, seq: &SequenceDefinition) -> bool {
        if let Some(known) = self.nested_timers.get(&seq.id) {
            return *known;
        }
        let has = sequence_has_nested_timers(seq);
        if self.nested_timers.len() >= WAITING_SWEEP_MEMO_MAX {
            self.nested_timers.clear();
        }
        self.nested_timers.insert(seq.id, has);
        has
    }
}

/// `true` when a step nested inside a composite block declares a `deadline`
/// or a `wait_for_input.timeout`. Top-level steps are covered by
/// [`check_waiting_instance`]'s root scan.
fn sequence_has_nested_timers(seq: &SequenceDefinition) -> bool {
    crate::evaluator::flatten_blocks(&seq.blocks)
        .iter()
        .any(|(id, block)| {
            matches!(block, orch8_types::sequence::BlockDefinition::Step(step)
                if step_has_timer(step))
                && !seq.blocks.iter().any(|root| {
                    matches!(root, orch8_types::sequence::BlockDefinition::Step(r) if &r.id == *id)
                })
        })
}

fn step_has_timer(step: &orch8_types::sequence::StepDef) -> bool {
    step.deadline.is_some()
        || step
            .wait_for_input
            .as_ref()
            .is_some_and(|h| h.timeout.is_some())
}

/// Batch-fetch the deadline block outputs for `keys` in a single query. An
/// empty key set short-circuits to an empty map without touching storage.
async fn prefetch_deadline_outputs(
    storage: &Arc<dyn StorageBackend>,
    keys: &[(InstanceId, &BlockId)],
) -> Result<HashMap<(InstanceId, BlockId), BlockOutput>, EngineError> {
    if keys.is_empty() {
        Ok(HashMap::new())
    } else {
        Ok(storage.get_block_outputs_batch(keys).await?)
    }
}

/// Borrowed-ref lookup view over a batch of prefetched deadline outputs,
/// keyed by (instance, block). Querying through borrowed keys avoids an
/// unnecessary `.clone()` on the `BlockId` (String allocation) for every
/// deadline step inside the hot scheduler tick loops.
struct DeadlineOutputs<'a> {
    refs: Vec<(&'a InstanceId, &'a BlockId, &'a BlockOutput)>,
}

impl<'a> DeadlineOutputs<'a> {
    fn new(outputs: &'a HashMap<(InstanceId, BlockId), BlockOutput>) -> Self {
        // ⚡ Bolt: We build a flat Vec sorted by the composite key instead of allocating
        // a HashMap. This avoids hashing and map-building overhead on the hot path
        // while still providing O(log N) zero-allocation lookups.
        let mut refs = Vec::with_capacity(outputs.len());
        for (k, v) in outputs {
            refs.push((&k.0, &k.1, v));
        }
        refs.sort_unstable_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
        Self { refs }
    }

    fn get(&self, instance: &InstanceId, block: &BlockId) -> Option<&BlockOutput> {
        self.refs
            .binary_search_by(|(i, b, _)| i.cmp(&instance).then_with(|| b.cmp(&block)))
            .ok()
            .map(|idx| self.refs[idx].2)
    }
}

/// Check SLA deadlines for instances stuck in `Waiting` state (dispatched to
/// external workers that never completed). Runs every tick with a bounded
/// query to avoid overhead when no deadlines are configured.
async fn process_waiting_deadlines(
    ctx: &SweepContext<'_>,
    cursor: &mut WaitingSweepCursor,
    max_pages: u32,
) -> Result<(), EngineError> {
    use orch8_types::filter::{InstanceFilter, Pagination};

    let SweepContext {
        storage,
        sequence_cache,
        batch_size,
        ..
    } = *ctx;

    let filter = InstanceFilter {
        states: Some(vec![InstanceState::Waiting]),
        ..Default::default()
    };
    // Bounded pagination: sweep up to `MAX_SWEEP_PAGES` pages of Waiting
    // instances so per-step deadlines and `wait_for_input` timeouts still fire
    // when the waiting backlog exceeds a single batch. Capping at the first
    // `batch_size` rows (the old behaviour) permanently starved any due
    // instance sorted past position `batch_size` — its timeout never fired.
    // Woken instances leave the Waiting set mid-sweep, so an offset window can
    // skip a few after wakes; those are picked up on the next rotation
    // (self-correcting, and this sweep runs every tick). The window resumes
    // from `cursor.offset` and wraps to 0 at the end of the set.
    let mut offset: u64 = cursor.offset;
    let mut wrapped = false;
    for _page in 0..max_pages {
        let pagination = Pagination {
            offset,
            limit: batch_size,
            sort_ascending: true,
        };
        let waiting = storage.list_instances(&filter, &pagination).await?;
        let page_len = waiting.len();
        if waiting.is_empty() {
            // Idle / drained — skip the per-instance sequence-cache walk.
            wrapped = true;
            break;
        }

        // Build a map from instance -> sequence so we can collect all deadline
        // block IDs before issuing a single batch query.
        let mut instance_sequences = Vec::with_capacity(waiting.len());
        for instance in &waiting {
            let Ok(seq) = sequence_cache
                .get_by_id(storage.as_ref(), instance.sequence_id)
                .await
            else {
                continue;
            };
            instance_sequences.push((instance, seq));
        }

        // Collect all (instance_id, block_id) pairs that have a deadline.
        let mut deadline_keys = Vec::new();
        for (instance, seq) in &instance_sequences {
            for block in &seq.blocks {
                if let orch8_types::sequence::BlockDefinition::Step(step_def) = block
                    && step_def.deadline.is_some()
                {
                    deadline_keys.push((instance.id, &step_def.id));
                }
            }
        }
        let deadline_outputs = prefetch_deadline_outputs(storage, &deadline_keys).await?;
        let deadline_outputs_ref = DeadlineOutputs::new(&deadline_outputs);
        // Steps that already completed can no longer breach their deadline
        // (mirrors `check_fast_path_deadlines`): without this, an instance
        // Waiting on step 3 was failed for step 1's long-finished deadline.
        let mut completed = if deadline_keys.is_empty() {
            HashMap::new()
        } else {
            let mut ids: Vec<InstanceId> = deadline_keys.iter().map(|(id, _)| *id).collect();
            ids.dedup();
            storage.get_completed_block_ids_batch(&ids).await?
        };

        for (instance, seq) in instance_sequences {
            let completed_ids = completed.remove(&instance.id).unwrap_or_default();
            let nested_timers = cursor.has_nested_timers(&seq);
            // Per-instance failures (a lost CAS, a transient write error) must
            // not abort the sweep for every other Waiting instance.
            if let Err(e) = check_waiting_instance(
                ctx,
                instance,
                &seq,
                &deadline_outputs_ref,
                &completed_ids,
                nested_timers,
            )
            .await
            {
                warn!(
                    instance_id = %instance.id,
                    error = %e,
                    "waiting-deadline sweep: instance check failed; continuing sweep"
                );
            }
        }

        // A short page means we reached the end of the Waiting set; otherwise
        // advance the window to the next page. `max_pages` bounds the
        // per-tick work so a pathological backlog can't stall the tick loop.
        if u32::try_from(page_len).unwrap_or(u32::MAX) < batch_size {
            wrapped = true;
            break;
        }
        offset += u64::from(batch_size);
    }
    cursor.offset = if wrapped { 0 } else { offset };
    Ok(())
}

/// Check one Waiting instance's steps: fail it on a breached SLA deadline, or
/// wake it when the `wait_for_input` timeout of the step it is actually
/// waiting on has expired. Stops at the first triggering step.
async fn check_waiting_instance(
    ctx: &SweepContext<'_>,
    instance: &orch8_types::instance::TaskInstance,
    seq: &SequenceDefinition,
    deadline_outputs_ref: &DeadlineOutputs<'_>,
    completed_block_ids: &[BlockId],
    nested_timers: bool,
) -> Result<(), EngineError> {
    for block in &seq.blocks {
        if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
            if step_def.deadline.is_some() && !completed_block_ids.contains(&step_def.id) {
                let prev = deadline_outputs_ref.get(&instance.id, &step_def.id);
                if check_step_deadline_waiting(
                    ctx.storage,
                    ctx.handlers,
                    instance,
                    step_def,
                    ctx.webhook_config,
                    ctx.cancel,
                    prev,
                    ctx.clock,
                    ctx.hooks,
                )
                .await?
                {
                    return Ok(()); // Instance was failed — no need to check more steps.
                }
            }

            // Check wait_for_input timeout for human-review steps.
            // The flat-path scheduler transitions to Waiting but never
            // re-polls timeout — we must wake the instance so the next
            // tick's check_human_input fires the escalation/failure path.
            if let Some(human_def) = &step_def.wait_for_input
                && let Some(timeout) = human_def.timeout
            {
                // Only the step the instance is actually waiting on may
                // wake it. Without this guard, a sequence with several
                // wait_for_input steps would compare an earlier step's
                // timeout against the current step's start time.
                if let Some(current) = &instance.context.runtime.current_step
                    && current != &step_def.id
                {
                    continue;
                }
                let baseline = instance
                    .context
                    .runtime
                    .current_step_started_at
                    .or(instance.context.runtime.started_at);
                if let Some(started) = baseline {
                    let elapsed = ctx.clock.now() - started;
                    if elapsed
                        > chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::days(365))
                    {
                        debug!(
                            instance_id = %instance.id,
                            block_id = %step_def.id,
                            "wait_for_input timeout expired, waking instance"
                        );
                        reactivate_waiting_gate_node(ctx.storage, instance.id, &step_def.id)
                            .await?;
                        crate::lifecycle::transition_instance(
                            ctx.storage.as_ref(),
                            instance.id,
                            Some(&instance.tenant_id),
                            InstanceState::Waiting,
                            InstanceState::Scheduled,
                            Some(ctx.clock.now()),
                        )
                        .await?;
                        return Ok(());
                    }
                }
            }
        }
    }
    if nested_timers {
        wake_on_expired_nested_timer(ctx, instance, seq).await?;
    }
    Ok(())
}

/// L3-02: steps nested in a composite (`parallel`, `try_catch`, `loop`, a
/// saga action, ...) park their *node* `Waiting`; their `deadline` and
/// `wait_for_input.timeout` were only enforced inside `evaluate`, which never
/// runs while the instance itself is `Waiting`. Walk the execution tree and,
/// when a live node's timer (measured from the node's own `started_at`, the
/// same baseline `evaluate` uses) has expired, wake the instance so the
/// evaluator's own SLA / gate-timeout handling fires on the next claim.
async fn wake_on_expired_nested_timer(
    ctx: &SweepContext<'_>,
    instance: &orch8_types::instance::TaskInstance,
    seq: &SequenceDefinition,
) -> Result<(), EngineError> {
    use orch8_types::execution::NodeState;
    use orch8_types::sequence::BlockDefinition;

    let tree = ctx.storage.get_execution_tree(instance.id).await?;
    let blocks = crate::evaluator::flatten_blocks(&seq.blocks);
    let now = ctx.clock.now();
    let exceeds = |started: chrono::DateTime<Utc>, limit: std::time::Duration| {
        now - started >= chrono::Duration::from_std(limit).unwrap_or(chrono::TimeDelta::MAX)
    };
    let mut deadline_breached = false;
    let mut timed_out_gates = Vec::new();
    for node in &tree {
        if !matches!(node.state, NodeState::Running | NodeState::Waiting) {
            continue;
        }
        let Some(started) = node.started_at else {
            continue;
        };
        let Ok(idx) = blocks.binary_search_by_key(&&node.block_id, |(id, _)| *id) else {
            continue;
        };
        let BlockDefinition::Step(step) = blocks[idx].1 else {
            continue;
        };
        // `evaluate`'s SLA check fails Running *and* Waiting nodes itself.
        if step.deadline.is_some_and(|d| exceeds(started, d)) {
            deadline_breached = true;
        }
        // The gate check (strict `>`) only runs when the node is re-dispatched,
        // and the evaluator re-dispatches Running step nodes only.
        if node.state == NodeState::Waiting
            && let Some(timeout) = step.wait_for_input.as_ref().and_then(|h| h.timeout)
            && now - started > chrono::Duration::from_std(timeout).unwrap_or(chrono::TimeDelta::MAX)
        {
            timed_out_gates.push(node.id);
        }
    }
    if !deadline_breached && timed_out_gates.is_empty() {
        return Ok(());
    }
    debug!(
        instance_id = %instance.id,
        deadline_breached,
        timed_out_gates = timed_out_gates.len(),
        "nested step deadline/timeout expired while Waiting, waking instance"
    );
    if !timed_out_gates.is_empty() {
        // Same hand-off as a `human_input:` answer (`signals.rs`): flip the
        // gate node back to Running so the evaluator re-runs its gate check,
        // which now takes the timeout / escalation path.
        ctx.storage
            .update_nodes_state(&timed_out_gates, NodeState::Running)
            .await?;
    }
    crate::lifecycle::transition_instance(
        ctx.storage.as_ref(),
        instance.id,
        Some(&instance.tenant_id),
        InstanceState::Waiting,
        InstanceState::Scheduled,
        Some(now),
    )
    .await?;
    Ok(())
}

/// Tree-path counterpart of the root gate-timeout wake: flip `block_id`'s
/// `Waiting` gate node back to Running so the woken instance re-runs the gate
/// check. Without it the evaluator (which only re-dispatches Running step
/// nodes) re-parked the instance and the sweep woke it again every tick. A
/// no-op for flat-path instances, which have no execution tree.
async fn reactivate_waiting_gate_node(
    storage: &Arc<dyn StorageBackend>,
    instance_id: InstanceId,
    block_id: &BlockId,
) -> Result<(), EngineError> {
    use orch8_types::execution::NodeState;
    let nodes: Vec<_> = storage
        .get_execution_tree(instance_id)
        .await?
        .into_iter()
        .filter(|n| &n.block_id == block_id && n.state == NodeState::Waiting)
        .map(|n| n.id)
        .collect();
    if !nodes.is_empty() {
        storage
            .update_nodes_state(&nodes, NodeState::Running)
            .await?;
    }
    Ok(())
}

/// Max wall-clock time a terminal-cleanup or deadline-escalation hook may run
/// before it is abandoned. These hooks are dispatched **inline on the scheduler
/// tick** (the `on_cancel` signal path in [`run_cleanup_hooks`] and the
/// `on_deadline_breach` sweep in [`check_step_deadline_waiting`]), so a hook
/// that blocks — e.g. a hung HTTP notify — would otherwise stall ALL scheduling
/// engine-wide. Bounding each hook caps that blast radius; the hook result is
/// best-effort and already swallowed on error.
const HOOK_DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Run a hook handler future on its own task, bounded by
/// [`HOOK_DISPATCH_TIMEOUT`]. An elapsed timeout or a **panic** is mapped to a
/// `Permanent` error so callers treat it exactly like any other swallowed
/// hook failure. The separate task is what isolates panics: these hooks run
/// tenant-authored handler code on scheduler-owned tasks, and an unwinding
/// panic there used to kill the whole tick loop (L2-01).
///
/// `pub(crate)`: shared with the tree-path SLA escalation dispatch
/// (`evaluator::sla`), which runs inline in the per-instance evaluate loop and
/// needs the same bound.
pub(crate) async fn dispatch_hook_bounded<F>(
    fut: F,
) -> Result<serde_json::Value, orch8_types::error::StepError>
where
    F: std::future::Future<Output = Result<serde_json::Value, orch8_types::error::StepError>>
        + Send
        + 'static,
{
    let mut task = tokio::spawn(fut);
    match tokio::time::timeout(HOOK_DISPATCH_TIMEOUT, &mut task).await {
        Ok(Ok(r)) => r,
        Ok(Err(join_err)) => Err(orch8_types::error::StepError::Permanent {
            message: format!("cleanup/escalation hook panicked: {join_err}"),
            details: None,
        }),
        Err(_) => {
            task.abort();
            Err(orch8_types::error::StepError::Permanent {
                message: "cleanup/escalation hook exceeded dispatch timeout".to_string(),
                details: None,
            })
        }
    }
}

/// Run a sequence's best-effort cleanup hook (`on_failure` / `on_cancel`) as
/// the instance reaches its terminal state. Each top-level `Step` block is
/// dispatched once with the instance's final context; its output is recorded
/// under the step's block id. Errors are **swallowed** — the instance is
/// already terminal and cleanup must never resurrect, wedge, or re-fail it.
/// Non-step blocks are skipped with a warning (v1 supports step hooks only).
async fn run_cleanup_hooks(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    blocks: &[orch8_types::sequence::BlockDefinition],
    hook: &'static str,
) {
    use orch8_types::sequence::BlockDefinition;
    for block in blocks {
        let BlockDefinition::Step(step) = block else {
            warn!(
                instance_id = %instance.id,
                hook,
                "cleanup hook skipped a non-step block (v1 runs step blocks only)"
            );
            continue;
        };
        let step_ctx = crate::handlers::StepContext {
            claim_token: None,
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step.id.clone(),
            params: step.params.clone(),
            context: Arc::new(instance.context.clone()),
            attempt: 0,
            storage: Arc::clone(storage),
            wait_for_input: None,
        };
        let Some(result) =
            dispatch_cleanup_step(storage, handlers, instance, step, hook, step_ctx).await
        else {
            continue;
        };
        record_cleanup_output(storage, instance, step, hook, result).await;
    }
}

/// Dispatch a single cleanup-hook step to its handler (builtin registry,
/// `ActivePieces`, gRPC, or wasm plugin), bounded by [`HOOK_DISPATCH_TIMEOUT`]
/// so a hung cleanup hook cannot stall the tick loop it runs on. Returns
/// `None` when the step must be skipped — the skip warning is already logged.
pub(crate) async fn dispatch_cleanup_step(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step: &orch8_types::sequence::StepDef,
    hook: &'static str,
    ctx: crate::handlers::StepContext,
) -> Option<Result<serde_json::Value, orch8_types::error::StepError>> {
    // Sub-tenant sandbox: cleanup hooks of an embedded end customer's
    // sequence run only allowlisted handlers.
    match crate::embed_sandbox::deny_handler(
        storage.as_ref(),
        instance,
        handlers.embed_sandbox(),
        step.id.as_str(),
        &step.handler,
    )
    .await
    {
        Ok(None) => {}
        Ok(Some(reason)) => {
            warn!(instance_id = %instance.id, hook, reason = %reason, "embed sandbox denied cleanup hook step");
            return Some(Err(orch8_types::error::StepError::Permanent {
                message: reason,
                details: None,
            }));
        }
        Err(e) => {
            warn!(instance_id = %instance.id, hook, error = %e, "embed sandbox lookup failed; skipping cleanup hook step");
            return None;
        }
    }
    let plugin_kind = crate::handlers::PluginKind::detect(&step.handler);
    let result = if let Some(handler) = handlers.get(&step.handler) {
        dispatch_hook_bounded(handler(ctx)).await
    } else if plugin_kind == Some(crate::handlers::PluginKind::ActivePieces) {
        let handler_name = step.handler.clone();
        dispatch_hook_bounded(async move {
            crate::handlers::activepieces::handle_ap(ctx, &handler_name).await
        })
        .await
    } else if plugin_kind == Some(crate::handlers::PluginKind::Grpc) {
        let Some(endpoint) = crate::handlers::step_dispatch::resolve_plugin_source(
            storage.as_ref(),
            &instance.tenant_id,
            &step.handler,
            orch8_types::plugin::PluginType::Grpc,
        )
        .await
        else {
            warn!(
                instance_id = %instance.id,
                hook,
                handler = %step.handler,
                "cleanup gRPC plugin not registered — skipping"
            );
            return None;
        };
        let mut ctx = ctx;
        ctx.params["_grpc_endpoint"] = serde_json::Value::String(endpoint);
        dispatch_hook_bounded(crate::handlers::grpc_plugin::handle_grpc_plugin(ctx)).await
    } else if let Some(plugin_name) = (plugin_kind == Some(crate::handlers::PluginKind::Wasm))
        .then(|| crate::handlers::wasm_plugin::parse_plugin_name(&step.handler))
        .flatten()
    {
        let Some(wasm_path) = crate::handlers::step_dispatch::resolve_plugin_source(
            storage.as_ref(),
            &instance.tenant_id,
            plugin_name,
            orch8_types::plugin::PluginType::Wasm,
        )
        .await
        else {
            warn!(
                instance_id = %instance.id,
                hook,
                plugin = %plugin_name,
                "cleanup wasm plugin not registered — skipping"
            );
            return None;
        };
        dispatch_hook_bounded(async move {
            crate::handlers::wasm_plugin::handle_wasm_plugin(ctx, &wasm_path).await
        })
        .await
    } else {
        warn!(
            instance_id = %instance.id,
            hook,
            handler = %step.handler,
            "cleanup handler not registered — skipping"
        );
        return None;
    };
    Some(result)
}

/// Record the outcome of a cleanup-hook step: persist a successful output
/// under the step's block id, or log the failure. Both error paths are
/// swallowed — the instance is already terminal.
pub(crate) async fn record_cleanup_output(
    storage: &Arc<dyn StorageBackend>,
    instance: &orch8_types::instance::TaskInstance,
    step: &orch8_types::sequence::StepDef,
    hook: &'static str,
    result: Result<serde_json::Value, orch8_types::error::StepError>,
) {
    match result {
        Ok(output) => {
            let output_size = u32::try_from(serde_json::to_vec(&output).map_or(0, |v| v.len()))
                .unwrap_or(u32::MAX);
            let bo = orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: instance.id,
                block_id: step.id.clone(),
                output,
                output_ref: None,
                output_size,
                attempt: 0,
                created_at: Utc::now(),
            };
            if let Err(e) = storage.save_block_output(&bo).await {
                warn!(
                    instance_id = %instance.id,
                    hook,
                    error = %e,
                    "failed to persist cleanup hook output (swallowed)"
                );
            }
        }
        Err(e) => {
            warn!(
                instance_id = %instance.id,
                hook,
                block_id = %step.id,
                error = %e,
                "cleanup hook step failed (swallowed)"
            );
        }
    }
}

/// Max pages of active instances the SLA sweep scans per tick. Bounds
/// per-tick work for the tick loop; the rotating [`SlaSweepCursor`] gives
/// full coverage over successive ticks regardless of active-set size.
const SLA_SWEEP_MAX_PAGES_PER_TICK: u32 = 4;

/// Ticks the sweep skips after a full rotation found no SLA-bearing sequence
/// before re-scanning once (≈30s at the default 100ms tick). Bounds how long
/// a newly registered SLA sequence can go unnoticed while keeping the
/// no-SLA-anywhere steady state free of instance queries.
const SLA_SWEEP_IDLE_RESCAN_TICKS: u32 = 300;

/// Upper bound on the cursor's sequence → has-SLA memo. Reached only on
/// version-heavy deployments; clearing just relearns lazily from the
/// sequence cache.
const SLA_SWEEP_MEMO_MAX: usize = 4096;

/// Bundles the per-tick sweep dependencies shared by the Waiting-deadline
/// sweep and its per-instance check helper. This replaces the long parameter
/// list on `process_waiting_deadlines`, mirroring the [`TickContext`] pattern
/// used for tick-level parameters.
struct SweepContext<'a> {
    pub storage: &'a Arc<dyn StorageBackend>,
    pub handlers: &'a HandlerRegistry,
    pub sequence_cache: &'a SequenceCache,
    pub webhook_config: &'a WebhookConfig,
    pub cancel: &'a CancellationToken,
    pub batch_size: u32,
    pub clock: &'a SharedClock,
    /// Where escalation hooks fired by the sweep are spawned (L2-06).
    pub hooks: &'a HookTasks,
}

/// Same idea as [`SweepContext`] but for the alert-only SLA sweep, which
/// never dispatches handlers. Kept separate so `process_sla_breaches` can
/// retain its original signature (called directly by `scheduler/tests.rs`).
struct SlaSweepContext<'a> {
    pub storage: &'a Arc<dyn StorageBackend>,
    pub sequence_cache: &'a SequenceCache,
    pub webhook_config: &'a WebhookConfig,
}

/// A pending SLA alert: which instance, what kind, the sentinel block id used
/// for once-only de-dup, and the timing facts for the payload.
struct SlaCandidate {
    instance_id: InstanceId,
    tenant_id: orch8_types::ids::TenantId,
    sequence_id: orch8_types::ids::SequenceId,
    kind: &'static str,
    block_id: BlockId,
    elapsed_ms: i64,
    limit_ms: u64,
    step_id: Option<String>,
}

/// Rotating-scan state for the SLA breach sweep, carried across ticks by the
/// tick loop. (`tick_once` builds a fresh one per call — see its call site.)
///
/// - `offset`: rotating page offset into the active-instance list
///   (`ORDER BY updated_at ASC`). The sweep scans a bounded number of pages
///   per tick starting here, so successive ticks cover the whole active
///   set — the old fixed first-page window never saw instances sorted past
///   `batch_size`, and the lease heartbeat (which touches `updated_at` for
///   in-flight instances) actively pushes the longest-running ones out of
///   it, so their `max_runtime` / `max_step_runtime` breaches never fired.
///   Heartbeats reorder the list mid-rotation, making coverage statistical
///   rather than exact; alert-only detection tolerates a tick or two of
///   latency, and the once-only sentinel makes a re-scan harmless.
/// - `sla_by_sequence`: memo of `sequence_id` → declares an SLA. SLA is
///   immutable per sequence id (a changed SLA ships as a new version under a
///   new id; `create_sequence` is insert-only), so entries never go stale.
/// - `idle_ticks_remaining`: when a full rotation observes no SLA-bearing
///   sequence at all, the sweep skips the instance query for this many
///   ticks before re-scanning once to discover newly registered SLA
///   sequences. Once any SLA sequence is seen the sweep scans every tick —
///   telling "sequence deleted" apart from "no instances right now" would
///   need refcounting, and the memo keeps each scan cheap.
#[derive(Default)]
pub(crate) struct SlaSweepCursor {
    offset: u64,
    sla_by_sequence: HashMap<orch8_types::ids::SequenceId, bool>,
    idle_ticks_remaining: u32,
}

/// Alert-only SLA sweep. For every non-terminal instance whose sequence
/// declares an `sla` policy, emit an `instance.sla_breached` webhook and
/// increment `orch8_sla_breached_total` when the instance's wall-clock lifetime
/// exceeds `max_runtime`, or the current step's runtime exceeds
/// `max_step_runtime`. Each breach alerts exactly once — a sentinel block
/// output per breach kind de-duplicates across ticks. The instance is never
/// failed or paused (alert-only).
///
/// Pages through the active set via `cursor` (see its docs) instead of
/// re-reading the first page every tick, and skips the query entirely while
/// idling on a known-SLA-free deployment.
///
/// Note on projection: `list_instances` always returns full `TaskInstance`
/// rows (context JSON included). A projected variant would cut the per-page
/// deserialization cost, but the trait has none and adding one just for this
/// sweep would contort every backend — the idle-skip and memo keep the
/// steady-state cost low without it.
#[allow(clippy::too_many_arguments)]
async fn process_sla_breaches(
    storage: &Arc<dyn StorageBackend>,
    sequence_cache: &SequenceCache,
    webhook_config: &WebhookConfig,
    // Unused since alerts go through the durable outbox (no in-line
    // delivery to cancel); kept so the sweep's call shape stays uniform.
    _cancel: &CancellationToken,
    batch_size: u32,
    clock: &SharedClock,
    cursor: &mut SlaSweepCursor,
    max_pages_per_tick: u32,
) -> Result<(), EngineError> {
    use orch8_types::filter::Pagination;

    let ctx = SlaSweepContext {
        storage,
        sequence_cache,
        webhook_config,
    };

    // Idle skip: the last full rotation found no SLA-bearing sequence, so
    // nothing can breach. Bounded — see `SlaSweepCursor` docs.
    if cursor.idle_ticks_remaining > 0 && !cursor.sla_by_sequence.values().any(|known| *known) {
        cursor.idle_ticks_remaining -= 1;
        return Ok(());
    }

    let filter = InstanceFilter {
        states: Some(vec![
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
        ]),
        ..Default::default()
    };

    let now = clock.now();
    let mut rotation_complete = false;

    for _page in 0..max_pages_per_tick {
        let pagination = Pagination {
            offset: cursor.offset,
            limit: batch_size,
            sort_ascending: true,
        };
        let page = storage.list_instances(&filter, &pagination).await?;
        let page_len = page.len();
        if page_len == 0 {
            cursor.offset = 0;
            rotation_complete = true;
            break;
        }

        let candidates = collect_page_sla_candidates(&ctx, cursor, &page, now).await;
        emit_sla_alerts(&ctx, &candidates, now).await?;

        if page_len < usize::try_from(batch_size).unwrap_or(usize::MAX) {
            // Short page — end of the active set; restart from the front on
            // the next tick.
            cursor.offset = 0;
            rotation_complete = true;
            break;
        }
        cursor.offset += page_len as u64;
    }

    if rotation_complete && !cursor.sla_by_sequence.values().any(|known| *known) {
        cursor.idle_ticks_remaining = SLA_SWEEP_IDLE_RESCAN_TICKS;
    }

    Ok(())
}

/// Scan one page of active instances for SLA breaches, returning a candidate
/// per breach kind. Learns the `sequence_id` → has-SLA memo in `cursor` as a
/// side effect (sequences known to carry no SLA skip the cache lookup).
async fn collect_page_sla_candidates(
    ctx: &SlaSweepContext<'_>,
    cursor: &mut SlaSweepCursor,
    page: &[orch8_types::instance::TaskInstance],
    now: chrono::DateTime<Utc>,
) -> Vec<SlaCandidate> {
    let mut candidates: Vec<SlaCandidate> = Vec::new();
    for instance in page {
        // Pre-filter on the memo: instances whose sequence is already
        // known to carry no SLA are skipped without a sequence lookup.
        if matches!(
            cursor.sla_by_sequence.get(&instance.sequence_id),
            Some(false)
        ) {
            continue;
        }
        let Ok(seq) = ctx
            .sequence_cache
            .get_by_id(ctx.storage.as_ref(), instance.sequence_id)
            .await
        else {
            continue;
        };
        if !cursor.sla_by_sequence.contains_key(&instance.sequence_id) {
            if cursor.sla_by_sequence.len() >= SLA_SWEEP_MEMO_MAX {
                cursor.sla_by_sequence.clear();
            }
            cursor
                .sla_by_sequence
                .insert(instance.sequence_id, seq.sla.is_some());
        }
        let Some(sla) = seq.sla.as_ref() else {
            continue;
        };

        if let Some(max_runtime) = sla.max_runtime {
            let elapsed = now - instance.created_at;
            let limit = chrono::Duration::from_std(max_runtime).unwrap_or(chrono::TimeDelta::MAX);
            if elapsed > limit {
                candidates.push(SlaCandidate {
                    instance_id: instance.id,
                    tenant_id: instance.tenant_id.clone(),
                    sequence_id: instance.sequence_id,
                    kind: "max_runtime",
                    block_id: BlockId::new("_sla:runtime"),
                    elapsed_ms: elapsed.num_milliseconds(),
                    limit_ms: u64::try_from(max_runtime.as_millis()).unwrap_or(u64::MAX),
                    step_id: None,
                });
            }
        }

        if let Some(max_step) = sla.max_step_runtime
            && let (Some(step), Some(started)) = (
                instance.context.runtime.current_step.as_ref(),
                instance.context.runtime.current_step_started_at,
            )
        {
            let elapsed = now - started;
            let limit = chrono::Duration::from_std(max_step).unwrap_or(chrono::TimeDelta::MAX);
            if elapsed > limit {
                candidates.push(SlaCandidate {
                    instance_id: instance.id,
                    tenant_id: instance.tenant_id.clone(),
                    sequence_id: instance.sequence_id,
                    kind: "max_step_runtime",
                    block_id: BlockId::new(format!("_sla:step:{}", step.as_str())),
                    elapsed_ms: elapsed.num_milliseconds(),
                    limit_ms: u64::try_from(max_step.as_millis()).unwrap_or(u64::MAX),
                    step_id: Some(step.as_str().to_string()),
                });
            }
        }
    }
    candidates
}

/// Emit one alert per fresh breach candidate: a batch sentinel lookup
/// de-duplicates against alerts from earlier ticks, then each new breach
/// persists its sentinel BEFORE the webhook emit so a crash mid-emit cannot
/// double-alert (at-most-once for alerts). The per-candidate sentinel write
/// and webhook emit are intentionally serial — batching them is a deferred
/// perf item, not part of this structure.
async fn emit_sla_alerts(
    ctx: &SlaSweepContext<'_>,
    candidates: &[SlaCandidate],
    now: chrono::DateTime<Utc>,
) -> Result<(), EngineError> {
    if candidates.is_empty() {
        return Ok(());
    }
    // One batch query for all sentinel keys: a present sentinel means
    // this breach already alerted and must not alert again.
    let keys: Vec<(InstanceId, &BlockId)> = candidates
        .iter()
        .map(|c| (c.instance_id, &c.block_id))
        .collect();
    let existing = ctx.storage.get_block_outputs_batch(&keys).await?;

    let mut existing_ref = std::collections::HashSet::with_capacity(existing.len());
    for key in existing.keys() {
        existing_ref.insert((&key.0, &key.1));
    }

    for c in candidates {
        if existing_ref.contains(&(&c.instance_id, &c.block_id)) {
            continue;
        }
        // Persist the sentinel BEFORE emitting so a crash mid-emit cannot
        // double-alert on the next tick (we prefer at-most-once for alerts).
        let sentinel = orch8_types::output::BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: c.instance_id,
            block_id: c.block_id.clone(),
            output: serde_json::json!({
                "_sla_breach": c.kind,
                "_alerted_at": now.to_rfc3339(),
            }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: Utc::now(),
        };
        ctx.storage.save_block_output(&sentinel).await?;

        crate::metrics::inc_with(crate::metrics::SLA_BREACHED, &[("type", c.kind)]);

        let event = crate::webhooks::instance_event(
            "instance.sla_breached",
            c.instance_id,
            serde_json::json!({
                "type": c.kind,
                "limit_ms": c.limit_ms,
                "elapsed_ms": c.elapsed_ms,
                "step_id": c.step_id,
                "sequence_id": c.sequence_id.into_uuid(),
                "tenant_id": c.tenant_id.as_str(),
            }),
        );
        // Durable, non-blocking enqueue: `emit` awaited a 64-permit
        // semaphore inline on the tick loop, so a slow webhook receiver
        // stalled scheduling engine-wide (ENG-R-N6). The outbox loop owns
        // delivery and retries.
        crate::webhooks::enqueue_durable(ctx.storage.as_ref(), ctx.webhook_config, &event).await;

        warn!(
            instance_id = %c.instance_id,
            sla_type = c.kind,
            elapsed_ms = c.elapsed_ms,
            limit_ms = c.limit_ms,
            "SLA breach (alert-only)"
        );
    }
    Ok(())
}

/// SLA deadline check variant for instances in `Waiting` state.
/// Delegates to the shared `handle_deadline_breach` with `from_state = Waiting`.
#[allow(clippy::too_many_arguments)]
async fn check_step_deadline_waiting(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    _webhook_config: &WebhookConfig,
    _cancel: &CancellationToken,
    prev_output: Option<&orch8_types::output::BlockOutput>,
    clock: &SharedClock,
    hooks: &HookTasks,
) -> Result<bool, EngineError> {
    handle_deadline_breach(
        storage,
        handlers,
        instance,
        step_def,
        prev_output,
        InstanceState::Waiting,
        clock,
        Some(hooks),
    )
    .await
}

/// Shared SLA deadline-breach handler parameterized on the originating state.
///
/// Both the fast-path `check_step_deadline` (Running instances) and the
/// waiting-deadline sweep (`check_step_deadline_waiting`) contain identical
/// logic: compute elapsed time since the step's baseline, invoke the
/// escalation handler if configured, record a breach output, and fail the
/// instance. The only difference is the `from_state` passed to
/// `transition_instance`. This function captures that shared logic.
///
/// `hooks`: when `Some` (the Waiting sweep, which runs on the tick task) the
/// escalation handler is spawned there instead of awaited inline (L2-06).
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) async fn handle_deadline_breach(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    prev_output: Option<&orch8_types::output::BlockOutput>,
    from_state: InstanceState,
    clock: &SharedClock,
    hooks: Option<&HookTasks>,
) -> Result<bool, EngineError> {
    let Some(deadline) = step_def.deadline else {
        return Ok(false);
    };
    let baseline = prev_output
        .map(|o| o.created_at)
        .or(instance.context.runtime.started_at);
    let Some(baseline) = baseline else {
        return Ok(false);
    };
    let elapsed = clock.now() - baseline;
    if elapsed < chrono::Duration::from_std(deadline).unwrap_or(chrono::TimeDelta::MAX) {
        return Ok(false);
    }

    let instance_id = instance.id;
    let state_label = match from_state {
        InstanceState::Running => "fast path",
        InstanceState::Waiting => "waiting instance",
        other => {
            // Defensive: log the unexpected state but proceed with the breach.
            warn!(
                instance_id = %instance_id,
                from_state = %other,
                "handle_deadline_breach called with unexpected from_state"
            );
            "unknown"
        }
    };
    warn!(
        instance_id = %instance_id,
        block_id = %step_def.id,
        deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
        elapsed_ms = elapsed.num_milliseconds(),
        "SLA deadline breached ({state_label})"
    );

    // Commit the breach marker with the terminal state. Only the CAS winner
    // may escalate; failed writes leave the instance eligible for a retry.
    if !from_state.can_transition_to(InstanceState::Failed) {
        return Err(EngineError::InvalidTransition {
            instance_id,
            from: from_state,
            to: InstanceState::Failed,
        });
    }
    let breach_output = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id,
        block_id: step_def.id.clone(),
        output: serde_json::json!({
            "_error": "sla_deadline_breached",
            "_deadline_ms": u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
            "_elapsed_ms": elapsed.num_milliseconds(),
        }),
        output_ref: None,
        output_size: 0,
        attempt: prev_output.as_ref().map_or(0, |o| o.attempt),
        created_at: Utc::now(),
    };
    if !storage
        .save_output_and_conditional_transition(
            &breach_output,
            from_state,
            InstanceState::Failed,
            None,
        )
        .await?
    {
        debug!(instance_id = %instance_id, block_id = %step_def.id,
            "SLA breach: instance already moved by a concurrent writer; skipping escalation");
        return Ok(true);
    }
    crate::lifecycle::audit_transition(
        storage.as_ref(),
        instance_id,
        &instance.tenant_id,
        from_state,
        InstanceState::Failed,
        None,
    )
    .await;
    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
    // Parent progress must not depend on a slow or rejected escalation.
    wake_parent_if_child(storage.as_ref(), instance).await;

    // Invoke escalation handler if configured (never for a sandboxed
    // sub-tenant sequence unless the handler is allowlisted).
    let escalation_denied = match &step_def.on_deadline_breach {
        Some(escalation) => crate::embed_sandbox::deny_handler(
            storage.as_ref(),
            instance,
            handlers.embed_sandbox(),
            step_def.id.as_str(),
            &escalation.handler,
        )
        .await?
        .is_some(),
        None => false,
    };
    if let Some(ref escalation) = step_def.on_deadline_breach
        && !escalation_denied
        && let Some(handler) = handlers.get(&escalation.handler)
    {
        let mut params = escalation.params.clone();
        if let serde_json::Value::Object(ref mut map) = params {
            map.insert(
                "_breach_block_id".into(),
                serde_json::json!(step_def.id.as_str()),
            );
            map.insert(
                "_breach_instance_id".into(),
                serde_json::json!(instance_id.into_uuid()),
            );
            map.insert(
                "_breach_elapsed_ms".into(),
                serde_json::json!(elapsed.num_milliseconds()),
            );
            map.insert(
                "_breach_deadline_ms".into(),
                serde_json::json!(u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX)),
            );
        }
        let step_ctx = crate::handlers::StepContext {
            claim_token: None,
            instance_id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step_def.id.clone(),
            params,
            context: Arc::new(instance.context.clone()),
            attempt: 0,
            storage: Arc::clone(storage),
            wait_for_input: None,
        };
        // Bounded and panic-isolated. From the Waiting sweep (tick task) it
        // also runs in the background so a slow escalation target cannot
        // stall scheduling.
        let escalation = handler(step_ctx);
        let run = async move {
            if let Err(e) = dispatch_hook_bounded(escalation).await {
                warn!(instance_id = %instance_id, error = %e, "SLA escalation handler failed");
            }
        };
        match hooks {
            Some(hooks) => hooks.spawn(instance.tenant_id.as_str(), run),
            None => run.await,
        }
    }

    Ok(true)
}

/// Build a map from `instance_id` to `PrefetchedData` from the batch query results.
///
/// Consumes both inputs and merges them in place — no intermediate `HashSet`
/// allocation, no redundant lookups.
fn build_prefetch_map(
    signals_map: HashMap<InstanceId, Vec<Signal>>,
    mut completed_map: HashMap<InstanceId, Vec<BlockId>>,
) -> HashMap<InstanceId, PrefetchedData> {
    // Pre-size for the worst case (no key overlap). Sparse overlap is the common case.
    let mut result: HashMap<InstanceId, PrefetchedData> =
        HashMap::with_capacity(signals_map.len() + completed_map.len());

    // Drain the signals map first — every signal owner definitely needs an entry,
    // and we opportunistically drain its matching completed-blocks entry in the
    // same pass.
    for (id, signals) in signals_map {
        let completed_block_ids = completed_map.remove(&id).unwrap_or_default();
        result.insert(
            id,
            PrefetchedData {
                signals,
                completed_block_ids,
            },
        );
    }

    // Anything left in `completed_map` had no signals — insert with an empty signal list.
    for (id, completed_block_ids) in completed_map {
        result.insert(
            id,
            PrefetchedData {
                signals: Vec::new(),
                completed_block_ids,
            },
        );
    }

    result
}

/// Outcome of executing a single step block.
enum StepOutcome {
    /// Step succeeded, block output saved. Continue to next block.
    Completed,
    /// Step was deferred (delay, rate-limit). Stop processing this instance.
    Deferred,
    /// Step failed permanently or after retries. Instance transitioned to Failed.
    Failed,
    /// Step `when` guard evaluated to false — advance to next block without
    /// incrementing the step counter.
    Skipped,
}

/// Bundles the per-instance processing dependencies that stay constant while
/// one claimed instance is worked through its steps. This replaces the long
/// parameter list on `process_instance` and its extracted stage helpers,
/// mirroring the [`TickContext`] pattern used for tick-level parameters.
struct InstanceRunCtx<'a> {
    pub storage: &'a Arc<dyn StorageBackend>,
    pub handlers: &'a HandlerRegistry,
    pub webhook_config: &'a WebhookConfig,
    pub sequence_cache: &'a SequenceCache,
    pub externalize_threshold: u32,
    pub max_steps_per_instance: u32,
    pub cancel: &'a CancellationToken,
    pub clock: &'a SharedClock,
    /// Set once the instance's lease heartbeat has failed
    /// [`HEARTBEAT_LEASE_LOST_AFTER`] times in a row (ENG-R-N8).
    pub lease_lost: &'a std::sync::atomic::AtomicBool,
    /// The claim token this run holds (see `heartbeat_instance`).
    pub claim_token: chrono::DateTime<chrono::Utc>,
}

/// Process a single claimed instance: execute ALL pending steps in one go.
async fn process_instance(
    ctx: &InstanceRunCtx<'_>,
    instance: orch8_types::instance::TaskInstance,
    prefetched: PrefetchedData,
) -> Result<(), EngineError> {
    let InstanceRunCtx {
        storage,
        sequence_cache,
        ..
    } = *ctx;
    let instance_id = instance.id;

    // Continuity fencing: never advance an instance whose portable execution
    // is being exported, or has been handed to another runtime. A superseded
    // source parks for good; a transferring one re-checks shortly (the
    // export may still be revoked or fail).
    match crate::ownership::local_ownership(storage.as_ref(), &instance.tenant_id, instance_id)
        .await?
    {
        crate::ownership::LocalOwnership::Owned => {}
        crate::ownership::LocalOwnership::Transferring => {
            info!(%instance_id, "continuity execution is transferring; deferring instance");
            crate::lifecycle::transition_claimed_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                ctx.claim_token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            )
            .await?;
            return Ok(());
        }
        crate::ownership::LocalOwnership::Superseded => {
            warn!(%instance_id, "continuity execution is owned elsewhere; parking local instance");
            crate::lifecycle::transition_claimed_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                ctx.claim_token,
                InstanceState::Running,
                InstanceState::Waiting,
                None,
            )
            .await?;
            return Ok(());
        }
    }

    // Fetch sequence definition early — needed for cancellation scopes and evaluation.
    let sequence = sequence_cache
        .get_by_id(storage.as_ref(), instance.sequence_id)
        .await?;
    let optimization = sequence_cache.optimization_for(&sequence).await;

    let instance = ensure_run_started(ctx, instance).await?;

    if process_pending_signals(ctx, &instance, prefetched.signals, &sequence).await? {
        return Ok(());
    }

    // Budget enforcement: if the instance carries a budget and any configured
    // limit is already exceeded, pause it (pre-flight — before any step work
    // this tick). Instances without a budget take zero extra queries here.
    if step_exec::check_budget(storage, &instance).await? {
        return Ok(());
    }

    // Merge dynamically injected blocks with the sequence definition.
    let blocks = crate::evaluator::merged_blocks(storage.as_ref(), instance.id, &sequence).await?;

    if check_fast_path_deadlines(ctx, &instance, &blocks, &prefetched.completed_block_ids).await? {
        return Ok(());
    }

    // Concurrency control: if this instance has a concurrency key, check the limit.
    if defer_if_over_concurrency(ctx, &instance).await? {
        return Ok(());
    }

    if use_tree_evaluator(&blocks, &sequence, optimization.as_deref()) {
        return process_instance_tree(ctx, &instance, &sequence).await;
    }

    if !execute_step_loop(
        ctx,
        &instance,
        &sequence,
        &blocks,
        prefetched.completed_block_ids,
    )
    .await?
    {
        return Ok(());
    }

    // All blocks completed.
    finalize_completed(ctx, &instance, &sequence).await
}

/// Finalize an instance whose step loop finished every block: emit the
/// completion interceptor trace and webhook, transition to `Completed`, and
/// wake the parent if this is a sub-sequence child.
///
/// The `on_complete` trace is emitted BEFORE flipping the instance to
/// `Completed`. Clients (tests, dashboards, workers) commonly observe
/// the state change and immediately read outputs; if the trace were
/// persisted afterwards, those clients would race the second write and
/// occasionally see `Completed` with no `_interceptor:on_complete`
/// row. Interceptor saves are non-fatal internally, so ordering them
/// ahead of the state transition doesn't risk blocking completion.
async fn finalize_completed(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<(), EngineError> {
    let instance_id = instance.id;
    if let Some(ref interceptors) = sequence.interceptors {
        crate::interceptors::emit_on_complete(ctx.storage.as_ref(), interceptors, instance_id)
            .await;
    }

    let completed_event =
        crate::webhooks::instance_event("instance.completed", instance_id, serde_json::json!({}));
    crate::lifecycle::transition_claimed_instance_with_webhook(
        ctx.storage.as_ref(),
        instance_id,
        Some(&instance.tenant_id),
        ctx.claim_token,
        InstanceState::Running,
        InstanceState::Completed,
        None,
        ctx.webhook_config,
        &completed_event,
    )
    .await?;

    crate::metrics::inc(crate::metrics::INSTANCES_COMPLETED);

    info!(instance_id = %instance_id, "instance completed all blocks");

    // Wake parent if this is a sub-sequence child.
    wake_parent_if_child(ctx.storage.as_ref(), instance).await;

    Ok(())
}

/// `claim_due_instances` already set state to Running.
/// Ensure the run identity and start timestamp together. Retry/resume may
/// already have installed a fresh `run_id`; first execution creates one.
/// The scheduler clock is the baseline for timeout scheduling decisions.
async fn ensure_run_started(
    ctx: &InstanceRunCtx<'_>,
    instance: orch8_types::instance::TaskInstance,
) -> Result<orch8_types::instance::TaskInstance, EngineError> {
    let instance_id = instance.id;
    let missing_run_id = instance.context.runtime.run_id.is_none();
    let missing_started_at = instance.context.runtime.started_at.is_none();
    let instance = if missing_run_id || missing_started_at {
        let started_at = instance
            .context
            .runtime
            .started_at
            .unwrap_or_else(|| ctx.clock.now());
        let run_id = instance
            .context
            .runtime
            .run_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        if !ctx
            .storage
            .ensure_instance_run_started_claimed(instance_id, &run_id, started_at, ctx.claim_token)
            .await?
        {
            return Err(EngineError::ClaimLost { instance_id });
        }
        let mut inst = instance;
        inst.context.runtime.run_id = Some(run_id);
        inst.context.runtime.started_at = Some(started_at);
        inst.context.runtime.instance_id = Some(instance_id.to_string());
        inst
    } else {
        let mut inst = instance;
        if inst.context.runtime.instance_id.is_none() {
            inst.context.runtime.instance_id = Some(instance_id.to_string());
        }
        inst
    };
    Ok(instance)
}

/// Process any pending signals (using pre-fetched data). Returns `true` when
/// a signal aborted the instance and the caller must stop processing it.
///
/// A cancel signal that drove the instance to `Cancelled` here (an unscoped
/// cancel of a claimed instance -- including every API-requested cancel of a
/// scheduled one) owes the same terminal effects as the tree path: the
/// compensations of its interrupted sagas, its `on_cancel` hook, and a wake
/// for a waiting sub-sequence parent. This runs on the per-instance task,
/// like [`run_tree_terminal_effects`], never on the tick.
async fn process_pending_signals(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    signals: Vec<Signal>,
    sequence: &SequenceDefinition,
) -> Result<bool, EngineError> {
    let instance_id = instance.id;
    if !signals.is_empty() {
        let abort = crate::signals::process_signals_prefetched(
            ctx.storage.as_ref(),
            instance_id,
            InstanceState::Running,
            signals,
            Some(sequence),
        )
        .await?;
        if abort {
            if let Ok(Some(fresh)) = ctx.storage.get_instance(instance_id).await
                && fresh.state == InstanceState::Cancelled
            {
                run_tree_terminal_effects(ctx, &fresh, sequence, InstanceState::Cancelled).await;
                wake_parent_if_child(ctx.storage.as_ref(), &fresh).await;
            }
            return Ok(true);
        }
    }
    Ok(false)
}

/// Fast path SLA deadline check for all steps BEFORE concurrency checks.
/// Batch-fetch any previous block outputs so the loop is N queries -> 1 query.
/// Returns `true` when a breached deadline failed the instance and the caller
/// must stop processing it.
async fn check_fast_path_deadlines(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    blocks: &[orch8_types::sequence::BlockDefinition],
    completed_block_ids: &[BlockId],
) -> Result<bool, EngineError> {
    let deadline_keys: Vec<(InstanceId, &BlockId)> = blocks
        .iter()
        .filter_map(|b| match b {
            orch8_types::sequence::BlockDefinition::Step(s) if s.deadline.is_some() => {
                Some((instance.id, &s.id))
            }
            _ => None,
        })
        .collect();
    let deadline_outputs = prefetch_deadline_outputs(ctx.storage, &deadline_keys).await?;
    let deadline_outputs_ref = DeadlineOutputs::new(&deadline_outputs);

    for block in blocks {
        if let orch8_types::sequence::BlockDefinition::Step(step_def) = block
            && step_def.deadline.is_some()
            && !completed_block_ids.contains(&step_def.id)
        {
            // SLA deadline check: if a previous attempt exists and the deadline has
            // been breached (wall-clock time since first attempt), fail the instance.
            let prev = deadline_outputs_ref.get(&instance.id, &step_def.id);
            if step_exec::check_step_deadline(
                ctx.storage,
                ctx.handlers,
                instance,
                step_def,
                prev,
                ctx.clock,
            )
            .await?
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Concurrency control: if this instance has a concurrency key, check the
/// limit. Returns `true` when the instance was deferred back to Scheduled
/// because the limit is reached and the caller must stop processing it.
async fn defer_if_over_concurrency(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
) -> Result<bool, EngineError> {
    if let (Some(key), Some(max)) = (&instance.concurrency_key, instance.max_concurrency) {
        let instance_id = instance.id;
        let position = ctx.storage.concurrency_position(instance_id, key).await?;
        if position > i64::from(max) {
            let defer_at = ctx.clock.now() + chrono::Duration::seconds(2);
            ctx.storage
                .conditional_update_claimed_instance_state(
                    instance_id,
                    ctx.claim_token,
                    InstanceState::Running,
                    InstanceState::Scheduled,
                    Some(defer_at),
                )
                .await?;
            debug!(
                instance_id = %instance_id,
                concurrency_key = %key,
                position = position,
                max = max,
                "concurrency limit reached, deferring instance"
            );
            return Ok(true);
        }
    }
    Ok(false)
}

/// Decide execution path: if the sequence has any composite (non-Step) blocks
/// or any plugin handlers (ap://, grpc://, wasm://), use the tree-based
/// evaluator. Plugin handlers are only dispatched correctly through the tree
/// evaluator's `step_block.rs` which knows about all plugin prefixes.
/// Dynamically injected blocks are not part of the immutable compiler IR;
/// retain the exact legacy scan whenever the evaluator changed the roots.
fn use_tree_evaluator(
    blocks: &[orch8_types::sequence::BlockDefinition],
    sequence: &SequenceDefinition,
    optimization: Option<&crate::optimizer::OptimizationIr>,
) -> bool {
    let static_roots = blocks.len() == sequence.blocks.len();
    let has_composite = optimization.filter(|_| static_roots).map_or_else(
        || {
            blocks
                .iter()
                .any(|b| !matches!(b, orch8_types::sequence::BlockDefinition::Step(_)))
        },
        |ir| ir.top_level_has_composite,
    );

    let has_plugin_handler = optimization.filter(|_| static_roots).map_or_else(
        || {
            blocks.iter().any(|b| {
                if let orch8_types::sequence::BlockDefinition::Step(step) = b {
                    crate::handlers::PluginKind::detect(&step.handler).is_some()
                } else {
                    false
                }
            })
        },
        |ir| ir.top_level_has_plugin_handler,
    );

    has_composite || has_plugin_handler
}

/// Fast path: all blocks are Steps. Execute multi-block per claim cycle.
/// Returns `true` when every block completed and the caller should finalize
/// the instance; `false` when an outcome already ended processing for this
/// tick (step failure, deferral, cooperative preemption, or the
/// `max_steps_per_instance` budget being exceeded).
#[allow(clippy::too_many_lines)]
async fn execute_step_loop(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    blocks: &[orch8_types::sequence::BlockDefinition],
    completed_block_ids: Vec<BlockId>,
) -> Result<bool, EngineError> {
    let instance_id = instance.id;
    let mut completed_blocks = completed_block_ids;
    completed_blocks.sort_unstable();
    // ⚡ Bolt: Deduplicating the list of indices eliminates redundant comparisons
    // during the `binary_search` filtering phase, improving hot path performance.
    completed_blocks.dedup();

    for block in blocks {
        let orch8_types::sequence::BlockDefinition::Step(step_def) = block else {
            unreachable!("checked above: all blocks are steps");
        };

        if completed_blocks.binary_search(&step_def.id).is_ok() {
            // A `wait_for_input` step can land in the completed set on gate
            // acceptance ALONE (the acceptance row is stored with
            // `output_ref: None`). Skipping it here would advance the
            // instance while the step's side-effecting handler never finished
            // after a mid-handler crash or retryable failure. Consult the
            // step's latest output and re-execute unless it proves the
            // handler actually completed (at-least-once, same contract as the
            // `__in_progress__` sentinel).
            if step_def.wait_for_input.is_some()
                && step_exec::gated_step_incomplete(ctx.storage.as_ref(), instance_id, &step_def.id)
                    .await?
            {
                // Fall through — re-execute the step. Its gate check finds
                // the persisted acceptance and goes straight to the handler.
            } else {
                continue;
            }
        }

        // Lease presumed lost (heartbeats failing): another node may already
        // have re-dispatched this instance, so do not start another step.
        // Hand the instance back (best-effort — if storage is what's down,
        // the stale-instance reaper recovers it).
        if lease_presumed_lost(ctx) {
            warn!(
                instance_id = %instance_id,
                block_id = %step_def.id,
                "instance lease presumed lost; not starting further steps"
            );
            hand_back_after_lease_loss(ctx, instance_id).await;
            return Ok(false);
        }

        // Interceptor: before_step
        if let Some(ref interceptors) = sequence.interceptors {
            crate::interceptors::emit_before_step(
                ctx.storage.as_ref(),
                interceptors,
                instance_id,
                &step_def.id,
            )
            .await;
        }

        let outcome = step_exec::execute_step_block(ctx, instance, step_def).await?;

        // Interceptor: after_step (fires regardless of outcome)
        if let Some(ref interceptors) = sequence.interceptors {
            crate::interceptors::emit_after_step(
                ctx.storage.as_ref(),
                interceptors,
                instance_id,
                &step_def.id,
            )
            .await;
        }

        match outcome {
            StepOutcome::Completed => {
                // Insert in sorted position (keeps the Vec sorted for
                // binary_search without an O(N log N) re-sort per completion;
                // dedups too).
                if let Err(pos) = completed_blocks.binary_search(&step_def.id) {
                    completed_blocks.insert(pos, step_def.id.clone());
                }

                // Atomically bump the step counter. Touches only the counter
                // path, so context mutations made during step execution (e.g.
                // check_human_input's merge_context_data) are preserved without
                // a read + full-context rewrite per step.
                let total = ctx.storage.increment_total_steps(instance_id).await?;

                if ctx.max_steps_per_instance > 0 && total > ctx.max_steps_per_instance {
                    warn!(
                        instance_id = %instance_id,
                        total = total,
                        max = ctx.max_steps_per_instance,
                        "instance exceeded max_steps_per_instance, failing"
                    );
                    crate::lifecycle::transition_claimed_instance(
                        ctx.storage.as_ref(),
                        instance_id,
                        Some(&instance.tenant_id),
                        ctx.claim_token,
                        InstanceState::Running,
                        InstanceState::Failed,
                        None,
                    )
                    .await?;
                    return Ok(false);
                }
                if cooperatively_preempt(ctx.storage, instance, ctx.clock, ctx.claim_token).await? {
                    return Ok(false);
                }
            }
            StepOutcome::Failed => {
                // Best-effort cleanup: on_failure (fast-path step failure).
                if let Some(ref blocks) = sequence.on_failure {
                    run_cleanup_hooks(ctx.storage, ctx.handlers, instance, blocks, "on_failure")
                        .await;
                }
                // Interceptor: on_failure
                if let Some(ref interceptors) = sequence.interceptors {
                    crate::interceptors::emit_on_failure(
                        ctx.storage.as_ref(),
                        interceptors,
                        instance_id,
                    )
                    .await;
                }
                return Ok(false);
            }
            StepOutcome::Deferred => {
                return Ok(false);
            }
            StepOutcome::Skipped => {
                if let Err(pos) = completed_blocks.binary_search(&step_def.id) {
                    completed_blocks.insert(pos, step_def.id.clone());
                }
                if cooperatively_preempt(ctx.storage, instance, ctx.clock, ctx.claim_token).await? {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Yield a scheduler slot at a durable block boundary when higher-priority
/// work is ready. This is intentionally cooperative: aborting an in-flight
/// handler could duplicate an external effect or discard an uncommitted
/// result. The completed block output is already durable before this runs.
async fn cooperatively_preempt(
    storage: &Arc<dyn StorageBackend>,
    instance: &orch8_types::instance::TaskInstance,
    clock: &SharedClock,
    claim_token: chrono::DateTime<Utc>,
) -> Result<bool, EngineError> {
    let now = clock.now();
    if !storage
        .has_due_higher_priority(now, instance.priority)
        .await?
    {
        return Ok(false);
    }
    let yielded = storage
        .conditional_update_claimed_instance_state(
            instance.id,
            claim_token,
            InstanceState::Running,
            InstanceState::Scheduled,
            Some(now),
        )
        .await?;
    if yielded {
        debug!(
            instance_id = %instance.id,
            priority = ?instance.priority,
            "cooperatively preempted instance for higher-priority work"
        );
    }
    Ok(yielded)
}

/// Cancel live sub-workflows of failed/cancelled parents (L3-07). A storage
/// error is logged and retried on the next tick.
///
/// Each cancelled child then gets what any other cancel gets, off the tick:
/// the compensations of its interrupted sagas (L3-11) and its `on_cancel`
/// cleanup hook.
async fn cancel_orphans(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    sequence_cache: &SequenceCache,
    hooks: &HookTasks,
) {
    let cancelled = match crate::lifecycle::cancel_orphaned_children(storage.as_ref()).await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "orphaned sub-workflow sweep failed");
            return;
        }
    };
    for inst in cancelled {
        let Ok(seq) = sequence_cache
            .get_by_id(storage.as_ref(), inst.sequence_id)
            .await
        else {
            continue;
        };
        let storage = Arc::clone(storage);
        let handlers = Arc::clone(handlers);
        let tenant = inst.tenant_id.clone();
        hooks.spawn(tenant.as_str(), async move {
            crate::handlers::saga::compensate_aborted_sagas(
                &storage,
                &handlers,
                &inst,
                &seq,
                "cancelled",
            )
            .await;
            if let Some(ref blocks) = seq.on_cancel {
                run_cleanup_hooks(&storage, &handlers, &inst, blocks, "on_cancel").await;
            }
        });
    }
}

/// If this instance has a `parent_instance_id`, transition the parent from
/// `Waiting` -> `Scheduled` so it is picked up on the next tick and can observe
/// the child's terminal state.
///
/// Uses a CAS (conditional update) so a concurrent cancel/fail cannot be
/// silently overwritten.
///
/// Clock note: this intentionally uses real `Utc::now()` rather than the
/// scheduler clock — the semantics are "wake immediately". Under a
/// `ManualClock` that starts at or after real time (the documented
/// discipline), real now <= virtual now, so the parent is due on the very
/// next virtual tick.
async fn wake_parent_if_child(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
) {
    if let Some(parent_id) = instance.parent_instance_id {
        let now = Utc::now();
        match storage
            .conditional_update_instance_state(
                parent_id,
                InstanceState::Waiting,
                InstanceState::Scheduled,
                Some(now),
            )
            .await
        {
            Ok(true) => {
                info!(
                    parent_id = %parent_id,
                    child_id = %instance.id,
                    "woke parent instance after child reached terminal state"
                );
            }
            Ok(false) => {
                // Usually the parent already moved on, but it may still be
                // `Running` (evaluating another branch) and about to park
                // without having seen this child finish (L3-01): queue a
                // retry the tick loop delivers once it parks.
                debug!(
                    parent_id = %parent_id,
                    child_id = %instance.id,
                    "parent not in Waiting state; deferring wake retry"
                );
                defer_parent_wake(parent_id);
            }
            Err(e) => {
                warn!(
                    parent_id = %parent_id,
                    child_id = %instance.id,
                    error = %e,
                    "failed to wake parent instance after child completion"
                );
            }
        }
    }
}

/// Handle `EvalOutcome::MoreWork`: if nodes are waiting for external workers,
/// transition to Waiting (the worker completion callback will re-schedule).
/// Otherwise, re-schedule for the next tick.
///
/// But: if a step handler drove a pause/cancel transition mid-tick
/// (see `handle_sleep` in `handlers/builtin.rs`), the instance may
/// already be in `Paused` or `Cancelled` when we land here — we
/// must NOT overwrite that with `Scheduled`, or the observable
/// state would flip right back to running.
async fn handle_tree_more_work(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    has_waiting_nodes: bool,
) -> Result<(), EngineError> {
    let instance_id = instance.id;
    let current = ctx
        .storage
        .get_instance(instance_id)
        .await?
        .map_or(InstanceState::Running, |i| i.state);
    // Any terminal state, not just Cancelled/Failed (L3-12): a concurrent
    // runner that completed the instance mid-evaluation must not be
    // resurrected through the `Completed -> Scheduled` resume-from-block edge
    // (duplicate `instance.completed` webhook and parent wake).
    if current == InstanceState::Paused || current.is_terminal() {
        // Terminal/paused — leave state as-is.
    } else if has_waiting_nodes {
        match crate::lifecycle::transition_claimed_instance(
            ctx.storage.as_ref(),
            instance_id,
            Some(&instance.tenant_id),
            ctx.claim_token,
            InstanceState::Running,
            InstanceState::Waiting,
            None,
        )
        .await
        {
            Ok(()) => recheck_parked_sub_sequence_parent(ctx, instance, sequence).await,
            Err(e) => {
                if !matches!(e, crate::error::EngineError::InvalidTransition { .. }) {
                    return Err(e);
                }
                debug!(
                    instance_id = %instance_id,
                    "concurrent writer moved instance before Running->Waiting transition"
                );
            }
        }
    } else if let Err(e) = crate::lifecycle::transition_claimed_instance(
        ctx.storage.as_ref(),
        instance_id,
        Some(&instance.tenant_id),
        ctx.claim_token,
        current,
        InstanceState::Scheduled,
        Some(ctx.clock.now()),
    )
    .await
    {
        if !matches!(e, crate::error::EngineError::InvalidTransition { .. }) {
            return Err(e);
        }
        debug!(
            instance_id = %instance_id,
            current = %current,
            "concurrent writer moved instance before tree->Scheduled transition"
        );
    }
    Ok(())
}

/// Park-then-recheck for sub-sequence parents (L3-01). A child that reaches a
/// terminal state while its parent is still `Running` (evaluating another
/// branch) finds the parent not `Waiting` and its wake CAS is a no-op; if the
/// parent's last read of the child predates that, the parent then parks
/// `Waiting` with nothing left to wake it. Re-reading the awaited children
/// *after* the `Running -> Waiting` commit closes the window: either this read
/// sees the terminal child, or the child's terminal write — and therefore its
/// wake attempt — comes after the parent's park and succeeds.
async fn recheck_parked_sub_sequence_parent(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
) {
    let has_sub_sequence = crate::evaluator::flatten_blocks(&sequence.blocks)
        .iter()
        .any(|(_, b)| matches!(b, orch8_types::sequence::BlockDefinition::SubSequence(_)));
    if !has_sub_sequence {
        return;
    }
    match crate::evaluator::awaited_sub_sequence_child_terminal(ctx.storage.as_ref(), instance)
        .await
    {
        Ok(false) => {}
        Ok(true) => match ctx
            .storage
            .conditional_update_instance_state(
                instance.id,
                InstanceState::Waiting,
                InstanceState::Scheduled,
                Some(ctx.clock.now()),
            )
            .await
        {
            Ok(woke) => debug!(
                instance_id = %instance.id,
                woke,
                "awaited sub-sequence child finished before the parent parked; re-scheduling"
            ),
            Err(e) => {
                warn!(instance_id = %instance.id, error = %e, "failed to re-schedule parked parent");
                defer_parent_wake(instance.id);
            }
        },
        Err(e) => {
            warn!(
                instance_id = %instance.id,
                error = %e,
                "post-park sub-sequence recheck failed; deferring a wake retry"
            );
            defer_parent_wake(instance.id);
        }
    }
}

/// Parents whose wake could not be delivered yet: the child's
/// `Waiting -> Scheduled` CAS found them still `Running`, or the post-park
/// recheck failed. Value: when to try next. In-process safety net behind the
/// park-then-recheck fix (L3-01); bounded and lost on restart.
static DEFERRED_PARENT_WAKES: std::sync::LazyLock<
    std::sync::Mutex<HashMap<InstanceId, (std::time::Instant, std::time::Instant)>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
/// Delay between deferred parent-wake attempts.
const DEFERRED_WAKE_RETRY: Duration = Duration::from_secs(1);
/// Give up on a deferred parent wake after this long.
const DEFERRED_WAKE_MAX_AGE: Duration = Duration::from_secs(300);
/// Upper bound on tracked deferred parent wakes.
const DEFERRED_WAKE_CAP: usize = 10_000;

fn defer_parent_wake(parent_id: InstanceId) {
    let now = std::time::Instant::now();
    let mut map = DEFERRED_PARENT_WAKES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    insert_bounded(&mut map, parent_id, now, DEFERRED_WAKE_CAP);
}

/// Insert/refresh a deferred wake. At `cap`, the entry closest to its max age
/// (oldest `first`) is evicted instead of dropping the new wake. An evicted
/// parent is recovered by the park-then-recheck path
/// (`recheck_parked_sub_sequence_parent`), which re-reads the awaited
/// children after the parent's `Running -> Waiting` commit; the drop is
/// counted in a metric.
fn insert_bounded(
    map: &mut HashMap<InstanceId, (std::time::Instant, std::time::Instant)>,
    parent_id: InstanceId,
    now: std::time::Instant,
    cap: usize,
) {
    if map.len() >= cap
        && !map.contains_key(&parent_id)
        && let Some(oldest) = map
            .iter()
            .min_by_key(|(_, (first, _))| *first)
            .map(|(id, _)| *id)
    {
        map.remove(&oldest);
        crate::metrics::inc(crate::metrics::DEFERRED_PARENT_WAKES_DROPPED);
    }
    map.entry(parent_id)
        .and_modify(|(_, next)| *next = now + DEFERRED_WAKE_RETRY)
        .or_insert((now, now + DEFERRED_WAKE_RETRY));
}

/// Retry deferred parent wakes that are due. A parent still `Running` (or
/// `Scheduled`, about to run) keeps its entry until it parks; a `Waiting`
/// parent is re-scheduled so it re-evaluates its finished child; anything
/// else (paused, terminal, gone) is dropped. Called once per tick.
async fn retry_deferred_parent_wakes(storage: &Arc<dyn StorageBackend>) {
    let now = std::time::Instant::now();
    let due: Vec<InstanceId> = {
        let mut map = DEFERRED_PARENT_WAKES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if map.is_empty() {
            return;
        }
        let before = map.len();
        map.retain(|_, (first, _)| now.duration_since(*first) < DEFERRED_WAKE_MAX_AGE);
        for _ in map.len()..before {
            crate::metrics::inc(crate::metrics::DEFERRED_PARENT_WAKES_DROPPED);
        }
        map.iter()
            .filter(|(_, (_, next))| *next <= now)
            .map(|(id, _)| *id)
            .collect()
    };
    for parent_id in due {
        let keep = match storage.get_instance(parent_id).await {
            Ok(Some(p)) if p.state == InstanceState::Waiting => {
                if let Err(e) = storage
                    .conditional_update_instance_state(
                        parent_id,
                        InstanceState::Waiting,
                        InstanceState::Scheduled,
                        Some(Utc::now()),
                    )
                    .await
                {
                    warn!(parent_id = %parent_id, error = %e, "deferred parent wake failed");
                    true
                } else {
                    info!(parent_id = %parent_id, "delivered deferred sub-sequence parent wake");
                    false
                }
            }
            Ok(Some(p)) => p.state == InstanceState::Running,
            Ok(None) => false,
            Err(_) => true,
        };
        let mut map = DEFERRED_PARENT_WAKES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if keep {
            if let Some((_, next)) = map.get_mut(&parent_id) {
                *next = now + DEFERRED_WAKE_RETRY;
            }
        } else {
            map.remove(&parent_id);
        }
    }
}

/// Pre-flight step-budget check for the tree path: a fresh read guards
/// against the counter having outgrown the claim-time snapshot. Returns
/// `true` when the instance was failed for exceeding
/// `max_steps_per_instance` and the caller must stop processing it.
async fn fail_if_over_step_budget(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<bool, EngineError> {
    let instance_id = instance.id;
    if ctx.max_steps_per_instance > 0 {
        let fresh = ctx.storage.get_instance(instance_id).await?;
        if let Some(inst) = fresh
            && inst.context.runtime.total_steps_executed > ctx.max_steps_per_instance
        {
            warn!(
                instance_id = %instance_id,
                total = inst.context.runtime.total_steps_executed,
                max = ctx.max_steps_per_instance,
                "instance exceeded max_steps_per_instance (tree path), failing"
            );
            crate::lifecycle::transition_claimed_instance(
                ctx.storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                ctx.claim_token,
                InstanceState::Running,
                InstanceState::Failed,
                None,
            )
            .await?;
            // L3-11: undo the completed steps of any saga the budget kill
            // cut short, and (like every other terminal path) wake a
            // sub-sequence parent that is waiting on this instance.
            crate::handlers::saga::compensate_aborted_sagas(
                ctx.storage,
                ctx.handlers,
                instance,
                sequence,
                "step_budget",
            )
            .await;
            wake_parent_if_child(ctx.storage.as_ref(), instance).await;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether this run's lease heartbeat has flagged the lease as lost.
fn lease_presumed_lost(ctx: &InstanceRunCtx<'_>) -> bool {
    ctx.lease_lost.load(std::sync::atomic::Ordering::Acquire)
}

/// Lease presumed lost (heartbeats failing): another node may already have
/// re-dispatched this instance. Hand it back to `Scheduled` (best-effort — if
/// storage is what's down, the stale-instance reaper recovers it), unless
/// another claim already owns the row: releasing it then would yank the new
/// owner's run back to `Scheduled`.
async fn hand_back_after_lease_loss(ctx: &InstanceRunCtx<'_>, instance_id: InstanceId) {
    if let Err(e) = ctx
        .storage
        .conditional_update_claimed_instance_state(
            instance_id,
            ctx.claim_token,
            InstanceState::Running,
            InstanceState::Scheduled,
            Some(ctx.clock.now()),
        )
        .await
    {
        warn!(instance_id = %instance_id, error = %e, "failed to release instance after lease loss");
    }
}

/// Process an instance using the tree-based evaluator for composite blocks.
///
/// The evaluator manages an execution tree (persisted in DB) and dispatches each
/// node to its block-type handler (`Parallel`, `Race`, `Loop`, `ForEach`, `Router`, `TryCatch`, `Step`).
/// The evaluator returns an `EvalOutcome` carrying the tree state so we can
/// transition the instance without re-reading the tree from the database.
async fn process_instance_tree(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<(), EngineError> {
    let InstanceRunCtx {
        storage, handlers, ..
    } = *ctx;

    if fail_if_over_step_budget(ctx, instance, sequence).await? {
        return Ok(());
    }

    // GAP-005: the L6-01 lease fence covers composite workflows too — do not
    // start evaluating (and dispatching steps) once the lease is presumed lost.
    if lease_presumed_lost(ctx) {
        warn!(instance_id = %instance.id, "instance lease presumed lost; not evaluating tree");
        hand_back_after_lease_loss(ctx, instance.id).await;
        return Ok(());
    }

    match crate::evaluator::evaluate_with_claim_token(
        storage,
        handlers,
        instance,
        sequence,
        ctx.clock,
        ctx.claim_token,
        ctx.lease_lost,
    )
    .await
    {
        // The evaluator stopped early because the lease was lost mid-tick:
        // hand back instead of parking/rescheduling as if it were ours.
        Ok(EvalOutcome::MoreWork { .. }) if lease_presumed_lost(ctx) => {
            warn!(instance_id = %instance.id, "instance lease lost during tree evaluation; stopping");
            hand_back_after_lease_loss(ctx, instance.id).await;
        }
        Ok(EvalOutcome::MoreWork { has_waiting_nodes }) => {
            handle_tree_more_work(ctx, instance, sequence, has_waiting_nodes).await?;
        }
        Ok(EvalOutcome::Done { reason }) => {
            handle_tree_done(ctx, instance, sequence, reason).await?;
        }
        // L3-06: same classification as the flat path. A transient storage
        // error (connection reset, failover, pool timeout) mid-evaluate must
        // reach `process_tick`'s transient arm and be rescheduled with
        // backoff, not fail the instance permanently.
        Err(e @ EngineError::ClaimLost { .. }) => return Err(e),
        Err(e) if e.is_transient_storage() => return Err(e),
        Err(e) => {
            handle_tree_eval_error(ctx, instance, sequence, &e).await?;
        }
    }

    Ok(())
}

/// Handle `EvalOutcome::Done`: CAS-transition the instance to its terminal
/// state (Cancelled / Failed / Completed), then run the post-transition
/// effects and wake the parent if this is a sub-sequence child.
async fn handle_tree_done(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    reason: TerminalReason,
) -> Result<(), EngineError> {
    let instance_id = instance.id;
    // Re-read current state so we use a CAS transition instead of
    // unconditional overwrite — protects against concurrent pause/
    // cancel/fail that arrived mid-evaluation.
    let current = ctx
        .storage
        .get_instance(instance_id)
        .await?
        .map_or(InstanceState::Running, |i| i.state);

    // If a signal (pause / cancel) or concurrent writer already moved
    // the instance out of Running, do not overwrite it.
    if current == InstanceState::Paused || current.is_terminal() {
        debug!(
            instance_id = %instance_id,
            current = %current,
            "instance already moved by signal or concurrent writer -- skipping terminal transition"
        );
        return Ok(());
    }

    // Distinguish user-initiated cancel from genuine failure: a
    // Cancelled root node (and no Failed root) means the instance
    // was cancelled, not that a step failed.
    let target = match reason {
        TerminalReason::Cancelled => InstanceState::Cancelled,
        TerminalReason::Failed => InstanceState::Failed,
        TerminalReason::Succeeded => InstanceState::Completed,
    };

    let terminal_event = match target {
        InstanceState::Failed => Some(crate::webhooks::instance_event(
            "instance.failed",
            instance_id,
            serde_json::json!({}),
        )),
        InstanceState::Completed => Some(crate::webhooks::instance_event(
            "instance.completed",
            instance_id,
            serde_json::json!({}),
        )),
        _ => None,
    };
    let transition = if let Some(event) = terminal_event.as_ref() {
        crate::lifecycle::transition_claimed_instance_with_webhook(
            ctx.storage.as_ref(),
            instance_id,
            Some(&instance.tenant_id),
            ctx.claim_token,
            current,
            target,
            None,
            ctx.webhook_config,
            event,
        )
        .await
    } else {
        crate::lifecycle::transition_claimed_instance(
            ctx.storage.as_ref(),
            instance_id,
            Some(&instance.tenant_id),
            ctx.claim_token,
            current,
            target,
            None,
        )
        .await
    };

    if let Err(e) = transition {
        if !matches!(e, crate::error::EngineError::InvalidTransition { .. }) {
            return Err(e);
        }
        debug!(
            instance_id = %instance_id,
            current = %current,
            target = %target,
            "concurrent writer moved instance before tree terminal transition"
        );
    } else {
        run_tree_terminal_effects(ctx, instance, sequence, target).await;
    }

    // Wake parent if this is a sub-sequence child that reached a terminal state.
    wake_parent_if_child(ctx.storage.as_ref(), instance).await;
    Ok(())
}

/// Post-transition effects for a tree-path terminal state: best-effort
/// cleanup hooks, interceptor traces, metrics, and the info log line. Runs
/// only after the CAS transition actually landed.
async fn run_tree_terminal_effects(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    target: InstanceState,
) {
    let instance_id = instance.id;
    match target {
        InstanceState::Cancelled => {
            // Undo the completed steps of sagas the cancel cut short (L3-11).
            crate::handlers::saga::compensate_aborted_sagas(
                ctx.storage,
                ctx.handlers,
                instance,
                sequence,
                "cancelled",
            )
            .await;
            // Best-effort cleanup: on_cancel.
            if let Some(ref blocks) = sequence.on_cancel {
                run_cleanup_hooks(ctx.storage, ctx.handlers, instance, blocks, "on_cancel").await;
            }
            info!(instance_id = %instance_id, "instance cancelled (tree evaluation)");
        }
        InstanceState::Failed => {
            // Sagas that did not roll themselves back (a failure elsewhere
            // in the tree) still owe their compensations (L3-11).
            crate::handlers::saga::compensate_aborted_sagas(
                ctx.storage,
                ctx.handlers,
                instance,
                sequence,
                "failed",
            )
            .await;
            // Best-effort cleanup: on_failure.
            if let Some(ref blocks) = sequence.on_failure {
                run_cleanup_hooks(ctx.storage, ctx.handlers, instance, blocks, "on_failure").await;
            }
            // Interceptor: on_failure
            if let Some(ref interceptors) = sequence.interceptors {
                crate::interceptors::emit_on_failure(
                    ctx.storage.as_ref(),
                    interceptors,
                    instance_id,
                )
                .await;
            }
            crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
            info!(instance_id = %instance_id, "instance failed (tree evaluation)");
        }
        InstanceState::Completed => {
            // Persist the `on_complete` trace BEFORE flipping state to
            // `Completed`. See the fast-path comment for rationale: an
            // observer who sees `Completed` must be guaranteed to also
            // see the trace artifact, otherwise test suites and
            // dashboards race a second write and intermittently miss it.
            if let Some(ref interceptors) = sequence.interceptors {
                crate::interceptors::emit_on_complete(
                    ctx.storage.as_ref(),
                    interceptors,
                    instance_id,
                )
                .await;
            }
            crate::metrics::inc(crate::metrics::INSTANCES_COMPLETED);
            info!(instance_id = %instance_id, "instance completed (tree evaluation)");
        }
        _ => {}
    }
}

/// Handle a tree-evaluation error: fail the instance, run the best-effort
/// `on_failure` cleanup and interceptor trace, bump the failure metric, and
/// wake the parent.
async fn handle_tree_eval_error(
    ctx: &InstanceRunCtx<'_>,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    error: &EngineError,
) -> Result<(), EngineError> {
    let instance_id = instance.id;
    error!(instance_id = %instance_id, error = %error, "tree evaluation failed");
    crate::lifecycle::transition_claimed_instance(
        ctx.storage.as_ref(),
        instance_id,
        Some(&instance.tenant_id),
        ctx.claim_token,
        InstanceState::Running,
        InstanceState::Failed,
        None,
    )
    .await?;
    // Sagas the evaluation error cut short still owe their compensations
    // (L3-11).
    crate::handlers::saga::compensate_aborted_sagas(
        ctx.storage,
        ctx.handlers,
        instance,
        sequence,
        "evaluation_error",
    )
    .await;
    // Best-effort cleanup: on_failure (tree evaluation blew up).
    if let Some(ref blocks) = sequence.on_failure {
        run_cleanup_hooks(ctx.storage, ctx.handlers, instance, blocks, "on_failure").await;
    }
    // Interceptor: on_failure
    if let Some(ref interceptors) = sequence.interceptors {
        crate::interceptors::emit_on_failure(ctx.storage.as_ref(), interceptors, instance_id).await;
    }
    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);

    // Wake parent if this child's tree evaluation blew up mid-flight —
    // otherwise a SubSequence parent would remain Waiting forever,
    // since the Done branch above is the only other place that
    // calls `wake_parent_if_child`.
    wake_parent_if_child(ctx.storage.as_ref(), instance).await;
    Ok(())
}
