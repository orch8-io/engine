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
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::Signal;

use crate::error::EngineError;
use crate::handlers::HandlerRegistry;
use crate::sequence_cache::SequenceCache;

mod step_exec;
#[cfg(test)]
mod tests;

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

    process_signalled_instances(
        storage,
        handlers,
        sequence_cache,
        config.batch_size,
        &config.clock,
    )
    .await?;

    process_waiting_deadlines(
        storage,
        handlers,
        sequence_cache,
        &webhook_config,
        cancel,
        config.batch_size,
        &config.clock,
    )
    .await?;

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

    let scheduled_filter = InstanceFilter {
        states: Some(vec![InstanceState::Scheduled]),
        ..InstanceFilter::default()
    };
    let has_pending = storage
        .count_instances(&scheduled_filter)
        .await
        .unwrap_or(0)
        > 0;

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
#[allow(clippy::too_many_lines)]
pub async fn run_tick_loop(
    storage: Arc<dyn StorageBackend>,
    handlers: Arc<HandlerRegistry>,
    config: &SchedulerConfig,
    cancel: CancellationToken,
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

    // Wire the webhook outbox: exhausted deliveries are parked via storage and
    // can be redelivered through the API. Idempotent across restarts.
    crate::webhooks::init_outbox(Arc::clone(&storage), config.webhooks.clone());

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

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!("tick loop cancelled, draining in-flight tasks");
                let grace = Duration::from_secs(config.shutdown_grace_period_secs);
                if tokio::time::timeout(grace, wait_for_drain(&semaphore, max_concurrent)).await.is_err() {
                    warn!("grace period expired, some tasks may not have completed");
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
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
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
                )
                .await
                {
                    error!(error = %e, "signalled instance processing failed");
                }
                // Check SLA deadlines for waiting instances (external worker
                // dispatch). These instances are not claimed by the normal
                // tick, but deadlines should still fire.
                if let Err(e) = process_waiting_deadlines(
                    &storage,
                    &handlers,
                    &sequence_cache,
                    &webhook_config,
                    &cancel,
                    batch_size,
                    &config.clock,
                ).await {
                    error!(error = %e, "waiting deadline processing failed");
                }
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
            }
        }
    }
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
    let mut instances = enforce_concurrency_limits(ctx.storage, instances, ctx.clock).await?;

    let count = instances.len();
    tracing::Span::current().record("claimed", count);
    debug!(count = count, "claimed instances for processing");
    #[allow(clippy::cast_precision_loss)]
    crate::metrics::set_gauge(crate::metrics::QUEUE_DEPTH, count as f64);

    // Batch-fetch signals and completed block IDs for all claimed instances (2 queries total).
    let instance_ids: Vec<InstanceId> = instances.iter().map(|i| i.id).collect();
    let (signals_map, completed_map) = tokio::try_join!(
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
    )?;

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
                .conditional_update_instance_state(
                    instance.id,
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
            let heartbeat_handle = {
                let heartbeat_stop = heartbeat_stop.clone();
                // Tick at a fraction of the reaper's own staleness window so
                // several heartbeats land comfortably before this instance
                // could ever look stale.
                let heartbeat_interval =
                    Duration::from_secs(std::cmp::max(stale_instance_threshold_secs / 3, 5));
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(heartbeat_interval);
                    ticker.tick().await; // first tick fires immediately; claiming already set updated_at
                    loop {
                        tokio::select! {
                            () = heartbeat_stop.cancelled() => break,
                            _ = ticker.tick() => {
                                if let Err(e) = heartbeat_storage.heartbeat_instance(instance_id).await {
                                    warn!(instance_id = %instance_id, error = %e, "instance heartbeat failed");
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
            let result = tokio::spawn(async move {
                process_instance(
                    &s2,
                    &handlers,
                    &webhooks,
                    &seq_cache,
                    instance,
                    data,
                    externalize_threshold,
                    max_steps_per_instance,
                    &cancel,
                    &clock,
                )
                .await
            })
            .await;

            heartbeat_stop.cancel();
            let _ = heartbeat_handle.await;

            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) if e.is_transient_storage() => {
                    // A transient storage failure (connection loss, pool
                    // exhaustion, DB failover) is NOT a processing failure —
                    // failing the instance here would DLQ otherwise-healthy
                    // work on a momentary infrastructure blip. Reschedule it
                    // back to Scheduled so a later tick retries; the CAS guard
                    // (Running -> Scheduled) is a no-op if a concurrent writer
                    // already moved it.
                    warn!(
                        instance_id = %instance_id,
                        error = %e,
                        "instance processing hit a transient storage error -- rescheduling instead of failing"
                    );
                    if let Err(te) = storage
                        .conditional_update_instance_state(
                            instance_id,
                            InstanceState::Running,
                            InstanceState::Scheduled,
                            Some(clock_outer.now()),
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
                Ok(Err(e)) => {
                    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
                    error!(
                        instance_id = %instance_id,
                        error = %e,
                        "instance processing failed"
                    );
                    // Safety net: transition to Failed so the instance doesn't
                    // stay in Running forever and get re-claimed every tick.
                    if let Err(te) = storage
                        .conditional_update_instance_state(
                            instance_id,
                            InstanceState::Running,
                            InstanceState::Failed,
                            None,
                        )
                        .await
                    {
                        error!(
                            instance_id = %instance_id,
                            error = %te,
                            "failed to transition instance to Failed after processing error"
                        );
                    }
                }
                Err(join_err) => {
                    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
                    error!(
                        instance_id = %instance_id,
                        error = %join_err,
                        "instance processing panicked"
                    );
                    if let Err(te) = storage
                        .conditional_update_instance_state(
                            instance_id,
                            InstanceState::Running,
                            InstanceState::Failed,
                            None,
                        )
                        .await
                    {
                        error!(
                            instance_id = %instance_id,
                            error = %te,
                            "failed to transition instance to Failed after panic"
                        );
                    }
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
    let mut key_instances: HashMap<&str, Vec<usize>> = HashMap::with_capacity(instances.len() / 2);
    for (idx, inst) in instances.iter().enumerate() {
        if let (Some(key), Some(_max)) = (&inst.concurrency_key, inst.max_concurrency) {
            key_instances.entry(key.as_str()).or_default().push(idx);
        }
    }

    if key_instances.is_empty() {
        return Ok(instances);
    }

    // Batch count running instances for all concurrency keys in a single query.
    let keys: Vec<&str> = key_instances.keys().copied().collect();
    let running_counts = storage.count_running_by_concurrency_keys(&keys).await?;

    // For each concurrency key, determine how many slots are available.
    let mut deferred_indices = Vec::new();
    for (key, indices) in &key_instances {
        // All instances in the group share the same max_concurrency.
        let max = instances[indices[0]].max_concurrency.unwrap_or(u32::MAX);

        // Count how many instances with this key are currently Running in the
        // DB. This count includes the instances we just claimed (since
        // claim_due_instances already set them to Running). Subtract the batch
        // members to get the pre-existing running count.
        let total_running = running_counts.get(*key).copied().unwrap_or(0);
        #[allow(clippy::cast_possible_wrap)]
        let batch_count = indices.len() as i64;
        let already_running = total_running - batch_count;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let slots = i64::from(max).saturating_sub(already_running).max(0) as usize;

        // Keep the first `slots` instances (by batch order, which preserves
        // priority ordering from claim_due_instances), defer the rest.
        if slots < indices.len() {
            for &idx in &indices[slots..] {
                deferred_indices.push(idx);
            }
        }
    }

    if deferred_indices.is_empty() {
        return Ok(instances);
    }

    let defer_at = clock.now() + chrono::Duration::seconds(1);
    let mut deferred_ids: Vec<InstanceId> = Vec::with_capacity(deferred_indices.len());
    for &idx in &deferred_indices {
        let inst = &instances[idx];
        debug!(
            instance_id = %inst.id,
            concurrency_key = %inst.concurrency_key.as_deref().unwrap_or(""),
            "concurrency limit exceeded at claim time, deferring"
        );
        deferred_ids.push(inst.id);
    }
    storage
        .batch_reschedule_instances(&deferred_ids, defer_at)
        .await?;

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

/// Process signals for instances in paused/waiting state.
///
/// `claim_due_instances` only picks up `scheduled` instances. Signals (resume,
/// cancel, `update_context`) enqueued against paused or waiting instances would
/// sit unprocessed indefinitely without this sweep. The function is cheap when
/// no such signals exist (single indexed query returning empty).
async fn process_signalled_instances(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    sequence_cache: &SequenceCache,
    batch_size: u32,
    clock: &SharedClock,
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

        // Scheduled instances with pending signals just need a wake-up so the
        // normal tick picks them up immediately (check_human_input will consume
        // the signal). Skip the full signal processor to avoid invalid state
        // transitions.
        if current_state == InstanceState::Scheduled {
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
            continue;
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
            // whose terminal path runs the same hook.)
            if current_state != InstanceState::Cancelled
                && let Ok(Some(inst)) = storage.get_instance(instance_id).await
                && inst.state == InstanceState::Cancelled
                && let Ok(seq) = sequence_cache
                    .get_by_id(storage.as_ref(), inst.sequence_id)
                    .await
                && let Some(ref blocks) = seq.on_cancel
            {
                run_cleanup_hooks(storage, handlers, &inst, blocks, "on_cancel").await;
            }
        }
    }

    Ok(())
}

/// Max pages of Waiting instances swept for deadlines/timeouts per tick. Bounds
/// per-tick work while still covering a backlog larger than one batch (the old
/// single-page cap permanently starved instances sorted past `batch_size`).
const MAX_SWEEP_PAGES: u32 = 64;

/// Check SLA deadlines for instances stuck in `Waiting` state (dispatched to
/// external workers that never completed). Runs every tick with a bounded
/// query to avoid overhead when no deadlines are configured.
#[allow(clippy::too_many_lines)]
async fn process_waiting_deadlines(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    sequence_cache: &SequenceCache,
    webhook_config: &WebhookConfig,
    cancel: &CancellationToken,
    batch_size: u32,
    clock: &SharedClock,
) -> Result<(), EngineError> {
    use orch8_types::filter::{InstanceFilter, Pagination};

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
    // skip a few after wakes; those are picked up on the next tick
    // (self-correcting, and this sweep runs every tick).
    let mut offset: u64 = 0;
    for _page in 0..MAX_SWEEP_PAGES {
        let pagination = Pagination {
            offset,
            limit: batch_size,
            sort_ascending: true,
        };
        let waiting = storage.list_instances(&filter, &pagination).await?;
        let page_len = waiting.len();
        if waiting.is_empty() {
            // Idle / drained — skip the per-instance sequence-cache walk.
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
        let deadline_outputs = if deadline_keys.is_empty() {
            std::collections::HashMap::new()
        } else {
            storage.get_block_outputs_batch(&deadline_keys).await?
        };

        let mut deadline_outputs_ref =
            std::collections::HashMap::with_capacity(deadline_outputs.len());
        for (k, v) in &deadline_outputs {
            deadline_outputs_ref.insert((&k.0, &k.1), v);
        }

        for (instance, seq) in instance_sequences {
            let mut handled = false;
            for block in &seq.blocks {
                if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
                    if step_def.deadline.is_some() {
                        let prev = deadline_outputs_ref
                            .get(&(&instance.id, &step_def.id))
                            .copied();
                        if check_step_deadline_waiting(
                            storage,
                            handlers,
                            instance,
                            step_def,
                            webhook_config,
                            cancel,
                            prev,
                            clock,
                        )
                        .await?
                        {
                            handled = true;
                            break; // Instance was failed — no need to check more steps.
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
                            let elapsed = clock.now() - started;
                            if elapsed
                                > chrono::Duration::from_std(timeout)
                                    .unwrap_or(chrono::Duration::days(365))
                            {
                                debug!(
                                    instance_id = %instance.id,
                                    block_id = %step_def.id,
                                    "wait_for_input timeout expired, waking instance"
                                );
                                crate::lifecycle::transition_instance(
                                    storage.as_ref(),
                                    instance.id,
                                    Some(&instance.tenant_id),
                                    InstanceState::Waiting,
                                    InstanceState::Scheduled,
                                    Some(clock.now()),
                                )
                                .await?;
                                handled = true;
                                break;
                            }
                        }
                    }
                }
            }
            let _ = handled;
        }

        // A short page means we reached the end of the Waiting set; otherwise
        // advance the window to the next page. `MAX_SWEEP_PAGES` bounds the
        // per-tick work so a pathological backlog can't stall the tick loop.
        if u32::try_from(page_len).unwrap_or(u32::MAX) < batch_size {
            break;
        }
        offset += u64::from(batch_size);
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

/// Await a hook handler future with [`HOOK_DISPATCH_TIMEOUT`], mapping an
/// elapsed timeout to a `Permanent` error so callers treat it exactly like any
/// other swallowed hook failure.
async fn dispatch_hook_bounded<F>(
    fut: F,
) -> Result<serde_json::Value, orch8_types::error::StepError>
where
    F: std::future::Future<Output = Result<serde_json::Value, orch8_types::error::StepError>>,
{
    match tokio::time::timeout(HOOK_DISPATCH_TIMEOUT, fut).await {
        Ok(r) => r,
        Err(_) => Err(orch8_types::error::StepError::Permanent {
            message: "cleanup/escalation hook exceeded dispatch timeout".to_string(),
            details: None,
        }),
    }
}

/// Run a sequence's best-effort cleanup hook (`on_failure` / `on_cancel`) as
/// the instance reaches its terminal state. Each top-level `Step` block is
/// dispatched once with the instance's final context; its output is recorded
/// under the step's block id. Errors are **swallowed** — the instance is
/// already terminal and cleanup must never resurrect, wedge, or re-fail it.
/// Non-step blocks are skipped with a warning (v1 supports step hooks only).
#[allow(clippy::too_many_lines)]
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
        let ctx = crate::handlers::StepContext {
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step.id.clone(),
            params: step.params.clone(),
            context: Arc::new(instance.context.clone()),
            attempt: 0,
            storage: Arc::clone(storage),
            wait_for_input: None,
        };
        // Each handler invocation is bounded by HOOK_DISPATCH_TIMEOUT so a
        // hung cleanup hook cannot stall the tick loop it runs on.
        let result = if let Some(handler) = handlers.get(&step.handler) {
            dispatch_hook_bounded(handler(ctx)).await
        } else if crate::handlers::activepieces::is_ap_handler(&step.handler) {
            // ⚡ Bolt: Avoid cloning `step.handler` String just to pass a reference.
            dispatch_hook_bounded(crate::handlers::activepieces::handle_ap(ctx, &step.handler))
                .await
        } else if crate::handlers::grpc_plugin::is_grpc_handler(&step.handler) {
            let Some(endpoint) = crate::handlers::step_dispatch::resolve_plugin_source(
                storage.as_ref(),
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
                continue;
            };
            let mut ctx = ctx;
            ctx.params["_grpc_endpoint"] = serde_json::Value::String(endpoint);
            dispatch_hook_bounded(crate::handlers::grpc_plugin::handle_grpc_plugin(ctx)).await
        } else if let Some(plugin_name) =
            crate::handlers::wasm_plugin::is_wasm_handler(&step.handler)
                .then(|| crate::handlers::wasm_plugin::parse_plugin_name(&step.handler))
                .flatten()
        {
            let Some(wasm_path) = crate::handlers::step_dispatch::resolve_plugin_source(
                storage.as_ref(),
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
                continue;
            };
            dispatch_hook_bounded(crate::handlers::wasm_plugin::handle_wasm_plugin(
                ctx, &wasm_path,
            ))
            .await
        } else {
            warn!(
                instance_id = %instance.id,
                hook,
                handler = %step.handler,
                "cleanup handler not registered — skipping"
            );
            continue;
        };
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
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn process_sla_breaches(
    storage: &Arc<dyn StorageBackend>,
    sequence_cache: &SequenceCache,
    webhook_config: &WebhookConfig,
    cancel: &CancellationToken,
    batch_size: u32,
    clock: &SharedClock,
    cursor: &mut SlaSweepCursor,
    max_pages_per_tick: u32,
) -> Result<(), EngineError> {
    // A pending SLA alert: which instance, what kind, the sentinel block id used
    // for once-only de-dup, and the timing facts for the payload.
    struct Candidate {
        instance_id: InstanceId,
        tenant_id: orch8_types::ids::TenantId,
        sequence_id: orch8_types::ids::SequenceId,
        kind: &'static str,
        block_id: BlockId,
        elapsed_ms: i64,
        limit_ms: u64,
        step_id: Option<String>,
    }

    use orch8_types::filter::Pagination;

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

        let mut candidates: Vec<Candidate> = Vec::new();
        for instance in &page {
            // Pre-filter on the memo: instances whose sequence is already
            // known to carry no SLA are skipped without a sequence lookup.
            if matches!(
                cursor.sla_by_sequence.get(&instance.sequence_id),
                Some(false)
            ) {
                continue;
            }
            let Ok(seq) = sequence_cache
                .get_by_id(storage.as_ref(), instance.sequence_id)
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
                let limit =
                    chrono::Duration::from_std(max_runtime).unwrap_or(chrono::TimeDelta::MAX);
                if elapsed > limit {
                    candidates.push(Candidate {
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
                    candidates.push(Candidate {
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

        if !candidates.is_empty() {
            // One batch query for all sentinel keys: a present sentinel means
            // this breach already alerted and must not alert again.
            let keys: Vec<(InstanceId, &BlockId)> = candidates
                .iter()
                .map(|c| (c.instance_id, &c.block_id))
                .collect();
            let existing = storage.get_block_outputs_batch(&keys).await?;
            let mut existing_ref = std::collections::HashSet::with_capacity(existing.len());
            for k in existing.keys() {
                existing_ref.insert((&k.0, &k.1));
            }

            for c in &candidates {
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
                storage.save_block_output(&sentinel).await?;

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
                crate::webhooks::emit(webhook_config, &event, cancel).await;

                warn!(
                    instance_id = %c.instance_id,
                    sla_type = c.kind,
                    elapsed_ms = c.elapsed_ms,
                    limit_ms = c.limit_ms,
                    "SLA breach (alert-only)"
                );
            }
        }

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
) -> Result<bool, EngineError> {
    handle_deadline_breach(
        storage,
        handlers,
        instance,
        step_def,
        prev_output,
        InstanceState::Waiting,
        clock,
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
pub(crate) async fn handle_deadline_breach(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    prev_output: Option<&orch8_types::output::BlockOutput>,
    from_state: InstanceState,
    clock: &SharedClock,
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

    // Invoke escalation handler if configured.
    if let Some(ref escalation) = step_def.on_deadline_breach
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
            instance_id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step_def.id.clone(),
            params,
            context: Arc::new(instance.context.clone()),
            attempt: 0,
            storage: Arc::clone(storage),
            wait_for_input: None,
        };
        // Bounded: this escalation runs inline on the deadline sweep (tick
        // loop), so a hung escalation handler must not stall scheduling.
        if let Err(e) = dispatch_hook_bounded(handler(step_ctx)).await {
            warn!(instance_id = %instance_id, error = %e, "SLA escalation handler failed");
        }
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
    storage.save_block_output(&breach_output).await?;
    crate::lifecycle::transition_instance(
        storage.as_ref(),
        instance_id,
        Some(&instance.tenant_id),
        from_state,
        InstanceState::Failed,
        None,
    )
    .await?;
    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);

    // Wake parent: SLA deadline breach → terminal Failed.
    wake_parent_if_child(storage.as_ref(), instance).await;

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

/// Process a single claimed instance: execute ALL pending steps in one go.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn process_instance(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    sequence_cache: &SequenceCache,
    instance: orch8_types::instance::TaskInstance,
    prefetched: PrefetchedData,
    externalize_threshold: u32,
    max_steps_per_instance: u32,
    cancel: &CancellationToken,
    clock: &SharedClock,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    // Fetch sequence definition early — needed for cancellation scopes and evaluation.
    let sequence = sequence_cache
        .get_by_id(storage.as_ref(), instance.sequence_id)
        .await?;

    // claim_due_instances already set state to Running.
    // Ensure the run identity and start timestamp together. Retry/resume may
    // already have installed a fresh run_id; first execution creates one.
    // The scheduler clock is the baseline for timeout scheduling decisions.
    let missing_run_id = instance.context.runtime.run_id.is_none();
    let missing_started_at = instance.context.runtime.started_at.is_none();
    let instance = if missing_run_id || missing_started_at {
        let started_at = instance
            .context
            .runtime
            .started_at
            .unwrap_or_else(|| clock.now());
        let run_id = instance
            .context
            .runtime
            .run_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        storage
            .ensure_instance_run_started(instance_id, &run_id, started_at)
            .await?;
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

    // Process any pending signals (using pre-fetched data).
    if !prefetched.signals.is_empty() {
        let abort = crate::signals::process_signals_prefetched(
            storage.as_ref(),
            instance_id,
            InstanceState::Running,
            prefetched.signals,
            Some(&sequence),
        )
        .await?;
        if abort {
            return Ok(());
        }
    }

    // Budget enforcement: if the instance carries a budget and any configured
    // limit is already exceeded, pause it (pre-flight — before any step work
    // this tick). Instances without a budget take zero extra queries here.
    if step_exec::check_budget(storage, &instance).await? {
        return Ok(());
    }

    // Merge dynamically injected blocks with the sequence definition.
    let blocks = crate::evaluator::merged_blocks(storage.as_ref(), instance.id, &sequence).await?;

    // Fast path SLA deadline check for all steps BEFORE concurrency checks.
    // Batch-fetch any previous block outputs so the loop is N queries -> 1 query.
    // ⚡ Bolt: By passing &s.id as a reference to get_block_outputs_batch, we avoid
    // an unnecessary `.clone()` on the `BlockId` (String allocation) for every deadline step
    // inside this hot scheduler tick loop.
    let deadline_keys: Vec<(InstanceId, &BlockId)> = blocks
        .iter()
        .filter_map(|b| match b {
            orch8_types::sequence::BlockDefinition::Step(s) if s.deadline.is_some() => {
                Some((instance.id, &s.id))
            }
            _ => None,
        })
        .collect();
    let deadline_outputs = if deadline_keys.is_empty() {
        std::collections::HashMap::new()
    } else {
        storage.get_block_outputs_batch(&deadline_keys).await?
    };

    let mut deadline_outputs_ref = std::collections::HashMap::with_capacity(deadline_outputs.len());
    for (k, v) in &deadline_outputs {
        deadline_outputs_ref.insert((&k.0, &k.1), v);
    }

    for block in blocks.iter() {
        if let orch8_types::sequence::BlockDefinition::Step(step_def) = block
            && step_def.deadline.is_some()
            && !prefetched.completed_block_ids.contains(&step_def.id)
        {
            // SLA deadline check: if a previous attempt exists and the deadline has
            // been breached (wall-clock time since first attempt), fail the instance.
            let prev = deadline_outputs_ref
                .get(&(&instance.id, &step_def.id))
                .copied();
            if step_exec::check_step_deadline(storage, handlers, &instance, step_def, prev, clock)
                .await?
            {
                return Ok(());
            }
        }
    }

    // Concurrency control: if this instance has a concurrency key, check the limit.
    if let (Some(key), Some(max)) = (&instance.concurrency_key, instance.max_concurrency) {
        let position = storage.concurrency_position(instance_id, key).await?;
        if position > i64::from(max) {
            let defer_at = clock.now() + chrono::Duration::seconds(2);
            storage
                .conditional_update_instance_state(
                    instance_id,
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
            return Ok(());
        }
    }

    // Decide execution path: if the sequence has any composite (non-Step) blocks
    // or any plugin handlers (ap://, grpc://, wasm://), use the tree-based
    // evaluator. Plugin handlers are only dispatched correctly through the tree
    // evaluator's step_block.rs which knows about all plugin prefixes.
    let has_composite = blocks
        .iter()
        .any(|b| !matches!(b, orch8_types::sequence::BlockDefinition::Step(_)));

    let has_plugin_handler = blocks.iter().any(|b| {
        if let orch8_types::sequence::BlockDefinition::Step(step) = b {
            crate::handlers::activepieces::is_ap_handler(&step.handler)
                || crate::handlers::grpc_plugin::is_grpc_handler(&step.handler)
                || crate::handlers::wasm_plugin::is_wasm_handler(&step.handler)
        } else {
            false
        }
    });

    if has_composite || has_plugin_handler {
        return process_instance_tree(
            storage,
            handlers,
            webhook_config,
            &instance,
            &sequence,
            max_steps_per_instance,
            cancel,
            clock,
        )
        .await;
    }

    // Fast path: all blocks are Steps. Execute multi-block per claim cycle.
    let mut completed_blocks = prefetched.completed_block_ids;
    completed_blocks.sort_unstable();
    // ⚡ Bolt: Deduplicating the list of indices eliminates redundant comparisons
    // during the `binary_search` filtering phase, improving hot path performance.
    completed_blocks.dedup();

    for block in blocks.iter() {
        let orch8_types::sequence::BlockDefinition::Step(step_def) = block else {
            unreachable!("checked above: all blocks are steps");
        };

        if completed_blocks.binary_search(&step_def.id).is_ok() {
            continue;
        }

        // Interceptor: before_step
        if let Some(ref interceptors) = sequence.interceptors {
            crate::interceptors::emit_before_step(
                storage.as_ref(),
                interceptors,
                instance_id,
                &step_def.id,
            )
            .await;
        }

        let outcome = step_exec::execute_step_block(
            storage,
            handlers,
            webhook_config,
            externalize_threshold,
            &instance,
            step_def,
            cancel,
            clock,
        )
        .await?;

        // Interceptor: after_step (fires regardless of outcome)
        if let Some(ref interceptors) = sequence.interceptors {
            crate::interceptors::emit_after_step(
                storage.as_ref(),
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
                let total = storage.increment_total_steps(instance_id).await?;

                if max_steps_per_instance > 0 && total > max_steps_per_instance {
                    warn!(
                        instance_id = %instance_id,
                        total = total,
                        max = max_steps_per_instance,
                        "instance exceeded max_steps_per_instance, failing"
                    );
                    crate::lifecycle::transition_instance(
                        storage.as_ref(),
                        instance_id,
                        Some(&instance.tenant_id),
                        InstanceState::Running,
                        InstanceState::Failed,
                        None,
                    )
                    .await?;
                    return Ok(());
                }
                if cooperatively_preempt(storage, &instance, clock).await? {
                    return Ok(());
                }
            }
            StepOutcome::Failed => {
                // Best-effort cleanup: on_failure (fast-path step failure).
                if let Some(ref blocks) = sequence.on_failure {
                    run_cleanup_hooks(storage, handlers, &instance, blocks, "on_failure").await;
                }
                // Interceptor: on_failure
                if let Some(ref interceptors) = sequence.interceptors {
                    crate::interceptors::emit_on_failure(
                        storage.as_ref(),
                        interceptors,
                        instance_id,
                    )
                    .await;
                }
                return Ok(());
            }
            StepOutcome::Deferred => {
                return Ok(());
            }
            StepOutcome::Skipped => {
                if let Err(pos) = completed_blocks.binary_search(&step_def.id) {
                    completed_blocks.insert(pos, step_def.id.clone());
                }
                if cooperatively_preempt(storage, &instance, clock).await? {
                    return Ok(());
                }
            }
        }
    }

    // All blocks completed.
    //
    // Emit the `on_complete` trace BEFORE flipping the instance to
    // `Completed`. Clients (tests, dashboards, workers) commonly observe
    // the state change and immediately read outputs; if the trace were
    // persisted afterwards, those clients would race the second write and
    // occasionally see `Completed` with no `_interceptor:on_complete`
    // row. Interceptor saves are non-fatal internally, so ordering them
    // ahead of the state transition doesn't risk blocking completion.
    if let Some(ref interceptors) = sequence.interceptors {
        crate::interceptors::emit_on_complete(storage.as_ref(), interceptors, instance_id).await;
    }

    let completed_event =
        crate::webhooks::instance_event("instance.completed", instance_id, serde_json::json!({}));
    crate::lifecycle::transition_instance_with_webhook(
        storage.as_ref(),
        instance_id,
        Some(&instance.tenant_id),
        InstanceState::Running,
        InstanceState::Completed,
        None,
        webhook_config,
        &completed_event,
    )
    .await?;

    crate::metrics::inc(crate::metrics::INSTANCES_COMPLETED);

    info!(instance_id = %instance_id, "instance completed all blocks");

    // Wake parent if this is a sub-sequence child.
    wake_parent_if_child(storage.as_ref(), &instance).await;

    Ok(())
}

/// Yield a scheduler slot at a durable block boundary when higher-priority
/// work is ready. This is intentionally cooperative: aborting an in-flight
/// handler could duplicate an external effect or discard an uncommitted
/// result. The completed block output is already durable before this runs.
async fn cooperatively_preempt(
    storage: &Arc<dyn StorageBackend>,
    instance: &orch8_types::instance::TaskInstance,
    clock: &SharedClock,
) -> Result<bool, EngineError> {
    let now = clock.now();
    if !storage
        .has_due_higher_priority(now, instance.priority)
        .await?
    {
        return Ok(false);
    }
    let yielded = storage
        .conditional_update_instance_state(
            instance.id,
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
                debug!(
                    parent_id = %parent_id,
                    child_id = %instance.id,
                    "parent no longer in Waiting state, skipping wake"
                );
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

/// Process an instance using the tree-based evaluator for composite blocks.
///
/// The evaluator manages an execution tree (persisted in DB) and dispatches each
/// node to its block-type handler (`Parallel`, `Race`, `Loop`, `ForEach`, `Router`, `TryCatch`, `Step`).
/// The evaluator returns an `EvalOutcome` carrying the tree state so we can
/// transition the instance without re-reading the tree from the database.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn process_instance_tree(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    max_steps_per_instance: u32,
    _cancel: &CancellationToken,
    clock: &SharedClock,
) -> Result<(), EngineError> {
    use crate::evaluator::EvalOutcome;
    let instance_id = instance.id;

    if max_steps_per_instance > 0 {
        let fresh = storage.get_instance(instance_id).await?;
        if let Some(inst) = fresh
            && inst.context.runtime.total_steps_executed > max_steps_per_instance
        {
            warn!(
                instance_id = %instance_id,
                total = inst.context.runtime.total_steps_executed,
                max = max_steps_per_instance,
                "instance exceeded max_steps_per_instance (tree path), failing"
            );
            crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                InstanceState::Running,
                InstanceState::Failed,
                None,
            )
            .await?;
            return Ok(());
        }
    }

    match crate::evaluator::evaluate(storage, handlers, instance, sequence).await {
        Ok(EvalOutcome::MoreWork { has_waiting_nodes }) => {
            // More work — if nodes are waiting for external workers, transition
            // to Waiting (the worker completion callback will re-schedule).
            // Otherwise, re-schedule for the next tick.
            //
            // But: if a step handler drove a pause/cancel transition mid-tick
            // (see `handle_sleep` in `handlers/builtin.rs`), the instance may
            // already be in `Paused` or `Cancelled` when we land here — we
            // must NOT overwrite that with `Scheduled`, or the observable
            // state would flip right back to running.
            let current = storage
                .get_instance(instance_id)
                .await?
                .map_or(InstanceState::Running, |i| i.state);
            if matches!(
                current,
                InstanceState::Paused | InstanceState::Cancelled | InstanceState::Failed
            ) {
                // Terminal/paused — leave state as-is.
            } else if has_waiting_nodes {
                if let Err(e) = crate::lifecycle::transition_instance(
                    storage.as_ref(),
                    instance_id,
                    Some(&instance.tenant_id),
                    InstanceState::Running,
                    InstanceState::Waiting,
                    None,
                )
                .await
                {
                    if !matches!(e, crate::error::EngineError::InvalidTransition { .. }) {
                        return Err(e);
                    }
                    debug!(
                        instance_id = %instance_id,
                        "concurrent writer moved instance before Running->Waiting transition"
                    );
                }
            } else if let Err(e) = crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                current,
                InstanceState::Scheduled,
                Some(clock.now()),
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
        }
        Ok(EvalOutcome::Done {
            any_failed,
            any_cancelled,
        }) => {
            // Re-read current state so we use a CAS transition instead of
            // unconditional overwrite — protects against concurrent pause/
            // cancel/fail that arrived mid-evaluation.
            let current = storage
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
            let target = if any_cancelled && !any_failed {
                InstanceState::Cancelled
            } else if any_failed {
                InstanceState::Failed
            } else {
                InstanceState::Completed
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
                crate::lifecycle::transition_instance_with_webhook(
                    storage.as_ref(),
                    instance_id,
                    Some(&instance.tenant_id),
                    current,
                    target,
                    None,
                    webhook_config,
                    event,
                )
                .await
            } else {
                crate::lifecycle::transition_instance(
                    storage.as_ref(),
                    instance_id,
                    Some(&instance.tenant_id),
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
                match target {
                    InstanceState::Cancelled => {
                        // Best-effort cleanup: on_cancel.
                        if let Some(ref blocks) = sequence.on_cancel {
                            run_cleanup_hooks(storage, handlers, instance, blocks, "on_cancel")
                                .await;
                        }
                        info!(instance_id = %instance_id, "instance cancelled (tree evaluation)");
                    }
                    InstanceState::Failed => {
                        // Best-effort cleanup: on_failure.
                        if let Some(ref blocks) = sequence.on_failure {
                            run_cleanup_hooks(storage, handlers, instance, blocks, "on_failure")
                                .await;
                        }
                        // Interceptor: on_failure
                        if let Some(ref interceptors) = sequence.interceptors {
                            crate::interceptors::emit_on_failure(
                                storage.as_ref(),
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
                                storage.as_ref(),
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

            // Wake parent if this is a sub-sequence child that reached a terminal state.
            wake_parent_if_child(storage.as_ref(), instance).await;
        }
        Err(e) => {
            error!(instance_id = %instance_id, error = %e, "tree evaluation failed");
            crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                InstanceState::Running,
                InstanceState::Failed,
                None,
            )
            .await?;
            // Best-effort cleanup: on_failure (tree evaluation blew up).
            if let Some(ref blocks) = sequence.on_failure {
                run_cleanup_hooks(storage, handlers, instance, blocks, "on_failure").await;
            }
            // Interceptor: on_failure
            if let Some(ref interceptors) = sequence.interceptors {
                crate::interceptors::emit_on_failure(storage.as_ref(), interceptors, instance_id)
                    .await;
            }
            crate::metrics::inc(crate::metrics::INSTANCES_FAILED);

            // Wake parent if this child's tree evaluation blew up mid-flight —
            // otherwise a SubSequence parent would remain Waiting forever,
            // since the Done branch above is the only other place that
            // calls `wake_parent_if_child`.
            wake_parent_if_child(storage.as_ref(), instance).await;
        }
    }

    Ok(())
}

// `get_sequence_cached` was replaced by `SequenceCache::get_by_id`, which
// unifies id/name lookups and centralizes invalidation.
