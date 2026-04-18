use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use moka::future::Cache;
use tokio::sync::Semaphore;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::config::{SchedulerConfig, WebhookConfig};
use orch8_types::ids::{BlockId, InstanceId, SequenceId};
use orch8_types::instance::InstanceState;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::Signal;

use crate::error::EngineError;
use crate::handlers::HandlerRegistry;

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
    let tick_duration = Duration::from_millis(config.tick_interval_ms);
    let batch_size = config.batch_size;
    let max_concurrent = config.max_concurrent_steps as usize;
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    // In-memory LRU cache for sequence definitions (avoids 1 DB query per instance).
    let sequence_cache: Arc<Cache<SequenceId, Arc<SequenceDefinition>>> = Arc::new(
        Cache::builder()
            .max_capacity(1_000)
            .time_to_live(Duration::from_mins(5))
            .build(),
    );

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
                if let Err(e) = process_tick(
                    &storage,
                    &handlers,
                    &semaphore,
                    batch_size,
                    config.max_instances_per_tenant,
                    &webhook_config,
                    &sequence_cache,
                    externalize_threshold,
                    &cancel,
                ).await {
                    error!(error = %e, "tick processing failed");
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
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(batch_size = batch_size, claimed = tracing::field::Empty))]
async fn process_tick(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    semaphore: &Arc<Semaphore>,
    batch_size: u32,
    max_per_tenant: u32,
    webhook_config: &Arc<WebhookConfig>,
    sequence_cache: &Arc<Cache<SequenceId, Arc<SequenceDefinition>>>,
    externalize_threshold: u32,
    cancel: &CancellationToken,
) -> Result<(), EngineError> {
    let _tick_timer = crate::metrics::Timer::start(crate::metrics::TICK_DURATION);

    let available = semaphore.available_permits();
    if available == 0 {
        return Ok(());
    }
    let fetch_limit = std::cmp::min(batch_size, u32::try_from(available).unwrap_or(u32::MAX));

    let now = Utc::now();
    let mut instances = storage
        .claim_due_instances(now, fetch_limit, max_per_tenant)
        .await?;

    if instances.is_empty() {
        return Ok(());
    }

    let count = instances.len();
    tracing::Span::current().record("claimed", count);
    debug!(count = count, "claimed instances for processing");
    #[allow(clippy::cast_precision_loss)]
    crate::metrics::set_gauge(crate::metrics::QUEUE_DEPTH, count as f64);

    // Batch-fetch signals and completed block IDs for all claimed instances (2 queries total).
    let instance_ids: Vec<InstanceId> = instances.iter().map(|i| i.id).collect();
    let (signals_map, completed_map) = tokio::try_join!(
        async {
            storage
                .get_pending_signals_batch(&instance_ids)
                .await
                .map_err(EngineError::from)
        },
        async {
            storage
                .get_completed_block_ids_batch(&instance_ids)
                .await
                .map_err(EngineError::from)
        },
    )?;

    // Hydrate externalized `context.data` markers for every claimed
    // instance in one batched fetch. This removes the per-step N+1 that
    // would otherwise occur inside `resolve_markers`. Storage errors are
    // non-fatal: the per-step resolver remains the correctness fallback.
    crate::preload::preload_externalized_markers(storage.as_ref(), &mut instances).await;

    // Wrap in Arc so we can share cheaply across spawned tasks.
    let prefetched = Arc::new(build_prefetch_map(signals_map, completed_map));

    for instance in instances {
        let storage = Arc::clone(storage);
        let handlers = Arc::clone(handlers);
        let semaphore = Arc::clone(semaphore);
        let webhooks = Arc::clone(webhook_config);
        let seq_cache = Arc::clone(sequence_cache);
        let prefetched = Arc::clone(&prefetched);
        let cancel = cancel.clone();

        crate::metrics::inc(crate::metrics::INSTANCES_CLAIMED);

        tokio::spawn(async move {
            // Acquire semaphore permit to bound concurrency.
            let Ok(_permit) = semaphore.acquire().await else {
                error!("semaphore closed unexpectedly");
                return;
            };

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

            match process_instance(
                &storage,
                &handlers,
                &webhooks,
                &seq_cache,
                instance,
                data,
                externalize_threshold,
                &cancel,
            )
            .await
            {
                Ok(()) => {}
                Err(e) => {
                    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
                    error!(
                        instance_id = %instance_id,
                        error = %e,
                        "instance processing failed"
                    );
                }
            }
        });
    }

    Ok(())
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
}

/// Process a single claimed instance: execute ALL pending steps in one go.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn process_instance(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    sequence_cache: &Cache<SequenceId, Arc<SequenceDefinition>>,
    instance: orch8_types::instance::TaskInstance,
    prefetched: PrefetchedData,
    externalize_threshold: u32,
    cancel: &CancellationToken,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    // Fetch sequence definition early — needed for cancellation scopes and evaluation.
    let sequence =
        get_sequence_cached(storage.as_ref(), sequence_cache, instance.sequence_id).await?;

    // claim_due_instances already set state to Running.
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

    // Merge dynamically injected blocks with the sequence definition.
    let blocks = crate::evaluator::merged_blocks(storage.as_ref(), instance.id, &sequence).await?;

    // Fast path SLA deadline check for all steps BEFORE concurrency checks.
    // This prevents SLA breaches from being ignored while an instance is artificially
    // deferred due to rate / concurrency limits.
    for block in blocks.iter() {
        if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
            if !prefetched
                .completed_block_ids
                .iter()
                .any(|id| id == &step_def.id)
            {
                // SLA deadline check: if a previous attempt exists and the deadline has
                // been breached (wall-clock time since first attempt), fail the instance.
                if check_step_deadline(storage, handlers, &instance, step_def).await? {
                    return Ok(());
                }
            }
        }
    }

    // Concurrency control: if this instance has a concurrency key, check the limit.
    if let (Some(ref key), Some(max)) = (&instance.concurrency_key, instance.max_concurrency) {
        let position = storage.concurrency_position(instance_id, key).await?;
        if position > i64::from(max) {
            let defer_at = Utc::now() + chrono::Duration::seconds(2);
            storage
                .update_instance_state(instance_id, InstanceState::Scheduled, Some(defer_at))
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

    // Decide execution path: if the sequence has any composite (non-Step) blocks,
    // use the tree-based evaluator. Otherwise, use the fast step-only loop.
    let has_composite = blocks
        .iter()
        .any(|b| !matches!(b, orch8_types::sequence::BlockDefinition::Step(_)));

    if has_composite {
        return process_instance_tree(
            storage,
            handlers,
            webhook_config,
            &instance,
            &sequence,
            cancel,
        )
        .await;
    }

    // Fast path: all blocks are Steps. Execute multi-block per claim cycle.
    //
    // Typical sequences have a handful of blocks; a linear scan over a `Vec`
    // is cheaper than the HashSet allocation + hashing overhead that a
    // `HashSet<String>` would incur on every instance tick.
    let mut completed_blocks: Vec<BlockId> = prefetched.completed_block_ids;

    for block in blocks.iter() {
        let orch8_types::sequence::BlockDefinition::Step(step_def) = block else {
            unreachable!("checked above: all blocks are steps");
        };

        if completed_blocks.iter().any(|id| id == &step_def.id) {
            continue;
        }

        match execute_step_block(
            storage,
            handlers,
            webhook_config,
            externalize_threshold,
            &instance,
            step_def,
            cancel,
        )
        .await?
        {
            StepOutcome::Completed => {
                completed_blocks.push(step_def.id.clone());
            }
            StepOutcome::Deferred | StepOutcome::Failed => {
                return Ok(());
            }
        }
    }

    // All blocks completed.
    crate::lifecycle::transition_instance(
        storage.as_ref(),
        instance_id,
        InstanceState::Running,
        InstanceState::Completed,
        None,
    )
    .await?;

    crate::metrics::inc(crate::metrics::INSTANCES_COMPLETED);
    crate::webhooks::emit(
        webhook_config,
        &crate::webhooks::instance_event("instance.completed", instance_id, serde_json::json!({})),
        cancel,
    );

    info!(instance_id = %instance_id, "instance completed all blocks");
    Ok(())
}

/// Process an instance using the tree-based evaluator for composite blocks.
///
/// The evaluator manages an execution tree (persisted in DB) and dispatches each
/// node to its block-type handler (`Parallel`, `Race`, `Loop`, `ForEach`, `Router`, `TryCatch`, `Step`).
/// Returns `true` from `evaluate()` when more work remains (re-schedule), `false` when done.
async fn process_instance_tree(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    cancel: &CancellationToken,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    match crate::evaluator::evaluate(storage, handlers, instance, sequence).await {
        Ok(true) => {
            // More work — check if the tree has nodes waiting for external workers.
            // If so, transition to Waiting (the worker completion callback will
            // re-schedule). Otherwise, re-schedule for the next tick.
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
            } else {
                let tree = storage.get_execution_tree(instance_id).await?;
                if crate::evaluator::has_waiting_nodes(&tree) {
                    crate::lifecycle::transition_instance(
                        storage.as_ref(),
                        instance_id,
                        InstanceState::Running,
                        InstanceState::Waiting,
                        None,
                    )
                    .await?;
                } else {
                    storage
                        .update_instance_state(
                            instance_id,
                            InstanceState::Scheduled,
                            Some(Utc::now()),
                        )
                        .await?;
                }
            }
        }
        Ok(false) => {
            // Evaluator says done. Check if any root node failed or was cancelled.
            let tree = storage.get_execution_tree(instance_id).await?;
            let root_failed = tree
                .iter()
                .filter(|n| n.parent_id.is_none())
                .any(|n| matches!(n.state, orch8_types::execution::NodeState::Failed));
            let root_cancelled = tree
                .iter()
                .filter(|n| n.parent_id.is_none())
                .any(|n| matches!(n.state, orch8_types::execution::NodeState::Cancelled));

            // Read current instance state — may differ from Running if an external
            // worker dispatch changed it to Waiting within this tick.
            let current_state = storage
                .get_instance(instance_id)
                .await?
                .map_or(InstanceState::Running, |i| i.state);

            // Distinguish user-initiated cancel from genuine failure: a
            // Cancelled root node (and no Failed root) means the instance
            // was cancelled, not that a step failed. Preserve the Cancelled
            // terminal state for the instance so callers can tell the two
            // apart.
            if root_cancelled && !root_failed {
                storage
                    .update_instance_state(instance_id, InstanceState::Cancelled, None)
                    .await?;
                info!(instance_id = %instance_id, from = %current_state, "instance cancelled (tree evaluation)");
            } else if root_failed {
                // Use update_instance_state directly to handle any current state.
                storage
                    .update_instance_state(instance_id, InstanceState::Failed, None)
                    .await?;
                crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
                crate::webhooks::emit(
                    webhook_config,
                    &crate::webhooks::instance_event(
                        "instance.failed",
                        instance_id,
                        serde_json::json!({}),
                    ),
                    cancel,
                );
                info!(instance_id = %instance_id, from = %current_state, "instance failed (tree evaluation)");
            } else {
                storage
                    .update_instance_state(instance_id, InstanceState::Completed, None)
                    .await?;
                crate::metrics::inc(crate::metrics::INSTANCES_COMPLETED);
                crate::webhooks::emit(
                    webhook_config,
                    &crate::webhooks::instance_event(
                        "instance.completed",
                        instance_id,
                        serde_json::json!({}),
                    ),
                    cancel,
                );
                info!(instance_id = %instance_id, from = %current_state, "instance completed (tree evaluation)");
            }
        }
        Err(e) => {
            error!(instance_id = %instance_id, error = %e, "tree evaluation failed");
            crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                InstanceState::Running,
                InstanceState::Failed,
                None,
            )
            .await?;
            crate::metrics::inc(crate::metrics::INSTANCES_FAILED);
        }
    }

    Ok(())
}

/// Fetch a sequence definition, using the in-memory cache when available.
///
/// Emits `orch8_cache_hits_total` / `orch8_cache_misses_total` labelled with
/// `cache="sequence"` so operators can monitor the hit ratio and tune `max_capacity` / TTL.
async fn get_sequence_cached(
    storage: &dyn StorageBackend,
    cache: &Cache<SequenceId, Arc<SequenceDefinition>>,
    sequence_id: SequenceId,
) -> Result<Arc<SequenceDefinition>, EngineError> {
    if let Some(seq) = cache.get(&sequence_id).await {
        crate::metrics::inc_with(
            crate::metrics::CACHE_HITS,
            &[("cache", "sequence".to_string())],
        );
        return Ok(seq);
    }

    crate::metrics::inc_with(
        crate::metrics::CACHE_MISSES,
        &[("cache", "sequence".to_string())],
    );

    let seq = storage
        .get_sequence(sequence_id)
        .await?
        .ok_or_else(|| EngineError::StepFailed {
            instance_id: orch8_types::ids::InstanceId(uuid::Uuid::nil()),
            block_id: orch8_types::ids::BlockId("_root".into()),
            message: format!("sequence {sequence_id} not found"),
            retryable: false,
        })?;

    let seq = Arc::new(seq);
    cache.insert(sequence_id, Arc::clone(&seq)).await;
    Ok(seq)
}

/// Check if a step's SLA deadline has been breached (fast path).
/// Looks at the previous attempt's `created_at` as the start time. If the deadline
/// has elapsed, invokes the escalation handler, records the breach, and fails the instance.
/// Returns `true` if the deadline was breached and the instance was failed.
async fn check_step_deadline(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<bool, EngineError> {
    let Some(deadline) = step_def.deadline else {
        return Ok(false);
    };
    let Some(prev_output) = storage.get_block_output(instance.id, &step_def.id).await? else {
        return Ok(false);
    };
    let elapsed = Utc::now() - prev_output.created_at;
    if elapsed < chrono::Duration::from_std(deadline).unwrap_or(chrono::TimeDelta::MAX) {
        return Ok(false);
    }

    let instance_id = instance.id;
    warn!(
        instance_id = %instance_id,
        block_id = %step_def.id,
        deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
        elapsed_ms = elapsed.num_milliseconds(),
        "SLA deadline breached (fast path)"
    );

    // Invoke escalation handler if configured.
    if let Some(ref escalation) = step_def.on_deadline_breach {
        if let Some(handler) = handlers.get(&escalation.handler) {
            let mut params = escalation.params.clone();
            if let serde_json::Value::Object(ref mut map) = params {
                map.insert("_breach_block_id".into(), serde_json::json!(step_def.id.0));
                map.insert(
                    "_breach_instance_id".into(),
                    serde_json::json!(instance_id.0),
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
                context: instance.context.clone(),
                attempt: 0,
                storage: Arc::clone(storage),
            };
            if let Err(e) = handler(step_ctx).await {
                warn!(
                    instance_id = %instance_id,
                    block_id = %step_def.id,
                    error = %e,
                    "SLA escalation handler failed"
                );
            }
        }
    }

    // Record breach output and fail the instance.
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
        attempt: prev_output.attempt,
        created_at: Utc::now(),
    };
    storage.save_block_output(&breach_output).await?;
    crate::lifecycle::transition_instance(
        storage.as_ref(),
        instance_id,
        InstanceState::Running,
        InstanceState::Failed,
        None,
    )
    .await?;
    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);

    Ok(true)
}

/// Check if the step has a delay and defer if so. Returns `true` if deferred.
async fn check_step_delay(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<bool, EngineError> {
    let Some(delay) = &step_def.delay else {
        return Ok(false);
    };

    let fire_at = crate::scheduling::delay::calculate_next_fire_at(
        Utc::now(),
        delay,
        &instance.timezone,
        Some(&instance.context.config),
    );

    crate::lifecycle::transition_instance(
        storage,
        instance.id,
        InstanceState::Running,
        InstanceState::Scheduled,
        Some(fire_at),
    )
    .await?;

    debug!(
        instance_id = %instance.id,
        block_id = %step_def.id,
        fire_at = %fire_at,
        "step delayed, re-scheduling instance"
    );
    Ok(true)
}

/// Check if the step has a send window and defer if outside it. Returns `true` if deferred.
async fn check_send_window(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<bool, EngineError> {
    let Some(window) = &step_def.send_window else {
        return Ok(false);
    };

    let Some(next_open) =
        crate::scheduling::send_window::check_window(Utc::now(), window, &instance.timezone)
    else {
        return Ok(false); // Inside window
    };

    crate::lifecycle::transition_instance(
        storage,
        instance.id,
        InstanceState::Running,
        InstanceState::Scheduled,
        Some(next_open),
    )
    .await?;

    debug!(
        instance_id = %instance.id,
        block_id = %step_def.id,
        next_open = %next_open,
        "step outside send window, deferring"
    );
    Ok(true)
}

/// Check rate limit for this step. Returns `true` if rate-limited and deferred.
async fn check_step_rate_limit(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<bool, EngineError> {
    let Some(key) = &step_def.rate_limit_key else {
        return Ok(false);
    };

    let resource_key = orch8_types::ids::ResourceKey(key.clone());
    let check = storage
        .check_rate_limit(&instance.tenant_id, &resource_key, Utc::now())
        .await?;

    if let orch8_types::rate_limit::RateLimitCheck::Exceeded { retry_after } = check {
        info!(
            instance_id = %instance.id,
            block_id = %step_def.id,
            resource_key = %key,
            retry_after = %retry_after,
            "rate limit exceeded, deferring instance"
        );

        crate::metrics::inc(crate::metrics::RATE_LIMITS_EXCEEDED);

        crate::lifecycle::transition_instance(
            storage,
            instance.id,
            InstanceState::Running,
            InstanceState::Scheduled,
            Some(retry_after),
        )
        .await?;
        return Ok(true);
    }
    Ok(false)
}

/// Check if a human-in-the-loop step has received its input signal.
/// If the response signal exists, stores it as block output and returns `false` (continue).
/// If not received yet, pauses the instance and returns `true` (deferred).
async fn check_human_input(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    human_def: &orch8_types::sequence::HumanInputDef,
) -> Result<bool, EngineError> {
    let signal_name = format!("human_input:{}", step_def.id.0);

    // Check if the response signal has already been delivered.
    let signals = storage.get_pending_signals(instance.id).await?;
    for signal in &signals {
        if let orch8_types::signal::SignalType::Custom(name) = &signal.signal_type {
            if *name == signal_name {
                // Human responded — store payload as block output and continue execution.
                let output = orch8_types::output::BlockOutput {
                    id: uuid::Uuid::now_v7(),
                    instance_id: instance.id,
                    block_id: step_def.id.clone(),
                    output: signal.payload.clone(),
                    output_ref: None,
                    output_size: serde_json::to_vec(&signal.payload)
                        .map_or(0, |v| i32::try_from(v.len()).unwrap_or(i32::MAX)),
                    attempt: 0,
                    created_at: chrono::Utc::now(),
                };
                storage.save_block_output(&output).await?;
                storage.mark_signal_delivered(signal.id).await?;
                debug!(
                    instance_id = %instance.id,
                    block_id = %step_def.id,
                    "human input received"
                );
                return Ok(false); // Continue execution
            }
        }
    }

    // No response yet. Check timeout.
    if let Some(timeout) = human_def.timeout {
        if let Some(started) = instance.context.runtime.started_at {
            let elapsed = chrono::Utc::now() - started;
            if elapsed > chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::days(365))
            {
                // Timeout expired — fail or escalate.
                if let Some(ref escalation) = human_def.escalation_handler {
                    debug!(
                        instance_id = %instance.id,
                        block_id = %step_def.id,
                        escalation = %escalation,
                        "human input timeout, escalating"
                    );
                    // Store escalation marker as output so the next step can handle it.
                    let output = orch8_types::output::BlockOutput {
                        id: uuid::Uuid::now_v7(),
                        instance_id: instance.id,
                        block_id: step_def.id.clone(),
                        output: serde_json::json!({
                            "_escalated": true,
                            "_escalation_handler": escalation,
                            "_timeout_seconds": timeout.as_secs()
                        }),
                        output_ref: None,
                        output_size: 0,
                        attempt: 0,
                        created_at: chrono::Utc::now(),
                    };
                    storage.save_block_output(&output).await?;
                    return Ok(false); // Continue — escalation handler proceeds
                }
                return Err(EngineError::StepTimeout {
                    block_id: step_def.id.clone(),
                    timeout,
                });
            }
        }
    }

    // Still waiting — re-schedule the instance for a future tick.
    let check_interval = chrono::Duration::seconds(5);
    let next_check = chrono::Utc::now() + check_interval;
    crate::lifecycle::transition_instance(
        storage,
        instance.id,
        InstanceState::Running,
        InstanceState::Scheduled,
        Some(next_check),
    )
    .await?;

    debug!(
        instance_id = %instance.id,
        block_id = %step_def.id,
        "waiting for human input, deferring"
    );
    Ok(true) // Deferred
}

/// Execute a single step block within an instance.
/// Returns `StepOutcome` wrapped in `Result` — the outcome tells the caller
/// whether to continue executing more blocks or stop.
#[allow(clippy::too_many_lines)]
async fn execute_step_block(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    externalize_threshold: u32,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    cancel: &CancellationToken,
) -> Result<StepOutcome, EngineError> {
    let instance_id = instance.id;

    // Debug mode: if instance has breakpoints set and this step is in the list, pause.
    if let Some(breakpoints) = instance.metadata.get("_debug_breakpoints") {
        if let Some(arr) = breakpoints.as_array() {
            if arr.iter().any(|v| v.as_str() == Some(&step_def.id.0)) {
                debug!(
                    instance_id = %instance_id,
                    block_id = %step_def.id,
                    "debug breakpoint hit, pausing instance"
                );
                crate::lifecycle::transition_instance(
                    storage.as_ref(),
                    instance_id,
                    InstanceState::Running,
                    InstanceState::Paused,
                    None,
                )
                .await?;
                return Ok(StepOutcome::Deferred);
            }
        }
    }

    if check_step_delay(storage.as_ref(), instance, step_def).await? {
        return Ok(StepOutcome::Deferred);
    }

    if check_send_window(storage.as_ref(), instance, step_def).await? {
        return Ok(StepOutcome::Deferred);
    }

    if check_step_rate_limit(storage.as_ref(), instance, step_def).await? {
        return Ok(StepOutcome::Deferred);
    }

    // Human-in-the-loop: if this step waits for human input, check if a response
    // signal has been delivered. If not, pause the instance.
    if let Some(human_def) = &step_def.wait_for_input {
        if check_human_input(storage.as_ref(), instance, step_def, human_def).await? {
            return Ok(StepOutcome::Deferred);
        }
    }

    // Determine attempt number from previous output for this block.
    // Skip the DB lookup on the first attempt (attempt=0, no prior output exists).
    let attempt = if let Some(prev) = storage.get_block_output(instance_id, &step_def.id).await? {
        u32::from(prev.attempt.unsigned_abs()) + 1
    } else {
        0
    };

    // Build the step's context snapshot once and reuse for both template
    // resolution and handler dispatch. Mirrors the tree-evaluator path in
    // `handlers::step_block::execute_step_node` so both dispatch sites apply
    // the same `context_access` filtering and externalization-marker
    // inflation before running user templates.
    let step_context =
        crate::handlers::step_block::context_for_step(storage.as_ref(), instance, step_def).await?;

    // Resolve `{{path}}` templates first, then `credentials://` refs. Both
    // dispatch branches below (external worker + in-process handler) must
    // see fully materialised params — external workers have no access to
    // the credential registry and raw `{{…}}` would leak to user handlers.
    // Template errors (unknown root/section) fail the instance the same way
    // credential errors do.
    let mut resolved_params = match crate::handlers::param_resolve::resolve_templates_in_params(
        storage.as_ref(),
        instance,
        &step_context,
        step_def.params.clone(),
    )
    .await
    {
        Ok(p) => p,
        Err(tpl_err) => {
            tracing::error!(
                instance_id = %instance_id,
                block_id = %step_def.id,
                error = %tpl_err,
                "failed to resolve templates for step"
            );
            return fail_instance_with_error(
                storage.as_ref(),
                instance_id,
                webhook_config,
                cancel,
                &tpl_err.to_string(),
            )
            .await;
        }
    };

    if let Err(step_err) = crate::credentials::resolve_in_value(
        storage.as_ref(),
        &instance.tenant_id.0,
        &mut resolved_params,
    )
    .await
    {
        tracing::warn!(
            instance_id = %instance_id,
            block_id = %step_def.id,
            error = ?step_err,
            "failed to resolve credentials for step"
        );
        return fail_instance_with_error(
            storage.as_ref(),
            instance_id,
            webhook_config,
            cancel,
            &step_err.to_string(),
        )
        .await;
    }

    // If the handler is not registered in-process, dispatch to external worker queue.
    if !handlers.contains(&step_def.handler) {
        return dispatch_to_external_worker(
            storage.as_ref(),
            instance,
            step_def,
            attempt,
            resolved_params,
            step_context,
        )
        .await;
    }

    crate::metrics::inc(crate::metrics::STEPS_EXECUTED);

    let exec_params = crate::handlers::step::StepExecParams {
        instance_id,
        tenant_id: instance.tenant_id.clone(),
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        params: resolved_params,
        context: step_context,
        attempt,
        timeout: step_def.timeout,
        externalize_threshold,
    };

    let result = crate::handlers::step::execute_step_dry(storage, handlers, exec_params).await;

    match result {
        Ok(block_output) => {
            // Step succeeded — just save the output (no state transition yet).
            // The caller will continue executing more blocks or complete the instance.
            storage.save_block_output(&block_output).await?;
            Ok(StepOutcome::Completed)
        }
        Err(EngineError::StepFailed {
            retryable: true,
            ref message,
            ..
        }) => {
            crate::metrics::inc(crate::metrics::STEPS_FAILED);
            // If a step handler drove a pause/cancel transition mid-flight
            // (see `handle_sleep` in `handlers/builtin.rs`), the instance
            // already sits in `Paused` or `Cancelled`. Treat the
            // `Retryable` as a benign yield in that case — falling through
            // to `handle_retryable_failure` would otherwise attempt an
            // invalid `Paused→Failed` transition and error out.
            let current = storage
                .get_instance(instance_id)
                .await?
                .map_or(InstanceState::Running, |i| i.state);
            if matches!(current, InstanceState::Paused | InstanceState::Cancelled) {
                return Ok(StepOutcome::Deferred);
            }
            handle_retryable_failure(
                storage.as_ref(),
                instance_id,
                step_def,
                attempt,
                webhook_config,
                message,
                cancel,
            )
            .await?;
            Ok(StepOutcome::Failed)
        }
        Err(e) => {
            crate::metrics::inc(crate::metrics::STEPS_FAILED);
            // Permanent failure or timeout.
            crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                InstanceState::Running,
                InstanceState::Failed,
                None,
            )
            .await?;

            crate::webhooks::emit(
                webhook_config,
                &crate::webhooks::instance_event(
                    "instance.failed",
                    instance_id,
                    serde_json::json!({ "error": e.to_string() }),
                ),
                cancel,
            );

            Ok(StepOutcome::Failed)
        }
    }
}

async fn handle_retryable_failure(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
    step_def: &orch8_types::sequence::StepDef,
    attempt: u32,
    webhook_config: &WebhookConfig,
    message: &str,
    cancel: &CancellationToken,
) -> Result<(), EngineError> {
    if let Some(retry) = &step_def.retry {
        // Check if max_attempts has been exhausted.
        if attempt >= retry.max_attempts {
            warn!(
                instance_id = %instance_id,
                block_id = %step_def.id,
                attempt = attempt,
                max_attempts = retry.max_attempts,
                "max retry attempts exhausted, failing instance"
            );

            crate::lifecycle::transition_instance(
                storage,
                instance_id,
                InstanceState::Running,
                InstanceState::Failed,
                None,
            )
            .await?;

            crate::webhooks::emit(
                webhook_config,
                &crate::webhooks::instance_event(
                    "instance.failed",
                    instance_id,
                    serde_json::json!({
                        "error": message,
                        "attempts": attempt,
                        "block_id": step_def.id.0,
                    }),
                ),
                cancel,
            );

            return Ok(());
        }

        crate::metrics::inc(crate::metrics::STEPS_RETRIED);

        let backoff = crate::handlers::step::calculate_backoff(
            attempt,
            retry.initial_backoff,
            retry.max_backoff,
            retry.backoff_multiplier,
        );
        let fire_at = Utc::now()
            + chrono::Duration::from_std(backoff).unwrap_or_else(|_| chrono::Duration::zero());

        warn!(
            instance_id = %instance_id,
            block_id = %step_def.id,
            attempt = attempt,
            max_attempts = retry.max_attempts,
            backoff_ms = backoff.as_millis(),
            message = %message,
            "retryable failure, re-scheduling with backoff"
        );

        crate::lifecycle::transition_instance(
            storage,
            instance_id,
            InstanceState::Running,
            InstanceState::Scheduled,
            Some(fire_at),
        )
        .await?;
        return Ok(());
    }

    // No retry policy — fail the instance.
    crate::lifecycle::transition_instance(
        storage,
        instance_id,
        InstanceState::Running,
        InstanceState::Failed,
        None,
    )
    .await?;

    crate::webhooks::emit(
        webhook_config,
        &crate::webhooks::instance_event(
            "instance.failed",
            instance_id,
            serde_json::json!({ "error": message }),
        ),
        cancel,
    );

    Ok(())
}

/// Transition the instance to `Failed` and emit an `instance.failed`
/// webhook carrying the supplied error message. Used when step-param
/// resolution (templates or credentials) fails before the handler is even
/// invoked — there is no retry policy for these malformed-params errors,
/// so the instance is failed immediately.
async fn fail_instance_with_error(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
    webhook_config: &WebhookConfig,
    cancel: &CancellationToken,
    error: &str,
) -> Result<StepOutcome, EngineError> {
    crate::metrics::inc(crate::metrics::STEPS_FAILED);
    crate::lifecycle::transition_instance(
        storage,
        instance_id,
        InstanceState::Running,
        InstanceState::Failed,
        None,
    )
    .await?;

    crate::webhooks::emit(
        webhook_config,
        &crate::webhooks::instance_event(
            "instance.failed",
            instance_id,
            serde_json::json!({ "error": error }),
        ),
        cancel,
    );

    Ok(StepOutcome::Failed)
}

/// Dispatch a step to the external worker queue.
///
/// Creates a `WorkerTask` row (idempotent via `ON CONFLICT DO NOTHING`)
/// and transitions the instance to `Waiting`. The instance will be
/// re-scheduled when the worker reports completion via the API.
///
/// `resolved_params` and `step_context` must already have been through
/// template + credential resolution — external workers receive fully
/// materialised values, not raw `{{…}}` or `credentials://…` strings.
async fn dispatch_to_external_worker(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    attempt: u32,
    resolved_params: serde_json::Value,
    step_context: orch8_types::context::ExecutionContext,
) -> Result<StepOutcome, EngineError> {
    use orch8_types::worker::{WorkerTask, WorkerTaskState};

    let task = WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        queue_name: step_def.queue_name.clone(),
        params: resolved_params,
        // Context has already had `context_access` filtering and
        // externalization-marker inflation applied upstream — the remote
        // process cannot be trusted to filter on its own.
        context: serde_json::to_value(&step_context)
            .map_err(orch8_types::error::StorageError::Serialization)?,
        attempt: i16::try_from(attempt).unwrap_or(i16::MAX),
        timeout_ms: step_def
            .timeout
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)),
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: chrono::Utc::now(),
    };

    storage.create_worker_task(&task).await?;

    // Transition instance Running → Waiting so the scheduler doesn't re-claim it.
    crate::lifecycle::transition_instance(
        storage,
        instance.id,
        InstanceState::Running,
        InstanceState::Waiting,
        None,
    )
    .await?;

    info!(
        instance_id = %instance.id,
        block_id = %step_def.id,
        handler = %step_def.handler,
        "dispatched step to external worker queue"
    );

    Ok(StepOutcome::Deferred)
}

#[cfg(test)]
mod tests {
    use super::*;

    use orch8_types::ids::BlockId;
    use orch8_types::signal::{Signal, SignalType};
    use uuid::Uuid;

    #[test]
    fn build_prefetch_map_merges_signals_and_blocks() {
        let id1 = InstanceId(Uuid::now_v7());
        let id2 = InstanceId(Uuid::now_v7());

        let mut signals = HashMap::new();
        signals.insert(
            id1,
            vec![Signal {
                id: Uuid::now_v7(),
                instance_id: id1,
                signal_type: SignalType::Pause,
                payload: serde_json::Value::Null,
                delivered: false,
                created_at: Utc::now(),
                delivered_at: None,
            }],
        );

        let mut completed = HashMap::new();
        completed.insert(id1, vec![BlockId("step1".into())]);
        completed.insert(id2, vec![BlockId("step2".into())]);

        let result = build_prefetch_map(signals, completed);

        assert_eq!(result.len(), 2);
        assert_eq!(result[&id1].signals.len(), 1);
        assert_eq!(result[&id1].completed_block_ids.len(), 1);
        assert_eq!(result[&id2].signals.len(), 0);
        assert_eq!(result[&id2].completed_block_ids.len(), 1);
    }

    #[test]
    fn build_prefetch_map_empty_inputs() {
        let result = build_prefetch_map(HashMap::new(), HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn build_prefetch_map_signals_only_instance() {
        // An instance with signals but no completed blocks must still appear.
        let id = InstanceId(Uuid::now_v7());
        let mut signals = HashMap::new();
        signals.insert(
            id,
            vec![Signal {
                id: Uuid::now_v7(),
                instance_id: id,
                signal_type: SignalType::Resume,
                payload: serde_json::Value::Null,
                delivered: false,
                created_at: Utc::now(),
                delivered_at: None,
            }],
        );

        let result = build_prefetch_map(signals, HashMap::new());
        assert_eq!(result.len(), 1);
        assert_eq!(result[&id].signals.len(), 1);
        assert!(result[&id].completed_block_ids.is_empty());
    }

    #[test]
    fn build_prefetch_map_completed_only_instance() {
        // An instance with only completed-block data must still appear with empty signals.
        let id = InstanceId(Uuid::now_v7());
        let mut completed = HashMap::new();
        completed.insert(id, vec![BlockId("step-a".into()), BlockId("step-b".into())]);

        let result = build_prefetch_map(HashMap::new(), completed);
        assert_eq!(result.len(), 1);
        assert!(result[&id].signals.is_empty());
        assert_eq!(result[&id].completed_block_ids.len(), 2);
    }

    // ------------------------------------------------------------------
    // R6 follow-up: template::resolve wiring into scheduler dispatch path
    // ------------------------------------------------------------------

    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::sequence::StepDef;
    use std::sync::Mutex;

    async fn seed_instance_with_context(
        storage: &dyn StorageBackend,
        id: InstanceId,
        context: ExecutionContext,
    ) {
        let now = Utc::now();
        let inst = TaskInstance {
            id,
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context,
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&inst).await.unwrap();
    }

    fn mk_step_def(id: &str, handler: &str, params: serde_json::Value) -> StepDef {
        StepDef {
            id: BlockId(id.into()),
            handler: handler.into(),
            params,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
        }
    }

    #[tokio::test]
    async fn scheduler_execute_step_block_resolves_templates_for_inprocess_handler() {
        // Regression: the fast-path scheduler loop (`execute_step_block`)
        // must also run template + credential resolution before invoking
        // the handler. Without this, `{{context.data.*}}` leaks through to
        // any handler dispatched via the queue-driven path.
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

        let instance_id = InstanceId::new();
        let ctx = ExecutionContext {
            data: serde_json::json!({"slug": "user.signed_up"}),
            ..ExecutionContext::default()
        };
        seed_instance_with_context(storage.as_ref(), instance_id, ctx).await;
        let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

        let step_def = mk_step_def(
            "fire",
            "capture_params",
            serde_json::json!({"trigger_slug": "{{context.data.slug}}"}),
        );

        let captured: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
        let captured_clone = Arc::clone(&captured);

        let mut registry = HandlerRegistry::new();
        registry.register("capture_params", move |ctx| {
            let captured = Arc::clone(&captured_clone);
            async move {
                *captured.lock().unwrap() = Some(ctx.params.clone());
                Ok(serde_json::json!({"ok": true}))
            }
        });

        let webhook_config = WebhookConfig::default();
        let cancel = CancellationToken::new();

        let outcome = execute_step_block(
            &storage,
            &registry,
            &webhook_config,
            0,
            &instance,
            &step_def,
            &cancel,
        )
        .await
        .expect("execute_step_block errored");

        assert!(matches!(outcome, StepOutcome::Completed));
        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("handler not called");
        assert_eq!(seen["trigger_slug"], "user.signed_up");
    }

    #[tokio::test]
    async fn scheduler_execute_step_block_template_failure_fails_instance() {
        // An unknown template root (`{{nope.x}}`) must fail the instance
        // before the handler runs — just like the tree-evaluator path does.
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

        let instance_id = InstanceId::new();
        seed_instance_with_context(storage.as_ref(), instance_id, ExecutionContext::default())
            .await;
        let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

        let step_def = mk_step_def(
            "bad",
            "capture_params",
            serde_json::json!({"x": "{{nope.x}}"}),
        );

        let handler_called = Arc::new(Mutex::new(false));
        let handler_called_clone = Arc::clone(&handler_called);
        let mut registry = HandlerRegistry::new();
        registry.register("capture_params", move |_ctx| {
            let flag = Arc::clone(&handler_called_clone);
            async move {
                *flag.lock().unwrap() = true;
                Ok(serde_json::json!({}))
            }
        });

        let webhook_config = WebhookConfig::default();
        let cancel = CancellationToken::new();

        let outcome = execute_step_block(
            &storage,
            &registry,
            &webhook_config,
            0,
            &instance,
            &step_def,
            &cancel,
        )
        .await
        .expect("execute_step_block errored");

        assert!(matches!(outcome, StepOutcome::Failed));
        assert!(
            !*handler_called.lock().unwrap(),
            "handler must not run when template resolution fails"
        );

        let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
        assert_eq!(refreshed.state, InstanceState::Failed);
    }

    #[tokio::test]
    async fn scheduler_execute_step_block_resolves_params_for_external_worker() {
        // When the handler is not registered in-process, the scheduler
        // dispatches to the external-worker queue. The queued task must
        // carry resolved params, not raw `{{…}}` strings.
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

        let instance_id = InstanceId::new();
        let ctx = ExecutionContext {
            data: serde_json::json!({"slug": "user.signed_up"}),
            ..ExecutionContext::default()
        };
        seed_instance_with_context(storage.as_ref(), instance_id, ctx).await;
        let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

        let step_def = mk_step_def(
            "external",
            "not_registered_handler",
            serde_json::json!({"trigger_slug": "{{context.data.slug}}"}),
        );

        // Empty registry — forces the external-worker dispatch branch.
        let registry = HandlerRegistry::new();
        let webhook_config = WebhookConfig::default();
        let cancel = CancellationToken::new();

        let outcome = execute_step_block(
            &storage,
            &registry,
            &webhook_config,
            0,
            &instance,
            &step_def,
            &cancel,
        )
        .await
        .expect("execute_step_block errored");

        assert!(matches!(outcome, StepOutcome::Deferred));

        // Inspect the queued WorkerTask: its params must be resolved.
        let tasks = storage
            .claim_worker_tasks("not_registered_handler", "test-worker", 10)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1, "exactly one worker task should be queued");
        assert_eq!(tasks[0].instance_id, instance_id);
        assert_eq!(
            tasks[0].params["trigger_slug"], "user.signed_up",
            "external worker must receive fully-resolved params"
        );
    }

    #[tokio::test]
    async fn wait_for_drain_waits_for_permits_returned() {
        // Acquire a permit, spawn a task that holds it briefly, then ensure drain waits.
        let sem = Arc::new(Semaphore::new(2));
        let held = Arc::clone(&sem).acquire_owned().await.unwrap();

        // Drain future shouldn't complete while we're holding a permit.
        let drain = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { wait_for_drain(&sem, 2).await })
        };

        // Give the task a chance to start.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!drain.is_finished(), "drain should still be waiting");

        drop(held);
        // After releasing, drain must complete promptly.
        tokio::time::timeout(Duration::from_millis(500), drain)
            .await
            .expect("drain timed out after permit released")
            .expect("drain task panicked");
        assert_eq!(sem.available_permits(), 2);
    }
}
