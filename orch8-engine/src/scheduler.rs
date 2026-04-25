use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::config::{SchedulerConfig, WebhookConfig};
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

pub use step_exec::check_human_input;

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
    // Unified `SequenceCache` covers both by-id and by-name lookups so future
    // callers in evaluator/triggers can cut DB roundtrips on sub-sequence
    // resolution and trigger firing without reshaping this plumbing.
    let sequence_cache: Arc<SequenceCache> =
        Arc::new(SequenceCache::new(1_000, Duration::from_mins(5)));

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
                // Process signals for paused/waiting instances that won't be
                // picked up by claim_due_instances (which only claims
                // scheduled instances). Without this, resume/cancel signals
                // on paused instances would never be processed.
                if let Err(e) = process_signalled_instances(&storage, batch_size).await {
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
                ).await {
                    error!(error = %e, "waiting deadline processing failed");
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
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
#[tracing::instrument(skip_all, fields(batch_size = batch_size, claimed = tracing::field::Empty))]
async fn process_tick(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    semaphore: &Arc<Semaphore>,
    batch_size: u32,
    max_per_tenant: u32,
    webhook_config: &Arc<WebhookConfig>,
    sequence_cache: &Arc<SequenceCache>,
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
    let instances = storage
        .claim_due_instances(now, fetch_limit, max_per_tenant)
        .await?;

    if instances.is_empty() {
        return Ok(());
    }

    // Enforce concurrency limits BEFORE spawning tasks. `claim_due_instances`
    // sets all claimed instances to Running in a single batch — if multiple
    // instances share a concurrency_key, more than `max_concurrency` may be
    // Running simultaneously. We put the excess back to Scheduled here,
    // synchronously, so the observable Running count never exceeds the limit.
    let mut instances = enforce_concurrency_limits(storage, instances).await?;

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
        // Reserve the permit synchronously BEFORE spawning so fast ticks
        // cannot claim more rows than available concurrency slots. The permit
        // is moved into the spawned task and released when processing completes.
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            // All permits taken — defer this instance back to Scheduled so
            // a future tick picks it up.
            debug!(
                instance_id = %instance.id,
                "no semaphore permit available, deferring instance"
            );
            let _ = storage
                .update_instance_state(instance.id, InstanceState::Scheduled, Some(Utc::now()))
                .await;
            continue;
        };

        let storage = Arc::clone(storage);
        let handlers = Arc::clone(handlers);
        let webhooks = Arc::clone(webhook_config);
        let seq_cache = Arc::clone(sequence_cache);
        let prefetched = Arc::clone(&prefetched);
        let cancel = cancel.clone();

        crate::metrics::inc(crate::metrics::INSTANCES_CLAIMED);

        tokio::spawn(async move {
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

            // Spawn a nested task so we can `.await` its JoinHandle — this
            // catches panics (JoinError::is_panic) that would otherwise
            // silently kill the outer task and leave the instance stuck in
            // Running forever (leaking a semaphore permit).
            let s2 = Arc::clone(&storage);
            let result = tokio::spawn(async move {
                process_instance(
                    &s2,
                    &handlers,
                    &webhooks,
                    &seq_cache,
                    instance,
                    data,
                    externalize_threshold,
                    &cancel,
                )
                .await
            })
            .await;

            match result {
                Ok(Ok(())) => {}
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
                        .update_instance_state(instance_id, InstanceState::Failed, None)
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
                        .update_instance_state(instance_id, InstanceState::Failed, None)
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
        });
    }

    Ok(())
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
) -> Result<Vec<orch8_types::instance::TaskInstance>, EngineError> {
    // Collect concurrency keys present in the batch.
    let mut key_instances: HashMap<&str, Vec<usize>> = HashMap::with_capacity(instances.len() / 2);
    for (idx, inst) in instances.iter().enumerate() {
        if let (Some(ref key), Some(_max)) = (&inst.concurrency_key, inst.max_concurrency) {
            key_instances.entry(key.as_str()).or_default().push(idx);
        }
    }

    if key_instances.is_empty() {
        return Ok(instances);
    }

    // Batch count running instances for all concurrency keys in a single query.
    let keys: Vec<String> = key_instances.keys().map(|&k| k.to_owned()).collect();
    let running_counts = storage.count_running_by_concurrency_keys(&keys).await?;

    // For each concurrency key, determine how many slots are available.
    let mut deferred_indices = std::collections::HashSet::new();
    for (key, indices) in &key_instances {
        // All instances in the group share the same max_concurrency.
        let max = instances[indices[0]].max_concurrency.unwrap_or(i32::MAX);

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
                deferred_indices.insert(idx);
            }
        }
    }

    if deferred_indices.is_empty() {
        return Ok(instances);
    }

    // Transition excess instances back to Scheduled.
    let defer_at = Utc::now() + chrono::Duration::seconds(1);
    for &idx in &deferred_indices {
        let inst = &instances[idx];
        debug!(
            instance_id = %inst.id,
            concurrency_key = %inst.concurrency_key.as_deref().unwrap_or(""),
            "concurrency limit exceeded at claim time, deferring"
        );
        storage
            .update_instance_state(inst.id, InstanceState::Scheduled, Some(defer_at))
            .await?;
    }

    let mut kept = instances;
    let mut deferred_sorted: Vec<_> = deferred_indices.into_iter().collect();
    deferred_sorted.sort_unstable_by(|a, b| b.cmp(a));
    for idx in deferred_sorted {
        kept.swap_remove(idx);
    }

    Ok(kept)
}

/// Process signals for instances in paused/waiting state.
///
/// `claim_due_instances` only picks up `scheduled` instances. Signals (resume,
/// cancel, `update_context`) enqueued against paused or waiting instances would
/// sit unprocessed indefinitely without this sweep. The function is cheap when
/// no such signals exist (single indexed query returning empty).
async fn process_signalled_instances(
    storage: &Arc<dyn StorageBackend>,
    batch_size: u32,
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
            let _ = storage
                .update_instance_state(instance_id, InstanceState::Scheduled, Some(Utc::now()))
                .await;
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
            debug!(
                instance_id = %instance_id,
                from_state = %current_state,
                "signal processed for non-scheduled instance"
            );
        }
    }

    Ok(())
}

/// Check SLA deadlines for instances stuck in `Waiting` state (dispatched to
/// external workers that never completed). Runs every tick with a bounded
/// query to avoid overhead when no deadlines are configured.
async fn process_waiting_deadlines(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    sequence_cache: &SequenceCache,
    webhook_config: &WebhookConfig,
    cancel: &CancellationToken,
    batch_size: u32,
) -> Result<(), EngineError> {
    use orch8_types::filter::{InstanceFilter, Pagination};

    let filter = InstanceFilter {
        states: Some(vec![InstanceState::Waiting]),
        ..Default::default()
    };
    let pagination = Pagination {
        offset: 0,
        limit: batch_size,
        sort_ascending: true,
    };
    let waiting = storage.list_instances(&filter, &pagination).await?;

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
            if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
                if step_def.deadline.is_some() {
                    deadline_keys.push((instance.id, step_def.id.clone()));
                }
            }
        }
    }
    let deadline_outputs = if deadline_keys.is_empty() {
        std::collections::HashMap::new()
    } else {
        storage.get_block_outputs_batch(&deadline_keys).await?
    };

    for (instance, seq) in instance_sequences {
        let mut handled = false;
        for block in &seq.blocks {
            if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
                if step_def.deadline.is_some() {
                    let prev = deadline_outputs.get(&(instance.id, step_def.id.clone()));
                    if check_step_deadline_waiting(
                        storage,
                        handlers,
                        instance,
                        step_def,
                        webhook_config,
                        cancel,
                        prev,
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
                if let Some(human_def) = &step_def.wait_for_input {
                    if let Some(timeout) = human_def.timeout {
                        let baseline = instance
                            .context
                            .runtime
                            .current_step_started_at
                            .or(instance.context.runtime.started_at);
                        if let Some(started) = baseline {
                            let elapsed = Utc::now() - started;
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
                                    Some(Utc::now()),
                                )
                                .await?;
                                handled = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        let _ = handled;
    }
    Ok(())
}

/// SLA deadline check variant for instances in `Waiting` state.
/// Mirrors `check_step_deadline` but transitions from `Waiting` instead of `Running`.
async fn check_step_deadline_waiting(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    _webhook_config: &WebhookConfig,
    _cancel: &CancellationToken,
    prev_output: Option<&orch8_types::output::BlockOutput>,
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
    let elapsed = Utc::now() - baseline;
    if elapsed < chrono::Duration::from_std(deadline).unwrap_or(chrono::TimeDelta::MAX) {
        return Ok(false);
    }

    let instance_id = instance.id;
    warn!(
        instance_id = %instance_id,
        block_id = %step_def.id,
        deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
        elapsed_ms = elapsed.num_milliseconds(),
        "SLA deadline breached (waiting instance)"
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
                wait_for_input: None,
            };
            if let Err(e) = handler(step_ctx).await {
                warn!(instance_id = %instance_id, error = %e, "SLA escalation handler failed");
            }
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
        InstanceState::Waiting,
        InstanceState::Failed,
        None,
    )
    .await?;
    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);

    // Wake parent: SLA deadline breach on a waiting child → terminal Failed.
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
    cancel: &CancellationToken,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    // Fetch sequence definition early — needed for cancellation scopes and evaluation.
    let sequence = sequence_cache
        .get_by_id(storage.as_ref(), instance.sequence_id)
        .await?;

    // claim_due_instances already set state to Running.
    // Stamp runtime.started_at on the first run so timeout / escalation
    // handlers (e.g. human_review) can compute elapsed time.
    let instance = if instance.context.runtime.started_at.is_none() {
        let started_at = Utc::now();
        storage
            .update_instance_started_at(instance_id, started_at)
            .await?;
        let mut inst = instance;
        inst.context.runtime.started_at = Some(started_at);
        inst
    } else {
        instance
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

    // Merge dynamically injected blocks with the sequence definition.
    let blocks = crate::evaluator::merged_blocks(storage.as_ref(), instance.id, &sequence).await?;

    // Fast path SLA deadline check for all steps BEFORE concurrency checks.
    // Batch-fetch any previous block outputs so the loop is N queries → 1 query.
    let deadline_keys: Vec<(InstanceId, BlockId)> = blocks
        .iter()
        .filter_map(|b| match b {
            orch8_types::sequence::BlockDefinition::Step(s) if s.deadline.is_some() => {
                Some((instance.id, s.id.clone()))
            }
            _ => None,
        })
        .collect();
    let deadline_outputs = if deadline_keys.is_empty() {
        std::collections::HashMap::new()
    } else {
        storage.get_block_outputs_batch(&deadline_keys).await?
    };

    for block in blocks.iter() {
        if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
            if !prefetched.completed_block_ids.contains(&step_def.id) {
                // SLA deadline check: if a previous attempt exists and the deadline has
                // been breached (wall-clock time since first attempt), fail the instance.
                let prev = deadline_outputs.get(&(instance.id, step_def.id.clone()));
                if step_exec::check_step_deadline(storage, handlers, &instance, step_def, prev)
                    .await?
                {
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
            cancel,
        )
        .await;
    }

    // Fast path: all blocks are Steps. Execute multi-block per claim cycle.
    let mut completed_blocks: std::collections::HashSet<BlockId> =
        prefetched.completed_block_ids.into_iter().collect();

    for block in blocks.iter() {
        let orch8_types::sequence::BlockDefinition::Step(step_def) = block else {
            unreachable!("checked above: all blocks are steps");
        };

        if completed_blocks.contains(&step_def.id) {
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
                completed_blocks.insert(step_def.id.clone());
            }
            StepOutcome::Failed => {
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

    crate::lifecycle::transition_instance(
        storage.as_ref(),
        instance_id,
        Some(&instance.tenant_id),
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

    // Wake parent if this is a sub-sequence child.
    wake_parent_if_child(storage.as_ref(), &instance).await;

    Ok(())
}

/// If this instance has a `parent_instance_id`, transition the parent from
/// `Waiting` → `Scheduled` so it is picked up on the next tick and can observe
/// the child's terminal state.
///
/// Uses a CAS (conditional update) so a concurrent cancel/fail cannot be
/// silently overwritten.
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
/// The evaluator returns an [`EvalOutcome`] carrying the tree state so we can
/// transition the instance without re-reading the tree from the database.
#[allow(clippy::too_many_lines)]
async fn process_instance_tree(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
    cancel: &CancellationToken,
) -> Result<(), EngineError> {
    use crate::evaluator::EvalOutcome;
    let instance_id = instance.id;

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
                crate::lifecycle::transition_instance(
                    storage.as_ref(),
                    instance_id,
                    Some(&instance.tenant_id),
                    InstanceState::Running,
                    InstanceState::Waiting,
                    None,
                )
                .await?;
            } else if let Err(e) = crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                current,
                InstanceState::Scheduled,
                Some(Utc::now()),
            )
            .await
            {
                if !matches!(e, crate::error::EngineError::InvalidTransition { .. }) {
                    return Err(e);
                }
                debug!(
                    instance_id = %instance_id,
                    current = %current,
                    "concurrent writer moved instance before tree→Scheduled transition"
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
                    "instance already moved by signal or concurrent writer — skipping terminal transition"
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

            if let Err(e) = crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                current,
                target,
                None,
            )
            .await
            {
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
                        info!(instance_id = %instance_id, "instance cancelled (tree evaluation)");
                    }
                    InstanceState::Failed => {
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
                        crate::webhooks::emit(
                            webhook_config,
                            &crate::webhooks::instance_event(
                                "instance.failed",
                                instance_id,
                                serde_json::json!({}),
                            ),
                            cancel,
                        );
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
                        crate::webhooks::emit(
                            webhook_config,
                            &crate::webhooks::instance_event(
                                "instance.completed",
                                instance_id,
                                serde_json::json!({}),
                            ),
                            cancel,
                        );
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
