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
            .time_to_live(Duration::from_secs(300))
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
                ).await {
                    error!(error = %e, "tick processing failed");
                }
            }
        }
    }
}

/// Wait until all semaphore permits are available (all tasks drained).
async fn wait_for_drain(semaphore: &Semaphore, max_permits: usize) {
    loop {
        if semaphore.available_permits() == max_permits {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Process a single tick: claim due instances and spawn processing tasks.
#[allow(clippy::too_many_arguments)]
async fn process_tick(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    semaphore: &Arc<Semaphore>,
    batch_size: u32,
    max_per_tenant: u32,
    webhook_config: &Arc<WebhookConfig>,
    sequence_cache: &Arc<Cache<SequenceId, Arc<SequenceDefinition>>>,
    externalize_threshold: u32,
) -> Result<(), EngineError> {
    let _tick_timer = crate::metrics::Timer::start(crate::metrics::TICK_DURATION);

    let now = Utc::now();
    let instances = storage.claim_due_instances(now, batch_size, max_per_tenant).await?;

    if instances.is_empty() {
        return Ok(());
    }

    let count = instances.len();
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

    // Wrap in Arc so we can share cheaply across spawned tasks.
    let prefetched = Arc::new(build_prefetch_map(signals_map, completed_map));

    for instance in instances {
        let storage = Arc::clone(storage);
        let handlers = Arc::clone(handlers);
        let semaphore = Arc::clone(semaphore);
        let webhooks = Arc::clone(webhook_config);
        let seq_cache = Arc::clone(sequence_cache);
        let prefetched = Arc::clone(&prefetched);

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
            let data = prefetched.get(&instance_id).cloned().unwrap_or_else(|| {
                PrefetchedData {
                    signals: Vec::new(),
                    completed_block_ids: Vec::new(),
                }
            });

            match process_instance(
                storage.as_ref(),
                &handlers,
                &webhooks,
                &seq_cache,
                instance,
                data,
                externalize_threshold,
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
fn build_prefetch_map(
    mut signals_map: HashMap<InstanceId, Vec<Signal>>,
    mut completed_map: HashMap<InstanceId, Vec<BlockId>>,
) -> HashMap<InstanceId, PrefetchedData> {
    // Merge both maps into a single PrefetchedData per instance.
    let all_ids: std::collections::HashSet<InstanceId> = signals_map
        .keys()
        .chain(completed_map.keys())
        .copied()
        .collect();

    let mut result = HashMap::with_capacity(all_ids.len());
    for id in all_ids {
        result.insert(
            id,
            PrefetchedData {
                signals: signals_map.remove(&id).unwrap_or_default(),
                completed_block_ids: completed_map.remove(&id).unwrap_or_default(),
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
async fn process_instance(
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    sequence_cache: &Cache<SequenceId, Arc<SequenceDefinition>>,
    instance: orch8_types::instance::TaskInstance,
    prefetched: PrefetchedData,
    externalize_threshold: u32,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    // Fetch sequence definition early — needed for cancellation scopes and evaluation.
    let sequence = get_sequence_cached(storage, sequence_cache, instance.sequence_id).await?;

    // claim_due_instances already set state to Running.
    // Process any pending signals (using pre-fetched data).
    if !prefetched.signals.is_empty() {
        let abort = crate::signals::process_signals_prefetched(
            storage,
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
    let has_composite = sequence
        .blocks
        .iter()
        .any(|b| !matches!(b, orch8_types::sequence::BlockDefinition::Step(_)));

    if has_composite {
        return process_instance_tree(storage, handlers, webhook_config, &instance, &sequence)
            .await;
    }

    // Fast path: all blocks are Steps. Execute multi-block per claim cycle.
    let mut completed_blocks: std::collections::HashSet<String> = prefetched
        .completed_block_ids
        .into_iter()
        .map(|id| id.0)
        .collect();

    for block in &sequence.blocks {
        let orch8_types::sequence::BlockDefinition::Step(step_def) = block else {
            unreachable!("checked above: all blocks are steps");
        };

        if completed_blocks.contains(&step_def.id.0) {
            continue;
        }

        match execute_step_block(storage, handlers, webhook_config, externalize_threshold, &instance, step_def).await? {
            StepOutcome::Completed => {
                completed_blocks.insert(step_def.id.0.clone());
            }
            StepOutcome::Deferred | StepOutcome::Failed => {
                return Ok(());
            }
        }
    }

    // All blocks completed.
    crate::lifecycle::transition_instance(
        storage,
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
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    instance: &orch8_types::instance::TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    match crate::evaluator::evaluate(storage, handlers, instance, sequence).await {
        Ok(true) => {
            // More work — check if the tree has nodes waiting for external workers.
            // If so, transition to Waiting (the worker completion callback will
            // re-schedule). Otherwise, re-schedule for the next tick.
            let tree = storage.get_execution_tree(instance_id).await?;
            if crate::evaluator::has_waiting_nodes(&tree) {
                crate::lifecycle::transition_instance(
                    storage,
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
        Ok(false) => {
            // Evaluator says done. Check if any root node failed.
            let tree = storage.get_execution_tree(instance_id).await?;
            let root_failed = tree
                .iter()
                .filter(|n| n.parent_id.is_none())
                .any(|n| {
                    matches!(
                        n.state,
                        orch8_types::execution::NodeState::Failed
                            | orch8_types::execution::NodeState::Cancelled
                    )
                });

            // Read current instance state — may differ from Running if an external
            // worker dispatch changed it to Waiting within this tick.
            let current_state = storage
                .get_instance(instance_id)
                .await?
                .map_or(InstanceState::Running, |i| i.state);

            if root_failed {
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
                );
                info!(instance_id = %instance_id, from = %current_state, "instance completed (tree evaluation)");
            }
        }
        Err(e) => {
            error!(instance_id = %instance_id, error = %e, "tree evaluation failed");
            crate::lifecycle::transition_instance(
                storage,
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
async fn get_sequence_cached(
    storage: &dyn StorageBackend,
    cache: &Cache<SequenceId, Arc<SequenceDefinition>>,
    sequence_id: SequenceId,
) -> Result<Arc<SequenceDefinition>, EngineError> {
    if let Some(seq) = cache.get(&sequence_id).await {
        return Ok(seq);
    }

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
                    id: uuid::Uuid::new_v4(),
                    instance_id: instance.id,
                    block_id: step_def.id.clone(),
                    output: signal.payload.clone(),
                    output_ref: None,
                    output_size: serde_json::to_vec(&signal.payload)
                        .map(|v| i32::try_from(v.len()).unwrap_or(i32::MAX))
                        .unwrap_or(0),
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
                        id: uuid::Uuid::new_v4(),
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
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    externalize_threshold: u32,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
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
                    storage,
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

    if check_step_delay(storage, instance, step_def).await? {
        return Ok(StepOutcome::Deferred);
    }

    if check_send_window(storage, instance, step_def).await? {
        return Ok(StepOutcome::Deferred);
    }

    if check_step_rate_limit(storage, instance, step_def).await? {
        return Ok(StepOutcome::Deferred);
    }

    // Human-in-the-loop: if this step waits for human input, check if a response
    // signal has been delivered. If not, pause the instance.
    if let Some(human_def) = &step_def.wait_for_input {
        if check_human_input(storage, instance, step_def, human_def).await? {
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

    // If the handler is not registered in-process, dispatch to external worker queue.
    if !handlers.contains(&step_def.handler) {
        return dispatch_to_external_worker(storage, instance, step_def, attempt).await;
    }

    crate::metrics::inc(crate::metrics::STEPS_EXECUTED);

    let exec_params = crate::handlers::step::StepExecParams {
        instance_id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        params: step_def.params.clone(),
        context: match &step_def.context_access {
            Some(access) => instance.context.filtered(access),
            None => instance.context.clone(),
        },
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
            handle_retryable_failure(
                storage,
                instance_id,
                step_def,
                attempt,
                webhook_config,
                message,
            )
            .await?;
            Ok(StepOutcome::Failed)
        }
        Err(e) => {
            crate::metrics::inc(crate::metrics::STEPS_FAILED);
            // Permanent failure or timeout.
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
                    serde_json::json!({ "error": e.to_string() }),
                ),
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
    );

    Ok(())
}

/// Dispatch a step to the external worker queue.
///
/// Creates a `WorkerTask` row (idempotent via `ON CONFLICT DO NOTHING`)
/// and transitions the instance to `Waiting`. The instance will be
/// re-scheduled when the worker reports completion via the API.
async fn dispatch_to_external_worker(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    attempt: u32,
) -> Result<StepOutcome, EngineError> {
    use orch8_types::worker::{WorkerTask, WorkerTaskState};

    let task = WorkerTask {
        id: uuid::Uuid::new_v4(),
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        queue_name: step_def.queue_name.clone(),
        params: step_def.params.clone(),
        context: serde_json::to_value(&instance.context).unwrap_or_default(),
        attempt: i16::try_from(attempt).unwrap_or(i16::MAX),
        timeout_ms: step_def.timeout.map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)),
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
