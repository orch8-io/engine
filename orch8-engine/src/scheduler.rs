use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::config::{SchedulerConfig, WebhookConfig};
use orch8_types::instance::InstanceState;

use crate::error::EngineError;
use crate::handlers::HandlerRegistry;

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
                    &config.webhooks,
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
async fn process_tick(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    semaphore: &Arc<Semaphore>,
    batch_size: u32,
    webhook_config: &WebhookConfig,
) -> Result<(), EngineError> {
    let _tick_timer = crate::metrics::Timer::start(crate::metrics::TICK_DURATION);

    let now = Utc::now();
    let instances = storage.claim_due_instances(now, batch_size).await?;

    if instances.is_empty() {
        return Ok(());
    }

    let count = instances.len();
    debug!(count = count, "claimed instances for processing");
    #[allow(clippy::cast_precision_loss)]
    crate::metrics::set_gauge(crate::metrics::QUEUE_DEPTH, count as f64);

    for instance in instances {
        let storage = Arc::clone(storage);
        let handlers = Arc::clone(handlers);
        let semaphore = Arc::clone(semaphore);
        let webhooks = webhook_config.clone();

        crate::metrics::inc(crate::metrics::INSTANCES_CLAIMED);

        tokio::spawn(async move {
            // Acquire semaphore permit to bound concurrency.
            let Ok(_permit) = semaphore.acquire().await else {
                error!("semaphore closed unexpectedly");
                return;
            };

            let _instance_timer = crate::metrics::Timer::start(crate::metrics::INSTANCE_DURATION);
            let instance_id = instance.id;

            match process_instance(storage.as_ref(), &handlers, &webhooks, instance).await {
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

/// Process a single claimed instance: execute the first pending step.
/// Composite blocks (Parallel, Race, etc.) are deferred to Stage 3.
async fn process_instance(
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    instance: orch8_types::instance::TaskInstance,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    // claim_due_instances already set state to Running.
    // Process any pending signals.
    let abort =
        crate::signals::process_signals(storage, instance_id, InstanceState::Running).await?;
    if abort {
        return Ok(());
    }

    // Concurrency control: if this instance has a concurrency key, check the limit.
    // Use position-based check to deterministically pick which instances proceed.
    if let (Some(ref key), Some(max)) = (&instance.concurrency_key, instance.max_concurrency) {
        let position = storage.concurrency_position(instance_id, key).await?;
        if position > i64::from(max) {
            // This instance is beyond the allowed concurrency — defer.
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

    let sequence = storage
        .get_sequence(instance.sequence_id)
        .await?
        .ok_or_else(|| EngineError::StepFailed {
            instance_id,
            block_id: orch8_types::ids::BlockId("_root".into()),
            message: format!("sequence {} not found", instance.sequence_id),
            retryable: false,
        })?;

    let existing_outputs = storage.get_all_outputs(instance_id).await?;
    let completed_blocks: std::collections::HashSet<&str> = existing_outputs
        .iter()
        .map(|o| o.block_id.0.as_str())
        .collect();

    for block in &sequence.blocks {
        if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
            if completed_blocks.contains(step_def.id.0.as_str()) {
                continue;
            }
            return execute_step_block(storage, handlers, webhook_config, &instance, step_def)
                .await;
        }
        // Composite blocks are Stage 3. Log and skip.
        log_skipped_composite(instance_id, block);
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

/// Check if the step has a delay and defer if so. Returns `true` if deferred.
async fn check_step_delay(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<bool, EngineError> {
    let Some(delay) = &step_def.delay else {
        return Ok(false);
    };

    let fire_at =
        crate::scheduling::delay::calculate_next_fire_at(Utc::now(), delay, &instance.timezone);

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

/// Execute a single step block within an instance.
async fn execute_step_block(
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    webhook_config: &WebhookConfig,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<(), EngineError> {
    let instance_id = instance.id;

    if check_step_delay(storage, instance, step_def).await? {
        return Ok(());
    }

    if check_step_rate_limit(storage, instance, step_def).await? {
        return Ok(());
    }

    // Determine attempt number from previous outputs for this block.
    let attempt = match storage.get_block_output(instance_id, &step_def.id).await? {
        Some(prev) => u32::from(prev.attempt.unsigned_abs()) + 1,
        None => 0,
    };

    crate::metrics::inc(crate::metrics::STEPS_EXECUTED);

    let exec_params = crate::handlers::step::StepExecParams {
        instance_id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        params: step_def.params.clone(),
        context: instance.context.clone(),
        attempt,
        timeout: step_def.timeout,
    };

    let result = crate::handlers::step::execute_step(storage, handlers, exec_params).await;

    match result {
        Ok(_output) => {
            // Step succeeded — re-schedule immediately for next block.
            crate::lifecycle::transition_instance(
                storage,
                instance_id,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(Utc::now()),
            )
            .await?;
            Ok(())
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
            .await
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

            Err(e)
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

            return Err(EngineError::StepFailed {
                instance_id,
                block_id: step_def.id.clone(),
                message: format!("{message} (exhausted {attempt} attempts)"),
                retryable: false,
            });
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

    Err(EngineError::StepFailed {
        instance_id,
        block_id: step_def.id.clone(),
        message: message.to_string(),
        retryable: true,
    })
}

fn log_skipped_composite(
    instance_id: orch8_types::ids::InstanceId,
    block: &orch8_types::sequence::BlockDefinition,
) {
    let block_id = match block {
        orch8_types::sequence::BlockDefinition::Parallel(p) => &p.id,
        orch8_types::sequence::BlockDefinition::Race(r) => &r.id,
        orch8_types::sequence::BlockDefinition::Loop(l) => &l.id,
        orch8_types::sequence::BlockDefinition::ForEach(f) => &f.id,
        orch8_types::sequence::BlockDefinition::Router(r) => &r.id,
        orch8_types::sequence::BlockDefinition::TryCatch(t) => &t.id,
        orch8_types::sequence::BlockDefinition::Step(_) => return,
    };
    warn!(
        instance_id = %instance_id,
        block_id = %block_id,
        "composite block type not yet implemented (Stage 3), skipping"
    );
}
