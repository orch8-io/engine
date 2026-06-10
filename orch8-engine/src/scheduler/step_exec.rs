//! Step execution fast-path and per-step pre-flight checks.
//!
//! These run when an instance only contains step blocks (no composites).
//! The evaluator handles the composite/tree path via a separate module.

use std::borrow::Cow;
use std::sync::Arc;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::config::WebhookConfig;
use orch8_types::instance::InstanceState;

use crate::error::EngineError;
use crate::handlers::HandlerRegistry;

use super::{wake_parent_if_child, StepOutcome};

/// Check if a step's SLA deadline has been breached (fast path).
/// Delegates to the shared `handle_deadline_breach` with `from_state = Running`.
/// Returns `true` if the deadline was breached and the instance was failed.
pub(super) async fn check_step_deadline(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    prev_output: Option<&orch8_types::output::BlockOutput>,
) -> Result<bool, EngineError> {
    super::handle_deadline_breach(
        storage,
        handlers,
        instance,
        step_def,
        prev_output,
        InstanceState::Running,
    )
    .await
}

/// Budget enforcement pre-flight: pause the instance if any configured
/// budget limit is exceeded. Returns `true` if the instance was paused (the
/// caller must skip step execution for this tick).
///
/// Cost model: instances without a budget short-circuit with zero extra
/// queries; instances with only `max_steps` use the in-memory
/// `context.runtime.total_steps_executed` counter (still zero queries);
/// token limits cost one `usage_events` aggregate query.
///
/// On breach the instance is paused via the same `transition_instance`
/// pattern as `check_human_input` (CAS Running -> Paused) and the
/// machine-readable reason is merged into `metadata`:
/// `{"paused_reason": "budget_exceeded", "budget_breach": {...}}`.
/// Resume via the normal `Paused -> Scheduled` transition; see
/// [`orch8_types::instance::Budget`] for the one-more-tick v1 semantics.
pub(super) async fn check_budget(
    storage: &Arc<dyn StorageBackend>,
    instance: &orch8_types::instance::TaskInstance,
) -> Result<bool, EngineError> {
    let Some(budget) = &instance.budget else {
        return Ok(false);
    };

    let steps = i64::from(instance.context.runtime.total_steps_executed);
    // Only pay for the usage_events aggregate when a token limit is set.
    let (input_tokens, output_tokens) = if budget.has_token_limits() {
        storage.query_instance_usage_totals(instance.id).await?
    } else {
        (0, 0)
    };

    let Some(breach) = budget.first_breach(input_tokens, output_tokens, steps) else {
        return Ok(false);
    };

    warn!(
        instance_id = %instance.id,
        limit = breach.limit,
        limit_value = breach.limit_value,
        actual = breach.actual,
        "instance exceeded budget, pausing"
    );

    // Record the machine-readable reason BEFORE the state flip so an observer
    // who sees Paused is guaranteed to also see the breach metadata.
    storage
        .merge_instance_metadata(
            instance.id,
            &serde_json::json!({
                "paused_reason": "budget_exceeded",
                "budget_breach": {
                    "limit": breach.limit,
                    "limit_value": breach.limit_value,
                    "actual": breach.actual,
                },
            }),
        )
        .await?;

    crate::lifecycle::transition_instance(
        storage.as_ref(),
        instance.id,
        Some(&instance.tenant_id),
        InstanceState::Running,
        InstanceState::Paused,
        None,
    )
    .await?;

    Ok(true) // Paused — skip step execution this tick.
}

/// Check if the step has a delay and defer if so. Returns `true` if deferred.
pub(super) async fn check_step_delay(
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
        Some(&instance.tenant_id),
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
pub(super) async fn check_send_window(
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
        Some(&instance.tenant_id),
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
pub(super) async fn check_step_rate_limit(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) -> Result<bool, EngineError> {
    let Some(key) = &step_def.rate_limit_key else {
        return Ok(false);
    };

    let resource_key = orch8_types::ids::ResourceKey::new(key.clone());
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
            Some(&instance.tenant_id),
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
///
/// If the response signal exists, stores it as block output and returns `false` (continue).
/// If not received yet, pauses the instance and returns `true` (deferred).
///
/// Visibility: `pub` so integration tests under `tests/human_input_coverage.rs`
/// can drive the signal-validation path directly without stringing up the full
/// tick loop. Still called exclusively from `process_step_block` in-crate.
/// Accept a human-input `value` for a gated step: persist the canonical
/// `{ "value": <choice> }` block output and merge it into
/// `context.data[store_key]`. Shared by the real signal path and the dry-run
/// auto-approve path so both produce identical post-approval state.
async fn accept_human_input(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    human_def: &orch8_types::sequence::HumanInputDef,
    value: String,
) -> Result<(), EngineError> {
    let store_key = human_def
        .store_as
        .clone()
        .unwrap_or_else(|| step_def.id.as_str().to_owned());
    let output_json = serde_json::json!({ "value": value.clone() });

    let output = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        output: output_json.clone(),
        output_ref: None,
        output_size: u32::try_from(output_json.to_string().len()).unwrap_or(u32::MAX),
        attempt: 0,
        created_at: chrono::Utc::now(),
    };
    storage.save_block_output(&output).await?;

    // Per-key JSONB merge — the rest of `context.data` is preserved.
    storage
        .merge_context_data(instance.id, &store_key, &serde_json::Value::String(value))
        .await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub async fn check_human_input(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    human_def: &orch8_types::sequence::HumanInputDef,
) -> Result<bool, EngineError> {
    // Dry-run auto-approve: resolve the gate with the default (first) choice so
    // the simulation flows past human gates instead of pausing. Behaviorally
    // identical to a received valid signal carrying that choice. Only when both
    // dry_run and dry_run_auto_approve are set on the instance.
    //
    // Note: always picks the first effective choice, so a dry-run only exercises
    // the default branch of any post-gate routing — not the alternatives.
    if instance.context.runtime.dry_run && instance.context.runtime.dry_run_auto_approve {
        let value = human_def
            .effective_choices()
            .into_iter()
            .next()
            .map_or_else(|| "approved".to_string(), |c| c.value);
        debug!(
            instance_id = %instance.id,
            block_id = %step_def.id,
            choice = %value,
            "dry-run: auto-approving human gate with default choice"
        );
        accept_human_input(storage, instance, step_def, human_def, value).await?;
        return Ok(false); // Continue execution
    }

    let mut signal_name = String::with_capacity(12 + step_def.id.as_str().len());
    signal_name.push_str("human_input:");
    signal_name.push_str(step_def.id.as_str());

    // Check if the response signal has already been delivered.
    let signals = storage.get_pending_signals(instance.id).await?;
    // Compute the allowed choices once, not per matching signal (each call
    // allocates a fresh Vec).
    let effective_choices = human_def.effective_choices();
    for signal in &signals {
        if let orch8_types::signal::SignalType::Custom(name) = &signal.signal_type {
            if *name == signal_name {
                // Strict validation: payload must be `{"value": "<string>"}` where
                // `<string>` is one of `human_def.effective_choices()[].value`.
                // On malformed/invalid payload we mark the signal delivered (so a
                // poison message can't replay forever) but keep the block waiting
                // by falling through to the reschedule branch.
                let candidate = signal.payload.get("value").and_then(|v| v.as_str());
                let Some(value) =
                    candidate.filter(|v| effective_choices.iter().any(|c| c.value == **v))
                else {
                    warn!(
                        instance_id = %instance.id,
                        block_id = %step_def.id,
                        payload = %signal.payload,
                        "human input: payload rejected (missing `value` field or \
                         not in effective_choices); marking delivered, staying waiting"
                    );
                    storage.mark_signal_delivered(signal.id).await?;
                    continue;
                };

                // Valid input — canonical output shape and context merge.
                accept_human_input(storage, instance, step_def, human_def, value.to_string())
                    .await?;
                storage.mark_signal_delivered(signal.id).await?;
                debug!(
                    instance_id = %instance.id,
                    block_id = %step_def.id,
                    "human input accepted"
                );
                return Ok(false); // Continue execution
            }
        }
    }

    // No response yet. Check timeout.
    if let Some(timeout) = human_def.timeout {
        let baseline = instance
            .context
            .runtime
            .current_step_started_at
            .or(instance.context.runtime.started_at);
        if let Some(started) = baseline {
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
                    // Re-schedule immediately so the next tick sees the block
                    // output in completed_blocks and skips this step.
                    crate::lifecycle::transition_instance(
                        storage,
                        instance.id,
                        Some(&instance.tenant_id),
                        InstanceState::Running,
                        InstanceState::Scheduled,
                        Some(chrono::Utc::now()),
                    )
                    .await?;
                    return Ok(true); // Deferred — do NOT fall through to the step handler
                }
                return Err(EngineError::StepTimeout {
                    block_id: step_def.id.clone(),
                    timeout,
                });
            }
        }
    }

    // Still waiting — caller is responsible for the instance state transition.
    // The flat-path caller (execute_step_block) transitions Running -> Scheduled
    // with a delay; the tree-path caller (execute_step_node) sets the node to
    // Waiting and lets process_tick transition the instance to Waiting.
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
pub(super) async fn execute_step_block(
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
            if arr.iter().any(|v| v.as_str() == Some(step_def.id.as_str())) {
                debug!(
                    instance_id = %instance_id,
                    block_id = %step_def.id,
                    "debug breakpoint hit, pausing instance"
                );
                crate::lifecycle::transition_instance(
                    storage.as_ref(),
                    instance_id,
                    Some(&instance.tenant_id),
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

    // Stamp per-step start time and current step ID so wait_for_input
    // timeouts are measured correctly and the mobile notifier can identify
    // which step the instance is waiting on.
    {
        let step_started = chrono::Utc::now();
        if let Some(mut inst) = storage.get_instance(instance_id).await? {
            let expected_updated_at = inst.updated_at;
            inst.context.runtime.current_step = Some(step_def.id.clone());
            inst.context.runtime.current_step_started_at = Some(step_started);
            storage
                .update_instance_context_cas(instance_id, &inst.context, expected_updated_at)
                .await?;
        }
    }

    // Human-in-the-loop: if this step waits for human input, check if a response
    // signal has been delivered. If not, transition to Waiting so the approvals
    // API can discover the instance and process_signalled_instances will wake it
    // when the signal arrives.
    if let Some(human_def) = &step_def.wait_for_input {
        if check_human_input(storage.as_ref(), instance, step_def, human_def).await? {
            crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
                InstanceState::Running,
                InstanceState::Waiting,
                None,
            )
            .await?;
            return Ok(StepOutcome::Deferred);
        }
    }

    // Determine attempt number from previous output for this block. Shared
    // helper keeps retry-count semantics identical to the tree evaluator.
    let attempt = crate::handlers::param_resolve::compute_attempt(
        storage.as_ref(),
        instance_id,
        &step_def.id,
    )
    .await?;

    // Resolve context snapshot + templates + credentials + cache key (shared
    // with the tree path via `step_block::prepare_step`). The fast path runs at
    // most one step per call, so a fresh outputs snapshot is all we need.
    // Template/credential failures fail the *instance*; infra errors propagate.
    let outputs = crate::handlers::param_resolve::OutputsSnapshot::new();
    let crate::handlers::step_block::PreparedStep {
        step_context,
        resolved_params,
        resolved_cache_key,
    } = match crate::handlers::step_block::prepare_step(
        storage.as_ref(),
        instance,
        step_def,
        &outputs,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(crate::handlers::step_block::PrepareError::Infra(e)) => return Err(e),
        Err(other) => {
            return fail_instance_with_error(
                storage.as_ref(),
                instance,
                webhook_config,
                cancel,
                &other.fail_message(),
            )
            .await;
        }
    };

    // Circuit breaker pre-flight (shared with the tree path via
    // `step_block::breaker_preflight`). Runs BEFORE the in-process vs
    // external-worker fork so it applies uniformly: an Open breaker either swaps
    // dispatch to `fallback_handler` or defers the instance for the cooldown
    // window.
    let cb_step_def: Cow<'_, orch8_types::sequence::StepDef> =
        match crate::handlers::step_block::breaker_preflight(
            handlers,
            instance_id,
            &instance.tenant_id,
            step_def,
        ) {
            crate::handlers::step_block::BreakerDecision::Proceed(cow) => cow,
            crate::handlers::step_block::BreakerDecision::Defer { fire_at } => {
                crate::lifecycle::transition_instance(
                    storage.as_ref(),
                    instance_id,
                    Some(&instance.tenant_id),
                    InstanceState::Running,
                    InstanceState::Scheduled,
                    Some(fire_at),
                )
                .await?;
                return Ok(StepOutcome::Deferred);
            }
        };
    // Shadow `step_def` so the rest of dispatch transparently sees the
    // fallback-swapped version (if any). `cb_step_def` owns the borrow.
    let step_def = cb_step_def.as_ref();
    // Recompute tracked-ness for the (possibly swapped) effective handler so
    // record_success/record_failure further down gate on the right name.
    let breaker_tracked = crate::circuit_breaker::is_breaker_tracked(&step_def.handler);

    // If the handler is not registered in-process, dispatch to an external
    // worker queue. This mirrors the tree evaluator path in `step_block.rs`
    // which always dispatches unregistered handlers to external workers.
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

    // Write a sentinel output BEFORE executing the handler. If the process
    // crashes between handler execution and the real output save, recovery
    // will see this row and skip the block — preventing double side effects
    // (e.g. duplicate emails, duplicate HTTP POST calls). The real output is
    // appended as a new row afterwards; `get_block_output` (most-recent)
    // will return the real one.
    let sentinel = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id,
        block_id: step_def.id.clone(),
        output: serde_json::json!({"_sentinel": true, "_handler": &step_def.handler}),
        output_ref: Some("__in_progress__".into()),
        output_size: 0,
        // Use attempt=u16::MAX so the memoization check in `execute_step_dry`
        // (which compares existing.attempt == exec.attempt) never matches
        // the sentinel — a real attempt never reaches u16::MAX.
        attempt: u16::MAX,
        created_at: chrono::Utc::now(),
    };
    storage.save_block_output(&sentinel).await?;

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
        wait_for_input: step_def.wait_for_input.clone(),
        cache_key: resolved_cache_key,
    };

    let result = crate::handlers::step::execute_step_dry(storage, handlers, exec_params).await;

    match result {
        Ok(block_output) => {
            storage.save_block_output(&block_output).await?;

            // Handler succeeded — reset the (tenant, handler) breaker. A
            // successful call is the probe that closes a half-open breaker
            // and clears any accumulated failure count.
            if breaker_tracked {
                if let Some(cb) = handlers.circuit_breakers() {
                    cb.record_success(&instance.tenant_id, &step_def.handler);
                }
            }

            // Clean up the sentinel now that the real output is persisted.
            // The sentinel served its purpose (preventing double-execution if
            // the process crashed during handler execution). Leaving it would
            // double the output count visible to callers of `get_all_outputs`.
            //
            // Swallowing the error was convenient but hid a real operational
            // problem — if this delete keeps failing, `block_outputs` grows
            // unboundedly with `__in_progress__` rows that never get
            // reclaimed. A warn-log makes the leak observable without
            // failing the step (the real output is already saved — the
            // instance must advance even if the sentinel lingers).
            if let Err(err) = storage.delete_block_output_by_id(sentinel.id).await {
                tracing::warn!(
                    instance_id = %instance_id,
                    block_id = %step_def.id,
                    sentinel_id = %sentinel.id,
                    error = %err,
                    "failed to delete in-progress sentinel after step success -- leaked row"
                );
            }

            // Check for self-modify output: inject blocks into the instance.
            // Shared helper keeps injection semantics identical to the tree
            // evaluator; fast path then transitions back to Scheduled so the
            // next tick sees the newly injected steps.
            match crate::handlers::param_resolve::apply_self_modify(
                storage.as_ref(),
                instance_id,
                &block_output.output,
            )
            .await
            {
                crate::handlers::param_resolve::SelfModifyResult::Applied => {
                    crate::lifecycle::transition_instance(
                        storage.as_ref(),
                        instance_id,
                        Some(&instance.tenant_id),
                        InstanceState::Running,
                        InstanceState::Scheduled,
                        Some(chrono::Utc::now()),
                    )
                    .await?;
                    Ok(StepOutcome::Deferred)
                }
                crate::handlers::param_resolve::SelfModifyResult::NotApplicable => {
                    Ok(StepOutcome::Completed)
                }
                crate::handlers::param_resolve::SelfModifyResult::Failed => Ok(StepOutcome::Failed),
            }
        }
        Err(EngineError::StepFailed {
            retryable: true,
            ref message,
            ..
        }) => {
            crate::metrics::inc(crate::metrics::STEPS_FAILED);
            // Record the failure with the circuit breaker. Retryable failures
            // count toward tripping the breaker so a consistently-failing
            // dependency eventually protects the workers from burning retries
            // on a hopeless handler.
            if breaker_tracked {
                if let Some(cb) = handlers.circuit_breakers() {
                    cb.record_failure(&instance.tenant_id, &step_def.handler);
                }
            }
            // Handler returned cleanly with a retryable error — the side
            // effect either didn't happen or is idempotent. Clear the
            // in-progress sentinel so the block is eligible for retry, then
            // persist a retry marker so the attempt counter advances.
            //
            // The marker is essential: `compute_attempt` derives the next
            // attempt from the most-recent `BlockOutput`. The sentinel we
            // just deleted carried `attempt = u16::MAX`, and without a
            // replacement marker `get_block_output` would return `None` on
            // the next tick → `attempt` resets to 0 → `handle_retryable_failure`
            // never sees `attempt >= max_attempts` → the step retries forever
            // and never reaches the DLQ. This mirrors the tree path's
            // `__retry__` marker in `handlers::step_block` so retry-count
            // semantics are identical across both dispatch paths.
            //
            // Log failures rather than silently dropping them: a failed
            // delete leaks an orphan sentinel row, and a failed marker save
            // stalls the attempt counter (worst case: a few extra retries).
            if let Err(err) = storage
                .delete_block_outputs(instance_id, &step_def.id)
                .await
            {
                tracing::warn!(
                    instance_id = %instance_id,
                    block_id = %step_def.id,
                    error = %err,
                    "failed to delete in-progress sentinel after retryable failure -- leaked row"
                );
            }
            let retry_marker = orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id,
                block_id: step_def.id.clone(),
                output: serde_json::json!({ "_retry_marker": true, "error": message }),
                output_ref: Some("__retry__".into()),
                output_size: 0,
                attempt: u16::try_from(attempt).unwrap_or(u16::MAX),
                created_at: chrono::Utc::now(),
            };
            if let Err(err) = storage.save_block_output(&retry_marker).await {
                tracing::warn!(
                    instance_id = %instance_id,
                    block_id = %step_def.id,
                    error = %err,
                    "failed to save retry marker after retryable failure -- attempt counter may not advance"
                );
            }

            // If a step handler drove a pause/cancel transition mid-flight
            // (see `handle_sleep` in `handlers/builtin.rs`), the instance
            // already sits in `Paused` or `Cancelled`. Treat the
            // `Retryable` as a benign yield in that case — falling through
            // to `handle_retryable_failure` would otherwise attempt an
            // invalid `Paused->Failed` transition and error out.
            let current = storage
                .get_instance(instance_id)
                .await?
                .map_or(InstanceState::Running, |i| i.state);
            if matches!(current, InstanceState::Paused | InstanceState::Cancelled) {
                return Ok(StepOutcome::Deferred);
            }
            handle_retryable_failure(
                storage.as_ref(),
                instance,
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
            // Permanent step failures also trip the breaker: if a handler is
            // consistently returning `StepError::Permanent` for every input we
            // still want to stop hammering it. (Template/credential errors
            // fail earlier and never reach this arm, so they can't poison the
            // breaker.)
            if breaker_tracked {
                if let Some(cb) = handlers.circuit_breakers() {
                    cb.record_failure(&instance.tenant_id, &step_def.handler);
                }
            }
            // Keep the sentinel on permanent failure — the handler already
            // executed (side effects may have occurred). If the user later
            // triggers a DLQ retry, the sentinel ensures this step is
            // skipped rather than re-executed (preventing double emails,
            // duplicate HTTP calls, etc.).

            // Permanent failure or timeout.
            crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&instance.tenant_id),
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

            // Wake parent if this child reached a terminal Failed state via a
            // permanent step error — the tree-evaluator branches above are
            // bypassed when the step handler itself returns
            // `StepError::Permanent`.
            wake_parent_if_child(storage.as_ref(), instance).await;

            Ok(StepOutcome::Failed)
        }
    }
}

pub(super) async fn handle_retryable_failure(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    attempt: u32,
    webhook_config: &WebhookConfig,
    message: &str,
    cancel: &CancellationToken,
) -> Result<(), EngineError> {
    let instance_id = instance.id;
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
                Some(&instance.tenant_id),
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
                        "block_id": step_def.id.as_str(),
                    }),
                ),
                cancel,
            );

            // Wake parent: max retries exhausted -> terminal Failed.
            wake_parent_if_child(storage, instance).await;

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
            Some(&instance.tenant_id),
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
        Some(&instance.tenant_id),
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

    // Wake parent: no retry policy -> terminal Failed.
    wake_parent_if_child(storage, instance).await;

    Ok(())
}

/// Transition the instance to `Failed` and emit an `instance.failed`
/// webhook carrying the supplied error message. Used when step-param
/// resolution (templates or credentials) fails before the handler is even
/// invoked — there is no retry policy for these malformed-params errors,
/// so the instance is failed immediately.
pub(super) async fn fail_instance_with_error(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    webhook_config: &WebhookConfig,
    cancel: &CancellationToken,
    error: &str,
) -> Result<StepOutcome, EngineError> {
    let instance_id = instance.id;
    crate::metrics::inc(crate::metrics::STEPS_FAILED);
    crate::lifecycle::transition_instance(
        storage,
        instance_id,
        Some(&instance.tenant_id),
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

    // Wake parent: template/credential resolution failure -> terminal Failed.
    wake_parent_if_child(storage, instance).await;

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
pub(super) async fn dispatch_to_external_worker(
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
        attempt: u16::try_from(attempt).map_err(|_| {
            tracing::warn!(
                instance_id = %instance.id,
                attempt = %attempt,
                "attempt counter exceeds u16::MAX, saturating"
            );
            orch8_types::error::StorageError::Query("attempt counter overflow".into())
        })?,
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

    // Transition instance Running -> Waiting so the scheduler doesn't re-claim it.
    crate::lifecycle::transition_instance(
        storage,
        instance.id,
        Some(&instance.tenant_id),
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
