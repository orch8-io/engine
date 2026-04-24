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
/// Looks at the previous attempt's `created_at` as the start time. If the deadline
/// has elapsed, invokes the escalation handler, records the breach, and fails the instance.
/// Returns `true` if the deadline was breached and the instance was failed.
pub(super) async fn check_step_deadline(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    prev_output: Option<&orch8_types::output::BlockOutput>,
) -> Result<bool, EngineError> {
    let Some(deadline) = step_def.deadline else {
        return Ok(false);
    };
    // Compute elapsed time: use previous output's created_at if available
    // (retry scenario), otherwise fall back to instance runtime start time
    // (external worker scenario where no output exists yet).
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
                wait_for_input: None,
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
        attempt: prev_output.as_ref().map_or(0, |o| o.attempt),
        created_at: Utc::now(),
    };
    storage.save_block_output(&breach_output).await?;
    crate::lifecycle::transition_instance(
        storage.as_ref(),
        instance_id,
        Some(&instance.tenant_id),
        InstanceState::Running,
        InstanceState::Failed,
        None,
    )
    .await?;
    crate::metrics::inc(crate::metrics::INSTANCES_FAILED);

    // Wake parent: SLA deadline breach on a running child → terminal Failed.
    wake_parent_if_child(storage.as_ref(), instance).await;

    Ok(true)
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
/// If the response signal exists, stores it as block output and returns `false` (continue).
/// If not received yet, pauses the instance and returns `true` (deferred).
///
/// Visibility: `pub` so integration tests under `tests/human_input_coverage.rs`
/// can drive the signal-validation path directly without stringing up the full
/// tick loop. Still called exclusively from `process_step_block` in-crate.
#[allow(clippy::too_many_lines)]
pub async fn check_human_input(
    storage: &dyn StorageBackend,
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    human_def: &orch8_types::sequence::HumanInputDef,
) -> Result<bool, EngineError> {
    let mut signal_name = String::with_capacity(12 + step_def.id.0.len());
    signal_name.push_str("human_input:");
    signal_name.push_str(&step_def.id.0);

    // Check if the response signal has already been delivered.
    let signals = storage.get_pending_signals(instance.id).await?;
    for signal in &signals {
        if let orch8_types::signal::SignalType::Custom(name) = &signal.signal_type {
            if *name == signal_name {
                // Strict validation: payload must be `{"value": "<string>"}` where
                // `<string>` is one of `human_def.effective_choices()[].value`.
                // On malformed/invalid payload we mark the signal delivered (so a
                // poison message can't replay forever) but keep the block waiting
                // by falling through to the reschedule branch.
                let candidate = signal.payload.get("value").and_then(|v| v.as_str());
                let accepted = match candidate {
                    Some(v) => human_def.effective_choices().iter().any(|c| c.value == v),
                    None => false,
                };

                if !accepted {
                    warn!(
                        instance_id = %instance.id,
                        block_id = %step_def.id,
                        payload = %signal.payload,
                        "human input: payload rejected (missing `value` field or \
                         not in effective_choices); marking delivered, staying waiting"
                    );
                    storage.mark_signal_delivered(signal.id).await?;
                    continue;
                }

                // Valid input — canonical output shape and context merge.
                let value = candidate
                    .expect("candidate is Some when accepted")
                    .to_string();
                let store_key = human_def
                    .store_as
                    .clone()
                    .unwrap_or_else(|| step_def.id.0.clone());
                let output_json = serde_json::json!({"value": value.clone()});

                let output = orch8_types::output::BlockOutput {
                    id: uuid::Uuid::now_v7(),
                    instance_id: instance.id,
                    block_id: step_def.id.clone(),
                    output: output_json.clone(),
                    output_ref: None,
                    output_size: i32::try_from(output_json.to_string().len()).unwrap_or(i32::MAX),
                    attempt: 0,
                    created_at: chrono::Utc::now(),
                };
                storage.save_block_output(&output).await?;

                // Merge into `context.data[store_key]`. `merge_context_data` is a
                // per-key JSONB merge — the rest of `context.data` is preserved.
                // (Compare `StorageBackend::update_instance_context`, which is a
                // full replacement; see `storage_integration.rs` §5.2.)
                storage
                    .merge_context_data(instance.id, &store_key, &serde_json::Value::String(value))
                    .await?;

                storage.mark_signal_delivered(signal.id).await?;
                debug!(
                    instance_id = %instance.id,
                    block_id = %step_def.id,
                    store_key = %store_key,
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
    // The flat-path caller (execute_step_block) transitions Running → Scheduled
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
            if arr.iter().any(|v| v.as_str() == Some(&step_def.id.0)) {
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

    // Stamp per-step start time so wait_for_input timeouts are measured from
    // when this step began waiting, not from workflow start. We set this before
    // human-input check so the timeout baseline is correct when the instance
    // transitions to Waiting.
    let step_started = chrono::Utc::now();
    storage
        .update_instance_current_step_started_at(instance_id, step_started)
        .await?;

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
    // Fast path executes at most one step per instance per call, so a fresh
    // snapshot is all we need — the OnceCell is populated on first `get`
    // inside `resolve_templates_in_params` and dropped when this scope ends.
    let outputs = crate::handlers::param_resolve::OutputsSnapshot::new();
    let mut resolved_params = match crate::handlers::param_resolve::resolve_templates_in_params(
        storage.as_ref(),
        instance,
        &step_context,
        &step_def.params,
        &outputs,
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
                instance,
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
            instance,
            webhook_config,
            cancel,
            &step_err.to_string(),
        )
        .await;
    }

    // Circuit breaker pre-flight. Runs BEFORE the in-process vs external-worker
    // fork so it applies uniformly: an Open breaker on the primary handler
    // either (a) swaps dispatch to `fallback_handler` when one is configured,
    // or (b) defers the instance for the cooldown window. Skipped for pure
    // control-flow built-ins (see `circuit_breaker::is_breaker_tracked`) and
    // when no registry is wired (keeps pure-in-memory test configs unaffected).
    let cb_step_def: Cow<'_, orch8_types::sequence::StepDef> = {
        let primary_tracked = crate::circuit_breaker::is_breaker_tracked(&step_def.handler);
        if let (true, Some(cb)) = (primary_tracked, handlers.circuit_breakers()) {
            match cb.check(&instance.tenant_id, &step_def.handler) {
                Ok(()) => Cow::Borrowed(step_def),
                Err(remaining_secs) => {
                    // Primary is Open. Prefer `fallback_handler`; fall back to
                    // deferral only when no fallback is configured or the
                    // fallback's own breaker is also Open.
                    if let Some(fb) = step_def.fallback_handler.as_deref() {
                        // Re-check fallback's breaker. Skip-listed fallbacks
                        // (e.g. a `fail`-style sentinel) bypass the check.
                        let fb_remaining_open = if crate::circuit_breaker::is_breaker_tracked(fb) {
                            cb.check(&instance.tenant_id, fb).err()
                        } else {
                            None
                        };
                        if let Some(fb_rem) = fb_remaining_open {
                            let defer_secs = remaining_secs.max(fb_rem);
                            #[allow(clippy::cast_possible_wrap)]
                            let fire_at = Utc::now() + chrono::Duration::seconds(defer_secs as i64);
                            debug!(
                                instance_id = %instance_id,
                                primary = %step_def.handler,
                                fallback = %fb,
                                remaining_secs = defer_secs,
                                "both primary and fallback circuit breakers open, deferring instance"
                            );
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
                        debug!(
                            instance_id = %instance_id,
                            primary = %step_def.handler,
                            fallback = %fb,
                            "primary circuit breaker open, dispatching fallback"
                        );
                        let mut cloned = step_def.clone();
                        cloned.handler = fb.to_string();
                        Cow::Owned(cloned)
                    } else {
                        #[allow(clippy::cast_possible_wrap)]
                        let fire_at = Utc::now() + chrono::Duration::seconds(remaining_secs as i64);
                        debug!(
                            instance_id = %instance_id,
                            handler = %step_def.handler,
                            remaining_secs = remaining_secs,
                            "circuit breaker open, deferring instance"
                        );
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
                }
            }
        } else {
            Cow::Borrowed(step_def)
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
        // Use attempt=-1 so the memoization check in `execute_step_dry`
        // (which compares existing.attempt == exec.attempt) never matches
        // the sentinel — a real attempt is always >= 0.
        attempt: -1,
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
                    "failed to delete in-progress sentinel after step success — leaked row"
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
            // effect either didn't happen or is idempotent. Remove the
            // in-progress sentinel so the block is eligible for retry.
            //
            // Log failures rather than silently dropping them: if this
            // delete fails the retry cycle still works (the sentinel was
            // only there to block re-execution after a crash), but the
            // `block_outputs` table accumulates orphan rows that no cleanup
            // path will ever reclaim — surface the pattern to operators.
            if let Err(err) = storage
                .delete_block_outputs(instance_id, &step_def.id)
                .await
            {
                tracing::warn!(
                    instance_id = %instance_id,
                    block_id = %step_def.id,
                    error = %err,
                    "failed to delete in-progress sentinel after retryable failure — leaked row"
                );
            }

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
                        "block_id": step_def.id.0,
                    }),
                ),
                cancel,
            );

            // Wake parent: max retries exhausted → terminal Failed.
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

    // Wake parent: no retry policy → terminal Failed.
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

    // Wake parent: template/credential resolution failure → terminal Failed.
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
        attempt: i16::try_from(attempt).map_err(|_| {
            tracing::warn!(
                instance_id = %instance.id,
                attempt = %attempt,
                "attempt counter exceeds i16::MAX, saturating"
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

    // Transition instance Running → Waiting so the scheduler doesn't re-claim it.
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
