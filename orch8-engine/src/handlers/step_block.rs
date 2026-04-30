use std::borrow::Cow;
use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::execution::ExecutionNode;
use orch8_types::ids::InstanceId;
use orch8_types::instance::TaskInstance;
use orch8_types::plugin::PluginType;
use orch8_types::sequence::StepDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::externalized;
use crate::handlers::param_resolve::OutputsSnapshot;
use crate::handlers::step::StepExecParams;
use crate::handlers::HandlerRegistry;

use super::step_dispatch::{
    dispatch_plugin, dispatch_step_to_external_worker, resolve_plugin_source,
};

/// Walk `context.data` top-level fields and inflate any externalization
/// markers found. Does not recurse into nested objects — only top-level
/// keys can be externalized. See `docs/CONTEXT_MANAGEMENT.md` §8.5.
///
/// If the referenced payload is missing, the marker is left in place so
/// downstream code can detect the broken reference.
///
/// Delegates to [`externalized::resolve_context_markers`] so every engine
/// site (step dispatch, router conditions, …) shares one implementation.
async fn resolve_markers(
    storage: &dyn StorageBackend,
    _instance_id: InstanceId,
    ctx: ExecutionContext,
) -> Result<ExecutionContext, EngineError> {
    externalized::resolve_context_markers(storage, ctx)
        .await
        .map_err(EngineError::Storage)
}

/// Build the context snapshot a step will see, honouring its
/// `context_access` declaration and inflating externalization markers.
/// Mirrors the fast-path logic in `scheduler.rs` so both dispatch paths
/// enforce the same policy.
pub(crate) async fn context_for_step(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    step_def: &StepDef,
) -> Result<ExecutionContext, EngineError> {
    let filtered = match &step_def.context_access {
        Some(access) => instance.context.filtered(access),
        None => instance.context.clone(),
    };
    resolve_markers(storage, instance.id, filtered).await
}

/// Execute a step node within the execution tree.
/// Returns `true` if the instance has more work (should re-schedule).
#[allow(clippy::too_many_lines)]
pub async fn execute_step_node(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    step_def: &StepDef,
    outputs: &OutputsSnapshot,
) -> Result<bool, EngineError> {
    // Human-in-the-loop: if this step has `wait_for_input`, check for a
    // matching signal before running the handler. If no signal exists yet,
    // set the node to Waiting so the tree evaluator transitions the instance
    // to Waiting state. When the signal arrives, process_signalled_instances
    // transitions the instance back to Scheduled, and the next evaluator pass
    // finds the signal and completes the node.
    if let Some(ref human_def) = step_def.wait_for_input {
        let deferred =
            crate::scheduler::check_human_input(storage.as_ref(), instance, step_def, human_def)
                .await?;
        if deferred {
            // No signal yet — set node to Waiting so the instance transitions
            // to Waiting state and process_signalled_instances can wake it.
            storage
                .update_node_state(node.id, orch8_types::execution::NodeState::Waiting)
                .await?;
            return Ok(true);
        }
        // Signal accepted — fall through to run the handler.
    }

    // Determine attempt number from previous output for this block. Mirrors
    // the fast-path logic in `scheduler::step_exec` so retry-count semantics
    // (and any memoisation keyed on `(instance_id, block_id, attempt)`) are
    // identical across both dispatch sites.
    let attempt =
        super::param_resolve::compute_attempt(storage.as_ref(), instance.id, &step_def.id).await?;

    // Build the step's context snapshot once and reuse across both template
    // resolution and every downstream dispatch branch. `context_for_step` only
    // reads `instance.context` and `step_def.context_access` — it does not
    // depend on step params, so evaluating it before template resolution is
    // safe and avoids duplicate work.
    let step_context = context_for_step(storage.as_ref(), instance, step_def).await?;

    // Resolve `{{path}}` templates first. User templates should be fully
    // materialised before any further transformation — this lets an author
    // write `{{context.data.cred_id}}` that evaluates to a `credentials://…`
    // ref, which the following credential pass then substitutes. Template
    // errors (unknown context section, unknown root) fail the node the same
    // way credential errors do.
    let mut resolved_params = match super::param_resolve::resolve_templates_in_params(
        storage.as_ref(),
        instance,
        &step_context,
        &step_def.params,
        outputs,
    )
    .await
    {
        Ok(p) => p,
        Err(tpl_err) => {
            tracing::error!(
                instance_id = %instance.id,
                block_id = %step_def.id,
                error = %tpl_err,
                "failed to resolve templates for step"
            );
            evaluator::fail_node(storage.as_ref(), node.id).await?;
            return Ok(false);
        }
    };

    // Resolve `credentials://` references in step params before handing off to
    // any handler. Resolving once up front means every dispatch target (AP,
    // gRPC, WASM, in-process) sees the same expanded params without having to
    // know about the credential registry. Missing/disabled/cross-tenant refs
    // surface as StepError::Permanent which fails the node immediately.
    if let Err(step_err) = crate::credentials::resolve_in_value(
        storage.as_ref(),
        &instance.tenant_id.0,
        &mut resolved_params,
    )
    .await
    {
        tracing::warn!(
            instance_id = %instance.id,
            block_id = %step_def.id,
            error = ?step_err,
            "failed to resolve credentials for step"
        );
        evaluator::fail_node(storage.as_ref(), node.id).await?;
        return Ok(false);
    }

    // Resolve cache_key template if present.
    let resolved_cache_key = if let Some(ref ck) = step_def.cache_key {
        if crate::template::contains_template(&serde_json::Value::String(ck.clone())) {
            let wrapped = serde_json::Value::String(ck.clone());
            match crate::template::resolve(&wrapped, &step_context, &serde_json::json!({})) {
                Ok(serde_json::Value::String(s)) => Some(s),
                _ => Some(ck.clone()),
            }
        } else {
            Some(ck.clone())
        }
    } else {
        None
    };

    // Each dispatch branch below ends in `return`, so we can *move*
    // `resolved_params` and `step_context` into the branch that matches —
    // later branches are unreachable from that point. The clone calls were
    // defensive but redundant and measurable on the step hot path.

    // Circuit breaker pre-flight for the tree path. Mirrors the fast-path
    // check in `scheduler::step_exec::execute_step_block`: when the breaker
    // for this (tenant, handler) is Open, either swap dispatch to the step's
    // `fallback_handler` or push the instance back to `Scheduled` with a
    // `fire_at` of now + remaining cooldown so the tick loop stops churning
    // on it. The node stays `Running`; when the evaluator re-enters after
    // cooldown, the breaker will be HalfOpen and the next attempt probes
    // the handler.
    let cb_step_def: Cow<'_, StepDef> = {
        let primary_tracked = crate::circuit_breaker::is_breaker_tracked(&step_def.handler);
        if let (true, Some(cb)) = (primary_tracked, handlers.circuit_breakers()) {
            match cb.check(&instance.tenant_id, &step_def.handler) {
                Ok(()) => Cow::Borrowed(step_def),
                Err(remaining_secs) => {
                    if let Some(fb) = step_def.fallback_handler.as_deref() {
                        let fb_remaining_open = if crate::circuit_breaker::is_breaker_tracked(fb) {
                            cb.check(&instance.tenant_id, fb).err()
                        } else {
                            None
                        };
                        if let Some(fb_rem) = fb_remaining_open {
                            let defer_secs = remaining_secs.max(fb_rem);
                            #[allow(clippy::cast_possible_wrap)]
                            let fire_at =
                                chrono::Utc::now() + chrono::Duration::seconds(defer_secs as i64);
                            tracing::debug!(
                                instance_id = %instance.id,
                                primary = %step_def.handler,
                                fallback = %fb,
                                remaining_secs = defer_secs,
                                "both primary and fallback circuit breakers open in tree path, deferring instance"
                            );
                            storage
                                .update_instance_state(
                                    instance.id,
                                    orch8_types::instance::InstanceState::Scheduled,
                                    Some(fire_at),
                                )
                                .await?;
                            return Ok(false);
                        }
                        tracing::debug!(
                            instance_id = %instance.id,
                            primary = %step_def.handler,
                            fallback = %fb,
                            "primary circuit breaker open in tree path, dispatching fallback"
                        );
                        let mut cloned = step_def.clone();
                        cloned.handler = fb.to_string();
                        Cow::Owned(cloned)
                    } else {
                        #[allow(clippy::cast_possible_wrap)]
                        let fire_at =
                            chrono::Utc::now() + chrono::Duration::seconds(remaining_secs as i64);
                        tracing::debug!(
                            instance_id = %instance.id,
                            handler = %step_def.handler,
                            remaining_secs = remaining_secs,
                            "circuit breaker open in tree path, deferring instance"
                        );
                        storage
                            .update_instance_state(
                                instance.id,
                                orch8_types::instance::InstanceState::Scheduled,
                                Some(fire_at),
                            )
                            .await?;
                        return Ok(false);
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
    let breaker_tracked = crate::circuit_breaker::is_breaker_tracked(&step_def.handler);

    // If the handler is an ActivePieces sidecar call, dispatch via HTTP to the
    // Node worker. No plugin-registry lookup needed — the endpoint is a single
    // env-configured sidecar, and piece/action names live in the handler string.
    if super::activepieces::is_ap_handler(&step_def.handler) {
        let handler_name = step_def.handler.clone();
        let ctx = super::StepContext {
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step_def.id.clone(),
            params: resolved_params,
            context: step_context,
            attempt,
            storage: Arc::clone(storage),
            wait_for_input: step_def.wait_for_input.clone(),
        };
        return dispatch_plugin(storage.as_ref(), node, attempt, move || async move {
            super::activepieces::handle_ap(ctx, &handler_name).await
        })
        .await;
    }

    // If the handler is a gRPC plugin, resolve via the plugin registry then dispatch.
    if super::grpc_plugin::is_grpc_handler(&step_def.handler) {
        let Some(endpoint) =
            resolve_plugin_source(storage.as_ref(), &step_def.handler, PluginType::Grpc).await
        else {
            tracing::warn!(
                instance_id = %instance.id,
                handler = %step_def.handler,
                "gRPC plugin not registered; failing node (raw-endpoint fallback is disabled)"
            );
            evaluator::fail_node(storage.as_ref(), node.id).await?;
            return Ok(false);
        };
        let mut params = resolved_params;
        params["_grpc_endpoint"] = serde_json::Value::String(endpoint);
        let ctx = super::StepContext {
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step_def.id.clone(),
            params,
            context: step_context,
            attempt,
            storage: Arc::clone(storage),
            wait_for_input: step_def.wait_for_input.clone(),
        };
        return dispatch_plugin(storage.as_ref(), node, attempt, || {
            super::grpc_plugin::handle_grpc_plugin(ctx)
        })
        .await;
    }

    // If the handler is a WASM plugin, resolve via the plugin registry then dispatch.
    if super::wasm_plugin::is_wasm_handler(&step_def.handler) {
        if let Some(plugin_name) = super::wasm_plugin::parse_plugin_name(&step_def.handler) {
            // SECURITY: WASM source MUST come from the plugin registry — which
            // is the only place an operator has curated and validated the
            // filesystem path. Previously this code fell back to the raw
            // handler string (`plugin_name`) when the registry had no entry,
            // turning `wasm://../../etc/passwd` or `wasm:///abs/path/evil.wasm`
            // into a direct `Module::from_file` call — arbitrary-file-read /
            // arbitrary-code-execute surface for anyone who can submit a
            // sequence definition. Fail closed: no registry row → fail the
            // node with a permanent error.
            let Some(wasm_path) =
                resolve_plugin_source(storage.as_ref(), plugin_name, PluginType::Wasm).await
            else {
                tracing::warn!(
                    instance_id = %instance.id,
                    plugin = %plugin_name,
                    "wasm plugin not registered; failing node (raw-path fallback is disabled)"
                );
                evaluator::fail_node(storage.as_ref(), node.id).await?;
                return Ok(false);
            };
            let ctx = super::StepContext {
                instance_id: instance.id,
                tenant_id: instance.tenant_id.clone(),
                block_id: step_def.id.clone(),
                params: resolved_params,
                context: step_context,
                attempt,
                storage: Arc::clone(storage),
                wait_for_input: step_def.wait_for_input.clone(),
            };
            return dispatch_plugin(storage.as_ref(), node, attempt, || {
                super::wasm_plugin::handle_wasm_plugin(ctx, &wasm_path)
            })
            .await;
        }
    }

    // If the handler is not registered in-process, dispatch to external worker queue.
    let handler_registered = handlers.contains(&step_def.handler);
    if !handler_registered {
        return dispatch_step_to_external_worker(
            storage.as_ref(),
            instance,
            node,
            step_def,
            resolved_params,
            step_context,
            attempt,
        )
        .await;
    }

    let exec_params = StepExecParams {
        instance_id: instance.id,
        tenant_id: instance.tenant_id.clone(),
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        params: resolved_params,
        context: step_context,
        attempt,
        timeout: step_def.timeout,
        externalize_threshold: 0, // Tree evaluator does not externalize (no config available)
        wait_for_input: step_def.wait_for_input.clone(),
        cache_key: resolved_cache_key,
    };

    match crate::handlers::step::execute_step(storage, handlers, exec_params).await {
        Ok(output) => {
            if breaker_tracked {
                if let Some(cb) = handlers.circuit_breakers() {
                    cb.record_success(&instance.tenant_id, &step_def.handler);
                }
            }
            // Check for self-modify output: inject blocks into the instance.
            // Shared helper keeps block-injection semantics identical to the
            // fast-path dispatch in `scheduler::step_exec`.
            match super::param_resolve::apply_self_modify(storage.as_ref(), instance.id, &output)
                .await
            {
                super::param_resolve::SelfModifyResult::Applied
                | super::param_resolve::SelfModifyResult::NotApplicable => {
                    evaluator::complete_node(storage.as_ref(), node.id).await?;
                    Ok(true)
                }
                super::param_resolve::SelfModifyResult::Failed => {
                    evaluator::fail_node(storage.as_ref(), node.id).await?;
                    Ok(false)
                }
            }
        }
        Err(EngineError::StepFailed {
            retryable: true,
            ref message,
            ..
        }) => {
            if breaker_tracked {
                if let Some(cb) = handlers.circuit_breakers() {
                    cb.record_failure(&instance.tenant_id, &step_def.handler);
                }
            }
            // Previously this arm left the node Running and returned Ok,
            // which caused the evaluator's `find_running_step` to re-pick
            // the same node every iteration within the current tick — a
            // hot loop that ignored both the retry policy's `max_attempts`
            // and any configured backoff. Mirror the fast-path semantics
            // in `scheduler::step_exec::handle_retryable_failure`: honour
            // `max_attempts` and re-schedule the instance with backoff so
            // the next attempt only runs after the configured delay.
            //
            // To make the attempt counter advance across retries, persist
            // a retry marker `BlockOutput`. `compute_attempt` derives the
            // next attempt number from the most-recent output, so without
            // a marker the count would reset to 0 each tick. The marker
            // carries the numeric attempt of the *just-failed* try; the
            // next `compute_attempt` call returns `attempt + 1`.
            let marker = orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: instance.id,
                block_id: step_def.id.clone(),
                output: serde_json::json!({
                    "_retry_marker": true,
                    "error": message,
                }),
                output_ref: Some("__retry__".into()),
                output_size: 0,
                attempt: i16::try_from(attempt).unwrap_or(i16::MAX),
                created_at: chrono::Utc::now(),
            };
            // Marker-save failures are non-fatal — worst case the attempt
            // counter stalls and the handler retries more times than
            // `max_attempts` allows. Prefer logging over surfacing an error
            // that would abort the whole tick and leave the instance in
            // Running with a half-applied retry decision.
            if let Err(e) = storage.save_block_output(&marker).await {
                tracing::warn!(
                    instance_id = %instance.id,
                    block_id = %step_def.id,
                    error = %e,
                    "tree path: failed to save retry marker; attempt counter may not advance"
                );
            }

            if let Some(retry) = &step_def.retry {
                if attempt >= retry.max_attempts {
                    tracing::warn!(
                        instance_id = %instance.id,
                        block_id = %step_def.id,
                        attempt,
                        max_attempts = retry.max_attempts,
                        message = %message,
                        "tree path: max retry attempts exhausted, failing node"
                    );
                    evaluator::fail_node(storage.as_ref(), node.id).await?;
                    return Ok(false);
                }
                let backoff = crate::handlers::step::calculate_backoff(
                    attempt,
                    retry.initial_backoff,
                    retry.max_backoff,
                    retry.backoff_multiplier,
                );
                let fire_at = chrono::Utc::now()
                    + chrono::Duration::from_std(backoff)
                        .unwrap_or_else(|_| chrono::Duration::zero());
                tracing::warn!(
                    instance_id = %instance.id,
                    block_id = %step_def.id,
                    attempt,
                    max_attempts = retry.max_attempts,
                    backoff_ms = backoff.as_millis(),
                    message = %message,
                    "tree path: retryable failure, re-scheduling instance with backoff"
                );
                crate::lifecycle::transition_instance(
                    storage.as_ref(),
                    instance.id,
                    Some(&instance.tenant_id),
                    orch8_types::instance::InstanceState::Running,
                    orch8_types::instance::InstanceState::Scheduled,
                    Some(fire_at),
                )
                .await?;
                // The evaluator's top-of-loop instance-state guard will
                // see the new `Scheduled` state and exit the tick. The
                // scheduler then re-dispatches once `fire_at` elapses.
                return Ok(false);
            }

            // No retry policy — treat a retryable failure as a permanent
            // node failure. Mirrors the fast-path fall-through in
            // `handle_retryable_failure` (line ~893) but at node granularity
            // since tree leaves fail individually without taking down the
            // whole instance unless the parent escalates.
            tracing::warn!(
                instance_id = %instance.id,
                block_id = %step_def.id,
                message = %message,
                "tree path: retryable failure with no retry policy, failing node"
            );
            evaluator::fail_node(storage.as_ref(), node.id).await?;
            Ok(false)
        }
        Err(EngineError::StepFailed {
            retryable: false, ..
        }) => {
            if breaker_tracked {
                if let Some(cb) = handlers.circuit_breakers() {
                    cb.record_failure(&instance.tenant_id, &step_def.handler);
                }
            }
            // Permanent failure - mark node as failed
            evaluator::fail_node(storage.as_ref(), node.id).await?;
            Ok(false)
        }
        Err(e) => {
            if breaker_tracked {
                if let Some(cb) = handlers.circuit_breakers() {
                    cb.record_failure(&instance.tenant_id, &step_def.handler);
                }
            }
            evaluator::fail_node(storage.as_ref(), node.id).await?;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};

    /// Seed parent `task_instances` row to satisfy the `externalized_state` FK.
    async fn seed_instance(storage: &SqliteStorage, id: InstanceId) {
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
            context: ExecutionContext::default(),
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

    #[tokio::test]
    async fn resolve_markers_inflates_externalized_fields() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;
        let payload = serde_json::json!({"inflated": true, "n": 42});
        storage
            .save_externalized_state(instance_id, "inst:ctx:data:big", &payload)
            .await
            .unwrap();

        let ctx = ExecutionContext {
            data: serde_json::json!({
                "small": "left-alone",
                "big": {"_externalized": true, "_ref": "inst:ctx:data:big"}
            }),
            ..ExecutionContext::default()
        };

        let resolved = resolve_markers(&storage, instance_id, ctx).await.unwrap();
        assert_eq!(resolved.data["small"], "left-alone");
        assert_eq!(resolved.data["big"], payload);
    }

    #[tokio::test]
    async fn resolve_markers_leaves_broken_ref_in_place() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let marker = serde_json::json!({"_externalized": true, "_ref": "missing:key"});
        let ctx = ExecutionContext {
            data: serde_json::json!({"broken": marker.clone()}),
            ..ExecutionContext::default()
        };
        let resolved = resolve_markers(&storage, InstanceId::new(), ctx)
            .await
            .unwrap();
        assert_eq!(resolved.data["broken"], marker);
    }

    #[tokio::test]
    async fn resolve_markers_is_noop_for_non_object_data() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let ctx = ExecutionContext {
            data: serde_json::json!("scalar"),
            ..ExecutionContext::default()
        };
        let resolved = resolve_markers(&storage, InstanceId::new(), ctx)
            .await
            .unwrap();
        assert_eq!(resolved.data, serde_json::json!("scalar"));
    }

    // ------------------------------------------------------------------
    // R6: template::resolve wiring into execute_step_node
    // ------------------------------------------------------------------

    use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
    use orch8_types::ids::{BlockId, ExecutionNodeId};
    use orch8_types::output::BlockOutput;
    use orch8_types::sequence::StepDef;
    use std::sync::{Arc, Mutex};

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
            fallback_handler: None,
            cache_key: None,
        }
    }

    async fn mk_step_node(
        storage: &dyn StorageBackend,
        instance_id: InstanceId,
        block_id: &str,
    ) -> ExecutionNode {
        let node = ExecutionNode {
            id: ExecutionNodeId(uuid::Uuid::now_v7()),
            instance_id,
            block_id: BlockId(block_id.into()),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Running,
            started_at: Some(Utc::now()),
            completed_at: None,
        };
        storage.create_execution_node(&node).await.unwrap();
        node
    }

    async fn save_prior_output(
        storage: &dyn StorageBackend,
        instance_id: InstanceId,
        block_id: &str,
        output: serde_json::Value,
    ) {
        let bo = BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id,
            block_id: BlockId(block_id.into()),
            output,
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: Utc::now(),
        };
        storage.save_block_output(&bo).await.unwrap();
    }

    /// Fetch a freshly-populated `TaskInstance` from storage.
    async fn load_instance(storage: &dyn StorageBackend, id: InstanceId) -> TaskInstance {
        storage.get_instance(id).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn execute_step_node_resolves_context_and_output_templates() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();

        let instance_id = InstanceId::new();
        let ctx = ExecutionContext {
            data: serde_json::json!({"order_id": 42}),
            ..ExecutionContext::default()
        };
        seed_instance_with_context(sqlite, instance_id, ctx).await;
        save_prior_output(
            sqlite,
            instance_id,
            "prev_step",
            serde_json::json!({"status": "done"}),
        )
        .await;

        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "templated_step").await;
        let step_def = mk_step_def(
            "templated_step",
            "capture_params",
            serde_json::json!({
                "instance_id": "{{context.data.order_id}}",
                "prev_status": "{{outputs.prev_step.status}}"
            }),
        );

        let captured: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
        let captured_clone = Arc::clone(&captured);

        let mut registry = super::HandlerRegistry::new();
        registry.register("capture_params", move |ctx| {
            let captured = Arc::clone(&captured_clone);
            async move {
                *captured.lock().unwrap() = Some(ctx.params.clone());
                Ok(serde_json::json!({}))
            }
        });

        let more = execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        assert!(more);

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("handler not called");
        assert_eq!(seen["instance_id"], 42);
        assert_eq!(seen["prev_status"], "done");
    }

    #[tokio::test]
    async fn execute_step_node_template_failure_fails_node() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();

        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "bad_template").await;

        // `{{nope.x}}` has an unknown template root — resolve_path returns
        // TemplateError, which should bubble up and fail the node.
        let step_def = mk_step_def(
            "bad_template",
            "capture_params",
            serde_json::json!({"x": "{{nope.x}}"}),
        );

        let mut registry = super::HandlerRegistry::new();
        registry.register("capture_params", |_ctx| async { Ok(serde_json::json!({})) });

        let more = execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        assert!(!more, "node should have failed");

        // Node state must have been transitioned to Failed by evaluator::fail_node.
        let refreshed = storage
            .get_execution_tree(instance_id)
            .await
            .unwrap()
            .into_iter()
            .find(|n| n.id == node.id)
            .unwrap();
        assert_eq!(refreshed.state, NodeState::Failed);
    }

    #[tokio::test]
    async fn execute_step_node_resolves_templates_for_emit_event_params() {
        // Regression test for finding #1: emit_event handler (and every other
        // in-process handler) must receive fully-templated params.
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();

        let instance_id = InstanceId::new();
        let ctx = ExecutionContext {
            data: serde_json::json!({"slug": "user.signed_up"}),
            ..ExecutionContext::default()
        };
        seed_instance_with_context(sqlite, instance_id, ctx).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "fire").await;
        let step_def = mk_step_def(
            "fire",
            "pretend_emit_event",
            serde_json::json!({"trigger_slug": "{{context.data.slug}}"}),
        );

        let captured: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
        let captured_clone = Arc::clone(&captured);

        let mut registry = super::HandlerRegistry::new();
        registry.register("pretend_emit_event", move |ctx| {
            let captured = Arc::clone(&captured_clone);
            async move {
                *captured.lock().unwrap() = Some(ctx.params.clone());
                Ok(serde_json::json!({"emitted": 1}))
            }
        });

        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("handler not called");
        assert_eq!(seen["trigger_slug"], "user.signed_up");
    }

    // ------------------------------------------------------------------
    // step_block edge cases (SB1-SB8)
    // ------------------------------------------------------------------

    // SB1: unregistered handler dispatches to external worker queue; node → Waiting.
    #[tokio::test]
    async fn execute_step_node_unregistered_handler_dispatches_external() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "ext_step").await;
        let step_def = mk_step_def(
            "ext_step",
            "unregistered_remote_handler",
            serde_json::json!({}),
        );

        let registry = super::HandlerRegistry::new();
        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();

        let refreshed = storage
            .get_execution_tree(instance_id)
            .await
            .unwrap()
            .into_iter()
            .find(|n| n.id == node.id)
            .unwrap();
        assert_eq!(
            refreshed.state,
            NodeState::Waiting,
            "external-dispatched step waits"
        );
    }

    // SB2: registered handler returns Ok → node Completed, block output saved.
    #[tokio::test]
    async fn execute_step_node_registered_handler_completes_and_saves_output() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "done_step").await;
        let step_def = mk_step_def("done_step", "h", serde_json::json!({}));

        let mut registry = super::HandlerRegistry::new();
        registry.register("h", |_ctx| async { Ok(serde_json::json!({"ok": true})) });

        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();

        let refreshed = storage
            .get_execution_tree(instance_id)
            .await
            .unwrap()
            .into_iter()
            .find(|n| n.id == node.id)
            .unwrap();
        assert_eq!(refreshed.state, NodeState::Completed);
        let outs = storage.get_all_outputs(instance_id).await.unwrap();
        assert!(!outs.is_empty(), "block output persisted");
        assert!(outs.iter().any(|o| o.block_id == step_def.id));
    }

    // SB3: self-modify output injects blocks into the instance.
    #[tokio::test]
    async fn execute_step_node_self_modify_injects_blocks() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "sm_step").await;
        let step_def = mk_step_def("sm_step", "sm", serde_json::json!({}));

        let injected =
            serde_json::json!([{"type": "step", "id": "new", "handler": "h", "params": {}}]);
        let inj_clone = injected.clone();
        let mut registry = super::HandlerRegistry::new();
        registry.register("sm", move |_ctx| {
            let inj = inj_clone.clone();
            async move { Ok(serde_json::json!({"_self_modify": true, "blocks": inj})) }
        });

        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();

        let stored = storage.get_injected_blocks(instance_id).await.unwrap();
        assert_eq!(stored, Some(injected), "self-modify blocks persisted");
    }

    // SB4: self-modify without blocks key is a no-op (no injection).
    #[tokio::test]
    async fn execute_step_node_self_modify_without_blocks_is_noop() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "sm2").await;
        let step_def = mk_step_def("sm2", "sm2h", serde_json::json!({}));

        let mut registry = super::HandlerRegistry::new();
        registry.register("sm2h", |_ctx| async {
            Ok(serde_json::json!({"_self_modify": true}))
        });

        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let stored = storage.get_injected_blocks(instance_id).await.unwrap();
        assert!(stored.is_none() || stored == Some(serde_json::Value::Null));
    }

    // SB5: context_access filter hides non-allowed fields from handler.
    #[tokio::test]
    async fn execute_step_node_context_access_filters_fields() {
        use orch8_types::sequence::{ContextAccess, FieldAccess};
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        let ctx = ExecutionContext {
            data: serde_json::json!({"allowed": "yes", "secret": "nope"}),
            ..ExecutionContext::default()
        };
        seed_instance_with_context(sqlite, instance_id, ctx).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "filtered").await;
        let mut step_def = mk_step_def("filtered", "cap", serde_json::json!({}));
        step_def.context_access = Some(ContextAccess {
            data: FieldAccess::Fields {
                fields: vec!["allowed".into()],
            },
            config: true,
            audit: false,
            runtime: false,
        });

        let captured_ctx: Arc<Mutex<Option<ExecutionContext>>> = Arc::new(Mutex::new(None));
        let captured_clone = Arc::clone(&captured_ctx);
        let mut registry = super::HandlerRegistry::new();
        registry.register("cap", move |ctx| {
            let cap = Arc::clone(&captured_clone);
            async move {
                *cap.lock().unwrap() = Some(ctx.context.clone());
                Ok(serde_json::json!({}))
            }
        });

        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let seen = captured_ctx
            .lock()
            .unwrap()
            .clone()
            .expect("handler not called");
        assert_eq!(seen.data.get("allowed"), Some(&serde_json::json!("yes")));
        assert!(
            seen.data.get("secret").is_none() || seen.data["secret"].is_null(),
            "secret should be filtered out, got: {:?}",
            seen.data
        );
    }

    // SB6: unregistered handler external dispatch preserves queue_name on worker_task.
    #[tokio::test]
    async fn execute_step_node_external_dispatch_preserves_queue_name() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "q_step").await;
        let mut step_def = mk_step_def("q_step", "external_q", serde_json::json!({}));
        step_def.queue_name = Some("high-priority".into());

        let registry = super::HandlerRegistry::new();
        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();

        let filter = orch8_types::worker_filter::WorkerTaskFilter {
            tenant_id: None,
            states: None,
            handler_name: Some("external_q".into()),
            worker_id: None,
            queue_name: None,
        };
        let tasks = storage
            .list_worker_tasks(&filter, &orch8_types::filter::Pagination::default())
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].queue_name.as_deref(), Some("high-priority"));
    }

    // SB7: unknown template root path fails the node with no handler invocation.
    #[tokio::test]
    async fn execute_step_node_template_error_does_not_invoke_handler() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "tpl_err").await;
        let step_def = mk_step_def(
            "tpl_err",
            "should_not_run",
            serde_json::json!({"v": "{{unknown.path}}"}),
        );

        let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let call_count_clone = Arc::clone(&call_count);
        let mut registry = super::HandlerRegistry::new();
        registry.register("should_not_run", move |_ctx| {
            let c = Arc::clone(&call_count_clone);
            async move {
                *c.lock().unwrap() += 1;
                Ok(serde_json::json!({}))
            }
        });

        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            *call_count.lock().unwrap(),
            0,
            "handler must not run on template error"
        );
        let refreshed = storage
            .get_execution_tree(instance_id)
            .await
            .unwrap()
            .into_iter()
            .find(|n| n.id == node.id)
            .unwrap();
        assert_eq!(refreshed.state, NodeState::Failed);
    }

    // SB8: self-modify with position inserts into existing injected list at position.
    #[tokio::test]
    async fn execute_step_node_self_modify_with_position_inserts() {
        let storage: Arc<dyn orch8_storage::StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let sqlite: &dyn StorageBackend = storage.as_ref();
        let instance_id = InstanceId::new();
        seed_instance_with_context(sqlite, instance_id, ExecutionContext::default()).await;
        // Pre-seed an existing injected list.
        storage
            .inject_blocks(
                instance_id,
                &serde_json::json!([{"existing": "a"}, {"existing": "c"}]),
            )
            .await
            .unwrap();
        let instance = load_instance(sqlite, instance_id).await;
        let node = mk_step_node(sqlite, instance_id, "sm_pos").await;
        let step_def = mk_step_def("sm_pos", "smp", serde_json::json!({}));

        let mut registry = super::HandlerRegistry::new();
        registry.register("smp", |_ctx| async {
            Ok(serde_json::json!({
                "_self_modify": true,
                "blocks": [{"existing": "b"}],
                "position": 1,
            }))
        });

        execute_step_node(
            &storage,
            &registry,
            &instance,
            &node,
            &step_def,
            &OutputsSnapshot::new(),
        )
        .await
        .unwrap();
        let stored = storage
            .get_injected_blocks(instance_id)
            .await
            .unwrap()
            .unwrap();
        let arr = stored.as_array().unwrap();
        assert_eq!(arr.len(), 3, "block inserted at position 1");
        assert_eq!(arr[0]["existing"], "a");
        assert_eq!(arr[1]["existing"], "b");
        assert_eq!(arr[2]["existing"], "c");
    }
}
