use std::future::Future;
use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::InstanceId;
use orch8_types::instance::TaskInstance;
use orch8_types::plugin::PluginType;
use orch8_types::sequence::StepDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::externalized;
use crate::handlers::step::StepExecParams;
use crate::handlers::HandlerRegistry;

/// Walk `context.data` top-level fields and inflate any externalization
/// markers found. Does not recurse into nested objects — only top-level
/// keys can be externalized. See `docs/CONTEXT_MANAGEMENT.md` §8.5.
///
/// If the referenced payload is missing, the marker is left in place so
/// downstream code can detect the broken reference.
async fn resolve_markers(
    storage: &dyn StorageBackend,
    _instance_id: InstanceId,
    mut ctx: ExecutionContext,
) -> Result<ExecutionContext, EngineError> {
    let Some(obj) = ctx.data.as_object_mut() else {
        return Ok(ctx);
    };
    for (_key, value) in obj.iter_mut() {
        if let Some(ref_key) = externalized::extract_ref_key(value) {
            if let Some(resolved) = storage
                .get_externalized_state(ref_key)
                .await
                .map_err(EngineError::Storage)?
            {
                *value = resolved;
            }
        }
    }
    Ok(ctx)
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

/// Dispatch a plugin handler and map the `StepError` result to node state transitions.
async fn dispatch_plugin<F, Fut>(
    storage: &dyn StorageBackend,
    node: &ExecutionNode,
    handler_fn: F,
) -> Result<bool, EngineError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<serde_json::Value, orch8_types::error::StepError>>,
{
    match handler_fn().await {
        Ok(_output) => {
            evaluator::complete_node(storage, node.id).await?;
            Ok(true)
        }
        Err(orch8_types::error::StepError::Retryable { .. }) => Ok(true),
        Err(_) => {
            evaluator::fail_node(storage, node.id).await?;
            Ok(false)
        }
    }
}

/// Build the `outputs` JSON shape expected by `template::resolve`:
/// `{ "block_id_1": <output>, "block_id_2": <output>, ... }`.
/// Fetches all prior block outputs for the instance from storage.
async fn build_outputs_shape(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
) -> Result<serde_json::Value, EngineError> {
    let outputs = storage
        .get_all_outputs(instance_id)
        .await
        .map_err(EngineError::Storage)?;
    let mut map = serde_json::Map::with_capacity(outputs.len());
    for o in outputs {
        map.insert(o.block_id.0.clone(), o.output);
    }
    Ok(serde_json::Value::Object(map))
}

/// Resolve `{{path}}` template placeholders in `params`, using the step's
/// context snapshot and all prior block outputs in the instance. Mutates
/// `params` in place.
///
/// Runs BEFORE credential resolution so user templates can dynamically
/// produce `credentials://…` refs (or any other value) that the credential
/// pass then materialises. Template errors (unknown root/section) bubble up
/// to the caller which fails the node.
async fn resolve_templates_in_params(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    context: &ExecutionContext,
    params: &mut serde_json::Value,
) -> Result<(), EngineError> {
    let outputs = build_outputs_shape(storage, instance.id).await?;
    let resolved = crate::template::resolve(params, context, &outputs)?;
    *params = resolved;
    Ok(())
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
) -> Result<bool, EngineError> {
    // Build the step's context snapshot once and reuse across both template
    // resolution and every downstream dispatch branch. `context_for_step` only
    // reads `instance.context` and `step_def.context_access` — it does not
    // depend on step params, so evaluating it before template resolution is
    // safe and avoids duplicate work.
    let step_context = context_for_step(storage.as_ref(), instance, step_def).await?;

    let mut resolved_params = step_def.params.clone();

    // Resolve `{{path}}` templates first. User templates should be fully
    // materialised before any further transformation — this lets an author
    // write `{{context.data.cred_id}}` that evaluates to a `credentials://…`
    // ref, which the following credential pass then substitutes. Template
    // errors (unknown context section, unknown root) fail the node the same
    // way credential errors do.
    if let Err(tpl_err) = resolve_templates_in_params(
        storage.as_ref(),
        instance,
        &step_context,
        &mut resolved_params,
    )
    .await
    {
        tracing::warn!(
            instance_id = %instance.id,
            block_id = %step_def.id,
            error = ?tpl_err,
            "failed to resolve templates for step"
        );
        evaluator::fail_node(storage.as_ref(), node.id).await?;
        return Ok(false);
    }

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

    // If the handler is an ActivePieces sidecar call, dispatch via HTTP to the
    // Node worker. No plugin-registry lookup needed — the endpoint is a single
    // env-configured sidecar, and piece/action names live in the handler string.
    if super::activepieces::is_ap_handler(&step_def.handler) {
        let handler_name = step_def.handler.clone();
        let ctx = super::StepContext {
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step_def.id.clone(),
            params: resolved_params.clone(),
            context: step_context.clone(),
            attempt: 0,
            storage: Arc::clone(storage),
        };
        return dispatch_plugin(storage.as_ref(), node, move || async move {
            super::activepieces::handle_ap(ctx, &handler_name).await
        })
        .await;
    }

    // If the handler is a gRPC plugin, resolve via the plugin registry then dispatch.
    if super::grpc_plugin::is_grpc_handler(&step_def.handler) {
        let endpoint = resolve_plugin_source(storage.as_ref(), &step_def.handler, PluginType::Grpc)
            .await
            .unwrap_or_else(|| step_def.handler.clone());
        let mut params = resolved_params.clone();
        params["_grpc_endpoint"] = serde_json::Value::String(endpoint);
        let ctx = super::StepContext {
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: step_def.id.clone(),
            params,
            context: step_context.clone(),
            attempt: 0,
            storage: Arc::clone(storage),
        };
        return dispatch_plugin(storage.as_ref(), node, || {
            super::grpc_plugin::handle_grpc_plugin(ctx)
        })
        .await;
    }

    // If the handler is a WASM plugin, resolve via the plugin registry then dispatch.
    if super::wasm_plugin::is_wasm_handler(&step_def.handler) {
        if let Some(plugin_name) = super::wasm_plugin::parse_plugin_name(&step_def.handler) {
            let wasm_path = resolve_plugin_source(storage.as_ref(), plugin_name, PluginType::Wasm)
                .await
                .unwrap_or_else(|| plugin_name.to_string());
            let ctx = super::StepContext {
                instance_id: instance.id,
                tenant_id: instance.tenant_id.clone(),
                block_id: step_def.id.clone(),
                params: resolved_params.clone(),
                context: step_context.clone(),
                attempt: 0,
                storage: Arc::clone(storage),
            };
            return dispatch_plugin(storage.as_ref(), node, || {
                super::wasm_plugin::handle_wasm_plugin(ctx, &wasm_path)
            })
            .await;
        }
    }

    // If the handler is not registered in-process, dispatch to external worker queue.
    if !handlers.contains(&step_def.handler) {
        return dispatch_step_to_external_worker(
            storage.as_ref(),
            instance,
            node,
            step_def,
            resolved_params,
            step_context,
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
        attempt: 0,
        timeout: step_def.timeout,
        externalize_threshold: 0, // Tree evaluator does not externalize (no config available)
    };

    match crate::handlers::step::execute_step(storage, handlers, exec_params).await {
        Ok(output) => {
            // Check for self-modify output: inject blocks into the instance.
            if output
                .get("_self_modify")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                if let Some(blocks) = output.get("blocks").filter(|v| v.is_array()) {
                    let position = output.get("position").and_then(serde_json::Value::as_u64);
                    let final_blocks = if let Some(pos) = position {
                        let existing = storage
                            .get_injected_blocks(instance.id)
                            .await
                            .unwrap_or(None);
                        let mut arr = existing
                            .and_then(|v| v.as_array().cloned())
                            .unwrap_or_default();
                        let new = blocks.as_array().cloned().unwrap_or_default();
                        #[allow(clippy::cast_possible_truncation)]
                        let at = (pos.min(usize::MAX as u64) as usize).min(arr.len());
                        for (i, b) in new.into_iter().enumerate() {
                            arr.insert(at + i, b);
                        }
                        serde_json::Value::Array(arr)
                    } else {
                        blocks.clone()
                    };
                    if let Err(e) = storage.inject_blocks(instance.id, &final_blocks).await {
                        tracing::warn!(
                            instance_id = %instance.id,
                            error = %e,
                            "failed to inject self-modify blocks"
                        );
                    }
                }
            }
            evaluator::complete_node(storage.as_ref(), node.id).await?;
            Ok(true)
        }
        Err(EngineError::StepFailed {
            retryable: true, ..
        }) => {
            // Leave node as Running for retry on next tick.
            Ok(true)
        }
        Err(e) => {
            evaluator::fail_node(storage.as_ref(), node.id).await?;
            Err(e)
        }
    }
}

/// Dispatch a step within the execution tree to the external worker queue.
/// The node is marked Waiting; the instance state is NOT changed here.
///
/// `resolved_params` must already have been through template + credential
/// resolution — external workers receive materialised values, not raw
/// `{{…}}` or `credentials://…` strings.
async fn dispatch_step_to_external_worker(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node: &ExecutionNode,
    step_def: &StepDef,
    resolved_params: serde_json::Value,
    step_context: ExecutionContext,
) -> Result<bool, EngineError> {
    use orch8_types::worker::{WorkerTask, WorkerTaskState};

    let task = WorkerTask {
        id: uuid::Uuid::new_v4(),
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        handler_name: step_def.handler.clone(),
        queue_name: step_def.queue_name.clone(),
        params: resolved_params,
        // Apply the step's context_access policy before handing the context
        // off to an external worker. The remote process can't be trusted to
        // filter on its own.
        context: serde_json::to_value(step_context)
            .map_err(orch8_types::error::StorageError::Serialization)?,
        attempt: 0,
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

    // Mark the execution node as Waiting so the evaluator won't re-dispatch it.
    // The instance state is NOT changed here — the evaluator may have other steps
    // to execute within the same composite. The caller (evaluator / process_instance_tree)
    // is responsible for transitioning the instance to Waiting when no more work remains.
    storage
        .update_node_state(node.id, orch8_types::execution::NodeState::Waiting)
        .await?;

    tracing::info!(
        instance_id = %instance.id,
        block_id = %step_def.id,
        handler = %step_def.handler,
        "dispatched tree step to external worker queue"
    );

    Ok(false) // No more work in this tick — instance is now Waiting.
}

/// Called when an external worker completes a task that belongs to an execution tree.
/// Marks the corresponding execution node as completed.
pub async fn complete_external_step_node(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
    block_id: &orch8_types::ids::BlockId,
) -> Result<(), EngineError> {
    let tree = storage.get_execution_tree(instance_id).await?;
    if let Some(node) = tree.iter().find(|n| {
        n.block_id == *block_id && matches!(n.state, NodeState::Running | NodeState::Waiting)
    }) {
        evaluator::complete_node(storage, node.id).await?;
    }
    Ok(())
}

/// Look up a plugin name in the registry and return its source path/endpoint.
/// Returns `None` if the plugin isn't registered (caller falls back to the raw handler name).
async fn resolve_plugin_source(
    storage: &dyn StorageBackend,
    name: &str,
    expected_type: PluginType,
) -> Option<String> {
    let plugin = match storage.get_plugin(name).await {
        Ok(Some(p)) => p,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(plugin = %name, error = %e, "failed to resolve plugin source");
            return None;
        }
    };
    if plugin.enabled && plugin.plugin_type == expected_type {
        Some(plugin.source)
    } else {
        None
    }
}

/// Called when an external worker permanently fails a task in an execution tree.
/// Marks the corresponding execution node as failed.
pub async fn fail_external_step_node(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
    block_id: &orch8_types::ids::BlockId,
) -> Result<(), EngineError> {
    let tree = storage.get_execution_tree(instance_id).await?;
    if let Some(node) = tree.iter().find(|n| {
        n.block_id == *block_id && matches!(n.state, NodeState::Running | NodeState::Waiting)
    }) {
        evaluator::fail_node(storage, node.id).await?;
    }
    Ok(())
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
        }
    }

    async fn mk_step_node(
        storage: &dyn StorageBackend,
        instance_id: InstanceId,
        block_id: &str,
    ) -> ExecutionNode {
        let node = ExecutionNode {
            id: ExecutionNodeId(uuid::Uuid::new_v4()),
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
            id: uuid::Uuid::new_v4(),
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

        let more = execute_step_node(&storage, &registry, &instance, &node, &step_def)
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

        let more = execute_step_node(&storage, &registry, &instance, &node, &step_def)
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

        execute_step_node(&storage, &registry, &instance, &node, &step_def)
            .await
            .unwrap();

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("handler not called");
        assert_eq!(seen["trigger_slug"], "user.signed_up");
    }
}
