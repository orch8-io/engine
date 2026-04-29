use std::future::Future;

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::InstanceId;
use orch8_types::instance::TaskInstance;
use orch8_types::plugin::PluginType;
use orch8_types::sequence::StepDef;

use crate::error::EngineError;
use crate::evaluator;

/// Dispatch a plugin handler and map the `StepError` result to node state transitions.
///
/// On success the output is persisted as a `BlockOutput` for the node's
/// `(instance_id, block_id)` so downstream steps can reference
/// `{{outputs.<block_id>.*}}` — matching how the in-process registry path
/// saves step outputs. Previously the output was discarded, leaving plugin
/// steps invisible to templating and causing `getOutputs` to miss them.
pub(crate) async fn dispatch_plugin<F, Fut>(
    storage: &dyn StorageBackend,
    node: &ExecutionNode,
    attempt: u32,
    handler_fn: F,
) -> Result<bool, EngineError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<serde_json::Value, orch8_types::error::StepError>>,
{
    #[allow(clippy::single_match_else)]
    match handler_fn().await {
        Ok(output) => {
            let output_size = serde_json::to_vec(&output)
                .map_or(0, |v| i32::try_from(v.len()).unwrap_or(i32::MAX));
            let bo = orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: node.instance_id,
                block_id: node.block_id.clone(),
                output,
                output_ref: None,
                output_size,
                attempt: i16::try_from(attempt).unwrap_or(i16::MAX),
                created_at: chrono::Utc::now(),
            };
            // Output persistence is part of the success contract. Previously
            // this was logged and swallowed, completing the node while its
            // output was missing — downstream `{{outputs.<block_id>...}}`
            // references then evaluated against a blank value while the
            // workflow advanced as if the step succeeded. Fail the node
            // instead so templating sees a consistent world.
            if let Err(e) = storage.save_block_output(&bo).await {
                tracing::error!(
                    instance_id = %node.instance_id,
                    block_id = %node.block_id,
                    error = %e,
                    "plugin handler: save_block_output failed — failing node to avoid stale-output downstream"
                );
                evaluator::fail_node(storage, node.id).await?;
                return Ok(false);
            }
            evaluator::complete_node(storage, node.id).await?;
            Ok(true)
        }
        Err(step_err) => {
            // Previously the StepError was matched as `Err(_)` — the error
            // message, details, and retryable flag were all discarded before
            // `fail_node`. That left operators staring at a silently-failed
            // node with zero signal about why: an empty `log` stream, no
            // `error_message` on the `BlockOutput`, no trace of the HTTP/gRPC
            // transport error, timeout, or upstream 4xx/5xx.
            //
            // At minimum, surface the structured error at tracing::error so
            // log-based alerting can pick it up. Persist an error-tagged
            // `BlockOutput` so downstream templating / the UI can distinguish
            // "step never ran" from "step ran and blew up".
            let (msg, is_retryable, details) = match &step_err {
                orch8_types::error::StepError::Permanent { message, details } => {
                    (message.clone(), false, details.clone())
                }
                orch8_types::error::StepError::Retryable { message, details } => {
                    (message.clone(), true, details.clone())
                }
            };
            tracing::error!(
                instance_id = %node.instance_id,
                block_id = %node.block_id,
                retryable = is_retryable,
                error = %msg,
                details = ?details,
                "plugin handler returned an error"
            );
            let err_output = serde_json::json!({
                "__error__": true,
                "retryable": is_retryable,
                "message": msg,
                "details": details,
            });
            let output_size = serde_json::to_vec(&err_output)
                .map_or(0, |v| i32::try_from(v.len()).unwrap_or(i32::MAX));
            let bo = orch8_types::output::BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: node.instance_id,
                block_id: node.block_id.clone(),
                output: err_output,
                output_ref: Some("__error__".into()),
                output_size,
                attempt: i16::try_from(attempt).unwrap_or(i16::MAX),
                created_at: chrono::Utc::now(),
            };
            if let Err(persist_err) = storage.save_block_output(&bo).await {
                // Non-fatal: still fail the node. A missing error-marker is
                // strictly worse UX than a missing success-output, but not
                // load-bearing for correctness.
                tracing::warn!(
                    instance_id = %node.instance_id,
                    block_id = %node.block_id,
                    error = %persist_err,
                    "plugin handler: failed to persist error marker; proceeding with fail_node"
                );
            }
            // NOTE: plugin dispatch currently fails the node permanently on
            // any StepError. A future change should honour `retryable` and
            // re-schedule via the same backoff path as the in-process
            // handler — that requires threading `instance` + `step_def.retry`
            // into this helper. Tracked as follow-up to Issue 3's fix.
            evaluator::fail_node(storage, node.id).await?;
            Ok(false)
        }
    }
}

/// Dispatch a step within the execution tree to the external worker queue.
/// The node is marked Waiting; the instance state is NOT changed here.
///
/// `resolved_params` must already have been through template + credential
/// resolution — external workers receive materialised values, not raw
/// `{{…}}` or `credentials://…` strings.
pub(crate) async fn dispatch_step_to_external_worker(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node: &ExecutionNode,
    step_def: &StepDef,
    resolved_params: serde_json::Value,
    step_context: ExecutionContext,
    attempt: u32,
) -> Result<bool, EngineError> {
    use orch8_types::worker::{WorkerTask, WorkerTaskState};

    let task = WorkerTask {
        id: uuid::Uuid::now_v7(),
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
    instance_id: InstanceId,
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
pub(crate) async fn resolve_plugin_source(
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
    instance_id: InstanceId,
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
    use orch8_types::execution::{BlockType, ExecutionNode};
    use orch8_types::ids::*;
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::plugin::PluginType;
    use serde_json::json;

    async fn mk_storage() -> SqliteStorage {
        SqliteStorage::in_memory().await.unwrap()
    }

    fn mk_instance(id: InstanceId) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id,
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn mk_node(instance_id: InstanceId, block_id: &str, state: NodeState) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            parent_id: None,
            block_id: BlockId(block_id.into()),
            block_type: BlockType::Step,
            state,
            branch_index: None,
            started_at: None,
            completed_at: None,
        }
    }

    // ------------------------------------------------------------------
    // dispatch_plugin
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn dispatch_plugin_success_saves_output_and_completes_node() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let node = mk_node(inst.id, "step1", NodeState::Running);
        s.create_execution_node(&node).await.unwrap();

        let result = dispatch_plugin(&s, &node, 0, || async {
            Ok(json!({"result": "success"}))
        })
        .await
        .unwrap();

        assert!(result, "dispatch_plugin should return true on success");

        let tree = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(tree[0].state, NodeState::Completed);

        let output = s
            .get_block_output(inst.id, &BlockId("step1".into()))
            .await
            .unwrap();
        assert!(output.is_some());
        assert_eq!(output.unwrap().output["result"], "success");
    }

    #[tokio::test]
    async fn dispatch_plugin_permanent_error_fails_node_and_saves_error_output() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let node = mk_node(inst.id, "step_fail", NodeState::Running);
        s.create_execution_node(&node).await.unwrap();

        let result = dispatch_plugin(&s, &node, 0, || async {
            Err(orch8_types::error::StepError::Permanent {
                message: "boom".into(),
                details: None,
            })
        })
        .await
        .unwrap();

        assert!(!result, "dispatch_plugin should return false on error");

        let tree = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(tree[0].state, NodeState::Failed);

        let output = s
            .get_block_output(inst.id, &BlockId("step_fail".into()))
            .await
            .unwrap();
        assert!(output.is_some());
        let out = output.unwrap();
        assert_eq!(out.output["__error__"], true);
        assert_eq!(out.output["message"], "boom");
        assert_eq!(out.output["retryable"], false);
    }

    #[tokio::test]
    async fn dispatch_plugin_retryable_error_marks_retryable_in_output() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let node = mk_node(inst.id, "step_retry", NodeState::Running);
        s.create_execution_node(&node).await.unwrap();

        let result = dispatch_plugin(&s, &node, 1, || async {
            Err(orch8_types::error::StepError::Retryable {
                message: "timeout".into(),
                details: Some(json!({"code": 504})),
            })
        })
        .await
        .unwrap();

        assert!(!result);
        let output = s
            .get_block_output(inst.id, &BlockId("step_retry".into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.output["retryable"], true);
        assert_eq!(output.output["details"]["code"], 504);
        assert_eq!(output.attempt, 1);
    }

    // ------------------------------------------------------------------
    // complete_external_step_node / fail_external_step_node
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn complete_external_step_node_marks_waiting_node_completed() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let node = mk_node(inst.id, "ext_step", NodeState::Waiting);
        s.create_execution_node(&node).await.unwrap();

        complete_external_step_node(&s, inst.id, &BlockId("ext_step".into()))
            .await
            .unwrap();

        let tree = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(tree[0].state, NodeState::Completed);
    }

    #[tokio::test]
    async fn fail_external_step_node_marks_waiting_node_failed() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let node = mk_node(inst.id, "ext_step", NodeState::Waiting);
        s.create_execution_node(&node).await.unwrap();

        fail_external_step_node(&s, inst.id, &BlockId("ext_step".into()))
            .await
            .unwrap();

        let tree = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(tree[0].state, NodeState::Failed);
    }

    #[tokio::test]
    async fn complete_external_step_node_noop_when_no_matching_node() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        // Node exists but already completed — should be skipped
        let node = mk_node(inst.id, "done_step", NodeState::Completed);
        s.create_execution_node(&node).await.unwrap();

        complete_external_step_node(&s, inst.id, &BlockId("done_step".into()))
            .await
            .unwrap();

        let tree = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(tree[0].state, NodeState::Completed);
    }

    #[tokio::test]
    async fn fail_external_step_node_noop_for_nonexistent_block_id() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        // No nodes at all for this instance
        fail_external_step_node(&s, inst.id, &BlockId("ghost".into()))
            .await
            .unwrap();
    }

    // ------------------------------------------------------------------
    // resolve_plugin_source
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_plugin_source_returns_source_for_enabled_plugin() {
        use orch8_types::plugin::PluginDef;
        let s = mk_storage().await;
        let plugin = PluginDef {
            name: "my-plugin".into(),
            tenant_id: String::new(),
            plugin_type: PluginType::Wasm,
            source: "/path/to/plugin.wasm".into(),
            config: json!({}),
            description: None,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        s.create_plugin(&plugin).await.unwrap();

        let source = resolve_plugin_source(&s, "my-plugin", PluginType::Wasm).await;
        assert_eq!(source, Some("/path/to/plugin.wasm".into()));
    }

    #[tokio::test]
    async fn resolve_plugin_source_returns_none_for_disabled_plugin() {
        use orch8_types::plugin::PluginDef;
        let s = mk_storage().await;
        let plugin = PluginDef {
            name: "disabled-plugin".into(),
            tenant_id: String::new(),
            plugin_type: PluginType::Wasm,
            source: "/path/to/plugin.wasm".into(),
            config: json!({}),
            description: None,
            enabled: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        s.create_plugin(&plugin).await.unwrap();

        let source = resolve_plugin_source(&s, "disabled-plugin", PluginType::Wasm).await;
        assert_eq!(source, None);
    }

    #[tokio::test]
    async fn resolve_plugin_source_returns_none_for_wrong_type() {
        use orch8_types::plugin::PluginDef;
        let s = mk_storage().await;
        let plugin = PluginDef {
            name: "grpc-plugin".into(),
            tenant_id: String::new(),
            plugin_type: PluginType::Grpc,
            source: "localhost:9090".into(),
            config: json!({}),
            description: None,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        s.create_plugin(&plugin).await.unwrap();

        let source = resolve_plugin_source(&s, "grpc-plugin", PluginType::Wasm).await;
        assert_eq!(source, None);
    }

    #[tokio::test]
    async fn resolve_plugin_source_returns_none_for_unknown_plugin() {
        let s = mk_storage().await;
        let source = resolve_plugin_source(&s, "nonexistent", PluginType::Wasm).await;
        assert_eq!(source, None);
    }

    // ------------------------------------------------------------------
    // dispatch_step_to_external_worker
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn dispatch_to_external_worker_creates_task_and_marks_node_waiting() {
        let s = mk_storage().await;
        let inst = mk_instance(InstanceId::new());
        s.create_instance(&inst).await.unwrap();

        let node = mk_node(inst.id, "ext", NodeState::Running);
        s.create_execution_node(&node).await.unwrap();

        let step_def = StepDef {
            id: BlockId("ext".into()),
            handler: "http".into(),
            params: json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: Some("my-queue".into()),
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
        };

        let result = dispatch_step_to_external_worker(
            &s,
            &inst,
            &node,
            &step_def,
            json!({"url": "https://example.com"}),
            ExecutionContext::default(),
            0,
        )
        .await
        .unwrap();

        assert!(!result, "should return false (no more work this tick)");

        // Node should now be Waiting
        let tree = s.get_execution_tree(inst.id).await.unwrap();
        assert_eq!(tree[0].state, NodeState::Waiting);

        // Worker task should exist
        let tasks = s
            .claim_worker_tasks("http", "test-worker", 10)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].handler_name, "http");
        assert_eq!(tasks[0].params["url"], "https://example.com");
    }
}
