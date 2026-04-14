//! SLA deadline check: fail step nodes whose per-step deadline has been breached,
//! invoking an escalation handler if configured.

use std::sync::Arc;

use tracing::warn;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::BlockDefinition;

use super::{fail_node, find_block};
use crate::error::EngineError;
use crate::handlers::HandlerRegistry;

/// Check all Running/Waiting step nodes for SLA deadline breaches.
/// On breach: invoke escalation handler (if configured), then fail the node.
/// Returns `true` if any deadline was breached (tree state was modified).
pub(super) async fn check_sla_deadlines(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    blocks: &[BlockDefinition],
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let now = chrono::Utc::now();
    let mut breached = false;
    for node in tree {
        if !matches!(node.state, NodeState::Running | NodeState::Waiting) {
            continue;
        }
        let Some(started_at) = node.started_at else {
            continue;
        };
        let Some(BlockDefinition::Step(step_def)) = find_block(blocks, &node.block_id) else {
            continue;
        };
        let Some(deadline) = step_def.deadline else {
            continue;
        };
        let elapsed = now - started_at;
        if elapsed < chrono::Duration::from_std(deadline).unwrap_or(chrono::TimeDelta::MAX) {
            continue;
        }
        // Deadline breached!
        breached = true;
        warn!(
            instance_id = %instance.id,
            block_id = %node.block_id,
            deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
            elapsed_ms = elapsed.num_milliseconds(),
            "SLA deadline breached"
        );

        // Invoke escalation handler if configured.
        if let Some(ref escalation) = step_def.on_deadline_breach {
            if let Some(handler) = handlers.get(&escalation.handler) {
                let mut params = escalation.params.clone();
                // Inject breach metadata into escalation params.
                if let serde_json::Value::Object(ref mut map) = params {
                    map.insert(
                        "_breach_block_id".into(),
                        serde_json::json!(node.block_id.0),
                    );
                    map.insert(
                        "_breach_instance_id".into(),
                        serde_json::json!(instance.id.0),
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
                    instance_id: instance.id,
                    tenant_id: instance.tenant_id.clone(),
                    block_id: node.block_id.clone(),
                    params,
                    context: instance.context.clone(),
                    attempt: 0,
                    storage: Arc::clone(storage),
                    wait_for_input: None,
                };
                // Fire-and-forget: escalation handler failure doesn't block the deadline fail.
                if let Err(e) = handler(step_ctx).await {
                    warn!(
                        instance_id = %instance.id,
                        block_id = %node.block_id,
                        error = %e,
                        "SLA escalation handler failed"
                    );
                }
            } else {
                warn!(
                    instance_id = %instance.id,
                    handler = %escalation.handler,
                    "SLA escalation handler not found"
                );
            }
        }

        // Fail the node.
        fail_node(storage.as_ref(), node.id).await?;

        // Record as block output for diagnostics.
        let output = orch8_types::output::BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: node.block_id.clone(),
            output: serde_json::json!({
                "_error": "sla_deadline_breached",
                "_deadline_ms": u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
                "_elapsed_ms": elapsed.num_milliseconds(),
            }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: now,
        };
        storage.save_block_output(&output).await?;
    }
    Ok(breached)
}
