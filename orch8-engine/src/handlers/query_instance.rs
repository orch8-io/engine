//! `query_instance` builtin: read another instance's context + state.
//!
//! Same-tenant only. Returns `{ found: false }` for missing targets;
//! cross-tenant attempts return `Permanent` (does not leak existence).
//!
//! Response shape (per design doc `2026-04-17-emit-event-design.md` §Handlers):
//! `{ found, context?, state?, started_at?, completed_at?, current_node? }`.
//!
//! Timestamp derivation:
//! * `started_at` = `TaskInstance.created_at` (workflows begin immediately on
//!   instance creation — there is no separate "start" transition).
//! * `completed_at` = `Some(updated_at)` iff the instance is in a terminal
//!   state (`Completed`/`Failed`/`Cancelled`). `updated_at` alone is unreliable
//!   as "completion time" because metadata touches bump it on terminal
//!   instances; gating on `is_terminal()` makes the field meaningful.
//!
//! `current_node` derivation (matches design spec):
//!   "latest non-terminal node from the execution tree, or the most recently
//!    updated node if all terminal". We pick the latest non-terminal node by
//!   `started_at`; if every node is terminal we pick the latest by
//!   `completed_at`. The returned value is the node's `block_id` string, which
//!   is the identifier callers use to reason about workflow position.

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use orch8_types::error::StepError;
use orch8_types::execution::{ExecutionNode, NodeState};

use super::util::{check_same_tenant, map_storage_err, parse_instance_id};
use super::StepContext;

/// Node states that count as "done" for the purpose of `current_node`
/// derivation.
fn node_is_terminal(state: NodeState) -> bool {
    matches!(
        state,
        NodeState::Completed | NodeState::Failed | NodeState::Cancelled | NodeState::Skipped
    )
}

/// Derive `current_node` (`block_id` string) from the execution tree per the
/// design spec. Returns `None` only for an empty tree.
fn derive_current_node(tree: &[ExecutionNode]) -> Option<String> {
    // Prefer the latest non-terminal node by `started_at`. Nodes that haven't
    // started yet (`started_at == None`) are treated as "earliest" — they
    // can't be the current node.
    let non_terminal_latest = tree
        .iter()
        .filter(|n| !node_is_terminal(n.state))
        .max_by_key(|n| n.started_at.unwrap_or(DateTime::<Utc>::MIN_UTC));
    if let Some(node) = non_terminal_latest {
        return Some(node.block_id.0.clone());
    }

    // All terminal: pick the most recently completed.
    tree.iter()
        .max_by_key(|n| n.completed_at.unwrap_or(DateTime::<Utc>::MIN_UTC))
        .map(|n| n.block_id.0.clone())
}

pub(crate) async fn handle_query_instance(ctx: StepContext) -> Result<Value, StepError> {
    let target_id = parse_instance_id(&ctx.params, "instance_id")?;

    let storage = ctx.storage.as_ref();

    let Some(target) = storage
        .get_instance(target_id)
        .await
        .map_err(|e| map_storage_err(&e))?
    else {
        return Ok(json!({ "found": false }));
    };

    check_same_tenant(&ctx.tenant_id, &target.tenant_id, "query_instance")?;

    let completed_at = if target.state.is_terminal() {
        Some(target.updated_at)
    } else {
        None
    };

    let tree = storage
        .get_execution_tree(target_id)
        .await
        .map_err(|e| map_storage_err(&e))?;
    let current_node = derive_current_node(&tree);

    Ok(json!({
        "found": true,
        "state": target.state,
        "context": target.context,
        "started_at": target.created_at,
        "completed_at": completed_at,
        "current_node": current_node,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::{
        context::{ExecutionContext, RuntimeContext},
        execution::{BlockType, ExecutionNode, NodeState},
        ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId},
        instance::{InstanceState, Priority, TaskInstance},
    };
    use serde_json::json;
    use std::sync::Arc;

    fn mk_instance(tenant: &str) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId(tenant.into()),
            namespace: Namespace("default".into()),
            state: InstanceState::Scheduled,
            next_fire_at: Some(now),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data: json!({}),
                config: json!({}),
                audit: vec![],
                runtime: RuntimeContext::default(),
            },
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn mk_node(
        instance_id: InstanceId,
        block_id: &str,
        state: NodeState,
        started_at: Option<chrono::DateTime<Utc>>,
        completed_at: Option<chrono::DateTime<Utc>>,
    ) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId(block_id.into()),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state,
            started_at,
            completed_at,
        }
    }

    fn mk_ctx(
        caller: &TaskInstance,
        storage: Arc<dyn StorageBackend>,
        params: Value,
    ) -> StepContext {
        StepContext {
            instance_id: caller.id,
            tenant_id: caller.tenant_id.clone(),
            block_id: BlockId("q".into()),
            params,
            context: ExecutionContext::default(),
            attempt: 1,
            storage,
        }
    }

    #[tokio::test]
    async fn query_instance_returns_context_for_same_tenant() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        let target = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({ "instance_id": target.id.0.to_string() }),
        );
        let result = handle_query_instance(ctx).await.unwrap();

        assert_eq!(result["found"], json!(true));
        // state is serialized as a snake_case string (serde rename_all).
        assert_eq!(result["state"], json!("scheduled"));
        // started_at is always present when found.
        assert!(result.get("started_at").is_some());
        assert!(!result["started_at"].is_null());
        // Non-terminal target → completed_at present but null.
        assert!(result.get("completed_at").is_some());
        assert!(result["completed_at"].is_null());
        // current_node present in the shape; null when there are no nodes yet.
        assert!(result.get("current_node").is_some());
        assert!(result["current_node"].is_null());
    }

    #[tokio::test]
    async fn query_instance_completed_at_set_for_terminal_target() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        let mut target = mk_instance("T1");
        target.state = InstanceState::Completed;
        let completion_ts = Utc::now();
        target.updated_at = completion_ts;
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({ "instance_id": target.id.0.to_string() }),
        );
        let result = handle_query_instance(ctx).await.unwrap();

        assert_eq!(result["found"], json!(true));
        assert_eq!(result["state"], json!("completed"));
        assert!(result["completed_at"].is_string(), "expected ISO string");
    }

    #[tokio::test]
    async fn query_instance_current_node_picks_latest_non_terminal() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        let target = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let t0 = Utc::now();
        let t1 = t0 + chrono::Duration::seconds(1);
        let t2 = t0 + chrono::Duration::seconds(2);
        // done step (terminal), running step started later than done
        let done = mk_node(target.id, "stepA", NodeState::Completed, Some(t0), Some(t1));
        let running = mk_node(target.id, "stepB", NodeState::Running, Some(t2), None);
        storage.create_execution_node(&done).await.unwrap();
        storage.create_execution_node(&running).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({ "instance_id": target.id.0.to_string() }),
        );
        let result = handle_query_instance(ctx).await.unwrap();

        assert_eq!(result["current_node"], json!("stepB"));
    }

    #[tokio::test]
    async fn query_instance_current_node_falls_back_to_latest_terminal() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        let target = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let t0 = Utc::now();
        let t_early = t0 + chrono::Duration::seconds(1);
        let t_late = t0 + chrono::Duration::seconds(5);
        let first = mk_node(
            target.id,
            "stepA",
            NodeState::Completed,
            Some(t0),
            Some(t_early),
        );
        let last = mk_node(
            target.id,
            "stepB",
            NodeState::Completed,
            Some(t0),
            Some(t_late),
        );
        storage.create_execution_node(&first).await.unwrap();
        storage.create_execution_node(&last).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({ "instance_id": target.id.0.to_string() }),
        );
        let result = handle_query_instance(ctx).await.unwrap();

        assert_eq!(result["current_node"], json!("stepB"));
    }

    #[tokio::test]
    async fn query_instance_returns_found_false_for_missing_target() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();

        let missing_id = InstanceId::new();
        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({ "instance_id": missing_id.0.to_string() }),
        );
        let result = handle_query_instance(ctx).await.unwrap();

        assert_eq!(result["found"], json!(false));
    }

    #[tokio::test]
    async fn query_instance_denies_cross_tenant_query() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        let target = mk_instance("T2");
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({ "instance_id": target.id.0.to_string() }),
        );
        let err = handle_query_instance(ctx).await.unwrap_err();

        assert!(matches!(err, StepError::Permanent { .. }));
        if let StepError::Permanent { message, .. } = &err {
            assert!(
                message.contains("cross-tenant"),
                "expected 'cross-tenant' in message, got: {message}"
            );
        }
    }

    #[tokio::test]
    async fn query_instance_rejects_missing_instance_id_param() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();

        let ctx = mk_ctx(&caller, storage_dyn, json!({}));
        let err = handle_query_instance(ctx).await.unwrap_err();

        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn query_instance_rejects_invalid_uuid_param() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();

        let ctx = mk_ctx(&caller, storage_dyn, json!({ "instance_id": "not-a-uuid" }));
        let err = handle_query_instance(ctx).await.unwrap_err();

        assert!(matches!(err, StepError::Permanent { .. }));
    }
}
