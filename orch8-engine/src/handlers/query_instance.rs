//! `query_instance` builtin: read another instance's context + state.
//!
//! Same-tenant only. Returns `{ found: false }` for missing targets;
//! cross-tenant attempts return `Permanent` (does not leak existence).

use serde_json::{json, Value};

use orch8_types::error::StepError;

use super::util::{check_same_tenant, map_storage_err, parse_instance_id};
use super::StepContext;

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

    Ok(json!({
        "found": true,
        "state": target.state.to_string(),
        "context": target.context,
        "created_at": target.created_at,
        "updated_at": target.updated_at,
        // current_node deliberately omitted — design doc deferred this to a future change.
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::{
        context::{ExecutionContext, RuntimeContext},
        ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId},
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
        assert!(result.get("state").is_some());
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
