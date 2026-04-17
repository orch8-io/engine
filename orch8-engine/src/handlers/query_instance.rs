//! `query_instance` builtin: read another instance's context + state.
//!
//! Same-tenant only. Returns `{ found: false }` for missing targets;
//! cross-tenant attempts return `Permanent` (does not leak existence).

use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::{
    error::{StepError, StorageError},
    ids::InstanceId,
};

use super::StepContext;

pub(crate) async fn handle_query_instance(
    ctx: StepContext,
    storage: &dyn StorageBackend,
) -> Result<Value, StepError> {
    let id_str = ctx
        .params
        .get("instance_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StepError::Permanent {
            message: "missing 'instance_id' string param".into(),
            details: None,
        })?;
    let target_id = InstanceId(Uuid::parse_str(id_str).map_err(|e| StepError::Permanent {
        message: format!("invalid 'instance_id' uuid: {e}"),
        details: None,
    })?);

    let caller = storage
        .get_instance(ctx.instance_id)
        .await
        .map_err(|e| map_storage_err(&e))?
        .ok_or_else(|| StepError::Permanent {
            message: "caller instance not found".into(),
            details: None,
        })?;

    let Some(target) = storage
        .get_instance(target_id)
        .await
        .map_err(|e| map_storage_err(&e))?
    else {
        return Ok(json!({ "found": false }));
    };

    if target.tenant_id != caller.tenant_id {
        warn!(
            caller_tenant = %caller.tenant_id.0,
            target_tenant = %target.tenant_id.0,
            caller_instance_id = %ctx.instance_id.0,
            target_instance_id = %target_id.0,
            "query_instance: cross-tenant query denied"
        );
        return Err(StepError::Permanent {
            message: "cross-tenant query denied".into(),
            details: Some(json!({
                "caller_tenant": caller.tenant_id.0,
                "target_tenant": target.tenant_id.0,
            })),
        });
    }

    Ok(json!({
        "found": true,
        "state": target.state.to_string(),
        "context": target.context,
        "created_at": target.created_at,
        "updated_at": target.updated_at,
        // current_node deliberately omitted — design doc deferred this to a future change.
    }))
}

#[allow(dead_code)] // used by handle_query_instance (registered in T14)
fn map_storage_err(e: &StorageError) -> StepError {
    match e {
        StorageError::Connection(_) | StorageError::PoolExhausted | StorageError::Query(_) => {
            StepError::Retryable {
                message: format!("storage: {e}"),
                details: None,
            }
        }
        _ => StepError::Permanent {
            message: format!("storage: {e}"),
            details: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::{
        context::{ExecutionContext, RuntimeContext},
        ids::{BlockId, Namespace, SequenceId, TenantId},
        instance::{InstanceState, Priority, TaskInstance},
    };
    use serde_json::json;

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

    #[tokio::test]
    async fn query_instance_returns_context_for_same_tenant() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1");
        let target = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("q".into()),
            params: serde_json::json!({ "instance_id": target.id.0.to_string() }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let result = handle_query_instance(ctx, &storage).await.unwrap();

        assert_eq!(result["found"], serde_json::json!(true));
        assert!(result.get("state").is_some());
    }

    #[tokio::test]
    async fn query_instance_returns_found_false_for_missing_target() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();

        let missing_id = InstanceId::new();
        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("q".into()),
            params: serde_json::json!({ "instance_id": missing_id.0.to_string() }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let result = handle_query_instance(ctx, &storage).await.unwrap();

        assert_eq!(result["found"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn query_instance_denies_cross_tenant_query() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1");
        let target = mk_instance("T2");
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("q".into()),
            params: serde_json::json!({ "instance_id": target.id.0.to_string() }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_query_instance(ctx, &storage).await.unwrap_err();

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
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("q".into()),
            params: serde_json::json!({}),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_query_instance(ctx, &storage).await.unwrap_err();

        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn query_instance_rejects_invalid_uuid_param() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("q".into()),
            params: serde_json::json!({ "instance_id": "not-a-uuid" }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_query_instance(ctx, &storage).await.unwrap_err();

        assert!(matches!(err, StepError::Permanent { .. }));
    }
}
