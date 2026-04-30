//! Self-modify handler: allows a running step to inject blocks into its own
//! instance's sequence at runtime. Enables agent self-modification patterns
//! where an AI agent decides its next steps dynamically.
//!
//! Params:
//! - `blocks` (array, required): Array of `BlockDefinition` objects to inject.
//! - `position` (u64, optional): 0-indexed position to insert at. If omitted, appends.
//!
//! Returns: `{ "injected_count": N, "position": P }`

use serde_json::{json, Value};

use orch8_types::error::StepError;

use super::StepContext;

/// Handle self-modification: inject blocks into the running instance.
pub async fn handle_self_modify(ctx: StepContext) -> Result<Value, StepError> {
    let blocks = ctx
        .params
        .get("blocks")
        .filter(|v| v.is_array())
        .cloned()
        .ok_or_else(|| StepError::Permanent {
            message: "missing required param: blocks (must be a JSON array)".into(),
            details: None,
        })?;

    let count = blocks.as_array().map_or(0, Vec::len);
    #[allow(clippy::cast_possible_truncation)]
    let position = ctx
        .params
        .get("position")
        .and_then(Value::as_u64)
        .map(|p| p.min(usize::MAX as u64) as usize);

    // Validate blocks parse as BlockDefinition.
    if let Err(e) =
        serde_json::from_value::<Vec<orch8_types::sequence::BlockDefinition>>(blocks.clone())
    {
        return Err(StepError::Permanent {
            message: format!("invalid blocks: {e}"),
            details: Some(blocks),
        });
    }

    // Store the injection request in the step output. The evaluator picks up
    // injected blocks from storage on the next tick. We use a special output
    // key `_self_modify` so the caller knows this step performed injection.
    //
    // The actual injection is done via the storage layer — we write to the
    // instance's injected_blocks field. This requires access to storage,
    // which step handlers don't have directly. Instead, we return a special
    // output that the step_block executor recognizes and acts on.
    Ok(json!({
        "_self_modify": true,
        "blocks": blocks,
        "position": position,
        "injected_count": count,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::StepContext;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};
    use serde_json::json;
    use std::sync::Arc;

    async fn make_storage() -> Arc<dyn orch8_storage::StorageBackend> {
        Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        )
    }

    fn mk_ctx(
        params: serde_json::Value,
        storage: Arc<dyn orch8_storage::StorageBackend>,
    ) -> StepContext {
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId("t".into()),
            block_id: BlockId("self_mod".into()),
            params,
            context: ExecutionContext::default(),
            attempt: 1,
            storage,
            wait_for_input: None,
        }
    }

    #[tokio::test]
    async fn missing_blocks_param_returns_permanent() {
        let s = make_storage().await;
        let ctx = mk_ctx(json!({}), s);
        let err = handle_self_modify(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
        if let StepError::Permanent { message, .. } = &err {
            assert!(message.contains("missing required param: blocks"));
        }
    }

    #[tokio::test]
    async fn blocks_not_array_returns_permanent() {
        let s = make_storage().await;
        let ctx = mk_ctx(json!({"blocks": "not_array"}), s);
        let err = handle_self_modify(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn blocks_null_returns_permanent() {
        let s = make_storage().await;
        let ctx = mk_ctx(json!({"blocks": null}), s);
        let err = handle_self_modify(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn invalid_block_defs_returns_permanent_with_details() {
        let s = make_storage().await;
        let ctx = mk_ctx(json!({"blocks": [{"not": "a block"}]}), s);
        let err = handle_self_modify(ctx).await.unwrap_err();
        match &err {
            StepError::Permanent { message, details } => {
                assert!(message.contains("invalid blocks"));
                assert!(details.is_some());
            }
            StepError::Retryable { .. } => panic!("expected Permanent, got: {err:?}"),
        }
    }

    fn valid_step_block() -> serde_json::Value {
        // Serialize an actual BlockDefinition to get the correct JSON shape.
        serde_json::to_value(orch8_types::sequence::BlockDefinition::Step(Box::new(
            orch8_types::sequence::StepDef {
                id: BlockId("injected".into()),
                handler: "noop".into(),
                params: json!({}),
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
            },
        )))
        .unwrap()
    }

    #[tokio::test]
    async fn valid_blocks_returns_self_modify_output() {
        let s = make_storage().await;
        let blocks = json!([valid_step_block()]);
        let ctx = mk_ctx(json!({"blocks": blocks}), s);
        let result = handle_self_modify(ctx).await.unwrap();
        assert_eq!(result["_self_modify"], true);
        assert_eq!(result["injected_count"], 1);
        assert!(result["position"].is_null());
    }

    #[tokio::test]
    async fn valid_blocks_with_position() {
        let s = make_storage().await;
        let blocks = json!([valid_step_block()]);
        let ctx = mk_ctx(json!({"blocks": blocks, "position": 3}), s);
        let result = handle_self_modify(ctx).await.unwrap();
        assert_eq!(result["position"], 3);
        assert_eq!(result["injected_count"], 1);
    }

    #[tokio::test]
    async fn empty_blocks_array_returns_zero_count() {
        let s = make_storage().await;
        let ctx = mk_ctx(json!({"blocks": []}), s);
        let result = handle_self_modify(ctx).await.unwrap();
        assert_eq!(result["injected_count"], 0);
        assert_eq!(result["_self_modify"], true);
    }

    #[tokio::test]
    async fn multiple_valid_blocks() {
        let s = make_storage().await;
        let mut b2 = valid_step_block();
        b2["Step"]["id"] = json!("s2");
        let blocks = json!([valid_step_block(), b2]);
        let ctx = mk_ctx(json!({"blocks": blocks}), s);
        let result = handle_self_modify(ctx).await.unwrap();
        assert_eq!(result["injected_count"], 2);
    }
}
