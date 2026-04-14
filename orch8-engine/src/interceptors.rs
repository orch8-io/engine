//! Interceptor dispatch: saves trace-artifact `BlockOutput` records at
//! defined lifecycle points (before/after step, on-signal, on-complete,
//! on-failure).  The interceptor handler is **not** invoked — we only
//! persist the artifact so it appears in the instance's block-output
//! timeline.

use chrono::Utc;
use tracing::warn;

use orch8_storage::StorageBackend;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::interceptor::InterceptorDef;
use orch8_types::output::BlockOutput;

/// Emit a `before_step` interceptor artifact.
pub async fn emit_before_step(
    storage: &dyn StorageBackend,
    interceptors: &InterceptorDef,
    instance_id: InstanceId,
    step_block_id: &BlockId,
) {
    if let Some(ref action) = interceptors.before_step {
        let block_id = BlockId(format!("_interceptor:before:{}", step_block_id.0));
        save_interceptor_output(storage, instance_id, block_id, &action.params).await;
    }
}

/// Emit an `after_step` interceptor artifact.
pub async fn emit_after_step(
    storage: &dyn StorageBackend,
    interceptors: &InterceptorDef,
    instance_id: InstanceId,
    step_block_id: &BlockId,
) {
    if let Some(ref action) = interceptors.after_step {
        let block_id = BlockId(format!("_interceptor:after:{}", step_block_id.0));
        save_interceptor_output(storage, instance_id, block_id, &action.params).await;
    }
}

/// Emit an `on_signal` interceptor artifact.
pub async fn emit_on_signal(
    storage: &dyn StorageBackend,
    interceptors: &InterceptorDef,
    instance_id: InstanceId,
    signal_info: &serde_json::Value,
) {
    if let Some(ref action) = interceptors.on_signal {
        let block_id = BlockId("_interceptor:on_signal".into());
        let mut output = action.params.clone();
        if let serde_json::Value::Object(ref mut map) = output {
            map.insert("_signal".into(), signal_info.clone());
        } else {
            output = serde_json::json!({
                "_params": action.params,
                "_signal": signal_info,
            });
        }
        save_interceptor_output(storage, instance_id, block_id, &output).await;
    }
}

/// Emit an `on_complete` interceptor artifact.
pub async fn emit_on_complete(
    storage: &dyn StorageBackend,
    interceptors: &InterceptorDef,
    instance_id: InstanceId,
) {
    if let Some(ref action) = interceptors.on_complete {
        let block_id = BlockId("_interceptor:on_complete".into());
        save_interceptor_output(storage, instance_id, block_id, &action.params).await;
    }
}

/// Emit an `on_failure` interceptor artifact.
pub async fn emit_on_failure(
    storage: &dyn StorageBackend,
    interceptors: &InterceptorDef,
    instance_id: InstanceId,
) {
    if let Some(ref action) = interceptors.on_failure {
        let block_id = BlockId("_interceptor:on_failure".into());
        save_interceptor_output(storage, instance_id, block_id, &action.params).await;
    }
}

/// Persist a single interceptor trace artifact as a `BlockOutput`.
async fn save_interceptor_output(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    block_id: BlockId,
    output: &serde_json::Value,
) {
    let bo = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id,
        block_id: block_id.clone(),
        output: output.clone(),
        output_ref: None,
        output_size: serde_json::to_vec(output)
            .map_or(0, |v| i32::try_from(v.len()).unwrap_or(i32::MAX)),
        attempt: 0,
        created_at: Utc::now(),
    };
    if let Err(e) = storage.save_block_output(&bo).await {
        warn!(
            instance_id = %instance_id,
            block_id = %block_id.0,
            error = %e,
            "failed to save interceptor output"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::interceptor::{InterceptorAction, InterceptorDef};

    async fn seed_instance(storage: &dyn StorageBackend, id: InstanceId) {
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
    async fn emit_before_and_after_step_saves_outputs() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef {
            before_step: Some(InterceptorAction {
                handler: "audit_log".into(),
                params: serde_json::json!({"stage": "pre"}),
            }),
            after_step: Some(InterceptorAction {
                handler: "audit_log".into(),
                params: serde_json::json!({"stage": "post"}),
            }),
            ..Default::default()
        };

        let step_id = BlockId("s1".into());
        emit_before_step(&storage, &interceptors, instance_id, &step_id).await;
        emit_after_step(&storage, &interceptors, instance_id, &step_id).await;

        let before = storage
            .get_block_output(instance_id, &BlockId("_interceptor:before:s1".into()))
            .await
            .unwrap();
        assert!(before.is_some());
        assert_eq!(before.unwrap().output["stage"], "pre");

        let after = storage
            .get_block_output(instance_id, &BlockId("_interceptor:after:s1".into()))
            .await
            .unwrap();
        assert!(after.is_some());
        assert_eq!(after.unwrap().output["stage"], "post");
    }

    #[tokio::test]
    async fn emit_on_signal_saves_output() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef {
            on_signal: Some(InterceptorAction {
                handler: "signal_hook".into(),
                params: serde_json::json!({"notify": true}),
            }),
            ..Default::default()
        };

        let signal_info = serde_json::json!({"type": "pause"});
        emit_on_signal(&storage, &interceptors, instance_id, &signal_info).await;

        let out = storage
            .get_block_output(instance_id, &BlockId("_interceptor:on_signal".into()))
            .await
            .unwrap();
        assert!(out.is_some());
        let output = out.unwrap().output;
        assert_eq!(output["notify"], true);
        assert_eq!(output["_signal"]["type"], "pause");
    }

    #[tokio::test]
    async fn emit_on_complete_saves_output() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef {
            on_complete: Some(InterceptorAction {
                handler: "done_hook".into(),
                params: serde_json::json!({"completed": true}),
            }),
            ..Default::default()
        };

        emit_on_complete(&storage, &interceptors, instance_id).await;

        let out = storage
            .get_block_output(instance_id, &BlockId("_interceptor:on_complete".into()))
            .await
            .unwrap();
        assert!(out.is_some());
        assert_eq!(out.unwrap().output["completed"], true);
    }

    #[tokio::test]
    async fn emit_on_failure_saves_output() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef {
            on_failure: Some(InterceptorAction {
                handler: "fail_hook".into(),
                params: serde_json::json!({"failed": true}),
            }),
            ..Default::default()
        };

        emit_on_failure(&storage, &interceptors, instance_id).await;

        let out = storage
            .get_block_output(instance_id, &BlockId("_interceptor:on_failure".into()))
            .await
            .unwrap();
        assert!(out.is_some());
        assert_eq!(out.unwrap().output["failed"], true);
    }

    #[tokio::test]
    async fn emit_on_signal_wraps_non_object_params_in_params_key() {
        // #104/#107 — when the interceptor's `params` is not a JSON object
        // (string, number, array), we can't insert `_signal` alongside it.
        // The code wraps it: `{ "_params": <original>, "_signal": {...} }`.
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef {
            on_signal: Some(InterceptorAction {
                handler: "signal_hook".into(),
                params: serde_json::json!(["a", "b"]), // array, not object
            }),
            ..Default::default()
        };

        let signal_info = serde_json::json!({"type": "cancel"});
        emit_on_signal(&storage, &interceptors, instance_id, &signal_info).await;

        let out = storage
            .get_block_output(instance_id, &BlockId("_interceptor:on_signal".into()))
            .await
            .unwrap()
            .expect("interceptor output should be saved");
        assert_eq!(out.output["_params"], serde_json::json!(["a", "b"]));
        assert_eq!(out.output["_signal"]["type"], "cancel");
    }

    #[tokio::test]
    async fn interceptor_output_size_matches_serialized_length() {
        // #106 — `output_size` persisted on the BlockOutput must match the
        // byte length of the serialized JSON output, so downstream size
        // budgets / externalization decisions are correct.
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let params = serde_json::json!({"k": "v", "n": 42});
        let interceptors = InterceptorDef {
            on_complete: Some(InterceptorAction {
                handler: "done_hook".into(),
                params: params.clone(),
            }),
            ..Default::default()
        };

        emit_on_complete(&storage, &interceptors, instance_id).await;

        let out = storage
            .get_block_output(instance_id, &BlockId("_interceptor:on_complete".into()))
            .await
            .unwrap()
            .expect("interceptor output should be saved");
        let expected = i32::try_from(serde_json::to_vec(&params).unwrap().len()).unwrap();
        assert_eq!(out.output_size, expected);
    }

    #[tokio::test]
    async fn before_step_uses_distinct_block_id_per_step() {
        // #98 (format assertion) — the before/after interceptor output
        // includes the target block_id in its own block_id so traces
        // correlate 1:1 with the step they wrap.
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef {
            before_step: Some(InterceptorAction {
                handler: "audit_log".into(),
                params: serde_json::json!({}),
            }),
            ..Default::default()
        };

        for step in &["alpha", "beta"] {
            emit_before_step(
                &storage,
                &interceptors,
                instance_id,
                &BlockId((*step).into()),
            )
            .await;
        }

        let a = storage
            .get_block_output(instance_id, &BlockId("_interceptor:before:alpha".into()))
            .await
            .unwrap();
        let b = storage
            .get_block_output(instance_id, &BlockId("_interceptor:before:beta".into()))
            .await
            .unwrap();
        assert!(a.is_some() && b.is_some(), "both outputs should be saved");
    }

    #[tokio::test]
    async fn after_step_saves_output_independently_of_before_step() {
        // Covers the case where only `after_step` is configured — an
        // earlier regression mistakenly tied after-emission to before-
        // emission, causing after-only configs to silently no-op.
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef {
            // `before_step` intentionally None.
            after_step: Some(InterceptorAction {
                handler: "audit_log".into(),
                params: serde_json::json!({"only": "after"}),
            }),
            ..Default::default()
        };

        let step_id = BlockId("s1".into());
        emit_before_step(&storage, &interceptors, instance_id, &step_id).await;
        emit_after_step(&storage, &interceptors, instance_id, &step_id).await;

        assert!(storage
            .get_block_output(instance_id, &BlockId("_interceptor:before:s1".into()))
            .await
            .unwrap()
            .is_none());
        let after = storage
            .get_block_output(instance_id, &BlockId("_interceptor:after:s1".into()))
            .await
            .unwrap();
        assert!(after.is_some());
        assert_eq!(after.unwrap().output["only"], "after");
    }

    #[tokio::test]
    async fn no_interceptor_configured_does_nothing() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        seed_instance(&storage, instance_id).await;

        let interceptors = InterceptorDef::default();
        let step_id = BlockId("s1".into());

        emit_before_step(&storage, &interceptors, instance_id, &step_id).await;
        emit_after_step(&storage, &interceptors, instance_id, &step_id).await;
        emit_on_signal(&storage, &interceptors, instance_id, &serde_json::json!({})).await;
        emit_on_complete(&storage, &interceptors, instance_id).await;
        emit_on_failure(&storage, &interceptors, instance_id).await;

        // No outputs should exist.
        let before = storage
            .get_block_output(instance_id, &BlockId("_interceptor:before:s1".into()))
            .await
            .unwrap();
        assert!(before.is_none());
    }
}
