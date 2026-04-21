use std::time::Duration;

use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;

use crate::error::EngineError;

/// Recover instances that were Running when the engine crashed.
/// Resets them to Scheduled so they will be re-claimed on the next tick.
pub async fn recover_stale_instances(
    storage: &dyn StorageBackend,
    stale_threshold_secs: u64,
) -> Result<u64, EngineError> {
    let threshold = Duration::from_secs(stale_threshold_secs);

    match storage.recover_stale_instances(threshold).await {
        Ok(count) => {
            if count > 0 {
                warn!(
                    count = count,
                    threshold_secs = stale_threshold_secs,
                    "recovered stale instances after crash"
                );
            } else {
                info!("no stale instances found during recovery check");
            }
            Ok(count)
        }
        Err(StorageError::Connection(msg)) => {
            warn!(error = %msg, "storage unavailable during recovery, will retry on next tick");
            Ok(0)
        }
        Err(e) => Err(EngineError::Storage(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_when_no_instances_exist() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let n = recover_stale_instances(&storage, 300).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn zero_threshold_still_safe_when_db_is_empty() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        // Edge case — extremely aggressive threshold must not panic or error.
        let n = recover_stale_instances(&storage, 0).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn recovers_stale_running_instances() {
        use chrono::Utc;
        use orch8_types::context::ExecutionContext;
        use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
        use orch8_types::instance::{InstanceState, Priority, TaskInstance};
        use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
        use serde_json::json;

        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();

        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            name: "rec".into(),
            version: 1,
            deprecated: false,
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: orch8_types::ids::BlockId("s1".into()),
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
            }))],
            interceptors: None,
            created_at: Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();

        let inst = TaskInstance {
            id: InstanceId::new(),
            sequence_id: seq.id,
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: Some(Utc::now() - chrono::Duration::seconds(600)),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: Utc::now() - chrono::Duration::seconds(600),
            updated_at: Utc::now() - chrono::Duration::seconds(600),
        };
        storage.create_instance(&inst).await.unwrap();

        // With a 60-second threshold, the instance (updated 600s ago) is stale.
        let n = recover_stale_instances(&storage, 60).await.unwrap();
        assert_eq!(n, 1);

        let recovered = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(recovered.state, InstanceState::Scheduled);
    }

    #[tokio::test]
    async fn skips_fresh_instances_within_threshold() {
        use chrono::Utc;
        use orch8_types::context::ExecutionContext;
        use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
        use orch8_types::instance::{InstanceState, Priority, TaskInstance};
        use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
        use serde_json::json;

        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();

        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            name: "fresh".into(),
            version: 1,
            deprecated: false,
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: orch8_types::ids::BlockId("s1".into()),
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
            }))],
            interceptors: None,
            created_at: Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();

        let inst = TaskInstance {
            id: InstanceId::new(),
            sequence_id: seq.id,
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: Some(Utc::now()),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        storage.create_instance(&inst).await.unwrap();

        let n = recover_stale_instances(&storage, 300).await.unwrap();
        assert_eq!(n, 0);

        let unchanged = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(unchanged.state, InstanceState::Running);
    }
}
