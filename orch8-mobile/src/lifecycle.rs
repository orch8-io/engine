//! Instance lifecycle management: start, cancel, complete, query, and GC.
//!
//! Extracted from `MobileEngine` to separate instance orchestration from
//! the `UniFFI` facade and tick loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::ids::{InstanceId, Namespace, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};

use crate::error::MobileError;
use crate::storage::MobileStorage;

/// Manages instance lifecycle operations: creation, cancellation,
/// completion, querying, and garbage collection.
pub struct InstanceLifecycleManager {
    storage: Arc<dyn StorageBackend>,
    mobile_storage: Arc<MobileStorage>,
    sequence_cache: Arc<SequenceCache>,
    dedup_keys: Arc<Mutex<HashMap<String, String>>>,
    max_concurrent_instances: u32,
}

impl InstanceLifecycleManager {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        mobile_storage: Arc<MobileStorage>,
        sequence_cache: Arc<SequenceCache>,
        max_concurrent_instances: u32,
    ) -> Self {
        Self {
            storage,
            mobile_storage,
            sequence_cache,
            dedup_keys: Arc::new(Mutex::new(HashMap::new())),
            max_concurrent_instances,
        }
    }

    /// Hydrate the in-memory dedup cache from persistent storage.
    pub async fn hydrate_dedup(&self) -> Result<(), MobileError> {
        match self.mobile_storage.list_all_dedup().await {
            Ok(rows) => {
                let mut dedup = self.dedup_keys.lock().await;
                for (key, instance_id) in rows {
                    dedup.insert(key, instance_id);
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to hydrate dedup keys from storage");
            }
        }
        Ok(())
    }

    /// Start a new workflow instance.
    pub async fn start(
        &self,
        sequence_name: &str,
        input: &str,
        dedup_key: Option<&str>,
    ) -> Result<String, MobileError> {
        if let Some(key) = dedup_key {
            let dedup = self.dedup_keys.lock().await;
            if let Some(existing_id) = dedup.get(key) {
                return Ok(existing_id.clone());
            }
            drop(dedup);
            if let Ok(Some(persisted_id)) = self.mobile_storage.get_dedup_instance(key).await {
                self.dedup_keys
                    .lock()
                    .await
                    .insert(key.to_string(), persisted_id.clone());
                return Ok(persisted_id);
            }
        }

        let active = self.count_active_instances().await?;
        if active >= u64::from(self.max_concurrent_instances) {
            return Err(MobileError::ResourceLimit {
                message: format!(
                    "max concurrent instances ({}) reached",
                    self.max_concurrent_instances
                ),
            });
        }

        let tenant = TenantId::new("mobile").expect("valid tenant");
        let ns = Namespace::new("default");
        let seq = self
            .storage
            .get_sequence_by_name(&tenant, &ns, sequence_name, None)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("sequence '{sequence_name}' not found"),
            })?;

        let input_data: serde_json::Value = serde_json::from_str(input)
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        let instance_id = InstanceId::new();
        let now = chrono::Utc::now();

        let context = orch8_types::context::ExecutionContext {
            data: input_data,
            ..orch8_types::context::ExecutionContext::default()
        };

        let instance = TaskInstance {
            id: instance_id,
            sequence_id: seq.id,
            tenant_id: tenant,
            namespace: ns,
            state: InstanceState::Scheduled,
            next_fire_at: Some(now),
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".to_string(),
            metadata: serde_json::json!({}),
            context,
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: dedup_key.map(std::string::ToString::to_string),
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };

        self.storage.create_instance(&instance).await?;

        let id_str = instance_id.to_string();

        if let Some(key) = dedup_key {
            self.dedup_keys
                .lock()
                .await
                .insert(key.to_string(), id_str.clone());
            let _ = self.mobile_storage.set_dedup(key, &id_str).await;
        }

        info!(instance_id = %id_str, sequence = %sequence_name, "started mobile instance");
        Ok(id_str)
    }

    /// Cancel a running instance.
    pub async fn cancel_instance(&self, instance_id: &str) -> Result<(), MobileError> {
        let id = parse_instance_id(instance_id)?;

        let _ = self
            .storage
            .get_instance(id)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("instance '{instance_id}' not found"),
            })?;

        self.storage
            .update_instance_state(id, InstanceState::Cancelled, None)
            .await?;

        let mut dedup = self.dedup_keys.lock().await;
        dedup.retain(|_, v| v != instance_id);
        drop(dedup);
        let _ = self.mobile_storage.remove_dedup(instance_id).await;

        info!(instance_id = %instance_id, "cancelled mobile instance");
        Ok(())
    }

    /// Complete a step that transitioned to Waiting state.
    ///
    /// Enqueues a `human_input:{step_name}` signal so the engine's normal
    /// tick loop picks it up via `check_human_input`, which validates the
    /// value against `effective_choices` and stores it under the step's
    /// `store_as` key in `context.data`.
    pub async fn complete_step(
        &self,
        instance_id: &str,
        step_name: &str,
        output: &str,
    ) -> Result<(), MobileError> {
        let id = parse_instance_id(instance_id)?;

        let inst = self
            .storage
            .get_instance(id)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("instance '{instance_id}' not found"),
            })?;

        if inst.state != InstanceState::Waiting {
            return Err(MobileError::InvalidInput {
                message: format!("instance is in {:?} state, expected Waiting", inst.state),
            });
        }

        let payload: serde_json::Value = serde_json::from_str(output)?;

        // Merge output into instance context so it's immediately available.
        let mut context = inst.context;
        if let serde_json::Value::Object(map) = &payload {
            for (k, v) in map {
                context.data[k] = v.clone();
            }
        } else {
            context.data[step_name] = payload.clone();
        }
        self.storage.update_instance_context(id, &context).await?;

        let signal = orch8_types::signal::Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: id,
            signal_type: orch8_types::signal::SignalType::Custom(format!(
                "human_input:{step_name}"
            )),
            payload,
            delivered: false,
            created_at: chrono::Utc::now(),
            delivered_at: None,
        };
        self.storage.enqueue_signal(&signal).await?;
        self.storage
            .update_instance_state(id, InstanceState::Scheduled, Some(chrono::Utc::now()))
            .await?;

        debug!(instance_id = %instance_id, step_name = %step_name, "enqueued human_input signal");
        Ok(())
    }

    /// Get a single instance by ID, returning the instance and its sequence name.
    pub async fn get_instance(
        &self,
        instance_id: &str,
    ) -> Result<(TaskInstance, String), MobileError> {
        let id = parse_instance_id(instance_id)?;
        let inst = self
            .storage
            .get_instance(id)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("instance '{instance_id}' not found"),
            })?;

        let seq = self
            .storage
            .get_sequence(inst.sequence_id)
            .await?
            .map(|s| s.name)
            .unwrap_or_default();

        Ok((inst, seq))
    }

    /// List all non-terminal instances with their sequence names.
    pub async fn active_instances(&self) -> Result<Vec<(TaskInstance, String)>, MobileError> {
        let filter = orch8_types::filter::InstanceFilter {
            states: Some(vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
                InstanceState::Paused,
            ]),
            ..Default::default()
        };
        let pagination = orch8_types::filter::Pagination {
            offset: 0,
            limit: 100,
            sort_ascending: false,
        };

        let instances = self.storage.list_instances(&filter, &pagination).await?;
        let mut result = Vec::with_capacity(instances.len());

        for inst in instances {
            let seq_name = self
                .sequence_cache
                .get_by_id(self.storage.as_ref(), inst.sequence_id)
                .await
                .ok()
                .map(|s| s.name.clone())
                .unwrap_or_default();

            result.push((inst, seq_name));
        }

        Ok(result)
    }

    /// Garbage-collect instances that have exceeded their max lifetime.
    /// Returns the number of instances expired.
    pub async fn gc_expired_instances(&self, max_lifetime_secs: u64) -> Result<u64, MobileError> {
        let max_lifetime = Duration::from_secs(max_lifetime_secs);
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(max_lifetime).unwrap_or(chrono::Duration::hours(24));

        let filter = orch8_types::filter::InstanceFilter {
            states: Some(vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
                InstanceState::Paused,
            ]),
            ..Default::default()
        };
        let pagination = orch8_types::filter::Pagination {
            offset: 0,
            limit: 50,
            sort_ascending: true,
        };

        let instances = self.storage.list_instances(&filter, &pagination).await?;
        let mut expired = 0;

        for inst in instances {
            if inst.created_at < cutoff {
                if let Err(e) = self
                    .storage
                    .update_instance_state(inst.id, InstanceState::Failed, None)
                    .await
                {
                    warn!(instance_id = %inst.id, error = %e, "failed to expire instance");
                } else {
                    info!(instance_id = %inst.id, "expired instance (exceeded max lifetime)");
                    expired += 1;
                }
            }
        }

        Ok(expired)
    }

    /// Remove a dedup entry associated with the given instance ID.
    pub async fn cleanup_dedup(&self, instance_id: &str) {
        let mut dedup = self.dedup_keys.lock().await;
        dedup.retain(|_, v| v != instance_id);
        let _ = self.mobile_storage.remove_dedup(instance_id).await;
    }

    async fn count_active_instances(&self) -> Result<u64, MobileError> {
        let filter = orch8_types::filter::InstanceFilter {
            states: Some(vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
            ]),
            ..Default::default()
        };
        let count = self.storage.count_instances(&filter).await?;
        Ok(count)
    }
}

fn parse_instance_id(s: &str) -> Result<InstanceId, MobileError> {
    let uuid = uuid::Uuid::parse_str(s).map_err(|e| MobileError::InvalidInput {
        message: format!("invalid instance ID '{s}': {e}"),
    })?;
    Ok(InstanceId::from_uuid(uuid))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (
        InstanceLifecycleManager,
        Arc<MobileStorage>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::file_mobile(&path)
                .await
                .unwrap(),
        );
        let storage: Arc<dyn StorageBackend> = sqlite.clone();
        let mobile_storage = Arc::new(MobileStorage::new(sqlite));
        let sequence_cache = Arc::new(SequenceCache::new(50, Duration::from_secs(3600)));
        let lifecycle = InstanceLifecycleManager::new(
            storage.clone(),
            mobile_storage.clone(),
            sequence_cache.clone(),
            10,
        );
        (lifecycle, mobile_storage, dir)
    }

    async fn seed_sequence(storage: &Arc<dyn StorageBackend>, name: &str) {
        let seq = orch8_types::sequence::SequenceDefinition {
            id: orch8_types::ids::SequenceId::new(),
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: Namespace::new("default"),
            name: name.to_string(),
            version: 1,
            deprecated: false,
            status: orch8_types::sequence::SequenceStatus::Production,
            blocks: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: orch8_types::ids::BlockId::new("s1"),
                    handler: "noop".to_string(),
                    params: serde_json::json!({}),
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
            ))],
            interceptors: None,
            created_at: chrono::Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();
    }

    #[tokio::test]
    async fn hydrate_dedup_loads_from_storage() {
        let (lifecycle, mobile_storage, _dir) = setup().await;

        mobile_storage.set_dedup("dk1", "inst-1").await.unwrap();
        mobile_storage.set_dedup("dk2", "inst-2").await.unwrap();

        lifecycle.hydrate_dedup().await.unwrap();

        // After hydration, start with the same dedup key should return the persisted ID.
        let id = lifecycle.start("seq", "{}", Some("dk1")).await;
        assert!(id.is_ok());
        assert_eq!(id.unwrap(), "inst-1");
    }

    #[tokio::test]
    async fn start_creates_instance_and_returns_id() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle
            .start("seq", r#"{"key":"val"}"#, None)
            .await
            .unwrap();
        assert!(!id.is_empty());

        let (inst, seq_name) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(seq_name, "seq");
        assert_eq!(inst.state, InstanceState::Scheduled);
        assert_eq!(inst.context.data["key"], "val");
    }

    #[tokio::test]
    async fn start_enforces_max_concurrent_instances() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        // Create a lifecycle manager with max_concurrent_instances = 2.
        let limited = InstanceLifecycleManager::new(
            lifecycle.storage.clone(),
            lifecycle.mobile_storage.clone(),
            lifecycle.sequence_cache.clone(),
            2,
        );

        limited.start("seq", "{}", None).await.unwrap();
        limited.start("seq", "{}", None).await.unwrap();

        let result = limited.start("seq", "{}", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("max concurrent instances"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn start_with_dedup_returns_same_id() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id1 = lifecycle.start("seq", "{}", Some("dedup-1")).await.unwrap();
        let id2 = lifecycle.start("seq", "{}", Some("dedup-1")).await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn cancel_instance_sets_state_and_cleans_dedup() {
        let (lifecycle, mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", Some("dk1")).await.unwrap();
        lifecycle.cancel_instance(&id).await.unwrap();

        let (inst, _) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(inst.state, InstanceState::Cancelled);

        // Dedup should be cleaned up in both memory and storage.
        assert!(mobile_storage
            .get_dedup_instance("dk1")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn complete_step_transitions_waiting_to_scheduled() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle
            .start("seq", r#"{"name":"Alice"}"#, None)
            .await
            .unwrap();

        // Manually set state to Waiting (simulating scheduler pause).
        let parsed = parse_instance_id(&id).unwrap();
        lifecycle
            .storage
            .update_instance_state(parsed, InstanceState::Waiting, Some(chrono::Utc::now()))
            .await
            .unwrap();

        lifecycle
            .complete_step(&id, "s1", r#"{"age":30}"#)
            .await
            .unwrap();

        let (inst, _) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(inst.state, InstanceState::Scheduled);
        assert_eq!(inst.context.data["name"], "Alice");
        assert_eq!(inst.context.data["age"], 30);
    }

    #[tokio::test]
    async fn complete_step_rejects_non_waiting() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", None).await.unwrap();
        let result = lifecycle.complete_step(&id, "s1", "{}").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected Waiting"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn active_instances_filters_terminal_states() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id1 = lifecycle.start("seq", "{}", None).await.unwrap();
        let id2 = lifecycle.start("seq", "{}", None).await.unwrap();

        let active = lifecycle.active_instances().await.unwrap();
        assert_eq!(active.len(), 2);

        // Cancel one instance — it should no longer appear in active_instances.
        lifecycle.cancel_instance(&id1).await.unwrap();
        let active = lifecycle.active_instances().await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].0.id.to_string(), id2);
    }

    #[tokio::test]
    async fn gc_expired_instances_fails_old_instances() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        // Create an instance directly with an old created_at timestamp.
        let old_instance = TaskInstance {
            id: InstanceId::new(),
            sequence_id: {
                let seq = lifecycle
                    .storage
                    .get_sequence_by_name(
                        &TenantId::new("mobile").unwrap(),
                        &Namespace::new("default"),
                        "seq",
                        None,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                seq.id
            },
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: Namespace::new("default"),
            state: InstanceState::Scheduled,
            next_fire_at: Some(chrono::Utc::now()),
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".to_string(),
            metadata: serde_json::json!({}),
            context: orch8_types::context::ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: chrono::Utc::now() - chrono::Duration::hours(48),
            updated_at: chrono::Utc::now() - chrono::Duration::hours(48),
        };
        lifecycle
            .storage
            .create_instance(&old_instance)
            .await
            .unwrap();

        let expired = lifecycle.gc_expired_instances(86_400).await.unwrap();
        assert_eq!(expired, 1);

        let (inst, _) = lifecycle
            .get_instance(&old_instance.id.to_string())
            .await
            .unwrap();
        assert_eq!(inst.state, InstanceState::Failed);
    }

    #[tokio::test]
    async fn gc_expired_instances_skips_recent_instances() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", None).await.unwrap();

        let expired = lifecycle.gc_expired_instances(86_400).await.unwrap();
        assert_eq!(expired, 0);

        let (inst, _) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(inst.state, InstanceState::Scheduled);
    }

    #[tokio::test]
    async fn cleanup_dedup_removes_entry() {
        let (lifecycle, mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", Some("dk1")).await.unwrap();
        assert_eq!(
            mobile_storage.get_dedup_instance("dk1").await.unwrap(),
            Some(id.clone())
        );

        lifecycle.cleanup_dedup(&id).await;

        assert!(mobile_storage
            .get_dedup_instance("dk1")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn get_instance_not_found() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        let result = lifecycle
            .get_instance(&uuid::Uuid::new_v4().to_string())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn start_rejects_unknown_sequence() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        let result = lifecycle.start("unknown", "{}", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found"), "unexpected error: {err}");
    }
}
