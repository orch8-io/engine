//! Bounded notification tracking for mobile engine lifecycle events.
//!
//! `MobileNotifier` deduplicates `on_instance_completed`, `on_instance_failed`,
//! and `on_step_pending` callbacks so the host app receives each event at most once
//! (modulo eviction after `MAX_TRACKED` entries).

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::debug;

use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::instance::InstanceState;

use crate::handlers::EngineListener;

/// Maximum number of tracked instance/step IDs before the set is cleared.
/// After clearing, a small number of duplicate notifications may be emitted,
/// which is acceptable for mobile use-cases.
const MAX_TRACKED: usize = 1000;

/// Manages listener registration and bounded deduplication of lifecycle events.
pub(crate) struct MobileNotifier {
    listener: Mutex<Option<Arc<dyn EngineListener>>>,
    terminal_seen: Mutex<HashSet<String>>,
    waiting_seen: Mutex<HashSet<String>>,
    /// Terminal instance IDs already returned for dedup cleanup. Tracked
    /// separately from `terminal_seen` so cleanup happens even when no
    /// listener is registered, without suppressing the listener callbacks
    /// that fire once a listener *is* set.
    cleanup_seen: Mutex<HashSet<String>>,
}

impl MobileNotifier {
    pub fn new() -> Self {
        Self {
            listener: Mutex::new(None),
            terminal_seen: Mutex::new(HashSet::new()),
            waiting_seen: Mutex::new(HashSet::new()),
            cleanup_seen: Mutex::new(HashSet::new()),
        }
    }

    /// Replace the current listener. Called from `MobileEngine::set_listener`.
    pub async fn set_listener(&self, listener: Arc<dyn EngineListener>) {
        *self.listener.lock().await = Some(listener);
    }

    /// Clone the current listener (if any). Used by the tick loop to obtain a
    /// cheaply-shared reference without holding the lock.
    pub async fn listener(&self) -> Option<Arc<dyn EngineListener>> {
        self.listener.lock().await.clone()
    }

    // ------------------------------------------------------------------
    // Terminal events (Completed / Failed / Cancelled)
    // ------------------------------------------------------------------

    /// Scan for terminal instances and fire `on_instance_completed` /
    /// `on_instance_failed` for each newly-seen instance.
    ///
    /// Returns the list of terminal instance IDs that were notified, so the
    /// caller can clean up dedup keys in the lifecycle manager. The scan runs
    /// even when no listener is registered — dedup cleanup must not depend on
    /// listener presence, or the dedup tables grow unboundedly — only the
    /// callbacks are gated on a listener.
    pub async fn fire_terminal_events(&self, storage: &Arc<dyn StorageBackend>) -> Vec<String> {
        let listener = self.listener().await;

        let filter = InstanceFilter {
            states: Some(vec![
                InstanceState::Completed,
                InstanceState::Failed,
                InstanceState::Cancelled,
            ]),
            ..Default::default()
        };
        let pagination = Pagination {
            offset: 0,
            limit: 100,
            sort_ascending: false,
        };

        let Ok(instances) = storage.list_instances(&filter, &pagination).await else {
            return Vec::new();
        };

        let mut cleanup_seen = self.cleanup_seen.lock().await;
        if cleanup_seen.len() > MAX_TRACKED {
            debug!(
                count = cleanup_seen.len(),
                limit = MAX_TRACKED,
                "terminal cleanup set exceeded limit — clearing"
            );
            cleanup_seen.clear();
        }

        let mut seen = self.terminal_seen.lock().await;
        if seen.len() > MAX_TRACKED {
            debug!(
                count = seen.len(),
                limit = MAX_TRACKED,
                "terminal notification set exceeded limit — clearing"
            );
            seen.clear();
        }

        let mut terminal_ids = Vec::new();

        for inst in instances {
            let id_str = inst.id.to_string();

            // Dedup cleanup is listener-independent: report each terminal
            // instance exactly once regardless of callback delivery.
            if cleanup_seen.insert(id_str.clone()) {
                terminal_ids.push(id_str.clone());
            }

            let Some(ref listener) = listener else {
                continue;
            };
            if !seen.insert(id_str.clone()) {
                continue;
            }

            match inst.state {
                InstanceState::Completed => {
                    let output = serde_json::to_string(&inst.context.data).unwrap_or_default();
                    debug!(instance_id = %id_str, "firing on_instance_completed");
                    let cb = Arc::clone(listener);
                    let id = id_str.clone();
                    tokio::task::spawn_blocking(move || cb.on_instance_completed(id, output));
                }
                InstanceState::Failed => {
                    let error = serde_json::to_string(&inst.context.data).unwrap_or_default();
                    let cb = Arc::clone(listener);
                    let id = id_str.clone();
                    tokio::task::spawn_blocking(move || cb.on_instance_failed(id, error));
                }
                _ => {}
            }
        }

        terminal_ids
    }

    // ------------------------------------------------------------------
    // Step-pending events (Waiting)
    // ------------------------------------------------------------------

    /// Scan for waiting instances and fire `on_step_pending` for each newly-seen
    /// instance+step combination.
    pub async fn fire_step_pending_events(
        &self,
        storage: &Arc<dyn StorageBackend>,
        sequence_cache: &Arc<SequenceCache>,
    ) {
        let Some(listener) = self.listener().await else {
            return;
        };

        let filter = InstanceFilter {
            states: Some(vec![InstanceState::Waiting]),
            ..Default::default()
        };
        let pagination = Pagination {
            offset: 0,
            limit: 100,
            sort_ascending: false,
        };

        let Ok(instances) = storage.list_instances(&filter, &pagination).await else {
            return;
        };

        let mut seen = self.waiting_seen.lock().await;
        if seen.len() > MAX_TRACKED {
            debug!(
                count = seen.len(),
                limit = MAX_TRACKED,
                "waiting notification set exceeded limit — clearing"
            );
            seen.clear();
        }

        for inst in instances {
            let Some(ref step_id) = inst.context.runtime.current_step else {
                continue;
            };
            let dedup_key = format!("{}:{}", inst.id, step_id);
            if !seen.insert(dedup_key) {
                continue;
            }
            let handler = sequence_cache
                .get_by_id(storage.as_ref(), inst.sequence_id)
                .await
                .ok()
                .and_then(|seq| {
                    seq.blocks.iter().find_map(|b| {
                        if let orch8_types::sequence::BlockDefinition::Step(s) = b
                            && s.id == *step_id
                        {
                            return Some(s.handler.clone());
                        }
                        None
                    })
                })
                .unwrap_or_default();

            debug!(instance_id = %inst.id, step_id = %step_id, handler = %handler, "firing on_step_pending");
            let cb = Arc::clone(&listener);
            let id = inst.id.to_string();
            let step = step_id.to_string();
            tokio::task::spawn_blocking(move || cb.on_step_pending(id, step, handler));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{Priority, TaskInstance};
    use orch8_types::sequence::{SequenceDefinition, SequenceStatus};

    async fn setup_storage_with_terminal_instance(
        state: InstanceState,
    ) -> (Arc<dyn StorageBackend>, String) {
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let storage: Arc<dyn StorageBackend> = sqlite;
        let tenant = TenantId::new("mobile").unwrap();
        let ns = Namespace::new("default");

        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: tenant.clone(),
            namespace: ns.clone(),
            name: "seq".to_string(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::Production,
            blocks: vec![],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();

        let now = chrono::Utc::now();
        let instance = TaskInstance {
            id: InstanceId::new(),
            sequence_id: seq.id,
            tenant_id: tenant,
            namespace: ns,
            state,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".to_string(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        let id_str = instance.id.to_string();
        storage.create_instance(&instance).await.unwrap();
        (storage, id_str)
    }

    #[tokio::test]
    async fn notifier_starts_without_listener() {
        let notifier = MobileNotifier::new();
        assert!(notifier.listener().await.is_none());
    }

    #[test]
    fn max_tracked_is_reasonable() {
        assert_eq!(MAX_TRACKED, 1000);
    }

    /// Dedup cleanup must not depend on a listener being registered: terminal
    /// instance ids are returned for cleanup exactly once even with no
    /// listener, otherwise the dedup tables grow unboundedly.
    #[tokio::test]
    async fn terminal_ids_returned_for_cleanup_without_listener() {
        let (storage, id_str) =
            setup_storage_with_terminal_instance(InstanceState::Completed).await;
        let notifier = MobileNotifier::new();

        let ids = notifier.fire_terminal_events(&storage).await;
        assert_eq!(
            ids,
            vec![id_str],
            "terminal id must be returned for cleanup"
        );

        // Second scan: already reported for cleanup — no repeat work.
        assert!(notifier.fire_terminal_events(&storage).await.is_empty());
    }

    /// A listener registered AFTER an instance went terminal must still
    /// receive the terminal callback — the listener-independent cleanup scan
    /// must not suppress listener notifications.
    #[tokio::test]
    async fn listener_set_later_still_gets_terminal_callbacks() {
        struct RecordingListener {
            completed: tokio::sync::Mutex<Vec<String>>,
        }
        impl EngineListener for RecordingListener {
            fn on_instance_completed(&self, instance_id: String, _output: String) {
                self.completed.blocking_lock().push(instance_id);
            }
            fn on_instance_failed(&self, _instance_id: String, _error: String) {}
            fn on_step_pending(&self, _instance_id: String, _step: String, _handler: String) {}
        }

        let (storage, id_str) =
            setup_storage_with_terminal_instance(InstanceState::Completed).await;
        let notifier = MobileNotifier::new();

        // Scan before any listener is registered (marks the id for cleanup).
        assert_eq!(
            notifier.fire_terminal_events(&storage).await,
            vec![id_str.clone()]
        );

        let listener = Arc::new(RecordingListener {
            completed: tokio::sync::Mutex::new(Vec::new()),
        });
        notifier.set_listener(listener.clone()).await;

        let ids = notifier.fire_terminal_events(&storage).await;
        assert!(ids.is_empty(), "cleanup must not repeat");

        // Callbacks are delivered via spawn_blocking — give them a moment.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(*listener.completed.lock().await, vec![id_str]);
    }
}
