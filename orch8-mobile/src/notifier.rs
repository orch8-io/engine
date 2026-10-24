//! Bounded notification tracking for mobile engine lifecycle events.
//!
//! `MobileNotifier` deduplicates `on_instance_completed`, `on_instance_failed`,
//! and `on_step_pending` callbacks so the host app receives each event at most once
//! (the tracked sets are pruned to the instances still present in storage, so
//! nothing is re-notified while its row exists).

use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;

use crate::handlers::EngineListener;
use crate::storage::MobileStorage;

/// Tracked-set size above which a debug line is logged. The sets are pruned to
/// the rows still present in storage (MC-03: scans now drain every page, so
/// clearing a full set would re-notify every retained instance).
const MAX_TRACKED: usize = 1000;

// Callbacks share the runtime's blocking pool with handlers and memory probes.
// Keep running plus queued listener work below its minimum four-thread pool.
const MAX_IN_FLIGHT_CALLBACKS: usize = 2;

/// Manages listener registration and bounded deduplication of lifecycle events.
pub(crate) struct MobileNotifier {
    listener: StdMutex<ListenerState>,
    cancelled: CancellationToken,
    callback_slots: Arc<Semaphore>,
    terminal_seen: Mutex<HashSet<InstanceId>>,
    waiting_seen: Mutex<HashSet<(InstanceId, BlockId)>>,
    /// Terminal instance IDs already returned for dedup cleanup. Tracked
    /// separately from `terminal_seen` so cleanup happens even when no
    /// listener is registered, without suppressing the listener callbacks
    /// that fire once a listener *is* set.
    cleanup_seen: Mutex<HashSet<InstanceId>>,
}

struct ListenerState {
    closed: bool,
    listener: Option<Arc<dyn EngineListener>>,
}

// Keep physical capacity through foreign destruction as well as invocation.
struct ListenerCall {
    listener: Arc<dyn EngineListener>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl MobileNotifier {
    pub fn new() -> Self {
        Self {
            listener: StdMutex::new(ListenerState {
                closed: false,
                listener: None,
            }),
            cancelled: CancellationToken::new(),
            callback_slots: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CALLBACKS)),
            terminal_seen: Mutex::new(HashSet::new()),
            waiting_seen: Mutex::new(HashSet::new()),
            cleanup_seen: Mutex::new(HashSet::new()),
        }
    }

    /// Replace the current listener. Called from `MobileEngine::set_listener`.
    pub fn set_listener(&self, listener: Arc<dyn EngineListener>) {
        let retired = {
            let mut state = self
                .listener
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                drop(state);
                return;
            }
            state.listener.replace(listener)
        };
        drop(retired);
    }

    /// Clone the current listener (if any). Used by the tick loop to obtain a
    /// cheaply-shared reference without holding the lock.
    pub fn listener(&self) -> Option<Arc<dyn EngineListener>> {
        self.listener
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .listener
            .clone()
    }

    pub fn close(&self) -> Option<Arc<dyn EngineListener>> {
        let retired = {
            let mut state = self
                .listener
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.listener.take()
        };
        self.cancelled.cancel();
        self.callback_slots.close();
        retired
    }

    // ------------------------------------------------------------------
    // Terminal events (Completed / Failed / Cancelled)
    // ------------------------------------------------------------------

    /// Scan for terminal instances and fire `on_instance_completed` /
    /// `on_instance_failed` for each newly-seen instance.
    ///
    /// Returns newly terminal instance IDs for lifecycle dedup cleanup,
    /// independently of callback admission or delivery. The scan runs
    /// even when no listener is registered — dedup cleanup must not depend on
    /// listener presence, or the dedup tables grow unboundedly — only the
    /// callbacks are gated on a listener.
    pub async fn fire_terminal_events(
        &self,
        storage: &Arc<dyn StorageBackend>,
        mobile_storage: &MobileStorage,
    ) -> Vec<String> {
        let listener = self.listener();
        let Ok(instances) = mobile_storage.list_terminal_instance_states().await else {
            return Vec::new();
        };
        let present: HashSet<InstanceId> = instances.iter().map(|(id, _)| *id).collect();

        let mut cleanup_seen = self.cleanup_seen.lock().await;
        cleanup_seen.retain(|id| present.contains(id));
        if cleanup_seen.len() > MAX_TRACKED {
            debug!(
                count = cleanup_seen.len(),
                limit = MAX_TRACKED,
                "terminal cleanup set is unusually large"
            );
        }

        let mut terminal_ids = Vec::new();
        for (id, _) in &instances {
            // Dedup cleanup is listener-independent: report each terminal
            // instance exactly once regardless of callback delivery.
            if cleanup_seen.insert(*id) {
                terminal_ids.push(id.to_string());
            }
        }
        drop(cleanup_seen);

        let Some(listener) = listener else {
            return terminal_ids;
        };
        {
            let mut seen = self.terminal_seen.lock().await;
            seen.retain(|id| present.contains(id));
        }

        for (id, projected_state) in instances {
            if self.terminal_seen.lock().await.contains(&id) {
                continue;
            }
            let Ok(Some(inst)) = storage.get_instance(id).await else {
                continue;
            };
            if inst.state != projected_state {
                continue;
            }
            let Ok(permit) = Arc::clone(&self.callback_slots).try_acquire_owned() else {
                // Leave this and subsequent rows unseen so a later scan can
                // deliver them when a listener callback actually finishes.
                break;
            };
            if !self.terminal_seen.lock().await.insert(id) {
                continue;
            }

            let id_str = inst.id.to_string();

            match inst.state {
                InstanceState::Completed => {
                    let output = serde_json::to_string(&inst.context.data).unwrap_or_default();
                    debug!(instance_id = %id_str, "firing on_instance_completed");
                    let call = ListenerCall {
                        listener: Arc::clone(&listener),
                        _permit: permit,
                    };
                    let cancelled = self.cancelled.clone();
                    let id = id_str.clone();
                    tokio::task::spawn_blocking(move || {
                        if !cancelled.is_cancelled() {
                            call.listener.on_instance_completed(id, output);
                        }
                        drop(call);
                    });
                }
                InstanceState::Failed => {
                    let error = serde_json::to_string(&inst.context.data).unwrap_or_default();
                    let call = ListenerCall {
                        listener: Arc::clone(&listener),
                        _permit: permit,
                    };
                    let cancelled = self.cancelled.clone();
                    let id = id_str.clone();
                    tokio::task::spawn_blocking(move || {
                        if !cancelled.is_cancelled() {
                            call.listener.on_instance_failed(id, error);
                        }
                        drop(call);
                    });
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
    /// instance+step combination. Terminal and pending callbacks share bounded
    /// admission; rows deferred by a full callback pool remain eligible for the
    /// next scan.
    pub async fn fire_step_pending_events(
        &self,
        storage: &Arc<dyn StorageBackend>,
        mobile_storage: &MobileStorage,
        sequence_cache: &Arc<SequenceCache>,
    ) {
        let Some(listener) = self.listener() else {
            return;
        };

        let Ok(instances) = mobile_storage.list_waiting_steps().await else {
            return;
        };

        let mut seen = self.waiting_seen.lock().await;
        // Forget steps that are no longer waiting so a later wait on the same
        // step (loop iteration) notifies again, and the set stays bounded.
        let present: HashSet<(InstanceId, BlockId)> = instances
            .iter()
            .map(|inst| (inst.id, inst.step_id.clone()))
            .collect();
        seen.retain(|key| present.contains(key));
        if seen.len() > MAX_TRACKED {
            debug!(
                count = seen.len(),
                limit = MAX_TRACKED,
                "waiting notification set is unusually large"
            );
        }

        drop(seen);

        for inst in instances {
            let key = (inst.id, inst.step_id.clone());
            if self.waiting_seen.lock().await.contains(&key) {
                continue;
            }
            let Ok(permit) = Arc::clone(&self.callback_slots).try_acquire_owned() else {
                break;
            };
            let handler = sequence_cache
                .get_by_id_owned(storage, inst.sequence_id)
                .await
                .ok()
                .and_then(|seq| {
                    match orch8_engine::evaluator::find_block(&seq.blocks, &inst.step_id)? {
                        orch8_types::sequence::BlockDefinition::Step(step) => {
                            Some(step.handler.clone())
                        }
                        _ => None,
                    }
                })
                .unwrap_or_default();

            // Only deduplicate after admission and all async lookup work.
            // A cancelled scan must not suppress a callback it never spawned.
            if !self.waiting_seen.lock().await.insert(key) {
                continue;
            }
            debug!(instance_id = %inst.id, step_id = %inst.step_id, handler = %handler, "firing on_step_pending");
            let call = ListenerCall {
                listener: Arc::clone(&listener),
                _permit: permit,
            };
            let cancelled = self.cancelled.clone();
            let id = inst.id.to_string();
            let step = inst.step_id.into_string();
            tokio::task::spawn_blocking(move || {
                if !cancelled.is_cancelled() {
                    call.listener.on_step_pending(id, step, handler);
                }
                drop(call);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{Priority, TaskInstance};
    use orch8_types::sequence::{SequenceDefinition, SequenceStatus};

    async fn setup_storage_with_terminal_instance(
        state: InstanceState,
    ) -> (Arc<dyn StorageBackend>, Arc<MobileStorage>, String) {
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let storage: Arc<dyn StorageBackend> = sqlite.clone();
        let mobile_storage = Arc::new(MobileStorage::new(sqlite));
        let tenant = TenantId::new("mobile").unwrap();
        let ns = Namespace::new("default");

        let seq = SequenceDefinition {
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
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
            placement: None,
            created_at: chrono::Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();

        let now = chrono::Utc::now();
        let instance = TaskInstance {
            claimed_at: None,
            sub_tenant: None,
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
        (storage, mobile_storage, id_str)
    }

    struct GatedListener {
        ids: std::sync::Mutex<Vec<String>>,
        released: (std::sync::Mutex<bool>, std::sync::Condvar),
    }

    impl GatedListener {
        fn record(&self, id: String) {
            self.ids.lock().unwrap().push(id);
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
        }

        fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }

    impl EngineListener for GatedListener {
        fn on_instance_completed(&self, id: String, _output: String) {
            self.record(id);
        }
        fn on_instance_failed(&self, id: String, _error: String) {
            self.record(id);
        }
        fn on_step_pending(&self, id: String, _step: String, _handler: String) {
            self.record(id);
        }
    }

    #[test]
    fn close_cancels_queued_terminal_callbacks_before_host_invocation() {
        for state in [InstanceState::Completed, InstanceState::Failed] {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let (storage, mobile, _) =
                runtime.block_on(setup_storage_with_terminal_instance(state));
            let notifier = MobileNotifier::new();
            let listener = Arc::new(GatedListener {
                ids: std::sync::Mutex::new(Vec::new()),
                released: (std::sync::Mutex::new(true), std::sync::Condvar::new()),
            });
            notifier.set_listener(listener.clone());
            let (entered, started) = std::sync::mpsc::channel();
            let (release, gate) = std::sync::mpsc::channel();
            drop(runtime.spawn_blocking(move || {
                entered.send(()).unwrap();
                let _ = gate.recv();
            }));
            started
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            runtime.block_on(notifier.fire_terminal_events(&storage, &mobile));
            let admitted = notifier.callback_slots.available_permits();
            drop(notifier.close());
            release.send(()).unwrap();
            runtime.block_on(async { runtime.spawn_blocking(|| ()).await.unwrap() });
            assert_eq!(admitted, MAX_IN_FLIGHT_CALLBACKS - 1);
            assert!(listener.ids.lock().unwrap().is_empty());
            assert_eq!(
                notifier.callback_slots.available_permits(),
                MAX_IN_FLIGHT_CALLBACKS
            );
        }
    }

    struct ReleaseListener(Arc<GatedListener>);
    impl Drop for ReleaseListener {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    async fn notification_backlog() -> (Arc<dyn StorageBackend>, Arc<MobileStorage>, HashSet<String>)
    {
        let (storage, mobile_storage, first_id) =
            setup_storage_with_terminal_instance(InstanceState::Completed).await;
        let base = storage
            .get_instance(InstanceId::from_uuid(
                uuid::Uuid::parse_str(&first_id).unwrap(),
            ))
            .await
            .unwrap()
            .unwrap();
        let mut expected = HashSet::from([first_id]);
        for index in 1..12 {
            let mut instance = base.clone();
            instance.id = InstanceId::new();
            instance.state = if index >= 6 {
                instance.context.runtime.current_step = Some(BlockId::new("review"));
                InstanceState::Waiting
            } else if index % 2 == 0 {
                InstanceState::Completed
            } else {
                InstanceState::Failed
            };
            expected.insert(instance.id.to_string());
            storage.create_instance(&instance).await.unwrap();
        }
        (storage, mobile_storage, expected)
    }

    async fn check_bounded_callback_admission(waiting_first: bool) {
        let (storage, mobile_storage, expected) = notification_backlog().await;
        let notifier = MobileNotifier::new();
        let listener = Arc::new(GatedListener {
            ids: std::sync::Mutex::new(Vec::new()),
            released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        });
        let _release = ReleaseListener(Arc::clone(&listener));
        notifier.set_listener(listener.clone());
        let cache = Arc::new(SequenceCache::new(16, std::time::Duration::from_secs(60)));
        if waiting_first {
            notifier
                .fire_step_pending_events(&storage, &mobile_storage, &cache)
                .await;
        }
        let cleanup_ids = notifier
            .fire_terminal_events(&storage, &mobile_storage)
            .await;
        assert_eq!(
            cleanup_ids.len(),
            6,
            "cleanup must not wait on callback capacity"
        );
        notifier
            .fire_step_pending_events(&storage, &mobile_storage, &cache)
            .await;
        let admitted =
            notifier.terminal_seen.lock().await.len() + notifier.waiting_seen.lock().await.len();
        assert_eq!(
            admitted, 2,
            "blocked listeners must bound running AND queued callback work"
        );
        assert!(
            notifier
                .fire_terminal_events(&storage, &mobile_storage)
                .await
                .is_empty()
        );
        notifier
            .fire_step_pending_events(&storage, &mobile_storage, &cache)
            .await;
        assert_eq!(
            notifier.terminal_seen.lock().await.len() + notifier.waiting_seen.lock().await.len(),
            2
        );

        listener.release();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                notifier
                    .fire_terminal_events(&storage, &mobile_storage)
                    .await;
                notifier
                    .fire_step_pending_events(&storage, &mobile_storage, &cache)
                    .await;
                if listener.ids.lock().unwrap().len() == expected.len() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let delivered = listener.ids.lock().unwrap();
        assert_eq!(
            delivered.len(),
            expected.len(),
            "callbacks must not duplicate"
        );
        assert_eq!(
            delivered.iter().cloned().collect::<HashSet<_>>(),
            expected,
            "capacity-deferred callbacks must remain eligible for later scans"
        );
    }

    #[tokio::test]
    async fn terminal_callbacks_bound_admission_and_retry_deferred_notifications() {
        check_bounded_callback_admission(false).await;
    }

    #[tokio::test]
    async fn waiting_callbacks_share_admission_with_terminal_notifications() {
        check_bounded_callback_admission(true).await;
    }

    #[tokio::test]
    async fn notifier_starts_without_listener() {
        let notifier = MobileNotifier::new();
        assert!(notifier.listener().is_none());
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
        let (storage, mobile_storage, id_str) =
            setup_storage_with_terminal_instance(InstanceState::Completed).await;
        let notifier = MobileNotifier::new();

        let ids = notifier
            .fire_terminal_events(&storage, &mobile_storage)
            .await;
        assert_eq!(
            ids,
            vec![id_str],
            "terminal id must be returned for cleanup"
        );

        // Second scan: already reported for cleanup — no repeat work.
        assert!(
            notifier
                .fire_terminal_events(&storage, &mobile_storage)
                .await
                .is_empty()
        );
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

        let (storage, mobile_storage, id_str) =
            setup_storage_with_terminal_instance(InstanceState::Completed).await;
        let notifier = MobileNotifier::new();

        // Scan before any listener is registered (marks the id for cleanup).
        assert_eq!(
            notifier
                .fire_terminal_events(&storage, &mobile_storage)
                .await,
            vec![id_str.clone()]
        );

        let listener = Arc::new(RecordingListener {
            completed: tokio::sync::Mutex::new(Vec::new()),
        });
        notifier.set_listener(listener.clone());

        let ids = notifier
            .fire_terminal_events(&storage, &mobile_storage)
            .await;
        assert!(ids.is_empty(), "cleanup must not repeat");

        // Callbacks are delivered via spawn_blocking — give them a moment.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(*listener.completed.lock().await, vec![id_str]);
    }

    #[tokio::test]
    async fn waiting_projection_delivers_handler_once() {
        assert_waiting_projection_delivers_handler_once(serde_json::json!([
            {"type": "step", "id": "review", "handler": "human_review", "params": {}}
        ]))
        .await;
    }

    #[tokio::test]
    async fn waiting_projection_delivers_nested_handler_once() {
        assert_waiting_projection_delivers_handler_once(serde_json::json!([
            {"type": "router", "id": "approval_route", "routes": [], "default": [
                {"type": "step", "id": "review", "handler": "human_review", "params": {}}
            ]}
        ]))
        .await;
    }

    async fn assert_waiting_projection_delivers_handler_once(blocks: serde_json::Value) {
        struct RecordingListener {
            pending: std::sync::Mutex<Vec<(String, String, String)>>,
            notified: tokio::sync::Notify,
        }
        impl EngineListener for RecordingListener {
            fn on_instance_completed(&self, _instance_id: String, _output: String) {}
            fn on_instance_failed(&self, _instance_id: String, _error: String) {}
            fn on_step_pending(&self, instance_id: String, step: String, handler: String) {
                self.pending
                    .lock()
                    .unwrap()
                    .push((instance_id, step, handler));
                self.notified.notify_one();
            }
        }

        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let storage: Arc<dyn StorageBackend> = sqlite.clone();
        let mobile_storage = MobileStorage::new(sqlite);
        let sequence: SequenceDefinition = serde_json::from_value(serde_json::json!({
            "id": SequenceId::new(),
            "tenant_id": "mobile",
            "namespace": "default",
            "name": "approval",
            "version": 1,
            "deprecated": false,
            "blocks": blocks,
            "created_at": chrono::Utc::now()
        }))
        .unwrap();
        storage.create_sequence(&sequence).await.unwrap();

        let now = chrono::Utc::now();
        let mut context = ExecutionContext::default();
        context.runtime.current_step = Some(BlockId::new("review"));
        let instance = TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: InstanceId::new(),
            sequence_id: sequence.id,
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: Namespace::new("default"),
            state: InstanceState::Waiting,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".to_string(),
            metadata: serde_json::json!({}),
            context,
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&instance).await.unwrap();

        let listener = Arc::new(RecordingListener {
            pending: std::sync::Mutex::new(Vec::new()),
            notified: tokio::sync::Notify::new(),
        });
        let notifier = MobileNotifier::new();
        notifier.set_listener(listener.clone());
        let cache = Arc::new(SequenceCache::new(4, std::time::Duration::from_secs(60)));

        notifier
            .fire_step_pending_events(&storage, &mobile_storage, &cache)
            .await;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            listener.notified.notified(),
        )
        .await
        .expect("waiting callback should complete");
        notifier
            .fire_step_pending_events(&storage, &mobile_storage, &cache)
            .await;

        assert_eq!(
            *listener.pending.lock().unwrap(),
            vec![(
                instance.id.to_string(),
                "review".to_string(),
                "human_review".to_string()
            )]
        );
    }
}
