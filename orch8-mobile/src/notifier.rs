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
}

impl MobileNotifier {
    pub fn new() -> Self {
        Self {
            listener: Mutex::new(None),
            terminal_seen: Mutex::new(HashSet::new()),
            waiting_seen: Mutex::new(HashSet::new()),
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
    /// caller can clean up dedup keys in the lifecycle manager.
    pub async fn fire_terminal_events(&self, storage: &Arc<dyn StorageBackend>) -> Vec<String> {
        let Some(listener) = self.listener().await else {
            return Vec::new();
        };

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
            if !seen.insert(id_str.clone()) {
                continue;
            }

            match inst.state {
                InstanceState::Completed => {
                    let output = serde_json::to_string(&inst.context.data).unwrap_or_default();
                    debug!(instance_id = %id_str, "firing on_instance_completed");
                    let cb = Arc::clone(&listener);
                    let id = id_str.clone();
                    tokio::task::spawn_blocking(move || cb.on_instance_completed(id, output));
                }
                InstanceState::Failed => {
                    let error = serde_json::to_string(&inst.context.data).unwrap_or_default();
                    let cb = Arc::clone(&listener);
                    let id = id_str.clone();
                    tokio::task::spawn_blocking(move || cb.on_instance_failed(id, error));
                }
                _ => {}
            }

            terminal_ids.push(id_str);
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

    #[tokio::test]
    async fn notifier_starts_without_listener() {
        let notifier = MobileNotifier::new();
        assert!(notifier.listener().await.is_none());
    }

    #[test]
    fn max_tracked_is_reasonable() {
        assert_eq!(MAX_TRACKED, 1000);
    }
}
