//! Bounded best-effort retries owned by the backend that created the wake.
//! Child/parent durable state and park-then-recheck remain the recovery source;
//! this queue is process-local and does not promise restart durability.
use crate::backend_identity::BackendIdentity;
use chrono::Utc;
use orch8_storage::StorageBackend;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub(super) const RETRY: Duration = Duration::from_secs(1);
const MAX_AGE: Duration = Duration::from_secs(300);
const CAP: usize = 10_000;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct Key(BackendIdentity, InstanceId);

pub(super) struct Wake {
    first: Instant,
    next: Instant,
    generation: Arc<()>,
}
struct Registry {
    entries: HashMap<Key, Wake>,
    next_prune: Instant,
}
static STATE: LazyLock<Mutex<Registry>> = LazyLock::new(|| {
    Mutex::new(Registry {
        entries: HashMap::new(),
        next_prune: Instant::now(),
    })
});

pub(super) fn key(storage: &Arc<dyn StorageBackend>, parent: InstanceId) -> Key {
    Key(BackendIdentity::new(storage), parent)
}

fn prune(registry: &mut Registry, now: Instant) {
    if now < registry.next_prune && registry.entries.len() < CAP {
        return;
    }
    registry.next_prune = now + RETRY;
    let map = &mut registry.entries;
    let before = map.len();
    map.retain(|key, wake| key.0.is_live() && now.duration_since(wake.first) < MAX_AGE);
    for _ in map.len()..before {
        crate::metrics::inc(crate::metrics::DEFERRED_PARENT_WAKES_DROPPED);
    }
}

pub(super) fn insert_bounded(
    map: &mut HashMap<Key, Wake>,
    storage: &Arc<dyn StorageBackend>,
    parent: InstanceId,
    now: Instant,
    cap: usize,
) {
    let key = key(storage, parent);
    if map.len() >= cap
        && !map.contains_key(&key)
        && let Some(oldest) = map
            .iter()
            .min_by_key(|(_, wake)| wake.first)
            .map(|(key, _)| key.clone())
    {
        map.remove(&oldest);
        crate::metrics::inc(crate::metrics::DEFERRED_PARENT_WAKES_DROPPED);
    }
    let wake = map.entry(key).or_insert_with(|| Wake {
        first: now,
        next: now + RETRY,
        generation: Arc::new(()),
    });
    wake.next = now + RETRY;
    wake.generation = Arc::new(());
}

pub(super) fn defer(storage: &Arc<dyn StorageBackend>, parent: InstanceId) {
    let now = Instant::now();
    let mut registry = STATE.lock().unwrap_or_else(PoisonError::into_inner);
    prune(&mut registry, now);
    insert_bounded(&mut registry.entries, storage, parent, now, CAP);
}

pub(super) async fn retry(storage: &Arc<dyn StorageBackend>) {
    let now = Instant::now();
    let due: Vec<_> = {
        let mut registry = STATE.lock().unwrap_or_else(PoisonError::into_inner);
        prune(&mut registry, now);
        let owner = BackendIdentity::new(storage);
        registry
            .entries
            .iter()
            .filter(|(key, wake)| key.0 == owner && wake.next <= now)
            .map(|(key, wake)| (key.clone(), Arc::clone(&wake.generation)))
            .collect()
    };
    for (key, generation) in due {
        let parent = key.1;
        let keep = match storage.get_instance(parent).await {
            Ok(Some(instance)) if instance.state == InstanceState::Waiting => {
                match storage
                    .conditional_update_instance_state(
                        parent,
                        InstanceState::Waiting,
                        InstanceState::Scheduled,
                        Some(Utc::now()),
                    )
                    .await
                {
                    Ok(true) => {
                        info!(parent_id=%parent,"delivered deferred sub-sequence parent wake");
                        false
                    }
                    Ok(false) => true,
                    Err(error) => {
                        warn!(parent_id=%parent,%error,"deferred parent wake failed");
                        true
                    }
                }
            }
            Ok(Some(instance)) => matches!(
                instance.state,
                InstanceState::Running | InstanceState::Scheduled
            ),
            Ok(None) => false,
            Err(_) => true,
        };
        let mut registry = STATE.lock().unwrap_or_else(PoisonError::into_inner);
        let map = &mut registry.entries;
        // A new deferral can arrive while the storage read/CAS is in flight.
        // That event owns its own retry and cannot be removed by this result.
        if map
            .get(&key)
            .is_some_and(|wake| Arc::ptr_eq(&wake.generation, &generation))
        {
            if keep {
                if let Some(wake) = map.get_mut(&key) {
                    wake.next = Instant::now() + RETRY;
                }
            } else {
                map.remove(&key);
            }
        }
    }
}
