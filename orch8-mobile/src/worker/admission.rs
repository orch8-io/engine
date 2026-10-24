//! Admission shared by every worker generation of one mobile engine.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::claims::ClaimKey;

#[derive(Clone, PartialEq, Eq, Hash)]
struct TaskScope {
    api_base: String,
    worker_id: String,
    task_id: uuid::Uuid,
}

impl From<&ClaimKey> for TaskScope {
    fn from(key: &ClaimKey) -> Self {
        Self {
            api_base: key.api_base.clone(),
            worker_id: key.worker_id.clone(),
            task_id: key.task_id,
        }
    }
}

struct Active {
    epoch: u64,
    cancel: CancellationToken,
}

#[derive(Default)]
struct State {
    limit: usize,
    active: HashMap<TaskScope, Active>,
    releasing: HashMap<TaskScope, BTreeSet<u64>>,
}

pub(super) struct Admission {
    state: Mutex<State>,
    pub wake: Arc<Notify>,
    pub release_slots: tokio::sync::Semaphore,
    pub poll_owner: tokio::sync::Mutex<()>,
}

impl Default for Admission {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            wake: Arc::default(),
            release_slots: tokio::sync::Semaphore::new(8),
            poll_owner: tokio::sync::Mutex::new(()),
        }
    }
}

pub(super) enum Decision {
    Execute(Execution),
    Release {
        guard: Releasing,
        superseded: Option<CancellationToken>,
    },
    Ignore,
}

pub(super) struct Execution {
    scope: TaskScope,
    epoch: u64,
    cancel: CancellationToken,
    owner: Arc<Admission>,
}

impl Execution {
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        let mut state = self.owner.lock();
        if state
            .active
            .get(&self.scope)
            .is_some_and(|active| active.epoch == self.epoch)
        {
            state.active.remove(&self.scope);
        }
        drop(state);
        self.owner.wake.notify_waiters();
        self.owner.wake.notify_one();
    }
}

pub(super) struct Releasing {
    scope: TaskScope,
    epoch: u64,
    owner: Arc<Admission>,
}

impl Drop for Releasing {
    fn drop(&mut self) {
        let mut state = self.owner.lock();
        if let Some(epochs) = state.releasing.get_mut(&self.scope) {
            epochs.remove(&self.epoch);
            if epochs.is_empty() {
                state.releasing.remove(&self.scope);
            }
        }
        drop(state);
        self.owner.wake.notify_waiters();
        self.owner.wake.notify_one();
    }
}

impl Admission {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A successor's configured budget includes its draining predecessors.
    /// Shrinking never aborts already admitted work; expanding adds capacity.
    pub fn configure(&self, limit: u32) {
        self.lock().limit = limit.clamp(1, 64) as usize;
        self.wake.notify_waiters();
    }

    pub fn available(&self) -> usize {
        let state = self.lock();
        if state.releasing.is_empty() {
            state.limit.saturating_sub(state.active.len())
        } else {
            // Do not accumulate another response batch while unstarted
            // releases from the last one are waiting or in flight.
            0
        }
    }

    pub fn in_flight(&self) -> usize {
        self.lock().active.len()
    }

    pub fn admit(self: &Arc<Self>, key: &ClaimKey) -> Decision {
        let scope = TaskScope::from(key);
        let epoch = key.claim_epoch;
        let mut state = self.lock();
        if state
            .releasing
            .get(&scope)
            .and_then(BTreeSet::last)
            .is_some_and(|known| epoch <= *known)
        {
            return Decision::Ignore;
        }
        let superseded = if let Some(active) = state.active.get(&scope) {
            if epoch <= active.epoch {
                return Decision::Ignore;
            }
            Some(active.cancel.clone())
        } else {
            None
        };
        if superseded.is_none() && state.active.len() < state.limit {
            let cancel = CancellationToken::new();
            state.active.insert(
                scope.clone(),
                Active {
                    epoch,
                    cancel: cancel.clone(),
                },
            );
            return Decision::Execute(Execution {
                scope,
                epoch,
                cancel,
                owner: Arc::clone(self),
            });
        }
        state
            .releasing
            .entry(scope.clone())
            .or_default()
            .insert(epoch);
        Decision::Release {
            guard: Releasing {
                scope,
                epoch,
                owner: Arc::clone(self),
            },
            superseded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ClaimKey {
        ClaimKey {
            task_id: uuid::Uuid::new_v4(),
            claim_epoch: 7,
            worker_id: "phone".into(),
            api_base: "https://control.example/api/v1".into(),
        }
    }

    #[test]
    fn resize_counts_predecessors_without_aborting_them() {
        let admission = Arc::new(Admission::default());
        admission.configure(2);
        let Decision::Execute(first) = admission.admit(&key()) else {
            panic!("first slot")
        };
        let Decision::Execute(second) = admission.admit(&key()) else {
            panic!("second slot")
        };
        admission.configure(1);
        assert_eq!(admission.available(), 0);
        assert!(!first.cancellation().is_cancelled());
        drop(first);
        assert_eq!(admission.available(), 0);
        drop(second);
        assert_eq!(admission.available(), 1);
        admission.configure(4);
        assert_eq!(admission.available(), 4);
    }

    #[test]
    fn release_reservation_survives_predecessor_completion() {
        let admission = Arc::new(Admission::default());
        admission.configure(1);
        let mut key = key();
        let Decision::Execute(first) = admission.admit(&key) else {
            panic!("first slot")
        };
        assert!(matches!(admission.admit(&key), Decision::Ignore));
        key.claim_epoch += 1;
        let Decision::Release { guard, superseded } = admission.admit(&key) else {
            panic!("must defer newer epoch")
        };
        superseded.unwrap().cancel();
        assert!(first.cancellation().is_cancelled());
        drop(first);
        assert!(matches!(admission.admit(&key), Decision::Ignore));
        drop(guard);
        assert!(matches!(admission.admit(&key), Decision::Execute(_)));
    }
}
