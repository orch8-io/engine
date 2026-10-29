//! Admission shared by every worker generation of one mobile engine.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::claims::ClaimKey;

#[derive(Clone, PartialEq, Eq, Hash)]
struct TaskScope {
    api_base: String,
    task_id: uuid::Uuid,
}

impl From<&ClaimKey> for TaskScope {
    fn from(key: &ClaimKey) -> Self {
        Self {
            api_base: key.api_base.clone(),
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
    poll_after: Option<Instant>,
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
        if state.releasing.is_empty()
            && state.poll_after.is_none_or(|until| until <= Instant::now())
        {
            state.limit.saturating_sub(state.active.len())
        } else {
            // Do not accumulate another response batch while unstarted
            // releases from the last one are waiting or in flight.
            0
        }
    }

    /// Pace new polls after physical contention. Already delivered tasks still
    /// use normal admission, so unrelated work need not wait for the timer.
    pub fn defer_poll(&self, delay: Duration) {
        let until = Instant::now() + delay.min(Duration::from_secs(30));
        {
            let mut state = self.lock();
            state.poll_after = Some(state.poll_after.map_or(until, |prior| prior.max(until)));
        }
        self.wake.notify_waiters();
        self.wake.notify_one();
    }

    pub fn poll_delay(&self) -> Duration {
        self.lock().poll_after.map_or(Duration::ZERO, |until| {
            until.saturating_duration_since(Instant::now())
        })
    }

    pub fn in_flight(&self) -> usize {
        self.lock().active.len()
    }

    pub fn admit(self: &Arc<Self>, key: &ClaimKey) -> Decision {
        // A changed runtime/worker identity does not create another physical
        // task. Keep reporter identity in ClaimKey for journaling and transport,
        // while serializing generations of one control-plane task here.
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

    #[tokio::test(start_paused = true)]
    async fn contention_paces_polls_without_aborting_or_refusing_delivered_work() {
        let admission = Arc::new(Admission::default());
        admission.configure(2);
        let Decision::Execute(first) = admission.admit(&key()) else {
            panic!("first claim")
        };
        admission.defer_poll(Duration::from_secs(1));
        assert_eq!(admission.available(), 0);
        let Decision::Execute(second) = admission.admit(&key()) else {
            panic!("already delivered independent work must remain admissible")
        };
        assert!(!first.cancellation().is_cancelled());
        drop(second);
        admission.configure(3);
        assert_eq!(admission.available(), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(admission.available(), 2);
        drop(first);
        assert_eq!(admission.available(), 3);
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

    #[test]
    fn changing_worker_identity_cannot_overlap_the_same_remote_task() {
        let admission = Arc::new(Admission::default());
        admission.configure(2);
        let mut key = key();
        let Decision::Execute(first) = admission.admit(&key) else {
            panic!("first claim must start")
        };
        key.worker_id = "replacement-phone".into();
        assert!(matches!(admission.admit(&key), Decision::Ignore));
        key.claim_epoch += 1;
        let Decision::Release { guard, superseded } = admission.admit(&key) else {
            panic!("a changed worker ID must not overlap the same remote task")
        };
        superseded
            .expect("the predecessor must be cancelled")
            .cancel();
        assert!(first.cancellation().is_cancelled());
        assert_eq!(admission.in_flight(), 1);
        drop(first);
        assert!(matches!(admission.admit(&key), Decision::Ignore));
        drop(guard);
        assert!(matches!(admission.admit(&key), Decision::Execute(_)));
    }

    #[test]
    fn different_control_planes_keep_independent_task_identities() {
        let admission = Arc::new(Admission::default());
        admission.configure(2);
        let mut key = key();
        let Decision::Execute(first) = admission.admit(&key) else {
            panic!("first control plane")
        };
        key.api_base = "https://other.example/api/v1".into();
        let Decision::Execute(second) = admission.admit(&key) else {
            panic!("independent control plane")
        };
        assert_eq!(admission.in_flight(), 2);
        assert!(!first.cancellation().is_cancelled());
        assert!(!second.cancellation().is_cancelled());
    }
}
