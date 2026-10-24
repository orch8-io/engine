//! In-process fencing for one executor's live callbacks and unstarted releases.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    id: uuid::Uuid,
    epoch: u64,
}

struct Running {
    epoch: u64,
    cancel: CancellationToken,
}

#[derive(Default)]
struct State {
    running: HashMap<uuid::Uuid, Running>,
    releasing: HashMap<uuid::Uuid, BTreeSet<u64>>,
}

type Shared = Arc<Mutex<State>>;

#[derive(Default)]
pub(super) struct Claims {
    state: Shared,
}

pub(super) enum Admission {
    Execute(Execution),
    Release {
        guard: Release,
        superseded: Option<CancellationToken>,
    },
    Ignore,
}

pub(super) struct Execution {
    key: Key,
    state: Shared,
    cancel: CancellationToken,
    permit: Option<OwnedSemaphorePermit>,
}

impl Execution {
    pub(super) fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        drop(self.permit.take());
        if state
            .running
            .get(&self.key.id)
            .is_some_and(|claim| claim.epoch == self.key.epoch)
        {
            state.running.remove(&self.key.id);
        }
    }
}

pub(super) struct Release {
    key: Key,
    state: Shared,
}

impl Drop for Release {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        if let Some(epochs) = state.releasing.get_mut(&self.key.id) {
            epochs.remove(&self.key.epoch);
            if epochs.is_empty() {
                state.releasing.remove(&self.key.id);
            }
        }
    }
}

impl Claims {
    pub(super) fn in_flight(&self) -> usize {
        lock(&self.state).running.len()
    }

    pub(super) fn cancellations(&self) -> Vec<CancellationToken> {
        lock(&self.state)
            .running
            .values()
            .map(|claim| claim.cancel.clone())
            .collect()
    }

    pub(super) fn admit(&self, id: uuid::Uuid, epoch: u64, slots: &Arc<Semaphore>) -> Admission {
        let key = Key { id, epoch };
        let mut state = lock(&self.state);
        if state
            .releasing
            .get(&id)
            .and_then(BTreeSet::last)
            .is_some_and(|known| epoch <= *known)
        {
            return Admission::Ignore;
        }
        let superseded = if let Some(active) = state.running.get(&id) {
            if epoch <= active.epoch {
                return Admission::Ignore;
            }
            Some(active.cancel.clone())
        } else {
            None
        };
        if superseded.is_none() {
            // Slot acquisition and claim reservation are atomic relative to
            // duplicate admission. A full-slot duplicate cannot release the
            // original between acquiring its permit and starting its task.
            if let Ok(permit) = Arc::clone(slots).try_acquire_owned() {
                let cancel = CancellationToken::new();
                state.running.insert(
                    id,
                    Running {
                        epoch,
                        cancel: cancel.clone(),
                    },
                );
                return Admission::Execute(Execution {
                    key,
                    state: Arc::clone(&self.state),
                    cancel,
                    permit: Some(permit),
                });
            }
        }
        // Keep the exact release generation reserved until its request ends.
        // Otherwise another delivery could start before a delayed release
        // revokes that very same lease. The predecessor stays independently
        // tracked until its callback/settlement actually exits.
        state.releasing.entry(id).or_default().insert(epoch);
        Admission::Release {
            guard: Release {
                key,
                state: Arc::clone(&self.state),
            },
            superseded,
        }
    }
}

fn lock(state: &Shared) -> std::sync::MutexGuard<'_, State> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_release_blocks_redelivery_even_after_capacity_returns() {
        let claims = Claims::default();
        let slots = Arc::new(Semaphore::new(0));
        let id = uuid::Uuid::new_v4();
        let Admission::Release { guard, .. } = claims.admit(id, 7, &slots) else {
            panic!("expected an unstarted release")
        };
        slots.add_permits(1);
        assert!(matches!(claims.admit(id, 7, &slots), Admission::Ignore));
        drop(guard);
        let Admission::Execute(active) = claims.admit(id, 7, &slots) else {
            panic!("release completion must free its generation")
        };
        drop(active);
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(claims.in_flight(), 0);
    }

    #[test]
    fn predecessor_completion_cannot_clear_a_replacement_release() {
        let claims = Claims::default();
        let slots = Arc::new(Semaphore::new(1));
        let id = uuid::Uuid::new_v4();
        let Admission::Execute(old) = claims.admit(id, 7, &slots) else {
            panic!("expected a callback reservation")
        };
        let Admission::Release { guard, superseded } = claims.admit(id, 8, &slots) else {
            panic!("new generation must wait")
        };
        superseded.unwrap().cancel();
        assert!(old.cancellation().is_cancelled());
        drop(old);
        assert!(matches!(claims.admit(id, 8, &slots), Admission::Ignore));
        drop(guard);
        assert!(matches!(claims.admit(id, 8, &slots), Admission::Execute(_)));
    }

    #[test]
    fn a_pending_newer_release_fences_older_generations() {
        let claims = Claims::default();
        let slots = Arc::new(Semaphore::new(0));
        let id = uuid::Uuid::new_v4();
        let Admission::Release { guard, .. } = claims.admit(id, 8, &slots) else {
            panic!("expected a release reservation")
        };
        slots.add_permits(1);
        assert!(matches!(claims.admit(id, 7, &slots), Admission::Ignore));
        let Admission::Execute(newer) = claims.admit(id, 9, &slots) else {
            panic!("a fenced old release cannot block a newer lease")
        };
        drop(guard);
        assert_eq!(claims.in_flight(), 1);
        drop(newer);
        assert_eq!(claims.in_flight(), 0);
    }
}
