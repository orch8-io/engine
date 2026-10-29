//! An observer cannot renew or extend a supervisor's authority.
use super::LeaseEnd;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::time::Instant;

#[derive(Clone, Copy)]
enum State {
    Pending(Instant),
    Live(Instant),
    Ended(LeaseEnd),
}
/// Read-only monotonic authority for work owned by a running lease supervisor.
/// It is not bearer authentication, account ownership or a task identity proof.
/// Clones do not retain execution authority after supervision ends. Unconfirmed
/// also means supervision has not started, finished normally, or was dropped.
#[derive(Clone)]
pub struct LeaseAuthority {
    state: Arc<Mutex<State>>,
}
impl std::fmt::Debug for LeaseAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseAuthority").finish_non_exhaustive()
    }
}
impl LeaseAuthority {
    pub(super) fn new(until: Instant) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::Pending(until))),
        }
    }
    /// Refuse expired or unsupervised work even when its watcher has not been
    /// polled. Expiration is permanent; a delayed renewal cannot revive it.
    ///
    /// # Errors
    /// Returns why physical provider entry is no longer authorized by this lease.
    pub fn check(&self) -> Result<(), LeaseEnd> {
        let mut state = self.state.lock().map_err(|_| LeaseEnd::Unconfirmed)?;
        match *state {
            State::Pending(until) | State::Live(until) if Instant::now() >= until => {
                *state = State::Ended(LeaseEnd::Expired);
                Err(LeaseEnd::Expired)
            }
            State::Live(_) => Ok(()),
            State::Pending(_) => Err(LeaseEnd::Unconfirmed),
            State::Ended(end) => Err(end),
        }
    }
    pub(super) fn activate(&self) -> Result<(), LeaseEnd> {
        let mut state = self.state.lock().map_err(|_| LeaseEnd::Unconfirmed)?;
        match *state {
            State::Pending(until) if Instant::now() < until => {
                *state = State::Live(until);
                Ok(())
            }
            State::Pending(_) => {
                *state = State::Ended(LeaseEnd::Expired);
                Err(LeaseEnd::Expired)
            }
            State::Live(_) => Err(LeaseEnd::Unconfirmed),
            State::Ended(end) => Err(end),
        }
    }
    pub(super) fn renew(&self, until: Instant) -> Result<(), LeaseEnd> {
        let mut state = self.state.lock().map_err(|_| LeaseEnd::Unconfirmed)?;
        match *state {
            State::Live(prior) if Instant::now() < prior && Instant::now() < until => {
                *state = State::Live(until);
                Ok(())
            }
            State::Live(_) => {
                *state = State::Ended(LeaseEnd::Expired);
                Err(LeaseEnd::Expired)
            }
            State::Pending(_) => Err(LeaseEnd::Unconfirmed),
            State::Ended(end) => Err(end),
        }
    }
    pub(super) fn revoke(&self, end: LeaseEnd) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, State::Ended(_)) {
            *state = State::Ended(end);
        }
    }
}
/// Revocation precedes destruction of both operation and renewal futures.
/// This also handles cancellation/panic, without relying on select drop order.
pub(super) struct RevokeOnDrop<F: Future> {
    future: Pin<Box<F>>,
    authority: LeaseAuthority,
}
impl<F: Future> RevokeOnDrop<F> {
    pub(super) fn new(future: F, authority: LeaseAuthority) -> Self {
        Self {
            future: Box::pin(future),
            authority,
        }
    }
}
impl<F: Future> Future for RevokeOnDrop<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
impl<F: Future> Drop for RevokeOnDrop<F> {
    fn drop(&mut self) {
        self.authority.revoke(LeaseEnd::Unconfirmed);
    }
}
#[cfg(test)]
#[path = "authority/tests.rs"]
mod tests;
