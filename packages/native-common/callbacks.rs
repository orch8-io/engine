//! Physical host invocations remain charged after their logical observer leaves.
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Default)]
pub(crate) struct Drain {
    state: Mutex<DrainState>,
    settled: Notify,
}

#[derive(Default)]
struct DrainState {
    closed: bool,
    active: usize,
}

impl Drain {
    pub(crate) fn enter(self: &Arc<Self>) -> Option<Active> {
        let mut state = super::lock(&self.state);
        if state.closed {
            return None;
        }
        state.active += 1;
        Some(Active(self.clone()))
    }

    pub(crate) fn close(&self) {
        super::lock(&self.state).closed = true;
    }

    pub(crate) async fn wait(&self) {
        loop {
            let settled = self.settled.notified();
            if super::lock(&self.state).active == 0 {
                return;
            }
            settled.await;
        }
    }
}

pub(crate) struct Active(Arc<Drain>);

impl Drop for Active {
    fn drop(&mut self) {
        let mut state = super::lock(&self.0.state);
        state.active -= 1;
        if state.active == 0 {
            self.0.settled.notify_waiters();
        }
    }
}

pub(crate) struct Budget {
    slots: Arc<Semaphore>,
}
impl Budget {
    pub(crate) fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<Budget>> = OnceLock::new();
        SHARED
            .get_or_init(|| {
                Self::new(
                    orch8_types::config::SchedulerConfig::default().max_concurrent_steps as usize,
                )
            })
            .clone()
    }
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(limit.max(1))),
        })
    }
    pub(crate) fn reserve(&self) -> Option<OwnedSemaphorePermit> {
        self.slots.clone().try_acquire_owned().ok()
    }
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.slots.available_permits()
    }
}

/// A binding must move this owner into the task that observes actual host
/// settlement, not leave it in the cancellable logical step future.
pub struct Invocation {
    request: String,
    _physical: OwnedSemaphorePermit,
    _active: Active,
}
impl Invocation {
    pub(crate) fn new(request: String, physical: OwnedSemaphorePermit, active: Active) -> Self {
        Self {
            request,
            _physical: physical,
            _active: active,
        }
    }
    /// Move the request out while retaining the physical admission owner.
    pub fn take_request(&mut self) -> String {
        std::mem::take(&mut self.request)
    }
}
impl std::ops::Deref for Invocation {
    type Target = str;
    fn deref(&self) -> &str {
        &self.request
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::poll_fn, task::Poll};

    #[tokio::test]
    async fn close_fences_entry_and_wakes_all_waiters_after_final_exit() {
        let drain = Arc::new(Drain::default());
        let first = drain.enter().unwrap();
        let second = drain.enter().unwrap();
        drain.close();
        assert!(drain.enter().is_none());

        let mut first_waiter = std::pin::pin!(drain.wait());
        let mut second_waiter = std::pin::pin!(drain.wait());
        poll_fn(|cx| {
            assert!(first_waiter.as_mut().poll(cx).is_pending());
            assert!(second_waiter.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(first);
        poll_fn(|cx| {
            assert!(first_waiter.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(second);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(first_waiter, second_waiter);
        })
        .await
        .unwrap();
        drain.wait().await;
    }
}
