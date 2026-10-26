//! Physical host invocations remain charged after their logical observer leaves.
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
}
impl Invocation {
    pub(crate) fn new(request: String, physical: OwnedSemaphorePermit) -> Self {
        Self {
            request,
            _physical: physical,
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
