//! Physical calls and retirement share one stable-account/process budget.
use super::{CatalogScope, busy};
use std::{
    collections::HashMap,
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Default)]
pub(super) struct Signal {
    version: Mutex<u64>,
    changed: Condvar,
}
impl Signal {
    pub(super) fn version(&self) -> u64 {
        *self
            .version
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn poke(&self) {
        let mut version = self
            .version
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *version = version.wrapping_add(1);
        self.changed.notify_all();
    }
    pub(super) fn wait(&self, observed: u64) {
        let mut version = self
            .version
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *version == observed {
            version = self
                .changed
                .wait(version)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

pub(super) struct Budget {
    global: Arc<Semaphore>,
    accounts: Mutex<HashMap<String, Weak<Account>>>,
    pub(super) wake: Arc<Signal>,
}
impl Budget {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            global: Arc::new(Semaphore::new(2)),
            accounts: Mutex::new(HashMap::new()),
            wake: Arc::new(Signal::default()),
        })
    }
    pub(super) fn account(
        &self,
        scope: &CatalogScope,
    ) -> Result<Arc<Account>, orch8_types::error::StepError> {
        let mut accounts = self
            .accounts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        accounts.retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = accounts.get(scope.account_id()).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        if accounts.len() >= 1024 {
            return Err(busy());
        }
        let gate = Arc::new(Account {
            slot: Arc::new(Semaphore::new(1)),
            retiring: AtomicUsize::new(0),
        });
        accounts.insert(scope.account_id().into(), Arc::downgrade(&gate));
        Ok(gate)
    }
    pub(super) fn reserve(
        &self,
        scope: &CatalogScope,
    ) -> Result<Permit, orch8_types::error::StepError> {
        self.reserve_inner(scope, false)
    }
    pub(super) fn reserve_retirement(
        &self,
        scope: &CatalogScope,
    ) -> Result<Permit, orch8_types::error::StepError> {
        self.reserve_inner(scope, true)
    }
    fn reserve_inner(
        &self,
        scope: &CatalogScope,
        retirement: bool,
    ) -> Result<Permit, orch8_types::error::StepError> {
        let gate = self.account(scope)?;
        if !retirement {
            if gate.retiring.load(Ordering::Acquire) != 0 {
                return Err(busy());
            }
            // A ready retirement has priority over new callbacks. An account
            // held by live host code cannot prevent other accounts proceeding.
            if self
                .accounts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .filter_map(Weak::upgrade)
                .any(|gate| {
                    gate.retiring.load(Ordering::Acquire) != 0 && gate.slot.available_permits() != 0
                })
            {
                return Err(busy());
            }
        }
        // Only normal callback failures signal. A worker's failed retirement
        // scan must not wake itself into a busy loop while host code is held.
        let mut attempt = Attempt((!retirement).then(|| self.wake.clone()));
        let global = self
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let account = gate.slot.clone().try_acquire_owned().map_err(|_| busy())?;
        attempt.0 = None;
        Ok(Permit {
            _scope: scope.clone(),
            _gate: gate,
            _account: account,
            _global: global,
            _released: Released(self.wake.clone()),
        })
    }
    #[cfg(test)]
    pub(super) fn account_gate(&self, scope: &CatalogScope) -> Option<Arc<Semaphore>> {
        self.accounts
            .lock()
            .unwrap()
            .get(scope.account_id())
            .and_then(Weak::upgrade)
            .map(|gate| gate.slot.clone())
    }
}

// Field order releases capacity before waking a retirement waiter.
pub(crate) struct Permit {
    _scope: CatalogScope,
    _account: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    _gate: Arc<Account>,
    _released: Released,
}
struct Released(Arc<Signal>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.poke();
    }
}

pub(super) struct Account {
    slot: Arc<Semaphore>,
    retiring: AtomicUsize,
}
impl Account {
    pub(super) fn retire(self: &Arc<Self>, wake: Arc<Signal>) -> Priority {
        self.retiring.fetch_add(1, Ordering::AcqRel);
        Priority {
            account: self.clone(),
            wake,
        }
    }
}
pub(super) struct Priority {
    account: Arc<Account>,
    wake: Arc<Signal>,
}
impl Drop for Priority {
    fn drop(&mut self) {
        self.account.retiring.fetch_sub(1, Ordering::AcqRel);
        self.wake.poke();
    }
}
struct Attempt(Option<Arc<Signal>>);
impl Drop for Attempt {
    fn drop(&mut self) {
        if let Some(wake) = &self.0 {
            wake.poke();
        }
    }
}
