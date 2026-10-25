//! Registration preflight owns references in the registry, not its async waiter.
use super::{
    Binding, Bindings, CatalogHandlers, CatalogScope, Stragglers, check_capacity, lock, retirement,
};
use crate::handlers::StepHandler;
use orch8_types::error::StepError;
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub(super) struct Pending<'a> {
    bindings: &'a Mutex<Bindings>,
    key: Option<usize>,
    epoch: Weak<AtomicBool>,
    _binding: Weak<Binding>,
    stop: tokio_util::sync::CancellationToken,
}
impl<'a> Pending<'a> {
    pub(super) fn new(
        owner: &'a CatalogHandlers,
        scope: &CatalogScope,
        name: &str,
        handler: Arc<dyn StepHandler>,
        timeout: Duration,
    ) -> Result<Self, StepError> {
        let mut bindings = lock(&owner.bindings);
        bindings.retiring.retain(|_, done| !done.finished());
        if owner.stop.is_cancelled() {
            return Err(StepError::ClaimLost);
        }
        let key = (scope.clone(), name.to_owned());
        check_capacity(&bindings, &key)?;
        let registration = owner.capacity.registrations.reserve(scope)?;
        let retirement = owner.capacity.executor()?;
        bindings.epochs.retain(|_, epoch| epoch.strong_count() != 0);
        let identity = (scope.account_id().to_owned(), scope.epoch());
        let epoch = bindings
            .epochs
            .get(&identity)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let epoch = Arc::new(AtomicBool::new(false));
                bindings.epochs.insert(identity, Arc::downgrade(&epoch));
                epoch
            });
        if epoch.load(Ordering::Acquire) {
            return Err(StepError::ClaimLost);
        }
        let account = owner.capacity.physical.account(scope)?;
        let fence = Arc::downgrade(&epoch);
        let binding = Arc::new(Binding {
            lifetime: owner.lifetime.clone(),
            handler,
            scope: scope.clone(),
            name: name.to_owned(),
            timeout,
            stragglers: Arc::new(Stragglers::default()),
            _epoch: epoch,
            account,
            retirement,
            _registration: Arc::new(registration),
        });
        // The weak binding reference below prevents address reuse until this
        // operation ends, even if shutdown has already retired the binding.
        let weak = Arc::downgrade(&binding);
        let operation = Arc::as_ptr(&binding) as usize;
        bindings.pending.insert(operation, (key, binding));
        Ok(Self {
            bindings: &owner.bindings,
            key: Some(operation),
            epoch: fence,
            _binding: weak,
            stop: owner.stop.clone(),
        })
    }
    pub(super) fn publish(mut self) -> Result<(), StepError> {
        let mut bindings = lock(self.bindings);
        if self.stop.is_cancelled()
            || self
                .epoch
                .upgrade()
                .is_none_or(|epoch| epoch.load(Ordering::Acquire))
        {
            return Err(StepError::ClaimLost);
        }
        let operation = self.key.ok_or(StepError::ClaimLost)?;
        let (key, binding) = bindings
            .pending
            .remove(&operation)
            .ok_or(StepError::ClaimLost)?;
        self.key = None;
        bindings.live.insert(key, binding);
        Ok(())
    }
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        let Some(operation) = self.key.take() else {
            return;
        };
        let retired = {
            let mut bindings = lock(self.bindings);
            bindings.pending.remove(&operation).map(|(key, binding)| {
                let completion = retirement::Completion::new();
                bindings.retiring.insert(key, completion.clone());
                (binding, completion)
            })
        };
        if let Some((binding, completion)) = retired {
            let scope = binding.scope.clone();
            let executor = binding.retirement.clone();
            executor.enqueue(retirement::Job::new(scope, vec![binding], completion, None));
        }
    }
}
