//! Local reference retirement remains separate from durable account retirement.
use super::{
    Arc, Binding, CatalogHandlers, CatalogScope, HashMap, HashSet, Ordering, StepError, lock,
    retirement, storage_error, unavailable,
};
use std::sync::Weak;

impl CatalogHandlers {
    /// Local runtime stop never retires the shared durable account epoch.
    pub(crate) fn shutdown(&self) {
        drop(self.close());
    }

    /// Transfer accepted references to owned retirement without async I/O.
    /// Repeated callers observe the same pending groups.
    pub(crate) fn close(&self) -> Vec<retirement::Receipt> {
        self.stop.cancel();
        let mut bindings = lock(&self.bindings);
        let mut groups: HashMap<_, Vec<Arc<Binding>>> = HashMap::new();
        let mut references: Vec<_> = bindings.live.drain().map(|(_, binding)| binding).collect();
        references.extend(bindings.pending.drain().map(|(_, (_, binding))| binding));
        for binding in references {
            groups
                .entry((binding.scope.account_id().to_owned(), binding.scope.epoch()))
                .or_default()
                .push(binding);
        }
        let mut jobs = Vec::new();
        for references in groups.into_values() {
            let first = &references[0];
            let completion = retirement::Completion::new();
            for binding in &references {
                bindings.retiring.insert(
                    (binding.scope.clone(), binding.name.clone()),
                    completion.clone(),
                );
            }
            let scope = first.scope.clone();
            let executor = first.retirement.clone();
            jobs.push((
                executor,
                retirement::Job::new(scope, references, completion, None),
            ));
        }
        let mut seen = HashSet::new();
        let receipts = bindings
            .retiring
            .values()
            .filter(|group| seen.insert(Arc::as_ptr(group)))
            .map(|group| group.receipt())
            .collect();
        drop(bindings);
        for (executor, job) in jobs {
            executor.enqueue(job);
        }
        receipts
    }

    /// Explicit retired-epoch cleanup keeps its existing busy/retry contract.
    /// Once admitted, cleanup is independent of this observer and Tokio runtime.
    pub(crate) async fn release_retired(
        self: &Arc<Self>,
        scope: &CatalogScope,
    ) -> Result<retirement::Receipt, StepError> {
        if !self
            .storage
            .catalog_epoch_retired(scope)
            .await
            .map_err(storage_error)?
        {
            return Err(unavailable());
        }
        let permit = self.capacity.physical.reserve_retirement(scope)?;
        Ok(self.detach_epoch(scope, Some(permit)))
    }

    /// Queue cleanup once a durable retirement is confirmed. A live callback
    /// retains its slot; the caller need not retry when that slot returns.
    pub(crate) async fn close_retired(
        &self,
        scope: &CatalogScope,
    ) -> Result<retirement::Receipt, StepError> {
        if !self
            .storage
            .catalog_epoch_retired(scope)
            .await
            .map_err(storage_error)?
        {
            return Err(unavailable());
        }
        Ok(self.detach_epoch(scope, None))
    }

    fn detach_epoch(
        &self,
        scope: &CatalogScope,
        permit: Option<super::NativePermit>,
    ) -> retirement::Receipt {
        let mut bindings = lock(&self.bindings);
        bindings.retiring.retain(|_, done| !done.finished());
        if let Some(epoch) = bindings
            .epochs
            .get(&(scope.account_id().to_owned(), scope.epoch()))
            .and_then(Weak::upgrade)
        {
            epoch.store(true, Ordering::Release);
        }
        let keys: Vec<_> = bindings
            .live
            .keys()
            .filter(|(owner, _)| {
                owner.account_id() == scope.account_id() && owner.epoch() == scope.epoch()
            })
            .cloned()
            .collect();
        let completion = retirement::Completion::new();
        let mut detached = Vec::new();
        for key in keys {
            if let Some(binding) = bindings.live.remove(&key) {
                detached.push(binding);
                bindings.retiring.insert(key, completion.clone());
            }
        }
        let pending_keys: Vec<_> = bindings
            .pending
            .iter()
            .filter(|(_, ((owner, _), _))| {
                owner.account_id() == scope.account_id() && owner.epoch() == scope.epoch()
            })
            .map(|(key, _)| *key)
            .collect();
        for operation in pending_keys {
            if let Some((key, binding)) = bindings.pending.remove(&operation) {
                detached.push(binding);
                bindings.retiring.insert(key, completion.clone());
            }
        }
        let existing = if detached.is_empty() {
            bindings
                .retiring
                .iter()
                .find(|((owner, _), _)| {
                    owner.account_id() == scope.account_id() && owner.epoch() == scope.epoch()
                })
                .map(|(_, done)| done.receipt())
        } else {
            None
        };
        drop(bindings);
        if let Some(receipt) = existing {
            drop(permit);
            return receipt;
        }
        let receipt = completion.receipt();
        if let Some(first) = detached.first() {
            let executor = first.retirement.clone();
            executor.enqueue(retirement::Job::new(
                scope.clone(),
                detached,
                completion,
                permit,
            ));
        } else {
            drop(permit);
            completion.finish();
        }
        receipt
    }
}

impl Drop for CatalogHandlers {
    fn drop(&mut self) {
        // close only holds a short metadata mutex; it performs no storage I/O
        // and transfers every foreign reference before ordinary field drop.
        drop(self.close());
    }
}
