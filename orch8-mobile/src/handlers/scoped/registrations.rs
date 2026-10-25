//! Process-owned registration charges survive router and login replacement.
use crate::storage::scopes::CatalogScope;
use orch8_types::error::StepError;
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) const GLOBAL_LIMIT: usize = 256;
pub(super) const ACCOUNT_LIMIT: usize = 64;
pub(super) const CATALOG_LIMIT: usize = 32;

fn busy() -> StepError {
    StepError::Retryable {
        message: "scoped native registration capacity is occupied".into(),
        details: None,
    }
}

#[derive(Default)]
struct Owners {
    accounts: HashMap<String, Weak<Semaphore>>,
    catalogs: HashMap<(String, String), Weak<Semaphore>>,
}

pub(super) struct Capacity {
    global: Arc<Semaphore>,
    owners: Mutex<Owners>,
}
impl Capacity {
    pub(super) fn new() -> Self {
        Self {
            global: Arc::new(Semaphore::new(GLOBAL_LIMIT)),
            owners: Mutex::new(Owners::default()),
        }
    }
    pub(super) fn reserve(&self, scope: &CatalogScope) -> Result<Permit, StepError> {
        // Reserve global capacity first. Even concurrent unsuccessful owners
        // cannot grow the account/catalog gate maps past this physical bound.
        let global = self
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let (account, catalog) = {
            let mut owners = self
                .owners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            owners.accounts.retain(|_, gate| gate.strong_count() != 0);
            owners.catalogs.retain(|_, gate| gate.strong_count() != 0);
            let account = gate(
                &mut owners.accounts,
                scope.account_id().to_owned(),
                ACCOUNT_LIMIT,
            );
            let catalog = gate(
                &mut owners.catalogs,
                (scope.account_id().to_owned(), scope.catalog_id().to_owned()),
                CATALOG_LIMIT,
            );
            (account, catalog)
        };
        let account = account.try_acquire_owned().map_err(|_| busy())?;
        let catalog = catalog.try_acquire_owned().map_err(|_| busy())?;
        Ok(Permit {
            _catalog: catalog,
            _account: account,
            _global: global,
        })
    }
}

fn gate<K: Eq + Hash>(
    gates: &mut HashMap<K, Weak<Semaphore>>,
    key: K,
    limit: usize,
) -> Arc<Semaphore> {
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(Semaphore::new(limit));
    gates.insert(key, Arc::downgrade(&gate));
    gate
}

// Account/catalog keys omit the epoch. The owning Binding declares this after
// the foreign object, retaining its charges through final host destruction.
pub(super) struct Permit {
    _catalog: OwnedSemaphorePermit,
    _account: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}
