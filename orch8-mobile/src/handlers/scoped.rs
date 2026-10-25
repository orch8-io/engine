//! Internal frozen-owner native routing. No production scoped materializer yet.
use super::{
    Callback, HandlerRegistry, StepContext, StepHandler, Stragglers, invoke_foreign_handler,
};
use crate::storage::{MobileStorage, scopes::CatalogScope};
use orch8_storage::StorageBackend;
use orch8_types::error::StepError;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

fn busy() -> StepError {
    StepError::Retryable {
        message: "scoped native callback capacity is occupied".into(),
        details: None,
    }
}
fn unavailable() -> StepError {
    StepError::Permanent {
        message: "scoped native handler is unavailable".into(),
        details: None,
    }
}
fn storage_error(error: orch8_types::error::StorageError) -> StepError {
    if error.is_transient() {
        StepError::Retryable {
            message: "scoped native routing unavailable".into(),
            details: None,
        }
    } else {
        StepError::ClaimLost
    }
}

struct NativeCapacity {
    global: Arc<Semaphore>,
    accounts: Mutex<HashMap<String, Weak<Semaphore>>>,
}
impl NativeCapacity {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            global: Arc::new(Semaphore::new(2)),
            accounts: Mutex::new(HashMap::new()),
        })
    }
    fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<NativeCapacity>> = OnceLock::new();
        SHARED.get_or_init(Self::new).clone()
    }
    fn reserve(&self, scope: &CatalogScope) -> Result<NativePermit, StepError> {
        let gate = {
            let mut accounts = self
                .accounts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            accounts.retain(|_, gate| gate.strong_count() != 0);
            if let Some(gate) = accounts.get(scope.account_id()).and_then(Weak::upgrade) {
                gate
            } else {
                if accounts.len() >= 1024 {
                    return Err(busy());
                }
                let gate = Arc::new(Semaphore::new(1));
                accounts.insert(scope.account_id().into(), Arc::downgrade(&gate));
                gate
            }
        };
        let account = gate.try_acquire_owned().map_err(|_| busy())?;
        let global = self
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        Ok(NativePermit {
            _scope: scope.clone(),
            _account: account,
            _global: global,
        })
    }
}

/// Owned by the physical closure, including queued cancellation and preflight.
/// Account keys omit the login epoch so retirement cannot reclaim a live slot.
pub(super) struct NativePermit {
    _scope: CatalogScope,
    _account: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

pub(super) struct Binding {
    // Foreign object drops before its plain metadata; never under the map lock.
    handler: Arc<dyn StepHandler>,
    scope: CatalogScope,
    name: String,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
}

pub(crate) struct CatalogHandlers {
    storage: Arc<MobileStorage>,
    backend: Arc<dyn StorageBackend>,
    bindings: Mutex<HashMap<(CatalogScope, String), Arc<Binding>>>,
    capacity: Arc<NativeCapacity>,
}
impl CatalogHandlers {
    /// Trusted runtime supplies the same backend used by its step executor.
    /// Decorators are allowed, but a context from another backend is refused.
    pub(crate) fn new(storage: Arc<MobileStorage>, backend: Arc<dyn StorageBackend>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            backend,
            bindings: Mutex::new(HashMap::new()),
            capacity: NativeCapacity::shared(),
        })
    }
    #[cfg(test)]
    pub(crate) fn isolated(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
    ) -> Arc<Self> {
        let mut owner = Self::new(storage, backend);
        Arc::get_mut(&mut owner).unwrap().capacity = NativeCapacity::new();
        owner
    }
    #[cfg(test)]
    pub(crate) fn account_gate(&self, scope: &CatalogScope) -> Option<Arc<Semaphore>> {
        self.capacity
            .accounts
            .lock()
            .unwrap()
            .get(scope.account_id())
            .and_then(Weak::upgrade)
    }

    pub(crate) async fn bind(
        &self,
        scope: &CatalogScope,
        name: &str,
        handler: Arc<dyn StepHandler>,
        timeout: Duration,
    ) -> Result<(), StepError> {
        if name.trim().is_empty()
            || name.len() > 256
            || name.chars().any(char::is_control)
            || timeout.is_zero()
        {
            return Err(unavailable());
        }
        self.storage
            .catalog_metadata(scope, "native_handler_binding")
            .await
            .map_err(storage_error)?;
        let key = (scope.clone(), name.to_owned());
        let mut bindings = self
            .bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bindings.contains_key(&key) {
            return Err(unavailable());
        }
        // Retired entries stay charged; cleanup is explicit runtime integration.
        if bindings.len() >= 256
            || bindings
                .keys()
                .filter(|(owner, _)| owner.account_id() == scope.account_id())
                .count()
                >= 64
            || bindings
                .keys()
                .filter(|(owner, _)| {
                    owner.catalog_id() == scope.catalog_id()
                        && owner.account_id() == scope.account_id()
                })
                .count()
                >= 32
        {
            return Err(busy());
        }
        bindings.insert(
            key,
            Arc::new(Binding {
                handler,
                scope: scope.clone(),
                name: name.into(),
                timeout,
                stragglers: Arc::new(Stragglers::default()),
            }),
        );
        Ok(())
    }
    pub(crate) async fn names(&self, scope: &CatalogScope) -> Result<HashSet<String>, StepError> {
        self.storage
            .catalog_metadata(scope, "native_handler_binding")
            .await
            .map_err(storage_error)?;
        Ok(self
            .bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .filter(|(owner, _)| owner == scope)
            .map(|(_, name)| name.clone())
            .collect())
    }
    pub(crate) fn register(self: &Arc<Self>, registry: &mut HandlerRegistry, name: &str) {
        let owner = self.clone();
        let name = name.to_owned();
        registry.register_with_deferred_effect_admission(&name.clone(), move |ctx| {
            let owner = owner.clone();
            let name = name.clone();
            async move { owner.invoke(ctx, &name).await }
        });
    }
    async fn invoke(&self, ctx: StepContext, name: &str) -> Result<serde_json::Value, StepError> {
        if !Arc::ptr_eq(&ctx.storage, &self.backend) {
            return Err(StepError::ClaimLost);
        }
        let token = ctx.claim_token.ok_or(StepError::ClaimLost)?;
        let scope = self
            .storage
            .catalog_callback_scope(ctx.instance_id, &ctx.tenant_id, token)
            .await
            .map_err(storage_error)?;
        let binding = self
            .bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(scope.clone(), name.to_owned()))
            .cloned()
            .ok_or_else(unavailable)?;
        let permits = if ctx.is_dry_run() {
            None
        } else {
            Some(self.capacity.reserve(&scope)?)
        };
        let tracking_key = format!(
            "{}/{}/{}/{}",
            scope.account_id(),
            scope.catalog_id(),
            scope.epoch(),
            name
        );
        invoke_foreign_handler(
            ctx,
            Callback {
                handler: binding.handler.clone(),
                name: binding.name.clone(),
                timeout: binding.timeout,
                stragglers: binding.stragglers.clone(),
                tracking_key,
                permits,
                binding: Some(binding),
            },
        )
        .await
    }
}
