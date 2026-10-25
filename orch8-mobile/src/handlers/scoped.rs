//! Internal frozen-owner native routing. No production scoped materializer yet.
use super::{
    Callback, HandlerRegistry, StepContext, StepHandler, Stragglers, invoke_foreign_handler,
};
use crate::storage::{MobileStorage, scopes::CatalogScope};
use orch8_storage::StorageBackend;
use orch8_types::error::StepError;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

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

mod registrations;

struct NativeCapacity {
    global: Arc<Semaphore>,
    accounts: Mutex<HashMap<String, Weak<Semaphore>>>,
    registrations: registrations::Capacity,
}
impl NativeCapacity {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            global: Arc::new(Semaphore::new(2)),
            accounts: Mutex::new(HashMap::new()),
            registrations: registrations::Capacity::new(),
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
    _registration: registrations::Permit,
}

type BindingKey = (CatalogScope, String);
#[derive(Default)]
struct Bindings {
    live: HashMap<BindingKey, Arc<Binding>>,
    retiring: HashMap<BindingKey, Arc<AtomicBool>>,
}
impl Bindings {
    fn keys(&self) -> impl Iterator<Item = &BindingKey> {
        self.live.keys().chain(
            self.retiring
                .iter()
                .filter(|(_, done)| !done.load(Ordering::Acquire))
                .map(|(key, _)| key),
        )
    }
}

pub(crate) struct CatalogHandlers {
    storage: Arc<MobileStorage>,
    backend: Arc<dyn StorageBackend>,
    bindings: AsyncMutex<Bindings>,
    capacity: Arc<NativeCapacity>,
    stop: tokio_util::sync::CancellationToken,
}
impl CatalogHandlers {
    /// Trusted runtime supplies the same backend used by its step executor.
    /// Decorators are allowed, but a context from another backend is refused.
    pub(crate) fn new(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
        stop: tokio_util::sync::CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            backend,
            bindings: AsyncMutex::new(Bindings::default()),
            capacity: NativeCapacity::shared(),
            stop,
        })
    }
    #[cfg(test)]
    pub(crate) fn isolated(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
    ) -> Arc<Self> {
        let mut owner = Self::new(storage, backend, tokio_util::sync::CancellationToken::new());
        Arc::get_mut(&mut owner).unwrap().capacity = NativeCapacity::new();
        owner
    }
    #[cfg(test)]
    pub(crate) fn sibling(&self) -> Arc<Self> {
        let mut owner = Self::new(
            self.storage.clone(),
            self.backend.clone(),
            tokio_util::sync::CancellationToken::new(),
        );
        Arc::get_mut(&mut owner).unwrap().capacity = self.capacity.clone();
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

    /// Local runtime stop never retires the shared durable account epoch.
    pub(crate) fn shutdown(&self) {
        self.stop.cancel();
    }

    pub(crate) async fn bind(
        &self,
        scope: &CatalogScope,
        name: &str,
        handler: Arc<dyn StepHandler>,
        timeout: Duration,
    ) -> Result<(), StepError> {
        if self.stop.is_cancelled()
            || name.trim().is_empty()
            || name.len() > 256
            || name.chars().any(char::is_control)
            || timeout.is_zero()
        {
            return Err(unavailable());
        }
        let mut bindings = self.bindings.lock().await;
        bindings
            .retiring
            .retain(|_, done| !done.load(Ordering::Acquire));
        self.storage
            .catalog_metadata(scope, "native_handler_binding")
            .await
            .map_err(storage_error)?;
        if self.stop.is_cancelled() {
            return Err(StepError::ClaimLost);
        }
        let key = (scope.clone(), name.to_owned());
        if bindings.live.contains_key(&key) || bindings.retiring.contains_key(&key) {
            return Err(unavailable());
        }
        // Retired entries stay charged; cleanup is explicit runtime integration.
        if bindings.keys().count() >= registrations::GLOBAL_LIMIT
            || bindings
                .keys()
                .filter(|(owner, _)| owner.account_id() == scope.account_id())
                .count()
                >= registrations::ACCOUNT_LIMIT
            || bindings
                .keys()
                .filter(|(owner, _)| {
                    owner.catalog_id() == scope.catalog_id()
                        && owner.account_id() == scope.account_id()
                })
                .count()
                >= registrations::CATALOG_LIMIT
        {
            return Err(busy());
        }
        let registration = self.capacity.registrations.reserve(scope)?;
        bindings.live.insert(
            key,
            Arc::new(Binding {
                handler,
                scope: scope.clone(),
                name: name.into(),
                timeout,
                stragglers: Arc::new(Stragglers::default()),
                _registration: registration,
            }),
        );
        Ok(())
    }
    pub(crate) async fn names(&self, scope: &CatalogScope) -> Result<HashSet<String>, StepError> {
        if self.stop.is_cancelled() {
            return Err(StepError::ClaimLost);
        }
        self.storage
            .catalog_metadata(scope, "native_handler_binding")
            .await
            .map_err(storage_error)?;
        let bindings = self.bindings.lock().await;
        if self.stop.is_cancelled() {
            return Err(StepError::ClaimLost);
        }
        Ok(bindings
            .live
            .keys()
            .filter(|(owner, _)| owner == scope)
            .map(|(_, name)| name.clone())
            .collect())
    }
    /// Call after durable account retirement. No foreign destruction runs on
    /// the async caller or under the registry lock. Busy cleanup is retryable.
    /// Dropping the returned receiver detaches observation, not physical cleanup.
    pub(crate) async fn release_retired(
        self: &Arc<Self>,
        scope: &CatalogScope,
    ) -> Result<tokio::sync::oneshot::Receiver<()>, StepError> {
        let mut bindings = self.bindings.lock().await;
        bindings
            .retiring
            .retain(|_, done| !done.load(Ordering::Acquire));
        if !self
            .storage
            .catalog_epoch_retired(scope)
            .await
            .map_err(storage_error)?
        {
            return Err(unavailable());
        }
        let keys: Vec<_> = bindings
            .live
            .keys()
            .filter(|(owner, _)| {
                owner.account_id() == scope.account_id() && owner.epoch() == scope.epoch()
            })
            .cloned()
            .collect();
        // Reserve before moving references. A hung call/destructor cannot be
        // bypassed by retirement, reopen or another cleanup request.
        let permit = self.capacity.reserve(scope)?;
        let mut detached = Vec::with_capacity(keys.len());
        let completed = Arc::new(AtomicBool::new(false));
        for key in &keys {
            if let Some(binding) = bindings.live.remove(key) {
                detached.push(binding);
                bindings.retiring.insert(key.clone(), completed.clone());
            }
        }
        drop(bindings);
        let retirement = Retirement {
            detached,
            completed,
            _permit: permit,
        };
        let (finished, receiver) = tokio::sync::oneshot::channel();
        // The observer cannot abort the owned cleanup task.
        tokio::task::spawn_blocking(move || {
            drop(retirement);
            let _ = finished.send(());
        });
        Ok(receiver)
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
        if self.stop.is_cancelled() || !Arc::ptr_eq(&ctx.storage, &self.backend) {
            return Err(StepError::ClaimLost);
        }
        let token = ctx.claim_token.ok_or(StepError::ClaimLost)?;
        let scope = self
            .storage
            .catalog_callback_scope(ctx.instance_id, &ctx.tenant_id, token)
            .await
            .map_err(storage_error)?;
        let bindings = self.bindings.lock().await;
        if self.stop.is_cancelled() {
            return Err(StepError::ClaimLost);
        }
        let binding = bindings
            .live
            .get(&(scope.clone(), name.to_owned()))
            .ok_or_else(unavailable)?;
        if ctx.is_dry_run() {
            return Ok(serde_json::json!({
                "dry_run": true, "handler": name, "would": "invoke registered handler",
            }));
        }
        // Reserve before cloning any host reference, serialized with cleanup.
        let permits = Some(self.capacity.reserve(&scope)?);
        let binding = binding.clone();
        drop(bindings);
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
                stop: self.stop.clone(),
            },
        )
        .await
    }
}

// Drop order keeps permits through foreign destruction, even if the runtime
// discards queued work. Pending registration charges clear only after dropping.
struct Retirement {
    detached: Vec<Arc<Binding>>,
    completed: Arc<AtomicBool>,
    _permit: NativePermit,
}
impl Drop for Retirement {
    fn drop(&mut self) {
        for binding in self.detached.drain(..) {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(binding)));
        }
        self.completed.store(true, Ordering::Release);
    }
}
