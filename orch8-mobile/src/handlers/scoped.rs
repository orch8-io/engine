//! Internal frozen-owner local routing and scoped runtime admission.
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
#[cfg(test)]
use tokio::sync::Semaphore;

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
pub(super) fn storage_error(error: orch8_types::error::StorageError) -> StepError {
    if error.is_transient() {
        StepError::Retryable {
            message: "scoped native routing unavailable".into(),
            details: None,
        }
    } else {
        StepError::ClaimLost
    }
}

pub(crate) mod access;
pub(crate) mod context;
pub(crate) mod history;
mod lifecycle;
pub(crate) mod lifetime;
pub(crate) mod materialize;
mod pending;
mod physical;
mod registrations;
mod resolution;
mod retirement;
pub(super) use physical::Permit as NativePermit;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
struct NativeCapacity {
    physical: Arc<physical::Budget>,
    registrations: registrations::Capacity,
    retirement: Mutex<Option<retirement::Executor>>,
}
impl NativeCapacity {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            physical: physical::Budget::new(),
            registrations: registrations::Capacity::new(),
            retirement: Mutex::new(None),
        })
    }
    fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<NativeCapacity>> = OnceLock::new();
        SHARED.get_or_init(Self::new).clone()
    }
    fn reserve(&self, scope: &CatalogScope) -> Result<NativePermit, StepError> {
        self.physical.reserve(scope)
    }
    fn executor(&self) -> Result<retirement::Handle, StepError> {
        let mut owner = lock(&self.retirement);
        if owner.is_none() {
            *owner = Some(retirement::Executor::new(self.physical.clone())?);
        }
        Ok(owner.as_ref().ok_or_else(busy)?.handle())
    }
}

pub(super) struct Binding {
    // Foreign object drops before its plain metadata; never under the map lock.
    handler: context::BoundHandler,
    pub(super) lifetime: Arc<lifetime::Lifetime>,
    scope: CatalogScope,
    name: String,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
    _epoch: Arc<AtomicBool>,
    account: Arc<physical::Account>,
    retirement: retirement::Handle,
    _registration: Arc<registrations::Permit>,
}

impl Binding {
    pub(in crate::handlers) fn registration_charge(&self) -> Arc<registrations::Permit> {
        self._registration.clone()
    }
}

type BindingKey = (CatalogScope, String);
#[derive(Default)]
struct Bindings {
    live: HashMap<BindingKey, Arc<Binding>>,
    pending: HashMap<usize, (BindingKey, Arc<Binding>)>,
    retiring: HashMap<BindingKey, Arc<retirement::Completion>>,
    epochs: HashMap<(String, i64), Weak<AtomicBool>>,
}
impl Bindings {
    fn keys(&self) -> impl Iterator<Item = &BindingKey> {
        self.live
            .keys()
            .chain(self.pending.values().map(|(key, _)| key))
            .chain(
                self.retiring
                    .iter()
                    .filter(|(_, done)| !done.finished())
                    .map(|(key, _)| key),
            )
    }
}

pub(crate) struct CatalogHandlers {
    lifetime: Arc<lifetime::Lifetime>,
    storage: Arc<MobileStorage>,
    backend: Arc<dyn StorageBackend>,
    bindings: Mutex<Bindings>,
    capacity: Arc<NativeCapacity>,
    stop: tokio_util::sync::CancellationToken,
    #[cfg(test)]
    bind_pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}
impl CatalogHandlers {
    pub(crate) fn mobile_storage(&self) -> Arc<MobileStorage> {
        self.storage.clone()
    }
    /// Trusted runtime supplies the same backend used by its step executor.
    /// Decorators are allowed, but a context from another backend is refused.
    #[cfg(test)]
    pub(crate) fn new(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
        stop: tokio_util::sync::CancellationToken,
    ) -> Arc<Self> {
        Self::new_with_lifetime(
            storage,
            backend,
            stop,
            crate::MobileEngineConfig::default().max_instance_lifetime_secs,
        )
    }
    pub(crate) fn new_with_lifetime(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
        stop: tokio_util::sync::CancellationToken,
        seconds: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            lifetime: Arc::new(lifetime::Lifetime::new(storage.clone(), seconds)),
            storage,
            backend,
            bindings: Mutex::new(Bindings::default()),
            capacity: NativeCapacity::shared(),
            stop,
            #[cfg(test)]
            bind_pause: Mutex::new(None),
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
    pub(crate) fn isolated_with_lifetime(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
        seconds: u64,
        clock: orch8_types::clock::SharedClock,
    ) -> Arc<Self> {
        let mut owner = Self::new_with_lifetime(
            storage,
            backend,
            tokio_util::sync::CancellationToken::new(),
            seconds,
        );
        let state = Arc::get_mut(&mut owner).unwrap();
        state.capacity = NativeCapacity::new();
        Arc::get_mut(&mut state.lifetime).unwrap().set_clock(clock);
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
    pub(crate) fn occupy_native_for_test(&self, scope: &CatalogScope) -> Box<dyn Send> {
        Box::new(self.capacity.reserve(scope).unwrap())
    }

    #[cfg(test)]
    pub(crate) fn account_gate(&self, scope: &CatalogScope) -> Option<Arc<Semaphore>> {
        self.capacity.physical.account_gate(scope)
    }

    #[cfg(test)]
    pub(crate) fn test_storage(&self) -> Arc<MobileStorage> {
        self.storage.clone()
    }
    #[cfg(test)]
    pub(crate) fn registry_unlocked(&self) -> bool {
        self.bindings.try_lock().is_ok()
    }
    #[cfg(test)]
    pub(crate) fn shared_test_gate() -> Arc<tokio::sync::Mutex<()>> {
        static GATE: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
        GATE.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    // Existing storage/admission fixtures can ignore callback metadata. The
    // production scoped registration path always uses the contextual trait.
    #[cfg(test)]
    pub(crate) async fn bind(
        &self,
        scope: &CatalogScope,
        name: &str,
        handler: Arc<dyn StepHandler>,
        timeout: Duration,
    ) -> Result<(), StepError> {
        self.bind_owned(scope, name, context::BoundHandler::Legacy(handler), timeout)
            .await
    }

    pub(crate) async fn bind_contextual(
        &self,
        scope: &CatalogScope,
        name: &str,
        handler: Arc<dyn context::CatalogStepHandler>,
        timeout: Duration,
    ) -> Result<(), StepError> {
        self.bind_owned(
            scope,
            name,
            context::BoundHandler::Contextual(handler),
            timeout,
        )
        .await
    }

    async fn bind_owned(
        &self,
        scope: &CatalogScope,
        name: &str,
        handler: context::BoundHandler,
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
        let pending = pending::Pending::new(self, scope, name, handler, timeout)?;
        self.storage
            .catalog_metadata(scope, "native_handler_binding")
            .await
            .map_err(storage_error)?;
        #[cfg(test)]
        {
            let pause = { lock(&self.bind_pause).take() };
            if let Some((entered, release)) = pause {
                let _ = entered.send(());
                let _ = release.await;
            }
        }
        pending.publish()
    }
    #[cfg(test)]
    pub(crate) fn hold_next_bind(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, held) = tokio::sync::oneshot::channel();
        *lock(&self.bind_pause) = Some((entered, held));
        (observed, release)
    }

    pub(crate) async fn names(&self, scope: &CatalogScope) -> Result<HashSet<String>, StepError> {
        if self.stop.is_cancelled() {
            return Err(StepError::ClaimLost);
        }
        self.storage
            .catalog_metadata(scope, "native_handler_binding")
            .await
            .map_err(storage_error)?;
        let bindings = lock(&self.bindings);
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
        self.invoke_selected(ctx, name, None).await
    }
    async fn invoke_selected(
        &self,
        ctx: StepContext,
        name: &str,
        selected: Option<&Weak<Binding>>,
    ) -> Result<serde_json::Value, StepError> {
        if self.stop.is_cancelled() || !Arc::ptr_eq(&ctx.storage, &self.backend) {
            return Err(StepError::ClaimLost);
        }
        let token = ctx.claim_token.ok_or(StepError::ClaimLost)?;
        let scope = self
            .storage
            .catalog_callback_scope(ctx.instance_id, &ctx.tenant_id, token)
            .await
            .map_err(storage_error)?;
        let (binding, permits, handler) = {
            let bindings = lock(&self.bindings);
            if self.stop.is_cancelled() {
                return Err(StepError::ClaimLost);
            }
            let binding = bindings
                .live
                .get(&(scope.clone(), name.to_owned()))
                .ok_or_else(unavailable)?;
            if selected.is_some_and(|expected| !expected.ptr_eq(&Arc::downgrade(binding))) {
                return Err(unavailable());
            }
            if ctx.is_dry_run() {
                return Ok(
                    serde_json::json!({ "dry_run": true, "handler": name, "would": "invoke registered handler" }),
                );
            }
            // A missing effect proof must fail while the registry still owns
            // the foreign reference, before any independently held clone.
            let handler = binding.handler.for_invocation(&scope, &ctx)?;
            let permits = Some(self.capacity.reserve(&scope).map_err(|error| {
                ctx.effect_admission.as_ref().map_or(error, |admission| {
                    admission.reject_preflight(orch8_engine::error::EngineError::AdmissionBusy {
                        instance_id: ctx.instance_id,
                    })
                })
            })?);
            (binding.clone(), permits, handler)
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
                handler,
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

fn check_capacity(bindings: &Bindings, key: &BindingKey) -> Result<(), StepError> {
    if bindings.live.contains_key(key)
        || bindings.retiring.contains_key(key)
        || bindings.pending.values().any(|(pending, _)| pending == key)
    {
        return Err(unavailable());
    }
    let scope = &key.0;
    if bindings.keys().count() >= registrations::GLOBAL_LIMIT
        || bindings
            .keys()
            .filter(|(owner, _)| owner.account_id() == scope.account_id())
            .count()
            >= registrations::ACCOUNT_LIMIT
        || bindings
            .keys()
            .filter(|(owner, _)| {
                owner.account_id() == scope.account_id() && owner.catalog_id() == scope.catalog_id()
            })
            .count()
            >= registrations::CATALOG_LIMIT
    {
        return Err(busy());
    }
    Ok(())
}
