//! Fault-injecting [`StorageBackend`] decorator for tests.
//!
//! [`FaultStorage`] wraps any backend and forwards every one of the
//! `StorageBackend` sub-trait methods to it, except that a test can arm
//! *rules* that make a chosen method fail, fail after the write landed
//! ("ambiguous commit": the inner call ran, the caller still sees an error),
//! or stall for a while. Every call is counted, so a test can also assert how
//! often a code path touched storage.
//!
//! ```ignore
//! let (storage, faults) = FaultStorage::wrap(Arc::new(SqliteStorage::in_memory().await?));
//! faults.on("save_block_output").times(1).fail_transient();
//! faults.on("create_instance").nth(2).fail_after_commit(StorageError::Connection("ambiguous".into()));
//! faults.on("get_execution_tree").when(|n| n > 3).fail_permanent();
//! // ... drive the code under test with `storage` ...
//! assert_eq!(faults.calls("save_block_output"), 2);
//! assert_eq!(faults.injected("save_block_output"), 1);
//! ```
//!
//! Rules match on the method name; [`FaultHandle::on`] panics for a name that
//! is not a `StorageBackend` method, so a typo cannot silently arm nothing.
//! Call numbers passed to triggers are 1-based and count only the calls made
//! *since the rule was armed*.
//!
//! Every trait method is listed explicitly (default methods included), so the
//! decorator never shadows the inner backend's override with a trait default
//! — the M1 class of bug `EncryptingStorage` once had. A unit test parses
//! `lib.rs` and fails when a trait method is missing here.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
// Glob-import the crate root so the signatures below can be copied verbatim
// from the trait definitions in `lib.rs` (same unqualified type names).
#[allow(clippy::wildcard_imports)]
use crate::*;

/// Builds the [`StorageError`] an armed rule returns. Called once per
/// injected fault so each caller gets an owned error.
pub type ErrorFactory = Arc<dyn Fn(&'static str) -> StorageError + Send + Sync>;

/// When an armed rule fires, given the 1-based number of matching calls made
/// since the rule was armed.
#[derive(Clone)]
pub enum Trigger {
    /// Every call.
    Always,
    /// The first `n` calls, then never again.
    Times(u64),
    /// Only call number `n`.
    Nth(u64),
    /// Calls `n`, `2n`, `3n`, ...
    EveryNth(u64),
    /// Every call after the first `n` succeed.
    After(u64),
    /// Calls for which the predicate holds.
    When(Arc<dyn Fn(u64) -> bool + Send + Sync>),
}

impl Trigger {
    fn fires(&self, n: u64) -> bool {
        match self {
            Self::Always => true,
            Self::Times(k) => n <= *k,
            Self::Nth(k) => n == *k,
            Self::EveryNth(k) => *k > 0 && n.is_multiple_of(*k),
            Self::After(k) => n > *k,
            Self::When(pred) => pred(n),
        }
    }
}

impl std::fmt::Debug for Trigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Always => write!(f, "Always"),
            Self::Times(n) => write!(f, "Times({n})"),
            Self::Nth(n) => write!(f, "Nth({n})"),
            Self::EveryNth(n) => write!(f, "EveryNth({n})"),
            Self::After(n) => write!(f, "After({n})"),
            Self::When(_) => write!(f, "When(..)"),
        }
    }
}

/// What a fired rule does.
#[derive(Clone)]
enum Action {
    /// Return the error without calling the inner backend.
    Fail(ErrorFactory),
    /// Call the inner backend (the write lands), then return the error anyway.
    FailAfterCommit(ErrorFactory),
    /// Sleep (tokio time, so paused-clock tests stay instant), then forward.
    Delay(Duration),
}

struct Rule {
    method: &'static str,
    trigger: Trigger,
    action: Action,
    seen: u64,
}

#[derive(Default)]
struct Plan {
    rules: Vec<Rule>,
    calls: HashMap<&'static str, u64>,
    injected: HashMap<&'static str, u64>,
    log: Vec<&'static str>,
}

/// Transient error used by [`RuleBuilder::fail_transient`]:
/// [`StorageError::Connection`], which `StorageError::is_transient` accepts.
#[must_use]
pub fn transient_error(method: &'static str) -> StorageError {
    StorageError::Connection(format!("injected transient fault in {method}"))
}

/// Permanent error used by [`RuleBuilder::fail_permanent`]:
/// [`StorageError::Query`], which is never retried.
#[must_use]
pub fn permanent_error(method: &'static str) -> StorageError {
    StorageError::Query(format!("injected permanent fault in {method}"))
}

/// Shared control surface of a [`FaultStorage`]. Cheap to clone.
#[derive(Clone, Default)]
pub struct FaultHandle {
    plan: Arc<Mutex<Plan>>,
}

impl FaultHandle {
    fn plan(&self) -> MutexGuard<'_, Plan> {
        // A panicking test thread must not poison every later assertion.
        self.plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Start arming a rule for `method`. Panics if `method` is not a
    /// `StorageBackend` method name.
    pub fn on(&self, method: &'static str) -> RuleBuilder<'_> {
        assert!(
            is_known_method(method),
            "FaultStorage: `{method}` is not a StorageBackend method"
        );
        RuleBuilder {
            handle: self,
            method,
            trigger: Trigger::Always,
        }
    }

    /// Drop every armed rule (call counters are kept).
    pub fn clear(&self) {
        self.plan().rules.clear();
    }

    /// Reset call and injection counters and the call log (rules are kept).
    pub fn reset_counts(&self) {
        let mut plan = self.plan();
        plan.calls.clear();
        plan.injected.clear();
        plan.log.clear();
    }

    /// Calls made to `method` since the wrapper was built (or counts reset),
    /// including calls that were failed by a rule.
    #[must_use]
    pub fn calls(&self, method: &str) -> u64 {
        self.plan().calls.get(method).copied().unwrap_or(0)
    }

    /// Faults (errors, not delays) injected into `method` so far.
    #[must_use]
    pub fn injected(&self, method: &str) -> u64 {
        self.plan().injected.get(method).copied().unwrap_or(0)
    }

    /// Every intercepted call, in order.
    #[must_use]
    pub fn call_log(&self) -> Vec<&'static str> {
        self.plan().log.clone()
    }

    fn arm(&self, rule: Rule) {
        self.plan().rules.push(rule);
    }

    /// Count the call and pick the action of the first armed rule that
    /// fires for it. Every matching rule advances its own call counter.
    fn decide(&self, method: &'static str) -> Option<Action> {
        let mut plan = self.plan();
        *plan.calls.entry(method).or_insert(0) += 1;
        plan.log.push(method);
        let mut chosen = None;
        for rule in plan.rules.iter_mut().filter(|r| r.method == method) {
            rule.seen += 1;
            if chosen.is_none() && rule.trigger.fires(rule.seen) {
                chosen = Some(rule.action.clone());
            }
        }
        if matches!(chosen, Some(Action::Fail(_) | Action::FailAfterCommit(_))) {
            *plan.injected.entry(method).or_insert(0) += 1;
        }
        chosen
    }

    /// Run `call` (the inner backend's future) under the armed plan.
    async fn intercept<T, E, F>(&self, method: &'static str, call: F) -> Result<T, E>
    where
        E: InjectedError,
        F: Future<Output = Result<T, E>>,
    {
        match self.decide(method) {
            None => call.await,
            Some(Action::Fail(make)) => Err(E::injected(make(method))),
            Some(Action::FailAfterCommit(make)) => {
                // The inner write's own result is discarded on purpose: the
                // caller must not learn whether it landed.
                drop(call.await);
                Err(E::injected(make(method)))
            }
            Some(Action::Delay(d)) => {
                tokio::time::sleep(d).await;
                call.await
            }
        }
    }

    fn count_sync(&self, method: &'static str) {
        let mut plan = self.plan();
        *plan.calls.entry(method).or_insert(0) += 1;
        plan.log.push(method);
    }
}

/// Fluent builder returned by [`FaultHandle::on`]. Pick a trigger (default
/// [`Trigger::Always`]), then finish with an action.
#[must_use]
pub struct RuleBuilder<'a> {
    handle: &'a FaultHandle,
    method: &'static str,
    trigger: Trigger,
}

impl RuleBuilder<'_> {
    /// Fire on the first `n` calls only.
    pub fn times(mut self, n: u64) -> Self {
        self.trigger = Trigger::Times(n);
        self
    }

    /// Fire on call number `n` only.
    pub fn nth(mut self, n: u64) -> Self {
        self.trigger = Trigger::Nth(n);
        self
    }

    /// Fire on every `n`th call.
    pub fn every_nth(mut self, n: u64) -> Self {
        self.trigger = Trigger::EveryNth(n);
        self
    }

    /// Let the first `n` calls through, then fire on every call.
    pub fn after(mut self, n: u64) -> Self {
        self.trigger = Trigger::After(n);
        self
    }

    /// Fire when `pred(call_number)` holds.
    pub fn when(mut self, pred: impl Fn(u64) -> bool + Send + Sync + 'static) -> Self {
        self.trigger = Trigger::When(Arc::new(pred));
        self
    }

    fn finish(self, action: Action) {
        self.handle.arm(Rule {
            method: self.method,
            trigger: self.trigger,
            action,
            seen: 0,
        });
    }

    /// Fail with a transient [`StorageError::Connection`].
    pub fn fail_transient(self) {
        self.finish(Action::Fail(Arc::new(transient_error)));
    }

    /// Fail with a permanent [`StorageError::Query`].
    pub fn fail_permanent(self) {
        self.finish(Action::Fail(Arc::new(permanent_error)));
    }

    /// Fail with a copy of `error` (re-built per call from its `Display`
    /// form, since `StorageError` is not `Clone`; the variant is preserved for
    /// the string-carrying variants and `PoolExhausted`).
    pub fn fail_with(self, error: StorageError) {
        self.finish(Action::Fail(clone_factory(error)));
    }

    /// Fail with an error built by `make(method)` on every fired call.
    pub fn fail_with_factory(
        self,
        make: impl Fn(&'static str) -> StorageError + Send + Sync + 'static,
    ) {
        self.finish(Action::Fail(Arc::new(make)));
    }

    /// Let the inner call run (its write lands), then return `error`.
    pub fn fail_after_commit(self, error: StorageError) {
        self.finish(Action::FailAfterCommit(clone_factory(error)));
    }

    /// Sleep for `delay` before forwarding. Uses tokio time, so it is instant
    /// under `#[tokio::test(start_paused = true)]`.
    pub fn delay(self, delay: Duration) {
        self.finish(Action::Delay(delay));
    }
}

fn clone_factory(error: StorageError) -> ErrorFactory {
    Arc::new(move |_| clone_storage_error(&error))
}

/// Best-effort clone of a [`StorageError`] (it is not `Clone` because of the
/// `serde_json::Error` variant, which is rebuilt as a `Query`).
#[must_use]
pub fn clone_storage_error(error: &StorageError) -> StorageError {
    match error {
        StorageError::Connection(m) => StorageError::Connection(m.clone()),
        StorageError::Query(m) => StorageError::Query(m.clone()),
        StorageError::Constraint(m) => StorageError::Constraint(m.clone()),
        StorageError::Encryption(m) => StorageError::Encryption(m.clone()),
        StorageError::NotFound { entity, id } => StorageError::NotFound {
            entity,
            id: id.clone(),
        },
        StorageError::Conflict(m) => StorageError::Conflict(m.clone()),
        StorageError::QuotaExceeded(m) => StorageError::QuotaExceeded(m.clone()),
        StorageError::TerminalTarget { entity, id } => StorageError::TerminalTarget {
            entity: entity.clone(),
            id: id.clone(),
        },
        StorageError::Migration(m) => StorageError::Migration(m.clone()),
        StorageError::Serialization(e) => StorageError::Query(e.to_string()),
        StorageError::PoolExhausted => StorageError::PoolExhausted,
        StorageError::Unsupported(m) => StorageError::Unsupported(m.clone()),
        StorageError::Backend(m) => StorageError::Backend(m.clone()),
    }
}

/// Error types an intercepted method can return.
pub trait InjectedError {
    fn injected(error: StorageError) -> Self;
}

impl InjectedError for StorageError {
    fn injected(error: StorageError) -> Self {
        error
    }
}

/// `PushOutboxStore` reports errors as `String`.
impl InjectedError for String {
    fn injected(error: StorageError) -> Self {
        error.to_string()
    }
}

/// The fault-injecting decorator. See the module docs.
pub struct FaultStorage {
    inner: Arc<dyn StorageBackend>,
    faults: FaultHandle,
}

impl FaultStorage {
    /// Wrap `inner`; no rule is armed yet.
    pub fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            faults: FaultHandle::default(),
        }
    }

    /// Wrap `inner` and return it ready to hand to code under test, plus the
    /// handle that arms rules and reads counters.
    pub fn wrap(inner: Arc<dyn StorageBackend>) -> (Arc<dyn StorageBackend>, FaultHandle) {
        let storage = Self::new(inner);
        let faults = storage.faults();
        (Arc::new(storage), faults)
    }

    /// The control handle (shares state with this wrapper).
    #[must_use]
    pub fn faults(&self) -> FaultHandle {
        self.faults.clone()
    }

    /// The wrapped backend, bypassing fault injection (for test arrange /
    /// assert steps that must not be counted or failed).
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn StorageBackend> {
        &self.inner
    }
}

/// `true` iff `method` names a `StorageBackend` (sub-)trait method.
#[must_use]
pub fn is_known_method(method: &str) -> bool {
    ALL_METHODS.iter().any(|methods| methods.contains(&method))
}

/// Same TT-muncher shape as `encrypting.rs`'s `passthrough_impl!` (see the
/// long comment there for why the whole block must be expanded before
/// `#[async_trait]` sees it). Every `;`-terminated signature becomes a
/// forwarding method that runs through [`FaultHandle::intercept`]; the inner
/// call is fully qualified because a few names exist on two sub-traits
/// (`batch_save_externalized_state`). Also
/// emits `$list`, the method names of the block, used by
/// [`is_known_method`] and the drift test.
macro_rules! fault_impl {
    ([$list:ident] impl $trait_name:ident { $($body:tt)* }) => {
        fault_impl!(@munch [$list] [$trait_name] [] [] $($body)*);
    };

    (@munch [$list:ident] [$trait_name:ident] [$($methods:tt)*] [$($names:expr),*]) => {
        #[async_trait]
        impl $trait_name for FaultStorage {
            $($methods)*
        }

        const $list: &[&str] = &[$($names),*];
    };

    (@munch [$list:ident] [$trait_name:ident] [$($methods:tt)*] [$($names:expr),*] async fn $name:ident(&self $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty ; $($rest:tt)*) => {
        fault_impl!(@munch [$list] [$trait_name] [$($methods)* async fn $name(&self, $($arg: $ty),*) -> $ret {
            self.faults
                .intercept(stringify!($name), $trait_name::$name(&*self.inner, $($arg),*))
                .await
        }] [$($names,)* stringify!($name)] $($rest)*);
    };

    // Non-async methods (`ResourceStore::artifacts_enabled`) are counted but
    // never failed: they cannot return an error.
    (@munch [$list:ident] [$trait_name:ident] [$($methods:tt)*] [$($names:expr),*] fn $name:ident(&self $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty ; $($rest:tt)*) => {
        fault_impl!(@munch [$list] [$trait_name] [$($methods)* fn $name(&self, $($arg: $ty),*) -> $ret {
            self.faults.count_sync(stringify!($name));
            $trait_name::$name(&*self.inner, $($arg),*)
        }] [$($names,)* stringify!($name)] $($rest)*);
    };
}

include!("faulty_methods.rs");

fault_impl! {
    [PUSH_OUTBOX_STORE_METHODS] impl PushOutboxStoreAlias {
        async fn enqueue_wake(&self, tenant_id: &str, device_id: &str, command_id: &str, created_at: DateTime<Utc>) -> Result<Uuid, String>;
        async fn enqueue_collapsible_wake(&self, wake: &orch8_push::CollapsibleWake) -> Result<Uuid, String>;
        async fn claim_due_wakes(&self, now: DateTime<Utc>, lease_until: DateTime<Utc>, limit: u32) -> Result<Vec<orch8_push::ClaimedWake>, String>;
        async fn record_wake_outcome(&self, wake: &orch8_push::ClaimedWake, outcome: &orch8_push::WakeAttemptOutcome, recorded_at: DateTime<Utc>) -> Result<(), String>;
        async fn record_command_acks(&self, tenant_id: &str, device_id: &str, command_ids: &[String], acked_at: DateTime<Utc>) -> Result<u64, String>;
        async fn prune_wakes(&self, created_before: DateTime<Utc>) -> Result<u64, String>;
    }
}

use orch8_push::PushOutboxStore as PushOutboxStoreAlias;

/// Method names of every sub-trait, in `StorageBackend` supertrait order.
const ALL_METHODS: &[&[&str]] = &[
    SEQUENCE_STORE_METHODS,
    INSTANCE_STORE_METHODS,
    EXECUTION_TREE_STORE_METHODS,
    OUTPUT_STORE_METHODS,
    SIGNAL_STORE_METHODS,
    WORKER_STORE_METHODS,
    SCHEDULING_STORE_METHODS,
    ADMIN_STORE_METHODS,
    TELEMETRY_STORE_METHODS,
    RESOURCE_STORE_METHODS,
    MOBILE_SYNC_STORE_METHODS,
    CONTINUITY_STORE_METHODS,
    INVARIANT_STORE_METHODS,
    EVALUATION_STORE_METHODS,
    ATTENTION_STORE_METHODS,
    AI_STORE_METHODS,
    TENANCY_STORE_METHODS,
    PUSH_OUTBOX_STORE_METHODS,
];

#[cfg(test)]
#[path = "faulty_tests.rs"]
mod tests;
