//! In-process engine harness for tests (T1 HX-3).
//!
//! [`TestEngine`] runs instances through the **real scheduler**
//! ([`crate::scheduler::tick_once`]: claim, CAS, execute, commit, signals,
//! hooks) on a [`ManualClock`], instead of the evaluator-only `drive()`
//! helpers that set the final state themselves. Retries, delays and timers
//! fire by advancing virtual time, never by sleeping.
//!
//! ```ignore
//! let engine = TestEngine::builder()
//!     .register("flaky", |_ctx| Box::pin(async { Err(StepError::Retryable { .. }) }))
//!     .build()
//!     .await;
//! let seq = SequenceBuilder::new("t").step("a", "flaky").build();
//! let id = engine.start(&seq).await;
//! let done = engine.drive(id).await;           // ticks, jumping to each next_fire_at
//! assert_eq!(done.state, InstanceState::Failed);
//! ```
//!
//! [`TestEngine::spawn`] runs the full [`crate::Engine::run`] (tick loop,
//! reapers, cron, outbox) with short intervals for loop-level tests.
//!
//! Also here: [`http`] (a scriptable loopback HTTP mock), [`ssrf`] (the
//! shared table of URLs every outbound guard must block) and
//! [`step_context`] (a `StepContext` for calling a handler directly).
//!
//! Compiled for this crate's tests and, elsewhere, behind the `test-util`
//! feature (dev-dependencies only).

pub mod http;
pub mod ssrf;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use orch8_storage::StorageBackend;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::clock::{Clock, ManualClock, SharedClock};
use orch8_types::config::SchedulerConfig;
use orch8_types::error::StepError;
use orch8_types::ids::InstanceId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::SequenceDefinition;

use crate::error::EngineError;
use crate::handlers::{HandlerRegistry, StepContext, builtin::register_builtins};
use crate::scheduler::{TickOnceResult, tick_once};
use crate::sequence_cache::SequenceCache;

/// Default upper bound on ticks in [`TestEngine::drive`].
pub const DEFAULT_MAX_TICKS: usize = 200;

/// Scheduler config for tests: small batches and the defaults otherwise;
/// short loop intervals so [`TestEngine::spawn`] reacts quickly.
#[must_use]
pub fn test_scheduler_config() -> SchedulerConfig {
    SchedulerConfig {
        tick_interval_ms: 10,
        worker_reaper_tick_secs: 1,
        node_reaper_tick_secs: 1,
        cron_tick_secs: 1,
        ..SchedulerConfig::default()
    }
}

/// Builder for [`TestEngine`].
#[must_use]
pub struct TestEngineBuilder {
    storage: Option<Arc<dyn StorageBackend>>,
    registry: HandlerRegistry,
    config: SchedulerConfig,
    start: Option<DateTime<Utc>>,
}

impl TestEngineBuilder {
    /// Run on this storage (default: a fresh in-memory `SQLite`). Wrap it in
    /// `orch8_storage::testing::faulty::FaultStorage` first to inject faults.
    pub fn storage(mut self, storage: Arc<dyn StorageBackend>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Register a handler (on top of the builtins).
    pub fn register<F, Fut>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(StepContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<serde_json::Value, StepError>> + Send + 'static,
    {
        self.registry.register(name, handler);
        self
    }

    /// Replace the handler registry (builtins are not added).
    pub fn registry(mut self, registry: HandlerRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Tweak the scheduler config (the clock is always the engine's
    /// [`ManualClock`]).
    pub fn config(mut self, edit: impl FnOnce(&mut SchedulerConfig)) -> Self {
        edit(&mut self.config);
        self
    }

    /// Virtual start time (default: the real now; never start in the past,
    /// see the `ManualClock` discipline in `orch8_types::clock`).
    pub fn start_at(mut self, at: DateTime<Utc>) -> Self {
        self.start = Some(at);
        self
    }

    pub async fn build(self) -> TestEngine {
        let storage = match self.storage {
            Some(s) => s,
            None => Arc::new(
                SqliteStorage::in_memory()
                    .await
                    .expect("in-memory SQLite storage"),
            ),
        };
        let clock = Arc::new(ManualClock::new(self.start.unwrap_or_else(Utc::now)));
        let mut config = self.config;
        config.clock = SharedClock::from_arc(Arc::clone(&clock) as Arc<dyn Clock>);
        TestEngine {
            storage,
            handlers: Arc::new(self.registry),
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_steps.max(1) as usize)),
            sequence_cache: Arc::new(SequenceCache::new(1000, Duration::from_secs(300))),
            cancel: CancellationToken::new(),
            clock,
            config,
        }
    }
}

/// An engine driven tick by tick on virtual time. See the module docs.
pub struct TestEngine {
    pub storage: Arc<dyn StorageBackend>,
    pub handlers: Arc<HandlerRegistry>,
    pub clock: Arc<ManualClock>,
    pub config: SchedulerConfig,
    semaphore: Arc<Semaphore>,
    sequence_cache: Arc<SequenceCache>,
    cancel: CancellationToken,
}

impl TestEngine {
    /// Builtins registered, in-memory `SQLite`, clock at the real now.
    pub fn builder() -> TestEngineBuilder {
        let mut registry = HandlerRegistry::new();
        register_builtins(&mut registry);
        TestEngineBuilder {
            storage: None,
            registry,
            config: test_scheduler_config(),
            start: None,
        }
    }

    /// Virtual now.
    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        self.clock.now()
    }

    /// Move virtual time forward.
    pub fn advance(&self, by: Duration) {
        self.clock
            .advance(chrono::Duration::from_std(by).expect("advance fits chrono::Duration"));
    }

    /// The scheduler's shared clock (for `process_cron_tick`, `outbox_tick`,
    /// `refresh_pass_at`, ...).
    #[must_use]
    pub fn shared_clock(&self) -> SharedClock {
        self.config.clock.clone()
    }

    /// One real scheduling pass (`tick_once`) at the current virtual time.
    pub async fn try_tick(&self) -> Result<TickOnceResult, EngineError> {
        tick_once(
            &self.storage,
            &self.handlers,
            &self.semaphore,
            &self.config,
            &self.sequence_cache,
            &self.cancel,
        )
        .await
    }

    /// [`Self::try_tick`], panicking on a tick error.
    pub async fn tick(&self) -> TickOnceResult {
        self.try_tick().await.expect("tick_once failed")
    }

    /// Store `seq` and a due instance of it; returns the instance id.
    pub async fn start(&self, seq: &SequenceDefinition) -> InstanceId {
        self.storage
            .create_sequence(seq)
            .await
            .expect("create sequence");
        let inst = orch8_storage::testing::fixtures::InstanceBuilder::for_sequence(seq)
            .due_at(self.now())
            .build();
        self.create(&inst).await
    }

    /// Store `inst` as-is; returns its id.
    pub async fn create(&self, inst: &TaskInstance) -> InstanceId {
        self.storage
            .create_instance(inst)
            .await
            .expect("create instance");
        inst.id
    }

    /// Current row of instance `id`.
    pub async fn instance(&self, id: InstanceId) -> TaskInstance {
        self.storage
            .get_instance(id)
            .await
            .expect("get_instance")
            .unwrap_or_else(|| panic!("instance {id} does not exist"))
    }

    /// [`Self::drive_for`] with [`DEFAULT_MAX_TICKS`].
    pub async fn drive(&self, id: InstanceId) -> TaskInstance {
        self.drive_for(id, DEFAULT_MAX_TICKS).await
    }

    /// Tick until instance `id` settles and return the row the *scheduler*
    /// wrote. Settled = terminal, `Waiting` or `Paused` (a signal or an
    /// explicit [`Self::advance`] is needed to go on). Whenever the instance
    /// is `Scheduled` in the virtual future, the clock jumps to its
    /// `next_fire_at` first, so retry backoffs and delays elapse instantly.
    /// Panics after `max_ticks` ticks, printing the last state.
    pub async fn drive_for(&self, id: InstanceId, max_ticks: usize) -> TaskInstance {
        for _ in 0..max_ticks {
            let inst = self.instance(id).await;
            if inst.state.is_terminal()
                || matches!(inst.state, InstanceState::Waiting | InstanceState::Paused)
            {
                return inst;
            }
            if inst.state == InstanceState::Scheduled
                && let Some(at) = inst.next_fire_at
                && at > self.now()
            {
                self.clock.set(at);
            }
            self.tick().await;
        }
        let inst = self.instance(id).await;
        panic!(
            "instance {id} did not settle within {max_ticks} ticks: state {:?}, next_fire_at {:?}",
            inst.state, inst.next_fire_at
        );
    }

    /// Run the full [`crate::Engine`] (tick loop, reapers, cron, outbox, ...)
    /// on this engine's storage, handlers and config, until
    /// [`RunningEngine::stop`]. Background loops tick on tokio time; the
    /// scheduler reads the virtual clock. Consumes the harness because the
    /// engine takes ownership of the handler registry.
    #[must_use]
    pub fn spawn(self) -> RunningEngine {
        let handlers = Arc::try_unwrap(self.handlers)
            .unwrap_or_else(|_| panic!("TestEngine::spawn: the handler registry is still shared"));
        let cancel = CancellationToken::new();
        let engine = crate::Engine::new(
            Arc::clone(&self.storage),
            self.config,
            handlers,
            cancel.clone(),
        );
        let liveness = engine.liveness();
        let join = tokio::spawn(async move { engine.run().await });
        RunningEngine {
            storage: self.storage,
            clock: self.clock,
            cancel,
            liveness,
            join,
        }
    }
}

/// A running [`crate::Engine`] started by [`TestEngine::spawn`].
pub struct RunningEngine {
    pub storage: Arc<dyn StorageBackend>,
    pub clock: Arc<ManualClock>,
    pub cancel: CancellationToken,
    pub liveness: Arc<crate::liveness::EngineLiveness>,
    join: tokio::task::JoinHandle<Result<(), EngineError>>,
}

impl RunningEngine {
    /// Cancel and wait for `Engine::run` to return; returns its result.
    pub async fn stop(self) -> Result<(), EngineError> {
        self.cancel.cancel();
        self.join.await.expect("engine task panicked")
    }

    /// Abort without the graceful drain (an in-process "crash").
    pub fn abort(self) {
        self.join.abort();
    }
}

/// A [`StepContext`] for calling a handler directly (T1 HX-4): a fresh
/// in-memory `SQLite` storage, tenant `t`, block `step`, attempt 0, empty
/// context. Mutate the returned value for anything else.
pub async fn step_context(params: serde_json::Value) -> StepContext {
    StepContext {
        effect_admission: None,
        claim_token: None,
        instance_id: InstanceId::new(),
        tenant_id: orch8_types::ids::TenantId::unchecked("t"),
        block_id: orch8_types::ids::BlockId::new("step"),
        params,
        context: Arc::new(orch8_types::context::ExecutionContext::default()),
        attempt: 0,
        storage: Arc::new(
            SqliteStorage::in_memory()
                .await
                .expect("in-memory SQLite storage"),
        ),
        wait_for_input: None,
    }
}

/// Run builtin handler `name` once with `params` (see [`step_context`]).
/// Panics if `name` is not a builtin.
pub async fn call_builtin(
    name: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, StepError> {
    let mut registry = HandlerRegistry::new();
    register_builtins(&mut registry);
    let handler = registry
        .get(name)
        .unwrap_or_else(|| panic!("`{name}` is not a builtin handler"));
    handler(step_context(params).await).await
}

/// Poll `probe` every 10 ms until it returns `Some`, or panic after
/// `timeout` (tokio time). For loop-level tests where a tick count cannot be
/// controlled; prefer [`TestEngine::drive`] otherwise.
pub async fn wait_until<T, F, Fut>(what: &str, timeout: Duration, mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = probe().await {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;

#[cfg(test)]
mod claim_write_tests;

#[cfg(test)]
mod parallel_deferral_tests;
