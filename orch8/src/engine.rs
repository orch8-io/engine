use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::{TickOnceResult, run_tick_loop, tick_once};
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use orch8_types::context::ExecutionContext;
use orch8_types::continuity::EffectReceipt;
use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{Budget, InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::{Signal, SignalType};

use crate::builder::EngineBuilder;
use crate::error::Error;

/// Options for [`Engine::create_instance`]. All fields have defaults, so
/// `Default::default()` starts an instance immediately with an empty context
/// in the engine's default namespace.
#[derive(Debug, Clone)]
pub struct CreateInstanceOptions {
    /// Namespace the instance is scoped to. Default: `"default"`.
    pub namespace: Namespace,
    /// Initial execution context (`context.data` is what step handlers
    /// read/write). Default: empty.
    pub context: ExecutionContext,
    /// Free-form metadata stored on the instance. Default: `{}`.
    pub metadata: serde_json::Value,
    /// Scheduling priority. Default: [`Priority::Normal`].
    pub priority: Priority,
    /// Optional resource budget (token/step caps) enforced by the scheduler.
    pub budget: Option<Budget>,
    /// Idempotency key: re-creating with the same key returns the existing
    /// instance's id instead of starting a duplicate.
    pub idempotency_key: Option<String>,
    /// When the instance should first fire. Default: immediately.
    pub next_fire_at: Option<DateTime<Utc>>,
    /// IANA timezone for time-window evaluation. Default: `"UTC"`.
    pub timezone: String,
}

impl Default for CreateInstanceOptions {
    fn default() -> Self {
        Self {
            namespace: Namespace::new("default"),
            context: ExecutionContext::default(),
            metadata: serde_json::json!({}),
            priority: Priority::Normal,
            budget: None,
            idempotency_key: None,
            next_fire_at: None,
            timezone: "UTC".to_string(),
        }
    }
}

struct Inner {
    storage: Arc<dyn StorageBackend>,
    storage_owner: crate::storage::ConnectedStorage,
    handlers: Arc<HandlerRegistry>,
    config: SchedulerConfig,
    tenant: TenantId,
    cancel: CancellationToken,
    /// Bounds concurrent step execution for the manual [`Engine::tick_once`]
    /// path. The background loop manages its own semaphore internally.
    semaphore: Arc<Semaphore>,
    sequence_cache: Arc<SequenceCache>,
    tick_task: Mutex<Option<JoinHandle<()>>>,
}

/// L1: dropping the last `Engine` handle without calling
/// [`Engine::shutdown`] used to leave the spawned tick loop running forever
/// on the host runtime (it owns its own clones of storage and handlers).
/// Cancelling here stops it; in-flight steps are not awaited -- call
/// `shutdown` for a graceful drain.
impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// An embedded Orch8 engine. Cheap to clone (all state is behind an `Arc`),
/// so it can be shared across tasks — e.g. stored in an axum router's state.
///
/// Construct via [`Engine::builder`]; drive it with [`Engine::start`] /
/// [`Engine::shutdown`] or manually with [`Engine::tick_once`].
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

impl Engine {
    /// Start configuring an embedded engine.
    pub fn builder() -> EngineBuilder {
        EngineBuilder::new()
    }

    pub(crate) fn storage_backend(&self) -> &Arc<dyn StorageBackend> {
        &self.inner.storage
    }

    pub(crate) fn tenant_id(&self) -> &TenantId {
        &self.inner.tenant
    }

    pub(crate) fn from_parts(
        storage: crate::storage::ConnectedStorage,
        handlers: HandlerRegistry,
        config: SchedulerConfig,
        tenant: TenantId,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_steps as usize));
        Self {
            inner: Arc::new(Inner {
                storage: storage.backend(),
                storage_owner: storage,
                handlers: Arc::new(handlers),
                semaphore,
                sequence_cache: Arc::new(SequenceCache::new(1_000, Duration::from_secs(300))),
                tenant,
                cancel: CancellationToken::new(),
                config,
                tick_task: Mutex::new(None),
            }),
        }
    }

    /// The default tenant instances are created under.
    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.inner.tenant
    }

    /// Spawn the scheduler tick loop as a background task on the **host's**
    /// tokio runtime. Idempotent — calling it while the loop is already
    /// running is a no-op; if a previous loop died (e.g. panicked), a new one
    /// is spawned so scheduling does not silently stop. Must be called from
    /// within a tokio runtime.
    pub fn start(&self) {
        // A concurrent start already owns this short critical section, or a
        // shutdown is draining the task and the engine is terminal.
        let Ok(mut guard) = self.inner.tick_task.try_lock() else {
            return;
        };
        if self.inner.cancel.is_cancelled() {
            return;
        }
        if let Some(task) = guard.as_ref()
            && !task.is_finished()
        {
            return;
        }
        let storage = Arc::clone(&self.inner.storage);
        let handlers = Arc::clone(&self.inner.handlers);
        let config = self.inner.config.clone();
        let cancel = self.inner.cancel.clone();
        *guard = Some(tokio::spawn(async move {
            if let Err(e) = run_tick_loop(storage, handlers, &config, cancel).await {
                tracing::error!(error = %e, "orch8 tick loop exited with error");
            }
        }));
    }

    /// Gracefully shut down: cancel the tick loop and wait for in-flight
    /// steps to drain (bounded by the scheduler's grace period). Terminal —
    /// the engine cannot be restarted afterwards. Concurrent callers wait for
    /// the same drain; cancelling a caller leaves it available to later callers.
    pub async fn shutdown(&self) {
        self.inner.cancel.cancel();
        // Keep the join handle in shared state while awaiting it. Cancelling
        // this caller releases the mutex without detaching the drain, and
        // concurrent callers wait for the same task to finish.
        let mut task = self.inner.tick_task.lock().await;
        if let Some(handle) = task.as_mut() {
            if let Err(error) = handle.await {
                tracing::error!(%error, "orch8 tick task failed during shutdown");
            }
            task.take();
        }
    }

    /// Stop execution and gracefully close this engine's database connections.
    ///
    /// All clones share the closed storage. Use [`Self::shutdown`] when only
    /// stopping execution while retaining database access for inspection.
    pub async fn close(&self) {
        self.shutdown().await;
        self.inner.storage_owner.close().await;
    }

    /// Execute a single scheduling pass and return what it did. For hosts
    /// that drive the engine manually (test harnesses, cooperative
    /// schedulers) instead of running the background loop.
    pub async fn tick_once(&self) -> Result<TickOnceResult, Error> {
        if self.inner.cancel.is_cancelled() {
            return Err(orch8_engine::error::EngineError::ShuttingDown.into());
        }
        let result = tick_once(
            &self.inner.storage,
            &self.inner.handlers,
            &self.inner.semaphore,
            &self.inner.config,
            &self.inner.sequence_cache,
            &self.inner.cancel,
        )
        .await?;
        Ok(result)
    }

    /// Load an instance only if it belongs to this engine's tenant (M6).
    /// A foreign-tenant instance is reported as `NotFound`, never leaked.
    /// Every id-addressed facade call goes through this helper.
    async fn scoped_instance(&self, id: InstanceId) -> Result<TaskInstance, Error> {
        self.inner
            .storage
            .get_instance(id)
            .await?
            .filter(|instance| instance.tenant_id == self.inner.tenant)
            .ok_or_else(|| Error::NotFound(format!("instance {id}")))
    }

    /// Register a sequence definition (idempotently).
    ///
    /// The sequence is always stored under this engine's tenant: its
    /// `tenant_id` is overwritten with [`Self::tenant`] (M6), so an embedder
    /// cannot write into, or collide with, another tenant's sequences.
    ///
    /// The definition is validated, then stored. If a sequence with the same
    /// `(tenant_id, namespace, name, version)` already exists, the existing
    /// definition is left untouched and its id is returned — published
    /// versions are immutable so running instances stay pinned to the
    /// definition they started with. Publish changes by bumping `version`.
    pub async fn upsert_sequence(&self, mut seq: SequenceDefinition) -> Result<SequenceId, Error> {
        seq.tenant_id = self.inner.tenant.clone();
        seq.validate()
            .map_err(|e| Error::InvalidSequence(e.to_string()))?;
        orch8_engine::input_schema::validate_config_schema(seq.config_schema())?;
        orch8_engine::input_schema::validate_state_schema(seq.state_schema())?;
        if let Some(existing) = self
            .inner
            .storage
            .get_sequence_by_name(&seq.tenant_id, &seq.namespace, &seq.name, Some(seq.version))
            .await?
        {
            return Ok(existing.id);
        }
        orch8_engine::handlers::wasm_plugin::pin_sequence_modules(
            self.inner.storage.as_ref(),
            &mut seq,
        )
        .await?;
        // Concurrent upserts of the same `(name, version)` can both miss the
        // lookup above; on backends with a unique index the loser gets a
        // conflict. Resolve it to the winner's id, matching the doc contract.
        if let Err(error) = self.inner.storage.create_sequence(&seq).await {
            if matches!(error, StorageError::Conflict(_))
                && let Some(existing) = self
                    .inner
                    .storage
                    .get_sequence_by_name(
                        &seq.tenant_id,
                        &seq.namespace,
                        &seq.name,
                        Some(seq.version),
                    )
                    .await?
            {
                return Ok(existing.id);
            }
            return Err(error.into());
        }
        Ok(seq.id)
    }

    /// Create (start) a workflow instance of `sequence_id`.
    ///
    /// The instance is persisted in `Scheduled` state and picked up by the
    /// next tick. If `opts.idempotency_key` matches an existing instance of
    /// this tenant, that instance's id is returned instead of creating a
    /// duplicate.
    pub async fn create_instance(
        &self,
        sequence_id: SequenceId,
        opts: CreateInstanceOptions,
    ) -> Result<InstanceId, Error> {
        let now = Utc::now();
        let sequence = self.require_sequence(sequence_id).await?;

        // Normalize an empty key to `None`: the unique index on
        // `(tenant_id, idempotency_key)` includes every non-NULL value, so
        // storing `""` would make a *second* create with an empty key fail
        // with a constraint violation even though the caller opted out of
        // idempotency.
        let idempotency_key = opts.idempotency_key.as_deref().filter(|k| !k.is_empty());

        if let Some(key) = idempotency_key
            && let Some(existing) = self
                .inner
                .storage
                .find_by_idempotency_key(&self.inner.tenant, key)
                .await?
        {
            return Ok(existing.id);
        }

        orch8_engine::input_schema::validate_config(
            sequence.config_schema(),
            &opts.context.config,
        )?;
        let instance = self.new_instance(sequence_id, opts, now);

        // Resolve an overlapping create which committed after our key lookup.
        if let Err(error) = self.inner.storage.create_instance(&instance).await {
            if let Some(key) = &instance.idempotency_key
                && matches!(error, StorageError::Conflict(_))
                && let Some(existing) = self
                    .inner
                    .storage
                    .find_by_idempotency_key(&self.inner.tenant, key)
                    .await?
            {
                return Ok(existing.id);
            }
            return Err(error.into());
        }
        Ok(instance.id)
    }

    /// Atomically create all missing instances in a batch of at most 1000.
    /// Every input requires a nonempty idempotency key. The returned IDs
    /// follow input order, and repeated keys reuse the first instance.
    pub async fn create_instance_batch(
        &self,
        items: Vec<(SequenceId, CreateInstanceOptions)>,
    ) -> Result<Vec<InstanceId>, Error> {
        if items.len() > 1000 {
            return Err(Error::Config(
                "instance batch may contain at most 1000 entries".into(),
            ));
        }
        let now = Utc::now();
        let mut checked_sequences = HashMap::new();
        let mut seen = HashSet::new();
        let mut positions = Vec::with_capacity(items.len());
        let mut pending = Vec::new();
        for (index, (sequence, options)) in items.into_iter().enumerate() {
            let key = options
                .idempotency_key
                .as_ref()
                .filter(|key| !key.is_empty())
                .ok_or_else(|| {
                    Error::Config(format!(
                        "batch item {index} requires a nonempty idempotency key"
                    ))
                })?;
            if let std::collections::hash_map::Entry::Vacant(entry) =
                checked_sequences.entry(sequence)
            {
                entry.insert(
                    self.require_sequence(sequence)
                        .await?
                        .config_schema()
                        .cloned(),
                );
            }
            positions.push(key.clone());
            if seen.insert(key.clone()) {
                pending.push(self.new_instance(sequence, options, now));
            }
        }

        let mut resolved = HashMap::with_capacity(seen.len());
        let mut conflict = None;
        while !pending.is_empty() {
            let previous_len = pending.len();
            let mut missing = Vec::with_capacity(previous_len);
            for instance in pending {
                let key = instance
                    .idempotency_key
                    .as_ref()
                    .expect("batch keys are required");
                if let Some(existing) = self
                    .inner
                    .storage
                    .find_by_idempotency_key(&self.inner.tenant, key)
                    .await?
                {
                    resolved.insert(key.clone(), existing.id);
                } else {
                    let schema = checked_sequences
                        .get(&instance.sequence_id)
                        .and_then(Option::as_ref);
                    orch8_engine::input_schema::validate_config(schema, &instance.context.config)?;
                    missing.push(instance);
                }
            }
            if missing.is_empty() {
                break;
            }
            if let Some(error) = conflict.take()
                && missing.len() == previous_len
            {
                return Err(error);
            }
            match self.inner.storage.create_instances_batch(&missing).await {
                Ok(_) => {
                    for instance in missing {
                        resolved.insert(
                            instance.idempotency_key.expect("batch keys are required"),
                            instance.id,
                        );
                    }
                    break;
                }
                Err(error @ StorageError::Conflict(_)) => {
                    // The unsuccessful transaction inserted nothing. Retry only
                    // if the next lookup resolves at least one more raced key.
                    conflict = Some(error.into());
                    pending = missing;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(positions.iter().map(|key| resolved[key]).collect())
    }

    async fn require_sequence(&self, sequence_id: SequenceId) -> Result<SequenceDefinition, Error> {
        self.inner
            .storage
            .get_sequence(sequence_id)
            .await?
            .filter(|sequence| sequence.tenant_id == self.inner.tenant)
            .ok_or_else(|| Error::NotFound(format!("sequence {}", sequence_id.into_uuid())))
    }

    fn new_instance(
        &self,
        sequence_id: SequenceId,
        opts: CreateInstanceOptions,
        now: DateTime<Utc>,
    ) -> TaskInstance {
        let mut context = opts.context;
        orch8_storage::externalizing::escape_literals(&mut context.data);
        TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: InstanceId::new(),
            sequence_id,
            tenant_id: self.inner.tenant.clone(),
            namespace: opts.namespace,
            state: InstanceState::Scheduled,
            next_fire_at: Some(
                opts.next_fire_at
                    .unwrap_or_else(|| self.inner.config.clock.now()),
            ),
            priority: opts.priority,
            timezone: opts.timezone,
            metadata: opts.metadata,
            context,
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: opts.idempotency_key.filter(|key| !key.is_empty()),
            session_id: None,
            parent_instance_id: None,
            budget: opts.budget,
            created_at: now,
            updated_at: now,
        }
    }

    /// Current result per block, including retry diagnostics. In-progress
    /// markers and interceptor/SLA metadata are excluded. Ordered by creation
    /// time and UUID, with historical payloads omitted by the storage query.
    pub async fn current_block_outputs(
        &self,
        id: InstanceId,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, Error> {
        self.scoped_instance(id).await?;
        Ok(self.inner.storage.get_latest_diagnostic_outputs(id).await?)
    }

    /// List every block output recorded for an instance so far, in storage
    /// order. Useful for progress reporting while an instance runs (each
    /// completed step persists one output row) and for inspecting results
    /// after it reaches a terminal state.
    pub async fn block_outputs(
        &self,
        id: InstanceId,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, Error> {
        self.scoped_instance(id).await?;
        Ok(self.inner.storage.get_all_outputs(id).await?)
    }

    /// Read one block attempt without decoding any unrelated outputs or context.
    /// When a loop repeats the same attempt number, return its latest output.
    pub async fn get_output(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
        attempt: u16,
    ) -> Result<Option<BlockOutput>, Error> {
        let mut output = self
            .inner
            .storage
            .get_scoped_block_output_attempt(&self.inner.tenant, instance_id, block_id, attempt)
            .await?;
        if let Some(row) = &mut output
            && let Some(reference) = row.output_ref.as_deref()
            && !matches!(reference, "__in_progress__" | "__retry__" | "__error__")
        {
            row.output = self
                .inner
                .storage
                .get_externalized_state(instance_id, reference)
                .await?
                .ok_or_else(|| Error::NotFound(format!("payload of block output {}", row.id)))?;
        }
        Ok(output)
    }

    /// List the durable effect ledger for an instance (oldest first).
    ///
    /// This includes planned, dispatched, committed, and ambiguous receipts,
    /// allowing an operator or host application to reconcile external side
    /// effects without inspecting Orch8's database directly. At most 1,000
    /// receipts are returned.
    pub async fn effect_receipts(&self, id: InstanceId) -> Result<Vec<EffectReceipt>, Error> {
        // Resolve through the tenant-scoped instance API first so an unknown
        // ID is distinguishable from a known instance with no effects.
        self.scoped_instance(id).await?;
        Ok(self
            .inner
            .storage
            .list_instance_effect_receipts(&self.inner.tenant, id, 1_000)
            .await?)
    }

    /// Seed a durable block output before manual execution starts.
    ///
    /// This is intentionally crate-private: the contract/time-travel runner
    /// uses it to model blocks completed before a selected checkpoint, while
    /// the public engine API must not let callers forge production evidence.
    pub(crate) async fn seed_block_output(&self, output: &BlockOutput) -> Result<(), Error> {
        self.inner.storage.save_block_output(output).await?;
        Ok(())
    }

    /// Fetch the current snapshot of an instance (state, context, timestamps).
    /// Instances of other tenants are reported as [`Error::NotFound`].
    pub async fn get_instance(&self, id: InstanceId) -> Result<TaskInstance, Error> {
        let mut instance = self.scoped_instance(id).await?;
        instance.context = orch8_engine::externalized::resolve_context_markers(
            self.inner.storage.as_ref(),
            id,
            instance.context,
        )
        .await?;
        Ok(instance)
    }

    /// List instances matching `filter` (most recently updated first,
    /// at most 100 rows). Use [`InstanceFilter::default`] to list everything.
    /// Always restricted to this engine's tenant: `filter.tenant_id` is
    /// overridden (M6).
    pub async fn list_instances(
        &self,
        filter: &InstanceFilter,
    ) -> Result<Vec<TaskInstance>, Error> {
        let filter = InstanceFilter {
            tenant_id: Some(self.inner.tenant.clone()),
            ..filter.clone()
        };
        let instances = self
            .inner
            .storage
            .list_instances(&filter, &Pagination::default().capped())
            .await?;
        Ok(instances)
    }

    /// List a page for this tenant in descending instance ID order. Pass the
    /// last returned ID as `before` to continue. The page is capped at 1000.
    pub async fn list_instances_before(
        &self,
        filter: &InstanceFilter,
        before: Option<InstanceId>,
        limit: u32,
    ) -> Result<Vec<TaskInstance>, Error> {
        let filter = InstanceFilter {
            tenant_id: Some(self.inner.tenant.clone()),
            ..filter.clone()
        };
        Ok(self
            .inner
            .storage
            .list_instances_keyset(&filter, before, limit.min(1000))
            .await?)
    }

    /// Ingest an external event for durable correlation (see the
    /// `wait_for_event` handler). Idempotent by `producer_event_id`:
    /// re-sending the same id is a safe no-op reported as `duplicate`.
    /// When the event satisfies a waiting block's join policy, the
    /// waiting instance resumes on the next tick with the matched
    /// payloads in `context.data[<block_id>]`.
    pub async fn ingest_event(
        &self,
        event_name: &str,
        producer_event_id: &str,
        correlation_key: &str,
        payload: serde_json::Value,
    ) -> Result<orch8_types::event_correlation::IngestOutcome, Error> {
        let envelope = orch8_engine::event_correlation::envelope(
            self.inner.tenant.as_str(),
            event_name,
            producer_event_id,
            correlation_key,
            payload,
        );
        orch8_engine::event_correlation::ingest(&self.inner.storage, envelope)
            .await
            .map_err(Error::Storage)
    }

    /// Apply a queued cancellation to a running workflow so a host handler
    /// can be notified while its physical callback is still active.
    pub async fn interrupt_running_instance(&self, id: InstanceId) -> Result<bool, Error> {
        let instance = self.scoped_instance(id).await?;
        Ok(orch8_engine::scheduler::interrupt_running_instance(
            &self.inner.storage,
            &self.inner.handlers,
            instance,
        )
        .await?)
    }

    /// Send a signal to a live instance: `Pause`, `Resume`, `Cancel`,
    /// `UpdateContext`, or `Custom` (e.g. to resolve a `wait_for_input`
    /// step). Wakes the instance so the signal is processed on the next tick.
    ///
    /// Fails with [`Error::TerminalInstance`] if the instance has already
    /// completed, failed or been cancelled.
    pub async fn send_signal(
        &self,
        id: InstanceId,
        signal_type: SignalType,
        payload: serde_json::Value,
    ) -> Result<(), Error> {
        self.scoped_instance(id).await?;
        let signal = Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: id,
            signal_type,
            payload,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        };

        // Atomic enqueue: rejects if the instance is terminal (or missing),
        // mirroring the HTTP API's behavior.
        self.inner
            .storage
            .enqueue_signal_if_active(&signal)
            .await
            .map_err(|e| match e {
                StorageError::TerminalTarget { .. } => Error::TerminalInstance(id.to_string()),
                StorageError::NotFound { .. } => Error::NotFound(format!("instance {id}")),
                other => Error::Storage(other),
            })?;

        // Wake a Scheduled instance whose next_fire_at is in the future so
        // the signal is handled promptly (critical for human-input waits).
        // Best-effort: a failure only delays handling until the scheduled
        // fire time, so log instead of failing the send.
        if let Ok(Some(fresh)) = self.inner.storage.get_instance(id).await
            && fresh.state == InstanceState::Scheduled
        {
            let now = self.inner.config.clock.now();
            // CAS on Scheduled: between the re-fetch above and this write the
            // instance may have transitioned (e.g. to Running or a terminal
            // state) — an unconditional update would clobber that transition
            // and resurrect the row as Scheduled.
            match self
                .inner
                .storage
                .conditional_update_instance_state(
                    id,
                    InstanceState::Scheduled,
                    InstanceState::Scheduled,
                    Some(now),
                )
                .await
            {
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, instance_id = %id, "orch8: failed to wake instance for pending signal");
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Install a scheduler task whose completion is controlled by the test.
    async fn blocked_tick_loop() -> (Engine, Arc<tokio::sync::Notify>) {
        let storage = crate::Storage::sqlite_in_memory().connect().await.unwrap();
        let engine = Engine::from_parts(
            storage,
            HandlerRegistry::new(),
            SchedulerConfig::default(),
            TenantId::unchecked("default"),
        );
        let release = Arc::new(tokio::sync::Notify::new());
        let wait = Arc::clone(&release);
        *engine.inner.tick_task.lock().await = Some(tokio::spawn(async move {
            wait.notified().await;
        }));
        (engine, release)
    }

    #[tokio::test]
    async fn concurrent_shutdown_callers_wait_for_the_same_drain() {
        let (engine, release) = blocked_tick_loop().await;
        let first_engine = engine.clone();
        let first = tokio::spawn(async move { first_engine.shutdown().await });
        // On the current-thread runtime the first caller reaches its join
        // await before this cancellation waiter is polled again.
        engine.inner.cancel.cancelled().await;
        let second = engine.shutdown();
        tokio::pin!(second);
        tokio::select! {
            biased;
            () = &mut second => panic!("a concurrent shutdown returned before drain completed"),
            () = tokio::task::yield_now() => {}
        }
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            first.await.unwrap();
            second.await;
        })
        .await
        .expect("all shutdown callers finish after drain");
    }

    #[tokio::test]
    async fn cancelling_shutdown_preserves_the_join_for_a_later_caller() {
        let (engine, release) = blocked_tick_loop().await;
        let first_engine = engine.clone();
        let first = tokio::spawn(async move { first_engine.shutdown().await });
        engine.inner.cancel.cancelled().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second = engine.shutdown();
        tokio::pin!(second);
        tokio::select! {
            biased;
            () = &mut second => panic!("cancelling a caller detached the scheduler drain"),
            () = tokio::task::yield_now() => {}
        }
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("a later caller can finish the original drain");
    }

    /// L1: dropping the last handle cancels the background tick loop.
    #[tokio::test]
    async fn dropping_last_handle_cancels_tick_loop() {
        let storage = crate::Storage::sqlite_in_memory().connect().await.unwrap();
        let engine = Engine::from_parts(
            storage,
            HandlerRegistry::new(),
            SchedulerConfig::default(),
            TenantId::unchecked("default"),
        );
        engine.start();
        let token = engine.inner.cancel.clone();
        let task = engine
            .inner
            .tick_task
            .lock()
            .await
            .as_ref()
            .map(JoinHandle::abort_handle)
            .unwrap();
        let clone = engine.clone();
        drop(engine);
        assert!(
            !token.is_cancelled(),
            "a live clone keeps the engine running"
        );
        drop(clone);
        assert!(token.is_cancelled());
        // The loop observes the cancellation and exits on its own.
        for _ in 0..200 {
            if task.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("tick loop kept running after the engine was dropped");
    }
}
