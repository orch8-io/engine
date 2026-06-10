use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::{run_tick_loop, tick_once, TickOnceResult};
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{Budget, InstanceState, Priority, TaskInstance};
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

    pub(crate) fn from_parts(
        storage: Arc<dyn StorageBackend>,
        handlers: HandlerRegistry,
        config: SchedulerConfig,
        tenant: TenantId,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_steps as usize));
        Self {
            inner: Arc::new(Inner {
                storage,
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
    /// running is a no-op. Must be called from within a tokio runtime.
    pub fn start(&self) {
        let mut guard = self
            .inner
            .tick_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.is_some() || self.inner.cancel.is_cancelled() {
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
    /// the engine cannot be restarted afterwards.
    pub async fn shutdown(&self) {
        self.inner.cancel.cancel();
        let task = self
            .inner
            .tick_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// Execute a single scheduling pass and return what it did. For hosts
    /// that drive the engine manually (test harnesses, cooperative
    /// schedulers) instead of running the background loop.
    pub async fn tick_once(&self) -> Result<TickOnceResult, Error> {
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

    /// Register a sequence definition (idempotently).
    ///
    /// The definition is validated, then stored. If a sequence with the same
    /// `(tenant_id, namespace, name, version)` already exists, the existing
    /// definition is left untouched and its id is returned — published
    /// versions are immutable so running instances stay pinned to the
    /// definition they started with. Publish changes by bumping `version`.
    pub async fn upsert_sequence(&self, seq: SequenceDefinition) -> Result<SequenceId, Error> {
        seq.validate()
            .map_err(|e| Error::InvalidSequence(e.to_string()))?;
        if let Some(existing) = self
            .inner
            .storage
            .get_sequence_by_name(&seq.tenant_id, &seq.namespace, &seq.name, Some(seq.version))
            .await?
        {
            return Ok(existing.id);
        }
        self.inner.storage.create_sequence(&seq).await?;
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

        // Resolve the sequence up-front so a missing id surfaces as NotFound
        // rather than a foreign-key violation from the storage layer.
        self.inner
            .storage
            .get_sequence(sequence_id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("sequence {}", sequence_id.into_uuid())))?;

        if let Some(ref key) = opts.idempotency_key {
            if !key.is_empty() {
                if let Some(existing) = self
                    .inner
                    .storage
                    .find_by_idempotency_key(&self.inner.tenant, key)
                    .await?
                {
                    return Ok(existing.id);
                }
            }
        }

        let instance = TaskInstance {
            id: InstanceId::new(),
            sequence_id,
            tenant_id: self.inner.tenant.clone(),
            namespace: opts.namespace,
            state: InstanceState::Scheduled,
            next_fire_at: Some(opts.next_fire_at.unwrap_or(now)),
            priority: opts.priority,
            timezone: opts.timezone,
            metadata: opts.metadata,
            context: opts.context,
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: opts.idempotency_key,
            session_id: None,
            parent_instance_id: None,
            budget: opts.budget,
            created_at: now,
            updated_at: now,
        };

        self.inner.storage.create_instance(&instance).await?;
        Ok(instance.id)
    }

    /// List every block output recorded for an instance so far, in storage
    /// order. Useful for progress reporting while an instance runs (each
    /// completed step persists one output row) and for inspecting results
    /// after it reaches a terminal state.
    pub async fn block_outputs(
        &self,
        id: InstanceId,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, Error> {
        Ok(self.inner.storage.get_all_outputs(id).await?)
    }

    /// Fetch the current snapshot of an instance (state, context, timestamps).
    pub async fn get_instance(&self, id: InstanceId) -> Result<TaskInstance, Error> {
        self.inner
            .storage
            .get_instance(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("instance {id}")))
    }

    /// List instances matching `filter` (most recently updated first,
    /// at most 100 rows). Use [`InstanceFilter::default`] to list everything.
    pub async fn list_instances(
        &self,
        filter: &InstanceFilter,
    ) -> Result<Vec<TaskInstance>, Error> {
        let instances = self
            .inner
            .storage
            .list_instances(filter, &Pagination::default().capped())
            .await?;
        Ok(instances)
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
        if let Ok(Some(fresh)) = self.inner.storage.get_instance(id).await {
            if fresh.state == InstanceState::Scheduled {
                let _ = self
                    .inner
                    .storage
                    .update_instance_state(id, InstanceState::Scheduled, Some(Utc::now()))
                    .await;
            }
        }

        Ok(())
    }
}
