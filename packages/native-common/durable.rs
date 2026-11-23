//! Host-language-neutral core of the durable, in-process engine exposed by
//! `@orch8/engine-native` (napi) and `orch8-engine-native` (`PyO3`).
//!
//! Both bindings include this file with `#[path = ...]` so the semantics are
//! identical: JSON strings in, JSON strings out, and host handlers invoked
//! through a JSON envelope. Nothing here depends on napi or `PyO3`.
//!
//! Handler protocol (per step dispatch):
//! - request: `{"params", "data", "outputs", "instance_id", "step_id", "attempt"}`
//!   where `outputs` maps every already-completed step id to its output;
//! - reply: `{"ok": <output>}` or `{"error": {"message", "permanent"}}`.
//!   A non-permanent error (or a reply that cannot be decoded) is
//!   `StepError::Retryable`, a permanent one `StepError::Permanent`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orch8::{
    CreateInstanceOptions, Engine, EngineError, ExecutionContext, InstanceFilter, InstanceId,
    InstanceState, Namespace, SequenceId, SignalType, StepContext, StepError, Storage,
};
use serde_json::{Map, Value, json};
use tokio::sync::Mutex as AsyncMutex;

pub(crate) mod callbacks;
pub(crate) mod database;
pub use callbacks::Invocation;
mod passes;

/// Future returned by a host handler invocation.
pub type InvokeFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
/// A host handler: takes the JSON request, resolves to the JSON reply.
pub type Invoke = Arc<dyn Fn(Invocation) -> InvokeFuture + Send + Sync>;

#[derive(Debug)]
pub enum RunError {
    ObserverTimeout,
    Engine(String),
}

pub const OBSERVER_TIMEOUT_MESSAGE: &str = "run observer deadline elapsed";

#[derive(Clone)]
struct Deployed {
    id: SequenceId,
    namespace: Namespace,
    version: i32,
}

#[derive(Default)]
struct HandlerRegistrations {
    frozen: bool,
    entries: Vec<(String, Invoke)>,
}

/// A durable engine backed by one `SQLite` file owned by this process.
///
/// The Rust engine is built lazily on first use so host handlers can be
/// registered after `open` (handlers are fixed once the engine is built).
pub struct DurableEngine {
    path: String,
    handlers: Mutex<HandlerRegistrations>,
    engine: AsyncMutex<Option<Engine>>,
    sequences: Mutex<HashMap<String, Vec<Deployed>>>,
    closed: Arc<AtomicBool>,
    passes: passes::Passes,
    callbacks: Arc<callbacks::Budget>,
    callback_drain: Arc<callbacks::Drain>,
    database: Mutex<Option<Arc<database::Owner>>>,
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl DurableEngine {
    pub fn new(path: impl Into<String>) -> Self {
        Self::with_pass_limit(
            path,
            orch8_types::config::SchedulerConfig::default().max_concurrent_steps as usize,
        )
    }

    /// Trusted construction budget; bindings use the scheduler's own ceiling.
    pub(crate) fn with_pass_limit(path: impl Into<String>, pass_limit: usize) -> Self {
        Self {
            path: path.into(),
            handlers: Mutex::new(HandlerRegistrations::default()),
            engine: AsyncMutex::new(None),
            sequences: Mutex::new(HashMap::new()),
            closed: Arc::new(AtomicBool::new(false)),
            passes: passes::Passes::new(pass_limit),
            callbacks: callbacks::Budget::shared(),
            callback_drain: Arc::default(),
            database: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_callback_budget(path: &str, callbacks: Arc<callbacks::Budget>) -> Self {
        let mut engine = Self::new(path);
        engine.callbacks = callbacks;
        engine
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Register a host handler. Fails once the engine has been used.
    pub fn register(&self, name: &str, invoke: Invoke) -> Result<(), String> {
        let closed = self.closed.load(Ordering::Acquire);
        if closed {
            return Err("engine is closed".into());
        }
        if name.is_empty() {
            return Err("handler name must not be empty".into());
        }
        let mut handlers = lock(&self.handlers);
        if handlers.frozen {
            return Err(format!(
                "cannot register handler '{name}': handlers must be registered before the first \
                 deploy/start/run/get call"
            ));
        }
        let retired = handlers
            .entries
            .iter()
            .position(|(existing, _)| existing == name)
            .map(|index| handlers.entries.remove(index));
        handlers.entries.push((name.to_string(), invoke));
        drop(handlers);
        // Host captures can run arbitrary finalizers, including another engine
        // call. Publish the replacement, then retire its predecessor unlocked.
        drop(retired);
        Ok(())
    }

    async fn engine(&self) -> Result<Engine, String> {
        // Serialize initialization with close. Return a cheap Engine clone so
        // close can remove the owned cache and release idle database handles.
        let mut cached = self.engine.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err("engine is closed".into());
        }
        if let Some(engine) = cached.as_ref() {
            return Ok(engine.clone());
        }
        // Refuse a second owner before opening SQLite or running recovery.
        // The owned build retains this reservation through cancellation.
        let database = database::Owner::acquire(&self.path)?;
        let handlers = {
            let mut handlers = lock(&self.handlers);
            handlers.frozen = true;
            // Keep a retry copy until initialization succeeds: build may fail
            // or its caller may be cancelled after registration freezes.
            handlers.entries.clone()
        };
        let mut builder = Engine::builder()
            .storage(
                database
                    .as_ref()
                    .map_or_else(Storage::sqlite_in_memory, |owner| {
                        Storage::sqlite(owner.path())
                    }),
            )
            // Single-process ownership: anything left Running was interrupted
            // by a crash, so resume it immediately on initialization.
            .stale_instance_threshold(Duration::ZERO);
        for (name, invoke) in handlers {
            builder = builder.deferred_effect_handler(
                &name,
                owned_host_handler_with_budget(
                    invoke,
                    self.closed.clone(),
                    self.callbacks.clone(),
                    self.callback_drain.clone(),
                ),
            );
        }
        let (engine, database) = database::initialize(database, builder.build()).await?;
        *lock(&self.database) = database;
        *cached = Some(engine.clone());
        // The cached engine now owns the callbacks. A second copy would
        // unnecessarily retain host resources after the engine is closed.
        let registrations = std::mem::take(&mut lock(&self.handlers).entries);
        // close may have been called while storage was opening. Do not hand
        // the new engine to a caller after close stopped accepting work.
        let closed = self.closed.load(Ordering::Acquire);
        if closed {
            engine.close().await;
            self.release_database();
            let retired = cached.take();
            drop(cached);
            drop(retired);
            drop(registrations);
            return Err("engine is closed".into());
        }
        drop(cached);
        drop(registrations);
        Ok(engine)
    }

    /// Store a sequence (idempotent per `name` + `version`) and return its id.
    ///
    /// Missing `id`, `tenant_id`, `namespace`, `version` and `created_at`
    /// fields are filled in, so a definition only needs `name` and `blocks`.
    pub async fn deploy(&self, sequence_json: &str) -> Result<String, String> {
        let mut value: Value = serde_json::from_str(sequence_json)
            .map_err(|e| format!("invalid sequence JSON: {e}"))?;
        let object = value
            .as_object_mut()
            .ok_or("a sequence must be a JSON object")?;
        let defaults = [
            ("id", serde_json::to_value(SequenceId::new()).map_err(err)?),
            ("tenant_id", json!("default")),
            ("namespace", json!("default")),
            ("version", json!(1)),
            ("created_at", json!(chrono::Utc::now().to_rfc3339())),
        ];
        for (key, default) in defaults {
            object.entry(key).or_insert(default);
        }
        let sequence = orch8_types::sequence::deserialize_sequence_strict(&value).map_err(err)?;
        let (name, namespace, version) = (
            sequence.name.clone(),
            sequence.namespace.clone(),
            sequence.version,
        );
        let id = self
            .engine()
            .await?
            .upsert_sequence(sequence)
            .await
            .map_err(err)?;
        let mut sequences = lock(&self.sequences);
        let versions = sequences.entry(name).or_default();
        versions.retain(|deployed| deployed.version != version);
        versions.push(Deployed {
            id,
            namespace,
            version,
        });
        Ok(id.to_string())
    }

    /// Start an instance of a deployed sequence (latest deployed version
    /// unless `version` is given). With an idempotency key, starting again
    /// returns the existing instance id instead of creating a duplicate.
    pub async fn start(
        &self,
        name: &str,
        input_json: Option<&str>,
        idempotency_key: Option<String>,
        version: Option<i32>,
    ) -> Result<String, String> {
        let engine = self.engine().await?;
        let deployed = self.deployed_sequence(name, version)?;
        let data = match input_json.map(serde_json::from_str::<Value>).transpose() {
            Ok(None | Some(Value::Null)) => json!({}),
            Ok(Some(value @ Value::Object(_))) => value,
            Ok(Some(_)) => return Err("input must be a JSON object".into()),
            Err(e) => return Err(format!("invalid input JSON: {e}")),
        };
        let id = engine
            .create_instance(
                deployed.id,
                CreateInstanceOptions {
                    namespace: deployed.namespace,
                    context: ExecutionContext {
                        data,
                        ..Default::default()
                    },
                    idempotency_key,
                    ..Default::default()
                },
            )
            .await
            .map_err(err)?;
        Ok(id.to_string())
    }

    pub async fn start_batch(&self, items_json: &str) -> Result<String, String> {
        let values: Vec<Value> = serde_json::from_str(items_json)
            .map_err(|error| format!("invalid start batch: {error}"))?;
        if values.len() > 1000 {
            return Err("instance batch may contain at most 1000 entries".into());
        }
        let mut items = Vec::with_capacity(values.len());
        for (index, value) in values.into_iter().enumerate() {
            let item = (|| {
                let Value::Object(mut object) = value else {
                    return Err("expected an object".into());
                };
                let name = object
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or("name is required")?;
                let version = object
                    .get("version")
                    .map(|value| serde_json::from_value::<Option<i32>>(value.clone()).map_err(err))
                    .transpose()?
                    .flatten();
                let deployed = self.deployed_sequence(name, version)?;
                let key = match object.remove("idempotency_key") {
                    Some(Value::String(key)) if !key.is_empty() => key,
                    _ => return Err("a nonempty idempotency key is required".into()),
                };
                let data = match object.remove("input") {
                    None | Some(Value::Null) => json!({}),
                    Some(value @ Value::Object(_)) => value,
                    _ => return Err("input must be a JSON object".into()),
                };
                Ok((
                    deployed.id,
                    CreateInstanceOptions {
                        namespace: deployed.namespace,
                        context: ExecutionContext {
                            data,
                            ..Default::default()
                        },
                        idempotency_key: Some(key),
                        ..Default::default()
                    },
                ))
            })()
            .map_err(|error: String| format!("batch item {index}: {error}"))?;
            items.push(item);
        }
        let ids = self
            .engine()
            .await?
            .create_instance_batch(items)
            .await
            .map_err(err)?;
        serde_json::to_string(&ids).map_err(err)
    }

    fn deployed_sequence(&self, name: &str, version: Option<i32>) -> Result<Deployed, String> {
        let sequences = lock(&self.sequences);
        let versions = sequences.get(name).map(Vec::as_slice).unwrap_or_default();
        match version {
            Some(v) => versions.iter().find(|d| d.version == v).cloned(),
            None => versions.iter().max_by_key(|d| d.version).cloned(),
        }
        .ok_or_else(|| {
            format!(
                "sequence '{name}' is not deployed by this engine; call deploy() first \
                 (deploy is idempotent, so do it on every startup)"
            )
        })
    }

    /// Bound observation independently of the scheduling budget. A timeout
    /// leaves any admitted scheduler pass in its original owner.
    pub async fn run_with_observer_deadline(
        self: &Arc<Self>,
        id: &str,
        scheduling_timeout: Option<Duration>,
        observer_timeout: Option<Duration>,
    ) -> Result<String, RunError> {
        let Some(timeout) = observer_timeout else {
            return self
                .run(id, scheduling_timeout)
                .await
                .map_err(RunError::Engine);
        };
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            RunError::Engine("observer timeout is too large for the monotonic clock".into())
        })?;
        if timeout.is_zero() {
            return Err(RunError::ObserverTimeout);
        }
        let engine = self.clone();
        let id = id.to_owned();
        // SQL decoding can do synchronous work after I/O resolves. Keep it
        // outside the task that observes the deadline.
        let mut observation = tokio::spawn(async move {
            engine
                .run_until(&id, scheduling_timeout, Some(deadline))
                .await
        });
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline.into()) => {
                observation.abort();
                Err(RunError::ObserverTimeout)
            },
            result = &mut observation => {
                if Instant::now() >= deadline {
                    Err(RunError::ObserverTimeout)
                } else {
                    result.map_err(|error| RunError::Engine(format!("run observation failed: {error}")))?
                        .map_err(RunError::Engine)
                }
            }
        }
    }

    /// Drive scheduling and read the instance snapshot once it settles or the
    /// scheduling budget expires.
    pub async fn run(&self, id: &str, timeout: Option<Duration>) -> Result<String, String> {
        self.run_until(id, timeout, None).await
    }

    async fn run_until(
        &self,
        id: &str,
        timeout: Option<Duration>,
        observer_deadline: Option<Instant>,
    ) -> Result<String, String> {
        let id = parse_instance_id(id)?;
        let now = Instant::now();
        let deadline = timeout
            .map(|duration| {
                now.checked_add(duration)
                    .ok_or_else(|| "run timeout is too large for the monotonic clock".to_string())
            })
            .transpose()?
            .into_iter()
            .chain(observer_deadline)
            .min();
        let engine = self.engine().await?;
        let mut observed_unparked = false;
        loop {
            // Validate the target and budget before a pass can dispatch other
            // queued instances. Waiting/paused targets still need a pass to
            // process queued signals; terminal targets cannot resume.
            let instance = engine.get_instance(id).await.map_err(err)?;
            let parked = matches!(
                instance.state,
                InstanceState::Waiting | InstanceState::Paused
            );
            observed_unparked |= !parked;
            if instance.state.is_terminal() || deadline.is_some_and(|d| Instant::now() >= d) {
                return self.snapshot(&engine, id).await;
            }
            if parked && observed_unparked {
                return self.snapshot(&engine, id).await;
            }
            let allow_new = instance.state != InstanceState::Running;
            let Some(pass) = self.passes.acquire(
                engine.clone(),
                id,
                allow_new,
                deadline,
                lock(&self.database).clone(),
            )?
            else {
                return self.snapshot(&engine, id).await;
            };
            let tick = match pass.wait_until(deadline).await? {
                passes::Wait::Finished(tick) => tick,
                passes::Wait::Refresh => continue,
                passes::Wait::Deadline => return self.snapshot(&engine, id).await,
            };
            let instance = engine.get_instance(id).await.map_err(err)?;
            let settled = instance.state.is_terminal()
                || matches!(
                    instance.state,
                    InstanceState::Waiting | InstanceState::Paused
                );
            if settled || deadline.is_some_and(|d| Instant::now() >= d) {
                return self.snapshot(&engine, id).await;
            }
            if tick.steps_executed == 0 && tick.instances_advanced == 0 {
                // Nothing was due: sleep until the instance's next fire time
                // (retry backoff, delay step), polling at least every 200 ms.
                let wait = instance
                    .next_fire_at
                    .and_then(|at| (at - chrono::Utc::now()).to_std().ok())
                    .unwrap_or(Duration::from_millis(5));
                let mut wait = wait.clamp(Duration::from_millis(1), Duration::from_millis(200));
                if let Some(deadline) = deadline {
                    wait = wait.min(deadline.saturating_duration_since(Instant::now()));
                }
                tokio::time::sleep(wait).await;
            }
        }
    }

    /// Current snapshot: `{"id", "state", "data", "outputs"}`.
    pub async fn get(&self, id: &str) -> Result<String, String> {
        let engine = self.engine().await?;
        self.snapshot(&engine, parse_instance_id(id)?).await
    }

    pub async fn get_output(&self, id: &str, block: &str, attempt: u16) -> Result<String, String> {
        let engine = self.engine().await?;
        let output = engine
            .get_output(
                parse_instance_id(id)?,
                &orch8_types::ids::BlockId::new(block),
                attempt,
            )
            .await
            .map_err(err)?;
        serde_json::to_string(&output).map_err(err)
    }

    pub async fn list_instance_page(
        &self,
        state: Option<&str>,
        sequence_id: Option<&str>,
        before: Option<&str>,
        page_size: u32,
    ) -> Result<String, String> {
        if !(1..=1000).contains(&page_size) {
            return Err("page size must be between 1 and 1000".into());
        }
        let filter = InstanceFilter {
            states: state
                .map(str::parse::<InstanceState>)
                .transpose()?
                .map(|state| vec![state]),
            sequence_id: sequence_id
                .map(|id| {
                    serde_json::from_value(Value::String(id.into()))
                        .map_err(|_| format!("invalid sequence id '{id}'"))
                })
                .transpose()?,
            ..Default::default()
        };
        let before = before.map(parse_instance_id).transpose()?;
        let instances = self
            .engine()
            .await?
            .list_instances_before(&filter, before, page_size)
            .await
            .map_err(err)?;
        let summaries: Vec<_> = instances
            .into_iter()
            .map(|instance| {
                json!({
                    "id": instance.id,
                    "sequence_id": instance.sequence_id,
                    "state": instance.state,
                    "created_at": instance.created_at,
                })
            })
            .collect();
        serde_json::to_string(&summaries).map_err(err)
    }

    async fn snapshot(&self, engine: &Engine, id: InstanceId) -> Result<String, String> {
        let instance = engine.get_instance(id).await.map_err(err)?;
        let outputs = latest_outputs(engine.current_block_outputs(id).await.map_err(err)?, true);
        Ok(json!({
            "id": id.to_string(),
            "state": instance.state,
            "data": instance.context.data,
            "outputs": outputs,
        })
        .to_string())
    }

    /// Send `pause`, `resume`, `cancel`, `update_context` or any custom
    /// signal name (e.g. to resolve a `wait_for_input` step).
    pub async fn signal(
        &self,
        id: &str,
        signal: &str,
        payload_json: Option<&str>,
    ) -> Result<(), String> {
        let engine = self.engine().await?;
        let signal_type = match signal {
            "pause" => SignalType::Pause,
            "resume" => SignalType::Resume,
            "cancel" => SignalType::Cancel,
            "update_context" => SignalType::UpdateContext,
            custom => SignalType::Custom(custom.to_string()),
        };
        let payload = payload_json
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e| format!("invalid payload JSON: {e}"))?
            .unwrap_or(Value::Null);
        engine
            .send_signal(parse_instance_id(id)?, signal_type, payload)
            .await
            .map_err(err)
    }

    #[allow(dead_code)] // The Node binding exposes cooperative cancellation.
    pub async fn interrupt_running_instance(&self, id: &str) -> Result<bool, String> {
        self.engine()
            .await?
            .interrupt_running_instance(parse_instance_id(id)?)
            .await
            .map_err(err)
    }

    fn release_database(&self) {
        if let Some(owner) = lock(&self.database).take() {
            owner.release();
        }
    }

    /// Stop accepting calls and release the database. Idempotent.
    pub async fn close(&self) {
        self.close_with_drain(false).await;
    }

    /// Optionally retain database ownership until host invocations settle.
    pub async fn close_with_drain(&self, drain: bool) {
        self.callback_drain.close();
        self.closed.store(true, Ordering::Release);
        self.passes.stop();
        if drain {
            self.callback_drain.wait().await;
        }
        let mut cached = self.engine.lock().await;
        if let Some(engine) = cached.as_ref() {
            engine.close().await;
        }
        self.release_database();
        // Retain ownership until close completes so cancellation can retry.
        let retired_engine = cached.take();
        drop(cached);
        let retired_handlers = {
            let mut handlers = lock(&self.handlers);
            handlers.frozen = true;
            std::mem::take(&mut handlers.entries)
        };
        // Both cached engine destruction and unused registration destruction
        // can release the last host reference. Neither may hold lifecycle locks.
        drop(retired_engine);
        drop(retired_handlers);
    }
}

impl Drop for DurableEngine {
    fn drop(&mut self) {
        // Signal the owned pass without blocking a host finalizer. Its
        // coordinator requests engine shutdown and retains any live join.
        self.callback_drain.close();
        self.closed.store(true, Ordering::Release);
        self.passes.stop();
    }
}

fn parse_instance_id(id: &str) -> Result<InstanceId, String> {
    serde_json::from_value(Value::String(id.to_string()))
        .map_err(|_| format!("invalid instance id '{id}'"))
}

/// Completed results exclude internal rows; snapshots additionally keep retry diagnostics.
fn latest_outputs(outputs: Vec<orch8::BlockOutput>, retry_diagnostics: bool) -> Value {
    let mut map = Map::new();
    for output in outputs {
        let block = output.block_id.as_str();
        if output.output_ref.as_deref() == Some("__in_progress__")
            || (!retry_diagnostics && output.output_ref.as_deref() == Some("__retry__"))
            || block.starts_with("_interceptor:")
            || block.starts_with("_sla:")
        {
            continue;
        }
        map.insert(output.block_id.to_string(), output.output);
    }
    Value::Object(map)
}

#[cfg(test)]
pub(crate) fn host_handler(
    invoke: Invoke,
) -> impl Fn(StepContext) -> InvokeStepFuture + Send + Sync + 'static {
    owned_host_handler(invoke, Arc::new(AtomicBool::new(false)))
}

type InvokeStepFuture = Pin<Box<dyn Future<Output = Result<Value, StepError>> + Send>>;

#[cfg(test)]
pub(crate) fn owned_host_handler(
    invoke: Invoke,
    closed: Arc<AtomicBool>,
) -> impl Fn(StepContext) -> InvokeStepFuture + Send + Sync + 'static {
    owned_host_handler_with_budget(invoke, closed, callbacks::Budget::shared(), Arc::default())
}

fn owned_host_handler_with_budget(
    invoke: Invoke,
    closed: Arc<AtomicBool>,
    budget: Arc<callbacks::Budget>,
    drain: Arc<callbacks::Drain>,
) -> impl Fn(StepContext) -> InvokeStepFuture + Send + Sync + 'static {
    move |ctx: StepContext| {
        let invoke = Arc::clone(&invoke);
        let closed = Arc::clone(&closed);
        let budget = budget.clone();
        let drain = drain.clone();
        Box::pin(async move {
            if closed.load(Ordering::Acquire) {
                return Err(StepError::ClaimLost);
            }
            ctx.verify_claim().await?;
            let Some(physical) = budget.reserve() else {
                return Err(ctx.effect_admission.as_ref().map_or_else(
                    || retryable("native callback capacity is occupied".into()),
                    |admission| {
                        admission.reject_preflight(EngineError::AdmissionBusy {
                            instance_id: ctx.instance_id,
                        })
                    },
                ));
            };
            let outputs = ctx
                .storage
                .get_latest_completed_outputs(ctx.instance_id)
                .await;
            // Loading the request envelope is an asynchronous phase. Its
            // initiating claim must still own execution before host entry,
            // including when the load itself failed after ownership changed.
            ctx.verify_claim().await?;
            let outputs = outputs.map_err(|e| retryable(format!("loading step outputs: {e}")))?;
            let request = json!({
                "params": ctx.params,
                "data": ctx.context.data,
                "outputs": latest_outputs(outputs, false),
                "instance_id": ctx.instance_id.to_string(),
                "step_id": ctx.block_id.to_string(),
                "attempt": ctx.attempt,
            });
            let request = request.to_string();
            // Runtime shutdown need not retire a durable account/claim. Fence
            // factory entry with the local owner's retirement flag too.
            if closed.load(Ordering::Acquire) {
                return Err(StepError::ClaimLost);
            }
            if let Some(admission) = &ctx.effect_admission {
                admission.admit().await?;
                ctx.verify_claim().await?;
            }
            if closed.load(Ordering::Acquire) {
                return Err(StepError::ClaimLost);
            }
            let active = drain.enter().ok_or(StepError::ClaimLost)?;
            let reply = invoke(Invocation::new(request, physical, active))
                .await
                .map_err(retryable)?;
            decode_reply(&reply)
        })
    }
}

fn retryable(message: String) -> StepError {
    StepError::Retryable {
        message,
        details: None,
    }
}

fn decode_reply(reply: &str) -> Result<Value, StepError> {
    let mut value: Value = serde_json::from_str(reply)
        .map_err(|e| retryable(format!("handler reply is not JSON: {e}")))?;
    if let Some(ok) = value.get_mut("ok") {
        return Ok(ok.take());
    }
    let error = value.get("error").cloned().unwrap_or(Value::Null);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("handler failed")
        .to_string();
    let details = error.get("details").cloned().filter(|d| !d.is_null());
    if error.get("permanent").and_then(Value::as_bool) == Some(true) {
        Err(StepError::Permanent { message, details })
    } else {
        Err(StepError::Retryable { message, details })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transactional_start_batches_resolve_overlapping_idempotency_keys() {
        let engine = DurableEngine::new(":memory:");
        engine
            .deploy(r#"{"name":"batch","blocks":[{"type":"step","id":"one","handler":"noop"}]}"#)
            .await
            .unwrap();
        let left = r#"[{"name":"batch","idempotency_key":"left"},{"name":"batch","idempotency_key":"shared"}]"#;
        let right = r#"[{"name":"batch","idempotency_key":"shared"},{"name":"batch","idempotency_key":"right"}]"#;
        let (left, right) = tokio::join!(engine.start_batch(left), engine.start_batch(right));
        let left: Vec<String> = serde_json::from_str(&left.unwrap()).unwrap();
        let right: Vec<String> = serde_json::from_str(&right.unwrap()).unwrap();
        assert_eq!(left[1], right[0]);
        assert_ne!(left[0], right[1]);
        let rows = engine
            .list_instance_page(None, None, None, 10)
            .await
            .unwrap();
        assert_eq!(serde_json::from_str::<Vec<Value>>(&rows).unwrap().len(), 3);
        engine.close().await;
    }

    #[tokio::test]
    async fn instance_page_rejects_invalid_arguments_before_opening_database() {
        let engine = DurableEngine::new(":memory:");
        for (state, sequence, cursor, size, message) in [
            (None, None, None, 0, "page size"),
            (None, None, None, 1001, "page size"),
            (Some("bad"), None, None, 1, "unknown instance state"),
            (None, Some("bad"), None, 1, "invalid sequence id"),
            (None, None, Some("bad"), 1, "invalid instance id"),
        ] {
            let error = engine
                .list_instance_page(state, sequence, cursor, size)
                .await
                .unwrap_err();
            assert!(error.contains(message), "{error}");
        }
        assert!(engine.engine.lock().await.is_none());
    }

    #[tokio::test]
    async fn observer_deadline_bounds_waiting_for_engine_open() {
        let engine = Arc::new(DurableEngine::new(":memory:"));
        let opening = engine.engine.lock().await;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            engine.run_with_observer_deadline(
                &InstanceId::new().to_string(),
                None,
                Some(Duration::from_millis(10)),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(RunError::ObserverTimeout)));
        assert!(opening.is_none());
    }
}
