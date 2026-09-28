//! Host-language-neutral core of the durable, in-process engine exposed by
//! `@orch8/engine-native` (napi) and `orch8-engine-native` (PyO3).
//!
//! Both bindings include this file with `#[path = ...]` so the semantics are
//! identical: JSON strings in, JSON strings out, and host handlers invoked
//! through a JSON envelope. Nothing here depends on napi or PyO3.
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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orch8::{
    CreateInstanceOptions, Engine, ExecutionContext, InstanceId, InstanceState, Namespace,
    SequenceId, SignalType, StepContext, StepError, Storage,
};
use serde_json::{Map, Value, json};
use tokio::sync::OnceCell;

/// Future returned by a host handler invocation.
pub type InvokeFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
/// A host handler: takes the JSON request, resolves to the JSON reply.
pub type Invoke = Arc<dyn Fn(String) -> InvokeFuture + Send + Sync>;

#[derive(Clone)]
struct Deployed {
    id: SequenceId,
    namespace: Namespace,
    version: i32,
}

/// A durable engine backed by one `SQLite` file owned by this process.
///
/// The Rust engine is built lazily on first use so host handlers can be
/// registered after `open` (handlers are fixed once the engine is built).
pub struct DurableEngine {
    path: String,
    pending: Mutex<Option<Vec<(String, Invoke)>>>,
    engine: OnceCell<Engine>,
    sequences: Mutex<HashMap<String, Vec<Deployed>>>,
    closed: Mutex<bool>,
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
        Self {
            path: path.into(),
            pending: Mutex::new(Some(Vec::new())),
            engine: OnceCell::new(),
            sequences: Mutex::new(HashMap::new()),
            closed: Mutex::new(false),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Register a host handler. Fails once the engine has been used.
    pub fn register(&self, name: &str, invoke: Invoke) -> Result<(), String> {
        if name.is_empty() {
            return Err("handler name must not be empty".into());
        }
        let mut pending = lock(&self.pending);
        let Some(handlers) = pending.as_mut() else {
            return Err(format!(
                "cannot register handler '{name}': handlers must be registered before the first \
                 deploy/start/run/get call"
            ));
        };
        handlers.retain(|(existing, _)| existing != name);
        handlers.push((name.to_string(), invoke));
        Ok(())
    }

    async fn engine(&self) -> Result<&Engine, String> {
        if *lock(&self.closed) {
            return Err("engine is closed".into());
        }
        self.engine
            .get_or_try_init(|| async {
                let handlers = lock(&self.pending).take().unwrap_or_default();
                let mut builder = Engine::builder()
                    .storage(Storage::sqlite(&self.path))
                    // Single-process ownership: anything left `Running` was
                    // interrupted by a crash, so resume it right away.
                    .stale_instance_threshold(Duration::ZERO);
                for (name, invoke) in handlers {
                    builder = builder.handler(&name, host_handler(invoke));
                }
                builder.build().await.map_err(err)
            })
            .await
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
        let deployed = {
            let sequences = lock(&self.sequences);
            let versions = sequences.get(name).map(Vec::as_slice).unwrap_or_default();
            match version {
                Some(v) => versions.iter().find(|d| d.version == v).cloned(),
                None => versions.iter().max_by_key(|d| d.version).cloned(),
            }
        }
        .ok_or_else(|| {
            format!(
                "sequence '{name}' is not deployed by this engine; call deploy() first \
                 (deploy is idempotent, so do it on every startup)"
            )
        })?;
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

    /// Drive the scheduler until the instance is completed, failed,
    /// cancelled, paused or waiting (for a signal/event), or `timeout`
    /// elapses. Returns the snapshot JSON either way.
    pub async fn run(&self, id: &str, timeout: Option<Duration>) -> Result<String, String> {
        let engine = self.engine().await?;
        let id = parse_instance_id(id)?;
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            let tick = engine.tick_once().await.map_err(err)?;
            let instance = engine.get_instance(id).await.map_err(err)?;
            let settled = instance.state.is_terminal()
                || matches!(
                    instance.state,
                    InstanceState::Waiting | InstanceState::Paused
                );
            if settled || deadline.is_some_and(|d| Instant::now() >= d) {
                return self.snapshot(engine, id).await;
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
        self.snapshot(engine, parse_instance_id(id)?).await
    }

    async fn snapshot(&self, engine: &Engine, id: InstanceId) -> Result<String, String> {
        let instance = engine.get_instance(id).await.map_err(err)?;
        let outputs = latest_outputs(engine.block_outputs(id).await.map_err(err)?);
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

    /// Stop accepting calls and release the database. Idempotent.
    pub async fn close(&self) {
        *lock(&self.closed) = true;
        if let Some(engine) = self.engine.get() {
            engine.shutdown().await;
        }
    }
}

fn parse_instance_id(id: &str) -> Result<InstanceId, String> {
    serde_json::from_value(Value::String(id.to_string()))
        .map_err(|_| format!("invalid instance id '{id}'"))
}

/// Last recorded output per step id (retries append rows; the newest wins).
fn latest_outputs(outputs: Vec<orch8::BlockOutput>) -> Value {
    let mut map = Map::new();
    for output in outputs {
        map.insert(output.block_id.to_string(), output.output);
    }
    Value::Object(map)
}

fn host_handler(
    invoke: Invoke,
) -> impl Fn(StepContext) -> Pin<Box<dyn Future<Output = Result<Value, StepError>> + Send>>
+ Send
+ Sync
+ 'static {
    move |ctx: StepContext| {
        let invoke = Arc::clone(&invoke);
        Box::pin(async move {
            let outputs = ctx
                .storage
                .get_all_outputs(ctx.instance_id)
                .await
                .map_err(|e| retryable(format!("loading step outputs: {e}")))?;
            let request = json!({
                "params": ctx.params,
                "data": ctx.context.data,
                "outputs": latest_outputs(outputs),
                "instance_id": ctx.instance_id.to_string(),
                "step_id": ctx.block_id.to_string(),
                "attempt": ctx.attempt,
            });
            let reply = invoke(request.to_string()).await.map_err(retryable)?;
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
