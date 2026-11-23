use napi_derive::napi;

/// Strictly decode and validate a sequence using the same Rust types as the server.
#[napi]
pub fn validate_sequence_json(input: String) -> napi::Result<String> {
    let value = serde_json::from_str(&input)
        .map_err(|error| napi::Error::from_reason(format!("invalid JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    sequence
        .validate()
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    serde_json::to_string(&sequence).map_err(|error| napi::Error::from_reason(error.to_string()))
}

#[napi]
pub fn sequence_schema_version() -> u32 {
    orch8_types::sequence::SEQUENCE_SCHEMA_VERSION
}

/// Run a workflow in an isolated, in-memory Orch8 engine.
#[napi]
pub async fn run_sequence_json(
    sequence_json: String,
    input_json: Option<String>,
    max_ticks: Option<u32>,
) -> napi::Result<String> {
    let value = serde_json::from_str(&sequence_json)
        .map_err(|error| napi::Error::from_reason(format!("invalid sequence JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    let input = input_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| napi::Error::from_reason(format!("invalid input JSON: {error}")))?
        .unwrap_or_else(|| serde_json::json!({}));
    let result = orch8::run_sequence_once(sequence, input, max_ticks.unwrap_or(1_000))
        .await
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    serde_json::to_string(&result).map_err(|error| napi::Error::from_reason(error.to_string()))
}

// ---------------------------------------------------------------------------
// Durable, SQLite-backed engine. `index.js` wraps this low-level class (JSON
// strings in and out) into the ergonomic `Engine` API.
// ---------------------------------------------------------------------------

#[path = "../../native-common/durable.rs"]
mod durable;

use std::sync::Arc;
use std::time::Duration;

use napi::Status;
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::ThreadsafeFunction;

/// JS handler: `(requestJson: string) => Promise<string>` (reply envelope).
/// Weak so a registered handler never keeps the Node process alive.
type HostHandler = ThreadsafeFunction<String, Promise<String>, String, Status, false, true>;

fn to_napi(error: String) -> napi::Error {
    napi::Error::from_reason(error)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn duration_from_ms(ms: Option<f64>, name: &str) -> napi::Result<Option<Duration>> {
    ms.map(|ms| {
        if !ms.is_finite() || ms < 0.0 || ms.fract() != 0.0 || ms > f64::from(u32::MAX) {
            return Err(napi::Error::new(
                Status::InvalidArg,
                format!("{name} must be a finite nonnegative integer at most 4294967295"),
            ));
        }
        Ok(Duration::from_millis(ms as u64))
    })
    .transpose()
}

/// Low-level durable engine; use the `Engine` wrapper from `index.js`.
#[napi]
pub struct NativeDurableEngine {
    inner: Arc<durable::DurableEngine>,
}

#[napi]
impl NativeDurableEngine {
    #[napi(constructor)]
    pub fn new(path: String) -> Self {
        Self {
            inner: Arc::new(durable::DurableEngine::new(path)),
        }
    }

    #[napi(getter)]
    pub fn path(&self) -> String {
        self.inner.path().to_string()
    }

    #[napi]
    pub fn register(&self, name: String, callback: HostHandler) -> napi::Result<()> {
        let callback = Arc::new(callback);
        let invoke: durable::Invoke = Arc::new(move |mut invocation: durable::Invocation| {
            let callback = Arc::clone(&callback);
            Box::pin(async move {
                // The Node environment owns this task. Dropping the step waiter
                // or closing an engine cannot free a still-running JS promise.
                tokio::spawn(async move {
                    let request = invocation.take_request();
                    let result = async {
                        let promise = callback
                            .call_async_catch(request)
                            .await
                            .map_err(|error| error.to_string())?;
                        promise.await.map_err(|error| error.to_string())
                    }
                    .await;
                    drop(callback);
                    drop(invocation);
                    result
                })
                .await
                .map_err(|error| error.to_string())?
            })
        });
        self.inner.register(&name, invoke).map_err(to_napi)
    }

    #[napi]
    pub async fn deploy(&self, sequence_json: String) -> napi::Result<String> {
        self.inner.deploy(&sequence_json).await.map_err(to_napi)
    }

    #[napi]
    pub async fn start(
        &self,
        name: String,
        input_json: Option<String>,
        idempotency_key: Option<String>,
        version: Option<i32>,
    ) -> napi::Result<String> {
        self.inner
            .start(&name, input_json.as_deref(), idempotency_key, version)
            .await
            .map_err(to_napi)
    }

    #[napi]
    pub async fn start_batch(&self, items_json: String) -> napi::Result<String> {
        self.inner.start_batch(&items_json).await.map_err(to_napi)
    }

    #[napi]
    pub async fn run(
        &self,
        id: String,
        timeout_ms: Option<f64>,
        observer_timeout_ms: Option<f64>,
    ) -> napi::Result<String> {
        let timeout = duration_from_ms(timeout_ms, "timeoutMs")?;
        let observer_timeout = duration_from_ms(observer_timeout_ms, "observerTimeoutMs")?;
        self.inner
            .run_with_observer_deadline(&id, timeout, observer_timeout)
            .await
            .map_err(|error| match error {
                durable::RunError::ObserverTimeout => {
                    napi::Error::new(Status::Cancelled, durable::OBSERVER_TIMEOUT_MESSAGE)
                }
                durable::RunError::Engine(message) => to_napi(message),
            })
    }

    #[napi]
    pub async fn get(&self, id: String) -> napi::Result<String> {
        self.inner.get(&id).await.map_err(to_napi)
    }

    #[napi]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub async fn get_output(
        &self,
        id: String,
        block: String,
        attempt: f64,
    ) -> napi::Result<String> {
        if !attempt.is_finite()
            || attempt.fract() != 0.0
            || !(0.0..=f64::from(u16::MAX)).contains(&attempt)
        {
            return Err(napi::Error::new(
                Status::InvalidArg,
                "attempt must be an integer from 0 to 65535",
            ));
        }
        self.inner
            .get_output(&id, &block, attempt as u16)
            .await
            .map_err(to_napi)
    }

    #[napi]
    pub async fn list_instance_page(
        &self,
        state: Option<String>,
        sequence_id: Option<String>,
        before: Option<String>,
        page_size: u32,
    ) -> napi::Result<String> {
        self.inner
            .list_instance_page(
                state.as_deref(),
                sequence_id.as_deref(),
                before.as_deref(),
                page_size,
            )
            .await
            .map_err(to_napi)
    }

    #[napi]
    pub async fn signal(
        &self,
        id: String,
        signal: String,
        payload_json: Option<String>,
    ) -> napi::Result<()> {
        self.inner
            .signal(&id, &signal, payload_json.as_deref())
            .await
            .map_err(to_napi)
    }

    #[napi]
    pub async fn interrupt_running_instance(&self, id: String) -> napi::Result<bool> {
        self.inner
            .interrupt_running_instance(&id)
            .await
            .map_err(to_napi)
    }

    #[napi]
    pub async fn close(&self) {
        self.inner.close().await;
    }
}
