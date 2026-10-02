use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
fn validate_sequence_json(input: &str) -> PyResult<String> {
    let value = serde_json::from_str(input)
        .map_err(|error| PyValueError::new_err(format!("invalid JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    sequence
        .validate()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serde_json::to_string(&sequence).map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
fn sequence_schema_version() -> u32 {
    orch8_types::sequence::SEQUENCE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(signature = (sequence_json, input_json=None, max_ticks=1000))]
fn run_sequence_json(
    py: Python<'_>,
    sequence_json: &str,
    input_json: Option<&str>,
    max_ticks: u32,
) -> PyResult<String> {
    let value = serde_json::from_str(sequence_json)
        .map_err(|error| PyValueError::new_err(format!("invalid sequence JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let input = input_json
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| PyValueError::new_err(format!("invalid input JSON: {error}")))?
        .unwrap_or_else(|| serde_json::json!({}));

    py.detach(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let result = runtime
            .block_on(orch8::run_sequence_once(sequence, input, max_ticks))
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        serde_json::to_string(&result).map_err(|error| PyValueError::new_err(error.to_string()))
    })
}

// ---------------------------------------------------------------------------
// Durable, SQLite-backed engine. `orch8_engine.Engine` wraps this low-level
// class (JSON strings in and out) into the ergonomic Python API.
// ---------------------------------------------------------------------------

#[path = "../../native-common/durable.rs"]
mod durable;

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;

fn to_py(error: String) -> PyErr {
    PyRuntimeError::new_err(error)
}

/// Low-level durable engine; use `orch8_engine.Engine` instead.
///
/// Owns a tokio runtime. Every blocking call releases the GIL while the
/// engine works; host handlers re-acquire it on a blocking worker thread.
#[pyclass(module = "orch8_engine._native")]
struct DurableEngine {
    inner: Arc<durable::DurableEngine>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl DurableEngine {
    fn block_on<T: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(
            Arc<durable::DurableEngine>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, String>> + Send>,
        > + Send,
    ) -> PyResult<T> {
        let inner = Arc::clone(&self.inner);
        let runtime = Arc::clone(&self.runtime);
        py.detach(move || runtime.block_on(work(inner)))
            .map_err(to_py)
    }
}

#[pymethods]
impl DurableEngine {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("orch8-engine")
            .enable_all()
            .build()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(durable::DurableEngine::new(path)),
            runtime: Arc::new(runtime),
        })
    }

    #[getter]
    fn path(&self) -> String {
        self.inner.path().to_string()
    }

    /// Register `callback(request_json: str) -> reply_json: str`.
    fn register(&self, name: &str, callback: Py<PyAny>) -> PyResult<()> {
        let callback = Arc::new(callback);
        let invoke: durable::Invoke = Arc::new(move |request: String| {
            let callback = Arc::clone(&callback);
            Box::pin(async move {
                tokio::task::spawn_blocking(move || {
                    Python::attach(|py| {
                        callback
                            .call1(py, (request,))
                            .and_then(|reply| reply.extract::<String>(py))
                            .map_err(|error| error.to_string())
                    })
                })
                .await
                .map_err(|error| error.to_string())?
            })
        });
        self.inner.register(name, invoke).map_err(to_py)
    }

    fn deploy(&self, py: Python<'_>, sequence_json: String) -> PyResult<String> {
        self.block_on(py, move |inner| {
            Box::pin(async move { inner.deploy(&sequence_json).await })
        })
    }

    #[pyo3(signature = (name, input_json=None, idempotency_key=None, version=None))]
    fn start(
        &self,
        py: Python<'_>,
        name: String,
        input_json: Option<String>,
        idempotency_key: Option<String>,
        version: Option<i32>,
    ) -> PyResult<String> {
        self.block_on(py, move |inner| {
            Box::pin(async move {
                inner
                    .start(&name, input_json.as_deref(), idempotency_key, version)
                    .await
            })
        })
    }

    #[pyo3(signature = (id, timeout_secs=None))]
    fn run(&self, py: Python<'_>, id: String, timeout_secs: Option<f64>) -> PyResult<String> {
        let timeout = timeout_secs
            .map(|secs| Duration::try_from_secs_f64(secs.max(0.0)))
            .transpose()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        self.block_on(py, move |inner| {
            Box::pin(async move { inner.run(&id, timeout).await })
        })
    }

    fn get(&self, py: Python<'_>, id: String) -> PyResult<String> {
        self.block_on(py, move |inner| {
            Box::pin(async move { inner.get(&id).await })
        })
    }

    #[pyo3(signature = (id, signal, payload_json=None))]
    fn signal(
        &self,
        py: Python<'_>,
        id: String,
        signal: String,
        payload_json: Option<String>,
    ) -> PyResult<()> {
        self.block_on(py, move |inner| {
            Box::pin(async move { inner.signal(&id, &signal, payload_json.as_deref()).await })
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.block_on(py, move |inner| {
            Box::pin(async move {
                inner.close().await;
                Ok(())
            })
        })
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<DurableEngine>()?;
    module.add_function(wrap_pyfunction!(validate_sequence_json, module)?)?;
    module.add_function(wrap_pyfunction!(sequence_schema_version, module)?)?;
    module.add_function(wrap_pyfunction!(run_sequence_json, module)?)?;
    Ok(())
}
