use std::sync::Arc;

use tokio::runtime::Runtime;

/// Manages a dedicated Tokio runtime for the mobile engine.
/// The runtime runs on a background thread so `UniFFI` sync methods can bridge
/// into async engine code via `block_on`.
pub(crate) struct MobileRuntime {
    runtime: Arc<Runtime>,
}

impl MobileRuntime {
    pub fn new() -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("orch8-mobile")
            .enable_all()
            .build()
            .map_err(|e| format!("failed to create tokio runtime: {e}"))?;

        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// Run an async future on the mobile runtime, blocking the current thread.
    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.runtime.block_on(f)
    }

    /// Get a handle to spawn tasks without blocking.
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }

    /// Shut down the runtime, waiting for spawned tasks to complete.
    #[allow(dead_code)]
    pub fn shutdown(self) {
        // Arc::try_unwrap will only succeed if we hold the last reference.
        // If other tasks still hold handles, we just drop our reference
        // and the runtime shuts down when the last handle is dropped.
        if let Ok(rt) = Arc::try_unwrap(self.runtime) {
            rt.shutdown_timeout(std::time::Duration::from_secs(5));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_creates_and_runs_async() {
        let rt = MobileRuntime::new().unwrap();
        let result = rt.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn runtime_handle_spawns_tasks() {
        let rt = MobileRuntime::new().unwrap();
        let handle = rt.handle();
        let result = rt.block_on(async move { handle.spawn(async { 99 }).await.unwrap() });
        assert_eq!(result, 99);
    }
}
