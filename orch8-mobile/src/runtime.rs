use std::sync::Arc;

use tokio::runtime::Runtime;

const BLOCKING_THREAD_HEADROOM: u32 = 4;
const MAX_BLOCKING_THREADS: u32 = 32;

/// Manages a dedicated Tokio runtime for the mobile engine.
/// The runtime runs on a background thread so `UniFFI` sync methods can bridge
/// into async engine code via `block_on`.
pub(crate) struct MobileRuntime {
    runtime: Arc<Runtime>,
}

impl MobileRuntime {
    pub fn new(max_concurrent_steps: u32) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            // Tokio otherwise permits 512 blocking threads. Foreign handlers,
            // listener callbacks, and RSS probes all use this pool, so a stuck
            // host callback could consume hundreds of mobile thread stacks.
            .max_blocking_threads(blocking_thread_limit(max_concurrent_steps))
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

fn blocking_thread_limit(max_concurrent_steps: u32) -> usize {
    let bounded = max_concurrent_steps
        .saturating_add(BLOCKING_THREAD_HEADROOM)
        .clamp(BLOCKING_THREAD_HEADROOM, MAX_BLOCKING_THREADS);
    bounded as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_creates_and_runs_async() {
        let rt = MobileRuntime::new(4).unwrap();
        let result = rt.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn runtime_handle_spawns_tasks() {
        let rt = MobileRuntime::new(4).unwrap();
        let handle = rt.handle();
        let result = rt.block_on(async move { handle.spawn(async { 99 }).await.unwrap() });
        assert_eq!(result, 99);
    }

    #[test]
    fn blocking_pool_is_bounded_for_mobile_memory() {
        assert_eq!(blocking_thread_limit(0), 4);
        assert_eq!(blocking_thread_limit(4), 8);
        assert_eq!(blocking_thread_limit(128), 32);
    }
}
