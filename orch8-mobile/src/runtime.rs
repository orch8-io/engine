use tokio::runtime::Runtime;

const BLOCKING_THREAD_HEADROOM: u32 = 4;
const MAX_BLOCKING_THREADS: u32 = 32;

/// Manages a dedicated Tokio runtime for the mobile engine.
/// The runtime runs on a background thread so `UniFFI` sync methods can bridge
/// into async engine code via `block_on`.
pub(crate) struct MobileRuntime {
    runtime: Option<Runtime>,
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
            runtime: Some(runtime),
        })
    }

    /// Run an async future on the mobile runtime, blocking the current thread.
    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.runtime().block_on(f)
    }

    /// Get a handle to spawn tasks without blocking.
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime().handle().clone()
    }

    fn runtime(&self) -> &Runtime {
        // Only Drop takes the runtime, under exclusive access to self.
        self.runtime.as_ref().expect("mobile runtime is alive")
    }
}

impl Drop for MobileRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // The last engine owner may be released inside its own async task
            // or foreign blocking callback. Default Runtime::drop either
            // panics in async context or waits for that very callback forever.
            // Signal shutdown without joining threads. An already-running
            // native call retains its resources until it actually returns;
            // this cannot interrupt arbitrary host code.
            runtime.shutdown_background();
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

    fn drop_from_own_executor(blocking: bool) {
        let (finished, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = MobileRuntime::new(0).unwrap();
            let handle = runtime.handle();
            let retire = move || {
                let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(runtime)))
                    .is_ok();
                let _ = finished.send(ok);
            };
            if blocking {
                drop(handle.spawn_blocking(retire));
            } else {
                drop(handle.spawn(async move { retire() }));
            }
        });
        assert_eq!(
            result.recv_timeout(std::time::Duration::from_secs(5)),
            Ok(true),
            "the last owner must retire without panicking or waiting for its own executor"
        );
    }

    #[test]
    fn runtime_drop_from_own_async_task_does_not_panic() {
        drop_from_own_executor(false);
    }

    #[test]
    fn runtime_drop_from_own_blocking_callback_does_not_deadlock() {
        drop_from_own_executor(true);
    }

    fn drop_engine_from_own_executor(blocking: bool) {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::MobileEngine::new(
            directory
                .path()
                .join("engine.db")
                .to_string_lossy()
                .into_owned(),
            crate::MobileEngineConfig::default(),
        )
        .unwrap();
        let weak = std::sync::Arc::downgrade(&engine);
        let handle = engine.runtime.handle();
        let (finished, result) = std::sync::mpsc::channel();
        let retire = move || {
            let ok =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(engine))).is_ok();
            let _ = finished.send(ok);
        };
        if blocking {
            drop(handle.spawn_blocking(retire));
        } else {
            drop(handle.spawn(async move { retire() }));
        }
        assert_eq!(
            result.recv_timeout(std::time::Duration::from_secs(5)),
            Ok(true)
        );
        assert!(
            weak.upgrade().is_none(),
            "the entire engine must be released"
        );
    }

    #[test]
    fn last_engine_owner_can_drop_from_own_async_task() {
        drop_engine_from_own_executor(false);
    }

    #[test]
    fn last_engine_owner_can_drop_from_own_blocking_callback() {
        drop_engine_from_own_executor(true);
    }

    #[test]
    fn runtime_drop_does_not_wait_for_noncooperative_host_callback() {
        struct HostResource(std::sync::mpsc::Sender<()>);
        impl Drop for HostResource {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let runtime = MobileRuntime::new(0).unwrap();
        let (entered, started) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let (finished, retired) = std::sync::mpsc::channel();
        let (cleanup, cleaned) = std::sync::mpsc::channel();
        drop(runtime.handle().spawn_blocking(move || {
            let _resource = HostResource(cleanup);
            entered.send(()).unwrap();
            let _ = gate.recv();
        }));
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let dropping = std::thread::spawn(move || {
            drop(runtime);
            let _ = finished.send(());
        });
        let before_release = retired.recv_timeout(std::time::Duration::from_secs(5));
        let resource_before_release = cleaned.try_recv();
        // Release even on failure so the baseline cannot strand the test thread.
        release.send(()).unwrap();
        dropping.join().unwrap();
        cleaned
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(before_release, Ok(()));
        assert_eq!(
            resource_before_release,
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
    }

    #[test]
    fn blocking_pool_is_bounded_for_mobile_memory() {
        assert_eq!(blocking_thread_limit(0), 4);
        assert_eq!(blocking_thread_limit(4), 8);
        assert_eq!(blocking_thread_limit(28), 32, "exactly at the ceiling");
        assert_eq!(blocking_thread_limit(128), 32);
        assert_eq!(
            blocking_thread_limit(u32::MAX),
            32,
            "headroom addition saturates instead of overflowing"
        );
    }
}
