//! Runtime ownership for synchronous foreign-language engine objects.
use std::future::Future;
use tokio::runtime::Runtime;

pub(crate) struct HostRuntime {
    runtime: Option<Runtime>,
}

impl HostRuntime {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime
            .as_ref()
            .expect("host runtime is alive")
            .block_on(future)
    }
}

impl Drop for HostRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // A Python finalizer holds the GIL; joining a blocking callback
            // that needs it deadlocks. The last owner can also be an async
            // task, where Runtime::drop may not block. Signal shutdown without
            // joining: live blocking calls keep their captures until return.
            runtime.shutdown_background();
        }
    }
}
