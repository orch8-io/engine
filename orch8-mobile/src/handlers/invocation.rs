//! Keep native callback ownership visible when its async caller goes away.

use std::sync::{Arc, Mutex};

use tokio::task::{JoinError, JoinHandle};

use super::{HandlerError, StepHandler, Stragglers};

pub(super) struct NativeInvocation {
    call: JoinHandle<Result<String, HandlerError>>,
    completion: Arc<Completion>,
}

#[derive(Default)]
struct State {
    finished: bool,
    detached: bool,
}

struct Completion {
    state: Mutex<State>,
    stragglers: Arc<Stragglers>,
    name: String,
}

// Owned by the blocking closure, so completion is recorded even on panic,
// queued-task cancellation, or runtime shutdown without an async observer.
struct FinishOnDrop(Arc<Completion>);

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.finished = true;
        if state.detached {
            self.0.stragglers.end(&self.0.name);
        }
    }
}

impl NativeInvocation {
    pub(super) fn spawn(
        handler: Arc<dyn StepHandler>,
        name: String,
        input: String,
        stragglers: Arc<Stragglers>,
    ) -> Self {
        let completion = Arc::new(Completion {
            state: Mutex::new(State::default()),
            stragglers,
            name: name.clone(),
        });
        let finish = FinishOnDrop(Arc::clone(&completion));
        let call = tokio::task::spawn_blocking(move || {
            let _finish = finish;
            handler.execute(name, input)
        });
        Self { call, completion }
    }

    pub(super) async fn result(&mut self) -> Result<Result<String, HandlerError>, JoinError> {
        (&mut self.call).await
    }
}

impl Drop for NativeInvocation {
    fn drop(&mut self) {
        // Abort prevents a queued callback from starting. An already-running
        // native call cannot be aborted and remains quarantined until its
        // closure drops FinishOnDrop. Serialize the completion/detach race so
        // neither order can strand or prematurely clear the quarantine.
        self.call.abort();
        let mut state = self
            .completion
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.finished {
            self.completion.stragglers.begin(&self.completion.name);
            state.detached = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    struct CountingHandler(Arc<AtomicUsize>);
    impl StepHandler for CountingHandler {
        fn execute(&self, _name: String, _input: String) -> Result<String, HandlerError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok("{}".into())
        }
    }

    #[test]
    fn cancellation_prevents_a_queued_native_callback_from_starting() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = runtime.spawn_blocking(move || {
            started_tx.send(()).unwrap();
            // Bounded only to guarantee cleanup if a regression assertion panics.
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let stragglers = Arc::new(Stragglers::default());
        runtime.block_on(async {
            let call = NativeInvocation::spawn(
                Arc::new(CountingHandler(Arc::clone(&calls))),
                "queued".into(),
                "{}".into(),
                Arc::clone(&stragglers),
            );
            drop(call);
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            // A sentinel behind the cancelled callback proves the blocking
            // queue has drained before checking that no side effect ran.
            tokio::task::spawn_blocking(|| ()).await.unwrap();
        });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!stragglers.blocks("queued"));
    }
}
