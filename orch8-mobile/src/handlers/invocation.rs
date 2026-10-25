//! Keep native callback ownership visible when its async caller goes away.

use std::sync::{Arc, Mutex};

use tokio::task::{JoinError, JoinHandle};

use super::{HandlerError, StepHandler, Stragglers};
use orch8_types::{
    error::StepError,
    ids::{InstanceId, TenantId},
};

pub(super) struct NativeClaim {
    pub(super) storage: Arc<dyn orch8_storage::StorageBackend>,
    pub(super) instance_id: InstanceId,
    pub(super) tenant_id: TenantId,
    pub(super) token: chrono::DateTime<chrono::Utc>,
}

impl NativeClaim {
    async fn verify(&self) -> Result<(), StepError> {
        match self
            .storage
            .verify_instance_claim(self.instance_id, &self.tenant_id, self.token)
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(StepError::ClaimLost),
            Err(error) if error.is_transient() => Err(StepError::Retryable {
                message: "native callback claim verification unavailable".into(),
                details: None,
            }),
            Err(_) => Err(StepError::Permanent {
                message: "native callback claim verification failed".into(),
                details: None,
            }),
        }
    }
}

pub(super) struct NativeOwnership {
    pub(super) claim: Option<NativeClaim>,
    pub(super) effect_admission: Option<orch8_engine::handlers::EffectAdmission>,
    pub(super) tracking_key: String,
    pub(super) _permits: Option<super::scoped::NativePermit>,
    pub(super) binding: Option<Arc<super::scoped::Binding>>,
    pub(super) stop: tokio_util::sync::CancellationToken,
}

// Declaration order matters: foreign references are released before quarantine
// ends and before physical capacity is returned. No foreign drop runs under a lock.
struct PhysicalCall {
    handler: Arc<dyn StepHandler>,
    _binding: Option<Arc<super::scoped::Binding>>,
    finish: FinishOnDrop,
    ownership: NativeOwnership,
}

impl PhysicalCall {
    fn run(
        self,
        name: String,
        input: String,
        runtime: tokio::runtime::Handle,
    ) -> Result<String, StepError> {
        if self.ownership.stop.is_cancelled() {
            return Err(StepError::ClaimLost);
        }
        let claim = &self.ownership.claim;
        let effect_admission = &self.ownership.effect_admission;
        let entry = &self.finish.0;
        // Borrow the proof for verification: its owner/backend must remain
        // captured until physical native completion, even after detach.
        if let Some(claim) = claim {
            runtime.block_on(claim.verify())?;
        }
        // Cancellation may arrive while the database preflight is pending.
        // Check at host admission, after that await, before any native code.
        if self.ownership.stop.is_cancelled()
            || entry
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .detached
        {
            return Err(StepError::ClaimLost);
        }
        if let Some(admission) = effect_admission {
            runtime.block_on(admission.admit())?;
            // The durable dispatch itself can wait on storage. Recheck the
            // claim/cancellation afterward; this window stays conservative.
            if let Some(claim) = claim {
                runtime.block_on(claim.verify())?;
            }
            if self.ownership.stop.is_cancelled()
                || entry
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .detached
            {
                return Err(StepError::ClaimLost);
            }
        }
        self.handler
            .execute(name, input)
            .map_err(|error| match error {
                HandlerError::Retryable { message } => StepError::Retryable {
                    message,
                    details: None,
                },
                HandlerError::Permanent { message } => StepError::Permanent {
                    message,
                    details: None,
                },
            })
    }
}

pub(super) struct NativeInvocation {
    call: JoinHandle<Result<String, StepError>>,
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
        mut ownership: NativeOwnership,
    ) -> Self {
        let completion = Arc::new(Completion {
            state: Mutex::new(State::default()),
            stragglers,
            name: ownership.tracking_key.clone(),
        });
        let finish = FinishOnDrop(Arc::clone(&completion));
        let runtime = tokio::runtime::Handle::current();
        let physical = PhysicalCall {
            handler,
            _binding: ownership.binding.take(),
            finish,
            ownership,
        };
        let call = tokio::task::spawn_blocking(move || physical.run(name, input, runtime));
        Self { call, completion }
    }

    pub(super) async fn result(&mut self) -> Result<Result<String, StepError>, JoinError> {
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
                NativeOwnership {
                    claim: None,
                    effect_admission: None,
                    tracking_key: "queued".into(),
                    _permits: None,
                    binding: None,
                    stop: tokio_util::sync::CancellationToken::new(),
                },
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
