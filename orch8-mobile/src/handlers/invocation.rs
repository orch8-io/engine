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
    pub(super) lifetime: Option<Arc<super::scoped::lifetime::Lifetime>>,
    pub(super) instance_id: InstanceId,
    pub(super) tenant_id: TenantId,
    pub(super) token: chrono::DateTime<chrono::Utc>,
}

impl NativeClaim {
    async fn verify_lifetime(
        &self,
        admission: Option<&orch8_engine::handlers::EffectAdmission>,
    ) -> Result<(), StepError> {
        if let Some(lifetime) = &self.lifetime {
            match lifetime
                .verify(self.instance_id, &self.tenant_id, self.token)
                .await
            {
                Ok(true) => {}
                Ok(false) => return Err(StepError::ClaimLost),
                Err(error) => {
                    if error.is_transient()
                        && let Some(admission) = admission
                    {
                        return Err(admission.reject_preflight(
                            orch8_engine::error::EngineError::Storage(
                                orch8_types::error::StorageError::Connection(
                                    "scoped lifetime verification unavailable".into(),
                                ),
                            ),
                        ));
                    }
                    return Err(super::scoped::storage_error(error));
                }
            }
        }
        Ok(())
    }
    async fn verify(
        &self,
        admission: Option<&orch8_engine::handlers::EffectAdmission>,
    ) -> Result<(), StepError> {
        match self
            .storage
            .verify_instance_claim(self.instance_id, &self.tenant_id, self.token)
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(StepError::ClaimLost),
            Err(error) if error.is_transient() => {
                if let Some(admission) = admission {
                    use orch8_types::error::StorageError;
                    // Retain infrastructure type without logging backend detail.
                    let error = match error {
                        StorageError::PoolExhausted => StorageError::PoolExhausted,
                        StorageError::Backend(_) => {
                            StorageError::Backend("native claim verification unavailable".into())
                        }
                        _ => {
                            StorageError::Connection("native claim verification unavailable".into())
                        }
                    };
                    Err(admission
                        .reject_preflight(orch8_engine::error::EngineError::Storage(error)))
                } else {
                    Err(StepError::Retryable {
                        message: "native callback claim verification unavailable".into(),
                        details: None,
                    })
                }
            }
            Err(_) => Err(StepError::Permanent {
                message: "native callback claim verification failed".into(),
                details: None,
            }),
        }?;
        // The claim read may wait; expiry must be the final awaited boundary
        // before dispatch/host entry, including the post-dispatch recheck.
        self.verify_lifetime(admission).await
    }
}

// Exactly one physical admission domain owns an invocation. Scoped callbacks
// keep their account-aware budget; legacy callbacks use the process gate.
pub(super) enum InvocationPermit {
    Scoped {
        _permit: super::scoped::NativePermit,
    },
    Legacy {
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
}

pub(super) struct NativeOwnership {
    pub(super) remote_lease: Option<orch8_engine::remote_worker::lease::LeaseAuthority>,
    pub(super) claim: Option<NativeClaim>,
    pub(super) effect_admission: Option<orch8_engine::handlers::EffectAdmission>,
    pub(super) tracking_key: String,
    pub(super) _permits: Option<InvocationPermit>,
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
        let Self {
            handler,
            _binding: binding,
            finish,
            mut ownership,
        } = self;
        let permits = ownership._permits.take();
        let registration = binding
            .as_ref()
            .map(|binding| binding.registration_charge());
        // Only owned Rust strings/errors leave this pool. Foreign references,
        // panic payloads and Apple temporaries finish before admission returns.
        let result = super::pool::run(|| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                invoke_native(handler.as_ref(), &ownership, &finish, name, input, runtime)
            }));
            let result = match result {
                Ok(result) => result,
                Err(payload) => {
                    super::pool::dispose_panic(payload);
                    Err(StepError::Permanent {
                        message: "handler task panicked".into(),
                        details: None,
                    })
                }
            };
            super::pool::release(handler);
            super::pool::release(binding);
            result
        });
        drop(registration);
        drop(ownership);
        drop(permits);
        drop(finish);
        result
    }
}

fn invoke_native(
    handler: &dyn StepHandler,
    ownership: &NativeOwnership,
    finish: &FinishOnDrop,
    name: String,
    input: String,
    runtime: tokio::runtime::Handle,
) -> Result<String, StepError> {
    if ownership.stop.is_cancelled()
        || ownership
            .remote_lease
            .as_ref()
            .is_some_and(|lease| lease.check().is_err())
    {
        return Err(StepError::ClaimLost);
    }
    let claim = &ownership.claim;
    let effect_admission = &ownership.effect_admission;
    let entry = &finish.0;
    // Borrow the proof for verification: its owner/backend must remain
    // captured until physical native completion, even after detach.
    if let Some(claim) = claim {
        runtime.block_on(claim.verify(effect_admission.as_ref()))?;
    }
    // Cancellation may arrive while the database preflight is pending.
    // Check at host admission, after that await, before any native code.
    if ownership.stop.is_cancelled()
        || ownership
            .remote_lease
            .as_ref()
            .is_some_and(|lease| lease.check().is_err())
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
            runtime.block_on(claim.verify(effect_admission.as_ref()))?;
        }
        if ownership.stop.is_cancelled()
            || ownership
                .remote_lease
                .as_ref()
                .is_some_and(|lease| lease.check().is_err())
            || entry
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .detached
        {
            return Err(StepError::ClaimLost);
        }
    }
    let result = handler.execute(name, input).map_err(|error| match error {
        HandlerError::Retryable { detail: message } => StepError::Retryable {
            message,
            details: None,
        },
        HandlerError::Permanent { detail: message } => StepError::Permanent {
            message,
            details: None,
        },
    });
    // A host that outlives its logical deadline retains physical admission,
    // but cannot publish a successful result into expired logical work.
    if let Some(claim) = claim {
        runtime.block_on(claim.verify_lifetime(effect_admission.as_ref()))?;
    }
    if ownership
        .remote_lease
        .as_ref()
        .is_some_and(|lease| lease.check().is_err())
    {
        return Err(StepError::ClaimLost);
    }
    result
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
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        runtime.block_on(async {
            let call = NativeInvocation::spawn(
                Arc::new(CountingHandler(Arc::clone(&calls))),
                "queued".into(),
                "{}".into(),
                Arc::clone(&stragglers),
                NativeOwnership {
                    remote_lease: None,
                    claim: None,
                    effect_admission: None,
                    tracking_key: "queued".into(),
                    _permits: Some(InvocationPermit::Legacy {
                        _permit: capacity.clone().try_acquire_owned().unwrap(),
                    }),
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
        assert_eq!(capacity.available_permits(), 1);
    }
}
