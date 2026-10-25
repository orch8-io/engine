//! Owned retirement outlives observers and the caller's async runtime.
use super::{Binding, CatalogScope, NativePermit, physical};
use orch8_types::error::StepError;
use std::{
    collections::VecDeque,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::watch;

use crate::handlers::pool;

pub(super) struct Completion {
    done: AtomicBool,
    signal: watch::Sender<bool>,
}
impl Completion {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            done: AtomicBool::new(false),
            signal: watch::channel(false).0,
        })
    }
    pub(super) fn finished(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
    pub(super) fn receipt(&self) -> Receipt {
        Receipt(self.signal.subscribe())
    }
    pub(super) fn finish(&self) {
        self.done.store(true, Ordering::Release);
        self.signal.send_replace(true);
    }
}

/// Dropping an observer does not cancel physical cleanup.
pub(crate) struct Receipt(watch::Receiver<bool>);
impl IntoFuture for Receipt {
    type Output = Result<(), String>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            self.0
                .wait_for(|done| *done)
                .await
                .map(|_| ())
                .map_err(|_| "native retirement ended without completion".into())
        })
    }
}

pub(super) struct Job {
    scope: CatalogScope,
    references: Vec<Arc<Binding>>,
    completion: Arc<Completion>,
    permit: Option<NativePermit>,
    priority: Option<physical::Priority>,
}
impl Job {
    pub(super) fn new(
        scope: CatalogScope,
        references: Vec<Arc<Binding>>,
        completion: Arc<Completion>,
        permit: Option<NativePermit>,
    ) -> Self {
        Self {
            scope,
            references,
            completion,
            permit,
            priority: None,
        }
    }
    fn run(mut self) {
        for binding in self.references.drain(..) {
            // Host autorelease-pool draining can run further finalizers. Keep
            // this charge, plus the job's physical permit, until it is done.
            let registration = binding._registration.clone();
            pool::run(|| {
                pool::release(binding);
            });
            drop(registration);
        }
        drop(self.permit.take());
        drop(self.priority.take());
        self.completion.finish();
    }
}

struct State {
    pending: Mutex<VecDeque<Job>>,
    budget: Arc<physical::Budget>,
    closed: AtomicBool,
}
impl State {
    fn work(&self) {
        loop {
            let observed = self.budget.wake.version();
            let (job, stop) = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let ready = pending.iter_mut().position(|job| {
                    if job.permit.is_none() {
                        job.permit = self.budget.reserve_retirement(&job.scope).ok();
                    }
                    job.permit.is_some()
                });
                let job = ready.and_then(|index| pending.remove(index));
                let stop =
                    job.is_none() && pending.is_empty() && self.closed.load(Ordering::Acquire);
                (job, stop)
            };
            if stop {
                return;
            }
            if let Some(job) = job {
                job.run();
            } else {
                self.budget.wake.wait(observed);
            }
        }
    }
}

// Every nonempty job owns at least one distinct accepted registration permit;
// the shared registration budget therefore bounds the pending queue at 256.
#[derive(Clone)]
pub(super) struct Handle(Arc<State>);
impl Handle {
    pub(super) fn enqueue(&self, mut job: Job) {
        if job.references.is_empty() {
            job.run();
            return;
        }
        job.priority = Some(job.references[0].account.retire(self.0.budget.wake.clone()));
        self.0
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(job);
        self.0.budget.wake.poke();
    }
}

pub(super) struct Executor {
    state: Arc<State>,
}
impl Executor {
    pub(super) fn new(budget: Arc<physical::Budget>) -> Result<Self, StepError> {
        let owner = Self {
            state: Arc::new(State {
                pending: Mutex::new(VecDeque::new()),
                budget,
                closed: AtomicBool::new(false),
            }),
        };
        for _ in 0..2 {
            let state = owner.state.clone();
            if std::thread::Builder::new()
                .name("orch8-retire".into())
                .spawn(move || state.work())
                .is_err()
            {
                return Err(StepError::Retryable {
                    message: "native retirement executor unavailable".into(),
                    details: None,
                });
            }
        }
        Ok(owner)
    }
    pub(super) fn handle(&self) -> Handle {
        Handle(self.state.clone())
    }
}
impl Drop for Executor {
    fn drop(&mut self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.budget.wake.poke();
    }
}
