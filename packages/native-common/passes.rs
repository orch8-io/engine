//! Bounded owned scheduler passes, independent of logical waiters.
use orch8::{Engine, InstanceId, TickResult};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::watch;

type Outcome = Result<TickResult, String>;

pub(super) struct Pass {
    target: InstanceId,
    result: watch::Receiver<Option<Outcome>>,
    stopped: watch::Receiver<bool>,
}

pub(super) enum Wait {
    Finished(TickResult),
    Refresh,
    Deadline,
}

impl Pass {
    fn finished(&self) -> bool {
        self.result.borrow().is_some() || self.result.has_changed().is_err()
    }

    async fn wait(&self) -> Outcome {
        let mut result = self.result.clone();
        let mut stopped = self.stopped.clone();
        loop {
            if *stopped.borrow() {
                return Err("engine is closed".into());
            }
            let completed = result.borrow().clone();
            if let Some(completed) = completed {
                return completed;
            }
            tokio::select! {
                changed = result.changed() => {
                    if changed.is_err() {
                        return result.borrow().clone().unwrap_or_else(||
                            Err("native scheduler pass ended without a result".into()));
                    }
                }
                _ = stopped.changed() => return Err("engine is closed".into()),
            }
        }
    }

    pub(super) async fn wait_until(&self, deadline: Option<Instant>) -> Result<Wait, String> {
        // A pass can contain another slow instance. Periodically observe this
        // caller's target rather than tying its completion to the whole batch.
        let refresh = Instant::now() + Duration::from_millis(200);
        let wake = deadline.map_or(refresh, |deadline| deadline.min(refresh));
        match tokio::time::timeout_at(tokio::time::Instant::from_std(wake), self.wait()).await {
            Ok(result) => result.map(Wait::Finished),
            Err(_) if deadline.is_some_and(|deadline| Instant::now() >= deadline) => {
                Ok(Wait::Deadline)
            }
            Err(_) => Ok(Wait::Refresh),
        }
    }
}

#[derive(Default)]
struct State {
    stopped: bool,
    active: Vec<Arc<Pass>>,
}

pub(super) struct Passes {
    state: Mutex<State>,
    stopped: watch::Sender<bool>,
    limit: usize,
}

impl Passes {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            stopped: watch::channel(false).0,
            limit: limit.max(1),
        }
    }

    pub(super) fn stop(&self) {
        super::lock(&self.state).stopped = true;
        self.stopped.send_replace(true);
    }

    pub(super) fn acquire(
        &self,
        engine: Engine,
        target: InstanceId,
        allow_new: bool,
        deadline: Option<Instant>,
        database: Option<Arc<super::database::Owner>>,
    ) -> Result<Option<Arc<Pass>>, String> {
        let mut state = super::lock(&self.state);
        if state.stopped {
            return Err("engine is closed".into());
        }
        // Recheck inside admission: time spent waiting for this lock cannot
        // authorize a fresh pass after the caller's deadline.
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(None);
        }
        state.active.retain(|pass| !pass.finished());
        if let Some(current) = state.active.iter().find(|pass| pass.target == target) {
            return Ok(Some(current.clone()));
        }
        if (!allow_new || state.active.len() >= self.limit)
            && let Some(current) = state.active.first()
        {
            return Ok(Some(current.clone()));
        }
        let (completed, result) = watch::channel(None);
        let mut stopped = self.stopped.subscribe();
        let pass = Arc::new(Pass {
            target,
            result,
            stopped: stopped.clone(),
        });
        let executor = engine.clone();
        let mut work = tokio::spawn(async move {
            // Dropping the native object cannot surrender recovery authority
            // while its detached pass still has live database work.
            let _database = database;
            executor.tick_once().await.map_err(super::err)
        });
        // This coordinator owns the join even if every run waiter disappears.
        // Neither caller timeout nor cancellation releases pass capacity.
        tokio::spawn(async move {
            let outcome = tokio::select! {
                outcome = &mut work => outcome,
                _ = stopped.changed() => {
                    engine.shutdown().await;
                    work.await
                }
            };
            let outcome = outcome
                .unwrap_or_else(|error| Err(format!("native scheduler pass task failed: {error}")));
            completed.send_replace(Some(outcome));
        });
        state.active.push(pass.clone());
        Ok(Some(pass))
    }
}
