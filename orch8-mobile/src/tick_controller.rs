//! Tick loop lifecycle management for the mobile engine.
//!
//! `TickController` owns the tick loop cancellation token, the tick mutex that
//! serializes tick execution, the power-state atom, and the dirty flag used for
//! stale-instance recovery on resume.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;

use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;

use crate::lifecycle::InstanceLifecycleManager;
use crate::memory;
use crate::notifier::MobileNotifier;
use crate::runtime::MobileRuntime;
use crate::sync_reporter::SyncReporter;
use crate::PowerState;

/// Manages the tick loop lifecycle: resume, pause, power-state adaptation,
/// and stale-instance recovery.
pub(crate) struct TickController {
    tick_mutex: Arc<Mutex<()>>,
    tick_loop_cancel: StdMutex<CancellationToken>,
    power_state: Arc<AtomicU8>,
    dirty: Arc<AtomicBool>,
}

impl TickController {
    pub fn new() -> Self {
        Self {
            tick_mutex: Arc::new(Mutex::new(())),
            tick_loop_cancel: StdMutex::new(CancellationToken::new()),
            power_state: Arc::new(AtomicU8::new(0)),
            dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Acquire the tick mutex (used by `tick_once` to serialize ticks).
    pub fn tick_mutex(&self) -> &Arc<Mutex<()>> {
        &self.tick_mutex
    }

    // ------------------------------------------------------------------
    // Power state
    // ------------------------------------------------------------------

    /// Store the current power state. The tick loop reads this to skip ticks
    /// under low-battery conditions.
    pub fn report_power_state(&self, state: PowerState) {
        let val = match state {
            PowerState::Charging => 0,
            PowerState::Unplugged => 1,
            PowerState::LowBattery => 2,
            PowerState::CriticalBattery => 3,
        };
        self.power_state.store(val, Ordering::Release);
        debug!(state = ?state, multiplier = state.tick_multiplier(), "power state updated");
    }

    // ------------------------------------------------------------------
    // Resume / Pause
    // ------------------------------------------------------------------

    /// Start (or restart) the background tick loop.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    pub fn resume(
        &self,
        runtime: &MobileRuntime,
        storage: &Arc<dyn StorageBackend>,
        handlers: &StdRwLock<Arc<HandlerRegistry>>,
        semaphore: &Arc<Semaphore>,
        scheduler_config: &orch8_types::config::SchedulerConfig,
        sequence_cache: &Arc<SequenceCache>,
        cancel: &CancellationToken,
        notifier: &Arc<MobileNotifier>,
        lifecycle: &Arc<InstanceLifecycleManager>,
        tick_interval_ms: u64,
        max_tick_duration_ms: u64,
        max_instance_lifetime_secs: u64,
        memory_budget: u64,
        sync_reporter: Option<&Arc<SyncReporter>>,
    ) {
        // Cancel the previous tick loop (if any).
        {
            let mut guard = self
                .tick_loop_cancel
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.cancel();
            *guard = CancellationToken::new();
        }

        // Recover stale instances if the engine was paused dirty.
        if self.dirty.swap(false, Ordering::AcqRel) {
            info!("dirty flag set — recovering stale instances before resuming");
            runtime.block_on(async {
                let threshold = scheduler_config.stale_instance_threshold_secs;
                if let Err(e) =
                    orch8_engine::recovery::recover_stale_instances(storage.as_ref(), threshold)
                        .await
                {
                    warn!(error = %e, "stale instance recovery failed");
                }
            });
        }

        // Clone everything the spawned task needs.
        let storage = Arc::clone(storage);
        let handlers = handlers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let semaphore = Arc::clone(semaphore);
        let config = scheduler_config.clone();
        let seq_cache = Arc::clone(sequence_cache);
        let cancel = cancel.clone();
        let tick_mutex = Arc::clone(&self.tick_mutex);
        let loop_cancel = self
            .tick_loop_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let tick_interval = Duration::from_millis(tick_interval_ms);
        let tick_budget = Duration::from_millis(max_tick_duration_ms);
        let notifier = Arc::clone(notifier);
        let lifecycle = Arc::clone(lifecycle);
        let power_state = Arc::clone(&self.power_state);
        let sync_reporter = sync_reporter.map(Arc::clone);

        runtime.handle().spawn(async move {
            tracing::debug!("[orch8] tick loop task spawned, interval={tick_interval:?}");
            let mut ticker = tokio::time::interval(tick_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut tick_count: u64 = 0;

            loop {
                tokio::select! {
                    () = cancel.cancelled() => { tracing::debug!("[orch8] tick loop: cancel signal"); break; }
                    () = loop_cancel.cancelled() => { tracing::debug!("[orch8] tick loop: loop_cancel signal"); break; }
                    _ = ticker.tick() => {
                        if tick_count.is_multiple_of(50) {
                            tracing::debug!("[orch8] tick #{tick_count}");
                        }
                        let ps = PowerState::from_atomic(
                            power_state.load(Ordering::Acquire),
                        );
                        let multiplier = ps.tick_multiplier();
                        if multiplier > 1 && !tick_count.is_multiple_of(u64::from(multiplier)) {
                            tick_count += 1;
                            continue;
                        }

                        if memory::exceeds_budget(memory_budget) {
                            warn!(
                                budget = memory_budget,
                                rss = memory::current_rss_bytes().unwrap_or(0),
                                "tick skipped — memory budget exceeded"
                            );
                            continue;
                        }

                        // Acquire mutex only for tick_once — release before
                        // notifier queries and listener callbacks to prevent
                        // stacking when listeners call back into the engine
                        // (e.g. completeStep from onStepPending).
                        {
                            tracing::trace!("[orch8] tick #{tick_count}: acquiring mutex");
                            let _guard = tick_mutex.lock().await;
                            tracing::trace!("[orch8] tick #{tick_count}: calling tick_once");
                            let tick_result = tokio::time::timeout(
                                tick_budget,
                                tick_once(
                                    &storage, &handlers, &semaphore,
                                    &config, &seq_cache, &cancel,
                                ),
                            ).await;
                            tracing::trace!("[orch8] tick #{tick_count}: tick_once done");
                            match tick_result {
                                Ok(Ok(ref r)) => {
                                    tracing::debug!("[orch8] tick #{tick_count}: advanced={} steps={} pending={}", r.instances_advanced, r.steps_executed, r.has_pending_work);
                                }
                                Ok(Err(ref e)) => tracing::warn!("[orch8] tick #{tick_count} error: {e}"),
                                Err(_) => {
                                    tracing::warn!("[orch8] tick #{tick_count}: TIMEOUT after {}ms", tick_budget.as_millis());
                                }
                            }
                            // _guard dropped here — mutex released before notifications
                        }

                        // Fire notifications outside the mutex so listener
                        // callbacks (which may call completeStep → block_on)
                        // don't deadlock with the next tick or pause().
                        let terminal_ids =
                            notifier.fire_terminal_events(&storage).await;
                        for id in terminal_ids {
                            lifecycle.cleanup_dedup(&id).await;
                        }
                        notifier
                            .fire_step_pending_events(&storage, &seq_cache)
                            .await;

                        // Sync reporter: queue status/approvals periodically,
                        // then sync with server when the interval fires.
                        if let Some(ref reporter) = sync_reporter {
                            if tick_count.is_multiple_of(10) {
                                reporter.scan_and_queue(&storage, &seq_cache).await;
                            }
                            if reporter.should_sync() {
                                reporter.sync_once(&storage, &lifecycle).await;
                            }
                        }

                        tick_count += 1;
                        if tick_count.is_multiple_of(60) {
                            let _ = lifecycle
                                .gc_expired_instances(max_instance_lifetime_secs)
                                .await;
                        }
                    }
                }
            }
            info!("mobile tick loop stopped");
        });

        info!("mobile tick loop started");
    }

    /// Cancel the tick loop and wait for the current tick to finish.
    pub fn pause(&self, runtime: &MobileRuntime, max_tick_duration_ms: u64) {
        self.tick_loop_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancel();
        runtime.block_on(async {
            let timeout = Duration::from_millis(max_tick_duration_ms);
            if tokio::time::timeout(timeout, self.tick_mutex.lock())
                .await
                .is_ok()
            {
                debug!("mobile engine paused cleanly");
            } else {
                self.dirty.store(true, Ordering::Release);
                warn!("pause timed out waiting for current tick — marked dirty for recovery");
            }
        });
    }

    /// Cancel the tick loop without waiting (used during shutdown).
    pub fn cancel_loop(&self) {
        self.tick_loop_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_controller_has_clean_state() {
        let ctrl = TickController::new();
        assert!(!ctrl.dirty.load(Ordering::Acquire));
        assert_eq!(ctrl.power_state.load(Ordering::Acquire), 0);
    }

    #[test]
    fn report_power_state_stores_value() {
        let ctrl = TickController::new();
        ctrl.report_power_state(PowerState::CriticalBattery);
        assert_eq!(ctrl.power_state.load(Ordering::Acquire), 3);
        ctrl.report_power_state(PowerState::Charging);
        assert_eq!(ctrl.power_state.load(Ordering::Acquire), 0);
    }

    #[test]
    fn cancel_loop_does_not_panic() {
        let ctrl = TickController::new();
        ctrl.cancel_loop();
        // Calling again should also be fine.
        ctrl.cancel_loop();
    }
}
