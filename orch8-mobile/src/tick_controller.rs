//! Tick loop lifecycle management for the mobile engine.
//!
//! `TickController` owns the tick loop cancellation token, the tick mutex that
//! serializes tick execution, the power-state atom, and the dirty flag used for
//! stale-instance recovery on resume.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;

use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::filter::InstanceFilter;
use orch8_types::instance::InstanceState;

use crate::PowerState;
use crate::lifecycle::InstanceLifecycleManager;
use crate::memory;
use crate::notifier::MobileNotifier;
use crate::runtime::MobileRuntime;
use crate::sync_reporter::SyncReporter;

/// Manages the tick loop lifecycle: resume, pause, power-state adaptation,
/// and stale-instance recovery.
pub(crate) struct TickController {
    tick_mutex: Arc<Mutex<()>>,
    tick_loop_cancel: StdMutex<CancellationToken>,
    power_state: Arc<AtomicU8>,
    work_available: Arc<Notify>,
    power_changed: Arc<Notify>,
    dirty: Arc<AtomicBool>,
}

const RSS_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
const RSS_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const MAX_IDLE_INTERVAL: Duration = Duration::from_secs(5);
const SYNC_SCAN_INTERVAL: Duration = Duration::from_secs(5);
const INSTANCE_GC_INTERVAL: Duration = Duration::from_secs(60);

impl TickController {
    pub fn new() -> Self {
        Self {
            tick_mutex: Arc::new(Mutex::new(())),
            tick_loop_cancel: StdMutex::new(CancellationToken::new()),
            power_state: Arc::new(AtomicU8::new(0)),
            work_available: Arc::new(Notify::new()),
            power_changed: Arc::new(Notify::new()),
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

    /// Store the current power state and wake the loop so its timer is rebuilt
    /// with the new battery-aware interval.
    pub fn report_power_state(&self, state: PowerState) {
        let val = match state {
            PowerState::Charging => 0,
            PowerState::Unplugged => 1,
            PowerState::LowBattery => 2,
            PowerState::CriticalBattery => 3,
        };
        self.power_state.store(val, Ordering::Release);
        self.power_changed.notify_one();
        debug!(state = ?state, multiplier = state.tick_multiplier(), "power state updated");
    }

    /// Wake the background loop because host activity made work immediately
    /// runnable. `Notify` retains one permit, so signals are not lost when the
    /// loop is currently executing a tick.
    pub fn wake(&self) {
        self.work_available.notify_one();
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
        let tick_interval = Duration::from_millis(tick_interval_ms.max(1));
        let tick_budget = Duration::from_millis(max_tick_duration_ms);
        let notifier = Arc::clone(notifier);
        let lifecycle = Arc::clone(lifecycle);
        let power_state = Arc::clone(&self.power_state);
        let work_available = Arc::clone(&self.work_available);
        let power_changed = Arc::clone(&self.power_changed);
        let sync_reporter = sync_reporter.map(Arc::clone);

        runtime.handle().spawn(async move {
            tracing::debug!("[orch8] tick loop task spawned, interval={tick_interval:?}");
            let mut tick_count: u64 = 0;
            let mut next_tick = Instant::now();
            let mut next_memory_check = Instant::now();
            let mut memory_budget_exceeded = false;
            let mut idle_streak: u32 = 0;
            let mut next_sync_scan = Instant::now();
            let mut next_instance_gc = Instant::now() + INSTANCE_GC_INTERVAL;
            let mut notification_scan_due = true;
            let mut scheduler_is_quiescent = false;

            loop {
                let sleep = tokio::time::sleep_until(next_tick);
                tokio::pin!(sleep);
                let woke_for_work = tokio::select! {
                    biased;
                    () = cancel.cancelled() => { tracing::debug!("[orch8] tick loop: cancel signal"); break; }
                    () = loop_cancel.cancelled() => { tracing::debug!("[orch8] tick loop: loop_cancel signal"); break; }
                    () = power_changed.notified() => {
                        let state = PowerState::from_atomic(power_state.load(Ordering::Acquire));
                        let now = Instant::now();
                        next_tick = now + if scheduler_is_quiescent {
                            quiescent_tick_interval(
                                now,
                                next_instance_gc,
                                sync_reporter.as_deref(),
                            )
                        } else {
                            scheduled_tick_interval(tick_interval, state, idle_streak)
                        };
                        continue;
                    }
                    () = work_available.notified() => true,
                    () = &mut sleep => false,
                };
                if woke_for_work {
                    idle_streak = 0;
                    scheduler_is_quiescent = false;
                }

                        if tick_count.is_multiple_of(50) {
                            tracing::debug!("[orch8] tick #{tick_count}");
                        }
                        let ps = PowerState::from_atomic(
                            power_state.load(Ordering::Acquire),
                        );

                        // RSS sampling is deliberately much slower than the
                        // scheduler cadence. On Darwin it launches `ps`; on
                        // Linux/Android it reads procfs. Doing either on every
                        // tick wastes CPU and battery when memory is healthy.
                        let now = Instant::now();
                        if memory_budget != 0 && now >= next_memory_check {
                            let rss = tokio::task::spawn_blocking(memory::current_rss_bytes)
                                .await
                                .ok()
                                .flatten();
                            memory_budget_exceeded =
                                rss.is_some_and(|bytes| bytes > memory_budget);
                            next_memory_check = now
                                + memory_sample_interval(memory_budget_exceeded);
                            if memory_budget_exceeded {
                                warn!(
                                    budget = memory_budget,
                                    rss = rss.unwrap_or(0),
                                    "tick skipped — memory budget exceeded"
                                );
                            }
                        }

                        if memory_budget_exceeded {
                            next_tick = Instant::now()
                                + effective_tick_interval(tick_interval, ps);
                            continue;
                        }

                        let mut scheduler_has_work = true;
                        let mut lifecycle_may_have_changed = true;

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
                                    lifecycle_may_have_changed =
                                        r.instances_advanced != 0 || r.steps_executed != 0;
                                    scheduler_has_work = r.has_pending_work
                                        || lifecycle_may_have_changed;
                                    tracing::debug!("[orch8] tick #{tick_count}: advanced={} steps={} pending={}", r.instances_advanced, r.steps_executed, r.has_pending_work);
                                }
                                Ok(Err(ref e)) => tracing::warn!("[orch8] tick #{tick_count} error: {e}"),
                                Err(_) => {
                                    tracing::warn!("[orch8] tick #{tick_count}: TIMEOUT after {}ms", tick_budget.as_millis());
                                }
                            }
                            // _guard dropped here — mutex released before notifications
                        }

                        // `has_pending_work` covers every Scheduled instance,
                        // including future timers. Only pay for this second,
                        // cheap count when none exist. Running, Waiting, and
                        // Paused instances still need polling for deadlines and
                        // SLA alerts; terminal-only storage can sleep until the
                        // next maintenance deadline.
                        if !scheduler_has_work {
                            match storage_is_scheduler_quiescent(&storage).await {
                                Ok(true) => scheduler_is_quiescent = true,
                                Ok(false) => scheduler_has_work = true,
                                Err(e) => {
                                    scheduler_has_work = true;
                                    warn!(error = %e, "failed to check scheduler quiescence");
                                }
                            }
                        }

                        // Query notifications only when lifecycle state could
                        // have changed. The startup scan covers persisted events;
                        // settled idle ticks contain no new information.
                        if notification_scan_due || lifecycle_may_have_changed || woke_for_work {
                            let terminal_ids = notifier.fire_terminal_events(&storage).await;
                            for id in terminal_ids {
                                lifecycle.cleanup_dedup(&id).await;
                            }
                            notifier
                                .fire_step_pending_events(&storage, &seq_cache)
                                .await;
                            notification_scan_due = false;
                        }

                        // Sync reporter: queue status/approvals periodically,
                        // then sync when its wall-clock deadline fires.
                        if let Some(ref reporter) = sync_reporter {
                            let now = Instant::now();
                            let should_sync = reporter.should_sync();
                            let activity_scan_due =
                                (scheduler_has_work || woke_for_work) && now >= next_sync_scan;
                            if should_sync || activity_scan_due {
                                reporter.scan_and_queue(&storage, &seq_cache).await;
                                next_sync_scan = now + SYNC_SCAN_INTERVAL;
                            }
                            if should_sync {
                                reporter.sync_once(&storage, &lifecycle).await;
                                // Server commands may have scheduled work after
                                // `tick_once` computed its result. Re-check the
                                // cheap state count instead of forcing another
                                // full scheduler pass after an empty response.
                                match storage_is_scheduler_quiescent(&storage).await {
                                    Ok(is_quiescent) => {
                                        scheduler_is_quiescent = is_quiescent;
                                        scheduler_has_work = !is_quiescent;
                                    }
                                    Err(e) => {
                                        scheduler_has_work = true;
                                        scheduler_is_quiescent = false;
                                        warn!(error = %e, "failed to check post-sync scheduler quiescence");
                                    }
                                }
                            }
                        }

                        tick_count += 1;
                        let now = Instant::now();
                        if now >= next_instance_gc
                            && let Err(e) = lifecycle
                                .gc_expired_instances(max_instance_lifetime_secs)
                                .await
                        {
                            warn!(error = %e, "periodic instance GC failed");
                        }
                        if now >= next_instance_gc {
                            next_instance_gc = now + INSTANCE_GC_INTERVAL;
                        }

                        if scheduler_has_work {
                            idle_streak = 0;
                            scheduler_is_quiescent = false;
                        } else {
                            idle_streak = idle_streak.saturating_add(1).min(4);
                        }
                        let now = Instant::now();
                        next_tick = now + if scheduler_is_quiescent {
                            quiescent_tick_interval(
                                now,
                                next_instance_gc,
                                sync_reporter.as_deref(),
                            )
                        } else {
                            scheduled_tick_interval(tick_interval, ps, idle_streak)
                        };
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

fn effective_tick_interval(base: Duration, power_state: PowerState) -> Duration {
    base.saturating_mul(power_state.tick_multiplier())
}

fn scheduled_tick_interval(base: Duration, power_state: PowerState, idle_streak: u32) -> Duration {
    let active = effective_tick_interval(base, power_state);
    if idle_streak == 0 {
        return active;
    }

    let multiplier = 1_u32 << idle_streak.min(4);
    active
        .saturating_mul(multiplier)
        .min(MAX_IDLE_INTERVAL.max(active))
}

fn quiescent_tick_interval(
    now: Instant,
    next_instance_gc: Instant,
    sync_reporter: Option<&SyncReporter>,
) -> Duration {
    let until_gc = next_instance_gc.saturating_duration_since(now);
    sync_reporter.map_or(until_gc, |reporter| {
        until_gc.min(reporter.next_sync_delay())
    })
}

async fn storage_is_scheduler_quiescent(
    storage: &Arc<dyn StorageBackend>,
) -> Result<bool, orch8_types::error::StorageError> {
    Ok(storage
        .count_instances(&scheduler_active_instance_filter())
        .await?
        == 0)
}

fn scheduler_active_instance_filter() -> InstanceFilter {
    InstanceFilter {
        states: Some(vec![
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
        ]),
        ..Default::default()
    }
}

const fn memory_sample_interval(budget_exceeded: bool) -> Duration {
    if budget_exceeded {
        RSS_RETRY_INTERVAL
    } else {
        RSS_SAMPLE_INTERVAL
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

    #[test]
    fn power_state_changes_real_timer_interval() {
        let base = Duration::from_millis(500);
        assert_eq!(
            effective_tick_interval(base, PowerState::Charging),
            Duration::from_millis(500)
        );
        assert_eq!(
            effective_tick_interval(base, PowerState::LowBattery),
            Duration::from_secs(1)
        );
        assert_eq!(
            effective_tick_interval(base, PowerState::CriticalBattery),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn rss_sampling_slows_down_when_memory_is_healthy() {
        assert_eq!(memory_sample_interval(false), Duration::from_secs(30));
        assert_eq!(memory_sample_interval(true), Duration::from_secs(5));
    }

    #[test]
    fn idle_scheduler_backs_off_but_keeps_configured_floor() {
        let base = Duration::from_millis(500);
        assert_eq!(
            scheduled_tick_interval(base, PowerState::Unplugged, 0),
            Duration::from_millis(500)
        );
        assert_eq!(
            scheduled_tick_interval(base, PowerState::Unplugged, 1),
            Duration::from_secs(1)
        );
        assert_eq!(
            scheduled_tick_interval(base, PowerState::Unplugged, 2),
            Duration::from_secs(2)
        );
        assert_eq!(
            scheduled_tick_interval(base, PowerState::Unplugged, 3),
            Duration::from_secs(4)
        );
        assert_eq!(
            scheduled_tick_interval(base, PowerState::Unplugged, 4),
            Duration::from_secs(5)
        );
        assert_eq!(
            scheduled_tick_interval(Duration::from_secs(10), PowerState::Unplugged, 4),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn critical_battery_and_idle_backoff_compose() {
        let base = Duration::from_millis(500);
        assert_eq!(
            scheduled_tick_interval(base, PowerState::CriticalBattery, 0),
            Duration::from_secs(2)
        );
        assert_eq!(
            scheduled_tick_interval(base, PowerState::CriticalBattery, 1),
            Duration::from_secs(4)
        );
        assert_eq!(
            scheduled_tick_interval(base, PowerState::CriticalBattery, 2),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn quiescent_scheduler_sleeps_until_gc_without_sync() {
        let now = Instant::now();
        assert_eq!(
            quiescent_tick_interval(now, now + INSTANCE_GC_INTERVAL, None),
            INSTANCE_GC_INTERVAL
        );
    }

    #[test]
    fn quiescence_excludes_every_active_or_deadline_bearing_state() {
        assert_eq!(
            scheduler_active_instance_filter().states,
            Some(vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
                InstanceState::Paused,
            ])
        );
    }
}
