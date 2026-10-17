//! Engine liveness: a last-tick heartbeat shared with the health probes.
//!
//! `/health/ready` used to report ready as long as `engine.run()` had not
//! *returned*, and `/health/live` was always 200 (I-H11). A wedged tick loop
//! (a blocked claim query, a sweep that never finishes) therefore stayed
//! ready and live forever, and the orchestrator never restarted it. The tick
//! loop now stamps [`EngineLiveness::record_tick`] at the end of every
//! iteration (claim + all sweeps); the probes compare the age of that stamp
//! against two thresholds.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Floor for the readiness staleness window, whatever the tick interval.
/// One iteration (claim + marker preload + sweeps) can legitimately take
/// tens of seconds when the database is slow; a 30 s floor made
/// `/health/ready` flap to 503 under such load (D-04).
const MIN_READY_STALE: Duration = Duration::from_secs(90);

/// Shared, lock-free tick heartbeat. Cheap to read from request handlers.
#[derive(Debug)]
pub struct EngineLiveness {
    /// Monotonic base for `last_tick_ms`.
    epoch: Instant,
    /// Milliseconds since `epoch` of the last completed tick iteration (or of
    /// [`Self::arm`], so startup gets a full grace window).
    last_tick_ms: AtomicU64,
    /// Set while an engine is running on this process. Unarmed liveness
    /// (API-only roles, an active-passive standby that never started the
    /// scheduler, a cleanly stopped engine) never fails a probe.
    armed: AtomicBool,
    ready_stale_after: Duration,
    live_stale_after: Duration,
}

impl EngineLiveness {
    /// `tick_interval` sizes the readiness window (`max(10 × tick, 90 s)`);
    /// `live_stale_after` is the liveness window. A zero liveness window
    /// disables the liveness check (readiness still applies).
    #[must_use]
    pub fn new(tick_interval: Duration, live_stale_after: Duration) -> Self {
        Self {
            epoch: Instant::now(),
            last_tick_ms: AtomicU64::new(0),
            armed: AtomicBool::new(false),
            ready_stale_after: tick_interval.saturating_mul(10).max(MIN_READY_STALE),
            live_stale_after,
        }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Mark the engine as running and reset the baseline to now. Called once
    /// when `Engine::run` starts, before the startup steps, so a startup that
    /// keeps retrying eventually fails liveness and gets restarted.
    pub fn arm(&self) {
        self.record_tick();
        self.armed.store(true, Ordering::Release);
    }

    /// The engine stopped cleanly; stop judging staleness.
    pub fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    /// Stamp a completed tick-loop iteration.
    pub fn record_tick(&self) {
        self.last_tick_ms.store(self.now_ms(), Ordering::Release);
    }

    /// Age of the last tick stamp, or `None` when no engine is armed.
    #[must_use]
    pub fn tick_age(&self) -> Option<Duration> {
        if !self.armed.load(Ordering::Acquire) {
            return None;
        }
        let last = self.last_tick_ms.load(Ordering::Acquire);
        Some(Duration::from_millis(self.now_ms().saturating_sub(last)))
    }

    /// `true` when an armed engine has not completed a tick within the
    /// readiness window (`max(10 × tick, 90 s)`).
    #[must_use]
    pub fn is_ready_stale(&self) -> bool {
        self.tick_age()
            .is_some_and(|age| age > self.ready_stale_after)
    }

    /// `true` when an armed engine has not completed a tick within the
    /// liveness window, so the process should be restarted.
    #[must_use]
    pub fn is_live_stale(&self) -> bool {
        !self.live_stale_after.is_zero()
            && self
                .tick_age()
                .is_some_and(|age| age > self.live_stale_after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unarmed_liveness_never_reports_stale() {
        let l = EngineLiveness::new(Duration::from_millis(100), Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(l.tick_age(), None);
        assert!(!l.is_ready_stale());
        assert!(!l.is_live_stale());
    }

    #[test]
    fn armed_liveness_goes_stale_without_ticks_and_recovers_on_tick() {
        let l = EngineLiveness::new(Duration::from_millis(1), Duration::from_millis(20));
        l.arm();
        assert!(!l.is_live_stale());
        std::thread::sleep(Duration::from_millis(40));
        assert!(l.is_live_stale(), "no tick for 40ms > 20ms window");
        // Readiness floor is 90 s, so it is not stale yet.
        assert!(!l.is_ready_stale());
        l.record_tick();
        assert!(!l.is_live_stale());
        l.disarm();
        std::thread::sleep(Duration::from_millis(40));
        assert!(!l.is_live_stale(), "a disarmed engine is never stale");
    }

    #[test]
    fn readiness_window_scales_with_tick_interval() {
        let l = EngineLiveness::new(Duration::from_secs(10), Duration::ZERO);
        assert_eq!(l.ready_stale_after, Duration::from_secs(100));
        let l = EngineLiveness::new(Duration::from_millis(100), Duration::ZERO);
        assert_eq!(l.ready_stale_after, MIN_READY_STALE);
        assert!(
            MIN_READY_STALE >= Duration::from_secs(90),
            "a slow-but-progressing DB iteration must not flap readiness (D-04)"
        );
        l.arm();
        assert!(!l.is_live_stale(), "zero window disables liveness");
    }
}
