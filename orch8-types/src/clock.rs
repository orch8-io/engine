//! Virtual time for the scheduler.
//!
//! All *scheduling decisions* in the engine (claiming due instances, delay /
//! send-window / rate-limit deferrals, retry backoff, SLA-deadline and
//! human-input timeout checks, cron evaluation) read "now" through a
//! [`Clock`] instead of calling [`Utc::now()`] directly. Production code uses
//! [`SystemClock`] (the default everywhere), which is byte-for-byte the old
//! behavior. Tests inject a [`ManualClock`] and advance it explicitly, so a
//! workflow with a 3-day delay completes in a millisecond-scale test run.
//!
//! # What the clock does NOT govern
//!
//! Record timestamps stay on real time by design:
//!
//! - `created_at` / `updated_at` stamping of rows (instances, outputs,
//!   signals, worker tasks) — these are audit data, not scheduling inputs.
//! - Database-side `NOW()` in `UPDATE ... SET updated_at = NOW()` clauses.
//!   The claim queries themselves bind an app-side timestamp parameter
//!   (`next_fire_at <= $now`), so the virtual clock fully controls claiming;
//!   clock skew between app and DB only affects audit columns.
//! - Background sweeps outside the tick loop (stale-instance recovery, GC,
//!   trigger sync) and logging.
//!
//! # `ManualClock` discipline
//!
//! Start a [`ManualClock`] **at or after the current real time** and only move
//! it forward. A few "wake immediately" paths (e.g. parent wake-up after a
//! child completes) stamp `next_fire_at` with real time; as long as virtual
//! time >= real time those instances remain due under the virtual clock.

use std::fmt;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};

/// Source of "now" for scheduling decisions.
///
/// Implementations must be cheap to call and safe to share across tasks.
pub trait Clock: Send + Sync + 'static {
    /// The current instant according to this clock.
    fn now(&self) -> DateTime<Utc>;
}

/// Production clock: returns the real [`Utc::now()`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Test clock: starts at a fixed instant and only moves when told to.
///
/// Share it via `Arc<ManualClock>` — keep one handle for the scheduler (as a
/// [`SharedClock`]) and one for the test body to call [`ManualClock::advance`]
/// or [`ManualClock::set`].
#[derive(Debug)]
pub struct ManualClock {
    now: RwLock<DateTime<Utc>>,
}

impl ManualClock {
    /// Create a clock frozen at `start`.
    #[must_use]
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            now: RwLock::new(start),
        }
    }

    /// Move the clock forward (or backward, for negative durations) by `delta`.
    pub fn advance(&self, delta: Duration) {
        let mut now = self.now.write().expect("ManualClock lock poisoned");
        *now += delta;
    }

    /// Jump the clock to an absolute instant.
    pub fn set(&self, to: DateTime<Utc>) {
        let mut now = self.now.write().expect("ManualClock lock poisoned");
        *now = to;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.read().expect("ManualClock lock poisoned")
    }
}

/// Cheaply cloneable, type-erased clock handle.
///
/// Lives inside [`crate::config::SchedulerConfig`] so the scheduler, cron
/// loop, and step pre-flight checks all read the same time source without any
/// public signature changes. Defaults to [`SystemClock`]; the field is skipped
/// during (de)serialization, so config files are unaffected.
#[derive(Clone)]
pub struct SharedClock(Arc<dyn Clock>);

impl SharedClock {
    /// Wrap a concrete clock.
    pub fn new<C: Clock>(clock: C) -> Self {
        Self(Arc::new(clock))
    }

    /// Wrap an already-shared clock (e.g. an `Arc<ManualClock>` the test also
    /// keeps a handle to for advancing time).
    #[must_use]
    pub fn from_arc(clock: Arc<dyn Clock>) -> Self {
        Self(clock)
    }

    /// The current instant according to the wrapped clock.
    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        self.0.now()
    }
}

impl Clock for SharedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0.now()
    }
}

impl Default for SharedClock {
    fn default() -> Self {
        Self(Arc::new(SystemClock))
    }
}

impl fmt::Debug for SharedClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SharedClock(..)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_tracks_real_time() {
        let clock = SystemClock;
        let before = Utc::now();
        let observed = clock.now();
        let after = Utc::now();
        assert!(before <= observed && observed <= after);
    }

    #[test]
    fn manual_clock_is_frozen_until_advanced() {
        let start = Utc::now();
        let clock = ManualClock::new(start);
        assert_eq!(clock.now(), start);
        assert_eq!(clock.now(), start, "repeated reads must not drift");
    }

    #[test]
    fn manual_clock_advance_moves_forward() {
        let start = Utc::now();
        let clock = ManualClock::new(start);
        clock.advance(Duration::days(3));
        assert_eq!(clock.now(), start + Duration::days(3));
        clock.advance(Duration::seconds(1));
        assert_eq!(
            clock.now(),
            start + Duration::days(3) + Duration::seconds(1)
        );
    }

    #[test]
    fn manual_clock_set_jumps_to_absolute_instant() {
        let start = Utc::now();
        let clock = ManualClock::new(start);
        let target = start + Duration::days(30);
        clock.set(target);
        assert_eq!(clock.now(), target);
        // set() can also move backwards.
        clock.set(start);
        assert_eq!(clock.now(), start);
    }

    #[test]
    fn manual_clock_is_shareable_across_handles() {
        let start = Utc::now();
        let manual = Arc::new(ManualClock::new(start));
        let shared = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);
        manual.advance(Duration::hours(2));
        assert_eq!(shared.now(), start + Duration::hours(2));
    }

    #[test]
    fn shared_clock_defaults_to_system_time() {
        let shared = SharedClock::default();
        let before = Utc::now();
        let observed = shared.now();
        let after = Utc::now();
        assert!(before <= observed && observed <= after);
    }
}
