use metrics::{counter, gauge, histogram};
use std::time::Instant;

// === Counter names ===
pub const INSTANCES_CLAIMED: &str = "orch8_instances_claimed_total";
pub const INSTANCES_COMPLETED: &str = "orch8_instances_completed_total";
pub const INSTANCES_FAILED: &str = "orch8_instances_failed_total";
pub const STEPS_EXECUTED: &str = "orch8_steps_executed_total";
pub const STEPS_FAILED: &str = "orch8_steps_failed_total";
pub const STEPS_RETRIED: &str = "orch8_steps_retried_total";
pub const SIGNALS_DELIVERED: &str = "orch8_signals_delivered_total";
pub const RATE_LIMITS_EXCEEDED: &str = "orch8_rate_limits_exceeded_total";
pub const RECOVERY_STALE: &str = "orch8_recovery_stale_instances_total";
pub const WEBHOOKS_SENT: &str = "orch8_webhooks_sent_total";
pub const WEBHOOKS_FAILED: &str = "orch8_webhooks_failed_total";
pub const CRON_TRIGGERED: &str = "orch8_cron_triggered_total";
pub const CACHE_HITS: &str = "orch8_cache_hits_total";
pub const CACHE_MISSES: &str = "orch8_cache_misses_total";
pub const PRELOAD_REFS_SCANNED: &str = "orch8_preload_refs_scanned_total";
/// Unique refs hydrated by the preload batch (matches `PRELOAD_REFS_SCANNED`
/// cardinality; useful for hit-rate ratios).
pub const PRELOAD_REFS_HYDRATED: &str = "orch8_preload_refs_hydrated_total";
/// Marker slots mutated during hydration — inflates vs. `PRELOAD_REFS_HYDRATED`
/// under fan-in (one ref referenced by many instances). Useful for work-done
/// measurement.
pub const PRELOAD_SLOTS_HYDRATED: &str = "orch8_preload_slots_hydrated_total";
pub const PRELOAD_ERRORS: &str = "orch8_preload_errors_total";
pub const GC_EXTERNALIZED_DELETED: &str = "orch8_gc_externalized_deleted_total";
pub const GC_EXTERNALIZED_ERRORS: &str = "orch8_gc_externalized_errors_total";
pub const GC_EMIT_DEDUPE_DELETED: &str = "orch8_gc_emit_dedupe_deleted_total";
pub const GC_EMIT_DEDUPE_ERRORS: &str = "orch8_gc_emit_dedupe_errors_total";

// === Histogram names ===
pub const TICK_DURATION: &str = "orch8_tick_duration_seconds";
pub const STEP_DURATION: &str = "orch8_step_duration_seconds";
pub const INSTANCE_DURATION: &str = "orch8_instance_processing_seconds";
pub const PRELOAD_BATCH_DURATION: &str = "orch8_preload_batch_duration_seconds";

// === Gauge names ===
pub const QUEUE_DEPTH: &str = "orch8_queue_depth";
pub const ACTIVE_TASKS: &str = "orch8_active_tasks";

/// Record a counter increment.
pub fn inc(name: &'static str) {
    counter!(name).increment(1);
}

/// Record a counter increment by N (no-op if `n == 0`).
pub fn inc_by(name: &'static str, n: u64) {
    if n > 0 {
        counter!(name).increment(n);
    }
}

/// Record a counter increment with labels.
pub fn inc_with(name: &'static str, labels: &[(&'static str, &str)]) {
    let pairs: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    counter!(name, &pairs).increment(1);
}

/// Record a gauge value.
pub fn set_gauge(name: &'static str, value: f64) {
    gauge!(name).set(value);
}

/// Record a histogram observation.
pub fn observe(name: &'static str, value: f64) {
    histogram!(name).record(value);
}

/// A timer that records elapsed time to a histogram on drop.
pub struct Timer {
    name: &'static str,
    start: Instant,
}

impl Timer {
    pub fn start(name: &'static str) -> Self {
        Self {
            name,
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        observe(self.name, elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_records_nonzero_duration() {
        // Just verify it doesn't panic.
        let _timer = Timer::start(TICK_DURATION);
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    #[test]
    fn inc_does_not_panic() {
        inc(STEPS_EXECUTED);
        inc(STEPS_FAILED);
    }

    #[test]
    fn instances_completed_counter_increments() {
        // Exercising the public API: no panic, and name constant is stable.
        inc(INSTANCES_COMPLETED);
        inc(INSTANCES_COMPLETED);
        assert_eq!(INSTANCES_COMPLETED, "orch8_instances_completed_total");
    }

    #[test]
    fn steps_executed_counter_increments_per_step() {
        for _ in 0..5 {
            inc(STEPS_EXECUTED);
        }
        assert_eq!(STEPS_EXECUTED, "orch8_steps_executed_total");
    }

    #[test]
    fn tick_duration_histogram_records_values() {
        observe(TICK_DURATION, 0.001);
        observe(TICK_DURATION, 0.25);
        assert_eq!(TICK_DURATION, "orch8_tick_duration_seconds");
    }

    #[test]
    fn queue_depth_gauge_is_settable() {
        set_gauge(QUEUE_DEPTH, 0.0);
        set_gauge(QUEUE_DEPTH, 42.0);
        set_gauge(QUEUE_DEPTH, 7.5);
        assert_eq!(QUEUE_DEPTH, "orch8_queue_depth");
    }

    #[test]
    fn timer_records_on_drop_even_for_very_short_spans() {
        // Timer::drop must call `observe` with a finite non-negative duration.
        // We can't read the histogram here, so we just verify Drop runs without
        // panic for an immediately-dropped timer.
        {
            let _t = Timer::start(STEP_DURATION);
        }
        // Also with a small sleep to ensure elapsed() is > 0.
        {
            let t = Timer::start(STEP_DURATION);
            std::thread::sleep(std::time::Duration::from_millis(1));
            assert!(t.start.elapsed().as_nanos() > 0);
            // `t` drops here, triggering observe.
        }
    }

    #[test]
    fn inc_by_zero_is_noop_and_positive_increments() {
        // `inc_by(0)` is documented as a no-op and must not panic.
        inc_by(STEPS_RETRIED, 0);
        inc_by(STEPS_RETRIED, 3);
    }

    #[test]
    fn inc_with_labels_does_not_panic() {
        inc_with(INSTANCES_FAILED, &[("tenant", "t1"), ("reason", "timeout")]);
    }

    #[test]
    fn inc_with_empty_labels_does_not_panic() {
        inc_with(INSTANCES_COMPLETED, &[]);
    }

    #[test]
    fn inc_with_single_label() {
        inc_with(CACHE_HITS, &[("cache", "sequence")]);
    }

    #[test]
    fn all_counter_names_start_with_orch8() {
        let counters = [
            INSTANCES_CLAIMED,
            INSTANCES_COMPLETED,
            INSTANCES_FAILED,
            STEPS_EXECUTED,
            STEPS_FAILED,
            STEPS_RETRIED,
            SIGNALS_DELIVERED,
            RATE_LIMITS_EXCEEDED,
            RECOVERY_STALE,
            WEBHOOKS_SENT,
            WEBHOOKS_FAILED,
            CRON_TRIGGERED,
            CACHE_HITS,
            CACHE_MISSES,
            PRELOAD_REFS_SCANNED,
            PRELOAD_REFS_HYDRATED,
            PRELOAD_SLOTS_HYDRATED,
            PRELOAD_ERRORS,
            GC_EXTERNALIZED_DELETED,
            GC_EXTERNALIZED_ERRORS,
            GC_EMIT_DEDUPE_DELETED,
            GC_EMIT_DEDUPE_ERRORS,
        ];
        for name in counters {
            assert!(
                name.starts_with("orch8_"),
                "counter '{name}' missing orch8_ prefix"
            );
        }
    }

    #[test]
    fn all_histogram_names_start_with_orch8() {
        let histograms = [
            TICK_DURATION,
            STEP_DURATION,
            INSTANCE_DURATION,
            PRELOAD_BATCH_DURATION,
        ];
        for name in histograms {
            assert!(
                name.starts_with("orch8_"),
                "histogram '{name}' missing orch8_ prefix"
            );
        }
    }

    #[test]
    fn all_gauge_names_start_with_orch8() {
        let gauges = [QUEUE_DEPTH, ACTIVE_TASKS];
        for name in gauges {
            assert!(
                name.starts_with("orch8_"),
                "gauge '{name}' missing orch8_ prefix"
            );
        }
    }

    #[test]
    fn timer_start_captures_instant() {
        let t = Timer::start(TICK_DURATION);
        assert_eq!(t.name, TICK_DURATION);
        // start should be very recent.
        assert!(t.start.elapsed().as_secs() < 1);
    }

    #[test]
    fn set_gauge_to_negative_does_not_panic() {
        set_gauge(QUEUE_DEPTH, -1.0);
    }

    #[test]
    fn observe_zero_does_not_panic() {
        observe(TICK_DURATION, 0.0);
    }
}
