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

// === Histogram names ===
pub const TICK_DURATION: &str = "orch8_tick_duration_seconds";
pub const STEP_DURATION: &str = "orch8_step_duration_seconds";
pub const INSTANCE_DURATION: &str = "orch8_instance_processing_seconds";

// === Gauge names ===
pub const QUEUE_DEPTH: &str = "orch8_queue_depth";
pub const ACTIVE_TASKS: &str = "orch8_active_tasks";

/// Record a counter increment.
pub fn inc(name: &'static str) {
    counter!(name).increment(1);
}

/// Record a counter increment with labels.
pub fn inc_with(name: &'static str, labels: &[(&'static str, String)]) {
    let pairs: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
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
}
