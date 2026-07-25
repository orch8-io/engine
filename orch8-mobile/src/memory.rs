use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) const HEALTHY_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const EXCEEDED_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(crate) struct MemoryBudgetSampler {
    state: Mutex<MemorySampleState>,
}

#[derive(Default)]
struct MemorySampleState {
    checked_at: Option<Instant>,
    rss: Option<u64>,
    exceeded: bool,
}

impl MemoryBudgetSampler {
    /// Return the cached over-budget RSS, refreshing it only when stale.
    /// This prevents rapid manual ticks and `run_until_idle` batches from
    /// launching one Darwin `ps` subprocess per tick.
    pub(crate) fn over_budget(&self, budget_bytes: u64) -> Option<u64> {
        if budget_bytes == 0 {
            return None;
        }

        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let interval = if state.exceeded {
            EXCEEDED_SAMPLE_INTERVAL
        } else {
            HEALTHY_SAMPLE_INTERVAL
        };
        let sample_due = state
            .checked_at
            .is_none_or(|checked_at| now.duration_since(checked_at) >= interval);

        if sample_due {
            state.rss = current_rss_bytes();
            state.exceeded = state.rss.is_some_and(|rss| rss > budget_bytes);
            state.checked_at = Some(now);
        }

        state.exceeded.then_some(state.rss).flatten()
    }
}

/// Returns the current process RSS in bytes, or `None` if unavailable.
pub fn current_rss_bytes() -> Option<u64> {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        darwin_rss()
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        linux_rss()
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "linux",
        target_os = "android"
    )))]
    {
        None
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn darwin_rss() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let kb: u64 = text.trim().parse().ok()?;
    Some(kb * 1024)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().strip_suffix("kB")?.trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(test)]
pub fn exceeds_budget(budget_bytes: u64) -> bool {
    rss_over_budget(budget_bytes).is_some()
}

/// Return the sampled RSS only when it exceeds the configured budget.
/// Sampling once lets callers report the value without launching a duplicate
/// process/procfs read on the over-budget path.
#[cfg(test)]
pub fn rss_over_budget(budget_bytes: u64) -> Option<u64> {
    (budget_bytes != 0)
        .then(current_rss_bytes)
        .flatten()
        .filter(|rss| *rss > budget_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_returns_positive_value() {
        if let Some(rss) = current_rss_bytes() {
            assert!(rss > 0, "RSS should be positive");
        }
    }

    #[test]
    fn zero_budget_never_exceeds() {
        assert!(!exceeds_budget(0));
    }

    #[test]
    fn huge_budget_never_exceeds() {
        assert!(!exceeds_budget(u64::MAX));
    }

    #[test]
    fn zero_budget_does_not_sample_as_exceeded() {
        assert_eq!(rss_over_budget(0), None);
    }

    #[test]
    fn sampler_caches_a_healthy_measurement() {
        let sampler = MemoryBudgetSampler::default();
        assert_eq!(sampler.over_budget(u64::MAX), None);
        let first_check = sampler.state.lock().unwrap().checked_at;

        assert_eq!(sampler.over_budget(u64::MAX), None);
        assert_eq!(sampler.state.lock().unwrap().checked_at, first_check);
    }

    #[test]
    fn sampler_skips_disabled_budget_without_sampling() {
        let sampler = MemoryBudgetSampler::default();
        assert_eq!(sampler.over_budget(0), None);
        assert!(sampler.state.lock().unwrap().checked_at.is_none());
    }
}
