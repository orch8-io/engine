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
    /// probing the kernel on every tick.
    pub(crate) fn over_budget(&self, budget_bytes: u64) -> Option<u64> {
        self.over_budget_with(budget_bytes, current_rss_bytes)
    }

    /// [`Self::over_budget`] with an injectable measurement, so the budget
    /// decision is testable without a real (platform-specific) probe.
    pub(crate) fn over_budget_with(
        &self,
        budget_bytes: u64,
        measure: impl FnOnce() -> Option<u64>,
    ) -> Option<u64> {
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
            state.rss = measure();
            state.checked_at = Some(now);
        }

        // Cache the RSS sample, not the verdict: a caller may supply a
        // different budget before the next probe is due.
        state.exceeded = state.rss.is_some_and(|rss| rss > budget_bytes);
        state.exceeded.then_some(state.rss).flatten()
    }
}

/// Returns the current process memory footprint in bytes, or `None` if
/// unavailable. On Apple platforms this is `phys_footprint` (the figure jetsam
/// enforces), read via `task_info` with no subprocess.
pub fn current_rss_bytes() -> Option<u64> {
    #[cfg(target_vendor = "apple")]
    {
        darwin_footprint()
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        linux_rss()
    }
    #[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
    {
        None
    }
}

/// Word (u64) index of `phys_footprint` in `task_vm_info` (REV1+): 16 bytes of
/// `virtual_size/region_count/page_size`, then 16 `mach_vm_size_t` fields.
#[cfg(any(target_vendor = "apple", test))]
const TASK_VM_INFO_PHYS_FOOTPRINT_WORD: usize = 18;
/// `TASK_VM_INFO_REV1_COUNT` in `natural_t` units: the shortest reply that
/// still contains `phys_footprint`.
#[cfg(any(target_vendor = "apple", test))]
const TASK_VM_INFO_REV1_COUNT: u32 = 38;

/// Extract `phys_footprint` from a `task_info(TASK_VM_INFO)` reply of
/// `returned_count` natural_t words. `None` when the kernel's reply is too
/// short to contain it or reports zero.
#[cfg(any(target_vendor = "apple", test))]
fn phys_footprint_from_task_vm_info(buf: &[u64], returned_count: u32) -> Option<u64> {
    if returned_count < TASK_VM_INFO_REV1_COUNT {
        return None;
    }
    buf.get(TASK_VM_INFO_PHYS_FOOTPRINT_WORD)
        .copied()
        .filter(|bytes| *bytes > 0)
}

/// Process footprint via `task_info(mach_task_self(), TASK_VM_INFO)`.
/// `ps` is unavailable inside the iOS sandbox, which previously made the
/// memory budget a silent no-op there.
#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
fn darwin_footprint() -> Option<u64> {
    const TASK_VM_INFO: u32 = 22;
    const KERN_SUCCESS: i32 = 0;
    unsafe extern "C" {
        static mach_task_self_: u32;
        fn task_info(task: u32, flavor: u32, info_out: *mut u64, count: *mut u32) -> i32;
    }
    // Generously sized (kernel writes only what it supports and reports the
    // length back in `count`).
    let mut buf = [0u64; 64];
    let mut count = u32::try_from(buf.len() * 2).ok()?;
    // SAFETY: `buf` is valid for `count` natural_t words, `count` is a valid
    // in/out pointer, and `mach_task_self_` is initialized by libSystem.
    let status = unsafe { task_info(mach_task_self_, TASK_VM_INFO, buf.as_mut_ptr(), &raw mut count) };
    if status != KERN_SUCCESS {
        return None;
    }
    phys_footprint_from_task_vm_info(&buf, count)
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
    fn budget_decision_is_independent_of_the_probe() {
        let over = MemoryBudgetSampler::default();
        assert_eq!(over.over_budget_with(100, || Some(150)), Some(150));
        let under = MemoryBudgetSampler::default();
        assert_eq!(under.over_budget_with(100, || Some(99)), None);
        let unavailable = MemoryBudgetSampler::default();
        assert_eq!(
            unavailable.over_budget_with(100, || None),
            None,
            "no measurement means no enforcement, not a false positive"
        );
        let disabled = MemoryBudgetSampler::default();
        assert_eq!(
            disabled.over_budget_with(0, || panic!("must not probe when disabled")),
            None
        );
    }

    #[test]
    fn task_vm_info_reply_is_parsed_defensively() {
        let mut words = [0u64; 64];
        words[TASK_VM_INFO_PHYS_FOOTPRINT_WORD] = 123_456;
        assert_eq!(
            phys_footprint_from_task_vm_info(&words, TASK_VM_INFO_REV1_COUNT),
            Some(123_456)
        );
        // Kernel replied with a pre-REV1 struct: the field is not valid.
        assert_eq!(
            phys_footprint_from_task_vm_info(&words, TASK_VM_INFO_REV1_COUNT - 1),
            None
        );
        // Zero is "not reported", not "0 bytes used".
        words[TASK_VM_INFO_PHYS_FOOTPRINT_WORD] = 0;
        assert_eq!(
            phys_footprint_from_task_vm_info(&words, TASK_VM_INFO_REV1_COUNT),
            None
        );
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn apple_footprint_probe_reports_this_process_without_a_subprocess() {
        let bytes = current_rss_bytes().expect("task_info(TASK_VM_INFO) should succeed");
        assert!(bytes > 1024 * 1024, "a test process uses more than 1 MiB");
    }

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

    #[test]
    fn tiny_budget_reports_the_sampled_rss_as_over_budget() {
        if current_rss_bytes().is_some() {
            let sampler = MemoryBudgetSampler::default();

            let reported = sampler.over_budget(1);

            assert!(
                reported.is_some_and(|rss| rss > 1),
                "a 1-byte budget is always exceeded"
            );
            assert!(rss_over_budget(1).is_some());
        }
    }

    #[test]
    fn sampler_serves_a_cached_over_budget_sample_without_reprobing() {
        if current_rss_bytes().is_some() {
            let sampler = MemoryBudgetSampler::default();
            let first = sampler.over_budget(1);
            let first_check = sampler.state.lock().unwrap().checked_at;

            let second = sampler.over_budget(1);

            assert_eq!(first, second);
            assert_eq!(sampler.state.lock().unwrap().checked_at, first_check);
        }
    }

    #[test]
    fn cached_rss_is_reevaluated_against_a_changed_budget_without_reprobing() {
        let sampler = MemoryBudgetSampler::default();
        let checked_at = Instant::now();
        *sampler.state.lock().unwrap() = MemorySampleState {
            checked_at: Some(checked_at),
            rss: Some(1_024),
            exceeded: true,
        };

        assert_eq!(sampler.over_budget(u64::MAX), None);
        assert_eq!(sampler.state.lock().unwrap().checked_at, Some(checked_at));
        assert_eq!(sampler.over_budget(1), Some(1_024));
        assert_eq!(sampler.state.lock().unwrap().checked_at, Some(checked_at));
    }

    fn sampler_checked(ago: Duration, exceeded: bool) -> (MemoryBudgetSampler, Instant) {
        let checked_at = Instant::now()
            .checked_sub(ago)
            .expect("monotonic clock is past the test offset");
        let sampler = MemoryBudgetSampler::default();
        *sampler.state.lock().unwrap() = MemorySampleState {
            checked_at: Some(checked_at),
            rss: Some(1_024),
            exceeded,
        };
        (sampler, checked_at)
    }

    #[test]
    fn exceeded_sampler_reprobes_after_the_short_interval() {
        let (sampler, checked_at) = sampler_checked(Duration::from_secs(6), true);

        sampler.over_budget(1);

        assert_ne!(sampler.state.lock().unwrap().checked_at, Some(checked_at));
    }

    #[test]
    fn healthy_sampler_keeps_its_sample_until_the_long_interval() {
        let (sampler, checked_at) = sampler_checked(Duration::from_secs(6), false);

        sampler.over_budget(u64::MAX);

        assert_eq!(sampler.state.lock().unwrap().checked_at, Some(checked_at));
    }
}
