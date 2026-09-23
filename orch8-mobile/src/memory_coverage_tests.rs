//! Memory budget coverage: sampling cadence and the cached-verdict contract
//! that keeps RSS probes off the per-tick path.
//!
//! Count contract: 5 independently named unit tests.

use super::*;

#[test]
fn coverage_memory_001_sample_intervals_bound_probe_cost() {
    assert_eq!(HEALTHY_SAMPLE_INTERVAL, Duration::from_secs(30));
    assert_eq!(EXCEEDED_SAMPLE_INTERVAL, Duration::from_secs(5));
}

#[test]
fn coverage_memory_002_tiny_budget_reports_over_budget_when_rss_available() {
    if current_rss_bytes().is_some() {
        let sampler = MemoryBudgetSampler::default();
        let reported = sampler.over_budget(1);
        assert!(reported.is_some(), "a 1-byte budget is always exceeded");
        assert!(reported.unwrap() > 1);
        assert!(rss_over_budget(1).is_some());
    }
}

#[test]
fn coverage_memory_003_sampler_caches_the_over_budget_sample() {
    if current_rss_bytes().is_some() {
        let sampler = MemoryBudgetSampler::default();
        let first = sampler.over_budget(1);
        let first_check = sampler.state.lock().unwrap().checked_at;

        let second = sampler.over_budget(1);

        assert_eq!(
            first, second,
            "the cached RSS is served within the interval"
        );
        assert_eq!(
            sampler.state.lock().unwrap().checked_at,
            first_check,
            "a cached verdict must not re-probe the process"
        );
    }
}

#[test]
fn coverage_memory_004_cached_rss_respects_budget_change_within_interval() {
    let sampler = MemoryBudgetSampler::default();
    let checked_at = Instant::now();
    *sampler.state.lock().unwrap() = MemorySampleState {
        checked_at: Some(checked_at),
        rss: Some(1_024),
        exceeded: true,
    };

    // Changing the budget must re-evaluate the cached RSS, without
    // launching another process/procfs probe within the sample interval.
    assert_eq!(sampler.over_budget(u64::MAX), None);
    assert_eq!(sampler.state.lock().unwrap().checked_at, Some(checked_at));
    assert_eq!(sampler.over_budget(1), Some(1_024));
    assert_eq!(sampler.state.lock().unwrap().checked_at, Some(checked_at));
}

#[test]
fn coverage_memory_005_huge_budget_never_reports_over_budget() {
    let sampler = MemoryBudgetSampler::default();
    assert_eq!(sampler.over_budget(u64::MAX), None);
    assert_eq!(rss_over_budget(u64::MAX), None);
}
