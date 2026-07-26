//! Runtime coverage: the blocking-thread pool ceiling that bounds mobile
//! memory when host callbacks stall.
//!
//! Count contract: 4 independently named unit tests.

use super::*;

macro_rules! limit_case {
    ($name:ident, $steps:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(blocking_thread_limit($steps), $expected);
        }
    };
}

limit_case!(
    coverage_runtime_001_blocking_limit_scales_with_step_concurrency,
    1,
    5
);
limit_case!(
    coverage_runtime_002_blocking_limit_caps_at_thirty_two,
    28,
    32
);
limit_case!(
    coverage_runtime_003_blocking_limit_saturates_on_huge_step_count,
    u32::MAX,
    32
);

#[test]
fn coverage_runtime_004_runtime_with_max_config_executes_async_work() {
    let rt = MobileRuntime::new(u32::MAX).unwrap();
    let result = rt.block_on(async { 7 * 6 });
    assert_eq!(result, 42);
}
