//! Engine facade coverage: background-window stop conditions, power-state
//! mapping, and batch sequence parsing edges.
//!
//! Count contract: 10 independently named unit tests.

use super::*;

macro_rules! stop_case {
    ($name:ident, $advanced:expr, $steps:expr, $pending:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let tick = TickResult {
                instances_advanced: $advanced,
                steps_executed: $steps,
                has_pending_work: $pending,
            };
            assert_eq!(background_tick_should_stop(&tick), $expected);
        }
    };
}

stop_case!(
    coverage_engine_001_background_batch_stops_when_idle,
    0,
    0,
    false,
    true
);
stop_case!(
    coverage_engine_002_background_batch_stops_without_progress,
    0,
    0,
    true,
    true
);
stop_case!(
    coverage_engine_003_background_batch_continues_on_progress,
    1,
    2,
    true,
    false
);

macro_rules! multiplier_case {
    ($name:ident, $state:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!($state.tick_multiplier(), $expected);
        }
    };
}

multiplier_case!(
    coverage_engine_004_power_multiplier_matches_battery_doc,
    PowerState::CriticalBattery,
    4
);

macro_rules! atomic_case {
    ($name:ident, $value:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(PowerState::from_atomic($value), $expected);
        }
    };
}

atomic_case!(
    coverage_engine_005_power_state_from_atomic_covers_all_levels,
    2,
    PowerState::LowBattery
);
atomic_case!(
    coverage_engine_006_power_state_from_atomic_defaults_unknown_to_unplugged,
    255,
    PowerState::Unplugged
);
atomic_case!(
    coverage_engine_009_power_state_from_atomic_maps_zero_to_charging,
    0,
    PowerState::Charging
);
atomic_case!(
    coverage_engine_010_power_state_from_atomic_maps_three_to_critical,
    3,
    PowerState::CriticalBattery
);

#[test]
fn coverage_engine_007_sequence_batch_accepts_empty_array() {
    let parsed = parse_sequence_batch(b"[]").unwrap();
    assert!(parsed.is_empty());
}

#[test]
fn coverage_engine_008_sequence_batch_rejects_non_array_document() {
    let result = parse_sequence_batch(br#"{"name":"not-an-array"}"#);
    assert!(matches!(result, Err(MobileError::InvalidInput { .. })));
}
