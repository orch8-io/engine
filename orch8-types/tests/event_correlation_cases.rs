//! Extensive unit tests for `orch8_types::event_correlation`.
//!
//! Covers join-mode matrices (`Any` / `All` / `Count`), `listens_for`
//! matching, `record_match` return-value semantics, matched-set dedup
//! behavior, serde shapes for every type, and `FromStr`/`as_str`/serde
//! parity for the status enums.

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use orch8_types::event_correlation::{
    EventEnvelope, EventStatus, EventWait, IngestOutcome, JoinMode, WaitStatus,
};

// ============================================================================
// Fixtures
// ============================================================================

fn event(name: &str) -> EventEnvelope {
    EventEnvelope {
        id: Uuid::now_v7(),
        tenant_id: "t1".into(),
        event_name: name.into(),
        producer_event_id: Uuid::now_v7().to_string(),
        correlation_key: "order-1".into(),
        payload: json!({}),
        status: EventStatus::Pending,
        consumed_by: None,
        received_at: Utc::now(),
    }
}

fn event_with_id(name: &str, id: Uuid) -> EventEnvelope {
    let mut e = event(name);
    e.id = id;
    e
}

fn wait(names: &[&str], mode: JoinMode) -> EventWait {
    EventWait {
        id: Uuid::now_v7(),
        tenant_id: "t1".into(),
        instance_id: Uuid::now_v7(),
        block_id: "wait".into(),
        event_names: names.iter().map(ToString::to_string).collect(),
        correlation_key: "order-1".into(),
        join_mode: mode,
        status: WaitStatus::Waiting,
        matched_names: vec![],
        matched_event_ids: vec![],
        created_at: Utc::now(),
    }
}

fn fixed_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 15, 10, 30, 0).unwrap()
}

// ============================================================================
// listens_for
// ============================================================================

#[test]
fn listens_for_exact_single_name() {
    let w = wait(&["paid"], JoinMode::Any);
    assert!(w.listens_for("paid"));
}

#[test]
fn listens_for_rejects_other_name() {
    let w = wait(&["paid"], JoinMode::Any);
    assert!(!w.listens_for("shipped"));
}

#[test]
fn listens_for_is_case_sensitive_query_upper() {
    let w = wait(&["paid"], JoinMode::Any);
    assert!(!w.listens_for("Paid"));
    assert!(!w.listens_for("PAID"));
}

#[test]
fn listens_for_is_case_sensitive_name_upper() {
    let w = wait(&["Paid"], JoinMode::Any);
    assert!(!w.listens_for("paid"));
    assert!(w.listens_for("Paid"));
}

#[test]
fn listens_for_no_prefix_substring_match() {
    let w = wait(&["paid"], JoinMode::Any);
    assert!(!w.listens_for("paid_full"));
    assert!(!w.listens_for("pai"));
}

#[test]
fn listens_for_no_suffix_substring_match() {
    let w = wait(&["payment_received"], JoinMode::Any);
    assert!(!w.listens_for("received"));
    assert!(!w.listens_for("payment"));
}

#[test]
fn listens_for_empty_names_list_matches_nothing() {
    let w = wait(&[], JoinMode::Any);
    assert!(!w.listens_for("paid"));
    assert!(!w.listens_for(""));
}

#[test]
fn listens_for_multi_names_all_hit() {
    let w = wait(&["a", "b", "c"], JoinMode::All);
    assert!(w.listens_for("a"));
    assert!(w.listens_for("b"));
    assert!(w.listens_for("c"));
    assert!(!w.listens_for("d"));
}

#[test]
fn listens_for_empty_string_name_is_exact() {
    let w = wait(&[""], JoinMode::Any);
    assert!(w.listens_for(""));
    assert!(!w.listens_for("x"));
}

#[test]
fn listens_for_does_not_trim_whitespace() {
    let w = wait(&[" paid"], JoinMode::Any);
    assert!(!w.listens_for("paid"));
    assert!(w.listens_for(" paid"));
}

#[test]
fn listens_for_unicode_names() {
    let w = wait(&["платіж"], JoinMode::Any);
    assert!(w.listens_for("платіж"));
    assert!(!w.listens_for("платi ж"));
}

// ============================================================================
// Any join
// ============================================================================

#[test]
fn any_join_unsatisfied_when_empty() {
    let w = wait(&["a", "b"], JoinMode::Any);
    assert!(!w.is_satisfied());
}

#[test]
fn any_join_first_match_satisfies() {
    let mut w = wait(&["a"], JoinMode::Any);
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
}

#[test]
fn any_join_second_listed_name_satisfies() {
    let mut w = wait(&["a", "b"], JoinMode::Any);
    assert!(w.record_match(&event("b")));
    assert!(w.is_satisfied());
}

#[test]
fn any_join_first_listed_name_satisfies() {
    let mut w = wait(&["a", "b"], JoinMode::Any);
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
}

#[test]
fn any_join_repeat_name_returns_true_and_accumulates_ids() {
    let mut w = wait(&["a"], JoinMode::Any);
    assert!(w.record_match(&event("a")));
    // Any joins do NOT gate on already-matched names: a fresh event with
    // the same name is a genuine new match.
    assert!(w.record_match(&event("a")));
    assert_eq!(w.matched_event_ids.len(), 2);
}

#[test]
fn any_join_matched_names_dedup_on_repeat() {
    let mut w = wait(&["a"], JoinMode::Any);
    w.record_match(&event("a"));
    w.record_match(&event("a"));
    w.record_match(&event("a"));
    assert_eq!(w.matched_names, vec!["a".to_string()]);
    assert_eq!(w.matched_event_ids.len(), 3);
}

#[test]
fn any_join_same_event_id_replay_returns_false() {
    let mut w = wait(&["a"], JoinMode::Any);
    let e = event("a");
    assert!(w.record_match(&e));
    assert!(!w.record_match(&e));
}

#[test]
fn any_join_same_event_id_replay_leaves_state_unchanged() {
    let mut w = wait(&["a"], JoinMode::Any);
    let e = event("a");
    w.record_match(&e);
    let ids_before = w.matched_event_ids.clone();
    let names_before = w.matched_names.clone();
    w.record_match(&e);
    assert_eq!(w.matched_event_ids, ids_before);
    assert_eq!(w.matched_names, names_before);
}

#[test]
fn any_join_unlistened_event_returns_false() {
    let mut w = wait(&["a"], JoinMode::Any);
    assert!(!w.record_match(&event("other")));
}

#[test]
fn any_join_unlistened_event_leaves_state_unchanged() {
    let mut w = wait(&["a"], JoinMode::Any);
    w.record_match(&event("other"));
    assert!(w.matched_event_ids.is_empty());
    assert!(w.matched_names.is_empty());
    assert!(!w.is_satisfied());
}

#[test]
fn any_join_stays_satisfied_after_more_matches() {
    let mut w = wait(&["a", "b"], JoinMode::Any);
    w.record_match(&event("a"));
    assert!(w.is_satisfied());
    w.record_match(&event("b"));
    assert!(w.is_satisfied());
}

#[test]
fn any_join_empty_names_list_never_satisfied() {
    let mut w = wait(&[], JoinMode::Any);
    assert!(!w.is_satisfied());
    assert!(!w.record_match(&event("anything")));
    assert!(!w.is_satisfied());
}

#[test]
fn any_join_multi_names_ids_accumulate_names_dedup() {
    let mut w = wait(&["a", "b"], JoinMode::Any);
    assert!(w.record_match(&event("a")));
    assert!(w.record_match(&event("b")));
    assert!(w.record_match(&event("a")));
    assert_eq!(w.matched_event_ids.len(), 3);
    assert_eq!(w.matched_names, vec!["a".to_string(), "b".to_string()]);
}

// ============================================================================
// All join
// ============================================================================

#[test]
fn all_join_empty_names_list_is_vacuously_satisfied() {
    // `.all()` over an empty iterator is true: an empty all-join is
    // satisfied with zero matches. Assert the actual (surprising) shape.
    let w = wait(&[], JoinMode::All);
    assert!(w.is_satisfied());
    assert!(w.matched_event_ids.is_empty());
}

#[test]
fn all_join_single_name_satisfied_by_one_event() {
    let mut w = wait(&["a"], JoinMode::All);
    assert!(!w.is_satisfied());
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
}

#[test]
fn all_join_two_names_needs_both() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    assert!(w.record_match(&event("a")));
    assert!(!w.is_satisfied());
    assert!(w.record_match(&event("b")));
    assert!(w.is_satisfied());
}

#[test]
fn all_join_order_independent_forward() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.record_match(&event("a"));
    w.record_match(&event("b"));
    assert!(w.is_satisfied());
}

#[test]
fn all_join_order_independent_reverse() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.record_match(&event("b"));
    w.record_match(&event("a"));
    assert!(w.is_satisfied());
}

#[test]
fn all_join_three_names_every_permutation_order_independent() {
    let orders: [[&str; 3]; 6] = [
        ["a", "b", "c"],
        ["a", "c", "b"],
        ["b", "a", "c"],
        ["b", "c", "a"],
        ["c", "a", "b"],
        ["c", "b", "a"],
    ];
    for order in orders {
        let mut w = wait(&["a", "b", "c"], JoinMode::All);
        for (i, name) in order.iter().enumerate() {
            assert!(!w.is_satisfied(), "satisfied too early at {i} in {order:?}");
            assert!(w.record_match(&event(name)));
        }
        assert!(w.is_satisfied(), "unsatisfied after full order {order:?}");
    }
}

#[test]
fn all_join_repeat_name_returns_false() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    assert!(w.record_match(&event("a")));
    assert!(!w.record_match(&event("a")));
}

#[test]
fn all_join_repeat_name_does_not_consume_second_event() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.record_match(&event("a"));
    w.record_match(&event("a"));
    assert_eq!(w.matched_event_ids.len(), 1);
    assert_eq!(w.matched_names, vec!["a".to_string()]);
}

#[test]
fn all_join_repeat_name_leaves_join_unsatisfied() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.record_match(&event("a"));
    w.record_match(&event("a"));
    assert!(!w.is_satisfied());
}

#[test]
fn all_join_duplicate_names_in_list_satisfied_by_single_event() {
    // event_names may contain duplicates; the all-check is per name
    // occurrence but a single matched name covers every duplicate.
    let mut w = wait(&["a", "a"], JoinMode::All);
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
    assert_eq!(w.matched_event_ids.len(), 1);
}

#[test]
fn all_join_duplicate_names_in_list_second_event_rejected() {
    let mut w = wait(&["a", "a", "b"], JoinMode::All);
    assert!(w.record_match(&event("a")));
    // Second "a" is rejected even though "a" appears twice in the list.
    assert!(!w.record_match(&event("a")));
    assert!(!w.is_satisfied());
    assert!(w.record_match(&event("b")));
    assert!(w.is_satisfied());
}

#[test]
fn all_join_partial_three_of_two_not_satisfied() {
    let mut w = wait(&["a", "b", "c"], JoinMode::All);
    w.record_match(&event("a"));
    w.record_match(&event("b"));
    assert!(!w.is_satisfied());
}

#[test]
fn all_join_three_names_complete() {
    let mut w = wait(&["a", "b", "c"], JoinMode::All);
    w.record_match(&event("c"));
    w.record_match(&event("a"));
    w.record_match(&event("b"));
    assert!(w.is_satisfied());
    assert_eq!(w.matched_event_ids.len(), 3);
}

#[test]
fn all_join_same_event_id_replay_returns_false() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    let e = event("a");
    assert!(w.record_match(&e));
    assert!(!w.record_match(&e));
    assert_eq!(w.matched_event_ids.len(), 1);
}

#[test]
fn all_join_unlistened_event_returns_false() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    assert!(!w.record_match(&event("z")));
    assert!(w.matched_event_ids.is_empty());
}

#[test]
fn all_join_matched_names_reflect_arrival_order() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.record_match(&event("b"));
    w.record_match(&event("a"));
    assert_eq!(w.matched_names, vec!["b".to_string(), "a".to_string()]);
}

#[test]
fn all_join_matched_ids_are_the_event_ids() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    let ea = event("a");
    let eb = event("b");
    w.record_match(&ea);
    w.record_match(&eb);
    assert_eq!(w.matched_event_ids, vec![ea.id, eb.id]);
}

#[test]
fn all_join_matched_name_outside_list_does_not_satisfy() {
    // Manually constructed state: matched name that isn't in the list
    // contributes nothing to the all-check.
    let mut w = wait(&["a"], JoinMode::All);
    w.matched_names = vec!["x".into()];
    w.matched_event_ids = vec![Uuid::now_v7()];
    assert!(!w.is_satisfied());
}

#[test]
fn all_join_superset_matched_names_satisfies() {
    let mut w = wait(&["a"], JoinMode::All);
    w.matched_names = vec!["x".into(), "a".into()];
    w.matched_event_ids = vec![Uuid::now_v7(), Uuid::now_v7()];
    assert!(w.is_satisfied());
}

// ============================================================================
// Count join
// ============================================================================

#[test]
fn count_zero_is_immediately_satisfied() {
    let w = wait(&["a"], JoinMode::Count { count: 0 });
    assert!(w.is_satisfied());
}

#[test]
fn count_one_unsatisfied_before_any_match() {
    let w = wait(&["a"], JoinMode::Count { count: 1 });
    assert!(!w.is_satisfied());
}

#[test]
fn count_one_boundary_satisfied_by_single_event() {
    let mut w = wait(&["a"], JoinMode::Count { count: 1 });
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
}

#[test]
fn count_two_boundary() {
    let mut w = wait(&["a"], JoinMode::Count { count: 2 });
    assert!(w.record_match(&event("a")));
    assert!(!w.is_satisfied());
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
}

#[test]
fn count_ten_boundary() {
    let mut w = wait(&["reading"], JoinMode::Count { count: 10 });
    for i in 0..9 {
        assert!(w.record_match(&event("reading")));
        assert!(!w.is_satisfied(), "satisfied too early after {}", i + 1);
    }
    assert!(w.record_match(&event("reading")));
    assert!(w.is_satisfied());
}

#[test]
fn count_repeated_single_name_allowed() {
    let mut w = wait(&["reading"], JoinMode::Count { count: 3 });
    for _ in 0..3 {
        assert!(w.record_match(&event("reading")));
    }
    assert!(w.is_satisfied());
    assert_eq!(w.matched_event_ids.len(), 3);
}

#[test]
fn count_larger_than_distinct_names_with_repeats() {
    // Two distinct names, count 4: repeats of both names count.
    let mut w = wait(&["a", "b"], JoinMode::Count { count: 4 });
    assert!(w.record_match(&event("a")));
    assert!(w.record_match(&event("a")));
    assert!(w.record_match(&event("b")));
    assert!(!w.is_satisfied());
    assert!(w.record_match(&event("b")));
    assert!(w.is_satisfied());
}

#[test]
fn count_matched_names_dedup_while_ids_accumulate() {
    let mut w = wait(&["a", "b"], JoinMode::Count { count: 4 });
    w.record_match(&event("a"));
    w.record_match(&event("a"));
    w.record_match(&event("b"));
    w.record_match(&event("b"));
    assert_eq!(w.matched_names, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(w.matched_event_ids.len(), 4);
}

#[test]
fn count_same_event_id_never_double_counts() {
    let mut w = wait(&["a"], JoinMode::Count { count: 2 });
    let e = event("a");
    assert!(w.record_match(&e));
    assert!(!w.record_match(&e));
    assert!(!w.is_satisfied());
    assert_eq!(w.matched_event_ids.len(), 1);
}

#[test]
fn count_unlistened_event_returns_false() {
    let mut w = wait(&["a"], JoinMode::Count { count: 1 });
    assert!(!w.record_match(&event("z")));
    assert!(!w.is_satisfied());
}

#[test]
fn count_mixed_names_both_count_toward_total() {
    let mut w = wait(&["a", "b"], JoinMode::Count { count: 2 });
    assert!(w.record_match(&event("a")));
    assert!(!w.is_satisfied());
    assert!(w.record_match(&event("b")));
    assert!(w.is_satisfied());
}

#[test]
fn count_record_match_keeps_returning_true_past_satisfaction() {
    let mut w = wait(&["a"], JoinMode::Count { count: 1 });
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
    // record_match itself does not gate on satisfaction.
    assert!(w.record_match(&event("a")));
    assert_eq!(w.matched_event_ids.len(), 2);
    assert!(w.is_satisfied());
}

#[test]
fn count_is_satisfied_when_ids_exceed_count() {
    let mut w = wait(&["a"], JoinMode::Count { count: 2 });
    for _ in 0..3 {
        w.record_match(&event("a"));
    }
    assert!(w.is_satisfied());
}

#[test]
fn count_empty_names_list_never_progresses() {
    let mut w = wait(&[], JoinMode::Count { count: 1 });
    assert!(!w.record_match(&event("a")));
    assert!(!w.is_satisfied());
}

// ============================================================================
// record_match return-value semantics per branch
// ============================================================================

#[test]
fn record_match_unlistened_branch_wins_over_duplicate_id() {
    // An event whose id was already recorded but whose name is not
    // listened for is rejected by the listens_for branch first.
    let mut w = wait(&["a"], JoinMode::Any);
    let e = event("a");
    w.record_match(&e);
    let mut renamed = e.clone();
    renamed.event_name = "z".into();
    assert!(!w.record_match(&renamed));
    assert_eq!(w.matched_event_ids.len(), 1);
}

#[test]
fn record_match_duplicate_id_branch_applies_across_join_modes() {
    for mode in [JoinMode::Any, JoinMode::All, JoinMode::Count { count: 5 }] {
        let mut w = wait(&["a"], mode);
        let e = event("a");
        assert!(w.record_match(&e), "first match must succeed for {mode:?}");
        assert!(!w.record_match(&e), "id replay must fail for {mode:?}");
        assert_eq!(w.matched_event_ids.len(), 1);
    }
}

#[test]
fn record_match_all_name_gate_rejects_before_pushing_id() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    let first = event("a");
    let second = event("a");
    assert!(w.record_match(&first));
    assert!(!w.record_match(&second));
    // The second event's id was never recorded.
    assert!(!w.matched_event_ids.contains(&second.id));
    assert!(w.matched_event_ids.contains(&first.id));
}

#[test]
fn record_match_true_pushes_id_and_name() {
    let mut w = wait(&["a"], JoinMode::Any);
    let e = event("a");
    assert!(w.record_match(&e));
    assert_eq!(w.matched_event_ids, vec![e.id]);
    assert_eq!(w.matched_names, vec!["a".to_string()]);
}

#[test]
fn record_match_true_second_time_same_name_does_not_duplicate_name() {
    let mut w = wait(&["a"], JoinMode::Count { count: 3 });
    assert!(w.record_match(&event("a")));
    assert!(w.record_match(&event("a")));
    assert_eq!(w.matched_names.len(), 1);
    assert_eq!(w.matched_event_ids.len(), 2);
}

#[test]
fn record_match_does_not_touch_status() {
    let mut w = wait(&["a"], JoinMode::Any);
    w.record_match(&event("a"));
    // record_match reports; it never flips the wait status itself.
    assert_eq!(w.status, WaitStatus::Waiting);
}

#[test]
fn record_match_ignores_event_tenant_and_key() {
    // Scoping by tenant/correlation key is the storage layer's job:
    // record_match only checks name + id.
    let mut w = wait(&["a"], JoinMode::Any);
    let mut e = event("a");
    e.tenant_id = "OTHER".into();
    e.correlation_key = "OTHER-KEY".into();
    assert!(w.record_match(&e));
    assert!(w.is_satisfied());
}

#[test]
fn record_match_ignores_event_status() {
    let mut w = wait(&["a"], JoinMode::Any);
    let mut e = event("a");
    e.status = EventStatus::Consumed;
    assert!(w.record_match(&e));
}

// ============================================================================
// JoinMode serde + Default
// ============================================================================

#[test]
fn join_mode_default_is_all() {
    assert_eq!(JoinMode::default(), JoinMode::All);
}

#[test]
fn join_mode_any_serializes_with_mode_tag() {
    assert_eq!(
        serde_json::to_string(&JoinMode::Any).unwrap(),
        r#"{"mode":"any"}"#
    );
}

#[test]
fn join_mode_all_serializes_with_mode_tag() {
    assert_eq!(
        serde_json::to_string(&JoinMode::All).unwrap(),
        r#"{"mode":"all"}"#
    );
}

#[test]
fn join_mode_count_serializes_with_count_field() {
    assert_eq!(
        serde_json::to_string(&JoinMode::Count { count: 7 }).unwrap(),
        r#"{"mode":"count","count":7}"#
    );
}

#[test]
fn join_mode_any_deserializes() {
    let m: JoinMode = serde_json::from_str(r#"{"mode":"any"}"#).unwrap();
    assert_eq!(m, JoinMode::Any);
}

#[test]
fn join_mode_all_deserializes() {
    let m: JoinMode = serde_json::from_str(r#"{"mode":"all"}"#).unwrap();
    assert_eq!(m, JoinMode::All);
}

#[test]
fn join_mode_count_deserializes() {
    let m: JoinMode = serde_json::from_str(r#"{"mode":"count","count":3}"#).unwrap();
    assert_eq!(m, JoinMode::Count { count: 3 });
}

#[test]
fn join_mode_count_zero_deserializes_at_type_level() {
    // The type itself allows 0; validation lives in parse_wait_params.
    let m: JoinMode = serde_json::from_str(r#"{"mode":"count","count":0}"#).unwrap();
    assert_eq!(m, JoinMode::Count { count: 0 });
}

#[test]
fn join_mode_count_missing_count_fails() {
    assert!(serde_json::from_str::<JoinMode>(r#"{"mode":"count"}"#).is_err());
}

#[test]
fn join_mode_unknown_mode_fails() {
    assert!(serde_json::from_str::<JoinMode>(r#"{"mode":"quorum"}"#).is_err());
}

#[test]
fn join_mode_missing_tag_fails() {
    assert!(serde_json::from_str::<JoinMode>("{}").is_err());
}

#[test]
fn join_mode_case_sensitive_tag() {
    assert!(serde_json::from_str::<JoinMode>(r#"{"mode":"Any"}"#).is_err());
}

#[test]
fn join_mode_round_trips_all_variants() {
    for mode in [
        JoinMode::Any,
        JoinMode::All,
        JoinMode::Count { count: 1 },
        JoinMode::Count { count: u32::MAX },
    ] {
        let back: JoinMode = serde_json::from_str(&serde_json::to_string(&mode).unwrap()).unwrap();
        assert_eq!(back, mode);
    }
}

#[test]
fn join_mode_default_applies_via_wrapper_struct() {
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default)]
        join: JoinMode,
    }
    let w: Wrapper = serde_json::from_str("{}").unwrap();
    assert_eq!(w.join, JoinMode::All);
    let w: Wrapper = serde_json::from_str(r#"{"join":{"mode":"any"}}"#).unwrap();
    assert_eq!(w.join, JoinMode::Any);
}

// ============================================================================
// EventStatus: FromStr / as_str / serde parity
// ============================================================================

#[test]
fn event_status_as_str_values() {
    assert_eq!(EventStatus::Pending.as_str(), "pending");
    assert_eq!(EventStatus::Consumed.as_str(), "consumed");
    assert_eq!(EventStatus::Expired.as_str(), "expired");
}

#[test]
fn event_status_from_str_pending() {
    assert_eq!(
        "pending".parse::<EventStatus>().unwrap(),
        EventStatus::Pending
    );
}

#[test]
fn event_status_from_str_consumed() {
    assert_eq!(
        "consumed".parse::<EventStatus>().unwrap(),
        EventStatus::Consumed
    );
}

#[test]
fn event_status_from_str_expired() {
    assert_eq!(
        "expired".parse::<EventStatus>().unwrap(),
        EventStatus::Expired
    );
}

#[test]
fn event_status_from_str_unknown_error_message() {
    let err = "bogus".parse::<EventStatus>().unwrap_err();
    assert_eq!(err, "unknown event status: bogus");
}

#[test]
fn event_status_from_str_empty_errors() {
    let err = "".parse::<EventStatus>().unwrap_err();
    assert_eq!(err, "unknown event status: ");
}

#[test]
fn event_status_from_str_is_case_sensitive() {
    assert!("Pending".parse::<EventStatus>().is_err());
    assert!("PENDING".parse::<EventStatus>().is_err());
}

#[test]
fn event_status_from_str_rejects_whitespace() {
    assert!(" pending".parse::<EventStatus>().is_err());
    assert!("pending ".parse::<EventStatus>().is_err());
}

#[test]
fn event_status_serde_serializes_snake_case() {
    assert_eq!(
        serde_json::to_string(&EventStatus::Pending).unwrap(),
        "\"pending\""
    );
    assert_eq!(
        serde_json::to_string(&EventStatus::Consumed).unwrap(),
        "\"consumed\""
    );
    assert_eq!(
        serde_json::to_string(&EventStatus::Expired).unwrap(),
        "\"expired\""
    );
}

#[test]
fn event_status_serde_deserializes_snake_case() {
    assert_eq!(
        serde_json::from_str::<EventStatus>("\"pending\"").unwrap(),
        EventStatus::Pending
    );
    assert_eq!(
        serde_json::from_str::<EventStatus>("\"expired\"").unwrap(),
        EventStatus::Expired
    );
}

#[test]
fn event_status_serde_rejects_unknown() {
    assert!(serde_json::from_str::<EventStatus>("\"gone\"").is_err());
}

#[test]
fn event_status_serde_and_fromstr_agree() {
    for status in [
        EventStatus::Pending,
        EventStatus::Consumed,
        EventStatus::Expired,
    ] {
        let via_serde = serde_json::to_string(&status).unwrap();
        // Strip the JSON quotes: the inner token must round-trip FromStr.
        let token = via_serde.trim_matches('"');
        assert_eq!(token, status.as_str());
        assert_eq!(token.parse::<EventStatus>().unwrap(), status);
        let back: EventStatus = serde_json::from_str(&format!("\"{}\"", status.as_str())).unwrap();
        assert_eq!(back, status);
    }
}

// ============================================================================
// WaitStatus: FromStr / as_str / serde parity
// ============================================================================

#[test]
fn wait_status_as_str_values() {
    assert_eq!(WaitStatus::Waiting.as_str(), "waiting");
    assert_eq!(WaitStatus::Satisfied.as_str(), "satisfied");
    assert_eq!(WaitStatus::Cancelled.as_str(), "cancelled");
}

#[test]
fn wait_status_from_str_waiting() {
    assert_eq!(
        "waiting".parse::<WaitStatus>().unwrap(),
        WaitStatus::Waiting
    );
}

#[test]
fn wait_status_from_str_satisfied() {
    assert_eq!(
        "satisfied".parse::<WaitStatus>().unwrap(),
        WaitStatus::Satisfied
    );
}

#[test]
fn wait_status_from_str_cancelled() {
    assert_eq!(
        "cancelled".parse::<WaitStatus>().unwrap(),
        WaitStatus::Cancelled
    );
}

#[test]
fn wait_status_from_str_unknown_error_message() {
    let err = "paused".parse::<WaitStatus>().unwrap_err();
    assert_eq!(err, "unknown wait status: paused");
}

#[test]
fn wait_status_from_str_is_case_sensitive() {
    assert!("Waiting".parse::<WaitStatus>().is_err());
    assert!("SATISFIED".parse::<WaitStatus>().is_err());
}

#[test]
fn wait_status_from_str_rejects_american_spelling() {
    // "canceled" (one l) is not the canonical token.
    assert!("canceled".parse::<WaitStatus>().is_err());
}

#[test]
fn wait_status_serde_serializes_snake_case() {
    assert_eq!(
        serde_json::to_string(&WaitStatus::Waiting).unwrap(),
        "\"waiting\""
    );
    assert_eq!(
        serde_json::to_string(&WaitStatus::Satisfied).unwrap(),
        "\"satisfied\""
    );
    assert_eq!(
        serde_json::to_string(&WaitStatus::Cancelled).unwrap(),
        "\"cancelled\""
    );
}

#[test]
fn wait_status_serde_deserializes_snake_case() {
    assert_eq!(
        serde_json::from_str::<WaitStatus>("\"waiting\"").unwrap(),
        WaitStatus::Waiting
    );
    assert_eq!(
        serde_json::from_str::<WaitStatus>("\"cancelled\"").unwrap(),
        WaitStatus::Cancelled
    );
}

#[test]
fn wait_status_serde_rejects_unknown() {
    assert!(serde_json::from_str::<WaitStatus>("\"done\"").is_err());
}

#[test]
fn wait_status_serde_and_fromstr_agree() {
    for status in [
        WaitStatus::Waiting,
        WaitStatus::Satisfied,
        WaitStatus::Cancelled,
    ] {
        let token = status.as_str();
        assert_eq!(token.parse::<WaitStatus>().unwrap(), status);
        assert_eq!(
            serde_json::to_string(&status).unwrap(),
            format!("\"{token}\"")
        );
        let back: WaitStatus = serde_json::from_str(&format!("\"{token}\"")).unwrap();
        assert_eq!(back, status);
    }
}

// ============================================================================
// EventEnvelope serde
// ============================================================================

fn full_envelope() -> EventEnvelope {
    EventEnvelope {
        id: Uuid::now_v7(),
        tenant_id: "acme".into(),
        event_name: "payment_received".into(),
        producer_event_id: "stripe-evt-42".into(),
        correlation_key: "order-77".into(),
        payload: json!({"amount": 100, "currency": "USD", "nested": {"a": [1, 2, 3]}}),
        status: EventStatus::Pending,
        consumed_by: None,
        received_at: fixed_time(),
    }
}

#[test]
fn envelope_round_trips_without_consumer() {
    let e = full_envelope();
    let back: EventEnvelope = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
    assert_eq!(back, e);
}

#[test]
fn envelope_round_trips_with_consumer() {
    let mut e = full_envelope();
    e.status = EventStatus::Consumed;
    e.consumed_by = Some(Uuid::now_v7());
    let back: EventEnvelope = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
    assert_eq!(back, e);
}

#[test]
fn envelope_consumed_by_none_is_omitted_from_json() {
    let e = full_envelope();
    let v: serde_json::Value = serde_json::to_value(&e).unwrap();
    assert!(
        v.get("consumed_by").is_none(),
        "consumed_by must be omitted when None"
    );
}

#[test]
fn envelope_consumed_by_some_is_present_in_json() {
    let mut e = full_envelope();
    let consumer = Uuid::now_v7();
    e.consumed_by = Some(consumer);
    let v: serde_json::Value = serde_json::to_value(&e).unwrap();
    assert_eq!(v["consumed_by"], json!(consumer.to_string()));
}

#[test]
fn envelope_status_serialized_as_snake_case_string() {
    let e = full_envelope();
    let v: serde_json::Value = serde_json::to_value(&e).unwrap();
    assert_eq!(v["status"], json!("pending"));
}

#[test]
fn envelope_payload_defaults_to_null_when_missing() {
    let id = Uuid::now_v7();
    let j = json!({
        "id": id.to_string(),
        "tenant_id": "t1",
        "event_name": "paid",
        "producer_event_id": "p-1",
        "correlation_key": "k",
        "status": "pending",
        "received_at": "2026-01-15T10:30:00Z"
    });
    let e: EventEnvelope = serde_json::from_value(j).unwrap();
    assert_eq!(e.payload, serde_json::Value::Null);
    assert_eq!(e.consumed_by, None);
}

#[test]
fn envelope_missing_event_name_fails() {
    let j = json!({
        "id": Uuid::now_v7().to_string(),
        "tenant_id": "t1",
        "producer_event_id": "p-1",
        "correlation_key": "k",
        "status": "pending",
        "received_at": "2026-01-15T10:30:00Z"
    });
    assert!(serde_json::from_value::<EventEnvelope>(j).is_err());
}

#[test]
fn envelope_missing_status_fails() {
    let j = json!({
        "id": Uuid::now_v7().to_string(),
        "tenant_id": "t1",
        "event_name": "paid",
        "producer_event_id": "p-1",
        "correlation_key": "k",
        "received_at": "2026-01-15T10:30:00Z"
    });
    assert!(serde_json::from_value::<EventEnvelope>(j).is_err());
}

#[test]
fn envelope_tolerates_unknown_fields() {
    let j = json!({
        "id": Uuid::now_v7().to_string(),
        "tenant_id": "t1",
        "event_name": "paid",
        "producer_event_id": "p-1",
        "correlation_key": "k",
        "payload": {},
        "status": "pending",
        "received_at": "2026-01-15T10:30:00Z",
        "totally_unknown": true
    });
    assert!(serde_json::from_value::<EventEnvelope>(j).is_ok());
}

#[test]
fn envelope_received_at_preserves_instant() {
    let e = full_envelope();
    let back: EventEnvelope = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
    assert_eq!(back.received_at, fixed_time());
}

#[test]
fn envelope_payload_complex_value_round_trips() {
    let mut e = full_envelope();
    e.payload = json!([{"k": null}, 3.5, "s", true]);
    let back: EventEnvelope = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
    assert_eq!(back.payload, e.payload);
}

#[test]
fn envelope_invalid_status_string_fails() {
    let j = json!({
        "id": Uuid::now_v7().to_string(),
        "tenant_id": "t1",
        "event_name": "paid",
        "producer_event_id": "p-1",
        "correlation_key": "k",
        "status": "sideways",
        "received_at": "2026-01-15T10:30:00Z"
    });
    assert!(serde_json::from_value::<EventEnvelope>(j).is_err());
}

// ============================================================================
// EventWait serde
// ============================================================================

fn full_wait() -> EventWait {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.created_at = fixed_time();
    w
}

#[test]
fn wait_round_trips_empty_matches() {
    let w = full_wait();
    let back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert_eq!(back, w);
}

#[test]
fn wait_round_trips_with_matches() {
    let mut w = full_wait();
    w.record_match(&event("a"));
    w.status = WaitStatus::Satisfied;
    let back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert_eq!(back, w);
}

#[test]
fn wait_matched_fields_default_to_empty_when_missing() {
    let j = json!({
        "id": Uuid::now_v7().to_string(),
        "tenant_id": "t1",
        "instance_id": Uuid::now_v7().to_string(),
        "block_id": "b",
        "event_names": ["a"],
        "correlation_key": "k",
        "join_mode": {"mode": "any"},
        "status": "waiting",
        "created_at": "2026-01-15T10:30:00Z"
    });
    let w: EventWait = serde_json::from_value(j).unwrap();
    assert!(w.matched_names.is_empty());
    assert!(w.matched_event_ids.is_empty());
}

#[test]
fn wait_missing_join_mode_fails() {
    // join_mode has no #[serde(default)]: it is required on the wire.
    let j = json!({
        "id": Uuid::now_v7().to_string(),
        "tenant_id": "t1",
        "instance_id": Uuid::now_v7().to_string(),
        "block_id": "b",
        "event_names": ["a"],
        "correlation_key": "k",
        "status": "waiting",
        "created_at": "2026-01-15T10:30:00Z"
    });
    assert!(serde_json::from_value::<EventWait>(j).is_err());
}

#[test]
fn wait_join_mode_serialized_as_tagged_object() {
    let w = full_wait();
    let v: serde_json::Value = serde_json::to_value(&w).unwrap();
    assert_eq!(v["join_mode"], json!({"mode": "all"}));
}

#[test]
fn wait_status_serialized_as_snake_case_string() {
    let mut w = full_wait();
    w.status = WaitStatus::Satisfied;
    let v: serde_json::Value = serde_json::to_value(&w).unwrap();
    assert_eq!(v["status"], json!("satisfied"));
}

#[test]
fn wait_empty_event_names_round_trips() {
    let mut w = full_wait();
    w.event_names = vec![];
    let back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert!(back.event_names.is_empty());
}

#[test]
fn wait_matched_event_ids_round_trip_as_uuids() {
    let mut w = full_wait();
    let e1 = event("a");
    let e2 = event("b");
    w.record_match(&e1);
    w.record_match(&e2);
    let back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert_eq!(back.matched_event_ids, vec![e1.id, e2.id]);
}

#[test]
fn wait_count_join_round_trips() {
    let mut w = full_wait();
    w.join_mode = JoinMode::Count { count: 9 };
    let back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert_eq!(back.join_mode, JoinMode::Count { count: 9 });
}

// ============================================================================
// IngestOutcome serde
// ============================================================================

#[test]
fn outcome_round_trips_full() {
    let o = IngestOutcome {
        event_id: Uuid::now_v7(),
        duplicate: false,
        matched_wait: Some(Uuid::now_v7()),
        satisfied: true,
    };
    let back: IngestOutcome = serde_json::from_str(&serde_json::to_string(&o).unwrap()).unwrap();
    assert_eq!(back, o);
}

#[test]
fn outcome_round_trips_minimal() {
    let o = IngestOutcome {
        event_id: Uuid::now_v7(),
        duplicate: true,
        matched_wait: None,
        satisfied: false,
    };
    let back: IngestOutcome = serde_json::from_str(&serde_json::to_string(&o).unwrap()).unwrap();
    assert_eq!(back, o);
}

#[test]
fn outcome_matched_wait_none_omitted_from_json() {
    let o = IngestOutcome {
        event_id: Uuid::now_v7(),
        duplicate: false,
        matched_wait: None,
        satisfied: false,
    };
    let v: serde_json::Value = serde_json::to_value(&o).unwrap();
    assert!(v.get("matched_wait").is_none());
}

#[test]
fn outcome_matched_wait_some_present_in_json() {
    let wait_id = Uuid::now_v7();
    let o = IngestOutcome {
        event_id: Uuid::now_v7(),
        duplicate: false,
        matched_wait: Some(wait_id),
        satisfied: true,
    };
    let v: serde_json::Value = serde_json::to_value(&o).unwrap();
    assert_eq!(v["matched_wait"], json!(wait_id.to_string()));
}

#[test]
fn outcome_satisfied_defaults_to_false_when_missing() {
    let j = json!({"event_id": Uuid::now_v7().to_string(), "duplicate": false});
    let o: IngestOutcome = serde_json::from_value(j).unwrap();
    assert!(!o.satisfied);
    assert_eq!(o.matched_wait, None);
}

#[test]
fn outcome_matched_wait_defaults_to_none_when_missing() {
    let j = json!({"event_id": Uuid::now_v7().to_string(), "duplicate": true, "satisfied": true});
    let o: IngestOutcome = serde_json::from_value(j).unwrap();
    assert_eq!(o.matched_wait, None);
    assert!(o.satisfied);
    assert!(o.duplicate);
}

#[test]
fn outcome_missing_duplicate_fails() {
    let j = json!({"event_id": Uuid::now_v7().to_string()});
    assert!(serde_json::from_value::<IngestOutcome>(j).is_err());
}

#[test]
fn outcome_missing_event_id_fails() {
    let j = json!({"duplicate": false});
    assert!(serde_json::from_value::<IngestOutcome>(j).is_err());
}

// ============================================================================
// Cross-cutting scenarios on the pure types
// ============================================================================

#[test]
fn satisfied_wait_state_survives_serde_round_trip() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.record_match(&event("a"));
    w.record_match(&event("b"));
    assert!(w.is_satisfied());
    let back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    // Behavior (not just data) must survive the round trip.
    assert!(back.is_satisfied());
}

#[test]
fn partially_matched_wait_resumes_after_round_trip() {
    let mut w = wait(&["a", "b"], JoinMode::All);
    w.record_match(&event("a"));
    let mut back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert!(!back.is_satisfied());
    assert!(back.record_match(&event("b")));
    assert!(back.is_satisfied());
}

#[test]
fn count_progress_survives_round_trip() {
    let mut w = wait(&["r"], JoinMode::Count { count: 3 });
    w.record_match(&event("r"));
    w.record_match(&event("r"));
    let mut back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert!(!back.is_satisfied());
    assert!(back.record_match(&event("r")));
    assert!(back.is_satisfied());
}

#[test]
fn duplicate_id_still_rejected_after_round_trip() {
    let mut w = wait(&["r"], JoinMode::Count { count: 5 });
    let e = event("r");
    w.record_match(&e);
    let mut back: EventWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
    assert!(!back.record_match(&e));
}

#[test]
fn event_with_same_name_different_id_is_distinct() {
    let shared = Uuid::now_v7();
    let mut w = wait(&["a"], JoinMode::Count { count: 2 });
    assert!(w.record_match(&event_with_id("a", shared)));
    assert!(!w.record_match(&event_with_id("a", shared)));
    assert!(w.record_match(&event("a")));
    assert!(w.is_satisfied());
}

#[test]
fn wait_equality_reflects_matched_state() {
    let w1 = wait(&["a"], JoinMode::Any);
    let mut w2 = w1.clone();
    assert_eq!(w1, w2);
    w2.record_match(&event("a"));
    assert_ne!(w1, w2);
}

#[test]
fn envelope_equality_reflects_status() {
    let e1 = full_envelope();
    let mut e2 = e1.clone();
    assert_eq!(e1, e2);
    e2.status = EventStatus::Consumed;
    assert_ne!(e1, e2);
}
