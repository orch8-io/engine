//! Coverage tests for the Postgres push wake outbox's pure outcome mapping.
//!
//! These pin the same status/reason vocabulary as the `SQLite` backend so the
//! two drain loops stay interchangeable. The SQL paths themselves need a live
//! Postgres and are covered by `tests/postgres_integration.rs`.
//!
//! Count contract: 12 independently named unit tests.

use super::*;

#[test]
fn coverage_pg_push_001_delivered_maps_status_and_timestamp() {
    let now = Utc::now();
    let (status, next, error, reason, delivered) =
        outcome_fields(&WakeAttemptOutcome::Delivered, now);
    assert_eq!(status, "delivered");
    assert_eq!(delivered, Some(now));
    assert!(next.is_none() && error.is_none() && reason.is_none());
}

#[test]
fn coverage_pg_push_002_retry_maps_back_to_pending() {
    let now = Utc::now();
    let retry_at = now + chrono::Duration::minutes(5);
    let outcome = WakeAttemptOutcome::Retry {
        next_attempt_at: retry_at,
        error: "provider busy".into(),
    };
    let (status, next, error, reason, delivered) = outcome_fields(&outcome, now);
    assert_eq!(status, "pending");
    assert_eq!(next, Some(retry_at));
    assert_eq!(error, Some("provider busy"));
    assert!(reason.is_none() && delivered.is_none());
}

#[test]
fn coverage_pg_push_003_terminal_invalid_token_maps_reason() {
    let outcome = WakeAttemptOutcome::Terminal {
        reason: PushTerminalReason::InvalidToken,
        error: "gone".into(),
    };
    let (status, _, error, reason, _) = outcome_fields(&outcome, Utc::now());
    assert_eq!(status, "terminal");
    assert_eq!(reason, Some("invalid_token"));
    assert_eq!(error, Some("gone"));
}

#[test]
fn coverage_pg_push_004_terminal_permanent_failure_maps_reason() {
    let (_, _, _, reason, _) = outcome_fields(
        &WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::PermanentFailure,
            error: "boom".into(),
        },
        Utc::now(),
    );
    assert_eq!(reason, Some("permanent_failure"));
}

#[test]
fn coverage_pg_push_005_terminal_misconfigured_maps_reason() {
    let (_, _, _, reason, _) = outcome_fields(
        &WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::Misconfigured,
            error: "no cert".into(),
        },
        Utc::now(),
    );
    assert_eq!(reason, Some("misconfigured"));
}

#[test]
fn coverage_pg_push_006_terminal_retry_limit_maps_reason() {
    let (_, _, _, reason, _) = outcome_fields(
        &WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::RetryLimit,
            error: "exhausted".into(),
        },
        Utc::now(),
    );
    assert_eq!(reason, Some("retry_limit"));
}

#[test]
fn coverage_pg_push_007_terminal_never_reschedules() {
    let (_, next, _, _, delivered) = outcome_fields(
        &WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::PermanentFailure,
            error: "boom".into(),
        },
        Utc::now(),
    );
    assert!(
        next.is_none(),
        "terminal wakes must not carry next_attempt_at"
    );
    assert!(delivered.is_none(), "terminal is not a delivery");
}

#[test]
fn coverage_pg_push_008_reason_names_are_distinct() {
    let names = [
        reason_name(PushTerminalReason::InvalidToken),
        reason_name(PushTerminalReason::PermanentFailure),
        reason_name(PushTerminalReason::Misconfigured),
        reason_name(PushTerminalReason::RetryLimit),
    ];
    let unique: std::collections::HashSet<_> = names.iter().collect();
    assert_eq!(unique.len(), 4);
}

#[test]
fn coverage_pg_push_009_reason_names_are_lowercase_snake_case() {
    for reason in [
        PushTerminalReason::InvalidToken,
        PushTerminalReason::PermanentFailure,
        PushTerminalReason::Misconfigured,
        PushTerminalReason::RetryLimit,
    ] {
        let name = reason_name(reason);
        assert!(
            name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "reason name '{name}' must match the SQLite/Postgres shared vocabulary"
        );
    }
}

#[test]
fn coverage_pg_push_010_delivered_carries_no_error() {
    let (_, _, error, reason, _) = outcome_fields(&WakeAttemptOutcome::Delivered, Utc::now());
    assert!(error.is_none());
    assert!(reason.is_none());
}

#[test]
fn coverage_pg_push_011_retry_error_text_passes_through_verbatim() {
    let long_error = "x".repeat(500);
    let outcome = WakeAttemptOutcome::Retry {
        next_attempt_at: Utc::now(),
        error: long_error.clone(),
    };
    let (_, _, error, _, _) = outcome_fields(&outcome, Utc::now());
    assert_eq!(error, Some(long_error.as_str()));
}

#[test]
fn coverage_pg_push_012_terminal_error_text_passes_through_verbatim() {
    let unicode_error = "デバイストークンが無効です 🚫".to_string();
    let outcome = WakeAttemptOutcome::Terminal {
        reason: PushTerminalReason::InvalidToken,
        error: unicode_error.clone(),
    };
    let (_, _, error, _, _) = outcome_fields(&outcome, Utc::now());
    assert_eq!(error, Some(unicode_error.as_str()));
}
