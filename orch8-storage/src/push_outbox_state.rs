//! Persisted column values for a push wake outcome, shared by the `SQLite`
//! and Postgres outboxes so both backends write one status/reason vocabulary.

use chrono::{DateTime, Utc};
use orch8_push::{PushTerminalReason, WakeAttemptOutcome};

/// `(status, next_attempt_at, last_error, terminal_reason, delivered_at)`.
pub(crate) type OutcomeFields<'a> = (
    &'static str,
    Option<DateTime<Utc>>,
    Option<&'a str>,
    Option<&'static str>,
    Option<DateTime<Utc>>,
);

pub(crate) fn outcome_fields(
    outcome: &WakeAttemptOutcome,
    now: DateTime<Utc>,
) -> OutcomeFields<'_> {
    match outcome {
        WakeAttemptOutcome::Delivered => ("delivered", None, None, None, Some(now)),
        WakeAttemptOutcome::Retry {
            next_attempt_at,
            error,
        } => ("pending", Some(*next_attempt_at), Some(error), None, None),
        WakeAttemptOutcome::Terminal { reason, error } => (
            "terminal",
            None,
            Some(error),
            Some(reason_name(*reason)),
            None,
        ),
    }
}

fn reason_name(reason: PushTerminalReason) -> &'static str {
    match reason {
        PushTerminalReason::InvalidToken => "invalid_token",
        PushTerminalReason::PermanentFailure => "permanent_failure",
        PushTerminalReason::Misconfigured => "misconfigured",
        PushTerminalReason::RetryLimit => "retry_limit",
    }
}
