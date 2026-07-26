//! Coverage tests for the SQLite push wake outbox: outcome mapping, enqueue
//! dedupe, lease fencing, terminal CAS, quarantine, and collapsible wakes.
//!
//! Count contract: 48 independently named unit tests.

use super::*;
use crate::{MobileCommand, MobileDevice, MobileSyncStore};
use chrono::Duration;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn device(
    device_id: &str,
    tenant_id: &str,
    push_token: Option<&str>,
    active: bool,
) -> MobileDevice {
    MobileDevice {
        device_id: device_id.into(),
        tenant_id: tenant_id.into(),
        push_token: push_token.map(str::to_string),
        platform: "ios".into(),
        app_version: None,
        active,
        last_sync_at: None,
        registered_at: String::new(),
    }
}

async fn storage_with(registrations: &[MobileDevice]) -> SqliteStorage {
    let storage = SqliteStorage::in_memory().await.unwrap();
    for registration in registrations {
        storage.register_mobile_device(registration).await.unwrap();
    }
    storage
}

async fn standard_storage() -> SqliteStorage {
    storage_with(&[device("device-a", "tenant-a", Some("token-a"), true)]).await
}

async fn outbox_row(
    storage: &SqliteStorage,
    command_id: &str,
) -> (
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    sqlx::query_as(
        "SELECT status,attempts,next_attempt_at,last_error,terminal_reason,delivered_at FROM push_wake_outbox WHERE command_id=?",
    )
    .bind(command_id)
    .fetch_one(&storage.pool)
    .await
    .unwrap()
}

async fn row_count(storage: &SqliteStorage) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM push_wake_outbox")
        .fetch_one(&storage.pool)
        .await
        .unwrap()
}

fn collapsible(execution: &str, command_id: &str, offset: i64) -> CollapsibleWake {
    CollapsibleWake {
        tenant_id: "tenant-a".into(),
        device_id: "device-a".into(),
        execution_id: execution.into(),
        topic: "resume".into(),
        command_id: command_id.into(),
        created_at: Utc::now() + Duration::seconds(offset),
    }
}

// ---------------------------------------------------------------------------
// Pure outcome mapping (drives the UPDATE column bindings)
// ---------------------------------------------------------------------------

#[test]
fn coverage_push_001_delivered_maps_status_and_timestamp() {
    let now = Utc::now();
    let (status, next, error, reason, delivered) =
        outcome_fields(&WakeAttemptOutcome::Delivered, now);
    assert_eq!(status, "delivered");
    assert_eq!(delivered, Some(now));
    assert!(next.is_none() && error.is_none() && reason.is_none());
}

#[test]
fn coverage_push_002_retry_maps_back_to_pending_with_backoff() {
    let now = Utc::now();
    let retry_at = now + Duration::minutes(5);
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
fn coverage_push_003_terminal_invalid_token_maps_reason() {
    let outcome = WakeAttemptOutcome::Terminal {
        reason: PushTerminalReason::InvalidToken,
        error: "gone".into(),
    };
    let (_, _, error, reason, delivered) = outcome_fields(&outcome, Utc::now());
    assert_eq!(reason, Some("invalid_token"));
    assert_eq!(error, Some("gone"));
    assert!(delivered.is_none());
}

#[test]
fn coverage_push_004_terminal_permanent_failure_maps_reason() {
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
fn coverage_push_005_terminal_misconfigured_maps_reason() {
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
fn coverage_push_006_terminal_retry_limit_maps_reason() {
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
fn coverage_push_007_reason_names_are_distinct() {
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
fn coverage_push_008_terminal_never_sets_next_attempt() {
    let (_, next, _, _, _) = outcome_fields(
        &WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::RetryLimit,
            error: "exhausted".into(),
        },
        Utc::now(),
    );
    assert!(next.is_none(), "a terminal wake must never be rescheduled");
}

// ---------------------------------------------------------------------------
// Enqueue dedupe on (tenant_id, device_id, command_id)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_push_009_enqueue_returns_stored_row_id() {
    let storage = standard_storage().await;
    let id = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", Utc::now())
        .await
        .unwrap();
    let stored: String =
        sqlx::query_scalar("SELECT id FROM push_wake_outbox WHERE command_id='cmd-1'")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
    assert_eq!(id.to_string(), stored);
}

#[tokio::test]
async fn coverage_push_010_duplicate_enqueue_is_idempotent() {
    let storage = standard_storage().await;
    let now = Utc::now();
    let first = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let second = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now + Duration::seconds(5))
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(row_count(&storage).await, 1);
}

#[tokio::test]
async fn coverage_push_011_same_command_other_tenant_enqueues_separately() {
    let storage = standard_storage().await;
    let a = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", Utc::now())
        .await
        .unwrap();
    let b = storage
        .enqueue_wake("tenant-b", "device-a", "cmd-1", Utc::now())
        .await
        .unwrap();
    assert_ne!(a, b);
    assert_eq!(row_count(&storage).await, 2);
}

#[tokio::test]
async fn coverage_push_012_same_command_other_device_enqueues_separately() {
    let storage = standard_storage().await;
    let a = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", Utc::now())
        .await
        .unwrap();
    let b = storage
        .enqueue_wake("tenant-a", "device-b", "cmd-1", Utc::now())
        .await
        .unwrap();
    assert_ne!(a, b);
    assert_eq!(row_count(&storage).await, 2);
}

#[tokio::test]
async fn coverage_push_013_distinct_commands_get_distinct_ids() {
    let storage = standard_storage().await;
    let a = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", Utc::now())
        .await
        .unwrap();
    let b = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-2", Utc::now())
        .await
        .unwrap();
    assert_ne!(a, b);
    assert_eq!(row_count(&storage).await, 2);
}

// ---------------------------------------------------------------------------
// Claim eligibility: device targeting and tenant isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_push_014_device_without_push_token_not_claimed() {
    let storage = storage_with(&[device("device-a", "tenant-a", None, true)]).await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now, now + Duration::seconds(30), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn coverage_push_015_inactive_device_not_claimed() {
    // Registration always (re-)activates, so deactivate directly — this is
    // the state an invalid-token quarantine or stale sweep leaves behind.
    let storage = storage_with(&[device("device-a", "tenant-a", Some("token-a"), true)]).await;
    sqlx::query("UPDATE mobile_devices SET active=0 WHERE device_id='device-a'")
        .execute(&storage.pool)
        .await
        .unwrap();
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now, now + Duration::seconds(30), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn coverage_push_016_unregistered_device_not_claimed() {
    let storage = storage_with(&[]).await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now, now + Duration::seconds(30), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn coverage_push_017_registration_under_other_tenant_not_claimed() {
    // The device exists, but its registration belongs to tenant-b; a wake
    // addressed to (tenant-a, device-a) must not leak tenant-b's token.
    let storage = storage_with(&[device("device-a", "tenant-b", Some("token-b"), true)]).await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now, now + Duration::seconds(30), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn coverage_push_018_claim_returns_registered_token_and_platform() {
    let storage = storage_with(&[MobileDevice {
        platform: "android".into(),
        ..device("device-a", "tenant-a", Some("token-xyz"), true)
    }])
    .await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wakes = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(wakes.len(), 1);
    assert_eq!(wakes[0].push_token, "token-xyz");
    assert_eq!(wakes[0].platform, "android");
    assert_eq!(wakes[0].attempts, 0);
}

// ---------------------------------------------------------------------------
// Claim leasing: order, limit, visibility windows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_push_019_claim_respects_limit() {
    let storage = standard_storage().await;
    let now = Utc::now();
    for index in 0..5 {
        storage
            .enqueue_wake("tenant-a", "device-a", &format!("cmd-{index}"), now)
            .await
            .unwrap();
    }
    let wakes = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 2)
        .await
        .unwrap();
    assert_eq!(wakes.len(), 2);
}

#[tokio::test]
async fn coverage_push_020_claim_orders_oldest_first() {
    let storage = standard_storage().await;
    let now = Utc::now();
    // Insert newest-first; the claim must still drain oldest-first.
    for (command, age_secs) in [("cmd-new", 10_i64), ("cmd-mid", 20), ("cmd-old", 30)] {
        storage
            .enqueue_wake(
                "tenant-a",
                "device-a",
                command,
                now - Duration::seconds(age_secs),
            )
            .await
            .unwrap();
    }
    let wakes = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 3)
        .await
        .unwrap();
    let order: Vec<&str> = wakes.iter().map(|wake| wake.command_id.as_str()).collect();
    assert_eq!(order, vec!["cmd-old", "cmd-mid", "cmd-new"]);
}

#[tokio::test]
async fn coverage_push_021_claimed_row_leased_in_flight() {
    let storage = standard_storage().await;
    let now = Utc::now();
    let lease = now + Duration::seconds(30);
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wakes = storage.claim_due_wakes(now, lease, 10).await.unwrap();
    assert_eq!(wakes[0].lease_until, lease);
    let (status, _, _, _, _, _) = outbox_row(&storage, "cmd-1").await;
    assert_eq!(status, "in_flight");
}

#[tokio::test]
async fn coverage_push_022_active_lease_blocks_second_claim() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    storage
        .claim_due_wakes(now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now + Duration::seconds(10), now + Duration::seconds(40), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn coverage_push_023_expired_lease_is_reclaimed() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    storage
        .claim_due_wakes(now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    let reclaimed_at = now + Duration::seconds(31);
    let wakes = storage
        .claim_due_wakes(reclaimed_at, reclaimed_at + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(wakes.len(), 1);
    assert_eq!(wakes[0].command_id, "cmd-1");
}

#[tokio::test]
async fn coverage_push_024_future_next_attempt_not_claimed() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    let retry_at = now + Duration::hours(1);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Retry {
                next_attempt_at: retry_at,
                error: "busy".into(),
            },
            now,
        )
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now + Duration::minutes(30), now + Duration::hours(2), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn coverage_push_025_due_retry_is_claimed_again() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    let retry_at = now + Duration::minutes(1);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Retry {
                next_attempt_at: retry_at,
                error: "busy".into(),
            },
            now,
        )
        .await
        .unwrap();
    let wakes = storage
        .claim_due_wakes(retry_at, retry_at + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(wakes.len(), 1);
    assert_eq!(wakes[0].attempts, 1);
}

#[tokio::test]
async fn coverage_push_026_zero_limit_claims_nothing() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now, now + Duration::seconds(30), 0)
            .await
            .unwrap()
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// Outcome persistence and the terminal lease CAS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_push_027_delivered_increments_attempts_and_stamps_delivery() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(&wake, &WakeAttemptOutcome::Delivered, now)
        .await
        .unwrap();
    let (status, attempts, _, _, _, delivered) = outbox_row(&storage, "cmd-1").await;
    assert_eq!(status, "delivered");
    assert_eq!(attempts, 1);
    assert!(delivered.is_some());
}

#[tokio::test]
async fn coverage_push_028_retry_restores_pending_with_error() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Retry {
                next_attempt_at: now + Duration::minutes(5),
                error: "HTTP 503".into(),
            },
            now,
        )
        .await
        .unwrap();
    let (status, attempts, next, error, _, _) = outbox_row(&storage, "cmd-1").await;
    assert_eq!(status, "pending");
    assert_eq!(attempts, 1);
    assert!(next.is_some());
    assert_eq!(error.as_deref(), Some("HTTP 503"));
}

#[tokio::test]
async fn coverage_push_029_terminal_records_reason_and_error() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::RetryLimit,
                error: "exhausted".into(),
            },
            now,
        )
        .await
        .unwrap();
    let (status, _, _, error, reason, _) = outbox_row(&storage, "cmd-1").await;
    assert_eq!(status, "terminal");
    assert_eq!(reason.as_deref(), Some("retry_limit"));
    assert_eq!(error.as_deref(), Some("exhausted"));
}

#[tokio::test]
async fn coverage_push_030_stale_lease_outcome_is_rejected_and_row_untouched() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let first = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    let reclaimed_at = now + Duration::seconds(31);
    let second = storage
        .claim_due_wakes(reclaimed_at, reclaimed_at + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    // The fenced-out claimant loses the CAS...
    assert!(
        storage
            .record_wake_outcome(&first, &WakeAttemptOutcome::Delivered, reclaimed_at)
            .await
            .is_err()
    );
    // ...and the row is untouched until the live claimant writes.
    let (status, attempts, _, _, _, _) = outbox_row(&storage, "cmd-1").await;
    assert_eq!(status, "in_flight");
    assert_eq!(attempts, 0);
    storage
        .record_wake_outcome(&second, &WakeAttemptOutcome::Delivered, reclaimed_at)
        .await
        .unwrap();
}

#[tokio::test]
async fn coverage_push_031_outcome_for_unclaimed_wake_is_rejected() {
    let storage = standard_storage().await;
    let now = Utc::now();
    let id = storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let fabricated = ClaimedWake {
        id,
        tenant_id: "tenant-a".into(),
        device_id: "device-a".into(),
        command_id: "cmd-1".into(),
        push_token: "token-a".into(),
        platform: "ios".into(),
        attempts: 0,
        lease_until: now + Duration::seconds(30),
    };
    assert!(
        storage
            .record_wake_outcome(&fabricated, &WakeAttemptOutcome::Delivered, now)
            .await
            .is_err(),
        "a wake that was never leased must fail the CAS"
    );
}

#[tokio::test]
async fn coverage_push_032_double_outcome_write_is_rejected() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(&wake, &WakeAttemptOutcome::Delivered, now)
        .await
        .unwrap();
    assert!(
        storage
            .record_wake_outcome(&wake, &WakeAttemptOutcome::Delivered, now)
            .await
            .is_err(),
        "replaying the same claim must not double-count attempts"
    );
    let (_, attempts, _, _, _, _) = outbox_row(&storage, "cmd-1").await;
    assert_eq!(attempts, 1);
}

#[tokio::test]
async fn coverage_push_033_invalid_token_quarantines_device() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::InvalidToken,
                error: "unregistered".into(),
            },
            now,
        )
        .await
        .unwrap();
    let device = storage
        .get_mobile_device("device-a")
        .await
        .unwrap()
        .unwrap();
    assert!(!device.active);
    assert!(device.push_token.is_none());
}

#[tokio::test]
async fn coverage_push_034_non_token_terminal_keeps_device_registered() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::PermanentFailure,
                error: "boom".into(),
            },
            now,
        )
        .await
        .unwrap();
    let device = storage
        .get_mobile_device("device-a")
        .await
        .unwrap()
        .unwrap();
    assert!(device.active);
    assert_eq!(device.push_token.as_deref(), Some("token-a"));
}

#[tokio::test]
async fn coverage_push_035_quarantined_device_no_longer_claimable() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::InvalidToken,
                error: "gone".into(),
            },
            now,
        )
        .await
        .unwrap();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-2", now)
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now, now + Duration::seconds(30), 10)
            .await
            .unwrap()
            .is_empty(),
        "quarantine must take the device out of the drain loop"
    );
}

// ---------------------------------------------------------------------------
// Command acknowledgements
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_push_036_empty_ack_list_is_noop() {
    let storage = standard_storage().await;
    assert_eq!(
        storage
            .record_command_acks("device-a", &[], Utc::now())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn coverage_push_037_ack_marks_only_listed_commands() {
    let storage = standard_storage().await;
    let now = Utc::now();
    for command in ["cmd-1", "cmd-2", "cmd-3"] {
        storage
            .enqueue_wake("tenant-a", "device-a", command, now)
            .await
            .unwrap();
    }
    let acked = storage
        .record_command_acks("device-a", &["cmd-1".into(), "cmd-3".into()], now)
        .await
        .unwrap();
    assert_eq!(acked, 2);
    let marked: Vec<String> = sqlx::query_scalar(
        "SELECT command_id FROM push_wake_outbox WHERE command_acked_at IS NOT NULL ORDER BY command_id",
    )
    .fetch_all(&storage.pool)
    .await
    .unwrap();
    assert_eq!(marked, vec!["cmd-1", "cmd-3"]);
}

#[tokio::test]
async fn coverage_push_038_ack_scoped_to_device() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    storage
        .enqueue_wake("tenant-a", "device-b", "cmd-1", now)
        .await
        .unwrap();
    let acked = storage
        .record_command_acks("device-a", &["cmd-1".into()], now)
        .await
        .unwrap();
    assert_eq!(acked, 1);
    let marked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM push_wake_outbox WHERE device_id='device-b' AND command_acked_at IS NOT NULL",
    )
    .fetch_one(&storage.pool)
    .await
    .unwrap();
    assert_eq!(marked, 0);
}

#[tokio::test]
async fn coverage_push_039_ack_unknown_command_matches_nothing() {
    let storage = standard_storage().await;
    let acked = storage
        .record_command_acks("device-a", &["nope".into()], Utc::now())
        .await
        .unwrap();
    assert_eq!(acked, 0);
}

// ---------------------------------------------------------------------------
// Collapsible wakes and command+wake atomicity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_push_040_collapse_key_is_deterministic_sha256_hex() {
    let wake = collapsible("exec-a", "cmd-1", 0);
    let key = wake.collapse_key();
    assert_eq!(key.len(), 64);
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(key, wake.collapse_key());
}

#[tokio::test]
async fn coverage_push_041_collapse_key_varies_with_execution() {
    let a = collapsible("exec-a", "cmd-1", 0).collapse_key();
    let b = collapsible("exec-b", "cmd-1", 0).collapse_key();
    assert_ne!(a, b);
}

#[tokio::test]
async fn coverage_push_042_collapsible_enqueue_stores_governance_columns() {
    let storage = standard_storage().await;
    storage
        .enqueue_collapsible_wake(&collapsible("exec-a", "cmd-1", 0))
        .await
        .unwrap();
    let (execution, topic, key): (String, String, String) = sqlx::query_as(
        "SELECT execution_id,topic,collapse_key FROM push_wake_outbox WHERE command_id='cmd-1'",
    )
    .fetch_one(&storage.pool)
    .await
    .unwrap();
    assert_eq!(execution, "exec-a");
    assert_eq!(topic, "resume");
    assert_eq!(key, collapsible("exec-a", "cmd-1", 0).collapse_key());
}

#[tokio::test]
async fn coverage_push_043_collapsible_supersede_marks_old_row_terminal() {
    let storage = standard_storage().await;
    storage
        .enqueue_collapsible_wake(&collapsible("exec-a", "old", 0))
        .await
        .unwrap();
    storage
        .enqueue_collapsible_wake(&collapsible("exec-a", "new", 1))
        .await
        .unwrap();
    let (status, reason, superseded_by): (String, String, String) = sqlx::query_as(
        "SELECT status,terminal_reason,superseded_by FROM push_wake_outbox WHERE command_id='old'",
    )
    .fetch_one(&storage.pool)
    .await
    .unwrap();
    assert_eq!(status, "terminal");
    assert_eq!(reason, "superseded");
    assert_eq!(superseded_by, "new");
}

#[tokio::test]
async fn coverage_push_044_collapsible_supersede_ignores_other_tenants() {
    let storage = standard_storage().await;
    storage
        .enqueue_collapsible_wake(&CollapsibleWake {
            tenant_id: "tenant-b".into(),
            ..collapsible("exec-a", "other-tenant", 0)
        })
        .await
        .unwrap();
    storage
        .enqueue_collapsible_wake(&collapsible("exec-a", "mine", 1))
        .await
        .unwrap();
    let status: String =
        sqlx::query_scalar("SELECT status FROM push_wake_outbox WHERE command_id='other-tenant'")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
    assert_eq!(status, "pending", "supersede must stay inside the tenant");
}

#[tokio::test]
async fn coverage_push_045_collapsible_duplicate_command_keeps_first_row() {
    let storage = standard_storage().await;
    let first = storage
        .enqueue_collapsible_wake(&collapsible("exec-a", "cmd-1", 0))
        .await
        .unwrap();
    let second = storage
        .enqueue_collapsible_wake(&collapsible("exec-b", "cmd-1", 1))
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(row_count(&storage).await, 1);
}

#[tokio::test]
async fn coverage_push_046_command_with_wake_commits_both_rows() {
    let storage = standard_storage().await;
    let now = Utc::now();
    let command = MobileCommand {
        id: "cmd-1".into(),
        device_id: "device-a".into(),
        command_type: "resume".into(),
        payload: "{}".into(),
        created_at: String::new(),
        acked_at: None,
    };
    let wake_id = storage
        .create_mobile_command_with_wake(&command, "tenant-a", now)
        .await
        .unwrap();
    let stored_wake: String =
        sqlx::query_scalar("SELECT id FROM push_wake_outbox WHERE command_id='cmd-1'")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
    assert_eq!(wake_id.to_string(), stored_wake);
    let pending = storage
        .fetch_pending_commands("device-a", 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, "cmd-1");
}

#[tokio::test]
async fn coverage_push_047_delivered_row_is_never_reclaimed() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(&wake, &WakeAttemptOutcome::Delivered, now)
        .await
        .unwrap();
    // Only in_flight rows with an expired lease are reclaimable; a delivered
    // row must stay out of the drain loop forever.
    assert!(
        storage
            .claim_due_wakes(now + Duration::hours(1), now + Duration::hours(2), 10)
            .await
            .unwrap()
            .is_empty(),
        "a delivered wake must never be reclaimed"
    );
}

#[tokio::test]
async fn coverage_push_048_terminal_row_is_never_reclaimed() {
    let storage = standard_storage().await;
    let now = Utc::now();
    storage
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let wake = storage
        .claim_due_wakes(now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    storage
        .record_wake_outcome(
            &wake,
            &WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::RetryLimit,
                error: "exhausted".into(),
            },
            now,
        )
        .await
        .unwrap();
    assert!(
        storage
            .claim_due_wakes(now + Duration::hours(1), now + Duration::hours(2), 10)
            .await
            .unwrap()
            .is_empty(),
        "a terminal wake must never be reclaimed"
    );
}
