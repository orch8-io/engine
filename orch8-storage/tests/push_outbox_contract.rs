//! Push wake outbox contract, identical for `SQLite` and Postgres.
//!
//! A wake is delivered to exactly the device registration it was addressed
//! to (same tenant, active, with a token), leased to one claimant at a time,
//! and finished exactly once: an outcome from a claimant that no longer holds
//! the lease is rejected, delivered/terminal wakes never come back, and an
//! invalid-token verdict quarantines only the token the provider rejected.
//! Everything is observed through `PushOutboxStore` / `MobileSyncStore`.
//!
//! ```text
//! DATABASE_URL=postgres://… cargo test -p orch8-storage --test push_outbox_contract
//! ```

#[macro_use]
mod support;

use chrono::{DateTime, Duration, Utc};
use orch8_push::{
    ClaimedWake, CollapsibleWake, PushOutboxStore, PushTerminalReason, WakeAttemptOutcome,
};
use orch8_storage::{MobileCommand, MobileDevice, MobileSyncStore};
use orch8_types::error::StorageError;

trait Store: PushOutboxStore + MobileSyncStore {}
impl<T: PushOutboxStore + MobileSyncStore> Store for T {}

fn device(device_id: &str, tenant_id: &str, push_token: Option<&str>) -> MobileDevice {
    MobileDevice {
        device_id: device_id.into(),
        tenant_id: tenant_id.into(),
        push_token: push_token.map(str::to_string),
        platform: "ios".into(),
        app_version: None,
        active: true,
        last_sync_at: None,
        registered_at: String::new(),
    }
}

async fn register(s: &impl Store, registration: MobileDevice) {
    s.register_mobile_device(&registration).await.unwrap();
}

/// Registers `device-a` for `tenant-a` with `token-a`.
async fn standard(s: &impl Store) {
    register(s, device("device-a", "tenant-a", Some("token-a"))).await;
}

async fn enqueue(s: &impl Store, command_id: &str, at: DateTime<Utc>) -> uuid::Uuid {
    s.enqueue_wake("tenant-a", "device-a", command_id, at)
        .await
        .unwrap()
}

async fn claim(s: &impl Store, now: DateTime<Utc>, limit: u32) -> Vec<ClaimedWake> {
    s.claim_due_wakes(now, now + Duration::seconds(30), limit)
        .await
        .unwrap()
}

async fn claim_one(s: &impl Store, now: DateTime<Utc>) -> ClaimedWake {
    let mut wakes = claim(s, now, 1).await;
    assert_eq!(wakes.len(), 1, "expected exactly one due wake");
    wakes.remove(0)
}

fn retry_at(at: DateTime<Utc>) -> WakeAttemptOutcome {
    WakeAttemptOutcome::Retry {
        next_attempt_at: at,
        error: "HTTP 503".into(),
    }
}

fn terminal(reason: PushTerminalReason) -> WakeAttemptOutcome {
    WakeAttemptOutcome::Terminal {
        reason,
        error: "provider verdict".into(),
    }
}

fn collapsible(
    tenant: &str,
    execution: &str,
    command_id: &str,
    at: DateTime<Utc>,
) -> CollapsibleWake {
    CollapsibleWake {
        tenant_id: tenant.into(),
        device_id: "device-a".into(),
        execution_id: execution.into(),
        topic: "resume".into(),
        command_id: command_id.into(),
        created_at: at,
    }
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

async fn re_enqueueing_a_command_returns_the_original_wake(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    let first = enqueue(s, "cmd-1", now).await;
    let replay = enqueue(s, "cmd-1", now + Duration::seconds(5)).await;
    assert_eq!(first, replay);
    assert_eq!(claim(s, now + Duration::seconds(5), 10).await.len(), 1);
}

async fn same_command_for_another_tenant_or_device_is_a_separate_wake(s: &impl Store) {
    let now = Utc::now();
    let base = s
        .enqueue_wake("tenant-a", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let other_tenant = s
        .enqueue_wake("tenant-b", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let other_device = s
        .enqueue_wake("tenant-a", "device-b", "cmd-1", now)
        .await
        .unwrap();
    assert_ne!(base, other_tenant);
    assert_ne!(base, other_device);
    assert_ne!(other_tenant, other_device);
}

async fn command_with_wake_persists_command_and_claimable_wake(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    let command = MobileCommand {
        id: "cmd-1".into(),
        device_id: "device-a".into(),
        command_type: "resume".into(),
        payload: "{}".into(),
        created_at: String::new(),
        acked_at: None,
    };
    let wake_id = s
        .create_mobile_command_with_wake(&command, "tenant-a", now)
        .await
        .unwrap();
    assert_eq!(claim_one(s, now).await.id, wake_id);
    let pending = s.fetch_pending_commands("device-a", 10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, "cmd-1");
}

// ---------------------------------------------------------------------------
// Claim eligibility
// ---------------------------------------------------------------------------

async fn wakes_for_a_device_without_a_push_token_are_never_claimed(s: &impl Store) {
    register(s, device("device-a", "tenant-a", None)).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    assert!(claim(s, now, 10).await.is_empty());
}

async fn wakes_for_an_unregistered_device_are_never_claimed(s: &impl Store) {
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    assert!(claim(s, now, 10).await.is_empty());
}

async fn wake_never_uses_another_tenants_registration_of_the_device(s: &impl Store) {
    register(s, device("device-a", "tenant-b", Some("token-b"))).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    assert!(
        claim(s, now, 10).await.is_empty(),
        "a tenant-a wake must not be delivered with tenant-b's token"
    );
}

async fn claim_returns_the_registered_token_and_platform(s: &impl Store) {
    register(
        s,
        MobileDevice {
            platform: "android".into(),
            ..device("device-a", "tenant-a", Some("token-xyz"))
        },
    )
    .await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let wake = claim_one(s, now).await;
    assert_eq!(wake.push_token, "token-xyz");
    assert_eq!(wake.platform, "android");
    assert_eq!(wake.attempts, 0);
}

async fn claim_takes_at_most_limit_wakes_oldest_first(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    // Enqueued newest-first so insertion order cannot explain the result.
    for (command, age_secs) in [("cmd-new", 10), ("cmd-mid", 20), ("cmd-old", 30)] {
        enqueue(s, command, now - Duration::seconds(age_secs)).await;
    }
    let mut claimed: Vec<String> = claim(s, now, 2)
        .await
        .into_iter()
        .map(|wake| wake.command_id)
        .collect();
    claimed.sort();
    assert_eq!(claimed, vec!["cmd-mid", "cmd-old"]);
}

// ---------------------------------------------------------------------------
// Leasing and outcome fencing
// ---------------------------------------------------------------------------

async fn leased_wake_is_hidden_until_its_lease_expires(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let lease = now + Duration::seconds(30);
    let first = s.claim_due_wakes(now, lease, 10).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].lease_until, lease);

    assert!(claim(s, now + Duration::seconds(10), 10).await.is_empty());
    let reclaimed = claim(s, now + Duration::seconds(31), 10).await;
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].command_id, "cmd-1");
}

async fn retry_returns_the_wake_at_its_next_attempt_time(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let wake = claim_one(s, now).await;
    let next = now + Duration::minutes(1);
    s.record_wake_outcome(&wake, &retry_at(next), now)
        .await
        .unwrap();

    assert!(claim(s, next - Duration::seconds(1), 10).await.is_empty());
    let retried = claim_one(s, next).await;
    assert_eq!(retried.command_id, "cmd-1");
    assert_eq!(retried.attempts, 1);
}

async fn outcome_from_a_claimant_that_lost_its_lease_is_rejected(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let stale = claim_one(s, now).await;
    let reclaimed_at = now + Duration::seconds(31);
    let live = claim_one(s, reclaimed_at).await;

    assert!(
        s.record_wake_outcome(&stale, &WakeAttemptOutcome::Delivered, reclaimed_at)
            .await
            .is_err()
    );
    // The live claimant still owns the row and its attempt is the only one counted.
    s.record_wake_outcome(&live, &retry_at(reclaimed_at), reclaimed_at)
        .await
        .unwrap();
    assert_eq!(claim_one(s, reclaimed_at).await.attempts, 1);
}

async fn outcome_for_a_wake_that_was_never_claimed_is_rejected(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    let id = enqueue(s, "cmd-1", now).await;
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
        s.record_wake_outcome(&fabricated, &WakeAttemptOutcome::Delivered, now)
            .await
            .is_err()
    );
    assert_eq!(claim_one(s, now).await.attempts, 0);
}

async fn replayed_outcome_is_rejected_and_not_counted_twice(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let wake = claim_one(s, now).await;
    s.record_wake_outcome(&wake, &retry_at(now), now)
        .await
        .unwrap();
    assert!(
        s.record_wake_outcome(&wake, &retry_at(now), now)
            .await
            .is_err()
    );
    assert_eq!(claim_one(s, now).await.attempts, 1);
}

async fn delivered_and_terminal_wakes_are_never_claimed_again(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    let outcomes = [
        ("delivered", WakeAttemptOutcome::Delivered),
        ("retry-limit", terminal(PushTerminalReason::RetryLimit)),
        ("permanent", terminal(PushTerminalReason::PermanentFailure)),
        ("misconfigured", terminal(PushTerminalReason::Misconfigured)),
    ];
    for (command, outcome) in &outcomes {
        enqueue(s, command, now).await;
        let wake = claim_one(s, now).await;
        s.record_wake_outcome(&wake, outcome, now).await.unwrap();
    }
    assert!(
        claim(s, now + Duration::hours(1), 10).await.is_empty(),
        "finished wakes must stay out of the drain loop"
    );
}

// ---------------------------------------------------------------------------
// Invalid-token quarantine
// ---------------------------------------------------------------------------

async fn invalid_token_quarantines_the_device_and_stops_its_wakes(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let wake = claim_one(s, now).await;
    s.record_wake_outcome(&wake, &terminal(PushTerminalReason::InvalidToken), now)
        .await
        .unwrap();

    let quarantined = s.get_mobile_device("device-a").await.unwrap().unwrap();
    assert!(!quarantined.active);
    assert_eq!(quarantined.push_token, None);
    enqueue(s, "cmd-2", now).await;
    assert!(claim(s, now, 10).await.is_empty());
}

async fn other_terminal_reasons_keep_the_device_registered(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let wake = claim_one(s, now).await;
    s.record_wake_outcome(&wake, &terminal(PushTerminalReason::PermanentFailure), now)
        .await
        .unwrap();
    let registered = s.get_mobile_device("device-a").await.unwrap().unwrap();
    assert!(registered.active);
    assert_eq!(registered.push_token.as_deref(), Some("token-a"));
}

async fn invalid_token_cleanup_spares_a_token_reregistered_mid_flight(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    let wake = claim_one(s, now).await;
    register(s, device("device-a", "tenant-a", Some("token-b"))).await;
    s.record_wake_outcome(&wake, &terminal(PushTerminalReason::InvalidToken), now)
        .await
        .unwrap();
    let registered = s.get_mobile_device("device-a").await.unwrap().unwrap();
    assert!(registered.active);
    assert_eq!(registered.push_token.as_deref(), Some("token-b"));
}

async fn device_registration_refuses_cross_tenant_overwrite(s: &impl Store) {
    standard(s).await;
    let err = s
        .register_mobile_device(&device("device-a", "tenant-b", Some("attacker")))
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "got {err:?}");
    let registered = s.get_mobile_device("device-a").await.unwrap().unwrap();
    assert_eq!(registered.tenant_id, "tenant-a");
    assert_eq!(registered.push_token.as_deref(), Some("token-a"));
}

// ---------------------------------------------------------------------------
// Command acknowledgements
// ---------------------------------------------------------------------------

async fn empty_ack_list_acknowledges_nothing(s: &impl Store) {
    standard(s).await;
    enqueue(s, "cmd-1", Utc::now()).await;
    assert_eq!(
        s.record_command_acks("tenant-a", "device-a", &[], Utc::now())
            .await
            .unwrap(),
        0
    );
}

async fn ack_marks_only_the_listed_commands_of_that_device(s: &impl Store) {
    let now = Utc::now();
    for command in ["cmd-1", "cmd-2", "cmd-3"] {
        s.enqueue_wake("tenant-a", "device-a", command, now)
            .await
            .unwrap();
    }
    s.enqueue_wake("tenant-a", "device-b", "cmd-1", now)
        .await
        .unwrap();
    let acked = s
        .record_command_acks(
            "tenant-a",
            "device-a",
            &["cmd-1".into(), "cmd-3".into(), "unknown".into()],
            now,
        )
        .await
        .unwrap();
    assert_eq!(acked, 2);
}

/// Device ids are unique across tenants (see the cross-tenant overwrite
/// refusal), so an ack from `device-a` belongs to its registered tenant only.
async fn ack_never_marks_another_tenants_wake_for_the_same_device_id(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "cmd-1", now).await;
    s.enqueue_wake("tenant-b", "device-a", "cmd-1", now)
        .await
        .unwrap();
    let acked = s
        .record_command_acks("tenant-a", "device-a", &["cmd-1".into()], now)
        .await
        .unwrap();
    assert_eq!(acked, 1, "the ack must not cross into tenant-b's wake row");
}

// ---------------------------------------------------------------------------
// Collapsible wakes and retention
// ---------------------------------------------------------------------------

async fn newer_collapsible_wake_supersedes_pending_wake_of_the_same_execution(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    s.enqueue_collapsible_wake(&collapsible("tenant-a", "exec-a", "old", now))
        .await
        .unwrap();
    let newer = s
        .enqueue_collapsible_wake(&collapsible(
            "tenant-a",
            "exec-a",
            "new",
            now + Duration::seconds(1),
        ))
        .await
        .unwrap();
    s.enqueue_collapsible_wake(&collapsible(
        "tenant-a",
        "exec-b",
        "distinct",
        now + Duration::seconds(1),
    ))
    .await
    .unwrap();

    let mut claimed: Vec<String> = claim(s, now + Duration::seconds(1), 10)
        .await
        .into_iter()
        .map(|wake| wake.command_id)
        .collect();
    claimed.sort();
    assert_eq!(claimed, vec!["distinct", "new"]);
    // Replaying the newest command is idempotent and does not resurrect `old`.
    assert_eq!(
        s.enqueue_collapsible_wake(&collapsible(
            "tenant-a",
            "exec-a",
            "new",
            now + Duration::seconds(1),
        ))
        .await
        .unwrap(),
        newer
    );
}

async fn collapsible_supersede_never_crosses_tenants(s: &impl Store) {
    // Only tenant-b's registration exists, so only its wake is claimable.
    register(s, device("device-a", "tenant-b", Some("token-b"))).await;
    let now = Utc::now();
    s.enqueue_collapsible_wake(&collapsible("tenant-b", "exec-a", "theirs", now))
        .await
        .unwrap();
    s.enqueue_collapsible_wake(&collapsible(
        "tenant-a",
        "exec-a",
        "mine",
        now + Duration::seconds(1),
    ))
    .await
    .unwrap();
    let claimed = claim(s, now + Duration::seconds(1), 10).await;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].command_id, "theirs");
}

async fn prune_deletes_only_old_wakes_that_are_not_leased(s: &impl Store) {
    standard(s).await;
    let now = Utc::now();
    enqueue(s, "old-leased", now - Duration::days(11)).await;
    enqueue(s, "old-pending", now - Duration::days(10)).await;
    enqueue(s, "fresh", now).await;
    // Leases the oldest row: in-flight work is never pruned.
    assert_eq!(claim_one(s, now).await.command_id, "old-leased");

    assert_eq!(s.prune_wakes(now - Duration::days(7)).await.unwrap(), 1);

    let mut left: Vec<String> = claim(s, now + Duration::seconds(31), 10)
        .await
        .into_iter()
        .map(|wake| wake.command_id)
        .collect();
    left.sort();
    assert_eq!(left, vec!["fresh", "old-leased"]);
}

backend_contract!(
    re_enqueueing_a_command_returns_the_original_wake,
    same_command_for_another_tenant_or_device_is_a_separate_wake,
    command_with_wake_persists_command_and_claimable_wake,
    wakes_for_a_device_without_a_push_token_are_never_claimed,
    wakes_for_an_unregistered_device_are_never_claimed,
    wake_never_uses_another_tenants_registration_of_the_device,
    claim_returns_the_registered_token_and_platform,
    claim_takes_at_most_limit_wakes_oldest_first,
    leased_wake_is_hidden_until_its_lease_expires,
    retry_returns_the_wake_at_its_next_attempt_time,
    outcome_from_a_claimant_that_lost_its_lease_is_rejected,
    outcome_for_a_wake_that_was_never_claimed_is_rejected,
    replayed_outcome_is_rejected_and_not_counted_twice,
    delivered_and_terminal_wakes_are_never_claimed_again,
    invalid_token_quarantines_the_device_and_stops_its_wakes,
    other_terminal_reasons_keep_the_device_registered,
    invalid_token_cleanup_spares_a_token_reregistered_mid_flight,
    device_registration_refuses_cross_tenant_overwrite,
    empty_ack_list_acknowledges_nothing,
    ack_marks_only_the_listed_commands_of_that_device,
    ack_never_marks_another_tenants_wake_for_the_same_device_id,
    newer_collapsible_wake_supersedes_pending_wake_of_the_same_execution,
    collapsible_supersede_never_crosses_tenants,
    prune_deletes_only_old_wakes_that_are_not_leased,
);
