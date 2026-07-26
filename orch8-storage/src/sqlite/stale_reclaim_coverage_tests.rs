//! Coverage tests for stale-row reclaim, heartbeat leasing, the terminal
//! transition/outbox CAS, and entitlement admission on the SQLite backend.
//!
//! Mirrors the Postgres behaviors pinned by `tests/postgres_integration.rs`
//! (commits 9c8a062 / e2d98ff) against the in-memory SQLite backend.
//!
//! Count contract: 39 independently named unit tests.

use super::*;
use crate::InstanceStore;
use chrono::Duration;
use orch8_types::context::ExecutionContext;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

fn make_instance(tenant: &str, state: InstanceState) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        state,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

/// Backdate (or future-date) a row's bookkeeping columns directly, bypassing
/// the store API so the reaper sees exactly the staleness we want.
async fn set_row_clock(
    storage: &SqliteStorage,
    id: InstanceId,
    updated_at: DateTime<Utc>,
    next_fire_at: Option<DateTime<Utc>>,
) {
    sqlx::query("UPDATE task_instances SET updated_at=?1, next_fire_at=?2 WHERE id=?3")
        .bind(updated_at.to_rfc3339())
        .bind(next_fire_at.map(|t| t.to_rfc3339()))
        .bind(id.into_uuid().to_string())
        .execute(&storage.pool)
        .await
        .unwrap();
}

fn outbox_entry(instance_id: InstanceId, event_type: &str) -> WebhookOutboxEntry {
    WebhookOutboxEntry {
        id: Uuid::new_v4(),
        url: "https://hooks.example.test/deliver".into(),
        event_type: event_type.into(),
        instance_id: Some(instance_id.into_uuid()),
        payload: serde_json::json!({"event": event_type}),
        attempts: 0,
        last_error: None,
        created_at: Utc::now(),
        delivery_id: None,
        status: WebhookOutboxStatus::Pending,
        next_attempt_at: None,
        claimed_at: None,
    }
}

async fn outbox_rows(storage: &SqliteStorage) -> Vec<(String, String)> {
    sqlx::query_as("SELECT event_type, payload FROM webhook_outbox ORDER BY event_type")
        .fetch_all(&storage.pool)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// recover_stale_instances: only stale `running` rows are reclaimed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_reclaim_001_stale_running_returns_to_scheduled() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let stale_at = Utc::now() - Duration::hours(1);
    set_row_clock(&storage, instance.id, stale_at, None).await;

    let recovered = storage
        .recover_stale_instances(std::time::Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(recovered, 1);
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(after.state, InstanceState::Scheduled);
}

#[tokio::test]
async fn coverage_reclaim_002_fresh_running_is_untouched() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();

    let recovered = storage
        .recover_stale_instances(std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(recovered, 0);
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(after.state, InstanceState::Running);
}

#[tokio::test]
async fn coverage_reclaim_003_stale_waiting_is_never_reclaimed() {
    // Parked waiters (human-review gates, wait-for-event) never heartbeat;
    // the lease reaper must leave them to the deadline/timeout sweep.
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Waiting);
    storage.create_instance(&instance).await.unwrap();
    let stale_at = Utc::now() - Duration::days(7);
    set_row_clock(&storage, instance.id, stale_at, None).await;

    let recovered = storage
        .recover_stale_instances(std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(recovered, 0);
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(after.state, InstanceState::Waiting);
}

macro_rules! terminal_state_case {
    ($name:ident, $state:expr) => {
        #[tokio::test]
        async fn $name() {
            let storage = store().await;
            let instance = make_instance("t", $state);
            storage.create_instance(&instance).await.unwrap();
            let stale_at = Utc::now() - Duration::days(7);
            set_row_clock(&storage, instance.id, stale_at, None).await;
            let recovered = storage
                .recover_stale_instances(std::time::Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(recovered, 0);
            let after = storage.get_instance(instance.id).await.unwrap().unwrap();
            assert_eq!(after.state, $state);
        }
    };
}

terminal_state_case!(
    coverage_reclaim_004_stale_completed_is_untouched,
    InstanceState::Completed
);
terminal_state_case!(
    coverage_reclaim_005_stale_failed_is_untouched,
    InstanceState::Failed
);
terminal_state_case!(
    coverage_reclaim_006_stale_cancelled_is_untouched,
    InstanceState::Cancelled
);

#[tokio::test]
async fn coverage_reclaim_007_stale_scheduled_is_untouched() {
    // Only `running` rows are reclaimed; a merely-old scheduled row keeps
    // its fire time instead of being stamped "due now".
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Scheduled);
    storage.create_instance(&instance).await.unwrap();
    let stale_at = Utc::now() - Duration::days(7);
    let fire_at = Utc::now() + Duration::days(30);
    set_row_clock(&storage, instance.id, stale_at, Some(fire_at)).await;

    let recovered = storage
        .recover_stale_instances(std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(recovered, 0);
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(
        after.next_fire_at.map(|t| t.timestamp()),
        Some(fire_at.timestamp()),
        "recover must not reschedule rows it does not reclaim"
    );
}

#[tokio::test]
async fn coverage_reclaim_008_recovered_row_fire_time_reset_to_now() {
    // A wedged instance may carry a far-future next_fire_at from before it
    // stalled; reclaim must reset it or claim_due would never pick it up.
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let stale_at = Utc::now() - Duration::hours(1);
    let far_future = Utc::now() + Duration::days(30);
    set_row_clock(&storage, instance.id, stale_at, Some(far_future)).await;

    storage
        .recover_stale_instances(std::time::Duration::from_secs(300))
        .await
        .unwrap();
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert!(
        after.next_fire_at.unwrap() < Utc::now() + Duration::minutes(1),
        "recovered next_fire_at must be ~now, not {}",
        after.next_fire_at.unwrap()
    );
}

#[tokio::test]
async fn coverage_reclaim_009_recovered_row_updated_at_bumped() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let stale_at = Utc::now() - Duration::hours(1);
    set_row_clock(&storage, instance.id, stale_at, None).await;

    storage
        .recover_stale_instances(std::time::Duration::from_secs(300))
        .await
        .unwrap();
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert!(
        after.updated_at > stale_at,
        "reclaim must refresh the lease clock"
    );
}

#[tokio::test]
async fn coverage_reclaim_010_multiple_stale_rows_all_reclaimed() {
    let storage = store().await;
    let stale_at = Utc::now() - Duration::hours(1);
    for _ in 0..4 {
        let instance = make_instance("t", InstanceState::Running);
        storage.create_instance(&instance).await.unwrap();
        set_row_clock(&storage, instance.id, stale_at, None).await;
    }
    let fresh = make_instance("t", InstanceState::Running);
    storage.create_instance(&fresh).await.unwrap();

    let recovered = storage
        .recover_stale_instances(std::time::Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(recovered, 4);
    assert_eq!(
        storage.get_instance(fresh.id).await.unwrap().unwrap().state,
        InstanceState::Running
    );
}

#[tokio::test]
async fn coverage_reclaim_011_recovered_instance_is_claimable_again() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let stale_at = Utc::now() - Duration::hours(1);
    set_row_clock(&storage, instance.id, stale_at, None).await;

    storage
        .recover_stale_instances(std::time::Duration::from_secs(300))
        .await
        .unwrap();
    let claimed = storage
        .claim_due_instances(Utc::now() + Duration::seconds(5), 10, 0)
        .await
        .unwrap();
    assert!(
        claimed.iter().any(|candidate| candidate.id == instance.id),
        "a recovered row must re-enter the scheduler's claim set"
    );
}

// ---------------------------------------------------------------------------
// heartbeat_instance: lease renewal scoped to running/waiting
// ---------------------------------------------------------------------------

macro_rules! heartbeat_case {
    ($name:ident, $state:expr, $renews:expr) => {
        #[tokio::test]
        async fn $name() {
            let storage = store().await;
            let instance = make_instance("t", $state);
            storage.create_instance(&instance).await.unwrap();
            let stale_at = Utc::now() - Duration::hours(1);
            set_row_clock(&storage, instance.id, stale_at, None).await;

            storage.heartbeat_instance(instance.id).await.unwrap();
            let after = storage.get_instance(instance.id).await.unwrap().unwrap();
            assert_eq!(
                after.updated_at > stale_at,
                $renews,
                "heartbeat renewal expectation failed for {:?}",
                $state
            );
            assert_eq!(after.state, $state, "heartbeat must not transition");
        }
    };
}

heartbeat_case!(
    coverage_reclaim_012_heartbeat_renews_running_lease,
    InstanceState::Running,
    true
);
heartbeat_case!(
    coverage_reclaim_013_heartbeat_renews_waiting_lease,
    InstanceState::Waiting,
    true
);
heartbeat_case!(
    coverage_reclaim_014_heartbeat_ignores_scheduled,
    InstanceState::Scheduled,
    false
);
heartbeat_case!(
    coverage_reclaim_015_heartbeat_ignores_completed,
    InstanceState::Completed,
    false
);
heartbeat_case!(
    coverage_reclaim_016_heartbeat_ignores_failed,
    InstanceState::Failed,
    false
);

#[tokio::test]
async fn coverage_reclaim_017_heartbeat_unknown_instance_is_ok() {
    let storage = store().await;
    assert!(storage.heartbeat_instance(InstanceId::new()).await.is_ok());
}

// ---------------------------------------------------------------------------
// conditional_update_instance_state_with_outbox: atomic terminal CAS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_reclaim_018_cas_wins_when_expected_state_matches() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let won = storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
            &[],
        )
        .await
        .unwrap();
    assert!(won);
    assert_eq!(
        storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
}

#[tokio::test]
async fn coverage_reclaim_019_cas_loses_when_state_already_moved() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Completed);
    storage.create_instance(&instance).await.unwrap();
    let won = storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Failed,
            None,
            &[],
        )
        .await
        .unwrap();
    assert!(!won);
    assert_eq!(
        storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed,
        "a lost CAS must leave the row untouched"
    );
}

#[tokio::test]
async fn coverage_reclaim_020_lost_cas_enqueues_no_outbox_rows() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Completed);
    storage.create_instance(&instance).await.unwrap();
    let won = storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Failed,
            None,
            &[outbox_entry(instance.id, "instance.failed")],
        )
        .await
        .unwrap();
    assert!(!won);
    assert!(
        outbox_rows(&storage).await.is_empty(),
        "outbox rows for a transition that never happened must roll back"
    );
}

#[tokio::test]
async fn coverage_reclaim_021_won_cas_enqueues_all_entries_atomically() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let entries = vec![
        outbox_entry(instance.id, "instance.completed"),
        outbox_entry(instance.id, "tenant.usage"),
    ];
    let won = storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
            &entries,
        )
        .await
        .unwrap();
    assert!(won);
    let rows = outbox_rows(&storage).await;
    let event_types: Vec<&str> = rows
        .iter()
        .map(|(event_type, _)| event_type.as_str())
        .collect();
    assert_eq!(event_types, vec!["instance.completed", "tenant.usage"]);
}

#[tokio::test]
async fn coverage_reclaim_022_won_cas_persists_payload_verbatim() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let entry = outbox_entry(instance.id, "instance.failed");
    storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Failed,
            None,
            &[entry.clone()],
        )
        .await
        .unwrap();
    let rows = outbox_rows(&storage).await;
    assert_eq!(rows.len(), 1);
    let payload: serde_json::Value = serde_json::from_str(&rows[0].1).unwrap();
    assert_eq!(payload, entry.payload);
}

#[tokio::test]
async fn coverage_reclaim_023_won_cas_applies_next_fire_at() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let retry_at = Utc::now() + Duration::minutes(10);
    let won = storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Scheduled,
            Some(retry_at),
            &[],
        )
        .await
        .unwrap();
    assert!(won);
    let after = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(
        after.next_fire_at.map(|t| t.timestamp()),
        Some(retry_at.timestamp())
    );
}

#[tokio::test]
async fn coverage_reclaim_024_cas_cannot_replay_from_stale_expected_state() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    let first = storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
            &[],
        )
        .await
        .unwrap();
    let replay = storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
            &[],
        )
        .await
        .unwrap();
    assert!(first);
    assert!(!replay, "the second writer must observe the CAS loss");
}

#[tokio::test]
async fn coverage_reclaim_025_cas_outbox_row_carries_instance_id() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Running);
    storage.create_instance(&instance).await.unwrap();
    storage
        .conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
            &[outbox_entry(instance.id, "instance.completed")],
        )
        .await
        .unwrap();
    let stored: String = sqlx::query_scalar("SELECT instance_id FROM webhook_outbox")
        .fetch_one(&storage.pool)
        .await
        .unwrap();
    assert_eq!(stored, instance.id.into_uuid().to_string());
}

#[tokio::test]
async fn coverage_reclaim_026_cas_on_missing_instance_loses_cleanly() {
    let storage = store().await;
    let won = storage
        .conditional_update_instance_state_with_outbox(
            InstanceId::new(),
            InstanceState::Running,
            InstanceState::Completed,
            None,
            &[outbox_entry(InstanceId::new(), "instance.completed")],
        )
        .await
        .unwrap();
    assert!(!won);
    assert!(outbox_rows(&storage).await.is_empty());
}

// ---------------------------------------------------------------------------
// Entitlement admission: per-tenant active-instance ceilings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_reclaim_027_admitted_insert_under_ceiling_succeeds() {
    let storage = store().await;
    let instance = make_instance("t", InstanceState::Scheduled);
    assert!(storage.create_instance_admitted(&instance, 1).await.is_ok());
}

#[tokio::test]
async fn coverage_reclaim_028_admitted_insert_at_ceiling_is_quota_exceeded() {
    let storage = store().await;
    storage
        .create_instance_admitted(&make_instance("t", InstanceState::Running), 1)
        .await
        .unwrap();
    let result = storage
        .create_instance_admitted(&make_instance("t", InstanceState::Scheduled), 1)
        .await;
    assert!(matches!(result, Err(StorageError::QuotaExceeded(_))));
}

macro_rules! admission_counted_state_case {
    ($name:ident, $state:expr) => {
        #[tokio::test]
        async fn $name() {
            let storage = store().await;
            storage
                .create_instance_admitted(&make_instance("t", $state), 5)
                .await
                .unwrap();
            let result = storage
                .create_instance_admitted(&make_instance("t", InstanceState::Scheduled), 1)
                .await;
            assert!(
                matches!(result, Err(StorageError::QuotaExceeded(_))),
                "state {:?} must count against the active ceiling",
                $state
            );
        }
    };
}

admission_counted_state_case!(
    coverage_reclaim_029_running_counts_against_ceiling,
    InstanceState::Running
);
admission_counted_state_case!(
    coverage_reclaim_030_waiting_counts_against_ceiling,
    InstanceState::Waiting
);
admission_counted_state_case!(
    coverage_reclaim_031_paused_counts_against_ceiling,
    InstanceState::Paused
);

macro_rules! admission_free_state_case {
    ($name:ident, $state:expr) => {
        #[tokio::test]
        async fn $name() {
            let storage = store().await;
            storage
                .create_instance_admitted(&make_instance("t", $state), 5)
                .await
                .unwrap();
            assert!(
                storage
                    .create_instance_admitted(&make_instance("t", InstanceState::Scheduled), 1)
                    .await
                    .is_ok(),
                "state {:?} must not count against the active ceiling",
                $state
            );
        }
    };
}

admission_free_state_case!(
    coverage_reclaim_032_completed_does_not_count,
    InstanceState::Completed
);
admission_free_state_case!(
    coverage_reclaim_033_failed_does_not_count,
    InstanceState::Failed
);

#[tokio::test]
async fn coverage_reclaim_034_admission_ceiling_is_per_tenant() {
    let storage = store().await;
    storage
        .create_instance_admitted(&make_instance("tenant-a", InstanceState::Running), 1)
        .await
        .unwrap();
    // tenant-b's ceiling is independent of tenant-a's usage.
    assert!(
        storage
            .create_instance_admitted(&make_instance("tenant-b", InstanceState::Scheduled), 1)
            .await
            .is_ok()
    );
    assert!(matches!(
        storage
            .create_instance_admitted(&make_instance("tenant-a", InstanceState::Scheduled), 1)
            .await,
        Err(StorageError::QuotaExceeded(_))
    ));
}

#[tokio::test]
async fn coverage_reclaim_035_batch_admitted_enforces_per_tenant_limits() {
    let storage = store().await;
    let mut limits = HashMap::new();
    limits.insert(TenantId::unchecked("tenant-a"), 1_u64);
    limits.insert(TenantId::unchecked("tenant-b"), 2_u64);
    let batch = vec![
        make_instance("tenant-a", InstanceState::Scheduled),
        make_instance("tenant-b", InstanceState::Scheduled),
        make_instance("tenant-b", InstanceState::Scheduled),
    ];
    let inserted = storage
        .create_instances_batch_admitted(&batch, &limits)
        .await
        .unwrap();
    assert_eq!(inserted, 3);
}

#[tokio::test]
async fn coverage_reclaim_036_batch_admitted_rolls_back_atomically_on_breach() {
    let storage = store().await;
    let mut limits = HashMap::new();
    limits.insert(TenantId::unchecked("tenant-a"), 1_u64);
    let batch = vec![
        make_instance("tenant-a", InstanceState::Scheduled),
        make_instance("tenant-a", InstanceState::Scheduled),
    ];
    let result = storage
        .create_instances_batch_admitted(&batch, &limits)
        .await;
    assert!(matches!(result, Err(StorageError::QuotaExceeded(_))));
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(&storage.pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0, "a breached batch must not partially insert");
}

heartbeat_case!(
    coverage_reclaim_037_heartbeat_ignores_paused,
    InstanceState::Paused,
    false
);
heartbeat_case!(
    coverage_reclaim_038_heartbeat_ignores_cancelled,
    InstanceState::Cancelled,
    false
);

admission_free_state_case!(
    coverage_reclaim_039_cancelled_does_not_count,
    InstanceState::Cancelled
);
