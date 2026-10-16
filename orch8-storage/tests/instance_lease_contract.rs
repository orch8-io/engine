//! Instance lease, terminal-transition and admission contract, identical for
//! `SQLite` and Postgres.
//!
//! - The stale-lease reaper reclaims only `running` rows whose lease clock is
//!   past the threshold, and makes them due now; parked, scheduled and
//!   terminal rows are never touched.
//! - A heartbeat from the claim holder renews the lease of a `running` or
//!   `waiting` instance only; it never renews or transitions any other state.
//! - The state CAS and its webhook outbox rows commit together or not at all.
//! - Admission counts every non-terminal state (including `scheduled`)
//!   against the tenant's ceiling, and a tenant without a limit is denied.
//!
//! ```text
//! DATABASE_URL=postgres://… cargo test -p orch8-storage --test instance_lease_contract
//! ```

#[macro_use]
mod support;

use std::collections::HashMap;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use orch8_storage::{InstanceLease, StorageBackend};
use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};

const STALE_AFTER: StdDuration = StdDuration::from_secs(300);

/// Creates the sequence every instance in a check belongs to.
async fn sequence(s: &dyn StorageBackend) -> SequenceId {
    let id = SequenceId::new();
    s.create_sequence(&SequenceDefinition {
        embed: None,
        sub_tenant: None,
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        id,
        tenant_id: TenantId::unchecked("tenant-a"),
        namespace: Namespace::new("default"),
        name: format!("seq-{id}"),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        placement: None,
        created_at: Utc::now(),
    })
    .await
    .unwrap();
    id
}

fn instance(sequence_id: SequenceId, tenant: &str, state: InstanceState) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        sub_tenant: None,
        id: InstanceId::new(),
        sequence_id,
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

/// An instance whose lease clock last moved an hour ago.
fn stale(sequence_id: SequenceId, state: InstanceState) -> TaskInstance {
    TaskInstance {
        updated_at: Utc::now() - Duration::hours(1),
        ..instance(sequence_id, "tenant-a", state)
    }
}

fn webhook(instance_id: InstanceId, event_type: &str) -> WebhookOutboxEntry {
    WebhookOutboxEntry {
        id: Uuid::now_v7(),
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

async fn state_of(s: &dyn StorageBackend, id: InstanceId) -> InstanceState {
    s.get_instance(id).await.unwrap().unwrap().state
}

async fn updated_at(s: &dyn StorageBackend, id: InstanceId) -> DateTime<Utc> {
    s.get_instance(id).await.unwrap().unwrap().updated_at
}

// ---------------------------------------------------------------------------
// Stale-lease reaper
// ---------------------------------------------------------------------------

async fn stale_running_instance_is_rescheduled_due_now_and_claimable(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let wedged = TaskInstance {
        next_fire_at: Some(Utc::now() + Duration::days(30)),
        ..stale(seq, InstanceState::Running)
    };
    s.create_instance(&wedged).await.unwrap();

    assert_eq!(s.recover_stale_instances(STALE_AFTER).await.unwrap(), 1);

    let recovered = s.get_instance(wedged.id).await.unwrap().unwrap();
    assert_eq!(recovered.state, InstanceState::Scheduled);
    assert!(
        recovered.updated_at > wedged.updated_at,
        "lease clock refreshed"
    );
    let claimed = s
        .claim_due_instances(Utc::now() + Duration::seconds(5), 10, 0)
        .await
        .unwrap();
    assert!(
        claimed.iter().any(|c| c.id == wedged.id),
        "a far-future fire time must be reset so the row is due again"
    );
}

async fn only_running_instances_past_the_threshold_are_reclaimed(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    for _ in 0..3 {
        s.create_instance(&stale(seq, InstanceState::Running))
            .await
            .unwrap();
    }
    let fresh = instance(seq, "tenant-a", InstanceState::Running);
    s.create_instance(&fresh).await.unwrap();

    assert_eq!(s.recover_stale_instances(STALE_AFTER).await.unwrap(), 3);
    assert_eq!(state_of(s, fresh.id).await, InstanceState::Running);
}

async fn reaper_never_touches_parked_scheduled_or_terminal_instances(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let fire_at = Utc::now() + Duration::days(30);
    let mut untouched = Vec::new();
    for state in [
        InstanceState::Waiting,
        InstanceState::Paused,
        InstanceState::Scheduled,
        InstanceState::Completed,
        InstanceState::Failed,
        InstanceState::Cancelled,
    ] {
        let row = TaskInstance {
            next_fire_at: Some(fire_at),
            ..stale(seq, state)
        };
        s.create_instance(&row).await.unwrap();
        untouched.push(row);
    }

    assert_eq!(s.recover_stale_instances(STALE_AFTER).await.unwrap(), 0);

    for row in &untouched {
        let after = s.get_instance(row.id).await.unwrap().unwrap();
        assert_eq!(after.state, row.state, "{:?} changed state", row.state);
        assert_eq!(
            after.next_fire_at.map(|t| t.timestamp()),
            Some(fire_at.timestamp()),
            "{:?} was rescheduled",
            row.state
        );
    }
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

async fn heartbeat_renews_only_running_and_waiting_leases(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let cases = [
        (InstanceState::Running, true),
        (InstanceState::Waiting, true),
        (InstanceState::Scheduled, false),
        (InstanceState::Paused, false),
        (InstanceState::Completed, false),
        (InstanceState::Failed, false),
        (InstanceState::Cancelled, false),
    ];
    for (state, renews) in cases {
        let row = instance(seq, "tenant-a", InstanceState::Scheduled);
        s.create_instance(&row).await.unwrap();
        let claimed = s
            .claim_due_instances(Utc::now(), 100, 0)
            .await
            .unwrap()
            .into_iter()
            .find(|c| c.id == row.id)
            .expect("fresh scheduled row is claimable");
        let token = claimed.updated_at;
        if state != InstanceState::Running {
            s.update_instance_state(row.id, state, None).await.unwrap();
        }
        let before = updated_at(s, row.id).await;

        let lease = s.heartbeat_instance(row.id, token).await.unwrap();

        assert_eq!(
            lease,
            InstanceLease::Held,
            "{state:?}: the claim holder keeps its lease"
        );
        assert_eq!(
            updated_at(s, row.id).await > before,
            renews,
            "{state:?}: renewal expectation"
        );
        assert_eq!(
            state_of(s, row.id).await,
            state,
            "{state:?}: heartbeat must not transition"
        );
    }
}

// ---------------------------------------------------------------------------
// Terminal CAS with outbox
// ---------------------------------------------------------------------------

async fn won_state_cas_enqueues_every_outbox_entry(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let row = instance(seq, "tenant-a", InstanceState::Running);
    s.create_instance(&row).await.unwrap();
    let entries = [
        webhook(row.id, "instance.completed"),
        webhook(row.id, "tenant.usage"),
    ];

    let won = s
        .conditional_update_instance_state_with_outbox(
            row.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
            &entries,
        )
        .await
        .unwrap();

    assert!(won);
    for entry in &entries {
        let stored = s.get_webhook_outbox(entry.id).await.unwrap();
        assert_eq!(
            stored.map(|e| e.event_type),
            Some(entry.event_type.clone()),
            "outbox row {} missing",
            entry.event_type
        );
    }
}

async fn won_state_cas_applies_the_requested_fire_time(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let row = instance(seq, "tenant-a", InstanceState::Running);
    s.create_instance(&row).await.unwrap();
    let retry_at = Utc::now() + Duration::minutes(10);

    assert!(
        s.conditional_update_instance_state_with_outbox(
            row.id,
            InstanceState::Running,
            InstanceState::Scheduled,
            Some(retry_at),
            &[],
        )
        .await
        .unwrap()
    );
    let after = s.get_instance(row.id).await.unwrap().unwrap();
    assert_eq!(after.state, InstanceState::Scheduled);
    assert_eq!(
        after.next_fire_at.map(|t| t.timestamp()),
        Some(retry_at.timestamp())
    );
}

async fn lost_state_cas_changes_nothing_and_enqueues_nothing(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let moved_on = instance(seq, "tenant-a", InstanceState::Completed);
    s.create_instance(&moved_on).await.unwrap();
    let missing = InstanceId::new();
    let cases = [
        ("state already moved", moved_on.id),
        ("instance does not exist", missing),
    ];
    for (case, id) in cases {
        let entry = webhook(id, "instance.failed");
        let won = s
            .conditional_update_instance_state_with_outbox(
                id,
                InstanceState::Running,
                InstanceState::Failed,
                None,
                std::slice::from_ref(&entry),
            )
            .await
            .unwrap();
        assert!(!won, "{case}: CAS must lose");
        assert!(
            s.get_webhook_outbox(entry.id).await.unwrap().is_none(),
            "{case}: outbox row for a transition that never happened"
        );
    }
    assert_eq!(state_of(s, moved_on.id).await, InstanceState::Completed);
}

// ---------------------------------------------------------------------------
// Entitlement admission
// ---------------------------------------------------------------------------

async fn non_terminal_states_count_against_the_admission_ceiling(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let cases = [
        (InstanceState::Scheduled, true),
        (InstanceState::Running, true),
        (InstanceState::Waiting, true),
        (InstanceState::Paused, true),
        (InstanceState::Completed, false),
        (InstanceState::Failed, false),
        (InstanceState::Cancelled, false),
    ];
    for (state, counts) in cases {
        // A dedicated tenant per row so earlier rows never count.
        let tenant = format!("tenant-{state:?}").to_lowercase();
        s.create_instance_admitted(&instance(seq, &tenant, state), 5)
            .await
            .unwrap();

        let result = s
            .create_instance_admitted(&instance(seq, &tenant, InstanceState::Scheduled), 1)
            .await;

        assert_eq!(
            matches!(result, Err(StorageError::QuotaExceeded(_))),
            counts,
            "{state:?}: got {result:?}"
        );
    }
}

async fn admission_ceiling_is_per_tenant(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    s.create_instance_admitted(&instance(seq, "tenant-a", InstanceState::Running), 1)
        .await
        .unwrap();

    let other_tenant = s
        .create_instance_admitted(&instance(seq, "tenant-b", InstanceState::Scheduled), 1)
        .await;
    let same_tenant = s
        .create_instance_admitted(&instance(seq, "tenant-a", InstanceState::Scheduled), 1)
        .await;

    assert!(other_tenant.is_ok(), "got {other_tenant:?}");
    assert!(matches!(same_tenant, Err(StorageError::QuotaExceeded(_))));
}

async fn batch_admission_applies_each_tenants_own_limit(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let limits = HashMap::from([
        (TenantId::unchecked("tenant-a"), 1_u64),
        (TenantId::unchecked("tenant-b"), 2_u64),
    ]);
    let batch = [
        instance(seq, "tenant-a", InstanceState::Scheduled),
        instance(seq, "tenant-b", InstanceState::Scheduled),
        instance(seq, "tenant-b", InstanceState::Scheduled),
    ];

    assert_eq!(
        s.create_instances_batch_admitted(&batch, &limits)
            .await
            .unwrap(),
        3
    );
}

async fn breached_batch_admission_inserts_nothing(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let limits = HashMap::from([(TenantId::unchecked("tenant-a"), 1_u64)]);
    let batch = [
        instance(seq, "tenant-a", InstanceState::Scheduled),
        instance(seq, "tenant-a", InstanceState::Scheduled),
    ];

    let result = s.create_instances_batch_admitted(&batch, &limits).await;

    assert!(
        matches!(result, Err(StorageError::QuotaExceeded(_))),
        "got {result:?}"
    );
    for row in &batch {
        assert!(s.get_instance(row.id).await.unwrap().is_none());
    }
}

async fn batch_admission_denies_a_tenant_missing_from_limits(s: &dyn StorageBackend) {
    let seq = sequence(s).await;
    let limits = HashMap::from([(TenantId::unchecked("tenant-a"), 10_u64)]);
    let batch = [
        instance(seq, "tenant-a", InstanceState::Scheduled),
        instance(seq, "tenant-unlisted", InstanceState::Scheduled),
    ];

    let result = s.create_instances_batch_admitted(&batch, &limits).await;

    assert!(
        matches!(result, Err(StorageError::QuotaExceeded(_))),
        "got {result:?}"
    );
    for row in &batch {
        assert!(s.get_instance(row.id).await.unwrap().is_none());
    }
}

backend_contract!(
    stale_running_instance_is_rescheduled_due_now_and_claimable,
    only_running_instances_past_the_threshold_are_reclaimed,
    reaper_never_touches_parked_scheduled_or_terminal_instances,
    heartbeat_renews_only_running_and_waiting_leases,
    won_state_cas_enqueues_every_outbox_entry,
    won_state_cas_applies_the_requested_fire_time,
    lost_state_cas_changes_nothing_and_enqueues_nothing,
    non_terminal_states_count_against_the_admission_ceiling,
    admission_ceiling_is_per_tenant,
    batch_admission_applies_each_tenants_own_limit,
    breached_batch_admission_inserts_nothing,
    batch_admission_denies_a_tenant_missing_from_limits,
);
