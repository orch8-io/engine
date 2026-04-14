//! Storage-layer bug reproducers — Group B.
//!
//! Covers the storage-side fixes landed alongside the Group B triage:
//!
//!   Issue 1 — `claim_worker_tasks_for_tenant` must never return/claim
//!             another tenant's row. Backed by a JOIN inside the lock
//!             window; tested here with a cross-tenant seed.
//!   Issue 4 — scheduler claim must include `next_fire_at IS NULL` rows
//!             (`SQLite` already did, Postgres did not; `SQLite` serves as
//!             the behavioural oracle the PG fix now matches).
//!
//! Issue 5 (Postgres CTE + `FOR UPDATE SKIP LOCKED` restructure) is a
//! PG-only SQL shape and cannot be exercised against `SqliteStorage`.
//! The `storage_integration` harness runs against PG in CI; a dedicated
//! PG-gated test belongs there, not here.
//!
//! Running only this file:
//!
//! ```text
//! cargo test -p orch8-storage --test bugs_group_b
//! ```

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

fn make_instance(
    tenant: &str,
    state: InstanceState,
    next_fire_at: Option<chrono::DateTime<Utc>>,
) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId(tenant.into()),
        namespace: Namespace("default".into()),
        state,
        next_fire_at,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    }
}

fn make_task(instance_id: InstanceId, handler: &str) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id,
        block_id: BlockId("step_1".into()),
        handler_name: handler.into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// Issue 1 — tenant-scoped claim must not touch another tenant's task.
// ---------------------------------------------------------------------------

/// Two tenants, same handler. A poll scoped to `tenant_a` must leave
/// `tenant_b`'s pending task untouched — neither returned nor moved to
/// `claimed` state. Previously the HTTP layer filtered post-claim, which
/// left the foreign row stuck in `claimed` under the polling worker's id
/// until heartbeat reap.
#[tokio::test]
async fn tenant_claim_ignores_other_tenant_tasks() {
    let s = store().await;

    let inst_a = make_instance("tenant_a", InstanceState::Running, None);
    let inst_b = make_instance("tenant_b", InstanceState::Running, None);
    s.create_instance(&inst_a).await.unwrap();
    s.create_instance(&inst_b).await.unwrap();

    let task_a = make_task(inst_a.id, "http_send");
    let task_b = make_task(inst_b.id, "http_send");
    s.create_worker_task(&task_a).await.unwrap();
    s.create_worker_task(&task_b).await.unwrap();

    // tenant_a poll — must not see task_b.
    let claimed = s
        .claim_worker_tasks_for_tenant("http_send", "w-a", &TenantId("tenant_a".into()), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "only tenant_a's task should be returned");
    assert_eq!(claimed[0].id, task_a.id);

    // tenant_b's task must still be pending (untouched), so a tenant_b poll
    // can still claim it. This is the critical assertion — a post-claim
    // filter would have left task_b in `claimed` with w-a as the owner.
    let b_fetched = s.get_worker_task(task_b.id).await.unwrap().unwrap();
    assert_eq!(b_fetched.state, WorkerTaskState::Pending);
    assert!(b_fetched.worker_id.is_none());

    let b_claimed = s
        .claim_worker_tasks_for_tenant("http_send", "w-b", &TenantId("tenant_b".into()), 10)
        .await
        .unwrap();
    assert_eq!(b_claimed.len(), 1);
    assert_eq!(b_claimed[0].id, task_b.id);
}

/// Same guarantee for the queue-scoped variant — tenant tag must live
/// inside the lock window, not be applied post-hoc.
#[tokio::test]
async fn tenant_queue_claim_ignores_other_tenant_tasks() {
    let s = store().await;

    let inst_a = make_instance("tenant_a", InstanceState::Running, None);
    let inst_b = make_instance("tenant_b", InstanceState::Running, None);
    s.create_instance(&inst_a).await.unwrap();
    s.create_instance(&inst_b).await.unwrap();

    let mut task_a = make_task(inst_a.id, "email_send");
    task_a.queue_name = Some("priority".into());
    let mut task_b = make_task(inst_b.id, "email_send");
    task_b.queue_name = Some("priority".into());
    s.create_worker_task(&task_a).await.unwrap();
    s.create_worker_task(&task_b).await.unwrap();

    let claimed = s
        .claim_worker_tasks_from_queue_for_tenant(
            "priority",
            "email_send",
            "w-a",
            &TenantId("tenant_a".into()),
            10,
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, task_a.id);

    let b_fetched = s.get_worker_task(task_b.id).await.unwrap().unwrap();
    assert_eq!(b_fetched.state, WorkerTaskState::Pending);
}

// ---------------------------------------------------------------------------
// Issue 4 — scheduler claim must include next_fire_at IS NULL rows.
// ---------------------------------------------------------------------------

/// An instance created without a `next_fire_at` (legacy or API-created row
/// with state=`Scheduled` and no fire time) must still be claimable by the
/// scheduler. Previously the Postgres predicate was `next_fire_at <= now`
/// which silently excluded NULL rows; `SQLite` always used
/// `next_fire_at IS NULL OR next_fire_at <= now`. This test pins the
/// behavioural contract so the two backends stay aligned.
#[tokio::test]
async fn claim_due_instances_includes_null_next_fire_at() {
    let s = store().await;

    // Row 1: explicit past next_fire_at — clearly due.
    let past = make_instance(
        "t",
        InstanceState::Scheduled,
        Some(Utc::now() - Duration::seconds(30)),
    );
    // Row 2: NULL next_fire_at — the regression target.
    let null_fire = make_instance("t", InstanceState::Scheduled, None);

    s.create_instance(&past).await.unwrap();
    s.create_instance(&null_fire).await.unwrap();

    let claimed = s.claim_due_instances(Utc::now(), 10, 0).await.unwrap();
    let ids: Vec<InstanceId> = claimed.iter().map(|i| i.id).collect();

    assert!(
        ids.contains(&past.id),
        "instance with past next_fire_at should be claimed"
    );
    assert!(
        ids.contains(&null_fire.id),
        "instance with NULL next_fire_at should be claimed (matches `SQLite`/PG parity)"
    );
}

/// Same inclusion rule must hold on the per-tenant-capped path — the
/// `max_per_tenant > 0` branch is a separate SQL query and could easily
/// drift from the no-cap path. Seed several NULL-fire rows for one tenant
/// and assert the cap still observes them.
#[tokio::test]
async fn claim_due_instances_null_fire_observed_under_tenant_cap() {
    let s = store().await;

    for _ in 0..3 {
        let inst = make_instance("t", InstanceState::Scheduled, None);
        s.create_instance(&inst).await.unwrap();
    }

    // Cap=2 → only 2 out of 3 should come back.
    let claimed = s.claim_due_instances(Utc::now(), 10, 2).await.unwrap();
    assert_eq!(
        claimed.len(),
        2,
        "tenant cap must apply to NULL-fire rows just like past-fire rows"
    );
}
