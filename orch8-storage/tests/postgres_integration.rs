//! Integration tests that require a live Postgres instance.
//!
//! Gated on `DATABASE_URL` being set (as CI's `test` job always sets it,
//! pointing at the service container it provisions). Locally, these tests
//! are skipped with a message if `DATABASE_URL` is absent rather than
//! failing the run for contributors without a local Postgres.
//!
//! Each test runs against its own freshly-migrated schema in the same
//! database (`DATABASE_URL`'s database), using a unique tenant/resource
//! namespace per test so tests can run concurrently without interfering.

use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;

use orch8_storage::postgres::PostgresStorage;
use orch8_storage::{InstanceStore, MobileSyncStore, SchedulingStore, SequenceStore, WorkerStore};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::ids::{BlockId, InstanceId, Namespace, ResourceKey, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

/// Returns `None` (test should skip) if `DATABASE_URL` isn't set.
async fn store() -> Option<PostgresStorage> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let storage = PostgresStorage::new(&url, 5, None)
        .await
        .expect("connect to DATABASE_URL");
    storage.run_migrations().await.expect("run migrations");
    Some(storage)
}

macro_rules! require_postgres {
    () => {
        match store().await {
            Some(s) => s,
            None => {
                eprintln!("skipping: DATABASE_URL not set");
                return;
            }
        }
    };
}

fn mk_sequence(tenant: &str, seq_id: SequenceId) -> SequenceDefinition {
    SequenceDefinition {
        id: seq_id,
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        name: format!("seq-{seq_id}"),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    }
}

fn mk_instance(tenant: &str, seq_id: SequenceId, concurrency_key: Option<&str>) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext {
            data: serde_json::json!({}),
            config: serde_json::json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        },
        concurrency_key: concurrency_key.map(String::from),
        max_concurrency: concurrency_key.map(|_| 5),
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

/// #14: the rate-limit check must be genuinely atomic (single locked
/// statement) rather than an UPDATE followed by a racy fallback SELECT.
/// This doesn't reproduce the race directly (that needs precise
/// interleaving), but it pins the still-correct decision + `retry_after`
/// shape across the reset boundary, which the rewrite must preserve.
#[tokio::test]
async fn rate_limit_check_is_atomic_and_correct_across_window_reset() {
    let s = require_postgres!();
    let tenant = TenantId::unchecked(format!("t-ratelimit-{}", Uuid::new_v4()));
    let key = ResourceKey::new("api:endpoint");
    let now = Utc::now();

    s.upsert_rate_limit(&RateLimit {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        resource_key: key.clone(),
        max_count: 2,
        window_seconds: 60,
        current_count: 0,
        window_start: now,
    })
    .await
    .unwrap();

    assert!(matches!(
        s.check_rate_limit(&tenant, &key, now).await.unwrap(),
        RateLimitCheck::Allowed
    ));
    assert!(matches!(
        s.check_rate_limit(&tenant, &key, now).await.unwrap(),
        RateLimitCheck::Allowed
    ));
    // Third request within the window must be denied.
    match s.check_rate_limit(&tenant, &key, now).await.unwrap() {
        RateLimitCheck::Exceeded { retry_after } => {
            let expected = now + chrono::Duration::seconds(60);
            let diff = (retry_after - expected).num_milliseconds().abs();
            assert!(
                diff < 1000,
                "retry_after should match the window that denied this request"
            );
        }
        RateLimitCheck::Allowed => panic!("third request must be denied"),
    }

    // After the window elapses, a request must be allowed again (fresh window).
    let after_window = now + chrono::Duration::seconds(61);
    assert!(matches!(
        s.check_rate_limit(&tenant, &key, after_window)
            .await
            .unwrap(),
        RateLimitCheck::Allowed
    ));
}

/// #14: many concurrent callers hitting the same (tenant, resource) must
/// never admit more than `max_count` within one window -- the atomic
/// lock-decide-write query must serialize them correctly.
#[tokio::test]
async fn rate_limit_check_admits_exactly_max_count_under_concurrency() {
    let s = Arc::new(require_postgres!());
    let tenant = TenantId::unchecked(format!("t-ratelimit-conc-{}", Uuid::new_v4()));
    let key = ResourceKey::new("api:endpoint");
    let now = Utc::now();

    s.upsert_rate_limit(&RateLimit {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        resource_key: key.clone(),
        max_count: 5,
        window_seconds: 60,
        current_count: 0,
        window_start: now,
    })
    .await
    .unwrap();

    let mut handles = Vec::new();
    for _ in 0..20 {
        let s = Arc::clone(&s);
        let tenant = tenant.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            s.check_rate_limit(&tenant, &key, now).await.unwrap()
        }));
    }
    let mut allowed = 0;
    for h in handles {
        if matches!(h.await.unwrap(), RateLimitCheck::Allowed) {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 5, "exactly max_count requests must be admitted");
}

/// #7: `claim_for_tenant`'s `FOR UPDATE OF wt SKIP LOCKED` must lock only the
/// `worker_tasks` row, not the joined `task_instances` row -- otherwise a
/// tenant-scoped worker poll in flight would make the scheduler's
/// `claim_due_instances` (its own `SKIP LOCKED` claim) skip that instance.
///
/// This test holds open a transaction running the *same* locking clause
/// `claim_for_tenant` uses, rather than calling `claim_worker_tasks_for_tenant`
/// directly: that function is a single autocommitting UPDATE, so its lock is
/// held for microseconds -- far too short a window to reliably interleave
/// with a concurrent claim in a test. Holding an equivalent query open lets
/// us assert the semantic guarantee (`FOR UPDATE OF wt` doesn't lock `ti`)
/// deterministically. `source_pins_for_update_of_wt_in_tenant_claim_queries`
/// below is the complementary guard that catches a regression to the actual
/// shipped query text, since this test's hardcoded copy wouldn't.
#[tokio::test]
async fn tenant_worker_claim_does_not_lock_task_instances_row() {
    let s = require_postgres!();
    let tenant = format!("t-claim-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();

    let instance = mk_instance(&tenant, seq_id, None);
    s.create_instance(&instance).await.unwrap();

    let task = WorkerTask {
        id: Uuid::new_v4(),
        instance_id: instance.id,
        block_id: BlockId::new("step_1"),
        handler_name: "http_call".into(),
        queue_name: None,
        params: serde_json::json!({}),
        context: serde_json::json!({}),
        attempt: 1,
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
    };
    s.create_worker_task(&task).await.unwrap();

    // Hold a transaction open that claims the worker task for this tenant,
    // simulating an in-flight tenant-scoped poll.
    let mut tx = s.pool().begin().await.unwrap();
    sqlx::query(
        r"UPDATE worker_tasks
          SET state = 'claimed', worker_id = $2, claimed_at = NOW(), heartbeat_at = NOW()
          WHERE id IN (
              SELECT wt.id FROM worker_tasks wt
              JOIN task_instances ti ON ti.id = wt.instance_id
              WHERE wt.handler_name = $1
                AND wt.state = 'pending'
                AND ti.tenant_id = $3
              FOR UPDATE OF wt SKIP LOCKED
          )",
    )
    .bind("http_call")
    .bind("worker-1")
    .bind(&tenant)
    .execute(&mut *tx)
    .await
    .unwrap();

    // While that transaction is still open (task_instances row must NOT be
    // locked by it), the scheduler's claim must still be able to pick up the
    // instance.
    let claimed = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.claim_due_instances(Utc::now(), 10, 0),
    )
    .await
    .expect("claim_due_instances must not block on the worker-claim transaction")
    .unwrap();

    assert!(
        claimed.iter().any(|i| i.id == instance.id),
        "task_instances row must not have been locked by the worker-task claim"
    );

    tx.rollback().await.unwrap();
}

/// Cheap, deterministic guard for the actual shipped SQL: pins that both
/// tenant-scoped worker-claim queries lock only `wt` (`worker_tasks`), not
/// the joined `task_instances` row. Complements
/// `tenant_worker_claim_does_not_lock_task_instances_row`, which verifies the
/// *semantic* consequence using a hardcoded copy of this same clause (a
/// hardcoded copy can't detect if the real source regresses).
#[test]
fn source_pins_for_update_of_wt_in_tenant_claim_queries() {
    let workers_rs = include_str!("../src/postgres/workers.rs");
    let misc_rs = include_str!("../src/postgres/misc.rs");
    assert!(
        workers_rs.contains("FOR UPDATE OF wt SKIP LOCKED"),
        "postgres/workers.rs::claim_for_tenant must lock only `wt`, not the joined task_instances row"
    );
    assert!(
        misc_rs.contains("FOR UPDATE OF wt SKIP LOCKED"),
        "postgres/misc.rs::claim_worker_tasks_from_queue_for_tenant must lock only `wt`, not the joined task_instances row"
    );
}

/// #15: mobile-sync timestamps must round-trip with sub-second precision and
/// an explicit UTC offset, not truncate to whole seconds with no timezone
/// marker (which made same-second updates across a sync boundary
/// indistinguishable, and made the client-side "changed since last sync"
/// comparison ambiguous about which timezone the string was even in).
#[tokio::test]
async fn mobile_device_timestamps_are_rfc3339_with_fractional_seconds() {
    use orch8_storage::MobileDevice;

    let s = require_postgres!();
    let device_id = format!("device-{}", Uuid::new_v4());
    s.register_mobile_device(&MobileDevice {
        device_id: device_id.clone(),
        tenant_id: "t1".into(),
        push_token: None,
        platform: "ios".into(),
        app_version: Some("1.0.0".into()),
        active: true,
        last_sync_at: None,
        registered_at: String::new(),
    })
    .await
    .unwrap();

    let device = s
        .get_mobile_device(&device_id)
        .await
        .unwrap()
        .expect("device must exist after registration");

    // Must parse as RFC 3339 (proves fractional seconds + offset are present
    // and well-formed, not just "looks like it has a T and Z").
    let parsed = chrono::DateTime::parse_from_rfc3339(&device.registered_at).unwrap_or_else(|e| {
        panic!(
            "registered_at '{}' is not RFC 3339: {e}",
            device.registered_at
        )
    });

    // Sub-second precision must survive: the raw string must carry fractional
    // digits, not just HH:MM:SS.
    assert!(
        device.registered_at.contains('.'),
        "registered_at '{}' must include fractional seconds",
        device.registered_at
    );
    // Explicit UTC offset marker must be present.
    assert!(
        device.registered_at.ends_with('Z'),
        "registered_at '{}' must carry an explicit UTC offset",
        device.registered_at
    );
    assert_eq!(parsed.timezone().local_minus_utc(), 0);
}

/// C-1: `heartbeat_instance` must renew a `Running` instance's lease
/// (`updated_at`) so `recover_stale_instances` does not reclaim a step that
/// is still genuinely executing, and must be a no-op for a terminal
/// instance (a stray heartbeat racing its own completion must not resurrect
/// its `updated_at`).
#[tokio::test]
async fn heartbeat_instance_renews_lease_on_postgres() {
    let s = require_postgres!();
    let tenant = format!("t-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();

    let mut running = mk_instance(&tenant, seq_id, None);
    running.state = InstanceState::Running;
    running.updated_at = Utc::now() - chrono::Duration::hours(1);
    s.create_instance(&running).await.unwrap();

    s.heartbeat_instance(running.id).await.unwrap();

    // Assert on this instance specifically rather than the reaper's global
    // recovered-count: this integration suite runs against a shared,
    // non-truncated database, so unrelated leftover stale rows from other
    // tests could otherwise make the count non-deterministic.
    s.recover_stale_instances(std::time::Duration::from_secs(300))
        .await
        .unwrap();
    let refreshed = s.get_instance(running.id).await.unwrap().unwrap();
    assert_eq!(
        refreshed.state,
        InstanceState::Running,
        "a heartbeated instance must not be reclaimed as stale"
    );

    let mut done = mk_instance(&tenant, seq_id, None);
    done.state = InstanceState::Completed;
    done.updated_at = Utc::now() - chrono::Duration::hours(1);
    s.create_instance(&done).await.unwrap();

    s.heartbeat_instance(done.id).await.unwrap();
    let refreshed_done = s.get_instance(done.id).await.unwrap().unwrap();
    assert!(
        refreshed_done.updated_at < Utc::now() - chrono::Duration::minutes(30),
        "heartbeat must not touch a non-running/waiting instance"
    );
}
