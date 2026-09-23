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

use orch8_storage::conformance::run_core_conformance;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::{
    AdminStore, ExecutionTreeStore, InstanceStore, MobileSyncStore, OutputStore, ResourceStore,
    SchedulingStore, SequenceStore, TelemetryStore, UsageEvent, WorkerStore,
};
use orch8_types::cluster::{ClusterNode, NodeStatus};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::continuity::{
    CapsuleRequirements, RuntimeCapabilities, RuntimeConnectivity, RuntimeId, RuntimeKind,
    RuntimeTrustLevel,
};
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{
    BlockId, ExecutionNodeId, InstanceId, Namespace, ResourceKey, SequenceId, TenantId,
};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use orch8_types::session::{Session, SessionState};
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};
use orch8_types::worker::{WorkerClaim, WorkerTask, WorkerTaskState};

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

#[tokio::test]
async fn postgres_passes_public_core_conformance() {
    let storage = require_postgres!();
    let report = run_core_conformance(&storage)
        .await
        .expect("Postgres must satisfy the scheduler storage contract");
    assert_eq!(report.checks.len(), 9);
}

#[tokio::test]
async fn postgres_unknown_dispatch_mode_is_not_treated_as_poll() {
    let storage = require_postgres!();
    let tenant = format!("dispatch-mode-{}", Uuid::new_v4());
    let now = Utc::now();
    storage
        .upsert_queue_dispatch(&QueueDispatchConfig {
            tenant_id: tenant.clone(),
            queue_name: "jobs".into(),
            mode: DispatchMode::Push,
            push_url: Some("https://example.invalid/push".into()),
            secret: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE queue_dispatch SET mode = 'future-mode' WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .unwrap();

    for result in [
        storage
            .get_queue_dispatch(&tenant, "jobs")
            .await
            .map(|_| ()),
        storage.list_queue_dispatch(Some(&tenant)).await.map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(orch8_types::error::StorageError::Query(message)) if message.contains("future-mode")
        ));
    }
    pool.close().await;
}

#[tokio::test]
async fn postgres_unknown_session_state_is_not_treated_as_active() {
    let storage = require_postgres!();
    let now = Utc::now();
    let session = Session {
        id: Uuid::now_v7(),
        tenant_id: TenantId::unchecked(format!("session-state-{}", Uuid::new_v4())),
        session_key: "key".into(),
        data: serde_json::json!({}),
        state: SessionState::Active,
        created_at: now,
        updated_at: now,
        expires_at: None,
    };
    storage.create_session(&session).await.unwrap();
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE sessions SET state = 'future-terminal' WHERE id = $1")
        .bind(session.id)
        .execute(&pool)
        .await
        .unwrap();

    for result in [
        storage.get_session(session.id).await,
        storage
            .get_session_by_key(&session.tenant_id, &session.session_key)
            .await,
    ] {
        assert!(matches!(
            result,
            Err(orch8_types::error::StorageError::Query(message)) if message.contains("future-terminal")
        ));
    }
    pool.close().await;
}

#[tokio::test]
async fn postgres_unknown_cluster_node_status_is_not_active() {
    let storage = require_postgres!();
    let now = Utc::now();
    let node = ClusterNode {
        id: Uuid::new_v4(),
        name: format!("status-test-{}", Uuid::new_v4()),
        status: NodeStatus::Active,
        registered_at: now,
        last_heartbeat_at: now,
        drain: false,
        drain_started_at: None,
        stopped_at: None,
        capabilities_withdrawn: false,
        execution_handoff_evidence: None,
    };
    storage.register_node(&node).await.unwrap();
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE cluster_nodes SET status = 'future-stopped' WHERE id = $1")
        .bind(node.id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        storage.list_nodes().await,
        Err(orch8_types::error::StorageError::Query(message)) if message.contains("future-stopped")
    ));
    pool.close().await;
}

#[tokio::test]
async fn postgres_usage_rejects_negative_token_counts_without_writes() {
    let storage = require_postgres!();
    let now = Utc::now();
    let mut event = UsageEvent {
        tenant_id: format!("usage-negative-{}", Uuid::new_v4()),
        instance_id: None,
        block_id: None,
        kind: "llm_tokens".into(),
        model: "model".into(),
        input_tokens: 0,
        output_tokens: 0,
        created_at: now,
    };
    for (input, output) in [(-1, 0), (0, -1), (-1, -1)] {
        event.input_tokens = input;
        event.output_tokens = output;
        assert!(matches!(
            storage.record_usage_event(&event).await,
            Err(orch8_types::error::StorageError::Constraint(_))
        ));
    }
    let start = now - chrono::Duration::seconds(1);
    let end = now + chrono::Duration::seconds(1);
    assert!(
        storage
            .query_usage(&event.tenant_id, start, end)
            .await
            .unwrap()
            .is_empty()
    );

    event.input_tokens = 0;
    event.output_tokens = 0;
    storage.record_usage_event(&event).await.unwrap();
    let totals = storage
        .query_usage(&event.tenant_id, start, end)
        .await
        .unwrap();
    assert_eq!(totals.len(), 1);
    assert_eq!((totals[0].input_tokens, totals[0].output_tokens), (0, 0));
}

#[tokio::test]
async fn postgres_usage_window_matches_fractional_boundary_contract() {
    let storage = require_postgres!();
    let tenant = format!("usage-window-{}", Uuid::new_v4());
    let timestamp = |value: &str| {
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    };
    for (model, at) in [
        ("whole", "2026-09-23T10:00:00Z"),
        ("before", "2026-09-23T10:00:00.099999Z"),
        ("start", "2026-09-23T10:00:00.100Z"),
        ("inside", "2026-09-23T10:00:00.100001Z"),
        ("last", "2026-09-23T10:00:00.199999Z"),
        ("end", "2026-09-23T10:00:00.200Z"),
        ("next", "2026-09-23T10:00:01Z"),
    ] {
        storage
            .record_usage_event(&UsageEvent {
                tenant_id: tenant.clone(),
                instance_id: None,
                block_id: None,
                kind: "llm_tokens".into(),
                model: model.into(),
                input_tokens: 1,
                output_tokens: 0,
                created_at: timestamp(at),
            })
            .await
            .unwrap();
    }
    let models = |usage: Vec<orch8_storage::UsageAggregate>| {
        usage.into_iter().map(|row| row.model).collect::<Vec<_>>()
    };
    let middle = storage
        .query_usage(
            &tenant,
            timestamp("2026-09-23T10:00:00.100Z"),
            timestamp("2026-09-23T10:00:00.200Z"),
        )
        .await
        .unwrap();
    assert_eq!(models(middle), ["inside", "last", "start"]);
    let first = storage
        .query_usage(
            &tenant,
            timestamp("2026-09-23T10:00:00Z"),
            timestamp("2026-09-23T10:00:00.100Z"),
        )
        .await
        .unwrap();
    assert_eq!(models(first), ["before", "whole"]);

    // The stored timestamps are on PostgreSQL's microsecond grid, but callers
    // can provide nanosecond bounds. A bound just after `start` must exclude
    // that row; the same bound as an exclusive end must include it.
    let after_start = timestamp("2026-09-23T10:00:00.100000001Z");
    let late = storage
        .query_usage(&tenant, after_start, timestamp("2026-09-23T10:00:00.200Z"))
        .await
        .unwrap();
    assert_eq!(models(late), ["inside", "last"]);
    let early = storage
        .query_usage(&tenant, timestamp("2026-09-23T10:00:00Z"), after_start)
        .await
        .unwrap();
    assert_eq!(models(early), ["before", "start", "whole"]);
}

#[tokio::test]
async fn postgres_memory_batch_deletes_handle_more_than_bind_limit() {
    let storage = require_postgres!();
    let tenant = format!("t-memory-batch-{}", Uuid::new_v4());
    let other_tenant = format!("t-memory-batch-other-{}", Uuid::new_v4());
    let sequence_id = SequenceId::new();
    storage
        .create_sequence(&mk_sequence(&tenant, sequence_id))
        .await
        .unwrap();
    let target = mk_instance(&tenant, sequence_id, None);
    let other = mk_instance(&tenant, sequence_id, None);
    storage.create_instance(&target).await.unwrap();
    storage.create_instance(&other).await.unwrap();

    let keys: Vec<String> = (0..65_536).map(|index| format!("memory-{index}")).collect();
    for key in [&keys[0], &keys[65_535]] {
        storage
            .set_instance_kv(target.id, key, &serde_json::json!(true))
            .await
            .unwrap();
        storage
            .set_instance_kv(other.id, key, &serde_json::json!(true))
            .await
            .unwrap();
        storage
            .set_shared_knowledge(&tenant, "ns", key, &serde_json::json!(true))
            .await
            .unwrap();
        storage
            .set_shared_knowledge(&other_tenant, "ns", key, &serde_json::json!(true))
            .await
            .unwrap();
    }

    storage
        .delete_instance_kv_batch(target.id, &keys)
        .await
        .unwrap();
    storage
        .delete_shared_knowledge_batch(&tenant, "ns", &keys)
        .await
        .unwrap();

    for key in [&keys[0], &keys[65_535]] {
        assert_eq!(storage.get_instance_kv(target.id, key).await.unwrap(), None);
        assert_eq!(
            storage.get_instance_kv(other.id, key).await.unwrap(),
            Some(serde_json::json!(true))
        );
        assert_eq!(
            storage
                .get_shared_knowledge(&tenant, "ns", key)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .get_shared_knowledge(&other_tenant, "ns", key)
                .await
                .unwrap(),
            Some(serde_json::json!(true))
        );
    }
}

fn mk_sequence(tenant: &str, seq_id: SequenceId) -> SequenceDefinition {
    SequenceDefinition {
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
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

#[tokio::test]
async fn postgres_max_concurrency_saturates_and_rejects_invalid_storage() {
    let storage = require_postgres!();
    let tenant = format!("max-concurrency-{}", Uuid::new_v4());
    let sequence_id = SequenceId::new();
    storage
        .create_sequence(&mk_sequence(&tenant, sequence_id))
        .await
        .unwrap();
    let mut inst = mk_instance(&tenant, sequence_id, Some("key"));
    inst.max_concurrency = Some(u32::MAX);
    storage.create_instance(&inst).await.unwrap();
    assert_eq!(
        storage
            .get_instance(inst.id)
            .await
            .unwrap()
            .unwrap()
            .max_concurrency,
        Some(i32::MAX.unsigned_abs())
    );

    inst.id = InstanceId::new();
    storage
        .create_instances_batch(&[inst.clone()])
        .await
        .unwrap();
    assert_eq!(
        storage
            .get_instance(inst.id)
            .await
            .unwrap()
            .unwrap()
            .max_concurrency,
        Some(i32::MAX.unsigned_abs())
    );

    sqlx::query("UPDATE task_instances SET max_concurrency = -1 WHERE id = $1")
        .bind(inst.id.into_uuid())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(
        matches!(storage.get_instance(inst.id).await, Err(orch8_types::error::StorageError::Query(message)) if message.contains("max_concurrency"))
    );
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
/// `FOR UPDATE SKIP LOCKED` claim skip that instance.
///
/// This test holds open a transaction running the *same* locking clause
/// `claim_for_tenant` uses, rather than calling `claim_worker_tasks_for_tenant`
/// directly: that function is a single autocommitting UPDATE, so its lock is
/// held for microseconds -- far too short a window to reliably interleave
/// with a concurrent claim in a test. Holding an equivalent query open lets
/// us assert the semantic guarantee (`FOR UPDATE OF wt` doesn't lock `ti`)
/// deterministically. The target row is probed by ID so unrelated scheduled
/// rows from other integration tests cannot consume a scheduler claim limit.
/// `source_pins_for_update_of_wt_in_tenant_claim_queries`
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
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({}),
        context: serde_json::json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
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

    // The scheduler's claim uses FOR UPDATE SKIP LOCKED on task_instances.
    // Probe only this instance so the assertion is independent of other tests.
    let claimed: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM task_instances WHERE id = $1 FOR UPDATE SKIP LOCKED")
            .bind(instance.id.into_uuid())
            .fetch_optional(s.pool())
            .await
            .unwrap();
    assert_eq!(
        claimed.map(|(id,)| id),
        Some(instance.id.into_uuid()),
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

#[tokio::test]
async fn capability_aware_postgres_claim_skips_incompatible_work_atomically() {
    let s = require_postgres!();
    let tenant = format!("t-capability-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let instance = mk_instance(&tenant, seq_id, None);
    s.create_instance(&instance).await.unwrap();
    let now = Utc::now();
    let incompatible_id = Uuid::new_v4();
    let compatible_id = Uuid::new_v4();

    for (id, block, region) in [
        (incompatible_id, "wrong-region", "brazil"),
        (compatible_id, "compatible", "norway"),
    ] {
        s.create_worker_task(&WorkerTask {
            id,
            instance_id: instance.id,
            block_id: BlockId::new(block),
            handler_name: "render".into(),
            queue_name: Some("gpu".into()),
            requirements: CapsuleRequirements {
                handlers: vec!["render".into()],
                regions: vec![region.into()],
                hardware: vec!["cuda".into()],
                requires_network: true,
                ..Default::default()
            },
            params: serde_json::json!({"block": block}),
            context: serde_json::json!({}),
            attempt: 1,
            timeout_ms: None,
            state: WorkerTaskState::Pending,
            worker_id: None,
            claimed_at: None,
            heartbeat_at: None,
            claim_epoch: 0,
            resume_checkpoint: None,
            checkpoint_seq: 0,
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: now,
        })
        .await
        .unwrap();
    }

    let capabilities = RuntimeCapabilities {
        runtime_id: RuntimeId::new(),
        kind: RuntimeKind::Desktop,
        trust: RuntimeTrustLevel::Registered,
        handlers: vec!["render".into()],
        plugins: Vec::new(),
        credentials: Vec::new(),
        regions: vec!["norway".into()],
        hardware: vec!["cuda".into()],
        offline_capable: false,
        connectivity: Some(RuntimeConnectivity::Ethernet),
        battery_percent: None,
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: None,
        observed_at: now,
        expires_at: now + chrono::Duration::minutes(5),
    };
    let claimed = s
        .claim_worker_tasks_matching(
            "render",
            "gpu-worker",
            Some(&TenantId::unchecked(&tenant)),
            Some("gpu"),
            &capabilities,
            1,
        )
        .await
        .unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].block_id.as_str(), "compatible");
    assert_eq!(claimed[0].claim_epoch, 1);
    let incompatible = s.get_worker_task(incompatible_id).await.unwrap().unwrap();
    assert_eq!(incompatible.state, WorkerTaskState::Pending);
}

#[tokio::test]
async fn worker_claim_epoch_fences_restarted_postgres_worker() {
    let s = require_postgres!();
    let tenant = format!("t-worker-fence-{}", Uuid::new_v4());
    let handler = format!("fenced_handler-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let instance = mk_instance(&tenant, seq_id, None);
    s.create_instance(&instance).await.unwrap();

    let task = WorkerTask {
        id: Uuid::new_v4(),
        instance_id: instance.id,
        block_id: BlockId::new("fenced_step"),
        handler_name: handler.clone(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({}),
        context: serde_json::json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task).await.unwrap();

    let first = s
        .claim_worker_tasks(&handler, "stable-worker", 1)
        .await
        .unwrap();
    assert_eq!(first[0].claim_epoch, 1);
    s.reap_stale_worker_tasks(std::time::Duration::ZERO)
        .await
        .unwrap();
    let replacement = s
        .claim_worker_tasks(&handler, "stable-worker", 1)
        .await
        .unwrap();
    assert_eq!(replacement[0].claim_epoch, 2);

    assert!(
        !s.heartbeat_worker_task(task.id, &WorkerClaim::new("stable-worker", 1))
            .await
            .unwrap(),
        "generation 1 must be fenced after reclaim"
    );
    assert!(
        s.heartbeat_worker_task(task.id, &WorkerClaim::new("stable-worker", 2))
            .await
            .unwrap(),
        "generation 2 must retain the lease"
    );
    let evidence = s
        .list_worker_task_attempt_events(task.id, 10)
        .await
        .unwrap();
    assert_eq!(
        evidence
            .iter()
            .map(|event| event.event.as_str())
            .collect::<Vec<_>>(),
        ["claimed", "reclaimed", "claimed"]
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

/// Regression test: `postgres::externalized::batch_save` used to build each
/// bulk INSERT with `QueryBuilder::new("... VALUES ")` and then call
/// `push_values(...)`, which *also* emits its own `VALUES` clause — producing
/// a literal `VALUES VALUES (...)` and a Postgres syntax error on every
/// non-empty call, on both the compressed and inline branches. Covers both
/// branches (a large payload crosses the zstd compression threshold, a small
/// one stays inline) in a single batch.
#[tokio::test]
async fn batch_save_externalized_state_mixed_sizes_does_not_hit_sql_syntax_error() {
    let s = require_postgres!();
    let tenant = format!("t-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let inst = mk_instance(&tenant, seq_id, None);
    s.create_instance(&inst).await.unwrap();

    let suffix = Uuid::new_v4();
    let small_key = format!("bs-small-{suffix}");
    let large_key = format!("bs-large-{suffix}");
    let entries = vec![
        (small_key.clone(), serde_json::json!({"n": 1})),
        (
            large_key.clone(),
            serde_json::json!({"blob": "x".repeat(5_000)}),
        ),
    ];

    InstanceStore::batch_save_externalized_state(&s, inst.id, &entries)
        .await
        .expect("batch_save_externalized_state must not hit a SQL syntax error");

    let fetched = s
        .batch_get_externalized_state(&[(inst.id, small_key.clone()), (inst.id, large_key.clone())])
        .await
        .unwrap();
    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[&(inst.id, small_key.clone())], entries[0].1);
    assert_eq!(fetched[&(inst.id, large_key.clone())], entries[1].1);

    // STO-N1: another instance naming these refs gets nothing.
    let other = orch8_types::ids::InstanceId::new();
    assert!(
        s.get_externalized_state(other, &small_key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        s.batch_get_externalized_state(&[(other, large_key.clone())])
            .await
            .unwrap()
            .is_empty()
    );
}

/// Regression test for H2 (`STORAGE_REFACTORING_2026-07.md`): Postgres's
/// `update_node_state` / `update_nodes_state` bound `started_at =
/// COALESCE($4, started_at)` with `$4` always `Some(now)` on a Running
/// transition, so COALESCE always picked the new value — overwriting the
/// original start time on every Waiting -> Running re-dispatch. `SQLite` was
/// already correct (see `review_fixes_2026_06.rs`); this covers the PG side.
#[tokio::test]
async fn update_node_state_preserves_started_at_on_redispatch_postgres() {
    let s = require_postgres!();
    let tenant = format!("t-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let inst = mk_instance(&tenant, seq_id, None);
    s.create_instance(&inst).await.unwrap();

    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: inst.id,
        block_id: BlockId::new("s1"),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };
    s.create_execution_nodes_batch(std::slice::from_ref(&node))
        .await
        .unwrap();

    s.update_node_state(node.id, NodeState::Running)
        .await
        .unwrap();
    let first = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .expect("first Running transition sets started_at");

    s.update_node_state(node.id, NodeState::Waiting)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    s.update_node_state(node.id, NodeState::Running)
        .await
        .unwrap();

    let second = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .unwrap();
    assert_eq!(
        first, second,
        "re-dispatch must not overwrite the original started_at on Postgres"
    );
}

#[tokio::test]
async fn batch_node_updates_preserve_started_at_postgres() {
    let s = require_postgres!();
    let tenant = format!("t-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let inst = mk_instance(&tenant, seq_id, None);
    s.create_instance(&inst).await.unwrap();

    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: inst.id,
        block_id: BlockId::new("s1"),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };
    s.create_execution_nodes_batch(std::slice::from_ref(&node))
        .await
        .unwrap();

    s.update_nodes_state(&[node.id], NodeState::Running)
        .await
        .unwrap();
    let first = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .expect("batch Running transition sets started_at");

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    s.update_nodes_state(&[node.id], NodeState::Running)
        .await
        .unwrap();
    let second = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .unwrap();
    assert_eq!(
        first, second,
        "batch re-transition must keep started_at on Postgres"
    );
}

/// Postgres parity for the `SQLite` test of the same name: internal
/// bookkeeping rows (`__retry__`, `__in_progress__`) are not completion
/// evidence. The sentinel exclusion is what makes crash recovery re-execute
/// a crashed step instead of skipping it (at-least-once).
#[tokio::test]
async fn completed_block_ids_exclude_internal_markers() {
    use orch8_types::output::BlockOutput;

    let s = require_postgres!();
    let tenant = format!("t-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let inst = mk_instance(&tenant, seq_id, None);
    s.create_instance(&inst).await.unwrap();

    let mk = |block: &str, attempt: u16, output_ref: Option<&str>| BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        block_id: BlockId::new(block),
        output: serde_json::json!({}),
        output_ref: output_ref.map(str::to_owned),
        output_size: 2,
        attempt,
        created_at: Utc::now(),
    };

    s.save_block_output(&mk("real", 0, None)).await.unwrap();
    s.save_block_output(&mk("retrying", 1, Some("__retry__")))
        .await
        .unwrap();
    s.save_block_output(&mk("crashed", 0, Some("__in_progress__")))
        .await
        .unwrap();

    let ids = s.get_completed_block_ids(inst.id).await.unwrap();
    assert_eq!(ids.len(), 1, "only the real output counts, got {ids:?}");
    assert_eq!(ids[0].as_str(), "real");

    let batch = s.get_completed_block_ids_batch(&[inst.id]).await.unwrap();
    let batch_ids = batch.get(&inst.id).map_or(&[][..], Vec::as_slice);
    assert_eq!(batch_ids.len(), 1, "batch: {batch_ids:?}");
    assert_eq!(batch_ids[0].as_str(), "real");
}

#[tokio::test]
async fn outputs_with_equal_timestamps_use_id_tiebreaker_postgres() {
    use orch8_types::output::BlockOutput;

    let s = require_postgres!();
    let tenant = format!("t-output-order-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let instance = mk_instance(&tenant, seq_id, None);
    s.create_instance(&instance).await.unwrap();
    let timestamp = Utc::now();
    let low_id = Uuid::parse_str("018f0000-0000-7000-8000-000000000001").unwrap();
    let high_id = Uuid::parse_str("018f0000-0000-7000-8000-000000000002").unwrap();

    for (id, value) in [(high_id, "high"), (low_id, "low")] {
        s.save_block_output(&BlockOutput {
            id,
            instance_id: instance.id,
            block_id: BlockId::new("same"),
            output: serde_json::json!({"value": value}),
            output_ref: None,
            output_size: 16,
            attempt: 0,
            created_at: timestamp,
        })
        .await
        .unwrap();
    }

    let outputs = s.get_all_outputs(instance.id).await.unwrap();
    assert_eq!(
        outputs.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![low_id, high_id]
    );
}

#[tokio::test]
async fn stale_recovery_skips_waiting_instances_postgres() {
    let s = require_postgres!();
    let tenant = format!("t-recovery-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let stale_at = Utc::now() - chrono::Duration::hours(1);
    let mut waiting = mk_instance(&tenant, seq_id, None);
    waiting.state = InstanceState::Waiting;
    waiting.next_fire_at = None;
    waiting.updated_at = stale_at;
    s.create_instance(&waiting).await.unwrap();
    let mut running = mk_instance(&tenant, seq_id, None);
    running.state = InstanceState::Running;
    running.next_fire_at = None;
    running.updated_at = stale_at;
    s.create_instance(&running).await.unwrap();

    assert_eq!(
        s.recover_stale_instances(std::time::Duration::from_secs(300))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        s.get_instance(waiting.id).await.unwrap().unwrap().state,
        InstanceState::Waiting
    );
    let recovered = s.get_instance(running.id).await.unwrap().unwrap();
    assert!(
        matches!(
            recovered.state,
            InstanceState::Scheduled | InstanceState::Running
        ),
        "a recovered row may be immediately reclaimed by a concurrent workspace scheduler"
    );
    assert!(recovered.updated_at > stale_at);
}

/// Postgres parity for the atomic terminal transition/outbox protocol. The
/// `SQLite` version exercises the same contract in `review_fixes_2026_06`.
#[tokio::test]
async fn terminal_transition_and_webhook_enqueue_share_one_transaction_postgres() {
    let s = require_postgres!();
    let tenant = format!("t-outbox-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    // A far-future fire time keeps this instance out of any concurrently
    // running scheduler's due-instance claim query (`cargo test --workspace`
    // shares one Postgres database across every crate's integration tests,
    // and some of them tick a real scheduler loop). `NULL` cannot be used
    // here because scheduled rows without an explicit fire time are due now.
    let mut instance = mk_instance(&tenant, seq_id, None);
    instance.next_fire_at = Some(Utc::now() + chrono::Duration::days(1));
    s.create_instance(&instance).await.unwrap();

    let entry = WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: "https://hooks.example.com/terminal".into(),
        event_type: "instance.completed".into(),
        instance_id: Some(instance.id.into_uuid()),
        payload: serde_json::json!({"event_type": "instance.completed"}),
        attempts: 0,
        last_error: None,
        created_at: Utc::now(),
        delivery_id: Some(Uuid::now_v7()),
        status: WebhookOutboxStatus::Pending,
        next_attempt_at: None,
        claimed_at: None,
    };
    assert!(
        s.conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Scheduled,
            InstanceState::Completed,
            None,
            std::slice::from_ref(&entry),
        )
        .await
        .unwrap()
    );
    assert_eq!(
        s.get_instance(instance.id).await.unwrap().unwrap().state,
        InstanceState::Completed
    );
    assert_eq!(
        s.get_webhook_outbox(entry.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        WebhookOutboxStatus::Pending
    );

    let losing = WebhookOutboxEntry {
        id: Uuid::now_v7(),
        ..entry.clone()
    };
    assert!(
        !s.conditional_update_instance_state_with_outbox(
            instance.id,
            InstanceState::Scheduled,
            InstanceState::Failed,
            None,
            std::slice::from_ref(&losing),
        )
        .await
        .unwrap()
    );
    assert!(s.get_webhook_outbox(losing.id).await.unwrap().is_none());

    let claimed = s.claim_due_webhook_outbox(Utc::now(), 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, entry.id);
    assert_eq!(claimed[0].status, WebhookOutboxStatus::InFlight);

    let retry_at = Utc::now() + chrono::Duration::minutes(5);
    s.fail_webhook_outbox_attempt(entry.id, "http 503", Some(retry_at))
        .await
        .unwrap();
    assert!(
        s.claim_due_webhook_outbox(Utc::now(), 10)
            .await
            .unwrap()
            .is_empty()
    );
    let retried = s
        .claim_due_webhook_outbox(retry_at + chrono::Duration::milliseconds(1), 10)
        .await
        .unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].attempts, 1);

    let recovered = s
        .recover_stale_webhook_claims(retry_at + chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert!(recovered >= 1);
    let claimed_at = retry_at + chrono::Duration::seconds(2);
    assert!(
        s.claim_webhook_outbox_row(entry.id, claimed_at)
            .await
            .unwrap()
    );
    assert!(
        !s.claim_webhook_outbox_row(entry.id, claimed_at)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn run_initialization_and_reset_preserve_postgres_context() {
    let s = require_postgres!();
    let tenant = format!("t-run-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let mut instance = mk_instance(&tenant, seq_id, None);
    instance.context.data = serde_json::json!({"keep": true});
    instance.context.runtime.current_step = Some(BlockId::new("old"));
    instance.context.runtime.total_steps_executed = 900;
    s.create_instance(&instance).await.unwrap();

    let started_at = Utc::now() - chrono::Duration::minutes(1);
    s.ensure_instance_run_started(instance.id, "run-1", started_at)
        .await
        .unwrap();
    s.ensure_instance_run_started(instance.id, "run-racing", Utc::now())
        .await
        .unwrap();
    let started = s.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(started.context.runtime.run_id.as_deref(), Some("run-1"));
    assert_eq!(started.context.runtime.started_at, Some(started_at));

    s.reset_instance_run(instance.id, "run-2").await.unwrap();
    let reset = s.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(reset.context.data, serde_json::json!({"keep": true}));
    assert_eq!(reset.context.runtime.run_id.as_deref(), Some("run-2"));
    assert_eq!(reset.context.runtime.total_steps_executed, 0);
    assert!(reset.context.runtime.current_step.is_none());
    assert!(reset.context.runtime.started_at.is_none());
}

#[tokio::test]
async fn duplicate_collapsible_wake_preserves_pending_replacement_postgres() {
    use orch8_push::{CollapsibleWake, PushOutboxStore};
    let storage = require_postgres!();
    let scope = Uuid::new_v4().to_string();
    let now = Utc::now();
    storage
        .register_mobile_device(&orch8_storage::MobileDevice {
            device_id: scope.clone(),
            tenant_id: scope.clone(),
            push_token: Some("test-token".into()),
            platform: "ios".into(),
            app_version: None,
            active: true,
            last_sync_at: None,
            registered_at: String::new(),
        })
        .await
        .unwrap();
    let old = CollapsibleWake {
        tenant_id: scope.clone(),
        device_id: scope.clone(),
        execution_id: scope,
        topic: "resume".into(),
        command_id: "old".into(),
        created_at: now,
    };
    let old_id = storage.enqueue_collapsible_wake(&old).await.unwrap();
    assert_eq!(
        storage.enqueue_collapsible_wake(&old).await.unwrap(),
        old_id
    );
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM push_wake_outbox WHERE id=$1")
        .bind(old_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "pending");
    let mut new = old.clone();
    new.command_id = "new".into();
    let new_id = storage.enqueue_collapsible_wake(&new).await.unwrap();
    assert_eq!(
        storage.enqueue_collapsible_wake(&old).await.unwrap(),
        old_id
    );
    assert_eq!(
        storage.enqueue_collapsible_wake(&new).await.unwrap(),
        new_id
    );
    let rows: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id,status FROM push_wake_outbox WHERE id=ANY($1) ORDER BY status")
            .bind(vec![old_id, new_id])
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![(new_id, "pending".into()), (old_id, "terminal".into())]
    );
    pool.close().await;
}

/// STO-N2: migration 037 created `enabled`/`alert_sent` as INTEGER and the
/// rate columns as REAL while the storage layer decodes bool/f64, so every
/// rollback-policy/history read failed on Postgres. Round-trips both tables.
#[tokio::test]
async fn postgres_rollback_policy_and_history_roundtrip() {
    let storage = require_postgres!();
    let tenant = format!("rollback-{}", Uuid::new_v4());
    storage
        .create_rollback_policy(&tenant, "seq-a", 0.125, 300, Some(10), Some(5), None)
        .await
        .unwrap();
    // Upsert path (`enabled = TRUE` on conflict) must also bind correctly.
    storage
        .create_rollback_policy(&tenant, "seq-a", 0.25, 600, None, None, Some("https://x"))
        .await
        .unwrap();
    let policy = storage
        .get_rollback_policy(&tenant, "seq-a")
        .await
        .unwrap()
        .expect("policy");
    assert!(policy.enabled);
    assert!((policy.error_rate_threshold - 0.25).abs() < f64::EPSILON);
    assert_eq!(policy.time_window_secs, 600);
    assert_eq!(policy.webhook_url.as_deref(), Some("https://x"));
    let listed = storage
        .list_rollback_policies(Some(&tenant), 10)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);

    storage
        .record_rollback(&tenant, "seq-a", 0.5, 0.25, "threshold_breach")
        .await
        .unwrap();
    let history = storage
        .list_rollback_history(Some(&tenant), Some("seq-a"), 10)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert!((history[0].error_rate - 0.5).abs() < f64::EPSILON);
    assert!((history[0].threshold - 0.25).abs() < f64::EPSILON);
    assert!(!history[0].alert_sent);
}

/// STO-N10: error reports are written to `telemetry_mobile_errors`; the
/// rollback error rate must count them (`SQLite` stores them as
/// `InstanceFailed` telemetry events and counts them).
#[tokio::test]
async fn postgres_error_rate_counts_error_reports() {
    let storage = require_postgres!();
    let tenant = format!("error-rate-{}", Uuid::new_v4());
    assert_eq!(
        storage
            .query_error_rate(&tenant, "seq", 3600)
            .await
            .unwrap(),
        None
    );
    for _ in 0..2 {
        storage
            .ingest_telemetry_error(
                "RuntimeError",
                "boom",
                None,
                "d1",
                "iOS",
                "17",
                "1.0",
                "0.1",
                &tenant,
                Some("i1"),
                Some("seq"),
            )
            .await
            .unwrap();
    }
    storage
        .ingest_telemetry_event(
            "InstanceCompleted",
            &serde_json::json!({"sequence_name": "seq"}).to_string(),
            "d1",
            "iOS",
            "17",
            "1.0",
            "0.1",
            &tenant,
            Utc::now(),
        )
        .await
        .unwrap();
    let rate = storage
        .query_error_rate(&tenant, "seq", 3600)
        .await
        .unwrap()
        .expect("rate");
    assert!((rate - 2.0 / 3.0).abs() < 1e-9, "rate = {rate}");
}

/// STO-N3: concurrent admitted creates through `EncryptingStorage` must be
/// serialized by the inner backend's tenant lock (the trait default is a
/// racy count-then-insert that admits several under contention).
#[tokio::test]
async fn postgres_encrypting_admitted_create_is_atomic() {
    let storage = require_postgres!();
    let tenant = format!("admitted-{}", Uuid::new_v4());
    let sequence_id = SequenceId::new();
    storage
        .create_sequence(&mk_sequence(&tenant, sequence_id))
        .await
        .unwrap();
    let inner: Arc<dyn orch8_storage::StorageBackend> = Arc::new(storage);
    let encrypting = Arc::new(orch8_storage::encrypting::EncryptingStorage::new(
        inner,
        orch8_types::encryption::FieldEncryptor::from_hex_key(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap(),
    ));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let encrypting = Arc::clone(&encrypting);
        let inst = mk_instance(&tenant, sequence_id, None);
        handles.push(tokio::spawn(async move {
            encrypting.create_instance_admitted(&inst, 1).await
        }));
    }
    let mut admitted = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            admitted += 1;
        }
    }
    assert_eq!(admitted, 1);
}

/// STO-N5: concurrency counts/positions are scoped by (tenant, key).
#[tokio::test]
async fn postgres_concurrency_key_is_tenant_scoped() {
    let storage = require_postgres!();
    let key = format!("shared-{}", Uuid::new_v4());
    let tenant_a = format!("ck-a-{}", Uuid::new_v4());
    let tenant_b = format!("ck-b-{}", Uuid::new_v4());
    let seq_a = SequenceId::new();
    let seq_b = SequenceId::new();
    storage
        .create_sequence(&mk_sequence(&tenant_a, seq_a))
        .await
        .unwrap();
    storage
        .create_sequence(&mk_sequence(&tenant_b, seq_b))
        .await
        .unwrap();
    let mut a = mk_instance(&tenant_a, seq_a, Some(&key));
    a.state = InstanceState::Running;
    storage.create_instance(&a).await.unwrap();
    let mut b = mk_instance(&tenant_b, seq_b, Some(&key));
    b.state = InstanceState::Running;
    storage.create_instance(&b).await.unwrap();

    let counts = storage
        .count_running_by_concurrency_keys(&[(&tenant_a, &key), (&tenant_b, &key)])
        .await
        .unwrap();
    assert_eq!(counts.get(&(tenant_a.clone(), key.clone())), Some(&1));
    assert_eq!(counts.get(&(tenant_b.clone(), key.clone())), Some(&1));
    assert_eq!(storage.concurrency_position(b.id, &key).await.unwrap(), 1);
}

/// M6: a claimed task with a NULL heartbeat must age from `claimed_at` and
/// be reclaimed (a bare `heartbeat_at < cutoff` never matched NULL on PG).
#[tokio::test]
async fn postgres_reaper_reclaims_null_heartbeat_by_claimed_at() {
    let s = require_postgres!();
    let tenant = format!("t-null-hb-{}", Uuid::new_v4());
    let handler = format!("null_hb-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let instance = mk_instance(&tenant, seq_id, None);
    s.create_instance(&instance).await.unwrap();
    let task = WorkerTask {
        id: Uuid::new_v4(),
        instance_id: instance.id,
        block_id: BlockId::new("step"),
        handler_name: handler.clone(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({}),
        context: serde_json::json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task).await.unwrap();
    assert_eq!(
        s.claim_worker_tasks(&handler, "w", 1).await.unwrap().len(),
        1
    );
    sqlx::query(
        "UPDATE worker_tasks SET heartbeat_at = NULL, claimed_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
    )
    .bind(task.id)
    .execute(s.pool())
    .await
    .unwrap();
    s.reap_stale_worker_tasks(std::time::Duration::from_secs(600))
        .await
        .unwrap();
    let reaped = s.get_worker_task(task.id).await.unwrap().unwrap();
    assert_eq!(reaped.state, WorkerTaskState::Pending);
}

/// M5: outbox fail/complete are fenced on the claim timestamp.
#[tokio::test]
async fn postgres_webhook_outbox_fail_and_complete_are_fenced_on_claim() {
    let s = require_postgres!();
    let entry = WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: "https://hooks.example.com/fenced".into(),
        event_type: "instance.completed".into(),
        instance_id: None,
        payload: serde_json::json!({}),
        attempts: 0,
        last_error: None,
        created_at: Utc::now(),
        delivery_id: Some(Uuid::now_v7()),
        // Inserted already in flight, with claim stamps in the future, so
        // other tests' global claim/recover sweeps never touch it.
        status: WebhookOutboxStatus::InFlight,
        next_attempt_at: None,
        claimed_at: Some(Utc::now() + chrono::Duration::days(1)),
    };
    s.park_webhook(&entry).await.unwrap();
    let stale_claim = entry.claimed_at.unwrap();
    // Simulate recovery + re-claim by another node.
    let fresh_claim = Utc::now() + chrono::Duration::days(2);
    sqlx::query("UPDATE webhook_outbox SET claimed_at = $2 WHERE id = $1")
        .bind(entry.id)
        .bind(fresh_claim)
        .execute(s.pool())
        .await
        .unwrap();
    assert!(
        !s.fail_webhook_outbox_attempt_fenced(entry.id, stale_claim, "late", None)
            .await
            .unwrap()
    );
    assert!(
        !s.complete_webhook_outbox_claim(entry.id, stale_claim)
            .await
            .unwrap()
    );
    assert_eq!(
        s.get_webhook_outbox(entry.id)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        0
    );
    // Nanosecond-precision caller timestamp still matches the stored
    // microsecond value.
    assert!(
        s.complete_webhook_outbox_claim(entry.id, fresh_claim)
            .await
            .unwrap()
    );
    assert!(s.get_webhook_outbox(entry.id).await.unwrap().is_none());
}

/// M2/M3: metadata merge is shallow and the metadata filter follows Postgres
/// `@>` containment (type-aware scalars, nested objects, arrays).
async fn assert_metadata_semantics(
    s: &dyn orch8_storage::StorageBackend,
    tenant: &str,
    seq_id: SequenceId,
    inst: TaskInstance,
) {
    use orch8_types::filter::{InstanceFilter, Pagination};
    let mut inst = inst;
    inst.metadata = serde_json::json!({
        "flag": true,
        "n": 1,
        "s": "1",
        "nested": {"a": 1, "b": {"c": "x"}},
        "tags": ["red", "blue", {"k": 2}],
        "keep": "me"
    });
    inst.sequence_id = seq_id;
    s.create_instance(&inst).await.unwrap();

    let matches = |filter: serde_json::Value| {
        let f = InstanceFilter {
            tenant_id: Some(TenantId::unchecked(tenant)),
            metadata_filter: Some(filter),
            ..InstanceFilter::default()
        };
        async move {
            s.list_instances(&f, &Pagination::default())
                .await
                .unwrap()
                .len()
                == 1
        }
    };
    assert!(matches(serde_json::json!({"flag": true})).await);
    assert!(!matches(serde_json::json!({"flag": 1})).await);
    assert!(matches(serde_json::json!({"n": 1})).await);
    assert!(!matches(serde_json::json!({"n": "1"})).await);
    assert!(matches(serde_json::json!({"s": "1"})).await);
    assert!(!matches(serde_json::json!({"s": 1})).await);
    assert!(matches(serde_json::json!({"nested": {"b": {"c": "x"}}})).await);
    assert!(!matches(serde_json::json!({"nested": {"b": {"c": "y"}}})).await);
    assert!(matches(serde_json::json!({"tags": ["blue"]})).await);
    assert!(matches(serde_json::json!({"tags": [{"k": 2}, "red"]})).await);
    assert!(!matches(serde_json::json!({"tags": ["green"]})).await);
    assert!(matches(serde_json::json!({})).await);
    assert!(!matches(serde_json::json!(["x"])).await);

    // Shallow merge: `nested` is replaced wholesale, `null` is stored.
    s.merge_instance_metadata(
        inst.id,
        &serde_json::json!({"nested": {"z": 1}, "keep": null, "new": 2}),
    )
    .await
    .unwrap();
    let got = s.get_instance(inst.id).await.unwrap().unwrap().metadata;
    assert_eq!(got["nested"], serde_json::json!({"z": 1}));
    assert_eq!(got["keep"], serde_json::Value::Null);
    assert!(got.as_object().unwrap().contains_key("keep"));
    assert_eq!(got["new"], serde_json::json!(2));
    assert_eq!(got["flag"], serde_json::json!(true));
}

#[tokio::test]
async fn postgres_metadata_merge_and_filter_semantics() {
    let storage = require_postgres!();
    let tenant = format!("meta-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    storage
        .create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let inst = mk_instance(&tenant, seq_id, None);
    assert_metadata_semantics(&storage, &tenant, seq_id, inst).await;
}

/// Same-`created_at` outputs must resolve deterministically by `id`
/// (`UUIDv7`, insertion order) in every reader, and a fork copy must preserve
/// that order instead of minting random v4 ids.
#[tokio::test]
async fn postgres_block_output_ties_break_by_id_and_copy_preserves_order() {
    use orch8_types::output::BlockOutput;
    let s = require_postgres!();
    let tenant = format!("t-out-tie-{}", Uuid::new_v4());
    let seq_id = SequenceId::new();
    s.create_sequence(&mk_sequence(&tenant, seq_id))
        .await
        .unwrap();
    let src = mk_instance(&tenant, seq_id, None);
    let dst = mk_instance(&tenant, seq_id, None);
    s.create_instance(&src).await.unwrap();
    s.create_instance(&dst).await.unwrap();
    let block = BlockId::new("b");
    let at = Utc::now();
    for attempt in 1..=5u16 {
        s.save_block_output(&BlockOutput {
            id: Uuid::now_v7(),
            instance_id: src.id,
            block_id: block.clone(),
            output: serde_json::json!({"attempt": attempt}),
            output_ref: None,
            output_size: 2,
            attempt,
            created_at: at,
        })
        .await
        .unwrap();
    }
    let latest = s
        .get_block_outputs_batch(&[(src.id, &block)])
        .await
        .unwrap();
    assert_eq!(latest[&(src.id, block.clone())].attempt, 5);
    let after: Vec<u16> = s
        .get_outputs_after_created_at(src.id, None)
        .await
        .unwrap()
        .iter()
        .map(|o| o.attempt)
        .collect();
    assert_eq!(after, vec![1, 2, 3, 4, 5]);

    assert_eq!(
        s.copy_block_outputs(src.id, dst.id, std::slice::from_ref(&block))
            .await
            .unwrap(),
        5
    );
    let copied: Vec<u16> = s
        .get_all_outputs(dst.id)
        .await
        .unwrap()
        .iter()
        .map(|o| o.attempt)
        .collect();
    assert_eq!(copied, vec![1, 2, 3, 4, 5]);
    let latest = s
        .get_block_outputs_batch(&[(dst.id, &block)])
        .await
        .unwrap();
    assert_eq!(latest[&(dst.id, block)].attempt, 5);
}
