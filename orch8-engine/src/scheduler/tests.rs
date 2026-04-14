//! Unit tests for the scheduler module.

#![allow(clippy::duration_suboptimal_units)]

use super::step_exec::{check_step_delay, check_step_rate_limit, execute_step_block};
use super::*;

use orch8_types::ids::BlockId;
use orch8_types::signal::{Signal, SignalType};
use uuid::Uuid;

#[test]
fn build_prefetch_map_merges_signals_and_blocks() {
    let id1 = InstanceId(Uuid::now_v7());
    let id2 = InstanceId(Uuid::now_v7());

    let mut signals = HashMap::new();
    signals.insert(
        id1,
        vec![Signal {
            id: Uuid::now_v7(),
            instance_id: id1,
            signal_type: SignalType::Pause,
            payload: serde_json::Value::Null,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        }],
    );

    let mut completed = HashMap::new();
    completed.insert(id1, vec![BlockId("step1".into())]);
    completed.insert(id2, vec![BlockId("step2".into())]);

    let result = build_prefetch_map(signals, completed);

    assert_eq!(result.len(), 2);
    assert_eq!(result[&id1].signals.len(), 1);
    assert_eq!(result[&id1].completed_block_ids.len(), 1);
    assert_eq!(result[&id2].signals.len(), 0);
    assert_eq!(result[&id2].completed_block_ids.len(), 1);
}

#[test]
fn build_prefetch_map_empty_inputs() {
    let result = build_prefetch_map(HashMap::new(), HashMap::new());
    assert!(result.is_empty());
}

#[test]
fn build_prefetch_map_signals_only_instance() {
    // An instance with signals but no completed blocks must still appear.
    let id = InstanceId(Uuid::now_v7());
    let mut signals = HashMap::new();
    signals.insert(
        id,
        vec![Signal {
            id: Uuid::now_v7(),
            instance_id: id,
            signal_type: SignalType::Resume,
            payload: serde_json::Value::Null,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        }],
    );

    let result = build_prefetch_map(signals, HashMap::new());
    assert_eq!(result.len(), 1);
    assert_eq!(result[&id].signals.len(), 1);
    assert!(result[&id].completed_block_ids.is_empty());
}

#[test]
fn build_prefetch_map_completed_only_instance() {
    // An instance with only completed-block data must still appear with empty signals.
    let id = InstanceId(Uuid::now_v7());
    let mut completed = HashMap::new();
    completed.insert(id, vec![BlockId("step-a".into()), BlockId("step-b".into())]);

    let result = build_prefetch_map(HashMap::new(), completed);
    assert_eq!(result.len(), 1);
    assert!(result[&id].signals.is_empty());
    assert_eq!(result[&id].completed_block_ids.len(), 2);
}

// ------------------------------------------------------------------
// R6 follow-up: template::resolve wiring into scheduler dispatch path
// ------------------------------------------------------------------

use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::StepDef;
use std::sync::Mutex;

async fn seed_instance_with_context(
    storage: &dyn StorageBackend,
    id: InstanceId,
    context: ExecutionContext,
) {
    let now = Utc::now();
    let inst = TaskInstance {
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        state: InstanceState::Running,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context,
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&inst).await.unwrap();
}

fn mk_step_def(id: &str, handler: &str, params: serde_json::Value) -> StepDef {
    StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
        params,
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
    }
}

#[tokio::test]
async fn scheduler_execute_step_block_resolves_templates_for_inprocess_handler() {
    // Regression: the fast-path scheduler loop (`execute_step_block`)
    // must also run template + credential resolution before invoking
    // the handler. Without this, `{{context.data.*}}` leaks through to
    // any handler dispatched via the queue-driven path.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let instance_id = InstanceId::new();
    let ctx = ExecutionContext {
        data: serde_json::json!({"slug": "user.signed_up"}),
        ..ExecutionContext::default()
    };
    seed_instance_with_context(storage.as_ref(), instance_id, ctx).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let step_def = mk_step_def(
        "fire",
        "capture_params",
        serde_json::json!({"trigger_slug": "{{context.data.slug}}"}),
    );

    let captured: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
    let captured_clone = Arc::clone(&captured);

    let mut registry = HandlerRegistry::new();
    registry.register("capture_params", move |ctx| {
        let captured = Arc::clone(&captured_clone);
        async move {
            *captured.lock().unwrap() = Some(ctx.params.clone());
            Ok(serde_json::json!({"ok": true}))
        }
    });

    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();

    let outcome = execute_step_block(
        &storage,
        &registry,
        &webhook_config,
        0,
        &instance,
        &step_def,
        &cancel,
    )
    .await
    .expect("execute_step_block errored");

    assert!(matches!(outcome, StepOutcome::Completed));
    let seen = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler not called");
    assert_eq!(seen["trigger_slug"], "user.signed_up");
}

#[tokio::test]
async fn scheduler_execute_step_block_template_failure_fails_instance() {
    // An unknown template root (`{{nope.x}}`) must fail the instance
    // before the handler runs — just like the tree-evaluator path does.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let instance_id = InstanceId::new();
    seed_instance_with_context(storage.as_ref(), instance_id, ExecutionContext::default()).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let step_def = mk_step_def(
        "bad",
        "capture_params",
        serde_json::json!({"x": "{{nope.x}}"}),
    );

    let handler_called = Arc::new(Mutex::new(false));
    let handler_called_clone = Arc::clone(&handler_called);
    let mut registry = HandlerRegistry::new();
    registry.register("capture_params", move |_ctx| {
        let flag = Arc::clone(&handler_called_clone);
        async move {
            *flag.lock().unwrap() = true;
            Ok(serde_json::json!({}))
        }
    });

    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();

    let outcome = execute_step_block(
        &storage,
        &registry,
        &webhook_config,
        0,
        &instance,
        &step_def,
        &cancel,
    )
    .await
    .expect("execute_step_block errored");

    assert!(matches!(outcome, StepOutcome::Failed));
    assert!(
        !*handler_called.lock().unwrap(),
        "handler must not run when template resolution fails"
    );

    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

#[tokio::test]
async fn scheduler_execute_step_block_resolves_params_for_external_worker() {
    // When the handler is not registered in-process, the scheduler
    // dispatches to the external-worker queue. The queued task must
    // carry resolved params, not raw `{{…}}` strings.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let instance_id = InstanceId::new();
    let ctx = ExecutionContext {
        data: serde_json::json!({"slug": "user.signed_up"}),
        ..ExecutionContext::default()
    };
    seed_instance_with_context(storage.as_ref(), instance_id, ctx).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let mut step_def = mk_step_def(
        "external",
        "not_registered_handler",
        serde_json::json!({"trigger_slug": "{{context.data.slug}}"}),
    );
    // Explicit queue_name so the fast path dispatches to external workers
    // instead of failing for unknown handler.
    step_def.queue_name = Some("not_registered_handler".to_string());

    // Empty registry — forces the external-worker dispatch branch.
    let registry = HandlerRegistry::new();
    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();

    let outcome = execute_step_block(
        &storage,
        &registry,
        &webhook_config,
        0,
        &instance,
        &step_def,
        &cancel,
    )
    .await
    .expect("execute_step_block errored");

    assert!(matches!(outcome, StepOutcome::Deferred));

    // Inspect the queued WorkerTask: its params must be resolved.
    let tasks = storage
        .claim_worker_tasks("not_registered_handler", "test-worker", 10)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1, "exactly one worker task should be queued");
    assert_eq!(tasks[0].instance_id, instance_id);
    assert_eq!(
        tasks[0].params["trigger_slug"], "user.signed_up",
        "external worker must receive fully-resolved params"
    );
}

// ------------------------------------------------------------------
// Scheduler core helpers (TEST_PLAN.md #198-212).
// ------------------------------------------------------------------

use orch8_types::sequence::{BlockDefinition, DelaySpec, SequenceDefinition};

fn mk_sequence(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "cache-test".into(),
        version: 1,
        deprecated: false,
        blocks,
        interceptors: None,
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn get_sequence_cached_hit_avoids_storage_query() {
    // #212 — second call with the same SequenceId must be served from
    // the moka cache, not the storage layer. We verify by deleting the
    // sequence from storage after the first call: if the function still
    // returns it, it came from the cache.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![]);
    storage.create_sequence(&seq).await.unwrap();

    let cache = crate::sequence_cache::SequenceCache::new(16, Duration::from_secs(60));

    let first = cache.get_by_id(storage.as_ref(), seq.id).await.unwrap();
    assert_eq!(first.id, seq.id);

    // Drop from storage. If the cache is working the second call still
    // succeeds because it never touches storage.
    storage.delete_sequence(seq.id).await.unwrap();

    let second = cache
        .get_by_id(storage.as_ref(), seq.id)
        .await
        .expect("cache hit should bypass the now-missing storage row");
    assert_eq!(second.id, seq.id);
}

#[tokio::test]
async fn get_sequence_cached_miss_returns_error_when_not_in_storage() {
    // Cache miss + not-in-storage must surface as an error so the
    // scheduler can fail the instance cleanly.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let cache = crate::sequence_cache::SequenceCache::new(16, Duration::from_secs(60));

    let err = cache.get_by_id(storage.as_ref(), SequenceId::new()).await;
    assert!(err.is_err(), "unknown sequence must not be fabricated");
}

async fn seed_instance_in_state(
    storage: &dyn StorageBackend,
    id: InstanceId,
    parent: Option<InstanceId>,
    state: InstanceState,
) {
    let now = Utc::now();
    let inst = TaskInstance {
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
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
        parent_instance_id: parent,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&inst).await.unwrap();
}

#[tokio::test]
async fn wake_parent_if_child_wakes_waiting_parent() {
    // #210 — when a child reaches a terminal state, any parent sitting in
    // `Waiting` must be moved back to `Scheduled` so the scheduler picks
    // it up on the next tick.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let parent_id = InstanceId::new();
    let child_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), parent_id, None, InstanceState::Waiting).await;
    seed_instance_in_state(
        storage.as_ref(),
        child_id,
        Some(parent_id),
        InstanceState::Completed,
    )
    .await;

    let child = storage.get_instance(child_id).await.unwrap().unwrap();
    wake_parent_if_child(storage.as_ref(), &child).await;

    let parent = storage.get_instance(parent_id).await.unwrap().unwrap();
    assert_eq!(
        parent.state,
        InstanceState::Scheduled,
        "waiting parent must be moved to scheduled after child terminal state"
    );
}

#[tokio::test]
async fn wake_parent_if_child_is_noop_when_parent_not_waiting() {
    // Parents that aren't in `Waiting` (e.g. already running, paused,
    // cancelled) must NOT be disturbed. The hook is idempotent and
    // conservative — any other state is someone else's business.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let parent_id = InstanceId::new();
    let child_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), parent_id, None, InstanceState::Running).await;
    seed_instance_in_state(
        storage.as_ref(),
        child_id,
        Some(parent_id),
        InstanceState::Failed,
    )
    .await;

    let child = storage.get_instance(child_id).await.unwrap().unwrap();
    wake_parent_if_child(storage.as_ref(), &child).await;

    let parent = storage.get_instance(parent_id).await.unwrap().unwrap();
    assert_eq!(
        parent.state,
        InstanceState::Running,
        "non-waiting parent must be left alone"
    );
}

#[tokio::test]
async fn wake_parent_if_child_is_noop_when_no_parent() {
    // Root instances have `parent_instance_id = None` — the function
    // must short-circuit without touching storage.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), child_id, None, InstanceState::Completed).await;
    let child = storage.get_instance(child_id).await.unwrap().unwrap();
    // Not panicking is the assertion — the function returns unit.
    wake_parent_if_child(storage.as_ref(), &child).await;
}

#[tokio::test]
async fn check_step_delay_defers_instance_with_correct_fire_at() {
    // #203 — a step with a delay re-schedules the instance with
    // `next_fire_at` set to approximately `now + duration`, and moves
    // it back to `Scheduled` so the tick loop can pick it up.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let mut step_def = mk_step_def("s", "noop", serde_json::json!({}));
    step_def.delay = Some(DelaySpec {
        duration: Duration::from_secs(30),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: Some(String::new()).filter(|s| !s.is_empty()),
        timezone: None,
    });

    let deferred = check_step_delay(storage.as_ref(), &instance, &step_def)
        .await
        .expect("check_step_delay errored");
    assert!(deferred, "delay present → must defer");

    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let fire_at = refreshed.next_fire_at.expect("fire_at should be set");
    let now = Utc::now();
    let diff = (fire_at - now).num_seconds();
    assert!(
        (25..=35).contains(&diff),
        "fire_at should be ~30s in the future, got {diff}s"
    );
}

#[tokio::test]
async fn check_step_delay_is_noop_when_no_delay() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let step_def = mk_step_def("s", "noop", serde_json::json!({}));
    let deferred = check_step_delay(storage.as_ref(), &instance, &step_def)
        .await
        .unwrap();
    assert!(!deferred);
    // State must stay Running since there's nothing to defer.
    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Running);
}

// ------------------------------------------------------------------
// Plan #263-265: check_step_rate_limit
// ------------------------------------------------------------------

// Helper: seed a rate-limit row at the current cap so the next check is
// guaranteed to be Exceeded. Reuses the storage's upsert path so we also
// exercise the real SQL shape, not an internal hack.
async fn seed_rate_limit_at_cap(
    storage: &dyn StorageBackend,
    tenant: &str,
    resource_key: &str,
    max_count: i32,
) {
    use orch8_types::ids::ResourceKey;
    use orch8_types::rate_limit::RateLimit;
    storage
        .upsert_rate_limit(&RateLimit {
            id: Uuid::now_v7(),
            tenant_id: TenantId(tenant.into()),
            resource_key: ResourceKey(resource_key.into()),
            max_count,
            window_seconds: 60,
            // Fill the bucket to its cap so the next `check_rate_limit`
            // tips it over.
            current_count: max_count,
            window_start: Utc::now(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn check_step_rate_limit_returns_false_when_no_rate_limit_key() {
    // Plan #265: a step without `rate_limit_key` must short-circuit to
    // `Ok(false)` without touching storage.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let step_def = mk_step_def("s", "noop", serde_json::json!({}));
    // rate_limit_key defaults to None in mk_step_def.
    let deferred = check_step_rate_limit(storage.as_ref(), &instance, &step_def)
        .await
        .unwrap();
    assert!(!deferred, "no rate_limit_key => must not defer");

    // State must remain Running — nothing to defer.
    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Running);
}

#[tokio::test]
async fn check_step_rate_limit_allows_under_threshold() {
    // Plan #263: a configured-but-not-full bucket must be `Ok(false)`.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let mut step_def = mk_step_def("s", "noop", serde_json::json!({}));
    step_def.rate_limit_key = Some("rk-under".into());
    // No upsert → no bucket row → check_rate_limit returns Allowed.
    let deferred = check_step_rate_limit(storage.as_ref(), &instance, &step_def)
        .await
        .unwrap();
    assert!(!deferred, "empty bucket must not defer");

    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Running);
}

#[tokio::test]
async fn check_step_rate_limit_defers_at_threshold() {
    // Plan #264: bucket at cap => function returns true, transitions
    // Running → Scheduled with next_fire_at = retry_after.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    seed_rate_limit_at_cap(storage.as_ref(), "t", "rk-full", 1).await;

    let mut step_def = mk_step_def("s", "noop", serde_json::json!({}));
    step_def.rate_limit_key = Some("rk-full".into());

    let deferred = check_step_rate_limit(storage.as_ref(), &instance, &step_def)
        .await
        .unwrap();
    assert!(deferred, "full bucket must defer");

    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    assert!(
        refreshed.next_fire_at.is_some(),
        "deferred instance must have next_fire_at set to retry_after"
    );
}

// ------------------------------------------------------------------
// Plan #266-270: enforce_concurrency_limits
// ------------------------------------------------------------------

async fn seed_instance_with_concurrency(
    storage: &dyn StorageBackend,
    id: InstanceId,
    key: Option<&str>,
    max: Option<i32>,
    state: InstanceState,
) {
    let now = Utc::now();
    let inst = TaskInstance {
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        state,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext::default(),
        concurrency_key: key.map(String::from),
        max_concurrency: max,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&inst).await.unwrap();
}

#[tokio::test]
async fn enforce_concurrency_limits_passthrough_when_no_keys() {
    // Plan #268: instances without concurrency_key are returned as-is, no
    // DB queries issued.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let id1 = InstanceId::new();
    let id2 = InstanceId::new();
    seed_instance_with_concurrency(storage.as_ref(), id1, None, None, InstanceState::Running).await;
    seed_instance_with_concurrency(storage.as_ref(), id2, None, None, InstanceState::Running).await;
    let insts = vec![
        storage.get_instance(id1).await.unwrap().unwrap(),
        storage.get_instance(id2).await.unwrap().unwrap(),
    ];

    let kept = enforce_concurrency_limits(&storage, insts).await.unwrap();
    assert_eq!(kept.len(), 2, "no keys => passthrough");
}

#[tokio::test]
async fn enforce_concurrency_limits_max_one_serialises() {
    // Plan #267: max_concurrency=1, two claimed instances sharing the
    // same key — one must be deferred back to Scheduled.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let id1 = InstanceId::new();
    let id2 = InstanceId::new();
    // Both seeded as Running (mirrors what claim_due_instances would leave).
    seed_instance_with_concurrency(
        storage.as_ref(),
        id1,
        Some("ck-serial"),
        Some(1),
        InstanceState::Running,
    )
    .await;
    seed_instance_with_concurrency(
        storage.as_ref(),
        id2,
        Some("ck-serial"),
        Some(1),
        InstanceState::Running,
    )
    .await;
    let insts = vec![
        storage.get_instance(id1).await.unwrap().unwrap(),
        storage.get_instance(id2).await.unwrap().unwrap(),
    ];

    let kept = enforce_concurrency_limits(&storage, insts).await.unwrap();
    assert_eq!(kept.len(), 1, "max=1 must keep exactly one");

    // The deferred instance must be back in Scheduled with a fire_at set.
    let kept_ids: std::collections::HashSet<InstanceId> = kept.iter().map(|i| i.id).collect();
    let deferred_id = if kept_ids.contains(&id1) { id2 } else { id1 };
    let deferred = storage.get_instance(deferred_id).await.unwrap().unwrap();
    assert_eq!(deferred.state, InstanceState::Scheduled);
    assert!(deferred.next_fire_at.is_some());
}

#[tokio::test]
async fn enforce_concurrency_limits_max_zero_blocks_all() {
    // Plan #269: max_concurrency=0 means no slots — every instance in
    // that group is deferred.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let id1 = InstanceId::new();
    let id2 = InstanceId::new();
    seed_instance_with_concurrency(
        storage.as_ref(),
        id1,
        Some("ck-zero"),
        Some(0),
        InstanceState::Running,
    )
    .await;
    seed_instance_with_concurrency(
        storage.as_ref(),
        id2,
        Some("ck-zero"),
        Some(0),
        InstanceState::Running,
    )
    .await;
    let insts = vec![
        storage.get_instance(id1).await.unwrap().unwrap(),
        storage.get_instance(id2).await.unwrap().unwrap(),
    ];

    let kept = enforce_concurrency_limits(&storage, insts).await.unwrap();
    assert_eq!(kept.len(), 0, "max=0 must defer everyone");

    for id in [id1, id2] {
        let refreshed = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(refreshed.state, InstanceState::Scheduled);
    }
}

#[tokio::test]
async fn enforce_concurrency_limits_allows_up_to_cap() {
    // Plan #266/270: max_concurrency=2 with three contenders — two kept,
    // one deferred. Also verifies count-accurate logic (one is kept even
    // though batch size exceeds cap).
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let id1 = InstanceId::new();
    let id2 = InstanceId::new();
    let id3 = InstanceId::new();
    for id in [id1, id2, id3] {
        seed_instance_with_concurrency(
            storage.as_ref(),
            id,
            Some("ck-two"),
            Some(2),
            InstanceState::Running,
        )
        .await;
    }
    let insts = vec![
        storage.get_instance(id1).await.unwrap().unwrap(),
        storage.get_instance(id2).await.unwrap().unwrap(),
        storage.get_instance(id3).await.unwrap().unwrap(),
    ];

    let kept = enforce_concurrency_limits(&storage, insts).await.unwrap();
    assert_eq!(kept.len(), 2, "max=2 must keep exactly two of three");

    // Exactly one of the three must be back in Scheduled.
    let mut sched = 0;
    for id in [id1, id2, id3] {
        let i = storage.get_instance(id).await.unwrap().unwrap();
        if i.state == InstanceState::Scheduled {
            sched += 1;
        }
    }
    assert_eq!(sched, 1, "exactly one deferred when cap=2 and batch=3");
}

#[tokio::test]
async fn wait_for_drain_waits_for_permits_returned() {
    // Acquire a permit, spawn a task that holds it briefly, then ensure drain waits.
    let sem = Arc::new(Semaphore::new(2));
    let held = Arc::clone(&sem).acquire_owned().await.unwrap();

    // Drain future shouldn't complete while we're holding a permit.
    let drain = {
        let sem = Arc::clone(&sem);
        tokio::spawn(async move { wait_for_drain(&sem, 2).await })
    };

    // Give the task a chance to start.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!drain.is_finished(), "drain should still be waiting");

    drop(held);
    // After releasing, drain must complete promptly.
    tokio::time::timeout(Duration::from_millis(500), drain)
        .await
        .expect("drain timed out after permit released")
        .expect("drain task panicked");
    assert_eq!(sem.available_permits(), 2);
}
