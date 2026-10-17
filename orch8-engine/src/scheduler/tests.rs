//! Unit tests for the scheduler module.

#![allow(unknown_lints, clippy::duration_suboptimal_units)]

use super::step_exec::{check_step_delay, check_step_rate_limit, execute_step_block};
use super::*;

use orch8_types::ids::BlockId;
use orch8_types::signal::{Signal, SignalType};
use uuid::Uuid;

#[test]
fn build_prefetch_map_merges_signals_and_blocks() {
    let id1 = InstanceId::new();
    let id2 = InstanceId::new();

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
    completed.insert(id1, vec![BlockId::new("step1")]);
    completed.insert(id2, vec![BlockId::new("step2")]);

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
    let id = InstanceId::new();
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
    let id = InstanceId::new();
    let mut completed = HashMap::new();
    completed.insert(id, vec![BlockId::new("step-a"), BlockId::new("step-b")]);

    let result = build_prefetch_map(HashMap::new(), completed);
    assert_eq!(result.len(), 1);
    assert!(result[&id].signals.is_empty());
    assert_eq!(result[&id].completed_block_ids.len(), 2);
}

#[tokio::test]
async fn cooperative_preemption_reschedules_lower_priority_instance() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let now = Utc::now();
    let make = |priority, state, fire_at| TaskInstance {
        sub_tenant: None,
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("tenant-a"),
        namespace: Namespace::new("default"),
        state,
        next_fire_at: fire_at,
        priority,
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
    };
    let low = make(Priority::Low, InstanceState::Running, None);
    let high = make(
        Priority::High,
        InstanceState::Scheduled,
        Some(now - chrono::Duration::seconds(1)),
    );
    storage.create_instance(&low).await.unwrap();
    storage.create_instance(&high).await.unwrap();

    assert!(
        cooperatively_preempt(&storage, &low, &SharedClock::default())
            .await
            .unwrap()
    );
    let low = storage.get_instance(low.id).await.unwrap().unwrap();
    assert_eq!(low.state, InstanceState::Scheduled);
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
        sub_tenant: None,
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
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
        budget: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&inst).await.unwrap();
}

fn mk_step_def(id: &str, handler: &str, params: serde_json::Value) -> StepDef {
    StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params,
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        rate_budget: None,
        placement: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
        output_schema: None,
        when: None,
        compensation: None,
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
        &SharedClock::default(),
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
        &SharedClock::default(),
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
async fn scheduler_execute_step_block_unparseable_when_guard_fails_instance() {
    // M7: a `when` guard that fails to parse is a malformed step definition,
    // not a falsy condition. The lenient evaluator swallowed parse errors to
    // `null` → guard false → the step was silently skipped. It must now fail
    // the instance loudly, before the handler runs.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let instance_id = InstanceId::new();
    seed_instance_with_context(storage.as_ref(), instance_id, ExecutionContext::default()).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();

    let mut step_def = mk_step_def("guarded", "capture_params", serde_json::json!({}));
    step_def.when = Some("5 5".into());

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
        &SharedClock::default(),
    )
    .await
    .expect("execute_step_block errored");

    assert!(matches!(outcome, StepOutcome::Failed));
    assert!(
        !*handler_called.lock().unwrap(),
        "handler must not run when the when guard is unparseable"
    );

    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

#[tokio::test]
async fn scheduler_fast_path_retryable_failure_honours_max_attempts() {
    // Regression: the fast path previously deleted ALL block outputs on a
    // retryable failure without persisting a retry marker, so
    // `compute_attempt` reset to 0 every tick and `attempt >= max_attempts`
    // was never true — the step retried forever and never reached the DLQ.
    // After the fix it writes a `__retry__` marker (mirroring the tree path)
    // so the counter advances and the instance fails after `max_attempts`.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let instance_id = InstanceId::new();
    seed_instance_with_context(storage.as_ref(), instance_id, ExecutionContext::default()).await;

    let mut step_def = mk_step_def("flaky", "always_retryable", serde_json::json!({}));
    step_def.retry = Some(orch8_types::sequence::RetryPolicy {
        max_attempts: 2,
        initial_backoff: std::time::Duration::from_millis(0),
        max_backoff: std::time::Duration::from_millis(0),
        backoff_multiplier: 1.0,
        retry_if: None,
        non_retryable_codes: None,
    });

    let mut registry = HandlerRegistry::new();
    registry.register("always_retryable", move |_ctx| async move {
        Err(orch8_types::error::StepError::Retryable {
            message: "transient".into(),
            details: None,
        })
    });

    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();

    let mut attempts = 0;
    let mut final_state = None;
    for _ in 0..10 {
        // Simulate the scheduler claim: a rescheduled instance sits in
        // `Scheduled`; move it back to `Running` before the next execution.
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        let inst = if inst.state == InstanceState::Scheduled {
            crate::lifecycle::transition_instance(
                storage.as_ref(),
                instance_id,
                Some(&inst.tenant_id),
                InstanceState::Scheduled,
                InstanceState::Running,
                None,
            )
            .await
            .unwrap();
            storage.get_instance(instance_id).await.unwrap().unwrap()
        } else {
            inst
        };
        if inst.state != InstanceState::Running {
            final_state = Some(inst.state);
            break;
        }

        execute_step_block(
            &storage,
            &registry,
            &webhook_config,
            0,
            &inst,
            &step_def,
            &cancel,
            &SharedClock::default(),
        )
        .await
        .expect("execute_step_block errored");
        attempts += 1;

        let after = storage.get_instance(instance_id).await.unwrap().unwrap();
        if after.state.is_terminal() {
            final_state = Some(after.state);
            break;
        }
    }

    // max_attempts = 2 → executes at attempt 0, 1, 2 then fails on the third
    // (2 >= 2). Crucially it must terminate, not loop the full 10 iterations.
    assert_eq!(
        final_state,
        Some(InstanceState::Failed),
        "instance must reach Failed, not retry forever"
    );
    assert_eq!(
        attempts, 3,
        "should fail on the third execution (attempt index 2)"
    );
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
        &SharedClock::default(),
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

use orch8_types::sequence::{BlockDefinition, DelaySpec, SequenceDefinition, SequenceStatus};

fn mk_sequence(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        embed: None,
        sub_tenant: None,
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "cache-test".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks,
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        placement: None,
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
        sub_tenant: None,
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
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
        budget: None,
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

    let deferred = check_step_delay(
        storage.as_ref(),
        &instance,
        &step_def,
        &SharedClock::default(),
    )
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
    let deferred = check_step_delay(
        storage.as_ref(),
        &instance,
        &step_def,
        &SharedClock::default(),
    )
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
            tenant_id: TenantId::unchecked(tenant),
            resource_key: ResourceKey::new(resource_key),
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
    let deferred = check_step_rate_limit(
        storage.as_ref(),
        &instance,
        &step_def,
        &SharedClock::default(),
    )
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
    let deferred = check_step_rate_limit(
        storage.as_ref(),
        &instance,
        &step_def,
        &SharedClock::default(),
    )
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

    let deferred = check_step_rate_limit(
        storage.as_ref(),
        &instance,
        &step_def,
        &SharedClock::default(),
    )
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
    max: Option<u32>,
    state: InstanceState,
) {
    let now = Utc::now();
    let inst = TaskInstance {
        sub_tenant: None,
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
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
        budget: None,
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

    let kept = enforce_concurrency_limits(&storage, insts, &SharedClock::default())
        .await
        .unwrap();
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

    let kept = enforce_concurrency_limits(&storage, insts, &SharedClock::default())
        .await
        .unwrap();
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

    let kept = enforce_concurrency_limits(&storage, insts, &SharedClock::default())
        .await
        .unwrap();
    assert_eq!(kept.len(), 0, "max=0 must defer everyone");

    for id in [id1, id2] {
        let refreshed = storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(refreshed.state, InstanceState::Scheduled);
    }
}

#[tokio::test]
async fn deferral_only_reschedules_instances_that_are_still_running() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let running_id = InstanceId::new();
    let cancelled_id = InstanceId::new();
    seed_instance_with_concurrency(
        storage.as_ref(),
        running_id,
        None,
        None,
        InstanceState::Running,
    )
    .await;
    seed_instance_with_concurrency(
        storage.as_ref(),
        cancelled_id,
        None,
        None,
        InstanceState::Cancelled,
    )
    .await;

    let fire_at = Utc::now() + chrono::Duration::seconds(5);
    defer_running_instances(&storage, &[running_id, cancelled_id], fire_at)
        .await
        .unwrap();

    let running = storage.get_instance(running_id).await.unwrap().unwrap();
    assert_eq!(running.state, InstanceState::Scheduled);
    assert_eq!(running.next_fire_at, Some(fire_at));

    let cancelled = storage.get_instance(cancelled_id).await.unwrap().unwrap();
    assert_eq!(cancelled.state, InstanceState::Cancelled);
    assert_eq!(cancelled.next_fire_at, None);
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

    let kept = enforce_concurrency_limits(&storage, insts, &SharedClock::default())
        .await
        .unwrap();
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
async fn enforce_concurrency_limits_preserves_claim_order() {
    // Regression: the deferred-index removal previously used `swap_remove`,
    // which pulls tail elements into the holes and scrambles the priority
    // order established by claim_due_instances. With keyed instances at
    // indices 1 and 3 deferred from [u0, k1, u2, k3, u4], the survivors must
    // come back as [u0, u2, u4] — not [u0, u4, u2].
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let unkeyed: Vec<InstanceId> = (0..3).map(|_| InstanceId::new()).collect();
    let keyed: Vec<InstanceId> = (0..2).map(|_| InstanceId::new()).collect();
    for &id in &unkeyed {
        seed_instance_with_concurrency(storage.as_ref(), id, None, None, InstanceState::Running)
            .await;
    }
    for &id in &keyed {
        seed_instance_with_concurrency(
            storage.as_ref(),
            id,
            Some("ck-order"),
            Some(0), // zero slots — both keyed instances are deferred
            InstanceState::Running,
        )
        .await;
    }

    // Claim order: u0, k0, u1, k1, u2 (keyed at indices 1 and 3).
    let order = [unkeyed[0], keyed[0], unkeyed[1], keyed[1], unkeyed[2]];
    let mut insts = Vec::new();
    for id in order {
        insts.push(storage.get_instance(id).await.unwrap().unwrap());
    }

    let kept = enforce_concurrency_limits(&storage, insts, &SharedClock::default())
        .await
        .unwrap();
    let kept_ids: Vec<InstanceId> = kept.iter().map(|i| i.id).collect();
    assert_eq!(
        kept_ids,
        vec![unkeyed[0], unkeyed[1], unkeyed[2]],
        "survivors must keep their claim-time order"
    );
}

#[tokio::test]
async fn wait_for_input_baseline_survives_reentry() {
    // Regression: execute_step_block used to re-stamp
    // `current_step_started_at` on EVERY entry into a step. A Waiting
    // instance woken by process_waiting_deadlines re-enters the same step,
    // so each wake reset the wait_for_input timeout baseline — the timeout
    // could only fire through a stale-snapshot accident. The stamp must be
    // first-entry-only.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_with_context(storage.as_ref(), instance_id, ExecutionContext::default()).await;

    let mut step_def = mk_step_def("gate", "human_review", serde_json::json!({}));
    step_def.wait_for_input = Some(orch8_types::sequence::HumanInputDef {
        prompt: "approve?".into(),
        timeout: None,
        escalation_handler: None,
        choices: None,
        store_as: None,
        allow_comment: false,
        auto_decide: None,
    });

    let registry = HandlerRegistry::new();
    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();

    // First entry: defers and stamps the baseline.
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    let outcome = execute_step_block(
        &storage,
        &registry,
        &webhook_config,
        0,
        &instance,
        &step_def,
        &cancel,
        &clock,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, StepOutcome::Deferred));
    let first_stamp = storage
        .get_instance(instance_id)
        .await
        .unwrap()
        .unwrap()
        .context
        .runtime
        .current_step_started_at
        .expect("first entry stamps current_step_started_at");

    // Simulate a wake: back to Running, then re-enter the same step later.
    storage
        .update_instance_state(instance_id, InstanceState::Running, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    let outcome = execute_step_block(
        &storage,
        &registry,
        &webhook_config,
        0,
        &instance,
        &step_def,
        &cancel,
        &clock,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, StepOutcome::Deferred));

    let second_stamp = storage
        .get_instance(instance_id)
        .await
        .unwrap()
        .unwrap()
        .context
        .runtime
        .current_step_started_at
        .unwrap();
    assert_eq!(
        first_stamp, second_stamp,
        "re-entry must keep the original timeout baseline"
    );
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

// ---------------------------------------------------------------------------
// Exponential backoff formula tests (H12)
// ---------------------------------------------------------------------------

#[test]
fn backoff_formula_doubles_with_cap() {
    let compute_backoff = |failures: u32| -> u64 {
        100u64
            .saturating_mul(2u64.saturating_pow(failures))
            .min(5000)
    };

    assert_eq!(compute_backoff(0), 100);
    assert_eq!(compute_backoff(1), 200);
    assert_eq!(compute_backoff(2), 400);
    assert_eq!(compute_backoff(3), 800);
    assert_eq!(compute_backoff(4), 1600);
    assert_eq!(compute_backoff(5), 3200);
    assert_eq!(compute_backoff(6), 5000, "should cap at 5000ms");
    assert_eq!(compute_backoff(10), 5000, "should stay at cap");
    assert_eq!(compute_backoff(u32::MAX), 5000, "should never overflow");
}

// ---------------------------------------------------------------------------
// SLA breach sweep: rotation cursor + idle skip (H6)
// ---------------------------------------------------------------------------

/// Build a sequence with a `max_runtime` SLA for the sweep tests below.
fn mk_sla_sequence(name: &str, max_runtime: std::time::Duration) -> SequenceDefinition {
    use orch8_types::sequence::{BlockDefinition, SequenceStatus, SlaPolicy};
    SequenceDefinition {
        embed: None,
        sub_tenant: None,
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: name.into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::Step(Box::new(mk_step_def(
            "s1",
            "noop",
            serde_json::json!({}),
        )))],
        interceptors: None,
        input_schema: None,
        sla: Some(SlaPolicy {
            max_runtime: Some(max_runtime),
            max_step_runtime: None,
        }),
        on_failure: None,
        on_cancel: None,
        placement: None,
        created_at: Utc::now(),
    }
}

/// A Waiting instance of `seq_id` created `created_age` ago with the given
/// `updated_at` (controls where it sorts in the sweep's `updated_at ASC`
/// pages). Waiting so no other sweep/claim path touches it mid-test.
fn mk_waiting_instance(
    seq_id: SequenceId,
    created_age: chrono::Duration,
    updated_at: chrono::DateTime<Utc>,
) -> TaskInstance {
    TaskInstance {
        sub_tenant: None,
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Waiting,
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
        created_at: Utc::now() - created_age,
        updated_at,
    }
}

async fn sla_sentinel_exists(storage: &dyn StorageBackend, instance_id: InstanceId) -> bool {
    storage
        .get_block_output(instance_id, &BlockId::new("_sla:runtime"))
        .await
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn sla_sweep_rotates_beyond_first_page_across_ticks() {
    // H6 regression: with more active instances than one page holds, the old
    // fixed `LIMIT batch_size` window never saw instances sorted past the
    // first page — their breaches never fired. The cursor must rotate the
    // scan window across ticks so every instance is swept.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sla_sequence("sla-rotate", std::time::Duration::from_secs(60));
    storage.create_sequence(&seq).await.unwrap();

    // Both instances are an hour past the 60s SLA. `older` sorts into page 1,
    // `newer` into page 2 (batch_size = 1 below).
    let older = mk_waiting_instance(
        seq.id,
        chrono::Duration::hours(1),
        Utc::now() - chrono::Duration::hours(2),
    );
    let newer = mk_waiting_instance(
        seq.id,
        chrono::Duration::hours(1),
        Utc::now() - chrono::Duration::hours(1),
    );
    storage.create_instance(&older).await.unwrap();
    storage.create_instance(&newer).await.unwrap();

    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    let mut cursor = SlaSweepCursor::default();

    // batch_size = 1, one page per tick → one instance swept per tick.
    process_sla_breaches(
        &storage,
        &cache,
        &webhook_config,
        &cancel,
        1,
        &clock,
        &mut cursor,
        1,
    )
    .await
    .unwrap();
    assert!(
        sla_sentinel_exists(storage.as_ref(), older.id).await,
        "first tick should alert the first-page instance"
    );
    assert!(
        !sla_sentinel_exists(storage.as_ref(), newer.id).await,
        "beyond-first-page instance must wait for the next tick's page"
    );

    process_sla_breaches(
        &storage,
        &cache,
        &webhook_config,
        &cancel,
        1,
        &clock,
        &mut cursor,
        1,
    )
    .await
    .unwrap();
    assert!(
        sla_sentinel_exists(storage.as_ref(), newer.id).await,
        "second tick must alert the instance beyond the first page"
    );
}

#[tokio::test]
async fn sla_sweep_idles_when_no_sla_and_rescans_after_idle_window() {
    // With no SLA-bearing sequence anywhere, a completed rotation arms the
    // idle skip: the sweep stops issuing the instance query every tick. The
    // idle window is bounded — once it elapses, the next tick rescans and
    // discovers a newly registered SLA sequence.
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    let mut cursor = SlaSweepCursor::default();

    // Empty database → the first sweep completes a rotation observing no SLA
    // and arms the idle window.
    process_sla_breaches(
        &storage,
        &cache,
        &webhook_config,
        &cancel,
        1,
        &clock,
        &mut cursor,
        1,
    )
    .await
    .unwrap();
    assert_eq!(cursor.idle_ticks_remaining, SLA_SWEEP_IDLE_RESCAN_TICKS);

    // While idle, ticks skip the scan entirely (the counter just runs down).
    process_sla_breaches(
        &storage,
        &cache,
        &webhook_config,
        &cancel,
        1,
        &clock,
        &mut cursor,
        1,
    )
    .await
    .unwrap();
    assert_eq!(cursor.idle_ticks_remaining, SLA_SWEEP_IDLE_RESCAN_TICKS - 1);

    // A new SLA sequence + breaching instance arrives mid-idle…
    let seq = mk_sla_sequence("sla-late", std::time::Duration::from_secs(60));
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_waiting_instance(
        seq.id,
        chrono::Duration::hours(1),
        Utc::now() - chrono::Duration::hours(1),
    );
    storage.create_instance(&inst).await.unwrap();

    // …and is picked up as soon as the idle window elapses (shorten it to 1
    // tick for the test: this tick runs the counter out, the next rescans).
    cursor.idle_ticks_remaining = 1;
    process_sla_breaches(
        &storage,
        &cache,
        &webhook_config,
        &cancel,
        1,
        &clock,
        &mut cursor,
        1,
    )
    .await
    .unwrap();
    assert_eq!(cursor.idle_ticks_remaining, 0);
    process_sla_breaches(
        &storage,
        &cache,
        &webhook_config,
        &cancel,
        1,
        &clock,
        &mut cursor,
        1,
    )
    .await
    .unwrap();
    assert!(
        sla_sentinel_exists(storage.as_ref(), inst.id).await,
        "the rescan after the idle window must discover the new SLA instance"
    );
}

#[test]
fn instance_heartbeat_cadence_precedes_short_and_default_stale_windows() {
    for threshold_secs in [1, 2, 3, 5, 10, 15, 300, u64::MAX] {
        let cadence = instance_heartbeat_interval(threshold_secs);
        assert!(!cadence.is_zero());
        assert!(cadence * 2 < Duration::from_secs(threshold_secs));
    }
    assert_eq!(instance_heartbeat_interval(300), Duration::from_secs(100));
    assert!(!instance_heartbeat_interval(0).is_zero());
}

/// ENG-R-N5: the deadline sweep runs on every node. When another node has
/// already moved the instance (lost CAS), the escalation handler and breach
/// output must NOT run again — side effects only follow a won CAS.
#[tokio::test]
async fn deadline_breach_lost_cas_skips_escalation_side_effects() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    // Another node already failed it.
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Failed).await;
    let mut instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    // Snapshot as the sweep saw it: still Waiting, started long ago.
    instance.state = InstanceState::Waiting;
    instance.context.runtime.started_at = Some(Utc::now() - chrono::Duration::hours(1));

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in = Arc::clone(&calls);
    let mut registry = HandlerRegistry::new();
    registry.register("escalate", move |_ctx| {
        let calls = Arc::clone(&calls_in);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({}))
        }
    });

    let mut step_def = mk_step_def("s", "noop", serde_json::json!({}));
    step_def.deadline = Some(Duration::from_secs(1));
    step_def.on_deadline_breach = Some(orch8_types::sequence::EscalationDef {
        handler: "escalate".into(),
        params: serde_json::json!({}),
    });

    let handled = handle_deadline_breach(
        &storage,
        &registry,
        &instance,
        &step_def,
        None,
        InstanceState::Waiting,
        &SharedClock::default(),
        None,
    )
    .await
    .expect("lost CAS is not an error");
    assert!(handled, "caller must stop processing the instance");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "escalation must not re-run"
    );
    assert!(
        storage
            .get_block_output(instance_id, &BlockId::new("s"))
            .await
            .unwrap()
            .is_none(),
        "no duplicate breach output"
    );
}

/// ENG-R-N8: once the heartbeat has flagged the lease as lost, the flat
/// step loop must not start another step; it hands the instance back.
#[tokio::test]
async fn lease_lost_stops_flat_loop_before_next_step() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    let blocks = vec![BlockDefinition::Step(Box::new(mk_step_def(
        "s1",
        "noop",
        serde_json::json!({}),
    )))];
    let sequence = mk_sequence(blocks.clone());

    let registry = HandlerRegistry::new();
    let webhook_config = WebhookConfig::default();
    let cache = crate::sequence_cache::SequenceCache::new(16, Duration::from_secs(60));
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    let lease_lost = std::sync::atomic::AtomicBool::new(true);
    let ctx = InstanceRunCtx {
        storage: &storage,
        handlers: &registry,
        webhook_config: &webhook_config,
        sequence_cache: &cache,
        externalize_threshold: 0,
        max_steps_per_instance: 0,
        cancel: &cancel,
        clock: &clock,
        lease_lost: &lease_lost,
        claim_token: None,
    };

    let finished = execute_step_loop(&ctx, &instance, &sequence, &blocks, Vec::new())
        .await
        .unwrap();
    assert!(!finished);
    assert!(
        storage
            .get_block_output(instance_id, &BlockId::new("s1"))
            .await
            .unwrap()
            .is_none(),
        "no step may start after lease loss"
    );
    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// L6-01: when the lease was taken over by another claim (the row no longer
/// carries this run's claim token), the flat loop stops *without* handing the
/// instance back — releasing it would yank the new owner's run to Scheduled.
#[tokio::test]
async fn lease_lost_to_other_claim_does_not_release_instance() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    // Seeded Running without this run's claim token: another node owns it.
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    let blocks = vec![BlockDefinition::Step(Box::new(mk_step_def(
        "s1",
        "noop",
        serde_json::json!({}),
    )))];
    let sequence = mk_sequence(blocks.clone());

    let registry = HandlerRegistry::new();
    let webhook_config = WebhookConfig::default();
    let cache = crate::sequence_cache::SequenceCache::new(16, Duration::from_secs(60));
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    let lease_lost = std::sync::atomic::AtomicBool::new(true);
    let ctx = InstanceRunCtx {
        storage: &storage,
        handlers: &registry,
        webhook_config: &webhook_config,
        sequence_cache: &cache,
        externalize_threshold: 0,
        max_steps_per_instance: 0,
        cancel: &cancel,
        clock: &clock,
        lease_lost: &lease_lost,
        claim_token: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
    };

    let finished = execute_step_loop(&ctx, &instance, &sequence, &blocks, Vec::new())
        .await
        .unwrap();
    assert!(!finished);
    assert!(
        storage
            .get_block_output(instance_id, &BlockId::new("s1"))
            .await
            .unwrap()
            .is_none(),
        "no step may start after lease loss"
    );
    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(
        refreshed.state,
        InstanceState::Running,
        "the new owner's claim must be left alone"
    );
}

/// A Parallel with one branch `[first, second]`: forces the tree path and
/// runs the two steps in separate evaluator iterations.
fn two_step_composite_sequence() -> SequenceDefinition {
    mk_sequence(vec![BlockDefinition::Parallel(Box::new(
        orch8_types::sequence::ParallelDef {
            id: BlockId::new("par"),
            branches: vec![vec![
                BlockDefinition::Step(Box::new(mk_step_def(
                    "first",
                    "trip_lease",
                    serde_json::json!({}),
                ))),
                BlockDefinition::Step(Box::new(mk_step_def(
                    "second",
                    "trip_lease",
                    serde_json::json!({}),
                ))),
            ]],
        },
    ))])
}

async fn run_tree_with_lease_flag(
    storage: &Arc<dyn StorageBackend>,
    instance: &TaskInstance,
    registry: &HandlerRegistry,
    lease_lost: &std::sync::atomic::AtomicBool,
    claim_token: Option<chrono::DateTime<chrono::Utc>>,
) {
    let webhook_config = WebhookConfig::default();
    let cache = crate::sequence_cache::SequenceCache::new(16, Duration::from_secs(60));
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    let ctx = InstanceRunCtx {
        storage,
        handlers: registry,
        webhook_config: &webhook_config,
        sequence_cache: &cache,
        externalize_threshold: 0,
        max_steps_per_instance: 0,
        cancel: &cancel,
        clock: &clock,
        lease_lost,
        claim_token,
    };
    process_instance_tree(&ctx, instance, &two_step_composite_sequence())
        .await
        .unwrap();
}

/// GAP-005 (L6-01 follow-up): the lease-lost fence also covers composite
/// workflows. A flag already set when the tree run starts dispatches nothing
/// and hands the instance back.
#[tokio::test]
async fn lease_lost_stops_tree_path_before_first_step() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register("trip_lease", |_ctx| async { Ok(serde_json::json!({})) });
    let lease_lost = std::sync::atomic::AtomicBool::new(true);

    run_tree_with_lease_flag(&storage, &instance, &registry, &lease_lost, None).await;

    assert!(
        storage
            .get_block_output(instance_id, &BlockId::new("first"))
            .await
            .unwrap()
            .is_none(),
        "no step may start after lease loss"
    );
    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// GAP-005: a lease lost *while* the evaluator is running (the heartbeat
/// flips the flag mid-tick) stops it before the next step is dispatched.
#[tokio::test]
async fn lease_lost_mid_tree_evaluation_stops_before_next_step() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    let lease_lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut registry = HandlerRegistry::new();
    let flag = Arc::clone(&lease_lost);
    registry.register("trip_lease", move |_ctx| {
        flag.store(true, std::sync::atomic::Ordering::Release);
        async { Ok(serde_json::json!({})) }
    });

    run_tree_with_lease_flag(&storage, &instance, &registry, &lease_lost, None).await;

    assert!(
        storage
            .get_block_output(instance_id, &BlockId::new("second"))
            .await
            .unwrap()
            .is_none(),
        "the step after the lease loss must not start"
    );
    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// GAP-005: when another claim already owns the row, the tree path stops
/// without releasing the instance (mirrors the flat-loop rule).
#[tokio::test]
async fn lease_lost_to_other_claim_leaves_tree_instance_running() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register("trip_lease", |_ctx| async { Ok(serde_json::json!({})) });
    let lease_lost = std::sync::atomic::AtomicBool::new(true);
    let stale_token = Some(chrono::Utc::now() - chrono::Duration::hours(1));

    run_tree_with_lease_flag(&storage, &instance, &registry, &lease_lost, stale_token).await;

    let refreshed = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Running);
}

/// A composite iteration reset clears the (otherwise final) delay marker of
/// its step descendants, so a delayed loop-body step waits every iteration.
#[tokio::test]
async fn subtree_reset_clears_step_delay_markers() {
    use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
    use orch8_types::ids::ExecutionNodeId;

    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), instance_id, None, InstanceState::Running).await;
    let key = delay_marker_key(&BlockId::new("body"));
    storage
        .merge_instance_metadata(
            instance_id,
            &serde_json::json!({ key.clone(): "2020-01-01T00:00:00Z" }),
        )
        .await
        .unwrap();
    let node = |block: &str, parent, kind| ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id,
        block_id: BlockId::new(block),
        parent_id: parent,
        block_type: kind,
        branch_index: None,
        state: NodeState::Completed,
        started_at: None,
        completed_at: None,
    };
    let root = node("loop", None, BlockType::Loop);
    let child = node("body", Some(root.id), BlockType::Step);
    storage
        .create_execution_nodes_batch(&[root.clone(), child.clone()])
        .await
        .unwrap();

    crate::evaluator::reset_subtree_to_pending(
        storage.as_ref(),
        &[root.clone(), child],
        &TenantId::unchecked("t"),
        instance_id,
        root.id,
    )
    .await
    .unwrap();

    let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
    assert!(
        inst.metadata.get(&key).and_then(|v| v.as_str()).is_none(),
        "delay marker must be cleared for the next iteration"
    );
}

// ------------------------------------------------------------------
// Jev decisions: confidence-gated human gates + decision-driven routing
// ------------------------------------------------------------------

fn gate_step(base_url: &str, threshold: f64) -> StepDef {
    let mut step = mk_step_def("approve", "noop", serde_json::json!({}));
    step.wait_for_input = Some(orch8_types::sequence::HumanInputDef {
        prompt: "Refund this order?".into(),
        timeout: None,
        escalation_handler: None,
        choices: Some(vec![
            orch8_types::sequence::HumanChoice {
                label: "Approve".into(),
                value: "approve".into(),
            },
            orch8_types::sequence::HumanChoice {
                label: "Reject".into(),
                value: "reject".into(),
            },
        ]),
        store_as: Some("decision".into()),
        allow_comment: false,
        auto_decide: Some(Box::new(orch8_types::sequence::AutoDecideDef {
            threshold,
            instructions: None,
            model: None,
            api_key: Some("test-key".into()),
            base_url: Some(base_url.into()),
            interpret_replies: true,
        })),
    });
    step
}

async fn gate_instance(storage: &dyn StorageBackend) -> TaskInstance {
    let id = InstanceId::new();
    seed_instance_with_context(
        storage,
        id,
        ExecutionContext {
            data: serde_json::json!({"order": {"amount": 12, "reason": "damaged"}}),
            ..Default::default()
        },
    )
    .await;
    storage.get_instance(id).await.unwrap().unwrap()
}

#[tokio::test]
async fn auto_decide_accepts_gate_when_confident() {
    use crate::handlers::jev::tests::{choice_response, mock_jev};
    let (base, seen, _) = mock_jev(200, choice_response("approve", 0.97)).await;
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance = gate_instance(storage.as_ref()).await;
    let step = gate_step(&base, 0.9);
    let human = step.wait_for_input.clone().unwrap();

    let deferred = check_human_input_at(storage.as_ref(), &instance, &step, &human, Utc::now())
        .await
        .unwrap();
    assert!(!deferred, "confident model decision must not park the step");

    let out = storage
        .get_block_output(instance.id, &BlockId::new("approve"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output["value"], "approve");
    assert_eq!(out.output["_decided_by"], "jev");
    assert_eq!(out.output["_auto_decide"]["confidence"], 0.97);
    let stored = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(stored.context.data["decision"], "approve");

    // The gate choices become the Choice criteria; the instance data is the state.
    let sent = seen.lock().await;
    let q = &sent[0]["questions"]["decision"];
    assert_eq!(q["type"], "choice");
    assert_eq!(q["criteria"]["approve"], "Approve");
    assert_eq!(q["instructions"], "Refund this order?");
    assert_eq!(sent[0]["state"]["order"]["reason"], "damaged");
}

#[tokio::test]
async fn auto_decide_below_threshold_parks_once_and_labels_the_human_answer() {
    use crate::handlers::jev::tests::{choice_response, mock_jev};
    let (base, _, hits) = mock_jev(200, choice_response("approve", 0.55)).await;
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance = gate_instance(storage.as_ref()).await;
    let step = gate_step(&base, 0.9);
    let human = step.wait_for_input.clone().unwrap();

    assert!(
        check_human_input_at(storage.as_ref(), &instance, &step, &human, Utc::now())
            .await
            .unwrap(),
        "low confidence must park for a human"
    );
    // A later wake must not re-ask the model.
    let instance = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert!(
        check_human_input_at(storage.as_ref(), &instance, &step, &human, Utc::now())
            .await
            .unwrap()
    );
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);

    // The human disagrees; the acceptance row keeps the model's prediction.
    storage
        .enqueue_signal(&Signal {
            id: Uuid::now_v7(),
            instance_id: instance.id,
            signal_type: SignalType::Custom("human_input:approve".into()),
            payload: serde_json::json!({"value": "reject"}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    assert!(
        !check_human_input_at(storage.as_ref(), &instance, &step, &human, Utc::now())
            .await
            .unwrap()
    );
    let out = storage
        .get_block_output(instance.id, &BlockId::new("approve"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output["value"], "reject");
    assert_eq!(out.output["_decided_by"], "human");
    assert_eq!(
        out.output["_auto_decide"]["prediction"]["choice"],
        "approve"
    );
    assert_eq!(out.output["_auto_decide"]["agreed"], false);
}

#[tokio::test]
async fn auto_decide_fails_safe_to_a_human_when_the_model_is_down() {
    use crate::handlers::jev::tests::mock_jev;
    let (base, _, _) = mock_jev(529, serde_json::json!({"error": "overloaded"})).await;
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance = gate_instance(storage.as_ref()).await;
    let step = gate_step(&base, 0.9);
    let human = step.wait_for_input.clone().unwrap();

    assert!(
        check_human_input_at(storage.as_ref(), &instance, &step, &human, Utc::now())
            .await
            .unwrap(),
        "an unavailable model must never block or auto-decide the gate"
    );
    assert!(
        storage
            .get_block_output(instance.id, &BlockId::new("approve"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn free_text_reply_is_interpreted_onto_a_choice() {
    use crate::handlers::jev::tests::{choice_response, mock_jev};
    let (base, seen, _) = mock_jev(200, choice_response("approve", 0.95)).await;
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let instance = gate_instance(storage.as_ref()).await;
    // Mark the model as already consulted so only the reply path calls it.
    storage
        .merge_instance_metadata(
            instance.id,
            &serde_json::json!({ super::step_exec::auto_decide_marker_key(&BlockId::new("approve")):
                {"choice": "reject", "confidence": 0.4} }),
        )
        .await
        .unwrap();
    let instance = storage.get_instance(instance.id).await.unwrap().unwrap();
    storage
        .enqueue_signal(&Signal {
            id: Uuid::now_v7(),
            instance_id: instance.id,
            signal_type: SignalType::Custom("human_input:approve".into()),
            payload: serde_json::json!({"text": "yeah sure, refund them"}),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    let step = gate_step(&base, 0.9);
    let human = step.wait_for_input.clone().unwrap();

    assert!(
        !check_human_input_at(storage.as_ref(), &instance, &step, &human, Utc::now())
            .await
            .unwrap()
    );
    let out = storage
        .get_block_output(instance.id, &BlockId::new("approve"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output["value"], "approve");
    assert_eq!(out.output["_decided_by"], "human_reply");
    assert_eq!(
        seen.lock().await[0]["state"]["reply"],
        "yeah sure, refund them"
    );
}

/// Decision-driven control flow: a `jev` step's typed answer drives a
/// router branch and a `when` guard, and its choice can template another
/// step's params (e.g. an `llm_call` model).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn jev_step_output_drives_router_and_when_guard() {
    use crate::handlers::jev::tests::mock_jev;
    let (base, _, _) = mock_jev(
        200,
        serde_json::json!({
            "model": "jev-1.13.0",
            "answers": {
                "team": {"type": "choice", "choice": "billing",
                         "probabilities": {"billing": 0.9, "technical": 0.1}, "confidence": 0.86},
                "urgent": {"type": "noul", "noul": 0.2}
            },
            "usage": {"input_tokens": 30, "output_tokens": 10}
        }),
    )
    .await;
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let triage = mk_step_def(
        "triage",
        "jev",
        serde_json::json!({
            "base_url": base,
            "api_key": "test-key",
            "state": "{{context.data.ticket}}",
            "questions": {
                "team": {"type": "choice", "instructions": "Which team?",
                         "criteria": {"billing": "Payments", "technical": "Bugs"}},
                "urgent": {"type": "noul", "instructions": "Is this urgent?"}
            }
        }),
    );
    let mut page = mk_step_def("page_oncall", "noop", serde_json::json!({}));
    page.when = Some("outputs.triage.answers.urgent.noul >= 0.5".into());
    let billing = mk_step_def(
        "billing",
        "transform",
        serde_json::json!({"output": {"model": "{{outputs.triage.answers.team.choice}}"}}),
    );
    let technical = mk_step_def("technical", "noop", serde_json::json!({}));
    let router = BlockDefinition::Router(Box::new(orch8_types::sequence::RouterDef {
        id: BlockId::new("route"),
        routes: vec![orch8_types::sequence::Route {
            condition: "outputs.triage.answers.team.choice == \"billing\" && outputs.triage.answers.team.confidence >= 0.8".into(),
            blocks: vec![BlockDefinition::Step(Box::new(billing))],
        }],
        default: Some(vec![BlockDefinition::Step(Box::new(technical))]),
    }));
    let seq = mk_sequence(vec![
        BlockDefinition::Step(Box::new(triage)),
        BlockDefinition::Step(Box::new(page)),
        router,
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let now = Utc::now();
    let instance = TaskInstance {
        sub_tenant: None,
        id: InstanceId::new(),
        sequence_id: seq.id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext {
            data: serde_json::json!({"ticket": "My payouts have been failing for 3 days"}),
            ..Default::default()
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&instance).await.unwrap();

    let mut registry = HandlerRegistry::new();
    crate::handlers::builtin::register_builtins(&mut registry);
    let registry = Arc::new(registry);
    let config = orch8_types::config::SchedulerConfig::default();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
    let cache = Arc::new(crate::sequence_cache::SequenceCache::new(
        16,
        Duration::from_secs(60),
    ));
    let cancel = CancellationToken::new();
    for _ in 0..8 {
        tick_once(&storage, &registry, &semaphore, &config, &cache, &cancel)
            .await
            .unwrap();
        let state = storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state;
        if state.is_terminal() {
            break;
        }
        // Keep re-arming in case a pass parked the instance briefly.
        let _ = storage
            .conditional_update_instance_state(
                instance.id,
                InstanceState::Scheduled,
                InstanceState::Scheduled,
                Some(Utc::now()),
            )
            .await;
    }
    let done = storage.get_instance(instance.id).await.unwrap().unwrap();
    assert_eq!(done.state, InstanceState::Completed);

    let tree = storage.get_execution_tree(instance.id).await.unwrap();
    let state_of = |b: &str| {
        tree.iter()
            .find(|n| n.block_id.as_str() == b)
            .map(|n| n.state)
    };
    assert_eq!(
        state_of("billing"),
        Some(orch8_types::execution::NodeState::Completed)
    );
    assert_ne!(
        state_of("technical"),
        Some(orch8_types::execution::NodeState::Completed)
    );
    assert_eq!(
        state_of("page_oncall"),
        Some(orch8_types::execution::NodeState::Skipped),
        "low urgency must skip the on-call page"
    );
    let billing_out = storage
        .get_block_output(instance.id, &BlockId::new("billing"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(billing_out.output["output"]["model"], "billing");
}

// ------------------------------------------------------------------
// Continuity fencing: the scheduler never advances an instance whose
// execution is transferring or owned elsewhere. SQLite always; Postgres
// too when DATABASE_URL is set.
// ------------------------------------------------------------------

/// `SQLite` plus Postgres (fresh schema per call; skipped locally without
/// `DATABASE_URL`, required under CI).
async fn fencing_backends() -> Vec<orch8_storage::testing::backends::TestBackend> {
    orch8_storage::testing::backends::TestBackend::sqlite_and_postgres().await
}

async fn run_process_instance(storage: &Arc<dyn StorageBackend>, instance: TaskInstance) {
    let registry = HandlerRegistry::new();
    let webhook_config = WebhookConfig::default();
    let cache = crate::sequence_cache::SequenceCache::new(16, Duration::from_secs(60));
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    let lease_lost = std::sync::atomic::AtomicBool::new(false);
    let ctx = InstanceRunCtx {
        storage,
        handlers: &registry,
        webhook_config: &webhook_config,
        sequence_cache: &cache,
        externalize_threshold: 0,
        max_steps_per_instance: 0,
        cancel: &cancel,
        clock: &clock,
        lease_lost: &lease_lost,
        claim_token: None,
    };
    process_instance(
        &ctx,
        instance,
        PrefetchedData {
            signals: Vec::new(),
            completed_block_ids: Vec::new(),
        },
    )
    .await
    .unwrap();
}

async fn seed_fenced_instance(
    storage: &Arc<dyn StorageBackend>,
    state: orch8_types::continuity::OwnershipState,
    superseded: bool,
) -> (InstanceId, SequenceDefinition) {
    use orch8_types::continuity::{ContinuityExecution, ContinuityId, ExecutionEpoch, RuntimeId};
    let blocks = vec![BlockDefinition::Step(Box::new(mk_step_def(
        "s1",
        "noop",
        serde_json::json!({}),
    )))];
    let mut sequence = mk_sequence(blocks);
    sequence.name = format!("fenced-{}", Uuid::now_v7());
    storage.create_sequence(&sequence).await.unwrap();
    let instance_id = InstanceId::new();
    let now = Utc::now();
    storage
        .create_instance(&TaskInstance {
            sub_tenant: None,
            id: instance_id,
            sequence_id: sequence.id,
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            state: InstanceState::Running,
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
        })
        .await
        .unwrap();

    let tenant = TenantId::unchecked("t");
    let execution = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: tenant.clone(),
        current_instance_id: instance_id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: orch8_types::continuity::OwnershipState::Owned,
        updated_at: now,
    };
    storage
        .create_continuity_execution(&execution)
        .await
        .unwrap();
    let mut next = execution.clone();
    next.state = state;
    if superseded {
        // Another instance (the imported destination) now owns it.
        let mut destination_instance = storage.get_instance(instance_id).await.unwrap().unwrap();
        let destination = InstanceId::new();
        destination_instance.id = destination;
        destination_instance.state = InstanceState::Paused;
        storage
            .create_instance(&destination_instance)
            .await
            .unwrap();
        next.current_instance_id = destination;
        next.epoch = ExecutionEpoch::from_u64(1);
        next.owner_runtime_id = RuntimeId::new();
    }
    assert!(
        storage
            .cas_continuity_owner(
                &tenant,
                execution.continuity_id,
                execution.epoch,
                execution.owner_runtime_id,
                &next,
            )
            .await
            .unwrap()
    );
    (instance_id, sequence)
}

#[tokio::test]
async fn scheduler_defers_instance_whose_execution_is_transferring() {
    for test_backend in fencing_backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
        let (instance_id, _) = seed_fenced_instance(
            &storage,
            orch8_types::continuity::OwnershipState::Transferring,
            false,
        )
        .await;
        let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
        Box::pin(run_process_instance(&storage, instance)).await;
        let after = storage.get_instance(instance_id).await.unwrap().unwrap();
        assert_eq!(after.state, InstanceState::Scheduled, "{backend}");
        assert!(after.next_fire_at.unwrap() > Utc::now(), "{backend}");
        assert!(
            storage
                .get_block_output(instance_id, &BlockId::new("s1"))
                .await
                .unwrap()
                .is_none(),
            "{backend}: no step ran while transferring"
        );
    }
}

#[tokio::test]
async fn scheduler_parks_instance_superseded_by_a_handoff() {
    for test_backend in fencing_backends().await {
        let (backend, storage) = (test_backend.name(), Arc::clone(&test_backend.storage));
        let (instance_id, _) = seed_fenced_instance(
            &storage,
            orch8_types::continuity::OwnershipState::Owned,
            true,
        )
        .await;
        let instance = storage.get_instance(instance_id).await.unwrap().unwrap();
        Box::pin(run_process_instance(&storage, instance)).await;
        let after = storage.get_instance(instance_id).await.unwrap().unwrap();
        assert_eq!(after.state, InstanceState::Waiting, "{backend}");
        assert!(
            storage
                .get_block_output(instance_id, &BlockId::new("s1"))
                .await
                .unwrap()
                .is_none(),
            "{backend}: superseded source never advances"
        );
    }
}

/// L2-01: a panicking cleanup/escalation hook is isolated on its own task
/// and reported as a swallowed hook failure instead of unwinding the tick.
#[tokio::test]
async fn panicking_hook_is_isolated_and_reported_as_failure() {
    let result = dispatch_hook_bounded(async {
        let boom: Result<serde_json::Value, orch8_types::error::StepError> =
            Err(orch8_types::error::StepError::Permanent {
                message: String::new(),
                details: None,
            });
        assert!(boom.is_ok(), "tenant hook bug");
        boom
    })
    .await;
    match result {
        Err(orch8_types::error::StepError::Permanent { message, .. }) => {
            assert!(message.contains("panicked"), "{message}");
        }
        other => panic!("expected a permanent hook failure, got {other:?}"),
    }
}

/// Seed a sub-sequence parent (with a `Waiting` `SubSequence` node "sub")
/// and one child in `child_state` linked to that node.
async fn seed_sub_sequence_parent(
    storage: &Arc<dyn StorageBackend>,
    parent_state: InstanceState,
    child_state: InstanceState,
) -> (TaskInstance, SequenceDefinition, InstanceId) {
    let seq = mk_sequence(vec![BlockDefinition::SubSequence(Box::new(
        orch8_types::sequence::SubSequenceDef {
            id: BlockId::new("sub"),
            sequence_name: "child".into(),
            version: None,
            input: serde_json::json!({}),
        },
    ))]);
    let parent_id = InstanceId::new();
    let child_id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), parent_id, None, parent_state).await;
    let mut child = storage.get_instance(parent_id).await.unwrap().unwrap();
    child.id = child_id;
    child.parent_instance_id = Some(parent_id);
    child.state = child_state;
    child.metadata = serde_json::json!({ "_parent_block_id": "sub" });
    storage.create_instance(&child).await.unwrap();
    storage
        .create_execution_node(&orch8_types::execution::ExecutionNode {
            id: orch8_types::ids::ExecutionNodeId::new(),
            instance_id: parent_id,
            block_id: BlockId::new("sub"),
            parent_id: None,
            block_type: orch8_types::execution::BlockType::SubSequence,
            branch_index: None,
            state: orch8_types::execution::NodeState::Waiting,
            started_at: Some(Utc::now()),
            completed_at: None,
        })
        .await
        .unwrap();
    let parent = storage.get_instance(parent_id).await.unwrap().unwrap();
    (parent, seq, child_id)
}

async fn run_more_work(
    storage: &Arc<dyn StorageBackend>,
    parent: &TaskInstance,
    seq: &SequenceDefinition,
) {
    run_more_work_with(storage, parent, seq, true).await;
}

async fn run_more_work_with(
    storage: &Arc<dyn StorageBackend>,
    parent: &TaskInstance,
    seq: &SequenceDefinition,
    has_waiting_nodes: bool,
) {
    let registry = HandlerRegistry::new();
    let webhook_config = WebhookConfig::default();
    let cache = crate::sequence_cache::SequenceCache::new(16, Duration::from_secs(60));
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    let lease_lost = std::sync::atomic::AtomicBool::new(false);
    let ctx = InstanceRunCtx {
        storage,
        handlers: &registry,
        webhook_config: &webhook_config,
        sequence_cache: &cache,
        externalize_threshold: 0,
        max_steps_per_instance: 0,
        cancel: &cancel,
        clock: &clock,
        lease_lost: &lease_lost,
        claim_token: None,
    };
    handle_tree_more_work(&ctx, parent, seq, has_waiting_nodes)
        .await
        .unwrap();
}

/// L3-01: the child finished while the parent was still `Running`, so its
/// wake was dropped. Parking must re-check the awaited child and re-schedule
/// the parent instead of leaving it `Waiting` forever.
#[tokio::test]
async fn parking_parent_rechecks_already_finished_sub_sequence_child() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (parent, seq, _) =
        seed_sub_sequence_parent(&storage, InstanceState::Running, InstanceState::Completed).await;
    run_more_work(&storage, &parent, &seq).await;
    let after = storage.get_instance(parent.id).await.unwrap().unwrap();
    assert_eq!(after.state, InstanceState::Scheduled);

    // A child that is still running keeps the parent parked.
    let (parent, seq, _) =
        seed_sub_sequence_parent(&storage, InstanceState::Running, InstanceState::Running).await;
    run_more_work(&storage, &parent, &seq).await;
    let after = storage.get_instance(parent.id).await.unwrap().unwrap();
    assert_eq!(after.state, InstanceState::Waiting);
}

/// L3-01 safety net: a wake that found the parent `Running` is retried by
/// the tick once the parent has parked.
#[tokio::test]
async fn dropped_parent_wake_is_retried_after_parent_parks() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (parent, _, child_id) =
        seed_sub_sequence_parent(&storage, InstanceState::Running, InstanceState::Completed).await;
    let child = storage.get_instance(child_id).await.unwrap().unwrap();
    wake_parent_if_child(storage.as_ref(), &child).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Running
    );
    // The parent parks without having seen the child (the lost wake-up).
    assert!(
        storage
            .conditional_update_instance_state(
                parent.id,
                InstanceState::Running,
                InstanceState::Waiting,
                None,
            )
            .await
            .unwrap()
    );
    tokio::time::sleep(DEFERRED_WAKE_RETRY + Duration::from_millis(100)).await;
    retry_deferred_parent_wakes(&storage).await;
    assert_eq!(
        storage
            .get_instance(parent.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
}

fn gate_step_with_timeout(id: &str, timeout: std::time::Duration) -> StepDef {
    let mut step = mk_step_def(id, "noop", serde_json::json!({}));
    step.wait_for_input = Some(orch8_types::sequence::HumanInputDef {
        prompt: "approve?".into(),
        timeout: Some(timeout),
        escalation_handler: None,
        choices: None,
        store_as: None,
        allow_comment: false,
        auto_decide: None,
    });
    step
}

async fn sweep_waiting(
    storage: &Arc<dyn StorageBackend>,
    cursor: &mut WaitingSweepCursor,
    batch_size: u32,
    max_pages: u32,
) {
    let handlers = HandlerRegistry::new();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let webhook_config = WebhookConfig::default();
    let cancel = CancellationToken::new();
    let clock = SharedClock::default();
    process_waiting_deadlines(
        &SweepContext {
            storage,
            handlers: &handlers,
            sequence_cache: &cache,
            webhook_config: &webhook_config,
            cancel: &cancel,
            batch_size,
            clock: &clock,
            hooks: &HookTasks::default(),
        },
        cursor,
        max_pages,
    )
    .await
    .unwrap();
}

/// L6-04: the Waiting sweep used to restart at offset 0 every tick and stop
/// after a fixed page count, so instances sorted past the cap never had
/// their timeouts checked. The cursor rotates the window across ticks.
#[tokio::test]
async fn waiting_sweep_rotates_past_the_page_cap() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![BlockDefinition::Step(Box::new(
        gate_step_with_timeout("gate", std::time::Duration::from_secs(3600)),
    ))]);
    storage.create_sequence(&seq).await.unwrap();
    // Three waiters on the gate; only the last-sorted one has expired.
    let mut ids = Vec::new();
    for (age_hours, sort_hours) in [(0, 3), (0, 2), (2, 1)] {
        let mut inst = mk_waiting_instance(
            seq.id,
            chrono::Duration::hours(3),
            Utc::now() - chrono::Duration::hours(sort_hours),
        );
        inst.context.runtime.current_step = Some(BlockId::new("gate"));
        inst.context.runtime.current_step_started_at =
            Some(Utc::now() - chrono::Duration::hours(age_hours));
        storage.create_instance(&inst).await.unwrap();
        ids.push(inst.id);
    }
    let state = |id: InstanceId| {
        let storage = Arc::clone(&storage);
        async move { storage.get_instance(id).await.unwrap().unwrap().state }
    };

    // One page of one row per tick: the old fixed window only ever saw ids[0].
    let mut cursor = WaitingSweepCursor::default();
    sweep_waiting(&storage, &mut cursor, 1, 1).await;
    sweep_waiting(&storage, &mut cursor, 1, 1).await;
    assert_eq!(state(ids[2]).await, InstanceState::Waiting);
    sweep_waiting(&storage, &mut cursor, 1, 1).await;
    assert_eq!(
        state(ids[2]).await,
        InstanceState::Scheduled,
        "third tick reaches the instance past the page cap"
    );
    assert_eq!(state(ids[0]).await, InstanceState::Waiting);
    assert_eq!(state(ids[1]).await, InstanceState::Waiting);
}

/// L3-02: a `wait_for_input.timeout` on a step nested in a composite fires
/// while the instance is `Waiting` (the sweep wakes it so the evaluator's
/// gate-timeout path runs); an unexpired nested gate stays parked.
#[tokio::test]
async fn waiting_sweep_wakes_instance_on_expired_nested_gate_timeout() {
    use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(vec![BlockDefinition::Parallel(Box::new(
        orch8_types::sequence::ParallelDef {
            id: BlockId::new("par"),
            branches: vec![
                vec![BlockDefinition::Step(Box::new(gate_step_with_timeout(
                    "gate",
                    std::time::Duration::from_secs(1),
                )))],
                vec![BlockDefinition::Step(Box::new(mk_step_def(
                    "other",
                    "noop",
                    serde_json::json!({}),
                )))],
            ],
        },
    ))]);
    storage.create_sequence(&seq).await.unwrap();

    let mut seeded = Vec::new();
    for gate_started in [Utc::now() - chrono::Duration::hours(1), Utc::now()] {
        let inst = mk_waiting_instance(seq.id, chrono::Duration::hours(1), Utc::now());
        storage.create_instance(&inst).await.unwrap();
        let root = ExecutionNode {
            id: orch8_types::ids::ExecutionNodeId::new(),
            instance_id: inst.id,
            block_id: BlockId::new("par"),
            parent_id: None,
            block_type: BlockType::Parallel,
            branch_index: None,
            state: NodeState::Running,
            started_at: Some(gate_started),
            completed_at: None,
        };
        let gate = ExecutionNode {
            id: orch8_types::ids::ExecutionNodeId::new(),
            instance_id: inst.id,
            block_id: BlockId::new("gate"),
            parent_id: Some(root.id),
            block_type: BlockType::Step,
            branch_index: Some(0),
            state: NodeState::Waiting,
            started_at: Some(gate_started),
            completed_at: None,
        };
        storage.create_execution_node(&root).await.unwrap();
        storage.create_execution_node(&gate).await.unwrap();
        seeded.push(inst.id);
    }

    sweep_waiting(&storage, &mut WaitingSweepCursor::default(), 10, 1).await;
    let expired = storage.get_instance(seeded[0]).await.unwrap().unwrap();
    assert_eq!(expired.state, InstanceState::Scheduled);
    let fresh = storage.get_instance(seeded[1]).await.unwrap().unwrap();
    assert_eq!(fresh.state, InstanceState::Waiting);
}

/// L2-17: transient reschedules back off exponentially (capped) and the
/// instance is failed only after both the attempt cap and the minimum age.
#[test]
fn transient_retry_policy_backs_off_and_eventually_exhausts() {
    use transient_retries::{Decision, MAX_ATTEMPTS, MIN_AGE, decide};
    let delay = |attempts| match decide(attempts, Duration::ZERO) {
        Decision::Retry(d) => d,
        Decision::Exhausted { .. } => panic!("must not exhaust before MIN_AGE"),
    };
    let first = delay(1);
    assert!(first >= Duration::from_millis(160) && first <= Duration::from_millis(240));
    assert!(delay(4) > delay(1));
    assert!(
        delay(MAX_ATTEMPTS + 5) <= Duration::from_secs(72),
        "capped at 60s + jitter"
    );
    assert!(matches!(
        decide(MAX_ATTEMPTS - 1, MIN_AGE * 2),
        Decision::Retry(_)
    ));
    assert_eq!(
        decide(MAX_ATTEMPTS, MIN_AGE),
        Decision::Exhausted {
            attempts: MAX_ATTEMPTS
        }
    );
}

/// D-08: the transient-retry map is hard-bounded by evicting the oldest
/// entries, even when none is older than the stale window.
#[test]
fn transient_retry_map_is_hard_bounded() {
    use std::collections::HashMap;
    let now = std::time::Instant::now();
    let mut map = HashMap::new();
    for i in 0..100u64 {
        map.insert(
            InstanceId::new(),
            (1u32, now.checked_sub(Duration::from_secs(i)).unwrap()),
        );
    }
    transient_retries::enforce_bounds(&mut map, now, 50, 100);
    assert_eq!(map.len(), 90, "oldest 10% evicted at the hard cap");
    assert!(
        map.values()
            .all(|(_, first)| now.duration_since(*first) < Duration::from_secs(90)),
        "the oldest entries are the ones evicted"
    );
}

/// D-08: at capacity a new deferred parent wake evicts the oldest entry
/// instead of being silently dropped.
#[test]
fn deferred_parent_wake_evicts_oldest_at_cap() {
    let now = std::time::Instant::now();
    let mut map = HashMap::new();
    let oldest = InstanceId::new();
    map.insert(
        oldest,
        (now.checked_sub(Duration::from_secs(100)).unwrap(), now),
    );
    for _ in 0..2 {
        map.insert(
            InstanceId::new(),
            (now.checked_sub(Duration::from_secs(1)).unwrap(), now),
        );
    }
    let newcomer = InstanceId::new();
    insert_bounded(&mut map, newcomer, now, 3);
    assert_eq!(map.len(), 3);
    assert!(map.contains_key(&newcomer), "new wake is kept");
    assert!(!map.contains_key(&oldest), "oldest wake is evicted");
}

/// L2-11: an instance whose signals cannot be applied (here: a context
/// written under a different encryption key, the H-10 class) must not block
/// signal delivery to the instances after it.
#[tokio::test]
async fn poison_instance_does_not_block_signal_sweep() {
    use orch8_storage::encrypting::EncryptingStorage;
    use orch8_types::encryption::FieldEncryptor;
    let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let old_key: Arc<dyn StorageBackend> = Arc::new(EncryptingStorage::new(
        Arc::clone(&raw),
        FieldEncryptor::from_hex_key(&"11".repeat(32)).unwrap(),
    ));
    let storage: Arc<dyn StorageBackend> = Arc::new(EncryptingStorage::new(
        Arc::clone(&raw),
        FieldEncryptor::from_hex_key(&"22".repeat(32)).unwrap(),
    ));
    let poison = InstanceId::new();
    let healthy = InstanceId::new();
    for (writer, id) in [(&old_key, poison), (&storage, healthy)] {
        seed_instance_in_state(writer.as_ref(), id, None, InstanceState::Paused).await;
        let mut inst = writer.get_instance(id).await.unwrap().unwrap();
        inst.context.data = serde_json::json!({ "secret": "s3cr3t" });
        writer
            .update_instance_context(id, &inst.context)
            .await
            .unwrap();
    }
    assert!(
        storage.get_instance(poison).await.is_err(),
        "precondition: the poison row is unreadable under the current key"
    );
    // The poison row's signal is older, so it sorts first in the sweep.
    for (id, age) in [(poison, 10), (healthy, 5)] {
        storage
            .enqueue_signal(&Signal {
                id: Uuid::now_v7(),
                instance_id: id,
                signal_type: SignalType::Resume,
                payload: serde_json::Value::Null,
                delivered: false,
                created_at: Utc::now() - chrono::Duration::seconds(age),
                delivered_at: None,
            })
            .await
            .unwrap();
    }

    let handlers = Arc::new(HandlerRegistry::new());
    let cache = SequenceCache::new(16, Duration::from_secs(60));
    let hooks = HookTasks::default();
    process_signalled_instances(
        &storage,
        &handlers,
        &cache,
        16,
        &SharedClock::default(),
        &hooks,
    )
    .await
    .unwrap();
    assert_eq!(
        storage.get_instance(healthy).await.unwrap().unwrap().state,
        InstanceState::Scheduled,
        "the healthy instance's resume is delivered despite the poison row"
    );
}

/// L2-06: an `on_cancel` hook against a hung target runs off the tick; the
/// signal sweep returns at once instead of waiting out the hook timeout.
#[tokio::test]
async fn cancel_hooks_run_off_the_signal_sweep() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mut seq = mk_sequence(vec![]);
    seq.on_cancel = Some(vec![BlockDefinition::Step(Box::new(mk_step_def(
        "cleanup",
        "hang",
        serde_json::json!({}),
    )))]);
    storage.create_sequence(&seq).await.unwrap();
    let mut inst = mk_waiting_instance(seq.id, chrono::Duration::minutes(1), Utc::now());
    inst.state = InstanceState::Paused;
    storage.create_instance(&inst).await.unwrap();
    storage
        .enqueue_signal(&Signal {
            id: Uuid::now_v7(),
            instance_id: inst.id,
            signal_type: SignalType::Cancel,
            payload: serde_json::Value::Null,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        })
        .await
        .unwrap();

    let mut registry = HandlerRegistry::new();
    registry.register("hang", |_ctx| async {
        std::future::pending::<()>().await;
        Ok(serde_json::json!({}))
    });
    let handlers = Arc::new(registry);
    let cache = SequenceCache::new(16, Duration::from_secs(60));
    let hooks = HookTasks::default();
    tokio::time::timeout(
        Duration::from_secs(5),
        process_signalled_instances(
            &storage,
            &handlers,
            &cache,
            16,
            &SharedClock::default(),
            &hooks,
        ),
    )
    .await
    .expect("the sweep must not wait for the hung hook")
    .unwrap();
    assert_eq!(
        storage.get_instance(inst.id).await.unwrap().unwrap().state,
        InstanceState::Cancelled
    );
    assert_eq!(
        hooks.pending(),
        1,
        "the on_cancel hook was spawned off the tick"
    );
}

/// L3-12: a tree pass that ends with more work must not resurrect an
/// instance a concurrent runner already completed.
#[tokio::test]
async fn tree_more_work_leaves_completed_instance_alone() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), id, None, InstanceState::Completed).await;
    let inst = storage.get_instance(id).await.unwrap().unwrap();
    run_more_work_with(&storage, &inst, &mk_sequence(vec![]), false).await;
    assert_eq!(
        storage.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Completed
    );
}

/// L3-13: a Pause signal for a `Waiting` instance pauses it (it used to be
/// marked delivered and dropped), and Resume re-schedules it.
#[tokio::test]
async fn pause_and_resume_a_waiting_instance() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let id = InstanceId::new();
    seed_instance_in_state(storage.as_ref(), id, None, InstanceState::Waiting).await;
    let handlers = Arc::new(HandlerRegistry::new());
    let cache = SequenceCache::new(16, Duration::from_secs(60));
    for (signal_type, expected) in [
        (SignalType::Pause, InstanceState::Paused),
        (SignalType::Resume, InstanceState::Scheduled),
    ] {
        storage
            .enqueue_signal(&Signal {
                id: Uuid::now_v7(),
                instance_id: id,
                signal_type,
                payload: serde_json::Value::Null,
                delivered: false,
                created_at: Utc::now(),
                delivered_at: None,
            })
            .await
            .unwrap();
        process_signalled_instances(
            &storage,
            &handlers,
            &cache,
            16,
            &SharedClock::default(),
            &HookTasks::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            storage.get_instance(id).await.unwrap().unwrap().state,
            expected
        );
    }
}

/// D-05: one tenant whose hook target hangs is capped at
/// `HOOK_PER_TENANT_CONCURRENCY` running hooks, so other tenants' hooks
/// still get a global permit.
#[tokio::test]
async fn hook_pool_caps_per_tenant_concurrency_and_does_not_starve_others() {
    use std::sync::atomic::AtomicUsize;

    let hooks = HookTasks::default();
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    for _ in 0..(HOOK_CONCURRENCY * 2) {
        let (running, peak, release) = (
            Arc::clone(&running),
            Arc::clone(&peak),
            Arc::clone(&release),
        );
        hooks.spawn("noisy", async move {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            release.notified().await;
            running.fetch_sub(1, Ordering::SeqCst);
        });
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    hooks.spawn("quiet", async move {
        let _ = tx.send(());
    });
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("a quiet tenant's hook must not wait behind the noisy tenant")
        .unwrap();
    assert_eq!(
        running.load(Ordering::SeqCst),
        HOOK_PER_TENANT_CONCURRENCY,
        "noisy tenant capped"
    );
    release.notify_waiters();
    // Waiters not yet registered miss notify_waiters; keep releasing until done.
    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            release.notify_waiters();
            if hooks.pending() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(drained.is_ok(), "all hooks finished");
    assert!(peak.load(Ordering::SeqCst) <= HOOK_PER_TENANT_CONCURRENCY);
}

/// D-05: the hook queue is bounded; overflow is dropped (and counted)
/// instead of queueing without limit.
#[tokio::test]
async fn hook_pool_drops_beyond_queue_capacity() {
    let hooks = HookTasks::default();
    for _ in 0..(HOOK_QUEUE_CAPACITY + 25) {
        hooks.spawn("t", std::future::pending::<()>());
    }
    assert_eq!(hooks.pending(), HOOK_QUEUE_CAPACITY);
    assert!(!hooks.drain(Duration::from_millis(50)).await, "hung hooks");
    // Aborted hooks release their queue slots.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(hooks.pending(), 0);
}

/// D-05: shutdown waits (bounded) for in-flight hooks.
#[tokio::test]
async fn hook_pool_drain_waits_for_running_hooks() {
    let hooks = HookTasks::default();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&done);
    hooks.spawn("t", async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        flag.store(true, Ordering::SeqCst);
    });
    assert!(hooks.drain(Duration::from_secs(5)).await);
    assert!(done.load(Ordering::SeqCst), "drain waited for the hook");
}
