//! Shared test helpers for orch8-engine integration tests.
#![allow(clippy::too_many_lines, dead_code, unused_imports)]

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};

use orch8_engine::evaluator::{self, EvalOutcome};
use orch8_engine::handlers::{builtin::register_builtins, HandlerRegistry};
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::config::{ExternalizationMode, SchedulerConfig, SecretString, WebhookConfig};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::error::StepError;
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{
    BlockDefinition, DelaySpec, EscalationDef, RetryPolicy, SendWindow, SequenceDefinition,
    SequenceStatus, StepDef,
};
use tokio::sync::Semaphore;

// ------------------------------------------------------------------
// Step constructors
// ------------------------------------------------------------------

pub fn mk_step(id: &str, handler: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
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
        cache_key: None,
    }))
}

pub fn mk_step_with_params(id: &str, handler: &str, params: Value) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
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
        cache_key: None,
    }))
}

pub fn mk_step_with_retry(id: &str, handler: &str, max_attempts: u32) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 1.0,
        }),
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
        cache_key: None,
    }))
}

pub fn mk_step_with_retry_backoff(
    id: &str,
    handler: &str,
    max_attempts: u32,
    multiplier: f64,
) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: Some(RetryPolicy {
            max_attempts,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(100),
            backoff_multiplier: multiplier,
        }),
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
        cache_key: None,
    }))
}

pub fn mk_step_with_timeout(id: &str, handler: &str, timeout_ms: u64) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: Some(Duration::from_millis(timeout_ms)),
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

pub fn mk_step_with_delay(id: &str, handler: &str, duration: Duration) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: Some(DelaySpec {
            duration,
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        }),
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
        cache_key: None,
    }))
}

pub fn mk_step_with_deadline(id: &str, handler: &str, deadline: Duration) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(deadline),
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

pub fn mk_step_with_deadline_and_escalation(
    id: &str,
    handler: &str,
    deadline: Duration,
    escalation_handler: &str,
) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: Some(deadline),
        on_deadline_breach: Some(EscalationDef {
            handler: escalation_handler.into(),
            params: json!({"alert": true}),
        }),
        fallback_handler: None,
        cache_key: None,
    }))
}

pub fn mk_step_with_fallback(id: &str, handler: &str, fallback: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
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
        fallback_handler: Some(fallback.into()),
        cache_key: None,
    }))
}

pub fn mk_step_with_rate_limit(id: &str, handler: &str, key: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: Some(key.into()),
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

pub fn mk_step_with_queue(id: &str, handler: &str, queue: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: Some(queue.into()),
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

pub fn mk_step_with_cache_key(id: &str, handler: &str, cache_key: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
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
        cache_key: Some(cache_key.into()),
    }))
}

pub fn mk_step_with_delay_spec(id: &str, handler: &str, delay: DelaySpec) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: Some(delay),
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
        cache_key: None,
    }))
}

pub fn mk_step_with_send_window(id: &str, handler: &str, window: SendWindow) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: Some(window),
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

pub fn mk_non_cancellable_step(id: &str, handler: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: false,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

// ------------------------------------------------------------------
// Sequence / instance constructors
// ------------------------------------------------------------------

pub fn mk_sequence(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: "test-flow".into(),
        version: 1,
        deprecated: false,
        blocks,
        interceptors: None,
        created_at: Utc::now(),
        status: SequenceStatus::Production,
    }
}

pub fn mk_instance(seq_id: SequenceId) -> TaskInstance {
    mk_instance_with_ctx(seq_id, json!({}))
}

pub fn mk_instance_with_ctx(seq_id: SequenceId, data: Value) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Running,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext {
            data,
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    }
}

pub fn mk_instance_with_priority(seq_id: SequenceId, priority: Priority) -> TaskInstance {
    let mut inst = mk_instance(seq_id);
    inst.priority = priority;
    inst
}

pub fn mk_instance_scheduled(seq_id: SequenceId, data: Value) -> TaskInstance {
    let mut inst = mk_instance_with_ctx(seq_id, data);
    inst.state = InstanceState::Scheduled;
    inst.next_fire_at = Some(Utc::now() - chrono::Duration::seconds(10));
    inst
}

pub fn mk_instance_in_state(seq_id: SequenceId, state: InstanceState) -> TaskInstance {
    let mut inst = mk_instance(seq_id);
    inst.state = state;
    inst.next_fire_at = if state == InstanceState::Scheduled {
        Some(Utc::now() - chrono::Duration::seconds(10))
    } else {
        None
    };
    inst.context = ExecutionContext::default();
    inst
}

pub fn mk_instance_with_concurrency(seq_id: SequenceId, key: &str, max: u32) -> TaskInstance {
    let mut inst = mk_instance_scheduled(seq_id, json!({}));
    inst.concurrency_key = Some(key.into());
    inst.max_concurrency = Some(max);
    inst
}

// ------------------------------------------------------------------
// Setup helpers
// ------------------------------------------------------------------

pub async fn setup(
    blocks: Vec<BlockDefinition>,
) -> (Arc<dyn StorageBackend>, SequenceDefinition, TaskInstance) {
    setup_with_ctx(blocks, json!({})).await
}

pub async fn setup_with_ctx(
    blocks: Vec<BlockDefinition>,
    ctx_data: Value,
) -> (Arc<dyn StorageBackend>, SequenceDefinition, TaskInstance) {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(blocks);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, ctx_data);
    storage.create_instance(&inst).await.unwrap();
    (storage, seq, inst)
}

// ------------------------------------------------------------------
// Drive helpers
// ------------------------------------------------------------------

pub async fn drive(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance_id: InstanceId,
    sequence: &SequenceDefinition,
) {
    for _ in 0..2000 {
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        match inst.state {
            InstanceState::Completed
            | InstanceState::Failed
            | InstanceState::Cancelled
            | InstanceState::Waiting
            | InstanceState::Paused => return,
            InstanceState::Scheduled => {
                storage
                    .update_instance_state(instance_id, InstanceState::Running, None)
                    .await
                    .unwrap();
            }
            _ => {}
        }
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        let outcome = evaluator::evaluate(storage, handlers, &inst, sequence)
            .await
            .unwrap();
        if let EvalOutcome::Done {
            any_failed,
            any_cancelled,
        } = outcome
        {
            let new_state = if any_cancelled && !any_failed {
                InstanceState::Cancelled
            } else if any_failed {
                InstanceState::Failed
            } else {
                InstanceState::Completed
            };
            storage
                .update_instance_state(instance_id, new_state, None)
                .await
                .unwrap();
            return;
        }
    }
    panic!("drive loop exceeded 2000 iterations");
}

pub async fn drive_n(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance_id: InstanceId,
    sequence: &SequenceDefinition,
    n: usize,
) {
    for _ in 0..n {
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        if !matches!(
            inst.state,
            InstanceState::Running | InstanceState::Scheduled
        ) {
            return;
        }
        if inst.state == InstanceState::Scheduled {
            storage
                .update_instance_state(instance_id, InstanceState::Running, None)
                .await
                .unwrap();
        }
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        let outcome = evaluator::evaluate(storage, handlers, &inst, sequence)
            .await
            .unwrap();
        if matches!(outcome, EvalOutcome::Done { .. }) {
            return;
        }
    }
}

// ------------------------------------------------------------------
// Tree inspection
// ------------------------------------------------------------------

pub fn node_state(tree: &[orch8_types::execution::ExecutionNode], block_id: &str) -> NodeState {
    tree.iter()
        .find(|n| n.block_id.as_str() == block_id)
        .unwrap_or_else(|| panic!("node '{block_id}' not found in tree"))
        .state
}

// ------------------------------------------------------------------
// Handler registries
// ------------------------------------------------------------------

pub fn registry() -> HandlerRegistry {
    let mut reg = HandlerRegistry::new();
    register_builtins(&mut reg);
    reg
}

pub fn registry_with_fail() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("fail", |_ctx| {
        Box::pin(async {
            Err(StepError::Permanent {
                message: "intentional failure".into(),
                details: None,
            })
        })
    });
    reg
}

pub fn registry_with_always_fail() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("always_fail", |_ctx| {
        Box::pin(async {
            Err(StepError::Permanent {
                message: "forced fail".into(),
                details: None,
            })
        })
    });
    reg
}

pub fn registry_with_retryable_fail() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("retryable_fail", |_ctx| {
        Box::pin(async {
            Err(StepError::Retryable {
                message: "transient".into(),
                details: None,
            })
        })
    });
    reg
}

pub fn registry_with_flaky(
    fail_count: u32,
) -> (HandlerRegistry, Arc<std::sync::atomic::AtomicU32>) {
    let mut reg = registry();
    let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);
    reg.register("flaky", move |_ctx| {
        let counter = Arc::clone(&counter_clone);
        Box::pin(async move {
            let attempt = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt < fail_count {
                Err(StepError::Retryable {
                    message: format!("flaky failure attempt {}", attempt + 1),
                    details: None,
                })
            } else {
                Ok(json!({"attempt": attempt + 1}))
            }
        })
    });
    (reg, counter)
}

pub fn registry_with_output() -> HandlerRegistry {
    let mut reg = registry();
    reg.register("produce_output", |ctx| {
        Box::pin(async move {
            let value = ctx
                .params
                .get("output")
                .cloned()
                .unwrap_or_else(|| json!({"result": "produced"}));
            Ok(value)
        })
    });
    reg
}

// ------------------------------------------------------------------
// Scheduler-specific helpers
// ------------------------------------------------------------------

pub fn default_config() -> SchedulerConfig {
    SchedulerConfig {
        tick_interval_ms: 100,
        batch_size: 256,
        max_concurrent_steps: 128,
        shutdown_grace_period_secs: 30,
        stale_instance_threshold_secs: 300,
        max_instances_per_tenant: 0,
        webhooks: WebhookConfig::default(),
        externalize_output_threshold: 0,
        encryption_key: SecretString::default(),
        max_context_bytes: 256 * 1024,
        externalization_mode: ExternalizationMode::default(),
        worker_reaper_tick_secs: 30,
        worker_reaper_stale_secs: 60,
        node_reaper_tick_secs: 60,
        node_reaper_stale_secs: 120,
        cron_tick_secs: 10,
        max_steps_per_instance: 0,
        artifact_retention_secs: 0,
    }
}

pub async fn storage() -> Arc<dyn StorageBackend> {
    Arc::new(SqliteStorage::in_memory().await.unwrap())
}

pub fn semaphore(permits: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(permits))
}

pub fn cache() -> Arc<SequenceCache> {
    Arc::new(SequenceCache::new(1000, Duration::from_secs(300)))
}
