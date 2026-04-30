//! Criterion benchmarks for engine hot paths: expression processing, template
//! resolution, externalized-marker preload.

use std::sync::Arc;

use chrono::Utc;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use serde_json::json;
use tokio::runtime::Runtime;

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};

fn make_sequence() -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("bench".into()),
        namespace: Namespace("default".into()),
        name: "bench_seq".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId("s1".into()),
            handler: "noop".into(),
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
        }))],
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn make_instance(seq_id: SequenceId) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId("bench".into()),
        namespace: Namespace("default".into()),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now - chrono::Duration::seconds(10)),
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

fn make_context() -> ExecutionContext {
    ExecutionContext {
        data: json!({
            "user": {
                "name": "Alice",
                "age": 30,
                "email": "alice@example.com",
                "scores": [85, 92, 78, 95, 88]
            },
            "order": {
                "id": "ORD-12345",
                "total": 149.99,
                "items": 3,
                "status": "confirmed"
            },
            "flags": {
                "premium": true,
                "beta": false,
                "region": "us-east"
            }
        }),
        config: json!({
            "max_retries": 5,
            "timeout_ms": 30000,
            "feature_flags": {"new_ui": true}
        }),
        audit: vec![],
        runtime: orch8_types::context::RuntimeContext::default(),
    }
}

fn make_outputs() -> serde_json::Value {
    json!({
        "fetch_user": {"status": 200, "body": {"id": 42, "verified": true}},
        "validate_order": {"valid": true, "amount": 149.99},
        "send_email": {"delivered": true, "message_id": "msg-abc123"}
    })
}

// Note: orch8_engine::expression::evaluate is a safe expression evaluator
// (not arbitrary code execution). It parses a simple DSL for path references,
// comparisons, and arithmetic against a typed context.

fn bench_expression_processing(c: &mut Criterion) {
    let ctx = make_context();
    let outputs = make_outputs();

    c.bench_function("expr_simple_path", |b| {
        b.iter(|| {
            orch8_engine::expression::evaluate(black_box("context.data.user.name"), &ctx, &outputs)
        });
    });

    c.bench_function("expr_comparison", |b| {
        b.iter(|| {
            orch8_engine::expression::evaluate(
                black_box("context.data.order.total > 100"),
                &ctx,
                &outputs,
            )
        });
    });

    c.bench_function("expr_logical_compound", |b| {
        b.iter(|| {
            orch8_engine::expression::evaluate(
                black_box("context.data.flags.premium == true && context.data.order.items > 2"),
                &ctx,
                &outputs,
            )
        });
    });

    c.bench_function("expr_arithmetic", |b| {
        b.iter(|| {
            orch8_engine::expression::evaluate(
                black_box("context.data.user.age * 2 + 10"),
                &ctx,
                &outputs,
            )
        });
    });

    c.bench_function("expr_output_reference", |b| {
        b.iter(|| {
            orch8_engine::expression::evaluate(
                black_box("outputs.fetch_user.status == 200"),
                &ctx,
                &outputs,
            )
        });
    });

    c.bench_function("expr_complex_nested", |b| {
        b.iter(|| {
            orch8_engine::expression::evaluate(
                black_box("(context.data.order.total > 100 && outputs.validate_order.valid == true) || context.data.flags.beta == true"),
                &ctx,
                &outputs,
            )
        });
    });

    c.bench_function("expr_is_truthy", |b| {
        let val = json!(42);
        b.iter(|| orch8_engine::expression::is_truthy(black_box(&val)));
    });
}

fn bench_template_resolution(c: &mut Criterion) {
    let ctx = make_context();
    let outputs = make_outputs();

    c.bench_function("template_simple", |b| {
        let template = json!("{{context.data.user.name}}");
        b.iter(|| orch8_engine::template::resolve(black_box(&template), &ctx, &outputs).unwrap());
    });

    c.bench_function("template_inline_mixed", |b| {
        let template =
            json!("Hello {{context.data.user.name}}, order {{context.data.order.id}} confirmed");
        b.iter(|| orch8_engine::template::resolve(black_box(&template), &ctx, &outputs).unwrap());
    });

    c.bench_function("template_nested_object", |b| {
        let template = json!({
            "to": "{{context.data.user.email}}",
            "subject": "Order {{context.data.order.id}}",
            "body": {
                "name": "{{context.data.user.name}}",
                "total": "{{context.data.order.total}}",
                "message_id": "{{outputs.send_email.message_id}}"
            }
        });
        b.iter(|| orch8_engine::template::resolve(black_box(&template), &ctx, &outputs).unwrap());
    });

    c.bench_function("template_no_placeholders", |b| {
        let template = json!({"static": "value", "count": 42, "active": true});
        b.iter(|| orch8_engine::template::resolve(black_box(&template), &ctx, &outputs).unwrap());
    });

    c.bench_function("template_with_fallback", |b| {
        let template = json!("{{context.data.missing_key|default_value}}");
        b.iter(|| orch8_engine::template::resolve(black_box(&template), &ctx, &outputs).unwrap());
    });
}

fn bench_expression_throughput(c: &mut Criterion) {
    let ctx = make_context();
    let outputs = make_outputs();

    let expressions = vec![
        "context.data.user.name",
        "context.data.order.total > 100",
        "context.data.flags.premium == true && context.data.order.items > 2",
        "outputs.fetch_user.status == 200",
        "context.data.user.age * 2 + 10",
    ];

    c.bench_function("expr_throughput_1000_mixed", |b| {
        b.iter(|| {
            for _ in 0..200 {
                for expr in &expressions {
                    orch8_engine::expression::evaluate(black_box(expr), &ctx, &outputs);
                }
            }
        });
    });
}

// ---- Preload benchmarks -----------------------------------------------------

fn mk_instance(data: serde_json::Value) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId("bench".into()),
        namespace: Namespace("ns".into()),
        state: InstanceState::Running,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext {
            data,
            config: json!({}),
            audit: vec![],
            runtime: orch8_types::context::RuntimeContext::default(),
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

fn marker(ref_key: &str) -> serde_json::Value {
    json!({ "_externalized": true, "_ref": ref_key })
}

/// Seed `n_instances`, each with `refs_per_instance` top-level externalized
/// markers. Payloads are a fixed ~1 KiB blob — small enough that the work is
/// dominated by fetch/marker-walk overhead rather than JSON clone cost.
async fn seed_preload_batch(
    n_instances: usize,
    refs_per_instance: usize,
) -> (orch8_storage::sqlite::SqliteStorage, Vec<TaskInstance>) {
    let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
        .await
        .unwrap();
    let payload = json!({ "blob": "x".repeat(1024) });

    let mut instances = Vec::with_capacity(n_instances);
    for i in 0..n_instances {
        // Build the instance shell FIRST so its row can land in task_instances
        // before any externalized_state rows reference it — the FK (see
        // `orch8-storage/src/sqlite/schema.rs::externalized_state.instance_id`)
        // is enforced under `PRAGMA foreign_keys = ON` and would otherwise
        // reject the insert with a constraint violation.
        let mut obj = serde_json::Map::with_capacity(refs_per_instance + 1);
        obj.insert("keep_small".into(), json!("scalar"));
        let mut inst = mk_instance(serde_json::Value::Object(obj));
        let inst_id = inst.id;
        storage.create_instance(&inst).await.unwrap();

        let mut data_obj = match inst.context.data {
            serde_json::Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        for j in 0..refs_per_instance {
            let ref_k = format!("{}:ctx:data:f{}_{}", inst_id.0, i, j);
            storage
                .save_externalized_state(inst_id, &ref_k, &payload)
                .await
                .unwrap();
            data_obj.insert(format!("f{j}"), marker(&ref_k));
        }
        inst.context.data = serde_json::Value::Object(data_obj);
        instances.push(inst);
    }
    (storage, instances)
}

fn bench_preload(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // 1) No markers: early-return path — measures the scan overhead only.
    c.bench_function("preload_no_markers_50_instances", |b| {
        let (storage, template) = rt.block_on(async { seed_preload_batch(50, 0).await });
        b.to_async(&rt).iter(|| async {
            let mut batch = template.clone();
            orch8_engine::preload::preload_externalized_markers(
                black_box(&storage),
                black_box(&mut batch),
            )
            .await;
        });
    });

    // 2) Typical: 50 instances × 2 refs each = 100 markers in one batch.
    c.bench_function("preload_50x2_markers", |b| {
        let (storage, template) = rt.block_on(async { seed_preload_batch(50, 2).await });
        b.to_async(&rt).iter(|| async {
            let mut batch = template.clone();
            orch8_engine::preload::preload_externalized_markers(
                black_box(&storage),
                black_box(&mut batch),
            )
            .await;
        });
    });

    // 3) Wide: 100 instances × 4 refs each = 400 markers. Exercises dedup
    //    and the hydration walk at realistic fan-out.
    c.bench_function("preload_100x4_markers", |b| {
        let (storage, template) = rt.block_on(async { seed_preload_batch(100, 4).await });
        b.to_async(&rt).iter(|| async {
            let mut batch = template.clone();
            orch8_engine::preload::preload_externalized_markers(
                black_box(&storage),
                black_box(&mut batch),
            )
            .await;
        });
    });
}

// ---- flatten_blocks benchmark ----------------------------------------------

fn make_nested_blocks(depth: usize, breadth: usize) -> Vec<orch8_types::sequence::BlockDefinition> {
    use orch8_types::ids::BlockId;
    use orch8_types::sequence::{BlockDefinition, StepDef};

    fn build_level(level: usize, breadth: usize) -> Vec<BlockDefinition> {
        if level == 0 {
            return (0..breadth)
                .map(|i| {
                    BlockDefinition::Step(Box::new(StepDef {
                        id: BlockId(format!("s_{level}_{i}")),
                        handler: "noop".into(),
                        params: serde_json::json!({}),
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
                })
                .collect();
        }
        vec![BlockDefinition::Parallel(Box::new(
            orch8_types::sequence::ParallelDef {
                id: BlockId(format!("p_{level}")),
                branches: (0..breadth)
                    .map(|_| build_level(level - 1, breadth))
                    .collect(),
            },
        ))]
    }

    build_level(depth, breadth)
}

fn bench_flatten_blocks(c: &mut Criterion) {
    c.bench_function("flatten_blocks_50_flat", |b| {
        let blocks = make_nested_blocks(0, 50);
        b.iter(|| orch8_engine::evaluator::flatten_blocks(black_box(&blocks)));
    });

    c.bench_function("flatten_blocks_100_nested", |b| {
        let blocks = make_nested_blocks(2, 3);
        b.iter(|| orch8_engine::evaluator::flatten_blocks(black_box(&blocks)));
    });

    c.bench_function("flatten_blocks_500_deep", |b| {
        let blocks = make_nested_blocks(3, 2);
        b.iter(|| orch8_engine::evaluator::flatten_blocks(black_box(&blocks)));
    });
}

// ---- evaluate() tree benchmark ---------------------------------------------

#[allow(clippy::too_many_lines)]
fn bench_evaluate_deep_tree(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("evaluate_flat_10_steps", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let s = rt.block_on(SqliteStorage::in_memory()).unwrap();
                let seq = make_sequence();
                rt.block_on(s.create_sequence(&seq)).unwrap();
                let mut inst = make_instance(seq.id);
                inst.state = InstanceState::Running;
                rt.block_on(s.create_instance(&inst)).unwrap();

                let mut nodes = Vec::new();
                for i in 0..10 {
                    nodes.push(ExecutionNode {
                        id: ExecutionNodeId::new(),
                        instance_id: inst.id,
                        block_id: BlockId(format!("s1_{i}")),
                        parent_id: None,
                        block_type: BlockType::Step,
                        branch_index: None,
                        state: NodeState::Pending,
                        started_at: None,
                        completed_at: None,
                    });
                }
                rt.block_on(s.create_execution_nodes_batch(&nodes)).unwrap();
                (s, inst)
            },
            |(s, inst)| async move {
                let seq = make_sequence();
                let handlers = orch8_engine::handlers::HandlerRegistry::new();
                let storage: Arc<dyn StorageBackend> = Arc::new(s);
                let _ = orch8_engine::evaluator::evaluate(&storage, &handlers, &inst, &seq).await;
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("evaluate_parallel_4_branches", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let s = rt.block_on(SqliteStorage::in_memory()).unwrap();
                let seq = SequenceDefinition {
                    id: SequenceId::new(),
                    tenant_id: TenantId("bench".into()),
                    namespace: Namespace("default".into()),
                    name: "parallel_seq".into(),
                    version: 1,
                    deprecated: false,
                    blocks: vec![BlockDefinition::Parallel(Box::new(
                        orch8_types::sequence::ParallelDef {
                            id: BlockId("par1".into()),
                            branches: (0..4)
                                .map(|i| {
                                    vec![BlockDefinition::Step(Box::new(StepDef {
                                        id: BlockId(format!("par_step_{i}")),
                                        handler: "noop".into(),
                                        params: serde_json::json!({}),
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
                                    }))]
                                })
                                .collect(),
                        },
                    ))],
                    interceptors: None,
                    created_at: Utc::now(),
                };
                rt.block_on(s.create_sequence(&seq)).unwrap();
                let mut inst = make_instance(seq.id);
                inst.state = InstanceState::Running;
                rt.block_on(s.create_instance(&inst)).unwrap();
                (s, inst, seq)
            },
            |(s, inst, seq)| async move {
                let handlers = orch8_engine::handlers::HandlerRegistry::new();
                let storage: Arc<dyn StorageBackend> = Arc::new(s);
                let _ = orch8_engine::evaluator::evaluate(&storage, &handlers, &inst, &seq).await;
            },
            BatchSize::SmallInput,
        );
    });
}

// ---- Scheduler tick throughput benchmark -----------------------------------

fn bench_scheduler_tick(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("scheduler_tick_50_instances", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let s = rt.block_on(SqliteStorage::in_memory()).unwrap();
                let seq = make_sequence();
                rt.block_on(s.create_sequence(&seq)).unwrap();
                let batch: Vec<TaskInstance> = (0..50)
                    .map(|_| {
                        let mut inst = make_instance(seq.id);
                        inst.state = InstanceState::Scheduled;
                        inst.next_fire_at = Some(Utc::now() - chrono::Duration::seconds(10));
                        inst
                    })
                    .collect();
                rt.block_on(s.create_instances_batch(&batch)).unwrap();

                let config = orch8_types::config::SchedulerConfig {
                    tick_interval_ms: 1000,
                    batch_size: 100,
                    max_concurrent_steps: 50,
                    max_instances_per_tenant: 0,
                    stale_instance_threshold_secs: 300,
                    node_reaper_tick_secs: 60,
                    node_reaper_stale_secs: 300,
                    worker_reaper_tick_secs: 60,
                    worker_reaper_stale_secs: 300,
                    cron_tick_secs: 60,
                    shutdown_grace_period_secs: 10,
                    max_context_bytes: 0,
                    externalization_mode: orch8_types::config::ExternalizationMode::Never,
                    externalize_output_threshold: 0,
                    ..orch8_types::config::SchedulerConfig::default()
                };
                let handlers = orch8_engine::handlers::HandlerRegistry::new();
                let cancel = tokio_util::sync::CancellationToken::new();
                (s, config, handlers, cancel)
            },
            |(s, config, handlers, cancel)| async move {
                let _ = orch8_engine::scheduler::run_tick_loop(
                    Arc::new(s),
                    Arc::new(handlers),
                    &config,
                    cancel,
                )
                .await;
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_expression_processing,
    bench_template_resolution,
    bench_expression_throughput,
    bench_preload,
    bench_flatten_blocks,
    bench_evaluate_deep_tree,
    bench_scheduler_tick,
);
criterion_main!(benches);
