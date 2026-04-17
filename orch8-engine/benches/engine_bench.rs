//! Criterion benchmarks for engine hot paths: expression processing, template
//! resolution, externalized-marker preload.

use chrono::Utc;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;
use tokio::runtime::Runtime;

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};

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
        let inst_id = InstanceId::new();
        let mut obj = serde_json::Map::with_capacity(refs_per_instance + 1);
        obj.insert("keep_small".into(), json!("scalar"));
        for j in 0..refs_per_instance {
            let ref_k = format!("{}:ctx:data:f{}_{}", inst_id.0, i, j);
            storage
                .save_externalized_state(inst_id, &ref_k, &payload)
                .await
                .unwrap();
            obj.insert(format!("f{j}"), marker(&ref_k));
        }
        let mut inst = mk_instance(serde_json::Value::Object(obj));
        inst.id = inst_id;
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

criterion_group!(
    benches,
    bench_expression_processing,
    bench_template_resolution,
    bench_expression_throughput,
    bench_preload,
);
criterion_main!(benches);
