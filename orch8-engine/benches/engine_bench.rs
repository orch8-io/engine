//! Criterion benchmarks for engine hot paths: expression processing, template resolution.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;

use orch8_types::context::ExecutionContext;

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
        runtime: Default::default(),
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

criterion_group!(
    benches,
    bench_expression_processing,
    bench_template_resolution,
    bench_expression_throughput,
);
criterion_main!(benches);
