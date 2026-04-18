//! Regression tests for the "loop nested inside for_each" iteration-marker
//! purge bug.
//!
//! Background: under the write-append `block_outputs` schema (migration 027)
//! composite blocks (`Loop`, `ForEach`) persist their iteration counter as
//! a `BlockOutput` keyed by their own `block_id`. When an outer for_each
//! advances to the next iteration it resets descendant subtree state to
//! `Pending`. Before the fix, the descendant inner loop's previous-iteration
//! marker was left in `block_outputs` — on the next tick its handler read
//! the stale counter, the top-of-function cap guard tripped, and the body
//! was never re-run. This test reproduces the exact "for_each over 3 items
//! containing an inner loop of 2 iterations should produce 6 inner step
//! outputs" scenario from the failing TS e2e test.

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator;
use orch8_engine::handlers::{builtin::register_builtins, HandlerRegistry};
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, ForEachDef, LoopDef, SequenceDefinition, StepDef};

/// Build the sequence equivalent to the failing e2e test:
///   for_each items {
///     loop max=2 condition=true {
///       step "inner" handler "noop"
///     }
///   }
fn build_sequence() -> SequenceDefinition {
    let inner = BlockDefinition::Step(StepDef {
        id: BlockId("inner".into()),
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
    });
    let inner_loop = BlockDefinition::Loop(LoopDef {
        id: BlockId("l1".into()),
        condition: "true".into(),
        body: vec![inner],
        max_iterations: 2,
    });
    let outer = BlockDefinition::ForEach(ForEachDef {
        id: BlockId("fe1".into()),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![inner_loop],
        max_iterations: 1000,
    });
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "loop-in-foreach".into(),
        version: 1,
        deprecated: false,
        blocks: vec![outer],
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn mk_instance(seq_id: SequenceId, items: serde_json::Value) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        state: InstanceState::Running,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext {
            data: json!({"items": items}),
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

/// Drive the evaluator until it reports no more work or a safety cap trips.
async fn drive_to_completion(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance_id: InstanceId,
    sequence: &SequenceDefinition,
) {
    let max_ticks = 1000;
    for tick in 0..max_ticks {
        // Re-fetch instance each tick so handlers see fresh context.
        let inst = storage
            .get_instance(instance_id)
            .await
            .expect("get_instance")
            .expect("instance row exists");
        let more = evaluator::evaluate(storage, handlers, &inst, sequence)
            .await
            .expect("evaluate ok");
        if !more {
            return;
        }
        // Safety: guard against runaway test loops.
        assert!(
            tick < max_ticks - 1,
            "evaluator did not terminate within {max_ticks} ticks"
        );
    }
}

#[tokio::test]
async fn r1_for_each_3_over_loop_2_yields_6_inner_outputs() {
    let storage_inner = SqliteStorage::in_memory().await.unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(storage_inner);
    let mut registry = HandlerRegistry::new();
    register_builtins(&mut registry);

    let seq = build_sequence();
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_instance(seq.id, json!(["x", "y", "z"]));
    storage.create_instance(&inst).await.unwrap();

    drive_to_completion(&storage, &registry, inst.id, &seq).await;

    // Inner step output rows = 3 outer items * 2 loop iterations = 6.
    let all = storage.get_all_outputs(inst.id).await.unwrap();
    let inner_count = all.iter().filter(|o| o.block_id.0 == "inner").count();
    assert_eq!(
        inner_count,
        6,
        "expected 6 inner step outputs (3 outer items * 2 inner loops); got {inner_count} \
         (all rows: {:?})",
        all.iter()
            .map(|o| (o.block_id.0.clone(), o.attempt))
            .collect::<Vec<_>>()
    );

    // Final for_each marker shows _index = 3 (it iterated through all items).
    let fe_marker = storage
        .get_block_output(inst.id, &BlockId("fe1".into()))
        .await
        .unwrap()
        .expect("for_each marker exists at completion");
    assert_eq!(fe_marker.output["_index"], json!(3));

    // Inner loop marker: when present, it must reflect the LAST outer
    // iteration's count (i.e. 2). The cap-reached short-circuit on the
    // outer for_each's final iteration deliberately skips the reset path
    // because the body is already done and the for_each is about to
    // complete on the next tick.
    if let Some(inner_marker) = storage
        .get_block_output(inst.id, &BlockId("l1".into()))
        .await
        .unwrap()
    {
        assert_eq!(
            inner_marker.output["_iterations"],
            json!(2),
            "if the inner loop marker survives, it must reflect the final iteration's count"
        );
    }
}

#[tokio::test]
async fn r2_for_each_3_over_loop_0_items_completes_immediately() {
    let storage_inner = SqliteStorage::in_memory().await.unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(storage_inner);
    let mut registry = HandlerRegistry::new();
    register_builtins(&mut registry);

    let seq = build_sequence();
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_instance(seq.id, json!([]));
    storage.create_instance(&inst).await.unwrap();

    drive_to_completion(&storage, &registry, inst.id, &seq).await;

    // No step outputs.
    let all = storage.get_all_outputs(inst.id).await.unwrap();
    assert!(
        all.iter().all(|o| o.block_id.0 != "inner"),
        "no inner step outputs expected for empty collection"
    );

    // No markers left.
    assert!(storage
        .get_block_output(inst.id, &BlockId("fe1".into()))
        .await
        .unwrap()
        .is_none());
    assert!(storage
        .get_block_output(inst.id, &BlockId("l1".into()))
        .await
        .unwrap()
        .is_none());

    // The root for_each must reach Completed; descendants that were never
    // activated (because the collection was empty) remain Pending — that
    // is the legitimate state for "never ran".
    let tree = storage.get_execution_tree(inst.id).await.unwrap();
    let root = tree
        .iter()
        .find(|n| n.parent_id.is_none())
        .expect("root node");
    assert_eq!(
        root.state,
        orch8_types::execution::NodeState::Completed,
        "root for_each must complete on empty collection"
    );
}
