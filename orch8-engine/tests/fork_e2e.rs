//! Engine-level e2e for fork-from (time travel).
//!
//! Runs a flat 3-step sequence to completion through the real scheduler
//! (`tick_once`), then performs the same storage clone the
//! `POST /instances/{id}/fork` handler performs (new instance with patched
//! context + `copy_block_outputs` for the pre-fork blocks) and ticks the
//! fork to completion. Verifies the fork resumes at the fork point: the
//! pre-fork block is NOT re-executed (its copied output short-circuits the
//! completed-blocks check) while the fork-point block and the tail run again
//! with the patched context.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use serde_json::json;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_storage::StorageBackend;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;

mod common;
use common::*;

/// Tick the scheduler until the instance reaches `target` (or panic).
async fn tick_until(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    instance_id: InstanceId,
    target: InstanceState,
) {
    let sem = semaphore(128);
    let config = default_config();
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    for _ in 0..50 {
        tick_once(storage, handlers, &sem, &config, &seq_cache, &cancel)
            .await
            .unwrap();
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        if inst.state == target {
            return;
        }
    }
    let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
    panic!("instance never reached {target}, stuck in {}", inst.state);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one linear scenario: run, fork, re-run, assert
async fn fork_from_middle_block_reruns_only_the_tail_with_patched_context() {
    // Per-step execution counters so we can prove what (re-)ran on the fork.
    let s1_runs = Arc::new(AtomicU32::new(0));
    let s2_runs = Arc::new(AtomicU32::new(0));
    let s3_runs = Arc::new(AtomicU32::new(0));

    let mut reg = registry();
    {
        let s1_runs = Arc::clone(&s1_runs);
        reg.register("count_s1", move |_ctx| {
            let s1_runs = Arc::clone(&s1_runs);
            Box::pin(async move {
                s1_runs.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"step": "s1"}))
            })
        });
    }
    {
        // s2 echoes the context flavour so we can assert the fork ran with
        // the patched context, not the source's.
        let s2_runs = Arc::clone(&s2_runs);
        reg.register("count_s2", move |ctx| {
            let s2_runs = Arc::clone(&s2_runs);
            Box::pin(async move {
                s2_runs.fetch_add(1, Ordering::SeqCst);
                let flavour = ctx
                    .context
                    .data
                    .get("flavour")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("none")
                    .to_owned();
                Ok(json!({"step": "s2", "flavour": flavour}))
            })
        });
    }
    {
        let s3_runs = Arc::clone(&s3_runs);
        reg.register("count_s3", move |_ctx| {
            let s3_runs = Arc::clone(&s3_runs);
            Box::pin(async move {
                s3_runs.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"step": "s3"}))
            })
        });
    }
    let handlers = Arc::new(reg);

    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let seq = mk_sequence(vec![
        mk_step("s1", "count_s1"),
        mk_step("s2", "count_s2"),
        mk_step("s3", "count_s3"),
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let source = mk_instance_scheduled(seq.id, json!({"flavour": "production"}));
    storage.create_instance(&source).await.unwrap();

    // Source runs to completion: every step executed exactly once.
    tick_until(&storage, &handlers, source.id, InstanceState::Completed).await;
    assert_eq!(s1_runs.load(Ordering::SeqCst), 1);
    assert_eq!(s2_runs.load(Ordering::SeqCst), 1);
    assert_eq!(s3_runs.load(Ordering::SeqCst), 1);
    let source_s2 = storage
        .get_block_output(source.id, &BlockId::new("s2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_s2.output["flavour"], "production");

    // --- Fork surgery (mirrors the API handler) ---
    // New instance of the same sequence with a patched context, seeded with
    // a copy of s1's output (the only block before the fork point s2).
    let mut fork = mk_instance_scheduled(seq.id, json!({"flavour": "sandbox"}));
    fork.metadata = json!({
        "forked_from": source.id.into_uuid(),
        "forked_at_block": "s2",
    });
    storage.create_instance(&fork).await.unwrap();
    let copied = storage
        .copy_block_outputs(source.id, fork.id, &[BlockId::new("s1")])
        .await
        .unwrap();
    assert_eq!(copied, 1, "s1's output copied onto the fork");

    // Tick the fork to completion.
    tick_until(&storage, &handlers, fork.id, InstanceState::Completed).await;

    // Pre-fork block did NOT re-execute: the copied output short-circuits
    // the completed-blocks check.
    assert_eq!(
        s1_runs.load(Ordering::SeqCst),
        1,
        "s1 must not re-execute on the fork"
    );
    // Fork-point block and the tail DID execute (once on the source, once
    // on the fork).
    assert_eq!(s2_runs.load(Ordering::SeqCst), 2, "s2 re-ran on the fork");
    assert_eq!(s3_runs.load(Ordering::SeqCst), 2, "s3 re-ran on the fork");

    // The fork's s2 ran with the patched context.
    let fork_s2 = storage
        .get_block_output(fork.id, &BlockId::new("s2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fork_s2.output["flavour"], "sandbox");

    // Source is untouched: still completed, its outputs unchanged.
    let source_after = storage.get_instance(source.id).await.unwrap().unwrap();
    assert_eq!(source_after.state, InstanceState::Completed);
    let source_s2_after = storage
        .get_block_output(source.id, &BlockId::new("s2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_s2_after.output["flavour"], "production");

    // And the fork's copied s1 row really belongs to the fork.
    let fork_s1 = storage
        .get_block_output(fork.id, &BlockId::new("s1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fork_s1.instance_id, fork.id);
    assert_eq!(fork_s1.output["step"], "s1");
}
