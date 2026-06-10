//! Engine-level e2e for resume-from-block (DLQ surgery).
//!
//! Drives a flat 3-step sequence through the real scheduler (`tick_once`)
//! until it fails at the middle step, then performs the same storage surgery
//! the `POST /instances/{id}/resume-from/{block_id}` handler performs (wipe
//! tree + tail outputs, patch context, re-schedule) and ticks the scheduler
//! to completion. Verifies the patched context unblocks the failing step and
//! that the already-completed first step is NOT re-executed.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use chrono::Utc;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_storage::StorageBackend;
use orch8_types::error::StepError;
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
async fn resume_from_failed_block_with_context_patch_runs_to_completion() {
    // s1 counts its executions; s2 fails permanently while
    // `context.data.broken == true`; s3 is a plain noop.
    let s1_runs = Arc::new(AtomicU32::new(0));
    let mut reg = registry();
    {
        let s1_runs = Arc::clone(&s1_runs);
        reg.register("count_me", move |_ctx| {
            let s1_runs = Arc::clone(&s1_runs);
            Box::pin(async move {
                s1_runs.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"counted": true}))
            })
        });
    }
    reg.register("fail_if_broken", |ctx| {
        Box::pin(async move {
            if ctx
                .context
                .data
                .get("broken")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                Err(StepError::Permanent {
                    message: "dependency broken".into(),
                    details: None,
                })
            } else {
                Ok(json!({"repaired": true}))
            }
        })
    });
    let handlers = Arc::new(reg);

    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let seq = mk_sequence(vec![
        mk_step("s1", "count_me"),
        mk_step("s2", "fail_if_broken"),
        mk_step("s3", "noop"),
    ]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_scheduled(seq.id, json!({"broken": true}));
    storage.create_instance(&inst).await.unwrap();

    // Run until the instance fails at s2.
    tick_until(&storage, &handlers, inst.id, InstanceState::Failed).await;
    assert_eq!(s1_runs.load(Ordering::SeqCst), 1, "s1 ran exactly once");
    assert!(
        storage
            .get_block_output(inst.id, &BlockId::new("s1"))
            .await
            .unwrap()
            .is_some(),
        "s1 output persisted before the failure"
    );

    // --- Resume-from surgery (mirrors the API handler) ---
    // Wipe the stale tree, the target block's and later blocks' outputs
    // (including s2's kept __in_progress__ sentinel), purge stale worker
    // tasks, patch the context so s2 now succeeds, and re-schedule.
    storage.delete_execution_tree(inst.id).await.unwrap();
    let wiped = storage
        .delete_block_outputs_batch(inst.id, &[BlockId::new("s2"), BlockId::new("s3")])
        .await
        .unwrap();
    assert!(wiped >= 1, "s2's sentinel/marker rows must be wiped");
    storage
        .cancel_worker_tasks_for_blocks(inst.id.into_uuid(), &["s2".into(), "s3".into()])
        .await
        .unwrap();
    let mut current = storage.get_instance(inst.id).await.unwrap().unwrap();
    current.context.data["broken"] = json!(false);
    storage
        .update_instance_context(inst.id, &current.context)
        .await
        .unwrap();
    storage
        .update_instance_state(
            inst.id,
            InstanceState::Scheduled,
            Some(Utc::now() - chrono::Duration::seconds(5)),
        )
        .await
        .unwrap();

    // The re-run completes: s2 succeeds with the patched context, s3 runs.
    tick_until(&storage, &handlers, inst.id, InstanceState::Completed).await;

    assert_eq!(
        s1_runs.load(Ordering::SeqCst),
        1,
        "s1 must NOT re-execute — its preserved output short-circuits the fast path"
    );
    let s2_out = storage
        .get_block_output(inst.id, &BlockId::new("s2"))
        .await
        .unwrap()
        .expect("s2 re-ran and produced a real output");
    assert_eq!(s2_out.output["repaired"], true);
    assert!(
        storage
            .get_block_output(inst.id, &BlockId::new("s3"))
            .await
            .unwrap()
            .is_some(),
        "s3 ran on the resumed pass"
    );
}
