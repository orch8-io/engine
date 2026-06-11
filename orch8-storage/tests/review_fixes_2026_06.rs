//! Regression tests for the 2026-06 deep-review fixes (SQLite backend).
//!
//! Each test reproduces a bug found during the review:
//!
//!   1. `recover_stale_instances` must reset `next_fire_at` (parity with
//!      Postgres) so recovered instances are immediately claimable.
//!   2. `update_node_state(Running)` must NOT overwrite an existing
//!      `started_at` on re-dispatch (COALESCE parity with Postgres). Same
//!      for the batch variants.
//!   3. `get_outputs_after_created_at` uses an inclusive bound so outputs
//!      sharing the cursor's exact timestamp are not skipped.
//!   4. `merge_context_data` serialises concurrent merges (BEGIN IMMEDIATE)
//!      so no key is lost to a read-modify-write race.
//!   5. `claim_worker_tasks_from_queue` must not double-claim under
//!      concurrent pollers (BEGIN IMMEDIATE).
//!   6. `copy_block_outputs` batch INSERT copies all rows faithfully.
//!   7. `create_instance_with_dedupe` returns `AlreadyExists` for the loser.
//!
//! Running only this file:
//!
//! ```text
//! cargo test -p orch8-storage --test review_fixes_2026_06
//! ```

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    DedupeScope, EmitDedupeOutcome, ExecutionTreeStore, InstanceStore, OutputStore, WorkerStore,
};
use orch8_types::context::ExecutionContext;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::worker::{WorkerTask, WorkerTaskState};

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

fn make_instance(state: InstanceState) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("default"),
        state,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

fn make_node(instance_id: InstanceId, block_id: &str) -> ExecutionNode {
    ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id,
        block_id: BlockId::new(block_id),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    }
}

fn make_output(instance_id: InstanceId, block_id: &str) -> BlockOutput {
    BlockOutput {
        id: Uuid::now_v7(),
        instance_id,
        block_id: BlockId::new(block_id),
        output: json!({"ok": true}),
        output_ref: None,
        output_size: 10,
        attempt: 0,
        created_at: Utc::now(),
    }
}

fn make_queue_task(instance_id: InstanceId, block: &str) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id,
        block_id: BlockId::new(block),
        handler_name: "h".into(),
        queue_name: Some("q1".into()),
        params: json!({}),
        context: json!({}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// 1. recover_stale_instances resets next_fire_at
// ---------------------------------------------------------------------------

#[tokio::test]
async fn recover_stale_resets_next_fire_at() {
    let s = store().await;

    // A wedged Running instance whose next_fire_at points far into the
    // future (set before it wedged) and whose updated_at is old enough to
    // cross the stale threshold.
    let mut inst = make_instance(InstanceState::Running);
    inst.next_fire_at = Some(Utc::now() + Duration::hours(6));
    inst.updated_at = Utc::now() - Duration::hours(1);
    s.create_instance(&inst).await.unwrap();

    let recovered = s
        .recover_stale_instances(StdDuration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(recovered, 1, "stale instance must be recovered");

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let fire_at = refreshed
        .next_fire_at
        .expect("recovered instance keeps a concrete next_fire_at");
    assert!(
        fire_at <= Utc::now() + Duration::seconds(5),
        "next_fire_at must be reset to now so claim_due picks the instance up, got {fire_at}"
    );
}

// ---------------------------------------------------------------------------
// 2. started_at survives a re-dispatch to Running
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_node_state_preserves_started_at_on_redispatch() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();
    let node = make_node(inst.id, "s1");
    s.create_execution_nodes_batch(std::slice::from_ref(&node))
        .await
        .unwrap();

    s.update_node_state(node.id, NodeState::Running)
        .await
        .unwrap();
    let first = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .expect("first Running transition sets started_at");

    // Waiting → Running again (e.g. worker retry / wake) must keep the
    // original timestamp.
    s.update_node_state(node.id, NodeState::Waiting)
        .await
        .unwrap();
    tokio::time::sleep(StdDuration::from_millis(15)).await;
    s.update_node_state(node.id, NodeState::Running)
        .await
        .unwrap();

    let second = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .unwrap();
    assert_eq!(
        first, second,
        "re-dispatch must not overwrite the original started_at"
    );
}

#[tokio::test]
async fn batch_node_updates_preserve_started_at() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();
    let node = make_node(inst.id, "s1");
    s.create_execution_nodes_batch(std::slice::from_ref(&node))
        .await
        .unwrap();

    s.update_nodes_state(&[node.id], NodeState::Running)
        .await
        .unwrap();
    let first = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .expect("batch Running transition sets started_at");

    tokio::time::sleep(StdDuration::from_millis(15)).await;
    s.update_nodes_state(&[node.id], NodeState::Running)
        .await
        .unwrap();
    let second = s.get_execution_tree(inst.id).await.unwrap()[0]
        .started_at
        .unwrap();
    assert_eq!(first, second, "batch re-transition must keep started_at");
}

// ---------------------------------------------------------------------------
// 3. get_outputs_after_created_at boundary is inclusive
// ---------------------------------------------------------------------------

#[tokio::test]
async fn outputs_after_created_at_includes_cursor_timestamp() {
    let s = store().await;
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    // Two outputs sharing the exact same created_at (same-millisecond batch).
    let shared_ts = Utc::now();
    let mut a = make_output(inst.id, "a");
    a.created_at = shared_ts;
    let mut b = make_output(inst.id, "b");
    b.created_at = shared_ts;
    s.save_block_output(&a).await.unwrap();
    s.save_block_output(&b).await.unwrap();

    // Polling with the shared timestamp as the cursor must return BOTH rows
    // (inclusive bound); the old strict `>` bound returned neither, so an
    // SSE client whose first poll saw only `a` would never receive `b`.
    let outputs = s
        .get_outputs_after_created_at(inst.id, Some(shared_ts))
        .await
        .unwrap();
    assert_eq!(
        outputs.len(),
        2,
        "outputs at the cursor timestamp must not be skipped"
    );
}

// ---------------------------------------------------------------------------
// 4. merge_context_data: concurrent merges keep all keys
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_context_merges_lose_no_keys() {
    let s = Arc::new(store().await);
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    let mut handles = Vec::new();
    for i in 0..16 {
        let s = Arc::clone(&s);
        let id = inst.id;
        handles.push(tokio::spawn(async move {
            s.merge_context_data(id, &format!("k{i}"), &json!(i))
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let data = refreshed.context.data.as_object().expect("data is object");
    for i in 0..16 {
        assert!(
            data.contains_key(&format!("k{i}")),
            "key k{i} lost to a read-modify-write race"
        );
    }
}

// ---------------------------------------------------------------------------
// 5. queue claims never hand the same task to two workers
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_queue_claims_do_not_double_claim() {
    let s = Arc::new(store().await);
    let inst = make_instance(InstanceState::Running);
    s.create_instance(&inst).await.unwrap();
    for i in 0..8 {
        s.create_worker_task(&make_queue_task(inst.id, &format!("b{i}")))
            .await
            .unwrap();
    }

    let mut handles = Vec::new();
    for w in 0..4 {
        let s = Arc::clone(&s);
        handles.push(tokio::spawn(async move {
            s.claim_worker_tasks_from_queue("q1", "h", &format!("w{w}"), 8)
                .await
                .unwrap()
        }));
    }
    let mut seen = std::collections::HashSet::new();
    let mut total = 0usize;
    for h in handles {
        for task in h.await.unwrap() {
            total += 1;
            assert!(
                seen.insert(task.id),
                "task {} claimed by two workers",
                task.id
            );
        }
    }
    assert_eq!(total, 8, "every pending task claimed exactly once");
}

// ---------------------------------------------------------------------------
// 6. copy_block_outputs copies all rows faithfully (batch INSERT)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn copy_block_outputs_copies_all_rows() {
    let s = store().await;
    let src = make_instance(InstanceState::Completed);
    let dst = make_instance(InstanceState::Scheduled);
    s.create_instance(&src).await.unwrap();
    s.create_instance(&dst).await.unwrap();

    let block_ids: Vec<BlockId> = (0..20).map(|i| BlockId::new(format!("b{i}"))).collect();
    for bid in &block_ids {
        let mut out = make_output(src.id, bid.as_str());
        out.output = json!({"block": bid.as_str()});
        s.save_block_output(&out).await.unwrap();
    }

    let copied = s
        .copy_block_outputs(src.id, dst.id, &block_ids)
        .await
        .unwrap();
    assert_eq!(copied, 20);

    let dst_outputs = s.get_all_outputs(dst.id).await.unwrap();
    assert_eq!(dst_outputs.len(), 20);
    for out in &dst_outputs {
        assert_eq!(out.instance_id, dst.id);
        assert_eq!(out.output, json!({"block": out.block_id.as_str()}));
    }
}

// ---------------------------------------------------------------------------
// 7. dedupe create: loser sees AlreadyExists with the winner's id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_instance_with_dedupe_second_call_returns_existing() {
    let s = store().await;
    let parent = make_instance(InstanceState::Running);
    s.create_instance(&parent).await.unwrap();
    let scope = DedupeScope::Parent(parent.id);

    let first_child = make_instance(InstanceState::Scheduled);
    let outcome = s
        .create_instance_with_dedupe(&scope, "once", &first_child)
        .await
        .unwrap();
    assert!(matches!(outcome, EmitDedupeOutcome::Inserted));

    let second_child = make_instance(InstanceState::Scheduled);
    let outcome = s
        .create_instance_with_dedupe(&scope, "once", &second_child)
        .await
        .unwrap();
    match outcome {
        EmitDedupeOutcome::AlreadyExists(existing) => assert_eq!(existing, first_child.id),
        EmitDedupeOutcome::Inserted => panic!("dedupe violated: second insert won"),
    }
    // The losing candidate must not have been persisted.
    assert!(s.get_instance(second_child.id).await.unwrap().is_none());
}
