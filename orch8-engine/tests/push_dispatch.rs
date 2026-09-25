//! Integration test: a push-mode queue POSTs a signed task envelope at enqueue.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use orch8_engine::push::maybe_push_task;
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

fn mk_task(queue: Option<&str>) -> WorkerTask {
    WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new("s1"),
        handler_name: "h".into(),
        queue_name: queue.map(String::from),
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({ "x": 1 }),
        context: serde_json::json!({}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: chrono::Utc::now(),
    }
}

// `push_queue_posts_signed_envelope` lives in `src/push.rs` unit tests: its
// loopback receiver is (correctly) refused by the send-time SSRF guard, and
// only in-crate tests can mark a loopback URL safe.

#[tokio::test]
async fn poll_queue_does_not_push() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    // No dispatch config at all → no push, no panic.
    let task = mk_task(Some("q1"));
    maybe_push_task(storage.as_ref(), "t1", &task, &CancellationToken::new()).await;
    // A task with no queue → no push.
    let task = mk_task(None);
    maybe_push_task(storage.as_ref(), "t1", &task, &CancellationToken::new()).await;
}
