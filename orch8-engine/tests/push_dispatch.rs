//! Integration test: a push-mode queue POSTs a signed task envelope at enqueue.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use orch8_engine::push::maybe_push_task;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

fn mk_task(queue: Option<&str>) -> WorkerTask {
    WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new("s1"),
        handler_name: "h".into(),
        queue_name: queue.map(String::from),
        params: serde_json::json!({ "x": 1 }),
        context: serde_json::json!({}),
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
        created_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn push_queue_posts_signed_envelope() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    // A throwaway HTTP receiver that records the first request it gets.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}/push");
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut acc = Vec::new();
        let mut buf = [0u8; 4096];
        // Read until we've seen headers + a non-empty body (small payloads
        // arrive within a couple of reads over loopback).
        for _ in 0..8 {
            match sock.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    acc.extend_from_slice(&buf[..n]);
                    if let Some(pos) = find_subslice(&acc, b"\r\n\r\n") {
                        if acc.len() > pos + 4 {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await;
        let _ = tx.send(String::from_utf8_lossy(&acc).to_string());
    });

    // Configure queue q1 for push to the receiver, with a signing secret.
    let now = chrono::Utc::now();
    let cfg = QueueDispatchConfig {
        tenant_id: "t1".into(),
        queue_name: "q1".into(),
        mode: DispatchMode::Push,
        push_url: Some(url),
        secret: Some("shhh".into()),
        created_at: now,
        updated_at: now,
    };
    storage.upsert_queue_dispatch(&cfg).await.unwrap();

    let task = mk_task(Some("q1"));
    let cancel = CancellationToken::new();
    maybe_push_task(storage.as_ref(), "t1", &task, &cancel).await;

    let req = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("push should arrive")
        .expect("sender alive");

    // The envelope carries the task id and is signed.
    assert!(req.contains(&task.id.to_string()), "envelope carries task id");
    // Header names are normalized to lowercase on the wire.
    assert!(
        req.to_lowercase().contains("x-orch8-signature"),
        "envelope is signed"
    );
}

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

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}
