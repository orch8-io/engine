//! Integration test: a push-mode queue POSTs a signed task envelope at enqueue.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use orch8_engine::push::maybe_push_task;
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
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
    let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut acc = Vec::new();
        let mut buf = [0u8; 4096];
        // Read until we have the full headers AND the complete body (Content-Length
        // bytes) — signature verification needs the exact body, not a truncation.
        loop {
            if let Some(pos) = find_subslice(&acc, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&acc[..pos]).to_lowercase();
                let content_len = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if acc.len() >= pos + 4 + content_len {
                    break;
                }
            }
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => acc.extend_from_slice(&buf[..n]),
            }
        }
        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await;
        let _ = tx.send(acc);
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

    let split = find_subslice(&req, b"\r\n\r\n").expect("request has a header/body boundary");
    let head = String::from_utf8_lossy(&req[..split]).to_string();
    let body = &req[split + 4..];

    // The envelope carries the task id.
    assert!(
        String::from_utf8_lossy(body).contains(&task.id.to_string()),
        "envelope carries task id"
    );

    // The signature is a real HMAC-SHA256 over "{timestamp}.{body}", not just a
    // header that happens to be present — recompute it with the queue secret and
    // assert it matches byte-for-byte. This catches signing the wrong bytes.
    let ts = header_value(&head, "x-orch8-timestamp").expect("timestamp header present");
    let sig = header_value(&head, "x-orch8-signature").expect("signature header present");
    let sig = sig
        .strip_prefix("sha256=")
        .expect("signature is sha256-prefixed");

    let mut mac = Hmac::<Sha256>::new_from_slice(b"shhh").unwrap();
    mac.update(ts.as_bytes());
    mac.update(b".");
    mac.update(body);
    let mut expected = String::new();
    for b in mac.finalize().into_bytes() {
        write!(expected, "{b:02x}").unwrap();
    }
    assert_eq!(
        sig, expected,
        "push signature must verify over timestamp.body"
    );
}

/// Extract a header value (case-insensitive name match) from a raw HTTP head.
fn header_value(head: &str, name_lower: &str) -> Option<String> {
    head.lines().find_map(|line| {
        let (n, v) = line.split_once(':')?;
        (n.trim().eq_ignore_ascii_case(name_lower)).then(|| v.trim().to_string())
    })
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
    haystack.windows(needle.len()).position(|w| w == needle)
}
