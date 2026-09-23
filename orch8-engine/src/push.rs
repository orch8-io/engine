//! Push-mode task dispatch.
//!
//! A queue configured for `push` has the engine POST a signed task envelope to
//! its target URL at enqueue, instead of waiting for a worker to poll. The
//! durable `worker_tasks` row is still written, so completion is reported the
//! usual way and a push failure only means the task waits (an operator can flip
//! the queue back to `poll`).

use std::sync::OnceLock;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::queue_dispatch::DispatchMode;
use orch8_types::worker::WorkerTask;

use crate::metrics;

/// Push targets are tenant-configured, so the client uses the `Untrusted`
/// outbound profile (SSRF resolver, checked redirects, no proxy).
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        crate::outbound::build(
            crate::outbound::builder(crate::outbound::Profile::Untrusted)
                .pool_max_idle_per_host(4)
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(30)),
        )
    })
}

/// If `task`'s queue is configured for push dispatch, POST a signed envelope to
/// its target URL (best-effort, spawned). Does nothing for poll queues, unnamed
/// queues, or push queues missing a URL.
pub async fn maybe_push_task(
    storage: &dyn StorageBackend,
    tenant_id: &str,
    task: &WorkerTask,
    cancel: &CancellationToken,
) {
    let Some(queue) = task.queue_name.as_deref() else {
        return;
    };
    let cfg = match storage.get_queue_dispatch(tenant_id, queue).await {
        Ok(Some(c)) => c,
        Ok(None) => return,
        Err(e) => {
            warn!(error = %e, queue, "queue dispatch lookup failed; leaving task for poll");
            return;
        }
    };
    if cfg.mode != DispatchMode::Push {
        return;
    }
    let Some(url) = cfg.push_url else {
        warn!(queue, "push queue has no push_url; leaving task for poll");
        return;
    };

    let envelope = serde_json::json!({
        "task_id": task.id,
        "instance_id": task.instance_id,
        "block_id": task.block_id,
        "handler_name": task.handler_name,
        "queue_name": task.queue_name,
        "params": task.params,
        "context": task.context,
        "attempt": task.attempt,
        "timeout_ms": task.timeout_ms,
    });
    let body = match serde_json::to_vec(&envelope) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to serialize push envelope");
            return;
        }
    };
    let secret = cfg.secret;
    let cancel = cancel.clone();
    tokio::spawn(async move {
        send_push(&url, &body, secret.as_deref(), &cancel).await;
    });
}

async fn send_push(url: &str, body: &[u8], secret: Option<&str>, cancel: &CancellationToken) {
    const MAX_RETRIES: u32 = 3;
    let shown = crate::outbound::redact_url(url);
    // The API validates `push_url` at configuration time without DNS; re-check
    // at send time so a hostname that resolves to an internal address never
    // receives the task envelope (params + context).
    if !crate::handlers::builtin::is_url_safe(url).await {
        metrics::inc(metrics::TASKS_PUSH_FAILED);
        warn!(
            url = %shown,
            "push_url targets an internal or non-public address; blocked by SSRF guard (task remains pending)"
        );
        return;
    }
    for attempt in 0..=MAX_RETRIES {
        match post_once(url, body, secret).await {
            Ok(status) if status < 400 => {
                metrics::inc(metrics::TASKS_PUSHED);
                debug!(url = %shown, "task pushed");
                return;
            }
            Ok(status) => warn!(url = %shown, status, attempt, "push returned error status"),
            Err(e) => warn!(url = %shown, error = %e, attempt, "push request failed"),
        }
        if attempt < MAX_RETRIES {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt + 1))) => {}
            }
        }
    }
    metrics::inc(metrics::TASKS_PUSH_FAILED);
    warn!(url = %shown, "task push failed after all retries (task remains pending)");
}

async fn post_once(url: &str, body: &[u8], secret: Option<&str>) -> Result<u16, String> {
    let mut req = http_client()
        .post(url)
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(10));
    if let Some(secret) = secret {
        let ts = chrono::Utc::now().timestamp();
        // Reuse the outbound-webhook signing scheme so workers can verify with
        // the same logic.
        let sig = crate::webhooks::sign(secret, ts, body);
        req = req
            .header("X-Orch8-Timestamp", ts.to_string())
            .header("X-Orch8-Signature", format!("sha256={sig}"));
    }
    let resp = req
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| crate::outbound::redact_error(&e))?;
    Ok(resp.status().as_u16())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;
    use std::sync::Arc;

    use hmac::{Hmac, KeyInit, Mac};
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::queue_dispatch::QueueDispatchConfig;
    use sha2::Sha256;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn mk_task(queue: Option<&str>) -> WorkerTask {
        WorkerTask {
            id: uuid::Uuid::now_v7(),
            instance_id: orch8_types::ids::InstanceId::new(),
            block_id: orch8_types::ids::BlockId::new("s1"),
            handler_name: "h".into(),
            queue_name: queue.map(String::from),
            requirements: orch8_types::continuity::CapsuleRequirements::default(),
            params: serde_json::json!({ "x": 1 }),
            context: serde_json::json!({}),
            attempt: 0,
            timeout_ms: None,
            state: orch8_types::worker::WorkerTaskState::Pending,
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

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
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
            push_url: Some(url.clone()),
            secret: Some("shhh".into()),
            created_at: now,
            updated_at: now,
        };
        storage.upsert_queue_dispatch(&cfg).await.unwrap();

        let task = mk_task(Some("q1"));
        let cancel = CancellationToken::new();
        // The send-time SSRF guard refuses loopback; mark this receiver safe.
        crate::handlers::builtin::mark_url_safe_for_test(&url).await;
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
    async fn send_push_refuses_internal_target_at_send_time() {
        // ENG-P-N4: a push_url that (re)resolves to an internal address must
        // never receive the task envelope.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/push");
        send_push(&url, b"{}", None, &CancellationToken::new()).await;
        let accepted = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
        assert!(
            accepted.is_err(),
            "internal push target must not be contacted"
        );
    }
}
