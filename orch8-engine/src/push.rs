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

fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .unwrap_or_default()
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
    for attempt in 0..=MAX_RETRIES {
        match post_once(url, body, secret).await {
            Ok(status) if status < 400 => {
                metrics::inc(metrics::TASKS_PUSHED);
                debug!(url = %url, "task pushed");
                return;
            }
            Ok(status) => warn!(url = %url, status, attempt, "push returned error status"),
            Err(e) => warn!(url = %url, error = %e, attempt, "push request failed"),
        }
        if attempt < MAX_RETRIES {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt + 1))) => {}
            }
        }
    }
    metrics::inc(metrics::TASKS_PUSH_FAILED);
    warn!(url = %url, "task push failed after all retries (task remains pending)");
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
        .map_err(|e| e.to_string())?;
    Ok(resp.status().as_u16())
}
