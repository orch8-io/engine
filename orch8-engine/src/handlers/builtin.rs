//! Built-in step handlers that ship with the engine.
//!
//! - `noop` — does nothing, returns empty object
//! - `log` — logs a message from params, returns it
//! - `sleep` — sleeps for a configured duration
//! - `http_request` — makes an HTTP request (minimal TCP-based)
//! - `fail` — always fails with a configurable message (for testing and
//!   explicit force-fail steps in user sequences)

use std::net::ToSocketAddrs;

use serde_json::{json, Value};
use tracing::{debug, info};

use orch8_types::error::StepError;

use super::{HandlerRegistry, StepContext};

/// Check whether a URL is safe to request (not targeting private/internal networks).
///
/// Returns `true` if the URL uses http/https and resolves to a public IP address.
/// Returns `false` for private, loopback, link-local, and cloud metadata addresses.
pub(crate) fn is_url_safe(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };

    match parsed.scheme() {
        "http" | "https" => {}
        _ => return false,
    }

    let Some(host) = parsed.host_str() else {
        return false;
    };

    let port = parsed.port_or_known_default().unwrap_or(80);
    let addr_str = format!("{host}:{port}");

    let Ok(addrs) = addr_str.to_socket_addrs() else {
        return false;
    };

    for addr in addrs {
        let ip = addr.ip();
        match ip {
            std::net::IpAddr::V4(v4) => {
                if v4.is_loopback()          // 127.0.0.0/8
                    || v4.is_private()        // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                    || v4.is_link_local()     // 169.254.0.0/16 (includes 169.254.169.254)
                    || v4.is_unspecified()
                {
                    return false;
                }
            }
            std::net::IpAddr::V6(v6) => {
                if v6.is_loopback()  // ::1
                    || v6.is_unspecified()
                {
                    return false;
                }
                // fc00::/7 — unique local addresses
                let segments = v6.segments();
                if segments[0] & 0xfe00 == 0xfc00 {
                    return false;
                }
            }
        }
    }

    true
}

/// Register all built-in handlers on the given registry.
///
/// Handlers that need access to storage (`emit_event`, `send_signal`,
/// `query_instance`) read it from `StepContext::storage` — no per-handler
/// closure capture needed.
pub fn register_builtins(registry: &mut HandlerRegistry) {
    registry.register("noop", handle_noop);
    registry.register("log", handle_log);
    registry.register("sleep", handle_sleep);
    registry.register("fail", handle_fail);
    registry.register("http_request", handle_http_request);
    registry.register("llm_call", super::llm::handle_llm_call);
    registry.register("tool_call", super::tool_call::handle_tool_call);
    registry.register("human_review", super::human_review::handle_human_review);
    registry.register("self_modify", super::self_modify::handle_self_modify);
    registry.register("emit_event", super::emit_event::handle_emit_event);
    registry.register("send_signal", super::send_signal::handle_send_signal);
    registry.register(
        "query_instance",
        super::query_instance::handle_query_instance,
    );
}

/// No-op handler. Always succeeds with an empty result.
async fn handle_noop(ctx: StepContext) -> Result<Value, StepError> {
    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        "noop step executed"
    );
    Ok(json!({}))
}

/// Log handler. Logs `params.message` at info level and returns it.
///
/// Params:
/// - `message` (string): The message to log. Defaults to "no message".
/// - `level` (string): Log level — "debug", "info", "warn". Defaults to "info".
async fn handle_log(ctx: StepContext) -> Result<Value, StepError> {
    let message = ctx
        .params
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("no message");

    let level = ctx
        .params
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("info");

    match level {
        "debug" => debug!(
            instance_id = %ctx.instance_id,
            block_id = %ctx.block_id,
            message = %message,
            "log step"
        ),
        "warn" => tracing::warn!(
            instance_id = %ctx.instance_id,
            block_id = %ctx.block_id,
            message = %message,
            "log step"
        ),
        _ => info!(
            instance_id = %ctx.instance_id,
            block_id = %ctx.block_id,
            message = %message,
            "log step"
        ),
    }

    Ok(json!({ "message": message }))
}

/// Sleep handler. Waits for a configured duration.
///
/// Params:
/// - `duration_ms` (u64): Milliseconds to sleep. Defaults to 100.
///
/// Long sleeps poll for a pending `cancel` signal on a short interval so
/// that in-flight sleeps are interruptible — a plain `tokio::time::sleep`
/// can't be cancelled from another task, and the scheduler doesn't
/// re-check signals until the handler returns. Without this polling, a
/// `cancel` signal queued mid-sleep would not take effect until the full
/// duration elapsed (by which point the instance is already completing).
async fn handle_sleep(ctx: StepContext) -> Result<Value, StepError> {
    let duration_ms = ctx
        .params
        .get("duration_ms")
        .and_then(Value::as_u64)
        .unwrap_or(100);

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        duration_ms = duration_ms,
        "sleep step starting"
    );

    // Short sleeps (<250ms) aren't worth the signal-poll overhead — fall
    // back to the trivial implementation.
    const CANCEL_POLL_MS: u64 = 250;
    if duration_ms <= CANCEL_POLL_MS {
        tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
        return Ok(json!({ "slept_ms": duration_ms }));
    }

    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(duration_ms);
    loop {
        let now = std::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        let tick = remaining.min(std::time::Duration::from_millis(CANCEL_POLL_MS));
        tokio::time::sleep(tick).await;

        // Check for a pending cancel signal. A failed lookup must not
        // abort the sleep — storage glitches should be transparent here.
        //
        // Skip cancel handling if this sleep is running inside a
        // `CancellationScope` — scoped children are intentionally shielded
        // from external cancel signals (`handlers/cancellation_scope.rs`).
        // The scheduler's regular signal path will defer the cancel until
        // the scope drains, matching `cancel_scoped`'s semantics.
        let inside_scope = match ctx.storage.get_execution_tree(ctx.instance_id).await {
            Ok(tree) => {
                use orch8_types::execution::BlockType;
                let scope_ids: Vec<_> = tree
                    .iter()
                    .filter(|n| n.block_type == BlockType::CancellationScope)
                    .map(|n| n.id)
                    .collect();
                let me = tree.iter().find(|n| n.block_id == ctx.block_id);
                me.map_or(false, |n| {
                    let mut cur = n.parent_id;
                    while let Some(pid) = cur {
                        if scope_ids.contains(&pid) {
                            return true;
                        }
                        cur = tree.iter().find(|x| x.id == pid).and_then(|x| x.parent_id);
                    }
                    false
                })
            }
            Err(_) => false,
        };
        if inside_scope {
            eprintln!(
                "SLEEP-SCOPE: inside=true inst={} block={}",
                ctx.instance_id, ctx.block_id
            );
            continue;
        } else {
            eprintln!(
                "SLEEP-SCOPE: inside=false inst={} block={}",
                ctx.instance_id, ctx.block_id
            );
        }
        if let Ok(signals) = ctx.storage.get_pending_signals(ctx.instance_id).await {
            let has_cancel = signals.iter().any(|s| {
                matches!(
                    s.signal_type,
                    orch8_types::signal::SignalType::Cancel
                )
            });
            if has_cancel {
                // A `cancel` signal is waiting to be processed. Drive it
                // through the normal signal pipeline so the instance state
                // is transitioned to Cancelled. This must happen from inside
                // the handler because the evaluator doesn't re-check signals
                // between sibling step dispatches, and if we wait for the
                // next scheduler tick the remaining parallel sleep siblings
                // would complete normally and the instance would land in
                // Completed instead of Cancelled.
                //
                // We also flip every active node in the tree to `Cancelled`
                // directly, because `process_signals` can't call
                // `cancel_scoped` without a `SequenceDefinition` (which the
                // handler doesn't carry). Setting node states here is
                // idempotent with the eventual scheduler-level cancel path.
                //
                // Returning `StepError::Retryable` keeps `dispatch_plugin`
                // from overwriting our node's `Cancelled` state with
                // `Completed` (via `complete_node`) or `Failed` (via
                // `Permanent`).
                debug!(
                    instance_id = %ctx.instance_id,
                    block_id = %ctx.block_id,
                    "sleep step interrupted by cancel signal — driving cancel"
                );
                let _ = crate::signals::process_signals(
                    ctx.storage.as_ref(),
                    ctx.instance_id,
                    orch8_types::instance::InstanceState::Running,
                )
                .await;
                // Mark every active (non-terminal) node Cancelled so the
                // evaluator's termination check trips on the next iteration.
                if let Ok(tree) = ctx.storage.get_execution_tree(ctx.instance_id).await {
                    for n in tree.iter().filter(|n| {
                        matches!(
                            n.state,
                            orch8_types::execution::NodeState::Pending
                                | orch8_types::execution::NodeState::Running
                                | orch8_types::execution::NodeState::Waiting
                        )
                    }) {
                        let _ = ctx
                            .storage
                            .update_node_state(n.id, orch8_types::execution::NodeState::Cancelled)
                            .await;
                    }
                }
                return Err(StepError::Retryable {
                    message: "sleep cancelled by signal".into(),
                    details: None,
                });
            }
        }
    }

    Ok(json!({ "slept_ms": duration_ms }))
}

/// Fail handler. Always returns an error so callers can force an instance
/// into the failed state — useful for DLQ/retry tests and for sequences
/// that need an explicit failure branch.
///
/// Params:
/// - `message` (string): Error message. Defaults to "forced failure".
/// - `retryable` (bool): If true, returns `StepError::Retryable` so the
///   runner schedules another attempt; defaults to false for
///   `StepError::Permanent`, which lands the instance in the DLQ.
async fn handle_fail(ctx: StepContext) -> Result<Value, StepError> {
    let message = ctx
        .params
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("forced failure")
        .to_string();

    let retryable = ctx
        .params
        .get("retryable")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        retryable = retryable,
        "fail step executing"
    );

    if retryable {
        Err(StepError::Retryable {
            message,
            details: None,
        })
    } else {
        Err(StepError::Permanent {
            message,
            details: None,
        })
    }
}

/// HTTP request handler. Makes an HTTP request via reqwest.
///
/// Params:
/// - `url` (string, required): The URL to request.
/// - `method` (string): HTTP method. Defaults to "GET".
/// - `body` (string): Request body for POST/PUT.
/// - `headers` (object): Extra HTTP headers.
/// - `timeout_ms` (u64): Timeout in milliseconds. Defaults to 10000.
async fn handle_http_request(ctx: StepContext) -> Result<Value, StepError> {
    let url = ctx
        .params
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| StepError::Permanent {
            message: "missing required param: url".into(),
            details: None,
        })?;

    if !is_url_safe(url) {
        return Err(StepError::Permanent {
            message: "blocked: URL targets a private/internal network address".into(),
            details: None,
        });
    }

    let method = ctx
        .params
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET");

    let body_str = ctx.params.get("body").and_then(Value::as_str).unwrap_or("");

    let timeout = std::time::Duration::from_millis(
        ctx.params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(10_000),
    );

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        url = %url,
        method = %method,
        "http_request step"
    );

    let client = super::llm::http_client();

    let mut req = match method.to_uppercase().as_str() {
        "POST" => client.post(url),
        "PUT" => client.put(url),
        "PATCH" => client.patch(url),
        "DELETE" => client.delete(url),
        _ => client.get(url),
    };

    req = req.timeout(timeout);

    if let Some(headers) = ctx.params.get("headers").and_then(Value::as_object) {
        for (k, v) in headers {
            if let Some(val) = v.as_str() {
                req = req.header(k.as_str(), val);
            }
        }
    }

    if !body_str.is_empty() {
        req = req
            .header("Content-Type", "application/json")
            .body(body_str.to_string());
    }

    let resp = req.send().await.map_err(|e| StepError::Retryable {
        message: format!("HTTP request failed: {e}"),
        details: None,
    })?;

    let status = resp.status().as_u16();
    let response_body = resp.text().await.unwrap_or_default();

    if status >= 500 {
        return Err(StepError::Retryable {
            message: format!("HTTP {status} from {url}"),
            details: Some(json!({ "status": status, "body": response_body })),
        });
    }
    if status >= 400 {
        return Err(StepError::Permanent {
            message: format!("HTTP {status} from {url}"),
            details: Some(json!({ "status": status, "body": response_body })),
        });
    }

    Ok(json!({
        "status": status,
        "body": response_body,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::StorageBackend;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};
    use std::sync::Arc;

    async fn test_ctx(params: Value) -> StepContext {
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId("t".into()),
            block_id: BlockId("test".into()),
            params,
            context: ExecutionContext::default(),
            attempt: 0,
            storage,
        }
    }

    #[tokio::test]
    async fn noop_returns_empty() {
        let result = handle_noop(test_ctx(json!({})).await).await.unwrap();
        assert_eq!(result, json!({}));
    }

    #[tokio::test]
    async fn log_returns_message() {
        let result = handle_log(test_ctx(json!({"message": "hello"})).await)
            .await
            .unwrap();
        assert_eq!(result, json!({"message": "hello"}));
    }

    #[tokio::test]
    async fn log_default_message() {
        let result = handle_log(test_ctx(json!({})).await).await.unwrap();
        assert_eq!(result, json!({"message": "no message"}));
    }

    #[tokio::test]
    async fn sleep_returns_duration() {
        let result = handle_sleep(test_ctx(json!({"duration_ms": 10})).await)
            .await
            .unwrap();
        assert_eq!(result, json!({"slept_ms": 10}));
    }

    #[tokio::test]
    async fn http_request_missing_url_fails() {
        let result = handle_http_request(test_ctx(json!({})).await).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let StepError::Permanent { message, .. } = &err else {
            panic!("expected Permanent, got {err:?}");
        };
        assert!(message.contains("url"));
    }

    #[tokio::test]
    async fn register_builtins_populates_registry() {
        let mut registry = HandlerRegistry::new();
        register_builtins(&mut registry);
        assert!(registry.contains("noop"));
        assert!(registry.contains("log"));
        assert!(registry.contains("sleep"));
        assert!(registry.contains("fail"));
        assert!(registry.contains("http_request"));
        assert!(registry.contains("emit_event"));
        assert!(registry.contains("send_signal"));
        assert!(registry.contains("query_instance"));
    }

    #[tokio::test]
    async fn fail_returns_permanent_by_default() {
        let err = handle_fail(test_ctx(json!({ "message": "nope" })).await)
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = &err else {
            panic!("expected Permanent, got {err:?}");
        };
        assert_eq!(message, "nope");
    }

    #[tokio::test]
    async fn fail_honors_retryable_flag() {
        let err = handle_fail(test_ctx(json!({ "retryable": true })).await)
            .await
            .unwrap_err();
        assert!(matches!(err, StepError::Retryable { .. }));
    }

    #[tokio::test]
    async fn fail_default_message() {
        let err = handle_fail(test_ctx(json!({})).await).await.unwrap_err();
        let StepError::Permanent { message, .. } = &err else {
            panic!("expected Permanent, got {err:?}");
        };
        assert_eq!(message, "forced failure");
    }
}
