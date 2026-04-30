//! Built-in step handlers that ship with the engine.
//!
//! - `noop` — does nothing, returns empty object
//! - `log` — logs a message from params, returns it
//! - `sleep` — sleeps for a configured duration
//! - `http_request` — makes an HTTP request (minimal TCP-based)
//! - `fail` — always fails with a configurable message (for testing and
//!   explicit force-fail steps in user sequences)

use std::sync::OnceLock;
use std::time::Duration;

use moka::future::Cache;
use serde_json::{json, Value};
use tracing::{debug, info};

use orch8_types::error::StepError;

use super::{HandlerRegistry, StepContext};

/// Perf#5: process-wide TTL cache for `is_url_safe` results keyed by the
/// full URL string. DNS lookups inside `is_address_safe` are a significant
/// per-step cost on tight loops or handlers that re-check the same
/// destination (retries, parallel fan-out, `tool_call` invocations against
/// the same endpoint).
///
/// TTL is deliberately short — 30s — so the cache absorbs burst traffic
/// but does not give DNS-rebinding attackers a long window in which to
/// swap a resolved public IP for a private one. Negative results are not
/// cached at all: a transient DNS failure should not pin an otherwise
/// legitimate URL as unsafe for the next half-minute.
const URL_SAFETY_TTL: Duration = Duration::from_secs(30);
const URL_SAFETY_CAPACITY: u64 = 4_096;

fn url_safety_cache() -> &'static Cache<String, bool> {
    static CACHE: OnceLock<Cache<String, bool>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(URL_SAFETY_CAPACITY)
            .time_to_live(URL_SAFETY_TTL)
            .build()
    })
}

/// Check whether a resolved `host:port` address is safe to contact
/// (not a private/internal/metadata target).
///
/// Shared SSRF guard used by HTTP, gRPC, and any other outbound-request
/// handler. Returns `false` if any A/AAAA record for `addr` lands in a
/// loopback, private, link-local (incl. 169.254.169.254 cloud metadata),
/// unspecified, or IPv6 ULA (`fc00::/7`) range.
pub(crate) async fn is_address_safe(addr: &str) -> bool {
    // Allow internal addresses in test environments.
    if std::env::var("ORCH8_ALLOW_INTERNAL_URLS").is_ok() {
        return true;
    }

    let Ok(addrs) = tokio::net::lookup_host(addr).await else {
        return false;
    };
    for socket_addr in addrs {
        match socket_addr.ip() {
            std::net::IpAddr::V4(v4) => {
                if v4.is_loopback()          // 127.0.0.0/8
                    || v4.is_private()        // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                    || v4.is_link_local()     // 169.254.0.0/16 (includes 169.254.169.254)
                    || v4.is_unspecified()
                    || v4.is_documentation()  // 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
                    || v4.is_multicast()
                // 224.0.0.0/4
                {
                    return false;
                }
            }
            std::net::IpAddr::V6(v6) => {
                if v6.is_loopback()              // ::1
                    || v6.is_unspecified()
                    || v6.is_unicast_link_local() // fe80::/10
                    || v6.is_multicast()          // ff00::/8
                    || v6.is_unique_local()
                // fc00::/7
                {
                    return false;
                }
            }
        }
    }
    true
}

/// Check whether a URL is safe to request (not targeting private/internal networks).
///
/// Returns `true` if the URL uses http/https and resolves to a public IP address.
/// Returns `false` for private, loopback, link-local, and cloud metadata addresses.
///
/// Successful results are memoized for [`URL_SAFETY_TTL`]; parse failures and
/// unsafe-classifications are NOT cached, so a transient DNS failure does not
/// become a short-term denylist.
pub(crate) async fn is_url_safe(url: &str) -> bool {
    if let Some(hit) = url_safety_cache().get(url).await {
        return hit;
    }

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
    let safe = is_address_safe(&addr_str).await;
    if safe {
        url_safety_cache().insert(url.to_string(), true).await;
    }
    safe
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
    registry.register("set_state", handle_set_state);
    registry.register("get_state", handle_get_state);
    registry.register("delete_state", handle_delete_state);
    registry.register("transform", handle_transform);
    registry.register("assert", handle_assert);
    registry.register("merge_state", handle_merge_state);
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
    // Preserve the caller's `message` value verbatim in the step output so
    // whole-string template resolution (e.g. a missing `{{path}}` that
    // resolved to JSON null) propagates through as-is. Only fall back to
    // the placeholder string when `message` is absent entirely — a present
    // null must stay null.
    let message_value = ctx.params.get("message").cloned();
    let log_str = message_value
        .as_ref()
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
        .or_else(|| {
            message_value
                .as_ref()
                .filter(|v| !v.is_null())
                .map(std::string::ToString::to_string)
        })
        .unwrap_or_else(|| "no message".to_string());

    let level = ctx
        .params
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("info");

    match level {
        "debug" => debug!(
            instance_id = %ctx.instance_id,
            block_id = %ctx.block_id,
            message = %log_str,
            "log step"
        ),
        "warn" => tracing::warn!(
            instance_id = %ctx.instance_id,
            block_id = %ctx.block_id,
            message = %log_str,
            "log step"
        ),
        _ => info!(
            instance_id = %ctx.instance_id,
            block_id = %ctx.block_id,
            message = %log_str,
            "log step"
        ),
    }

    // Echo back the original value (preserving null / non-string shapes)
    // when it was provided; otherwise return the placeholder string.
    let out_message = message_value.unwrap_or_else(|| Value::String("no message".into()));
    Ok(json!({ "message": out_message }))
}

/// Returns `true` if the given block is a descendant of a `CancellationScope` node.
async fn is_inside_cancellation_scope(ctx: &StepContext) -> bool {
    match ctx.storage.get_execution_tree(ctx.instance_id).await {
        Ok(tree) => {
            use orch8_types::execution::BlockType;
            let scope_ids: Vec<_> = tree
                .iter()
                .filter(|n| n.block_type == BlockType::CancellationScope)
                .map(|n| n.id)
                .collect();
            let me = tree.iter().find(|n| n.block_id == ctx.block_id);
            me.is_some_and(|n| {
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
    }
}

/// Check if the current step is inside a `finally` branch (`branch_index` == 2)
/// of a `TryCatch` block. Steps inside finally must not be interrupted by cancel
/// signals — the finally block must complete first.
async fn is_inside_finally(ctx: &StepContext) -> bool {
    match ctx.storage.get_execution_tree(ctx.instance_id).await {
        Ok(tree) => {
            use orch8_types::execution::BlockType;
            let me = tree.iter().find(|n| n.block_id == ctx.block_id);
            me.is_some_and(|n| {
                let mut current = n;
                loop {
                    let Some(parent_id) = current.parent_id else {
                        return false;
                    };
                    let Some(parent) = tree.iter().find(|x| x.id == parent_id) else {
                        return false;
                    };
                    if current.branch_index == Some(2) && parent.block_type == BlockType::TryCatch {
                        return true;
                    }
                    current = parent;
                }
            })
        }
        Err(_) => false,
    }
}

/// Check for pending pause/cancel signals during a sleep and handle them.
async fn check_sleep_signals(ctx: &StepContext) -> Option<Result<Value, StepError>> {
    let signals = ctx
        .storage
        .get_pending_signals(ctx.instance_id)
        .await
        .ok()?;
    let has_cancel = signals
        .iter()
        .any(|s| matches!(s.signal_type, orch8_types::signal::SignalType::Cancel));
    let has_pause = signals
        .iter()
        .any(|s| matches!(s.signal_type, orch8_types::signal::SignalType::Pause));
    if has_pause && !has_cancel {
        debug!(
            instance_id = %ctx.instance_id,
            block_id = %ctx.block_id,
            "sleep step interrupted by pause signal — driving pause"
        );
        let _ = crate::signals::process_signals(
            ctx.storage.as_ref(),
            ctx.instance_id,
            orch8_types::instance::InstanceState::Running,
        )
        .await;
        if let Ok(tree) = ctx.storage.get_execution_tree(ctx.instance_id).await {
            if let Some(n) = tree.iter().find(|n| {
                n.block_id == ctx.block_id && n.state == orch8_types::execution::NodeState::Running
            }) {
                let _ = ctx
                    .storage
                    .update_node_state(n.id, orch8_types::execution::NodeState::Pending)
                    .await;
            }
        }
        return Some(Err(StepError::Retryable {
            message: "sleep paused by signal".into(),
            details: None,
        }));
    }
    if has_cancel {
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
        return Some(Err(StepError::Retryable {
            message: "sleep cancelled by signal".into(),
            details: None,
        }));
    }
    None
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
    const CANCEL_POLL_MS: u64 = 250;

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
    if duration_ms <= CANCEL_POLL_MS {
        tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
        return Ok(json!({ "slept_ms": duration_ms }));
    }

    // Perf#1: whether this sleep is inside a cancellation scope or a `finally`
    // branch is a *static* property of its position in the execution tree —
    // it cannot change while we're asleep. The previous loop re-queried the
    // full execution tree every 250ms, producing ~14,400 DB round-trips per
    // hour of sleep per in-flight instance. Resolve both flags once up front.
    let suppress_signals =
        is_inside_cancellation_scope(&ctx).await || is_inside_finally(&ctx).await;

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(duration_ms);
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
        if suppress_signals {
            continue;
        }
        if let Some(result) = check_sleep_signals(&ctx).await {
            return result;
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

    if !is_url_safe(url).await {
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

    let mut req = if method.eq_ignore_ascii_case("POST") {
        client.post(url)
    } else if method.eq_ignore_ascii_case("PUT") {
        client.put(url)
    } else if method.eq_ignore_ascii_case("PATCH") {
        client.patch(url)
    } else if method.eq_ignore_ascii_case("DELETE") {
        client.delete(url)
    } else {
        client.get(url)
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
    let response_body = resp.text().await.map_err(|e| StepError::Retryable {
        message: format!("HTTP request failed reading body from {url}: {e}"),
        details: None,
    })?;

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

/// Set a key-value pair in the instance's KV state store.
///
/// Params:
/// - `key` (string, required): The key to set.
/// - `value` (any, required): The JSON value to store.
async fn handle_set_state(ctx: StepContext) -> Result<Value, StepError> {
    let key = ctx.params.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
        StepError::Permanent {
            message: "set_state: `key` (string) is required".into(),
            details: None,
        }
    })?;
    let value = ctx.params.get("value").ok_or_else(|| StepError::Permanent {
        message: "set_state: `value` is required".into(),
        details: None,
    })?;
    ctx.storage
        .set_instance_kv(ctx.instance_id, key, value)
        .await
        .map_err(|e| StepError::Retryable {
            message: format!("set_state storage error: {e}"),
            details: None,
        })?;
    Ok(json!({"key": key, "value": value}))
}

/// Get a value from the instance's KV state store.
///
/// Params:
/// - `key` (string, required): The key to retrieve.
///
/// Returns `{"key": "...", "value": ...}` or `{"key": "...", "value": null}` if not found.
async fn handle_get_state(ctx: StepContext) -> Result<Value, StepError> {
    let key = ctx.params.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
        StepError::Permanent {
            message: "get_state: `key` (string) is required".into(),
            details: None,
        }
    })?;
    let value = ctx
        .storage
        .get_instance_kv(ctx.instance_id, key)
        .await
        .map_err(|e| StepError::Retryable {
            message: format!("get_state storage error: {e}"),
            details: None,
        })?
        .unwrap_or(Value::Null);
    Ok(json!({"key": key, "value": value}))
}

/// Delete a key from the instance's KV state store.
///
/// Params:
/// - `key` (string, required): The key to delete.
async fn handle_delete_state(ctx: StepContext) -> Result<Value, StepError> {
    let key = ctx.params.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
        StepError::Permanent {
            message: "delete_state: `key` (string) is required".into(),
            details: None,
        }
    })?;
    ctx.storage
        .delete_instance_kv(ctx.instance_id, key)
        .await
        .map_err(|e| StepError::Retryable {
            message: format!("delete_state storage error: {e}"),
            details: None,
        })?;
    Ok(json!({"key": key, "deleted": true}))
}

/// Transform handler. Returns its resolved params as-is, enabling pure data
/// transformation via template expressions without external calls.
///
/// Params: any JSON object — every value is template-resolved before dispatch.
/// Output: the resolved params object.
async fn handle_transform(ctx: StepContext) -> Result<Value, StepError> {
    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        "transform step executed"
    );
    Ok(ctx.params)
}

/// Assert handler. Evaluates a condition expression and fails if falsy.
///
/// Params:
/// - `condition` (string, required): Expression to evaluate (same syntax as router/loop conditions).
/// - `message` (string): Error message on failure. Defaults to "assertion failed".
async fn handle_assert(ctx: StepContext) -> Result<Value, StepError> {
    let condition = ctx
        .params
        .get("condition")
        .and_then(Value::as_str)
        .ok_or_else(|| StepError::Permanent {
            message: "assert: `condition` (string) is required".into(),
            details: None,
        })?;

    let result = crate::expression::evaluate_condition(
        condition,
        &ctx.context,
        &serde_json::json!({}),
    );

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        condition = %condition,
        result = result,
        "assert step evaluated"
    );

    if result {
        Ok(json!({"condition": condition, "passed": true}))
    } else {
        let message = ctx
            .params
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("assertion failed")
            .to_string();
        Err(StepError::Permanent {
            message: format!("assert: {message} (condition: {condition})"),
            details: Some(json!({"condition": condition, "passed": false})),
        })
    }
}

/// Merge multiple key-value pairs into instance state in one step.
///
/// Params:
/// - `values` (object, required): Map of key→value pairs to store.
async fn handle_merge_state(ctx: StepContext) -> Result<Value, StepError> {
    let values = ctx
        .params
        .get("values")
        .and_then(Value::as_object)
        .ok_or_else(|| StepError::Permanent {
            message: "merge_state: `values` (object) is required".into(),
            details: None,
        })?;

    let mut keys = Vec::with_capacity(values.len());
    for (key, value) in values {
        ctx.storage
            .set_instance_kv(ctx.instance_id, key, value)
            .await
            .map_err(|e| StepError::Retryable {
                message: format!("merge_state storage error for key `{key}`: {e}"),
                details: None,
            })?;
        keys.push(key.clone());
    }

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        keys_count = keys.len(),
        "merge_state step executed"
    );

    Ok(json!({"merged_keys": keys}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::StorageBackend;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};
    use std::sync::Arc;

    async fn mk_test_storage() -> Arc<dyn StorageBackend> {
        Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        )
    }

    async fn test_ctx(params: Value) -> StepContext {
        let storage: Arc<dyn StorageBackend> = mk_test_storage().await;
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId("t".into()),
            block_id: BlockId("test".into()),
            params,
            context: ExecutionContext::default(),
            attempt: 0,
            storage,
            wait_for_input: None,
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
        assert!(registry.contains("set_state"));
        assert!(registry.contains("get_state"));
        assert!(registry.contains("delete_state"));
        assert!(registry.contains("transform"));
        assert!(registry.contains("assert"));
        assert!(registry.contains("merge_state"));
    }

    #[tokio::test]
    async fn set_state_stores_value() {
        let result = handle_set_state(
            test_ctx(json!({"key": "counter", "value": 42})).await,
        )
        .await
        .unwrap();
        assert_eq!(result["key"], "counter");
        assert_eq!(result["value"], 42);
    }

    #[tokio::test]
    async fn set_state_missing_key_fails() {
        let result = handle_set_state(test_ctx(json!({"value": 1})).await).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_state_returns_null_for_missing() {
        let result = handle_get_state(
            test_ctx(json!({"key": "nonexistent"})).await,
        )
        .await
        .unwrap();
        assert_eq!(result["value"], Value::Null);
    }

    #[tokio::test]
    async fn set_then_get_state_roundtrip() {
        let storage = mk_test_storage().await;
        let instance_id = InstanceId::new();

        let set_ctx = StepContext {
            instance_id,
            tenant_id: TenantId("t".into()),
            block_id: BlockId("s1".into()),
            params: json!({"key": "color", "value": "blue"}),
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        handle_set_state(set_ctx).await.unwrap();

        let get_ctx = StepContext {
            instance_id,
            tenant_id: TenantId("t".into()),
            block_id: BlockId("s2".into()),
            params: json!({"key": "color"}),
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        let result = handle_get_state(get_ctx).await.unwrap();
        assert_eq!(result["value"], "blue");
    }

    #[tokio::test]
    async fn delete_state_removes_key() {
        let storage = mk_test_storage().await;
        let instance_id = InstanceId::new();

        let set_ctx = StepContext {
            instance_id,
            tenant_id: TenantId("t".into()),
            block_id: BlockId("s1".into()),
            params: json!({"key": "temp", "value": true}),
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        handle_set_state(set_ctx).await.unwrap();

        let del_ctx = StepContext {
            instance_id,
            tenant_id: TenantId("t".into()),
            block_id: BlockId("s2".into()),
            params: json!({"key": "temp"}),
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        let result = handle_delete_state(del_ctx).await.unwrap();
        assert_eq!(result["deleted"], true);

        let get_ctx = StepContext {
            instance_id,
            tenant_id: TenantId("t".into()),
            block_id: BlockId("s3".into()),
            params: json!({"key": "temp"}),
            context: ExecutionContext::default(),
            attempt: 0,
            storage,
            wait_for_input: None,
        };
        let result = handle_get_state(get_ctx).await.unwrap();
        assert_eq!(result["value"], Value::Null);
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

    // --- transform handler tests ---

    #[tokio::test]
    async fn transform_passes_through_params() {
        let params = json!({"a": 1, "b": "hello", "c": [1, 2, 3]});
        let result = handle_transform(test_ctx(params.clone()).await)
            .await
            .unwrap();
        assert_eq!(result, params);
    }

    #[tokio::test]
    async fn transform_empty_params() {
        let result = handle_transform(test_ctx(json!({})).await)
            .await
            .unwrap();
        assert_eq!(result, json!({}));
    }

    #[tokio::test]
    async fn transform_null_params() {
        let result = handle_transform(test_ctx(Value::Null).await)
            .await
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    // --- assert handler tests ---

    #[tokio::test]
    async fn assert_true_condition_passes() {
        let result = handle_assert(test_ctx(json!({"condition": "true"})).await)
            .await
            .unwrap();
        assert_eq!(result["passed"], true);
        assert_eq!(result["condition"], "true");
    }

    #[tokio::test]
    async fn assert_false_condition_fails() {
        let err = handle_assert(test_ctx(json!({"condition": "false"})).await)
            .await
            .unwrap_err();
        let StepError::Permanent { message, details } = &err else {
            panic!("expected Permanent, got {err:?}");
        };
        assert!(message.contains("assertion failed"));
        assert_eq!(details.as_ref().unwrap()["passed"], false);
    }

    #[tokio::test]
    async fn assert_custom_message_on_failure() {
        let err = handle_assert(
            test_ctx(json!({"condition": "false", "message": "score too low"})).await,
        )
        .await
        .unwrap_err();
        let StepError::Permanent { message, .. } = &err else {
            panic!("expected Permanent, got {err:?}");
        };
        assert!(message.contains("score too low"));
    }

    #[tokio::test]
    async fn assert_missing_condition_fails() {
        let err = handle_assert(test_ctx(json!({"message": "oops"})).await)
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = &err else {
            panic!("expected Permanent, got {err:?}");
        };
        assert!(message.contains("`condition`"));
    }

    // --- merge_state handler tests ---

    #[tokio::test]
    async fn merge_state_stores_multiple_keys() {
        let storage = mk_test_storage().await;
        let instance_id = InstanceId::new();

        let ctx = StepContext {
            instance_id,
            tenant_id: TenantId("t".into()),
            block_id: BlockId("ms".into()),
            params: json!({"values": {"color": "red", "count": 42, "active": true}}),
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        let result = handle_merge_state(ctx).await.unwrap();
        let merged: Vec<String> = result["merged_keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(merged.len(), 3);
        assert!(merged.contains(&"color".to_string()));
        assert!(merged.contains(&"count".to_string()));
        assert!(merged.contains(&"active".to_string()));

        let val = storage
            .get_instance_kv(instance_id, "color")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(val, json!("red"));
        let val = storage
            .get_instance_kv(instance_id, "count")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(val, json!(42));
    }

    #[tokio::test]
    async fn merge_state_missing_values_fails() {
        let err = handle_merge_state(test_ctx(json!({"key": "x"})).await)
            .await
            .unwrap_err();
        let StepError::Permanent { message, .. } = &err else {
            panic!("expected Permanent, got {err:?}");
        };
        assert!(message.contains("`values`"));
    }

    #[tokio::test]
    async fn merge_state_empty_values_succeeds() {
        let result = handle_merge_state(test_ctx(json!({"values": {}})).await)
            .await
            .unwrap();
        assert_eq!(result["merged_keys"], json!([]));
    }
}
