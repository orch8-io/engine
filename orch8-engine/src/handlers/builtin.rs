//! Built-in step handlers that ship with the engine.
//!
//! - `noop` — does nothing, returns empty object
//! - `log` — logs a message from params, returns it
//! - `sleep` — sleeps for a configured duration
//! - `http_request` — makes an HTTP request (minimal TCP-based)

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

    tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;

    Ok(json!({ "slept_ms": duration_ms }))
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
        assert!(registry.contains("http_request"));
        assert!(registry.contains("emit_event"));
        assert!(registry.contains("send_signal"));
        assert!(registry.contains("query_instance"));
    }
}
