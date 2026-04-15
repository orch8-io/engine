//! Built-in step handlers that ship with the engine.
//!
//! - `noop` — does nothing, returns empty object
//! - `log` — logs a message from params, returns it
//! - `sleep` — sleeps for a configured duration
//! - `http_request` — makes an HTTP request (minimal TCP-based)

use serde_json::{json, Value};
use tracing::{debug, info};

use orch8_types::error::StepError;

use super::{HandlerRegistry, StepContext};

/// Register all built-in handlers on the given registry.
pub fn register_builtins(registry: &mut HandlerRegistry) {
    registry.register("noop", handle_noop);
    registry.register("log", handle_log);
    registry.register("sleep", handle_sleep);
    registry.register("http_request", handle_http_request);
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

/// HTTP request handler. Makes a simple HTTP request.
///
/// Params:
/// - `url` (string, required): The URL to request.
/// - `method` (string): HTTP method. Defaults to "GET".
/// - `body` (string): Request body for POST/PUT.
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

    let method = ctx
        .params
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET");

    let body = ctx.params.get("body").and_then(Value::as_str).unwrap_or("");

    let timeout_ms = ctx
        .params
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(10_000);

    let timeout = std::time::Duration::from_millis(timeout_ms);

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        url = %url,
        method = %method,
        "http_request step"
    );

    match send_http(url, method, body, timeout).await {
        Ok((status, response_body)) => {
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
        Err(e) => Err(StepError::Retryable {
            message: format!("HTTP request failed: {e}"),
            details: None,
        }),
    }
}

/// Minimal TCP-based HTTP client. For production, replace with reqwest.
async fn send_http(
    url: &str,
    method: &str,
    body: &str,
    timeout: std::time::Duration,
) -> Result<(u16, String), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let url_str = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"));
    let rest = url_str.ok_or("invalid URL scheme")?;

    let (host_port, path) = rest.split_once('/').unwrap_or((rest, ""));
    let path = format!("/{path}");

    let stream = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(host_port))
        .await
        .map_err(|_| "connection timeout".to_string())?
        .map_err(|e| e.to_string())?;

    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );

    let mut stream = stream;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    if !body.is_empty() {
        stream
            .write_all(body.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
    }

    let mut response = Vec::with_capacity(4096);
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|e| e.to_string())?;
    let response_str = String::from_utf8_lossy(&response);

    // Split headers from body.
    let (headers, resp_body) = response_str
        .split_once("\r\n\r\n")
        .unwrap_or((&response_str, ""));

    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(500);

    Ok((status, resp_body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId};

    fn test_ctx(params: Value) -> StepContext {
        StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("test".into()),
            params,
            context: ExecutionContext::default(),
            attempt: 0,
        }
    }

    #[tokio::test]
    async fn noop_returns_empty() {
        let result = handle_noop(test_ctx(json!({}))).await.unwrap();
        assert_eq!(result, json!({}));
    }

    #[tokio::test]
    async fn log_returns_message() {
        let result = handle_log(test_ctx(json!({"message": "hello"})))
            .await
            .unwrap();
        assert_eq!(result, json!({"message": "hello"}));
    }

    #[tokio::test]
    async fn log_default_message() {
        let result = handle_log(test_ctx(json!({}))).await.unwrap();
        assert_eq!(result, json!({"message": "no message"}));
    }

    #[tokio::test]
    async fn sleep_returns_duration() {
        let result = handle_sleep(test_ctx(json!({"duration_ms": 10})))
            .await
            .unwrap();
        assert_eq!(result, json!({"slept_ms": 10}));
    }

    #[tokio::test]
    async fn http_request_missing_url_fails() {
        let result = handle_http_request(test_ctx(json!({}))).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            StepError::Permanent { message, .. } => {
                assert!(message.contains("url"));
            }
            other => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[test]
    fn register_builtins_populates_registry() {
        let mut registry = HandlerRegistry::new();
        register_builtins(&mut registry);
        assert!(registry.contains("noop"));
        assert!(registry.contains("log"));
        assert!(registry.contains("sleep"));
        assert!(registry.contains("http_request"));
    }
}
