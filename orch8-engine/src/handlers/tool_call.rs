//! Built-in `tool_call` handler — dispatch to an external tool endpoint.
//!
//! Makes an HTTP POST to a configured URL with tool name, arguments, and
//! instance context. Collects the JSON result.
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `url` | string | **required** | Tool endpoint URL |
//! | `tool_name` | string | `"unknown"` | Name of the tool being invoked |
//! | `arguments` | object | `{}` | Arguments passed to the tool |
//! | `method` | string | `"POST"` | HTTP method |
//! | `headers` | object | `{}` | Extra HTTP headers |
//! | `timeout_ms` | u64 | `30000` | Request timeout in ms |

use std::time::Duration;

use serde_json::{json, Value};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

pub async fn handle_tool_call(ctx: StepContext) -> Result<Value, StepError> {
    let url = ctx
        .params
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| StepError::Permanent {
            message: "missing required param: url".into(),
            details: None,
        })?;

    if !super::builtin::is_url_safe(url).await {
        return Err(StepError::Permanent {
            message: "blocked: URL targets a private/internal network address".into(),
            details: None,
        });
    }

    let tool_name = ctx
        .params
        .get("tool_name")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    let arguments = ctx.params.get("arguments").cloned().unwrap_or(json!({}));

    let timeout = Duration::from_millis(
        ctx.params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(30_000),
    );

    let method = ctx
        .params
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("POST");

    let body = json!({
        "tool_name": tool_name,
        "arguments": arguments,
        "context": {
            "instance_id": ctx.instance_id.0.to_string(),
            "block_id": ctx.block_id.0,
            "attempt": ctx.attempt,
        }
    });

    debug!(url = %url, tool_name = %tool_name, "tool_call: dispatching");

    let client = super::llm::http_client();

    let mut req = match method.to_uppercase().as_str() {
        "GET" => client.get(url),
        "PUT" => client.put(url),
        "PATCH" => client.patch(url),
        _ => client.post(url),
    };

    req = req
        .header("Content-Type", "application/json")
        .timeout(timeout)
        .json(&body);

    // Apply custom headers.
    if let Some(headers) = ctx.params.get("headers").and_then(Value::as_object) {
        for (k, v) in headers {
            if let Some(val) = v.as_str() {
                req = req.header(k.as_str(), val);
            }
        }
    }

    let resp = req.send().await.map_err(|e| {
        if e.is_timeout() || e.is_connect() {
            StepError::Retryable {
                message: format!("tool_call network error: {e}"),
                details: None,
            }
        } else {
            StepError::Permanent {
                message: format!("tool_call request error: {e}"),
                details: None,
            }
        }
    })?;

    let status = resp.status().as_u16();
    let body_bytes = resp.bytes().await.map_err(|e| StepError::Retryable {
        message: format!("tool_call body read error: {e}"),
        details: None,
    })?;

    interpret_tool_response(tool_name, status, &body_bytes)
}

/// Turn a (status, body) pair into a `StepError` or success value.
///
/// Extracted from [`handle_tool_call`] so the branching logic (success /
/// 4xx / 5xx × JSON / empty / non-JSON body) is covered by unit tests
/// without spinning up an HTTP server — `is_url_safe` rightly blocks
/// loopback, so end-to-end tests can't hit 127.0.0.1.
///
/// Rules:
/// - 2xx + empty body → `result: null`.
/// - 2xx + valid JSON → `result: <parsed>`.
/// - 2xx + non-JSON  → **Permanent** error. (Previously silently coerced
///   to `null`, masking tool-side bugs — downstream steps then ran on
///   fake null results.)
/// - 4xx → Permanent, body parsed as JSON if possible, else raw text.
/// - 5xx → Retryable, same body handling.
fn interpret_tool_response(
    tool_name: &str,
    status: u16,
    body_bytes: &[u8],
) -> Result<Value, StepError> {
    fn parse_body(bytes: &[u8]) -> Result<Value, serde_json::Error> {
        if bytes.is_empty() {
            Ok(json!(null))
        } else {
            serde_json::from_slice(bytes)
        }
    }

    if status >= 500 {
        // Error-path JSON parsing is best-effort — expose raw text on
        // failure so operators can see what the tool actually returned.
        let body_repr = parse_body(body_bytes)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(body_bytes).to_string()));
        return Err(StepError::Retryable {
            message: format!("tool returned HTTP {status}"),
            details: Some(json!({"status": status, "body": body_repr})),
        });
    }
    if status >= 400 {
        let body_repr = parse_body(body_bytes)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(body_bytes).to_string()));
        return Err(StepError::Permanent {
            message: format!("tool returned HTTP {status}"),
            details: Some(json!({"status": status, "body": body_repr})),
        });
    }

    // Success path: JSON parse failure is a hard error, not a silent
    // null. Retrying won't help — the remote tool is misbehaving.
    let resp_body = parse_body(body_bytes).map_err(|e| StepError::Permanent {
        message: format!("tool returned non-JSON body despite {status}: {e}"),
        details: Some(json!({
            "status": status,
            "body_preview": String::from_utf8_lossy(
                &body_bytes[..body_bytes.len().min(512)]
            ).to_string(),
        })),
    })?;

    Ok(json!({
        "tool_name": tool_name,
        "result": resp_body,
        "status": status,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};
    use std::sync::Arc;

    #[tokio::test]
    async fn missing_url_fails() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId("T".into()),
            block_id: BlockId("t".into()),
            params: json!({"tool_name": "search"}),
            context: ExecutionContext::default(),
            attempt: 0,
            storage,
            wait_for_input: None,
        };
        let err = handle_tool_call(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    // The following tests target `interpret_tool_response` directly rather
    // than going through `handle_tool_call`, because the handler's
    // `is_url_safe` guard correctly refuses loopback addresses — so we
    // can't point it at a test server on 127.0.0.1. Unit-testing the pure
    // body-interpretation logic gives equivalent coverage of the fix.

    /// Success status + non-JSON body must be a Permanent error, not a
    /// silent null. Previously `resp.json().await.unwrap_or(null)` would
    /// mask tool-side bugs — downstream steps ran on `result=null`.
    #[test]
    fn non_json_success_returns_permanent_error() {
        let err = interpret_tool_response("search", 200, b"<html>not json</html>").unwrap_err();
        match err {
            StepError::Permanent { message, .. } => {
                assert!(
                    message.contains("non-JSON"),
                    "expected non-JSON diagnostic, got: {message}"
                );
            }
            other @ StepError::Retryable { .. } => {
                panic!("expected Permanent non-JSON error, got: {other:?}")
            }
        }
    }

    /// Empty body on 2xx is valid — `result` is null. Lets tools that
    /// return 204 No Content (or 200 with no body) continue to work.
    #[test]
    fn empty_body_success_returns_null_result() {
        let out = interpret_tool_response("search", 200, b"").unwrap();
        assert_eq!(out.get("result"), Some(&json!(null)));
        assert_eq!(
            out.get("status").and_then(serde_json::Value::as_u64),
            Some(200)
        );
    }

    /// Valid JSON body on 2xx is parsed and returned under `result`.
    #[test]
    fn valid_json_success_returns_parsed_result() {
        let out = interpret_tool_response("search", 200, br#"{"ok":true,"n":7}"#).unwrap();
        assert_eq!(out.get("result").unwrap(), &json!({"ok": true, "n": 7}));
        assert_eq!(
            out.get("tool_name").and_then(|n| n.as_str()),
            Some("search")
        );
    }

    /// 5xx status exposes a best-effort body representation — text bodies
    /// show up as a JSON string in the details payload rather than
    /// silently becoming `null`.
    #[test]
    fn status_5xx_text_body_surfaces_raw_text_in_retryable() {
        let err = interpret_tool_response("search", 500, b"upstream broke").unwrap_err();
        match err {
            StepError::Retryable {
                details: Some(d), ..
            } => {
                assert_eq!(
                    d.get("status").and_then(serde_json::Value::as_u64),
                    Some(500)
                );
                assert_eq!(
                    d.get("body").and_then(|b| b.as_str()),
                    Some("upstream broke")
                );
            }
            other => panic!("expected Retryable with body details, got: {other:?}"),
        }
    }

    /// 4xx with JSON body parses it as JSON (not raw text) so structured
    /// error info from the tool survives to the caller.
    #[test]
    fn status_4xx_json_body_parsed_in_permanent_details() {
        let err = interpret_tool_response("search", 400, br#"{"code":"bad_arg"}"#).unwrap_err();
        match err {
            StepError::Permanent {
                details: Some(d), ..
            } => {
                assert_eq!(
                    d.get("status").and_then(serde_json::Value::as_u64),
                    Some(400)
                );
                assert_eq!(d.get("body").unwrap(), &json!({"code": "bad_arg"}));
            }
            other => panic!("expected Permanent with parsed body, got: {other:?}"),
        }
    }
}
