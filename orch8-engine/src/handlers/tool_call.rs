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
    let resp_body: Value = resp.json().await.unwrap_or(json!(null));

    if status >= 500 {
        return Err(StepError::Retryable {
            message: format!("tool returned HTTP {status}"),
            details: Some(json!({"status": status, "body": resp_body})),
        });
    }
    if status >= 400 {
        return Err(StepError::Permanent {
            message: format!("tool returned HTTP {status}"),
            details: Some(json!({"status": status, "body": resp_body})),
        });
    }

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
}
