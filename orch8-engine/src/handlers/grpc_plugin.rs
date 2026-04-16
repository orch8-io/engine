//! gRPC sidecar plugin handler.
//!
//! Steps with handler names prefixed `grpc://` dispatch to an external gRPC
//! endpoint. The protocol uses a simple JSON-over-HTTP/2 unary call:
//!
//! - Handler name format: `grpc://host:port/service.Method`
//! - Request: JSON body with step context (params, instance context, block ID)
//! - Response: JSON body parsed as step output
//!
//! This enables a plugin architecture where external services (in any language)
//! can implement step handlers.

use std::sync::LazyLock;
use std::time::Duration;

use serde_json::{json, Value};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::StepContext;

/// Shared HTTP/2 client for all gRPC plugin calls. Reusing the client enables
/// connection pooling across invocations.
static GRPC_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .http2_prior_knowledge()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to build gRPC HTTP client, using default");
            reqwest::Client::new()
        })
});

/// Check if a handler name is a gRPC plugin handler.
pub fn is_grpc_handler(handler_name: &str) -> bool {
    handler_name.starts_with("grpc://")
}

/// Parse endpoint from handler name: `grpc://host:port/service.Method`
fn parse_endpoint(handler: &str) -> Result<(&str, &str), StepError> {
    let stripped = handler.strip_prefix("grpc://").unwrap_or(handler);
    stripped.split_once('/').ok_or_else(|| StepError::Permanent {
        message: format!(
            "grpc plugin: invalid endpoint format '{handler}', expected 'grpc://host:port/service.Method'"
        ),
        details: None,
    })
}

/// Execute a step by calling an external gRPC-compatible endpoint.
///
/// Uses reqwest with HTTP/2 to call the endpoint. The request body is JSON
/// containing the step context. The response body is parsed as JSON.
pub async fn handle_grpc_plugin(ctx: StepContext) -> Result<Value, StepError> {
    let endpoint = ctx
        .params
        .get("_grpc_endpoint")
        .and_then(Value::as_str)
        .ok_or_else(|| StepError::Permanent {
            message: "grpc plugin: missing _grpc_endpoint in params".into(),
            details: None,
        })?;

    let (addr, method) = parse_endpoint(endpoint)?;

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        addr = %addr,
        method = %method,
        "dispatching step to gRPC plugin"
    );

    let request_payload = json!({
        "instance_id": ctx.instance_id.to_string(),
        "block_id": ctx.block_id.to_string(),
        "params": ctx.params,
        "context": {
            "data": ctx.context.data,
            "config": ctx.context.config,
        },
        "attempt": ctx.attempt,
    });

    let url = format!("http://{addr}/{method}");

    let response = GRPC_CLIENT
        .post(&url)
        .header("content-type", "application/json")
        .json(&request_payload)
        .send()
        .await
        .map_err(|e| StepError::Retryable {
            message: format!("grpc plugin: request failed to {addr}: {e}"),
            details: None,
        })?;

    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();

    if status >= 500 {
        return Err(StepError::Retryable {
            message: format!("grpc plugin: server error {status} from {url}"),
            details: Some(json!({ "status": status, "body": body })),
        });
    }
    if status >= 400 {
        return Err(StepError::Permanent {
            message: format!("grpc plugin: client error {status} from {url}"),
            details: Some(json!({ "status": status, "body": body })),
        });
    }

    let output: Value = serde_json::from_str(&body).unwrap_or_else(|_| {
        warn!("grpc plugin: response is not valid JSON, wrapping as string");
        json!({ "raw": body })
    });

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_grpc_handler_detects_prefix() {
        assert!(is_grpc_handler("grpc://localhost:50051/MyService.Execute"));
        assert!(!is_grpc_handler("http_request"));
        assert!(!is_grpc_handler("noop"));
    }

    #[test]
    fn parse_endpoint_extracts_parts() {
        let (addr, method) = parse_endpoint("grpc://localhost:50051/MyService.Execute").unwrap();
        assert_eq!(addr, "localhost:50051");
        assert_eq!(method, "MyService.Execute");
    }

    #[test]
    fn parse_endpoint_rejects_invalid() {
        assert!(parse_endpoint("grpc://localhost-no-slash").is_err());
    }

    #[test]
    fn parse_endpoint_with_nested_path() {
        let (addr, method) = parse_endpoint("grpc://10.0.0.1:9090/pkg.Svc/Method").unwrap();
        assert_eq!(addr, "10.0.0.1:9090");
        assert_eq!(method, "pkg.Svc/Method");
    }

    #[test]
    fn is_grpc_handler_edge_cases() {
        assert!(!is_grpc_handler("wasm://my-plugin"));
        assert!(!is_grpc_handler(""));
        assert!(is_grpc_handler("grpc://"));
    }

    #[test]
    fn parse_endpoint_strips_prefix() {
        // Even without grpc:// prefix, parse_endpoint still works on raw input.
        let (addr, method) = parse_endpoint("host:50051/Svc.Run").unwrap();
        assert_eq!(addr, "host:50051");
        assert_eq!(method, "Svc.Run");
    }
}
