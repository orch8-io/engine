//! Built-in `mcp_call` handler — native Model Context Protocol client.
//!
//! Calls an MCP server over the **Streamable HTTP** transport (JSON-RPC 2.0
//! over a single HTTP endpoint). Unlike a chat-client MCP host (Claude
//! Desktop, Cursor) which is ephemeral and interactive, this handler runs
//! inside a durable workflow step: the call participates in the engine's
//! retry, rate-limiting, circuit-breaker, and crash-recovery machinery. A
//! tool invocation that fails transiently is retried; one that fails
//! permanently lands in the DLQ; the whole sequence resumes after a crash.
//!
//! Each invocation performs the full stateless handshake required by the
//! spec — `initialize` → `notifications/initialized` → the request — because
//! durable steps do not hold a long-lived session across process restarts.
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `url` | string | **required** | MCP server endpoint (Streamable HTTP) |
//! | `action` | string | `"call"` | `"call"` (invoke a tool) or `"list"` (discover tools) |
//! | `tool_name` | string | required for `call` | Name of the tool to invoke |
//! | `arguments` | object | `{}` | Arguments passed to the tool |
//! | `headers` | object | `{}` | Extra HTTP headers (e.g. `Authorization`) |
//! | `timeout_ms` | u64 | `30000` | Per-request timeout in ms |
//! | `protocol_version` | string | `"2025-06-18"` | MCP protocol version to advertise |
//!
//! ## Result
//!
//! - `action: "call"` → `{ "tool_name": <name>, "result": <content>, "is_error": <bool> }`
//!   where `content` is the MCP `content` array. A tool that returns
//!   `isError: true` is **not** a step error — it surfaces as `is_error: true`
//!   so an agent loop can observe and react to it.
//! - `action: "list"` → `{ "tools": [ ... ] }` — the server's tool catalog.

use std::time::Duration;

use serde_json::{json, Value};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

/// JSON-RPC id for the `initialize` request.
const ID_INITIALIZE: u64 = 1;
/// JSON-RPC id for the primary request (`tools/call` or `tools/list`).
const ID_REQUEST: u64 = 2;
/// Cap on the response body we will buffer, mirroring `tool_call`.
const MAX_RESPONSE_BYTES: usize = 10_485_760; // 10 MB
/// Protocol version advertised when the caller does not pin one.
const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";

/// Canonical dry-run stub for `handle_mcp_call`, mirroring the per-action output
/// shape so downstream templates still resolve. No network traffic.
fn mcp_dry_run_stub(action: &str, tool_name: Option<&str>, url: &str) -> Value {
    let would = json!({ "url": url, "action": action });
    let shape = match action {
        "list" => json!({ "tools": [] }),
        _ => json!({
            "tool_name": tool_name.map_or(Value::Null, |t| Value::String(t.to_string())),
            "result": Value::Null,
        }),
    };
    super::util::dry_run_stub("mcp_call", would, shape)
}

pub async fn handle_mcp_call(ctx: StepContext) -> Result<Value, StepError> {
    // Endpoint comes from an explicit `url`, or a named `server` resolved from
    // the read-only `context.config.mcp_servers` registry (engine-local; the
    // managed multi-tenant catalog is a cloud concern). A named server may
    // also supply auth headers.
    let (url, server_headers) = resolve_endpoint(&ctx.params, &ctx.context.config)?;

    if !super::builtin::is_url_safe(&url).await {
        return Err(permanent(
            "blocked: URL targets a private/internal network address",
        ));
    }

    let action = ctx
        .params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("call");

    // Build the primary request up-front so a bad `call` (missing tool_name)
    // fails permanently before any network traffic.
    let (primary, tool_name) = match action {
        "list" => (build_tools_list_request(ID_REQUEST), None),
        "call" => {
            let tool_name = ctx
                .params
                .get("tool_name")
                .and_then(Value::as_str)
                .ok_or_else(|| permanent("missing required param: tool_name (action=call)"))?;
            let arguments = ctx
                .params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            (
                build_tools_call_request(ID_REQUEST, tool_name, &arguments),
                Some(tool_name.to_string()),
            )
        }
        other => {
            return Err(permanent(format!(
                "unknown action: {other:?} (expected \"call\" or \"list\")"
            )))
        }
    };

    // Dry-run: endpoint, URL safety, action and (for `call`) tool_name are now
    // validated; skip only the network handshake/call.
    if ctx.is_dry_run() {
        return Ok(mcp_dry_run_stub(action, tool_name.as_deref(), &url));
    }

    let timeout = Duration::from_millis(
        ctx.params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(30_000),
    );

    let protocol_version = ctx
        .params
        .get("protocol_version")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);

    // Merge per-step `headers` over any server-supplied headers (step wins).
    let mut headers = server_headers;
    if let Some(h) = ctx.params.get("headers").and_then(Value::as_object) {
        for (k, v) in h {
            headers.insert(k.clone(), v.clone());
        }
    }
    let extra_headers = if headers.is_empty() {
        None
    } else {
        Some(&headers)
    };

    debug!(url = %url, action = %action, "mcp_call: handshaking");

    // 1. initialize — establishes the session and (optionally) returns an
    //    `Mcp-Session-Id` header we must echo on subsequent requests.
    let init_req = build_initialize_request(ID_INITIALIZE, protocol_version);
    let (init_status, init_ct, init_body, session_id) =
        post_jsonrpc(&url, &init_req, timeout, extra_headers, None).await?;
    parse_jsonrpc_response(init_status, &init_ct, &init_body, ID_INITIALIZE)?;

    // 2. notifications/initialized — a fire-and-forget notification (no id,
    //    no response expected). A non-2xx here is not fatal: some servers
    //    return 202 Accepted with an empty body.
    let initialized = build_initialized_notification();
    let _ = post_jsonrpc(
        &url,
        &initialized,
        timeout,
        extra_headers,
        session_id.as_deref(),
    )
    .await;

    debug!(url = %url, action = %action, "mcp_call: dispatching request");

    // 3. the actual request.
    let (status, content_type, body, _) = post_jsonrpc(
        &url,
        &primary,
        timeout,
        extra_headers,
        session_id.as_deref(),
    )
    .await?;
    let result = parse_jsonrpc_response(status, &content_type, &body, ID_REQUEST)?;

    match action {
        "list" => Ok(json!({ "tools": result.get("tools").cloned().unwrap_or(json!([])) })),
        _ => Ok(interpret_tools_call_result(
            tool_name.as_deref().unwrap_or("unknown"),
            &result,
        )),
    }
}

/// POST a JSON-RPC message to the MCP endpoint.
///
/// Returns `(status, content_type, body_bytes, session_id)`. The
/// `session_id` is read from the `Mcp-Session-Id` response header (present on
/// the `initialize` response of stateful servers).
async fn post_jsonrpc(
    url: &str,
    message: &Value,
    timeout: Duration,
    extra_headers: Option<&serde_json::Map<String, Value>>,
    session_id: Option<&str>,
) -> Result<(u16, String, Vec<u8>, Option<String>), StepError> {
    let client = super::llm::http_client();

    let mut req = client
        .post(url)
        .header("Content-Type", "application/json")
        // Streamable HTTP requires the client to accept both framings.
        .header("Accept", "application/json, text/event-stream")
        .timeout(timeout)
        .json(message);

    if let Some(sid) = session_id {
        req = req.header("Mcp-Session-Id", sid);
    }
    if let Some(headers) = extra_headers {
        for (k, v) in headers {
            if let Some(val) = v.as_str() {
                req = req.header(k.as_str(), val);
            }
        }
    }

    let resp = req.send().await.map_err(|e| {
        if e.is_timeout() || e.is_connect() {
            StepError::Retryable {
                message: format!("mcp_call network error: {e}"),
                details: None,
            }
        } else {
            StepError::Permanent {
                message: format!("mcp_call request error: {e}"),
                details: None,
            }
        }
    })?;

    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let content_length = resp.content_length().unwrap_or(0);
    if content_length > MAX_RESPONSE_BYTES as u64 {
        return Err(permanent(format!(
            "mcp response too large: {content_length} bytes exceeds 10MB limit"
        )));
    }
    let body_bytes = resp.bytes().await.map_err(|e| StepError::Retryable {
        message: format!("mcp_call body read error: {e}"),
        details: None,
    })?;
    if body_bytes.len() > MAX_RESPONSE_BYTES {
        return Err(permanent(format!(
            "mcp response too large: {} bytes exceeds 10MB limit",
            body_bytes.len()
        )));
    }

    Ok((status, content_type, body_bytes.to_vec(), session_id))
}

/// Resolve the MCP endpoint and any server-supplied auth headers.
///
/// Order of precedence:
/// 1. an explicit `url` param → used as-is, no extra headers.
/// 2. a named `server` param → looked up in the read-only
///    `context.config.mcp_servers` map (`{ "<name>": { url, token?, headers? } }`).
///    A `token` becomes an `Authorization: Bearer <token>` header.
///
/// The managed, multi-tenant server catalog is a cloud concern; this is the
/// engine-local resolution that works offline from per-instance config.
fn resolve_endpoint(
    params: &Value,
    config: &Value,
) -> Result<(String, serde_json::Map<String, Value>), StepError> {
    let mut headers = serde_json::Map::new();

    if let Some(url) = params.get("url").and_then(Value::as_str) {
        return Ok((url.to_string(), headers));
    }

    let Some(server) = params.get("server").and_then(Value::as_str) else {
        return Err(permanent("mcp_call: missing `url` or `server`"));
    };

    let entry = config
        .get("mcp_servers")
        .and_then(|m| m.get(server))
        .ok_or_else(|| {
            permanent(format!(
                "mcp_call: unknown server {server:?} (no context.config.mcp_servers entry)"
            ))
        })?;

    let url = entry
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| permanent(format!("mcp_call: server {server:?} has no url")))?
        .to_string();

    if let Some(token) = entry.get("token").and_then(Value::as_str) {
        headers.insert("Authorization".into(), json!(format!("Bearer {token}")));
    }
    if let Some(h) = entry.get("headers").and_then(Value::as_object) {
        for (k, v) in h {
            headers.insert(k.clone(), v.clone());
        }
    }

    Ok((url, headers))
}

// ---- Pure request builders (unit-tested without a network) -----------------

/// Build the JSON-RPC `initialize` request.
#[must_use]
fn build_initialize_request(id: u64, protocol_version: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": protocol_version,
            "capabilities": {},
            "clientInfo": { "name": "orch8", "version": env!("CARGO_PKG_VERSION") }
        }
    })
}

/// Build the `notifications/initialized` notification (no `id`).
#[must_use]
fn build_initialized_notification() -> Value {
    json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })
}

/// Build a `tools/call` request.
#[must_use]
fn build_tools_call_request(id: u64, tool_name: &str, arguments: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": { "name": tool_name, "arguments": arguments }
    })
}

/// Build a `tools/list` request.
#[must_use]
fn build_tools_list_request(id: u64) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": "tools/list" })
}

// ---- Pure response interpretation (unit-tested without a network) ----------

/// Parse a JSON-RPC response and return its `result` object.
///
/// Handles both Streamable-HTTP framings:
/// - `application/json` → a single JSON-RPC object (or batch array).
/// - `text/event-stream` → SSE frames; the `data:` payload carrying the
///   matching `id` is extracted.
///
/// Error mapping:
/// - 5xx → Retryable, 4xx → Permanent (HTTP-level).
/// - a JSON-RPC `error` member → Permanent (protocol-level: the request was
///   malformed or the method is unsupported — retrying will not help).
fn parse_jsonrpc_response(
    status: u16,
    content_type: &str,
    body: &[u8],
    expected_id: u64,
) -> Result<Value, StepError> {
    if status >= 500 {
        return Err(StepError::Retryable {
            message: format!("mcp server returned HTTP {status}"),
            details: Some(json!({ "status": status, "body": lossy(body) })),
        });
    }
    if status >= 400 {
        return Err(StepError::Permanent {
            message: format!("mcp server returned HTTP {status}"),
            details: Some(json!({ "status": status, "body": lossy(body) })),
        });
    }

    let message = if content_type.contains("text/event-stream") {
        extract_sse_jsonrpc(body, expected_id).ok_or_else(|| {
            permanent(format!(
                "mcp SSE response contained no JSON-RPC message with id {expected_id}"
            ))
        })?
    } else {
        let parsed: Value = serde_json::from_slice(body).map_err(|e| {
            permanent(format!(
                "mcp response was not valid JSON: {e} (body: {})",
                lossy_preview(body)
            ))
        })?;
        select_matching(parsed, expected_id).ok_or_else(|| {
            permanent(format!(
                "mcp response contained no JSON-RPC message with id {expected_id}"
            ))
        })?
    };

    if let Some(err) = message.get("error") {
        return Err(StepError::Permanent {
            message: format!(
                "mcp JSON-RPC error: {}",
                err.get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
            details: Some(err.clone()),
        });
    }

    Ok(message.get("result").cloned().unwrap_or_else(|| json!({})))
}

/// Pick the JSON-RPC message matching `expected_id` from either a single
/// object or a batch array.
fn select_matching(parsed: Value, expected_id: u64) -> Option<Value> {
    match parsed {
        Value::Array(items) => items.into_iter().find(|m| id_matches(m, expected_id)),
        obj @ Value::Object(_) => id_matches(&obj, expected_id).then_some(obj),
        _ => None,
    }
}

/// Scan SSE frames for a `data:` payload that parses to a JSON-RPC message
/// with the expected id.
fn extract_sse_jsonrpc(body: &[u8], expected_id: u64) -> Option<Value> {
    let text = String::from_utf8_lossy(body);
    for line in text.lines() {
        // SSE data lines: `data: {json}`. Ignore `event:`, `id:`, comments.
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim();
        if payload.is_empty() {
            continue;
        }
        if let Ok(msg) = serde_json::from_str::<Value>(payload) {
            if let Some(matched) = select_matching(msg, expected_id) {
                return Some(matched);
            }
        }
    }
    None
}

/// True when `msg.id` equals `expected_id`.
fn id_matches(msg: &Value, expected_id: u64) -> bool {
    msg.get("id").and_then(Value::as_u64) == Some(expected_id)
}

/// Turn a `tools/call` result into the handler's output shape.
///
/// MCP `tools/call` returns `{ "content": [...], "isError": bool }`. A tool
/// reporting `isError: true` is a *valid* result (the tool ran and signalled
/// a domain failure) — we surface it as `is_error: true` rather than a
/// `StepError`, so a `ReAct` loop can read the error content and decide its
/// next action instead of the whole step failing.
fn interpret_tools_call_result(tool_name: &str, result: &Value) -> Value {
    let content = result.get("content").cloned().unwrap_or_else(|| json!([]));
    let is_error = result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    json!({
        "tool_name": tool_name,
        "result": content,
        "is_error": is_error,
    })
}

fn permanent(message: impl Into<String>) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: None,
    }
}

/// Best-effort JSON-or-text rendering of an error body.
fn lossy(body: &[u8]) -> Value {
    serde_json::from_slice(body)
        .unwrap_or_else(|_| json!(String::from_utf8_lossy(body).to_string()))
}

/// First 512 bytes of a body, for diagnostics.
fn lossy_preview(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(512)]).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_request_shape() {
        let req = build_initialize_request(1, "2025-06-18");
        assert_eq!(req["jsonrpc"], "2.0");
        assert_eq!(req["id"], 1);
        assert_eq!(req["method"], "initialize");
        assert_eq!(req["params"]["protocolVersion"], "2025-06-18");
        assert_eq!(req["params"]["clientInfo"]["name"], "orch8");
        // capabilities must be present (servers reject a missing field).
        assert!(req["params"]["capabilities"].is_object());
    }

    #[test]
    fn initialized_notification_has_no_id() {
        let n = build_initialized_notification();
        assert_eq!(n["method"], "notifications/initialized");
        assert!(n.get("id").is_none(), "notifications must not carry an id");
    }

    #[test]
    fn tools_call_request_shape() {
        let req = build_tools_call_request(2, "search", &json!({"q": "rust"}));
        assert_eq!(req["method"], "tools/call");
        assert_eq!(req["params"]["name"], "search");
        assert_eq!(req["params"]["arguments"]["q"], "rust");
        assert_eq!(req["id"], 2);
    }

    #[test]
    fn tools_list_request_shape() {
        let req = build_tools_list_request(2);
        assert_eq!(req["method"], "tools/list");
        assert_eq!(req["id"], 2);
    }

    #[test]
    fn parse_plain_json_result() {
        let body =
            br#"{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"hi"}]}}"#;
        let result = parse_jsonrpc_response(200, "application/json", body, 2).unwrap();
        assert_eq!(result["content"][0]["text"], "hi");
    }

    #[test]
    fn parse_sse_framed_result() {
        let body =
            b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"ok\":true}}\n\n";
        let result = parse_jsonrpc_response(200, "text/event-stream", body, 2).unwrap();
        assert_eq!(result["ok"], true);
    }

    #[test]
    fn parse_sse_skips_unrelated_frames() {
        // A heartbeat/comment frame and a notification precede the real reply.
        let body = b": ping\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"found\":1}}\n\n";
        let result = parse_jsonrpc_response(200, "text/event-stream", body, 2).unwrap();
        assert_eq!(result["found"], 1);
    }

    #[test]
    fn parse_batch_array_selects_matching_id() {
        let body = br#"[{"jsonrpc":"2.0","id":99,"result":{"wrong":true}},{"jsonrpc":"2.0","id":2,"result":{"right":true}}]"#;
        let result = parse_jsonrpc_response(200, "application/json", body, 2).unwrap();
        assert_eq!(result["right"], true);
        assert!(result.get("wrong").is_none());
    }

    #[test]
    fn jsonrpc_error_is_permanent() {
        let body =
            br#"{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"Method not found"}}"#;
        let err = parse_jsonrpc_response(200, "application/json", body, 2).unwrap_err();
        match err {
            StepError::Permanent { message, details } => {
                assert!(message.contains("Method not found"));
                assert_eq!(details.unwrap()["code"], -32601);
            }
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[test]
    fn http_5xx_is_retryable() {
        let err = parse_jsonrpc_response(503, "application/json", b"down", 2).unwrap_err();
        match err {
            StepError::Retryable { details, .. } => {
                assert_eq!(details.unwrap()["status"], 503);
            }
            other @ StepError::Permanent { .. } => panic!("expected Retryable, got {other:?}"),
        }
    }

    #[test]
    fn http_4xx_is_permanent() {
        let err = parse_jsonrpc_response(401, "application/json", br#"{"e":"unauthorized"}"#, 2)
            .unwrap_err();
        match err {
            StepError::Permanent { details, .. } => {
                assert_eq!(details.unwrap()["body"]["e"], "unauthorized");
            }
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[test]
    fn missing_id_in_response_is_permanent() {
        let body = br#"{"jsonrpc":"2.0","id":7,"result":{"x":1}}"#;
        let err = parse_jsonrpc_response(200, "application/json", body, 2).unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn non_json_body_is_permanent() {
        let err = parse_jsonrpc_response(200, "application/json", b"<html>", 2).unwrap_err();
        match err {
            StepError::Permanent { message, .. } => assert!(message.contains("not valid JSON")),
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[test]
    fn interpret_call_result_passes_content_and_flags_error() {
        let ok = interpret_tools_call_result(
            "search",
            &json!({"content": [{"type": "text", "text": "result"}], "isError": false}),
        );
        assert_eq!(ok["tool_name"], "search");
        assert_eq!(ok["result"][0]["text"], "result");
        assert_eq!(ok["is_error"], false);

        let errored = interpret_tools_call_result(
            "search",
            &json!({"content": [{"type": "text", "text": "boom"}], "isError": true}),
        );
        assert_eq!(errored["is_error"], true);
        assert_eq!(errored["result"][0]["text"], "boom");
    }

    #[test]
    fn interpret_call_result_defaults_when_fields_absent() {
        let out = interpret_tools_call_result("t", &json!({}));
        assert_eq!(out["result"], json!([]));
        assert_eq!(out["is_error"], false);
    }

    #[test]
    fn resolve_endpoint_explicit_url() {
        let (url, headers) =
            resolve_endpoint(&json!({ "url": "https://x.example/mcp" }), &json!({})).unwrap();
        assert_eq!(url, "https://x.example/mcp");
        assert!(headers.is_empty());
    }

    #[test]
    fn resolve_endpoint_named_server_with_token_and_headers() {
        let config = json!({
            "mcp_servers": {
                "github": {
                    "url": "https://gh.example/mcp",
                    "token": "ghp_x",
                    "headers": { "X-Org": "acme" }
                }
            }
        });
        let (url, headers) = resolve_endpoint(&json!({ "server": "github" }), &config).unwrap();
        assert_eq!(url, "https://gh.example/mcp");
        assert_eq!(headers["Authorization"], "Bearer ghp_x");
        assert_eq!(headers["X-Org"], "acme");
    }

    #[test]
    fn resolve_endpoint_unknown_server_is_permanent() {
        let err = resolve_endpoint(&json!({ "server": "nope" }), &json!({ "mcp_servers": {} }))
            .unwrap_err();
        match err {
            StepError::Permanent { message, .. } => assert!(message.contains("unknown server")),
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[test]
    fn resolve_endpoint_server_without_url_is_permanent() {
        let config = json!({ "mcp_servers": { "s": { "token": "t" } } });
        let err = resolve_endpoint(&json!({ "server": "s" }), &config).unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn resolve_endpoint_missing_both_is_permanent() {
        let err = resolve_endpoint(&json!({ "tool_name": "x" }), &json!({})).unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn resolve_endpoint_url_takes_precedence_over_server() {
        let config = json!({ "mcp_servers": { "s": { "url": "https://server.example" } } });
        let (url, _) = resolve_endpoint(
            &json!({ "url": "https://explicit.example", "server": "s" }),
            &config,
        )
        .unwrap();
        assert_eq!(url, "https://explicit.example");
    }
}

/// Tests that drive the async network path (`handle_mcp_call`, `post_jsonrpc`)
/// against an in-process HTTP mock, so the full handshake is exercised without
/// the e2e server. The mock answers each request on its own connection
/// (`Connection: close`), dispatching by JSON-RPC `method`.
#[cfg(test)]
mod net_tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};

    /// Read one HTTP/1.1 request off the socket and return its body.
    async fn read_request_body(sock: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = sock.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            // Once headers are in, read exactly Content-Length more body bytes.
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buf[..pos]).to_lowercase();
                let want = headers
                    .split("content-length:")
                    .nth(1)
                    .and_then(|s| s.trim().split([' ', '\r', '\n']).next())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let body_start = pos + 4;
                if buf.len() >= body_start + want {
                    return String::from_utf8_lossy(&buf[body_start..body_start + want])
                        .to_string();
                }
            }
        }
        String::new()
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    /// Spawn a mock that handles `count` requests. `handler(body) -> (status, body)`.
    async fn spawn_mock<F>(count: usize, handler: F) -> String
    where
        F: Fn(String) -> (u16, String) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..count {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let body = read_request_body(&mut sock).await;
                let (status, resp) = handler(body);
                let out = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                    resp.len()
                );
                let _ = sock.write_all(out.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        format!("http://127.0.0.1:{}", addr.port())
    }

    /// Dispatch a JSON-RPC request body to a canned MCP response.
    /// Takes `String` by value to satisfy the `Fn(String)` mock handler bound.
    #[allow(clippy::needless_pass_by_value)]
    fn mcp_router(body: String) -> (u16, String) {
        let msg: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let id = msg.get("id").and_then(Value::as_u64);
        match method {
            "initialize" => (
                200,
                json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}}}}).to_string(),
            ),
            "notifications/initialized" => (202, String::new()),
            "tools/list" => (
                200,
                json!({"jsonrpc":"2.0","id":id,"result":{"tools":[{"name":"echo"}]}}).to_string(),
            ),
            "tools/call" => (
                200,
                json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":"ok"}],"isError":false}}).to_string(),
            ),
            _ => (400, json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"unknown"}}).to_string()),
        }
    }

    async fn mk_ctx(params: Value) -> StepContext {
        // Allow the loopback mock past the SSRF guard by seeding the cache —
        // not by mutating env, which would race other parallel tests.
        if let Some(u) = params.get("url").and_then(Value::as_str) {
            super::super::builtin::mark_url_safe_for_test(u).await;
        }
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params,
            context: ExecutionContext::default(),
            attempt: 0,
            storage,
            wait_for_input: None,
        }
    }

    #[test]
    fn dry_run_stub_shapes_per_action() {
        let call = mcp_dry_run_stub("call", Some("echo"), "https://mcp.example/api");
        assert_eq!(call["dry_run"], true);
        assert_eq!(call["handler"], "mcp_call");
        assert_eq!(call["tool_name"], "echo");
        assert!(call["result"].is_null());

        let list = mcp_dry_run_stub("list", None, "https://mcp.example/api");
        assert_eq!(list["dry_run"], true);
        assert_eq!(list["tools"], json!([]));
    }

    #[tokio::test]
    async fn dry_run_skips_handshake() {
        // Closed port: a real handshake would fail to connect. Ok proves skip.
        let mut ctx = mk_ctx(json!({
            "url": "http://127.0.0.1:1/mcp", "tool_name": "echo", "arguments": {}
        }))
        .await;
        ctx.context.runtime.dry_run = true;
        let out = handle_mcp_call(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["tool_name"], "echo");
    }

    #[tokio::test]
    async fn handle_call_full_handshake() {
        // initialize + initialized + tools/call = 3 connections.
        let url = spawn_mock(3, mcp_router).await;
        let ctx = mk_ctx(json!({
            "url": url,
            "tool_name": "echo",
            "arguments": { "q": "hi" }
        }))
        .await;
        let out = handle_mcp_call(ctx).await.unwrap();
        assert_eq!(out["tool_name"], "echo");
        assert_eq!(out["is_error"], false);
        assert_eq!(out["result"][0]["text"], "ok");
    }

    #[tokio::test]
    async fn handle_list_action() {
        let url = spawn_mock(3, mcp_router).await;
        let ctx = mk_ctx(json!({ "url": url, "action": "list" })).await;
        let out = handle_mcp_call(ctx).await.unwrap();
        assert_eq!(out["tools"][0]["name"], "echo");
    }

    #[tokio::test]
    async fn handle_missing_url_is_permanent() {
        let ctx = mk_ctx(json!({ "tool_name": "echo" })).await;
        let err = handle_mcp_call(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn handle_call_missing_tool_name_is_permanent() {
        let ctx = mk_ctx(json!({ "url": "https://example.com/mcp" })).await;
        // action defaults to "call"; tool_name is required and absent.
        let err = handle_mcp_call(ctx).await.unwrap_err();
        match err {
            StepError::Permanent { message, .. } => assert!(message.contains("tool_name")),
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handle_unknown_action_is_permanent() {
        let ctx = mk_ctx(json!({ "url": "https://example.com/mcp", "action": "delete" })).await;
        let err = handle_mcp_call(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn handle_jsonrpc_error_from_server_is_permanent() {
        // A server that errors on tools/call: initialize ok, initialized ok,
        // then an error response.
        let url = spawn_mock(3, |body: String| {
            let msg: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
            let id = msg.get("id").and_then(Value::as_u64);
            match method {
                "initialize" => (200, json!({"jsonrpc":"2.0","id":id,"result":{}}).to_string()),
                "notifications/initialized" => (202, String::new()),
                _ => (
                    200,
                    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":"Invalid params"}}).to_string(),
                ),
            }
        })
        .await;
        let ctx = mk_ctx(json!({ "url": url, "tool_name": "echo" })).await;
        let err = handle_mcp_call(ctx).await.unwrap_err();
        match err {
            StepError::Permanent { message, .. } => assert!(message.contains("Invalid params")),
            other @ StepError::Retryable { .. } => panic!("expected Permanent, got {other:?}"),
        }
    }
}
