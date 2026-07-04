//! MCP server mode — expose Orch8 operations as Model Context Protocol tools.
//!
//! The mirror image of `orch8-engine/src/handlers/mcp.rs` (the MCP *client*):
//! this module makes Orch8 itself an MCP **server** over the Streamable HTTP
//! transport, so Claude Code / Cursor / any MCP host can launch and supervise
//! durable workflows — a durable arm for an otherwise ephemeral agent.
//!
//! ## Protocol surface (v1)
//!
//! A single `POST /mcp` endpoint speaking JSON-RPC 2.0, one message per POST
//! (no batches), always answering with `Content-Type: application/json` —
//! SSE-framed streaming responses are intentionally out of scope for v1.
//!
//! - `initialize` → protocol version + `{"tools": {}}` capabilities.
//! - any notification (no `id`, e.g. `notifications/initialized`) → HTTP 202.
//! - `ping` → `{}` (keepalive; some hosts poll it).
//! - `tools/list` → the eight-tool catalog below.
//! - `tools/call` → dispatch to the matching REST handler logic.
//!
//! Error contract: **domain** failures (instance not found, wrong state, …)
//! come back as a successful `tools/call` result with `isError: true`, so an
//! agent loop can read the message and react. **Protocol** failures map to
//! JSON-RPC error objects: `-32700` malformed JSON, `-32600` invalid request,
//! `-32601` unknown method, `-32602` invalid `tools/call` params — including
//! an unknown tool name, which we surface as `-32602` (not `isError`) because
//! the catalog is static and a bad name is a caller bug, not a domain outcome.
//!
//! Tenant scoping flows through unchanged: the route is mounted inside
//! `api_routes` (in `lib.rs`), so the same auth / tenant middleware that guards the
//! REST routes injects a [`crate::auth::TenantContext`], and every tool calls
//! the very REST handlers that enforce it.
//!
//! ## Quickstart
//!
//! ```bash
//! # 1. handshake
//! curl -s http://localhost:8080/api/v1/mcp \
//!   -H 'Content-Type: application/json' \
//!   -d '{"jsonrpc":"2.0","id":1,"method":"initialize",
//!        "params":{"protocolVersion":"2025-06-18","capabilities":{},
//!                  "clientInfo":{"name":"curl","version":"0"}}}'
//!
//! # 2. launch a workflow
//! curl -s http://localhost:8080/api/v1/mcp \
//!   -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
//!   -d '{"jsonrpc":"2.0","id":2,"method":"tools/call",
//!        "params":{"name":"create_instance",
//!                  "arguments":{"sequence_name":"daily-report"}}}'
//! ```

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use uuid::Uuid;

use orch8_types::ids::{Namespace, TenantId};
use orch8_types::signal::SignalType;

use crate::AppState;
use crate::auth::OptionalTenant;
use crate::error::ApiError;

/// Protocol version advertised when the client requests one we don't know.
const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
/// Versions we echo back verbatim when a client pins one of them.
const KNOWN_PROTOCOL_VERSIONS: &[&str] = &["2024-11-05", "2025-03-26", "2025-06-18"];

/// JSON-RPC 2.0 error codes (protocol-level failures only — domain failures
/// surface as `isError: true` tool results instead, see module docs).
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

pub fn routes() -> Router<AppState> {
    Router::new().route("/mcp", post(handle_mcp))
}

/// A tool invocation outcome: `Ok` is the domain result rendered into the
/// `content` array; `Err` is a domain failure message (`isError: true`).
type ToolResult = Result<Value, String>;

/// Single MCP endpoint: parse one JSON-RPC message and dispatch it.
///
/// The body is taken raw (not via [`Json`]) so malformed JSON can be answered
/// with a proper `-32700` JSON-RPC error instead of an opaque axum 400.
async fn handle_mcp(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    body: Bytes,
) -> Response {
    let msg: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return rpc_error(&Value::Null, PARSE_ERROR, format!("parse error: {e}")),
    };
    if !msg.is_object() {
        return rpc_error(
            &Value::Null,
            INVALID_REQUEST,
            "expected a single JSON-RPC object (batch requests are not supported)",
        );
    }
    let id = msg.get("id").cloned();
    if msg.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return rpc_error(
            id.as_ref().unwrap_or(&Value::Null),
            INVALID_REQUEST,
            "missing or invalid jsonrpc field (expected \"2.0\")",
        );
    }
    let Some(method) = msg.get("method").and_then(Value::as_str) else {
        return rpc_error(
            id.as_ref().unwrap_or(&Value::Null),
            INVALID_REQUEST,
            "missing method",
        );
    };

    // Notifications (no id) expect no JSON-RPC reply: 202 Accepted, empty
    // body — mirrors what the engine-side MCP client tolerates on
    // `notifications/initialized`.
    let Some(id) = id else {
        return StatusCode::ACCEPTED.into_response();
    };

    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let outcome = match method {
        "initialize" => Ok(initialize_result(&params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_catalog() })),
        "tools/call" => tools_call(state, tenant_ctx, &params).await,
        other => Err((METHOD_NOT_FOUND, format!("method not found: {other}"))),
    };
    match outcome {
        Ok(result) => rpc_result(&id, &result),
        Err((code, message)) => rpc_error(&id, code, message),
    }
}

/// Build the `initialize` result: echo the client's protocol version when we
/// know it, otherwise advertise our default.
fn initialize_result(params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_str);
    let version = requested
        .filter(|v| KNOWN_PROTOCOL_VERSIONS.contains(v))
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "orch8", "version": env!("CARGO_PKG_VERSION") },
    })
}

/// Dispatch a `tools/call` request to the matching tool implementation.
///
/// Returns the MCP `tools/call` result envelope; only structurally invalid
/// params (missing/unknown tool name) escape as JSON-RPC errors.
async fn tools_call(
    state: AppState,
    tenant_ctx: OptionalTenant,
    params: &Value,
) -> Result<Value, (i64, String)> {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return Err((INVALID_PARAMS, "tools/call requires params.name".into()));
    };
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let result = match name {
        "list_sequences" => tool_list_sequences(state, tenant_ctx, &args).await,
        "create_instance" => tool_create_instance(state, tenant_ctx, &args).await,
        "get_instance_status" => tool_get_instance_status(state, tenant_ctx, &args).await,
        "get_instance_outputs" => tool_get_instance_outputs(state, tenant_ctx, &args).await,
        "send_signal" => tool_send_signal(state, tenant_ctx, &args).await,
        "retry_instance" => tool_retry_instance(state, tenant_ctx, &args).await,
        "list_dlq" => tool_list_dlq(state, tenant_ctx, &args).await,
        "get_usage" => tool_get_usage(state, tenant_ctx, &args).await,
        // Unknown tool name → -32602 (documented choice, see module docs):
        // the catalog is static, so a bad name is a protocol-level caller
        // bug rather than a domain outcome.
        other => return Err((INVALID_PARAMS, format!("unknown tool: {other}"))),
    };

    Ok(match result {
        Ok(value) => tool_ok(&value),
        Err(message) => tool_err(&message),
    })
}

// ---- Tool implementations ---------------------------------------------------
//
// Each tool funnels into the corresponding REST handler so behaviour — tenant
// enforcement, validation, status semantics — cannot drift between the two
// surfaces. The handler's `impl IntoResponse` body is decoded back to JSON;
// its `ApiError` becomes the domain-error text.

/// `list_sequences`: tenant-scoped sequence catalog, optional namespace filter.
async fn tool_list_sequences(
    state: AppState,
    tenant_ctx: OptionalTenant,
    args: &Value,
) -> ToolResult {
    let mut query = serde_json::Map::new();
    if let Some(ns) = args.get("namespace").and_then(Value::as_str) {
        query.insert("namespace".into(), json!(ns));
    }
    if let Some(limit) = args.get("limit").and_then(Value::as_u64) {
        query.insert("limit".into(), json!(limit));
    }
    let q: crate::sequences::ListSequencesQuery = parse_args(Value::Object(query))?;
    rest_json(crate::sequences::list_sequences(State(state), tenant_ctx, Query(q)).await).await
}

/// `create_instance`: launch a durable workflow from a sequence id or name.
async fn tool_create_instance(
    state: AppState,
    tenant_ctx: OptionalTenant,
    args: &Value,
) -> ToolResult {
    let namespace = args
        .get("namespace")
        .and_then(Value::as_str)
        .unwrap_or("default");
    // Tenant resolution order: explicit argument, then the middleware-bound
    // tenant, then "default". `create_instance` (REST) re-checks the result
    // against the header context, so a mismatching explicit arg still 403s.
    let tenant = args
        .get("tenant_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            tenant_ctx
                .as_ref()
                .map(|axum::Extension(ctx)| ctx.tenant_id.as_str().to_owned())
        })
        .unwrap_or_else(|| "default".to_owned());

    let sequence_id = if let Some(raw) = args.get("sequence_id").and_then(Value::as_str) {
        Uuid::parse_str(raw).map_err(|e| format!("invalid sequence_id: {e}"))?
    } else {
        let name = args
            .get("sequence_name")
            .and_then(Value::as_str)
            .ok_or("create_instance requires sequence_id or sequence_name")?;
        let seq = state
            .storage
            .get_sequence_by_name(
                &TenantId::unchecked(tenant.clone()),
                &Namespace::new(namespace.to_owned()),
                name,
                None,
            )
            .await
            .map_err(|e| format!("storage error: {e}"))?
            .ok_or_else(|| {
                format!("not found: sequence {name:?} (tenant {tenant:?}, namespace {namespace:?})")
            })?;
        seq.id.into_uuid()
    };

    let context = args
        .get("context")
        .filter(|c| c.is_object())
        .cloned()
        .unwrap_or_else(|| json!({}));

    let mut body = json!({
        "sequence_id": sequence_id,
        "tenant_id": tenant,
        "namespace": namespace,
        "context": context,
    });
    for key in ["metadata", "dry_run", "idempotency_key", "budget"] {
        if let Some(v) = args.get(key) {
            body[key] = v.clone();
        }
    }

    let req: crate::instances::CreateInstanceRequest = parse_args(body)?;
    rest_json(crate::instances::create_instance(State(state), tenant_ctx, Json(req)).await).await
}

/// `get_instance_status`: condensed view of one instance — state, timestamps,
/// and a top-level `paused_reason` when the metadata carries one.
async fn tool_get_instance_status(
    state: AppState,
    tenant_ctx: OptionalTenant,
    args: &Value,
) -> ToolResult {
    let id = arg_uuid(args, "instance_id")?;
    let instance =
        rest_json(crate::instances::get_instance(State(state), tenant_ctx, Path(id)).await).await?;
    let mut status = json!({
        "id": instance["id"],
        "state": instance["state"],
        "sequence_id": instance["sequence_id"],
        "namespace": instance["namespace"],
        "created_at": instance["created_at"],
        "updated_at": instance["updated_at"],
        "next_fire_at": instance["next_fire_at"],
        "metadata": instance["metadata"],
    });
    if let Some(reason) = instance
        .get("metadata")
        .and_then(|m| m.get("paused_reason"))
    {
        status["paused_reason"] = reason.clone();
    }
    Ok(status)
}

/// `get_instance_outputs`: per-block outputs of one instance.
async fn tool_get_instance_outputs(
    state: AppState,
    tenant_ctx: OptionalTenant,
    args: &Value,
) -> ToolResult {
    let id = arg_uuid(args, "instance_id")?;
    rest_json(crate::instances::get_outputs(State(state), tenant_ctx, Path(id)).await).await
}

/// `send_signal`: enqueue a signal (`pause` / `resume` / `cancel` /
/// `update_context` / `custom:<name>`) with an optional payload.
async fn tool_send_signal(state: AppState, tenant_ctx: OptionalTenant, args: &Value) -> ToolResult {
    let id = arg_uuid(args, "instance_id")?;
    let signal_type = arg_str(args, "signal")?
        .parse::<SignalType>()
        .map_err(|e| e.to_string())?;
    let req = crate::instances::SendSignalRequest {
        signal_type,
        payload: args.get("payload").cloned().unwrap_or(Value::Null),
    };
    rest_json(crate::instances::send_signal(State(state), tenant_ctx, Path(id), Json(req)).await)
        .await
}

/// `retry_instance`: reschedule a Failed instance for immediate re-execution.
async fn tool_retry_instance(
    state: AppState,
    tenant_ctx: OptionalTenant,
    args: &Value,
) -> ToolResult {
    let id = arg_uuid(args, "instance_id")?;
    rest_json(crate::instances::retry_instance(State(state), tenant_ctx, Path(id)).await).await
}

/// `list_dlq`: failed instances (the dead-letter queue), optional limit.
async fn tool_list_dlq(state: AppState, tenant_ctx: OptionalTenant, args: &Value) -> ToolResult {
    let mut query = serde_json::Map::new();
    if let Some(limit) = args.get("limit").and_then(Value::as_u64) {
        query.insert("limit".into(), json!(limit));
    }
    let q: crate::instances::ListQuery = parse_args(Value::Object(query))?;
    rest_json(crate::instances::list_dlq(State(state), tenant_ctx, Query(q)).await).await
}

/// `get_usage`: tenant-scoped LLM token/cost aggregation over a time window.
async fn tool_get_usage(state: AppState, tenant_ctx: OptionalTenant, args: &Value) -> ToolResult {
    let mut query = serde_json::Map::new();
    for key in ["tenant", "start", "end"] {
        if let Some(v) = args.get(key) {
            query.insert(key.into(), v.clone());
        }
    }
    let q: crate::usage::UsageQuery = parse_args(Value::Object(query))?;
    rest_json(crate::usage::get_usage(State(state), tenant_ctx, Query(q)).await).await
}

// ---- Plumbing ---------------------------------------------------------------

/// Decode a REST handler outcome into JSON: `Ok` bodies are parsed back into
/// a [`Value`], `Err(ApiError)` becomes the domain-error message.
/// Maximum response body the MCP bridge will buffer into memory.
const MAX_REST_JSON_BYTES: usize = 10 * 1024 * 1024;

async fn rest_json<T: IntoResponse>(res: Result<T, ApiError>) -> ToolResult {
    let resp = res.map_err(|e| e.to_string())?.into_response();
    let bytes = axum::body::to_bytes(resp.into_body(), MAX_REST_JSON_BYTES)
        .await
        .map_err(|e| format!("internal: failed to read handler response: {e}"))?;
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| format!("internal: handler returned non-JSON body: {e}"))
}

/// Deserialize composed tool arguments into a REST request/query type.
fn parse_args<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|e| format!("invalid arguments: {e}"))
}

/// Fetch a required string argument.
fn arg_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing required argument: {key}"))
}

/// Fetch a required UUID argument.
fn arg_uuid(args: &Value, key: &str) -> Result<Uuid, String> {
    Uuid::parse_str(arg_str(args, key)?).map_err(|e| format!("invalid {key}: {e}"))
}

/// Successful `tools/call` envelope: pretty-printed JSON as text content.
fn tool_ok(value: &Value) -> Value {
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    json!({ "content": [{ "type": "text", "text": text }], "isError": false })
}

/// Domain-failure `tools/call` envelope (`isError: true`, NOT a JSON-RPC error).
fn tool_err(message: &str) -> Value {
    json!({ "content": [{ "type": "text", "text": message }], "isError": true })
}

/// JSON-RPC success response.
fn rpc_result(id: &Value, result: &Value) -> Response {
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response()
}

/// JSON-RPC error response. HTTP status stays 200 so MCP clients parse the
/// error object instead of bailing on the transport.
fn rpc_error(id: &Value, code: i64, message: impl Into<String>) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() },
    }))
    .into_response()
}

/// The static tool catalog served by `tools/list`.
// One declarative literal per tool keeps the catalog greppable; the length is
// schema data, not logic.
#[allow(clippy::too_many_lines)]
fn tool_catalog() -> Value {
    fn instance_id() -> Value {
        json!({ "type": "string", "format": "uuid", "description": "Instance UUID" })
    }
    json!([
        {
            "name": "list_sequences",
            "description": "List workflow sequence definitions visible to the caller's tenant.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "namespace": { "type": "string", "description": "Filter by namespace" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 1000,
                               "description": "Max rows (default 200)" },
                },
                "additionalProperties": false,
            },
        },
        {
            "name": "create_instance",
            "description": "Launch a durable workflow instance from a sequence. \
                            Provide either sequence_id or sequence_name.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sequence_id": { "type": "string", "format": "uuid",
                                     "description": "Target sequence UUID" },
                    "sequence_name": { "type": "string",
                                       "description": "Target sequence name (latest version)" },
                    "tenant_id": { "type": "string",
                                   "description": "Tenant (defaults to the caller's tenant)" },
                    "namespace": { "type": "string", "description": "Namespace (default: \"default\")" },
                    "context": { "type": "object",
                                 "description": "Initial execution context ({data, config, ...})" },
                    "metadata": { "type": "object", "description": "Opaque instance metadata" },
                    "budget": { "type": "object",
                                "description": "Instance budget enforced by the scheduler \
                                                (max_input_tokens, max_output_tokens, \
                                                max_total_tokens, max_steps)" },
                    "dry_run": { "type": "boolean",
                                 "description": "Simulate side-effecting steps (default false)" },
                    "idempotency_key": { "type": "string",
                                         "description": "Dedup key for at-most-once creation" },
                },
                "additionalProperties": false,
            },
        },
        {
            "name": "get_instance_status",
            "description": "Get an instance's state, timestamps, and paused_reason metadata if present.",
            "inputSchema": {
                "type": "object",
                "properties": { "instance_id": instance_id() },
                "required": ["instance_id"],
                "additionalProperties": false,
            },
        },
        {
            "name": "get_instance_outputs",
            "description": "Get the per-block outputs an instance has produced so far.",
            "inputSchema": {
                "type": "object",
                "properties": { "instance_id": instance_id() },
                "required": ["instance_id"],
                "additionalProperties": false,
            },
        },
        {
            "name": "send_signal",
            "description": "Send a signal to a running instance (pause/resume/cancel/update_context/custom:<name>).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "instance_id": instance_id(),
                    "signal": { "type": "string",
                                "description": "pause | resume | cancel | update_context | custom:<name>" },
                    "payload": { "description": "Optional signal payload (any JSON)" },
                },
                "required": ["instance_id", "signal"],
                "additionalProperties": false,
            },
        },
        {
            "name": "retry_instance",
            "description": "Retry a failed instance: reset it to scheduled with an immediate fire time.",
            "inputSchema": {
                "type": "object",
                "properties": { "instance_id": instance_id() },
                "required": ["instance_id"],
                "additionalProperties": false,
            },
        },
        {
            "name": "list_dlq",
            "description": "List failed instances (the dead-letter queue).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "minimum": 1, "maximum": 1000,
                               "description": "Max rows (default 100)" },
                },
                "additionalProperties": false,
            },
        },
        {
            "name": "get_usage",
            "description": "Aggregate LLM token usage and estimated cost for a tenant over a time window.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tenant": { "type": "string",
                                "description": "Tenant to report on (required unless tenant-scoped)" },
                    "start": { "type": "string", "format": "date-time",
                               "description": "Window start, RFC 3339 (default: end - 30d)" },
                    "end": { "type": "string", "format": "date-time",
                             "description": "Window end, RFC 3339 (default: now)" },
                },
                "additionalProperties": false,
            },
        },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_echoes_known_protocol_version() {
        let result = initialize_result(&json!({ "protocolVersion": "2025-03-26" }));
        assert_eq!(result["protocolVersion"], "2025-03-26");
        assert_eq!(result["serverInfo"]["name"], "orch8");
        assert!(result["capabilities"]["tools"].is_object());
    }

    #[test]
    fn initialize_falls_back_on_unknown_protocol_version() {
        let result = initialize_result(&json!({ "protocolVersion": "1999-01-01" }));
        assert_eq!(result["protocolVersion"], DEFAULT_PROTOCOL_VERSION);
    }

    #[test]
    fn initialize_falls_back_when_version_absent() {
        let result = initialize_result(&Value::Null);
        assert_eq!(result["protocolVersion"], DEFAULT_PROTOCOL_VERSION);
        assert_eq!(result["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn catalog_lists_eight_tools_with_object_schemas() {
        let catalog = tool_catalog();
        let tools = catalog.as_array().expect("catalog is an array");
        assert_eq!(tools.len(), 8);
        for tool in tools {
            assert!(tool["name"].is_string());
            assert!(tool["description"].is_string());
            assert_eq!(tool["inputSchema"]["type"], "object");
            assert!(tool["inputSchema"]["properties"].is_object());
        }
    }

    #[test]
    fn tool_envelopes_flag_errors() {
        let ok = tool_ok(&json!({ "id": 1 }));
        assert_eq!(ok["isError"], false);
        assert_eq!(ok["content"][0]["type"], "text");
        assert!(
            ok["content"][0]["text"]
                .as_str()
                .expect("text content")
                .contains("\"id\": 1")
        );

        let err = tool_err("not found: instance x");
        assert_eq!(err["isError"], true);
        assert_eq!(err["content"][0]["text"], "not found: instance x");
    }

    #[test]
    fn arg_helpers_report_missing_and_invalid() {
        let args = json!({ "instance_id": "not-a-uuid", "signal": "pause" });
        assert!(
            arg_uuid(&args, "instance_id")
                .expect_err("invalid uuid")
                .contains("invalid instance_id")
        );
        assert!(
            arg_str(&args, "missing")
                .expect_err("missing key")
                .contains("missing required argument")
        );
        assert_eq!(arg_str(&args, "signal").expect("present"), "pause");
    }
}
