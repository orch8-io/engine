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
//!
//! ## Binary I/O (generic — composes into image gen, asset fetch, media upload)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `response_as` | string | `"artifact"` → store the (2xx) response body as a durable artifact; output is `{ tool_name, artifact: <ref>, status }`. |
//! | `body_artifact` | object | An `ArtifactRef` (or `{ "artifact": <ref> }`) whose bytes become the request body. |
//! | `upload` | object | `{ "mode": "multipart", "field": "file", "filename": "x.png" }` → multipart/form-data; omit for a raw body. |

use std::time::Duration;

use serde_json::{json, Value};
use tracing::debug;

use orch8_types::error::StepError;

use super::StepContext;

#[allow(clippy::too_many_lines)]
pub async fn handle_tool_call(ctx: StepContext) -> Result<Value, StepError> {
    const MAX_RESPONSE_BYTES: usize = 10_485_760; // 10 MB
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

    let arguments = ctx
        .params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

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

    // Dry-run: URL presence + SSRF safety + method were already validated
    // above; skip only the HTTP call. Canonical envelope mirrors the success
    // shape (`tool_name`/`result`) so downstream templates still resolve.
    if ctx.is_dry_run() {
        return Ok(super::util::dry_run_stub(
            "tool_call",
            json!({ "url": url, "method": method }),
            json!({ "tool_name": tool_name, "result": Value::Null }),
        ));
    }

    let upload = parse_body_artifact(&ctx.params);
    let want_response_artifact = wants_response_artifact(&ctx.params);

    debug!(url = %url, tool_name = %tool_name, upload = upload.is_some(), "tool_call: dispatching");

    let client = super::llm::http_client();

    let mut req = if method.eq_ignore_ascii_case("GET") {
        client.get(url)
    } else if method.eq_ignore_ascii_case("PUT") {
        client.put(url)
    } else if method.eq_ignore_ascii_case("PATCH") {
        client.patch(url)
    } else {
        client.post(url)
    };
    req = req.timeout(timeout);

    // Apply custom headers first so the body step can override Content-Type.
    if let Some(headers) = ctx.params.get("headers").and_then(Value::as_object) {
        for (k, v) in headers {
            if let Some(val) = v.as_str() {
                req = req.header(k.as_str(), val);
            }
        }
    }

    // Body: either upload an artifact's bytes (raw or multipart) or send the
    // default JSON envelope.
    if let Some(u) = &upload {
        let bytes = ctx
            .storage
            .get_artifact(&u.key)
            .await
            .map_err(|e| artifact_step_err("tool_call artifact fetch error", &e))?
            .ok_or_else(|| StepError::Permanent {
                message: format!("body_artifact not found: {}", u.key),
                details: None,
            })?;
        req = if let Some((field, filename)) = &u.multipart {
            let part = reqwest::multipart::Part::bytes(bytes)
                .file_name(filename.clone())
                .mime_str(&u.content_type)
                .map_err(|e| StepError::Permanent {
                    message: format!("invalid artifact content_type: {e}"),
                    details: None,
                })?;
            req.multipart(reqwest::multipart::Form::new().part(field.clone(), part))
        } else {
            req.header("Content-Type", u.content_type.clone())
                .body(bytes)
        };
    } else {
        let body = json!({
            "tool_name": tool_name,
            "arguments": arguments,
            "context": {
                "instance_id": ctx.instance_id.into_uuid().to_string(),
                "block_id": ctx.block_id.as_str(),
                "attempt": ctx.attempt,
            }
        });
        req = req.header("Content-Type", "application/json").json(&body);
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
    // Capture the response content type before the body is consumed — needed
    // when storing the response as an artifact.
    let resp_content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let content_length = resp.content_length().unwrap_or(0);
    if content_length > MAX_RESPONSE_BYTES as u64 {
        return Err(StepError::Permanent {
            message: format!("tool response too large: {content_length} bytes exceeds 10MB limit"),
            details: None,
        });
    }
    let body_bytes = resp.bytes().await.map_err(|e| StepError::Retryable {
        message: format!("tool_call body read error: {e}"),
        details: None,
    })?;
    if body_bytes.len() > MAX_RESPONSE_BYTES {
        return Err(StepError::Permanent {
            message: format!(
                "tool response too large: {} bytes exceeds 10MB limit",
                body_bytes.len()
            ),
            details: None,
        });
    }

    // Store a successful binary response as a durable artifact and return its
    // ref. Error statuses fall through to the normal error mapping.
    if want_response_artifact && (200..400).contains(&status) {
        let aref = ctx
            .storage
            .put_artifact(ctx.instance_id, &resp_content_type, body_bytes.clone())
            .await
            .map_err(|e| artifact_step_err("tool_call store artifact error", &e))?;
        return Ok(json!({ "tool_name": tool_name, "artifact": aref, "status": status }));
    }

    interpret_tool_response(tool_name, status, &body_bytes)
}

/// Map an artifact-backend storage error to a step error: a permanent
/// `Unsupported` (backend not configured / unsupported) must NOT be retried —
/// otherwise a misconfigured deployment burns its whole retry budget. Transient
/// `Backend` failures stay retryable.
fn artifact_step_err(context: &str, e: &orch8_types::error::StorageError) -> StepError {
    match e {
        orch8_types::error::StorageError::Unsupported(_) => StepError::Permanent {
            message: format!("{context}: {e}"),
            details: None,
        },
        _ => StepError::Retryable {
            message: format!("{context}: {e}"),
            details: None,
        },
    }
}

/// True when the caller asked for the response body to be stored as an artifact.
fn wants_response_artifact(params: &Value) -> bool {
    params.get("response_as").and_then(Value::as_str) == Some("artifact")
}

/// An artifact to upload as the request body.
struct UploadBody {
    key: String,
    content_type: String,
    /// `Some((field, filename))` → multipart/form-data; `None` → raw body.
    multipart: Option<(String, String)>,
}

/// Parse the `body_artifact` param (a raw [`orch8_types::artifact::ArtifactRef`]
/// or `{ "artifact": <ref> }`) plus the optional `upload` mode descriptor.
fn parse_body_artifact(params: &Value) -> Option<UploadBody> {
    let ba = params.get("body_artifact")?;
    // Accept the ref directly or nested under "artifact".
    let refobj = ba.get("artifact").unwrap_or(ba);
    let key = refobj.get("key").and_then(Value::as_str)?.to_string();
    let content_type = refobj
        .get("content_type")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream")
        .to_string();
    let multipart = match params.get("upload") {
        Some(u) if u.get("mode").and_then(Value::as_str) == Some("multipart") => {
            let field = u
                .get("field")
                .and_then(Value::as_str)
                .unwrap_or("file")
                .to_string();
            let filename = u
                .get("filename")
                .and_then(Value::as_str)
                .unwrap_or("file")
                .to_string();
            Some((field, filename))
        }
        _ => None,
    };
    Some(UploadBody {
        key,
        content_type,
        multipart,
    })
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
            tenant_id: TenantId::unchecked("T"),
            block_id: BlockId::new("t"),
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

    #[test]
    fn wants_response_artifact_flag() {
        assert!(wants_response_artifact(
            &json!({ "response_as": "artifact" })
        ));
        assert!(!wants_response_artifact(&json!({ "response_as": "json" })));
        assert!(!wants_response_artifact(&json!({})));
    }

    #[test]
    fn parse_body_artifact_raw_ref() {
        let u = parse_body_artifact(&json!({
            "body_artifact": { "key": "i1/a1", "content_type": "image/png" }
        }))
        .unwrap();
        assert_eq!(u.key, "i1/a1");
        assert_eq!(u.content_type, "image/png");
        assert!(u.multipart.is_none());
    }

    #[test]
    fn parse_body_artifact_nested_under_artifact() {
        let u = parse_body_artifact(&json!({
            "body_artifact": { "artifact": { "key": "i1/a2", "content_type": "text/html" } }
        }))
        .unwrap();
        assert_eq!(u.key, "i1/a2");
        assert_eq!(u.content_type, "text/html");
    }

    #[test]
    fn parse_body_artifact_multipart_mode() {
        let u = parse_body_artifact(&json!({
            "body_artifact": { "key": "i1/a1", "content_type": "image/png" },
            "upload": { "mode": "multipart", "field": "media", "filename": "pic.png" }
        }))
        .unwrap();
        assert_eq!(u.multipart, Some(("media".into(), "pic.png".into())));
    }

    #[test]
    fn parse_body_artifact_multipart_defaults() {
        let u = parse_body_artifact(&json!({
            "body_artifact": { "key": "k" },
            "upload": { "mode": "multipart" }
        }))
        .unwrap();
        assert_eq!(u.content_type, "application/octet-stream");
        assert_eq!(u.multipart, Some(("file".into(), "file".into())));
    }

    #[test]
    fn parse_body_artifact_absent_or_invalid() {
        assert!(parse_body_artifact(&json!({})).is_none());
        // body_artifact present but no key → None.
        assert!(
            parse_body_artifact(&json!({ "body_artifact": { "content_type": "x" } })).is_none()
        );
    }
}

/// Tests for the binary I/O paths (response→artifact, artifact→body upload)
/// against an in-process HTTP mock and an in-memory artifact backend.
#[cfg(test)]
mod net_tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    /// Read one HTTP request, returning the raw body bytes.
    async fn read_body(sock: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            let n = sock.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]).to_lowercase();
                let want = head
                    .split("content-length:")
                    .nth(1)
                    .and_then(|s| s.trim().split([' ', '\r', '\n']).next())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let start = pos + 4;
                if buf.len() >= start + want {
                    return buf[start..start + want].to_vec();
                }
            }
        }
        Vec::new()
    }

    /// Mock that records each request body and replies with `(status, content_type, body)`.
    async fn spawn_recording_mock(
        count: usize,
        status: u16,
        content_type: &'static str,
        reply: &'static [u8],
        received: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..count {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let body = read_body(&mut sock).await;
                received.lock().await.push(body);
                let out = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                );
                let _ = sock.write_all(out.as_bytes()).await;
                let _ = sock.write_all(reply).await;
                let _ = sock.flush().await;
            }
        });
        format!("http://127.0.0.1:{}", addr.port())
    }

    async fn mk_ctx(params: Value) -> (StepContext, Arc<dyn StorageBackend>) {
        if let Some(u) = params.get("url").and_then(Value::as_str) {
            super::super::builtin::mark_url_safe_for_test(u).await;
        }
        let storage: Arc<dyn StorageBackend> = Arc::new(
            SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        );
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("b"),
            params,
            context: ExecutionContext::default(),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        (ctx, storage)
    }

    #[tokio::test]
    async fn dry_run_skips_http_call() {
        // Points at a closed port: if the handler made the request it would
        // error (connect refused). Returning Ok proves no call was made.
        let (mut ctx, _s) = mk_ctx(json!({
            "url": "http://127.0.0.1:1/tool",
            "method": "POST",
            "tool_name": "search",
            "arguments": { "q": "rust" }
        }))
        .await;
        ctx.context.runtime.dry_run = true;
        let out = handle_tool_call(ctx).await.unwrap();
        assert_eq!(out["dry_run"], true);
        assert_eq!(out["handler"], "tool_call");
        assert_eq!(out["tool_name"], "search");
        assert!(out["result"].is_null());
        assert_eq!(out["would"]["method"], "POST");
    }

    #[tokio::test]
    async fn response_as_artifact_stores_bytes() {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let url =
            spawn_recording_mock(1, 200, "image/png", b"\x89PNGDATA", Arc::clone(&received)).await;
        let (ctx, storage) = mk_ctx(json!({
            "url": url, "method": "GET", "response_as": "artifact"
        }))
        .await;
        let out = handle_tool_call(ctx).await.unwrap();
        assert_eq!(out["status"], 200);
        assert_eq!(out["artifact"]["content_type"], "image/png");
        assert_eq!(out["artifact"]["size"], 8);
        // The stored bytes are retrievable.
        let key = out["artifact"]["key"].as_str().unwrap();
        let bytes = storage.get_artifact(key).await.unwrap().unwrap();
        assert_eq!(bytes, b"\x89PNGDATA");
    }

    #[tokio::test]
    async fn body_artifact_uploads_raw_bytes() {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let url =
            spawn_recording_mock(1, 200, "application/json", b"{}", Arc::clone(&received)).await;
        let (ctx, storage) = mk_ctx(json!({ "url": url, "method": "PUT" })).await;
        // Seed an artifact to upload.
        let aref = storage
            .put_artifact(
                ctx.instance_id,
                "image/png",
                bytes::Bytes::from_static(b"RAWIMAGE"),
            )
            .await
            .unwrap();
        let mut params = ctx.params.clone();
        params["body_artifact"] = serde_json::to_value(&aref).unwrap();
        let ctx2 = StepContext { params, ..ctx };
        handle_tool_call(ctx2).await.unwrap();
        let bodies = received.lock().await;
        assert_eq!(
            bodies[0], b"RAWIMAGE",
            "raw artifact bytes should be the request body"
        );
    }

    #[tokio::test]
    async fn body_artifact_multipart_upload() {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let url =
            spawn_recording_mock(1, 200, "application/json", b"{}", Arc::clone(&received)).await;
        let (ctx, storage) = mk_ctx(json!({
            "url": url,
            "upload": { "mode": "multipart", "field": "media", "filename": "x.png" }
        }))
        .await;
        let aref = storage
            .put_artifact(
                ctx.instance_id,
                "image/png",
                bytes::Bytes::from_static(b"MPDATA"),
            )
            .await
            .unwrap();
        let mut params = ctx.params.clone();
        params["body_artifact"] = serde_json::to_value(&aref).unwrap();
        let ctx2 = StepContext { params, ..ctx };
        handle_tool_call(ctx2).await.unwrap();
        let bodies = received.lock().await;
        // multipart body contains the field name, filename, and the bytes.
        let body = String::from_utf8_lossy(&bodies[0]);
        assert!(body.contains("name=\"media\""));
        assert!(body.contains("x.png"));
        assert!(body.contains("MPDATA"));
    }

    #[tokio::test]
    async fn body_artifact_missing_is_permanent() {
        let (ctx, _s) = mk_ctx(json!({
            "url": "https://example.invalid/tool",
            "body_artifact": { "key": "nope/missing", "content_type": "image/png" }
        }))
        .await;
        // example.invalid resolves? mark it safe so we reach the artifact fetch.
        super::super::builtin::mark_url_safe_for_test("https://example.invalid/tool").await;
        let err = handle_tool_call(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn normal_json_response_via_handler() {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let url = spawn_recording_mock(
            1,
            200,
            "application/json",
            br#"{"ok":true}"#,
            Arc::clone(&received),
        )
        .await;
        let (ctx, _s) = mk_ctx(json!({
            "url": url, "tool_name": "search", "arguments": { "q": "rust" }
        }))
        .await;
        let out = handle_tool_call(ctx).await.unwrap();
        assert_eq!(out["tool_name"], "search");
        assert_eq!(out["result"], json!({ "ok": true }));
        // The default JSON envelope was sent.
        let bodies = received.lock().await;
        let sent: Value = serde_json::from_slice(&bodies[0]).unwrap();
        assert_eq!(sent["arguments"]["q"], "rust");
    }

    #[tokio::test]
    async fn response_as_artifact_error_status_does_not_store() {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let url = spawn_recording_mock(1, 500, "image/png", b"boom", Arc::clone(&received)).await;
        let (ctx, _s) = mk_ctx(json!({
            "url": url, "method": "GET", "response_as": "artifact"
        }))
        .await;
        // 5xx must surface as a Retryable error, not a stored artifact.
        let err = handle_tool_call(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Retryable { .. }));
    }

    #[tokio::test]
    async fn patch_method_with_custom_headers() {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let url =
            spawn_recording_mock(1, 200, "application/json", b"{}", Arc::clone(&received)).await;
        let (ctx, _s) = mk_ctx(json!({
            "url": url,
            "method": "PATCH",
            "headers": { "X-Test": "1", "X-Other": "2" }
        }))
        .await;
        // Exercises the PATCH method arm and the custom-headers loop.
        handle_tool_call(ctx).await.unwrap();
    }

    #[tokio::test]
    async fn multipart_invalid_content_type_is_permanent() {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let url =
            spawn_recording_mock(1, 200, "application/json", b"{}", Arc::clone(&received)).await;
        let (ctx, storage) = mk_ctx(json!({
            "url": url,
            "upload": { "mode": "multipart", "field": "f", "filename": "x" }
        }))
        .await;
        // A content_type that is not a valid MIME makes multipart construction fail.
        let aref = storage
            .put_artifact(
                ctx.instance_id,
                "not-a-valid-mime",
                bytes::Bytes::from_static(b"DATA"),
            )
            .await
            .unwrap();
        let mut params = ctx.params.clone();
        params["body_artifact"] = serde_json::to_value(&aref).unwrap();
        let ctx2 = StepContext { params, ..ctx };
        let err = handle_tool_call(ctx2).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }
}
