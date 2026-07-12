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
use serde_json::{Value, json};
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

/// Whether outbound SSRF guards are intentionally disabled.
///
/// Requires an explicit truthy value (`1` / `true`, case-insensitive). A
/// bare, empty, `0`, or `false` value — e.g. a leftover `ORCH8_ALLOW_INTERNAL_URLS=`
/// in a Dockerfile, CI image, or shell profile — must NOT silently disable
/// the protection (the old `is_ok()` check was fail-open on any value).
pub(crate) fn internal_urls_allowed() -> bool {
    flag_is_truthy(std::env::var("ORCH8_ALLOW_INTERNAL_URLS").ok().as_deref())
}

/// Parse a boolean-ish env flag value. Only an explicit `1`/`true`
/// (case-insensitive, surrounding whitespace ignored) counts as enabled;
/// `None`, empty, `0`, and `false` are all disabled. Pure helper so the
/// fail-closed semantics are unit-testable without mutating process env.
fn flag_is_truthy(v: Option<&str>) -> bool {
    matches!(v.map(str::trim), Some(s) if s == "1" || s.eq_ignore_ascii_case("true"))
}

/// `true` if an IPv4 literal is a private/internal/metadata target that
/// outbound requests must never reach.
fn ipv4_is_blocked(v4: std::net::Ipv4Addr) -> bool {
    v4.is_loopback()          // 127.0.0.0/8
        || v4.is_private()        // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
        || v4.is_link_local()     // 169.254.0.0/16 (includes 169.254.169.254 metadata)
        || v4.is_unspecified()
        || v4.is_documentation()  // 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
        || v4.is_multicast() // 224.0.0.0/4
}

/// `true` if an IPv6 literal is a private/internal target.
fn ipv6_is_blocked(v6: std::net::Ipv6Addr) -> bool {
    v6.is_loopback()              // ::1
        || v6.is_unspecified()
        || v6.is_unicast_link_local() // fe80::/10
        || v6.is_multicast()          // ff00::/8
        || v6.is_unique_local() // fc00::/7
}

/// Check whether a resolved `host:port` address is safe to contact
/// (not a private/internal/metadata target).
///
/// Shared SSRF guard used by HTTP, gRPC, and any other outbound-request
/// handler. Returns `false` if any A/AAAA record for `addr` lands in a
/// loopback, private, link-local (incl. 169.254.169.254 cloud metadata),
/// unspecified, or IPv6 ULA (`fc00::/7`) range.
pub(crate) async fn is_address_safe(addr: &str) -> bool {
    // Allow internal addresses only when explicitly opted in.
    if internal_urls_allowed() {
        return true;
    }

    let Ok(addrs) = tokio::net::lookup_host(addr).await else {
        return false;
    };
    for socket_addr in addrs {
        match socket_addr.ip() {
            std::net::IpAddr::V4(v4) => {
                if ipv4_is_blocked(v4) {
                    return false;
                }
            }
            std::net::IpAddr::V6(v6) => {
                if ipv6_is_blocked(v6) {
                    return false;
                }
            }
        }
    }
    true
}

/// `true` if a resolved socket address lands in a blocked (private/internal/
/// metadata) range.
fn socket_addr_is_blocked(sa: &std::net::SocketAddr) -> bool {
    match sa.ip() {
        std::net::IpAddr::V4(v4) => ipv4_is_blocked(v4),
        std::net::IpAddr::V6(v6) => ipv6_is_blocked(v6),
    }
}

/// SSRF-hardening DNS resolver for outbound HTTP clients.
///
/// Resolves hostnames via the system resolver but drops any address in a
/// private/internal/metadata range **before the connection is made**. This
/// closes the DNS-rebinding TOCTOU that the pre-flight [`is_url_safe`] check
/// cannot: `is_url_safe` resolves at *check* time, but reqwest re-resolves at
/// *connect* time, so an attacker-controlled resolver that returns a public IP
/// for the check and a private IP for the connect would otherwise reach an
/// internal target. reqwest routes redirect connections through the same
/// resolver, so this also closes hostname-redirect-to-private-IP vectors that
/// the literal-only [`redirect_target_allowed`] guard misses.
///
/// When all resolved addresses are filtered out, reqwest receives an empty
/// address set and the connection fails — the intended block.
#[derive(Debug, Default, Clone)]
pub(crate) struct SsrfGuardResolver;

impl reqwest::dns::Resolve for SsrfGuardResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            // Honour the explicit opt-out: pass every address through unfiltered.
            let allow_internal = internal_urls_allowed();
            // Port 0 — reqwest overrides the port on the returned addresses.
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let filtered: Vec<std::net::SocketAddr> = addrs
                .filter(|sa| allow_internal || !socket_addr_is_blocked(sa))
                .collect();
            let iter: reqwest::dns::Addrs = Box::new(filtered.into_iter());
            Ok(iter)
        })
    }
}

/// Synchronous redirect-hop guard for outbound HTTP clients.
///
/// reqwest follows redirects by default (up to 10) without re-running the
/// async [`is_url_safe`] check, so an attacker-controlled public URL could
/// `302 -> http://169.254.169.254/...` and reach internal/metadata targets.
/// This guard runs inside the (synchronous) redirect policy and rejects any
/// hop to a non-http(s) scheme or to a private/loopback/link-local IP literal.
///
/// DNS resolution can't run in this sync context, so hostname redirect targets
/// that *resolve* to private IPs are not caught here — they remain bounded by
/// the hop cap and the initial [`is_url_safe`] check. The common metadata /
/// direct-internal-IP redirect vector is closed.
pub(crate) fn redirect_target_allowed(next: &url::Url) -> bool {
    if internal_urls_allowed() {
        return true;
    }
    match next.scheme() {
        "http" | "https" => {}
        _ => return false,
    }
    match next.host() {
        Some(url::Host::Ipv4(v4)) => !ipv4_is_blocked(v4),
        Some(url::Host::Ipv6(v6)) => !ipv6_is_blocked(v6),
        Some(url::Host::Domain(_)) | None => true,
    }
}

/// Whether a workflow-supplied HTTP header name may be set on an outbound
/// request. Rejects:
/// - `Host` (case-insensitive): overriding it can route a request to a
///   different backend than the URL implies behind a shared reverse proxy.
/// - names containing control characters (CR/LF, etc.): request-splitting /
///   header-injection vectors. (reqwest also rejects these at send time, but
///   filtering here keeps the behavior explicit and uniform across handlers.)
///
/// Header *values* are left to reqwest's `HeaderValue` parser, which rejects
/// control characters.
#[must_use]
pub(crate) fn outbound_header_name_allowed(name: &str) -> bool {
    if name.eq_ignore_ascii_case("host") {
        return false;
    }
    !name.bytes().any(|b| b.is_ascii_control())
}

/// Failure modes for [`read_body_capped`].
#[derive(Debug)]
pub(crate) enum BodyReadError {
    /// Body exceeded the cap (carries the cap, in bytes).
    TooLarge(usize),
    /// Transport error while streaming the body.
    Io(String),
}

/// Read a response body, aborting as soon as it exceeds `max_bytes`.
///
/// `reqwest::Response::bytes()` buffers the *entire* body into memory before
/// returning, so a response with no `Content-Length` (chunked) can OOM the
/// worker before any post-read size check fires. This streams chunk-by-chunk
/// and bails the moment the running total would exceed the cap, bounding peak
/// memory to `max_bytes + one chunk`.
pub(crate) async fn read_body_capped(
    mut resp: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, BodyReadError> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| BodyReadError::Io(e.to_string()))?
    {
        if buf.len() + chunk.len() > max_bytes {
            return Err(BodyReadError::TooLarge(max_bytes));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
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

/// Test-only: mark a URL as safe by seeding the SSRF cache, so handler unit
/// tests can point at a loopback mock without mutating `ORCH8_ALLOW_INTERNAL_URLS`
/// (env mutation is a data race across the parallel test threads).
#[cfg(test)]
pub(crate) async fn mark_url_safe_for_test(url: &str) {
    url_safety_cache().insert(url.to_string(), true).await;
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
    registry.register("mcp_call", super::mcp::handle_mcp_call);
    registry.register("agent", super::agent::handle_agent);
    registry.register("embed", super::memory::handle_embed);
    registry.register("memory_store", super::memory::handle_memory_store);
    registry.register("memory_search", super::memory::handle_memory_search);
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
    registry.register("blob_put", super::blob::handle_blob_put);
    registry.register(
        "wait_for_event",
        super::wait_for_event::handle_wait_for_event,
    );
    registry.register("blob_get", super::blob::handle_blob_get);
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
        if let Ok(tree) = ctx.storage.get_execution_tree(ctx.instance_id).await
            && let Some(n) = tree.iter().find(|n| {
                n.block_id == ctx.block_id && n.state == orch8_types::execution::NodeState::Running
            })
        {
            let _ = ctx
                .storage
                .update_node_state(n.id, orch8_types::execution::NodeState::Pending)
                .await;
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
#[allow(clippy::too_many_lines)]
async fn handle_http_request(ctx: StepContext) -> Result<Value, StepError> {
    const MAX_RESPONSE_BYTES: usize = 10_485_760; // 10 MB
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
                if outbound_header_name_allowed(k) {
                    req = req.header(k.as_str(), val);
                } else {
                    debug!(header = %k, "http_request: refusing forbidden header name");
                }
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

    // Stream the body with a hard cap so a chunked (no Content-Length)
    // response can't OOM the worker before a post-read size check.
    let bytes = read_body_capped(resp, MAX_RESPONSE_BYTES)
        .await
        .map_err(|e| match e {
            BodyReadError::TooLarge(max) => StepError::Permanent {
                message: format!("response too large: exceeds {max} byte limit"),
                details: None,
            },
            BodyReadError::Io(m) => StepError::Retryable {
                message: format!("HTTP request failed reading body from {url}: {m}"),
                details: None,
            },
        })?;
    let response_body = String::from_utf8_lossy(&bytes).into_owned();

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
    let key = ctx
        .params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StepError::Permanent {
            message: "set_state: `key` (string) is required".into(),
            details: None,
        })?;
    let value = ctx
        .params
        .get("value")
        .ok_or_else(|| StepError::Permanent {
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
    let key = ctx
        .params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StepError::Permanent {
            message: "get_state: `key` (string) is required".into(),
            details: None,
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
    let key = ctx
        .params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StepError::Permanent {
            message: "delete_state: `key` (string) is required".into(),
            details: None,
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

    let result =
        crate::expression::evaluate_condition(condition, &ctx.context, &serde_json::json!({}));

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

    #[test]
    fn flag_is_truthy_is_fail_closed() {
        // Only an explicit 1/true enables; everything else is disabled — a
        // leftover empty/0/false var must NOT open the SSRF guard.
        assert!(flag_is_truthy(Some("1")));
        assert!(flag_is_truthy(Some("true")));
        assert!(flag_is_truthy(Some("TRUE")));
        assert!(flag_is_truthy(Some(" 1 ")));
        assert!(!flag_is_truthy(None));
        assert!(!flag_is_truthy(Some("")));
        assert!(!flag_is_truthy(Some("0")));
        assert!(!flag_is_truthy(Some("false")));
        assert!(!flag_is_truthy(Some("yes")));
    }

    #[test]
    fn redirect_target_allowed_blocks_internal_and_bad_schemes() {
        let allow = |u: &str| redirect_target_allowed(&url::Url::parse(u).unwrap());
        // Public hosts (domain or public IP) are allowed.
        assert!(allow("https://api.example.com/x"));
        assert!(allow("http://93.184.216.34/x")); // example.com public IP
        // Internal / metadata / loopback IP literals are blocked.
        assert!(!allow("http://169.254.169.254/latest/meta-data/")); // cloud metadata
        assert!(!allow("http://127.0.0.1/")); // loopback
        assert!(!allow("http://10.0.0.5/")); // private
        assert!(!allow("http://192.168.1.1/")); // private
        assert!(!allow("http://[::1]/")); // ipv6 loopback
        assert!(!allow("http://[fc00::1]/")); // ipv6 ULA
        // Non-http(s) schemes are blocked (file://, gopher://, etc.).
        assert!(!allow("file:///etc/passwd"));
        assert!(!allow("gopher://10.0.0.1/"));
    }

    #[tokio::test]
    async fn read_body_capped_rejects_oversized_body() {
        // A body over the cap is rejected without returning the bytes — the
        // streaming guard that prevents OOM on a Content-Length-less response.
        let resp = reqwest::Response::from(http::Response::new(vec![0u8; 100]));
        let err = read_body_capped(resp, 50).await;
        assert!(matches!(err, Err(BodyReadError::TooLarge(50))));
    }

    #[tokio::test]
    async fn read_body_capped_returns_body_under_cap() {
        let resp = reqwest::Response::from(http::Response::new(b"hello".to_vec()));
        let body = read_body_capped(resp, 1024).await.expect("under cap");
        assert_eq!(body, b"hello");
    }

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
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("test"),
            params,
            context: Arc::new(ExecutionContext::default()),
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
        let result = handle_set_state(test_ctx(json!({"key": "counter", "value": 42})).await)
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
        let result = handle_get_state(test_ctx(json!({"key": "nonexistent"})).await)
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
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("s1"),
            params: json!({"key": "color", "value": "blue"}),
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        handle_set_state(set_ctx).await.unwrap();

        let get_ctx = StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("s2"),
            params: json!({"key": "color"}),
            context: Arc::new(ExecutionContext::default()),
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
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("s1"),
            params: json!({"key": "temp", "value": true}),
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        handle_set_state(set_ctx).await.unwrap();

        let del_ctx = StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("s2"),
            params: json!({"key": "temp"}),
            context: Arc::new(ExecutionContext::default()),
            attempt: 0,
            storage: Arc::clone(&storage),
            wait_for_input: None,
        };
        let result = handle_delete_state(del_ctx).await.unwrap();
        assert_eq!(result["deleted"], true);

        let get_ctx = StepContext {
            instance_id,
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("s3"),
            params: json!({"key": "temp"}),
            context: Arc::new(ExecutionContext::default()),
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
        let result = handle_transform(test_ctx(json!({})).await).await.unwrap();
        assert_eq!(result, json!({}));
    }

    #[tokio::test]
    async fn transform_null_params() {
        let result = handle_transform(test_ctx(Value::Null).await).await.unwrap();
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
            tenant_id: TenantId::unchecked("t"),
            block_id: BlockId::new("ms"),
            params: json!({"values": {"color": "red", "count": 42, "active": true}}),
            context: Arc::new(ExecutionContext::default()),
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
