//! Shared plumbing for unauthenticated, token-addressed public endpoints
//! (approval magic links, Slack interactions, public progress pages):
//! per-peer rate limiting, HTML escaping, and hardened HTML responses with a
//! hash-based Content-Security-Policy (no `unsafe-inline`).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use moka::sync::Cache;
use sha2::{Digest, Sha256};

use crate::error::ApiError;

static COUNTERS: LazyLock<Cache<String, Arc<AtomicU64>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(100_000)
        .time_to_live(Duration::from_secs(1))
        .build()
});

/// Fixed one-second window limiter keyed by `(bucket, peer ip)`.
pub(crate) fn check_rate(
    bucket: &str,
    peer: Option<SocketAddr>,
    per_second: u64,
) -> Result<(), ApiError> {
    let ip = peer.map_or_else(|| "unknown".to_owned(), |p| p.ip().to_string());
    let n = COUNTERS
        .entry(format!("{bucket}:{ip}"))
        .or_insert_with(|| Arc::new(AtomicU64::new(0)))
        .into_value()
        .fetch_add(1, Ordering::Relaxed);
    if n >= per_second {
        return Err(ApiError::RateLimited(format!(
            "{bucket} request rate exceeded"
        )));
    }
    Ok(())
}

#[must_use]
pub(crate) fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// `'sha256-…'` CSP source for an inline `<style>`/`<script>` body.
#[must_use]
pub(crate) fn csp_hash(body: &str) -> String {
    let digest = Sha256::digest(body.as_bytes());
    format!(
        "'sha256-{}'",
        base64::engine::general_purpose::STANDARD.encode(digest)
    )
}

pub(crate) const PAGE_CSS: &str = "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#f7f7f8;color:#111827;margin:0;padding:24px}main{max-width:520px;margin:40px auto;background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:24px}h1{font-size:20px;margin:0 0 12px}p{line-height:1.5}button{font-size:16px;padding:10px 18px;border-radius:6px;border:0;background:#1f2937;color:#fff;cursor:pointer}textarea{width:100%;box-sizing:border-box;margin:8px 0 16px;min-height:72px}.muted{color:#6b7280;font-size:13px}";

/// Common hardening for public pages and JSON: no caching, no referrer
/// (URLs carry capability tokens), no indexing, no framing unless allowed.
fn harden(resp: &mut Response, csp: &str) {
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(csp) {
        h.insert(header::CONTENT_SECURITY_POLICY, v);
    }
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        axum::http::HeaderName::from_static("x-robots-tag"),
        HeaderValue::from_static("noindex, nofollow"),
    );
}

/// Render a small standalone HTML page. `body_html` must already be escaped.
pub(crate) fn html_page(status: StatusCode, title: &str, body_html: &str) -> Response {
    let html = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"referrer\" content=\"no-referrer\"><title>{}</title><style>{PAGE_CSS}</style></head><body><main>{body_html}</main></body></html>",
        html_escape(title)
    );
    let csp = format!(
        "default-src 'none'; style-src {}; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
        csp_hash(PAGE_CSS)
    );
    let mut resp = (
        status,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response();
    harden(&mut resp, &csp);
    resp
}

/// JSON response with the public-endpoint hardening headers.
pub(crate) fn json_response(status: StatusCode, value: &serde_json::Value) -> Response {
    let mut resp = (status, axum::Json(value.clone())).into_response();
    harden(&mut resp, "default-src 'none'; frame-ancestors 'none'");
    resp
}

/// Apply hardening with a caller-provided CSP (embed pages / scripts).
pub(crate) fn with_csp(mut resp: Response, csp: &str) -> Response {
    harden(&mut resp, csp);
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_and_hashes() {
        assert_eq!(
            html_escape("<script>\"'&"),
            "&lt;script&gt;&quot;&#39;&amp;"
        );
        assert!(csp_hash("a").starts_with("'sha256-"));
    }

    #[test]
    fn page_has_strict_headers() {
        let r = html_page(StatusCode::OK, "t<", "<p>x</p>");
        let csp = r.headers()[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap();
        assert!(csp.contains("default-src 'none'"));
        assert!(!csp.contains("unsafe-inline"));
        assert_eq!(r.headers()[header::REFERRER_POLICY], "no-referrer");
        assert_eq!(r.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[test]
    fn limiter_trips() {
        for _ in 0..3 {
            assert!(check_rate("unit-bucket", None, 3).is_ok());
        }
        assert!(check_rate("unit-bucket", None, 3).is_err());
    }
}
