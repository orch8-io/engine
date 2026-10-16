//! One place to build outbound HTTP clients.
//!
//! Every engine component that talks to a URL it did not hard-code (workflow
//! handlers, OAuth refresh endpoints, push-dispatch targets, gRPC plugins,
//! webhooks) builds its `reqwest::Client` here so the SSRF hardening cannot
//! drift between call sites:
//!
//! - [`Profile::Untrusted`]: tenant/workflow-supplied URLs. DNS goes through
//!   [`SsrfGuardResolver`] (blocks rebinding to private IPs at connect time),
//!   every redirect hop is re-checked, and proxies are disabled — a configured
//!   `HTTP(S)_PROXY` would resolve the host itself and bypass the resolver.
//! - [`Profile::TokenEndpoint`]: like `Untrusted` but never follows redirects.
//!   A 307/308 re-POSTs the body (e.g. a `refresh_token`) to wherever the
//!   endpoint points, so token calls must not follow any hop.
//! - [`Profile::Operator`]: operator-configured targets that may legitimately
//!   be internal (webhooks). Redirects are disabled so signed request bodies
//!   cannot be forwarded to a different destination.
//! - [`Profile::LocalSidecar`]: operator-configured local services. Proxies
//!   and redirects are disabled so credential-bearing POSTs stay at the
//!   configured endpoint.
//!
//! Building fails closed: a builder error panics instead of silently falling
//! back to `reqwest::Client::new()`, which would drop every guard above.

use std::time::Duration;

use crate::handlers::builtin::{SsrfGuardResolver, redirect_target_allowed};

pub use crate::handlers::builtin::{BodyReadError, read_body_capped};

/// Maximum redirect hops followed by the checked redirect policy.
const MAX_REDIRECTS: usize = 10;

/// Hardening profile for an outbound client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Tenant/workflow-supplied URL: SSRF resolver, checked redirects, no proxy.
    Untrusted,
    /// Credential-bearing token endpoint: SSRF resolver, no redirects, no proxy.
    TokenEndpoint,
    /// Operator-configured URL: internal destinations allowed, redirects denied.
    Operator,
    /// Operator-configured local service that receives credentials: no proxy
    /// and no redirects, since a 307/308 would forward the POST body.
    LocalSidecar,
}

/// Tenant-configured trigger endpoints are public-only by default. Operators
/// running SQS/PubSub emulators can explicitly opt in to internal endpoints.
#[must_use]
pub fn trigger_endpoint_profile() -> Profile {
    if std::env::var("ORCH8_ALLOW_INTERNAL_TRIGGER_ENDPOINTS").is_ok_and(|v| v == "true") {
        Profile::LocalSidecar
    } else {
        Profile::TokenEndpoint
    }
}

/// Start a `ClientBuilder` with the hardening for `profile` applied. Callers
/// add their own pool/timeout/HTTP-version knobs, then call [`build`].
pub fn builder(profile: Profile) -> reqwest::ClientBuilder {
    let b = reqwest::Client::builder();
    match profile {
        Profile::Untrusted => b
            .dns_resolver(std::sync::Arc::new(SsrfGuardResolver))
            .no_proxy()
            .redirect(checked_redirects()),
        Profile::TokenEndpoint => b
            .dns_resolver(std::sync::Arc::new(SsrfGuardResolver))
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none()),
        Profile::Operator => b.redirect(reqwest::redirect::Policy::none()),
        Profile::LocalSidecar => b.no_proxy().redirect(reqwest::redirect::Policy::none()),
    }
}

/// Finish a hardened builder. Panics (fail closed) rather than degrading to an
/// unguarded default client.
#[allow(clippy::expect_used)]
pub fn build(builder: reqwest::ClientBuilder) -> reqwest::Client {
    builder
        .build()
        .expect("failed to build a hardened outbound HTTP client")
}

/// Redirect policy that re-validates every hop against the SSRF classifier.
fn checked_redirects() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            return attempt.error("too many redirects");
        }
        if redirect_target_allowed(attempt.url()) {
            attempt.follow()
        } else {
            attempt.error("blocked: redirect targets a private/internal network address")
        }
    })
}

/// Strip userinfo, query string, and fragment from a URL before it lands in
/// an error message, log line, or step output. Query strings routinely carry
/// API keys (`?key=…`, presigned signatures) and userinfo carries passwords.
pub fn redact_url(url: &str) -> String {
    if let Ok(u) = url::Url::parse(url) {
        return redact_parsed(&u);
    }
    // Unparseable: drop everything after the first `?`/`#` and any
    // `user:pass@` prefix rather than echoing it verbatim.
    let cut = url.find(['?', '#']).map_or(url, |i| &url[..i]);
    match (cut.find("://"), cut.rfind('@')) {
        (Some(s), Some(at)) if at > s => format!("{}{}", &cut[..s + 3], &cut[at + 1..]),
        _ => cut.to_string(),
    }
}

fn redact_parsed(u: &url::Url) -> String {
    let mut u = u.clone();
    let _ = u.set_username("");
    let _ = u.set_password(None);
    u.set_query(None);
    u.set_fragment(None);
    u.to_string()
}

/// Render a `reqwest::Error` without leaking the request URL's query string
/// or userinfo (reqwest's `Display` embeds the full URL).
pub fn redact_error(e: &reqwest::Error) -> String {
    let msg = e.to_string();
    match e.url() {
        Some(u) => msg.replace(u.as_str(), &redact_parsed(u)),
        None => msg,
    }
}

/// Clamp a caller-supplied timeout to `1..=300_000` ms so a workflow can't
/// pin a connection (and a worker slot) indefinitely or pass `0`.
pub fn clamp_timeout_ms(ms: u64) -> Duration {
    Duration::from_millis(ms.clamp(1, MAX_TIMEOUT_MS))
}

/// Upper bound for [`clamp_timeout_ms`] (5 minutes).
pub const MAX_TIMEOUT_MS: u64 = 300_000;

/// Truncate a response body for inclusion in an error message.
pub fn truncate_for_error(body: &[u8], max: usize) -> String {
    let s = String::from_utf8_lossy(&body[..body.len().min(max)]).into_owned();
    if body.len() > max {
        format!("{s}…[truncated]")
    } else {
        s
    }
}

/// Whether a non-2xx HTTP status is worth retrying: request timeout (408),
/// too early (425), rate limited (429) and every 5xx. Any other 4xx is a
/// request error that retrying cannot fix.
#[must_use]
pub const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 500..=599)
}

/// Longest `Retry-After` honoured; a larger value is clamped so a provider
/// cannot park a step for days.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(3600);

/// Key under which a handler's retryable-error `details` carry the
/// provider's `Retry-After`, in milliseconds. The scheduler uses it as the
/// minimum retry back-off.
pub const RETRY_AFTER_MS_KEY: &str = "retry_after_ms";

/// Parse a `Retry-After` header (delta-seconds or HTTP-date), clamped to
/// [`MAX_RETRY_AFTER`].
#[must_use]
pub fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?.trim();
    let wait = if let Ok(secs) = raw.parse::<u64>() {
        Duration::from_secs(secs)
    } else {
        let at = chrono::DateTime::parse_from_rfc2822(raw).ok()?;
        (at.with_timezone(&chrono::Utc) - chrono::Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO)
    };
    Some(wait.min(MAX_RETRY_AFTER))
}

/// The error for a non-2xx response: [`StepError::Retryable`] for
/// [`is_retryable_status`] (with the `Retry-After` hint folded into the
/// details under [`RETRY_AFTER_MS_KEY`]), otherwise permanent. `details` is
/// extended when it is an object.
#[must_use]
pub fn status_error(
    message: String,
    status: u16,
    headers: &reqwest::header::HeaderMap,
    details: Option<serde_json::Value>,
) -> orch8_types::error::StepError {
    use orch8_types::error::StepError;
    if !is_retryable_status(status) {
        return StepError::Permanent { message, details };
    }
    let details = match retry_after(headers) {
        Some(wait) => {
            let ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
            let mut details = details.unwrap_or_else(|| serde_json::json!({}));
            if let Some(map) = details.as_object_mut() {
                map.insert(RETRY_AFTER_MS_KEY.into(), ms.into());
            }
            Some(details)
        }
        None => details,
    };
    StepError::Retryable { message, details }
}

/// The `Retry-After` hint a handler put into its error details.
#[must_use]
pub fn retry_after_hint(details: Option<&serde_json::Value>) -> Option<Duration> {
    details?
        .get(RETRY_AFTER_MS_KEY)?
        .as_u64()
        .map(|ms| Duration::from_millis(ms).min(MAX_RETRY_AFTER))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_statuses_cover_rate_limits_and_server_errors() {
        for status in [408, 425, 429, 500, 502, 503, 504, 529] {
            assert!(is_retryable_status(status), "{status}");
        }
        for status in [400, 401, 403, 404, 409, 410, 422] {
            assert!(!is_retryable_status(status), "{status}");
        }
    }

    #[test]
    fn status_error_carries_a_bounded_retry_after() {
        use orch8_types::error::StepError;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "7".parse().unwrap());
        let err = status_error("rate limited".into(), 429, &headers, None);
        let StepError::Retryable { details, .. } = &err else {
            panic!("429 must be retryable: {err:?}");
        };
        assert_eq!(
            retry_after_hint(details.as_ref()),
            Some(Duration::from_secs(7))
        );

        headers.insert(reqwest::header::RETRY_AFTER, "999999".parse().unwrap());
        assert_eq!(retry_after(&headers), Some(MAX_RETRY_AFTER));
        headers.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(retry_after(&headers), Some(Duration::ZERO), "past date");

        let err = status_error("gone".into(), 410, &headers, None);
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn redact_url_strips_query_userinfo_fragment() {
        assert_eq!(
            redact_url("https://user:pw@api.example.com/v1/x?key=SECRET#frag"),
            "https://api.example.com/v1/x"
        );
        assert_eq!(redact_url("not a url?key=SECRET"), "not a url");
        assert_eq!(redact_url("weird://u:p@h/x?y"), "weird://h/x");
    }

    #[test]
    fn clamp_timeout_bounds() {
        assert_eq!(clamp_timeout_ms(0), Duration::from_millis(1));
        assert_eq!(
            clamp_timeout_ms(u64::MAX),
            Duration::from_millis(MAX_TIMEOUT_MS)
        );
        assert_eq!(clamp_timeout_ms(1500), Duration::from_millis(1500));
    }

    #[test]
    fn truncate_for_error_marks_truncation() {
        assert_eq!(truncate_for_error(b"abc", 10), "abc");
        assert_eq!(truncate_for_error(b"abcdef", 3), "abc…[truncated]");
    }

    #[tokio::test]
    async fn redact_error_hides_query_string() {
        // Port 1 on a documentation address never answers → fast connect error.
        let client = build(builder(Profile::Operator).timeout(Duration::from_millis(200)));
        let err = client
            .get("http://192.0.2.1:1/path?api_key=SECRET")
            .send()
            .await
            .unwrap_err();
        let msg = redact_error(&err);
        assert!(!msg.contains("SECRET"), "{msg}");
        assert!(msg.contains("192.0.2.1"), "{msg}");
    }

    #[tokio::test]
    async fn token_endpoint_profile_does_not_follow_redirects() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf).await;
            let _ = s
                .write_all(
                    b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://example.com/\r\nContent-Length: 0\r\n\r\n",
                )
                .await;
        });
        let client = build(builder(Profile::TokenEndpoint));
        let resp = client
            .post(format!("http://127.0.0.1:{port}/token"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 307);
    }

    #[tokio::test]
    async fn operator_profile_does_not_forward_signed_bodies_across_redirects() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            stream
                .write_all(
                    b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://example.com/collect\r\nContent-Length: 0\r\n\r\n",
                )
                .await
                .unwrap();
        });

        let response = build(builder(Profile::Operator))
            .post(format!("http://127.0.0.1:{port}/hook"))
            .header("X-Orch8-Signature", "sha256=secret")
            .body("sensitive payload")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(response.url().host_str(), Some("127.0.0.1"));
    }

    #[tokio::test]
    async fn local_sidecar_profile_does_not_forward_post_on_redirect() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let _ = stream
                .write_all(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/steal\r\nContent-Length: 0\r\n\r\n")
                .await;
        });
        let response = build(builder(Profile::LocalSidecar))
            .post(format!("http://127.0.0.1:{port}/execute"))
            .body("secret")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 307);
    }
}
