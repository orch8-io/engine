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
//!   be internal (webhooks). No resolver filter, but redirect hops are still
//!   re-checked so a trusted target can't bounce us into cloud metadata.
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
    /// Operator-configured URL: checked redirects only.
    Operator,
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
        Profile::Operator => b.redirect(checked_redirects()),
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
