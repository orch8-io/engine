//! The shared table of URLs every outbound SSRF guard must block (T1 HX-5,
//! T1-GAP-312..321), and a one-line assertion for any outbound handler.
//!
//! ```ignore
//! assert_all_blocked(|url| async move {
//!     call_builtin("http_request", json!({"url": url})).await
//! })
//! .await;
//! ```
//!
//! Entries containing `{port}` point at a live loopback listener started by
//! [`assert_all_blocked`]; it asserts the listener saw **no connection**, so
//! a passing case proves the guard refused the target, not that nothing was
//! listening. Run with the guard enabled: never set
//! `ORCH8_ALLOW_INTERNAL_URLS` in the test process.

use std::future::Future;

use orch8_types::error::StepError;

use super::http::MockHttp;

/// One URL a guard must refuse, and why.
#[derive(Debug, Clone, Copy)]
pub struct SsrfCase {
    /// May contain `{port}`, replaced by a live loopback listener's port.
    pub url: &'static str,
    pub why: &'static str,
}

/// Targets every outbound guard must block. The loopback entries carry
/// `{port}` so a reachable listener backs them.
pub const SSRF_BLOCKED_URLS: &[SsrfCase] = &[
    SsrfCase {
        url: "http://169.254.169.254/latest/meta-data/",
        why: "cloud metadata (link-local)",
    },
    SsrfCase {
        url: "http://169.254.170.2/v2/credentials",
        why: "ECS task metadata (link-local)",
    },
    SsrfCase {
        url: "http://127.0.0.1:{port}/",
        why: "IPv4 loopback",
    },
    SsrfCase {
        url: "http://127.1:{port}/",
        why: "short-form IPv4 loopback",
    },
    SsrfCase {
        url: "http://2130706433:{port}/",
        why: "decimal-encoded 127.0.0.1",
    },
    SsrfCase {
        url: "http://0x7f000001:{port}/",
        why: "hex-encoded 127.0.0.1",
    },
    SsrfCase {
        url: "http://0177.0.0.1:{port}/",
        why: "octal-encoded 127.0.0.1",
    },
    SsrfCase {
        url: "http://localhost:{port}/",
        why: "localhost alias",
    },
    SsrfCase {
        url: "http://LOCALHOST:{port}/",
        why: "localhost alias, upper case",
    },
    SsrfCase {
        url: "http://localhost.:{port}/",
        why: "localhost alias, trailing dot",
    },
    SsrfCase {
        url: "http://0.0.0.0:{port}/",
        why: "unspecified address (reaches loopback on Linux)",
    },
    SsrfCase {
        url: "http://[::1]:{port}/",
        why: "IPv6 loopback",
    },
    SsrfCase {
        url: "http://[::ffff:127.0.0.1]:{port}/",
        why: "IPv4-mapped IPv6 loopback",
    },
    SsrfCase {
        url: "http://[::ffff:10.0.0.1]/",
        why: "IPv4-mapped IPv6 private",
    },
    SsrfCase {
        url: "http://[::ffff:169.254.169.254]/",
        why: "IPv4-mapped IPv6 metadata",
    },
    SsrfCase {
        url: "http://[64:ff9b::a9fe:a9fe]/",
        why: "NAT64-embedded metadata address",
    },
    SsrfCase {
        url: "http://10.0.0.1/",
        why: "RFC 1918 10/8",
    },
    SsrfCase {
        url: "http://172.16.0.1/",
        why: "RFC 1918 172.16/12",
    },
    SsrfCase {
        url: "http://192.168.1.1/",
        why: "RFC 1918 192.168/16",
    },
    SsrfCase {
        url: "http://100.64.0.1/",
        why: "CGNAT 100.64/10",
    },
    SsrfCase {
        url: "http://[fc00::1]/",
        why: "IPv6 ULA",
    },
    SsrfCase {
        url: "http://[fe80::1]/",
        why: "IPv6 link-local",
    },
    SsrfCase {
        url: "http://255.255.255.255/",
        why: "broadcast",
    },
];

/// [`SSRF_BLOCKED_URLS`] with `{port}` replaced by `port`.
#[must_use]
pub fn blocked_urls(port: u16) -> Vec<(String, &'static str)> {
    SSRF_BLOCKED_URLS
        .iter()
        .map(|case| (case.url.replace("{port}", &port.to_string()), case.why))
        .collect()
}

/// `true` for the guard's refusal: a `Permanent` error whose message says
/// `blocked` (the wording every guarded handler uses).
#[must_use]
pub fn is_ssrf_block(error: &StepError) -> bool {
    matches!(error, StepError::Permanent { message, .. } if message.contains("blocked"))
}

/// Panic unless SSRF guards are enabled in this process.
pub fn require_guard_enabled() {
    assert!(
        !crate::handlers::builtin::internal_urls_allowed(),
        "ORCH8_ALLOW_INTERNAL_URLS is set: SSRF tests would pass for the wrong reason"
    );
}

/// Call `call(url)` for every [`SSRF_BLOCKED_URLS`] entry and assert each
/// returns the guard's refusal ([`is_ssrf_block`]) and that the loopback
/// listener behind the `{port}` entries was never connected to. Reports
/// every failing case at once.
pub async fn assert_all_blocked<F, Fut, T>(call: F)
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<T, StepError>>,
    T: std::fmt::Debug,
{
    assert_all_blocked_by(call, is_ssrf_block).await;
}

/// [`assert_all_blocked`] with a custom refusal predicate, for handlers that
/// word the refusal differently.
pub async fn assert_all_blocked_by<F, Fut, T>(call: F, is_block: impl Fn(&StepError) -> bool)
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<T, StepError>>,
    T: std::fmt::Debug,
{
    require_guard_enabled();
    let listener = MockHttp::start(Vec::new()).await;
    let mut failures = Vec::new();
    for (url, why) in blocked_urls(listener.addr().port()) {
        match call(url.clone()).await {
            Err(e) if is_block(&e) => {}
            other => failures.push(format!("{url} ({why}): {other:?}")),
        }
    }
    assert!(
        failures.is_empty(),
        "SSRF guard let these through (or failed differently):\n{}",
        failures.join("\n")
    );
    assert_eq!(
        listener.connection_count(),
        0,
        "a blocked URL reached the loopback listener"
    );
}
