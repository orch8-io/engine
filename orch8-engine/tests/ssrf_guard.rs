//! SSRF guards of outbound handlers with the guard ENABLED, against the
//! shared table in `orch8_engine::testing::ssrf` (T1-GAP-312..315, 320).
//!
//! Pattern to copy for another outbound handler: one
//! `assert_all_blocked(|url| call_builtin("<handler>", json!({..url..})))`
//! test plus a control that the same handler reaches a mock marked safe.

use serde_json::json;

use orch8_engine::testing::call_builtin;
use orch8_engine::testing::http::{MockHttp, Reply};
use orch8_engine::testing::ssrf::{assert_all_blocked, is_ssrf_block, require_guard_enabled};
use orch8_types::error::StepError;

/// T1-GAP-312: every private, loopback (incl. decimal / hex / octal /
/// mapped encodings and localhost aliases), link-local and metadata target
/// is refused before any connection is made.
#[tokio::test]
async fn http_request_blocks_every_private_and_metadata_target() {
    assert_all_blocked(
        |url| async move { call_builtin("http_request", json!({"url": url})).await },
    )
    .await;
}

/// Control for the table above: with the pre-check satisfied, the same
/// handler does reach a loopback mock, so "no connection" there is the
/// guard's doing.
#[tokio::test]
async fn http_request_reaches_a_loopback_mock_marked_safe() {
    require_guard_enabled();
    let mock = MockHttp::start(vec![Reply::ok_json(&json!({"pong": true}))]).await;
    let url = mock.mark_safe("/ping").await;

    let out = call_builtin("http_request", json!({"url": url})).await;

    assert!(out.is_ok(), "{out:?}");
    assert_eq!(mock.request_count(), 1);
}

/// T1-GAP-313: a safe-looking endpoint that redirects to an internal
/// address must not be followed: the internal target sees no connection
/// and the step fails permanently with the redirect guard's refusal (it was
/// `Retryable` before, so the step re-hit the redirect on every retry).
#[tokio::test]
async fn http_request_does_not_follow_a_redirect_to_an_internal_address() {
    require_guard_enabled();
    let internal = MockHttp::start(Vec::new()).await;
    let entry = MockHttp::start(vec![Reply::redirect(&internal.url("/secret"))]).await;
    let url = entry.mark_safe("/start").await;

    let err = call_builtin("http_request", json!({"url": url}))
        .await
        .expect_err("redirect to loopback must fail");

    assert_eq!(entry.request_count(), 1);
    assert_eq!(internal.connection_count(), 0, "redirect was followed");
    assert!(is_ssrf_block(&err), "{err:?}");
}

/// T1-GAP-314 (DNS rebinding): even when the pre-check said "safe" (here:
/// seeded, as a rebinding resolver would have answered a public IP at check
/// time), the connect-time resolver drops the private address, so a
/// hostname that resolves to loopback is never connected to.
#[tokio::test]
async fn connect_time_resolver_drops_loopback_after_a_passing_precheck() {
    require_guard_enabled();
    let listener = MockHttp::start(Vec::new()).await;
    let url = format!("http://localhost:{}/rebound", listener.addr().port());
    orch8_engine::testing::http::mark_url_safe(&url).await;

    let out = call_builtin("http_request", json!({"url": url})).await;

    assert!(out.is_err(), "{out:?}");
    assert_eq!(listener.connection_count(), 0, "rebinding reached loopback");
}

/// T1-GAP-315: `tool_call` refuses the same table.
#[tokio::test]
async fn tool_call_blocks_every_private_and_metadata_target() {
    assert_all_blocked(|url| async move {
        call_builtin(
            "tool_call",
            json!({"url": url, "tool_name": "t", "arguments": {}}),
        )
        .await
    })
    .await;
}

/// T1-GAP-320: `human_review` refuses a private `notify_url`.
#[tokio::test]
async fn human_review_blocks_a_metadata_notify_url() {
    require_guard_enabled();

    let err = call_builtin(
        "human_review",
        json!({"prompt": "ok?", "notify_url": "http://169.254.169.254/latest"}),
    )
    .await
    .expect_err("metadata notify_url must be refused");

    assert!(is_ssrf_block(&err), "{err:?}");
    assert!(matches!(err, StepError::Permanent { .. }));
}
