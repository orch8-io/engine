//! Route matrix for capability-scoped API keys (`capabilities_allow`).
//!
//! Rule under test: before routing, `api_key_middleware` admits a request only
//! when one of the key's capabilities grants that route family. `Operator`
//! grants everything; every other capability grants a fixed family; grants
//! combine as a union; an empty grant denies everything; the `/api/v1` prefix
//! is stripped exactly once before matching.

use super::*;

use axum::http::Method;

type Case = (
    &'static str,
    &'static [ApiCapability],
    Method,
    &'static str,
    bool,
);

const TASK: &str = "0190a0b4-6f43-7c1e-9a4b-1f2e3d4c5b6a";

fn assert_cases(cases: &[Case]) {
    for (case, caps, method, path, expected) in cases {
        assert_eq!(
            capabilities_allow(caps, method, path),
            *expected,
            "case: {case} ({method} {path} with {caps:?})"
        );
    }
}

fn task_path(action: &str) -> String {
    format!("/api/v1/workers/tasks/{TASK}/{action}")
}

#[test]
fn operator_is_allowed_on_every_route() {
    use ApiCapability::{Operator, Worker};
    assert_cases(&[
        (
            "instance create",
            &[Operator],
            Method::POST,
            "/api/v1/instances",
            true,
        ),
        (
            "release delete",
            &[Operator],
            Method::DELETE,
            "/api/v1/releases/rel-1",
            true,
        ),
        (
            "unknown route",
            &[Operator],
            Method::GET,
            "/api/v1/not-a-real-route",
            true,
        ),
        (
            "operator inside a mixed grant",
            &[Worker, Operator],
            Method::POST,
            "/api/v1/releases",
            true,
        ),
    ]);
}

#[test]
fn worker_is_limited_to_the_workers_family_handlers_and_runtime_registration() {
    use ApiCapability::Worker;
    assert_cases(&[
        (
            "workers collection",
            &[Worker],
            Method::GET,
            "/api/v1/workers",
            true,
        ),
        (
            "any method inside the family",
            &[Worker],
            Method::DELETE,
            "/api/v1/workers/worker-9",
            true,
        ),
        (
            "unversioned path",
            &[Worker],
            Method::POST,
            "/workers/tasks/poll",
            true,
        ),
        (
            "handlers listing",
            &[Worker],
            Method::GET,
            "/api/v1/handlers",
            true,
        ),
        (
            "handlers subpath",
            &[Worker],
            Method::GET,
            "/api/v1/handlers/echo",
            false,
        ),
        (
            "singular /worker",
            &[Worker],
            Method::GET,
            "/api/v1/worker",
            false,
        ),
        (
            "mobile",
            &[Worker],
            Method::GET,
            "/api/v1/mobile/devices",
            false,
        ),
        (
            "approvals",
            &[Worker],
            Method::POST,
            "/api/v1/approvals",
            false,
        ),
        (
            "instance signals",
            &[Worker],
            Method::POST,
            "/api/v1/instances/inst-1/signals",
            false,
        ),
    ]);
}

#[test]
fn device_is_limited_to_the_mobile_family() {
    use ApiCapability::Device;
    assert_cases(&[
        (
            "mobile sync",
            &[Device],
            Method::POST,
            "/api/v1/mobile/sync",
            true,
        ),
        (
            "mobile root",
            &[Device],
            Method::GET,
            "/api/v1/mobile",
            true,
        ),
        (
            "any method inside the family",
            &[Device],
            Method::DELETE,
            "/api/v1/mobile/devices/dev-1",
            true,
        ),
        (
            "truncated prefix",
            &[Device],
            Method::GET,
            "/api/v1/mobil",
            false,
        ),
        ("workers", &[Device], Method::GET, "/api/v1/workers", false),
    ]);
}

#[test]
fn publisher_manages_definitions_but_not_instances_or_plugin_writes() {
    use ApiCapability::Publisher;
    assert_cases(&[
        (
            "release publish",
            &[Publisher],
            Method::POST,
            "/api/v1/releases",
            true,
        ),
        (
            "release read",
            &[Publisher],
            Method::GET,
            "/api/v1/releases/rel-1",
            true,
        ),
        (
            "prompt write",
            &[Publisher],
            Method::POST,
            "/api/v1/prompts",
            true,
        ),
        (
            "sequence create",
            &[Publisher],
            Method::POST,
            "/api/v1/sequences",
            true,
        ),
        (
            "sequence read",
            &[Publisher],
            Method::GET,
            "/api/v1/sequences/seq-1",
            true,
        ),
        (
            "plugin read",
            &[Publisher],
            Method::GET,
            "/api/v1/plugins/plug-1",
            true,
        ),
        (
            "plugin create",
            &[Publisher],
            Method::POST,
            "/api/v1/plugins",
            false,
        ),
        (
            "plugin update",
            &[Publisher],
            Method::PATCH,
            "/api/v1/plugins/plug-1",
            false,
        ),
        (
            "plugin delete",
            &[Publisher],
            Method::DELETE,
            "/api/v1/plugins/plug-1",
            false,
        ),
        (
            "migrate a live instance",
            &[Publisher],
            Method::POST,
            "/api/v1/sequences/migrate-instance",
            false,
        ),
        (
            "migrate with trailing slash",
            &[Publisher],
            Method::POST,
            "/api/v1/sequences/migrate-instance/",
            false,
        ),
        (
            "instances",
            &[Publisher],
            Method::POST,
            "/api/v1/instances",
            false,
        ),
        (
            "bare API prefix",
            &[Publisher],
            Method::GET,
            "/api/v1",
            false,
        ),
    ]);
}

#[test]
fn approver_may_decide_approvals_and_signal_instances_only() {
    use ApiCapability::Approver;
    assert_cases(&[
        (
            "approval decision",
            &[Approver],
            Method::POST,
            "/api/v1/approvals/appr-1/decide",
            true,
        ),
        (
            "approvals collection",
            &[Approver],
            Method::GET,
            "/api/v1/approvals",
            true,
        ),
        (
            "instance signal",
            &[Approver],
            Method::POST,
            "/api/v1/instances/inst-1/signals",
            true,
        ),
        (
            "instance signal, any method",
            &[Approver],
            Method::GET,
            "/api/v1/instances/inst-1/signals",
            true,
        ),
        (
            "instance state transition",
            &[Approver],
            Method::POST,
            "/api/v1/instances/inst-1/state",
            false,
        ),
        (
            "instance collection",
            &[Approver],
            Method::GET,
            "/api/v1/instances",
            false,
        ),
        (
            "instances trailing slash",
            &[Approver],
            Method::POST,
            "/api/v1/instances/",
            false,
        ),
        (
            "bare signals path",
            &[Approver],
            Method::POST,
            "/api/v1/signals",
            false,
        ),
        (
            "workers",
            &[Approver],
            Method::POST,
            "/api/v1/workers/tasks/poll",
            false,
        ),
    ]);
}

#[test]
fn auditor_is_read_only_on_every_route() {
    use ApiCapability::Auditor;
    assert_cases(&[
        ("HEAD", &[Auditor], Method::HEAD, "/api/v1/changes", true),
        (
            "GET in another family",
            &[Auditor],
            Method::GET,
            "/api/v1/releases",
            true,
        ),
        (
            "GET unknown route",
            &[Auditor],
            Method::GET,
            "/api/v1/anything/at/all",
            true,
        ),
        ("GET unversioned", &[Auditor], Method::GET, "/changes", true),
        ("POST", &[Auditor], Method::POST, "/api/v1/changes", false),
        (
            "PUT",
            &[Auditor],
            Method::PUT,
            "/api/v1/instances/inst-1",
            false,
        ),
        (
            "DELETE",
            &[Auditor],
            Method::DELETE,
            "/api/v1/instances/inst-1",
            false,
        ),
        (
            "OPTIONS",
            &[Auditor],
            Method::OPTIONS,
            "/api/v1/changes",
            false,
        ),
    ]);
}

#[test]
fn browser_worker_may_only_drive_the_task_lease_protocol() {
    use ApiCapability::BrowserWorker;
    let complete = task_path("complete");
    let release = task_path("release");
    let unknown_action = task_path("cancel");
    let cases: Vec<(&str, Method, &str, bool)> = vec![
        ("poll", Method::POST, "/api/v1/workers/tasks/poll", true),
        (
            "poll queue",
            Method::POST,
            "/api/v1/workers/tasks/poll/queue",
            true,
        ),
        ("complete own task", Method::POST, &complete, true),
        ("release own task", Method::POST, &release, true),
        ("unknown task action", Method::POST, &unknown_action, false),
        (
            "non-uuid task id",
            Method::POST,
            "/api/v1/workers/tasks/abc/complete",
            false,
        ),
        ("GET poll", Method::GET, "/api/v1/workers/tasks/poll", false),
        ("workers collection", Method::POST, "/api/v1/workers", false),
        ("mobile sync", Method::POST, "/api/v1/mobile/sync", false),
    ];
    for (case, method, path, expected) in cases {
        assert_eq!(
            capabilities_allow(&[BrowserWorker], &method, path),
            expected,
            "case: {case} ({method} {path})"
        );
    }
}

#[test]
fn device_node_may_only_sync_its_device_lease_tasks_and_delegate_continuity() {
    use ApiCapability::DeviceNode;
    let fail = task_path("fail");
    let delegation = format!("/api/v1/continuity/delegations/{TASK}");
    let cases: Vec<(&str, Method, &str, bool)> = vec![
        ("list runtimes", Method::GET, "/api/v1/runtimes", true),
        ("mobile sync", Method::POST, "/api/v1/mobile/sync", true),
        (
            "device register",
            Method::POST,
            "/api/v1/mobile/devices/register",
            true,
        ),
        (
            "device runtime advertisement",
            Method::POST,
            "/api/v1/mobile/devices/dev-1/runtime",
            true,
        ),
        (
            "task poll",
            Method::POST,
            "/api/v1/workers/tasks/poll",
            true,
        ),
        ("fail own task", Method::POST, &fail, true),
        ("read a delegation", Method::GET, &delegation, true),
        (
            "claim a delegation",
            Method::POST,
            "/api/v1/continuity/delegations/claim",
            true,
        ),
        (
            "DELETE mobile sync",
            Method::DELETE,
            "/api/v1/mobile/sync",
            false,
        ),
        ("releases", Method::POST, "/api/v1/releases", false),
        ("instances", Method::GET, "/api/v1/instances", false),
    ];
    for (case, method, path, expected) in cases {
        assert_eq!(
            capabilities_allow(&[DeviceNode], &method, path),
            expected,
            "case: {case} ({method} {path})"
        );
    }
}

#[test]
fn capability_grants_combine_as_a_union() {
    use ApiCapability::{Approver, Auditor, Device, Publisher, Worker};
    assert_cases(&[
        (
            "worker+publisher: releases",
            &[Worker, Publisher],
            Method::POST,
            "/api/v1/releases",
            true,
        ),
        (
            "worker+publisher: workers",
            &[Worker, Publisher],
            Method::POST,
            "/api/v1/workers/tasks/poll",
            true,
        ),
        (
            "worker+publisher: mobile",
            &[Worker, Publisher],
            Method::POST,
            "/api/v1/mobile/sync",
            false,
        ),
        (
            "device+auditor: mobile write",
            &[Device, Auditor],
            Method::POST,
            "/api/v1/mobile/sync",
            true,
        ),
        (
            "device+auditor: foreign read",
            &[Device, Auditor],
            Method::GET,
            "/api/v1/releases",
            true,
        ),
        (
            "device+auditor: foreign write",
            &[Device, Auditor],
            Method::POST,
            "/api/v1/releases",
            false,
        ),
        (
            "approver+auditor: signal write",
            &[Approver, Auditor],
            Method::POST,
            "/api/v1/instances/inst-1/signals",
            true,
        ),
        (
            "approver+auditor: other write",
            &[Approver, Auditor],
            Method::POST,
            "/api/v1/instances",
            false,
        ),
        (
            "duplicate capability",
            &[Worker, Worker],
            Method::POST,
            "/api/v1/workers/tasks/poll",
            true,
        ),
    ]);
}

#[test]
fn empty_grant_denies_every_route() {
    assert_cases(&[
        ("read", &[], Method::GET, "/api/v1/changes", false),
        ("worker family", &[], Method::GET, "/api/v1/workers", false),
    ]);
}

#[test]
fn api_prefix_is_stripped_only_once() {
    assert_cases(&[(
        "doubled prefix",
        &[ApiCapability::Worker],
        Method::POST,
        "/api/v1/api/v1/workers/tasks/poll",
        false,
    )]);
}

/// Documents CURRENT behaviour, not an endorsed policy: family matching is a
/// string prefix, not a path-segment match, and the approver signal grant is
/// `starts_with("/instances/") && ends_with("/signals")`. None of these paths
/// is a real route today (the router answers 404), so the looseness is latent.
/// Tightening to segment matching should flip these rows deliberately.
#[test]
fn route_family_matching_is_currently_prefix_based_not_segment_based() {
    use ApiCapability::{Approver, Device, Publisher, Worker};
    assert_cases(&[
        (
            "worker /workersmith",
            &[Worker],
            Method::GET,
            "/api/v1/workersmith",
            true,
        ),
        (
            "device /mobilex",
            &[Device],
            Method::GET,
            "/api/v1/mobilex",
            true,
        ),
        (
            "publisher /releasesx",
            &[Publisher],
            Method::GET,
            "/api/v1/releasesx",
            true,
        ),
        (
            "approver /instances/signals without id",
            &[Approver],
            Method::POST,
            "/api/v1/instances/signals",
            true,
        ),
    ]);
}
