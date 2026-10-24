# Distributed, mobile, and embedded runtime review

> **Status: in progress.** This review follows the complete user goal: rethink
> distributed workers, mobile, and embedded use cases, improve the logic, and
> verify edge cases, tests, and security. An individual passing fix does not
> complete the review.

## Baseline and review method

Baseline: `164b3031`. The main CI and Mobile workflows both passed on this exact
commit when rechecked. Work happens in one checkout and one reusable Cargo
build directory to avoid duplicate temporary build trees.

Review lenses run sequentially: code quality, Rust ownership/async safety,
system and trust-boundary architecture, workflow/event transitions, behavioral
test gaps, release and protocol compatibility, and engineering trade-offs.
TypeScript boundary review applies to native/web wrappers. Technical-opportunity
synthesis and documentation polish follow the verified findings. Visual dashboard
and analytics/warehouse reviews are outside this runtime-focused goal.

## Use cases and completion evidence required

| Area | Scenarios to trace and verify | State |
|---|---|---|
| REST workers | Claim generations, duplicate poll delivery, heartbeat loss, completion/failure/release retries, cancellation, queue and capability routing | Pending |
| gRPC workers | Reconnect, demand/backpressure, resumable transfers, session identity, cross-tenant control, cancellation | Pending |
| Hybrid executors | Draining, reconnect, expired credentials, local secrets, TLS/SSRF boundaries, oversized results, ambiguous effects | Pending |
| Mobile remote workers | Stop/restart, cold recovery, app suspension, bounded background windows, stragglers, power transitions, credential refresh | Reviewing |
| Mobile control and sync | Device registration, account/control-plane changes, commands, approval deduplication, revocation, protected fields, hostile manifests | Pending |
| Device delegation and handoff | Interrupted ownership transfer, epoch fencing, idempotent acceptance, private data, return/resume, cleanup | Pending |
| Embedded Rust | Tick/run lifecycle, shutdown, independent engines, tenant/backend identity, cancellation and callback ownership | Pending |
| Node/Python bindings | Initialization failures, repeated close, host callbacks, concurrent calls, crash/restart, handler retention and errors | Pending |
| Swift/Kotlin/RN/Flutter wrappers | Initialization/disposal, listener retention, background wake, FFI errors and cross-platform protocol compatibility | Pending |
| Web embed | Customer-scoped token isolation, sandbox effects, quotas/races, output disclosure, component lifecycle and browser auth | Pending |
| Operations and security | Payload bounds, resource retention, diagnostics without secrets, dependency advisories, migration/rollback and supported platforms | Pending |

For each area, completion requires source and caller review, relevant existing
coverage inspection, reproduced confirmed defects, fixes with regression checks,
and explicit remaining limitations. Source file counts or green tests alone do
not establish full behavioral coverage.

## Initial findings: mobile worker ownership

| ID | Finding | Evidence and required fix | State |
|---|---|---|---|
| DME-001 | Restarted workers can release predecessor tasks that are still executing | `stop_worker` removes the worker slot but leaves tasks running; the successor's orphan drain has a new empty in-flight set. Share live claim ownership across worker generations. | Fixed; reproduced before the change and covered by the mobile suite |
| DME-002 | Journal mutations are not fenced to a lease generation | Start/outcome/delete match only `task_id`; delayed old cleanup can overwrite or delete a new generation. Fence every mutation to the complete claim identity. | Fixed; reproduced before the change and covered by the mobile suite |
| DME-003 | Redelivery replaces durable recovery evidence | `INSERT OR REPLACE` clears `started` and `outcome` even for an identical lease. Make insertion idempotent without erasing recovery evidence. | Fixed; reproduced before the change and covered by the mobile suite |
| DME-004 | Journal and recovery do not isolate control-plane/worker identities | The table's sole key is task ID; the stored worker ID is omitted from recovery reads. Preserve independent claims and recover only with the matching client identity. | Fixed; reproduced before the change and covered by the mobile suite |
| DME-005 | A lease-loss cancellation can detach the timeout-wrapped handler task | `invoke` owns a spawned task through a plain JoinHandle; dropping its caller does not run the inner native invocation's cancellation path immediately. Trace cancellation and straggler tracking before selecting a fix. | Under investigation |

## Check evidence

The initial five behavioral regressions all failed on baseline `164b3031`,
while all 14 existing worker tests passed. The corrected worker subtree passes
25 tests, including upgrade preservation, rollback, concurrent initialization,
generation-fenced writes, and panic cleanup. The complete mobile library passes
297 tests with zero failures. All-target Clippy passes with warnings denied.

Locked advisory policy checks pass for the root/default graph, all optional
features, Node native bindings, Python native bindings, and fuzz workspace.
Existing advisory exceptions remain governed by `deny.toml`; this is not a claim
that every dependency has no published advisory.

Commands used for the first fix batch:

```sh
cargo test --offline --locked -p orch8-mobile --lib worker:: -- --test-threads=4
cargo test --offline --locked -p orch8-mobile --lib -- --test-threads=4
cargo clippy --offline --locked -p orch8-mobile --all-targets -- -D warnings
```

Mock HTTP tests require loopback socket permission. One reusable target directory
is used with incremental compilation and debug information disabled. No complete
cross-runtime review, coverage percentage, throughput gain, or real-device
platform certification is claimed by these checks. The pending areas above and
DME-005 still require investigation and verification.
