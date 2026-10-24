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
| REST workers | Claim generations, duplicate poll delivery, heartbeat loss, completion/failure/release retries, cancellation, queue and capability routing | Reviewing; shared HTTP contract and batch ownership fixes verified; supervision checks open |
| gRPC workers | Reconnect, demand/backpressure, resumable transfers, session identity, cross-tenant control, cancellation | Reviewing; full 146-test library suite passes; demand/lease invariant audit ongoing |
| Hybrid executors | Draining, reconnect, expired credentials, local secrets, TLS/SSRF boundaries, oversized results, ambiguous effects | Reviewing; redirect boundary fixed; lease supervision still under investigation |
| Mobile remote workers | Stop/restart, cold recovery, app suspension, bounded background windows, stragglers, power transitions, credential refresh | Reviewing; journal ownership and native cancellation covered; lease supervisor validation below |
| Mobile control and sync | Device registration, account/control-plane changes, commands, approval deduplication, revocation, protected fields, hostile manifests | Pending |
| Device delegation and handoff | Interrupted ownership transfer, epoch fencing, idempotent acceptance, private data, return/resume, cleanup | Pending |
| Embedded Rust | Tick/run lifecycle, shutdown, independent engines, tenant/backend identity, cancellation and callback ownership | Pending |
| Node/Python bindings | Initialization failures, repeated close, host callbacks, concurrent calls, crash/restart, handler retention and errors | Pending |
| Swift/Kotlin/RN/Flutter wrappers | Initialization/disposal, listener retention, background wake, FFI errors and cross-platform protocol compatibility | Pending |
| Web embed | Customer-scoped token isolation, sandbox effects, quotas/races, output disclosure, component lifecycle and browser auth | Reviewing; HTTP refresh and component account ownership fixed; sandbox/quota review pending |
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
| DME-005 | A lease-loss cancellation can detach the timeout-wrapped handler task | `invoke` detached a spawned timeout wrapper on lease loss. Keep the handler future owned by execution so cancellation immediately reaches native-call quarantine. | Fixed; regression reproduced before the change and covered by the mobile suite |

## Cancellation and background admission findings

Baseline for this batch: main `7911a16b` (first recovery batch merged).

| ID | Finding | Evidence and correction | State |
|---|---|---|---|
| DME-006 | Cancelling a background-window future leaves background claims enabled | A shared boolean is only reset on normal return. Each window now owns a counted RAII permission. | Fixed; reproduced before the change and covered by the mobile suite |
| DME-007 | One background window ending revokes another live window | Concurrent OS wake handlers overwrite one boolean. Independent permissions remain until each owner returns or is cancelled. | Fixed; reproduced before the change and covered by the mobile suite |
| DME-008 | A stopped worker starts tasks returned by a pending poll | Admission checks foreground/power but omit worker cancellation. Recheck cancellation before marking a delivery started; release it unstarted. | Fixed; reproduced before the change and covered by the mobile suite |
| DME-009 | Quarantine expiry allows a retry while a native side effect is still running | Artificially age a live callback past 15 minutes: the old admission check returns false. Retain quarantine until physical completion; emit one prolonged-call diagnostic. | Fixed; reproduced before the change and covered by the mobile suite |

The DME-005 lease-loss regression failed against the original timeout path and
passed after removing the detached async task. A separate worker run reproduced
DME-006/007/008 (26 passed, three expected failures). The aged quarantine test
reproduced DME-009 without a 15-minute sleep. All five checks exercise observable
admission or ownership behavior, with native callback cleanup controlled by a
channel rather than a timing guess. The test harness now shares the same
straggler tracker between native callbacks and worker admission, as production
does.

A stuck native call deliberately blocks its own handler indefinitely: availability
cannot safely be recovered by guessing the physical side effect has ended. Other
handlers remain eligible. Host remediation or process restart is required for a
callback that never returns. No forced native cancellation is claimed.

## Check evidence

The initial five behavioral regressions all failed on baseline `164b3031`,
while all 14 existing worker tests passed. The corrected worker subtree passes
25 tests, including upgrade preservation, rollback, concurrent initialization,
generation-fenced writes, and panic cleanup. The first batch passed all
297 mobile library tests. The cancellation batch passes all 302 tests with zero
failures, including the five added behavioral regressions. All-target Clippy
passes with warnings denied.

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
platform certification is claimed by these checks. The pending areas above
remain under review, including worker resource bounds and the remaining
distributed and embedded trust boundaries.

## Web credential usage schema (TypeScript phase 0)

`TokenProvider` originates in application code, flows through the React stable
trampoline and the base custom-element property, and is called by `EmbedClient`.
The callback currently receives refresh reason only. `EmbedClient` mutates its
current bearer token; provider-level shared state retains a token, in-flight
promise, and fetch time. HTTP requests serialize the selected bearer into
`Authorization`. The token decoder supplies expiry/scope UX, base-element
identity comparison, and theme-cache identity; the engine remains authoritative
for signature and access verification.

The same callback may serve multiple engine origins, explicit initial customer
tokens, or successive application sessions. Cache ownership therefore requires
provider, engine base, explicit application auth context, and initial
authorization scope. Once a client has a readable principal, refresh must not
silently change it. Same-context widgets must continue deduplicating expiry and
401 refreshes. Context cache retention must be bounded independently of token
rotation. Provider-only callers must change auth context or construct a new
provider/client when the signed-in account changes.

The lifecycle follow-up below verifies the React trampoline, retained ready
views, dirty forms, and late asynchronous responses separately from HTTP refresh
isolation.

## Web credential findings and evidence

| ID | Finding | Evidence and correction | State |
|---|---|---|---|
| DME-010 | Provider cache sends another engine's bearer token | Two clients with one provider and different engines reused the first credential. Isolate engine bases. | Fixed; regression reproduced |
| DME-011 | A 401 can adopt another customer's refreshed credential | Customer A received customer B's run in a mock flow. Isolate initial authorization scopes and explicit provider-only auth contexts. | Fixed; regression reproduced |
| DME-012 | Refresh silently changes a known tenant/customer principal | Two regressions confirmed tenant and customer changes reached HTTP. Validate principal before publishing/adopting refresh tokens. | Fixed; regressions reproduced |
| DME-013 | A synchronous provider exception permanently poisons shared refresh | A second request reused the rejected promise after the provider recovered. Publish the promise before owner cleanup. | Fixed; regression reproduced |
| DME-014 | Embed tests/build were absent from CI | Existing workflows did not invoke `packages/embed`. Add a pinned Node/pnpm job for locked installation, advisory audit, typecheck, lint, tests, and build. | Added; actionlint passes; hosted validation pending |
| DME-015 | Embed test tooling retained seven published advisories | Audit reported two high, four moderate, and one low advisory across Happy DOM 21.0.3 and Vite 8.3.1. Upgrade the locked tooling and audit the full graph. | Fixed; refreshed audit passes |

The original isolation tests produced four expected failures while all six
pre-existing client-hardening tests passed. The provider-lifecycle test separately
reproduced a synchronous failure permanently blocking recovery (13 passed, one
expected failure). New coverage includes distinct application account contexts,
initial permission scopes, rejection recovery, malformed payload shapes, and
refresh after context eviction. All 115 embed tests pass locally, along with TypeScript checking, ESLint, and
the package/CDN build. Hosted CI remains pending. Additional checks preserve
coalescing across token rotations and isolate sequence restrictions, reject
unreadable principal replacements, and handle backwards client-clock jumps.

The decoder validates payload shape before returning its public typed payload;
it does not verify token authenticity. Opaque tokens remain server-validated,
but cannot replace a readable principal during an anchored refresh. Cache
retention is capped at 64 contexts per provider; eviction only reduces refresh
deduplication. Existing same-context expiry and 401 coalescing tests remain green.

The subsequent component batch exposes the auth context on elements and React
wrappers and verifies account-switch handling below.

The complete embed dependency audit initially reported seven advisories. Happy
DOM is now locked to 21.0.4 and Vite to 8.3.5, with patched minimum requirements
in the manifest. The refreshed complete graph audit reports zero vulnerabilities.
The affected tooling is used for development/tests; no browser-runtime impact is
inferred from those advisories. References: [Happy DOM advisory](https://github.com/advisories/GHSA-834v-8h66-3h79)
and [Vite/launch-editor advisory](https://github.com/advisories/GHSA-587h-7wmj-h5pm).
The new CI job audits this graph without severity exclusions. `actionlint` passes
for the updated workflow. Frozen installation, all 115 tests, typecheck, lint,
and the package build are rerun on the patched dependency graph.

## Component account ownership and cancellation

Baseline: `31ed6938`, the verified HTTP credential-isolation batch. Runtime state
ownership now follows account/session context, engine base, authorization scopes,
and credential/transport sources. Each component operation captures its owner's
abort signal. Boundary changes clear visible state before resetting models or
notifying dirty-state listeners. Disconnection cancels pending operations too.
A new signal is created on reconnection. Cosmetic changes and same-authorization
token rotation retain the view. Sequence-only changes retain the existing
same-account save-completion behavior.

| ID | Finding | Reproduction and correction | State |
|---|---|---|---|
| DME-016 | Dirty builders and approval comments cross customer/engine boundaries | A builder can submit account A's draft using account B's credentials; comments persist for matching approval IDs. Clear old models/forms synchronously on explicit ownership changes; expose account context to custom elements and React. | Fixed; baseline regressions reproduced |
| DME-017 | Late asynchronous results restore old-account models or events | An aborted initial read still mutates its model before the base check; a late save checks sequence name but not account. Capture ownership for reads, saves, approvals, starts, and theme results, guarding state and events. | Fixed; baseline read/save regressions reproduced |
| DME-018 | Cancelled requests still dispatch through custom transports | Already-aborted mutations and mutations cancelled during token refresh reach HTTP if the transport ignores signals. Check cancellation before dispatch and cancel each caller's wait without cancelling shared refreshes. | Fixed; two regressions reproduced |
| DME-019 | Theme caching merges opaque credentials and grows without bound | Two opaque tokens share the unknown-identity cache key; visiting 160 accounts retains every theme. Resolve identity before lookup, use structured authorization keys, and retain at most 128 entries. | Fixed; two regressions reproduced |

Four account-isolation regressions fail against the original component sources;
these cover dirty state, comment state, late reads, and late saves. The corrected
sources pass those scenarios. Additional checks cover engine changes, React's
stable provider trampoline with explicit account context, provider replacement,
late approval/run-start events, late themes, cancellation while a provider is
still pending, and theme cache eviction. Existing same-authorization token
rotation and inline provider stability tests remain green. Tests intentionally
use transports that ignore abort to verify ownership independently of transport
cooperation. The old tests that preserved unsaved data across different customers
were corrected to require preservation only during same-authorization rotation.

Local checks pass all 130 embed tests, typecheck, ESLint, package/CDN build,
frozen installation, and a fresh full advisory audit (zero vulnerabilities).
Hosted validation of this component batch remains pending.
The previous HTTP/cache commit's hosted Web embed SDK job passed.

Compatibility: switching accounts now deliberately discards old drafts. Hosts
that preserve drafts must store them under the originating account. Providers
must mint for the requested authenticated session; an auth-context label does
not authorize or change a static token. Physical server mutations already
accepted may finish after local cancellation, but cannot update a replacement
view or emit their old-account payload there. No automatic server rollback is
claimed. A cancelled builder save resolves false; a cancelled programmatic run
start rejects as aborted.

## Distributed HTTP transport and lease contract

Baseline: `428a5c69`. The production hybrid HTTP transport used a default reqwest
client while mobile already used the hardened token-endpoint profile. A
controlled 307 redirect captured the hybrid API key, custom routing header, and
registration body at a second local server. The hybrid now uses the shared
operator profile: redirects are disabled while operator-configured internal
endpoints, proxy configuration, HTTP/1.1 routing, timeouts, and custom roots keep
their existing behavior. The regression requires an HTTP 307 error from the
configured server, so a blanket connection failure cannot satisfy it.

The server poll contract explicitly makes each task's effective lease override
the global poll default (`orch8-api/src/workers.rs`). The shared client preserves
that precedence; it fills only missing/invalid task grants from valid defaults.
A poll-level zero or value outside the per-task u32 range is ignored for timer
calculation. No shorter global default is substituted for a valid explicit task
lease.

| ID | Finding | Reproduction and correction | State |
|---|---|---|---|
| DME-020 | Hybrid redirects forward control-plane credentials and bodies | A real production transport followed a 307 to a second server with API key, routing header, and JSON body intact. Use the shared redirect-disabled operator profile. | Fixed; regression and complete server tests pass |
| DME-021 | A later handler poll discards previously claimed work | First handler returns a leased task; second returns 503, causing the whole claim call to return an error. Return work immediately and advance the cursor on successive calls. | Fixed; regression and complete lean engine tests pass |
| DME-022 | Lease-client Debug exposes URL credentials | Debug included user information, query tokens, and fragments. Use the established URL redaction helper. | Fixed; regression and complete lean engine tests pass |
| DME-023 | Legacy task records lose their response-level lease grant | Poll returns a short default lease but tasks omit it; later settlement uses the unrelated protocol default. Retain the grant with each task without replacing explicit task leases. | Fixed; regression and complete lean engine tests pass |
| DME-024 | Invalid lease values bypass fallback or create unsafe timers | A zero task lease ignored a valid poll default; u64-max default produced an unrepresentable cadence. Validate positive values and wire range before choosing fallback. | Fixed; regressions and complete lean engine tests pass |
| DME-025 | Lean-build tests assume optional email support | Full no-default-feature engine tests pass 2007 cases but fail an email-delivery assertion. Test both feature-enabled delivery and feature-disabled rejection, retaining Slack/token checks. | Corrected; lean suite and email-enabled check pass |

The redirect regression failed before the production change. The five shared
contract regressions failed while all 15 existing remote tests passed; all 20
focused tests now pass. The server transport subtree passes its three tests.
Complete local validation passes 2008 lean engine tests, 302 mobile tests,
71 lean server binary tests, and the email-enabled approval check. All-target
Clippy passes with warnings denied for the engine/server/mobile default-feature
union. All 146 gRPC library tests pass, including the 16 client/session checks.
Already-existing controls include an 8 MiB response cap and status-based lease
fencing; they were inspected rather than reimplemented.

At this stage, live duplicate admission, surplus release fanout, and lease
supervision were still open. The subsequent sections record their fixes and
limits. Slow advertise/claim shutdown, gRPC demand/acknowledgement backpressure,
mobile account/tenant changes, and the remaining mobile, embedded, binding,
and sandbox areas above remain part of the full goal.

## Live claim admission and bounded release cleanup

The next batch protects in-process callback ownership. A synchronous admission
ledger tracks task UUID and lease generation before a spawned future can be
polled. Its RAII guards return permits and clear only their own generation on
normal exit, cancellation before first poll, and panic. A newer delivery cancels
the live predecessor and releases the new claim as unstarted rather than running
both callbacks concurrently. Duplicate/stale live deliveries are ignored without
releasing the original lease.

| ID | Finding | Correction and evidence | State |
|---|---|---|---|
| DME-026 | Duplicate delivery starts a second callback when slots are free | Reserve live task/generation synchronously before spawning. Baseline invokes twice; regression requires once. | Fixed |
| DME-027 | A duplicate received at capacity releases the original lease | Check ownership before capacity admission. Baseline releases a live claim; regression forbids that release. | Fixed |
| DME-028 | Older/newer task generations overlap the live callback | Fence stale generations; cancel the predecessor and defer the newer callback through unstarted release. Both overlap regressions fail on baseline. | Fixed |
| DME-029 | Overdelivery creates unbounded release RPC fanout and continues claiming | Limit release RPCs to eight, queue the rest, and stop claiming while the queue has work. Baseline starts 100 concurrent releases. | Fixed |
| DME-030 | A pending unstarted release races a second delivery of its generation | Retain a separate exact-generation reservation until the release future ends. Tests also verify predecessor exit cannot clear a newer reservation. | Fixed for live release futures |
| DME-031 | Concurrent or pre-cancelled run controllers advertise/withdraw the same executor | Serialize run ownership with a cancellable async mutex before advertising. Both baseline regressions fail; queued cancellation and sequential restart pass. | Fixed |

All six live-admission/fanout regressions failed against the original production
executor while its 11 existing tests passed. Both controller regressions also
failed before controller serialization. The final focused suite passes 25 tests,
including cancellation before first poll, generation-specific guard cleanup,
sequential controller restart, and bounded shutdown of a blocked release backlog.

These guarantees apply to live callbacks and live release futures in one executor
object. They do not establish durable exactly-once execution. Release ambiguity
after a timeout, replay after callback completion or process restart, renewal
failure past the last confirmed lease deadline, and slow advertise/claim shutdown
remain open. The release queue is backpressured to one transport claim batch;
custom transports must bound that batch themselves. HTTP responses already have
an 8 MiB cap. Mobile release fanout and admission across mobile worker generations
remain separate pending review areas. This batch does not close the broader
mobile, embedded, binding, sandbox, and security review above.

Final merge-candidate validation: all 2,022 lean engine library tests and
all 146 gRPC library tests, and all-target engine/server/mobile Clippy with
warnings denied pass.


## Confirmed lease authority across task phases

Baseline: main `f2d88364`. Four executor regressions failed while all 25 existing
executor tests passed. Through the real mobile HTTP client and native bridge,
two more regressions failed while all 302 existing mobile tests passed.

A shared `remote_worker::lease::ConfirmedLease` now requires a successful
heartbeat before starting work. Transient confirmation failures retry within a
bounded lease window; fenced or rejected confirmation cannot invoke a callback.
Renewal runs concurrently with preparation, callback execution, output sealing,
and outcome delivery. Every successful confirmation/renewal is measured from
before its request, and an explicit clock check rejects late acknowledgements.
Only confirmed renewals extend the deadline. The internal cadence also stays
at most one third of the grant, including one-second grants.

| ID | Finding | Correction and evidence | State |
|---|---|---|---|
| DME-032 | A fast callback executes a stale completed/fenced delivery before its first heartbeat | Check server ownership before invoking either Rust or native callbacks. Both core and mobile regressions fail on baseline. | Fixed; final validation below |
| DME-033 | Repeated renewal failures allow callbacks to outlive confirmed authority | Stop owned work at the last confirmed deadline. Core callback expiry and mobile native quarantine regressions fail on baseline. | Fixed; final validation below |
| DME-034 | Waiting inline for a heartbeat suspends callback and settlement polling | Poll lease supervision concurrently with the complete task operation. A blocked renewal must not block a callback result. | Fixed; final validation below |
| DME-035 | Settlement fabricates a new lease budget and its RPC can outlive the grant | Preserve one supervisor through all phases and bound each pending RPC by that authority. Baseline stalled settlement never frees its slot. | Fixed; final validation below |
| DME-036 | Expiry during mobile result delivery needs to preserve known outcome evidence | Keep the result outside the supervised future; retain/re-record it for orphan recovery rather than release it as unknown. New end-to-end recovery test requires one native invocation. | Covered by final validation below |
| DME-037 | A ready late acknowledgement can be observed after a timeout deadline | Recheck the previous monotonic deadline explicitly before accepting any renewal. A deliberately delayed single-thread runtime must not resume the operation. | Covered by final validation below |

Additional checks cover recovery after a transient renewal failure, request-time
rather than response-time authority, one-second grants, and dropping a cancelled
callback before waiting for its release RPC. Mobile stop continues to let already
started work drain, while cancellation of a pending ownership check releases an
unstarted claim. Mobile rechecks OS/power/stop admission after confirmation.

A physically running native call cannot be forcibly stopped: its wrapper drops
and quarantines it until physical completion. This does not guarantee rollback
of an external mutation accepted before cancellation. An accepted heartbeat
confirms server authority for the current generation; it is not durable evidence
that an earlier process never started that generation. Cold restart ambiguity,
core durable outcome recovery, ambiguous release replies, mobile concurrency
across worker generations, account/tenant boundaries, and the rest of the
original review remain open.


Final local validation passes all 2,031 lean engine library tests, all 305 mobile
library tests, all 146 gRPC library tests, and all-target Clippy across the
engine/server/mobile default-feature union with warnings denied. The native
expiry regression observes quarantine while the physical callback is still
blocked, then explicitly releases it and checks quarantine clears. The known
outcome test restores server connectivity, drains the journal, and verifies
exactly one handler invocation. Hosted checks for this batch remain pending.
