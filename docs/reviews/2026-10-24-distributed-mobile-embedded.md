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
| Mobile control and sync | Device registration, account/control-plane changes, commands, approval deduplication, revocation, protected fields, hostile manifests | Reviewing; provider and per-sync callback identity verified below; manifest callback execution bounded below; scoped cache and signed audience ingestion verified internally; internal authenticated private sync verified; public runtime/SDK integration remains open |
| Device delegation and handoff | Interrupted ownership transfer, epoch fencing, idempotent acceptance, private data, return/resume, cleanup | Reviewing; legacy queue fairness and response correlation verified; scoped transfer and acceptance audit remain open |
| Embedded Rust | Tick/run lifecycle, shutdown, independent engines, tenant/backend identity, cancellation and callback ownership | Reviewing; runtime retirement/terminal shutdown and backend-owned volatile wake/retry state verified; backend-qualified cache identity verified; sub-sequence restart recovery and indexed current/historical child selection pass SQLite/Postgres behavioral and query-plan tests; callbacks remain under review |
| Node/Python bindings | Initialization failures, repeated close, host callbacks, concurrent calls, crash/restart, handler retention and errors | Pending |
| Swift/Kotlin/RN/Flutter wrappers | Initialization/disposal, listener retention, background wake, FFI errors and cross-platform protocol compatibility | Reviewing; Swift/KMP ownership and disposal covered; RN/Flutter ports, artifact parity, and platform runtime E2E remain open |
| Web embed | Customer-scoped token isolation, sandbox effects, quotas/races, output disclosure, component lifecycle and browser auth | Reviewing; HTTP refresh and component account ownership fixed; sandbox/quota review pending |
| Operations and security | Payload bounds, resource retention, diagnostics without secrets, dependency advisories, migration/rollback and supported platforms | Pending |

For each area, completion requires source and caller review, relevant existing
coverage inspection, reproduced confirmed defects, fixes with regression checks,
and explicit remaining limitations. Source file counts or green tests alone do
not establish full behavioral coverage.

## Confirmed scoped-runtime requirement

The user confirmed that one engine/database must support **multiple accounts
and multiple catalogs with explicit scopes**. Stable account/catalog identity
must survive token refresh and cannot be inferred from rotating token strings.
[ADR-0002](../adr/0002-mobile-account-and-catalog-scopes.md) records the proposed
ownership/API/storage contract and gates S1-S12. These gates remain open; bounded
sync admission and atomic metadata are foundations, not scoped isolation.
The original distributed/mobile/embedded completion matrix remains required.

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
were still pending at this stage; the shared-admission section below records
their subsequent fixes. This batch does not close the broader
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
core durable outcome recovery, ambiguous release replies, account/tenant
boundaries, and the rest of the original review remain open. Mobile concurrency
across worker generations is addressed in the next section.


Final local validation passes all 2,031 lean engine library tests, all 305 mobile
library tests, all 146 gRPC library tests, and all-target Clippy across the
engine/server/mobile default-feature union with warnings denied. The native
expiry regression observes quarantine while the physical callback is still
blocked, then explicitly releases it and checks quarantine clears. The known
outcome test restores server connectivity, drains the journal, and verifies
exactly one handler invocation. Hosted checks for this batch remain pending.


## Shared mobile admission and cleanup across generations

Baseline: PR #266 `4da635cd`. Three behavioral regressions fail while the 28
previous worker-level tests pass. Separate gated HTTP tests observe 100 blocked
surplus release requests and 20 blocked requests from concurrent recovery passes.
A SQLite trigger injecting an INSERT failure also reproduces an unsafe unstarted
release despite a previously recorded start and outcome.

The engine's cloned claim store now owns a shared admission ledger. It reserves
capacity synchronously before spawning, keeps task scope separate from lease
epoch, and retains an exact-generation fence while an unstarted release is queued
or active. Newer deliveries cancel the old owned execution and defer the new
callback; duplicate/stale deliveries cannot revoke the live lease at capacity.
Stopping a worker still lets its existing tasks finish. A successor's configured
limit includes predecessors; shrinking waits for existing tasks, and expanding
adds capacity. Worker/window in-flight accounting includes draining predecessors.

| ID | Finding | Correction and evidence | State |
|---|---|---|---|
| DME-038 | Restart grants independent slots while predecessor callbacks still execute | Share the engine admission budget across worker generations. Baseline runs two callbacks at configured concurrency one. Count predecessors in stats/windows. | Fixed; full mobile suite passes |
| DME-039 | At-capacity duplicate delivery releases the live original | Apply task/generation ownership checks before the capacity decision. Baseline sends an unstarted release for an executing task. | Fixed; full mobile suite passes |
| DME-040 | A new epoch can overlap a predecessor through execution/restart paths | Cancel the superseded owned future and release the replacement unstarted; retain independent epoch guards. Baseline enters both callbacks. | Fixed; full mobile suite passes |
| DME-041 | Surplus deliveries and concurrent recovery bypass cleanup backpressure | Share eight cleanup I/O permits across surplus and orphan recovery; retain pending-release fences and block further polling while cleanup is pending. Baselines observe 100 and 20 blocked requests. | Fixed; full mobile suite passes |
| DME-042 | A journal INSERT error is treated as proof a task never started | Preserve existing evidence and defer to recovery/expiry without an unstarted release. A real SQLite trigger failure reproduces the incorrect baseline release. | Fixed; regression and subsequent recovery pass |
| DME-043 | Repeated restart can overlap predecessor poll batches | Serialize polls across engine worker generations with cancellable waiting. A gated pending-poll test also requires the stopped predecessor's late response to be released without invoking a callback. | Fixed; full mobile suite passes |

Additional coverage requires pending release fencing after callback capacity
returns, preservation and recovery of an already known outcome at capacity,
capacity cleanup when a task future is dropped before first poll, and budget
shrink/expand without cancelling existing work. All 317 mobile library tests pass.
All-target Clippy passes across engine/server/mobile with warnings denied.
Engine and gRPC sources are unchanged from the prior verified 2,031/146-test batch.

Limits: admission belongs to one engine object, not every engine or process using
the same database. The concurrency limit covers owned lease executions; physically
running native calls whose leases expired remain separately quarantined by
handler. Cleanup RPCs are bounded, while pending futures belong to a transport
response batch bounded by the existing 8 MiB response limit. A replacement may
wait for the predecessor's already-issued poll to finish; this does not claim
immediate cancellation of slow HTTP calls. At this stage, recovery response
classification and bulk journal reads still needed review; the next section
records their fixes. Principal changes behind opaque token providers,
independent-engine ownership, and the remaining mobile/embedded/binding/sandbox
areas remain open.


## Recovery authority, retained evidence, and scoped reads

Baseline: PR #266 `5aacd57f`. Three rejected-response regressions fail against
that production code: recovery clears four evidence states on rejection,
pre-contract release fallback clears a started claim on rejected fail, and live
result delivery clears its known outcome. The corrected matrix covers 400, 401,
403, 405, 422, 429, and 503, plus restoration to accepted responses without native
reinvocation. Explicit 404/409/410 fencing still clears resolved claims according
to the existing lease protocol.

Three further regressions reproduce deletion of old claims without a server
request, conversion of malformed outcomes into releases, and recovery of one
runtime being blocked by a different runtime's invalid UTF-8 outcome.
A real tonic server using production node-role authorization also returns four
permission denials that the old client misclassifies as ownership loss.
A native handler continues for its full two seconds after renewal is denied;
the corrected supervisor cancels its owned future promptly, observes quarantine,
and retains started evidence when the release is also denied.

| ID | Finding | Correction and evidence | State |
|---|---|---|---|
| DME-044 | Rejected recovery responses erase durable start/outcome evidence | Clear only accepted settlements or explicit ownership-loss replies; preserve all other records for correction/retry. | Fixed; status matrix and recovery pass |
| DME-045 | Rejected legacy fail fallback is treated as an acknowledged release | Apply the same authority criterion to fallback, retaining started evidence and its unknown-effect semantics. | Fixed; fallback/restoration cases pass |
| DME-046 | Live result rejection deletes the already performed outcome | Retain the journaled result for recovery instead of reporting settlement success. | Fixed; one native invocation across rejection/recovery |
| DME-047 | Seven-day age is treated as proof the server reaped a claim | Remove unconditional age-based deletion. Old attempts are reconciled with server authority like other records. | Fixed; old unstarted, started, and known-result records survive outage and later resolve |
| DME-048 | Malformed/future outcome encodings become absence of outcome | Return a generic decoding error, retain original bytes, and continue recovering healthy rows. Do not log stored content. | Fixed; malformed/future encodings and diagnostic secrecy checks pass |
| DME-049 | Recovery aggregates every identity's payloads before filtering | Query matching API/worker identity keys in pages of 64 using `(task_id, claim_epoch)` cursors; load one reserved payload under the shared cleanup quota. | Fixed; foreign corruption isolation, deletion-safe 130-epoch paging, and invalid-key page progress pass |
| DME-050 | gRPC permission denial falsely ends the task lease | Classify permission denial as rejection, matching HTTP authorization semantics. Explicit lease-fence statuses keep their existing mapping. | Fixed; production role-layer integration verifies heartbeat/complete/fail/release |
| DME-051 | Denied renewal leaves owned native work running | Return unconfirmed authority on rejected/unsupported renewal, rather than wait for expiry or assert ownership loss. Drop owned work, quarantine the native call, and retain recovery evidence until resolved. | Fixed; denied-native-renewal regression passes |

Final library validation passes 2,031 lean engine tests, 327 mobile tests, and
147 gRPC tests. The full main CI and Mobile workflows also passed on `f2d88364`
when rechecked; hosted checks for this follow-up are separate and still pending.

Paging bounds identity aggregation and concurrent decoded recovery payloads; it
does not bound the duration of a full recovery pass or the bytes in one native
result. Unresolved evidence can remain on disk until authority is restored;
explicit quota/backpressure, recovery scheduling/fairness, and retention policy
still need review. Malformed records require repair or compatible decoding and
are not silently discarded. This batch adds no journal schema change.
Opaque-provider principal changes, independent-engine ownership, durable core
outcomes, ambiguous core release replies, typed fencing versus generic gRPC
precondition errors, and all remaining control/delegation/binding/sandbox areas
remain part of the full objective. No exactly-once or external rollback guarantee
is claimed.

All-target Clippy also passes for the final engine/server/mobile default-feature
union, with warnings denied.


## Provider installation, refresh, and retry ownership

Baseline: `e5834e61` (the stable head of PR #266). Work in this section uses a
separate branch so the earlier hosted checks can finish. Five credential
regressions fail against that baseline: empty-provider bootstrap retention,
late refresh publication, delayed-401 replay through a replacement provider,
negative-cache inheritance with identical token bytes, and older installation
callbacks overwriting newer installations. A second controlled HTTP test
reproduces the replay through the shared lease client. AuthHeaders Debug also
exposes header values and its generation in a baseline regression.

Credential state is now one atomic snapshot of provider owner, token revision,
token, cancellation, and negative cache. Every installation reserves a new owner
before host callbacks and clears its predecessor's token, including an empty
replacement. Callback results publish only while their owner and reservation
remain current. An initial-token callback cannot overwrite a refresh that began
later. Refresh requests and queued waiters are cancellable; single-flight mutexes
belong to installations, so an unpolled old future cannot hold up its successor.

| ID | Finding | Correction and evidence | State |
|---|---|---|---|
| DME-052 | A late refresh overwrites replacement credentials | Fence callback publication and cancel old waits on installation replacement. | Fixed; gated callback and late-publication checks pass |
| DME-053 | An empty replacement retains an old bootstrap/operator key | Clear old credentials atomically when reserving the new owner. | Fixed; empty-provider regression passes |
| DME-054 | A delayed 401 replays an old payload under replacement authorization | Keep request-generation ownership through refresh and choose retry headers atomically within that owner. Both direct and shared HTTP paths are covered. | Fixed; real HTTP captures contain only the original credential |
| DME-055 | A new provider inherits an old provider's negative cache when token bytes match | Scope failure/backoff to an owner/revision; reset on installation. | Fixed; replacement gets its own successful refresh |
| DME-056 | Concurrent initial-token callbacks publish in completion order | Reserve installation ownership before calling host code; ignore superseded completions. | Fixed; slower older installation cannot replace the newer one |
| DME-057 | A suspended old refresh holds a successor's single-flight lock | Allocate locks by installation rather than globally, independently of old future polling. | Fixed; retained, unpolled predecessor regression passes |
| DME-058 | Timeout or A-B-A reinstallation allows overlapping physical refresh callbacks | Retain physical admission until the blocking closure returns; share the guard for a retained Rust provider instance using weak identity records. Abort queued callbacks on cancellation. | Fixed at the Rust callback-handle boundary; timeout, reinstallation, queue cancellation, and subsequent recovery pass |
| DME-059 | Auth-header diagnostics expose credentials and raw generations | Custom Debug prints header names and redacts values/generation. Node generations contain only opaque owner/revision identifiers. | Fixed; diagnostic regression passes |
| DME-060 | Foreign-provider destruction can re-enter a held credential lock | Move retired provider references outside the lock; compare weak identities without upgrading/dropping foreign strong references under the lock. | Lock-lifetime regression reproduced during implementation and fixed |

The retained global refresh-lock design, physical timeout overlap, A-B-A guard
bypass, and locked retirement were each reproduced during implementation before
the corresponding corrections. Extensive tests live in `credential_tests.rs`,
separate from the state machine. Positive controls preserve same-provider
single-flight, backoff, timeout, and one-retry behavior, with identical lease
payloads across authorized same-provider rotation. Another HTTP test models a
replacement after refresh's last check and requires retry dispatch to honor the
ownership veto.

Local validation passes 2,033 lean engine tests, all 339 mobile tests, all 147
gRPC tests, and all-target Clippy across engine/server/mobile with warnings
denied. No exported native method signatures or journal schema change in this
batch. The shared Rust RefreshingAuth trait gains a provided retry-header hook;
existing single-owner implementations keep their behavior, and replaceable
implementations must provide an atomic ownership check/snapshot.

Limits remain material: installation owners are not authenticated tenant/customer
identities. This does not validate opaque refreshed tokens, bind every ongoing
business operation or durable journal record to a principal, cancel mutations
already accepted by a server, or discard stale successful responses. Tokens from
one provider still need to represent its intended authenticated principal.
The physical registry recognizes retained Rust callback instances; generated
UniFFI wrappers may give the same foreign object distinct handles.

At the DME060 checkpoint, Swift `RuntimeNode.swift` started an unretained
Task.detached for token fetch and its timeout did not cancel that task (fixed
in DME061 below). A Rust
callback can return while that nested fetch still runs. Generated Swift lowering
also allocates a new callback handle on each installation. Reproduce nested-fetch
cancellation/overlap and inspect Kotlin/RN/Flutter equivalents before claiming
end-to-end foreign single-flight. TokenProvider's old callback doc comments also
need contract/metadata alignment in the binding review. Principal lifecycle,
independent engines, core durable outcomes, recovery quotas/scheduling, and the
remaining original areas stay open.


### DME061 — Swift nested token-fetch lifecycle (fixed)

The synchronous Swift provider returned on timeout without cancelling its
detached async fetch, allowing another refresh to overlap the unfinished host
operation. Regression tests reproduced overlap and missing cancellation. The
adapter now retains per-instance admission until physical fetch completion,
cancels cooperative tasks at timeout, rejects late token publication, and joins
concurrent active refreshes. Detached tasks retain the flight rather than the
provider, allowing retired providers to deallocate. Invalid/non-finite and
unrepresentable timeout values fail before host invocation.

All six repository adapter tests pass with strict concurrency and warnings
denied in a local SwiftPM harness using the exact production adapter and tests
with TokenProvider/MobileError FFI shape stubs. This is adapter evidence, not
full native integration. The complete Swift package also passes all 21 tests on
an iOS 26.3 simulator using PR #266's CI-built native framework (the adapter does
not change exported native signatures). Hosted CI validates the final head.
Swift initial async provider installation ordering and Kotlin/RN/Flutter nested
refresh ownership remain pending; this does not establish cross-instance or
principal ownership. The broader review remains in progress.


### DME062 — Credential readiness before dispatch (fixed in next batch)

**HIGH, effort M.** Both `Credential::send` and the shared lease client could
send an empty or whitespace-only credential while an installed provider had no usable initial token,
including while `current_token` was blocked. A control plane accepting anonymous
requests could accept a private sync payload or lease mutation. Real HTTP
regressions reproduced a successful anonymous direct request and an Accepted
lease mutation before the correction.

Credential preparation now obtains an owned snapshot before invoking the request
body builder. An empty installed provider gets one bounded, single-flight initial
refresh under that owner; failed or superseded initialization rejects locally.
A separate red regression reproduced blank refreshed tokens being accepted;
readiness and refresh publication now both reject whitespace-only tokens.
The request never adopts a replacement owner's authorization while waiting.
Shared lease operations classify unavailable authorization as Rejected, retaining
uncertain claim evidence rather than treating it as ownership loss. Successful
initial refreshes and valid replacements continue normally. Explicit legacy
configuration without a token provider keeps its existing behavior.

The Rust transport exposes `LeaseRequestError::{AuthorizationUnavailable,
Transport}` from `HttpLeaseClient::{send,post_json,get}`. Rust callers explicitly
using `reqwest::Result` must migrate their return type or map the variants.
Default Display/Debug redact transport URL secrets. `RefreshingAuth` has provided
readiness/preparation hooks; existing implementations retain their behavior.
Native exported method signatures and binding metadata are unchanged.

The readiness CI run exposed an existing retry-lifecycle test that sampled the
transient Scheduled state after reporting failure; the scheduler had already
advanced to Waiting. The repaired case observes a new claimable task with an
incremented attempt and fresh effect id, rejects completion by the old lease,
and requires the successful retry's final output. All seven lifecycle cases
pass against SQLite and isolated PostgreSQL 16.14; E2E TypeScript checking passes.

### DME063 — Initial publication after refresh failure (fixed in next batch)

**HIGH, effort S.** Starting refresh permanently disabled an older in-progress
`current_token` callback, even when refresh failed and the initial callback later
returned a usable token. Readiness tests exposed the stranded installation. Only
successful refresh publication now supersedes initial publication. The per-owner
refresh mutex permits a successful refresh to take precedence if the initial
callback publishes while it is running; failed refresh does not transfer negative
cache state onto that newly published revision. Controlled callback gates cover
both completion orders and successful/failed refresh outcomes.

### DME064 — Wrapper installation before the native fence (partially fixed)

**HIGH, effort L.** Swift `RuntimeNode.setTokenProvider`, Kotlin
`Orch8Engine.setTokenProvider`, RN `core.ts`, and Flutter `orch8_flutter.dart`
await their first host fetch before native installation. Native owner fencing
cannot prevent those first fetches completing in reverse order and installing
an older provider last. First-fetch timeout/cancellation is also not consistently
bounded. RN/Flutter refresh callbacks consult mutable current fetchers without
an immutable provider identity in their bridge events.

The next integration must reserve a native owner before the first host await,
initialize only that reserved owner, and reject superseded or cancelled attempts
across wrapper aliases of one engine. Native bridges must carry immutable
provider/request identities; shutdown must retire pending installations and
refresh responses. Reuse the bounded native refresh and readiness path rather
than replay an old operation under whichever provider is current. Test reverse
initial completion, sync-vs-async replacement, same-provider rotation, held old
callbacks, cancellation before host invocation, shutdown/reinitialize, invalid
timeouts, and engine aliases in every supported wrapper. Include provider closures retaining
their engine in shutdown/retain-cycle tests, and verify destruction of the last
engine reference from a callback thread.

DME070 adds native reservation tickets and ports Swift; DME071 ports KMP. RN and
Flutter still require ticket integration and immutable provider identities in their
bridge events. The complete cross-wrapper requirement remains open.

DME062/063 are prerequisites, not a resolution of DME064. Principal binding,
independent engines/processes, core durable outcomes, recovery quotas, and the
remaining original review matrix still require work.

Readiness-batch validation: 2,034 lean engine tests, 347 mobile tests, 147 gRPC
tests, all-target Clippy for engine/server/mobile with warnings denied, and the
documentation checker pass locally. Eight simultaneous direct/shared initial
requests share one host refresh. Hosted full-workspace/native CI is the merge
gate for this batch.

### DME065 — Physical refresh budget across replaced providers (native fix)

**HIGH, effort M, pre-existing.** Physical refresh admission is per retained
provider instance. A different provider gets an independent AtomicBool, and
`physical_owners` keeps weak records live while timed-out blocking closures hold
the provider and running guard. Repeated replacement with non-cooperative host
callbacks can therefore accumulate physical calls/queued closures and identity
records without a credential-wide cap. Timeout bounds the waiter, not the host
callback's lifetime.

The shared credential now admits at most two unfinished Rust blocking refresh
calls across all owners. The minimum production blocking pool has four threads;
a separate baseline using the initial eight-call limit reproduced starvation of
other blocking work. The two-call cap leaves headroom, verified on the actual
minimum mobile runtime. A failed try-acquire rejects immediately without
queuing foreign work or recording refresh backoff. The permit remains owned by
the blocking closure through timeout/cancellation, and through provider cleanup.
The guard stores its provider before its permit so foreign destruction finishes
before capacity returns, using Rust's defined field destruction order
([Rust Reference](https://doc.rust-lang.org/reference/destructors.html)). No
foreign destruction occurs under a credential lock.

Two baseline regressions reproduced 128 additional admitted refreshes despite
eight retired calls, and admission while eight provider destructors remained
blocked. Controlled gates now cover both owner cancellation and expired waits,
128 refused owner rotations with bounded identity metadata, recovery after one
slot returns, callback/destructor panics, and timeout before queued invocation.
Running-call timeout tests freeze Tokio time after confirmed host entry;
the queued timeout test freezes it after confirmed slot reservation. Cached
valid credentials still send direct requests and heartbeats while all refresh
slots are held.
All 352 mobile tests and all-target engine/server/mobile Clippy pass locally;
the focused lifecycle suite also covers cached-token progress and production
executor headroom. Native exported
signatures and metadata are unchanged. Full hosted CI remains a merge gate.

This cap covers Rust blocking closures, not detached work outliving them.
Independent engine/process budgets remain a separate original-scope requirement.
See DME066 for the remaining asynchronous wrapper boundary.


### DME066 — Nested async fetches across wrapper instances (partially fixed)

**HIGH, effort M/L.** At the review baseline, the Swift adapter's per-instance flight prevented a second
fetch until its first actually ends, but timeout can return from the native
callback while a non-cooperative detached fetch continues. A retired provider
may deallocate while that task remains (an existing regression explicitly
verifies this). Fresh provider instances therefore need a shared async-work
budget in addition to the native blocking-call cap. RN and Flutter await host
fetch promises/futures after native callers may already have timed out; their
refresh handlers also lack a shared physical admission bound.

Bound SDK-owned async fetches across provider instances, retain admission until
actual completion despite timeout/retirement, reject exhausted admission without
creating or queuing more host work, and recover when prior work finishes. Apply
the rule to first fetch as well as refresh. Preserve DME064's immutable provider
and engine-lifecycle ownership; cancellation must not publish stale results.
Test retired non-cooperative instances, repeated replacement, shared admission,
late completion, cancellation, and capacity recovery in the real wrappers.
Kotlin's structured timeout needs separate verification of completion semantics;
this report does not claim an async-work budget there.

The Swift shared initial/refresh budget is addressed in DME069, and KMP physical
completion/admission in DME071. RN/Flutter bounds and native installation
ownership in those wrappers remain open.

### DME067 — Runtime destruction from engine-owned execution (native fix)

**HIGH, effort S.** `MobileRuntime` used default `Runtime` destruction. Releasing
the last owner from its own asynchronous task panicked because Tokio cannot
block there; releasing it from its own blocking callback waited for that same
callback indefinitely. Foreground destruction also waited behind an unrelated
non-cooperative host callback. All three controlled regressions failed on main
`a31cadd0`: async panic and two bounded-observation timeouts.

The runtime now has one explicit owner and uses `shutdown_background()` during
destruction. No unused five-second joining API remains. Tokio signals scheduler
shutdown without waiting for blocking threads. Tests cover all three original
failures, destruction of the actual last `MobileEngine` reference on both
executors, and host-owned resources remaining alive until physical callback
completion. This changes no exported FFI signatures or protocol fields.

This resolves executor destruction only. The separate native shutdown ownership,
admission, and queued-listener boundaries are addressed in DME068 below. Wrapper
installation retirement (DME064) and cross-instance async budgets (DME066) remain
open.

Local validation: all 357 mobile library tests, mobile all-target Clippy with
warnings denied, formatting, documentation links/example syntax, and diff checks
pass. Hosted full-workspace/native CI is required before merging this batch.

### DME068 — Terminal shutdown and foreign callback ownership (native fix)

**HIGH, effort M.** Explicit shutdown left native providers, registered handler
adapters/metadata, and listeners installed. A callback retaining its engine formed
an idle cycle. Storage operations still ran, a new provider's getter was invoked,
and an initial getter finishing after shutdown published credentials. All four
baseline regressions failed on the runtime-retirement parent `a36cdad7`.

Shutdown now cancels operation authority first, closes the shared credential and
listener registrations, clears registry ownership, and stops background loops.
Credential closure invalidates owner/revision, clears cached tokens, cancels
refresh waits, and permanently rejects installation and anonymous authorization.
Existing node clients share that fence. All returned foreign owners are dropped
after locks have been released. Already-running callbacks retain their resources
until physical completion; completion cannot republish retired authorization.

The common operation bridge rejects unpolled work after closure and cancels
admitted waits. Worker, delegation, and node installation recheck cancellation
under their installation locks. Resume cannot restart a closed engine. Listener
registration uses a short synchronous lock, avoiding foreign destruction inside
a nested `block_on`; queued terminal/pending invocations check closure before
host entry. An owning listener-call guard retains physical capacity through
foreign destruction, including unwinding.

Registry updates are transactional under the registry gate: rejected replacement
preserves both the old handler and its advertised name. Old registry and callback
owners are dropped outside locks. Re-enabling an existing builtin remains
idempotent for an open engine with a shared registry.

Behavioral coverage includes provider/handler/listener retain cycles, repeated
shutdown, post-close registration and restarts, initial callback races, in-flight
refresh cancellation with physical ownership retained, direct/shared HTTP
authorization without payload building or network traffic, operation polling and
cancellation, foreign destruction reentry on replacement and shutdown, failed
handler replacement, and queued listener cancellation with a saturated executor.
No FFI signatures or journal schemas change. Closing authorization may prevent
an immediate remote settlement; existing uncertain/started evidence remains
journaled for later recovery rather than being erased.

This native batch does not close the wrapper first-fetch races or async budgets
(DME064/066), durable principal binding, independent-engine/process coordination,
or the remaining original distributed/embedded/security review matrix.

Validation: all 369 mobile library tests pass, including twelve added behavioral
tests. Engine/server/mobile all-target Clippy passes with warnings denied;
formatting, documentation links/examples, and diff checks pass. Hosted
full-workspace/native CI remains the merge gate for this batch.

### DME069 — Swift physical async fetch budget and initial waiting (Swift fix)

**HIGH, effort M.** The per-adapter flight did not bound detached fetches left
running after adapter retirement. A regression filled eight timed-out retired
providers and showed that a ninth replacement invoked more host work. The
wrapper's initial `await fetch()` also bypassed timeout validation, cancellation
handling, and any physical-work budget.

Initial fetches and refreshes now acquire one of eight process-wide Swift SDK
permits before spawning host work. Swift admission fails without a queue when
exhausted. The detached task retains its permit through actual
fetch completion despite caller timeout, cancellation, or adapter retirement.
Unrelated engines share this process resource ceiling; it is an availability
limit, not an authorization identity. Rust's per-credential native blocking-call
budget remains separate and still applies.

The initial fetch uses the same flight and deadline validation as refresh, with
an asynchronous continuation and cancellable dispatch-source timer instead of
blocking a Swift executor thread or waiting for a non-cooperative child task.
Completion, timeout, and cancellation arbitrate one result under a short lock;
continuations resume outside the lock. Deadline sources are cancelled when the
wait completes, and their handlers hold only weak references. Cancelled callers
are checked again before native installation. Blank/whitespace results do not
populate the cache; late completion cannot publish after abandonment. No FFI
signature changes.

Twelve adapter tests pass in the local strict-concurrency, warnings-as-errors
Swift harness, including shared admission across retired instances and 128
refused replacements, capacity recovery, first-fetch timeout and cancellation,
pre-cancelled callers, invalid deadlines, blank results, and late publication.
They also cover host failure cleanup and 64 completion/cancellation races. The
Rust credential still applies refresh-failure backoff to a failed foreign call;
immediate native retry recovery after Swift quota refusal is not proven by these
adapter tests and needs integration review.
The harness uses the exact production adapter and repository tests with FFI
protocol/error shape stubs; it does not prove full SDK/FFI integration. Hosted
iOS simulator tests and the full native workflows remain required merge gates.

DME064 remains open: first fetch still precedes native owner reservation, so
concurrent installations need native generation tickets across wrapper aliases.
RN/Flutter shared async bounds and Kotlin structured completion still require
work. This Swift batch does not complete DME066 or the original review matrix.

### DME070 — Native provider reservations and Swift installation ordering (partial fix)

**HIGH, effort L across wrappers.** Real Swift-to-Rust regressions reproduced
older async completion replacing a newer async installation, replacing a newer
synchronous provider through another alias of the same engine, and reporting
successful installation after shutdown. The synchronous case also proved that
the newer native callback was released by the stale completion.

The native engine now reserves an opaque installation ticket before any initial
fetch. Reservation installs the provider without invoking its synchronous getter,
clears old credentials, and establishes one owner shared by every wrapper alias.
Only the current ticket can complete. Duplicate completion cannot roll back an
already-published refresh. Cancelling a ticket retires only its owner, including
a just-completed owner when caller cancellation races completion; stale tickets
cannot revoke successors. Failed/cancelled reservation never enables anonymous
fallback. Shutdown permanently rejects reservation and completion.

Swift reserves before fetching, checks caller cancellation around completion,
and retires its own ticket on failure/cancellation. Its initial/refresh shared
flight remains bounded by DME069. Completion after supersession or shutdown
throws instead of silently reinstalling old authorization. Native tests cover
ticket identity, malformed/blank completion, cross-engine isolation, replay,
refresh-vs-initial ordering, cancellation and recovery. Real FFI tests cover
async/sync replacement through aliases, async replacement, shutdown, cancellation
that must preserve a successor, and closed-engine refusal before host invocation.

Validation: 375 mobile tests and engine/server/mobile all-target Clippy pass with
warnings denied. All 32 production Swift SDK tests pass against freshly generated
bindings and the actual macOS Rust library, including five new installation
tests. The three baseline FFI cases failed before the fix. Swift and Kotlin
binding generation succeeds; committed Swift bindings match generated output
after whitespace normalization. macOS FFI tests do not prove iOS simulator or
Android integration; hosted native/SDK checks remain required.

**Release requirement remains open.** These three additive FFI entry points
require matching generated headers and native artifacts. `Package.swift` still
pins the published 0.7.1 XCFramework by default. The new source must be paired
with a newly built artifact via `ORCH8_MOBILE_LOCAL_XCFRAMEWORK=1` until a matching
version/checksum is released and the default package pin is updated. Keep this
batch out of the supported default SDK release until that contract is resolved;
the generated-header CI path alone cannot prove default-package compatibility.

RN/Flutter first fetches still precede native reservation and their immutable
bridge provider identities remain unfinished; KMP is addressed in DME071. This batch does not complete
DME064, principal binding, SDK release parity, or the original review matrix.

### DME071 — KMP installation ownership, physical fetches, and Apple bridge (implemented)

**HIGH, effort L.** The KMP alias regression failed on the original setter: a
blocked initial fetch installed its older source after a newer alias completed.
The iOS node adapter also read one mutable `nodeTokens` field for every provider,
allowing a retired callback to invoke the replacement fetcher. A synchronous
`withTimeout` around structured host work could not by itself bound a wait on
non-cooperative child work; initial fetching bypassed it entirely.

KMP now uses native begin/complete/cancel tickets. The ticket is captured inside
a non-cancellable dispatcher bridge, so prompt cancellation cannot lose an owner
that native code just committed. Failure/cancellation retires only that ticket;
cleanup failures are suppressed onto the original error rather than replacing
cancellation. Finite positive timeout validation runs before reservation.

Initial and refresh fetches share a source flight and eight shared physical
permits. An SDK-owned child job retains admission through actual completion,
including non-cooperative cancellation and a queued job cancelled before entry.
Waiters use the flight's remaining deadline. A reproduced premature-publication regression now requires the full structured
host scope (children and completion handlers) to finish before publication.
Cache publication checks ownership and monotonic elapsed time; cache closure/publication are atomic. Initial calls
reuse an already bootstrapped token rather than minting another one. SDK-owned
coroutine exception handlers contain failing host cancellation handlers without
forwarding sensitive messages through native callbacks.

Bridge protocol 2 carries immutable node provider IDs. Each Swift node adapter
captures its callback object and ID, and releases only that source when native
ownership ends. The Kotlin registry is atomic, closed after shutdown, and drops
owned fetch jobs outside registry mutation. JVM/Android tracks SDK-owned sources
weakly and cancels live work after native shutdown without retaining retired
fetch closures. Manifest-sync callback identity is separate and still open in
DME072.

Actual Apple compilation exposed pre-existing type collisions between Swift SDK
records and Kotlin/Native exports (`PowerState`, `DelegationStatus`, and other
node/delegation records). Explicit module qualification resolves them. The new
Mobile CI job runs JVM tests against the same-commit host library, compiles the
real iOS simulator KMP framework, and type-checks the production Swift SDK and
bridge with warnings denied. Native library content is a JVM test-task input to
prevent stale cached integration results after Rust-only changes.

Validation: all 59 JVM tests pass, including 17 added cases and two actual
Kotlin/JNA-to-Rust integration tests (zero skipped with a native path). The
portable run passes 57 and explicitly skips the two native cases. Tests cover
alias replacement, cancellation during reserve/complete dispatcher returns,
successor-preserving cancellation, shutdown, finite deadlines, shared admission
with 128 refused first/refresh attempts, physical completion, queue cancellation,
host errors/cancellation-handler failures, immutable callbacks and failed bridge
reservation. The real iOS simulator Kotlin/Native framework compiles; the actual
Swift bridge type-checks against it and current SDK sources with warnings denied.
This is compile/protocol evidence, not an iOS bridge runtime E2E claim.

Matching native artifacts, generated bindings, and protocol-2 Swift bridge must
ship together; the DME070 default-release gate is still open. RN/Flutter ports,
principal binding, manifest-sync identity, and the original review matrix remain
unfinished.

### DME072 — KMP manifest-sync callback identity (fixed and merged)

**HIGH, effort M.** On main `fb473301`, `JsonBridgeBackend.sync` still wrote a
mutable `activeTokens`, and Swift's manifest adapter consulted the bridge's current
callbacks. A controlled nested bridge call reproduced account A's callback
returning account B's credential; the regression failed before the fix. Another
call's finally block could also clear the first call's source.

Manifest sync now registers a unique, immutable ID in an atomic registry separate
from node credentials. Swift captures both the callback object and ID. Return or
failure removes only that ID, and shutdown closes admission and retires all IDs
before native cleanup. Retired IDs cannot invoke the source or resolve a later
installation. IDs share a non-reused monotonic counter across source kinds;
manifest and node lookups remain separate. Sources supplied to sync are
caller-owned and are not closed on retirement, including a source shared with a
node. Anonymous calls do not acquire a source ID. Protocol 3 rejects stale
protocol-1/2 Swift bridges at open; the matching KMP framework/Swift adapter must
ship together. This does not resolve the existing native artifact release gate.

Coverage includes controlled nested overlap, a real two-thread bridge overlap,
failure and late callbacks, shutdown during sync, anonymous overlap, separate
node/manifest authority, non-reused IDs, and caller-owned source lifetime.
All 65 JVM tests pass with the actual Rust library and no skipped cases (six
added regressions). The portable run passes 63 cases and explicitly skips the
two native tests. The real iOS simulator Kotlin/Native framework links, and the
production Swift SDK and bridge type-check against it with warnings denied.
Documentation links, example syntax, and diff checks pass. PR #275 passed all 24 hosted checks and merged as `de8845e6`. The JVM bridge fixtures exercise production Kotlin registry behavior; Apple
compilation checks the actual Swift/Kotlin callback surface, not iOS runtime E2E.

### DME073 — Parent wake after handler-consumed cancellation (fixed and merged)

**HIGH, effort M.** Main `fb473301` contains the repair, with the complete CI and
Mobile workflows passing. The original draft #273 E2E failure was reproduced by
waiting for the child sleep to be Running before cancellation. Cancellation
consumed inside the sleep invalidated its claim; evaluation could exit before
scheduler terminal handling. Both already-terminal scheduler return paths also
skipped parent notification. This was distinct from the older parent-parking
race, whose park-and-recheck mechanism already existed.

Cancellation now notifies the parent at signal consumption, and both scheduler
terminal return paths also notify without overwriting terminal child state.
Direct signal and terminal-state matrix regressions pass; the strengthened
running-child E2E case failed before the fix and passes on SQLite and isolated
Postgres. The lean engine suite passed all 2,036 tests, and Clippy passed with
warnings denied. The combined PR #274 passed all 24 hosted checks; merging it
also included and marked #273 merged. The separate Swift startup-sensitive tests
were changed to synchronize on physical callback entry and then cancel logical
waiters; dedicated timeout tests remain intact. Both final Swift simulator runs
passed, as did the two main workflows after merge.

### DME074 — Manifest token getter blocked timeout and shutdown (fixed and merged)

**HIGH, effort M.** On `4026bae3`, `MobileEngine.sync` invoked the foreign
`TokenProvider.current_token()` directly while polling `run_with_timeout`.
Controlled blocking providers reproduced both failures: neither timeout nor
terminal shutdown returned before the host gate was released. The failing
baseline tests always released their physical gates before asserting.

Manifest getters now run on the blocking executor. A per-call wait guard marks
abandonment before aborting queued work; a queued closure checks cancellation
before host entry, and running callbacks retain their provider and physical
permit until actual completion, including foreign provider destruction. Logical
retirement drops the continuation that could dispatch the manifest request.
Late token results cannot publish into node credentials or resume network work.
Empty/whitespace tokens fail with InvalidInput; a callback panic maps to a generic
engine error without forwarding its payload. Anonymous URL-token sync remains
available only when the caller supplies no provider.

Manifest getters and node refreshes share two physical token callback permits per
engine, preserving blocking-pool headroom (the smallest valid mobile runtime has
five blocking threads). Exhaustion does not queue host work: sync returns
ResourceLimit and node refresh is refused without invoking the host. The pool
stays closed after shutdown. Manifest sources remain per-call and independent
of node credential installation/refresh generations. No FFI signature changes.

All 386 mobile tests pass, including 11 added cases: timeout/shutdown with a held
getter, 128 refused replacements, shared admission in both directions, foreign
destruction, queued timeout and shutdown, panic mapping, blank results, and
retirement of the last engine owner while a host callback retains its resources.
Real TLS connection probes show no late transport dispatch after timeout or
shutdown and no dispatch for blank tokens; a usable-token positive control
reaches the same listener. No external service is needed by these probes.
Clippy passes across all mobile targets with warnings denied; documentation
links/example syntax and diff checks pass. PR #276 passed all 24 hosted checks and merged as `9da12688`.

The native manifest path still snapshots one bearer token and does not perform
401 refresh. This repair does not establish principal-bound manifest/cache
ownership, finish RN/Flutter adoption, resolve native release artifacts, or
complete the original review matrix.

### DME075 — Bearer scope across manifest entry origins (fixed in source)

**HIGH, effort M.** On `769ac74a`, a fully signed manifest with valid sequence
hashes/signatures reproduced the app's bearer token reaching both a separate
sequence server and its detached-signature endpoint. Absolute and protocol-relative
entry URLs were accepted, and the same raw SyncAuth was passed to every download.
Content authenticity does not authorize another service to receive app credentials.

Bind the provided bearer to the parsed manifest origin once per sync. Private
request helpers require this scoped authority, and the shared HTTP GET path
compares normalized URL origins before writing Authorization on every attempt.
The request helper accepts only its needed conditional ETag header; no generic
extra-header bag can override the auth decision. Same-origin entries and
signatures retain authorization. Other origins can still serve valid signed
public content or entry-specific signed URLs without receiving the manifest
bearer. Existing redirect refusal remains active for manifest, sequence, and
signature requests. No public SDK or FFI signature changes.

Ten new tests cover controlled signed cross-origin sequence/signature transport,
same-origin relative/absolute/protocol-relative URLs, per-entry query tokens,
503 retry, all three redirect stages, different URL hosts resolved to one
physical server, invalid signatures, normalized HTTPS origin cases, invalid
scope diagnostics, and URL redaction (DME076). All 396 mobile tests pass;
Clippy passes for all mobile targets with warnings denied. The network fixtures
use production request/auth code and HTTP loopback servers with the existing
test-only URL policy exception. HTTPS origin normalization is checked separately;
these are not live cross-origin TLS certificate/handshake tests. Hosted
validation remains required.

### DME076 — Sync retry diagnostics retained URL credential components (fixed in source)

**MEDIUM, effort S.** The local redactor removed only the query portion. A
regression reproduced retained userinfo and fragments, including a fragment-only
URL without a query. Delegate to the established shared outbound URL redactor,
which removes userinfo, query, and fragment and handles malformed input.
The regression failed before the correction and passes with the full mobile
suite. Scoped-auth parse failures use generic errors rather than echoing the
provider token or signed manifest URL.

The broader sync review remains unfinished: the ETag, environment fingerprint,
and replay cursor are stored globally in the engine's sync metadata. Principal
and catalog ownership, overlapping reconciliation, stable refresh identity,
manifest 401 refresh, RN/Flutter adoption, artifact parity, and the original
review matrix still need authoritative evidence and corrections where required.
Origin-bound transport authorization does not establish durable data ownership.

### DME077 — Overlapping sync overwrote a newer replay fence (fixed in source)

**HIGH, effort M.** On `5ceb8ca0`, a controlled sequence-download gate allowed
an older signed manifest to overlap a newer signed removal. The older run
committed replay version 1 after version 2 had completed. The regression failed
before the correction and now verifies the version-2 fence and workflow removal.

A configured orchestrator and its snapshots share one bounded sync admission
permit. Admission precedes token authorization; concurrent callers receive
ResourceLimit before invoking the host or starting HTTP. Each run executes in an
owned task with its own cancellation token and shared HTTP/storage resources.
Retiring the caller cancels network waits and future phases while the task keeps
admission through an already-dispatched storage operation. Checkpoints prevent
additional key, reconciliation, installation, deprecation, pruning or eviction
mutations after retirement. Completed/failed runs release admission for retry.
This prevents a cancelled waiter from handing ownership to another sync while
its database write is still in flight. No SDK/FFI signature changes.

Coverage includes the stale-writer/removal reproduction, native rejection before
a second host getter, 128 rejected authorization attempts, already-retired run
admission, cancellation during download, and cancellation inside a real WAL
transaction with a dedicated pre-opened reader. The WAL test proves readers see
the old snapshot, another sync remains refused until commit, and the new snapshot
is complete after the original caller is cancelled. All 404 mobile tests pass;
Clippy passes on all mobile targets with warnings denied. The final fixture
refactor passes all seven focused reconciliation tests. Hosted checks remain
required. This is one configured engine's admission/ownership foundation;
persistent epochs across engine/process destruction and scope-specific fairness
remain gates in ADR-0002.

### DME078 — Failed sync persisted a mixed metadata snapshot (fixed in source)

**HIGH, effort M.** A SQLite trigger rejecting the replay-cursor write reproduced
partial persistence: the sync returned an error after changing its timestamp and
validator, leaving the previous cursor/environment. This could make a later
conditional fetch skip required evaluation.

The timestamp, ETag, replay version and evaluation fingerprint now commit in one
SQLite transaction. Removing an ETag from the successful representation retires
the old validator. An admitted transaction completes coherently if the caller
retires; its owned run keeps admission until the result settles. Injected-write
failure rolls back the complete snapshot. The baseline failure, success without
an ETag, and cancelled-caller transaction tests pass in the full mobile suite.
This does not scope the metadata to an account/catalog: S1-S4 and the complete
scoped-runtime contract still require implementation and verification.

### DME079 — Explicit persistent mobile owners (partial foundation)

**HIGH, effort L.** The user confirmed multiple accounts and catalogs must share
one database with explicit scopes. Legacy global cursors, keys and workflow data
do not enforce that ownership. ADR-0002 remains the full implementation contract.

The initial internal storage layer now creates immutable account/principal and
catalog/source/signing-authority bindings, UUID owner identities, durable active
session epochs, composite owner foreign keys, and atomic catalog metadata batches.
Retiring an account preserves its data; reopening advances the epoch without
changing IDs. Old handles cannot read/write or retire the newer session. Taking
SQLite's writer reservation before epoch validation orders admitted transactions
against retirement across independent pools. Source/principal mismatches and
epoch exhaustion fail closed with diagnostics that omit descriptor values.

Storage cases cover two accounts with two catalogs, identical metadata keys,
cross-owner handles/FK rejection, immutable bindings, independent-pool concurrent
opens, stale reads/writes after retirement/reopen, admitted-write retirement
ordering, injected batch rollback, invalid descriptors, epoch exhaustion, and
legacy-schema upgrade/full close/reopen without data adoption.
All 412 mobile library tests pass, including the eight new storage tests.
Clippy passes on all mobile targets with warnings denied; documentation, format
and whitespace checks pass. Hosted checks are required before merge.

This is not authentication, remote audience verification, workflow/key isolation,
or a public scoped engine. Existing sync/execution still uses legacy storage.
Credentials, running tasks/callbacks, workflow IDs, signing keys, journals,
workers, delegation, wrappers and all S1-S12 end-to-end gates remain open.

### DME080 — Failed key revocation restored authority through cache (fixed in source)

**HIGH, effort M.** An injected SQLite delete failure reproduced a signed
manifest with no signing keys still installing a sequence under the removed
key. Revocation errors were logged/ignored, then per-entry verification loaded
the surviving cached key. A second reproduction showed a malformed later key
leaving earlier new keys partially persisted even though sync returned an error.

Sync now decodes the entire root-authorized set and rejects empty/duplicate IDs
before any trust mutation. Replacement is one transaction; revocation or insert
failure aborts before reconciliation/download/install/cursor advancement and
retains the previous complete set. Per-entry verification uses only the current
manifest's read-only in-memory authorized keys and never recovers authority from
cache. Entries without a current authorized key are refused before body or
signature network requests.
No public SDK/FFI signature changes.

Five regressions cover failed revocation with a valid signed transport fixture,
invalid-later-key partial persistence, injected insert rollback, direct refusal
of an unlisted cached key, and empty/duplicate IDs. Internal scoped signing-key
storage also supports atomic owner/epoch-fenced replacement and single-snapshot
reads. Two storage cases exercise identical key IDs in four catalogs, isolated
rotation, stale/foreign/empty-set refusal, independent-pool reopen, duplicate
rejection and injected insert/delete rollback. This catalog persistence is not
yet connected to scoped sync or remote audience verification; workflow ownership
and the rest of ADR-0002 remain open.

The final 419-test mobile library suite passes with no failures. All-target
mobile Clippy passes with warnings denied; formatting, documentation and diff
checks pass. Hosted CI remains required before merge.

### DME081 — Partial sync admitted a rollback below already-applied revocation effects (fixed in source)

**HIGH, effort M.** A signed version-3 manifest removed an installed workflow and
rotated its key, then a download failure kept the applied cursor at version 1.
After fully closing/reopening the database, an authentic previously published
version-2 manifest passed that cursor and reinstalled the withdrawn workflow.
The regression reproduced the installation before correction.

Acceptance now has a durable version and SHA-256 digest of the original
root-verified body, committed atomically with the completely validated key set
before workflow effects. Its comparison takes SQLite's writer reservation first.
Evaluation retains a separate applied cursor so the identical accepted document
can retry after transient failures or cancellation. Older documents are refused
before key changes/downloads; changed content under an accepted version is
refused even after an environment change. Signature-line encoding is excluded
from the content identity. The publisher's generator already increases the
version on every generation. Legacy applied cursors remain the initial floor;
invalid/incomplete trust records fail closed.

Pending accepted work bypasses the old ETag. A 304 without an eligible applied
representation is an error, including an orphan ETag before initial sync.
Evaluation/progress commits recheck the exact accepted version/digest under a
writer reservation; they cannot replace acceptance identity or mislabel the
applied cursor. A queued acceptance from another pool compares against a newer
committed fence rather than an earlier read.

Internal catalog storage has matching owner/epoch-fenced acceptance and
evaluation transactions, isolated per catalog and durable through account
retirement/reopen. Generic metadata writes cannot modify reserved sync fields.
Coverage includes signed transport/reopen rollback, same-document retry and
conditional requests, same-version content changes, invalid signatures/keys,
injected digest-write rollback, legacy floor preservation, malformed state,
late evaluation, cross-pool ordering, four catalogs, reserved-field bypass,
stale/foreign handles and scoped pending work after reopen.

This is freshness/trust persistence. It does not fence all workflow effects
between independent engine/process lifetimes or implement scoped execution,
remote principal/audience proofs, workflow IDs, workers, callbacks or SDK handles.
Those ADR-0002 gates and the original review matrix remain open.

Validation: all 434 mobile library tests pass, including fifteen new freshness,
ownership and storage cases. All-target mobile Clippy passes with warnings
denied. All eight publisher manifest tests pass (including deterministic
canonicalization, monotonic generation and the persisted published-version
floor). Documentation, format and whitespace checks pass. Hosted CI remains
required before merge; no public SDK/FFI signature changed.

### DME082 — Owner-qualified workflow cache and retention (partial foundation)

**HIGH, effort L.** Explicit scopes must not share publisher-supplied workflow
IDs or name/version slots. Internal catalog installation now hashes the original
body, assigns a local engine ID/account tenant/catalog namespace, and records
immutable source ID/body identity in `mobile_catalog_workflows`. Legacy rows
remain separate. The remote sub-tenant/embed fields do not grant local ownership.

SQLite transaction adapters reuse the engine's definition encoding, row decoding
and guarded dependent-row deletion. The original backend read behavior remains
unchanged. Mapping/definition changes commit together after checking the account
epoch and exact current accepted manifest. Concurrent installs from independent
pools converge on one mapping/local ID. Active lookup by name or physical ID
checks ownership in one snapshot and refuses foreign or inconsistent rows.

Eviction deletes only the cached definition; compact published-version identity
and withdrawal evidence remain. Removal handles all cached versions atomically,
retains/deprecates definitions pinned by scheduled/running/waiting/paused
instances, and purges unpinned versions using normal engine cleanup. A failure
after an earlier version's purge rolls the whole removal back, including terminal
instance rows and mapping FK updates. The same withdrawing manifest cannot
resurrect a purged version. A newer accepted manifest can explicitly reauthorize
identical content while preserving an existing pinned definition/local ID.

Eleven storage/engine-row scenarios cover four catalogs sharing names and supplied
IDs, legacy coexistence, independent-pool idempotence, cross-owner reads, immutable
versions after eviction, source-ID collision rollback, injected mapping failure,
all nonterminal states, upgrades and explicit re-add, whole-removal rollback,
full close/reopen/session fencing, stale manifest writes, malformed/oversized
bodies and inconsistent stored owners.

All 445 mobile library and 225 storage library tests pass. All-target Clippy
passes for mobile and SQLite storage with warnings denied. The final partial
cached-version index passes all eleven focused workflow cases; query-plan
inspection against the canonical schema selects it for active name lookup and
cached-version removal. Documentation, formatting and diff checks pass. Hosted
checks remain required before merge; native/SDK signatures are unchanged.

This is not public scoped sync/execution. Remote signature/entry/audience checks
must be performed by the future scoped caller. Instance/handler/worker ownership,
sub-sequence and delegation references, account-wide cleanup/quotas, bindings,
native/platform runtime verification and mixed legacy/scoped operation gates
remain open under ADR-0002 and the full review matrix.

### DME083 — Volatile scheduler state crossed independent backend lifetimes (fixed in source)

**HIGH, effort M.** Two independent SQLite backends reproduced both failure
directions. A tick against a database without the parent consumed the other
backend's deferred wake, leaving its parent Waiting. With colliding instance IDs,
the wrong database's Waiting instance was instead moved to Scheduled. The
process-wide queue was keyed only by instance UUID. Transient failure counts
and clearing also used process-global UUID-only keys.

Both registries now qualify instance IDs by weak Arc allocation identity. The
weak key cannot retain a closed backend/connection/runtime or transfer its old
work to a replacement allocation. Arc clones share ownership; distinct storage
decorators do not drain one another's work, even when they reach the same DB.
Dead identities are pruned periodically or at admission pressure, avoiding a new
full-map scan on every ordinary enqueue/failure. Existing process-wide hard caps,
expiry and retry policies remain bounded.

Each wake deferral has an independent generation. A storage result finishing
after a newer deferral cannot remove that newer event. A failed initial wake is
queued by its owner, and a false compare-and-set retains the pending retry.
Scheduler/evaluator/built-in/step-failure signal paths retain the backend Arc
through explicit owned helpers. Existing borrowed signal APIs remain available
for immediate processing; new owned variants provide lifetime-bound deferral.
A borrow never registers process-global work without a lifetime owner.

Nine SQLite wake cases cover both reproductions, Arc clones, backend retirement,
decorators, initial write failure, a lost CAS race, a new deferral racing an old
attempt, and owned child cancellation. Two retry-budget tests cover colliding
UUIDs, one backend clearing another's failure history, exhaustion isolation and
retirement without retaining the backend.

These registries are still process-local best-effort mechanisms. They are not
authentication identities, persisted account epochs, durable restart wake
journals, or complete cache/tenant/process isolation. Restart recovery after
storage errors and the remaining original/ADR-0002 gates still require review.

### DME084 — Audit-persistence CI fixture assumed a timed flush (fixed in test)

**MEDIUM, effort S.** PR #281's E2E job failed because the persistence fixture
expected audit rows after a fixed 200 ms pause/resume delay. Instance state
publication precedes the best-effort audit write; observing Running did not
prove that a signal-driven audit row had persisted.

The fixture now completes a finite workflow and polls for persisted audit
evidence with a bounded monotonic deadline before restart. It verifies every
observed audit entry ID survives the actual stop/restart, replacing the previous
length check that allowed losing two entries. Startup health readiness replaces
another fixed sleep. The targeted case passes against the current rebuilt
server and isolated tmpfs Postgres; the owned server/container are cleaned up.
This verifies durability of observed records, not guaranteed auditing during
storage outages. TypeScript typecheck passes.

Validation for DME083/084: all 2,047 lean engine library tests and 445 mobile
library tests pass. All-target Clippy passes for both with warnings denied.
The current full server builds, and the targeted audit restart case passes with
isolated Postgres, no skipped tests and owned-resource cleanup. TypeScript,
documentation, formatting and whitespace checks pass. Hosted CI remains
required before merge; the full review and ADR gates remain open.

### DME085 — Shared sequence cache crossed backend boundaries (fixed)

**HIGH, effort M.** A public `SequenceCache` accepts a backend on each read but
keyed definitions only by sequence ID or tenant/namespace/name/version. Two
independent SQLite databases with colliding IDs or names reproduce reads of the
other database's definition. Both regression tests fail on the prior code.

Cached reads now require an explicit backend Arc through additive owned lookup
methods. Their keys include a weak backend lifetime identity: Arc clones share
entries, independent backends and decorators do not, and caches do not retain
connections or runtimes. Existing borrowed lookup methods remain source
compatible and read directly from the supplied backend. Scheduler, notifier and
sync-reporter paths retain their backend Arc for cached reads. Definition-cache
capacities and TTLs remain process-local bounds. Optimization plans continue to
verify the complete definition hash before reuse, including colliding IDs.

ID invalidation conservatively clears matching entries for every owner, including
name entries whose ID entry was independently evicted. This does not provide
account authentication, catalog retirement fencing, cross-process invalidation,
or a durable restart identity. Mutable views hidden behind one backend allocation
must still have stable lookup semantics; account/catalog runtime handles remain
an ADR-0002 gate. All 2,053 lean engine and 445 mobile library tests pass,
including six new cache ownership cases. All-target Clippy passes for both
with warnings denied; documentation, formatting and whitespace checks pass.
Hosted CI remains required before merge.

### DME086 — Waiting parent could remain stranded after backend restart (fixed)

**HIGH, effort M.** A transient execution-tree read failure after the parent
committed Running → Waiting was retried only by the volatile wake registry.
Closing that backend and reopening its SQLite file left the parent Waiting
forever even though the awaited child was durably Completed. The reproduction
fails on the prior code after an actual pool close/file reopen; it does not
claim an operating-system process kill test.

The existing bounded, rotating Waiting sweep now also reconciles workflows
containing sub-sequences against persisted execution-tree and child activation
state. It reuses dispatch's activation matching, so a terminal child from an
older loop activation or another block cannot authorize a wake. Only a
Waiting → Scheduled compare-and-set reschedules the parent. Failed reads or
writes remain eligible on later rotations without depending on a volatile
notification. A bounded sequence-feature memo keeps ordinary workflows from
paying for new tree/child reads. Deadline and approval timeout handling retain
priority. The original page bounds and separate embedded single-shot full scan remain
in effect. The native current-activation query cost gate is addressed by DME087.

All 2,060 lean engine and 445 mobile library tests pass. The final 16-case
wake/recovery matrix and complete workspace/all-target Clippy pass. The public
embedded tick case passes on both SQLite and an isolated required Postgres leg;
the owned tmpfs container is removed. Documentation, formatting and whitespace
checks pass. This proves persisted-state recovery behavior. Native indexed query validation
is documented below; hosted current-head checks remain required before merge.


### DME087 — Current-activation lookup loaded accumulated history (fixed)

**HIGH for long-running parents, effort M.** Dispatch and post-park
reconciliation previously fetched every child and matched one activation in
memory. Reusing that path in the Waiting sweep would repeatedly deserialize an
unbounded history. The page cap bounds parents, not their historical children.

The additive `get_child_for_activation` storage method preserves exact activation
precedence, otherwise the first legacy child whose activation key is absent and
block matches. Explicit JSON nulls/numbers do not become legacy/string matches.
Native SQLite/Postgres implementations select at most two candidate IDs in
one snapshot, then fetch the winning row once. Stable created-at/ID ordering
resolves ties.
Postgres indexes fixed-width hash buckets and also compares the original
strings, so hash equality never selects a different activation. A long-key
regression first reproduced a 19,000-byte raw index-entry failure; activation
and legacy keys built from 512 random UUID segments now work on all four
backends. Two partial composite indexes serve exact and legacy reads; migration 122 includes
rollback and the online precreation procedure for large Postgres installations.
Encrypted storage forwards the indexed read and decrypts the selected result;
fault wrappers preserve interception. Custom backends retain a correct default
using their existing child order and must override it for native-equivalent bounds.

Waiting reconciliation, post-park checks and current child dispatch use this
method. The embedded tick regression makes any full-history read fail, proving
recovery and consumption work without that fallback on actual SQLite/Postgres.
A storage contract uses 2,048 historical children on each plain/encrypted
SQLite/Postgres backend, covering exact-live precedence over terminal legacy,
timestamp/ID ties, parent isolation, non-string/null metadata and decrypted context.
EXPLAIN of the actual production queries selects both new indexes and rejects
Postgres table scans. This is query-shape evidence, not a production throughput
benchmark.

Final validation: 2,060 lean engine, 227 storage and 445 mobile library tests
pass. Full workspace/all-target Clippy passes with warnings denied. Both native
query contracts and embedded recovery pass on required isolated Postgres; all
owned containers are removed. Documentation, formatting and whitespace checks
pass. Hosted current-head checks remain required before merge.

Historical adoption after an activation miss was tracked separately; DME088
below addresses that path. Public account/catalog ownership and the remaining
original/ADR gates remain open.

## SDK main-branch inventory recheck

The current `main` head matches a successful latest Actions run in sdk-node,
sdk-go, sdk-python, sdk-php, sdk-ruby, sdk-dotnet, sdk-java, sdk-expo,
sdk-contract, react-native-orch8 and orch8-flutter. orch8-mobile-swift has no
Actions runs to establish the same evidence. The organization open-PR search
finds only the two monitored engine PRs. These are repository/CI observations,
not a claim that current native artifacts or the scoped SDK contract satisfy
ADR-0002. Those release/platform gates remain open.


### DME088 — Historical child adoption loaded the parent's full history (fixed)

**HIGH for long-running loops, effort M.** A rebuilt execution node or a new
loop activation fetched every historical child before selecting a prior run.
The claimed-evaluation regression fails on the previous code when full-history
reads are rejected, with 2,048 old children present.

The additive `get_child_for_adoption` lookup selects the newest eligible child
by parent, block and the exact enclosing-loop/for-each activation suffix. It
preserves reverse child ordering, including created-at/ID ties, and excludes
cancelled children, absent/non-string activation keys, and every present
supersession marker (including false/null). Native SQLite/Postgres use one
partial composite index, select one candidate ID and fetch that row. Postgres
hashes long block/suffix values for index width and still compares full strings.
Encrypted and fault wrappers delegate the lookup. Custom backends preserve their
existing behavior through a default implementation and need an override for the
native work bounds. Migration 123 includes rollback and online precreation.

Dispatch uses indexed current-activation and previous-run lookups throughout.
The old in-memory selector was removed, and its loop-key test now uses real
storage reads. Selection remains a read: the existing claimed adoption write
still rechecks the original parent claim, child activation, ownership and
eligibility before rebinding or retrying a child.

All 2,060 lean engine, 230 storage and 445 mobile library tests pass, as does
full workspace/all-target Clippy with warnings denied. Three storage contracts
pass on plain/encrypted SQLite/Postgres, covering 2,048 historical rows (half
older eligible matches), ordering, states, malformed metadata, supersession,
parent isolation, decrypted data and long Unicode keys. Production query plans
use the adoption index without a history sort, bitmap heap scan or Postgres
sequential table scan. This is access-path evidence, not a throughput benchmark.
Three claimed-evaluation and eleven lifecycle tests pass against required
isolated Postgres; the rebuilt-parent test forbids bulk history reads. The owned
container is removed. Hosted exact-head checks remain required before merge.

### DME089 — Swift cancellation fixture did not establish host entry (fixed in test)

**MEDIUM, effort S.** PR #285's simulator job 120975549580 failed waiting for a
host cancellation event. The test had a five-second provider deadline but never
established that the host fetch entered before it elapsed. Source inspection
shows queued expiry may correctly bypass host invocation; the old failure alone
did not distinguish that from delayed cancellation observation.

The fixture now observes physical host entry before checking cancellation,
drives the blocking callback on a host thread, and checks the actual timeout
error. Entry-sensitive tests use a larger startup window and isolated budgets;
the separate eight-provider test still exercises the real process-wide shared
budget. Retirement also asserts that physical admission remains held while the
non-cooperative callback is alive. Completion polls use monotonic deadlines,
report the calling test location and stop on failure. Real timeout, late-result,
single-flight, cancellation and capacity assertions remain covered.

All 12 token-provider tests pass on macOS with the production Swift wrappers and
existing ABI-compatible native artifact. This validates the Swift test logic;
current iOS simulator execution and native release parity remain hosted/platform
gates. The broader original review and ADR-0002 gates remain open.


### DME090 — Delegation queue prefixes starved later durable work (fixed)

**HIGH, effort M.** Discovery repeatedly examined the oldest 50 pending tasks.
Tasks assigned to this phone remain pending and could hide every later remote
placement. Outcome scans likewise repeatedly examined the oldest 50 open records,
so a backed-off or long-running prefix could hide later expired or ready work.
Both failures are reproduced by 51-row behavioral regressions.

Indexed keyset scans now persist an independent bookmark for discovery and open
records. The bookmark advances before decoding/network work, wraps after the tail,
and survives database reopen. A short SQLite writer transaction reserves and
updates the bookmark across independent pools; network calls hold no scan lock.
Idle scans make no database writes. Ordered partial indexes are explicit access
paths, avoiding the coarse state index's full pending-queue sort. This is
scheduling fairness, not an exclusive claim or authenticated owner boundary.

Retry hints survive page rotation and are capped at 10,000 entries; terminal
transitions explicitly forget hints without reinserting them after a poll.
Malformed records are retained and skipped individually instead of inventing
empty JSON, unknown kinds, missing epochs or a new deadline. The remaining valid
page entries can progress. Tests cover ties, independent pools, close/reopen,
bookmark-write rollback, idle writes, malformed fields, hint bounds and actual
production query plans. This bounds rows per pass, not total record/payload size.

### DME091 — Delegation outcomes lacked response identity correlation (fixed)

**HIGH, effort S.** A completed answer with the correct runtime/epoch but another
delegation ID or parent continuity ID could resolve local work. A loopback
regression reproduced the wrong resolution before the fix. Polls now require
both identities to match the durable record before interpreting results. Runtime
and epoch fencing remain required. Claimed rows missing stored parent identity
are retained without dispatch, rather than treating absent epochs as equal.
Tests assert that mismatched answers were actually read, preserve claimed state
and do not store the unrelated output; valid completion closes its retry hint.

All 456 mobile library tests and full workspace/all-target Clippy with warnings
denied pass on the local Rust runtime. Exact-head hosted checks remain required
before merge. Account/catalog
ownership of execution, delegation and authenticated control-plane bindings is
still an explicit ADR-0002 gate; these legacy-queue fixes do not establish it.

### DME092 — New child lookup migrations omitted release metadata (fixed)

PR #285's full hosted Unit Tests job reached the migration contract and exposed
an omitted rollback header and an unchanged exported storage schema version.
The exported version is used by health/backup metadata and must track the newest
forward migration. Migration 122 now reports version 122 and its rollback names
the indexes it removes. The complete 15-test migration rollback integration
suite passes locally. The dependent migration 123 carries the matching rollback
header and version 123; its complete 15-test contract also passes locally.
Exact-head hosted checks remain required.


### DME093 — Scoped signed-document ingestion boundary (implemented internally)

The explicit account/catalog requirement needs a signed audience and a verified
installation boundary, not only owner-qualified storage primitives. The publisher
now generates a distinct `orch8.catalog.v1` envelope; the root signature covers
format, stable source/catalog and explicit principal/public audience plus the
complete manifest. Existing legacy publisher output is unchanged.

Internal mobile ingestion checks the pinned Ed25519 authority and exact persisted
principal/source/catalog binding before changing keys or acceptance state. Unknown
or duplicate envelope fields, wrong format, foreign audiences, legacy documents,
malformed/duplicate key sets and implicit public fallback fail closed. Input size
is bounded. The original signed envelope bytes determine the replay digest.
Acceptance shares the existing owner/epoch/replay transaction.

An accepted document retains its storage owner and immutable scope. Installation
checks the selected entry's original-byte digest, detached signature and actual
workflow name/version. Existing normalization and immutable published identity
remain enforced. A later manifest/key revocation or account retirement fences
older verified handles; rejected storage writes preserve the previous complete
key set and acceptance cursor.

Eight actual publisher-to-mobile contracts pass, covering four independent
account/catalog combinations sharing source IDs/names/keys, foreign documents,
ambiguous protocol fields, key errors, valid and invalid installation, revocation,
account retirement/reopen, injected write failure and database close/reopen with
same-version equivocation. All 464 mobile, 119 publisher and 705 shared-type
library tests and full workspace/all-target Clippy with warnings denied pass.
Raw trust and install primitives are restricted to the internal scope module;
callers outside that module use the verified-document boundary. Hosted exact-head
CI remains required.

This is an offline trust boundary. It does not establish that a network credential
belongs to the claimed principal, expose public scoped sync/execution, or provide
persisted public-catalog access policy. Those requirements, reconciliation,
compatibility evaluation, runtime ownership, quotas and SDK/platform release gates
remain open under ADR-0002 and the original review matrix.


### DME094 — Authenticated owner-qualified private catalog sync (implemented internally)

The internal caller now connects the signed ingestion boundary to actual HTTP
sync. Immutable per-catalog public-HTTPS manifest/identity endpoints survive
reopen and cannot be silently rebound. A fresh nonce-bearing identity response
must match the current credential's principal and persisted source/catalog before
any private content request, including a 304 revalidation. The response is bounded,
short-lived and never cached; redirects and foreign CDN bearer forwarding remain
disabled. The backend contract is documented in
[Private scoped catalog sync](../mobile-catalog-identity.md).

Catalog ETags, acceptance/applied versions and environment fingerprints use only
owner-qualified storage. V1 manifests are complete snapshots; missing cached names
are withdrawn in metadata pages, without reading full workflow bodies. Compatibility
changes bypass a prior ETag. Definition bytes/signatures and entry identity are
checked before local-ID normalization. Legacy sync remains separate.

The coordinator admits two owned operations per engine and one per account;
retirement/reopen shares that account slot until the predecessor settles. Caller
cancellation stops future network phases while dispatched SQLite writes keep
admission through commit/rollback. Account epoch checks fence late identity
responses and writes. These operation slots do not establish physical native
callback fairness or persistent byte/history quotas; both remain open rollout gates.

### DME095 — Private retries could outlive the identity response (fixed)

A controlled HTTP/virtual-clock regression reproduced a retry wait continuing
past the verified identity lifetime. Each scoped run now applies a monotonic
private-I/O deadline to requests, reads, backoff and Retry-After sleeps. Every I/O
admission checks cancellation/deadline before polling the request. Already-dispatched
SQLite writes stay outside that cancellable wrapper and settle under their epoch
fence before the owned task releases admission.

### DME096 — A failed download left published workflow identity unrecorded (fixed)

An HTTP regression accepted a newer root document that changed an earlier
workflow version's hash after its original download failed. Acceptance now commits
name/version/hash history with the root document's keys and replay digest, before
downloads. Two indexed point lookups per entry check accepted and installed
history; newer root documents cannot change that workflow version or roll back
its version. Account/catalog keys qualify all reads and inserts. SQLite schema
53 adds transport bindings and entry history; no Postgres migration changes.

Collection sizes are capped at 1,000 sequences/keys/removals inside the existing
five MiB body cap. Contradictory active/removed names are refused. An injected
key-write failure verifies that new entry history, keys and acceptance cursor
roll back together. The original same-version equivocation fixture now changes a
non-active removed name so it tests the replay fence rather than the new
contradiction check.

### DME097 — An unavailable first entry starved later valid installations (fixed)

The initial scoped caller aborted at the first failed download. The reproduced
404-first/valid-second case now installs the valid entry and leaves the document
pending. Invalid entry signatures/hashes also remain pending; cancellation,
retirement and storage errors stop the run. Pending work cannot advance the
applied cursor or ETag, so another sync can retry without rolling back acceptance.

Validation includes all 478 mobile, 230 storage, 119 publisher and 705 shared-type
library tests, the 15 migration integration checks and full workspace/all-target
Clippy with warnings denied. Thirteen actual HTTP catalog contracts plus the
endpoint-binding contract cover four isolated scopes, current-token mismatch,
rotation, nonce/replay/time/size/redirect rejection, CDN header isolation,
withdrawal/environment changes, entry failure recovery, retirement, account/global
admission, real SQLite commit cancellation and full database reopen. Hosted
exact-head checks remain required before merge.

Public scoped instance/worker/delegation/handler ownership, SDK handles, production
identity backend integration, public-catalog access policy, total storage/native
callback quotas and the full original review matrix/ADR gates remain open.


### DME098 — Durable owner-qualified run preparation (implemented internally)

SQLite schema 54 adds durable scoped request records before scheduler materialization.
Each record freezes account/catalog/epoch, definition/version, input identity,
creation time and admitted manifest version/digest. Canonical input makes object-key
order irrelevant to scoped idempotency. The request's version selector, not today's
resolved `latest`, determines retry identity; upgrades cannot retarget a prior
operation. Changed inputs/selectors conflict. Old login epochs cannot re-admit old
keys; active owner handles may still inspect historical receipts.

Admission serializes global/account/catalog record and UTF-8 input-byte ceilings
with the insertion. Cancelled/retired records remain charged; explicit cleanup is
limited to cancelled, never-dispatched records and refuses an ID present in native
instances. The accounting query uses a covering metadata index rather than reading
inputs. Schema byte checks prevent understated counters; reads verify immutable
input fingerprints. These are request-store limits, not completed total cache,
instance-history or physical native-callback quota gates.

Prepared records pin their selected definition. Withdrawal preserves pinned data
as deprecated; eviction and canonical native deletion refuse it. A schema constraint
also rejects direct deletion that would detach a prepared record. Cancelled
receipts retain their original selection after the definition is purged. No native
scheduler row is emitted: an actual ordinary tick verifies no request is dispatched.
Scoped materialization/handler/builtin ownership remains unimplemented and must
precede exposing this as a public start API.

### DME099 — New admission needed a durable removal-reconciliation boundary (fixed)

A newer document can be accepted before removals finish. The HTTP contract exercises
that window with the old cached definition still present, and verifies that a new
request is refused. A protected version/digest marker now commits after the full
removal pass. Admission verifies it against current acceptance in the same fenced
transaction; an earlier marker cannot authorize a newer document. Completed
accepted/applied state safely supports older fully evaluated caches offline.

Download/evaluation remains a separate phase: a failed entry leaves the catalog
pending while a valid reconciled cached workflow can prepare a request. Existing
idempotent receipts remain readable without admitting work. Root and account
retirement fences remain enforced, including phase metadata writes.

Validation: 494 mobile library tests, 230 storage library tests and 15 migration
rollback tests passed, along with workspace/all-target Clippy with warnings denied.
The 15 request contracts and 14 scoped HTTP contracts cover four scopes, changed
requests, upgrades, full-capacity retries, retirement, reopen, pin retention,
injected insert failure, byte accounting/index plans, corruption and independent-
pool races. Hosted checks must also pass on the submitted commit before merging.
The full original matrix and ADR-0002 gates remain open.

### DME100 — Legacy mobile start could duplicate reruns or orphan runnable work (fixed)

Real SQLite lifecycle tests reproduced two failures: concurrent callers reusing a
terminal operation key created distinct fresh instances, and an injected dedup
mapping write failure returned an error after leaving a scheduled instance behind.
The capacity preflight also occurred outside the instance insert transaction.

Legacy start now reserves the SQLite writer before resolving durable dedup state,
checking database-wide active capacity, inserting the canonical native instance and
updating the mobile mapping. All effects commit together. Terminal-key reuse keeps
legacy fresh-run semantics, while concurrent retries see the same fresh mapping.
An active native instance from an older client with a missing mapping can be
recovered without another insertion. Empty legacy keys also serialize correctly.
Paused instances remain charged so resuming one cannot overbook capacity.

The in-memory dedup shortcut was removed: another manager's cancellation or
completion cannot leave a cached dead ID in an existing manager. Cleanup remains
qualified by the old instance ID, so late cleanup cannot delete a replacement's
mapping. No scoped instance is materialized by this legacy-only path.

Validation: the initial regression run failed on duplicate fresh IDs and the
runnable orphan. After the atomic change, all 500 mobile library tests passed,
including 27 lifecycle tests. Independent-pool, paused-capacity, cross-manager
stale-ID, empty-key and retry-after-rollback contracts passed. Workspace/all-target
Clippy with warnings denied, formatting, documentation and diff checks passed.
Hosted checks must pass on the submitted commit before merging.

Next: continue scoped materialization/callback ownership; durable preparation is
not a substitute for the required executable multi-account runtime. Preserve the
full original review matrix and ADR-0002 release gates.

### DME101 — Coordination authorization lost caller ownership on missing rows (fixed)

`caller_may_access` treated a missing caller row as unrestricted within the context's
tenant. It also compared the target only with the context tenant, without checking
the durable caller's tenant. A deleted embedded customer's source or a mismatched
execution context could therefore bypass the persisted sub-tenant boundary.
The guard now requires an existing caller with the context's tenant before
applying sub-tenant rules. Regression contracts exercise both `query_instance` and
`send_signal`, requiring the ordinary missing-target response and no signal row.
Existing tenant-level and same-sub-tenant access remain covered.

Both missing-caller contracts failed before the fix. Four separate missing-caller
and tenant-mismatch contracts pass after it, along with all 2,064 core engine
library tests (no default features), workspace/all-target Clippy with warnings
denied, formatting, documentation checks and diff checks. Hosted checks must pass
on the submitted commit before merging.

This check alone does not implement account-epoch/callback fencing or make catalog
namespaces a security boundary. Those execution gates remain open.

### DME102 — Scoped execution needs account-epoch fencing below scheduler claims (internal prerequisite)

The future scoped execution path cannot rely on a scheduler claim token alone.
Test-only native fixtures backed by real prepared requests reproduced a successful
KV write after account retirement, and a retired high-priority candidate winning
admission before another account. Production scoped materialization is still
absent; these tests do not claim an existing public scoped start API.

SQLite now checks immutable request ownership and the current active account epoch
inside the claim-bound write transaction. The same predicate fences claimed state
transitions (including outbox transactions), lease renewal and its ownership
fallback. Scheduler eligibility and higher-priority checks exclude invalid owners;
both ordinary and per-tenant candidate paths filter before their limit. Reopening
an account cannot authorize a predecessor epoch's existing native claim.

A request ID cannot be relabelled as legacy by changing its native tenant,
namespace or definition. Native rows in registered account tenants also require a
matching request; missing provenance fails closed. Existing legacy tenants retain
their original behavior. The correlated checks use request/account primary keys,
not scans of request input or account history.

Validation: 503 mobile, 231 storage and 2,064 core engine library tests passed.
The storage suite includes the primary-key query-plan contract. After extracting
shared test assertions, the three focused contracts passed again, strengthened to
verify a successor account epoch can claim/write while its predecessor stays
fenced. Workspace/all-target Clippy with warnings denied, formatting,
documentation and diff checks passed. Hosted checks must pass on the submitted
commit before merging.

This is an execution storage prerequisite. No native scoped materializer or public
start API is introduced. Handler/native callback ownership, scoped reads and
builtins, child/delegation provenance, listeners, signals/SLA/recovery sweeps and
complete resource accounting still require integration before scoped execution
can be exposed. Preserve all ADR-0002 gates and the original review matrix.

### DME103 — Queued native handlers entered without revalidating their claim (fixed)

A real blocking-queue regression reproduced a host callback entering after its
source instance returned to `scheduled`. The native invocation previously retained
only handler/name/input, so waiting in the blocking pool discarded the original
scheduler claim as an admission condition.

Native invocations now retain immutable instance/tenant/token/storage context and
verify it on the blocking thread immediately before host admission. The required
`InstanceStore::verify_instance_claim` read checks one exact running claim and
tenant. SQLite additionally applies the frozen request/account-epoch predicate;
Postgres checks its native running claim. Encrypted and fault-injecting decorators
explicitly delegate. This differs from heartbeat: a retained token on a completed
or waiting instance must not authorize new host entry, and verification does not
renew or modify the lease. Custom Rust storage implementors must implement this
new required method; it has no permissive default. No HTTP/FFI interface changes.

Cancellation or timeout while the database preflight is pending marks the physical
invocation detached. A second admission check then prevents host entry; quarantine
remains until the blocking closure actually settles. A clean direct-handler retry
works after settlement. Failed database checks never invoke native code, preserve transient
versus permanent error classes and do not include database error details. Invalid
claims return `ClaimLost`, avoiding a fabricated permanent workflow failure.

A separate weak-reference regression caught the claim/backend being dropped after
preflight while detached host work was still live. Verification now borrows the
owned proof; the blocking closure retains it through physical completion and then
releases it. This preserves context without retaining the backend after settlement.

Focused checks pass for valid claims, every non-running state, replaced tokens,
missing sources, tenant mismatch, cancelled/timed-out verification, storage
failures and clean retry. A real scoped request/native fixture verifies retirement
and reopen while queued both prevent host entry. Final validation passed all 508
mobile library tests, 231 storage library tests, the four native backend-contract
variants (plain/encrypted SQLite/Postgres on an isolated required Postgres), and
workspace/all-target Clippy with warnings denied. Formatting, documentation and
diff checks passed. Hosted checks must pass on the submitted commit before merging.

This is an entry preflight, not an atomic external-side-effect transaction or
forced native cancellation. Claims can change after admission; in-flight native
work retains its physical ownership and claim-bound result writes remain fenced.
Scoped handler-object routing, per-account physical fairness, public SDK handles
and the other ADR-0002 gates still require integration before scoped rollout.


### DME104 — Native effect evidence preceded actual admission (implemented)

A real `execute_step_dry` regression cancelled a native callback during claim
verification, proved no host call occurred, and then found the same attempt blocked
by `EffectBlocked(Unknown)`. The core had already recorded `Dispatched` before
invoking the native adapter.

Native registration now explicitly selects deferred effect admission. The core
prepares the immutable receipt and passes an opaque, non-deserializable single-use
`EffectAdmission` capability in `StepContext`. The native blocking closure verifies
the claim and cancellation first, then admits the durable dispatch, rechecks claim
and cancellation after that storage boundary, and only then enters host code.
Ordinary handlers and mocks retain their normal immediate guard behavior.

Cancellation before admission leaves `Prepared`, so the real step executor can
resume the same attempt and commit one receipt after one host call. An interrupted
durable admission or admitted host failure remains conservative: existing
`Dispatched`/`Unknown` evidence blocks unsafe replay. No unresolved evidence is
deleted and no elapsed-time heuristic authorizes re-entry.

Capabilities share one admission flag across clones. A caller that never admitted
its own capability cannot refresh/settle another caller's receipt. A controlled
overlap checks that an unadmitted failing caller leaves the admitted caller's
receipt dispatched and permits its eventual commit. Typed admission failures are
returned through the core rather than being flattened into a fabricated host
failure. Native admission retains its owning backend through physical settlement.

Rust adapters opting into deferred admission must honor the explicit admission
contract. `StepContext` literal constructors need `effect_admission: None` unless
the core supplies a capability. HTTP/FFI signatures and effect-state serialization
are unchanged. Validation passed 514 mobile library tests, 2,067 engine library
tests and four deferred-recovery backend-contract variants on plain/encrypted
SQLite/Postgres with an isolated required Postgres. The overlap regression was
rechecked after test-helper cleanup. Workspace/all-target Clippy with warnings
denied, formatting, documentation and diff checks passed. Fault cases distinguish
pre-commit storage failure (safe retry) from ambiguous commit (preserved unknown
evidence). Hosted checks must pass on the submitted commit before merging.

### DME105 — Native names and dry-run bypassed side-effect policy (fixed)

Two regressions reproduced native execution under `runtime.dry_run` and missing
effect evidence when a native callback replaced a pure builtin name such as
`noop`. Native registration now forces an effect receipt regardless of the name's
builtin classification. Dry-run returns a simulation envelope without entering
host code or echoing raw native inputs/resolved credentials; host mocks remain
available when realistic output shapes are required.
These checks apply to native callback registration; scoped routing and the rest of
the original runtime/SDK completion matrix remain open.


### DME106 — Frozen native owner routing and physical admission (internal)

The internal catalog router resolves a running native claim to its durable
account/catalog/epoch proof before selecting an immutable handler binding. It
requires the executor's exact backend identity, accepts configured decorators,
and refuses legacy fallback. Identical names in four account/catalog combinations
route to distinct host objects through the real step registry and effect ledger.
A foreign database with a colliding, independently valid claim cannot dispatch.

Scoped native invocations share a process-local limit of two physical calls and
one per stable account, across hubs, pools and login epochs. The owned permit
covers queued work, claim verification, durable effect admission, detached host
execution and final foreign-object destruction. Controlled tests show retirement,
caller cancellation and hub recreation cannot reclaim a live slot; another
account can progress. Retirement while queued prevents host entry.

Registration counts are bounded at 256 per hub, 64 per account and 32 per catalog.
Retired bindings remain resident and charged until hub teardown. These limits do
not bound opaque host-object bytes. Explicit account-close/shutdown detach and
bounded destruction of idle foreign references remain necessary before production
integration. The runtime constructor is trusted to pair the mobile store with the
executor backend; arbitrary decorator internals are not inspected.

Validation passed 522 mobile library tests, eight scoped callback contracts,
21 existing native-handler tests, and workspace/all-target Clippy with warnings
denied. Contracts include decorator preflight failure, immutable binding refusal,
global saturation, foreign destruction and registration limits across reopen.
No production scoped materializer or public SDK API is introduced. This budget
covers scoped native callbacks only; other callback classes, initial routing SQL,
independent processes, scoped builtin/read boundaries, listeners and recovery
remain open under ADR-0002. Hosted checks must pass on the submitted commit.


### DME107 — Explicit retirement cleanup retains physical ownership (internal)

The internal native router can now release bindings for an already-retired
account epoch. A durable account read rejects cleanup of an active epoch; late
cleanup after reopen selects only the old epoch across its catalogs. Binding
admission and detach are serialized, with the durable owner check inside that
boundary, so a previously validated registration cannot insert after cleanup.
Native admission reserves capacity before cloning a host reference; dry-run
returns its simulation without cloning foreign ownership.

Cleanup reserves the existing process/global and stable-account physical budget
before detaching references. If an old call still owns that slot, cleanup returns
a retryable busy result without moving references. Detached registrations remain
charged against hub/account/catalog counts until physical destruction completes.
The blocking owner drops references outside registry locks, catches each host
panic, then removes the charges and releases capacity. Its completion receiver
is observational: dropping it cannot abort owned cleanup. Another account can
execute while one account's destructor is blocked.

Four controlled contracts cover blocked destruction plus dropped observation,
quota retention, late old-epoch cleanup, active-epoch refusal, busy cleanup retry
after detached call settlement, panic isolation and queued cleanup after runtime
shutdown. Teardown publishes completion through an atomic marker and does not
re-enter the async registry lock or require a running executor. Completed marker
entries are pruned on later binding/cleanup admission. Workspace/all-target
Clippy with warnings denied, all 526 mobile library tests (including twelve
scoped callback contracts), formatting, documentation and diff checks pass.
Hosted checks must pass on the submitted commit before merging.

This is an explicit internal cleanup primitive, not production account-close or
shutdown wiring. Hosts must still call it after durable retirement and retry busy
cleanup. General hub destruction, runtime shutdown integration, host-created
reference cycles, cross-process bounds, builtin/read isolation and SDK lifecycle
contracts remain open. No workflow/effect evidence is deleted.


### DME108 — Coordination reads and signal writes require durable owners

The four-scope regression failed against the preceding core handlers: a caller
in Alice's first catalog received `found: true` for Alice's second catalog.
Tenant/sub-tenant checks alone did not express the chosen catalog boundary.
Mobile's public builtin allow-list still excludes these handlers; this regression
uses the actual core registry over scoped native fixtures, before production
scoped execution is exposed.

`InstanceStore::get_instance_for_caller` now authorizes and reads in one storage
statement. SQLite requires both native rows to satisfy their durable owner
proofs, with matching account/catalog/epoch, or both to be legacy rows. It rejects
retired epochs, proof/namespace/definition mismatch, cancelled admission proofs,
and registered-account rows without a proof. PostgreSQL preserves tenant and
caller sub-tenant confinement. Legacy namespace differences remain permitted.
Missing and unauthorized callers/targets remain indistinguishable. Encryption
wrappers decrypt only an authorized result; fault wrappers delegate the policy.

`SignalStore::enqueue_signal_for_caller` repeats authorization inside its insert
transaction. SQLite takes its writer reservation before checking both owner
proofs, serializing with retirement across independent pools. PostgreSQL locks
both endpoint rows in ID order before authorization/state checks, including
reciprocal and self signals. Unauthorized terminal targets return not-found;
authorized terminal targets retain their existing typed refusal. The core
handlers use these operations; their earlier read cannot authorize a later write.
The insert also verifies an optional exact running caller claim under those
locks; a stale/waiting/completed/replaced claim returns false without inserting,
and the handler returns typed `ClaimLost`. Trusted unclaimed callers retain the
explicit `None` path. Custom Rust storage backends must implement both methods.
Trusted raw storage
operations remain available; these methods do not sandbox arbitrary host Rust.

All sixteen contracts pass across plain/encrypted SQLite/Postgres on an
isolated required Postgres. They cover durable caller existence, tenant/sub-tenant denial,
legacy namespace compatibility, encrypted context/payload round trips, deletion
between preflight and enqueue, terminal denial, reciprocal/self signals and
running/replaced caller claims. Five mobile contracts cover real four-scope dispatch, cross-catalog signal refusal,
forged legacy rows, relabelled proofs, cancelled admission proofs, retirement and
reopen, and a queued writer in an independent pool. A query-plan regression checks
point-index ownership lookup without historical scans. Final verification passed
2,067 engine, 531 mobile and 232 storage library tests, workspace/all-target Clippy
with warnings denied, formatting, documentation and diff checks. Hosted checks
must pass on the submitted commit before merging.

This closes the coordination-handler boundary only. Public scope handles,
production materialization, instance-local builtin ownership, child/delegation
resolution, API reads, lifecycle callbacks, shutdown wiring, independent-process
bounds and the original distributed/mobile/embedded review matrix remain open.
