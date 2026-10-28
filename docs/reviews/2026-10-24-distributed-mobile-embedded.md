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
| Device delegation and handoff | Interrupted ownership transfer, epoch fencing, idempotent acceptance, private data, return/resume, cleanup | Reviewing; legacy queue fairness and response correlation verified; legacy admission/replay ownership (DME163) and local node/stop admission (DME164) verified; account-authenticated transfer and acceptance remain open |
| Embedded Rust | Tick/run lifecycle, shutdown, independent engines, tenant/backend identity, cancellation and callback ownership | Reviewing; runtime retirement/terminal shutdown and backend-owned volatile wake/retry state verified; backend-qualified cache identity verified; sub-sequence restart recovery and indexed current/historical child selection pass SQLite/Postgres behavioral and query-plan tests; callbacks remain under review |
| Node/Python bindings | Initialization failures, repeated close, host callbacks, concurrent calls, crash/restart, handler retention and errors | Reviewing; post-storage claim entry, physical callback count ownership through timeout/retirement (DME161), and safe Node/Python failure diagnostics (DME159/162) covered; byte bounds and platform parity remain open |
| Swift/Kotlin/RN/Flutter wrappers | Initialization/disposal, listener retention, background wake, FFI errors and cross-platform protocol compatibility | Reviewing; Swift/KMP ownership and disposal plus typed RN/Flutter scoped development facades covered; release artifact parity and full platform runtime E2E remain open |
| Web embed | Customer-scoped token isolation, sandbox effects, quotas/races, output disclosure, component lifecycle and browser auth | Reviewing; HTTP refresh and component account ownership fixed; sandbox/quota review pending |
| Operations and security | Payload bounds, resource retention, diagnostics without secrets, dependency advisories, migration/rollback and supported platforms | Reviewing; five dependency advisory graphs pass after removing unused vulnerable Wasmtime cache dependencies (DME160); complete retention/at-rest/migration/platform matrix remains open |

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


### DME109 — Claimed reads bypassed ownership through cache and KV paths

Three behavioral regressions failed against the preceding code: an executor with
its original token retained after transition to Waiting could obtain a memoized
block output, a step-cache value and builtin get_state data. Memoization/cache
returned before output persistence could detect claim loss, and get_state used a
raw KV read.

New required Rust storage operations `get_block_output_claimed` and
`get_instance_kv_claimed` combine tenant, exact running token, backend owner proof
and resource lookup in one SQL snapshot. SQLite applies the durable mobile epoch
predicate; PostgreSQL checks the running tenant claim. `ClaimedRead::Lost` is
separate from an authorized missing value, including JSON null. Native wrappers
preserve the result and decrypt only authorized values. Reads do not renew leases.

Step memoization and cache returns use these operations when a claim is supplied.
A failed claimed cache read cannot become a cache miss and dispatch a handler.
StepContext.get_state and the get_state builtin use the same claim-bound KV read,
with typed transient/permanent storage errors. Explicit unclaimed calls and
trusted raw storage operations retain their legacy behavior. Custom Rust backends
must implement both operations; HTTP/FFI signatures are unchanged.

Twenty contracts pass across plain/encrypted SQLite/Postgres with isolated,
required Postgres. They cover the three reproduced bypasses, missing versus null,
latest-output ordering on tied timestamps, encrypted cache/output returns,
non-running states, wrong tenants/tokens, absent owners and failed-read refusal
before handler entry. An independent-pool mobile test checks old-epoch KV/output
reads after retirement and reopen, without deleting evidence or renewing leases.
Final library validation passed 2,067 engine, 532 mobile and 232 storage tests.
The retirement regression also passed after extracting its fixture helper.
Workspace/all-target Clippy with warnings denied, formatting, documentation and
diff checks passed. Hosted checks must pass on the submitted commit before merge.

These checks cover the claimed read paths named above. They do not sandbox host
Rust, revoke values already delivered, or complete public scoped read APIs,
parameter/memory/agent read audits, callback lifecycle wiring, materialization or
the original distributed/mobile/embedded completion matrix.


### DME110 — Agent checkpoint recovery discarded ownership and failures

Two regressions failed against the preceding implementation. A stale agent read
and deleted a successor's checkpoint after the instance acquired a new running
claim; the stored successor record became absent. A checkpoint read also used raw
unclaimed storage and treated failure as permission to start fresh.

Agent checkpoint load/save/clear now use the initiating StepContext's claim-bound
state operations. Recovery loads and validates before agent discovery, embedding
or model/tool work. Missing checkpoints can start fresh; corrupt records,
missing/invalid counters and counters beyond the supported loop ceilings require
explicit recovery and are preserved. The loop propagates checkpoint errors:
a failed load starts no turn, a failed save prevents the next tool/model phase,
and a failed clear cannot report successful completion on either terminal path.

Agent registration uses deferred effect admission. The checkpoint is read and
validated before admitting the agent effect, with claim preflights around that
admission. Thus a pre-admission storage failure leaves Prepared evidence and can
retry the same attempt after storage recovers. Ambiguous admitted effects retain
existing conservative evidence; no uncertain dispatch is cleared. Subsequent
model/tool phases and automatic-memory boundaries revalidate the initiating
claim. A controlled real-provider mock loses its lease after the model response
and proves no tool HTTP request occurs.

The initial 50 agent tests pass; five additional loop/network contracts cover
load/save/clear errors, corrupt recovery and model-to-tool lease loss. Native and
encrypted backend contracts cover successor checkpoint preservation, failed load,
corrupt checkpoint preservation and same-attempt recovery through the real step
effect ledger. All sixteen backend contracts pass. Final validation passed
2,072 engine, 532 mobile and 232 storage library tests, workspace/all-target
Clippy with warnings denied, formatting, documentation and diff checks. Hosted
checks must pass on the submitted commit before merge. Valid legacy checkpoint
records keep their existing three-field format;
public HTTP/FFI signatures are unchanged. StepContext adds a Rust claim preflight.

This does not make tool execution and checkpoint persistence one atomic
transaction. Completion before a durable checkpoint remains ambiguous and safe
replay requires effect evidence/tool idempotency. Checkpoints remain keyed by
instance/block; no separate within-claim activation generation is introduced.
In-flight external work is not forcibly interrupted. Broader parameter/memory
reads, public scoped runtime/SDK integration and the original review matrix remain
open; this finding does not close those gates.


### DME111 — Unrelated invariant history hid synchronous dispatch policy

A regression creates 10,000 earlier observational invariants followed by an
active Http at-most-once commit guard. The preceding lookup loaded only the
first 10,000 entries before filtering for commit guards. The configured guard
was absent from dispatch policy, and the later-attempt duplicate did not receive
the required invariant refusal. The regression failed against that lookup.

Dispatch now uses a required storage lookup that applies tenant, sequence,
global/exact version, enabled flag, commit-guard flag, rule and effect kind before
its bounded result limit. Unrelated metadata cannot hide a matching policy.
SQLite/Postgres implement the filtered query; encrypted and fault wrappers
preserve it. The engine retains its defensive rule/kind filter. Public invariant
listing remains unchanged. The guard path still returns bounded diagnostic
aliases, while the presence of any matching policy is enforced independently of
unrelated history. This change does not claim complete unbounded audit-result
pagination or receipt-evidence coverage.

Four backend contracts cover the reproduced 10,000-entry boundary, exact/global
version and tenant/kind/enablement selection, ordinary unguarded per-attempt
retry semantics, and failed policy reads that cannot dispatch and remain safely
Prepared for recovery. The protected retry leaves the provider call counter at one.
Ordinary retry policy is intentionally per attempt; cross-attempt duplicate
exclusion is an explicit synchronous commit-guard policy, as documented in
[Continuity Operations](../CONTINUITY_OPERATIONS.md#synchronous-at-most-once-invariants).
The full library suites passed 2,072 engine, 532 mobile and 232 storage tests.
All sixteen native/encrypted backend contracts, workspace/all-target Clippy with
warnings denied, formatting, documentation and diff checks passed. Hosted checks
on the submitted commit must pass before merge. Custom Rust storage backends must implement
InvariantStore.list_effect_commit_guards. No schema migration or HTTP/FFI change
is introduced. The original distributed/mobile/embedded review remains open.


### DME112 — Ordinary handler factories entered without current claim proof

Three regressions failed against the preceding core: an ordinary handler factory
was entered for a retained token after transition to Waiting, for a mismatched
execution tenant, and after claim loss during durable dispatch's storage wait.
The factory counter reached one in each case. Claim-bound output writes did not
prevent the side effect of entering the handler in the first place.

Core execution now checks a supplied running tenant claim before effect-scope
creation. After durable preparation/dispatch awaits, it checks again inside the
handler timeout and before invoking the future factory itself. Pure handlers use
the same entry fence. A refused initial context cannot create a foreign receipt
namespace. Storage errors remain typed; a failed initial check creates no effect
scope and the exact same request can retry after storage recovers.

Twenty contracts pass across plain/encrypted SQLite/Postgres with isolated,
required Postgres. They cover the three reproduced cases, pure handler factories,
and failed initial claim reads followed by same-request recovery. A mobile
independent-pool retirement fixture checks that a retained running token cannot
enter a generic handler or create its effect scope after account retirement.
The native cancellation/real-ledger regression now observes adapter entry and
asserts Prepared evidence there, so generic preflight reads cannot accidentally
replace its physical-native coverage. Final validation passed 2,072 engine,
533 mobile and 232 storage library tests, all twenty backend contracts,
workspace/all-target Clippy with warnings denied, formatting, documentation and
diff checks. Hosted checks must pass on the submitted commit before merge.

Explicit unclaimed Rust helpers retain their trusted behavior. This is a
preflight, not an atomic external transaction or forced interruption of admitted
work. Dispatch already recorded before a later failed check retains conservative
unknown evidence; it is not erased. No schema or HTTP/FFI changes are introduced.
The broader parameter/memory read audit, scoped runtime/SDK integration and original
distributed/mobile/embedded matrix remain open.


### DME113 — Step preparation outlived its initiating claim

Three regressions failed against the preceding core: preparation started a
credential lookup after transition to Waiting; template I/O lost ownership but
continued to credential lookup; and deferred fallback resolved credentials using
a stale evaluation. Each observed one secret lookup where zero was expected.

Both flat and tree execution now pass the original evaluation nonce through
preparation and deferred fallback. Preparation checks the running tenant claim
at entry, after template and sandbox waits, and before returning. Credential
resolution checks ownership before and after each lookup and before publishing
results. Ownership errors remain infrastructure errors, including in deferred
fallback, rather than becoming user credential failures. A refreshed instance
cannot substitute a successor's nonce for the initiating evaluation's nonce.
Expansion uses a temporary JSON value, preserving the original parameters when
any reference or ownership check fails.

All nineteen preparation contracts passed, including native/encrypted SQLite
and required isolated Postgres. They cover the three reproduced cases, complete
nested resolution, partial-resolution rollback, claim loss during a lookup,
transient ownership-read recovery and successor-token substitution. The full
library suites passed 2,091 engine, 533 mobile and 232 storage tests. Workspace
all-target lint with warnings denied, formatting, documentation and diff checks
passed. Hosted checks must pass on the exact submitted commit before merge.

These are admission checks, not an atomic credential-read transaction or forced
interruption of already admitted I/O. Legacy credential lookup scope and
credential-error retry policy are unchanged. Broader parameter snapshots, memory
reads, scoped runtime/SDK integration and the original distributed/mobile/
embedded matrix remain open. No schema or HTTP/FFI changes are introduced.


### DME114 — Non-JSON native callback output leaked into diagnostics

A captured-subscriber regression reproduced a private callback-output canary in
the native adapter's warning fields. Native callbacks can return arbitrary text,
so this logging path copied workflow data into diagnostic sinks without a
separate logging decision by the host.

The warning now contains only its static message. Workflow behavior remains
compatible: valid JSON is parsed and non-JSON output is returned as the original
string. The regression requires both the warning and unchanged returned output,
while rejecting the canary anywhere in the captured event fields. This fix
covers the shared adapter used by ordinary and scoped native callbacks; it does
not claim a complete diagnostic-data audit of all runtime paths.

Validation passed all 534 mobile library tests, workspace/all-target Clippy with
warnings denied, formatting, documentation and diff checks. The diagnostic test
handles the same concurrent first-callsite registration race as the existing
engine span-capture regression: only an empty capture permits rebuilding tracing
interest and making one fresh mock invocation. A captured payload fails the
assertion. Hosted CI must pass on the submitted commit before merge.


### DME115 — Transient credential storage failures permanently failed workflows

Four real-scheduler regressions failed against the preceding implementation:
flat and parallel-tree executions on plain/encrypted SQLite became Failed after
one injected transient credential lookup error. No handler ran. The resolver had
converted the storage cause to StepError::Retryable, but preparation callers
handled all credential errors as user failures before reaching handler retry
policy. The existing resolver-only retryability test did not cover this boundary.

Lookup and expansion now preserve a typed storage cause internally. Claimed
preparation propagates it as EngineError::Storage through its infrastructure
branch, including deferred fallback, so the existing scheduler's bounded
infrastructure recovery applies. No handler retry policy is required. Genuine
missing/disabled/malformed credential references retain their authoring-failure
classification. The trusted legacy resolver uses the same lookup implementation
and keeps its existing StepError classification. Lookup authority, including the
legacy global credential fallback, does not change.

All 31 preparation contracts passed with required isolated PostgreSQL, including
plain/encrypted SQLite/Postgres. New contracts exercise actual scheduler
rescheduling and eventual completion in flat and tree paths, no dispatch during
the outage, unchanged failed-expansion parameters, deferred retry recovery,
permanent storage classification and missing-reference classification. The
isolated Postgres container was removed. Full library validation passed 2,103 engine, 534 mobile and 232 storage tests.
Workspace/all-target lint with warnings denied, formatting, documentation and
diff checks passed. Hosted checks must pass before merge.

This fixes preparation-stage database recovery, not an external provider's
credential rejection, cross-catalog credential policy, or the broader remaining
parameter/memory, scoped integration and runtime matrix audits.


### DME116 — Device-return E2E signed noncanonical whole-second timestamps

PR #302's E2E job rejected the hand-signed device-return capsule with a canonical
manifest hash conflict. Forcing the fixture's JavaScript timestamps to an exact
second reproduced the same HTTP 409 locally against the current server.
JavaScript emitted .000Z; typed Rust UTC serialization emitted Z. The fixture
hashed a representation different from the verifier's canonical typed manifest.
Its manually resealed checkpoint carried the same timestamp-format risk.

Normalize the fixture's typed checkpoint and manifest timestamps before hashing,
and deliberately use whole seconds so this formerly intermittent boundary runs
every time. The production verifier and signing protocol remain unchanged.
Document that host CapsuleSigner implementations sign the Rust-supplied digest,
while separate manual manifest construction must match typed canonical JSON.
No equivalent manual hashing implementation was found in the in-repository
Swift/KMP/RN/Flutter wrappers; this is not proof of full platform parity.

The focused regression first failed with HTTP 409, then passed. All 18 portable
continuity E2Es passed with fresh SQLite and all 18 passed with required isolated
Postgres, using freshly built current server and CLI binaries. The isolated
Postgres container was removed. E2E TypeScript typechecking passed. This corrects
the test producer; it does not weaken signature, payload-hash or ownership checks.
The broader runtime, scoped integration and platform audit remains open.


### DME117 — Shared Node/Python adapter entered hosts after claim loss

Four plain/encrypted SQLite regressions failed: a stale context entered the host
factory immediately, and an evaluation that lost its claim while loading outputs
entered afterward. The callback-entry counter reached one in all four cases.
The generic core's factory preflight precedes the adapter's own asynchronous
request-envelope loading and could not protect that later phase.

The shared adapter now checks its original context claim before loading outputs
and again after that await, before interpreting the result or invoking the host
factory. Lost ownership remains StepError::ClaimLost; ownership-read errors keep
the existing StepContext classification and cannot dispatch. Same-context retry
after a transient proof-read failure succeeds. Valid claimed and explicitly
unclaimed trusted calls preserve the host request/reply envelope.

All sixteen contracts passed across plain/encrypted SQLite/Postgres with required
isolated Postgres; its owned container was removed. The complete embedded facade
suite passed 158 test results and three documentation tests. Workspace/all-target
Clippy with warnings denied passed. The storage test-util dependency is enabled
only for the facade's regression tests. Actual macOS arm64 addons passed the
repository's three Node restart/error tests on Node 24.15.0 and three Python
restart/error tests on Python 3.14.6. The Python extension was linked with macOS
dynamic symbol lookup; Maturin is not installed in this local environment.
Formatting, documentation and diff checks passed. Hosted checks must pass on the
submitted commit before merge. No schema, callback envelope or public Node/Python
API changes are introduced.

These are shared-adapter admission checks. They do not prove that already queued
Node callbacks or Python blocking/GIL work can be interrupted, nor do they settle
physical callback admission, reentrant destruction, scoped account/catalog SDK
integration or the remaining distributed/mobile/embedded audit matrix.


### DME118 — Native callback destruction held reentrant lifecycle locks

An actual Python callback finalizer entered during handler replacement, attempted
to register another handler, and deadlocked. The isolated reproduction printed
FINALIZER_ENTERED but never returned and required killing its child after five
seconds. Four bounded Rust regressions also failed: callback destruction blocked
reentrant registration during replacement and reentrant close from unused,
initialized and failed-initialization engines.

Replacement now publishes the new registration, releases the registration mutex,
and only then drops the predecessor. Successful initialization detaches its retry
registration copies and releases the cache lock before retiring them. Close keeps
the cached engine through the asynchronous shutdown await, preserving ownership
for cancellation/retry, then detaches cached engine and registration values and
releases both lifecycle locks before destruction. The closed flag and frozen
registration state are established before host finalizers can reenter cleanup.
The initialization-versus-close path likewise releases the cache lock before its
last local engine and registration references drop.

All eleven native lifecycle tests passed, including the four reproduced cases.
The full facade suite passed 162 test results plus three documentation tests.
Actual macOS arm64 addons passed three Node restart/error tests and four Python
tests, including the committed real-finalizer subprocess regression. Workspace
all-target lint with warnings denied, formatting, documentation and diff checks
passed. Hosted CI must pass on the exact commit before merge. No public API, schema or callback envelope changes are introduced.

This removes lifecycle-lock deadlocks. Arbitrary host destruction can still block
its own calling thread, and queued physical host admission, general callback
resource bounds, full platform parity and scoped SDK/runtime integration remain
open. The original distributed/mobile/embedded matrix is not complete.


### DME119 — Native run dispatched work before validating its target or budget

Five Rust regressions failed against the preceding shared runner: a missing
valid ID dispatched another queued workflow; a completed target dispatched a new
workflow; zero budget entered a host callback; malformed IDs initialized and
froze handler registration; and Duration::MAX panicked when added to Instant.
Actual baseline addons also failed new timeout validation tests: Node accepted a
negative budget, while Python clamped negative/NaN input into zero and could
still execute the pending handler. The staged baseline addon hashes matched the
recorded artifacts from the preceding fix.

The runner parses IDs and checks deadline representability before initialization,
then reads the tenant-authorized target and checks terminal state and remaining
budget before every scheduler pass. Zero returns a snapshot without scheduling.
Waiting and paused targets still get a pass to process queued continuation
signals. Node accepts finite integral milliseconds in its existing u32 range;
Python accepts finite, nonnegative seconds with checked Duration conversion.
No callback envelope, schema or SDK method shape changes are introduced.

All seven run contracts passed, including concurrent same-target callers and
actual human-input continuation from Waiting. The complete facade suite passed
169 test results plus three documentation tests, and workspace/all-target lint
with warnings denied passed. Actual macOS arm64 addons passed all four Node and
five Python runtime tests. Formatting (including both standalone binding crates),
documentation and diff checks passed. Hosted CI must pass before merge.

The original deadline can still be exceeded by an already-running scheduler
pass. A complete logical run deadline needs owned pass/callback supervision and
bounded physical continuation; that work remains open, along with queued native
admission, global callback bounds, scoped runtime/SDK integration and the broader
review matrix. This preflight fix does not claim a complete timeout solution.


### DME120 — Python engine collection joined live callbacks while holding the GIL

An actual extension subprocess entered a blocking Python callback, observed its
step timeout and returned from run and close. Deleting the engine then hung:
ENGINE_DROPPED was never printed, and the isolated child required killing after
eight seconds. The default Tokio Runtime destructor joined blocking work while
Python object destruction held the GIL. That live callback could not make the
Python progress needed to let destruction finish. Two Rust regressions also
failed: retirement blocked on a physical callback, and dropping the owner inside
an async task panicked because blocking runtime destruction was forbidden.

Python now owns its runtime through a small shared HostRuntime owner whose final
drop signals background shutdown instead of joining. Arc ownership still retains
the runtime for an in-progress native API call. Started blocking callbacks retain
their resources until physical return; a detached logical observer does not free
those captures. Normal block_on behavior and the Node-managed runtime are not
changed by this Python ownership fix.

The two retirement regressions passed, including physical-capture lifetime after
logical retirement. The full facade suite passed 171 test results and three doc
tests. Actual macOS arm64 Python passed all six runtime tests, including the
committed child-process reproduction: collection returns first, then the test
releases and observes the physical callback's return. Workspace/all-target lint
with warnings denied passed. Formatting, documentation and diff checks passed;
hosted CI must pass before merge.

This does not force callback cancellation or establish process-wide callback
capacity/fairness. Physical admission and resource bounds, complete in-flight run
deadlines, scoped runtime/SDK integration and the remaining audit matrix stay
open. No Python API, schema or callback envelope changes are introduced.


### DME121 — Native run waited for the physical scheduler pass past its deadline

Three bounded Rust regressions failed against the previous runner: finite run
waiting, cancelled observer capacity, and close wakeup. The actual preceding
Node and Python addons also exceeded the logical run deadline until their held
host callback was released. Artifact hashes matched the recorded prior builds.

A shared owner now retains each scheduler pass independently of logical run
waiters. Admission is bounded by existing default scheduler concurrency, shares
same-target work, and retains capacity through real pass completion. Close or
owner drop signals shutdown and wakes logical waiters. A local owner flag is
checked before host factory entry, independently of durable claim verification;
this local retirement does not retire a durable account shared by another owner.
Already-admitted successful work may still commit under its valid durable claim.

Seven deadline contracts pass, covering finite waiting, cancellation, repeated
short observers, close wakeup, ambiguous-effect reopen refusal, independent fast
targets, and fast target completion within a batch containing held work. Four
additional backend cases hold request loading across local retirement and prove
no host factory entry while the durable claim remains valid. All 20 native claim
contracts passed against required isolated PostgreSQL and encrypted wrappers.
The full facade suite passed 182 test results plus three documentation tests;
workspace/all-target Clippy with warnings denied passed. Actual macOS arm64
addons passed five Node and seven Python runtime tests. Hosted CI must pass
before merge.

This bounds pass observation and pass admission, not arbitrary storage I/O or
physical foreign callback capacity across step timeouts and engines. Callback
queue fairness, process-wide physical limits, explicit account/catalog runtime
integration, and the broader review matrix remain open. No production scoped
SDK handle, schema change, or complete real-time API deadline is claimed.


### DME122 — Multiple native owners could run immediate crash recovery

Two baseline regressions admitted a second native handle to an existing SQLite
file, including while the first owner held a running host callback. The native
runner uses a zero stale threshold for immediate crash recovery, so this overlap
violates its required exclusive recovery authority even within one process.

Initialization now reserves a nonblocking operating-system lock before opening
SQLite, freezing registrations or running recovery. Owned initialization retains
its reservation through build I/O even if its observer is cancelled; successful observed builds transfer it to the native owner.
Close releases it only after the shared storage pool closes. A detached scheduler
pass also retains the reservation, so dropping the native object does not make
live database work look abandoned. This does not retire any durable account.

The lock uses a stable canonical-path companion; locking the SQLite data file
itself conflicted with the actual macOS SQLite VFS and was rejected during
validation. The companion is never unlinked automatically. Symlink aliases share
it; Unix hard-linked databases fail closed. Deleting/renaming live files, other
filesystem aliases and filesystems lacking advisory lock support remain outside
the supported ownership contract. This is exclusive execution, not a shared
multi-process SQLite scheduler or proof of all platform runtime behavior.

Ownership contracts cover overlap/refusal and retry, a live original claim,
independent-process exclusion, symlinks and hard-link refusal, object drop with a
live pass, independent memory databases, failed-initialization release, and
cancelled-build observation without surrendering the live reservation.
The full facade suite passed 191 test results plus three documentation tests.
Actual macOS arm64 addons passed all six Node and eight Python runtime tests;
workspace/all-target lint with warnings denied passed. Formatting, docs and diff
checks passed. Hosted CI must pass before merge. Process-wide physical callback
limits, scoped account/catalog runtime integration and the remaining review matrix stay open.


### DME123 — Mobile shutdown left retained and queued host admission live

Two production-registry Rust regressions failed on baseline: a retained registry
could invoke a callback after MobileEngine.shutdown, and a blocking-queued call
could enter after shutdown while its logical waiter remained live. The original
durable claim stayed valid. Runtime-local stop and account retirement therefore
need separate authority; clearing the current registry or cancelling the UI
operation cannot fence a captured registry or a pending database preflight.

Production host registration now captures the engine cancellation token. The
async adapter checks it before scheduling; physical admission checks it before
claim verification, after that await, and again after deferred durable dispatch
and its final claim check. Final engine drop also cancels local admission. An
already-entered callback can still return its truthful result; this does not
interrupt host code, reclaim physical capacity early, or retire an account used
by another engine. Internal catalog routers require an explicit runtime token,
refuse stopped registrations/reads/dispatch, and use the same physical gate.

The added contracts cover retained registry entry, queued calls without waiter
cancellation, stop during claim verification, final engine drop, entered callback
completion, and another router using the same still-active account. The durable
admission overlap test now covers both caller cancellation and local shutdown,
with zero host entry and preserved unknown-effect refusal. The scoped
queued-entry contract now exercises both durable retirement and local shutdown.
All 540 mobile unit tests plus 109 integration/example/coverage tests passed with
their required loopback servers. Workspace/all-target lint with warnings denied
passed. All 32 production Swift wrapper tests passed with strict concurrency and
warnings denied against fresh UniFFI bindings and the rebuilt actual macOS native
library. This is not an iOS simulator run; hosted platform CI remains required.

Hosted PR #311 also exposed a Swift test fixture with an independent natural
completion: its 120-second cooperative sleep could finish instead of observing
cancellation under delayed scheduling. The fixture now keeps sleeping until
cancelled; the adapter, timeout and cancellation assertions are unchanged. This
removes the competing completion path rather than extending the observation
budget. Strict-concurrency validation also rejected the worker URLProtocol
fixture's mutable static fields. They now live in one immutable, lock-owned
Sendable state object; all accesses retain the existing lock and client calls
remain outside it. The iOS CI invocation now denies concurrency warnings. Actual
iOS simulator CI remains required.

This is a local callback admission fix. Bounded destruction on account/runtime
close, production scoped materialization, builtin/read/recovery/listener ownership,
other callback classes and independent-process resource limits remain open.
ADR-0002 S1-S12 and the full review matrix are not complete.


### DME124 — Replacing scoped routers multiplied registration allowances

Four regressions failed on the preceding implementation. Sibling routers sharing
physical-call admission could each acquire another catalog/account/process-sized
set of registered host references. A new login epoch also bypassed the old
router's charges. A replacement could fill a catalog while an old registered
host destructor was still physically held. The 256/64/32 counts were enforced
only by each router's own live/retiring map.

Registration now also reserves process-owned permits: 256 globally, 64 per stable
account, 32 per account/catalog pair. Keys omit the login epoch. Global admission
precedes gate-map allocation, and weak owner records are pruned under a short
mutex; pending reservations and retained bindings bound those maps. The binding
declares its permit after the foreign reference, retaining charges through final
engine-owned reference release, physical callbacks and host destruction. Existing
local map limits remain conservative while explicit retirement is in progress.
Failed nested reservations roll back through ordinary owned-permit destruction.

Seven contracts cover catalog/account/process replacement, retired/reopened
accounts, a held foreign destructor, concurrent last-slot admission, repeated
refusal without leaked account/global charges, and the default shared router
constructor. All 547 mobile unit tests and 109 integration/example/coverage tests
passed with their required loopback services. Workspace/all-target Clippy with
warnings denied passed. Hosted CI remains required before merge.

These are count bounds for engine-owned scoped registrations, not total host
heap/caller allocation bounds. They do not route arbitrary foreign destruction
through bounded cleanup, expose public scoped execution, or prove cross-process
callback capacity. Bounded account/runtime close and all remaining ADR/runtime/SDK
gates stay open. The broader review is still in progress.


### DME125 — Scoped shutdown retained host objects or destroyed them on the caller

Both initial regressions failed: final router drop synchronously entered a held
foreign destructor, and shutdown retained idle registered host objects while the
router remained referenced. Scoped cleanup had an explicit retired-epoch helper,
but runtime ownership and final drop did not drive it. That helper also depended
on the caller's Tokio blocking pool.

A bounded process-owned retirement executor now owns accepted reference release.
It has two workers and shares physical global/two and stable-account/one permits
with native callbacks. Every nonempty queued group owns distinct accepted
registration charges, bounding pending references/groups by the process limit.
It skips busy accounts, gives ready cleanup priority over fresh callbacks, sleeps
on admission/change notifications, and tolerates reentrant destruction and Rust
panic payloads that themselves panic. Observers only watch completion; dropping
them or shutting down their Tokio runtime does not abort physical cleanup.

Production MobileEngine construction owns the scoped router. Shutdown and final
engine/router drop transfer its accepted references to retirement without storage
I/O or foreign destruction under the registry mutex. Local shutdown does not
retire a durable account used by another runtime. Confirmed retired epochs have
a queued cleanup operation that waits behind live callbacks without requiring a
caller retry after admission; repeated observers retain the same completion.
Old-epoch cleanup cannot detach reopened-epoch bindings.

The registry now uses short synchronous critical sections. Pending registration
references reside in that registry, with weak operation identities retained to
prevent address reuse. Shutdown can release accepted host references even while
DB connection acquisition is blocked. Weak account/epoch fences prevent a
preflight that read before retirement from publishing after cleanup. Cancelled
preflight transfers its accepted references to the same retirement executor.

Callback bodies, panic-payload disposal and reference releases drain an Apple
autorelease pool before returning physical admission. Registration charges are
retained through that drain. The thread-local pool token is confined to one
synchronous stack and is neither Send nor Sync; only its small audited runtime
boundary permits unsafe code. Two actual NSObject/zeroing-weak canaries prove
normal and unwind draining. Apple documents this requirement for long-lived
secondary threads in its [autorelease-pool guidance](https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/MemoryMgmt/Articles/mmAutoreleasePools.html).
Callback panic payloads no longer escape the physical call frame or appear in
returned permanent-error messages.

The final macOS suite passed 562 unit tests plus 109 integration/example/coverage
tests; workspace/all-target Clippy with warnings denied passed. Coverage includes
blocked DB preflight, cancellation, repeated close, queued cleanup after runtime
teardown, held calls/destructors, another account progressing, retired/reopened
epochs, reentrant drop, nested panic payloads and real engine shutdown/drop wiring.
The rebuilt macOS native library and freshly generated UniFFI bindings passed
all 32 Swift wrapper tests with strict concurrency and warnings denied. Hosted
CI must pass on this exact candidate before merge; iOS validation is separate.

This bounds engine-owned accepted references and synchronous native work, not
arbitrary host heap allocations or work a host starts outside its callback.
Arbitrary non-returning host code keeps its physical charges. Other callback
classes, full scoped materialization/builtin/read/recovery/listener ownership,
actual scoped SDK/platform cases, legacy migration and independent-process
resource limits remain open. ADR S1-S12 and the overall review are not complete.


### DME126 — Scoped native overrides need contextual effect policy

A four-scope regression failed against DME125: registering native `noop` in the
shared internal registry made another catalog's ordinary builtin unavailable.
Global name registration also cannot express that the native override requires
deferred effect admission while the other catalog's builtin remains pure.
Scoped execution must not inherit a legacy runtime-global callback as fallback.

The core now supports an optional trusted contextual resolver. Its result carries
the selected function and deferred-admission policy together. Contextual selection
runs before memoized/cache returns and durable effect preparation. Registries
without a resolver preserve their existing lookup/memoization order. Direct
`get()` remains an explicitly unscoped API; this hook is used by the ordinary
core step executor, not every independent cleanup/escalation/remote entry point.

Mobile registry construction and rebuilds attach a weak scoped resolver. A
registered-account tenant requires the same backend, running claim and frozen
request/account/catalog/epoch proof. Native selection captures weak binding
identity, which is checked again at invocation. Builtin selection captures an
independent trusted allowlist snapshot, never the ambient registry's native or
mock replacement. A later native registration cannot change an already selected
pure call into an effectful call. Missing/disabled handlers fail before creating
effect evidence; local stop and retired ownership cannot fall back to legacy.

Focused regressions cover native/pure `noop` across two accounts and two catalogs,
ambient legacy callbacks and disabled builtins, binding changes after selection,
and close during suspended effect preparation. The latter proves accepted host
references are released while only weak selected identity remains and the
undispatched receipt stays Prepared. An actual MobileEngine constructor/rebuild
regression covers legacy registration and builtin enablement. Validation passed
2,114 engine library tests, 567 mobile unit tests and 109 mobile integration/
example/coverage tests, plus workspace/all-target Clippy with warnings denied.
Fresh UniFFI bindings and the rebuilt macOS native library passed all 32 Swift
wrapper tests with strict concurrency and warnings denied. Formatting, docs and
SDK packaging checks passed. Hosted CI must pass the exact candidate before merge.

This connects contextual policy to production registry lifecycle; it does not
create production scoped instances or expose SDK handles. Materialization,
parameter/credential ownership before this boundary, independently dispatched
hooks/remote work, reads, listeners, children/delegation, migration and complete
SDK/platform contracts remain rollout gates. Native-capacity backpressure and
native-preflight infrastructure classification still require real scheduler
coverage without business retry policies. ADR S1-S12 remain open.


### DME127 — Native admission and preflight faults failed no-retry workflows

Four real-scheduler baselines failed for account/process capacity contention in
flat and parallel-tree workflows. Two additional baselines failed when a one-shot
storage fault fired only after native adapter entry, rather than in the generic
scheduler preflight. All became Failed without entering the host callback.
StepError::Retryable was being treated as an ordinary business failure, requiring
a workflow retry policy for runtime conditions the author cannot control.

The opaque effect-admission capability can now reject an owned preflight with a
typed scheduler outcome before dispatch starts. Rejection consumes the capability;
a later admit is refused, and even an adapter incorrectly returning success cannot
commit that Prepared receipt. Rejection after dispatch has started cannot relabel
its ambiguous outcome or erase the original admission. This is trusted runtime
control, not a serialized flag that workflow parameters can manufacture.

Scoped native physical-capacity refusal reports AdmissionBusy. Both flat and tree
paths propagate it without invoking retry/fallback handling. The scheduler hands
back the same claim as Scheduled with a fixed 250ms deferral, clears the prior
consecutive storage-fault streak and spends no business attempt or bounded
infrastructure-fault budget. Forty deferred dispatch opportunities retain one
Prepared receipt at attempt zero; after capacity returns, one host call completes
and commits that same receipt. A non-returning callback keeps its physical charge;
this change does not pretend to interrupt it or free its slot on logical timeout.

Transient claim-verification errors before native dispatch retain a typed storage
cause through the same owned capability. Diagnostic backend detail is removed,
while pool/connection/backend classification is retained. The scheduler's existing
bounded infrastructure recovery then applies without business retry configuration.
After dispatch starts, rejection cannot assert that retry is safe. Direct adapter
calls without a core admission capability keep their existing StepError mapping.

Six focused real-scheduler cases and two capability-state contracts pass. Full
validation passed 2,116 engine library tests, 573 mobile unit tests, 109 mobile
integration/example/coverage tests and all 32 Swift wrappers against the rebuilt
macOS native library and fresh UniFFI bindings, with strict concurrency and
warnings denied. Workspace/all-target Clippy, formatting, docs and SDK packaging
checks passed. Hosted CI must pass this exact candidate before merge. Tests use
the existing test-only scoped instance fixture; production materialization is
not introduced. These changes cover ordinary/scoped mobile native callback entry,
not every independently
scheduled foreign callback class. Scoped materialization, credential/read/recovery/
listener integration, public SDK handles and all ADR S1-S12 gates remain open.


### DME128 — Scoped execution needs materialization, credential grants and durable receipts

A real-scheduler baseline showed the internal scoped path expanding a global
credential into a native callback. Account/catalog routing after preparation did
not protect the earlier tenant-or-global lookup. Public scoped APIs were already
gated, but turning the test-only native fixture into production admission without
this boundary would have exposed private material across catalogs.

Claimed preparation now uses an explicit storage capability. SQLite binds the
original claim, frozen account/catalog/epoch and credential grant in one snapshot;
it never falls back to a global or sibling-catalog credential. PostgreSQL retains
legacy tenant/global authority inside an original-claim read snapshot. Encryption
and fault decorators forward the capability explicitly. The method is required on
storage adapters rather than silently inheriting a weaker default. Trusted direct
operator lookup retains its original semantics.

Catalog grants reference existing account-owned credential records through bounded
aliases and an epoch fence. They copy no secret values. The configured backend
continues to create/rotate/encrypt the physical record. Grants can be shared across
catalogs only by explicit authorization; revocation affects that catalog's future
reads, and a reopened account needs fresh epoch grants.

The internal MobileEngine entry now materializes the durable request. An owned
operation retains process/account physical admission through validation and SQL
settlement, independent of its observer. Input schema, DSL structure and executable
handler checks run outside the writer reservation. The CPU closure owns admission
through completion even if its async observer disappears. The writer rechecks the
request/definition fingerprints, active epoch, publication and runtime policy,
then counts global/account/catalog active work and inserts the native instance.
ID, chosen definition, input and original creation time come from the request;
workflow parameters cannot choose ownership, execution mode or claim evidence.

An independent materialization receipt commits with the instance. It survives
native history GC and prevents an old request from becoming a second execution.
Cancellation/forget of an unmaterialized request cannot remove that marker. A
failed receipt insert rolls back the instance too; independent pools cannot
oversubscribe the same admission ceiling. Idempotent existing receipts do not
need a second publication decision or active-instance charge.

Signed acceptance now stores per-version minimum SDK and required-handler policy
with the key/replay fence. Retained versions keep their signed policy across an
upgrade, while explicit withdrawal blocks new materialization. Admission pins the
policy to its receipt. Incompatible SDK or missing native bindings after restart
remain Scheduled with a 60-second runtime deferral, without consuming business
attempts or creating effect evidence. A compatible runtime can resume them.

Contextual ownership selects local dispatch before placement and worker fallback.
Template-produced `$runtime` placement is refused before effect/host entry. SQLite
also refuses scoped worker payloads until worker provenance is implemented.
Unimplemented independent lifecycle hooks, recovery/compensation, child and remote
constructs are refused by the internal admission validator; they are remaining
work, not claimed scope support.

Account retirement now atomically cancels its matching nonterminal native rows
and clears their claims without deleting request, materialization or effect
records. A held native-host test confirms its Dispatched receipt and physical
account charge remain while the host is running; stale completion cannot revive
the cancelled instance. Local engine stop remains separate from durable retirement.

Validation passed 591 mobile unit tests plus 109 integration/example/coverage,
2,116 engine library and 232 storage library tests. The claimed-preparation matrix
passed all 31 cases with PostgreSQL required, including 14 real plain/encrypted
PostgreSQL cases. The temporary RAM-backed service was removed afterward.
Workspace/all-target Clippy passed with warnings denied, and standalone Node and
Python native workspaces passed locked/offline checks. Fresh UniFFI bindings and
the rebuilt macOS native library passed all 32 Swift wrapper tests with strict
concurrency and warnings denied. Formatting, documentation and SDK packaging
checks passed; hosted CI must pass the exact candidate.

Four accounts/catalog combinations now execute through actual materialization and
the real scheduler, including colliding operation/credential aliases. This is an
internal local-execution integration, not public SDK rollout. Full scoped reads,
expiry/recovery, listeners, immutable host callback context, child/delegation and
remote-worker ownership, legacy migration, complete persistent-resource/at-rest
contracts and platform scope-handle evidence remain required. Existing grants'
encrypted-read/output tests do not prove encryption of every scoped request/history
record. ADR S1-S12 and the overall distributed/mobile/embedded review remain open.


### DME129 — Periodic expiry cleanup cannot authorize scoped execution

A real MobileEngine regression materialized a frozen scoped request whose original
creation time was older than its configured lifetime. Its first tick entered the
native host and completed before the post-tick expiry sweep could fail it. A
bounded maintenance scan is insufficient as an execution admission boundary.

Scoped handler selection now checks the configured lifetime by a point lookup of
the immutable request creation time and matching native timestamp, tenant and
original claim. Expiry atomically fails only that running claim and clears its
claim/next-fire fields. Foreign tenants, stale claim tokens and already terminal
rows cannot be overwritten. Lifetimes beyond the representable clock range do
not wrap or silently expire recent work.

Native physical admission repeats the check before effect dispatch and after the
durable dispatch wait. A host that finishes past expiry cannot publish its result;
its Dispatched receipt remains evidence of the external attempt. The physical
permit and foreign references stay owned until the host actually returns. A
retired host likewise returns claim loss instead of an apparently successful
logical result. Contextual builtins also check expiry independently of periodic
cleanup. Transient preflight failures retain the infrastructure path before an
effect is dispatched.

Regression coverage includes the original production first-tick case, builtin
admission without cleanup, flat/tree expiry after effect preparation but before
native entry, a native result after a manual-clock advance, stale claims, foreign
owners, terminal-state preservation and huge lifetimes. Counted native-entry
injection also reproduced an actual host call when expiry occurred during the
final post-dispatch claim read; verification now checks lifetime after that await.
All 597 mobile unit and 109 integration checks passed, alongside workspace/all-
target Clippy with warnings denied, formatting, docs and SDK packaging checks.
Native wrapper validation and exact-candidate hosted CI are recorded in the
candidate proof before merge.

This fixes the internal scoped dispatch boundary. Full public lifecycle/read/
listener APIs, legacy scheduler-wide lifetime semantics, persisted owner recovery,
children/delegation and the remaining ADR S1-S12 gates remain open. Already-running
native code is not preempted by logical expiry.


### DME130 — Startup recovery must not steal another live mobile engine's claim

A real MobileEngine reproduction opened two engines on the same persisted file.
The second constructor's zero-threshold recovery cleared the first engine's live
Running claim. The documented one-executor assumption was not enforced before
migrations or recovery.

MobileEngine now uses an explicitly owned SQLite constructor. It acquires a stable
OS file lock before initialization, resolves symlink aliases, refuses hard-linked
databases and treats database paths as literal filesystem paths. The persistent
`.orch8-owner` companion is never unlinked on close and uses the existing native
runner's lock protocol. In-memory engines remain independent and need no file
lock. Generic administrative SQLite pools retain their existing behavior; every
participating executor must acquire recovery authority.

The lease follows physical SQLite lifetime. A private connection-owned collation
retains the guard until SQLite's native connection destructor runs; it does not
change normal query comparison. Pool connection options retain only a weak owner.
This matters because dropping a SQLx observer can leave its native worker alive.
A detached connection therefore still prevents a successor from recovering the
same database, even after the logical pool closes.

Pool close is independently owned and starts once. Cancelling a waiter cannot
cancel draining or unlock unfinished work, and another observer can await the
same settlement. Shutdown revokes local host admission and requests close without
retiring accounts or clearing durable claims. Synchronous callers wait up to the
configured operation deadline; reentrant runtime callers do not block their own
executor. A timed-out waiter leaves recovery authority charged until physical
settlement. Drop requests close without joining an executing host.

A held-connection reproduction also showed the initial synchronous close ignoring
its operation deadline; the bounded observer now returns while the independently
owned close retains authority. Tests verify that another opener is refused until
the held connection settles and can open afterward. Existing shutdown/retirement
checks now inspect durable state through a fresh administrative pool rather than
a deliberately closed runtime pool, preserving their original assertions.

Validation covers live claim preservation, real separate-process contention and
reopen, literal path characters, symlink/hard-link aliases, independent in-memory
databases, detached physical connections and cancelled close observers. All 599
mobile unit and 109 integration checks, 239 storage library tests, workspace/all-
target Clippy with warnings denied, formatting, documentation and SDK packaging
checks passed. Fresh native-wrapper and exact-candidate hosted-CI results are
recorded before merge. This establishes
the mobile single-recovery-owner boundary; public scoped SDK handles, listener/
callback ownership, children/delegation, migration and ADR S1-S12 remain open.


### DME131 — Fresh generated bindings do not verify the committed SDK contract

The ownership candidate's hosted iOS tests crashed at UniFFI initialization. Its
shutdown documentation changed the generated method checksum from 65418 to 38962,
while the committed Swift wrapper still expected 65418. The local native harness
had substituted freshly generated Swift and therefore missed this packaging
boundary. Actual simulator evidence made the missing check visible.

All committed Swift, C-header and Kotlin copies now come from the same current
native metadata. The parity baseline identified four stale copies, including
older generated Swift/header declarations for token-installation tickets. A new
mobile binding check compares all five committed copies against fresh generation,
retaining code, documentation and checksum differences while normalizing only
formatting trivia. Generation explicitly disables ambient formatter tools so its
output is deterministic. CI checks parity before packaging bindings/frameworks.

The local native harness now tests the committed production Swift binding rather
than silently replacing it. All 32 strict Swift wrapper tests passed against the
actual current macOS library. An iOS simulator check with the original CI-built
framework is used to verify the repaired package before merge; hosted CI must
also pass the exact corrected candidate. This is SDK-contract validation, not
completion of the public scoped FFI surface.


### DME132 — In-process crash fixtures must observe physical teardown

The full hosted workspace run exposed the phone-local delegation restart fixture
reopening immediately after dropping its in-process MobileEngine. The old native
SQLite workers could still hold recovery authority, so the new constructor
correctly refused the open. The fixture's timing assumption had bypassed the
physical teardown boundary enforced by DME130.

The interrupted-phone fixture now waits, within its existing deadline, until it
can acquire and immediately release the same stable OS ownership lock. It never
unlinks the companion or weakens the production constructor. Both interruption
points still preserve the original parked workflow/delegation journal and all
exactly-once assertions. The fixture models in-process teardown; dedicated
abrupt-process crash coverage remains part of the wider S12 matrix.

All three phone-local delegation scenarios and eight related cloud/phone round-
trip scenarios passed against SQLite and a real isolated PostgreSQL service (22
backend legs), including kill during a native step, late completion after lease
loss, handoff refusal and offline delegation. The PostgreSQL service was removed
afterward. Focused all-target Clippy, formatting, binding parity, documentation
and SDK packaging checks passed before resubmission.

### DME133 — Legacy device sync must not adopt catalog-owned work

The scoped-canary review reproduced three distinct legacy protocol authority
failures using runs admitted through the actual scoped materializer: a scan
queued all four runs belonging to two accounts × two catalogs; a legacy cancel
command cancelled a catalog run; and a real HTTP sync sent pre-existing scoped
status/approval outbox rows under the legacy device credential without a new
scan. Baselines are retained in `orch8-legacy-sync-scope-baseline.log` and
`orch8-legacy-sync-http-baseline.log` in the task's temporary evidence directory.

Legacy reporting now positively requires the original `mobile` tenant and the
absence of both a catalog request and an independent materialization receipt.
This is not a string-prefix denylist. Active, terminal and explicit-ID status
projections use the same owner predicate. Before HTTP serialization, persisted
outbox keys and payload instance IDs must agree with a proven legacy instance;
unowned/mismatched rows are discarded instead of being sent or moved to another
account. The guarded scanner records `legacy_verified` atomically with each
new outbox row, so an offline report can survive native task garbage collection.
Startup adds this column idempotently to old outboxes with an unverified default;
an upgrade never invents an owner proof. Verified rows must still match their
key/payload ID and may never override retained catalog requests or receipts. An
unverified row with a missing native task cannot establish legacy authority and
is discarded. This conservative treatment is specific to the old unscoped
outbox; a new scoped outbox still needs its own durable ownership contract.

Every instance-targeting legacy command checks that same authority before any
replacement is started. Terminal state transitions include the ownership fence
in the atomic SQL write, clear stale claims, and cannot overwrite a terminal
result. Approval/delegation signals use the existing transactional caller-owner
signal API with the explicitly authorized legacy tenant; the encryption decorator
still encrypts their payloads. Late delegation results cannot enqueue into a
terminal instance. Scoped targets are permanently refused rather than retried
until they accidentally acquire authority.

This completes the demonstrated legacy reporter boundary only. It does not
implement a catalog-authenticated sync transport, scoped command idempotency,
scoped public reads/listeners, public callback context or the S1–S12 SDK rollout.

The final local mobile run passed 609 unit tests and 109 integration checks.
Ten new ownership regressions cover the four-scope scan, seven incoming command
paths, real HTTP replay of old outbox rows, owner corruption and retirement,
mismatched metadata, encrypted signals and late results, two-pool writer
contention, unavailable authority storage, native-history GC and idempotent old
outbox upgrades. All 44 reporter tests also passed as a focused suite.

### DME134 — Native callbacks need trusted immutable originating context

Scoped routing alone selected the right callback object but the existing
`execute(name,input)` interface gave a shared callback no trusted account,
catalog, run or effect identity. Workflow JSON and the currently selected UI
account cannot supply that authority.

The internal contextual callback path now supplies a separate immutable record
containing the frozen account/catalog IDs, original account execution epoch,
run ID, block, business attempt and durable effect ID. Its fields are private
and it is not deserialized from input. The effect capability projects an ID only
when its tenant/run/block/attempt match the original invocation; reading this
projection does not dispatch or consume it. Native claim, expiry, dispatch and
post-dispatch checks still run in the existing physically owned invocation path.

Production scoped registration requires the contextual trait. The previous
context-free scoped adapter exists only in test builds for older storage,
admission and lifetime fixtures. Legacy global host callbacks retain their
existing interface. Callback context holds ordinary immutable metadata rather
than an engine/runtime/handler reference that could create an ownership cycle.
The invocation adapter and accepted binding follow the existing foreign object
release path before physical callback and registration capacity returns. Missing
or mismatched effect authority fails before independently cloning the accepted
foreign reference out of the registry.

Real scheduler regressions register one callback object under identical names
in two accounts × two catalogs. Forged ownership and attempt fields remain in
ordinary input but cannot override the separately delivered owner; each delivered
effect ID matches its actual committed ledger row. A held callback retains its
old context through retirement/reopen, cannot publish its late result, and a new
run receives the new epoch and a distinct effect. A running claim alone cannot
invoke a contextual callback without durable effect admission. Core effect tests
also prove mismatched identity projections leave a prepared capability usable
for its original invocation.

This is internal native runtime functionality. It does not claim a public
UniFFI/Swift/Kotlin context contract, account/catalog/run SDK handles, scoped
read/lifecycle/listener completion, catalog-authenticated reporting, or S1–S12
completion. Those remain delivery requirements rather than optional follow-ups.

Local validation passed all 612 mobile unit tests, 109 integration checks, all
14 core effect-guard tests, and workspace/all-target Clippy with warnings denied.
Fresh generation from the rebuilt native library matches all five committed
mobile binding copies. These binding checks prove compatibility of the existing
exported SDK surface, not completion of the new scoped foreign callback API.

The first DME134 hosted unit run exposed a pre-existing synchronization race in
`retired_cleanup_retains_capacity_and_registration_charges_until_host_drop`.
All three new contextual callback tests passed in that run. The old cleanup
fixture dropped its observer, then treated an available physical semaphore slot
as proof that registration was ready. The retirement job releases that slot
before its priority token and completion metadata finish settling, so a correct
registration refusal could race the fixture's immediate rebind assertion.

The fixture now reattaches to the already-running owned cleanup receipt after
dropping its first observer. It waits for full completion before rebinding and
still asserts the blocked destructor's physical/registration charges, another
account's progress, observer independence and late cleanup's epoch isolation.
No runtime admission or cleanup invariant was relaxed. The repaired case passed
50 consecutive scheduling stress runs before the full suite was repeated.

A later hosted workspace run exposed an independent zero-timeout fixture race:
it expected a noop to complete within zero milliseconds even though claim
verification can suspend before the handler. That deadline may correctly fail.
The fixture now uses a permanently pending handler on a temporary file database
and checks bounded failure. All 100 engine edge-case tests, 100 isolated timeout
stress runs and targeted Clippy pass. Production runtime code is unchanged.
Stress also exposed a separate in-memory SQLite connection-lifetime issue,
tracked below rather than treated as fixed by changing this fixture's database.

### DME135 — Run reads, cancellation and legacy notifications need owner boundaries

Real catalog materializations exposed a second ownership gap: legacy lifecycle
get/list returned scoped runs and their private context, legacy cancellation
cancelled them, and legacy step completion merged output into their waiting
context. A real global listener also received scoped completion data. These
reproductions use two accounts and two catalogs rather than fabricated tenant
strings alone.

Storage now requires an explicit mobile access descriptor at the read/cancel
boundary. SQLite authorizes the actual read in one snapshot against the active
account session, catalog request, materialization receipt and original native
identity. Cancellation proves authority inside the same immediate transaction
as its state change, clears claim/wake metadata and preserves existing terminal
states. Fresh sessions can read earlier execution history; stale sessions lose
authority, and fresh sessions cannot mutate nonterminal work from an old epoch.
These descriptors are supplied by trusted runtime handles, not workflow JSON or
network bearer credentials. PostgreSQL supports the explicit legacy tenant and
rejects unsupported catalog access without a global-read fallback.

Internal run views bind their original account/catalog/session and weak runtime
reference. Closing a view or runtime prevents result delivery without retiring
the account or cancelling durable work. Reads pass through the encryption
decorator after backend authorization; cancellation never rewrites context.
Both methods are explicit fault-injection boundaries for decorator/race tests.

Legacy lifecycle reads, active lists, cancellation, approval reads and listener
projections now share the positive legacy-owner policy. Terminal hydration
rechecks that policy. Waiting projections guard JSON extraction, so malformed
foreign context cannot suppress legitimate legacy events. This closes the
legacy access paths; it does not add scoped approval or scoped listener APIs.

Eleven focused mobile regressions pass, including cross-account/catalog denial,
retirement while storage is paused, old-session rejection, fresh-session history,
view/runtime close during reads, encrypted access, inconsistent native identity,
missing materialization receipts, and real legacy listener isolation. Eight
plain/encrypted SQLite/PostgreSQL integration legs pass with PostgreSQL required
and actually running. All 623 mobile unit tests and 109 integration checks pass.
Workspace/all-target Clippy passes with warnings denied. All 239 storage unit
tests and 32 strict-concurrency Swift tests linked to the rebuilt macOS native
library pass; fresh generation matches all five committed binding copies.
Public foreign run handles, scoped approval/listener delivery and the remaining
ADR S1–S12 rollout are still open.

### DME136 — Waiting-step answers need one owner-bound commit

Two real legacy lifecycle regressions reproduced additional mutation gaps.
Rejecting the signal insert left newly merged answer fields in workflow data,
and naming a different step still merged output and woke the waiting instance.
The signal failure reproduction uses an actual SQLite insertion trigger.

The required storage completion operation now checks mobile ownership, waiting
state, observed row version and the current step inside the same transaction
that updates context, inserts the signal and schedules the next tick. A failed
insert rolls all changes back. Context and signal encryption are forwarded as
one operation; foreign/terminal/stale rows cannot be mutated. Legacy completion
also validates its pending step and refuses non-object context instead of
indexing arbitrary JSON. Its existing output-merge behavior is preserved for
accepted answers.

The internal catalog run view additionally requires the version from the
caller's waiting snapshot, rather than silently accepting a reply against a
newer snapshot. It retains its original runtime/session and rechecks local close
before publishing results. This does not add a version argument to the existing
legacy foreign API; a new public scoped reply contract remains part of the SDK
rollout. The engine's human-choice validation still occurs when consuming the
answer signal.

The initial stricter implementation incorrectly used the shared `current_step`
stamp as authority for every workflow. A real two-branch human-input workflow
proved that this rejects a valid pending branch: tree evaluators share that
telemetry slot. Completion now authorizes a durable waiting step node for tree
workflows and uses the current-step stamp only when no execution tree exists.
A completed tree branch cannot regain authority from a stale shared stamp.
The real workflow can answer both branches in either order and finish normally.

All 16 plain/encrypted SQLite/PostgreSQL completion regression legs pass with
PostgreSQL required and running. They verify successful context/signal/wake
commits, stale versions and replay, wrong steps, foreign/terminal targets, and
full rollback after a signal-ID collision. Scoped mobile tests verify four
account/catalog combinations and retirement/cancellation while the completion
operation is paused before storage authorization. All 629 mobile unit tests and
109 integration checks pass, as do all 239 storage unit tests. Workspace/all-target
Clippy, formatting, documentation and SDK packaging checks also pass.
Fresh native generation matches all five
committed binding copies. All 33 strict-concurrency Swift wrapper tests pass
against the rebuilt macOS library, including real committed-binding calls that
complete both parallel branches in either order and prove an unknown step makes
no context/state/version changes. Hosted CI remains pending for this change.

### DME137 — Memory database lifetime must outlive replaceable connections

An isolated stress run of the zero-timeout test failed after 17 passes with
`no such table: task_instances` in the evaluator. Five deterministic regressions
then lost real instance rows by retiring every pooled connection: all four
memory constructors, plus a raw pool alias retained after dropping its storage
wrapper. SQLite releases a memory database with its last physical connection;
replacing a pooled connection must not define application data lifetime. The
same boundary applies to normal idle/lifetime recycling in a long-running pool.
[SQLite memory database lifetime documentation](https://www.sqlite.org/inmemorydb.html)
explains that ownership boundary.

Memory pools now retain a passive connection through a private database owner.
The pool options keep that owner alive for retained aliases, while its owner
contains no pool reference. A close-event watcher owns an event listener and a
weak owner reference, never a pool clone. Raw pool close and the existing
independently owned storage close join the same keeper-close receipt. Cancelling
an observer cannot cancel physical cleanup, and closed retained aliases do not
retain the database contents. This adds one passive connection per memory pool;
on-disk recovery ownership and its physical SQLite-worker leases are unchanged.

Anonymous memory inputs receive a unique shared-memory name; explicitly named
shared-memory URLs preserve their identity. Encoded mode/cache parameters follow
the already validated SQLx input. The implementation does not serialize SQLx
0.9 options: that serializer panics on generated memory names. Memory pools with
an explicitly private cache and empty temporary-file paths fail explicitly,
rather than silently opening independent databases for different connections.
The existing `url` dependency is used directly for URI query decoding; no new
package version is added.

All 13 focused storage regressions pass. They cover repeated full connection
replacement, all constructors and URI forms, anonymous isolation, intentional
named sharing, retained aliases, cancelled close observers, raw pool close and
physical keeper release. A dedicated engine regression now runs zero-timeout
claim-query cancellation against memory storage; all 101 engine edge-case tests
and 100 isolated cancellation stress runs pass. All 252 storage unit tests,
629 mobile unit tests and 109 mobile integration checks pass. Workspace/all-target
Clippy with warnings denied, formatting, documentation and SDK packaging checks
pass. All five committed binding copies match fresh native generation; all 33
strict-concurrency Swift tests pass against the rebuilt macOS library, including
both parallel answer orders on both file-backed and memory engines. Hosted CI
remains pending; the broader scoped SDK rollout remains open.

### DME138 — Scoped history must include durable requests without hydrating private data

The legacy global instance list cannot represent an account/catalog history:
queued requests have no native instance yet, and native garbage collection can
remove a completed instance while its durable request/materialization receipt
remains. A scoped list must not adopt an unrelated row with a colliding ID or
parse another owner's private workflow context while projecting metadata.

Add bounded, owner-indexed keyset pages over durable catalog run requests. Each
page checks the active account/session and catalog in the same read transaction,
retains the original execution epoch, and excludes future-epoch rows. Cursors are
bound to the account, catalog and session; they provide pagination, never
authorization. The projection selects no request input or execution context.
A native state is returned only with a materialization receipt and the complete
native identity predicate shared with scoped get/cancel/step completion. Missing
or inconsistent native evidence reports `Unavailable`, without guessing an
execution outcome or allowing rematerialization. Pagination is a live keyset
view, not a snapshot across pages or a durable event stream.

The runtime history view retains only a weak runtime reference while idle.
Local view close and runtime shutdown cancel a pending read, including pool
acquisition, release its temporary runtime reference, and do not retire an
account. A deterministic pool-exhaustion test first reproduced a two-second
close timeout; the cancellation fix makes both local close and shutdown settle
without releasing any held database connection.

Eight focused tests exercise two accounts with two catalogs each, actual queued,
live, cancelled and garbage-collected runs, cursor/session boundaries, empty and
exact-size pages, future epochs, corrupt private input/context, inconsistent
native identity and missing receipts. An independent administrative file pool
retires/reopens the account while the first pool's read is waiting; the stale
read is rejected when it obtains a connection. This is not a second executing
engine or proof of independent recovery ownership.

This delivers the private storage/runtime history primitive. Public scoped
SDK handles, listeners, authenticated reporting, compact durable outcomes after
native GC and the remaining ADR S1–S12 matrix remain open.

Validation passes: 637 mobile unit tests, 109 mobile integration tests, all 252
storage unit tests and workspace/all-target Clippy with warnings denied. The
eight focused history tests pass again after the explicit-unit-pattern lint fix.
Formatting, documentation and SDK packaging checks also pass. No public FFI
surface or generated binding changes in this slice.

### DME139 — Expose an explicit-owner native SDK without losing physical admission

The ownership primitives now have a public Rust/UniFFI facade: account, catalog
and run objects, typed immutable-at-delivery callback identity, signed sync,
durable request preparation, materialization, owned snapshots/cancellation,
versioned waiting-step answers and bounded metadata history. Swift and Kotlin
bindings are generated from the same rebuilt native library. The development
contract and current rollout limits are documented in
[the scoped native SDK guide](../SDK_SCOPED_NATIVE.md).

The host supplies its trusted issuer/subject when opening an account. Signed sync
still independently verifies that principal and catalog before private downloads.
It uses the operation's provider and one shared per-engine coordinator, never an
ambient selected account. Handler routing returns the original execution epoch,
run, block, attempt and effect ID even when input JSON forges ownership fields.
Public snapshots revalidate the owned instance/version after reading its tree;
real waiting step nodes identify parallel approvals. Answers retain the backend's
single owner/version/pending-step transaction. No unsigned install or raw scope
constructor is exported.

Three real integration regressions were reproduced and fixed in this slice:

- Dropping a temporary account/catalog view implicitly cancelled an already
  issued run view. The regression failed with Shutdown. Issued children now
  survive temporary parent disposal, while explicit `close_view` still fences
  descendants and never retires durable work. Views retain weak engine owners.
- Cancelling sync freed its account slot before a blocked token getter finished.
  Retiring/reopening the same account could then enter another getter and consume
  the other shared token slot. Sync admission now follows both the logical task
  and the physical provider reference through destruction. A blocked getter or
  destructor keeps the old account charge; another account can sync. The lease
  holds no runtime, so it does not create an engine ownership cycle.
- Exporting `close()` collided with UniFFI's generated Kotlin AutoCloseable
  implementation. Kotlin 2.1.0 produced six conflicting-overload errors across
  the three handle classes. The explicit view operation is now `close_view`
  (`closeView` in Swift/Kotlin). Kotlin `close()` disposes only its native object;
  it does not retire the account or invalidate issued children.

Explicit account retirement owns its SQL/registry cleanup independently of the
calling observer, fences only its original epoch, and detaches accepted handlers
across every catalog. Running foreign code and its references retain physical
charges until they settle. Request preparation returns a durable handle before
dispatch, uses a mandatory idempotency key, and retains bounded history/effect
receipts after native GC. A missing native state makes no execution-outcome claim.

The signed public sync integration test uses the existing controlled loopback
transport fixture; production objects always construct validated public-HTTPS
transport and have no insecure configuration or injected synchronizer field.
It verifies four account/catalog combinations through actual signed manifests,
refuses a wrong principal before any content request, then starts their real
workflows. Separate public execution tests verify callback identity/effect IDs,
legacy/foreign read exclusion and parallel approvals in both orders on file and
memory engines. Other tests cover close during pool exhaustion, retirement across
catalogs, stale cursors/replies and disposal of temporary parent views.

This delivers the development native facade. Common KMP/RN/Flutter scoped
facades, change observation/listeners, public account credential management,
authenticated reporting, scoped workers/delegation, paired published artifacts
and the remaining ADR S1–S12 matrix are open. Host-JVM/macOS evidence is not an
Android-device or iOS-simulator claim. No release is published.

Validation passes: the full mobile suite has 646 unit tests and 109 integration
tests; all nine new public API regressions pass again after the exported close
operation is renamed. Workspace/all-target Clippy passes with warnings denied.
All 36 committed Swift tests pass against the final rebuilt macOS library with
strict concurrency and warnings as errors. Kotlin 2.1.0 compiles the generated
binding and two committed JVM native tests, which both pass against the same
library. Fresh generation matches all five binding copies; formatting, docs,
SDK packaging and whitespace checks pass.

### DME140 — Carry explicit scopes through the common KMP API and real Apple bridge

The common coroutine API now exposes trusted account identities, catalogs,
contextual handlers, per-call signed sync, durable run preparation/start,
owned snapshots/cancellation, versioned parallel answers and opaque paginated
history. JVM/Android adapters call the generated native objects directly.
Protocol 4 transfers owned child/cursor ports across the Swift/Kotlin boundary;
no global handle registry or ambient selected account is introduced. Callback
and token adapters retain their own immutable identities/providers. Scoped
objects keep no root engine reference. Disposing temporary parents preserves
issued children; explicit closeView and account retirement retain their fences.

The wrapper tests use an exported, checkpointed SQLite fixture with two accounts
and two catalogs each. Its workflows are installed only by the existing trusted
Rust test helper; there is no unsigned production install API. Both JVM and real
Kotlin/Native host flows execute these four workflows through the common facade,
check eight unique contextual effects, answer parallel waits in both orders,
reject stale replies and read scoped history. The Swift host links the actual
Kotlin framework, production Swift bridge and Rust library, without protocol
shape mocks. Native host evidence is not an iOS simulator/device execution claim.

Boundary regressions found while building this slice:

- A malformed callback identity was classified as retryable. It is now permanent
  before application handler entry; input JSON never supplies callback authority.
- A successfully opened bridge leaked its engine when protocol validation or
  callback installation failed. Cleanup now follows successful ownership
  transfer and preserves the original error even when cleanup itself throws.
- Kotlin/Native terminated the process on invalid configuration because the
  public factory did not export its exceptions to Swift. Public throwing engine
  and scoped operations now declare their exception contract; suspend exports
  include cancellation. The real host regression catches invalid configuration
  and verifies it creates no database before opening a valid engine.
- Completing a step on a disposed JVM run escaped the common error contract.
  It now uses the same disposed-view mapping as reads and cancellation.

CI exports the trusted fixture and runs the actual common/JVM native workflows,
the Kotlin/Native host flow, and the actual iOS framework/Swift typecheck. Android
CI stages this commit's AAR under a unique version and compiles the KMP Android
adapter against that artifact. This avoids silently validating development
bindings against the older published SDK. No release is published; all runtime,
bindings, KMP framework and protocol-4 adapter components must match.

Scoped listeners/observation, RN/Flutter facades, account credential management,
authenticated reporting, scoped workers/delegation, durable materialized-record
reclamation and the remaining ADR S1–S12 matrix remain open. Root legacy
observers deliberately exclude scoped runs. History still distinguishes a
missing native record from a known execution outcome.

Local validation: all 647 mobile unit tests and all 75 common/JVM tests pass
with no skipped JVM cases when native integration is enabled. The disposed-reply
regression fails before its mapping fix and passes in the full suite afterward.
The actual macOS native framework/Swift/Rust flow, actual iOS simulator framework
build and Swift bridge typecheck pass; strict Swift concurrency and warnings as
errors are enabled. Mobile all-target Clippy, Rust formatting, actionlint, docs,
SDK packaging and whitespace checks pass. Android source-cohort compilation is
passed in CI against this commit’s AAR (job 121310376983); no Android device
execution is claimed. The first native-host CI run exposed a missing root
`target/` directory in a fresh artifact-only checkout. The harness now creates
its work parent explicitly. A clean-root reproduction fails before this fix and
passes the real four-workflow Swift/Kotlin/Rust flow afterward.

### DME141 — Observe owned KMP runs without a polling spin or read queue

The three existing legacy KMP observers accepted zero, negative and infinite
intervals. Zero/negative delay can create a tight read loop; infinite delay can
leave an otherwise active observer permanently stale. A regression reproduced
the missing validation. Each observer now refuses these intervals before its
first backend read.

A scoped run now exposes `observeSnapshots()`, a cold common-code flow that
uses the existing authorized native snapshot operation at each poll. It retains
only the run view, applies collector backpressure, emits distinct snapshots and
completes on a known terminal state. It never routes through the legacy global
listener or active-instance list. Missing native state is emitted as null and
makes no outcome claim; callers cancel collection for an abandoned prepared
request or a departing screen. Retirement, close and backend errors remain
visible to the collector.

Controlled coroutine tests cover cold construction, distinct queued/live states,
terminal completion, missing-record uncertainty, slow collectors, cancellation,
close fencing and invalid intervals before reads. A real JVM/native integration
observes two workflows under different accounts through parallel approvals and
completion, checks every emitted owner/run/session and verifies terminal stream
completion. This supplies common KMP polling observation; native push listeners,
RN/Flutter observation and the rest of the full review matrix remain open.

Validation passes: 82 common/JVM tests with all native integration cases enabled,
73 common tests executed on the Kotlin/Native macOS host, and no failures/skips in
either suite. The real Swift/Kotlin/Rust four-scope flow still passes with the
updated framework. Actual iOS framework compilation and Swift bridge typecheck,
actionlint, documentation and whitespace checks pass. CI now runs the common
Kotlin/Native tests before linking its host framework. Production Rust and
bindings are unchanged in this slice.

### DME142 — Give Swift a safe lifetime and error boundary for scoped observation

An actual Swift/Kotlin/Native/Rust subprocess reproduced a process abort when
collecting the raw scoped Flow after closing its view. The generated Flow.collect
bridge converts only CancellationException; an ordinary typed Shutdown exception
is fatal at that foreign boundary. Annotating the flow factory cannot fix the
separate collection boundary.

Shared Kotlin keeps `observeSnapshots()`. Swift now has
`subscribeSnapshots(observer, pollIntervalMs)` and an explicitly cancellable
subscription. The raw scoped Flow is hidden from Objective-C exports. Snapshot,
error and completion callbacks stay within the typed observer protocol; broken
error callbacks cannot become unhandled native coroutine exceptions. Each
subscription owns and cancels its own task, clears its observer reference on
close/completion/error, and retains no root engine. Closing a subscription does
not invalidate a run or cancel durable work; an already accepted callback may
finish. The API accepts milliseconds directly, avoiding Kotlin Duration's ABI
representation in Swift calls.

Common tests cover completion, cancellation before scheduling, view closure and
throwing error callbacks. The real native host fixture now checks all four
terminal subscriptions and a closed-view error delivered to Swift without a
process abort. Native push listeners and the pre-existing raw legacy Flow Swift
exports still need their own supported observation boundary; this slice does
not claim that broader interop work is complete.

Final local validation: 84 common/JVM and 75 Kotlin/Native common tests pass,
with no failures or skips, including invalid millisecond intervals and overflow
before scheduling. The real Swift/Rust host fixture passes terminal and closed
subscriptions without an abort. The iOS framework/Swift bridge typecheck, docs
and whitespace checks pass. The generated header exposes the callback
subscription and omits the raw scoped Flow.

The final artifact-only CI host run also exposed Cargo's absolute debug dylib
install name (job 121316494872). The downloaded library linked successfully but
its loader still searched the absent producer `target/debug/deps` directory.
The host harness now normalizes and ad-hoc signs a private copy with an
`@rpath` install name; it preserves the input artifact and links/runs only that
copy. An isolated library with an intentionally nonexistent producer path
reproduces the loader abort before this fix and passes the real four-scope Swift
workflow/subscription fixture afterward. The source library checksum is unchanged.

### DME143 — Finish the KMP Swift observation boundary and own its teardown

A second actual Swift/Kotlin/Native/Rust subprocess reproduced the legacy raw
Flow abort: a root observer requesting a scoped instance correctly receives
NotFound from native authorization, then terminates when that typed error crosses
raw Flow.collect. Root instance, active-instance, delegation, lifecycle-event and
pending-step streams now have typed Swift subscriptions with ordinary error and
completion callbacks. Shared Kotlin keeps its Flow API; raw Objective-C exports
are hidden after the complete callback replacements are supplied.

All subscriptions share one callback/lifetime implementation with the scoped
observer. Root subscriptions are children of an engine observation job. Engine
close cancels their polling/collection and refuses new subscriptions; closing one
subscription never cancels siblings or durable work. Callback error handling and
retired-reference clearing also cover cancellation before the task starts.

A controlled test found that returning a newly scheduled hot subscription could
lose an event produced on the next line. Hot collectors now attach undispatched
before return, then yield before delivery so callbacks still use IO. The
readiness regression fails before this change and passes afterward. StateFlow's
initial pending-step value and ordinary event non-replay semantics are preserved.

A retained listener also pinned its common engine wrapper through the listener
lambda's outer `this`. A JVM weak-reference regression reproduces that pin while
retaining both the callback owner and readonly event flow. The listener now
captures only its mutable flow state; an already accepted callback can settle
without retaining or resurrecting the wrapper. This does not claim native
backend/resource GC destruction on all platforms; application shutdown remains
explicit.

Behavioral tests cover terminal/error delivery, throwing error callbacks,
independent cancellation, engine-wide stop, late events after close and immediate
hot-stream readiness. The real Swift host test verifies legacy error/terminal
subscriptions without aborting, absence of private events/pending steps through
four scoped workflows, positive legacy event delivery and refusal after engine
close. Generated callback names are verified by compiling the actual framework,
not by protocol shape stubs. Native artifact relocation uses the repaired
private-copy harness from PR329.


The shared error boundary also reproduced a callback failure whose custom
exception message getter threw before error delivery. Non-native callback
failures now use a fixed diagnostic with the original cause; the SDK does not
invoke that host getter. Cancellation is checked before each normal/completion
delivery and before error reporting. The regression fails before this change.

Final local evidence: 90 common/JVM tests and 80 Kotlin/Native common tests pass
with no failures/skips. Actual Swift/Kotlin/Rust execution passes private-event
exclusion, legacy completion/error/event delivery and shutdown refusal. The
actual iOS framework build and Swift bridge typecheck, SDK packaging, docs and
whitespace checks pass. No production Rust or generated UniFFI changes in this
slice. Root SDK behavior, scoped observation and both language boundaries use
the same tested callback lifetime implementation.

### DME144 — Remove the midnight-only false assumption from cron CI

The full Postgres E2E run passed 1587/1588 tests, but failed the cron-update
assertion at 23:59 UTC (job 121328635262). A daily-midnight expression and an
every-minute expression legitimately share their next midnight occurrence;
increasing frequency does not always move next_fire_at strictly earlier.
A deterministic Rust test confirms this exact boundary.

The HTTP regression now starts with a disabled UTC schedule about twelve hours
away, updates it to every minute, and checks both the stored expression and a
future fire within one minute of the update. This proves rescheduling rather
than weakening the comparison at the coincident boundary. Cleanup runs even
when an assertion fails. The targeted Rust boundary test, TypeScript typecheck
and all seven cron edge-case tests pass against the actual server and an isolated
SQLite database. The repaired #330 head also passes the full Postgres CI E2E
gate. Production cron behavior is unchanged.


### DME145 — Explicit acknowledged archives release admission without replay

User policy: archive only after app/backend acknowledgement, retaining compact
operation-key and outcome receipts. Completed catalog requests previously kept
consuming the 256-record hot catalog allowance even after native GC. Automatic
removal would permit replay or lose the distinction between a recorded outcome
and missing native data.

The archive operation takes an explicit trusted host acknowledgement reference.
It validates the current account/catalog session, frozen materialization identity
and native owner inside the same writer transaction as compaction. Active and
never-materialized requests return no receipt. A previously purged materialized
run can archive only with an unknown outcome. A terminal native row contributes
its actual state. The reference is hashed; neither raw acknowledgement nor input
is copied into the receipt. This is a host acknowledgement contract, not remote
credential verification.

Hot admission uses a separate covering partial index excluding archived markers.
Archival clears staged input to JSON null and releases its sequence FK, retaining
request identity, operation key and original materialization fingerprints. An
identical prepare retry recovers the archived ID; changed requests and start are
refused. The stored dispatch-disabled tag remains cancelled for existing schema
constraints, while public history explicitly reports Archived and Recorded
outcomes. Archived is not an execution cancellation. Native contexts, trees and
effect evidence are preserved for their existing GC policy.

Schema 56 adds a compact receipt ledger and a nullable FK marker on requests.
The upgrade explicitly adds the FK-bearing column before generic reconciliation,
which otherwise discards REFERENCES clauses. Both FK directions preserve evidence.
Cold record/encoded-receipt ceilings are independent and shared: 65536/32768/16384
records and 64/32/16 MiB globally/per account/per catalog. Each receipt is at most
4 KiB. Overflow refuses archival, leaving hot admission and input unchanged.
Permanent deduplication has finite retention capacity; keys are never silently
evicted. Original bounded key/metadata and materialization records remain durable.

The public native API, direct JVM adapter and Swift JSON adapter expose the same
acknowledged operation. Protocol 5 requires matched framework/adapter binaries.
History distinguishes a recorded outcome from a live native snapshot, and keeps
an unknown outcome unknown after GC.


The archive ownership review reproduced a subtenant collision: changing a native
row's subtenant still let it supply the original root catalog run's outcome.
The new regression fails before repair. The shared native identity predicate now
requires no subtenant and no parent, matching materialization's immutable root
identity. Read/answer/cancel/history and archive use the same exact predicate.
The covering-index regression also caught the first partial-index draft reading
table rows. Including its nullable marker restores a genuinely covering hot
quota plan, while archived receipts do not enlarge that scan.


Final local validation: 657 mobile and 252 storage unit tests, 109 mobile
integration/example/coverage tests, 92 JVM tests and 82 Kotlin/Native tests pass
with no failures/skips. The final scoped suite passes 127 tests after lint
cleanup. All-target mobile/storage Clippy passes with warnings denied. The
current-source native library passes all five generated binding comparisons
and the strict Swift/Kotlin/Rust four-scope host fixture, including archives,
receipt retries, recorded history and archived-key no-restart behavior. Actual
iOS framework/Swift bridge, SDK packaging, docs and whitespace checks pass.
The ten archive regressions include a populated v55 upgrade/reopen, input/quota
recovery, all cold quota dimensions, write failure after receipt insertion,
corrupted evidence, unknown outcomes, concurrent calls, foreign/retired owners
and the reproduced subtenant collision. No release is published.


### DME146 — Scope subscription retirement follows engine shutdown immediately

Root KMP subscriptions already belonged to the engine's observation job, but
scoped snapshot subscriptions created independent polling jobs. A new common
regression reproduced prepared and reopened run subscriptions continuing reads
after engine close. A JVM WeakReference regression also reproduced the observer
remaining retained through its one-day polling delay, even while the run and
subscription objects were intentionally kept alive. Both fail before the fix.

Account, catalog and run views now propagate only the existing observation Job
token. A scoped SDK subscription attaches its own supervised lifetime directly
to that shared owner; engine close cancels it during long delays or before its
first dispatch. New scoped subscriptions after close return typed Shutdown.
No per-view child job groups are created, and no root engine reference is passed
through the views. Closing one subscription preserves its sibling and the run;
parent view disposal continues to preserve issued child views and durable work.

Tests cover prepared/reopened view propagation, silent cancellation, shutdown
before the first scheduled poll, sibling independence, observer release with
retained handles, and a retained scope token not pinning the root wrapper.
The GC tests concern JVM wrapper/callback ownership, not native resource or ARC
cycle destruction on every platform. Caller-created Kotlin Flow collectors keep
their own coroutine lifetime. The real Swift/Kotlin/Rust fixture also subscribes
a queued run with a one-day interval, closes the engine, checks no shutdown
completion/error delivery and verifies new scoped subscription refusal.
Final local evidence: 97 JVM tests and 85 Kotlin/Native tests pass with zero
failures/skips. The actual Swift/Kotlin/Rust host fixture and iOS framework/Swift
bridge checks pass, along with binding parity, SDK packaging, docs and whitespace.
No Rust, UniFFI or bridge protocol change is needed.


### DME147 — Keep host callback failures inside the foreign-language contract

Six regressions reproduced ordinary exceptions escaping error envelopes: unknown
handler/cancellation exceptions with throwing message getters, root node and
manifest cached-token getter failure, scoped handler/token failure, and direct
UniFFI token-refresh conversion. Separately, the actual Swift compiler rejected
a token source with throwing methods despite refresh documentation requiring
Orch8Exception on failure. These were independent failures, not inferred from
an interface shape.

A shared callback diagnostic helper preserves messages only from final SDK error
classes and exact standard exception classes. Unknown host subclasses use the
fixed diagnostic; neither their message getter nor toString is invoked. A real
existing regression protects ordinary IllegalStateException("camera busy")
diagnostics, and a subclass with an overridden getter is still quarantined.
Root/scoped coroutine handler normalization keeps the original cause and
Permanent/Retryable classification. JSON and direct UniFFI handler/token ports
use the same policy. Cached-token callback failures consistently return empty,
matching the existing direct UniFFI/scoped policy; credential admission refuses
that empty value before authenticated transport. No ambient account fallback is
introduced.

Public token-source methods now export Throws so Swift implementations can
report a credential failure. Protocol 6 requires matched framework/adapter
artifacts. Actual Apple compilation caught a global Objective-C selector
collision with the nonthrowing scoped token port; its methods now use explicit
cachedToken/refreshEnvelope names. This avoids generated underscore renaming.

Common tests exercise both root token roles, scoped handler normalization,
unknown/cancellation getters, known error preservation and direct UniFFI refresh.
The Swift fixture forwards every operation to the real production bridge. Its
root sync instrumentation also exercises the installed production credential
callbacks before their operation retires; a throwing Swift token source verifies
nonthrowing cache/error-envelope results without leaking its private diagnostic.
Four scoped sync probes also reject a throwing getter before private transport.
Final local validation: 106 JVM tests and 93 Kotlin/Native tests pass with zero
failures/skips. The Swift throwing-token conformance probe that failed before
repair now compiles against the real framework. The strict Swift/Kotlin/Rust
four-scope fixture passes both installed root callbacks, scoped getter refusal,
all contextual workflow/approval/archive flows and shutdown. The actual iOS
framework/Swift adapter, binding parity, SDK packaging, docs and whitespace
checks pass. No Rust/UniFFI binding or release change is involved.


### DME148 — Dispose the JVM native owner after logical shutdown

The retained-closed-facade database reopen test passed, but a separate real JNI
resource test failed: retaining sixteen additional closed facades grew the Unix
process descriptor count from 56 to 104. The adapter called MobileEngine.shutdown
without disposing its generated UniFFI owner. SQLite shutdown could complete
while each retained native engine still owned its Tokio runtime resources.

The JVM/Android adapter now disposes that owner in finally, including cleanup
failure paths. The generated call-reference counter protects already reserved
native invocations. A volatile closed fence covers all root backend calls and
maps a concurrent disposal IllegalStateException to typed Shutdown only after
close wins. No lock is held across a native call or foreign cleanup. Repeated
close remains idempotent; token-source retirement keeps its existing lifecycle.

The repaired descriptor test measures 53 before and 53 after seventeen closed
facades remain deliberately reachable. The Unix descriptor assertion allows
bounded ambient process variance and waits for asynchronous physical teardown;
it does not invoke GC to make disposal pass. A separate real native test verifies
reopening a closed facade's database and a post-close root API matrix verifies
typed errors rather than raw disposed-handle exceptions.

An actual blocked host callback also survives logical close during recovery.
The successor registers the same handler and ticks the old work without invoking
it again; a fresh control workflow completes once. Returning the original host
callback still causes no replay. This tests durable dispatched-effect evidence,
not a rule that the SQLite lease must wait for every foreign callback: that lease
follows physical SQLite lifetime. The predecessor's accepted call must also
settle without leaking an untyped disposal exception.

Only the shared JVM/Android adapter and JVM integration tests change. Common,
Swift, native Rust and UniFFI contracts are unchanged; protocol 6 remains current.
All 111 JVM tests pass with zero failures/skips, including the real native
integration fixture and the new retirement/recovery cases. An injected shutdown
failure also proves disposal happens while the original failure is preserved;
repeated close and typed post-close errors remain correct. Documentation,
packaging, binding parity and whitespace checks pass. The descriptor measurement
is Unix-host coverage and does not claim an equivalent OS resource counter on
every platform. Android consumer compilation remains a required hosted CI gate.


### DME149 — Retire Android framework bridge owners deterministically

The React Native and Flutter Android bridges shut down the native engine without
closing its generated UniFFI owner. Replacement and framework detach could retain
native runtime resources. React Native invalidation also raced initialization,
allowing a candidate to publish after its JS runtime had disappeared. Flutter
constructed and shut down the engine while holding the initialization lock, and
its terminated work executor could not serve a reused plugin attachment.

Both independently distributed bridges now have a matching internal lifecycle
controller. A dedicated serial lane constructs and disposes native owners outside
the state lock. Initialization, shutdown and terminal detach synchronously revoke
publication eligibility; superseded candidates are disposed exactly once. Every
retired owner closes in finally even if logical shutdown fails. Normal engine
work retains a separate lane so host callback reentry does not wait behind its
own lifecycle operation. Completion callbacks run outside the state lock.

Generation scopes weakly reference the lifecycle controller. Retired callbacks
refuse event delivery and cached credentials, including Flutter callbacks posted
to the main queue before detach. Late handler/token replies cannot refresh the
retired generation. React Native retires pending replies on replacement/shutdown
and converts work-pool admission races to Shutdown. Flutter creates fresh work
and lifecycle owners on reattachment and gates old asynchronous replies against
the attachment that accepted them.

Each bridge has five deterministic lifecycle regression tests using the actual
generated MobileEngine NoHandle test constructor: supersession and scope
retirement, detach during construction, configuration failure followed by a
successful retry, shutdown failure with mandatory disposal and one completion,
and cross-thread lock acquisition during construction/closed completion.
Both native Android modules compile and all ten tests pass against the current
three-ABI AAR, producer-generated Maven POM, React Native 0.88.0 and the real
Flutter embedding. These are host JVM ownership tests; Android device execution
and framework-driven attach/detach integration remain separate coverage.

The Mobile workflow now runs this consumer check after publishing its current
commit's AAR to an isolated local Maven repository. Exclusive dependency routing
prevents fallback to a released Orch8 AAR. Flutter embedding acquisition pins its
engine commit and verifies the extracted jar's SHA-256; no protocol stubs or
external package publication are involved. Rust, UniFFI and protocol 6 are
unchanged. This gate supplements the existing JavaScript/Dart checks.


### DME150 — Preserve token callback admission across closed engine owners

The root credential object bounded refresh and manifest getter work to two
physical callbacks, retaining each local permit through foreign destruction.
Closing an engine and creating a successor allocated a fresh credential gate,
so non-cooperative retired callbacks could accumulate outside that bound. A
regression using distinct retired credential owners reproduced successor host
entry while the shared physical allowance was exhausted.

Every credential now also reserves from a process-wide 32-call semaphore before
queueing refresh or manifest getter work. The ceiling matches the largest mobile
runtime blocking pool; the existing two-call local ceiling still leaves room
for other work in each engine. Admission is immediate, with no waiting queue or
refresh backoff penalty on capacity refusal. A failed process reservation returns
its local slot. Closing one credential closes only its local gate and never
closes the shared process gate.

Both permits travel with the physical callback through queueing, timeout,
owner retirement and foreign provider destruction. A returning callback or an
unwinding callback/destructor releases admission. Refresh keeps its existing
fail-closed boolean behavior; a manifest getter reports ResourceLimit before
entering the host. No token identity or authorization ownership is shared
between engines.

Regression coverage includes mixed refresh/manifest refusal across closed
owners, recovery after physical return, no local-slot leak on global refusal,
capacity retained through a retired provider destructor, and permit return on
panic or queued cancellation. A separate subprocess exercises the actual
default constructor and 32-call static process gate without saturating unrelated
parallel tests. The smaller deterministic test injects a private shared gate
while using the same production callback admission path.

This bounds internally scheduled token callbacks, not arbitrary threads a host
creates to call a synchronous cached-token setter. Scoped handler capacity is
already shared across engine owners; legacy handler and listener process
ceilings remain separate review items. Rust implementation changes do not
change the UniFFI surface or protocol 6.

Local validation passes all 659 mobile library tests and 109 integration,
coverage and example-workflow tests, with zero failures or skips. The real
default process-capacity subprocess passes as part of the full library suite.
Committed binding parity, SDK packaging and documentation checks also pass.
Strict mobile all-target Clippy with warnings denied and workspace formatting checks pass.


### DME151 — Retired listeners retain process admission without consuming unseen events

MobileNotifier bounded running and queued callbacks per engine, but every new
notifier allocated fresh capacity. Real terminal/waiting event scans reproduced
successor callback admission while retired owners still held blocked host calls.
The baseline marked nine successor events seen while the intended shared
physical allowance remained occupied.

Listener callback admission now reserves both the existing two-call local gate
and a process-wide 32-call gate. Closing a notifier only closes its local gate;
the shared allowance follows accepted physical callbacks through host return and
foreign listener destruction. Reservations use immediate try-acquire, with no
process waiting queue. Failed process admission returns the local reservation.

Both terminal and waiting paths reserve before deduplication, so capacity refusal
does not consume undelivered events. Terminal cleanup tracking remains independent
of callback capacity. The mixed-event regression retains callbacks from distinct
closed notifier owners, verifies zero successor dedup entries and continued
cleanup, then releases physical work and drains all twelve terminal/waiting
notifications exactly once.

Additional coverage runs the real default 32-call static gate in an isolated
subprocess, retains admission while a retired listener destructor blocks, and
verifies capacity return after a callback panic or destructor panic. A queued
callback cancelled by close returns both permits without entering host code.
These panic probes use Rust callback implementations; they do not claim recovery
from arbitrary foreign ABI exceptions. The new capacity tests live in a dedicated
child module and reuse the existing real SQLite-backed notifier fixtures.

The process listener allowance is independent of token and scoped-handler
allowances, so one callback role cannot consume another role's process permits.
Legacy root handler admission remains a separate review item. The UniFFI API and
protocol 6 are unchanged; no release publication is involved.

Local validation passes 662 mobile library tests and 109 integration/coverage/
example-workflow tests with zero failures or skips. The final notifier suite
and strengthened token subprocess probe also pass after adding explicit child
completion markers; a subprocess matching zero tests can no longer pass silently.
Strict all-target mobile Clippy, workspace formatting, binding parity, SDK
packaging, docs and whitespace checks pass.


### DME152 — Reserve legacy native admission before scheduler or worker dispatch

Legacy native callbacks had no process-wide physical gate. A fresh registry or
engine could create more native frames while retired callbacks remained blocked.
Real flat and tree scheduler regressions reproduced dispatch despite an exhausted
shared allowance: both completed work instead of rescheduling before host entry.

Legacy registrations now share a process-wide 32-call semaphore. Scoped callbacks
retain their existing account-aware admission domain. An explicit invocation
permit variant keeps these domains distinct while preserving the native kernel's
ownership through physical return, foreign destruction, panic cleanup and queued
cancellation. Dry runs reserve no native slot. A stopped or invalid adapter fails
closed. Straggler contention and capacity refusal preserve AdmissionBusy through
the deferred effect capability, rather than consuming business retry attempts.

The scheduler tests retain one Prepared effect and attempt zero over forty
contention cycles, use the fixed admission delay rather than growing storage
backoff, then commit the same receipt with one host execution when capacity
returns. Separate tests retain physical admission across retired registries and
verify the actual default static gate in an isolated subprocess.

Worker review exposed a second ordering problem: execute_confirmed journaled
started before invoking the wrapper. With physical capacity exhausted, the old
ordering wrote a start marker even though no host code ran. A SQLite audit trigger
reproduced this false start. The journal is local; no separate remote mark-start
request is implied by this finding.

HandlerRegistry now offers opt-in synchronous native preparation. Its constructor
may reserve admission but must not enter host code or start effects. The owned
future executes only when polled. Ordinary handlers and mocks keep their existing
invocation boundary. Replacement, retention and mock precedence cannot leave a
stale native preparer active. Root native adapters provide a real reservation;
scoped routing continues through its existing ownership checks.

Workers prepare before journal start, release refused claims as unstarted, and
retain rejected releases as unstarted recovery evidence. The reserved future
survives journal/lease I/O and enters native code only after a successful start
record. Cancellation or journal-start failure drops the unpolled reservation
without invoking the host. Prepared callback field order retains admission through
last foreign-reference destruction, including cancellation before first poll.

A process availability hint suppresses foreign-handler polling while every slot
is occupied. It is not authority: an isolated real-default worker test exhausts
capacity during a delayed poll response, then verifies unstarted release, no
start/failure/host entry, and one execution after capacity recovers. This avoids
both claim/release churn under sustained exhaustion and a check/use race.

Regression coverage uses actual native adapters, SQLite journals and effect
receipts, plus the existing mock control-plane lease endpoints. It covers normal
and unpolled foreign destruction, callback/destructor panic cleanup, queued
cancellation, journal failure, cancellation before journal connection acquisition,
rejected unstarted release, metadata forwarding, and registry replacement/mocks.
No serialized admission capability, account fallback, UniFFI API or protocol 6
change is introduced.

Full local validation passes 2,120 core library tests, 675 mobile library tests
and 109 mobile integration/coverage/example-workflow tests, with zero failures
or skips. Strict all-target core/mobile Clippy passes with warnings denied.
The final worker preparation tests also verify an actual rejected release
request, rather than only checking its absent start/outcome state. Workspace
formatting, binding parity, SDK packaging, docs and whitespace checks pass.

### DME153 — Fence Apple bridge initialization and release native ownership

The React Native and Flutter Apple bridges could finish constructing and publish
an engine after framework invalidation/detach. Their construction lock serialized
initializers but did not permanently close the bridge. Retained listeners and
credential callbacks also lacked a generation fence.

Both packages now use the same independently shipped lifecycle controller.
Reservations synchronously revoke the previous generation, while a serial lane
constructs, configures, retires and releases native owners outside the state
lock. Publication checks the reservation again after configuration. Permanent
detach refuses subsequent initialization and disposes an in-flight candidate.
Replacement releases the old ARC owner before constructing its successor.
Scopes weakly reference the controller, so a retained callback cannot keep the
engine alive. Ordinary engine operations use the independent work queue.

Handlers, listeners and token providers carry the issued generation; retired
callbacks and late replies are refused. Flutter checks queued events and obtains
its method channel on the platform thread, and detachment unregisters both
channel handlers. React Native invalidation fails pending replies and suppresses
later lifecycle completions.

Seven Foundation-only lifecycle tests run independently against each package's
controller copy. They cover queued supersession, detach during construction and
configuration, configuration failure/retry, replacement ARC ordering, lock
reentry and weak ownership. A separate test compiles the actual Swift SDK against
the same-commit Rust library, retains 17 closed controllers and scopes, reopens
one file database with stable runtime identity, and verifies native peer release.
Both copies keep file descriptors at 4 before and after the loop on macOS.

CI also compiles the full Swift SDK, both actual iOS adapter source sets, and the
React Native Objective-C extern declarations against iOS 16 simulator SDK APIs.
Official React Native 0.88.0 and pinned Flutter engine frameworks are downloaded
with SHA-256 verification; companion React headers come from its locked npm
package. Swift warnings remain fatal, with only upstream Clang nullability and
incomplete-umbrella diagnostics suppressed. This validates native compilation
and host-runtime retirement, not device UI attach/detach integration or complete
ADR-0002 readiness. No Rust/UniFFI protocol or release artifact is changed.

### DME154 — Explicit React Native views and physical JS settlement

The development RN facade now exposes account, catalog, run and history-cursor
objects, scoped handler registration and credential sync, versioned answers,
cancellation and explicit acknowledged archive receipts. Native descriptors and
callback epochs use decimal strings to preserve signed 64-bit precision in JS.
Opaque native handles carry immutable ownership; no operation consults the UI's
current account. Cursors are checked against their issuing catalog/session.

Both native adapters bound their view tables at 4,096 globally, 1,024 per stable
account and 512 per catalog. Closing a view reclaims its descendant entries;
retirement reclaims the matching account epoch, including sibling views.
Disposal preserves issued children. ARC/JNI cleanup stays outside table locks.
A child returned after a concurrent parent close is explicitly disposed by JS.
Old retirement replies cannot close a replacement epoch, and JSON serialization
reentry is rechecked before native dispatch.

Handlers use unique registration keys and immutable callback ownership, with
serialized replacement per binding. Token sources use per-operation keys.
JS host promises keep physical admission through actual settlement: each loaded
SDK module permits 32 unsettled calls per handler/token role and 2 per stable
account. These bounds do not coordinate independent JS runtimes. Registration
queues are bounded at 1,024 and sync bridge requests at 32 across replacement.
Error handling reads own data fields instead of invoking hostile error getters.

Scoped reply slots are separate from legacy pending replies. Rust enforces the
logical operation deadline, while Swift/JVM foreign callbacks wait for actual JS
settlement. Root teardown cannot synthesize completion of an already-entered
host promise and free its native permit early. The real Swift/Rust test holds a
callback through its native deadline and root retirement, then verifies that its
late reply releases the physical pending slot.

Local evidence includes 36 JS tests, TypeScript checks, actual iOS adapter and
Objective-C extern compilation, both Android adapter compilations and 10 Android
lifecycle unit tests against regenerated Kotlin signatures. The local Android
AAR is explicitly compile-only; device JNI execution is not claimed. CI builds
and tests against its real same-commit AAR. The actual RN Swift adapter executes
four workflows across two accounts/two catalogs each against the Rust-produced
database fixture, checking eight callbacks, owned approvals, stale replies,
archive acknowledgement, cursor/principal isolation, default per-owner/global
view ceilings and reclamation. No scoped native event observer, account-owned
worker/delegation or Flutter scoped facade is claimed by this change.

### DME155 — Decode foreign callback errors instead of panicking

The late RN callback test exposed a native panic despite exit status zero:
`Can't lift flat errors`. MobileError and HandlerError used UniFFI flat errors,
whose Rust lifting implementation panics. Host retryable/permanent failures and
token refresh errors therefore lacked a valid bidirectional wire contract.

Both errors now use structured variants. The payload is named `detail`, avoiding
the Kotlin generator's collision with Throwable.message. Shutdown retains a
payload too; its native default display remains `shutdown in progress`. Rust
constructors/patterns and all Swift adapters are updated, and all five committed
binding copies match current-library generation. The KMP and Android bridges
extract native detail while preserving existing public error kinds. SyncError
remains an outbound-only flat error.

A new callback-contract-v2 export adds an initialization checksum. The actual
new Swift SDK is tested against PR339's native artifact and refuses its missing
contract symbol before host execution, then passes against the matching current
library without a panic. Direct Rust lower/lift regressions preserve both handler
classifications and token/shutdown detail. A real Kotlin/Rust workflow also
verifies permanent host failures through the foreign callback boundary.

Validation passes 677 mobile library tests, 109 integration/coverage/workflow
tests, 112 JVM tests with no skips, and strict all-target workspace Clippy.
The native RN test now treats panic output as failure even when the process exits
successfully. This is an unpublished development ABI change requiring paired
bindings/native artifacts; it does not publish or replace the 0.7.1 release.

### DME156 — Reject foreign health responses during process startup

PR340's two-scheduler CI failure reported a missing sequence, while its child
server log showed `Address already in use`. The harness selected ports by binding
and immediately dropping listeners, then accepted any successful health response
on the chosen HTTP port. A different test server could therefore satisfy
readiness while the intended child failed to bind, sending API calls to an
unrelated database.

HTTP and gRPC candidates are now reserved simultaneously to keep them distinct.
Readiness checks child exit status and requires this generation's own successful
HTTP-bind log before probing health, then checks child liveness again. Initial
startup retries at most four fresh pairs, only for an explicit listener bind
collision. Restarts preserve the already advertised addresses and fail clearly.
A real-process regression occupies each listener port with a foreign HTTP mock,
requires startup on a fresh pair, and proves the foreign endpoint received no
requests. The complete three-test crash suite passes with SQLite and an isolated
PostgreSQL database, including the exact failing two-scheduler case. The temporary
PostgreSQL cluster was stopped and removed after validation.

### DME157 — Expose explicit Flutter account and catalog views

Flutter now wraps development scoped native objects through typed Dart account,
catalog, run and history-cursor views. Handler bindings and token sources belong
to their exact scope/operation; session epochs stay strings. Callback DTOs retain
native owner/effect identity, answers carry the snapshot's exact update version,
and archives require acknowledgement. Account retirement fences sibling views and
late replies; an old retirement completion cannot close a freshly reopened epoch.
Closing a parent disposes a native child returned after its observer was fenced.

The Dart facade limits pending registrations, handler tables, sync calls and
physical host callbacks. Root replacement detaches old registration queues but
retains charges for unsettled host futures. Permanent errors use the final
CatalogHandlerFailure type; unknown host errors are bounded without invoking
arbitrary string conversion. Temporary disposal preserves children while
closeView fences descendants.

Swift/Kotlin native helpers now depend on small platform callback interfaces.
Flutter and RN carry equivalent helper copies; both actual Swift copies execute
the Rust-produced four-scope fixture independently in CI. Native pending slots
remain charged until a real Dart/JS reply, including after Rust timeout or root
retirement. Argument envelopes allow 4 MiB so a legal near-1 MiB input containing
escaped backslashes remains admissible after bridge JSON encoding. The actual
maximum-input case passes through both adapters.

Local validation passes Flutter analysis and all 20 tests, including the 1024
pending-registration boundary and recovery, late retirement, orphan cleanup,
callback fairness and exact answer/archive arguments. All 36 RN JS tests and
TypeScript checks pass. Actual iOS 16 adapter compilation passes; both Android
adapters compile and 10 lifecycle tests pass against current generated Kotlin
signatures. The local AAR is compile-only; device JNI execution is not claimed.
Strict workspace Clippy and docs/packaging/version/actionlint checks pass.
No artifacts are published. Native scoped observations, account-owned workers,
delegation/reporting and the remaining ADR-0002 rollout matrix remain required.

### DME158 — Exercise scoped recovery after actual process death

Existing mobile kill/reopen cases dropped an engine inside the test process.
That did not prove OS-level ownership-lock release, SQLite recovery without a
graceful close, or callbacks dying without their destructors running. A new Unix
subprocess case prepares four runs across two accounts/two catalogs, reopens them
in a child, and holds two native callbacks after fsyncing their effect identities
to an external file. Both accounts enter under the physical fairness allowance.
A second engine is refused while the child owns the database; the parent then
SIGKILLs and reaps that process.

After restart, account/catalog IDs, session epochs and operation-key run IDs are
unchanged. The two ambiguous effects fail without another host entry, while the
two undispatched runs complete. Every owned snapshot remains isolated from the
other catalogs and legacy reads. The child is killed/reaped even if an assertion
fails, and the temporary database/effect log is automatically removed. Handlers
for all owners are restored before scheduling, so the test does not itself
introduce unrelated missing-handler deferrals.

The full-suite run also exposed a racy existing destructor assertion: a try_lock
probe could observe another legitimate close caller and falsely report that the
foreign destructor held its own registry mutex. The test now requires reentrant
close to return before its bounded deadline, directly proving the deadlock
property, and removes the obsolete test-only probe.

All 678 mobile library tests and strict all-target mobile Clippy pass. This closes
one independent-process evidence gap; it does not complete account-owned remote
workers, authenticated reporting, migration/disk-failure cases or ADR-0002 S1–S12.

### DME159 — Node callback error reporting executed untrusted host hooks

Actual Node/native regressions against the baseline wrapper invoked thrown-value
accessors and proxy traps while formatting a failure. A serialization exception
also replaced the promised TypeError with a proxy's own exception. The wrapper's
catch block could therefore reject instead of returning its JSON error envelope,
and hostile hooks could run again during error reporting. Large diagnostics and
lone UTF-16 surrogates also lacked a bounded, native-decodable error contract.

The wrapper now normalizes only own data properties, excluding proxies with
Node's native proxy-brand check before reflection. Unknown objects use a constant
retryable diagnostic; no accessor, object coercion or instanceof hook is invoked.
PermanentError instances have a weak private brand, preserving terminal behavior
without prototype callbacks or trusting a replaced marker. Explicit own
permanent:true data records remain supported. Messages are limited to 1024
UTF-16 units without splitting a valid surrogate pair, and lone surrogates become
replacement characters before Rust JSON decoding. Input serialization uses the
same safe diagnostic path.

All six targeted baseline cases fail against the old wrapper with the current
actual addon. The corrected wrapper passes all 12 Node tests using a freshly
built addon packed into an actual npm tarball, including the existing hard-kill,
restart, idempotency, run-deadline and error-retry contracts. Packing verifies the
new helper is present in the package's explicit files list. The temporary package,
compiled copy and database fixtures are removed afterward.

CI gains a Node native runtime gate: build the same-commit addon, pack it, and run
the real host/process tests. Previously CI only compiled the standalone binding.
No native ABI, schema, callback payload or release version changes; no package is
published. Process-wide physical host capacity and the remaining review matrix
are still open.

### DME160 — Remove the unused vulnerable Wasmtime disk-cache graph

PR343's current all-feature dependency audit found zstd 0.13.3 and zstd-safe
7.3.0 through Wasmtime's optional disk-cache feature. Upstream advisories cover
[abandoned stream context reuse](https://raw.githubusercontent.com/RustSec/advisory-db/main/crates/zstd/RUSTSEC-2026-0391.md),
[out-of-bounds seekable decompression](https://raw.githubusercontent.com/RustSec/advisory-db/main/crates/zstd-safe/RUSTSEC-2026-0398.md),
and [failed legacy streaming decompression](https://raw.githubusercontent.com/RustSec/advisory-db/main/crates/zstd-safe/RUSTSEC-2026-0403.md).
The patched versions are zstd 0.14.3 and zstd-safe 8.2.0, already used by storage
compression. No advisory exception is added.

The WASM handler constructs Config::new(), whose disk cache is None, and never
sets it. Its bounded process-local compiled-module cache is independent. The
workspace now explicitly retains Wasmtime 49's other default runtime/compiler
features while excluding that unused disk-cache dependency. The lockfile drops
the old compression pair and their now-unreachable cache dependencies; module
compilation/execution and the actual in-memory cache remain enabled.

All five locked advisory checks pass: root default and all-feature graphs plus
standalone Node, Python and fuzz graphs. All 27 actual WASM plugin tests pass,
including ABI shape, import rejection, fuel, memory and wall-clock limits. The
iOS x86 CI failure was separately traced to GitHub's artifact-service request
timeouts after a successful build; it was not an iOS compilation defect.


### DME161 — Native timeouts returned capacity before host work settled

A real Node addon probe entered 129 held promises despite the scheduler's
128-step concurrency limit: each logical timeout freed scheduler capacity while
the foreign promise remained active. Closing or replacing an engine did not stop
that physical work. Repeated timeouts could therefore grow live host work without
a corresponding bound.

Node and Python now share a process-local budget per loaded binding, capped at
128 physical invocations across engine instances. An opaque invocation owner
moves its permit into the Node task observing actual promise settlement or the
Python blocking closure. Logical timeout, engine close and Python runtime
retirement cannot return that permit early. Capacity is reserved before loading
and encoding handler outputs. Saturation rejects preflight through deferred
effect admission, preserving the prepared receipt and attempt zero instead of
consuming a business retry. The existing facade now exposes deferred effect
registration with its admission contract documented.

Handler input omits internal in-progress/retry/interceptor/SLA rows, using row
metadata rather than business payload keys. Snapshots retain retry diagnostics.
A business result containing `_sentinel` remains intact.

Four Rust regressions verify physical ownership, prepared receipt continuity,
engine replacement, blocking-runtime retirement and output filtering. All 195
facade tests plus three doctests pass with required isolated PostgreSQL. All 13
actual packed Node addon tests and nine actual Python extension tests pass;
both hosts hold 128 calls across timeout/close, defer excess work, and resume only
after real settlement. The Python test also collects the original engine and
retires its runtime. Strict workspace Clippy passes. CI now runs both native host
suites against same-commit libraries, including Python's minimum supported 3.10.

This bounds invocation count, not retained bytes or per-owner fairness. Existing
admitted opaque calls may continue after cancellation. Cooperative cancellation,
account-scoped workers and the remaining review matrix remain open. No native
JSON protocol, database schema, release version or publication changes.


### DME162 — Python exception formatting could escape the host reply contract

The public Python wrapper called getattr(error, "permanent") and str(error) while
catching handler/serialization failures. A property or formatting hook could
raise again, replacing the JSON reply with a secondary Python exception; such
hooks could also execute arbitrary work after the logical handler had failed.
Nine targeted baseline cases fail against the old wrapper and actual extension.

The wrapper now reads args, own attributes and class ancestry through built-in
storage descriptors. It never formats opaque argument objects or invokes
subclass attribute/dictionary hooks. Exact single-string messages are sliced to
1024 code points before UTF-8 sanitization. Other argument shapes use a fixed
diagnostic. PermanentError ancestry preserves terminal policy without metaclass
instance checks. Explicit own permanent=True markers are recognized within a
bounded 64-attribute scan; exact string-key comparison avoids hostile colliding
key equality. These diagnostic and marker rules are documented in the package.

Ten regressions cover attribute/formatting hooks, opaque arguments, surrogate
and oversized messages, serialization exceptions, colliding dictionary keys,
attribute limits, dictionary subclasses, and actual native retry versus terminal
behavior. All 19 Python tests pass against the real native extension, including
existing hard-exit/restart and physical-capacity/runtime-retirement tests. This
changes only Python error normalization; no Rust, protocol/schema or release
version change and no publication.


### DME163 — Legacy delegation adopted catalog-owned parents and journal rows

Two real SQLite regressions fail on the baseline: explicit delegation accepts a
catalog-owned parent, and the legacy replay queue selects a persisted catalog
record. The other 11 scan tests pass. Public delegate only checked instance
existence; direct parked-task claims and journal status/list/retention paths also
lacked the account/catalog exclusion applied to legacy reads and sync reporting.
The legacy node credential could therefore adopt a scoped parent's continuity
identity and disclose or mutate its delegation journal.

Explicit admission now uses INSERT/SELECT with the shared positive legacy-owner
predicate in the write. Public preflight uses the authorized instance reader.
Discovery filters ownership before its ordered batch limit, and both parked-task
claim/rejection writes repeat that predicate. Replay, status/list and retention
use a shared delegation-owner predicate. Scoped request/materialization receipts
remain unconditional exclusions after tenant relabelling or native-history GC.
Unverified missing-parent records remain stored without being replayed, exposed
or purged by the legacy interface.

New accepted journal rows retain a legacy_verified bit atomically with their
original admission. An additive local-table upgrade reserves the SQLite writer,
adds the constrained column, and marks old rows only when a surviving native
instance proves legacy ownership. Schema change and backfill roll back together
on failure. Verified legacy delegations remain readable/replayable after native
GC; pre-upgrade orphan records are never assigned to a newly selected account.
Released migrations and the wire/UniFFI contract are unchanged.

Eight added regressions cover four account/catalog parents, actual pump replay
with zero HTTP requests, catalog receipts overriding stale markers after GC,
direct claim/rejection exclusion, retained legacy history, unknown-orphan
preservation, injected upgrade failure/retry, filtering before the scan limit,
and retention ownership. The indexed-query-plan checks still pass without a
temporary sort. The final source passes all 686 mobile unit and 109 integration
checks, with zero failures or skips. Documentation, SDK packaging/version,
released-migration immutability and whitespace checks pass. Strict all-target
mobile Clippy also passes with warnings denied.

This confines the original device protocol; it does not implement account-owned
workers, remote provenance, delegation grants or authenticated scoped outcome
reporting. Those ADR S8/S12 requirements and the remaining matrix stay open.


### DME164 — Delegation discovery and later HTTP phases survived local stop

Three baseline regressions confirm the lifecycle gap. A pump stopped before a
pass still claimed 50 newly discovered tasks. A pass first suspended behind its
SQLite connection did the same after stop. The public unregister_node operation
stopped the remote worker but left its delegation pump active. Dropping an
engine or replacing a node also lacked an admission generation for clients that
had already been captured by a concurrently starting worker/pump.

Each NodeClient now owns a cancellation parent for local admission. Workers and
pumps derive independent child tokens. Unregistration, replacement with a
different client, shutdown and final engine drop retire the parent. A retained
client cannot construct a live late worker/pump; stopping a single child does
not retire its active node or prevent an explicit successor. Re-advertising the
same client keeps its existing admission identity. Already-running worker
settlement keeps its existing policy.

The pump checks stop before discovery and after suspended discovery phases.
Parked-task claim and permanent-rejection paths reserve the SQLite writer before
the final stop check. Stop therefore cannot wake a pending database phase into a
new claim. The HTTP state machine checks before subsequent sequence-publication,
parent-registration, grant and claim phases, including readback requests.
Replies to already-admitted requests are still journaled; accepted work and
recovery evidence are not erased merely because the observer stopped.

Six regressions cover stopped and suspended discovery, writer-waiting claim and
rejection, actual public node unregistration, late worker/pump construction,
engine drop and gated HTTP phase transitions. The HTTP test separately stops
after sequence publication, parent registration and grant creation, observes no
next request, and resumes with a new pump from the durable journal. A persisted
parent registration is not repeated. Existing worker late-poll release and
physical callback ownership tests remain part of the full mobile suite.

The final source passes all 692 mobile unit tests and 109 integration tests with
zero failures or skips. Strict all-target mobile Clippy, documentation, SDK
packaging/version and whitespace checks pass.

No wire/UniFFI API, schema, SDK version or publication changes. This fixes local
node-generation admission, not stable authenticated account ownership for remote
journals/delegation or the remaining scoped-worker/reporting requirements.


### DME165 — Capacity tests confused observer deadlines with scheduler settlement

[PR345's unit-test job](https://github.com/orch8-io/engine/actions/runs/40352000145/job/121674252719)
returned Running where the new capacity regression expected Scheduled after a
100 ms observer deadline. The facade intentionally retains an owned scheduler
pass after that deadline. The snapshot therefore did not prove an admission
failure; the test asserted that an overloaded runner completed the pass inside
an arbitrary observation window. Node and Python capacity tests had the same
short-deadline assumption.

The Rust regression now permits the intermediate Running snapshot and then
observes a Scheduled instance with exactly one durable receipt, under a bounded
five-second wait. It still requires Prepared state, attempt zero, exactly two
physical callbacks while full, and the identical receipt becoming Committed only
after actual host settlement. Terminal excess work or duplicate/retried receipts
still fail. Node and Python likewise observe scheduled state after an
intermediate Running snapshot; host-entry/peak bounds and engine-replacement
checks remain intact. Increasing the initial timeout alone is not the fix.

All four Rust capacity contracts, 13 actual packed Node addon tests and 19 actual
Python extension tests pass, as does strict all-target facade Clippy. This changes
only three test harnesses. The mobile
ownership/lifecycle source was preserved byte for byte outside those test
harnesses; this report separately records the CI correction. Their already-passing
692 unit/109 integration evidence remains applicable. Their
new exact heads must pass CI again before ordered merge. No production behavior,
API, storage schema or release version changes; no publication.


### DME166 — Bound KMP callback subscriptions across engine/account replacement

The KMP observer adapters applied backpressure per collector but had no shared
subscription allowance. A real JVM regression enters four blocked callbacks,
closes their engine, then opens a successor for the same account: the baseline
accepts another callback subscription. Repeated replacement could therefore
retain more accepted observers and queued/polling tasks while physical callbacks
were still running. The new regression fails against that baseline.

Callback subscriptions now reserve from one loaded-KMP-runtime budget before
retaining their dispatch record or scheduling any snapshot read: 32 globally,
eight per engine and four per stable scoped account. Immutable count snapshots
use StateFlow CAS; metadata contains only owned job identities and account IDs,
with no observer equality or user callback hooks. Empty owner counters disappear
on release. Stable account IDs exclude the session epoch, so retirement/reopen
cannot reset the charge. Legacy subscriptions share the engine/global bounds.
Application-owned Flow collectors retain their existing backpressure contract.

The reservation follows coroutine completion, including a synchronous host
callback that ignores logical cancellation. Clearing a subscription or closing
its engine cannot return that charge before the callback returns. Queued
cancellation, ordinary completion, source/callback errors and throwing error or
completion callbacks release exactly once. Refusal raises the existing retryable
RESOURCE_LIMIT before another read or callback task is created. Idle accepted
subscriptions also count; callers close unused UI subscriptions explicitly.

Four new tests cover engine/account/global refusal, pre-dispatch cancellation,
callback failures, concurrent reservation, account allowance retained by blocked
callbacks after engine close, another account making progress, and capacity
return after actual completion. All 116 KMP JVM tests pass against the rebuilt
same-source native library and four-scope fixture, with zero skips or failures.
The real Kotlin/Native host framework also compiles. Its Swift bridge fixture
now holds four actual Swift observer callbacks, closes their subscriptions,
refuses an excess same-account observer, completes another account's observation,
then releases and resumes the first account. Strict Swift concurrency and
warnings-as-errors checks pass in that real host run.

These are accepted-subscription count limits, not byte quotas or a guarantee
about application-created asynchronous work or foreign garbage collection.
Native scoped event notifications, account-authenticated remote workers and
outcome reporting, and the remaining ADR completion matrix stay open. No
UniFFI/wire protocol, published SDK version or release publication changes.


### DME167 — A logical timeout did not prove a physically held native callback

[PR348's native runtime job](https://github.com/orch8-io/engine/actions/runs/40367600139/job/121729748084)
observed 126 active Node callbacks after 128 runs had failed logically. The
fixture assumed every timeout represented an entered, still-held callback.
Admission/preflight can lose to the timeout, and queued host entry may lag the
observer. The old test could therefore fail before it had actually exhausted the
physical allowance. The Python fixture made the same assumption.

Both fixtures now fill from observed host entry, with a 256-attempt/30-second
setup bound. They record immutable instance identities for held callbacks and
require 128 distinct physical calls. Every held instance must then reach Failed
before the excess-work probe. Unentered spare setup requests are cancelled and
observed terminal before successor testing, so they cannot consume the released
slot. Deferred snapshots still require Scheduled; the next call completes only
after actual settlement, peak physical work remains 128, and all callbacks are
released during cleanup. Node uses its public camelCase instanceId field;
Python uses its public instance_id field.

All 13 actual packed Node addon tests and all 19 actual Python extension tests
pass with zero failures/skips. This changes only the two capacity fixtures;
production bindings, callback limits, schemas, ABI and versions are unchanged.
The KMP observer source and its 116 JVM/native tests and strict Swift bridge
verification remain unchanged. PR348 must pass all checks on its updated head.

### DME168 — Terminal retention could erase an unresolved external outcome

[PRE-EXISTING, HIGH] SQLite and PostgreSQL selected old terminal instances even
when their effects were Dispatched or Unknown. Purge then deleted the continuity
and all its receipts, including ambiguous effects from a historical instance.
The new regression reproduced deletion in all eight backend/state/history cases.
A logical terminal state does not establish an external outcome.

Candidate selection now excludes ambiguous direct effects and ambiguous effects
anywhere in the continuity being deleted, before the batch limit. Purge repeats
both tenant-qualified checks. SQLite holds its existing immediate writer lock.
PostgreSQL locks the matching continuities and their existing receipts before a
fresh ambiguity check; an in-flight Prepared-to-Dispatched update cannot commit
unobserved between the check and deletion. Continuity FK locks also fence new
receipt insertion into the scopes being removed. Protected children keep their
parents through the existing family-pruning logic.

Prepared and Planned intents have not crossed the dispatch boundary and do not
pin retention. Committed, Verified, Compensated and Abandoned outcomes also
permit cleanup. Reconciliation of Unknown/Dispatched to Verified restores GC
eligibility. Two additive indexes support instance receipt lookups and ambiguous
continuity lookups; migration 124 includes online pre-creation instructions and
a matching down migration. Released migrations are unchanged.

The seven-test retention suite passes on SQLite and required isolated PostgreSQL,
including historical ambiguity, post-selection state changes, tenant isolation,
batch-limit starvation, whole-family retention and later reconciliation. A
PostgreSQL race fixture holds an uncommitted dispatch update, observes GC waiting
on its lock, commits the dispatch, and requires GC to retain the evidence. A second receipt
with reversed UUID/creation ordering reproduced a deadlock with id-only GC
locking; GC now follows dispatch's creation-time/id order, and the fixture
requires the writer to acquire its second receipt without deadlocking.
The full storage suite passes with required isolated PostgreSQL (four existing
ignored documentation examples). After the private locking-helper extraction,
all seven retention tests pass again, strict all-target storage Clippy passes,
and all 30 mobile lifecycle tests pass. Migration immutability/destructive-DDL,
documentation and formatting checks pass.
The wider long-running engine and scoped mobile completion matrix remains open.

### DME169 — Current output views materialized the whole execution history

[PRE-EXISTING, HIGH] Template resolution and the shared Node/Python callback
adapter loaded every append-only block-output row, decoded/decrypted them, then
kept only the latest substantive result per block. Loops therefore increased
per-step materialization and map allocation with their lifetime. A native adapter
regression reproduced another consequence on encrypted SQLite and PostgreSQL:
one damaged superseded ciphertext prevented an otherwise valid current result
from reaching the handler.

`OutputStore::get_latest_completed_outputs` now provides a current business view.
The supported backends seek distinct block keys and their latest substantive
rows inside one statement snapshot. SQLite keeps the key stream outside the
payload lookup; PostgreSQL uses a per-key lateral lookup with the instance
predicate preserved, including the final payload selection. Encryption unwraps
only selected rows. Custom storage implementations retain a correct full-history
fallback; the optimized read guarantee belongs to the provided backends.

The initial PostgreSQL candidate still visited 517 rows for two current outputs.
The revised plan passes a <=20 visited-row bound with 512 business results and
512 internal trace results; SQLite verifies indexed next-key and payload seeks.
Migration 125 supplies the partial current-output index, its down migration and
online pre-creation instructions. The durable schema marker advances to 125;
released migrations are unchanged.

Business semantics remain deterministic by creation time and UUID. Retry and
in-progress markers and interceptor/SLA metadata are excluded before selecting a
winner; genuine failure outcomes, custom output references and business fields
named `_sentinel` are preserved. Tests cover same-timestamp insertion disorder,
empty/Unicode keys, SQL-wildcard lookalikes, owner isolation and PostgreSQL output
UUID collisions across instance partitions. Selected corrupt ciphertext still
fails closed before host entry. Existing request-load claim/retirement gates now
pause on the new physical read and retain their original ownership assertions.

Template/preparation tests (27/31), composite/metadata contracts (9), native
claim/capacity contracts (26), and migration rollback checks (15) pass with
required isolated PostgreSQL. All nine projection checks pass, including 2,049 distinct blocks on both
backends. Strict all-target Clippy passes across storage, engine and the Rust
facade. Freshly rebuilt actual bindings pass all 13 packed Node tests and all
19 Python extension tests, with zero failures/skips. Documentation, formatting
and migration safety checks pass. Native diagnostic snapshots still have their separate
history/diagnostic contract; per-payload byte bounds and the wider review matrix
remain open. No published version, FFI ABI or release publication changes.

### DME170 — Native snapshots still decoded superseded execution history

[PRE-EXISTING, HIGH] Native `get()` and the snapshot returned by `run()` loaded
all block-output history before retaining the latest visible values. The
per-step business projection did not cover this diagnostic view, which also
exposes the latest retry error while another attempt is scheduled. The actual
native-core regression failed because damaged obsolete JSON was decoded before
a valid current retry result could be returned.

A separate indexed diagnostic projection now preserves retry rows, excluding
in-progress/interceptor/SLA records before selecting each winner. The storage
queries share their key-stream and dialect logic with the business projection,
with literal eligibility predicates and a matching diagnostic index in migration
126. The Rust facade exposes tenant-checked `current_block_outputs`; the native
snapshot uses that current view. Selected damaged payloads still fail closed.
Custom backends retain a correct history-based fallback.

The nine storage contracts now exercise both views, including 2,049 distinct
blocks, encrypted reads, foreign instance UUID collisions and marker/timestamp
semantics. Actual plans pass indexed-read bounds on 1,536 business/retry/trace
rows. The facade tenant test passes, as do all 41 native callback/claim/preflight/
deadline tests, including the new damaged-history/current-diagnostic regression.
All 15 migration contracts and strict all-target storage/engine/facade Clippy pass.
Fresh actual bindings pass 14 packed Node tests and 20 Python extension tests
with zero failures/skips. Node checks scheduled retry diagnostics without starting
the next attempt; Python exercises damaged old/internal JSON through public
`get` and zero-deadline `run`, then requires failure for a damaged selected result.

These current-row read bounds do not impose a per-payload byte quota. Capsule
exports and explicit historical APIs retain their separate history contracts;
bounded export materialization, native byte quotas and the broader review matrix
remain open. Public snapshot shape, published versions and FFI ABI are unchanged;
no release was published.

PR350's KMP/Apple bridge job 121787448101 was cancelled before runner assignment
(runner id 0, no steps). The authorized retry created live job 121798193932 on the
same tested head; its merge gate was restarted only after the replacement was
visible. The retry succeeded and PR350 merged with all 25 checks and tested-tree
equality proved. PR349 had already passed the same merge proof.

### DME171 — Online prebuild guidance targeted a partitioned parent

The newly written, unreleased migration 125 comment suggested concurrent index
creation on `block_outputs`, which migration 074 partitions by instance. The
PostgreSQL 14 baseline rejects that command (SQLSTATE 0A000). PostgreSQL documents
[concurrent leaf builds followed by parent attachment](https://www.postgresql.org/docs/14/sql-createindex.html).

The corrected 125/126 headers point to an executable psql prebuild. It discovers
leaf tables, uses schema-qualified catalog ownership, skips completed valid
parents and refuses invalid parent/prebuild indexes. Builds run in autocommit;
the ordinary transactional migrations attach matching valid leaves. The change
to 125 is comment-only and precedes release; no released migration is modified.

A permanent regression creates and removes its own schema, uses migration 074's
actual 16-leaf table definition, prebuilds 32 indexes, and applies both parent
migrations transactionally. It verifies parent validity, attachment of the same
32 index identities and unchanged physical file nodes, repeatability and retained
data. A deliberately failed concurrent unique index (SQLSTATE 23505) verifies
refusal, explicit repair and successful retry. It passes on PostgreSQL 14.23.
The unit CI job now installs the psql client and runs this check against its test
database. Workflow lint, documentation and migration-safety checks pass.


### DME172 — Capsule imports discarded completed output evidence

Capsule exports included `outputs`, but import persisted only the paused instance
and checkpoint. Every production caller discarded the returned payload, so the
scheduler could replay completed work after migration. A regression first failed
with zero imported outputs; after the fix, the real scheduler completes the
remaining step while an unavailable handler for the completed step is never run.

An additive storage operation publishes the immutable import binding, instance,
checkpoint and output history in one transaction. SQLite and PostgreSQL share
ownership/duplicate checks; encrypted storage seals outputs and checkpoints
before entering the transaction. Custom backends reject nonempty output imports
until they implement the atomic operation. The previous empty-history operation
remains available. Import retries return the original destination without
rewriting its later work.

The engine checks signed source ownership and duplicate output identities,
allocates fresh destination output IDs, and preserves relative `(created_at,id)`
order. This avoids SQLite global-ID collisions on same-store transfers and keeps
the winning result stable when source timestamps tie. Source timestamps and the
returned signed payload remain unchanged.

Eight storage regressions pass on SQLite, PostgreSQL 14.23 and their encrypted
variants. An injected failure on the second output insert proves rollback of the
first output, checkpoint, instance and binding, followed by successful retry.
Foreign/duplicate evidence is rejected before publication. Both capsule engine
scenarios pass, including isolated and same-store imports, tied timestamps and
actual scheduler continuation.

This fixes output restoration, not complete composite workflow portability.
Execution trees, externalized composite state and full effect-receipt recovery
still require a versioned portable-state contract. Export materialization bounds
and source/destination clock skew remain separate open review items; do not infer
full migration certification from these tests.

The facade's two capsule scenarios and the mobile activation/redelivery scenario
also pass. Raw output rows are checked for ciphertext on both encrypted backend
variants, in addition to verifying decrypted API results. Format, documentation
and strict all-target lint checks cover storage, engine, facade and mobile.


### DME173 — Composite capsules lost execution trees and frozen payloads

DME172 restored outputs, but composite execution also depends on tree state and
externalized payloads. A paused `for_each` carries its frozen collection and
shadowed item binding outside the checkpoint. The previous importer recreated
neither: the regression first found zero imported nodes instead of two, and the
handler's missing-snapshot path can finish a loop without processing its items.

Capsule schema 1.1 now requires an execution-state envelope. New exports include
the execution tree and complete paginated payload catalog. The 1.0 reader gate
rejects 1.1; new readers accept legacy flat capsules and reject missing required
composite recovery or a payload relabeled with the legacy schema. Counts are
bounded at 10,000 nodes and 10,000 payloads, within the existing 64 MiB encoded
capsule ceiling. Catalog ownership and repeated references are checked before advancing pages.

An additive atomic snapshot-import operation validates graph ownership, duplicate
identities, missing parents and cycles before writing. It publishes the paused
instance, checkpoint, immutable binding, nodes, payloads and outputs together.
SQLite/PostgreSQL order parents before children; payload-owner conflicts roll
back every newly written row and preserve the existing owner's bytes. Encrypted
storage uses the same snapshot sealing helper as archive restore, with destination
instance/key binding. Raw ciphertext, logical payloads, timestamps, expiry and
idempotent replay preservation are tested on both databases and wrappers.

Destination node identities are fresh while parent relationships and ordering
remain stable. Instance-derived payload keys are remapped; arbitrary keys receive
a destination namespace. Exact externalization markers, nested payload references
and engine-owned loop snapshot references follow those keys. Ordinary objects
that merely contain `_ref` retain their data. Missing or expired required references fail before
publication. Active loops also require their computed snapshot key even when an
older marker omitted its descriptive `_snapshot_ref` field. The runtime's instance identity is updated to the destination.

The real scheduler regression transfers an encrypted paused loop after its first
item, while the live collection has changed. It processes only the two frozen
remaining items and restores the original item variable. Its catalog exceeds one
1,000-row backend page; nested externalized references survive destination-local
remapping. Both databases and encrypted destination variants are exercised.

This extends composite recovery; it does not yet establish complete portability
of effect receipts, queued signals, waits or related instance families. Bounded
source materialization and clock skew remain open. Full scoped/platform evidence
and the broader distributed/mobile/embedded completion audit remain required.


Hosted [unit job 121843823267](https://github.com/orch8-io/engine/actions/runs/40400239722/job/121843823267)
exposed a cross-backend fingerprint defect: the source SQLite definition retained
nanoseconds in `created_at`, but PostgreSQL persisted lower timestamp precision.
The original macOS clock only exercised microseconds. The transfer fixture now
forces `123_456_789` nanoseconds on every host and reproduced the hosted failures
locally on both PostgreSQL variants before the fix.

Schema 1.1 fingerprints now exclude only backend-managed sequence creation time,
with all other definition fields still covered. The legacy 1.0 complete-document
hash remains unchanged. A focused contract regression proves creation-time
independence and sensitivity to executable parameters, tenant and version.
Import also checks the signed sequence version explicitly. Both PostgreSQL
transfers now pass with forced nanoseconds; signature/tenant/version rejection
and facade/mobile compatibility remain checked. Hosted CI must rerun on the new
head; the failed source revision is not retried unchanged.


### DME174 — Capsule import omitted destination sequence ownership

The exporter checked sequence ownership, but the importer checked only the
sequence ID and signed content hash. A trusted issuer could provide a valid
manifest and matching hash for a sequence stored under another tenant; SQLite
accepted a new requesting-tenant instance bound to that foreign sequence. The
regression reproduced this successful cross-tenant import before the fix.

Import now requires the stored sequence's tenant to equal the authenticated
capsule/request tenant before constructing or publishing any destination state.
The regression uses a correctly signed manifest whose sequence hash matches the
foreign sequence, so refusal depends on ownership rather than signature or hash
failure. It requires `OwnershipMismatch` and no destination instance.

Final targeted validation for DME173–174: 61 passing results — 12 protocol,
36 storage (capsule, catalog and atomic archive restore), three state-boundary
unit tests, seven capsule/scheduler cases, two facade cases and one mobile
activation/redelivery case. Strict all-target Clippy passes for types, storage,
engine, facade and mobile; format, diff and documentation checks pass. Hosted
checks on the exact PR head remain the merge gate; this is not a full platform
artifact or release certification.


### DME175 — Prepared effects could dispatch after their authority expired

Portable receipt review exposed a prerequisite race: final dispatch compared only
the receipt's prepared state. A retained native admission could advance it after
its instance paused or continuity entered `transferring`. Both regressions first
reproduced successful dispatch after that fence. A replaced scheduler claim could
likewise be adopted unless the original token traveled through preparation.

A new storage operation checks captured tenant/instance, continuity owner runtime
and epoch, live state, and original claim in the same transaction as receipt
admission. SQLite also checks durable mobile account/catalog ownership. PostgreSQL
locks ownership and instance rows before reading and transitioning the receipt;
guarded calls coordinate with the existing duplicate-dispatch lock. A blocked
writer regression observes the PostgreSQL lock wait before committing a changed
owner or claim, then requires stale admission and unchanged prepared evidence.

Engine step and worker enqueue paths forward their captured claim;
native admissions retain it without reloading a replacement. Explicit unclaimed
public step helpers and worker recovery keep their documented no-claim mode,
while still requiring live instance and continuity ownership. A safely rejected
prepared admission reports claim loss and remains prepared; its fresh owner can
reuse it. Already dispatched/unknown evidence retains its existing classification.

Both guarded and unguarded dispatch use the authority transaction. Unguarded
receipt lookup uses its primary key; invariant evidence is not loaded for an
empty guard set. Receipt identity cannot be substituted during admission, including replacement
of a prepared row under the same logical ID. Native
cancellation and before/after-commit fault tests target this new atomic boundary.
Shared step admission removes duplicate dry-run/claim plumbing from enqueue and
tree-handler paths. Mailbox delegation remains a grant-authorized unclaimed
action: its stored waiting proxy timestamp is not a scheduler lease. The durable
proxy reload/retry case is exercised, rather than relying on its unstored fixture. Custom backends fail closed until they implement owned dispatch.

This closes an admission prerequisite for portable effect recovery. It does not
yet carry full receipts across runtimes or establish an atomic source export cut;
those remain required alongside pending events, waits, family state, source byte
bounds and the full scoped/platform completion matrix.


DME175 validation: 103 passing targeted results (10 storage authority/concurrency,
17 engine guard, 24 distributed-worker/delegation, 11 mobile native-admission,
41 facade native claim/deadline/capacity/preflight), strict all-target Clippy for
storage/engine/facade/mobile, and fresh actual 14 Node plus 20 Python tests. The
proxy retry now reloads durable storage as the API does; a stale fixture could
otherwise conceal treating a waiting timestamp as a live claim. Format, diff and
documentation checks pass. Full platform/release certification remains open.

### DME176 — durable identities for repeated loop effects

A real scheduler/provider-deduplication regression reproduced lost work on all
four storage variants: separate `for_each` iterations reused the effect ID
computed from continuity, epoch, instance, block and retry attempt. A provider
using that ID as its idempotency key suppressed the second item. Hashing the
request instead would still suppress two legitimate identical items.

Migration 127 (SQLite schema 57) adds an active invocation binding. Atomic
preparation selects one fresh provider ID; concurrent preparations and recovery
of that same active attempt reuse the selected receipt. Existing deterministic
receipts are adopted with their original ID and state during a coordinated
upgrade. A separate retry-policy attempt keeps its existing distinct-intent
semantics. This does not promise exactly-once provider execution.

Iteration reset now retires active bindings separately from receipt history.
Persistent block reset markers prevent a retained legacy receipt being adopted
again as the next iteration. Uncertain dispatched/unknown receipts survive for
reconciliation. Enabled workflow invariants retain resolved history; otherwise
resolved iteration receipts are pruned. Unstarted planned/prepared rows can be
removed because admission never occurred. The reset and retention decision are
atomic, and claimed reset requires the original live scheduler claim. An
explicit low-level destructive delete remains separate from iteration retirement.

Configure invariants before execution: enabling one later cannot reconstruct
previously discarded evidence. Retained uncertain or guarded history requires
explicit resolution and the normal lifecycle/archive policy. Reset markers are
bounded by blocks per instance and cascade with instance deletion.

Deployment requires draining existing runtimes and upgrading writers together.
Older binaries do not participate in the binding/reset protocol. Rollback refuses
to discard nonempty bindings or reset markers; it requires explicit lifecycle
cleanup after draining. No release is published by this change. Portable receipts
and an atomic source export cut remain separate open review work.

DME176 validation: the provider-deduplication regression failed on all four
backends before the fix. All 12 loop/provider/guard cases and 22 storage identity,
concurrency, legacy-state, rollback, claim and reopen cases now pass. The guarded
loop requires durable invariant failure evidence. The full 2,127 engine unit
suite passes, alongside 10 dispatch-authority, 16 guard-discovery, 24 distributed,
11 mobile admission and 41 native facade cases. All-target strict Clippy passes
for types, storage, engine, facade and mobile. Real PostgreSQL migration 127
rollback refuses both active bindings and retired markers; an empty rollback
and reapply succeed. Hosted platform gates remain required before merge.

### DME177 — bounded effect-ledger enumeration at capsule export

The long-running history review reproduced successful publication of a truncated
capsule: 10,000 effect receipts exported successfully, and adding a 10,001st
receipt still produced another valid manifest because the legacy list operation
silently caps results. The reproduction used a real encrypted artifact, not a
mock list or an inferred count.

A new storage page operation follows the existing covering index on tenant,
continuity, physical `created_at` and ID, with at most 1,000 rows per read. Its
cursor comes from SQL columns rather than JSON receipt timestamps: PostgreSQL
rounds timestamp precision while the serialized receipt retains nanoseconds.
Using the JSON timestamp can repeat or skip rows at a page boundary. Encrypted
and fault wrappers forward the same operation. Indexed tenant/continuity/ID must
also agree with the JSON record before a page is returned. Custom backends fail
closed until they implement pagination.

Export follows pages through the end, checks identity and cursor progress, and
rejects histories exceeding the existing capsule item limit before publishing an
artifact or manifest. An exact-limit capsule is decrypted and imported into a
separate store with every expected effect ID present; the extra receipt must
produce a size error and leave the artifact count unchanged. The four-backend
matrix covers timestamp rounding, ID ties, page caps, empty reads, scope
isolation and corrupt JSON ownership.

This fixes silent truncation; it does not establish a source snapshot, carry full
receipt outcome/active-binding state, or solve large-history portability. Those
remain required work. Long-running history needs its explicit acknowledged
archive/compact-receipt policy rather than silently dropping guard evidence.
Export still needs an atomic writer fence, bounded materialization of other
state, and complete event/wait/family portability before the broader handoff
contract is achieved.

DME177 validation: 25 targeted results pass (12 storage page-matrix cases,
8 engine capsule cases, 2 embeddable facade cases and 3 mobile continuity cases).
The original overflow test first published a truncated manifest, then passes
with exact-ID roundtrip verification and unchanged artifact count on overflow.
Strict all-target Clippy passes for types/storage/engine/facade/mobile. Format,
diff and documentation checks pass. No migration is needed: both stores already
have the covering index used by the new keyset query. Hosted gates remain pending.

### DME178 — portable effect outcomes, invocation aliases, and frozen guards (in review)

Capsule schema 1.2 carries complete receipt outcomes, current invocation aliases,
retirement markers, and the effective commit guard policies for the execution.
Receipt origin coordinates remain immutable. Destination aliases identify the
accepted epoch and local instance, so a transferred prepared invocation keeps
its provider identity while its next loop iteration obtains a fresh identity.
Dispatched or unknown evidence prevents a second provider call. Frozen policies
remain attached to the continuity execution without enabling global policies
for unrelated runs.

Import atomically persists the paused snapshot, pending ownership, receipts,
aliases, reset markers, frozen policy context, immutable capsule manifest, and
manifest digest proof. Conflicting tenant, continuity, origin identity, policy,
or destination ownership rolls back the whole import. Frozen policies must also
match the manifest's workflow version; a regression reproduced acceptance of a
different-version guard on all four backends before this validation was added. Exact redelivery returns
the persisted instance and preserves advanced outcomes, including when the host
supplies its existing destination ID. A regression first reproduced a stale
paused return value for a completed run; both explicit and allocated destination
redeliveries now read the persisted state. A changed signed header
under the same capsule ID fails, even when the encrypted payload is unchanged.
Namespace lookup includes imported and superseded locations, preventing a fresh
continuity identity from bypassing the transferred execution's authority.

Pre-binding deterministic receipts receive an alias only when the historical ID
matches their current source coordinates. Noncanonical unbound uncertain
receipts prevent export. Legacy 1.0/1.1 capsules without effects remain readable;
legacy capsules containing effect IDs without recovery evidence are refused.
Pre-upgrade imports without the new proof cannot be silently adopted as 1.2
redeliveries. Deployment still requires draining and upgrading writers together.

PostgreSQL migration 128 and SQLite schema 58 persist recovery proofs and frozen
policy context. Downgrade refuses proofs, frozen guards, and origin-divergent
aliases rather than discarding evidence. Real PostgreSQL tests separately prove
each refusal, then execute an empty downgrade/reapply and a fresh import.
SQLite reopen tests verify aliases, retirement markers, policy context, and
immutable redelivery proof survive restart.

Current validation: all 2,127 engine unit tests pass, alongside 40 actual
capsule effect transfers, 34 storage recovery cases, and 13 protocol boundary
cases. The effect transfer matrix includes isolated and shared stores,
PostgreSQL/SQLite/encrypted destinations, legacy prepared and uncertain evidence,
provider identity preservation, loop retirement, and frozen guard enforcement.
Collection tests cover each recovery vector at 10,000 and 10,001 entries.
Affected facade/mobile tests and strict all-target linting pass. Native binding
verification and hosted platform gates remain required before merge.

This does not establish an atomic source export cut or atomic activation.
Mobile activation still needs one transaction for accepting ownership and
scheduling, with repeated activation preserving already progressed states.
Pending event/wait/family portability, bounded source materialization, and the
full principal/platform acceptance matrix remain required review work.

### DME179 — atomic capsule activation and acknowledgement retries (in review)

The public mobile roundtrip reproduced a delayed activation acknowledgement
changing a legitimate `Waiting` execution back to `Scheduled`. The previous
implementation also accepted ownership and scheduled the instance in separate
commits, leaving an owned but unscheduled run after a crash or storage failure.

A storage transaction now validates the persisted manifest and verified import
proof for the requested tenant, capsule, runtime, and destination instance. It
locks continuity ownership before the instance, matching effect dispatch and
GC. Only the expected transferring source epoch may become the destination's
next owned epoch. Ownership, location history, and initial scheduling commit
together. A scheduling error rolls all three back. Initial activation requires
a paused, unclaimed instance; SQLite also enforces the registered mobile
account/catalog execution predicate.

An exact accepted acknowledgement is a no-write operation, preserving scheduling,
waits, manual pauses, completion/failure/cancellation, timestamps, and live
claims. It cannot reverse a later ownership transfer. A missing verified import
proof fails closed. Custom backends must implement the atomic operation;
encryption and fault-injection decorators forward it.

The Rust embeddable facade exposes `activate_portable_capsule`, and mobile
activation uses the same storage operation with the mobile runtime's own tenant
rather than inferring authority from the requested instance. Identity conflicts
remain invalid mobile input while storage failures preserve their retryable
error class. Mobile import redelivery now reports the persisted state; a second
regression first reproduced a `"paused"` response for an already waiting run.

Current validation: all 59 recovery storage cases pass, including all four plain
and encrypted SQLite/PostgreSQL variants of concurrent activation, final-write
rollback, claim/state preservation, and stale transfer refusal. SQLite reopen
retains an acknowledged wait, and a registered account without catalog admission
cannot be scheduled. The public mobile wait regression and actual Rust facade
scheduler roundtrip pass. All 40 effect-transfer cases pass through
the real atomic activation API. A fault injected after the activation commit
simulates a lost acknowledgement; retry preserves a subsequently claimed run
on every backend. All four mobile continuity cases, the public Rust facade
roundtrip, and strict all-target Clippy pass. Freshly rebuilt bindings pass
14 Node and 20 Python checks. Hosted gates remain required before merge.
No migration is needed.

Activation remains a trusted-host operation performed after app/backend
acknowledgement of exclusive ownership. A local database cannot independently
prove current ownership in another offline database. The atomic source export
cut, complete wait/event/family portability, and scoped mobile import admission
remain required follow-up work for the overall transfer contract.

### DME180 — durable source export cut (work in progress)

A source could resume after capsule snapshot reads but before publication and
ownership transfer. A deferred native provider admission reproduced two physical
provider calls: one on the resumed source and one after destination activation.
The new regression in `orch8-engine/tests/capsule_export_cut.rs` fails with two
calls before the source-cut change and passes with one afterward.

Migration 129 and SQLite schema 59 introduce a durable source cut. Snapshot
capture, source claim revocation, pause, and ownership fencing share a database
transaction. Preparing retries reuse the captured state; sealing retains an
immutable manifest and encrypted transport payload. Publication uses a stable
artifact identity so an ambiguous upload can be retried. Only a pre-seal abort
can restore local ownership. Sealed transport may already have reached another
runtime, so upload errors cannot authorize resuming the source.

The coordinator export commit must recognize an already fenced local source.
The implementation accepts that state only with a sealed source cut matching the
capsule, tenant, continuity, epoch, source runtime and instance, and destination.
The existing owned-source path remains necessary when the coordinator attaches
a capsule prepared in a remote device's database. Both paths now compare the
source instance as well as owner and epoch. The service also binds the execution
to the handoff's source runtime, tenant, continuity, and expected epoch.

Validation: all 36 source-cut tests and 14 existing continuity storage tests
passed after the coordinator change. These cover captured-only refusal, wrong
capsule, wrong destination, wrong source instance, successful sealed commit, and
stale handoff retry across plain and encrypted SQLite and PostgreSQL.
Sealing and abort now read only bounded request metadata before taking the owner
lock, avoiding a redundant full snapshot allocation. After that change, all
36 source-cut tests, 14 continuity storage tests, and seven retention tests passed
again (57 total). Strict storage Clippy passed for all targets with warnings
denied. Documentation checks and whitespace validation also passed.

Remaining work before publication includes acknowledgement and sealed-cut
compaction, public API reopen/fault tests, immutable
export overflow tests, migration rollback/reopen checks, and broader engine and
mobile verification. Full wait/event/family and referenced-artifact portability
remain separate unresolved parts of the original review scope.

#### DME180 HTTP recovery follow-up

Real loopback HTTP tests in `orch8-api/tests/continuity_export_retry.rs`
reproduced three failures before the fix: upload acknowledgement loss and payload
read failure terminally failed the handoff while leaving the source fenced; a
lost coordinator commit acknowledgement made the next export request fail because
the handoff was already exported. All three passed after the retry change.

Export now permits Requested, Quiescing, and Exported phases, revalidates source
runtime and epoch, and reuses the immutable source cut. Publication/read errors
leave the operation retryable. Exported retries require the exact capsule ID and
return the same transport. The service accepts a Transferring expectation, while
the storage commit always requires matching sealed-cut proof for a Transferring
row; presenting the current state must not bypass the proof. Failed, cancelled,
and later accepted phases remain ineligible for export.

The HTTP tests also reject replacement transfer keys and advanced ownership
coordinates. An additional assertion checks that a larger requested TTL cannot
extend the original sealed expiry. Final verification passed all 57 backend
source-cut/continuity/retention tests and all seven HTTP retry/product tests.
Strict Clippy passed for the API library and retry integration test with warnings
denied; documentation and whitespace checks passed. These results cover retry
recovery, not the remaining complete portability and history-lifecycle scope.

The HTTP recovery tests exposed an audit gap: export provenance was appended after the coordinator commit.
A lost commit acknowledgement or crash can leave an Exported handoff without its
export boundary. The atomic-provenance follow-up below addresses newly committed exports;
successful HTTP retries alone did not prove that audit invariant.


#### DME180 atomic export provenance follow-up

Extending the real HTTP commit-acknowledgement-loss test to require an export
boundary reproduced a persisted Exported handoff with zero provenance entries.
The new audited storage primitive commits the ownership transition, handoff CAS,
and provenance insert in one transaction. A chain-predecessor conflict rolls
back the transaction. The engine binds the signed manifest to the handoff/source
coordinates, signs the boundary with the coordinator key, and retries bounded
head races. Existing storage implementations without the primitive fail closed.
The original unaudited primitive remains available for existing low-level callers.

Both HTTP source export and device-capsule attachment now use the audited
primitive. The encrypted wrapper and fault-injection wrapper forward it. HTTP
lost-acknowledgement tests passed with exactly one export boundary and validated
the resulting chain against the trusted coordinator signing key. Backend tests
exercise predecessor conflict rollback, cross-tenant refusal, successful commit,
and stale retry without a second entry. All 65 backend source-cut/continuity/retention tests passed, covering locally
fenced and remotely owned modes across SQLite/PostgreSQL and encrypted wrappers.
All seven HTTP retry/product tests passed with trusted-key signature checks.
Strict storage Clippy passed for all targets and strict API library/retry-test
Clippy passed, both with warnings denied. Documentation checks passed.

This establishes atomicity for new audited commits. It does not reconstruct
historically missing boundaries from older unaudited commits. Full device HTTP
principal/platform coverage and acknowledged source-cut compaction remain open.

#### DME180 source acknowledgement compaction (in progress)

Sealed cuts retained their transport ciphertext indefinitely. A new storage
acknowledgement primitive binds explicit trusted-host confirmation to the exact
capsule, tenant, continuity, next epoch, and permitted destination. In one
transaction it records a compact accepted-owner receipt, clears snapshot and
ciphertext, and advances source-side ownership indexes/JSON and location history
when needed. The local source stays paused. An acknowledgement retry returns the
immutable receipt without overwriting subsequent ownership or completion.

The storage test matrix exercises unsealed refusal, wrong tenant/continuity,
wrong destination/epoch/state, lost acknowledgement after commit, compacted SQL
columns, blocked resealing/source recapture, and preservation of later ownership.
The first run caught an index/JSON mismatch that prevented the next owner CAS;
ownership epoch/runtime indexes and destination location are now part of the
same acknowledgement transaction. Follow-up verification passed all 81
source-cut, handoff, and retention cases across plain/encrypted SQLite and
PostgreSQL. Delayed first acknowledgement during the next transfer and after a
later epoch preserves the current owner/state. Strict storage Clippy passed for
all targets with warnings denied.

This primitive is internal groundwork. External artifact deletion, publication
races with deletion, source archive/admission behavior, public host/mobile scope
authorization, and durable reopen/migration verification remain required before
exposing the complete acknowledgement lifecycle. Clearing a database payload
alone does not prove bounded total storage or external artifact cleanup.

#### DME180 external transport retirement (in progress)

Acknowledgement now atomically creates an artifact-deletion tombstone alongside
the compact receipt. Tombstones have no parent FK and survive cut/history purge.
The bounded GC pass claims due keys and reschedules them before external I/O;
it revisits successful deletions too, because an already started upload can
complete after deletion. This cleanup runs even when general artifact retention
is disabled: acknowledgement explicitly requests transport retirement. Tombstones
remain compact retained metadata; this is not a claim of constant total storage.

Export checks publication eligibility before and after upload, including an
upload that reports an error after writing. If retirement won, export removes
the transport and refuses to return a capsule. The tombstone covers crashes and
delete failures. Only the transport's canonical instance/capsule key is retired;
other instance artifacts remain intact. Eligibility also requires the original
source owner, epoch, instance, and Transferring fence.

Final verification passed 64 source-cut backend cases, seven retention tests,
and 15 rollback-file checks, plus four engine scenarios: double-provider
prevention, deletion failure followed by GC/late-upload cleanup, upload finishing
after acknowledgement, and SQLite/local-file artifact reopen after source-cut
purge. Ownership-bound publication eligibility is included in these results.
Strict storage Clippy passed for all targets and engine library/integration-test
Clippy passed with warnings denied. Documentation checks passed. Mobile
uses its own maintenance path rather than server GC, so mobile cleanup wiring,
SDK acknowledgement scope/API coverage, and source archive/admission lifecycle
remain open.

#### DME180 mobile production storage and maintenance follow-up

The real `MobileEngine::new` constructor configured no artifact backend, making
public continuity export fail with “artifact storage is not configured.” Earlier
continuity tests supplied a memory artifact store directly and missed this.
A real constructor regression reproduced the failure. Mobile now attaches a
local artifact store beside the opened database's canonical filename, so symlink
aliases and restart use the same companion directory. Explicit `:memory:` engines
use an ephemeral memory store. Hosts must keep the companion artifacts with the
database when moving/backing up engine data.

Mobile transfer-key identity was also constant across all provided AES keys.
It now fingerprints the actual decoded 32-byte transfer key, so an immutable
export retry cannot silently accept another key and return old ciphertext.
Legacy export refuses non-mobile/account-scoped tenants before source capture;
scoped export still needs its own explicit owner-authorized surface.

Transport retirement runs outside scheduler locks and host callbacks, including
manual ticks/OS background batches, foreground startup and quiescent deadlines,
and memory-budget scheduler skips. A mobile pass is limited to 32 keys and
250 ms, at most once per five minutes. It claims one tombstone at a time so a
cancelled pass does not reschedule a large unattempted batch. Server GC uses the
same shared bounded collector. Tombstones remain eligible after successful
object deletion to catch late uploads.

The real constructor regression passed through export, alias-path reopen,
unchanged retry/expiry, changed-key refusal, OS-background cleanup, and a second
restart into foreground cleanup of a recreated transport. The account-isolation
regression leaves ownership unchanged and creates no source cut. All 695 mobile
unit tests, eight integration tests, four shared engine export/cleanup
regressions, and 18 engine GC unit tests passed (725 total). Strict mobile Clippy
passed for all targets, and shared engine library/integration-test Clippy
passed, both with warnings denied. Documentation and whitespace checks passed.
SDK acknowledgement APIs, scoped mobile
import/export admission and authorization, source archive/quota semantics, and
complete wait/event/family/artifact portability remain open.

#### DME180 Rust source acknowledgement and effect-bound contract

The public embeddable facade now accepts explicit trusted app/backend acceptance
coordinates through `Engine::acknowledge_portable_capsule`: capsule ID, accepted
epoch, destination runtime, and destination instance. It derives tenant and
continuity from the engine's tenant-filtered stored manifest, then invokes the
durable receipt/cleanup path. An external artifact error may follow the receipt
commit; the host can retry the same coordinates. Source execution stays paused.
This does not replace the remaining scoped mobile acknowledgement/archive APIs.

Public facade tests passed for full import/activation/wait/acknowledgement,
wrong destination/epoch refusal, repeated acknowledgement, and a foreign tenant
sharing the same physical database. The foreign tenant cannot delete the source
transport; valid acknowledgement deletes it and blocks source re-export.
Strict facade Clippy passed with warnings denied. The standard `tempfile` test
dependency owns isolated database/artifact directories; the lockfile change is
limited to that existing workspace dependency.

The existing effect-bound test expected a retry to recapture a newly appended
receipt. Its failure was an obsolete expectation after immutable capture, not
permission to truncate history. Two tests now pass: exactly 10,000 receipts are
transported and a retry preserves the original set/header/artifact; a separate
fresh 10,001-receipt source refuses before retaining a cut, fencing ownership,
or publishing an artifact.

CI follow-up: PR357 failed in the CLI demo because it attempted to create
continuity ownership after atomic capsule import had already persisted it,
violating the current-instance uniqueness index. The exact PR357 checkout passed
all 320 CLI tests and strict CLI Clippy after reusing and validating imported
pending ownership. PR358 was rebased and also passed all 320 CLI tests and strict CLI Clippy,
using atomic activation instead of the demo's separate ownership/scheduling
writes. Both repaired heads were atomically pushed with exact old-head leases.
All 49 uncommitted working files were preserved byte-for-byte; the clean isolated
checkout was removed. CI merge gates are running on repaired heads 79d6bba3
(PR357) and 88482c6a (PR358). No source-cut WIP is included in that CI repair.


The complete embeddable Engine integration suite passed all 20 tests. Its old
conflict test assumed export left the source Owned; immutable source capture now
makes it Transferring, which legitimately permits another runtime to import into
the same database. The rejection case now uses acknowledged Owned coordinates
without a local import proof, and a separate positive test proves same-database
import/atomic activation/acknowledgement while the original source stays paused.
Neither change relaxes the production import predicate. Final facade and
effect-bound Clippy checks passed with warnings denied; documentation and
whitespace checks also passed.

#### DME180 transactional scoped source authority (in progress)

Scoped capsule capture now carries an explicit trusted mobile access descriptor.
SQLite checks account activity/session epoch, catalog membership, immutable native
identity, and current materialization/request eligibility inside the source-cut
writer transaction. A missing descriptor cannot downgrade a registered account
run into generic export. The descriptor is pinned in the durable cut and retry
identity. PostgreSQL refuses mobile descriptors because scoped mobile storage is
SQLite-specific. Generic non-mobile exports retain their existing path.

Publication rechecks the stored authority and source ownership in one read
transaction before and after artifact I/O. Account retirement and owned run
cancellation retire captured/sealed transport copies in their own writer
transaction: clear retry bytes, retain source ownership fencing, and enqueue
canonical artifact-deletion tombstones. This retires internal transport copies;
it does not archive the logical run or invent a remote outcome. A later verified
acceptance can still reconcile a sealed retired cut into a compact receipt.

Four initial scope race tests passed, then all 699 mobile unit tests passed.
The final focused set has six passing cases, adding shared-database proof that
retiring Alice leaves Bob's transport and publication authority intact, and that
a later verified acceptance reconciles a retired sealed cut without reviving the
cancelled source. The set
covers wrong account/catalog/session, generic/legacy bypass refusal, retirement
before capture, and retirement/cancellation while upload is held. All 85 backend
source-cut, continuity, and retention tests passed, as did strict mobile and
storage Clippy. The six engine export/effect-bound regressions also passed.
This is storage authority groundwork; public
scoped export/acknowledgement APIs, frozen catalog-policy validation, scoped
import admission, and archive/quota semantics remain open.

The physical-instance identity regression reproduced an unsafe acknowledgement:
an isolated destination could reuse the source UUID, and storage accepted those
coordinates even though local scheduling identifies ownership by instance ID.
Continuity IDs identify logical runs; destination instance IDs identify a fresh
physical execution. Public import, direct recovery admission in both backends,
atomic activation, and source acknowledgement now independently refuse immediate
source UUID reuse. Nine engine capsule tests pass, including rejection before
remote persistence, preservation of the source transfer fence, a successful fresh
ID retry, and superseded source ownership after valid acknowledgement. All 131
source-cut and portable recovery tests passed across SQLite, encrypted SQLite,
PostgreSQL, and encrypted PostgreSQL, including rollback and persisted legacy-state
refusal. Strict storage and engine Clippy passed for all targets with warnings
denied. Documentation and whitespace checks passed. This does not
prove arbitrary historical instance ID reuse safe; callers must choose fresh IDs.
Public mobile-origin enrollment/checkpoint ergonomics also need review; current
constructor coverage explicitly prepares those durable records in its fixture.

#### DME180 migration round-trip and complete guard audit follow-up

A real PostgreSQL migration 129 down/up regression passed. Empty installations
can roll back and reapply, including repeated application. Captured and sealed
cuts prevent rollback; refused DDL preserves exact sealed retry bytes. Compact
acknowledgement receipts also prevent rollback. After source-cut history is
explicitly purged, independent artifact-retirement tombstones still prevent
rollback and remain claimable. Only explicit reconciliation/removal of every
retained row permits down; reapplying preserves independent continuity ownership.

A separate regression reproduced a false Pass in the effect guard journal:
10,000 prepared receipts preceded two committed duplicates, and the capped list
omitted both duplicates. Atomic dispatch admission remained protected, but its
audit report did not describe the complete ledger. Both backends now aggregate
all dispatched/committed/unknown/verified receipts in one SQL statement/snapshot.
The verdict and counts cover every matching row; only the sorted duplicate
witness sample is capped (16 groups). Proposed dispatches replace their stored
identity rather than counting the same receipt twice. Wrappers forward this
primitive; unsupported custom backends fail explicitly rather than certify a
partial ledger. No migration or unbounded engine-side receipt collection is used.

Twenty receipt paging/audit tests passed on SQLite, encrypted SQLite, PostgreSQL,
and encrypted PostgreSQL. Cases cover empty history, state/kind filtering,
destination grouping, cross-tenant/continuity isolation, stable bounded witnesses,
proposal replacement and refusal of changed scope/kind/state. All 19 engine
effect-guard tests and strict storage/engine Clippy (all targets, warnings denied)
passed. Atomic duplicate refusals also fail the audit when
they reserve an earlier Prepared attempt; they cannot be relabeled Pass merely
because the earlier attempt has not dispatched yet. Audit fingerprints bind the
verdict, counts, candidate request/destination and bounded witnesses; they are
aggregate fingerprints rather than a cryptographic digest of every raw receipt.

Scope remains open: manual API invariant evaluation, compensation planning,
handoff compatibility and extracted fixtures still consume capped receipt lists
at `orch8-api/src/continuity.rs`; they need complete evidence semantics as well.
Mobile-origin enrollment needs a durable runtime identity established before
universal effect enrollment (which currently derives a per-instance runtime
namespace). Adding a facade method alone would not solve that ownership model.
The public scoped portability/archive and wait/event/family/artifact transport
requirements listed above remain outstanding.

PR357 merged as d3097076955bcea148771617f0b9203c812f1f1e and PR358 merged as
80e45155807001dd093910cc88d82ee425419950, each after all 25 checks passed. The
existing merge watchers verified exact heads, tested trees and ancestry before
removing the remote branches. The uncommitted source-cut/audit work was preserved.

#### DME180 API safety decisions beyond the receipt prefix

Two real HTTP regressions reproduced unsafe API decisions after 10,000 receipts:
manual NoUnknownEffects and EffectAtMostOnce evaluation returned Pass despite late
unknown/duplicate effects, and handoff preview returned compatible despite 21
late unresolved effects. Both paths have now stopped using the capped legacy list.

SQLite and PostgreSQL expose complete ledger state counts under a single SQL
statement/snapshot, retaining at most 16 unresolved receipts and 16 unknown request
witnesses. Empty and foreign-tenant/continuity scopes remain empty. Missing or
unrecognized receipt states make complete safety queries fail rather than silently
classify unparseable history as safe. The existing complete duplicate audit now
shares that state classification refusal. Plain/encrypted adapters forward the
summary operation; unsupported custom adapters fail explicitly.

Manual evaluation uses a shared engine batch evaluator. Receipt-based rules use
complete duplicate audits or complete unknown counts, caching each kind and state
summary within the batch; other rules keep their existing non-receipt evidence.
Public evidence vectors remain bounded samples of matching receipt request hashes.
Aggregate fingerprints bind deduplication separately. Results are computed before journal appends, so evidence-fetch
failure cannot first append a partial set of verdicts from that batch.

Handoff preview compatibility uses the full unresolved count. Its response adds
`unresolved_effect_count` and `unresolved_effects_truncated`; `unresolved_effects`
is now an explicit bounded sample. Preview digests bind the complete unresolved
count and sample. Handoff creation already rebuilds the preview, so it uses the
same complete safety predicate before accepting a placement decision/preview hash.
Clients must use the count/truncation fields rather than infer complete history
from the sample. This is a point-in-time decision; it does not claim to serialize
all later effect writes with handoff creation.

All 28 receipt paging/audit/state-summary backend tests passed across SQLite,
encrypted SQLite, PostgreSQL and encrypted PostgreSQL. All ten final HTTP tests
passed, including resolution/compensation restoring a Pass and refusal when a late
unknown receipt appears after a valid preview. Strict all-target Clippy passed
for storage, engine and API with warnings denied. Remaining capped API paths include receipt display, extracted
fixtures and compensation planning. Selection of workflow invariant definitions
itself still uses its existing 10,000-row limit; this receipt-evidence fix does not
claim complete enumeration of an arbitrarily large policy set. Full scoped/mobile
portability, source enrollment, archive/quota and transport dependencies remain
open as recorded above.


The complete receipt-evidence fixes were isolated onto current main
(80e45155807001dd093910cc88d82ee425419950) and published as
[PR359](https://github.com/orch8-io/engine/pull/359), head
ed969d6105844195d40f5cbcdda7b5468fb2539e. That exact independent patch passed
54 targeted tests (28 backend, 19 engine, 7 HTTP), strict all-target Clippy,
formatting, documentation and whitespace checks. It excludes pending source-cut,
scoped portability and migration 129 changes. A digest proved the active source
state stayed unchanged during isolation/publication. The clean temporary checkout
was removed after the verified commit was pushed; only the primary worktree
remains. Its exact-head/all-check merge watcher is running; no merge is claimed.

#### DME180 complete compensation evidence and bounded dependency traversal

The compensation HTTP regression reproduced omission after the 10,000-row prefix:
a history with 10,001 committed effects and 21 late unknown effects produced only
10,000 compensation steps and omitted the later uncertainty. Both backends now
stream a single complete SELECT snapshot, validate each raw receipt's indexed
identity and scope, and retain only compact committed/verified/unknown planning
fields. Unrelated raw history is not accumulated. Plan/output memory remains
linear in actionable steps and hazards; this does not claim constant-size plans.

The planner groups receipts by block once rather than scanning every receipt for
every block. It sorts each group newest-first with UUID tie-breaking and preserves
reverse dependency order. An explicit DFS frame stack replaces recursive graph
traversal. A 5,000-block dependency chain passes on a 256 KiB thread stack.

All 33 storage tests passed, including plain/encrypted SQLite/PostgreSQL and a
PostgreSQL advisory-lock barrier test: settlement changes while collection is
paused cannot alter the original SELECT snapshot, and a fresh collection observes
the update. Eight planner tests and eleven real HTTP tests passed. The HTTP case
also creates and reloads a durable 10,001-step plan with all 21 uncertainty hazards.
Strict all-target Clippy passed for storage, engine and API with warnings denied.

The broader compensation lifecycle still needs work. Run creation currently
inserts a record without atomically fencing forward execution; completion checks
run steps/residuals rather than reconciling the final effect ledger. Dispatched
receipts are not currently represented as uncertainty hazards. Template parameters
are resolved against current instance context/outputs rather than immutable
per-invocation compensation inputs. These existing gaps require source fencing,
late-outcome reconciliation and durable invocation-specific bindings before the
full compensation protocol is considered reviewed. The complete-history fix does
not certify those lifecycle contracts.


The compensation-history fix was published as
[PR360](https://github.com/orch8-io/engine/pull/360) after 49 targeted tests on its
isolated parent-based checkout (33 storage, 8 planner, 8 HTTP), strict all-target
Clippy, formatting, documentation and whitespace checks. It is gated behind PR359.

CI caught a response-contract regression in PR359: adding an aggregate fingerprint
to `evidence_sha256` made fresh NoUnknownEffects evidence nonempty and added an
extra entry for one unknown effect. The production code was corrected rather
than weakening the existing client assertions. Manual and automatic guard reports
now expose matching receipt request hashes only; aggregate fingerprints bind the
deduplication key separately. A guard regression checks every exposed evidence
hash against stored receipt requests.

The repaired stack passed all 123 original TypeScript invariant/compensation/
runtime-registration tests against its own real server and isolated PostgreSQL,
19 guard tests, eight HTTP tests, strict all-target Clippy, formatting and docs.
PR359 is now head 210da6ff9353832b4debf010339381c404befbf3; PR360 is rebased onto
it at head 696a49a48124d53f97e8b102f798a9bd52806aa0. Both heads were atomically
pushed with exact prior-head leases. Source digests verified preservation of the
active work, and both clean repair worktrees were removed. Exact-head merge
watchers have restarted; PR360 waits for PR359's tested merge proof. Neither PR
is claimed merged while CI remains in progress.

#### DME180 unsettled work remains visible during compensation

A new planner regression reproduced omission of three unsettled states: a history
containing Planned, Prepared, Dispatched and Unknown effects retained only one
hazard. Complete compensation evidence now retains all unsettled states alongside
committed/verified outcomes, while compensated/abandoned history remains excluded.
Plans expose `PENDING_EFFECT_MUST_BE_SETTLED` for planned/prepared receipts and
`IN_FLIGHT_EFFECT_MUST_BE_SETTLED` for dispatched receipts. Existing unknown-outcome
hazards retain their original representation.

The full HTTP recovery flow passed: one known committed effect is rolled back,
while three unsettled effects preserve CompletedWithResiduals. After the backend
confirms the in-flight commit and abandons never-dispatched work through the
normal resolution API, the next plan contains only that late committed effect.
Completing it produces a clean Completed result without compensating the prior
already-compensated receipt again.

The isolated stack passed all 55 Rust cases (37 backend, nine planner and
nine HTTP evidence tests), 124 TypeScript E2E cases, strict storage/engine/API
Clippy, formatting, documentation and whitespace checks. The real-worker case
verifies a dispatched effect remains an in-flight hazard. Published as
[PR361](https://github.com/orch8-io/engine/pull/361), initially at head
45796a72800a7214469f67c78b4bcdbecded6799, gated behind PR360. This addresses the missing unsettled-state hazards, not the remaining
source-forward fencing, final-ledger reconciliation or per-invocation binding
requirements described above. A receipt created after the plan's read snapshot
still requires those broader lifecycle contracts.


CI then exposed a second compatibility regression in PR359 and PR360: the
portable-continuity client expects the duplicate-guard summary to retain
`duplicate effect dispatch or commit evidence`. The refusal and durable failure
were correct; the complete-ledger summary had dropped that established phrase.
The repair restores the existing wording while retaining complete counts and
bounded witness details for both automatic and manual evaluations. The original
portable-continuity assertion remains unchanged. Both merge watchers stopped
on the failed jobs; no failed revision was merged.


The summary-contract repair passed the unchanged real-server portable duplicate
guard case, all 124 invariant/compensation/runtime E2E cases and 19 guard tests.
Strict all-target engine Clippy, formatting, documentation and whitespace checks
passed. The three PRs were updated atomically with exact prior-head leases:
PR359 `c455b10fb66380de64665d92e989eb6c58ba5981`, PR360 `c1b2aeb562a6be3dca967d86c6064502f92fd99d`,
PR361 `27aa80318c0c47259c5ccc0522cbfb8b7f451d92`. Source digests verified preservation of
the broader active changes. All completed temporary worktrees were removed;
only the active source-cut worktree remains. CI and tested-tree merge proofs
are still required before any of these revisions is claimed merged.


#### DME181 generic ownership CAS must preserve frozen source authority

A new backend-matrix regression reproduced a source-fence bypass on plain and
encrypted SQLite/PostgreSQL: after capture, the generic ownership CAS accepted a
stale same-epoch, same-runtime `Owned` record and revived the frozen physical
source. Its compare token did not include the source-cut phase. That undermines
both transfer exclusion and any later compensation protocol built on the fence.

Generic CAS now refuses to point ownership back to the physical source of a
captured, sealed or retired cut. Retired transport payloads do not authorize
source revival. A legitimate destination with a different physical instance can
still become owner; explicit abort may restore a merely captured source through
its existing transaction. CAS also rejects a record whose tenant/continuity
identity differs from the row named by its arguments.

PostgreSQL locks the ownership row before issuing the update and cut check as a
fresh statement. Adding only a subquery to the original UPDATE is insufficient:
its READ COMMITTED snapshot can miss a cut committed while the UPDATE waits for
capture's row lock. SQLite takes its immediate writer transaction before the
check. Validation is in progress, including a controlled PostgreSQL lock race.

Compensation startup remains an open protocol item. Its eventual durable barrier
must stop new forward admissions, preserve settlement of already-dispatched
workers, bind the plan to the frozen owner/instance and include effects arriving
between plan construction and the freeze. The existing transfer fence rejects
worker lease mutations, so reusing it as a compensation fence would discard valid
outcome reporting. A plain pause can be undone and is not a durable substitute.


The controlled PostgreSQL race pauses actual capsule capture at its cut INSERT,
then proves generic CAS is blocked on that capture's ownership row. Once capture
commits, the repaired CAS refuses the stale update. A mutation run removing only
the explicit pre-lock reproduced source revival despite retaining the UPDATE's
cut predicate. The original production fix was restored after the failed run.
The initial 69-case source-cut matrix and the repaired race passed; expanded
continuity/recovery coverage and strict lint are being verified on the restored
source. This source-cut repair remains part of the broader unmerged transfer work.


Final restored-source verification passed 155 tests: 74 source-cut cases,
14 general continuity cases and 67 portable-effect recovery cases. This includes
all plain/encrypted SQLite/PostgreSQL legs, the controlled capture/CAS race,
identity-mismatch refusal and valid post-abort/post-transfer operations. Strict
all-target storage Clippy with warnings denied, formatting, documentation and
whitespace checks passed. No local Rust compiler remains active from these runs.


#### DME182 atomic compensation startup separates admission from settlement

A real HTTP regression reproduced that rollback creation left the source owner
`owned` and did not durably stop forward execution. The new `compensating`
ownership state permanently closes forward effect admission at the same epoch.
Atomic startup locks ownership and the source instance, compares the owner,
instance metadata and complete compact effect set used by the plan, inserts the
run, updates ownership and parks nonterminal source execution in one transaction.
A changed snapshot or a concurrent active run refuses admission without a partial
freeze. This includes effects that appeared while the preview was being built.

Scheduler advancement, native effect admission, worker polling and generic owner
CAS cannot resume a compensating source. HTTP and gRPC workers can still
heartbeat and settle already-dispatched outcomes; completion records those
outcomes without advancing forward execution. A subsequent explicitly requested
run can compensate newly confirmed outcomes while the durable source fence
remains. No new owner epoch is invented, and ordinary instance state changes do
not authorize new effects.

All 24 new storage cases passed across plain/encrypted SQLite/PostgreSQL. They
cover concurrent starts, injected failure between run insertion and owner freeze,
stale effect evidence, prepared forward admission, all worker poll variants and
preserved in-flight heartbeat/completion authority. The initial six HTTP evidence
cases passed, including the startup regression. Real-worker E2E and cross-crate
verification are in progress. The new ownership value requires readers that
understand `compensating`; old readers fail to decode rather than reopening the
source. This feature is not published as a release.

Remaining work includes immutable per-invocation compensation parameters/rules,
explicit outcomes/archival for a compensation-fenced paused source, remote/mobile
owner acknowledgement where authority is held in a separate database, and deeper
final-ledger reconciliation against administrative/manual effect resolution.
The existing raw compensation-record insert remains a storage primitive; public
HTTP startup uses the new atomic admission method.


Further startup review reproduced two bypasses on all four backends: a cached live
migration could rewrite compensating ownership, and attempt identity preparation
could append a new planned receipt after the fence. Compound migration/handoff
updates now exclude `compensating`, and attempt preparation checks the state under
its ownership lock. All named queue paths also check the durable fence, with
positive controls proving that other eligible executions still dispatch.
The expanded isolated matrix passed 150 cases (40 compensation/startup, 14 general
continuity, 37 complete ledger and 59 portable recovery). HTTP/gRPC and E2E checks
remain in progress on the exact isolated patch.

The full distributed proof remains incomplete for independent offline owner
stores and legacy worker rows without effect bindings. Existing legacy completion
is intentionally ledger-free in `commit_external_worker_effect`; a local source
freeze cannot by itself account for such an unrecorded outcome. Those require
explicit owner stop/outcome acknowledgement and durable worker receipt adoption,
not an assumption that the local ledger already contains every remote effect.


Final isolated compensation-fence verification passed 189 Rust tests (150 storage/
continuity/ledger/recovery, six HTTP, 14 gRPC and 19 engine guard), all 125 TypeScript
E2E cases, strict all-target storage/engine/API/gRPC/CLI/mobile Clippy, formatting,
typecheck and docs checks. The final real-worker case records an in-flight commit,
refuses forward propagation after ordinary rescheduling and explicitly compensates
that newly known outcome in the next run. Complete ledger checks and tenant-indexed
poll fences were tested on the final isolated source.

PR359 merged at `0f95e86e309b73b140a7fb8b1a0a9940c86e21cc`, PR360 at
`4c9f2ee830342db852df64707a22098f5b321d23`, and PR361 at
`6c84d6c00441caa569dba54ddf8566fc23c04494`. Each had 25 successful checks,
exact tested/merged tree equality and head ancestry proof. Their merged local and
remote branches were removed. The compensation-fence patch is being published
separately from the remaining transfer work.


The isolated compensation fence was published as [PR362](https://github.com/orch8-io/engine/pull/362)
at head `973888ff9d61f5d4ffdf17a7190d45437410d82a` on merged main
`6c84d6c00441caa569dba54ddf8566fc23c04494`. Its completed clean temporary worktree was removed
after verifying the pushed head. The active review branch now uses that main
base; a file manifest proved preservation of every working source file through
the index/base change. The three merged review documents were materialized from
their exact committed blobs. Transfer/mobile work remains uncommitted; PR362
requires its own complete CI and tested-tree merge proof.


#### DME183 compensation creation needs lost-response recovery

The next confirmed lifecycle gap is a lost creation response. Startup allocates a
random run ID, persists the run and returns it only in the POST response. The
public API can fetch a run only by its known ID; the storage trait has no active
lookup by continuity. A retry receives the active-run conflict without learning
the committed ID. With a durable source fence, recovery must not depend on the
client having received that first acknowledgement.

Add an optional durable request UUID to creation, scope its deterministic run
identity to tenant and continuity, and return the original persisted run for a
retry of the same intent, including after completion. Preserve the existing
conflict behavior for distinct/legacy requests. Verify lost acknowledgement,
same-key concurrent starts, immutable plan replay after evidence changes,
completed-run replay and tenant/continuity isolation. This gap is still open;
PR362 provides the local source fence and does not claim to resolve it.


PR362 CI repair: deterministic sub-microsecond instance timestamps reproduced
the PostgreSQL admission failures. Comparing the bound expected timestamp in
the locking SQL predicate fixes native precision without weakening rejection of
changed state, sequence or revision. All 40 storage cases pass across plain and
encrypted PostgreSQL/SQLite. HTTP admission retries up to eight fresh complete
snapshots when normal completion races planning; the seven isolated HTTP tests
include a forced completion barrier. Nested cancellation now uses a durable
input barrier rather than a 200ms assumption.

Full shared verification exposed hard-coded event-test ports and SSRF tests
attached to a server configured to allow private networks. Event tests now honor
the configured URL/port. SSRF tests moved into the common self-managed registry
and explicitly disable private access. A reachable local HTTP positive control
proves the blocked workflow sends no request; it fails with protection disabled.
All 1,586 shared tests and five standalone SSRF cases pass, plus strict all-target
Clippy across storage/engine/API/gRPC/CLI/mobile, fmt, typecheck and docs.

DME183 now has an uncommitted recovery implementation with all 15 HTTP safety
tests passing on the active source: same-intent concurrent retries, saved-plan
replay, completed replay, committed-acknowledgement loss, legacy active lookup
and scope/auth validation. Backend/CLI/E2E verification and isolated publication
remain necessary. Switching checkouts with a shared Cargo target can reuse a
newer test executable from the other checkout; workspace input mtimes were
refreshed before this run, the root crates rebuilt, and the expected 15-test
count was checked rather than accepting an obsolete seven-test executable.

The CI repair is published at `f2c1fd9b1b274865ffe02c7ebe8ac3d5747ee027`
(tree `25e84b239ac5e3b31e7fa37b9168653cf1a71bcb`). The completed repair
worktree was removed after checking clean status and the exact pushed head; a
content digest proved preservation of the active source. Only the primary
checkout remains. The exact-head watcher is gating PR362 on all 25 successful
checks and requires tested/merged tree equality and ancestry before branch
cleanup. The PR is not yet merged.

DME183 refreshed-source verification also passed all 48 compensation storage
cases across both backends and encryption wrappers, including active-state
lookup, scope isolation, terminal hiding, decrypted parameters and fail-closed
record/index mismatch checks. Formatting, TypeScript and documentation checks
passed; isolated follow-up publication remains pending.

DME183 strict all-target Clippy also passed for storage, API and CLI (including
their engine dependencies) on the refreshed root source.


DME183 is implemented in [PR363](https://github.com/orch8-io/engine/pull/363),
head `f92626a10330bd32aeaca09e5e71a59c5d1ba2d2`, tested tree
`460f8ce4e5d77ec5eef1fdcfccf32cf4cf904786`, based on the exact repaired
PR362 head. Isolation copied only the compensation API region, active lookup
methods/forwarders, recovery CLI, tests and operations docs; the broader source-cut
work remained outside the PR. The final isolated stack passed 64 Rust cases
(48 storage, 15 HTTP, one identity unit), 129 real-server PostgreSQL E2E cases,
strict all-target storage/engine/API/gRPC/CLI/mobile Clippy, fmt, typecheck and
docs. The completed clean worktree was removed after verifying its exact pushed
head; root source content was preserved. CI and merge remain pending.

The isolation review additionally reproduced inconsistent known-ID reads on all
four storage variants. Active and known-ID lookups now share validated decoding
against indexed tenant, continuity, ID, state and version. The corruption matrix
checks all five fields on both paths. The prerequisite-aware PR363 watcher waits
for PR362's completed tested-tree/ancestry merge proof before considering the
follow-up's own 25 CI checks and exact-head merge proof.

#### DME184 compensation mutation identity needs adversarial review

Both backend `cas_compensation_run` implementations currently predicate writes
on the caller's tenant, run ID and expected version while serializing the whole
next record. Reader consistency checks now detect mismatched scope and progress,
but mutation should also prevent introducing inconsistent identity or regressing
revision data. The next review should reproduce tenant/continuity/source identity
changes and invalid revision increments through the storage API, then enforce
only the invariants supported by the actual compensation lifecycle. This is a
review candidate, not yet a verified fix. Immutable invocation-specific rules
and parameters, offline owner acknowledgement and explicit archival outcomes
remain separate required work.


PR362's repaired head `f2c1fd9b` passed the shared suite but its standalone
lifecycle suite still expected a completed compensation to return to `waiting`.
The source correctly remained paused. The correction is published at
`a7490e7f44ecb70dc54ff4140407f3f471eb439f`, tree
`b7f52dfd4430ba7b32ed165477a658b97d4c32e6`. Only the integration test and
source-fence review document changed; production Rust is unchanged. All 221
lifecycle/isolation cases passed with assertions that migration planning and
rejected apply preserve paused/compensating authority and compensated evidence.
The fixture uses its own OS-temporary artifact directory and removes it after
server shutdown; verification found no new directory left behind.

The recovery PR363 was refreshed onto that correction at
`bfa7d369024b82455ef015a819082f94778a2aeb`, tree
`06d7183baea8aea21f52332bd9eca44c1e272c81`. Its Rust code matches the earlier
verified recovery source. The exact refreshed server passed 221 lifecycle/
isolation and 129 invariant/compensation/runtime cases; typecheck, formatting
and docs checks pass. The old stopped watchers were archived and restarted for
the exact new heads.

DME184 is now reproduced and fixed in [PR364](https://github.com/orch8-io/engine/pull/364),
head `f1fceaa67a3891db6ce46f0c5dd2e62a4a4b4cdb`, tested tree
`a20a3c72d0b397da7a23651a38cce08268c5b006`, based on refreshed PR363. The
old CAS accepted a foreign tenant on all four storage variants. Both backends
now validate tenant and consecutive, SQL-storable revision inputs, and bind
continuity, source instance and original JSON creation time in their atomic
UPDATE. Rejected identity/revision changes preserve the stored run. Concurrent
valid claims still have one winner, and fresh parameter ciphertext remains
supported. A restored run at the final storable revision previously returned
500 on claim; version advancement now returns 409 without changing progress.

The final isolated guard stack passed 86 Rust cases (56 compensation storage,
14 continuity, 16 HTTP), 350 real-server PostgreSQL E2E cases (221 lifecycle/
isolation and 129 invariant/compensation/runtime), strict all-target Clippy for
storage/engine/API/gRPC/CLI/mobile, formatting, typecheck and docs. Completed
source-fence repair, recovery sync and mutation checkouts were removed only
after clean/pushed-head verification. A root content digest proved preservation
of every active working source through publication and final cleanup; only the
primary worktree remains.

CI and merges remain pending: PR362 has its own 25-check exact-head gate; PR363
waits for PR362's complete tested-tree/ancestry merge proof; PR364 then waits for
PR363's equivalent proof before evaluating its own 25 checks. No release is
published. Immutable invocation-specific compensation rules/parameters, offline
owner acknowledgement, explicit archive outcomes and the broader source-cut/
mobile work remain required; this closes the demonstrated mutation gap only.


#### DME185 acknowledged source cuts must keep their ownership fence

Fresh root compilation and the complete mobile library suite passed 701 tests
with the compensation guards and scoped source-cut changes. The next review
then reproduced a source authority regression on all four storage variants:
`cas_continuity_owner` excluded acknowledged cuts from its refusal, allowing the
accepted destination owner to point back to the old physical source at the same
epoch or a newer one. Both backends now retain that acknowledged-cut fence.

The final targeted matrix passed 159 cases (78 source-cut, 14 continuity,
67 portable recovery), plus strict all-target storage Clippy and docs checks.
The new test preserves the accepted owner after each refused source revival,
allows a genuinely fresh later destination and refuses the old source again
after that later move. Existing delayed-acknowledgement and portable proof/
recovery cases remain green. Logs: `/private/tmp/orch8-mobile-source-cut-final.log`,
`/private/tmp/orch8-acknowledged-source-before.log`,
`/private/tmp/orch8-source-cut-ack-final.log`,
`/private/tmp/orch8-source-cut-ack-lint.log`. This fix remains in the broader
unpublished source-cut work.

#### DME186 physical identity must survive history GC and archival

Retained-cut protection alone is insufficient. Source cuts are continuity
children removed by history purge, new continuity creation currently checks
import bindings rather than all retired sources, and compound import/activation
paths write ownership directly. A physical UUID must not gain fresh authority
through another continuity/account/catalog/tenant or through restore after GC.
The archive API also currently treats only terminal native states as outcomes;
a transferred source remains paused, and account retirement can locally cancel
it even though a verified destination owns the continuing logical execution.

The required identity lifetime, concurrency, downgrade and explicit-archive
verification gates are recorded in
[Physical instance lifetime and acknowledged archival](2026-10-27-physical-instance-lifetime.md).
The next implementation must use compact independent retirement evidence, cover
all admission/restore/ownership paths, preserve valid late outcomes and callback
quarantine, and represent transferred ownership in archival/history without
inventing completion. Source-cut publication remains pending these safety and
lifecycle contracts; immutable invocation-specific compensation inputs and
independent offline-owner acknowledgement remain required review work too.

#### DME187 permanent physical identity after history collection

A four-backend regression used real terminal-instance purge after acknowledged
transfer, then recreated the source physical ID as scheduled work. Every plain
and encrypted SQLite/PostgreSQL leg reproduced the authority-loss boundary.
The current work adds independent compact retirement evidence (PostgreSQL 130,
SQLite 60), backfills retained sealed/retired/acknowledged cuts and installs
database guards for native insertion and new/revived continuity ownership.
Markers contain IDs and retirement time, no payload/context, and survive native,
continuity and artifact history GC. Marker mutation/deletion is refused;
PostgreSQL rollback cannot remove nonempty security evidence. SQLite trigger
bodies are installed atomically outside its schema statement splitter.

The expanded run passes all 83 source-cut and 67 portable-recovery tests. Direct
coverage includes admitted writes, snapshot restore, mixed fresh/retired batch
atomicity, foreign tenants/new continuities, valid fresh IDs and a controlled
PostgreSQL insertion that resumes after waiting for the retiring transaction's
uniqueness lock. Strict all-target storage Clippy and documentation checks pass.
The full mobile suite is being rerun against this schema/trigger change.

This does not yet complete the lifetime contract. All existing-row forward
mutation/claim paths, inconsistent legacy ownership, upgrade/reopen/downgrade
writer cases, compound recovery/activation races and transfer-aware acknowledged
archive/native payload GC still require explicit tests and implementation.
Detailed evidence and remaining gates are in
[the lifetime plan](2026-10-27-physical-instance-lifetime.md).

DME187 validation update: the full mobile library suite passes all 701 tests
against the first retirement implementation, with no failures or skips. The
remaining lifetime gates and bounded SQLite backfill work remain open.

#### DME188 existing source rows, bounded upgrade and late outcomes

Retirement evidence now gates native claiming and every global/tenant/capability
worker polling path, including PostgreSQL's second capability-claim phase. An
eight-case four-backend baseline demonstrated that collecting destination
continuity history revived an existing source row and pending legacy task;
those regressions now pass alongside healthy native/worker controls. Native
per-tenant and uncapped modes refuse the source without modifying its original
claim; pending legacy evidence is retained. The engine checks permanent physical
retirement before treating missing continuity as legacy ownership, and propagates
retirement-read errors. Encryption/fault decorators preserve that contract.

SQLite schema-60 evidence/guard installation and its completion marker commit in
one immediate transaction. A failed-backfill injection proves failed open and no
partial completion; retry backfills old cuts, and subsequent reopen skips the
completed history scan. Rollback dependency order now preserves the permanent
source-recording trigger. Combined storage tests pass 215 cases; the engine
missing-history/foreign-scope/read-failure test passes on SQLite and PostgreSQL.

A real HTTP worker completion and changed-payload retry now verify all four
backends retain the original outcome without rescheduling the retired source,
restoring context or publishing a forward block output. All 20 HTTP safety cases
pass. Older-writer forward state changes, captured-cut admission and compound
races still require explicit enforcement/verification; transferred archive
receipts, native payload GC and immutable invocation-specific compensation
parameters remain open. The full objective is not complete.

DME188 validation update: strict all-target Clippy passes for all six affected
packages; both SQLite point-index tests and the complete 701-test mobile library
suite pass after the new admission/one-time-backfill changes. The 215 storage,
20 HTTP and engine authority checks remain green. One completed build executable
from an already removed repair worktree was removed (277,535,344 bytes); only the
active root worktree remains. The broader lifetime/portability objective remains
open with its remaining gates in the lifetime plan.

#### DME189 persistent source freeze and PostgreSQL worker snapshot freshness

Captured sources are excluded from native/worker claiming and mixed scheduling
batches; permanent native forward writes and legacy worker claims are also
refused by database triggers. Recording retirement atomically quarantines native
state without deleting context or existing worker outcome evidence. SQLite 61
backfills quarantine once and commits its completion atomically; interrupted
completion rolls back, retry repairs and reopen skips the completed scan.

A controlled PostgreSQL test demonstrated a repeatable-read capture returning a
snapshot that missed a worker committed while capture waited on its foreign-key
parent lock. New enqueue now touches the parent's MVCC version before its worker
row/index and refuses enqueue after capture. Stale capture aborts, and fresh
capture sees the pending task. Matching parent-before-worker resolution/retry
ordering passes a deterministic duplicate-enqueue/retry barrier without deadlock.
This worker-specific proof does not complete the other child-evidence race audit.

Expanded validation passes 123 full-matrix source-cut cases plus one schema-61
atomic/reopen case, 56 compensation, 67 portable-recovery, 136 storage integration
and 20 HTTP safety cases. Strict lint passes across storage, engine, API, gRPC,
CLI and mobile. Further scheduler/mobile checks and the remaining portability,
archive, invocation-binding and ingress gates remain active.

PR363 and PR364 merged after all 25 checks each, with exact tested-tree equality
and ancestry proved; their exact merged branches were deleted. The active review
branch advanced from 19c74d41 to main 21cb1454, preserving hashes/modes for all
2,195 existing working files and adding only the two merged review documents.
Only the active root worktree remains. No release was published.

DME189 scheduler update: a retirement stop must bypass a forward claim predicate
that deliberately rejects retired rows. It now relies on permanent retirement
plus a Running-to-Paused state CAS, preserving concurrent terminal outcomes and
the original claim fence for non-retired superseded work. All 11 selected engine
scheduler/driver tests pass, including the new inconsistent-retired-running-row
case on SQLite and PostgreSQL. The immutable retirement evidence never certifies
an independent/offline callback has stopped.

DME189 final local validation: all 701 mobile library cases pass after the final
source-freeze/enqueue/scheduler changes; both point-index checks and strict
six-package all-target Clippy pass. Main and origin/main are 21cb1454 after the
verified PR363/364 merges, with merged branches cleaned. The newer foundation is
being preserved in a draft PR while its named source/transfer/archive gates are
still under review; the full objective remains active.

#### DME190 production mobile artifact features and standalone lockfile

PR365 draft CI caught artifact constructors missing in a production-only mobile
build and a stale fuzz lockfile. The former reproduced locally without dev
feature unification. Mobile now opts into local filesystem/memory artifacts;
server artifact builds keep S3. The compiler feature trace verifies no object-store
cloud/S3 feature in the production mobile dependency graph. All 13 artifact cases
and all four locked workspace metadata gates pass, with no dependency version
upgrades. The fuzz repair adds only the existing SHA-256 dependency reference.
A native mobile build and refreshed CI remain the next verification steps.
Ten obsolete, non-running test executables were removed (1,119,354,208 bytes),
with newer-input, Cargo test-fingerprint and process checks recorded separately.

DME190 update: the production mobile native library build and strict
production-only Clippy both pass. The compiler graph verifies local artifacts
without cloud clients, and the lock repair changes no dependency versions.
Draft CI will be refreshed after the repair is pushed; portability/lifetime
behavioral gates remain open.

DME190 repair verification: combined six-package all-target strict Clippy passes
after the feature split, as do production-only mobile check/build/lint, the actual
no-cloud artifact feature trace, 13 artifact tests and all four locked graph gates.
The repaired revision will remain a draft with its existing behavioral gates.

#### DME191 compound worker resolution target identity

Reproduced ten unsafe transitions across the four-backend matrix: SQLite
accepted a valid task fence with another instance ID; both databases accepted
a replacement task under another instance or a node belonging to another
instance. The legacy retry API reproduced the mixed-parent mutation on all
four backends. These are storage API authorization-boundary failures even
when current engine callers normally construct matching targets.

Fenced resolution now validates the original task's instance and block, the
replacement task's instance and block, and the optional node's instance and
block before any mutation or attempt event. Missing/mismatched targets return
false. Legacy retry applies the same checks and returns a conflict. SQLite
uses BEGIN IMMEDIATE; PostgreSQL keeps native parent before worker and node
locks. Plain and encrypted tests include valid retries/node failure alongside
foreign-instance, foreign-node, wrong-block and missing-node refusals.

All 280 storage regressions pass: 20 identity cases, 124 source-cut cases
(including the controlled PostgreSQL enqueue/retry lock-order race), and 136
existing storage integration cases. Strict all-target storage Clippy also
passes. All 24 engine-level distributed scenarios and 23 HTTP continuity /
export-retry scenarios also pass. The identity fixtures additionally use
distinct tenants for foreign-target rejection; the final 20-case matrix and
24 distributed scenarios pass together.
This does not close the captured-owner, suspension-transport, durable transfer
receipt or immutable-invocation gates.

#### DME192 export regression fixtures must satisfy the new lifetime contract

The full engine run exposed an export-admission fixture with no artifact backend;
configuration rejection masked its intended worker-in-flight assertion. The
fixture now uses isolated in-memory artifacts on both SQLite and PostgreSQL,
while artifact-free fixtures remain available for configuration tests. The
24-case distributed suite passes.

Exact-head PR365 CI then failed all three HTTP retry scenarios because their
last step advanced ownership while pointing at the permanently retired source
ID. They now assert this revival is refused, create a fresh destination physical
instance, advance ownership there, and still assert stale export replay is
refused. All three scenarios pass alongside the 20 HTTP safety cases. No
production retirement guard was relaxed.

DME191/DME192 final validation: strict all-target Clippy passes across storage,
engine, API, gRPC, CLI and mobile. A separate SQLite-only storage library lint
with artifacts-local/test-util and no default features also passes, covering
the artifact-enabled test helper without PostgreSQL. Formatting, documentation
and whitespace checks pass. The broader transfer/archival gates remain open.

#### DME193 captured-source authority requires database enforcement

A four-backend baseline reproduced twelve unsafe transitions: direct native
forward-state writes after capture, raw old-writer ownership restoration, and
generic owner CAS advancing to a fresh physical destination before sealing.
The existing CAS predicate only rejected a destination that was itself a frozen
source; it did not freeze the originating continuity during capture.

PostgreSQL migration 131 and SQLite schema 62 add captured-source native and
owner guards. Native forward state, fresh claim/fire time, physical recreation,
owner/runtime/epoch/instance changes and JSON/indexed-state disagreement are
refused. A same-owner transferring timestamp refresh, compensation fence and
terminal native outcome remain admissible. Active captured/sealed cuts cannot
be silently deleted by an ordinary history sweep. Explicit unsealed abort marks
the cut aborted before restoring the owner/native state inside the same
transaction; a failure must roll back that release as well. Sealed publication
still permits ownership at a fresh destination; it never permits source revival.

SQLite installs complete trigger bodies outside the schema splitter. Its
captured-source quarantine and version-62 completion marker commit atomically,
then future opens skip the historical scan. PostgreSQL uses volatile AFTER
checks to re-read cuts committed while an older native/owner writer waited.
Pre-guard test fixtures explicitly disable/reinstall guards to inject invalid
legacy rows, rather than using normal forward APIs to create forbidden state.

New cases exercise authority rejection, valid abort, post-seal ownership,
compensation/terminal outcome preservation, abort rollback, active-cut deletion,
JSON/index disagreement, controlled PostgreSQL parent waits and interrupted
SQLite upgrade/reopen. All 175 source-cut cases, 56 compensation cases, 67 portable-recovery cases,
136 storage integration cases and 15 migration checks pass. All 701 mobile
case tests and 47 engine/HTTP integration cases also pass. Strict all-target
lint across the six affected runtime packages passes.
The broader child-evidence snapshot, suspension transport, accepted-transfer
receipt, archive and immutable-invocation gates remain open.

#### DME194 report the actual storage migration ceiling

The shared storage schema constant still reported 129 while the published draft
already contained migration 130. It now reports 131 alongside the new forward
migration. The migration-ceiling regression compares this constant with the
actual newest migration file, covering health/backup metadata drift.

#### DME195 retain active native history and keep admission checks indexed

Ordinary terminal purge already skips active source cuts; new mixed-batch
regressions verify it still deletes a healthy terminal sibling. Older raw DELETE
writers lacked that protection. Database guards now retain captured/sealed native
rows and their local input/suspension/outcome history until explicit abort,
retirement or acknowledgement. After release, normal terminal GC still succeeds.
An injected pre-guard missing native row cannot be recreated under the captured
physical ID, and its cut evidence remains. SQLite authorization-dependent replay
of already-lost native history is not certified by this result.

The installed SQLite trigger predicates initially caused full cut-table scans;
a regression reads their actual SQL from sqlite_master and EXPLAINs every cut
lookup. Explicit partial-index predicates and three separate owner identity
lookups now make all seven installed guard lookups indexed point searches.
Generic owner CAS also had a retained-history scan. It now uses the independent
permanent retirement key and separate active source/continuity point lookups on
both databases. A private-statement SQLite EXPLAIN test proves those exact CAS
queries use the existing identity and partial indexes without scanning history.
No extra history index or payload copy is added.

#### DME196 live migration must not retire its own rollback source (open)

PR365 head 349a7b87's E2E logs expose a production interaction: the live migration
apply endpoint in orch8-api/src/continuity.rs calls export_paused_capsule to
prepare a rollback capsule. That API now seals a physical ownership transfer,
permanently retiring the same source that the subsequent in-place migration
tries to retain. The database correctly refuses the revival. Live migration and
rollback need a separate coherent local recovery snapshot contract that does not
pretend a snapshot is a runtime handoff; do not relax source retirement to make
these tests pass. This is a merge gate alongside the remaining transport/archive
contracts. The E2E suite also retains an obsolete expectation that repeating an
identical exported handoff returns 409; the durable export retry contract instead
recovers the original response. That assertion needs updating and verification.

The same head's unit job exposed another artifact-free cloud fixture. The real
cloud/phone harness now configures isolated in-memory artifacts on each backend
so in-flight handoff tests reach worker/effect admission. All eight roundtrip
scenarios and strict harness/scenario lint pass.

DME193/DME195 final concurrency verification also includes native deletion
waiting behind capture, alongside native forward update, owner replacement and
downgrade. All four controlled scenarios refuse stale authority/history loss
after the wait. The final test orchestration passes strict lint. SQLite-only
embedded-feature lint, formatting, documentation and whitespace checks pass.
The DME196 migration/retry E2E work and broader transfer/archive gates remain open.


DME196 local recovery follow-up (work in progress): live migration now prepares a
bounded local source snapshot without a transfer cut, owner change, or physical
retirement. A separate purpose-bound signed/encrypted envelope authenticates the
plan, tenant, continuity, physical instance, original epoch, source definition,
checkpoint and expiry. Its stable create-only artifact has a durable independent
expiry cleanup reservation before upload. Rollback authenticates both the bytes
and the complete stored reference; a bare matching artifact hash is insufficient.

Commit locks continuity, native instance and plan; checks the exact target
sequence definition, native timestamp, latest checkpoint and a canonical digest
of the complete raw persisted plan/source evidence. Encryption decorators retain
that raw digest while opening source fields. Native execution nodes and queued
inputs remain in place; rollback restores the original absolute timer deadline.
No retirement guard is relaxed. PostgreSQL child-row locks protect existing
outputs, nodes, externalized payloads, checkpoints and effect receipts while the
final evidence is checked. Insert/callback races and generation semantics still
need their separate broader review.

Local verification: 16 HTTP cases pass across plain/encrypted SQLite/PostgreSQL,
including signed recovery with node/input/deadline preservation, changed source
context, newer paused target context and tampered signature metadata. Storage
snapshot cases (20), migration storage cases (14), compensation-start cases (56)
and effect transfer cases (40) pass. The latter now assert export itself freezes
authority, retired sources reject forward state writes, and direct provider entry
still fails. The new stale-evidence cases change output, execution tree, plan
payload or target definition without advancing native updated_at; each refuses
commit without advancing the owner/checkpoint or retiring the instance.

The stricter transition metadata/version validation and final expiry-after-wait
fencing pass the complete 146-case Rust regression run and strict all-target
Clippy across storage, engine and API. The three previously failing TypeScript
suites pass 299 cases per backend with fresh PostgreSQL/SQLite suite databases
(239 unchanged cases plus the final 60-case migration suite rerun). The rollback
case now delivers a real human response, reaches completion, preserves all prior
output history and produces exactly one acceptance receipt and one handler
result. TypeScript checking, minimal embedded storage lint and formatting pass. Legacy rollback envelope/key
rotation compatibility, atomic provenance, worker completion integration and the
other transport/archive merge gates remain open. This follow-up is not evidence
that PR365 is ready to merge.

#### DME197 retain the source definition through snapshot commit (fixed)

The PostgreSQL snapshot reader loaded the source sequence without retaining a
row lock. Local migration locks the destination definition, then compares its
full source fingerprint before updating the migration record. A concurrent
non-key source-definition update could commit after that comparison and before
the migration committed. The same reader is used by physical transfer capture.
SQLite already holds its writer reservation across the read and commit.

A controlled PostgreSQL regression pauses migration at its first record write,
after its final source proof. Before the fix, a source handler-definition edit
commits while migration is paused; the regression fails with “source definition
changed after the migration's final source proof.” The reader now takes FOR
SHARE on the source sequence, retained by the enclosing transaction. KEY SHARE
would allow the non-key definition edit. The writer must wait until migration
commits and then remains free to edit the original definition. All 21 local snapshot tests and 175 transfer-cut tests pass, including the
controlled source-definition race.

#### DME198 durable worker outcome integration must bind the invocation (open)

HTTP completion in orch8-api/src/workers.rs settles the effect, marks the task
completed, then separately writes native output/context/node/state. Its retry
path can repair a crash between these writes, but skips the old continuity-epoch
fence for an already completed task and finds a currently live node by block id.
A later migration or loop invocation can therefore be mistaken for the original
integration target. Paused/retired/foreign-owner native execution deliberately
skips propagation after accepting the task outcome; that evidence must remain
recoverable without the original worker retrying forever.

gRPC complete_task in orch8-grpc/src/service.rs uses the same separate task and
native writes, but rejects an already completed task instead of recovering the
integration. It also validates merged context size after settling the effect and
completing the task. Both snapshot readers currently exclude pending/claimed
workers but do not transport or reconcile a completed-but-unintegrated outcome.
Worker tasks carry continuity_epoch and effect_id, but no immutable native node
or invocation-generation binding. Cleanup/re-dispatch can remove old task rows.

Required next contract: one durable outcome and integration receipt per task
invocation, separate from hot task history, with exact tenant/physical execution,
continuity epoch, sequence and native node/generation binding. Atomic native
integration must consume the retained outcome at most once and preserve it when
the native target is paused, retired, migrated or gone. HTTP, gRPC, delegation
and background recovery must share this operation; a completion retry recovers
the original result without injecting it into a newer invocation. Context bounds
must be checked before first outcome commit while already durable provider
outcomes remain recorded for reconciliation. Pending integrations must be
reconciled or transported explicitly before a migration/transfer can certify a
complete recovery point. Blanket refusal of all waiting workflows is not a fix.

Verification must cover process failure after task completion, paused completion
followed by resume without another worker call, duplicate HTTP/gRPC callbacks,
old callbacks after loop reuse or migration, simultaneous completion/lease expiry,
completed outcome during capture, malformed/oversized output, scoped encryption,
receipt retention after native/task GC, and delegation recovery. DME198 remains
a merge gate; passing DME196 migration tests does not prove this contract.


#### DME199 retained execution nodes must keep their branch identity (fixed)

The migration compatibility check indexed block type and parent id, but omitted
branch_index. Moving a block to another parallel/race branch, router route or
try/catch arm retains its id/type/parent while changing the executable meaning
of the retained node. Completed native nodes also participate in branch outcome
selection, so accepting a completed node under a different branch is unsafe.

Two regressions fail before the fix: a waiting parallel node can change branch
without refusal, and a negative persisted branch index is accepted. The target
index now uses the same child-slice traversal and branch positions as native
build_nodes, and checks the retained branch identity as well as type and parent.
Unchanged nested loop/foreach bodies remain supported; a blanket refusal of
suspended/dynamic execution is not introduced. All 93 evaluator tests pass.
All 28 local recovery HTTP cases pass across plain/encrypted SQLite/PostgreSQL,
including rejection of a real parallel branch move, compatible parallel
apply/rollback with unchanged node identities/states and queued input, and
planning classification. Planning now reports INCOMPATIBLE:EXECUTION_GRAPH
before apply independently checks the coherent recovery point. All three
continuity E2E suites pass on PostgreSQL and SQLite (300 cases per backend:
239 unchanged cases plus the final 61-case migration suite rerun). A real
parallel human wait completes after rollback without replaying its completed
sibling branch or losing prior output history. Strict all-target Clippy,
TypeScript checking and formatting pass. Sibling execution order, dynamic
injection and invocation generation remain separate review gates.

DME198 binding design follow-up: task id/claim epoch must identify the durable
outcome; block id alone cannot identify its native integration target. Existing
RuntimeContext.run_id distinguishes retry/resume/DLQ runs, but is shared across
loop iterations and parallel activities. A tree outcome therefore also needs the
original node identity and activation generation (verify node.started_at reset
semantics before using that timestamp as a nonce). Flat execution needs the
original run/step-start binding. A global step counter is insufficient because
other parallel nodes can legitimately advance it. Capture the binding atomically
at durable dispatch, retain the accepted outcome/integration receipt independently
of native/task GC, and do not infer a missing legacy invocation binding from a
currently live node with the same block id. Scope/authentication for receipt
replay must remain enforceable when hot native rows have been collected.


#### DME198 implementation progress: dispatch admission (incomplete)

Local migrations 132–133 and SQLite schemas 63–64 separate invocation reservation
from actual dispatch admission. An execution node receives an activation token;
resetting a non-Pending node to Pending rotates it, while repeated Pending resets,
activation and same-activity resume retain it. started_at survives resets and
cannot serve as an invocation nonce. Immutable tenant-scoped bindings retain the
actual task ID, physical instance/sequence/run, block, attempt, mode, target
node/activation and full continuity ID/epoch/runtime independently of hot history.
Missing or ambiguous legacy identity remains Unbound evidence.

Both production dispatch paths capture their source fence before conditions,
output loading, templates and credentials, then carry it unchanged through
publication. Capture itself does not enroll ownership: doing so for in-process
pure work caused four embedded SDK capsule-export failures on a9477c51, because
an automatic owner displaced the caller's explicit initial runtime. For actual
remote work, a captured unowned source can enroll only its deterministic canonical
epoch-zero namespace inside the publication transaction. The transaction rechecks
the original claim/run/activation/mode and refuses foreign, advanced, transferring,
moved or historically orphaned ownership. Existing owners retain exact full-scope
fencing. Pure and dry-run remote tasks also receive stable durable authority.

An opaque validated dispatch intent binds the prepared logical payload to its
planned receipt, mode and run, and rejects stale lease/checkpoint/outcome fields.
Its debug output omits task inputs. Encryption decorators alone reseal transformed
storage payloads, retaining the logical fingerprint. The admission transaction
composes existing effect identity selection and at-most-once dispatch guards with
the exact queue payload, invocation binding, immutable admission and tree-node
suspension. Any refusal or insert failure rolls back the new receipt, active
attempt binding, initial owner/location, queue row and node changes. The separate
unfenced node update is removed; flat dispatch preserves the original timeout
baseline. Ownership, native parent, worker and node locks retain their ordering.

An unclaimable retry reservation receives the actual prepared handler, params,
context, requirements, routing, timeout and credential flag only during admission.
Its original scope binding is never rewritten. A separate immutable admission
pins the actual handler, logical/stored payload fingerprints, effect proof and
source. A unique key over physical/run/activation/attempt/mode/full-owner identity
makes a concurrent or repeated publication retain the first admitted payload and
receipt. Notifications and returns use the task ID actually retained by storage.
Cold admission lookup prevents recreation after hot task collection. Database
guards reject admitted specification changes, tenant-mirror changes and reuse of
an admitted task ID; lease/checkpoint/output mutations remain supported. Storage
source checks use admitted handler/effect identity and verify the stored payload
fingerprint, so a legitimate fallback can resolve under its original reservation.

Retry resolution checks the retained run, activation and full owner before
resetting native state. A replacement is reserved only after resetting the exact
original activation and remains unclaimable until admission. Retry markers are
transactional and encrypted; planning does not append history. Direct evaluators
persist run identity before callbacks. u16 counter exhaustion takes the terminal
path. PostgreSQL's SMALLINT attempt columns still require reconciliation with the
public u16 range; atomic admission explicitly rejects overflow instead of saturating.

Stranded recovery checks exact retained scope and waiting activation under locks,
recovers the epoch from immutable evidence, and follows historical locations.
Receipt recovery verifies request/destination hashes, kind, tenant, physical
execution, continuity/epoch, block, attempt and dispatched state. Missing or
uncertain effect evidence does not imply purity, and dry-run mode cannot become
live work. Unknown legacy recovery/quarantine policy remains a separate gate.

The gRPC context-size fix checks the projected merge before effect settlement or
consuming a worker claim. Rejected oversized outcomes retain their lease and
accept a corrected result. Concurrent acceptance/integration still needs the
atomic outcome contract below. Transaction finalizers await commit or rollback;
a rejected PostgreSQL acknowledgement no longer leaves an idle transaction holding
its owner lock. An independent pre-acquired connection verifies immediate release.

Current validation covers 38 new admission cases, 111 invocation cases, 20
resolution cases, 177 source-cut cases and 136 storage integration cases with
PostgreSQL required. Admission tests cover concurrent differing payloads, fallback
handlers, guarded duplicate rollback, later foreign ownership, stale generation,
context/mode/lease mismatch, injected insertion failure, encrypted privacy, cold
GC retention, immutable payloads/tenant routing, overflow refusal, downgrade
retention and SQLite reopen. The full engine suite passes 2,145 unit plus 29
distributed cases; the embedded public facade passes all 20 engine cases, including
the four failing CI exports. gRPC passes 148 cases and mobile passes 701, for 3,525
tests across these suites. Strict all-target lint passes for types, storage, engine,
gRPC and the embedded facade. Final tenant-mirror guard checks also pass.

The preceding three-file CI repair 0466f9ee fixes downgrade dependency lookup
falling through to an independent installation later on search_path. Its isolated
published-baseline candidate passed 176 source-cut cases and strict test-target
lint; all 25 exact-head checks passed. The later a9477c51 checkpoint's Unit job
failed the embedded export ownership cases described above. Exact-head CI must
be rerun for this atomic admission/lazy-enrollment checkpoint.

DME198 remains open. Durable accepted outcomes and atomic scoped native
integration, HTTP/gRPC/delegation/background parity, pending-integration snapshots,
cold claim/reporter authorization, acknowledged receipt retention/hot-quota release,
rolling coordinator protocol enforcement and explicit unknown-legacy quarantine
remain required. Identity-only legacy queue APIs are still present; blocking
mutation of already admitted tasks does not establish a complete protocol floor.
Invariant audit results still follow publication and need durable reconciliation.
Legacy backup/key rotation compatibility, compensation invocation identity and
independent offline-owner/outcome handling remain broader review gates. Passing
these tests does not prove those contracts or justify merging the draft.

### DME198 follow-up: accepted success evidence (checkpoint 0ef2acf9)

The new lost-acknowledgement regression reproduced `NotFound` when a paused
workflow's completed hot task was collected. Both completion transports now look
up immutable acceptance evidence using the authenticated tenant and exact winning
worker ID/claim epoch. A matching replay acknowledges the original acceptance;
its regenerated payload cannot overwrite the retained result. HTTP retains its
existing hot-history integration retry and uses the journal's original payload.
gRPC receipt replays acknowledge acceptance without repeating native writes.
This distinction is deliberate: neither acknowledgement proves native integration.

Migration 134 and SQLite schema 65 add `worker_accepted_outcomes`, with no foreign
keys to hot tasks or native history. The winning completion, original stored
output, admitted invocation (or explicitly retained legacy binding), and Completed
attempt event commit in one transaction. An insertion error rolls all of them
back. SQLite takes its writer reservation before the multi-statement operation;
both backends await rollback on failure. Oversized SQL claim epochs are rejected
instead of saturated. The encryption decorator seals the payload before storage
and decrypts it only after indexed scope and fingerprint validation. Debug output
excludes payloads. Update/delete guards and downgrade retention checks protect the
journal. SQLite reopening preserves receipts after hot collection.

The published 55f5e6c4 CI run finished with 24/25 checks passing. Four API coverage
failure cases created synthetic queue rows without a durable run/step identity;
the fixture now stamps that identity before dispatch capture. Source checks remain
strict. Migration structure checks additionally caught the previously stale
reported schema constant and a missing rollback header; both are corrected here.

Acceptance of success evidence is only one part of DME198. Effect settlement
still precedes task acceptance in a separate transaction. Native context/output,
exact activation completion and an Applied receipt still need one fenced
transaction, followed by bounded recovery independent of worker retries. Paused
results are now durably retained but are not yet automatically integrated on
resume. Failures/retries, delegation, issued-claim principal retention, snapshot
consistency, explicit acknowledged archive and the protocol floor remain open.
The success journal must not be represented as a completed outcome protocol or a
reason to merge/release the draft.

Checkpoint validation passes 2,810 tests: 60 admission/outcome, 111 invocation,
20 resolution, 136 integration, 42 encryption, 15 migration, 2,145 engine unit,
29 distributed, 149 gRPC and 103 API coverage cases. PostgreSQL is required in
the backend matrix. Strict all-target lint passes for storage, gRPC and API;
formatting and whitespace checks pass. The overflow regression proves that a
completion cannot saturate an oversized caller epoch to a real i64::MAX lease.
These results cover accepted-success retention, not the remaining protocol gates.

### DME198 follow-up: atomic success settlement and GC lock order

A new regression reproduced successful hot completion with its effect still
Dispatched on all four storage backends. `complete_worker_task` now routes through
an opaque `WorkerCompletion`: logical output fingerprint and bounded provider
receipt metadata are derived before encryption, while the decorator reseals only
the stored payload. The same transaction settles the original bound receipt,
accepts the original result, completes the task and appends its attempt event.
Journal insertion failure rolls back the receipt and task together. Both HTTP and
gRPC completion and local delegation resumption no longer precommit independently.

Admitted work checks the original request/kind/destination proof and the exact
effect-attempt alias. Legacy immutable bindings retain their weaker classification;
acceptance does not fabricate an admission or certify an unknown native target.
Unknown effects can accept a factual success under their original binding. An
already-Committed receipt without accepted result evidence refuses a regenerated
payload and requires reconciliation; a durable legacy reconciliation workflow
remains open. New success records retain an optional logical fingerprint, so old
134 records remain readable without rewriting a published migration.

The PostgreSQL lock review found an actual inversion: GC locked a native parent
before waiting for its continuity owner, while completion takes owner before
parent. A held-owner transaction deterministically reproduced a native UPDATE
lock timeout. GC now takes physical namespace locks, fences owners/receipts and
then locks eligible native rows. It skips busy namespaces and rechecks terminal
and family eligibility after waits. The namespace key is shared with foreground
admission. Cleanup awaits rollback on error; an independent pre-acquired
connection verifies that a failed native DELETE has restored evidence and released
the owner lock before returning. A separate test verifies busy history does not
block cleanup of a healthy execution.

This closes success effect/acceptance atomicity, not native outcome integration.
The original output, context quota, exact activation, native transition and Applied
receipt still need one fenced transaction and bounded recovery. Failure settlement
still precedes its resolution transaction, and delegation integration remains a
separate operation. Pending accepted outcomes must block or travel with snapshots
and GC; explicit acknowledged archive must compact payloads while retaining
immutable receipt identity. Reporter principal retention, protocol enforcement and
the broader review gates remain required before merging this draft.

An additional encrypted-backend regression reproduced storage interpreting an
application's own valid ciphertext as an already sealed storage value. The opaque
completion now distinguishes caller data from an internally sealed submission;
caller results are sealed without interpreting their contents. Logical output
fingerprints also prevent an accepted result that is already logical from being
unwrapped again. Exact ciphertext values survive both hot-task and journal reads
on PostgreSQL/SQLite with encryption enabled.

The preceding 0ef2acf9 CI Unit job reached the distributed-worker API suite and
found two additional synthetic fixtures without dispatch identity. The local
full API sweep checked all 70 targets and found the same omission in the final
workers suite. Both fixtures now capture a durable run/flat-step baseline and
model a Waiting parent before task insertion; no production fence was weakened.
All 70 API targets pass after rerunning the repaired target (1,357 cases).
Completed test executables are removed between targets to keep disk bounded.

Validation across the checkpoint's verification runs covers 5,003 distinct test
cases: 600 storage integration/schema/encryption/dispatch/outcome/GC cases, two
focused storage unit contracts (decorator interception and record codec), 2,145 engine unit and 29 distributed
cases, 149 gRPC, 701 mobile, 20 embedded facade cases and 1,357 API cases. The
checkpoint-resumption test uses the atomic completion path and proves the old
claim cannot inherit its replacement's acknowledgement. Strict all-target lint
passes for storage, engine, gRPC, API, mobile and the embedded facade; API lint was
rerun after the final fixture repair. Native integration and the other stated
merge gates remain open.

The numeric roundtrip regression then reproduced a committed PostgreSQL result
whose fingerprint could no longer validate: JSONB changed number representations.
The private journal codec now stores the original logical output as versioned JSON
text and decodes it before fingerprint validation. Public outcome serialization
and established fingerprint algorithms stay unchanged. Tests cover integral
floats, negative zero, nested numbers, legacy record decoding, unknown codec
refusal and malformed encoded data.

Migration 135 / SQLite schema 66 advertise the newer format and reject outdated
journal writers. PostgreSQL downgrade refuses encoded records requiring the newer
reader; empty downgrade/upgrade remains supported. Original unencoded 134 records
remain readable when their fingerprints are valid. Previously malformed legacy
numeric records require reconciliation rather than silent recertification.
This format floor is narrower than the full coordinator/worker protocol floor,
which remains open. After these final persistence changes, all 89 admission/outcome
cases, 177 source-cut cases, 15 migration cases, 149 gRPC cases and the 134 API
completion cases pass, along with strict all-target checks for six Rust packages.

## Atomic accepted-result application and independent recovery

The [worker result application review](2026-10-28-worker-result-application.md)
records the next checkpoint: shared HTTP/gRPC native application, exact original
invocation fencing, context CAS/quota, immutable application receipts, paused-state
preservation and durable recovery on continuous and explicit mobile ticks.
Migration 136 / SQLite schema 67 enforce queue insertion, pending-result native
retention and capsule-cut refusal in the database. Busy PostgreSQL authority and
scanner locks are skipped; recovery rotation does not depend on a device clock.
The original failure, delegation, principal, protocol, archive and SDK gates stay
open. This checkpoint does not establish completion of the original review.

## Atomic failure evidence and lossless native JSON checkpoint

Worker failure/lease resolution now atomically retains its exact report, original
effect uncertainty, claim result and native/retry action. Cold failure replay is
tenant/holder/generation scoped after hot and native cleanup. New attempts clear
old checkpoints; never-claimed evidence is required for abandonment. Concurrent
PostgreSQL deletion/reinsertion cannot reuse a retained task ID. See
[worker failure resolution](2026-10-28-worker-failure-resolution.md).

Local delegation and scheduler recovery share one application receipt, retaining
the original delegation attachment with accepted output. Context replacement
preserves runtime authority. PostgreSQL native inputs, contexts, outputs,
checkpoints and captured snapshots preserve NUL and number representations with
explicit codecs and an encoded-record reader floor. See
[native JSON encoding](2026-10-28-native-json-encoding.md).

The affected checkpoint passes 4,329 distinct selected cases and strict checks
on seven packages. Both previously failing CI scenarios are locally verified.
The draft still requires current-head CI and the full remaining review gates:
paused failure application, later factual outcomes and explicit reconciliation,
issued-claim/principal lifetime, complete coordinator protocol enforcement, native
payload compaction after acknowledgement, transported history/key/backup
compatibility and public SDK/platform integration. The existing acknowledged
catalog-run archive and account/catalog isolation are implemented and covered;
the remaining compaction gate must be audited against those current APIs.


## Retained paused failure application

The [paused worker failure application review](2026-10-28-paused-worker-failure-application.md)
records the next correction. Two reproductions showed that explicit resume left
reported retryable and permanent failures stranded. A bounded durable queue now
applies the original failure policy under exact invocation and owner fences,
with the native change and immutable application receipt committed together.
Retry reservations carry no parent application context and remain unclaimable
until fresh normal dispatch. Pause is preserved; old effect uncertainty remains.

PostgreSQL migration 139 / SQLite schema 69 guard pending failures during capsule
capture and refuse evidence-losing downgrade. Recovery survives hot task cleanup,
rotates deferred reports, skips busy authority/clock/task/effect rows and applies
once under concurrency. New PostgreSQL regressions reproduced and fixed both
busy-task and busy-effect scheduler stalls. Selected verification passes 3,591
cases across engine/storage/mobile/embedded and the affected integration suites.

The original review remains incomplete: late factual outcomes and reconciliation,
issued-claim/principal lifetime, full coordinator protocol enforcement, poisoned
recovery disposition/backoff, native acknowledged payload compaction, transported
history/key/backup compatibility, server delegation and SDK/platform integration
remain required. The existing catalog archive API is already implemented. This
checkpoint does not authorize release or establish readiness to merge the draft.

## gRPC authorization prerequisite for retained claim ownership

The [gRPC worker authorization review](2026-10-28-grpc-worker-authorization.md)
records a reproduced capability bypass: stored-key authentication retained only
the tenant, so an Auditor key could reach PollTasks. The auth layer now checks
explicit grants against the actual 37-method protobuf surface and retains the
verified request principal. This is not yet a durable issued-claim journal.

Real-transport tests also reproduced worker streams retaining expired/revoked
keys. Worker/artifact stream guards now reauthorize through the storage-scoped
cache while idle or backpressured, time out unavailable credential storage, and
drop blocked work on credential loss, consumer disconnect or shutdown. A separate
terminal channel and EOF-ordering regression prevent auth failure from being
hidden by queued data or a channel-close race. Validation passes 178 gRPC cases.

Durable original claimant identity, stable refresh scope and claim-bound artifact
access remain required implementation work. The generic artifact RPC has no
claim binding; granting Worker all tenant artifact reads is unsafe. Restricted
generic downloads must be followed by secure worker access to its exact admitted
payloads, not treated as a finished replacement for that capability. All broader
review gates listed above remain active and the draft is not ready to merge.


The schema-139 checkpoint's final CI result was 24/25 success. The sole failure
was an obsolete hand-written PostgreSQL locking fixture that omitted claim epoch
advancement. The fixture now performs a valid generation transition and retains
its original lock-scope and source-query checks. All 44 PostgreSQL integration
cases pass on the isolated repair source; production claim fences are unchanged.
Unfinished schema-140 issued-claim work was saved separately during verification.

## Issued-claim persistence prerequisite

PostgreSQL migration 140 / SQLite schema 70 retain an immutable, versioned
journal of every issued task generation in the lease transaction. All five
normal selectors capture original worker, tenant, physical instance and
invocation/admission metadata. Legacy callers are explicitly recorded as
unattributed, without inventing authentication. A validated internal selector
can retain verified API key, workload, runtime/device, root or unsecured identity.
The journal contains no task input or checkpoint payload. Transaction failure
rolls back every lease, event and journal batch; native/hot cleanup cannot erase
the claim or permit reuse of its task UUID.

The final issued-claim contract suite passes 32 tests, including four backend
configurations, after a red test exposed Serde unit variants silently dropping
unknown identity scope. Empty struct variants reject those extra fields.
Unsupported journal versions cannot satisfy ownership checks. Trusted-root lookup
can recover the indexed tenant after native cleanup without broadening scoped
lookup; retrieval itself confers no mutation or replay authority. Strict Clippy
passes across all eight affected packages. See
`2026-10-28-issued-worker-claim-journal.md` for evidence and limits.

This is persistence groundwork: HTTP/gRPC still use legacy selectors. Verified
transport identity wiring, ownership before mutation/replay/reconnect, legacy
reconciliation, the complete rolling coordinator protocol floor and functional
claim-bound artifact access remain open. Runtime normal polling also needs a
reproduced queue-scope bypass test and a functional fix. No original scope gate
is closed solely by adding this table or by passing these tests.

## Verified transport issuance and scoped queue grants

HTTP polling and gRPC poll/stream demand now journal the verified caller's stable
identity through the transactional selector. Private authentication markers
distinguish verified root from unsecured mode; reusable worker labels and request
JSON establish no ownership. Session identity retains runtime kind, runtime UUID
and device rather than token JTI. This closes issuance wiring, not the mutation,
reconnect, receipt replay, legacy reconciliation or artifact-authorization gates.

An authenticated HTTP regression proved unqualified browser polling claimed a
private named queue without a grant. Runtime-principal default selection now
constrains SQL to the default queue; named polling checks explicit handler and
queue grants. PostgreSQL also rechecks queue and tenant at its locking phase.
Phone session minting lacked named grants, so the functional scoped path adds
optional bounded queue allowlists and explicit named polling, with exact-grant
checks and default-only behavior for old sessions. Existing operator/server
selectors keep their broad queue scope. Renewed phone tokens preserve device
identity; another device sharing a runtime UUID remains a different principal.

NUL-containing names were also accepted by minting despite being rejected by
the retained-claim contract. Mint endpoints now reject these before signing or
device storage access. The final API run passes 412 cases, alongside 179 gRPC,
36 four-backend issued-claim, six service/scheduler integration and two real
executor process scenarios. Service/scheduler tests are in-process; separate
hybrid tests launch rebuilt executor binaries and cover HTTP/gRPC pickup, drain,
process loss and routing metadata on both SQLite and PostgreSQL. See
`2026-10-28-worker-transport-identity.md` for the current evidence and remaining
ownership gates. OpenAPI and the four mobile SDK integration guides reflect the
additive queue grant; token providers still receive opaque tokens.

## Recorded reporter enforcement: HTTP slice

A real two-key HTTP regression found the second Worker key could heartbeat the
first key's recorded lease by repeating its worker label and epoch. The new
classifier distinguishes recorded matches, mismatches and unattributed history.
HTTP complete/fail/heartbeat/release/upload now reject recorded mismatches before
mutation, body upload or accepted/resolution receipt integration. Original
reporting and completion replay remain functional after hot cleanup. Verification
passes 413 API and 40 issued-claim matrix cases, with strict all-target checks
across eight packages. See `2026-10-28-recorded-worker-reporter.md`.

This is deliberately not a closed ownership gate: gRPC mutations/reconnect
adoption still require equivalent enforcement and authenticated internal frame
context. Unattributed history still uses the old transport behavior; explicit
durable backend/operator approval, revocation and reconciliation must replace
that gap before a write floor can safely remove legacy compatibility. Claim-bound
artifact reads and all original scope gates remain required.
