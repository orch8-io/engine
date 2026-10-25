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
| Node/Python bindings | Initialization failures, repeated close, host callbacks, concurrent calls, crash/restart, handler retention and errors | Reviewing; shared adapter post-storage claim entry and replacement/close finalization covered below; queued physical host admission, resource bounds and platform parity remain open |
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
