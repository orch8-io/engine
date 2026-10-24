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
| Mobile control and sync | Device registration, account/control-plane changes, commands, approval deduplication, revocation, protected fields, hostile manifests | Reviewing; provider-installation/refresh/retry fencing verified below; durable principal binding and command/manifest review open |
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

### DME063 — Initial publication after refresh failure (fixed in next batch)

**HIGH, effort S.** Starting refresh permanently disabled an older in-progress
`current_token` callback, even when refresh failed and the initial callback later
returned a usable token. Readiness tests exposed the stranded installation. Only
successful refresh publication now supersedes initial publication. The per-owner
refresh mutex permits a successful refresh to take precedence if the initial
callback publishes while it is running; failed refresh does not transfer negative
cache state onto that newly published revision. Controlled callback gates cover
both completion orders and successful/failed refresh outcomes.

### DME064 — Wrapper installation before the native fence (open)

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
timeouts, and engine aliases in every supported wrapper.

DME062/063 are prerequisites, not a resolution of DME064. Principal binding,
independent engines/processes, core durable outcomes, recovery quotas, and the
remaining original review matrix still require work.

Readiness-batch validation: 2,034 lean engine tests, 347 mobile tests, 147 gRPC
tests, all-target Clippy for engine/server/mobile with warnings denied, and the
documentation checker pass locally. Eight simultaneous direct/shared initial
requests share one host refresh. Hosted full-workspace/native CI is the merge
gate for this batch.

### DME065 — Physical refresh budget across replaced providers (open)

**HIGH, effort M, pre-existing.** Physical refresh admission is per retained
provider instance. A different provider gets an independent AtomicBool, and
`physical_owners` keeps weak records live while timed-out blocking closures hold
the provider and running guard. Repeated replacement with non-cooperative host
callbacks can therefore accumulate physical calls/queued closures and identity
records without a credential-wide cap. Timeout bounds the waiter, not the host
callback's lifetime.

Add a bounded physical-call quota shared across owners and hold its permit until
the actual blocking closure returns or is dropped before starting. Reject
exhausted admission without starting/queuing another host callback; preserve
successor progress while capacity exists and recover after old calls return.
Test multiple timed-out owners, quota exhaustion, queued cancellation, and
post-release recovery. Independent engine/process budgets remain a separate
original-scope requirement. This finding is not fixed by readiness fencing.
