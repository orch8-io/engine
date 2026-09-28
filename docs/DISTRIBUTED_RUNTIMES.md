# Distributed runtimes (server, desktop, mobile, browser)

Any process that claims steps is a **runtime node**: a server-side worker, a
desktop app, a phone (`kind: mobile`), or a browser tab (`kind: browser`).
All of them use one lease protocol over the worker API:

```
poll (claim) → heartbeat* → complete | fail | release
```

The runtime-capabilities registry (`POST /runtimes/register`, or
`capabilities` on each poll) is the node registry. Capability advertisements
live at most five minutes; nodes re-advertise before they expire.

## Placing a step: `params.$runtime`

`$runtime` uses the `CapsuleRequirements` shape. It is stripped from the params
the handler sees and stored on the task.

| Field | Meaning |
|-------|---------|
| `runtime_kinds` | Only nodes of these kinds may claim (`server`, `edge`, `mobile`, `desktop`, `browser`). |
| `runtime_id` | Only this node may claim. The task is that node's **mailbox**: it stays `pending` until the node polls (no lease reaping while pending; the step `timeout` still applies). |
| `policy`, `classification` | A `LocalityPolicy` and data classification (default `internal`) evaluated at dispatch and re-evaluated against every claimant. |
| `handlers`, `plugins`, `credentials`, `regions`, `hardware`, `requires_network`, `requires_human_ui`, `minimum_trust` | Capability facts the claimant must advertise (unchanged). |

```json
{ "id": "scan", "handler": "scan_receipt",
  "params": { "image": "{{context.data.image}}",
              "$runtime": { "runtime_id": "0190f5a0-…" } } }
```

Rules:

- A step with `runtime_id`, or with `runtime_kinds` that exclude `server`, is
  **always** dispatched to the worker queue — even if the server has the
  handler registered in-process.
- Placement facts and policy are validated at dispatch; invalid placement
  fails the step permanently.
- For placed steps the locality policy is evaluated at dispatch and the
  outcome is saved as a **placement decision** (evidence). Definitive denials
  fail the step permanently with `placement denied by locality policy (…);
  decision <id>`: the policy contradicts the placement (targeted runtime id or
  placed kinds outside the rule), the classification is `confidential` or
  `restricted` with no rule, or the registered target is denied. Unknown facts
  do not block dispatch — every claim re-checks the policy and fails closed.
- Claims only match nodes whose advertisement satisfies the requirements
  (poll must send `capabilities`; legacy capability-less polls only claim
  unplaced tasks).

## Leases, effects, and failures

Every non-builtin handler is side-effecting. Dispatch creates an effect
receipt (`dispatched`) and stores its id on the task as `effect_id`; nodes
should pass it downstream as an idempotency key.

| Event | Pure task | Side-effecting task |
|-------|-----------|---------------------|
| Lease expired (no heartbeat within `lease_secs`) | back to `pending` | receipt → `unknown`; retryable failure (retry policy: new attempt + new `effect_id`, else fail node/instance) |
| Lease expired after the activity checkpointed (`checkpoint_seq > 0`) | back to `pending` | receipt → `unknown`; requeued so the next claimant resumes from `resume_checkpoint` (completion commits the receipt) |
| `release` with `started: false` | back to `pending` | back to `pending`, receipt untouched |
| `release` with `started: true` | back to `pending` | same as lease expiry |
| `timeout_ms` elapsed | instance advances (retry/fail) | receipt → `unknown` (claimed) or `abandoned` (never claimed); instance advances |
| `complete` | — | receipt → `committed` (by stored id) |
| `fail` | retry policy | receipt → `unknown`, retry policy |

`fail` (HTTP and gRPC) is resolved in the same single fenced transaction as
lease expiry, timeouts and release: the receipt becomes `unknown`, then the
task is either replaced by the next attempt (retry policy), or marked failed
together with its tree node (or flat instance). A repeated `fail` from the
same lease is `200`; once the task was superseded by a retry it is `404`. A
delegation task, or a task whose instance is terminal or paused, only has the
task marked failed.

A retry row inserted by any of these resolutions is **not claimable until
the scheduler re-dispatched it**: the re-dispatch creates the attempt's
receipt and binds its `effect_id` in the same statement that makes the row
claimable, so no worker ever holds an attempt without its effect id and
settlement never recomputes one.

Per-claim `lease_secs`: browser 30 s, mobile 120 s, others the server default
(`engine.worker_reaper_stale_secs`). Heartbeat every `lease_secs / 3`.

All transitions are fenced compare-and-swaps: a racing completion wins, and a
late call from a stale claim gets `409`.

gRPC workers get the same behaviour: `CompleteTask` commits the receipt by its
stored id (and integrates delegation results), `FailTask` uses the fenced
resolution above, and `ReleaseTask {task_id, worker_id, claim_epoch, started}`
is the twin of `POST /workers/tasks/{id}/release`. A stale claim is
`FAILED_PRECONDITION`; an effect-receipt conflict is `ABORTED`.

A placement rejected at dispatch (invalid `$runtime`, locality denial,
credentials placed on browsers) fails the step with an `__error__` block
output carrying the reason and a `remote_dispatch_rejected` audit event, on
both the tree and the flat (step-only) path.

## Handoff fencing

- complete / fail / heartbeat / release / artifact upload are fenced on the
  claim epoch **and** the continuity owner epoch recorded at dispatch
  (`continuity_epoch`). A task issued under an older owner, or while the
  execution is being exported, gets `409`.
- Capsule export (and the handoff export API) is refused while the source
  instance has pending or claimed worker tasks.
- The scheduler never advances an instance whose execution is `transferring`
  (it re-checks every 30 s) or has been handed to another instance/runtime
  (the source parks in `waiting`).

## Push wake-ups

Push-mode queues POST an id-only hint — `{task_id, runtime_id, reason:
"task_available"}` — never params or context. The receiver polls to claim the
task under a lease. Mobile silent pushes (APNs/FCM) carry no payload either.

## Browser runtimes

### Sessions

Your app backend (Operator key) mints a short-lived token per tab:

```
POST /runtimes/browser-sessions
{ "handlers": ["read_dom"], "ttl_secs": 900, "runtime_id": "…optional…", "queues": [] }
→ 201 { "token": "bst_…", "runtime_id": "…", "expires_at": "…", "handlers": ["read_dom"] }
```

- `ttl_secs` defaults to 900, maximum 3600; there is no refresh — mint a new
  token.
- The token is sent as `x-api-key` (or `Authorization: Bearer`). It may only
  call `POST /workers/tasks/poll`, `/workers/tasks/poll/queue`, and
  `/workers/tasks/{id}/{complete,fail,heartbeat,release}`; everything else is
  `403`. Expired or forged tokens are `401`.
- Polls must use `worker_id = runtime_id`, `kind: browser`, and a granted
  handler (and queue); conflicts are `403`. Advertised trust is capped at
  `registered`; the advertisement is synthesized when omitted.
- Tokens are signed with `ORCH8_BROWSER_SESSION_SECRET` (≥ 32 bytes, the
  same on every replica) when set, otherwise with a key derived from the root
  API key — either way every replica verifies every token. With neither
  (`--insecure`), a process-random key is used and a token only verifies on
  the replica that minted it; the server warns at startup. Set
  `ORCH8_CORS_ORIGINS` to the origins that host browser runtimes.

### Browsers never receive secrets

- A task whose params referenced `credentials://` material is never claimable
  by a browser. A step placed only on browser kinds (or targeted at a
  registered browser) that references credentials fails at dispatch:
  `steps placed on browser runtimes cannot receive credentials`.
- Context delivered to a browser drops `config` and the audit trail, removes
  data entries holding credential references, and applies the platform
  redaction policy. Other kinds receive the full context as before.

### Page data in

Browser steps may return page data (DOM, forms, user input). It is untrusted:
outputs larger than `ORCH8_BROWSER_OUTPUT_MAX_BYTES` (default 1 MiB) are
refused with `413`, and every remote output records its provenance (runtime
kind + id, output digest) in the audit log (`worker_output_provenance`) and,
for continuity-enrolled instances, the provenance chain.

## Device-mesh delegation (server mailbox)

`POST /continuity/delegations/claim` validates a destination-bound delegation
(registered same-tenant runtimes, owner epoch, destination handlers, one-time
grant) and — when the parent instance is hosted by this server — enqueues a
mailbox task targeted at the destination (`handler_name: "orch8.delegation"`,
params carry the delegation identity, sub-sequence and explicit `input`; the
response includes `mailbox_task_id`). The destination polls that handler,
runs the sub-sequence locally, and completes or fails the task. The result is
integrated into the parent as the `delegation-<id>` block output and
`context.data.delegations.<id>` (`status: completed | failed`), and a waiting
parent is woken. Delegation failures, lease loss, and expiry never fail the
parent.
