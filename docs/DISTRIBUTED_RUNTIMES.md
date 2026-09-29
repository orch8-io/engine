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

### Rolling upgrades

During a rolling upgrade from a release without migration 096, an older node
can re-dispatch a retry attempt: its insert is `ON CONFLICT DO NOTHING`, so
the pre-inserted row keeps `awaiting_dispatch = true` and no `effect_id`, and
upgraded pollers would never claim it. The worker reaper heals this: a row
still awaiting dispatch **two minutes** after it was written, whose step was
already re-dispatched (a flat instance parked `waiting`, or the step's tree
node `waiting`), is bound to the attempt's receipt (the one the older
dispatch created, otherwise a freshly dispatched one) and owner epoch and
made claimable — one fenced, idempotent update that loses cleanly to a
concurrent re-dispatch by an upgraded scheduler. Rows whose step was not
re-dispatched yet (backoff, paused or terminal instance) are left to the
scheduler.

Older nodes use explicit column lists and ignore the new columns. Their
pollers do not filter on `awaiting_dispatch`, so they may claim a retry row
before it is bound; completing or failing such a task settles the attempt's
still-open receipt (looked up by instance, block and attempt). Once every
node is upgraded no row is ever left awaiting dispatch.

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

## Phone runtimes: device sessions

A phone must not carry an operator (or any stored) API key. Your app backend
(Operator key) mints a short-lived token per device, bound to the phone's
persisted runtime id (`MobileEngine.nodeRuntimeId()`):

```
POST /runtimes/device-sessions
{ "device_id": "iphone-7F3A…", "runtime_id": "…", "handlers": ["scan_document"], "ttl_secs": 3600 }
→ 201 { "token": "dst_…", "device_id": "…", "runtime_id": "…", "expires_at": "…", "handlers": [...] }
```

- `ttl_secs` defaults to 3600, maximum 86400. The mobile SDK asks its host
  `TokenProvider` for a fresh token on `401` and retries once
  (`MobileEngine.setTokenProvider`). Minting for a `device_id` registered to
  another tenant is `409`.
- Same signer as browser sessions (`ORCH8_BROWSER_SESSION_SECRET`, else the
  root-key derivation). The signed claims carry the runtime kind; a `bst_`
  token never verifies as `dst_` or the reverse.
- Allowed, deny by default (`403` otherwise; expired or forged tokens `401`):

  | Route | Object-level check |
  |-------|--------------------|
  | `POST /mobile/devices/register`, `POST /mobile/sync` | the session's `device_id`; `step_delegations` (server-side credential resolution) refused |
  | `POST /mobile/devices/{device_id}/runtime` | the session's device and `runtime_id`, kind `mobile`; handlers clamped to the allowlist |
  | `POST /workers/tasks/poll`, `/workers/tasks/{id}/{complete,fail,heartbeat,release}` | `worker_id = runtime_id`, kind `mobile`, a granted handler |
  | `POST /continuity/executions` | `hosted_by_runtime: true`, `runtime_id` = the session's |
  | `POST /continuity/grants` | `allowed_actions: ["accept"]`, execution owned by the session's runtime (else `404`) |
  | `POST /continuity/delegations/claim` | `source_runtime_id` = the session's runtime (which must own the parent) |
  | `GET /continuity/delegations/{id}` | the session's runtime is the source or destination (else `404`) |
  | `GET /runtimes` | read-only list of live registrations |

- Refused: task listing and stats, `/workers/tasks/poll/queue`, worker
  commands, other devices' data (`GET /mobile/devices|approvals|status`,
  `POST /mobile/commands`), browser/device session minting,
  `/runtimes/register`, sequences, credentials, instances, API keys,
  handoffs, capsule import, grant consumption, and reading executions.
- An isolated delegated step (a one-step sequence) is published by the
  control plane itself when the claim carries `step: {handler, block_id}` and
  `sub_sequence_id` is that step's deterministic id — a device session never
  needs sequence-authoring rights.
- Stored keys hitting `/mobile/*` with Operator capability (or the root key)
  get `x-orch8-principal-scope: operator|root` on the response; the mobile
  SDK logs a warning when it sees it.

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
grant) and enqueues a mailbox task targeted at the destination
(`handler_name: "orch8.delegation"`, params carry the delegation identity,
sub-sequence and explicit `input`; the response includes `mailbox_task_id`).
The destination polls that handler, runs the sub-sequence locally, and
completes or fails the task. Delegation failures, lease loss, and expiry never
fail the parent: they integrate a `failed` outcome.

**Server-hosted parent.** The result is integrated into the parent as the
`delegation-<id>` block output and `context.data.delegations.<id>`
(`status: completed | failed`), and a waiting parent is woken.

**Runtime-hosted parent** (a workflow on a phone's local engine). The
runtime first registers its instance's continuity identity:

```
POST /continuity/executions
{ "tenant_id": "…", "instance_id": "<local instance>", "runtime_id": "<phone>",
  "hosted_by_runtime": true }
```

The instance must not exist on the server and the runtime must hold a live
registration; repeating the call for the same owner returns the same
execution (`200`). A claim against such an execution anchors the mailbox task
on a **delegation proxy** — a server instance whose id is the delegation id,
parked in `waiting`, that never runs; it receives the integrated outcome and
then turns `completed` / `failed`. The hosting runtime reads the outcome:

```
GET /continuity/delegations/{id}?tenant_id=…
→ { "delegation_id", "status": "pending|claimed|completed|failed",
    "delegation": {…}, "parent_instance_id", "parent_owner_runtime_id",
    "parent_epoch_now", "mailbox_task_id", "result": {status, runtime_id, output | error} }
```

A device session may make these calls only for its own runtime: register
executions it hosts, grant and claim delegations of executions it owns, and
read delegations it is the source or destination of (see
[device sessions](#phone-runtimes-device-sessions)). The claim may carry
`"step": {"handler": "…", "block_id": "…"}` for an isolated delegated step;
the control plane then publishes its one-step sequence (idempotently, under
the id the delegation names) before validating the delegation.

The runtime resumes its parked step itself — only while it still owns the parent at
the delegation's epoch (`parent_owner_runtime_id` / `parent_epoch_now`). The
mobile SDK does all of this automatically for steps placed off the phone (see
[MOBILE_SDK.md](MOBILE_SDK.md#delegating-from-a-phone-local-workflow)).
Results are read by polling rather than pushed through the sync `commands`
channel: the outcome is a durable record keyed by the delegation id, so
reading it again after any disconnect or kill is idempotent and nothing needs
acknowledging.
