# Hybrid deployments

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Hybrid means **Orch8 Cloud runs the engine** (API, scheduler, database) and
**executors in your network run the steps** that need your secrets, internal
APIs, or network. An executor holds no database: it dials **out** to the
engine, claims leased step tasks over the worker protocol, runs them next to
your systems, and reports the outcome. Credentials, network access, and side
effects stay in your network. Steps you do not place keep running in Cloud.

```
 Orch8 Cloud (engine + DB)                         your VPC
 ┌──────────────────────────┐   outbound only   ┌──────────────────────────┐
 │ scheduler, API, reaper   │◄──────────────────│ orch8-server (executor)  │
 │ worker_tasks, outputs    │  gRPC stream /    │  • local credentials     │
 │ credential *references*  │  HTTPS polling    │  • calls internal APIs   │
 └──────────────────────────┘                   │  • optional BYOK vault   │
                                                └──────────────────────────┘
```

## What Cloud sees and what never leaves your network

Derived from the code paths (`orch8_engine::remote_executor`,
`step_placement::defers_credentials`, the worker protocol):

| Data | Seen/stored by Cloud? | Notes |
|---|---|---|
| Sequence definitions, instance context (`context.data`, `config`), instance metadata | **Yes** | You create them through the Cloud API. |
| Step params **after template rendering** | **Yes** | Rendered by the Cloud engine and stored in `worker_tasks.params`; anything interpolated from context or earlier outputs is in them. Params are never externalized. |
| Step context shipped with a task | **Yes** | The step's context snapshot (`context_access` applies), including `runtime.traceparent`. |
| `credentials://<id>` references in a **placed** step | **Reference only** | The engine leaves them unresolved; the executor resolves them locally. Cloud stores the string `credentials://<id>/…` and the id as a task requirement. |
| Credential **values** for placed steps | **Never** | Read on the executor from `ORCH8_CREDENTIAL_<id>` or `<ORCH8_CREDENTIALS_DIR>/<id>`. Not sent, not stored (the e2e suite scans every engine table for the value). |
| Credentials referenced by **unplaced** steps | Yes (engine credential store) | Those steps run in Cloud and resolve from Cloud's store as before. |
| Network access / side effects of placed steps | **Never** | Requests originate from the executor. Cloud has no inbound path to your network. |
| Step outputs | **Yes**, unless sealed | Everything the handler returns is reported, including anything it echoes (a `transform` that echoes a resolved credential sends it). |
| Output fields sealed with BYOK on the executor | **Reference only** | `{"_o8vault": {object, kid, dek (wrapped by your key), ref}}`; plaintext is in your bucket. |
| Error messages of failed steps | **Yes** | Handler error text (URLs are redacted by the built-ins). |
| Executor capability advertisement | **Yes** | Runtime id, handler names, labels (e.g. `residency`, `site`), region, `host:<worker name>`, credential **ids**, draining flag. |
| Lease traffic | **Yes** | Claims, heartbeats (task id, claim epoch), completions, failures, releases. |
| Managed-control session | **Yes** | Token runtime id, worker name, region, liveness, drain acks. |
| BYOK keys, bucket credentials, KMS access | **Never** | Only the executor holds them. Cloud needs no KMS access; it stores and relays references. |
| Handler-local scratch state (LLM cache, usage events) | **Never** | Kept in an in-memory scratch store on the executor and discarded at exit. |

Not exactly-once: an executor that dies after its request reached a provider
but before it reported produces a redelivery on retry. The effect ledger marks
that attempt `unknown`; use the provider's idempotency key.

## Join an executor

Orch8 Cloud issues a **join token**, `o8x1.<base64url(json)>`:

```json
{ "v": 1, "endpoint": "https://…", "api_key": "…", "tenant_id": "acme",
  "runtime_id": "018f…", "worker_id_prefix": "acme-dc1",
  "labels": { "residency": "eu", "site": "vpc" }, "region": "eu-west-1" }
```

The token is a **secret** (it carries a dedicated API key) and is unsigned: the
engine authenticates the key; the token only carries it. The key needs the
`worker` capability (worker endpoints plus `POST /runtimes/register`).

Two optional **routing** fields (no version bump; older executors ignore them,
and tokens without them encode exactly as before):

- `api_url`: the REST base for the HTTP polling fallback when it is not
  `<endpoint>/api/v1`, e.g. when gRPC and REST are on different ports.
- `headers`: routing headers sent on **every** gRPC call (metadata) and REST
  request, e.g. a shared load balancer's instance pin. Up to 16 lowercase
  names; credential, tenant, `grpc-*` and transport headers are refused.

Orch8 Cloud's managed engines all sit behind one shared host and are reached
only through the proxy's instance pin, so Cloud issues:

```json
{ …, "endpoint": "https://<cloud engine host>:50051",
  "api_url": "https://<cloud engine host>/api/v1",
  "headers": { "fly-force-instance-id": "<machine id>" } }
```

The routing header is not a secret and not a credential: a request pinned to
another organisation's engine is authenticated by *that* engine, which has
never seen this key, and is refused.

The token is **all an executor needs**: no database, no API key of its own, no
encryption key.

```bash
# Containers: just the token.
ORCH8_JOIN_TOKEN='o8x1.…' orch8-server
# Or write orch8.toml ([node] role = "executor" + managed_control_*), mode 0600, and start it:
orch8 executor join - --config-out orch8.toml --label zone=b --run < token.txt
```

A joined `executor` with no `database.url` runs as a **remote executor**. (An
executor that also has a database keeps the older shared-database mode.)

What happens at startup:

1. It validates the config and opens a **managed-control** gRPC session to
   `endpoint` (ping, reload, drain).
2. It connects to the worker protocol at the same `endpoint`: the negotiated
   **gRPC worker stream** by default, falling back to **HTTP polling** at the
   token's `api_url` (default `<endpoint>/api/v1`) when the endpoint does not
   serve gRPC or cannot be reached (for example an egress firewall that only
   allows 443) (`ORCH8_EXECUTOR_TRANSPORT=auto|grpc|http`,
   `ORCH8_EXECUTOR_API_URL` overrides the token). Both carry the token's
   routing `headers`.
3. It advertises a runtime: kind `server`, the handlers it serves, the token
   labels merged with `[node] labels` / `--label`, the token region, the
   worker name as `host:<prefix>-<hostname>`, and the ids of its local
   credentials. Every replica gets its own runtime id, derived from the token
   runtime id and its worker name, so draining one pod never withdraws its
   siblings. The lease `worker_id` is that runtime id.
4. It claims tasks through the engine's capability predicate, runs the
   built-in handler, heartbeats, and settles with the task's `claim_epoch`.

It serves `/health/live` and `/health/ready` on `api.http_addr` and nothing
else. Built-ins it can run (`executor.handlers`, default all):
`http_request`, `llm_call`, `tool_call`, `email`, `notify`, `transform`,
`assert`, `log`, `sleep`, `noop`, `fail`. Built-ins that manipulate engine
state (`set_state`, `send_signal`, `human_review`, `wait_for_event`,
`memory_*`, `blob_*`, …) always run in Cloud and ignore placement.
Known limits on the executor: `llm_call` prompt-registry references, tenant
LLM budgets, and artifact-backed images need the engine database and are not
available; LLM usage is not reported back.

### Executor settings

| Setting | Env | Default |
|---|---|---|
| `[executor] transport` | `ORCH8_EXECUTOR_TRANSPORT` | `auto` |
| `[executor] api_url` | `ORCH8_EXECUTOR_API_URL` | token `api_url`, else `<endpoint>/api/v1` |
| `[node] managed_control_headers` (routing headers, all requests) | via `ORCH8_JOIN_TOKEN` | token `headers` |
| `[executor] ca_cert_path` (PEM trusted instead of public roots) | `ORCH8_EXECUTOR_CA_CERT` | public web PKI |
| `[executor] handlers` | `ORCH8_EXECUTOR_HANDLERS` (comma-separated) | all remote-executable built-ins |
| `[executor] max_concurrent_tasks` | `ORCH8_EXECUTOR_MAX_CONCURRENT_TASKS` | 16 |
| `[executor] credentials_dir` | `ORCH8_CREDENTIALS_DIR` | none (env only) |
| `[executor] drain_timeout_secs` | `ORCH8_EXECUTOR_DRAIN_TIMEOUT_SECS` | 25 |
| `[executor] externalize_bytes` (BYOK threshold) | `ORCH8_EXECUTOR_EXTERNALIZE_BYTES` | 65536 |
| `[executor] heartbeat_secs` (cap; 0 = engine hint) | `ORCH8_EXECUTOR_HEARTBEAT_SECS` | 0 |
| Internal networks steps may call | `ORCH8_ALLOWED_INTERNAL_CIDRS` | none |

The SSRF guard still applies on the executor: `http_request`/`tool_call`
reach only public addresses unless the network is listed in
`ORCH8_ALLOWED_INTERNAL_CIDRS` (e.g. `10.20.0.0/16`). Cloud metadata
endpoints (`169.254.169.254`) stay blocked unless you list them.
`ORCH8_ALLOW_INTERNAL_URLS=true` opens everything and is not recommended.

## Which steps run in your network

Placement decides (see [PLACEMENT.md](PLACEMENT.md)). A step with a hard
placement constraint (`region`, `labels`, `residency` — on the step, its
sequence, or from a tenant policy) is dispatched to executors whose
advertisement satisfies it; everything else runs in Cloud.

Recommended policy — all steps of instances tagged `vpc` require an executor
labelled `site=vpc` (issue the join token with that label):

```http
PUT /api/v1/placement/policies
{ "items": [ { "name": "vpc",
               "match": { "tag": "vpc" },
               "require": { "labels": { "site": "vpc" } } } ] }
```

Create instances with `"metadata": {"tags": ["vpc"]}`. Or place individual
steps: `"placement": {"residency": "eu"}` matches executors labelled
`residency=eu`. With no live matching executor the step waits
(`placement_unsatisfied`); it never falls back to Cloud.

## Credentials resolve on the executor

For a hard-placed step the engine **does not resolve** `credentials://`
references: the task carries `credentials://<id>[/<field>]` and requires the
runtime to advertise `<id>`, so only an executor holding the credential can
claim it. The executor resolves the reference from, in order:

1. `ORCH8_CREDENTIAL_<id>` — the rest of the variable name is the id,
   case-sensitive (`ORCH8_CREDENTIAL_stripe_prod` → `credentials://stripe_prod`);
2. `<ORCH8_CREDENTIALS_DIR>/<id>` — e.g. a mounted Kubernetes Secret whose
   keys are credential ids.

Values use the engine's format: JSON is parsed (so `credentials://api/token`
selects a field), anything else is a plain string. A reference the executor
cannot resolve fails the step permanently; it never asks Cloud. Unplaced
steps keep resolving from Cloud's credential store.

## BYOK: keep large outputs in your bucket

Configure the BYOK vault on the executor with the `ORCH8_BYOK_*` variables of
[FEDERATION.md](FEDERATION.md#2-byok-externalization) (bucket + AWS KMS key or
static key). The executor then seals every top-level output field larger than
`ORCH8_EXECUTOR_EXTERNALIZE_BYTES` (`0` = every field) into your bucket and
reports only the reference; small fields (status codes, ids) stay readable.
A later **placed** step that receives such a reference (through a template)
gets the plaintext on the executor. The encryption, the KMS calls, and the
bucket writes happen on the executor with your credentials; Cloud needs no KMS
access and cannot read sealed fields. Honest limits: step params and context
rendered by Cloud are never sealed; a Cloud-side (unplaced) step that
templates a sealed field receives the reference, not the data; a reference is
bound to its instance and only opens there.

## Drain, restarts, and failures

- **SIGTERM** or a managed **`drain`** command
  (`POST /api/v1/workers/commands {"worker_id": "<prefix>-<hostname>", "tenant_id": "…", "command": "drain"}`):
  the executor advertises `draining` (no new placement), stops claiming, lets
  in-flight steps finish for `drain_timeout_secs`, then releases the rest
  (`release {started: true}`: the attempt's effect becomes `unknown`, the
  step's retry policy schedules a new attempt elsewhere), and exits 0. A drain
  command reaches the executor on its next managed-control heartbeat (within
  15 s).
- **Kill / network loss**: heartbeats stop; the engine's lease reaper
  reclaims the task after `worker_reaper_stale_secs` (default 60 s), marks the
  attempt `unknown`, and retries it on another executor.
- A completion from a process whose lease moved on is rejected by its stale
  `claim_epoch`.

## Helm

```bash
kubectl create secret generic orch8-join --from-literal=join-token='o8x1.…'
kubectl create secret generic orch8-executor-credentials --from-file=vpc-api=./vpc-api.json
helm install exec deploy/helm/orch8 \
  --set mode=executor \
  --set hybrid.joinToken.existingSecret=orch8-join \
  --set hybrid.credentials.existingSecret=orch8-executor-credentials \
  --set hybrid.allowedInternalCidrs=10.0.0.0/8
```

`mode=executor` renders only the executor Deployment: `ORCH8_JOIN_TOKEN` from
the Secret, `HOSTNAME` = pod name (one worker name per replica), health probes
on HTTP, no Service, no database, no chart Secret. The chart fails if a
database or an ingress is configured in this mode. Optional:
`hybrid.caCert.existingSecret` (private CA), BYOK through `executor.extraEnv`.
See `deploy/helm/orch8/ci/hybrid-executor-values.yaml`.

## Docker Compose

[`deploy/hybrid-executor/docker-compose.yml`](../deploy/hybrid-executor/docker-compose.yml)
runs one executor from the token alone, with credentials from
`./credentials/<id>`:

```bash
export ORCH8_JOIN_TOKEN='o8x1.…'
docker compose -f deploy/hybrid-executor/docker-compose.yml up -d
```

## Drill: kill an executor

```bash
orch8 drill kill-executor            # table report, exit 1 on failure
orch8 -o json drill kill-executor --instances 50 --keep
```

The drill runs on loopback in a temp directory:

- a **control node** (this process): SQLite, the HTTP API, the scheduler
  and lease reaper, and a receipt signing key;
- **two executor processes** (separate OS processes) serving the workload's
  two side-effecting steps over the external worker protocol (poll,
  heartbeat, complete);
- a **provider** standing in for the external system, which logs every
  delivery and dedupes on the idempotency key carried in each receipt.

It sends `SIGKILL` to one executor while that executor holds claimed tasks,
lets the real lease reaper (3 s lease, 1 s tick in the drill) and retry
policy recover, then reconciles every `unknown` receipt against the provider
log (`verified` if delivered, `abandoned` if not) and exports a signed
receipt bundle. It reports measured values only and exits non-zero unless all
of these hold:

| Invariant | Meaning |
|---|---|
| `executor_killed_mid_flight` | the victim held at least one claimed task at `SIGKILL` |
| `all_instances_completed` | every instance reached `completed` |
| `recovered` | every orphaned step completed on the surviving executor |
| `no_effect_recorded_twice` | no attempt has two receipts; no step has two `committed` receipts |
| `no_silent_redelivery` | every provider redelivery follows an attempt the ledger marked `unknown` |
| `each_effect_applied_once_at_provider` | distinct idempotency keys applied = steps executed |
| `signed_receipts_verify` | the bundle verifies and has 0 unresolved receipts after reconciliation |

A real run on a laptop (24 instances, defaults):

```text
Drill: kill-executor  PASSED
  SIGKILL:         drill-executor-1 with 4 claimed task(s), 1 delivered but unacknowledged
  detection:       3349 ms (lease 3 s, reaper tick 1 s)
  time to recovery: 3872 ms
  completed:       24/24  (total 5216 ms)
  ledger:          52 receipts {"committed": 48, "unknown": 4}; 4 ambiguous -> 1 verified, 3 abandoned
  provider:        49 deliveries, 48 distinct effects, 1 redeliveries deduplicated
```

Detection and time to recovery are dominated by the lease and reaper tick.
Production defaults (`worker_reaper_tick_secs = 30`,
`worker_reaper_stale_secs = 60`) recover more slowly; tune them for your
tolerance.

The drill is honest about the guarantee: an executor killed *after* the
provider accepted a request but *before* it reported completion produces a
redelivery on retry. The ledger marks that attempt `unknown` instead of
hiding it, and the provider's idempotency key makes the redelivery harmless.
That is at-most-once dispatch evidence per attempt, not exactly-once
delivery.

## Signed effect receipts {#receipts}

Every side-effecting attempt gets a durable receipt (`planned → prepared →
dispatched → committed`, or `unknown` when the outcome is ambiguous). Export
them as a signed JSON Lines bundle:

```bash
orch8 receipts export --instance 018f… --out receipts.jsonl
orch8 receipts export --from 2026-09-01T00:00:00Z --to 2026-10-01T00:00:00Z --out sept.jsonl
orch8 receipts signing-key                     # pin this public key
orch8 receipts verify receipts.jsonl --public-key <base64>
```

HTTP: `GET /api/v1/instances/{id}/receipts/export`,
`GET /api/v1/receipts/export?from=&to=`, `GET /api/v1/receipts/signing-key`.

Bundle layout: a header (claim `at-most-once dispatch evidence`, scope,
tenant, signing key id and Ed25519 public key), one line per receipt, and a
trailer with the record count, the SHA-256 of all preceding bytes, and an
Ed25519 signature over `"orch8-effect-receipts-v1\n" + sha256`. The key is
the engine's continuity signing key, derived from `ORCH8_ENCRYPTION_KEY`
(export returns 503 without it).

`verify` checks the digest, signature, record count and tenant, and
summarizes states, unresolved (`dispatched`/`unknown`) receipts and duplicate
attempts. Without `--public-key` it proves integrity only; pin the key to
prove the bundle came from your engine. Window exports scan at most 10,000
instances touched in the window and set `scope.truncated=true` beyond that.

## Run-metadata export {#observability-export}

This applies to an engine you run yourself (self-hosted or shared-database
executors) that reports to the Cloud fleet view. A remote executor runs no
engine and exports nothing beyond the worker protocol above.

```toml
[cloud_observability]
endpoint = "https://cloud.orch8.io"
api_key = "…"            # ORCH8_CLOUD_OBSERVABILITY_API_KEY
engine_id = "acme-dc1"   # ORCH8_CLOUD_OBSERVABILITY_ENGINE_ID
interval_ms = 5000       # ORCH8_CLOUD_OBSERVABILITY_INTERVAL_MS
# max_buffered_events = 10000
```

When `endpoint` is set, instance state transitions recorded by the
lifecycle funnel are buffered in memory and POSTed every `interval_ms` to
`{endpoint}/api/ingest/v1/runs` with `Authorization: Bearer <api_key>`:

```json
{ "engine_id": "acme-dc1", "engine_version": "…", "sent_at": "…",
  "events": [{ "instance_id": "…", "sequence_name": "billing",
    "sequence_version": 3, "state": "completed", "at": "…", "step_id": null,
    "duration_ms": 1840, "error_kind": null, "sub_tenant": null }] }
```

- At most 500 events per request.
- The buffer is bounded (`max_buffered_events`); when full the **oldest**
  events are dropped (`orch8_cloud_observability_dropped_total`). Recording
  never blocks the engine.
- Failed batches are retried with exponential backoff (1 s doubling to 60 s)
  and are not reordered.
- `endpoint` must be HTTPS (plain HTTP only to loopback); `api_key` and
  `engine_id` are required. Startup fails otherwise.
- `error_kind` is only a coarse class (`failed`); error messages are never
  exported. `sub_tenant` is reserved and currently always `null`.
