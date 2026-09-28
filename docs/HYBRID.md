# Hybrid deployments

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Hybrid means the Orch8 control plane runs in Orch8 Cloud and the executors
that touch your data run in your infrastructure. This page covers the engine
side: joining an executor, deploying it with Helm or Compose, the
`kill-executor` drill, signed effect receipts, and run-metadata export.

What leaves your network:

| Channel | Carries | Never carries |
|---|---|---|
| Managed-control session (outbound gRPC, [NODE_ROLES.md](NODE_ROLES.md#managed-cloud-outbound-control)) | runtime id, worker id, coarse region, lease heartbeats, drain acks | contexts, params, outputs, artifacts, logs, credentials |
| Run-metadata export (`[cloud_observability]`, optional) | instance id, sequence name/version, state, timestamps, step id, duration, coarse error kind | context, inputs, outputs, params, error messages |

The managed-control channel is control-only today: `place` commands are
refused, so the cloud cannot push workload payloads to an executor. Work
reaches an executor through its own database (shared with a control node you
run) and its local workers.

## Join an executor

Orch8 Cloud issues a **join token**, `o8x1.<base64url(json)>`:

```json
{ "v": 1, "endpoint": "https://…", "api_key": "…", "tenant_id": "acme",
  "runtime_id": "018f…", "worker_id_prefix": "acme-dc1",
  "labels": { "gpu": "a10" }, "region": "eu-west-1" }
```

The token is a **secret** (it contains a dedicated API key) and is unsigned:
the control plane authenticates the key; the token only carries it.

```bash
# Write orch8.toml ([node] role = "executor" + managed_control_*), mode 0600.
ORCH8_JOIN_TOKEN='o8x1.…' orch8 executor join --config-out orch8.toml --label zone=b
# …or read it from stdin, and start the server right away:
orch8 executor join - --config-out orch8.toml --run < token.txt
```

`executor join` keeps every other section of an existing `orch8.toml`,
merges `--label k=v` over the token's labels, derives the worker id as
`<worker_id_prefix>-<hostname>` (override with `--hostname`), and refuses to
write a config the server would reject.

In containers, skip the file: set `ORCH8_JOIN_TOKEN` and `orch8-server`
decodes it at startup. Explicit `ORCH8_MANAGED_CONTROL_*` variables still
override individual fields; `ORCH8_NODE_ROLE=edge` keeps the edge role.

## Helm

```bash
kubectl create secret generic orch8-join --from-literal=join-token='o8x1.…'
helm install exec deploy/helm/orch8 \
  --set mode=executor \
  --set hybrid.joinToken.existingSecret=orch8-join \
  --set externalDatabase.existingSecret=orch8-db
```

`mode: executor` renders only the executor Deployment (HTTP health only,
worker gRPC surface), injects `ORCH8_JOIN_TOKEN` from the Secret and sets
`HOSTNAME` to the pod name so each replica has its own worker id. The chart
fails closed without the Secret, with an ingress, or on SQLite. See
`deploy/helm/orch8/ci/hybrid-executor-values.yaml`.

## Docker Compose

[`deploy/hybrid-executor/docker-compose.yml`](../deploy/hybrid-executor/docker-compose.yml)
runs one executor with a local Postgres:

```bash
export ORCH8_JOIN_TOKEN='o8x1.…' ORCH8_API_KEY=$(openssl rand -hex 24) \
       ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
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
