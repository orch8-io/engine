# Placement: residency, labels, affinity, lanes, budgets

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Placement decides **which runtime may run a worker step**. It extends the
existing capability placement (`$runtime`, see
[Distributed runtimes](DISTRIBUTED_RUNTIMES.md)) instead of adding a second
scheduler: every rule below compiles into the step's worker-task
requirements, and the single claim predicate enforces them for HTTP polls,
queue polls, and the negotiated gRPC stream, on PostgreSQL and SQLite alike.

| Feature | Where | Guarantee |
|---|---|---|
| Data residency | `placement.residency`, policy `require.residency` | Never dispatched to a non-matching runtime. The step waits with the visible reason `placement_unsatisfied`. |
| Capability labels | `placement.labels`, policy `require.labels` | Hard: every label must be advertised with the same value. |
| Region | `placement.region`, policy `require.region` | Hard: the runtime must advertise the region. |
| Soft preference | policy `prefer.labels` | Preferred runtimes get the task first for a bounded wait, then any eligible runtime. |
| Sticky affinity | `placement.affinity: "instance"` | Next steps prefer the runtime that ran the previous worker step, falling back after `affinity_wait_ms`. |
| Priority lanes | sequence `placement.priority_lane`, plan default | Maps onto instance priority and cooperative preemption. |
| Global rate budgets | step `rate_budget` | Durable token bucket shared by every node; over budget the instance is deferred, never failed. |
| Autoscaling metrics | `orch8_queue_depth{capability,region,priority_lane}` | Pending worker backlog for KEDA/HPA. |
| Trace propagation | `task.context.runtime.traceparent` | One trace spans control plane → executor → completion. |

## Step and sequence `placement`

```json
{
  "name": "billing",
  "placement": { "residency": "eu", "priority_lane": "premium" },
  "blocks": [
    { "type": "step", "id": "charge", "handler": "stripe_charge",
      "rate_budget": "stripe-api",
      "placement": { "region": "eu-west-1", "labels": { "pci": "true" },
                     "affinity": "instance", "affinity_wait_ms": 10000 } }
  ]
}
```

| Field | Level | Meaning |
|---|---|---|
| `region` | step, sequence | Only runtimes advertising this region (`capabilities.regions`). Narrows an explicit `$runtime.regions`. |
| `labels` | step, sequence | Every `key: value` must be advertised in `capabilities.labels`. |
| `residency` | step, sequence | Only runtimes advertising the label `residency=<zone>`. Never relaxed. |
| `affinity` | step, sequence | `instance` or `none` (default). |
| `affinity_wait_ms` | step, sequence | Bounded wait for affinity and preferred labels. Default 15000, max 600000. |
| `priority_lane` | sequence only | `premium` (high), `standard` (normal), `batch` (low). |

Sequence-level placement applies to every worker-dispatched step. A step may
add labels and set affinity, but it can never escape the sequence's region or
residency: a contradiction is rejected when the sequence is created
(`INVALID_BLOCK`). Placement is validated at create time: empty or oversized
facts (over 128 bytes, over 32 labels), `priority_lane` on a step, and
`region`/`labels`/`residency` on a built-in handler (built-ins run on the
engine node, not on a worker) are rejected.

A step with `region`, `labels`, or `residency` is **always** dispatched to
the worker queue, even when the engine has the handler registered
in-process. Plugin handlers (`ap://`, `grpc://`, `wasm://`) are not placed.

### Advertising labels

Runtimes advertise labels alongside their other capability facts:

- HTTP: `capabilities.labels` on `POST /workers/tasks/poll` (or
  `POST /runtimes/register`), e.g.
  `{"labels": {"residency": "eu", "gpu": "a100"}, "regions": ["eu-west-1"], ...}`.
- gRPC stream: the `runtime_capabilities_json` of the `WorkerStreamOpen`
  frame / `RuntimeHeartbeat`. A session that negotiated
  `runtime_capabilities` now claims through the capability predicate, so it
  receives placed work.
- Executors joined with `orch8 executor join <token> --label residency=eu`
  advertise the token's labels and region.

Legacy capability-less polls only claim unplaced tasks, so placed work never
leaks to a runtime that cannot prove where it runs.

### `placement_unsatisfied`

When a placed task is enqueued and no live runtime satisfies it, the task
stays `pending` and the instance records why:

- `metadata.placement = {"status": "placement_unsatisfied", "block_id", "task_id", "since", "regions", "labels", "residency"}`
  (flips to `{"status": "placed", "worker_id", ...}` once a runtime claims it);
- audit event `placement_unsatisfied`;
- counter `orch8_placement_unsatisfied_total` and gauge
  `orch8_placement_unsatisfied{capability,region}`;
- `GET /instances/{id}/diagnosis` reports `PLACEMENT_UNSATISFIED`
  ([ORCH8-D021](ERRORS.md#ORCH8-D021)).

The step timeout still applies; there is no silent fallback.

## Tenant placement policies

`GET|PUT /api/v1/placement/policies` manage the tenant's ordered policy list.
The tenant is the `X-Tenant-Id` principal (the root key may pass
`?tenant_id=`).

```json
PUT /api/v1/placement/policies
{
  "items": [
    { "name": "eu-billing",
      "match": { "sequence": "billing" },
      "require": { "residency": "eu" },
      "prefer": { "labels": { "tier": "fast" } } },
    { "name": "gpu-render",
      "match": { "handler": "render_video" },
      "require": { "labels": { "gpu": "a100" } },
      "prefer": null },
    { "name": "vip",
      "match": { "tag": "vip" },
      "require": { "region": "eu-west-1" } }
  ]
}
```

- `match` fields (`sequence` name, step `handler`, instance `tag` — an entry
  of the instance's `metadata.tags` array) must all match; an empty `match`
  applies to every worker step of the tenant.
- Every matching policy's `require` is added as a hard constraint. A
  contradiction with the step/sequence placement or another policy fails the
  step permanently with a `placement conflict` message; residency is never
  overridden by a more specific rule.
- `prefer.labels` from matching policies become a soft preference with the
  default 15 s wait.
- Limits: 256 policies, unique non-empty names, 32 labels per map.
- `PUT` replaces the whole list. The writing node applies it immediately;
  other nodes within 5 s (policy cache TTL).

## Sticky affinity

With `affinity: "instance"`, the engine looks up the runtime (worker id) that
completed the instance's previous worker step and adds a preference: until
`affinity_wait_ms` has passed, only that runtime may claim the task; after
that any runtime satisfying the hard constraints may. The first worker step
of an instance has no preference. Affinity is a preference, never a hard
constraint: a dead or draining preferred runtime delays the step by at most
the wait. Legacy polls honour it too (the worker id is compared in SQL).

## Priority lanes

Lanes map onto the existing instance priority, which orders scheduler claims
and drives [cooperative priority preemption](ENGINE_FEATURE_PRIORITIES.md):
`premium` → `high`, `standard` → `normal`, `batch` → `low`.

`POST /instances` (and `/instances/batch`) resolves the priority as: explicit
`priority`, else `priority_lane` on the request, else the sequence's
`placement.priority_lane`, else the tenant plan's `default_priority_lane`
(entitlements), else `normal`. Instances created by cron, triggers, and jobs
keep their explicit/default priority.

## Global rate budgets

A rate budget is a durable token bucket per `(tenant, key)` in the
`rate_budgets` table. Every engine node takes tokens from the same row under
a row lock, so the budget holds across the whole fleet.

```http
PUT /api/v1/rate-budgets/stripe-api
{ "capacity": 100, "refill_per_sec": 25 }
```

- `capacity` is the burst (1..=1000000); `refill_per_sec` the sustained rate
  (e.g. `0.5` = 30 per minute). Creating a budget fills it; reshaping keeps
  the current tokens clamped to the new capacity.
- `GET /api/v1/rate-budgets` lists them with the current `tokens`;
  `DELETE /api/v1/rate-budgets/{key}` removes one.
- A step with `"rate_budget": "stripe-api"` takes one token right before
  dispatch (after `delay`, `send_window`, and `rate_limit_key`). With no
  token the instance is parked until the next token refills
  (`orch8_rate_budget_deferred_total`); it is never failed. An unconfigured
  key does not gate.
- Keys are 1–128 bytes of `[A-Za-z0-9._:-]`, validated at sequence create.

`rate_limit_key` (fixed window, per tenant) keeps working; rate budgets add
the smooth, fleet-wide token bucket that downstream API quotas need.

## Autoscaling metrics (KEDA / HPA)

Every node publishes, every 15 s, the pending worker-task backlog of live
instances:

| Metric | Labels | Meaning |
|---|---|---|
| `orch8_queue_depth` | `capability` (handler), `region` (`any` or the required regions), `priority_lane` | Claimable pending worker tasks. Series that drain are set to 0. |
| `orch8_placement_unsatisfied` | `capability`, `region` | Pending placed tasks no live runtime satisfies. |

The unlabeled `orch8_queue_depth` series (instances claimed in the current
scheduler tick) is unchanged; select the backlog with `capability!=""`. All
nodes report the same cluster-wide backlog, so aggregate with **`max`**, not
`sum`.

A ready ScaledObject is in
[`deploy/keda/scaledobject.yaml`](../deploy/keda/scaledobject.yaml):

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orch8-executor-stripe
spec:
  scaleTargetRef:
    name: orch8-executor
  minReplicaCount: 1
  maxReplicaCount: 20
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring:9090
        query: max(orch8_queue_depth{capability="stripe_charge",region="eu-west-1"})
        threshold: "20"
```

For HPA without KEDA, expose the same query through prometheus-adapter as an
external metric.

## Tracing

Worker steps carry a W3C `traceparent` in `task.context.runtime.traceparent`
(the same task JSON is delivered by HTTP polls and the gRPC stream):

- with OpenTelemetry export enabled (`ORCH8_OTLP_ENDPOINT`), it is the
  dispatching engine span, so worker spans become its children;
- otherwise it is deterministic: trace id = instance id (hex), span id from
  the task id — every worker span of one instance still shares a trace.

Workers should start their span as a child of that `traceparent` and echo
their own span context on completion — the `traceparent` HTTP header or body
field of `POST /workers/tasks/{id}/complete`, or gRPC metadata `traceparent`
on `CompleteTask`. The engine parents its `orch8.worker_task.complete` span on
it, so the trace continues from control plane → executor → step → back.
The value is never persisted on the instance.

## Rejected designs

- **Relaxing residency after a timeout.** Residency is a legal guarantee;
  the step waits (`placement_unsatisfied`) and times out instead.
- **A separate placement scheduler.** Everything compiles into the existing
  capability requirements and claim predicate.
- **Per-step priority.** Priority and preemption are per instance; lanes are
  set on the sequence, request, or plan.
