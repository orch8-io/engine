# External Workers

Workers run handler code **outside** the engine binary. Use them when you want to:

- Write handlers in Node, Python, Go, or any language that can POST JSON.
- Keep handler code in an existing service so it shares code, secrets, and deploys.
- Scale handler concurrency independently from the scheduler.

The engine doesn't care how a worker is implemented — it's a pull-based REST protocol. This guide documents the protocol so you can implement a worker in any language; for Node specifically, use [`@orch8.io/sdk`](https://github.com/orch8-io/sdk-node).

---

## How it works

1. Sequence contains a step with a handler name the engine does **not** have registered as a built-in (see [Architecture — Built-in Step Handlers](ARCHITECTURE.md#built-in-step-handlers)) or a native Rust handler.
2. Scheduler queues a row in `worker_tasks` with the block's `params` and instance `context`.
3. Worker polls `POST /workers/tasks/poll` for tasks matching its handler name.
4. Worker executes, optionally sends heartbeats, then reports success or failure.
5. Engine resumes the workflow with the worker's output persisted as a `BlockOutput`.

Selection uses Postgres' `FOR UPDATE SKIP LOCKED` — many workers can poll concurrently without double-claiming.

---

## Protocol

All product endpoints below are relative to the canonical `/api/v1` base URL
and accept/return JSON. Authenticated deployments require `x-api-key` and
`x-tenant-id` on every worker request. See
[API.md](API.md#external-workers) for the canonical request/response shapes.

### 1. Poll for tasks

```
POST /workers/tasks/poll
Content-Type: application/json

{
  "handler_name": "send_email",
  "worker_id": "node-worker-42",
  "limit": 10,
  "version": "1.4.2"
}
```

Returns an array of up to `limit` `WorkerTask` objects. Empty array means no work. Each task is locked to this `worker_id` until completion or heartbeat timeout.

The optional `version` field is recorded on the worker registry and checked against
version pins (see [Fleet management](#fleet-management)) — a worker below a pinned
`min_version` for that handler is refused tasks. Workers bound to a named queue poll
`POST /workers/tasks/poll/queue` with an additional `queue_name` field instead.

### 2. Heartbeat (long-running tasks)

```
POST /workers/tasks/{task_id}/heartbeat
{ "worker_id": "node-worker-42" }
```

Call every 15–30s. Tasks with no heartbeat for 60s (`worker_reaper_stale_secs`) are reclaimed by the engine's reaper — which sweeps every 30s (`worker_reaper_tick_secs`) — and re-offered to other workers.

### 3. Complete

```
POST /workers/tasks/{task_id}/complete
{
  "worker_id": "node-worker-42",
  "output": { "message_id": "msg-123", "delivered": true }
}
```

The output JSON is persisted as a `BlockOutput` for this block and becomes available to downstream steps via `context`.

### 4. Fail

```
POST /workers/tasks/{task_id}/fail
{
  "worker_id": "node-worker-42",
  "message": "SMTP connection refused",
  "retryable": true
}
```

- **`retryable: true`** — the worker task is deleted and the instance is rescheduled. The block's retry policy controls backoff and max attempts.
- **`retryable: false`** — permanent failure. If the block sits inside a `try_catch`, the catch branch executes. Otherwise the instance enters the DLQ.

---

## Poll cadence and concurrency

A simple loop is fine:

```
every pollIntervalMs:
  poll -> up to N tasks
  for each task in parallel (bounded by maxConcurrent):
    run handler
    complete or fail
```

Tune:

- **Poll interval** — 500ms to 2s is typical. Lower increases responsiveness; higher reduces DB load.
- **Max concurrent** — start at 10. Raise until the downstream (API call, DB, LLM provider) becomes the bottleneck.
- **Multiple workers** — run N replicas. `SKIP LOCKED` guarantees no duplicate work.

---

## Error handling

| Situation | How to report |
|---|---|
| Transient network / rate limit / 5xx | `fail` with `retryable: true` |
| Permanent business logic error (e.g. user deleted) | `fail` with `retryable: false` |
| Handler crashes / uncaught exception | SDK convention: treat as retryable unless the exception type is explicitly non-retryable |
| Heartbeat missed due to worker crash | Reaper reclaims after 60s; retry runs on a different worker |

---

## Fleet management

Beyond the task protocol, the engine tracks and controls the worker fleet:

- **Registry.** Every poll registers the worker (id, handler, version, last-seen).
  Inspect the fleet with `GET /workers` and the handlers it serves with `GET /handlers`;
  `GET /workers/tasks` and `GET /workers/tasks/stats` show queued/claimed task state.
- **Control channel.** Enqueue a command with `POST /workers/commands`
  (`{"worker_id", "command": "drain" | "reload" | "ping", ...}`). Workers fetch pending
  commands via `GET /workers/{worker_id}/commands` and acknowledge with
  `DELETE /workers/commands/{id}`. `drain` asks the worker to stop claiming new tasks and
  finish in-flight work; `reload` asks it to re-read config; `ping` is a liveness probe.
- **Version pinning.** `POST /workers/version-pins` sets a `min_version` per
  `(tenant, handler_name)`; polls from workers reporting an older `version` return no
  tasks. List with `GET /workers/version-pins`, remove with
  `DELETE /workers/version-pins/{tenant_id}/{handler_name}`.
- **Push dispatch.** Queues can be switched from pull to push mode, where the engine POSTs
  a signed task envelope to the worker's URL instead of waiting for a poll. See
  [API reference — External Workers](API.md#external-workers).

---

## Worker identity

`worker_id` must be unique per process. Good choices:

- `${hostname}-${pid}`
- `${podName}` (in Kubernetes)
- `${service}-${instance_id}`

Do **not** reuse a `worker_id` across restarts without reading heartbeat state first — you may collide with tasks the previous process was still holding.

---

## Running handlers in other languages

A minimal Python worker:

```python
import httpx, time, socket, os

ENGINE = os.getenv("ORCH8_URL", "http://localhost:8080/api/v1")
API_KEY = os.environ["ORCH8_API_KEY"]
TENANT_ID = os.getenv("ORCH8_TENANT_ID", "demo")
WORKER_ID = f"py-worker-{socket.gethostname()}-{os.getpid()}"

def send_email(task):
    # ... your handler ...
    return {"message_id": "msg-123"}

HANDLERS = {"send_email": send_email}

with httpx.Client(base_url=ENGINE, timeout=10, headers={
    "x-api-key": API_KEY,
    "x-tenant-id": TENANT_ID,
}) as http:
    while True:
        for handler_name in HANDLERS:
            r = http.post("/workers/tasks/poll", json={
                "handler_name": handler_name,
                "worker_id": WORKER_ID,
                "limit": 5,
            })
            for task in r.json():
                try:
                    out = HANDLERS[handler_name](task)
                    http.post(f"/workers/tasks/{task['id']}/complete",
                              json={"worker_id": WORKER_ID, "output": out})
                except Exception as e:
                    http.post(f"/workers/tasks/{task['id']}/fail",
                              json={"worker_id": WORKER_ID,
                                    "message": str(e), "retryable": True})
        time.sleep(1)
```

Add heartbeats for any handler that may take more than ~30 seconds.

---

## See also

- [`@orch8.io/sdk`](https://github.com/orch8-io/sdk-node) — the Node implementation of this protocol
- [API reference — External Workers](API.md#external-workers)
- [Architecture — External Worker System](ARCHITECTURE.md#external-worker-system)
