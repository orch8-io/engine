# External Workers

Workers run handler code **outside** the engine binary. Use them when you want to:

- Write handlers in Node, Python, Go, or any language that can POST JSON.
- Keep handler code in an existing service so it shares code, secrets, and deploys.
- Scale handler concurrency independently from the scheduler.

The engine doesn't care how a worker is implemented — it's a pull-based REST protocol. This guide documents the protocol so you can implement a worker in any language; for Node specifically, use [`@orch8/worker-sdk`](../worker-sdk-node/README.md).

---

## How it works

1. Sequence contains a step with a handler name the engine does **not** have registered as a built-in (`noop`, `log`, `sleep`, `http_request`) or a native Rust handler.
2. Scheduler queues a row in `worker_tasks` with the block's `params` and instance `context`.
3. Worker polls `POST /workers/tasks/poll` for tasks matching its handler name.
4. Worker executes, optionally sends heartbeats, then reports success or failure.
5. Engine resumes the workflow with the worker's output persisted as a `BlockOutput`.

Selection uses Postgres' `FOR UPDATE SKIP LOCKED` — many workers can poll concurrently without double-claiming.

---

## Protocol

All endpoints accept and return JSON. See [API.md](API.md#external-workers) for the canonical request/response shapes.

### 1. Poll for tasks

```
POST /workers/tasks/poll
Content-Type: application/json

{
  "handler_name": "send_email",
  "worker_id": "node-worker-42",
  "limit": 10
}
```

Returns an array of up to `limit` `WorkerTask` objects. Empty array means no work. Each task is locked to this `worker_id` until completion or heartbeat timeout.

### 2. Heartbeat (long-running tasks)

```
POST /workers/tasks/{task_id}/heartbeat
{ "worker_id": "node-worker-42" }
```

Call every 15–30s. Tasks with no heartbeat for 60s are reclaimed by the engine's reaper and re-offered to other workers.

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

ENGINE = "http://localhost:8080"
WORKER_ID = f"py-worker-{socket.gethostname()}-{os.getpid()}"

def send_email(task):
    # ... your handler ...
    return {"message_id": "msg-123"}

HANDLERS = {"send_email": send_email}

with httpx.Client(base_url=ENGINE, timeout=10) as http:
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

- [`@orch8/worker-sdk` README](../worker-sdk-node/README.md) — the Node implementation of this protocol
- [API reference — External Workers](API.md#external-workers)
- [Architecture — External Worker System](ARCHITECTURE.md#external-worker-system)
