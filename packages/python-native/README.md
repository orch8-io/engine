# orch8-engine-native

Durable workflows inside your Python process — no server, just import. The
same Rust engine as `orch8-server` (PyO3, abi3, Python 3.10+), persisted to
one SQLite file.

```python
from orch8_engine import Engine
engine = Engine.open("app.db")
engine.handler("charge", lambda ctx: {"charged": ctx["data"]["amount"]})
engine.handler("email", lambda ctx: {"sent": ctx["outputs"]["charge"]["charged"]})
engine.deploy({"name": "pay", "blocks": [
    {"type": "step", "id": "charge", "handler": "charge"},
    {"type": "step", "id": "email", "handler": "email"}]})
run_id = engine.start("pay", {"amount": 42}, idempotency_key="order-1")
print(engine.run(run_id))  # {'id', 'state': 'completed', 'data', 'outputs'}
```

A runnable 3-step example with a simulated crash is in
[`examples/durable-python`](../../examples/durable-python).

## API

The API is synchronous; every call releases the GIL while the engine works.

| Call | Does |
|---|---|
| `Engine.open(path)` | Open/create the SQLite file. The engine starts on first use. Also a context manager. |
| `engine.handler(name, fn)` / `@engine.handler(name)` | Register a step handler. Register all handlers **before** any other call. |
| `engine.deploy(sequence)` | Store a sequence (dict or JSON). Only `name` and `blocks` are required; `version` defaults to 1. Idempotent per name + version — call it on every startup. Changed blocks need a new `version`. |
| `engine.start(name, input=None, *, idempotency_key=None, version=None)` | Create an instance; `input` becomes `context.data`. Returns the instance id. The same key returns the same instance. |
| `engine.run(id, *, timeout=None)` | Drive the engine until the instance is `completed`, `failed`, `cancelled`, `paused` or `waiting`, or `timeout` seconds pass. Sleeps through delays and retry backoff. Returns the snapshot. |
| `engine.get(id)` | Snapshot `{"id", "state", "data", "outputs"}`; `outputs` is the latest output per step id. |
| `engine.signal(id, name, payload=None)` | `pause`, `resume`, `cancel`, `update_context`, or a custom signal. |
| `engine.close()` | Stop the engine and release the file. |

Handlers receive a dict `{"params", "data", "outputs", "instance_id",
"step_id", "attempt"}` and return any JSON-serializable value. `async def`
handlers work too (each call runs in its own event loop). Handlers run on an
engine worker thread. Any exception is **retryable** (the step's `retry` policy
decides; without one the step fails); raise `PermanentError` to fail the step
immediately.

`validate_sequence_json`, `sequence_schema_version` and `run_sequence_json`
(an isolated in-memory dry run of built-in handlers) are still exported.

## Semantics

- **Completed steps are never repeated.** A step's output is committed to
  SQLite before the next step starts; after a crash or restart, execution
  resumes at the first step without an output.
- **Errors retry.** A handler that raises a retryable error runs again per the
  step's `retry` policy, so a handler can run more than once — make side
  effects idempotent (use `instance_id` + `step_id` as the idempotency key
  towards external systems).
- **A step interrupted mid-handler is not re-run automatically.** If the
  process dies while a handler is in flight, Orch8 cannot know whether its
  side effect happened, so on restart the instance goes to `failed` with an
  "automatic redispatch is blocked" output on that step instead of possibly
  doing it twice (at-most-once for interrupted dispatches). A crash *between*
  steps (e.g. during a `delay`) resumes normally.
- **Recovery happens on open.** Instances left `running` by a crash are picked
  up on the first call after reopening the file; call `start()` again with the
  same `idempotency_key` (or keep the id) and `run()` it.
- **Idempotency keys** make `start()` safe to repeat across restarts.
- **One process per database file.** The engine assumes it is the file's only
  owner. Do not open the same file from two processes; use `orch8-server`
  with Postgres for that.
- `run()` ticks the whole engine, so other due instances progress too.

## Build and test

```sh
maturin develop                      # or, without maturin (macOS):
cargo rustc --lib -- -C link-arg=-undefined -C link-arg=dynamic_lookup
cp "$CARGO_TARGET_DIR"/*/debug/lib_native.dylib python/orch8_engine/_native.abi3.so
PYTHONPATH=python python -m unittest discover -s tests
```
