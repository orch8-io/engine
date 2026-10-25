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

The API is synchronous. Blocking native engine operations release the GIL while
the Rust runtime works; Python handlers acquire it when executing Python code.

| Call                                                                    | Does                                                                                                                                                                                            |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Engine.open(path)`                                                     | Open/create the SQLite file. The engine starts on first use. Also a context manager.                                                                                                            |
| `engine.handler(name, fn)` / `@engine.handler(name)`                    | Register a step handler. Register all handlers **before** any other call.                                                                                                                       |
| `engine.deploy(sequence)`                                               | Store a sequence (dict or JSON). Only `name` and `blocks` are required; `version` defaults to 1. Idempotent per name + version — call it on every startup. Changed blocks need a new `version`. |
| `engine.start(name, input=None, *, idempotency_key=None, version=None)` | Create an instance; `input` becomes `context.data`. Returns the instance id. The same key returns the same instance.                                                                            |
| `engine.run(id, *, timeout=None)`                                       | Drive the engine until the instance is `completed`, `failed`, `cancelled`, `paused` or `waiting`, or `timeout` seconds pass. Sleeps through delays and retry backoff. Returns the snapshot.     |
| `engine.get(id)`                                                        | Snapshot `{"id", "state", "data", "outputs"}`; `outputs` is the latest output per step id.                                                                                                      |
| `engine.signal(id, name, payload=None)`                                 | `pause`, `resume`, `cancel`, `update_context`, or a custom signal.                                                                                                                              |
| `engine.close()`                                                        | Stop the engine and release the file.                                                                                                                                                           |

`timeout` must be finite, nonnegative seconds and fit the monotonic clock.
Zero returns the current snapshot without starting a scheduling pass. A missing
or terminal target cannot dispatch other queued workflows. Once a scheduling pass starts,
the timeout bounds waiting for that pass: it returns a durable snapshot while
the owned pass continues. Retrying observation shares live work. Pass admission
is bounded by the scheduler's default concurrency (128); timeout or caller
cancellation does not release a live pass's slot. Independent targets can make
progress while another pass is held, and callers periodically recheck their
own target within a mixed batch.

The timeout does not cancel the workflow or its admitted pass. Lazy initialization
and snapshot reads still require storage I/O and are outside this pass-wait
budget. Closing the engine wakes run waiters and prevents new host factory entry;
already-admitted host callbacks can still finish.

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
  doing it twice (at-most-once for interrupted dispatches). A crash _between_
  steps (e.g. during a `delay`) resumes normally.
- **Recovery happens on open.** Instances left `running` by a crash are picked
  up on the first call after reopening the file; call `start()` again with the
  same `idempotency_key` (or keep the id) and `run()` it.
- **Idempotency keys** make `start()` safe to repeat across restarts.
- **One native owner per database file.** Initialization takes a nonblocking
  operating-system lock before crash recovery. A second handle or process is
  refused while that owner is live; retry after closing the first handle.
  Close releases the lock after its SQLite pool closes. Dropping an engine with
  a live owned pass retains authority until that pass ends. In-memory engines
  are independent. Use `orch8-server` with Postgres for shared execution.
- A persistent zero-byte `.orch8-owner` companion file holds the advisory lock.
  Keep it beside the database and do not delete, replace or rename either file
  while an engine is live. Symlink paths resolve to the same companion; Unix
  hard-linked databases are refused. Other filesystem aliases and filesystems
  without working advisory locks are outside the supported ownership contract.
- `run()` ticks the whole engine, so other due instances progress too.

## Build and test

Run from the repository root with Python 3.10+ and the repository's Rust
toolchain. A virtual environment is required by `maturin develop`.

```sh
cd packages/python-native
python3 -m venv .venv
. .venv/bin/activate
python -m pip install 'maturin>=1.8,<2'
maturin develop --locked
python -m unittest discover -s tests
cd ../..  # return to the repository root; the virtual environment stays active
```

The `pyproject.toml` selects the `orch8_engine._native` module and abi3 Python
3.10 compatibility. Let maturin place the platform library; manually copying a
host library from an assumed Cargo target path is unnecessary.

Runtime retirement does not join an already-entered host callback during Python
object collection. A callback that outlives its step timeout keeps its resources
until it returns, even after `close()` and engine collection. Arbitrary Python
host code is not forcibly interrupted.
