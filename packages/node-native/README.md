# @orch8/engine-native

Durable workflows inside your Node process — no server, just import. The same
Rust engine as `orch8-server`, persisted to one SQLite file.

```js
import { Engine } from "@orch8/engine-native"; // ESM (top-level await)
const engine = await Engine.open("app.db");
engine.handler("charge", async ({ data }) => ({ charged: data.amount }));
engine.handler("email", async ({ outputs }) => ({
  sent: outputs.charge.charged,
}));
await engine.deploy({
  name: "pay",
  blocks: [
    { type: "step", id: "charge", handler: "charge" },
    { type: "step", id: "email", handler: "email" },
  ],
});
const id = await engine.start(
  "pay",
  { amount: 42 },
  { idempotencyKey: "order-1" },
);
console.log(await engine.run(id)); // { id, state: 'completed', data, outputs }
```

A runnable 3-step example with a simulated crash is in
[`examples/durable-node`](../../examples/durable-node).

## API

| Call                                                              | Does                                                                                                                                                                                                            |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `await Engine.open(path)`                                         | Open/create the SQLite file. The engine starts on first use.                                                                                                                                                    |
| `engine.handler(name, fn)`                                        | Register a step handler (chainable). Register all handlers **before** any other call.                                                                                                                           |
| `await engine.deploy(sequence)`                                   | Store a sequence (object or JSON). Only `name` and `blocks` are required; `version` defaults to 1. Idempotent per name + version — call it on every startup. Changed blocks need a new `version`.               |
| `await engine.start(name, input?, { idempotencyKey?, version? })` | Create an instance; `input` becomes `context.data`. Returns the instance id. The same key returns the same instance.                                                                                            |
| `await engine.run(id, { timeoutMs? })`                            | Drive the engine until the instance is `completed`, `failed`, `cancelled`, `paused` or `waiting` (for a signal/event), or the timeout elapses. Sleeps through `delay`s and retry backoff. Returns the snapshot. |
| `await engine.get(id)`                                            | Snapshot `{ id, state, data, outputs }`; `outputs` is the latest output per step id.                                                                                                                            |
| `await engine.signal(id, name, payload?)`                         | `pause`, `resume`, `cancel`, `update_context`, or a custom signal (e.g. for `wait_for_input`).                                                                                                                  |
| `await engine.close()`                                            | Stop the engine and release the file.                                                                                                                                                                           |

`timeoutMs` is a finite integer from 0 through 4,294,967,295. Zero returns
the current snapshot without starting a scheduling pass. A missing or terminal
target cannot dispatch other queued workflows. Once a scheduling pass starts,
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

Handlers receive `{ params, data, outputs, instanceId, stepId, attempt }`:
`params` are the step's params with `{{ ... }}` templates resolved, `data` is
the instance's `context.data`, and `outputs` holds every completed step's output
by step id. They may be sync or async and return any JSON-serializable value.
A thrown error is **retryable** (the step's `retry` policy decides; without one
the step fails); throw `PermanentError` (or any error with `permanent = true`)
to fail the step immediately.

`validateSequenceJson`, `sequenceSchemaVersion` and `runSequenceJson` (an
isolated in-memory dry run of built-in handlers) are still exported.

## Semantics

- **Completed steps are never repeated.** A step's output is committed to
  SQLite before the next step starts; after a crash or restart, execution
  resumes at the first step without an output.
- **Errors retry.** A handler that throws a retryable error runs again per the
  step's `retry` policy, so a step's handler can run more than once — make
  side effects idempotent (use `instanceId` + `stepId` as the idempotency key
  towards external systems).
- **A step interrupted mid-handler is not re-run automatically.** If the
  process dies while a handler is in flight, Orch8 cannot know whether its
  side effect happened, so on restart the instance goes to `failed` with an
  "automatic redispatch is blocked" output on that step instead of possibly
  charging twice (at-most-once for interrupted dispatches). A crash _between_
  steps (e.g. during a `delay`) resumes normally.
- **Recovery happens on open.** Instances left `running` by a crash are picked
  up on the first call after reopening the file; call `start()` again with the
  same `idempotencyKey` (or keep the id) and `run()` it.
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

## Build

From the repository root, with Node.js, pnpm, and the pinned Rust toolchain:

```sh
cd packages/node-native
pnpm install
pnpm run build:debug
# Or: pnpm run build  # optimized release addon
node --test test/*.test.js
cd ../..
```

This package has its own Cargo workspace and lockfile. The JavaScript wrapper
loads the addon from this package's directory; building the root Rust workspace
alone does not produce the Node addon.

### Callback failure diagnostics

`PermanentError` fails without retry. An own data property `permanent: true`
remains supported for explicit error records. Unknown thrown values are retryable.
Failure reporting reads only own string message properties; it does not execute
accessors, proxy traps, `instanceof` hooks or object coercion. Other objects use
`handler failed`. Diagnostics are bounded to 1024 UTF-16 units, preserve complete
surrogate pairs at the boundary, and replace lone surrogates before native JSON
decoding. These rules also apply when input serialization throws an unusual value.

### Physical callback admission (development native sources)

Each loaded native binding shares 128 physical callback slots across its engines,
matching the default scheduler step concurrency. A step timeout, cancelled waiter,
engine close or engine replacement does not release a callback that is still
running. Node retains admission until the real promise settles; Python retains
it through the blocking call, captured-reference release and runtime retirement.
Host callbacks must represent their real completion.

Excess work remains scheduled before durable effect dispatch, preserving its
attempt and prepared receipt. Run it again after capacity becomes available;
reopening the engine cannot reset this allowance. An already-admitted call is
not forcibly stopped, and a late result cannot overwrite its timed-out outcome.
Snapshots omit internal in-progress/interceptor/SLA rows. Handler input also
omits retry bookkeeping, while terminal failure diagnostics remain readable.
