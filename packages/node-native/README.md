# @orch8/engine-native

Durable workflows inside your Node process — no server, just import. The same
Rust engine as `orch8-server`, persisted to one SQLite file.

```js
import { Engine } from '@orch8/engine-native' // ESM (top-level await)
const engine = await Engine.open('app.db')
engine.handler('charge', async ({ data }) => ({ charged: data.amount }))
engine.handler('email', async ({ outputs }) => ({ sent: outputs.charge.charged }))
await engine.deploy({ name: 'pay', blocks: [
  { type: 'step', id: 'charge', handler: 'charge' },
  { type: 'step', id: 'email', handler: 'email' } ] })
const id = await engine.start('pay', { amount: 42 }, { idempotencyKey: 'order-1' })
console.log(await engine.run(id)) // { id, state: 'completed', data, outputs }
```

A runnable 3-step example with a simulated crash is in
[`examples/durable-node`](../../examples/durable-node).

## API

| Call | Does |
|---|---|
| `await Engine.open(path)` | Open/create the SQLite file. The engine starts on first use. |
| `engine.handler(name, fn)` | Register a step handler (chainable). Register all handlers **before** any other call. |
| `await engine.deploy(sequence)` | Store a sequence (object or JSON). Only `name` and `blocks` are required; `version` defaults to 1. Idempotent per name + version — call it on every startup. Changed blocks need a new `version`. |
| `await engine.start(name, input?, { idempotencyKey?, version? })` | Create an instance; `input` becomes `context.data`. Returns the instance id. The same key returns the same instance. |
| `await engine.run(id, { timeoutMs? })` | Drive the engine until the instance is `completed`, `failed`, `cancelled`, `paused` or `waiting` (for a signal/event), or the timeout elapses. Sleeps through `delay`s and retry backoff. Returns the snapshot. |
| `await engine.get(id)` | Snapshot `{ id, state, data, outputs }`; `outputs` is the latest output per step id. |
| `await engine.signal(id, name, payload?)` | `pause`, `resume`, `cancel`, `update_context`, or a custom signal (e.g. for `wait_for_input`). |
| `await engine.close()` | Stop the engine and release the file. |

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
  charging twice (at-most-once for interrupted dispatches). A crash *between*
  steps (e.g. during a `delay`) resumes normally.
- **Recovery happens on open.** Instances left `running` by a crash are picked
  up on the first call after reopening the file; call `start()` again with the
  same `idempotencyKey` (or keep the id) and `run()` it.
- **Idempotency keys** make `start()` safe to repeat across restarts.
- **One process per database file.** The engine assumes it is the file's only
  owner (that is what makes immediate crash recovery safe). Do not open the
  same file from two processes; use `orch8-server` with Postgres for that.
- `run()` ticks the whole engine, so other due instances progress too.

## Build

```sh
pnpm --package=@napi-rs/cli@3 dlx napi build --platform --no-js --dts native.d.ts  # debug
pnpm run build                                                                      # release (needs @napi-rs/cli)
node --test test/*.test.js
```
