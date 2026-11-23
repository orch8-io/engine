# Migrate to Orch8

> **Stability: stable**, covered by the [1.0 stability contract](../STABILITY.md).

Orch8 does not emulate another orchestrator's runtime. Migration is an explicit
translation into a versioned sequence plus external workers, followed by
effect-free replay and a canary. This keeps cutover observable and reversible.

## Common path

1. Inventory workflows, activities/tasks, schedules, signals, retries, timeouts,
   search attributes, and side effects.
2. Translate control flow to blocks and keep business code in workers.
3. Run `orch8 sequence upgrade-format legacy.json --out sequence.json` to stamp
   the current schema and normalize old `a_b_split` blocks.
4. Run `orch8 sequence preflight --file sequence.json` and contract tests.
5. Shadow traffic, then use `orch8 release validate`, `gate`, and `canary`.

## Source importers

`orch8 import` converts a workflow into a versioned sequence document plus a
conversion report (`--report report.json`). The report lists every mapped
construct, every TODO stub, the worker handlers you still have to implement,
trigger/cron bodies to create next to the sequence, and an `unmapped` section
with `file:line` for everything that could not be translated faithfully.
Nothing is dropped silently: an untranslatable condition is kept as a route
gated on an explicit `data.todo_*` flag, an unknown primitive becomes a
visible `log` stub.

```bash
orch8 import stepfunctions state-machine.asl.json --out sequence.json --report report.json
orch8 import temporal src/workflows/ --workflow orderWorkflow --out sequence.yaml
orch8 import inngest src/inngest/functions.ts --workflow user-onboarding --out sequence.json
orch8 import bullmq src/flows.ts --out sequence.json
orch8 import n8n workflow.json --out sequence.json   # and `zapier`
```

Then run `orch8 sequence preflight --file sequence.json`, implement the listed
worker handlers, and follow the common path above.

| Source                | Input                                                          | Fidelity                       |
| --------------------- | -------------------------------------------------------------- | ------------------------------ |
| AWS Step Functions    | ASL JSON, or `aws stepfunctions describe-state-machine` output | Full structural translation    |
| Temporal (TypeScript) | workflow file or directory                                     | Static skeleton + report       |
| Inngest (TypeScript)  | function file or directory                                     | Static skeleton + report       |
| BullMQ                | file or directory with `FlowProducer.add`                      | Flow tree translation + report |

### Step Functions mapping

| ASL                                                          | Orch8                                                                                                                             |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `Task` Lambda (`lambda:invoke` or function ARN) / activity   | worker step (`ValidateOrder` → handler `validate_order`)                                                                          |
| `Task` `http:invoke`                                         | `http_request`                                                                                                                    |
| `Task` `states:startExecution.sync`                          | `sub_sequence`                                                                                                                    |
| other service integrations (`sns:publish`, `aws-sdk:*`, ...) | worker stub `aws_<service>_<action>`                                                                                              |
| Ordered `Retry`                                              | Step `retry.rules` retain error names, per-rule retry budgets, interval, rate, maximum delay, and `JitterStrategy`.                 |
| `Parallel` and inline `Map` retries                         | The composite owns the retry budget. Each retry starts all branches or replays all items from the original collection.             |
| `TimeoutSeconds`                                             | step `timeout`                                                                                                                    |
| `HeartbeatSeconds` / `HeartbeatSecondsPath`                   | Renewable deadline on worker-backed tasks. The path resolves against the task input.                                             |
| Ordered `Catch`                                              | `try_catch` with an error router. Named errors match the first applicable rule, with wildcard fallback and per-catcher `ResultPath`. |
| `Choice`                                                     | `router`, branches converted up to their join state; no `Default` → `fail` with `States.NoChoiceMatched`                          |
| `Choice` that loops back (polling)                           | `loop` (forward part once, then continue path + forward part per iteration)                                                       |
| `Parallel` / `Map` (inline)                                  | `parallel` / `for_each` over `{{items path}}`                                                                                     |
| `Wait` `Seconds` / `Timestamp`                               | delayed `noop` / `fire_at_local` in UTC                                                                                           |
| `Pass` with `Result`/`Parameters`                            | `transform`                                                                                                                       |
| `Fail` / `Succeed`                                           | `fail` / end of chain                                                                                                             |

Composite retries preserve effect history from earlier attempts. If a child
effect's outcome is still unresolved, the engine refuses to start another
aggregate attempt. For a synchronous child execution, the engine waits for
the preceding attempt's child tree to settle and requests cancellation of
children that are still active. A retry starts fresh child executions and keeps
the old runs and their receipts. Dispatched or unknown child effects require
reconciliation before replay. The importer reports retry policies on distributed
maps as unsupported.

JSONPath references are resolved through the data flow: the execution input is
`data`, a task result is `outputs.<block>` (`ResultPath`, `InputPath`,
`OutputPath` and the Lambda `Payload` wrapper are tracked), `$$.Map.Item.Value`
is the `for_each` item. Inline `ItemSelector` builds the input object before
each iteration using literals, the item value, and its persisted zero-based
index. A task's `ResultSelector` creates a projected object
with nested arrays and direct result references before `ResultPath` exposes it
to later states. Projections over composite Map and Parallel results remain
unmapped. Other reported gaps include unsupported intrinsic functions, JSONata,
and distributed-Map `ItemReader` and `ResultWriter`.

Worker-backed `.waitForTaskToken` tasks receive a `callback_token` on the claim.
Use `$$.Task.Token` directly in parameters or inside the supported ASL
intrinsics. The engine resolves workflow references when it enqueues the task
and inserts the token only into the claim response. Forward
it to the callback producer, then submit it with the result as `{"token": "...",
"output": ...}` to `POST /api/v1/workers/callbacks/complete` using a credential
for the workflow's tenant. The token belongs to one claim, and repeated requests
return the original acceptance. The task's absolute timeout ends the wait.
Use `TimeoutSecondsPath` to read that timeout from the state input. The engine
validates the resolved seconds and persists the deadline when it enqueues the task.
A task without an explicit timeout uses a 365-day maximum. Imported callback
tasks accept success through this endpoint; ordinary worker completion cannot
advance them.

A worker-backed task's heartbeat deadline starts at claim and extends on each
accepted heartbeat or checkpoint. The claimed task advertises the workflow's
interval as its lease. An elapsed heartbeat deadline fails the attempt with
`States.Timeout`, so the source retry and catch rules apply. `TimeoutSeconds`
or `TimeoutSecondsPath` caps the total attempt duration. The import report flags
heartbeat and dynamic timeout fields on in-process HTTP and sub-sequence tasks.

Loop conditions read `context.data`, not step outputs. External worker outputs
merge into `context.data`, so the worker behind the polled task must return
the checked field at the top level of its output — the report says which.

Wait states using `SecondsPath` or `TimestampPath` read their input on entry
and persist the resulting wake time. A restart or early wake uses the same
deadline. Timestamp waits follow Step Functions' whole-second precision.

JSONPath parameters support `States.Format`, `States.Array`, and
`States.StringToJson`. Nested calls preserve JSON arrays, objects, booleans,
and nulls. The importer reports unsupported functions or references as
unmapped. At runtime, malformed JSON and invalid format arguments fail the
producing step.

### Code-first engines (Temporal, Inngest, BullMQ)

Workflows written in code need their business logic in workers anyway, so the
importer extracts the durable skeleton statically (no execution, no type
checking) and turns each step body into a worker handler to port:

| Source                                                          | Orch8                                                                                                                                 |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Inngest `step.run(id, fn)`                                      | worker step `id`, retry from the function's `retries` (default 4)                                                                     |
| Inngest `step.sleep` / `sleepUntil`                             | delayed `noop`                                                                                                                        |
| Inngest `step.waitForEvent({ event, match, timeout })`          | `wait_for_event` correlated on `{{data.<match>}}`, wrapped in `try_catch` when a timeout should resolve `null`                        |
| Inngest `step.sendEvent` / `step.invoke` / `step.fetch`         | `emit_event` / `sub_sequence` / `http_request`                                                                                        |
| Temporal activity from `proxyActivities`                        | worker step; `startToCloseTimeout` → `timeout`, `scheduleToCloseTimeout` → `deadline`, `retry` → `retry`, `taskQueue` → `queue_name`  |
| Temporal `sleep` / `condition(fn, timeout)`                     | delayed `noop` / `wait_for_input` gate released by the `human_input:<gate>` signal                                                    |
| Temporal `executeChild` / `CancellationScope.nonCancellable`    | `sub_sequence` / `cancellation_scope`                                                                                                 |
| BullMQ `FlowProducer.add({ children })`                         | children in `parallel` (recursively), then the parent job; `queueName` → `queue_name`, `attempts`/`backoff`/`delay` → `retry`/`delay` |
| `Promise.all([...])` / `Promise.race([...])`                    | `parallel` / `race`                                                                                                                   |
| `Promise.all(items.map(...))`                                   | `for_each` (sequential; reported)                                                                                                     |
| `if/else`, `for..of`, `while`, `try/catch/finally` around steps | `router`, `for_each`, `loop`, `try_catch` (a rethrow becomes a trailing `fail`)                                                       |

Conditions and arguments are translated when they only read the workflow
input (Inngest `event` → `data`, Temporal first argument → `data`) and earlier
step results (`const user = await step.run(...)` → `outputs.<block>`).
Reported with `file:line`: untranslatable conditions and arguments, early
`return`s inside branches, signal/query/update handlers, `continueAsNew`,
`startChild`, versioning `patched`, function options (`concurrency`,
`throttle`, `debounce`, `cancelOn`, `batchEvents`, ...), standalone BullMQ
`Queue.add` jobs and non-default flow failure options. Temporal activities
without `retry.maximumAttempts` retry forever in Temporal; the importer maps
them to 10 attempts and says so.

## Temporal

Map a Workflow to one sequence version, Activities to step handlers, Signals to
Orch8 signals, child workflows to `sub_sequence`, and Saga compensations to the
`saga` block. Preserve Temporal workflow IDs as idempotency keys during dual
run. Do not copy event history; export representative inputs/outputs as Orch8
contract fixtures and replay them without effects.

## Airflow

Map a DAG to a sequence, Operators to workers, BranchPythonOperator to `router`,
TaskGroup to composites, retries/timeouts directly, and schedules to Orch8 cron.
Replace XCom with typed block outputs. Run both schedulers only while tasks use
shared idempotency keys, or side effects may execute twice.

## Prefect

Map a Flow to a sequence, Tasks to step handlers, mapped tasks to `for_each`,
subflows to `sub_sequence`, and deployments/schedules to sequence releases and
cron. Convert result persistence into block outputs or the artifact store.

## Why guides, not runtime shims?

Compatibility shims preserve source syntax but cannot preserve failure,
determinism, and side-effect semantics. Orch8 therefore ships format-upgrade,
validation, and source-importer tooling (above), not a misleading drop-in
runtime: importers translate mechanically safe constructs and emit explicit
TODOs and `unmapped` entries for semantic gaps.

## Replay source-run evidence after import

Pass `--report` for the translation inventory and `--parity-from` to map
observed source steps into an offline contract suite:

```sh
orch8 import stepfunctions original.json --out imported.json --report report.json \
  --parity-from source-run.json --parity-out imported.contracts.json
orch8 test run imported.contracts.json --report coverage
```

Each entry in the evidence file's `cases` array needs a `name`, a sequence
`input`, ordered `observed_nodes` from the source execution, and a
`terminal_state`. Supply `mocks` and `signals` as needed by the translated
sequence. The importer requires each observed node to name exactly one
translated executable step. Parent branches, repeated loop iterations, and
manual stubs need a contract case authored against the converted sequence.
