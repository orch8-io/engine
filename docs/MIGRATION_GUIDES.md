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

| Source | Input | Fidelity |
|---|---|---|
| AWS Step Functions | ASL JSON, or `aws stepfunctions describe-state-machine` output | Full structural translation |
| Temporal (TypeScript) | workflow file or directory | Static skeleton + report |
| Inngest (TypeScript) | function file or directory | Static skeleton + report |
| BullMQ | file or directory with `FlowProducer.add` | Flow tree translation + report |

### Step Functions mapping

| ASL | Orch8 |
|---|---|
| `Task` Lambda (`lambda:invoke` or function ARN) / activity | worker step (`ValidateOrder` → handler `validate_order`) |
| `Task` `http:invoke` | `http_request` |
| `Task` `states:startExecution.sync` | `sub_sequence` |
| other service integrations (`sns:publish`, `aws-sdk:*`, ...) | worker stub `aws_<service>_<action>` |
| `Retry[0]` | step `retry` (`MaxAttempts` + 1 attempts, interval, rate, max delay) |
| `TimeoutSeconds` | step `timeout` |
| `Catch[0]` | `try_catch`; when the error path does not rejoin, a marker step + router keeps the success path from running after a caught error |
| `Choice` | `router`, branches converted up to their join state; no `Default` → `fail` with `States.NoChoiceMatched` |
| `Choice` that loops back (polling) | `loop` (forward part once, then continue path + forward part per iteration) |
| `Parallel` / `Map` (inline) | `parallel` / `for_each` over `{{items path}}` |
| `Wait` `Seconds` / `Timestamp` | delayed `noop` / `fire_at_local` in UTC |
| `Pass` with `Result`/`Parameters` | `transform` |
| `Fail` / `Succeed` | `fail` / end of chain |

JSONPath references are resolved through the data flow: the execution input is
`data`, a task result is `outputs.<block>` (`ResultPath`, `InputPath`,
`OutputPath` and the Lambda `Payload` wrapper are tracked), `$$.Map.Item.Value`
is the `for_each` item. Reported, not translated: additional retriers and
catchers, `ErrorEquals` filters, dynamic waits, `ResultSelector` reshaping,
intrinsic functions, JSONata, task tokens (`.waitForTaskToken` — the worker
task id plays that role), distributed-Map `ItemReader`/`ResultWriter`, and
heartbeats.

Loop conditions read `context.data`, not step outputs. External worker outputs
merge into `context.data`, so the worker behind the polled task must return
the checked field at the top level of its output — the report says which.

### Code-first engines (Temporal, Inngest, BullMQ)

Workflows written in code need their business logic in workers anyway, so the
importer extracts the durable skeleton statically (no execution, no type
checking) and turns each step body into a worker handler to port:

| Source | Orch8 |
|---|---|
| Inngest `step.run(id, fn)` | worker step `id`, retry from the function's `retries` (default 4) |
| Inngest `step.sleep` / `sleepUntil` | delayed `noop` |
| Inngest `step.waitForEvent({ event, match, timeout })` | `wait_for_event` correlated on `{{data.<match>}}`, wrapped in `try_catch` when a timeout should resolve `null` |
| Inngest `step.sendEvent` / `step.invoke` / `step.fetch` | `emit_event` / `sub_sequence` / `http_request` |
| Temporal activity from `proxyActivities` | worker step; `startToCloseTimeout` → `timeout`, `scheduleToCloseTimeout` → `deadline`, `retry` → `retry`, `taskQueue` → `queue_name` |
| Temporal `sleep` / `condition(fn, timeout)` | delayed `noop` / `wait_for_input` gate released by the `human_input:<gate>` signal |
| Temporal `executeChild` / `CancellationScope.nonCancellable` | `sub_sequence` / `cancellation_scope` |
| BullMQ `FlowProducer.add({ children })` | children in `parallel` (recursively), then the parent job; `queueName` → `queue_name`, `attempts`/`backoff`/`delay` → `retry`/`delay` |
| `Promise.all([...])` / `Promise.race([...])` | `parallel` / `race` |
| `Promise.all(items.map(...))` | `for_each` (sequential; reported) |
| `if/else`, `for..of`, `while`, `try/catch/finally` around steps | `router`, `for_each`, `loop`, `try_catch` (a rethrow becomes a trailing `fail`) |

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
