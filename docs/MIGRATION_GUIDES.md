# Migrate to Orch8

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
determinism, and side-effect semantics. Orch8 therefore ships format-upgrade and
validation tooling, not a misleading drop-in runtime. Small source importers can
be added later for mechanically safe constructs while emitting explicit TODOs
for semantic gaps.
