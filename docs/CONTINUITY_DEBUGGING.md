# Continuity debugging and production-to-test extraction

Use continuity checkpoints to inspect one execution across runtime handoffs,
run an effect-free continuation from a durable boundary, and turn the same
evidence into a reviewable offline regression fixture.

## Fault laboratory and scenario exploration

The fault laboratory is isolated from production provider I/O and is disabled by
default. Enable it only in a non-production process with
`ORCH8_CONTINUITY_LAB_ENABLED=true`. `POST /continuity/fault-lab/run` (or
`orch8 execution run-fault-lab --request <file>`) accepts a named profile, an
ownership transition, a `before` or `after` durable-write phase, and an initial
epoch. Its virtual-time trace states whether the write committed and whether a
retry is safe; an `exported_to_accepted` write advances the epoch only after the
commit.

`POST /continuity/scenarios/generate` derives a bounded schedule from stable case
IDs supplied by schema, router, event-join, retry, policy, invariant, and handoff
timing analysis. Always set `max_scenarios`, `max_steps`, and
`max_virtual_time_ms`; the server rejects dimensions above its hard limits.
Identical inputs and seeds return identical scenario IDs and schedules.
`POST /continuity/scenarios/reproduce` then removes irrelevant faults, events,
policy facts, retries, and delays while preserving the requested stable failure,
yielding a small fixture suitable for source control.

## Inspect a boundary

List the bounded checkpoint index:

```text
GET /continuity/executions/{continuity_id}/checkpoints?tenant_id={tenant_id}
```

Each entry includes `checkpoint_id`, continuity epoch, runtime-local instance,
sequence/version, block boundary, checkpoint hash, and the historical creation
time. The list spans all ownership epochs rather than only the current runtime.

Fetch a selected checkpoint with:

```text
GET /continuity/executions/{continuity_id}/checkpoints/{checkpoint_id}?tenant_id={tenant_id}
```

The detail contains the exact authenticated checkpoint payload and a bounded
`redacted_state_diff` against the preceding checkpoint. Treat exact state as
sensitive operator data: keep tenant authorization enabled and do not paste it
into tickets. The diff is the safer default for collaboration.

## Run an effect-free continuation

Post the selected checkpoint and optional patches to:

```text
POST /continuity/executions/{continuity_id}/what-if
```

For example:

```json
{
  "tenant_id": "tenant-a",
  "checkpoint_id": "019...",
  "context_patch": {"priority": "urgent"},
  "config_patch": {"routing_policy": "quality_first"},
  "block_param_overrides": {
    "classify": {"model": "premium-v2"}
  },
  "output_overrides": {
    "classify": {
      "tier": "premium",
      "usage": {
        "input_tokens": 20,
        "output_tokens": 12,
        "total_tokens": 32,
        "cost_microunits": 1000,
        "external_calls": 1
      }
    }
  },
  "handler_mocks": {"premium_path": {"selected": true}},
  "signals": [
    {
      "signal_type": "custom:human_input:approve",
      "payload": {"value": "continue"}
    }
  ],
  "max_ticks": 5000
}
```

Pre-boundary block outputs are seeded as completed evidence, so the sandbox
starts after the selected boundary and preserves downstream `outputs.*`
references. Recorded or explicitly supplied mocks may run after the boundary.
An unmocked handler fails the simulation; it never falls through to a real
provider, plugin, worker, or built-in effect. Time is virtual and bounded by
`max_ticks` and the logical-duration limit.

The response includes the immutable scenario description, `baseline_report`,
patched `report`, and a `comparison`. The comparison calls out added/removed
blocks, changed output values, terminal-state changes, handler-call deltas,
simulated external-call differences, configured invariant outcomes, and usage
totals/deltas. `effects.production_receipts_created` is always zero: a nonzero
value would violate the sandbox contract.
Usage is read from either the block output itself or its `usage` object;
`prompt_tokens`/`completion_tokens` are accepted aliases for
`input_tokens`/`output_tokens`. Cost is expressed as integer microunits.

List persisted summaries with:

```text
GET /continuity/executions/{continuity_id}/what-if?tenant_id={tenant_id}&limit=100
```

The ledger is tenant scoped and intentionally stores reports/comparisons, not
full production evidence. Apply PostgreSQL migration `063_what_if_runs.sql`
before using these endpoints after an upgrade. Embedded SQLite runtimes migrate
to schema version 29 automatically.

## Extract and run a fixture

Create an extraction request:

```json
{
  "tenant_id": "tenant-a",
  "checkpoint_id": "019...",
  "allowlisted_fields": ["order_id", "document_type"]
}
```

Then write the deterministic artifact set:

```text
orch8 --url http://127.0.0.1:8080 test extract \
  {continuity_id} extract.json --out-dir ./fixtures
```

The command writes:

- `continuation-{stable}.json` — sanitized sequence definition;
- `continuation-{stable}.contracts.json` — runnable continuation contract;
- `continuation-{stable}.evidence.json` — source hashes, receipt mocks, and
  explicit missing-evidence status.

Run it offline:

```text
orch8 test run ./fixtures/continuation-{stable}.contracts.json \
  --sequence ./fixtures/continuation-{stable}.json --report json
```

Extraction allowlists context fields before applying the platform redaction
policy. Sequence parameters and recorded outputs are redacted too. Unknown
effects, absent snapshots, unmocked downstream work, or a non-terminal offline
replay keep `complete: false`; do not promote an incomplete fixture into a
release gate until its evidence gaps are resolved.
