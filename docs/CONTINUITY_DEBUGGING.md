# Continuity debugging and production-to-test extraction

Use continuity checkpoints to inspect one execution across runtime handoffs,
run an effect-free continuation from a durable boundary, and turn the same
evidence into a reviewable offline regression fixture.

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

Pre-boundary block outputs are seeded as completed evidence, so the sandbox
starts after the selected boundary and preserves downstream `outputs.*`
references. Recorded or explicitly supplied mocks may run after the boundary.
An unmocked handler fails the simulation; it never falls through to a real
provider, plugin, worker, or built-in effect. Time is virtual and bounded by
`max_ticks` and the logical-duration limit.

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
