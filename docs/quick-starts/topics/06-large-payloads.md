# Externalize large workflow state

Use externalization when large outputs would otherwise inflate the scheduler's
hot database rows. Orch8 stores the payload separately, leaves a compact
reference on the output row, and transparently hydrates it for API consumers.

**Starting point:** the Level 3 project is available. This guide restarts the
server with an intentionally tiny output threshold so the behavior is visible
with a small payload.

**Result:** a terminal block output has an `output_ref`, while the normal
outputs API still returns the complete JSON value.

## 1. Restart with a demonstration threshold

Stop the Level 3 server. Restore its API and encryption variables, then start
it with a one-byte output threshold:

```bash
export ORCH8_EXTERNALIZE_THRESHOLD=1
orch8-server --config orch8.toml
```

This legacy environment override controls block outputs and is convenient for
the exercise. For production, configure the canonical
`externalization_mode` in `orch8.toml` and choose a threshold based on real
payload and database measurements.

## 2. Publish a sequence with a structured output

Create `externalized-sequence.json`:

```json
{
  "id": "019a0000-0000-7000-8000-000000000106",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "externalized-report",
  "version": 1,
  "blocks": [
    {
      "type": "step",
      "id": "build_report",
      "handler": "transform",
      "params": {
        "title": "Quarterly report",
        "summary": "This output is stored outside the hot block-output row."
      }
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

```bash
orch8 sequence apply externalized-sequence.json
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default externalized-report | jq -r '.id')
```

## 3. Run and inspect the hydrated output

```bash
INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo | jq -r '.id')
orch8 instance get "$INSTANCE_ID" --watch

orch8 --output json instance outputs "$INSTANCE_ID" |
  jq '.[] | {block_id, output_ref, output_size, output}'
```

`build_report.output_ref` is non-null, but `output.title` and
`output.summary` are present. Hydration is transparent to this API consumer.

In the current release, downstream `outputs.<block>.*` template resolution
does not reinflate an externalized block output. Keep intermediate producer
outputs below the threshold; externalize terminal, audit, or retrieval-only
outputs. Context-data externalization has a separate dispatch-time hydration
path.

## 4. Restore a realistic configuration

Stop the server and remove the demonstration override:

```bash
unset ORCH8_EXTERNALIZE_THRESHOLD
orch8-server --config orch8.toml
```

The canonical modes are:

- `never` for deliberately small deployments and benchmarks;
- `threshold` for large top-level context fields and outputs; and
- `always_outputs` for consistently heavy output workloads.

Large context values should live under dedicated top-level `context.data`
keys. Externalization evaluates top-level fields rather than walking every
nested leaf.

## Checkpoint

You are done when the instance completes, the output row exposes a non-null
reference, and the hydrated value remains readable through the outputs API.

Use [Externalized State](../../EXTERNALIZATION.md) for compression, cleanup,
atomicity, encryption interactions, and production monitoring.

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| `output_ref` is null | Server did not inherit the threshold override | Stop it, export the variable in the server terminal, and restart |
| A downstream template receives `null` | Intermediate output was externalized | Raise the threshold so intermediate producer outputs remain inline |
| Old encrypted data cannot be read after restart | Encryption key changed | Restore the same Level 3 encryption key |
| Storage keeps growing | Externalized state follows instance retention | Configure instance retention or archive and delete old instances safely |
