# Externalized State

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

The scheduler hot path assumes `task_instances.context` is small — small rows mean cheap claims, cheap prefetch, and cheap fan-out to step handlers. When a context field or a block output grows large (e.g. an LLM response, a big document, a scraped page), inlining it balloons every claim cycle.

Externalization solves this by replacing large inline payloads with a short **marker** and stashing the original value in the dedicated `externalized_state` table. Readers recognize the marker and re-hydrate the value only when a step actually needs it.

---

## The envelope

New context references use this exact two-key object:

```json
{ "_externalized": "ref:v1", "_ref": "<ref_key>" }
```

Historical top-level context references and block-output references use
`{ "_externalized": true, "_ref": "<ref_key>" }`. Context readers still
resolve those references under the owning instance. Newly supplied context
data that has a reserved shape is wrapped in
`{ "_externalized": "literal:v1", "_value": <original JSON> }` and restored
verbatim for application consumers. This includes a payload that deliberately
uses the same `_ref` string as a real reference.

Ref keys are structured:

- Context data field: `{instance_id}:ctx:v1:{revision}:{field}`
- Context JSON Pointer: `{instance_id}:ctx:pointer:v1:{revision}:{pointer}`
- Block output: `{instance_id}:{block_id}:{output_id}`

Context writes choose a fresh revision ID for newly externalized fields.
Existing markers pass through unchanged. Earlier context snapshots therefore
keep resolving their original values after a replacement commits. Legacy
`{instance_id}:ctx:data:{field}` keys remain readable. Earlier payload rows
retain their existing expiry and remain owned by the instance until cleanup.

---

## Modes

Configured via `[engine] externalization_mode` in `orch8.toml` (there is no environment-variable override for this setting).

| Mode                                         | Context data behavior                   | Block output behavior                      |
| -------------------------------------------- | --------------------------------------- | ------------------------------------------ |
| `never`                                      | Always inline                           | Always inline                              |
| `threshold { bytes = N }` (default, N=65536) | Externalize fields ≥ N bytes            | Externalize outputs ≥ N bytes              |
| `always_outputs`                             | Always inline unless per-field override | **Always externalize**, regardless of size |

Choose one configuration; each example is a separate `orch8.toml` fragment.

```toml
# Default — 64 KiB cutoff
[engine]
externalization_mode = { type = "threshold", bytes = 65536 }
```

```toml
# Never externalize — tests, benchmarks, small deployments
[engine]
externalization_mode = { type = "never" }
```

```toml
# Dense context, heavy outputs (e.g. LLM workflows)
[engine]
externalization_mode = { type = "always_outputs" }
```

The legacy `externalize_output_threshold` integer field is retained for backwards compatibility but the mode enum is the canonical way to configure this.

---

## What gets externalized

### Context data fields

Threshold-based externalization measures whole top-level fields. To keep the
rest of a large document inline, put `x-orch8-externalize` on the sequence's
`input_schema` and list the fields to store separately. For example:

```json
{
  "input_schema": {
    "type": "object",
    "x-orch8-externalize": ["/report/body", "/attachments/0/content"]
  }
}
```

Pointers are relative to `context.data` and support RFC 6901 escapes
(`~0` for `~` and `~1` for `/`). Each pointer must name a distinct slot.
Overlapping parent and child pointers are rejected. Missing paths are skipped.
The scheduler promotes present values before dispatch. The HTTP context update
endpoints also apply the sequence's pointers when persisting a change. Explicit
pointers promote even small values. Initial create requests still honor the
configured request and context size limits.

To edit a sibling without sending the large value back, send
`{"pointer_patch":{"/report/title":"Final"}}` to
`PATCH /instances/{id}/context/data`. This replaces the existing inline slot
and preserves the reference at `/report/body`. To replace the body itself,
target `/report/body` and send its new value. The patch operation can replace a
selected value as a whole. Its descendants cannot be patched individually while
the value is stored separately.

### Block outputs

Each step's return value is a `BlockOutput` row. If the serialized output exceeds the threshold (or `always_outputs` is set), the output JSON is written to `externalized_state` and `block_outputs.output_ref` is populated instead.

The `GET /instances/{id}/outputs` endpoint re-hydrates externalized outputs transparently — API consumers never see the marker envelope.

The current scheduler does not reinflate externalized block outputs when
resolving a downstream `outputs.<block>.*` template. Keep intermediate producer
outputs below the externalization threshold; use output externalization for
terminal, audit, or retrieval-only values. This limitation does not apply to
context fields, which have a separate dispatch-time hydration path.

---

## Compression

Payloads **≥ 1 KiB** are compressed with [zstd](https://facebook.github.io/zstd/) before storage. The `externalized_state` table has a `compression` column so decoders can tell compressed from raw rows. Typical compression ratio for JSON payloads is 3–5×.

Below 1 KiB the compression overhead isn't worth it; payloads are stored raw.

---

## Hydration

Externalized values are inflated in memory only — the persistent row on disk keeps the compact markers:

1. When the scheduler claims a batch of instances, it collects context references across the whole batch and fetches payloads from `externalized_state` in one query. This preload is best-effort. If it fails, the dispatch-time resolver retries when it needs the value.
2. Before a step dispatches, its context snapshot is filtered by the step's `context_access` declaration, and any markers still present in the filtered snapshot are resolved.
3. The hydrated values exist only in the dispatch snapshot; what is written back to `task_instances.context` stays compact.

Steps with a selective `context_access` field list (e.g. `{"data": {"fields": ["user_id"]}}`) never see fields outside their declaration, externalized or not.

---

## Lifecycle and cleanup

Externalized rows are owned by the instance via an `ON DELETE CASCADE` foreign key. When an instance is deleted, all of its externalized payloads are removed automatically.

A background garbage collector additionally sweeps rows whose `expires_at` timestamp (set by the writer, when one is set) has elapsed:

- **Scope:** `externalized_state` rows with an elapsed `expires_at`. Instance-deletion cleanup is not this sweeper's job — the FK cascade handles that.
- **Schedule:** Every 5 minutes by default, deleting at most 1,000 rows per sweep so a large backlog never starves foreground writes.
- **Shutdown:** The sweeper runs inside the engine's task set and stops cleanly on the shutdown signal.

---

## Transactional atomicity

On both concrete backends (Postgres and SQLite), writes that externalize run in a single transaction:

- `create_instance_externalized` — inserts the `task_instances` row **and** all externalized payloads in one transaction. Partial writes are impossible.
- `update_instance_context_externalized` — updates the inline context **and** upserts externalized payloads in one transaction.
- Selected path updates compare the stored context revision before committing the new context and payload rows. Scheduler updates also require a live claim.
- `batch_save_externalized_state` — multi-key writes share a single transaction.

Readers see either the pre-write state or the complete post-write state; there is no window where a marker can point to a missing row.

**With encryption at rest enabled**, the encrypting layer selects fields for externalization before sealing the residual context and each payload. SQLite and PostgreSQL commit the sealed context and all payload rows together on create, batch create, and update. A failed database write rolls back both the context and its payload rows.

---

## Metrics

The GC sweeper exports two Prometheus counters:

- `orch8_gc_externalized_deleted_total` — expired rows deleted.
- `orch8_gc_externalized_errors_total` — sweep failures, labelled by error kind.

To monitor storage growth, instrument your Postgres: `SELECT pg_total_relation_size('externalized_state')`.

---

## Troubleshooting

### "Externalized value not found" errors

Shouldn't happen in normal operation. If you see them:

1. Check Postgres logs for aborted transactions around instance creation.
2. Verify your `externalization_mode` config is consistent across all engine nodes pointing at the same DB.
3. Confirm you have **not** manually deleted rows from `externalized_state`.

### Hot rows are still huge

You're hitting a context shape that holds a large top-level key with many small sub-values. Restructure so large blobs live at dedicated top-level keys — or set `externalization_mode = { type = "always_outputs" }` if the bloat is coming from outputs, not inputs.

### Storage is growing faster than expected

Externalized rows live as long as their instance (the GC only removes rows with an elapsed `expires_at`), so growth tracks instance retention: delete or archive old instances and the cascade reclaims their payloads. Lowering the threshold does not reduce total storage — it moves bytes out of hot `task_instances` rows into `externalized_state`, which is usually the right trade anyway.

---

## See also

- [Configuration — `externalization_mode`](CONFIGURATION.md#engine)
- [Architecture — Database Schema](ARCHITECTURE.md#database-schema)

### Verify an offline database

Run `orch8 verify-externalized --database-url <database> --output json` to
scan a consistent read-only snapshot. Set `ORCH8_ENCRYPTION_KEY` when the
source uses field encryption. The command checks context references and
historical block output references under their owning instances. It reports
missing rows, wrong owners, and unreadable data without printing payload
contents or reference keys. Each reported reference includes a SHA-256
digest. Any issue results in a nonzero exit status. In BYOK deployments the
scan flags vault references for separate verification of the object store.
