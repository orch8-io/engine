# Externalized State

The scheduler hot path assumes `task_instances.context` is small — small rows mean cheap claims, cheap prefetch, and cheap fan-out to step handlers. When a context field or a block output grows large (e.g. an LLM response, a big document, a scraped page), inlining it balloons every claim cycle.

Externalization solves this by replacing large inline payloads with a short **marker** and stashing the original value in the dedicated `externalized_state` table. Readers recognize the marker and re-hydrate the value only when a step actually needs it.

---

## The envelope

When a payload is externalized, the inline slot is replaced with this exact two-key object:

```json
{ "_externalized": true, "_ref": "<ref_key>" }
```

Anything else with these keys is **not** a marker — the detector checks that the object has exactly two keys, `_externalized: true`, and a string `_ref`.

Ref keys are structured:

- Context data field: `{instance_id}:ctx:data:{field}`
- Block output: `{instance_id}:output:{block_id}:{attempt}`

---

## Modes

Configured via `[engine] externalization_mode` in `orch8.toml` (there is no environment-variable override for this setting).

| Mode | Context data behavior | Block output behavior |
|---|---|---|
| `never` | Always inline | Always inline |
| `threshold { bytes = N }` (default, N=65536) | Externalize fields ≥ N bytes | Externalize outputs ≥ N bytes |
| `always_outputs` | Always inline unless per-field override | **Always externalize**, regardless of size |

TOML examples:

```toml
# Default — 64 KiB cutoff
[engine]
externalization_mode = { type = "threshold", bytes = 65536 }

# Never externalize — tests, benchmarks, small deployments
[engine]
externalization_mode = { type = "never" }

# Dense context, heavy outputs (e.g. LLM workflows)
[engine]
externalization_mode = { type = "always_outputs" }
```

The legacy `externalize_output_threshold` integer field is retained for backwards compatibility but the mode enum is the canonical way to configure this.

---

## What gets externalized

### Context data fields

Only **top-level** keys of `context.data` are eligible. If you stuff a 500 KiB blob at `context.data.report.sections[4].body`, only the entire `context.data.report` subtree is considered — and it's measured as a whole.

This is deliberate: shallow walks are cheap and preserve atomic semantics per top-level key. Structure your context so large values live at top-level keys:

```json
{
  "data": {
    "user_id": "u123",          // small, stays inline
    "large_report": { ... },    // 500 KiB, externalized
    "scraped_html": "..."        // 2 MiB, externalized
  }
}
```

### Block outputs

Each step's return value is a `BlockOutput` row. If the serialized output exceeds the threshold (or `always_outputs` is set), the output JSON is written to `externalized_state` and `block_outputs.output_ref` is populated instead.

The `GET /instances/{id}/outputs` endpoint re-hydrates externalized outputs transparently — API consumers never see the marker envelope.

---

## Compression

Payloads **≥ 1 KiB** are compressed with [zstd](https://facebook.github.io/zstd/) before storage. The `externalized_state` table has a `compression` column so decoders can tell compressed from raw rows. Typical compression ratio for JSON payloads is 3–5×.

Below 1 KiB the compression overhead isn't worth it; payloads are stored raw.

---

## Hydration

Externalized values are inflated in memory only — the persistent row on disk keeps the compact markers:

1. When the scheduler claims a batch of instances, it collects every top-level marker across the whole batch and fetches all payloads from `externalized_state` in a single query. This preload is best-effort: if it fails, markers stay in place and the per-step resolver retries.
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
- `batch_save_externalized_state` — multi-key writes share a single transaction.

Readers see either the pre-write state or the complete post-write state; there is no window where a marker can point to a missing row.

**With encryption at rest enabled**, the encrypting layer must externalize before it encrypts (so each externalized payload is encrypted individually and the residual context stays a JSON object). This splits the write into two ordered steps — instance row first, then externalized rows — so a crash between them can briefly leave a marker whose payload row is missing. The FK ordering guarantees the reverse (a payload row without its instance) cannot occur, and a missing payload surfaces as a resolvable error at read time, not silent data corruption.

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
