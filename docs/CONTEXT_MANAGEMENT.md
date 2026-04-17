# Context Management

> How data flows through an Orch8 execution instance — what is persisted, how it is
> read and mutated, and which invariants hold across backends.

---

## 1. Data Surfaces

Every running instance has **three distinct data surfaces**. Mixing them up is the
single most common source of confusion:

| Surface | Storage | Lifetime | Mutability | Purpose |
|---|---|---|---|---|
| **`ExecutionContext`** | `task_instances.context` (JSON/JSONB) | Instance lifetime | RW by engine + step handlers | Shared state across blocks in an instance |
| **`BlockOutput`** | `block_outputs` table (1 row per block attempt) | Instance lifetime | Append-only per-block | Result of a single step execution |
| **`TaskInstance.metadata`** | `task_instances.metadata` (JSON/JSONB) | Instance lifetime | RW by engine (internal) | Engine-internal bookkeeping (`_injected_blocks`, etc.) |

**Rule of thumb**:
- Steps read from `context` and `outputs.<block_id>` via templates.
- Steps write to `context.data` (via `merge_context_data`) if they need *other* steps
  to see their result under a stable key.
- Everything else a step returns goes to `block_outputs` automatically.

---

## 2. The `ExecutionContext` Model

Defined in `orch8-types/src/context.rs`:

```rust
pub struct ExecutionContext {
    pub data:    serde_json::Value,  // RW — user-space, mutated by step handlers
    pub config:  serde_json::Value,  // RO — set at instance creation, never mutated
    pub audit:   Vec<AuditEntry>,    // Append-only — audit trail
    pub runtime: RuntimeContext,     // Engine-managed — current_step, attempt, etc.
}
```

### Section semantics

| Section | Who writes | When |
|---|---|---|
| `data` | Step handlers (via `merge_context_data`), external `PATCH /instances/{id}/context`, `UpdateContext` signals | Any time during execution |
| `config` | API caller at instance creation | Once, at `POST /instances` |
| `audit` | *Defined but not yet populated* (see §8 Known Limitations) | — |
| `runtime` | Engine | Automatically on each step dispatch |

### Section-level access control

Each `StepDef` may declare `context_access: ContextAccess { data, config, audit, runtime }`
as a set of per-section booleans. When present, the engine calls
`context.filtered(&access)` before handing the context to the step handler, replacing
denied sections with empty defaults.

When `context_access` is `None`, the step receives the full context.

---

## 3. Context Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│ POST /instances                                                     │
│   body.context = { data: {...}, config: {...} }                     │
│   → task_instances row created with full ExecutionContext           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│ SCHEDULER TICK                                                      │
│   claim_due()  → loads TaskInstance (context included)              │
│   evaluator::execute_block(...)                                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
          ┌────────────────────┴───────────────────┐
          │                                        │
          ▼                                        ▼
 ┌─────────────────────┐                ┌───────────────────────┐
 │ FAST PATH           │                │ TREE PATH             │
 │ (scheduler.rs)      │                │ (evaluator + step_    │
 │ Single-step or      │                │  block.rs)            │
 │ linear sequences    │                │ Composite blocks:     │
 │                     │                │ parallel, race, loop, │
 │ Applies             │                │ sub_sequence, etc.    │
 │ context_access      │                │                       │
 │ filter (L.930)      │                │ [see §8.3 limitation] │
 └──────────┬──────────┘                └──────────┬────────────┘
            │                                      │
            └───────────────┬──────────────────────┘
                            ▼
            ┌───────────────────────────────┐
            │ Step handler receives         │
            │   StepContext {               │
            │     params,                   │
            │     context,    ← filtered    │
            │     ...                       │
            │   }                           │
            │                               │
            │ Handler returns               │
            │   BlockOutput { output }      │
            └──────────────┬────────────────┘
                           │
                           ▼
            ┌───────────────────────────────┐
            │ storage.save_block_output()   │
            │                               │
            │ Output is NOT auto-merged     │
            │ into context.data.            │
            │                               │
            │ If the step wants others to   │
            │ see its output by key, it     │
            │ must call                     │
            │   merge_context_data(k, v)    │
            │ itself (or resolve templates  │
            │ via {{outputs.BLOCK_ID.*}}).  │
            └───────────────────────────────┘
```

---

## 4. Reading Context

### Inside a step handler

The runtime constructs a `StepContext` containing the (optionally filtered) context
snapshot. Handlers may read from:

- `ctx.context.data.*` — user-space data
- `ctx.context.config.*` — immutable config
- `ctx.context.runtime.*` — engine state (attempt, current step)

### Template resolution

`{{context.data.user.email}}` and `{{outputs.fetch_user.body.id}}` patterns resolve
against the same snapshot plus the instance's `block_outputs` rows. See
`orch8-engine/src/templates.rs`.

### HTTP / gRPC API

Current context is exposed on `GET /instances/{id}` (full instance row). There is no
dedicated `GET /instances/{id}/context` endpoint — callers pluck `.context` from the
instance body.

---

## 5. Writing Context

There are **four** ways context is mutated:

### 5.1 `merge_context_data(id, key, value)` — preferred

Partial update of a single key under `context.data`. Two different implementations
exist:

| Backend | Implementation | Atomicity |
|---|---|---|
| **Postgres** | `UPDATE ... SET context = jsonb_set(context, '{data,KEY}', $VALUE, true)` | Single statement, atomic |
| **SQLite** | Read-modify-write inside a transaction (SQLite has no `jsonb_set`) | Transactional, serialized |

Called by the engine from:
- `try_catch.rs` → injects `_error` under `context.data._error` before a `catch` branch.

**Intended** to be callable by step handlers too (e.g. to share a computed value under
a stable name). Today, step handlers rarely do this — most pass data via
`block_outputs` and resolve via `{{outputs.*}}` templates.

### 5.2 `update_instance_context(id, ctx)` — full replacement

Overwrites the entire context column. Used by:
- `PATCH /instances/{id}/context` (API)
- `UpdateContext` signal handler

**Warning:** this is a full replacement, not a deep merge. Callers that want to keep
existing data must read-then-write or use `merge_context_data`.

### 5.3 Block output side-effects

`save_block_output` writes to `block_outputs`. It does **not** touch
`task_instances.context`. Cross-block visibility of outputs happens via the
`{{outputs.<block_id>.*}}` template path, not via `context.data`.

### 5.4 Runtime section (engine-internal)

`runtime.current_step`, `runtime.attempt`, `runtime.started_at` are maintained by the
engine as part of state transitions. Step handlers must not write here.

---

## 6. Backend Differences

| Concern | Postgres | SQLite |
|---|---|---|
| Column type | `JSONB` | `TEXT` (serialized JSON) |
| Partial update | `jsonb_set` (atomic) | RMW in transaction |
| Read-modify-write contention | Row-lock via `FOR UPDATE` inside `jsonb_set` | SQLite writer lock (serialized) |
| Null semantics | SQL NULL vs JSON null distinction matters | Entire value is a string, `null` inside JSON text |
| Size limits | Practical limit ~1 GB per value | Practical limit hundreds of MB per row |

**Gotcha — SQL NULL vs JSON `null`:** when a Postgres JSONB path expression
(`metadata->'missing_key'`) returns SQL NULL, sqlx will refuse to decode it as
`serde_json::Value` unless the Rust tuple element is `Option<serde_json::Value>`.
This bit us in `get_injected_blocks` — fixed in `orch8-storage/src/postgres/misc.rs`
by wrapping the tuple element in `Option<_>`.

---

## 7. Cross-Block Context Flow

| Composite | How context flows to children |
|---|---|
| `sequence` (linear) | Same instance, same context row. All steps see mutations from earlier steps. |
| `parallel` | All branches share the same instance context. Concurrent `merge_context_data` calls are serialized at the DB layer but can race at the logical layer — last writer wins per key. |
| `race` | Same as parallel; the losing branches' mutations still land (they are not rolled back). |
| `loop` / `for_each` | Each iteration uses the same context row. Use block outputs + index templates (`{{outputs.step.iteration[i]}}`) if you need per-iteration isolation. |
| `try_catch` | `context.data._error` is injected by `merge_context_data` before `catch` activates. |
| `sub_sequence` | **Child gets a fresh context built from `SubSequenceDef.input`.** Parent's `config` and `audit` are NOT inherited. See §8.4. |

---

## 8. Known Limitations

### 8.1 Audit section is defined but not populated

`ExecutionContext.audit: Vec<AuditEntry>` is serialized and round-tripped, but no
engine code appends to it today. The dedicated `audit_log` table captures most audit
needs. The `audit` section is reserved for a future per-instance inline audit feature.

### 8.2 Block output is not auto-merged into `context.data`

Some users expect `output = {"x": 1}` from a step to appear at `context.data.x`
automatically. It does not. Cross-block access goes through `{{outputs.BLOCK_ID.x}}`.
If you want a stable key in `context.data`, the step must call `merge_context_data`
explicitly.

### 8.3 `context_access` filter is not enforced in the tree path

**Severity: Medium (security-relevant for composite blocks).**

- **Fast path** (`scheduler.rs:930-933`): applies `instance.context.filtered(access)`.
- **Tree path** (`orch8-engine/src/handlers/step_block.rs:76, 96, 115, 135`): passes
  `instance.context.clone()` **without** the filter.

Any step nested inside a composite (parallel, race, loop, for_each, sub_sequence,
try_catch) therefore sees the full context regardless of its `context_access`
declaration. Plug-in handlers (ActivePieces sidecar, gRPC plugin, WASM plugin) are
affected equally.

Tracked for v0.2. A regression test exists in
`orch8-storage/tests/storage_integration.rs::context_access_filter_enforced_on_tree_path`
and is currently **marked ignored** until the bug is fixed.

### 8.4 `sub_sequence` does not inherit parent config

When a parent block spawns a child sub-sequence, the child instance's context is
built from `SubSequenceDef.input`; the parent's `config` and `audit` sections are
dropped. Workflows that rely on shared `config` between parent and child must re-pass
it through `input`.

### 8.5 Externalized large outputs are not auto-dereferenced — FIXED in 0.1.x

When a block output exceeds `externalize_threshold` bytes, it is stored externally
and `block_outputs.output` contains a reference of the form
`{"_externalized": true, "_ref": "<key>"}`.

**Resolution:** the marker is now transparently inflated at every read site a
consumer can observe:

- **API path (`GET /instances/{id}/outputs`):** `orch8-api/src/instances.rs`
  scans each output and calls `storage.get_externalized_state(ref_key)` when a
  marker is detected, replacing the envelope with the real payload before the
  JSON response is serialized.
- **Engine dispatch path:** `orch8-engine/src/handlers/step_block.rs::context_for_step`
  walks the filtered `ExecutionContext.data` object and inflates any top-level
  marker before handing the context to either an in-process handler or an
  external worker. Both the in-process fast path (`scheduler.rs`) and the
  external-worker dispatch path share this helper, so every handler sees a
  fully resolved context.

Broken references (payload missing from `externalized_state`) are left in place
so callers can detect the dangling pointer. Template resolution
(`{{outputs.X.field}}`) builds on the dispatch-path context, so it also sees
inflated values — no separate fix needed there.

Only top-level `context.data` fields are scanned; nested objects are not
recursed into, consistent with the write side in `handlers/step.rs::maybe_externalize`.

---

## 9. Operational Notes

- **Context size**: the full context travels on every scheduler claim. Keep it under
  a few hundred KB for healthy tick latency. Anything bigger belongs in external
  storage with a pointer in `context.data`.
- **Concurrent writes**: `merge_context_data` is per-key atomic. Cross-key
  invariants (e.g. "`a + b` always stays <= 100") are NOT enforced — use a single
  key holding a structured value instead.
- **Backups**: instance context is included in standard DB backups. No separate
  handling needed.

---

## 10. Related Code

| Path | Role |
|---|---|
| `orch8-types/src/context.rs` | `ExecutionContext`, `RuntimeContext`, `AuditEntry`, `filtered()` |
| `orch8-types/src/sequence.rs` | `ContextAccess` declaration on `StepDef` |
| `orch8-storage/src/lib.rs` | `StorageBackend` trait: `update_instance_context`, `merge_context_data` |
| `orch8-storage/src/postgres/instances.rs` | Postgres implementations |
| `orch8-storage/src/sqlite/instances.rs` | SQLite implementations (RMW pattern) |
| `orch8-engine/src/scheduler.rs:930` | Fast-path context filtering |
| `orch8-engine/src/handlers/step_block.rs` | Tree-path dispatch (missing filter — see §8.3) |
| `orch8-engine/src/handlers/try_catch.rs` | `_error` injection via `merge_context_data` |
| `orch8-engine/src/templates.rs` | `{{context.*}}` and `{{outputs.*}}` resolution |
| `orch8-api/src/instances.rs` | `PATCH /instances/{id}/context` endpoint |
