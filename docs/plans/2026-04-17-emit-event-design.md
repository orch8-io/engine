# Workflow-to-Workflow Coordination — Design

**Date:** 2026-04-17
**Status:** Approved (brainstorming complete, ready for implementation plan)

## Problem

Workflows have no first-class way to coordinate with each other. The `TriggerType::Event` docstring promises an `emit_event` builtin handler, but it was never implemented. Today the only workaround is `http_request` → `POST /triggers/{slug}/fire`, which adds a network hop and loses tenant context.

## Goals

Provide three new builtin step handlers that let one workflow:

1. **Start another workflow** by firing an event trigger.
2. **Send a signal** (any `SignalType`) to a running instance.
3. **Query** another instance's context and state.

All three operate **within a single tenant** (the calling workflow's tenant). Cross-tenant orchestration is explicitly out of scope.

## Non-goals

- Synchronous "call workflow B and await its result" semantics. Use signals for handshakes.
- Cross-tenant orchestration. Future work; will require an explicit allow-list.
- A new block type. These are step handlers, registered in `register_builtins`.

## Handlers

| Handler | Purpose | Params | Returns |
|---|---|---|---|
| `emit_event` | Fire an event trigger → spawn new instance | `{ trigger_slug, data, meta?, dedupe_key? }` | `{ instance_id, sequence_name, deduped? }` |
| `send_signal` | Enqueue any signal to an existing instance | `{ instance_id, signal_type, payload? }` | `{ signal_id }` |
| `query_instance` | Read another instance's context + state | `{ instance_id }` | `{ found, context?, state?, started_at?, completed_at?, current_node? }` |

**File layout:** one file per handler under `orch8-engine/src/handlers/` (`emit_event.rs`, `send_signal.rs`, `query_instance.rs`), mirroring `llm.rs`, `human_review.rs`, etc. Registered in `builtin.rs::register_builtins`.

## Tenant scoping

Inside a running workflow there is no user identity — only the running instance's `tenant_id`. All three handlers:

1. Read the caller's tenant: `storage.get_instance(ctx.instance_id).tenant_id`.
2. Read the target's tenant (from `TriggerDef.tenant_id` for `emit_event`, from `TaskInstance.tenant_id` for the others).
3. Reject mismatch with `StepError::Permanent("cross-tenant <op> denied")`.

`query_instance` errors on cross-tenant rather than returning `{ found: false }` — silently masking would let a workflow probe whether an ID exists in another tenant.

## Data flow

### `emit_event`
1. Read `trigger_slug` from params.
2. `storage.get_trigger(slug)` → `Permanent` if missing or disabled.
3. Tenant check.
4. If `dedupe_key` present, run dedupe protocol (see below). Otherwise, generate a fresh `InstanceId`.
5. Call `orch8_engine::triggers::create_trigger_instance(storage, &trigger, params.data, meta_with_source)` where `meta = { "source": "emit_event", "parent_instance_id": <caller> }`.
6. Return `{ instance_id, sequence_name }` (plus `deduped: true` when the dedupe path returned an existing child).

### `send_signal`
1. Read `instance_id`, `signal_type`, optional `payload`.
2. `storage.get_instance(target)` → `Permanent` if missing.
3. Tenant check.
4. Reject if `target.state.is_terminal()` (mirror `instances.rs:478`).
5. Build `Signal { id: Uuid::new_v4(), instance_id: target, signal_type, payload, delivered: false, created_at: now, … }`.
6. `storage.enqueue_signal(&signal)`.
7. Return `{ signal_id }`.

All `SignalType`s allowed: `Pause`, `Resume`, `Cancel`, `UpdateContext`, `Custom(_)`. Maximum power; workflow author owns the policy.

### `query_instance`
1. Read `instance_id`.
2. `storage.get_instance(target)`.
3. `None` → return `{ "found": false }` (NOT an error — by design).
4. Tenant check (errors, does not mask).
5. Return `{ found: true, context, state, started_at, completed_at, current_node }`.

`current_node` derivation: read latest non-terminal node from the execution tree (or the most recently updated node if all terminal). Implementation detail to confirm during plan write-up.

## Dedupe (`emit_event` only)

**Param:** optional `dedupe_key: string`.

**Scope:** per-parent. Key is `(parent_instance_id, dedupe_key)`. Different parents may reuse the same key without collision.

**New storage:**

```rust
pub enum EmitDedupeOutcome {
    Inserted,
    AlreadyExists(InstanceId),
}

trait StorageBackend {
    async fn record_or_get_emit_dedupe(
        &self,
        parent: InstanceId,
        key: &str,
        candidate_child: InstanceId,
    ) -> Result<EmitDedupeOutcome, StorageError>;
}
```

**New table:**

```sql
CREATE TABLE emit_event_dedupe (
    parent_instance_id  UUID NOT NULL,
    dedupe_key          TEXT NOT NULL,
    child_instance_id   UUID NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_instance_id, dedupe_key)
);
```

Implemented as `INSERT … ON CONFLICT DO NOTHING RETURNING child_instance_id`; if no row returned, `SELECT` the existing row. Atomic per row.

**Handler protocol:**
1. Generate candidate `InstanceId` upfront.
2. Call `record_or_get_emit_dedupe`.
3. `Inserted` → proceed with `create_trigger_instance` using `candidate`.
4. `AlreadyExists(existing_id)` → skip creation, return `{ instance_id: existing_id, deduped: true }`.

**Orphan handling:** If the process dies between dedupe insert and instance creation, the dedupe row points to a non-existent instance. Acceptable — `query_instance` on the orphan returns `{ found: false }`, the dedupe contract still holds (same key never spawns twice), GC sweeps stale rows after TTL.

**GC:** Add `emit_event_dedupe` to the existing TTL sweeper (default 30 days), reusing the pattern from the recent externalization work.

**`create_trigger_instance` change:** Currently generates the InstanceId itself (`triggers.rs:171: id: InstanceId::new()`). Refactor to accept an optional pre-generated ID, or expose a lower-level helper that takes the ID.

## Error handling

`StepError` has two variants: `Retryable { message, details }` and `Permanent { message, details }`.

| Failure | Variant |
|---|---|
| Param validation (missing `trigger_slug`, bad UUID, unknown `signal_type`) | `Permanent` |
| Trigger not found / disabled | `Permanent` |
| Target instance not found (`send_signal`) | `Permanent` |
| Target instance not found (`query_instance`) | `{ found: false }` (not an error) |
| Cross-tenant (any handler) | `Permanent` |
| Target in terminal state (`send_signal`) | `Permanent` |
| `StorageError::Connection`, `PoolExhausted`, `Query` | `Retryable` |
| `StorageError::Serialization` | `Permanent` |
| `enqueue_signal` returns `Conflict` | `Permanent` (we generate fresh UUIDs — implies bug) |

`details` carries structured diagnostic info (e.g., `{ "trigger_slug": "...", "caller_tenant": "T1", "trigger_tenant": "T2" }`).

## Testing

**Per-handler unit tests** (each new file's `#[cfg(test)]` module, using `SqliteStorage` in-memory like `signals.rs` tests):

`emit_event.rs`:
- happy path: trigger exists, same tenant → returns `{ instance_id, sequence_name }`; child confirmed in storage with correct sequence + parent meta.
- trigger not found, trigger disabled, cross-tenant, missing `trigger_slug` → `Permanent`.
- dedupe: first call creates child; second call with same `dedupe_key` returns same `instance_id` and `deduped: true`; only one child in storage.
- dedupe: same key from two different parents → both succeed, two distinct children.

`send_signal.rs`:
- happy path Custom signal → `signal_id` returned; `storage.get_pending_signals(target)` shows it.
- happy path Cancel → enqueued.
- target not found, terminal state, cross-tenant, bad UUID, unknown `signal_type` → `Permanent`.

`query_instance.rs`:
- happy path → returns `{ found: true, context, state, started_at, completed_at, current_node }`.
- target not found → `{ found: false }`.
- cross-tenant → `Permanent` (does not leak existence).
- after instance completes → `state` and `completed_at` reflect terminal state.

**Storage test** (`orch8-storage/tests/storage_integration.rs` style):
- `record_or_get_emit_dedupe`: insert, conflict, concurrent inserts (spawn 10 tasks with same key, exactly one wins, all observe same `child_id`).

**E2E test** (`tests/e2e/emit_event.test.js`):
- Define `producer` and `consumer` sequences; register an event trigger for `consumer`.
- Run `producer` (which contains an `emit_event` step) → assert child reached terminal state.
- `query_instance` returns child's context.
- Run `producer` twice with same `dedupe_key` → assert only one child instance exists.

## Out of scope (future work)

- Cross-tenant orchestration with explicit allow-list.
- A first-class `SubWorkflow` block type with parent/child node linkage and synchronous await.
- Bulk emit (single step that fires N events).
- Scheduled emit (delay before child starts — already covered by trigger's existing scheduling).
