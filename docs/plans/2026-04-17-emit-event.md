# emit_event / send_signal / query_instance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add three builtin step handlers (`emit_event`, `send_signal`, `query_instance`) so workflows can start other workflows, signal running instances, and read another instance's context — all within a single tenant.

**Architecture:** Each handler is a new file under `orch8-engine/src/handlers/`, registered in `builtin.rs::register_builtins`. They reuse existing engine plumbing: `create_trigger_instance`, `enqueue_signal`, `get_instance`. `emit_event` adds a per-parent dedupe table guarded by a new `StorageBackend::record_or_get_emit_dedupe` method.

**Tech Stack:** Rust, Tokio, sqlx (Postgres + SQLite), serde_json, existing `StepError` / `StorageError` types.

**Reference:** Design doc at `docs/plans/2026-04-17-emit-event-design.md`.

---

## Task 1: Add `EmitDedupeOutcome` type and trait method (no impls)

**Files:**
- Modify: `orch8-storage/src/lib.rs` (add type + trait method with default `unimplemented!()`)

**Step 1: Add the enum and trait method**

Add to `orch8-storage/src/lib.rs` (near other public types, then in the `StorageBackend` trait):

```rust
/// Outcome of a dedupe insert attempt for `emit_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitDedupeOutcome {
    /// First call for (parent, key) — caller proceeds with `candidate_child`.
    Inserted,
    /// Key already used; caller should reuse the existing child instance ID.
    AlreadyExists(orch8_types::ids::InstanceId),
}
```

Inside `pub trait StorageBackend`:

```rust
/// Record a dedupe key for `emit_event`. If `(parent, key)` already exists,
/// returns the previously-recorded `child_instance_id` without modifying state.
///
/// Atomic per row. Default impl returns `unimplemented` so backends opt in.
async fn record_or_get_emit_dedupe(
    &self,
    parent: orch8_types::ids::InstanceId,
    key: &str,
    candidate_child: orch8_types::ids::InstanceId,
) -> Result<EmitDedupeOutcome, StorageError> {
    let _ = (parent, key, candidate_child);
    Err(StorageError::Query(
        "record_or_get_emit_dedupe not implemented for this backend".into(),
    ))
}
```

**Step 2: Verify compilation**

Run: `cd orch8-storage && cargo check`
Expected: clean build, no new warnings.

**Step 3: Commit**

```bash
git add orch8-storage/src/lib.rs
git commit -m "feat(storage): add EmitDedupeOutcome and record_or_get_emit_dedupe trait method"
```

---

## Task 2: SQL migration for `emit_event_dedupe` table

**Files:**
- Create: `migrations/025_emit_event_dedupe.sql`

**Step 1: Write the migration**

```sql
-- Per-parent dedupe table for the emit_event builtin handler.
-- Rows are GC'd by the externalized-state TTL sweeper (default 30d).
CREATE TABLE IF NOT EXISTS emit_event_dedupe (
    parent_instance_id  UUID         NOT NULL,
    dedupe_key          TEXT         NOT NULL,
    child_instance_id   UUID         NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_instance_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS emit_event_dedupe_created_at_idx
    ON emit_event_dedupe (created_at);
```

If the SQLite migration set lives in a separate dir (check `find . -name "*.sql" -path "*sqlite*"`), add an equivalent file there too — same shape but with `TEXT` instead of `UUID` and `DATETIME` instead of `TIMESTAMPTZ`, matching how existing tables look in the SQLite migration set.

**Step 2: Run the migration locally to verify**

Use whichever harness the repo uses (likely `sqlx migrate run` against a dev Postgres, or the SQLite test setup picks it up automatically).
Expected: migration applies cleanly; `emit_event_dedupe` exists.

**Step 3: Commit**

```bash
git add migrations/025_emit_event_dedupe.sql  # plus sqlite variant if present
git commit -m "feat(storage): add emit_event_dedupe migration"
```

---

## Task 3: Implement `record_or_get_emit_dedupe` for SQLite — happy path test first

**Files:**
- Modify: `orch8-storage/src/sqlite.rs` (find via `grep -rn "impl StorageBackend for SqliteStorage" orch8-storage/src/`)
- Test: same file's `#[cfg(test)]` module, or `orch8-storage/tests/storage_integration.rs`

**Step 1: Write failing test**

```rust
#[tokio::test]
async fn record_or_get_emit_dedupe_first_call_inserts() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let parent = InstanceId::new();
    let candidate = InstanceId::new();

    let outcome = storage
        .record_or_get_emit_dedupe(parent, "k1", candidate)
        .await
        .unwrap();

    assert_eq!(outcome, EmitDedupeOutcome::Inserted);
}
```

**Step 2: Run test to verify it fails**

Run: `cd orch8-storage && cargo test record_or_get_emit_dedupe_first_call_inserts`
Expected: FAIL — default impl returns `Err(...)`.

**Step 3: Implement minimal SQLite version**

In `impl StorageBackend for SqliteStorage`:

```rust
async fn record_or_get_emit_dedupe(
    &self,
    parent: InstanceId,
    key: &str,
    candidate_child: InstanceId,
) -> Result<EmitDedupeOutcome, StorageError> {
    let parent_str = parent.0.to_string();
    let cand_str = candidate_child.0.to_string();

    // Try insert; ignore on conflict.
    let inserted = sqlx::query(
        "INSERT INTO emit_event_dedupe (parent_instance_id, dedupe_key, child_instance_id)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(parent_instance_id, dedupe_key) DO NOTHING",
    )
    .bind(&parent_str)
    .bind(key)
    .bind(&cand_str)
    .execute(&self.pool)
    .await?
    .rows_affected();

    if inserted == 1 {
        return Ok(EmitDedupeOutcome::Inserted);
    }

    // Conflict — fetch the existing child id.
    let row: (String,) = sqlx::query_as(
        "SELECT child_instance_id FROM emit_event_dedupe
         WHERE parent_instance_id = ?1 AND dedupe_key = ?2",
    )
    .bind(&parent_str)
    .bind(key)
    .fetch_one(&self.pool)
    .await?;

    let existing = uuid::Uuid::parse_str(&row.0)
        .map_err(|e| StorageError::Query(format!("invalid uuid in dedupe row: {e}")))?;
    Ok(EmitDedupeOutcome::AlreadyExists(InstanceId(existing)))
}
```

**Step 4: Run test to verify it passes**

Run: `cd orch8-storage && cargo test record_or_get_emit_dedupe_first_call_inserts`
Expected: PASS.

**Step 5: Commit**

```bash
git add orch8-storage/src/sqlite.rs
git commit -m "feat(storage): implement record_or_get_emit_dedupe for SQLite"
```

---

## Task 4: Conflict and concurrency tests for dedupe

**Files:**
- Modify: `orch8-storage/src/sqlite.rs` `#[cfg(test)]` module (or integration tests file)

**Step 1: Write failing conflict test**

```rust
#[tokio::test]
async fn record_or_get_emit_dedupe_second_call_returns_existing() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let parent = InstanceId::new();
    let first = InstanceId::new();
    let second = InstanceId::new();

    let _ = storage.record_or_get_emit_dedupe(parent, "k", first).await.unwrap();
    let outcome = storage.record_or_get_emit_dedupe(parent, "k", second).await.unwrap();

    assert_eq!(outcome, EmitDedupeOutcome::AlreadyExists(first));
}

#[tokio::test]
async fn record_or_get_emit_dedupe_per_parent_isolation() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let p1 = InstanceId::new();
    let p2 = InstanceId::new();
    let c1 = InstanceId::new();
    let c2 = InstanceId::new();

    let o1 = storage.record_or_get_emit_dedupe(p1, "k", c1).await.unwrap();
    let o2 = storage.record_or_get_emit_dedupe(p2, "k", c2).await.unwrap();

    assert_eq!(o1, EmitDedupeOutcome::Inserted);
    assert_eq!(o2, EmitDedupeOutcome::Inserted);
}

#[tokio::test]
async fn record_or_get_emit_dedupe_concurrent_one_winner() {
    let storage = std::sync::Arc::new(SqliteStorage::in_memory().await.unwrap());
    let parent = InstanceId::new();

    let candidates: Vec<_> = (0..10).map(|_| InstanceId::new()).collect();
    let mut handles = Vec::new();
    for cand in candidates.iter().copied() {
        let s = storage.clone();
        handles.push(tokio::spawn(async move {
            s.record_or_get_emit_dedupe(parent, "race", cand).await.unwrap()
        }));
    }

    let mut inserted = 0;
    let mut existing_ids = std::collections::HashSet::new();
    for h in handles {
        match h.await.unwrap() {
            EmitDedupeOutcome::Inserted => inserted += 1,
            EmitDedupeOutcome::AlreadyExists(id) => { existing_ids.insert(id); }
        }
    }
    assert_eq!(inserted, 1, "exactly one task should win the race");
    assert_eq!(existing_ids.len(), 1, "all losers should observe the same winner id");
}
```

**Step 2: Run all three to verify they pass**

Run: `cd orch8-storage && cargo test record_or_get_emit_dedupe`
Expected: 3 PASSED (the first one from Task 3 plus the two new ones, plus the concurrency test).

If the concurrency test flakes on SQLite (single-writer), drop it down to 3 concurrent tasks or mark `#[ignore]` with a TODO to re-enable on Postgres.

**Step 3: Commit**

```bash
git add orch8-storage/src/sqlite.rs
git commit -m "test(storage): conflict + concurrency tests for emit dedupe"
```

---

## Task 5: Implement `record_or_get_emit_dedupe` for Postgres backend

**Files:**
- Modify: `orch8-storage/src/postgres.rs` (find via grep)

**Step 1: Implement using same SQL shape (Postgres dialect)**

```rust
async fn record_or_get_emit_dedupe(
    &self,
    parent: InstanceId,
    key: &str,
    candidate_child: InstanceId,
) -> Result<EmitDedupeOutcome, StorageError> {
    let inserted: Option<(uuid::Uuid,)> = sqlx::query_as(
        "INSERT INTO emit_event_dedupe (parent_instance_id, dedupe_key, child_instance_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (parent_instance_id, dedupe_key) DO NOTHING
         RETURNING child_instance_id",
    )
    .bind(parent.0)
    .bind(key)
    .bind(candidate_child.0)
    .fetch_optional(&self.pool)
    .await?;

    if inserted.is_some() {
        return Ok(EmitDedupeOutcome::Inserted);
    }

    let (existing,): (uuid::Uuid,) = sqlx::query_as(
        "SELECT child_instance_id FROM emit_event_dedupe
         WHERE parent_instance_id = $1 AND dedupe_key = $2",
    )
    .bind(parent.0)
    .bind(key)
    .fetch_one(&self.pool)
    .await?;

    Ok(EmitDedupeOutcome::AlreadyExists(InstanceId(existing)))
}
```

**Step 2: Verify compilation**

Run: `cd orch8-storage && cargo check --features postgres` (or whatever feature flag this repo uses for postgres).
Expected: clean build.

**Step 3: Commit**

```bash
git add orch8-storage/src/postgres.rs
git commit -m "feat(storage): implement record_or_get_emit_dedupe for Postgres"
```

---

## Task 6: Refactor `create_trigger_instance` to accept optional pre-generated InstanceId

**Files:**
- Modify: `orch8-engine/src/triggers.rs:148-...`

**Step 1: Add a new variant that takes `Option<InstanceId>`**

Either:
- (a) Add `id: Option<InstanceId>` parameter to `create_trigger_instance` and update all call sites (cleanest), OR
- (b) Add a new `create_trigger_instance_with_id` and have the original delegate to it with `None`.

Use **(a)**. Find call sites: `grep -rn "create_trigger_instance" orch8-*/src` — should be small. Update them all to pass `None`.

In `triggers.rs` change `id: InstanceId::new()` (line 171) to `id: id.unwrap_or_else(InstanceId::new)`.

Signature becomes:
```rust
pub async fn create_trigger_instance(
    storage: &dyn StorageBackend,
    trigger: &TriggerDef,
    data: serde_json::Value,
    event_meta: serde_json::Value,
    id: Option<InstanceId>,
) -> Result<InstanceId, crate::error::EngineError>
```

**Step 2: Verify build + existing tests**

Run: `cargo build && cargo test -p orch8-engine triggers`
Expected: PASS — no behavioural change for `None` callers.

**Step 3: Commit**

```bash
git add -p  # stage only the relevant files
git commit -m "refactor(engine): allow create_trigger_instance to accept pre-generated InstanceId"
```

---

## Task 7: `query_instance` handler — happy path TDD

**Files:**
- Create: `orch8-engine/src/handlers/query_instance.rs`
- Modify: `orch8-engine/src/handlers/mod.rs` (add `pub mod query_instance;`)

**Step 1: Write failing test**

```rust
// in orch8-engine/src/handlers/query_instance.rs
#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::{
        context::ExecutionContext,
        ids::{InstanceId, Namespace, SequenceId, TenantId},
        instance::{InstanceState, Priority, TaskInstance},
    };

    fn mk_instance(tenant: &str) -> TaskInstance { /* mirror helpers in signals.rs tests */ }

    #[tokio::test]
    async fn query_instance_returns_context_for_same_tenant() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1");
        let target = mk_instance("T1");
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("q".into()),
            params: serde_json::json!({ "instance_id": target.id.0.to_string() }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let storage_ref: &dyn orch8_storage::StorageBackend = &storage;
        let result = handle_query_instance(ctx, storage_ref).await.unwrap();

        assert_eq!(result["found"], serde_json::json!(true));
        assert!(result.get("state").is_some());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p orch8-engine query_instance_returns_context`
Expected: FAIL — file/function doesn't exist.

**Step 3: Implement minimal handler**

```rust
//! `query_instance` builtin: read another instance's context + state.
//!
//! Same-tenant only. Returns `{ found: false }` for missing targets;
//! cross-tenant attempts return `Permanent` (does not leak existence).

use serde_json::{json, Value};
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::{error::StepError, ids::InstanceId};

use super::StepContext;

pub(crate) async fn handle_query_instance(
    ctx: StepContext,
    storage: &dyn StorageBackend,
) -> Result<Value, StepError> {
    let id_str = ctx.params.get("instance_id").and_then(|v| v.as_str())
        .ok_or_else(|| StepError::Permanent {
            message: "missing 'instance_id' string param".into(),
            details: None,
        })?;
    let target_id = InstanceId(Uuid::parse_str(id_str).map_err(|e| StepError::Permanent {
        message: format!("invalid 'instance_id' uuid: {e}"),
        details: None,
    })?);

    // Read caller's tenant.
    let caller = storage.get_instance(ctx.instance_id).await
        .map_err(map_storage_err)?
        .ok_or_else(|| StepError::Permanent {
            message: "caller instance not found".into(),
            details: None,
        })?;

    let target = match storage.get_instance(target_id).await.map_err(map_storage_err)? {
        None => return Ok(json!({ "found": false })),
        Some(t) => t,
    };

    if target.tenant_id != caller.tenant_id {
        return Err(StepError::Permanent {
            message: "cross-tenant query denied".into(),
            details: Some(json!({
                "caller_tenant": caller.tenant_id.0,
                "target_tenant": target.tenant_id.0,
            })),
        });
    }

    Ok(json!({
        "found": true,
        "state": target.state.to_string(),
        "context": target.context,
        "started_at": target.started_at,
        "completed_at": target.completed_at,
        // current_node deliberately omitted in v1 — add later via execution tree query.
    }))
}

fn map_storage_err(e: orch8_types::error::StorageError) -> StepError {
    use orch8_types::error::StorageError::*;
    match e {
        Connection(_) | PoolExhausted | Query(_) => StepError::Retryable {
            message: format!("storage: {e}"),
            details: None,
        },
        _ => StepError::Permanent { message: format!("storage: {e}"), details: None },
    }
}
```

Note: handler signature in registry takes only `StepContext` and returns `Pin<Box<...>>`. We need a way to inject storage. **Check `mod.rs`** — if `StepContext` doesn't carry a storage handle, look at how `human_review` / `tool_call` handlers access storage today. There's likely a global registry or a thread-local. Mirror that pattern. If none exists, add a `storage: Arc<dyn StorageBackend>` field to `StepContext` as a separate prep task before this one.

**Step 4: Run tests until pass**

Run: `cargo test -p orch8-engine query_instance`
Expected: PASS.

**Step 5: Commit**

```bash
git add orch8-engine/src/handlers/mod.rs orch8-engine/src/handlers/query_instance.rs
git commit -m "feat(engine): add query_instance builtin handler — same-tenant read"
```

---

## Task 8: `query_instance` — error path tests

**Files:**
- Modify: `orch8-engine/src/handlers/query_instance.rs` test module

**Step 1: Add tests**

```rust
#[tokio::test]
async fn query_instance_target_missing_returns_found_false() { /* Some caller, target_id = InstanceId::new() never inserted, expect found: false */ }

#[tokio::test]
async fn query_instance_cross_tenant_errors() { /* caller in T1, target in T2, expect Permanent */ }

#[tokio::test]
async fn query_instance_missing_param_errors() { /* params = {} → Permanent */ }

#[tokio::test]
async fn query_instance_bad_uuid_errors() { /* params = { "instance_id": "not-a-uuid" } → Permanent */ }
```

**Step 2: Run all `query_instance` tests**

Run: `cargo test -p orch8-engine query_instance`
Expected: 5 PASSED.

**Step 3: Commit**

```bash
git add orch8-engine/src/handlers/query_instance.rs
git commit -m "test(engine): query_instance error paths (missing, cross-tenant, bad input)"
```

---

## Task 9: `send_signal` handler — happy path TDD

**Files:**
- Create: `orch8-engine/src/handlers/send_signal.rs`
- Modify: `orch8-engine/src/handlers/mod.rs` (add module decl)

**Step 1: Write failing test**

```rust
#[tokio::test]
async fn send_signal_custom_enqueues() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let caller = mk_instance("T1");
    let target = mk_instance("T1");
    storage.create_instance(&caller).await.unwrap();
    storage.create_instance(&target).await.unwrap();

    let ctx = StepContext {
        instance_id: caller.id,
        block_id: BlockId("s".into()),
        params: serde_json::json!({
            "instance_id": target.id.0.to_string(),
            "signal_type": { "custom": "approval_received" },
            "payload": { "approver": "alice" },
        }),
        context: ExecutionContext::default(),
        attempt: 1,
    };

    let result = handle_send_signal(ctx, &storage).await.unwrap();
    assert!(result.get("signal_id").is_some());

    let pending = storage.get_pending_signals(target.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(matches!(pending[0].signal_type, SignalType::Custom(ref n) if n == "approval_received"));
}
```

Note: SignalType serde format for Custom is `{ "custom": "name" }` per `#[serde(rename_all = "snake_case")]` on the enum. Verify with a quick `cargo run` or test if uncertain.

**Step 2: Run test to verify it fails**

Run: `cargo test -p orch8-engine send_signal_custom_enqueues`
Expected: FAIL — handler doesn't exist.

**Step 3: Implement**

```rust
use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::{
    error::StepError,
    ids::InstanceId,
    signal::{Signal, SignalType},
};

use super::StepContext;

pub(crate) async fn handle_send_signal(
    ctx: StepContext,
    storage: &dyn StorageBackend,
) -> Result<Value, StepError> {
    let target_id = parse_instance_id(&ctx.params, "instance_id")?;
    let signal_type: SignalType = serde_json::from_value(
        ctx.params.get("signal_type")
            .ok_or_else(|| permanent("missing 'signal_type'"))?
            .clone(),
    ).map_err(|e| permanent(&format!("invalid signal_type: {e}")))?;
    let payload = ctx.params.get("payload").cloned().unwrap_or(Value::Null);

    let caller = storage.get_instance(ctx.instance_id).await
        .map_err(map_storage_err)?
        .ok_or_else(|| permanent("caller instance not found"))?;
    let target = storage.get_instance(target_id).await
        .map_err(map_storage_err)?
        .ok_or_else(|| permanent("target instance not found"))?;

    if target.tenant_id != caller.tenant_id {
        return Err(StepError::Permanent {
            message: "cross-tenant send_signal denied".into(),
            details: Some(json!({
                "caller_tenant": caller.tenant_id.0,
                "target_tenant": target.tenant_id.0,
            })),
        });
    }

    if target.state.is_terminal() {
        return Err(StepError::Permanent {
            message: format!("cannot send signal to instance in {} state", target.state),
            details: None,
        });
    }

    let signal = Signal {
        id: Uuid::new_v4(),
        instance_id: target_id,
        signal_type,
        payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    storage.enqueue_signal(&signal).await.map_err(map_storage_err)?;

    Ok(json!({ "signal_id": signal.id }))
}

// helper functions (parse_instance_id, permanent, map_storage_err) —
// extract these to a shared `handlers/util.rs` after Task 11 to avoid duplication
// across emit_event/send_signal/query_instance.
```

**Step 4: Run test until pass**

Run: `cargo test -p orch8-engine send_signal_custom_enqueues`
Expected: PASS.

**Step 5: Commit**

```bash
git add orch8-engine/src/handlers/mod.rs orch8-engine/src/handlers/send_signal.rs
git commit -m "feat(engine): add send_signal builtin handler"
```

---

## Task 10: `send_signal` — error path tests

**Files:**
- Modify: `orch8-engine/src/handlers/send_signal.rs` test module

**Step 1: Add tests**

```rust
#[tokio::test] async fn send_signal_target_not_found() { /* Permanent */ }
#[tokio::test] async fn send_signal_cross_tenant_denied() { /* Permanent */ }
#[tokio::test] async fn send_signal_terminal_state_rejected() { /* mark target Completed, expect Permanent */ }
#[tokio::test] async fn send_signal_unknown_signal_type() { /* params.signal_type = "garbage" → Permanent */ }
#[tokio::test] async fn send_signal_cancel_type_works() { /* type "cancel" → enqueued, Cancel variant */ }
```

**Step 2: Run all `send_signal` tests**

Run: `cargo test -p orch8-engine send_signal`
Expected: 6 PASSED.

**Step 3: Commit**

```bash
git add orch8-engine/src/handlers/send_signal.rs
git commit -m "test(engine): send_signal error paths"
```

---

## Task 11: `emit_event` handler — happy path (no dedupe yet)

**Files:**
- Create: `orch8-engine/src/handlers/emit_event.rs`
- Modify: `orch8-engine/src/handlers/mod.rs`

**Step 1: Write failing test**

```rust
#[tokio::test]
async fn emit_event_starts_new_instance() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    // 1. Create a sequence definition for "consumer".
    // 2. Create a TriggerDef with type=Event for "consumer", tenant T1.
    // 3. Create caller instance in tenant T1.
    let caller = /* same-tenant TaskInstance */;
    storage.create_instance(&caller).await.unwrap();

    let ctx = StepContext {
        instance_id: caller.id,
        block_id: BlockId("e".into()),
        params: serde_json::json!({
            "trigger_slug": "on-foo",
            "data": { "msg": "hi" },
        }),
        context: ExecutionContext::default(),
        attempt: 1,
    };
    let result = handle_emit_event(ctx, &storage).await.unwrap();

    let child_id_str = result["instance_id"].as_str().unwrap();
    let child_id = InstanceId(Uuid::parse_str(child_id_str).unwrap());
    let child = storage.get_instance(child_id).await.unwrap().unwrap();
    assert_eq!(child.tenant_id, caller.tenant_id);
    assert_eq!(result["sequence_name"], serde_json::json!("consumer"));
}
```

Use the helpers in `signals.rs::tests` for sequence/instance setup; copy the pattern.

**Step 2: Run test to verify it fails**

Run: `cargo test -p orch8-engine emit_event_starts_new_instance`
Expected: FAIL — handler missing.

**Step 3: Implement (no dedupe path yet)**

```rust
use serde_json::{json, Value};

use orch8_storage::StorageBackend;
use orch8_types::{error::StepError, ids::InstanceId};

use super::StepContext;
use crate::triggers::create_trigger_instance;

pub(crate) async fn handle_emit_event(
    ctx: StepContext,
    storage: &dyn StorageBackend,
) -> Result<Value, StepError> {
    let slug = ctx.params.get("trigger_slug").and_then(|v| v.as_str())
        .ok_or_else(|| permanent("missing 'trigger_slug' string"))?
        .to_string();
    let data = ctx.params.get("data").cloned().unwrap_or(Value::Null);

    let trigger = storage.get_trigger(&slug).await.map_err(map_storage_err)?
        .ok_or_else(|| permanent(&format!("trigger '{slug}' not found")))?;
    if !trigger.enabled {
        return Err(permanent(&format!("trigger '{slug}' is disabled")));
    }

    let caller = storage.get_instance(ctx.instance_id).await
        .map_err(map_storage_err)?
        .ok_or_else(|| permanent("caller instance not found"))?;
    if trigger.tenant_id != caller.tenant_id.0 {
        return Err(StepError::Permanent {
            message: "cross-tenant emit_event denied".into(),
            details: Some(json!({
                "caller_tenant": caller.tenant_id.0,
                "trigger_tenant": trigger.tenant_id,
            })),
        });
    }

    let meta = json!({ "source": "emit_event", "parent_instance_id": ctx.instance_id });
    let child_id = create_trigger_instance(storage, &trigger, data, meta, None).await
        .map_err(|e| StepError::Permanent { message: format!("create_trigger_instance: {e}"), details: None })?;

    Ok(json!({
        "instance_id": child_id,
        "sequence_name": trigger.sequence_name,
    }))
}
```

**Step 4: Run test until pass**

Run: `cargo test -p orch8-engine emit_event_starts_new_instance`
Expected: PASS.

**Step 5: Commit**

```bash
git add orch8-engine/src/handlers/mod.rs orch8-engine/src/handlers/emit_event.rs
git commit -m "feat(engine): add emit_event builtin handler (no dedupe yet)"
```

---

## Task 12: `emit_event` — error path tests

**Files:**
- Modify: `orch8-engine/src/handlers/emit_event.rs` test module

**Step 1: Add tests**

```rust
#[tokio::test] async fn emit_event_trigger_not_found() { /* Permanent */ }
#[tokio::test] async fn emit_event_trigger_disabled() { /* Permanent */ }
#[tokio::test] async fn emit_event_cross_tenant_denied() { /* trigger in T2, caller in T1 → Permanent */ }
#[tokio::test] async fn emit_event_missing_slug_param() { /* Permanent */ }
```

**Step 2: Run all `emit_event` tests**

Run: `cargo test -p orch8-engine emit_event`
Expected: 5 PASSED.

**Step 3: Commit**

```bash
git add orch8-engine/src/handlers/emit_event.rs
git commit -m "test(engine): emit_event error paths"
```

---

## Task 13: `emit_event` — dedupe path

**Files:**
- Modify: `orch8-engine/src/handlers/emit_event.rs`

**Step 1: Write failing dedupe tests**

```rust
#[tokio::test]
async fn emit_event_dedupe_first_call_creates_child() {
    // call emit_event with dedupe_key="k1", expect new child + result.deduped absent or false
}

#[tokio::test]
async fn emit_event_dedupe_second_call_returns_same_child() {
    // call twice with dedupe_key="k1", expect same instance_id, second result has deduped: true
    // assert exactly one child instance exists in storage
}

#[tokio::test]
async fn emit_event_dedupe_per_parent() {
    // two distinct callers use same dedupe_key → two distinct children
}
```

**Step 2: Run to verify failure**

Run: `cargo test -p orch8-engine emit_event_dedupe`
Expected: FAIL — dedupe not yet wired in handler.

**Step 3: Modify handler**

After tenant check, before `create_trigger_instance`:

```rust
let dedupe_key = ctx.params.get("dedupe_key").and_then(|v| v.as_str()).map(|s| s.to_string());

let (child_id, deduped) = if let Some(key) = dedupe_key.as_deref() {
    let candidate = InstanceId::new();
    match storage.record_or_get_emit_dedupe(ctx.instance_id, key, candidate).await
        .map_err(map_storage_err)?
    {
        orch8_storage::EmitDedupeOutcome::Inserted => {
            // Proceed to create with the candidate id.
            let id = create_trigger_instance(storage, &trigger, data, meta, Some(candidate)).await
                .map_err(|e| StepError::Permanent { message: format!("create_trigger_instance: {e}"), details: None })?;
            (id, false)
        }
        orch8_storage::EmitDedupeOutcome::AlreadyExists(existing) => (existing, true),
    }
} else {
    let id = create_trigger_instance(storage, &trigger, data, meta, None).await
        .map_err(|e| StepError::Permanent { message: format!("create_trigger_instance: {e}"), details: None })?;
    (id, false)
};

let mut out = json!({ "instance_id": child_id, "sequence_name": trigger.sequence_name });
if deduped { out["deduped"] = json!(true); }
Ok(out)
```

**Step 4: Run all `emit_event` tests until pass**

Run: `cargo test -p orch8-engine emit_event`
Expected: 8 PASSED.

**Step 5: Commit**

```bash
git add orch8-engine/src/handlers/emit_event.rs
git commit -m "feat(engine): emit_event dedupe via record_or_get_emit_dedupe"
```

---

## Task 14: Register all three handlers in `register_builtins`

**Files:**
- Modify: `orch8-engine/src/handlers/builtin.rs` (add three `registry.register(...)` lines around lines 73-82)

**Step 1: Add registrations**

The handlers need a storage handle. Two options depending on what `StepContext` carries today:
- (a) If StepContext already gives access to storage (check `human_review` / `tool_call` registrations to confirm): wrap in a closure that pulls storage from context.
- (b) If not, the registration must capture an `Arc<dyn StorageBackend>` from the engine's bootstrap. Look at how `register_builtins` is called (`grep -rn "register_builtins" orch8-*/src`) — it likely takes `&mut HandlerRegistry` today; you'll need to extend it to take a storage Arc and bind those into the closures.

Add (assuming option (b)):

```rust
pub fn register_builtins(registry: &mut HandlerRegistry, storage: std::sync::Arc<dyn orch8_storage::StorageBackend>) {
    // ... existing 8 registrations ...
    {
        let s = storage.clone();
        registry.register("emit_event", move |ctx| {
            let s = s.clone();
            async move { super::emit_event::handle_emit_event(ctx, &*s).await }
        });
    }
    {
        let s = storage.clone();
        registry.register("send_signal", move |ctx| {
            let s = s.clone();
            async move { super::send_signal::handle_send_signal(ctx, &*s).await }
        });
    }
    {
        let s = storage.clone();
        registry.register("query_instance", move |ctx| {
            let s = s.clone();
            async move { super::query_instance::handle_query_instance(ctx, &*s).await }
        });
    }
}
```

Update all `register_builtins` call sites to pass storage.

**Step 2: Verify build + full engine tests**

Run: `cargo build && cargo test -p orch8-engine`
Expected: PASS.

**Step 3: Commit**

```bash
git add -p
git commit -m "feat(engine): register emit_event/send_signal/query_instance builtins"
```

---

## Task 15: Add `emit_event_dedupe` to TTL sweeper

**Files:**
- Find via grep: `grep -rn "TTL\|sweep\|gc" orch8-engine/src orch8-storage/src | head -20` — locate the existing externalized-state TTL sweeper added in commit `eccd2c3` / `070530` (the recent externalization work).

**Step 1: Add a method on StorageBackend**

```rust
/// Delete dedupe rows older than `older_than`. Returns rows deleted.
async fn gc_emit_event_dedupe(
    &self,
    older_than: chrono::DateTime<chrono::Utc>,
) -> Result<u64, StorageError> {
    let _ = older_than;
    Ok(0) // default no-op
}
```

Implement for SQLite + Postgres:
```sql
DELETE FROM emit_event_dedupe WHERE created_at < $1
```

**Step 2: Wire into the sweeper**

Find the existing sweeper task and add a call to `gc_emit_event_dedupe` alongside the externalized_state cleanup. Use the same TTL config knob (or add `emit_dedupe_ttl_days` defaulting to 30 if a separate one is desired).

**Step 3: Add test**

```rust
#[tokio::test]
async fn gc_emit_event_dedupe_removes_old_rows() {
    // insert two rows, manually backdate one's created_at, run gc with cutoff between them, assert one remains
}
```

**Step 4: Verify**

Run: `cargo test -p orch8-storage gc_emit_event_dedupe && cargo test -p orch8-engine`
Expected: PASS.

**Step 5: Commit**

```bash
git add -p
git commit -m "feat(storage): GC emit_event_dedupe via TTL sweeper"
```

---

## Task 16: E2E test in `tests/e2e/`

**Files:**
- Create: `tests/e2e/emit_event.test.js`

**Step 1: Write E2E test**

Mirror the structure of `tests/e2e/composites.test.js`. Two sequences:
- `consumer`: one step that returns its input data unchanged (or logs and exits).
- `producer`: one `emit_event` step with `trigger_slug: "on-consumer"` and `data: { msg: "hello" }`.

Test scenarios:
1. Register an event trigger `on-consumer` for the `consumer` sequence.
2. Fire `producer` via webhook trigger.
3. Poll `query_instance` (via API or direct DB) for the child until terminal.
4. Assert child's context contains `{ msg: "hello" }`.
5. Run `producer` twice with `dedupe_key: "x"` → assert only one child exists for that parent.

**Step 2: Run**

Run: `node --test tests/e2e/emit_event.test.js`
Expected: PASS.

**Step 3: Commit**

```bash
git add tests/e2e/emit_event.test.js
git commit -m "test(e2e): emit_event end-to-end with dedupe"
```

---

## Task 17: Update docs

**Files:**
- Modify: `orch8-types/src/trigger.rs` — update `TriggerType::Event` docstring to remove "documented but not implemented" caveat (it now exists).
- Modify: `docs/API.md` — add the three handlers under a "Workflow coordination" section with param schemas and examples.
- Modify: `docs/ARCHITECTURE.md` — add a paragraph on workflow-to-workflow coordination.
- Modify: `CHANGELOG.md` — add entry under the unreleased section.

**Step 1: Edits**

For each file, add concise (3-5 line) entries describing the handlers, their tenant scoping, and the dedupe behaviour.

**Step 2: Verify links / formatting**

Run: `cargo doc --no-deps` (catches docstring breakage). Manually scan `API.md` / `ARCHITECTURE.md` for rendering.

**Step 3: Commit**

```bash
git add -p
git commit -m "docs: emit_event/send_signal/query_instance handlers"
```

---

## Final verification

Run the full suite before pushing:

```bash
cargo build
cargo test
node --test tests/e2e/  # if there's an aggregator
```

Expected: all green.

```bash
git push origin main
```

---

## Notes for the implementer

- **Helpers to extract:** After Task 11, three handlers all use `parse_instance_id`, `permanent(msg)`, and `map_storage_err`. Move them to `orch8-engine/src/handlers/util.rs` or similar. Don't pre-extract — wait until duplication exists.
- **`StepContext` storage access:** Tasks 7/9/11 assume handlers can take `&dyn StorageBackend`. If the registry signature only allows `Fn(StepContext) -> Future`, the storage Arc must be captured via the closure in `register_builtins` (shown in Task 14). Verify how `human_review` and `tool_call` access storage — copy that pattern instead of inventing a new one.
- **Signal type serde:** `SignalType::Custom(String)` serialises as `{ "custom": "name" }` (snake_case enum). The handler's params should accept that exact shape — same as the HTTP API's `send_signal` endpoint (`instances.rs:457`). Test it.
- **Don't add `current_node`:** The design doc lists it as a return field but its derivation (read execution tree, find latest non-terminal node) isn't trivial. Ship v1 without it and add later as a separate change.
