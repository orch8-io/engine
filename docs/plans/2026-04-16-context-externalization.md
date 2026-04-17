# Context Externalization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Keep the scheduler tick path fast (<1ms context read/write) even when instances accumulate large state, by externalizing oversized payloads to a side table with transparent read-side resolution, compression at rest, and bounded retention.

**Architecture:** Add a composable `ExternalizingStorage` wrapper (same pattern as the existing `EncryptingStorage`) that transparently stores top-level `ExecutionContext.data` fields and `BlockOutput.output` payloads exceeding a configured threshold in `externalized_state`, and resolves reference markers on read. Compress payloads with `zstd` level 3. Reclaim orphaned rows via FK cascade + a background sweeper driven by `expires_at`.

**Tech Stack:** Rust, sqlx (Postgres + SQLite), `zstd` crate, axum, tokio, existing `storage::StorageBackend` trait + `EncryptingStorage` decorator pattern.

**Scope:** 4 milestones shippable independently, ordered by risk/value:

| # | Milestone | Value | Risk |
|---|---|---|---|
| M1 | Output read-side resolution (§8.5 fix) | Correctness bug fix | Low |
| M2 | Zstd compression at rest | 3–5x storage reduction, no behavior change | Low |
| M3 | Context externalization wrapper | Removes the 256 KiB ceiling | Medium |
| M4 | GC sweeper + FK cascade | Prevent unbounded row growth | Low |

Stop after any milestone and the engine is still correct. Each milestone ends with a git commit.

---

## Prerequisites

Before starting, read these to understand the existing shape:

| File | Why |
|---|---|
| `docs/CONTEXT_MANAGEMENT.md` | §8.5 documents the exact limitation M1 fixes. §9 documents the size ceiling M3 makes obsolete for `context.data`. |
| `orch8-storage/src/encrypting.rs` | Decorator pattern — copy this exactly. All StorageBackend methods pass through; only the relevant ones are intercepted. |
| `orch8-engine/src/handlers/step.rs:217-257` | `maybe_externalize()` — the existing write side of the externalization marker convention. |
| `orch8-storage/src/lib.rs:435-449` | `save_externalized_state`, `get_externalized_state`, `delete_externalized_state` — existing trait surface. Do not change these signatures. |
| `migrations/008_create_externalized_state.sql` | Existing schema — `ref_key UNIQUE`, FK to `task_instances(id)` without CASCADE, `expires_at` column already present but unused. |
| `orch8-storage/src/sqlite/schema.rs:149-154` | SQLite mirror — no FK. Must be kept in parity with Postgres. |

### Marker convention (already in production)

```json
{"_externalized": true, "_ref": "<ref_key>"}
```

Produced by `maybe_externalize()` for block outputs. M1–M3 treat this as the canonical externalization envelope. Any JSON object with exactly these two keys (and no others) is a reference to resolve. Do not invent a new convention.

### Ref-key namespacing

Current (outputs): `"{instance_id}:{block_id}"`
New (context fields): `"{instance_id}:ctx:{section}:{field_path}"`

The `:ctx:` segment is the discriminator. This prevents collisions if a block were ever named `ctx`. All keys start with the instance UUID so cascade-on-delete works uniformly.

---

## Milestone 1: Output Read-Side Resolution (§8.5 fix)

**Problem:** When `externalize_output_threshold > 0` and a block's output gets externalized, downstream readers of `context.data.<block>.output` see `{"_externalized": true, "_ref": "..."}` instead of the real payload. This is a correctness bug, not a performance concern — currently the engine writes externalized markers but never reads them back.

### Task M1.1: Add a JSON resolver helper

**Files:**
- Create: `orch8-engine/src/externalized.rs`
- Modify: `orch8-engine/src/lib.rs` (add `pub mod externalized;`)
- Test: inline `#[cfg(test)] mod tests` in the new file

**Step 1: Write the failing tests**

```rust
// orch8-engine/src/externalized.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn is_ref_marker_true_for_envelope() {
        let v = json!({"_externalized": true, "_ref": "k"});
        assert!(is_ref_marker(&v));
    }

    #[test]
    fn is_ref_marker_false_for_extra_keys() {
        // Payloads that happen to have _ref but also other keys
        // are NOT markers — they're real data.
        let v = json!({"_externalized": true, "_ref": "k", "other": 1});
        assert!(!is_ref_marker(&v));
    }

    #[test]
    fn is_ref_marker_false_for_non_object() {
        assert!(!is_ref_marker(&json!("string")));
        assert!(!is_ref_marker(&json!(42)));
        assert!(!is_ref_marker(&json!([1, 2])));
    }

    #[test]
    fn extract_ref_key_returns_key() {
        let v = json!({"_externalized": true, "_ref": "inst:block"});
        assert_eq!(extract_ref_key(&v), Some("inst:block"));
    }

    #[test]
    fn extract_ref_key_none_for_non_marker() {
        assert_eq!(extract_ref_key(&json!({"other": 1})), None);
    }
}
```

**Step 2: Run tests, verify they fail**

```bash
cargo test --package orch8-engine externalized::tests
```
Expected: FAIL with `cannot find function is_ref_marker`.

**Step 3: Implement the helpers**

```rust
// orch8-engine/src/externalized.rs
//! Externalization marker helpers.
//!
//! A "marker" is a JSON object of exactly the shape
//! `{"_externalized": true, "_ref": "<key>"}`. Producers write these; the
//! engine inflates them before handlers see the context.

use serde_json::Value;

pub const EXTERNALIZED_FLAG: &str = "_externalized";
pub const REF_KEY: &str = "_ref";

/// Return `true` iff `v` is exactly the externalization envelope — an object
/// with precisely two keys (`_externalized: true`, `_ref: "<string>"`).
///
/// The "exactly two keys" rule prevents false positives on user payloads that
/// happen to include a `_ref` field alongside other data.
#[must_use]
pub fn is_ref_marker(v: &Value) -> bool {
    let Some(obj) = v.as_object() else { return false; };
    if obj.len() != 2 { return false; }
    matches!(obj.get(EXTERNALIZED_FLAG), Some(Value::Bool(true)))
        && matches!(obj.get(REF_KEY), Some(Value::String(_)))
}

/// Return the `_ref` string if `v` is a marker, else `None`.
#[must_use]
pub fn extract_ref_key(v: &Value) -> Option<&str> {
    if !is_ref_marker(v) { return None; }
    v.as_object()?.get(REF_KEY)?.as_str()
}
```

**Step 4: Run tests, verify they pass**

```bash
cargo test --package orch8-engine externalized::tests
```
Expected: PASS, 5 tests.

**Step 5: Commit**

```bash
git add orch8-engine/src/externalized.rs orch8-engine/src/lib.rs
git commit -m "feat(engine): add externalization marker detection helpers"
```

---

### Task M1.2: Resolve markers in `get_outputs` API response

**Files:**
- Modify: `orch8-api/src/instances.rs` (the `get_outputs` handler, around line 503)
- Test: `orch8-storage/tests/storage_integration.rs` (new test at the end)

**Step 1: Write the failing test**

```rust
// orch8-storage/tests/storage_integration.rs
#[tokio::test]
async fn externalized_output_resolves_via_lookup() {
    let (storage, _guard) = fresh_sqlite().await;
    let inst = make_instance();
    storage.create_instance(&inst).await.unwrap();

    // Write an externalized output and its corresponding payload.
    let ref_key = format!("{}:step-a", inst.id);
    storage
        .save_externalized_state(
            inst.id,
            &ref_key,
            &serde_json::json!({"big": "payload"}),
        )
        .await
        .unwrap();

    let marker = serde_json::json!({"_externalized": true, "_ref": ref_key});
    storage
        .save_block_output(&BlockOutput {
            id: Uuid::new_v4(),
            instance_id: inst.id,
            block_id: BlockId("step-a".into()),
            output: marker.clone(),
            output_ref: Some(ref_key.clone()),
            output_size: 100,
            attempt: 0,
            created_at: Utc::now(),
        })
        .await
        .unwrap();

    // Raw storage read still returns the marker.
    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    assert_eq!(outputs[0].output, marker);
}
```

This test exercises the current (unresolved) behaviour — it is a **regression pin**, not a new capability. The resolution happens in the API handler (next task), so the storage-layer output stays a marker on purpose.

**Step 2: Run test, verify it passes immediately**

```bash
cargo test --package orch8-storage --test storage_integration externalized_output_resolves_via_lookup
```
Expected: PASS (this pins current behaviour).

**Step 3: Add the resolution layer in the API handler**

Edit `orch8-api/src/instances.rs` around line 503:

```rust
pub(crate) async fn get_outputs(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let instance = state
        .storage
        .get_instance(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let mut outputs = state
        .storage
        .get_all_outputs(InstanceId(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "outputs"))?;

    // Inflate externalization markers in-place so API consumers see real data.
    for out in &mut outputs {
        if let Some(ref_key) = orch8_engine::externalized::extract_ref_key(&out.output) {
            if let Some(resolved) = state
                .storage
                .get_externalized_state(ref_key)
                .await
                .map_err(|e| ApiError::from_storage(e, "externalized_state"))?
            {
                out.output = resolved;
            }
            // If lookup returns None the marker stays — caller can still see
            // the ref_key and decide how to handle missing payloads.
        }
    }

    Ok(Json(outputs))
}
```

**Step 4: Write the API-level resolution test**

Add to `orch8-storage/tests/storage_integration.rs`:

```rust
#[tokio::test]
async fn externalized_output_lookup_roundtrip() {
    let (storage, _guard) = fresh_sqlite().await;
    let inst = make_instance();
    storage.create_instance(&inst).await.unwrap();

    let ref_key = format!("{}:step-a", inst.id);
    let payload = serde_json::json!({"items": (0..100).collect::<Vec<_>>()});
    storage
        .save_externalized_state(inst.id, &ref_key, &payload)
        .await
        .unwrap();

    let got = storage.get_externalized_state(&ref_key).await.unwrap();
    assert_eq!(got, Some(payload));
}
```

**Step 5: Run all storage tests**

```bash
cargo test --package orch8-storage --test storage_integration
```
Expected: PASS, all tests including 2 new ones.

**Step 6: Add `orch8-engine` as a dep to `orch8-api` if not already**

Check `orch8-api/Cargo.toml` for `orch8-engine = { path = "../orch8-engine" }`. It should already be there — `orch8-api` already uses `orch8_engine::error::EngineError`.

**Step 7: Run full workspace check**

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --lib
```
Expected: clean.

**Step 8: Commit**

```bash
git add orch8-api/src/instances.rs orch8-storage/tests/storage_integration.rs
git commit -m "fix(api): resolve externalization markers in get_outputs response (§8.5)"
```

---

### Task M1.3: Resolve markers when the engine reads context for a handler

**Why this is a separate task:** The API path (M1.2) fixes what the user sees. The engine path fixes what step handlers see when they read `context.data.<prior-block>.output`. These are two independent read sites.

**Files:**
- Modify: `orch8-engine/src/handlers/step_block.rs` (the `context_for_step` helper added earlier)

**Step 1: Write the failing test**

Add to `orch8-engine/src/handlers/step_block.rs`:

```rust
#[cfg(test)]
mod resolve_tests {
    use super::*;
    use orch8_storage::memory::MemoryStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn resolves_externalized_field_in_data_section() {
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
        let inst_id = InstanceId::new();

        let ref_key = format!("{inst_id}:ctx:data:big_blob");
        storage
            .save_externalized_state(
                inst_id,
                &ref_key,
                &serde_json::json!({"real": "payload"}),
            )
            .await
            .unwrap();

        let ctx = ExecutionContext {
            data: serde_json::json!({
                "small_key": "inline",
                "big_blob": {
                    "_externalized": true,
                    "_ref": ref_key,
                }
            }),
            ..Default::default()
        };

        let resolved = resolve_markers(&*storage, inst_id, ctx).await.unwrap();
        assert_eq!(resolved.data["small_key"], "inline");
        assert_eq!(resolved.data["big_blob"], serde_json::json!({"real": "payload"}));
    }
}
```

**Step 2: Run test, verify it fails**

```bash
cargo test --package orch8-engine resolve_tests
```
Expected: FAIL (`cannot find function resolve_markers`).

**Step 3: Implement `resolve_markers`**

Add to `orch8-engine/src/handlers/step_block.rs`:

```rust
use orch8_storage::StorageBackend;
use orch8_types::ids::InstanceId;

/// Walk `context.data` top-level fields and inflate any externalization markers
/// found. Does not recurse into nested objects — only top-level keys can be
/// externalized. See `docs/CONTEXT_MANAGEMENT.md` §8.5.
async fn resolve_markers(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    mut ctx: ExecutionContext,
) -> Result<ExecutionContext, EngineError> {
    let Some(obj) = ctx.data.as_object_mut() else {
        return Ok(ctx);
    };
    for (_, value) in obj.iter_mut() {
        if let Some(ref_key) = crate::externalized::extract_ref_key(value) {
            if let Some(resolved) = storage
                .get_externalized_state(ref_key)
                .await
                .map_err(EngineError::Storage)?
            {
                *value = resolved;
            }
            // Missing payload: leave marker. Downstream code can detect.
        }
    }
    Ok(ctx)
}
```

**Step 4: Wire it into `context_for_step`**

Change `context_for_step` from sync to async, taking `&dyn StorageBackend` and `InstanceId`:

```rust
async fn context_for_step(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    step_def: &StepDef,
) -> Result<ExecutionContext, EngineError> {
    let filtered = match &step_def.context_access {
        Some(access) => instance.context.filtered(access),
        None => instance.context.clone(),
    };
    resolve_markers(storage, instance.id, filtered).await
}
```

Update all 5 call sites in `step_block.rs` and `scheduler.rs` from synchronous `context_for_step(instance, step_def)` to `context_for_step(&*storage, instance, step_def).await?`.

**Step 5: Run tests**

```bash
cargo test --package orch8-engine
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: PASS.

**Step 6: Commit**

```bash
git add orch8-engine/src/handlers/step_block.rs orch8-engine/src/scheduler.rs
git commit -m "fix(engine): resolve externalization markers before step dispatch (§8.5)"
```

---

### Task M1.4: Update docs — mark §8.5 as FIXED

**Files:**
- Modify: `docs/CONTEXT_MANAGEMENT.md`

**Step 1: Edit §8.5**

Change the header from `### 8.5 Externalized outputs not dereferenced` to `### 8.5 Externalized outputs not dereferenced — FIXED in 0.1.x`. Add a closing paragraph describing the resolution (API path + engine path).

**Step 2: Commit**

```bash
git add docs/CONTEXT_MANAGEMENT.md
git commit -m "docs: mark §8.5 (externalized output resolution) as fixed"
```

**End of M1. Ship this on its own if you stop here — the correctness bug is gone.**

---

## Milestone 2: Zstd Compression at Rest

**Problem:** `externalized_state.payload` is raw JSON. JSON compresses 3–5x with zstd at trivial CPU cost (~100 MB/s per core). This is a pure storage win with no behavior change.

### Task M2.1: Add the `zstd` dependency

**Files:**
- Modify: `orch8-storage/Cargo.toml`

**Step 1: Add dep**

```toml
[dependencies]
zstd = "0.13"
```

**Step 2: Run `cargo check`**

```bash
cargo check --package orch8-storage
```
Expected: clean.

**Step 3: Commit**

```bash
git add orch8-storage/Cargo.toml Cargo.lock
git commit -m "chore: add zstd dep for externalized payload compression"
```

---

### Task M2.2: Schema migration — add `compression` + `size_bytes` columns

**Files:**
- Create: `migrations/023_externalized_compression.sql`
- Modify: `orch8-storage/src/sqlite/schema.rs` (DDL string around line 149)

**Step 1: Write the Postgres migration**

```sql
-- migrations/023_externalized_compression.sql
ALTER TABLE externalized_state
    ADD COLUMN IF NOT EXISTS compression TEXT,
    ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS payload_bytes BYTEA;

-- `payload` (JSONB) is used when compression IS NULL.
-- `payload_bytes` (BYTEA) is used when compression = 'zstd'.
-- Legacy rows have compression NULL and payload populated — still work.

CREATE INDEX IF NOT EXISTS idx_externalized_state_instance
    ON externalized_state(instance_id);
```

**Step 2: Mirror the change in SQLite schema**

Edit `orch8-storage/src/sqlite/schema.rs` around line 149:

```rust
// Replace existing CREATE TABLE externalized_state with:
CREATE TABLE IF NOT EXISTS externalized_state (
    ref_key TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    payload TEXT,                    -- raw JSON when compression IS NULL
    payload_bytes BLOB,              -- compressed bytes when compression = 'zstd'
    compression TEXT,                -- NULL or 'zstd'
    size_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_externalized_state_instance
    ON externalized_state(instance_id);
```

**Step 3: Run migration on local dev DB**

```bash
# Assumes the server auto-applies migrations at startup.
# Manual check:
psql "$ORCH8_DATABASE__URL" -f migrations/023_externalized_compression.sql
```

**Step 4: Commit**

```bash
git add migrations/023_externalized_compression.sql orch8-storage/src/sqlite/schema.rs
git commit -m "feat(storage): add compression columns to externalized_state"
```

---

### Task M2.3: Compression helper

**Files:**
- Create: `orch8-storage/src/compression.rs`
- Modify: `orch8-storage/src/lib.rs` (add `pub(crate) mod compression;`)

**Step 1: Write the failing tests**

```rust
// orch8-storage/src/compression.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_preserves_value() {
        let v = json!({"items": (0..50).map(|i| format!("item-{i}")).collect::<Vec<_>>()});
        let compressed = compress(&v).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, v);
    }

    #[test]
    fn compressed_smaller_than_raw_on_repetitive_payload() {
        let v = json!({"blob": "x".repeat(10_000)});
        let raw = serde_json::to_vec(&v).unwrap();
        let compressed = compress(&v).unwrap();
        assert!(compressed.len() < raw.len() / 4,
            "expected >4x compression on repetitive payload, got {}B -> {}B",
            raw.len(), compressed.len());
    }

    #[test]
    fn decompress_rejects_invalid_bytes() {
        assert!(decompress(&[0xff, 0xff, 0xff]).is_err());
    }
}
```

**Step 2: Run, verify fail**

```bash
cargo test --package orch8-storage compression::tests
```
Expected: FAIL (`cannot find function compress`).

**Step 3: Implement**

```rust
// orch8-storage/src/compression.rs
//! zstd compression for externalized state payloads. Level 3 is the default —
//! best ratio/CPU tradeoff per upstream benchmarks, ~100 MB/s per core.

use orch8_types::error::StorageError;

const ZSTD_LEVEL: i32 = 3;

/// Serialize `value` to JSON and compress with zstd level 3.
pub fn compress(value: &serde_json::Value) -> Result<Vec<u8>, StorageError> {
    let json = serde_json::to_vec(value).map_err(StorageError::Serialization)?;
    zstd::encode_all(&json[..], ZSTD_LEVEL)
        .map_err(|e| StorageError::Internal(format!("zstd encode: {e}")))
}

/// Decompress zstd bytes and parse as JSON.
pub fn decompress(bytes: &[u8]) -> Result<serde_json::Value, StorageError> {
    let json = zstd::decode_all(bytes)
        .map_err(|e| StorageError::Internal(format!("zstd decode: {e}")))?;
    serde_json::from_slice(&json).map_err(StorageError::Serialization)
}
```

**Step 4: Run tests**

```bash
cargo test --package orch8-storage compression::tests
```
Expected: PASS, 3 tests.

**Step 5: Commit**

```bash
git add orch8-storage/src/compression.rs orch8-storage/src/lib.rs
git commit -m "feat(storage): add zstd compression helpers for externalized payloads"
```

---

### Task M2.4: Wire compression into Postgres save/get

**Files:**
- Modify: `orch8-storage/src/postgres/externalized.rs`

**Step 1: Decision — compression threshold**

Compress payloads whose raw JSON is ≥ 1 KiB. Below that, compression headers dominate and we'd spend more bytes than we save. This constant lives in `orch8-storage/src/compression.rs`:

```rust
pub const COMPRESSION_THRESHOLD_BYTES: usize = 1024;
```

**Step 2: Rewrite `save`**

```rust
// orch8-storage/src/postgres/externalized.rs
use uuid::Uuid;
use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use super::PostgresStorage;
use crate::compression::{compress, decompress, COMPRESSION_THRESHOLD_BYTES};

pub(super) async fn save(
    store: &PostgresStorage,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
) -> Result<(), StorageError> {
    let raw_size = serde_json::to_vec(payload)
        .map_err(StorageError::Serialization)?
        .len() as i64;

    if (raw_size as usize) >= COMPRESSION_THRESHOLD_BYTES {
        let compressed = compress(payload)?;
        sqlx::query(
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ($1, $2, $3, NULL, $4, 'zstd', $5, NOW())
              ON CONFLICT (ref_key) DO UPDATE
                SET payload = NULL, payload_bytes = $4, compression = 'zstd', size_bytes = $5",
        )
        .bind(Uuid::new_v4())
        .bind(instance_id.0)
        .bind(ref_key)
        .bind(&compressed)
        .bind(raw_size)
        .execute(&store.pool)
        .await?;
    } else {
        sqlx::query(
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ($1, $2, $3, $4, NULL, NULL, $5, NOW())
              ON CONFLICT (ref_key) DO UPDATE
                SET payload = $4, payload_bytes = NULL, compression = NULL, size_bytes = $5",
        )
        .bind(Uuid::new_v4())
        .bind(instance_id.0)
        .bind(ref_key)
        .bind(payload)
        .bind(raw_size)
        .execute(&store.pool)
        .await?;
    }
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    ref_key: &str,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row: Option<(Option<serde_json::Value>, Option<Vec<u8>>, Option<String>)> =
        sqlx::query_as(
            "SELECT payload, payload_bytes, compression
             FROM externalized_state WHERE ref_key = $1",
        )
        .bind(ref_key)
        .fetch_optional(&store.pool)
        .await?;

    let Some((payload, payload_bytes, compression)) = row else {
        return Ok(None);
    };

    match compression.as_deref() {
        Some("zstd") => {
            let bytes = payload_bytes.ok_or_else(|| {
                StorageError::Internal("compression=zstd but payload_bytes is NULL".into())
            })?;
            Ok(Some(decompress(&bytes)?))
        }
        None => Ok(payload),
        Some(other) => Err(StorageError::Internal(format!(
            "unknown compression codec: {other}"
        ))),
    }
}
```

**Step 3: Mirror the same logic in `orch8-storage/src/sqlite/externalized.rs`**

Identical structure, different sqlx placeholders (`?1` not `$1`) and INSERT OR REPLACE not ON CONFLICT. Payload_bytes is BLOB, payload is TEXT JSON.

**Step 4: Add integration test for roundtrip across the threshold**

Add to `orch8-storage/tests/storage_integration.rs`:

```rust
#[tokio::test]
async fn externalized_state_compresses_above_threshold() {
    let (storage, _guard) = fresh_sqlite().await;
    let inst = make_instance();
    storage.create_instance(&inst).await.unwrap();

    // Small payload — not compressed.
    let small = serde_json::json!({"k": "v"});
    storage.save_externalized_state(inst.id, "small", &small).await.unwrap();
    assert_eq!(storage.get_externalized_state("small").await.unwrap(), Some(small));

    // Large payload — compressed.
    let big = serde_json::json!({"blob": "x".repeat(5000)});
    storage.save_externalized_state(inst.id, "big", &big).await.unwrap();
    assert_eq!(storage.get_externalized_state("big").await.unwrap(), Some(big));
}
```

**Step 5: Run and commit**

```bash
cargo test --package orch8-storage
cargo clippy --workspace --all-targets -- -D warnings
git add orch8-storage/src/postgres/externalized.rs \
        orch8-storage/src/sqlite/externalized.rs \
        orch8-storage/tests/storage_integration.rs
git commit -m "feat(storage): compress externalized payloads >= 1 KiB with zstd"
```

**End of M2. Existing externalized outputs now take 3-5x less storage.**

---

## Milestone 3: Context Externalization + Selective Preload (v0.1.0)

**Problem:** `ExecutionContext` travels in full on every scheduler claim. The 256 KiB ceiling (`max_context_bytes`) rejects large writes instead of handling them. Meanwhile, steps rarely need the whole context — they read a handful of fields. We want three things simultaneously:

1. **Externalize large fields** transparently so the hot-path context stays small
2. **Only fetch what the step actually needs** — selective preload via per-step field declarations
3. **Batch** all storage calls so scheduler tick latency does not degrade when many instances are claimed per tick

**Architecture (v0.1.0 scope):**

- **Strategy enum** `ExternalizationMode` — `Never | Threshold{bytes} | AlwaysOutputs`. Default `Threshold{65536}`. `AlwaysAll` deferred to v0.2.0.
- **FieldAccess per step** — extends `ContextAccess` from boolean to `{All, Fields(Vec<String>), None}` per section. Backward compatible via serde (old `bool` still parses).
- **Required Field Tree (RFT)** — per-sequence static map `BlockId -> Vec<top_level_key>` built once at sequence parse and cached in memory. Answers "what fields does this step need?" in O(1).
- **Batch preload** — scheduler claims N instances, then computes the union of required ref-keys across all (instance, next-step) pairs, and fetches them in one batched storage call.
- **Transactional writes** — instance row + externalized rows commit in a single DB transaction. Eliminates dangling-marker risk from the pre-revision design.

**Deferred to v0.2.0 (explicitly out of scope):**
- `AlwaysAll` mode, per-sequence override, sub-path externalization (fine-grained paths like `data.user.profile.avatar`), LRU preload cache, GC sweeper beyond the simple orphan sweep in M4.

### Task M3.1: `ExternalizationMode` enum + config field

**Files:**
- Modify: `orch8-types/src/config.rs` (add enum + `SchedulerConfig.externalization_mode`)
- Modify: `orch8-api/src/lib.rs` (`AppState.externalization_mode`)
- Modify: `orch8-server/src/main.rs` (wire from `config.engine.externalization_mode`)

**Step 1: Write the failing test**

```rust
// orch8-types/src/config.rs — tests module
#[test]
fn externalization_mode_default_is_threshold_64k() {
    let cfg = SchedulerConfig::default();
    assert!(matches!(
        cfg.externalization_mode,
        ExternalizationMode::Threshold { bytes: 65536 }
    ));
}

#[test]
fn externalization_mode_parses_from_toml() {
    // Tagged enum: {type: "threshold", bytes: 32768}
    let mode: ExternalizationMode = toml::from_str(
        r#"type = "threshold"
bytes = 32768"#,
    ).unwrap();
    assert!(matches!(mode, ExternalizationMode::Threshold { bytes: 32768 }));
}
```

**Step 2: Verify fail** — `cargo test --package orch8-types`. Expected: no `ExternalizationMode`.

**Step 3: Implement**

```rust
// orch8-types/src/config.rs
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalizationMode {
    /// Never externalize — store everything inline. Small deployments, tests.
    Never,
    /// Externalize top-level `context.data` keys exceeding `bytes`.
    /// Default mode. 64 KiB default.
    Threshold { bytes: u32 },
    /// Always externalize block outputs regardless of size, but keep
    /// `context.data` inline unless threshold is separately reached.
    /// Useful when outputs dominate state volume.
    AlwaysOutputs,
}

impl Default for ExternalizationMode {
    fn default() -> Self {
        Self::Threshold { bytes: 64 * 1024 }
    }
}

impl ExternalizationMode {
    /// Returns the size threshold for `context.data` fields, or `None` if the
    /// mode does not externalize context data.
    pub fn context_threshold(&self) -> Option<u32> {
        match self {
            Self::Never => None,
            Self::Threshold { bytes } => Some(*bytes),
            Self::AlwaysOutputs => None,
        }
    }

    /// Returns `true` iff block outputs should always be externalized.
    pub fn always_externalize_outputs(&self) -> bool {
        matches!(self, Self::AlwaysOutputs)
    }
}
```

Add to `SchedulerConfig`:

```rust
#[serde(default)]
pub externalization_mode: ExternalizationMode,
```

**Step 4: Wire into `AppState` + scheduler runtime** (no defaulting — flow through from config).

**Step 5: Commit**

```bash
git add orch8-types/src/config.rs orch8-api/src/lib.rs orch8-server/src/main.rs \
        orch8-engine/src/scheduler.rs
git commit -m "feat(config): add ExternalizationMode enum (threshold default 64 KiB)"
```

---

### Task M3.2: Extend `ContextAccess` to `FieldAccess` (per-section granularity)

**Files:**
- Modify: `orch8-types/src/sequence.rs`

**Step 1: Design — backward-compat serde**

Old API:
```json
{"data": true, "config": false, "audit": false, "runtime": true}
```

New API:
```json
{"data": {"fields": ["user_id", "order_id"]}, "config": "all", "audit": "none", "runtime": "all"}
```

Serde must accept both shapes. Approach: untagged enum with `bool` variant mapping to `All`/`None`.

**Step 2: Write failing tests**

```rust
// orch8-types/src/sequence.rs — tests
#[test]
fn field_access_parses_bool_true_as_all() {
    let fa: FieldAccess = serde_json::from_str("true").unwrap();
    assert_eq!(fa, FieldAccess::All);
}

#[test]
fn field_access_parses_bool_false_as_none() {
    let fa: FieldAccess = serde_json::from_str("false").unwrap();
    assert_eq!(fa, FieldAccess::None);
}

#[test]
fn field_access_parses_field_list() {
    let fa: FieldAccess = serde_json::from_str(r#"{"fields": ["a", "b"]}"#).unwrap();
    assert_eq!(fa, FieldAccess::Fields(vec!["a".into(), "b".into()]));
}

#[test]
fn field_access_matches_top_level_key() {
    let fa = FieldAccess::Fields(vec!["user".into()]);
    assert!(fa.allows("user"));
    assert!(!fa.allows("other"));
    assert!(FieldAccess::All.allows("anything"));
    assert!(!FieldAccess::None.allows("anything"));
}
```

**Step 3: Implement**

```rust
// orch8-types/src/sequence.rs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum FieldAccess {
    /// Legacy `true`/`false` accepted via untagged variant.
    Bool(bool),
    /// Explicit field list.
    Fields { fields: Vec<String> },
    /// Explicit `"all"` / `"none"` string.
    Keyword(AccessKeyword),
}

impl FieldAccess {
    pub fn allows(&self, key: &str) -> bool {
        match self {
            Self::Bool(b) => *b,
            Self::Keyword(AccessKeyword::All) => true,
            Self::Keyword(AccessKeyword::None) => false,
            Self::Fields { fields } => fields.iter().any(|f| f == key),
        }
    }

    /// Return the explicit field list, or `None` for All/None access.
    pub fn required_fields(&self) -> Option<&[String]> {
        match self {
            Self::Fields { fields } => Some(fields),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum AccessKeyword { All, None }
```

Extend `ContextAccess` to use `FieldAccess` for the `data` field specifically (other sections stay boolean — they don't benefit from field-level slicing).

**Step 4: Run tests, commit**

```bash
cargo test --package orch8-types sequence::
git add orch8-types/src/sequence.rs
git commit -m "feat(types): FieldAccess enum with backward-compat serde"
```

---

### Task M3.3: `externalize_fields` helper

Identical to the pre-revision task — see the earlier draft. The helper walks top-level `context.data` keys, replaces large values with markers, returns `(ref_key, payload)` pairs. Stays in `orch8-storage/src/externalizing.rs`. Test the same four cases (large-key replacement, skip-existing-marker, non-object no-op, threshold-zero disables).

`threshold` parameter comes from `ExternalizationMode::context_threshold()`.

Commit: `feat(storage): add externalize_fields helper for context data`.

---

### Task M3.4: Required Field Tree — build once per sequence, cache

**Files:**
- Create: `orch8-engine/src/required_fields.rs`
- Modify: `orch8-engine/src/lib.rs` (`pub mod required_fields;`)
- Modify: `orch8-engine/src/scheduler.rs` (cache RFT per sequence)

**Step 1: Tests**

```rust
// orch8-engine/src/required_fields.rs — tests
#[test]
fn rft_collects_fields_from_step_context_access() {
    let seq = sequence_with_step("step-1", field_access_fields(&["user_id", "order_id"]));
    let rft = RequiredFieldTree::from_sequence(&seq);
    assert_eq!(
        rft.fields_for(&BlockId("step-1".into())),
        Some(&vec!["user_id".into(), "order_id".into()][..])
    );
}

#[test]
fn rft_returns_none_for_all_access() {
    // All access means "fetch everything" — not compatible with selective preload.
    // RFT returns None here; scheduler falls back to full fetch for this step.
    let seq = sequence_with_step("step-1", field_access_all());
    let rft = RequiredFieldTree::from_sequence(&seq);
    assert_eq!(rft.fields_for(&BlockId("step-1".into())), None);
}

#[test]
fn rft_empty_for_none_access() {
    // No access means "fetch nothing" — empty vec.
    let seq = sequence_with_step("step-1", field_access_none());
    let rft = RequiredFieldTree::from_sequence(&seq);
    assert_eq!(rft.fields_for(&BlockId("step-1".into())), Some(&vec![][..]));
}
```

**Step 2: Implement**

```rust
// orch8-engine/src/required_fields.rs
use std::collections::HashMap;
use orch8_types::ids::BlockId;
use orch8_types::sequence::{SequenceDefinition, FieldAccess};

/// Per-sequence static map: block -> list of top-level context.data keys
/// that block needs. `None` = fetch everything (All access or no declaration).
/// `Some(empty)` = fetch nothing.
#[derive(Debug, Clone, Default)]
pub struct RequiredFieldTree {
    per_block: HashMap<BlockId, Option<Vec<String>>>,
}

impl RequiredFieldTree {
    pub fn from_sequence(seq: &SequenceDefinition) -> Self {
        let mut per_block = HashMap::new();
        for block in &seq.blocks {
            let fields = block.context_access
                .as_ref()
                .and_then(|ca| ca.data.required_fields())
                .map(|f| f.to_vec());
            per_block.insert(block.id.clone(), fields);
        }
        Self { per_block }
    }

    /// Returns `Some(&fields)` for a declared field list, `None` for All access
    /// or unknown block (caller should fetch everything in those cases).
    pub fn fields_for(&self, block: &BlockId) -> Option<&[String]> {
        self.per_block.get(block)?.as_deref()
    }
}

/// Extract top-level key from a dotted path (`"user.profile.avatar"` -> `"user"`).
#[must_use]
pub fn top_level_key(path: &str) -> &str {
    path.split_once('.').map_or(path, |(head, _)| head)
}
```

**Step 3: Cache per sequence in scheduler**

In `orch8-engine/src/scheduler.rs`, add an `Arc<DashMap<SequenceId, Arc<RequiredFieldTree>>>` field. Populate on first access; invalidate on sequence definition update.

**Step 4: Commit**

```bash
git add orch8-engine/src/required_fields.rs orch8-engine/src/lib.rs \
        orch8-engine/src/scheduler.rs
git commit -m "feat(engine): RequiredFieldTree for selective context preload"
```

---

### Task M3.5: Batch `get_externalized_state` (ref_key = ANY on Postgres, chunked IN on SQLite)

**Files:**
- Modify: `orch8-storage/src/lib.rs` (trait)
- Modify: `orch8-storage/src/postgres/externalized.rs`
- Modify: `orch8-storage/src/sqlite/externalized.rs`
- Modify: `orch8-storage/src/memory.rs`

**Step 1: Trait signature**

```rust
// orch8-storage/src/lib.rs — StorageBackend trait
async fn batch_get_externalized_state(
    &self,
    ref_keys: &[String],
) -> Result<HashMap<String, serde_json::Value>, StorageError>;
```

Default impl in terms of `get_externalized_state` (for `MemoryStorage` and friends):

```rust
async fn batch_get_externalized_state(
    &self,
    ref_keys: &[String],
) -> Result<HashMap<String, serde_json::Value>, StorageError> {
    let mut out = HashMap::with_capacity(ref_keys.len());
    for k in ref_keys {
        if let Some(v) = self.get_externalized_state(k).await? {
            out.insert(k.clone(), v);
        }
    }
    Ok(out)
}
```

**Step 2: Postgres impl**

```rust
pub(super) async fn batch_get(
    store: &PostgresStorage,
    ref_keys: &[String],
) -> Result<HashMap<String, Value>, StorageError> {
    if ref_keys.is_empty() { return Ok(HashMap::new()); }

    let rows: Vec<(String, Option<Value>, Option<Vec<u8>>, Option<String>)> = sqlx::query_as(
        "SELECT ref_key, payload, payload_bytes, compression
           FROM externalized_state
          WHERE ref_key = ANY($1::text[])"
    )
    .bind(ref_keys)
    .fetch_all(&store.pool)
    .await?;

    let mut out = HashMap::with_capacity(rows.len());
    for (k, payload, bytes, comp) in rows {
        let v = match comp.as_deref() {
            Some("zstd") => decompress(&bytes.ok_or_else(|| {
                StorageError::Internal("compression=zstd but payload_bytes NULL".into())
            })?)?,
            None => payload.ok_or_else(|| {
                StorageError::Internal("compression NULL but payload NULL".into())
            })?,
            Some(other) => return Err(StorageError::Internal(format!(
                "unknown compression codec: {other}"
            ))),
        };
        out.insert(k, v);
    }
    Ok(out)
}
```

**Step 3: SQLite impl — chunk IN() at 500**

SQLite caps `SQLITE_MAX_VARIABLE_NUMBER` at 999. Chunk at 500 to leave room for other bound params.

```rust
const SQLITE_BATCH_CHUNK: usize = 500;

pub(super) async fn batch_get(
    store: &SqliteStorage,
    ref_keys: &[String],
) -> Result<HashMap<String, Value>, StorageError> {
    let mut out = HashMap::with_capacity(ref_keys.len());
    for chunk in ref_keys.chunks(SQLITE_BATCH_CHUNK) {
        let placeholders = (0..chunk.len()).map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT ref_key, payload, payload_bytes, compression \
               FROM externalized_state WHERE ref_key IN ({placeholders})"
        );
        let mut q = sqlx::query_as::<_, (String, Option<String>, Option<Vec<u8>>, Option<String>)>(&sql);
        for k in chunk { q = q.bind(k); }
        for (k, payload, bytes, comp) in q.fetch_all(&store.pool).await? {
            let v = decode_payload(payload, bytes, comp)?;
            out.insert(k, v);
        }
    }
    Ok(out)
}
```

**Step 4: Integration test**

```rust
#[tokio::test]
async fn batch_get_externalized_returns_all_requested() {
    let (storage, _guard) = fresh_sqlite().await;
    let inst = make_instance();
    storage.create_instance(&inst).await.unwrap();

    for i in 0..10 {
        let key = format!("{}:k{i}", inst.id);
        let value = serde_json::json!({"i": i});
        storage.save_externalized_state(inst.id, &key, &value).await.unwrap();
    }

    let keys: Vec<String> = (0..10).map(|i| format!("{}:k{i}", inst.id)).collect();
    let got = storage.batch_get_externalized_state(&keys).await.unwrap();
    assert_eq!(got.len(), 10);
    for i in 0..10 {
        assert_eq!(got[&format!("{}:k{i}", inst.id)], serde_json::json!({"i": i}));
    }
}

#[tokio::test]
async fn batch_get_handles_chunk_boundary() {
    // 600 keys — forces two SQLite chunks.
    let (storage, _guard) = fresh_sqlite().await;
    let inst = make_instance();
    storage.create_instance(&inst).await.unwrap();

    let mut keys = Vec::with_capacity(600);
    for i in 0..600 {
        let k = format!("{}:k{i}", inst.id);
        storage.save_externalized_state(inst.id, &k, &serde_json::json!(i)).await.unwrap();
        keys.push(k);
    }
    let got = storage.batch_get_externalized_state(&keys).await.unwrap();
    assert_eq!(got.len(), 600);
}
```

**Step 5: Commit**

```bash
git add orch8-storage/src/lib.rs orch8-storage/src/postgres/externalized.rs \
        orch8-storage/src/sqlite/externalized.rs orch8-storage/src/memory.rs \
        orch8-storage/tests/storage_integration.rs
git commit -m "feat(storage): batch_get_externalized_state (ANY on PG, chunked IN on SQLite)"
```

---

### Task M3.6: Batch `save_externalized_state` (UNNEST on Postgres, tx on SQLite)

**Files:** same storage files as M3.5.

**Step 1: Trait signature**

```rust
async fn batch_save_externalized_state(
    &self,
    instance_id: InstanceId,
    items: &[(String, serde_json::Value)],
) -> Result<(), StorageError>;
```

**Step 2: Postgres — UNNEST multi-row insert**

```sql
INSERT INTO externalized_state
    (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
SELECT gen_random_uuid(), $1, k, p, b, c, s, NOW()
  FROM unnest($2::text[], $3::jsonb[], $4::bytea[], $5::text[], $6::bigint[])
    AS t(k, p, b, c, s)
ON CONFLICT (ref_key) DO UPDATE
   SET payload = EXCLUDED.payload,
       payload_bytes = EXCLUDED.payload_bytes,
       compression = EXCLUDED.compression,
       size_bytes = EXCLUDED.size_bytes;
```

Each item produces either `(payload=jsonb, bytes=NULL, comp=NULL)` or `(payload=NULL, bytes=blob, comp='zstd')` depending on size.

**Step 3: SQLite — transaction-wrapped prepared inserts**

```rust
pub(super) async fn batch_save(
    store: &SqliteStorage,
    instance_id: InstanceId,
    items: &[(String, Value)],
) -> Result<(), StorageError> {
    if items.is_empty() { return Ok(()); }
    let mut tx = store.pool.begin().await?;
    for (k, v) in items {
        save_one(&mut tx, instance_id, k, v).await?;
    }
    tx.commit().await?;
    Ok(())
}
```

**Step 4: Default impl falls back to per-key save** (for MemoryStorage).

**Step 5: Integration tests + commit**

```bash
git commit -m "feat(storage): batch_save_externalized_state (UNNEST on PG, tx on SQLite)"
```

---

### Task M3.7: Transactional `update_instance_context`

**Problem in the pre-revision design:** instance row is updated, then externalized rows are saved separately. If the second call fails, the instance holds dangling markers. Merge into one tx.

**Files:**
- Modify: `orch8-storage/src/postgres/mod.rs` (`update_instance_context`, `merge_instance_context_keys`)
- Modify: `orch8-storage/src/sqlite/mod.rs` (same)

**Step 1: Write failing test — partial-failure behaviour**

Injection: wrap the storage with a failpoint that makes the externalized insert fail. After the attempted write, the instance row must still show the *pre-write* context. Currently (pre-revision) it would show the new context with a dangling marker.

```rust
#[tokio::test]
async fn update_context_with_externalization_is_atomic() {
    // Two keys: one small, one large. Large one externalizes.
    // Inject failure on the externalized insert. Assert instance row unchanged.
    // (Test infrastructure uses a custom wrapper that rejects specific ref_keys.)
}
```

**Step 2: Implement (Postgres)**

```rust
async fn update_instance_context(
    &self,
    id: InstanceId,
    mut context: ExecutionContext,
) -> Result<(), StorageError> {
    let id_str = id.to_string();
    let refs = crate::externalizing::externalize_fields(
        &mut context.data,
        &id_str,
        self.externalization_mode.context_threshold().unwrap_or(0),
    );

    let mut tx = self.pool.begin().await?;
    sqlx::query("UPDATE task_instances SET context = $1, updated_at = NOW() WHERE id = $2")
        .bind(Json(&context))
        .bind(id.0)
        .execute(&mut *tx)
        .await?;

    if !refs.is_empty() {
        batch_save_in_tx(&mut tx, id, &refs).await?;
    }

    tx.commit().await?;
    Ok(())
}
```

**Step 3: SQLite — same shape with `sqlx::Transaction`**

**Step 4: `merge_instance_context_keys` gets the same treatment**

**Step 5: Commit**

```bash
git commit -m "feat(storage): transactional update_instance_context (no dangling markers)"
```

---

### Task M3.8: Transactional `create_instance` + `create_instances_batch`

Same pattern as M3.7: instance insert + externalized inserts in one tx.

`create_instances_batch` (existing endpoint) already takes N instances. For each, compute externalization refs; then issue one instance-batch-insert + one externalized-batch-insert inside a single tx.

Commit: `feat(storage): transactional create_instance(s) with externalization`.

---

### Task M3.9: Scheduler preload step — after claim, before dispatch

**Files:**
- Modify: `orch8-engine/src/scheduler.rs`

**Problem:** Claimed instances carry full contexts including markers. We want the scheduler to, for each `(instance, next_step)` pair:

1. Look up the step's `RequiredFieldTree` entry.
2. If `None` (full access), skip — the full filtered context is used as today.
3. If `Some(fields)`, compute the ref-keys for marker-shaped values at those top-level keys.
4. Union all ref-keys across all claimed-and-dispatched instances in this tick.
5. Call `batch_get_externalized_state` once.
6. Inflate markers in-place.

**Step 1: Write failing test**

```rust
#[tokio::test]
async fn scheduler_preloads_only_requested_fields() {
    // Setup:
    //   - 3 instances, each with externalized fields "needed" and "unused".
    //   - Sequence declares ContextAccess::data::Fields(["needed"]).
    // Expected:
    //   - batch_get_externalized_state called once with 3 ref_keys (the "needed" ones).
    //   - "unused" stays as marker in the dispatched context.
    //   - Handler sees resolved "needed" value.
    // (Uses a recording storage wrapper to verify call count + args.)
}
```

**Step 2: Implement in scheduler tick**

```rust
// orch8-engine/src/scheduler.rs — within claim-then-dispatch
let mut all_refs: Vec<String> = Vec::new();
let mut per_instance_refs: HashMap<InstanceId, Vec<String>> = HashMap::new();

for (instance, next_step) in claimed_pairs.iter() {
    let rft = self.rft_for(&instance.sequence_id);
    let Some(fields) = rft.fields_for(&next_step.id) else { continue; };
    let mut local = Vec::new();
    for key in fields {
        if let Some(v) = instance.context.data.get(key) {
            if let Some(rk) = crate::externalized::extract_ref_key(v) {
                local.push(rk.to_string());
            }
        }
    }
    all_refs.extend(local.iter().cloned());
    per_instance_refs.insert(instance.id, local);
}

let resolved = if all_refs.is_empty() {
    HashMap::new()
} else {
    self.storage.batch_get_externalized_state(&all_refs).await?
};

// Inflate in-place per instance.
for (instance, _) in claimed_pairs.iter_mut() {
    let Some(keys) = per_instance_refs.get(&instance.id) else { continue; };
    let Some(obj) = instance.context.data.as_object_mut() else { continue; };
    for (k, v) in obj.iter_mut() {
        if let Some(rk) = crate::externalized::extract_ref_key(v) {
            if keys.contains(&rk.to_string()) {
                if let Some(payload) = resolved.get(rk) {
                    *v = payload.clone();
                }
            }
        }
        let _ = k; // shut up clippy
    }
}
```

**Step 3: Full-context fallback for `RequiredFieldTree::fields_for` == None**

If any pair returns `None` from RFT, that instance falls back to the M1.3 resolve-all path. Correctness-preserving.

**Step 4: Commit**

```bash
git commit -m "feat(engine): batched selective context preload in scheduler tick"
```

---

### Task M3.10: Metrics + benchmarks

**Files:**
- Modify: `orch8-engine/src/metrics.rs`
- Create: `orch8-engine/benches/preload.rs`

**Step 1: Metrics**

```rust
// orch8-engine/src/metrics.rs
pub static PRELOAD_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "orch8_context_preload_seconds",
        "Time spent in batched context preload per scheduler tick",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]
    ).unwrap()
});

pub static PRELOAD_REFS_PER_TICK: Lazy<Histogram> = /* 0, 1, 5, 10, 50, 100, 500 */;
pub static PRELOAD_BATCH_SIZE: Lazy<Histogram> = /* same buckets */;
```

Record from the scheduler preload step.

**Step 2: Criterion benchmark**

`orch8-engine/benches/preload.rs` — compares:
- Baseline: 100 instances, all fields inline. One scheduler tick.
- With externalization: 100 instances, 2 large fields each, all externalized. One preload batch + one tick.
- Degenerate: same as above, but no RFT (fallback to per-instance resolve).

Target: batched mode ≤ 2x inline; degenerate ≥ 5x worse than batched.

**Step 3: Commit**

```bash
git commit -m "feat(engine): preload metrics + criterion bench for M3"
```

---

### M3 Wrap-up: Docs + Config

- Update `docs/CONTEXT_MANAGEMENT.md`: §9 (two-layer protection), new §10 (ExternalizationMode table + RFT explanation).
- Update `docs/CONFIGURATION.md`: document `ORCH8_ENGINE__EXTERNALIZATION_MODE__TYPE` / `__BYTES` env vars.

Commit: `docs: document ExternalizationMode + selective preload`.

**End of M3. v0.1.0 ships with: large-field externalization, selective preload via RFT, batched storage calls, transactional writes.**

---

## Milestone 4: GC — FK Cascade + Orphan Sweeper

**Problem:** `externalized_state` rows accumulate. Postgres has an FK without CASCADE — deleting an instance leaves orphan externalized rows. SQLite has no FK at all.

### Task M4.1: Add FK CASCADE

**Files:**
- Create: `migrations/024_externalized_cascade.sql`

```sql
ALTER TABLE externalized_state
    DROP CONSTRAINT externalized_state_instance_id_fkey,
    ADD CONSTRAINT externalized_state_instance_id_fkey
        FOREIGN KEY (instance_id) REFERENCES task_instances(id) ON DELETE CASCADE;
```

SQLite has no FK — rely on the sweeper for cleanup. Do not add a FK to SQLite at this stage (the existing schema was shipped without one; adding now requires a table rebuild, out of scope).

Commit.

---

### Task M4.2: Background sweeper task

**Files:**
- Create: `orch8-engine/src/sweeper.rs`
- Modify: `orch8-engine/src/lib.rs` (`pub mod sweeper;`)
- Modify: `orch8-server/src/main.rs` (spawn on startup)

**Step 1: Test**

```rust
#[tokio::test]
async fn sweeper_deletes_orphan_externalized_rows() {
    // An externalized row whose instance was deleted without CASCADE
    // (simulating SQLite) — sweeper must remove it.
    // ... implementation ...
}
```

**Step 2: Implement**

```rust
// orch8-engine/src/sweeper.rs
pub async fn run_orphan_sweep(storage: Arc<dyn StorageBackend>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(e) = storage.sweep_orphan_externalized_state().await {
            tracing::warn!(error = %e, "orphan sweep failed");
        }
    }
}
```

Add `sweep_orphan_externalized_state` to the `StorageBackend` trait. Postgres impl:

```sql
DELETE FROM externalized_state
 WHERE instance_id NOT IN (SELECT id FROM task_instances);
```

Expected runtime: O(orphans). Run every 5 minutes (configurable).

**Step 3: Spawn on startup** in `orch8-server/src/main.rs`:

```rust
tokio::spawn(orch8_engine::sweeper::run_orphan_sweep(
    storage.clone(),
    std::time::Duration::from_secs(300),
));
```

**Step 4: Commit**

```bash
git add orch8-engine/src/sweeper.rs orch8-engine/src/lib.rs \
        orch8-server/src/main.rs orch8-storage/src/lib.rs \
        orch8-storage/src/postgres/mod.rs orch8-storage/src/sqlite/mod.rs
git commit -m "feat(engine): periodic orphan sweep of externalized_state"
```

---

### Task M4.3: Expire-after-retention for TTL-based cleanup

**Files:**
- Modify: `orch8-storage/src/postgres/externalized.rs` (populate `expires_at` on save)

Optional: use the existing unused `expires_at` column. When saving, if `ExecutionConfig.externalized_retention_secs > 0`, set `expires_at = NOW() + retention`. The sweeper deletes rows where `expires_at < NOW()`.

This is out of v1 scope — orphan sweep handles the common case. Document as future work in `docs/CONTEXT_MANAGEMENT.md`.

---

## Review

Self-review against the audit checklist:

### Architecture
- **Decorator pattern?** M3 follows `EncryptingStorage`. ✅
- **Backward compatible?** All new columns have defaults. Legacy uncompressed rows (`compression IS NULL`) keep working. ✅
- **Two backends in parity?** Postgres + SQLite treated equivalently at every task. ⚠️ Exception: no FK on SQLite — documented in M4.1.
- **YAGNI?** GC deferred to M4; deep-nested externalization rejected (top-level only); per-key compression (instead of whole-payload) rejected — unnecessary complexity.

### Risk
- **Marker-in-user-data false positive?** Mitigated by "exactly 2 keys" rule in `is_ref_marker`. Users can still express `{_ref: "x"}` alone (no `_externalized`), just not the exact envelope shape.
- **Compression overhead on small payloads?** 1 KiB threshold ensures we never pay framing cost for trivial values.
- **Data loss on partial write?** Externalized row is saved *after* the instance row. If the instance insert succeeds but the externalized insert fails, the instance holds a dangling marker. Downstream reads degrade gracefully (marker stays visible — caller can detect missing payload). Acceptable for v1. Future: wrap in transaction.
- **Marker resolution latency?** M1.3 adds one storage round-trip per externalized field per tick. For typical workloads (0–2 externalized fields per instance) this is fine. For pathological workloads (many externalized fields), add batch-fetch of markers later.

### Tests
- Every task has failing test written first, then implementation. ✅
- Integration tests at storage layer exercise the roundtrip. ✅
- Unit tests at helper layer cover edge cases (non-object, threshold=0, already-marker). ✅
- **Missing:** concurrency test — what if two scheduler ticks claim and write the same instance simultaneously? Answer: they can't. Instance claim is serialized via `FOR UPDATE SKIP LOCKED`. Covered by existing tests.

### Documentation
- `docs/CONTEXT_MANAGEMENT.md` updated at §8.5 (M1.4) and §9 (M3.4). ✅
- `docs/CONFIGURATION.md` — missing update for `externalize_context_threshold`. Add to M3.1 before commit.

### Shippability
- M1 alone: ships a bug fix. Low risk. 2-3 hours of focused work.
- M2 alone: ships compression. Low risk. 1-2 hours.
- M3 alone: feature. Medium risk. 4-6 hours.
- M4 alone: ops hardening. Low risk. 1-2 hours.

**Recommendation:** Ship M1 immediately (v0.1.1), M2 next, M3 in v0.2.0, M4 right after M3.

### Open questions for the user

1. **Marker collision tolerance:** the "exactly 2 keys" rule is defensive but means a user's payload `{"_externalized": true, "_ref": "x", "other": 1}` stays unresolved. Acceptable?
2. **Retention:** is a 7-day default retention (unused here) reasonable for future M4.3? Or should orphan-only sweep stay forever?
3. **Ship M1 before v0.1.0 or after?** Currently §8.5 is a documented limitation, not a blocker.
