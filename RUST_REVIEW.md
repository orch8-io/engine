# Rust Codebase Review — orch8 Engine

**Scope:** `orch8-types`, `orch8-storage`, `orch8-engine`, `orch8-api`, `orch8-grpc`, `orch8-server`, `orch8-cli`  
**Build status:** `cargo check` ✅ | `cargo clippy --all-targets --all-features` ✅ (0 warnings)  
**Date:** 2026-04-21

---

## 1. Security 🔒

| # | Issue | File | Risk |
|---|-------|------|------|
| 1 | **gRPC Plugin SSRF** — unregistered `grpc://` handlers bypass `is_url_safe`, can target internal IPs | `engine/handlers/step_block.rs:308` | SSRF, metadata exfiltration |
| 2 | **WASM Path Traversal** — unregistered `wasm://` handlers fall back to raw filesystem paths | `engine/handlers/step_block.rs:330` | File system leak, traversal |
| 3 | **gRPC auth bypass** — no API-key check, no tenant isolation, no state-transition validation | `grpc/service.rs` | Full auth bypass on gRPC port |
| 4 | **Circuit breaker tenant bypass** — any authenticated caller can reset/read another tenant's breakers | `api/circuit_breakers.rs` | Cross-tenant state mutation |
| 5 | **Webhook replay attacks** — no timestamp/nonce validation on public webhooks | `api/webhooks.rs` | Replay, idempotency violation |
| 6 | **API key length timing leak** — pre-checks `provided.len() == expected_key.len()` before constant-time comparison | `api/auth.rs:87` | Side-channel key length leak |
| 7 | **CORS missing headers** — `x-tenant-id` and `x-trigger-secret` not in `Access-Control-Allow-Headers` | `server/main.rs:440` | Browser preflight rejection |
| 8 | **CLI has no API key auth** — works only against `--insecure` servers | `cli/main.rs` | Operational security gap |
| 9 | **Scaffold config insecure defaults** — generated `orch8.toml` sets `cors_origins = "*"`, `api_key` commented out | `cli/commands/init.rs` | Users run insecure by default |

### Security Fixes (Priority Order)
1. Add `is_url_safe` check before gRPC plugin dispatch.
2. Sanitize WASM plugin names — reject `..`, path separators, leading slashes.
3. Add tonic auth interceptor + tenant enforcement to gRPC surface.
4. Add tenant scope checks to all circuit breaker endpoints.
5. Add webhook replay protection (timestamp + nonce window).
6. Remove length pre-check in auth middleware; let `subtle::ct_eq` handle it.
7. Add `--api-key` / `ORCH8_API_KEY` to CLI.

---

## 2. Stability / Correctness 🛡️

| # | Issue | File | Impact |
|---|-------|------|--------|
| 1 | **`merge_context_data` corrupts encrypted context** — `jsonb_set` replaces encrypted scalar with object | `storage/encrypting.rs:297` | **Irreversible data loss** |
| 2 | **`SignalRow::into_signal` panic** — `.unwrap()` on unknown DB enum value | `storage/postgres/rows.rs:185` | Production panic |
| 3 | **`self_modify` "append" replaces blocks** — docs say append, code replaces | `engine/handlers/param_resolve.rs:143` | Data loss on block injection |
| 4 | **`dispatch_plugin` swallows all errors** — retryable errors become permanent failures silently | `engine/handlers/step_block.rs:62` | Silent data loss, no diagnostics |
| 5 | **`complete_task` drops context-update failures** — `let _ = update_instance_context(...)` | `api/workers.rs:196` | Stale context, incorrect workflow results |
| 6 | **gRPC `retry_instance` broken** — doesn't delete execution tree / sentinel outputs | `grpc/service.rs:284` | Retry deadlocks or re-fails |
| 7 | **SQLite `claim_due` race condition** — `BEGIN DEFERRED` allows double-claiming | `storage/sqlite/instances.rs:83` | Exceeds concurrency limits |
| 8 | **SQLite connection poisoning** — failed COMMIT without ROLLBACK | `storage/sqlite/signals.rs:70`, `workers.rs:51` | Undefined transaction state in pool |
| 9 | **`try_catch` ignores finally failures** — always transitions to Completed | `engine/handlers/try_catch.rs:74` | Silent error swallowing |
| 10 | **`send_signal` TOCTOU** — tenant check and enqueue are separate reads | `engine/handlers/send_signal.rs:47` | Cross-tenant signal delivery |
| 11 | **`tool_call` JSON parse failure → null** — malformed 200 OK body becomes `null` | `engine/handlers/tool_call.rs:114` | Silent contract violation |
| 12 | **Non-transactional `complete_task` / `fail_task`** — 8+ sequential storage calls | `api/workers.rs:133`, `270` | Partial state on crash |
| 13 | **Batch create bypasses empty tenant validation** — `create_instances_batch` lacks `trim().is_empty()` checks | `api/instances/lifecycle.rs:131` | Invalid instances injected |
| 14 | **`migrate_instance` missing target sequence tenant check** — can migrate to another tenant's sequence | `api/sequences.rs:304` | Cross-tenant data mixing |
| 15 | **Circuit breaker persistence lost on shutdown** — `spawn_upsert`/`spawn_delete` use ad-hoc `tokio::spawn` outside engine `JoinSet` | `engine/circuit_breaker.rs:267` | Breaker state not durable |
| 16 | **Sentinel cleanup silently ignored** — `let _ = delete_block_output_by_id(...)` | `engine/scheduler/step_exec.rs:689`, `733` | Unbounded output table growth |
| 17 | **Inject blocks race condition** — read-modify-write without transaction | `api/instances/inject.rs:69` | Concurrent injections lost |
| 18 | **Inconsistent `next_fire_at IS NULL` semantics** — SQLite includes NULLs, Postgres excludes | `storage/sqlite/instances.rs:99`, `postgres/instances.rs:149` | Different claim behavior per backend |
| 19 | **Inconsistent corrupted `InstanceState` handling** — Postgres errors, SQLite defaults to `Scheduled` | `storage/postgres/rows.rs:88`, `sqlite/helpers.rs:54` | SQLite masks corruption |
| 20 | **Dead parameter binding** — `$2` bound but unused in worker claim query | `storage/postgres/workers.rs:124` | Confusing, potential future bug |

---

## 3. Performance ⚡

| # | Issue | File | Impact |
|---|-------|------|--------|
| 1 | **`builtin::handle_sleep` O(n) queries every 250ms** — fetches full execution tree + linear ancestry walk | `engine/handlers/builtin.rs:163` | ~14,400 DB round-trips per 1h sleep |
| 2 | **SSE tight DB-polling loop on errors** — no backoff, immediate `continue` on every error | `api/streaming.rs:90` | DoS under DB pressure |
| 3 | **Unbounded SSE streams** — no limit on concurrent streaming connections | `api/streaming.rs:44` | Tokio task + connection exhaustion |
| 4 | **`poll_tasks` N+1 tenant filter** — loops over claimed tasks calling `get_instance` per task | `api/workers.rs:48` | Up to 1,000 extra DB round-trips |
| 5 | **`is_url_safe` DNS lookup on every call** — no TTL cache | `engine/handlers/builtin.rs:21` | Redundant DNS overhead |
| 6 | **`ExecutionContext::serialized_size` allocates full Vec** — uses `serde_json::to_vec` | `types/context.rs:44` | Wasteful for large contexts |
| 7 | **SQLite batch inserts lack chunking** — single transaction, unlimited params | `storage/sqlite/instances.rs:58` | Hits `SQLITE_MAX_VARIABLE_NUMBER` |
| 8 | **Postgres `bulk_reschedule` f64 precision loss** — `offset_secs: i64` cast to `f64` for `make_interval` | `storage/postgres/instances.rs:672` | Incorrect intervals for huge offsets |
| 9 | **LLM failover no per-provider timeout** — waits full 5-minute global timeout | `engine/handlers/llm.rs:93` | Failover defeated by hung provider |
| 10 | **Rate limit config misnamed** — `rate_limit_rps` implements concurrency limiting, not RPS | `server/main.rs:203` | Misleading config, no real rate limiting |

### Performance Fixes (Quick Wins)
1. Cache `is_inside_cancellation_scope`/`is_inside_finally` at sleep start.
2. Add exponential backoff to SSE error path; cap max concurrent streams.
3. Push tenant filtering into `claim_worker_tasks` storage query.
4. Add TTL cache (e.g. `moka`) for `is_url_safe` results.
5. Chunk SQLite batch inserts (~500 rows/statement).
6. Set per-provider timeout on LLM failover attempts (30s).
7. Replace `ConcurrencyLimitLayer` with real token-bucket rate limiting.

---

## 4. Refactoring / Code Quality 🔧

| # | Issue | File | Suggestion |
|---|-------|------|------------|
| 1 | **`dispatch_plugin` catch-all `Err(_)`** — loses error classification | `engine/handlers/step_block.rs:62` | Match on `StepError` variants, propagate retryability, log with `tracing::error!` |
| 2 | **AB Split uses unstable `DefaultHasher`** — routing changes across Rust upgrades | `engine/handlers/ab_split.rs:118` | Switch to stable hasher (`seahash`, `xxhash_rust`) |
| 3 | **Router `branch_index` is `i16`** — overflows at 32,767 branches | `engine/handlers/router.rs:61` | Use `i32`, or fail permanently on overflow |
| 4 | **`attempt` clamps to `i16::MAX`** — memoization collision after 32,767 retries | `engine/handlers/step.rs:46` | Change schema to `i32`, or fail permanently |
| 5 | **Duplicate tenant-create logic** — `create_instance` re-implements `enforce_tenant_create` | `api/instances/lifecycle.rs:28` | Call the shared helper |
| 6 | **OpenAPI missing endpoints** — SSE, webhooks, triggers, workers list not documented | `api/openapi.rs` | Add missing `#[utoipa::path(...)]` entries |
| 7 | **`get_outputs` all-or-nothing** — one missing externalized ref fails entire request | `api/instances/outputs.rs:43` | Log and continue, return remaining outputs |
| 8 | **CLI cron table wrong JSON key** — looks for `cron_expression`, API returns `cron_expr` | `cli/commands/cron.rs:55` | Fix key name |
| 9 | **Test server spawn race** — `spawn_test_server` doesn't wait for listening | `api/test_harness.rs:69` | Add readiness signal or short sleep |
| 10 | **Dead code / unused parameter** in worker stats | `storage/postgres/workers.rs:252` | Clean up `format!` usage |
| 11 | **Missing `#[must_use]` on key Result functions** | Various | Add `#[must_use]` to storage and API result functions |
| 12 | **Hardcoded `http://` in gRPC plugin** — no TLS support | `engine/handlers/grpc_plugin.rs:88` | Allow `https://` prefix pass-through |
| 13 | **WASM `alloc` return lacks bounds check** | `engine/handlers/wasm_plugin.rs:256` | Validate pointer before cast |
| 14 | **Negative duration deserialization** | `types/lib.rs:72` | Reject negative values, cap overflow |
| 15 | **SQLite no migration system** — runs full `SCHEMA` on every boot | `storage/sqlite/mod.rs:82` | Integrate `sqlx::migrate` or versioned runner |

---

## 5. Recommended Action Plan

### Week 1 — Security & Critical Bugs
1. Fix encrypted context corruption (`storage/encrypting.rs`).
2. Fix `SignalRow::into_signal` panic (`storage/postgres/rows.rs`).
3. Add SSRF protection to gRPC plugin dispatch.
4. Add path sanitization to WASM plugin dispatch.
5. Add auth interceptor to gRPC surface.
6. Add tenant checks to circuit breaker endpoints.
7. Fix `self_modify` append behavior.
8. Fix `complete_task` silent context failure.

### Week 2 — Stability & Race Conditions
9. Fix SQLite `claim_due` race (`BEGIN IMMEDIATE`).
10. Fix SQLite connection poisoning (ROLLBACK on failed COMMIT).
11. Make `complete_task` / `fail_task` atomic (single storage method + transaction).
12. Fix `try_catch` finally failure handling.
13. Fix `send_signal` TOCTOU.
14. Add batch validation for empty tenant/namespace.
15. Fix `migrate_instance` target tenant check.

### Week 3 — Performance
16. Cache cancellation scope / finally check at sleep start.
17. Add SSE backoff + stream limits.
18. Push tenant filter into `claim_worker_tasks`.
19. Add `moka` TTL cache for `is_url_safe`.
20. Chunk SQLite batch inserts.
21. Add per-provider LLM timeout.

### Week 4 — Refactoring & Polish
22. Switch AB Split to stable hasher.
23. Fix router / attempt `i16` overflow.
24. Fix OpenAPI coverage.
25. Fix CLI cron table key.
26. Add API-key support to CLI.
27. Harden scaffold config defaults.
28. Add SQLite migration system.

---

*Generated by automated multi-crate review + `cargo clippy` validation.*
