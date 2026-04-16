# Orch8.io Engine — Rust Issues & Gaps

> Audit date: 2026-04-16
> Cross-referenced STATUS.md claims against actual codebase state.

---

## Critical: Claims Marked Done But Not Actually Implemented

### 1. Encryption at Rest — NOT WIRED
- **Claimed:** Encryption at rest implemented (FieldEncryptor with AES-256-GCM)
- **Reality:** `FieldEncryptor` in `orch8-types/src/encryption.rs` is fully implemented with 5 unit tests — but **never imported or called** anywhere in `orch8-storage`, `orch8-api`, or `orch8-engine`. No storage wrapper encrypts/decrypts before DB writes/reads. No config field wires an encryption key to the storage layer.
- **Fix:** Wire `FieldEncryptor` into storage create/read paths for context and metadata fields. Add `encryption_key` to `EngineConfig`.

### 2. Webhook Transport — Raw TCP Stub
- **Claimed:** Webhook retry with exponential backoff
- **Reality:** Retry logic with exponential backoff IS correct. But the HTTP transport in `orch8-engine/src/webhooks.rs` is a hand-rolled raw `TcpStream` — no TLS, no HTTP/2, no connection pooling, no proper headers. Comment in code: `"For production, replace with reqwest."`
- **Fix:** Replace raw TCP with `reqwest` client. Add TLS support. ~30 min fix.

### 3. Stage 3 Summary Was Inaccurate — FIXED
- **Was:** Summary claimed ~90% (no HITL, cancellation scopes)
- **Reality:** HITL IS implemented in `scheduler.rs:597-696`. Fixed in STATUS.md consolidation.
- **Remaining:** Cancellation scopes are per-step only, not per-subtree.

---

## High: Missing Features That Should Exist

### 4. Zero Engine Unit Tests
- `orch8-engine/src/` has **no `#[cfg(test)]` modules** for scheduler, evaluator, signals, recovery, or webhooks.
- Expression (14 tests), template (10), evaluator (3), circuit breaker (5), cron (3) — these exist but are the ONLY engine tests.
- Core scheduling loop, step execution, delay calculation, crash recovery, parallel/race evaluation — all untested at the Rust level. Only covered by Node.js e2e tests.
- **Impact:** Refactoring the engine is high-risk. Regressions go undetected until e2e.

### 5. No Dockerfile
- No Dockerfile exists anywhere. Only `docker-compose.yml` for Postgres dev container.
- **Impact:** Cannot distribute the engine as a container. Blocks adoption.
- **Fix:** Multi-stage Dockerfile (builder + distroless/alpine runtime). Target < 30MB.

### 6. No Tenant Isolation Middleware
- Tenant filtering is optional and caller-supplied via query params.
- `auth.rs` is a single API key check — no JWT, no tenant extraction, no `X-Tenant-Id` header enforcement.
- Any caller with a valid API key can read/write ANY tenant's data.
- **Impact:** Multi-tenancy claim is hollow without enforcement. Security risk.

### 7. No API Rate Limiting
- Per-resource workflow rate limiting exists (for step execution throttling).
- But there is NO HTTP-level rate limiting middleware on the Axum router.
- No `tower-governor`, no `RateLimitLayer`, nothing.
- **Impact:** API can be DoS'd trivially. Required for production.

### 8. SLA Deadlines Skip Fast Path
- Deadline enforcement in `evaluator.rs:206-207` only runs for instances going through the tree evaluator.
- Step-only sequences (no composite blocks) use the "fast path" in the scheduler and **never check deadlines**.
- **Impact:** Simple sequences (the most common case) silently ignore deadline configuration.

### 9. E2E Tests Not in CI
- 46 Node.js e2e tests exist in `tests/e2e/` but `.github/workflows/ci.yml` does not run them.
- Benchmarks also not in CI.
- **Impact:** Regressions can merge to main undetected.

### 10. SQLite `claim_due` Ignores `max_per_tenant`
- Parameter is `_max_per_tenant` (prefixed with underscore = unused).
- Postgres has the proper `ROW_NUMBER() OVER (PARTITION BY tenant_id)` CTE.
- SQLite just ignores the parameter entirely.
- **Impact:** Noisy-neighbor protection doesn't work on SQLite.

---

## Medium: Incomplete Features

### 11. Cancellation Scopes — Per-Step Only
- `StepDef` has `cancellable: bool` field.
- `cancel_scoped()` in `signals.rs` defers cancel when non-cancellable nodes are active.
- But there's no `CancellationScope` block type for subtree-level non-cancellability.
- Only per-step, not per-subtree (Temporal-style structured concurrency).

### 12. Node.js SDK — Worker Only
- `worker-sdk-node/` is a polling SDK only: poll, claim, heartbeat, complete/fail.
- No methods for: creating sequences, launching instances, querying state, sending signals.
- No tests, no npm publish config.

### 13. Dashboard — No Auth
- `dashboard/` is a minimal React SPA (Vite + TypeScript).
- Settings page stores API key in localStorage — no proper auth flow.
- No tests.

### 14. No Python or Go SDKs
- Both listed as future stages. Zero code exists.

### 15. No Helm Chart
- Listed in Stage 5 as deliverable. Nothing exists.

### 16. No Landing Pages
- exists. under /web

---

## Low: Documentation Inconsistencies

### 17. Stage Completion Percentages Are Stale
| Stage | Claimed | Actual |
|-------|---------|--------|
| Stage 3 | ~90% (no HITL, cancellation scopes) | ~95% (HITL done, only cancellation scopes partial) |
| Stage 4 | ~98% (no SQLite test mode, Grafana) | ~90% (no Dockerfile, no API rate limiting, webhook transport is stub) |
| Stage 5 | ~55% | ~45% (no Docker, no Helm, no landing pages, no Python SDK) |
| Stage 6 | ~60% (8 features implemented) | ~55% (encryption not wired, dashboard has no auth) |

### 18. Missing from Any Stage
These features are NOT listed in any stage but should be:
- API rate limiting middleware
- Proper HTTP client for webhooks (reqwest)
- Engine Rust unit tests
- E2e test CI integration
- Binary distribution (curl install script)
- `orch8 init` scaffolding command
