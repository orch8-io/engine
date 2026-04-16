# Orch8.io Engine — Rust Issues & Gaps

> Audit date: 2026-04-16
> Cross-referenced STATUS.md claims against actual codebase state.

---

## Critical: Claims Marked Done But Not Actually Implemented

### 1. ~~Encryption at Rest — NOT WIRED~~ FIXED
- **Fixed:** `EncryptingStorage` decorator wraps `StorageBackend`, transparently encrypts/decrypts `context.data` using AES-256-GCM. Wired in `main.rs` via `encryption_key` config or `ORCH8_ENCRYPTION_KEY` env var.

### 2. ~~Webhook Transport — Raw TCP Stub~~ FIXED
- **Fixed:** Replaced raw `TcpStream` with `reqwest` HTTP client. TLS, connection pooling, proper headers. Added unit tests for backoff calculation and event serialization.

### 3. Stage 3 Summary Was Inaccurate — FIXED
- **Was:** Summary claimed ~90% (no HITL, cancellation scopes)
- **Reality:** HITL IS implemented in `scheduler.rs:597-696`. Fixed in STATUS.md consolidation.
- **Remaining:** Cancellation scopes are per-step only, not per-subtree.

---

## High: Missing Features That Should Exist

### 4. ~~Zero Engine Unit Tests~~ PARTIALLY FIXED
- **Fixed:** Added unit tests for webhooks (backoff, event serialization, emit skip) and scheduler (prefetch map, drain). Core scheduling loop, step execution, crash recovery remain untested at the Rust level.

### 5. No Dockerfile
- No Dockerfile exists anywhere. Only `docker-compose.yml` for Postgres dev container.
- **Impact:** Cannot distribute the engine as a container. Blocks adoption.
- **Fix:** Multi-stage Dockerfile (builder + distroless/alpine runtime). Target < 30MB.

### 6. ~~No Tenant Isolation Middleware~~ FIXED
- **Fixed:** Added `TenantContext` extraction from `X-Tenant-Id` header. Tenant middleware enforces header when `require_tenant_header=true`. Instance handlers reject cross-tenant reads/writes with 403/404.

### 7. ~~No API Rate Limiting~~ FIXED
- **Fixed:** Added `ConcurrencyLimitLayer` on Axum router when `rate_limit_rps > 0`. Configurable via `ORCH8_RATE_LIMIT_RPS` env var. Returns 503 at capacity.

### 8. ~~SLA Deadlines Skip Fast Path~~ FIXED
- **Fixed:** Fast path in scheduler now calls `check_sla_deadlines` for step-only sequences.

### 9. E2E Tests Not in CI
- 46 Node.js e2e tests exist in `tests/e2e/` but `.github/workflows/ci.yml` does not run them.
- Benchmarks also not in CI.
- **Impact:** Regressions can merge to main undetected.

### 10. ~~SQLite `claim_due` Ignores `max_per_tenant`~~ FIXED
- **Fixed:** SQLite `claim_due` now enforces `max_per_tenant` with per-tenant counting logic matching Postgres behavior.

---

## Medium: Incomplete Features

### 11. ~~Cancellation Scopes — Per-Step Only~~ FIXED
- **Fixed:** Added `CancellationScope` block type (`CancellationScopeDef` struct, `BlockDefinition::CancellationScope` variant, `BlockType::CancellationScope`). Sequential child execution handler. `cancel_scoped()` now walks ancestry to detect nodes inside a scope and treats them as non-cancellable.

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

### 17. ~~Stage Completion Percentages Are Stale~~ UPDATED
- Percentages updated in STATUS.md (2026-04-16). Stage 4 now complete, Stage 5 ~75%, Stage 6 ~70%.

### 18. Missing from Any Stage
These features are NOT listed in any stage but should be:
- ~~API rate limiting middleware~~ DONE
- ~~Proper HTTP client for webhooks (reqwest)~~ DONE
- ~~Engine Rust unit tests~~ PARTIALLY DONE
- E2E test CI integration
- Binary distribution (curl install script)
- `orch8 init` scaffolding command
