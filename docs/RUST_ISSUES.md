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
- **Fixed:** Added unit tests for webhooks (backoff, event serialization, emit skip) and scheduler (prefetch map, drain). Added 47 unit tests across `orch8-types` (trigger, context, sequence, execution, lib serde helpers) and `orch8-engine` (gRPC plugin, WASM plugin edge cases). Core scheduling loop, step execution, crash recovery remain untested at the Rust level.

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

### 12. ~~Node.js SDK — Worker Only~~ FIXED
- **Fixed:** Created `sdk-node/` — full management SDK (`Orch8Client`) with typed methods for all API resources (sequences, instances, cron, triggers, plugins, sessions, workers, cluster, circuit breakers). Includes `Orch8Worker` (polling worker with per-handler heartbeats, timeout support, timer-leak-free). 16 typed response interfaces (`FireTriggerResponse`, `BulkResponse`, `BatchCreateResponse`, `HealthResponse`, etc.). 12 unit tests with vitest.

### 13. Dashboard — No Auth
- `dashboard/` is a minimal React SPA (Vite + TypeScript).
- Settings page stores API key in localStorage — no proper auth flow.
- No tests.

### 14. ~~No Python or Go SDKs~~ FIXED
- **Fixed:** Created `sdk-python/` (async httpx + pydantic, 18 typed models, polling worker with per-handler parallel polling and per-task heartbeats, 7 tests) and `sdk-go/` (net/http, zero external deps, idiomatic nil-on-error returns, polling worker with inflight tracking + heartbeats + logging, 5 tests). Both cover the full API surface. All SDK types verified against Rust source types (field names, defaults, response shapes).

### 15. ~~No Helm Chart~~ FIXED
- **Fixed:** Created `helm/orch8/` — standard Helm 3 chart with deployment, service, configmap, secret, ingress (optional), serviceaccount. Configurable Postgres, env vars, autoscaling, resource limits.

### 16. ~~No Landing Pages~~ FIXED
- **Fixed:** Exists under `/web`.

---

## Low: Documentation Inconsistencies

### 17. ~~Stage Completion Percentages Are Stale~~ UPDATED
- Percentages updated in STATUS.md (2026-04-16). Stage 4 now complete, Stage 5 ~75%, Stage 6 ~70%.

### 18. Missing from Any Stage
These features are NOT listed in any stage but should be:
- ~~API rate limiting middleware~~ DONE
- ~~Proper HTTP client for webhooks (reqwest)~~ DONE
- ~~Engine Rust unit tests~~ PARTIALLY DONE (47 new tests across types + engine crates)
- ~~Node.js SDK~~ DONE (full management SDK + worker, 12 tests)
- ~~Python SDK~~ DONE (async httpx + pydantic, 7 tests)
- ~~Go SDK~~ DONE (net/http, zero deps, 5 tests)
- ~~Helm chart~~ DONE (standard Helm 3)
- E2E test CI integration
- Binary distribution (curl install script)
- `orch8 init` scaffolding command
