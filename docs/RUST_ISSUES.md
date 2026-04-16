# Orch8.io Engine — Open Issues

> Last updated: 2026-04-16

---

## Open

### 1. Engine Unit Test Coverage — PARTIAL
- 47 unit tests across `orch8-types` and `orch8-engine` (triggers, context, sequence, execution, serde, gRPC/WASM handler edge cases).
- **Missing:** Core scheduling loop, step execution, crash recovery, signal processing remain untested at the Rust level.
- **Target:** >80% coverage on `orch8-engine` crate before v0.1.0.

### 2. Dashboard — Minimal, No Auth
- `dashboard/` is a minimal React SPA (Vite + TypeScript).
- Settings page stores API key in localStorage — no proper auth flow.
- No tests.

### 3. Benchmarks Not in CI
- `cargo bench` (criterion) exists but is not wired into CI.
- No regression detection on merge.

---

## Resolved (audit trail)

All items below were identified on 2026-04-16 and fixed in the same session.

| # | Issue | Resolution |
|---|---|---|
| 1 | Encryption at rest not wired | `EncryptingStorage` decorator, AES-256-GCM, config + env var |
| 2 | Webhook transport — raw TCP stub | Replaced with `reqwest` (TLS, connection pooling) |
| 3 | Stage 3 summary inaccurate | HITL was implemented; corrected STATUS.md |
| 4 | No Dockerfile | Multi-stage build (rust builder + debian-slim runtime) |
| 5 | No tenant isolation middleware | `X-Tenant-Id` header enforcement, cross-tenant rejection |
| 6 | No API rate limiting | `ConcurrencyLimitLayer`, configurable RPS |
| 7 | SLA deadlines skip fast path | Fast path now checks deadlines |
| 8 | E2E tests not in CI | Added `e2e` job to CI workflow |
| 9 | SQLite `claim_due` ignores `max_per_tenant` | Per-tenant counting logic added |
| 10 | Cancellation scopes per-step only | `CancellationScope` block type for subtree-level |
| 11 | Node.js SDK — worker only | Full management SDK + polling worker, 12 tests |
| 12 | No Python or Go SDKs | Both created with full API surface |
| 13 | No Helm chart | Standard Helm 3 chart |
| 14 | No landing pages | Exists under `/web` |
| 15 | Stage percentages stale | Updated in STATUS.md |
| 16 | Missing from stages: CI, Docker, init, SDKs, binaries | All shipped |
