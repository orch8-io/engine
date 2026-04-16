# Orch8.io Engine — Release Checklist

> First public release (v0.1.0)
> License: BUSL-1.1
> Target: Developer can go from zero to first completed instance in < 5 minutes

---

## Phase 1: Legal & Repository

### License

- [x] Add `LICENSE` file (BUSL-1.1) to repo root
- [ ] Verify no third-party dependencies have incompatible licenses (`cargo deny check licenses`)

### Repository Hygiene

- [ ] Remove or redact any hardcoded secrets, API keys, or internal URLs
- [x] Add `CONTRIBUTING.md`
- [ ] Add `CODE_OF_CONDUCT.md` (Contributor Covenant or similar)
- [x] Add `SECURITY.md`
- [x] Update root `README.md` with: one-liner, quick start, architecture, config reference, SDK links

---

## Phase 2: Build & Test Integrity

### Rust Build

- [x] `cargo check --workspace` passes
- [x] `cargo clippy --workspace -- -D warnings` passes (zero warnings)
- [x] `cargo fmt --check` passes
- [x] `cargo test --workspace` passes (218 passed, 0 failed)
- [x] `cargo doc --workspace --no-deps` builds without warnings
- [x] `SQLX_OFFLINE=true` build works (offline mode for CI)

### E2E Tests

- [x] Node.js e2e tests pass against SQLite backend (6/7, 1 known issue: unknown handler routes to worker queue)
- [ ] Node.js e2e tests pass against Postgres backend
- [x] E2E test job added to CI

### Benchmarks

- [ ] `cargo bench` runs without errors
- [ ] Baseline numbers recorded

---

## Phase 3: Critical Bug Fixes

- [x] Replace raw TCP webhook transport with `reqwest`
- [x] Fix SQLite `claim_due` to respect `max_per_tenant`
- [x] Fix SLA deadline enforcement on fast path
- [x] Wire `EncryptingStorage` into storage layer
- [x] Verify STATUS.md accuracy
- [x] Add API rate limiting middleware
- [x] Add tenant isolation middleware
- [x] Fix BlockDefinition stack overflow (#[schema(no_recursion)])
- [x] Fix SQLite batch update correctness (claim_due, worker tasks)
- [x] Constant-time API key comparison (subtle crate)

---

## Phase 4: Dockerfile & Binary Distribution

- [x] Multi-stage Dockerfile
- [x] HEALTHCHECK instruction
- [x] ENV-based configuration
- [x] Default runs with SQLite
- [x] Docker Compose for Orch8 + Postgres
- [x] GitHub Actions release workflow (4 targets)
- [x] Binaries uploaded to GitHub Releases with checksums
- [x] Docker image pushed to ghcr.io on tag

---

## Phase 5: Developer Experience

- [x] `install.sh` — detects OS/arch, downloads binary, verifies checksum
- [x] `orch8 init` scaffolding command
- [ ] End-to-end quick start flow verified (Docker one-liner + binary install)

---

## Phase 6: CI/CD Pipeline

- [x] `ci.yml` — check, clippy, fmt, test, e2e
- [x] `release.yml` — build binaries, Docker image, GitHub Release
- [ ] Benchmark CI job (optional)

---

## Phase 7: Documentation

- [x] README.md — one-liner, quick start, architecture, config, SDKs, development
- [x] `docs/QUICK_START.md` — three paths to first instance
- [x] `docs/CONFIGURATION.md` — complete env var and TOML reference
- [x] `docs/ARCHITECTURE.md` — core concepts and execution model
- [x] `docs/API.md` — API reference
- [ ] OpenAPI spec review (Swagger UI at `/swagger-ui`)

---

## Phase 8: Pre-Release Smoke Test

- [ ] Fresh `docker run` starts and responds on `/health`
- [ ] Create sequence + instance via API, watch it complete
- [ ] Signal a paused instance (HITL)
- [ ] Postgres backend works with docker-compose
- [ ] Binary works on macOS arm64
- [ ] 1,000 instances complete without errors (SQLite)
- [ ] 10,000 instances complete without errors (Postgres)

---

## Phase 9: Release

- [ ] All changes committed and pushed to `main`
- [ ] Create annotated tag: `git tag -a v0.1.0 -m "First public release"`
- [ ] Push tag, verify release workflow triggers
- [ ] Verify binaries on GitHub Releases
- [ ] Verify Docker image on ghcr.io
- [ ] GitHub Release notes written
