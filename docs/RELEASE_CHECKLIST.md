# Orch8.io Engine — Release Checklist

> First public release (v0.1.0)
> License: Apache 2.0
> Target: Developer can go from zero to first completed instance in < 5 minutes

---

## Phase 1: Legal & Repository (Day 1)

### License

- [ ] Change `workspace.package.license` in root `Cargo.toml` from `BUSL-1.1` to `Apache-2.0`
- [ ] Add `LICENSE` file (Apache 2.0 full text) to repo root
- [ ] Verify no third-party dependencies have incompatible licenses (`cargo deny check licenses`)

### Repository Hygiene

- [ ] Remove or redact any hardcoded secrets, API keys, or internal URLs
- [ ] Add `CONTRIBUTING.md` (how to build, test, submit PRs)
- [ ] Add `CODE_OF_CONDUCT.md` (Contributor Covenant or similar)
- [ ] Add `SECURITY.md` (how to report vulnerabilities)
- [ ] Update root `README.md` with: badges, one-liner, quick start, architecture diagram, license badge

---

## Phase 2: Build & Test Integrity (Day 1-2)

### Rust Build

- [x] `cargo check --workspace` passes
- [x] `cargo clippy --workspace -- -D warnings` passes
- [x] `cargo fmt --check` passes
- [ ] `cargo test --workspace` passes (all unit/integration tests)
- [ ] `cargo doc --workspace --no-deps` builds without warnings
- [x] `SQLX_OFFLINE=true` build works (offline mode for CI)

### E2E Tests

- [ ] Node.js e2e tests pass against SQLite backend
- [ ] Node.js e2e tests pass against Postgres backend
- [x] E2E test job added to CI

### Benchmarks

- [ ] `cargo bench` runs without errors
- [ ] Baseline numbers recorded

---

## Phase 3: Critical Bug Fixes (Day 2-4)

### P0: Blocking Release

- [x] Replace raw TCP webhook transport with `reqwest`
- [x] Fix SQLite `claim_due` to respect `max_per_tenant`
- [x] Fix SLA deadline enforcement on fast path

### P1: Fix or Remove False Claims

- [x] Wire `EncryptingStorage` into storage layer
- [x] Verify STATUS.md accuracy

### P2: Should Fix

- [x] Add API rate limiting middleware
- [x] Add tenant isolation middleware

---

## Phase 4: Dockerfile & Binary Distribution (Day 3-5)

### Dockerfile

- [x] Multi-stage Dockerfile (rust:1.83-bookworm builder + debian:bookworm-slim runtime)
- [x] `HEALTHCHECK` instruction configured
- [x] ENV-based configuration (all `ORCH8_*` vars)
- [x] Default runs with SQLite (zero-config start)
- [x] Docker Compose for Orch8 + Postgres setup

### Pre-Built Binaries

- [x] GitHub Actions workflow builds release binaries on tag push
- [x] Targets: linux-amd64, linux-arm64, darwin-amd64, darwin-arm64
- [x] Binaries uploaded to GitHub Releases as `.tar.gz` with `.sha256` checksums

### Container Registry

- [x] GitHub Actions pushes Docker image to ghcr.io on tag
- [x] Tags: version + `latest`

---

## Phase 5: Developer Experience (Day 4-6)

### Install Script

- [ ] Create `install.sh` — detects OS/arch, downloads binary from GitHub Releases
- [ ] Verifies checksum after download

### `orch8 init` Command

- [x] Scaffolds `orch8.toml` with sensible defaults
- [x] Generates example sequence definition JSON
- [x] Generates `docker-compose.yml` (Orch8 + Postgres)
- [x] Prints "next steps" instructions

### Quick Start Flow (must work end-to-end)

- [ ] Docker one-liner works
- [ ] Binary install works
- [ ] Example sequence + instance creation documented in README

---

## Phase 6: CI/CD Pipeline (Day 5-6)

- [x] `ci.yml` — check, clippy, fmt, test, e2e
- [x] `release.yml` — build binaries (4 targets), Docker image, GitHub Release
- [ ] Benchmark CI job (optional)

---

## Phase 7: Documentation (Day 5-7)

### README.md

- [ ] One-sentence description + badges
- [ ] 30-second quick start (Docker one-liner)
- [ ] 5-minute tutorial
- [ ] Architecture overview
- [ ] Configuration reference
- [ ] Link to API docs + contributing guide
- [ ] License section

### API Documentation

- [ ] OpenAPI spec is up-to-date
- [ ] Swagger UI accessible at `/swagger-ui`
- [ ] All endpoints documented with examples

### Guides

- [ ] `docs/QUICK_START.md` — zero to first instance in 5 minutes
- [ ] `docs/CONFIGURATION.md` — all env vars, config file format
- [x] `docs/ARCHITECTURE.md` — exists (review for accuracy)

---

## Phase 8: Pre-Release Smoke Test

- [ ] Fresh `docker run` starts and responds on `/health`
- [ ] Create sequence + instance via API, watch it complete
- [ ] Signal a paused instance (HITL)
- [ ] Dashboard loads
- [ ] Postgres backend works with docker-compose
- [ ] Binary works on macOS arm64
- [ ] Binary works on Linux amd64
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
- [ ] README badges point to correct URLs
