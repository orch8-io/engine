# Orch8.io Engine — Release Checklist

> First public release (v0.1.0)
> License: Apache 2.0
> Target: Developer can go from zero to first completed instance in < 5 minutes

---

## Phase 1: Legal & Repository (Day 1)

### License

- [ ] Change `workspace.package.license` in root `Cargo.toml` from `BUSL-1.1` to `Apache-2.0`
- [ ] Add `LICENSE` file (Apache 2.0 full text) to repo root
- [ ] Add license header comment to all `main.rs` / `lib.rs` files (optional but standard)
- [ ] Verify no third-party dependencies have incompatible licenses (`cargo deny check licenses`)

### Repository Hygiene

- [ ] Ensure `.gitignore` covers: `target/`, `.env`, `*.db`, `.worktrees/`
- [ ] Remove or redact any hardcoded secrets, API keys, or internal URLs
- [ ] Add `CONTRIBUTING.md` (how to build, test, submit PRs)
- [ ] Add `CODE_OF_CONDUCT.md` (Contributor Covenant or similar)
- [ ] Add `SECURITY.md` (how to report vulnerabilities)
- [ ] Update root `README.md` with: badges, one-liner description, quick start, architecture diagram, license badge

---

## Phase 2: Build & Test Integrity (Day 1-2)

### Rust Build

- [ ] `cargo check --workspace` passes
- [ ] `cargo clippy --workspace -- -D warnings` passes (zero warnings)
- [ ] `cargo fmt --check` passes
- [ ] `cargo test --workspace` passes (all unit/integration tests)
- [ ] `cargo doc --workspace --no-deps` builds without warnings
- [ ] `SQLX_OFFLINE=true` build works (offline mode for CI without DB)

### E2E Tests

- [ ] Node.js e2e tests pass against SQLite backend (`tests/e2e/`)
- [ ] Node.js e2e tests pass against Postgres backend
- [ ] E2e test job added to `.github/workflows/ci.yml`

### Benchmarks

- [ ] `cargo bench` runs without errors (criterion)
- [ ] Baseline benchmark numbers recorded in `docs/` or `benches/README.md`

---

## Phase 3: Critical Bug Fixes (Day 2-4)

> From `docs/RUST_ISSUES.md` — must-fix before release.

### P0: Blocking Release

- [ ] Replace raw TCP webhook transport with `reqwest` (TLS, connection pooling, proper headers)
- [ ] Fix SQLite `claim_due` to respect `max_per_tenant` (currently ignores the parameter)
- [ ] Fix SLA deadline enforcement on fast path (step-only sequences skip deadlines)

### P1: Fix or Remove False Claims

- [ ] Wire `FieldEncryptor` into storage layer — OR remove encryption claims from docs/README
- [ ] Verify `docs/STATUS.md` stage completion data matches reality

### P2: Should Fix

- [ ] Add API rate limiting middleware (`tower-governor` or `tower::limit`)
- [ ] Add basic tenant isolation (extract tenant from auth, enforce on queries)

---

## Phase 4: Dockerfile & Binary Distribution (Day 3-5)

### Dockerfile

- [ ] Create multi-stage `Dockerfile`: builder (`rust:1.80-slim`) + runtime (`gcr.io/distroless/cc-debian12` or `alpine:3.20`)
- [ ] Compressed image size < 30MB
- [ ] `HEALTHCHECK` instruction configured (hits `/health` endpoint)
- [ ] ENV-based configuration: `ORCH8_DATABASE_URL`, `ORCH8_HOST`, `ORCH8_PORT`, `ORCH8_API_KEY`, `ORCH8_LOG_LEVEL`
- [ ] Default runs with SQLite (zero-config start): `docker run ghcr.io/orch8/engine`
- [ ] Add `docker-compose.yml` for Orch8 + Postgres setup
- [ ] Document all `ORCH8_*` environment variables in README or `docs/CONFIGURATION.md`

### Pre-Built Binaries

- [ ] GitHub Actions workflow builds release binaries on tag push
- [ ] Targets: `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`, `aarch64-apple-darwin`
- [ ] Binaries uploaded to GitHub Releases as `.tar.gz` with checksums (`sha256sum`)
- [ ] Release notes auto-generated from commits or manually written

### Container Registry

- [ ] GitHub Actions pushes Docker image to `ghcr.io/orch8/engine` on tag
- [ ] Tags: `latest`, `v0.1.0`, `0.1`
- [ ] Image is public (no auth required to pull)

---

## Phase 5: Developer Experience (Day 4-6)

### Install Script

- [ ] Create `install.sh` — detects OS/arch, downloads correct binary from GitHub Releases
- [ ] `curl -sSL https://raw.githubusercontent.com/orch8/engine/main/install.sh | sh`
- [ ] First-run banner prints quick start instructions
- [ ] Script verifies checksum after download

### `orch8 init` Command

- [ ] Scaffolds `orch8.toml` with sensible defaults (SQLite, port 8080, debug logging)
- [ ] Generates example sequence definition JSON
- [ ] Generates `docker-compose.yml` (Orch8 + Postgres)
- [ ] Prints "next steps" instructions

### Quick Start Flow (must work end-to-end)

```bash
# Option A: Docker (zero install)
docker run -p 8080:8080 ghcr.io/orch8/engine

# Option B: Binary
curl -sSL https://raw.githubusercontent.com/orch8/engine/main/install.sh | sh
orch8 server

# Then:
curl -X POST http://localhost:8080/api/v1/sequences -d @example.json
curl -X POST http://localhost:8080/api/v1/instances -d '{"sequence_id":"..."}'
curl http://localhost:8080/api/v1/instances/{id}
```

- [ ] Option A works
- [ ] Option B works
- [ ] Example sequence + instance creation documented in README quick start

---

## Phase 6: CI/CD Pipeline (Day 5-6)

### GitHub Actions Workflows

- [ ] `ci.yml` — on push/PR: check, clippy, fmt, test (existing, extend with e2e)
- [ ] `release.yml` — on tag `v*`: build binaries (4 targets), build Docker image, push to ghcr.io, create GitHub Release with artifacts
- [ ] `docker.yml` (or merged into release) — build + push Docker image

### CI Jobs

| Job | Trigger | What It Does |
|-----|---------|--------------|
| `check` | push/PR | `cargo check`, `clippy`, `fmt` |
| `test-unit` | push/PR | `cargo test --workspace` |
| `test-e2e` | push/PR | Start server, run Node.js e2e tests |
| `build-binaries` | tag `v*` | Cross-compile 4 targets, upload to Release |
| `build-docker` | tag `v*` | Multi-stage build, push to ghcr.io |
| `benchmark` | push to main | `cargo bench`, fail on >10% regression (optional) |

---

## Phase 7: Documentation (Day 5-7)

### README.md

- [ ] One-sentence description: "Embeddable workflow engine. Like SQLite for task orchestration."
- [ ] Badges: CI status, crates.io (if published), Docker image size, license
- [ ] 30-second quick start (Docker one-liner)
- [ ] 5-minute tutorial (create sequence, launch instance, query result)
- [ ] Architecture overview (crate diagram)
- [ ] Configuration reference (all ORCH8_* vars)
- [ ] Link to API docs
- [ ] Link to contributing guide
- [ ] License section

### API Documentation

- [ ] OpenAPI spec is up-to-date and auto-generated (utoipa)
- [ ] Swagger UI accessible at `/swagger-ui` when server runs
- [ ] All endpoints documented with request/response examples

### Guides (can be brief for v0.1.0)

- [ ] `docs/QUICK_START.md` — zero to first instance in 5 minutes
- [ ] `docs/CONFIGURATION.md` — all env vars, config file format
- [ ] `docs/ARCHITECTURE.md` — sequences, instances, blocks, signals (review for accuracy)

---

## Phase 8: Pre-Release Smoke Test

### Manual Verification

- [ ] Fresh `docker run ghcr.io/orch8/engine` starts and responds on `/health`
- [ ] Create a sequence via API
- [ ] Create an instance, watch it run to completion
- [ ] Send a signal to a paused instance (HITL)
- [ ] Dashboard loads and shows instance state
- [ ] Postgres backend works with docker-compose setup
- [ ] Binary install works on macOS (arm64)
- [ ] Binary install works on Linux (amd64)

### Load Sanity Check

- [ ] 1,000 instances created and completed without errors (SQLite)
- [ ] 10,000 instances created and completed without errors (Postgres)
- [ ] No memory leaks over 10-minute sustained load

---

## Phase 9: Release

### Git

- [ ] All changes committed and pushed to `main`
- [ ] Create annotated tag: `git tag -a v0.1.0 -m "First public release"`
- [ ] Push tag: `git push origin v0.1.0`
- [ ] Verify GitHub Actions triggers release workflow
- [ ] Verify binaries appear on GitHub Releases page
- [ ] Verify Docker image appears on ghcr.io

### Announce

- [ ] GitHub Release notes written (what's included, quick start, known limitations)
- [ ] README badges point to correct URLs
- [ ] (Optional) Post on: Hacker News, r/rust, r/programming, Twitter/X

---

## Post-Release Monitoring (Week 1)

- [ ] Monitor GitHub Issues for install/build problems
- [ ] Watch Docker pull counts
- [ ] Track GitHub stars
- [ ] Fix any reported P0 bugs within 24 hours
- [ ] Collect feedback on DX friction (what confused people?)

---

## Kill Criteria

> From `docs/ROADMAP.md`

- If after 8 weeks with Docker + landing page, fewer than 50 GitHub stars: re-evaluate positioning
- If AI agent use case doesn't resonate with early users: pivot to email/notification sequencing
- If a well-funded competitor ships the same concept: assess differentiation; if none, pivot to niche
