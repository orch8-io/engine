# Orch8 Engine — Tooling & Distribution

> SDKs, Docker, binary distribution, CI, developer experience.
> Last updated: 2026-04-16

---

## Docker

- Multi-stage build: builder (rust) + runtime (distroless or alpine)
- Target: < 30MB compressed
- Published to GitHub Container Registry (ghcr.io)
- Default runs with SQLite (zero-config start): `docker run ghcr.io/orch8/engine`
- `HEALTHCHECK` instruction configured
- ENV-based configuration (all `ORCH8_*` vars)

---

## Pre-Built Binaries

- GitHub Actions workflow builds release binaries on tag push
- Targets: linux-amd64, linux-arm64, darwin-amd64, darwin-arm64
- Uploaded to GitHub Releases as `.tar.gz` with checksums
- Install script: `curl -sSL https://install.orch8.io | sh`
- Detects platform, downloads binary, adds to PATH
- First-run prints getting-started instructions

---

## `orch8 init` Command

- Scaffolds `orch8.toml` with sensible defaults (SQLite, port 8080, debug logging)
- Generates example sequence definition (JSON)
- Generates `docker-compose.yml` (Orch8 + Postgres)
- Prints "next steps" instructions

---

## Helm Chart

- Kubernetes deployment with Postgres dependency
- Configurable resources

---

## CI Hardening

- Add e2e test job to CI (start server, run Node.js tests)
- Add benchmark job (criterion, fail on regression)
- Add Docker build + push job on tag
- Release workflow: build binaries (4 targets), build Docker image, push to ghcr.io, create GitHub Release

---

## Client SDKs

### Node.js SDK (expand existing worker-sdk-node)
- Sequence CRUD
- Instance lifecycle (create, query, signal, cancel)
- Worker task management (existing: poll, claim, heartbeat, complete/fail)
- TypeScript types auto-generated from OpenAPI spec
- Published to npm

### Python SDK
- Same API surface as Node.js SDK
- Async support (asyncio)
- Published to PyPI

### Go SDK
- Idiomatic Go client
- Context support
- gRPC + REST dual interface
