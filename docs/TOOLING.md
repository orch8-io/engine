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

## Helm Chart — DONE

Repo: [orch8-io/helm-charts](https://github.com/orch8-io/helm-charts) — standard Helm 3 chart:
- Deployment with health probes (liveness + readiness)
- Service (http 8080 + grpc 50051)
- ConfigMap for non-secret env vars
- Secret for `DATABASE_URL` and `ORCH8_ENCRYPTION_KEY`
- Optional ingress with TLS
- Optional serviceaccount with annotations
- Autoscaling support (HPA)
- Configurable resource limits

---

## CI Hardening

- Add e2e test job to CI (start server, run Node.js tests)
- Add benchmark job (criterion, fail on regression)
- Add Docker build + push job on tag
- Release workflow: build binaries (4 targets), build Docker image, push to ghcr.io, create GitHub Release

---

## Client SDKs — DONE

### Node.js SDK — [orch8-io/sdk-node](https://github.com/orch8-io/sdk-node)
- Full `Orch8Client` class with typed methods for all 45+ API endpoints
- 16 TypeScript interfaces matching Rust source types (`SequenceDefinition`, `TaskInstance`, `FireTriggerResponse`, `BulkResponse`, etc.)
- `Orch8Worker` polling worker (per-handler polling, heartbeats, timeout support, concurrency semaphore)
- `Orch8Error` class with `status`, `body`, `path`
- 12 unit tests (vitest)
- TODO: publish to npm

### Python SDK — [orch8-io/sdk-python](https://github.com/orch8-io/sdk-python)
- Async `Orch8Client` with httpx (context manager pattern)
- 18 Pydantic models matching Rust types
- `Orch8Worker` async polling worker (per-handler parallel polling, per-task heartbeats, semaphore concurrency)
- 7 tests (pytest + respx)
- TODO: publish to PyPI

### Go SDK — [orch8-io/sdk-go](https://github.com/orch8-io/sdk-go)
- Zero external dependencies (`net/http`, `encoding/json`)
- `context.Context` on all methods, idiomatic nil-on-error returns
- `Worker` with goroutine-based polling, channel semaphore, inflight tracking, heartbeats, logging
- 5 tests (`httptest`)
- TODO: publish module
