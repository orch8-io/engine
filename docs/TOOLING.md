# Orch8 Engine — Tooling & Distribution

> SDKs, Docker, binary distribution, CI, developer experience.
> Last updated: 2026-04-16

---

## Docker — DONE

- Multi-stage build: rust:1.83-bookworm builder + debian:bookworm-slim runtime
- Builds both `orch8-server` and `orch8` CLI binaries
- Default runs with SQLite (zero-config start)
- `HEALTHCHECK` via `orch8 health` CLI command
- ENV-based configuration (all `ORCH8_*` vars)
- Published to ghcr.io via release workflow on tag push

---

## Pre-Built Binaries — DONE

- GitHub Actions release workflow builds on `v*` tag push
- Targets: linux-amd64, linux-arm64, darwin-amd64, darwin-arm64
- Uploaded to GitHub Releases as `.tar.gz` with `.sha256` checksums
- Each archive contains `orch8-server` and `orch8` CLI binaries

---

## `orch8 init` Command — DONE

- Scaffolds `orch8.toml` with sensible defaults (SQLite, port 8080, info logging)
- Generates example `sequence.json` (3-step hello-world)
- Generates `docker-compose.yml` (Postgres + Engine)
- Prints quick-start instructions for both SQLite and Postgres
- Uses write-if-absent pattern (won't overwrite existing files)

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

## CI Hardening — DONE

- E2E test job in CI (Postgres service, build server, run all Node.js test files)
- Release workflow on `v*` tag: build binaries (4 targets), Docker image to ghcr.io, GitHub Release with checksums
- TODO: benchmark job (criterion, fail on regression)

---

## Client SDKs — DONE

### Node.js SDK — [orch8-io/sdk-node](https://github.com/orch8-io/sdk-node)
- Full `Orch8Client` class with typed methods for all 45+ API endpoints
- 16 TypeScript interfaces matching Rust source types (`SequenceDefinition`, `TaskInstance`, `FireTriggerResponse`, `BulkResponse`, etc.)
- `Orch8Worker` polling worker (per-handler polling, heartbeats, timeout support, concurrency semaphore)
- `Orch8Error` class with `status`, `body`, `path`
- 12 unit tests (vitest)
- CI: test on Node 18/20/22, publish to npm on tag via `NPM_TOKEN` secret

### Python SDK — [orch8-io/sdk-python](https://github.com/orch8-io/sdk-python)
- Async `Orch8Client` with httpx (context manager pattern)
- 18 Pydantic models matching Rust types
- `Orch8Worker` async polling worker (per-handler parallel polling, per-task heartbeats, semaphore concurrency)
- 7 tests (pytest + respx)
- CI: test on Python 3.10-3.13, publish to PyPI on tag via `PYPI_TOKEN` secret

### Go SDK — [orch8-io/sdk-go](https://github.com/orch8-io/sdk-go)
- Zero external dependencies (`net/http`, `encoding/json`)
- `context.Context` on all methods, idiomatic nil-on-error returns
- `Worker` with goroutine-based polling, channel semaphore, inflight tracking, heartbeats, logging
- 5 tests (`httptest`)
- CI: test on Go 1.21-1.23, module published via `v0.1.0` tag
