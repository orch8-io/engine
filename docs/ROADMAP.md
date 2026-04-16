# Orch8 Engine — Roadmap

> Product features left to build, in priority order.
> For SDKs, Docker, CI, distribution — see [TOOLING.md](TOOLING.md).
> Last updated: 2026-04-16

---

## Recently Completed (2026-04-16)

- **Encryption at rest wired** — `EncryptingStorage` decorator, AES-256-GCM, config + env var
- **Webhook transport** — replaced raw TCP with `reqwest` (TLS, connection pooling)
- **Tenant isolation middleware** — `X-Tenant-Id` header enforcement, cross-tenant rejection
- **API rate limiting** — `ConcurrencyLimitLayer` on Axum router, configurable RPS
- **SLA deadline fast-path** — step-only sequences now check deadlines
- **SQLite `claim_due`** — now enforces `max_per_tenant`
- **Cancellation scopes** — `CancellationScope` block type for subtree-level non-cancellability
- **Engine unit tests** — webhooks (backoff, serialization) + scheduler (prefetch, drain) + 47 new tests across orch8-types and orch8-engine
- **Node.js SDK** — [orch8-io/sdk-node](https://github.com/orch8-io/sdk-node) full management client with typed interfaces, polling worker, 12 tests
- **Python SDK** — [orch8-io/sdk-python](https://github.com/orch8-io/sdk-python) async httpx + pydantic, 18 models, parallel-polling worker, 7 tests
- **Go SDK** — [orch8-io/sdk-go](https://github.com/orch8-io/sdk-go) zero deps, idiomatic nil-on-error, polling worker with heartbeats, 5 tests
- **SDK type audit** — all 3 SDKs verified against Rust source types (18 field-name/default/shape fixes)
- **Helm chart** — [orch8-io/helm-charts](https://github.com/orch8-io/helm-charts) standard Helm 3 (deployment, service, configmap, secret, ingress, serviceaccount)
- **Agent-optimized handlers** — `llm_call` (9+ providers), `tool_call`, `human_review`
- **Agent pattern templates** — ReAct loop, tool-calling pipeline, multi-agent delegation, guardrail validation
- **SSE streaming** — `GET /instances/{id}/stream` for real-time block output
- **Dynamic injection wired** — evaluator merges injected blocks; position-based insert works end-to-end
- **Self-modify handler** — `self_modify` built-in: steps can inject blocks into own instance at runtime
- **gRPC sidecar plugins** — `grpc://host:port/Service.Method` handler prefix for external plugin dispatch
- **Event-driven triggers** — `POST /triggers/{slug}/fire` inbound webhook creates instances; trigger CRUD API
- **Triggers persisted to DB** — `StorageBackend` CRUD for triggers, Postgres + SQLite implementations, API rewritten to use DB
- **NATS message queue trigger** — `async-nats` subscription listener, config-driven (`url`, `subject`), auto-creates instances on messages
- **File watch trigger** — `notify` crate, config-driven (`path`, `recursive`), fires on create/modify events
- **Trigger processor loop** — background task syncs trigger definitions from DB, starts/stops NATS and file watch listeners dynamically
- **Plugin registry** — `PluginDef` type, DB persistence (Postgres + SQLite), plugin CRUD REST API (`/plugins`), WASM + gRPC plugin types
- **WASM handler support** — `wasmtime` runtime, `wasm://plugin-name` handler prefix, alloc/handle/dealloc ABI, JSON-in/JSON-out, feature-gated
- **WASM dispatch wired** — step evaluator dispatches `wasm://` handlers to WASM runtime alongside gRPC and in-process handlers

---

## Next Up

### Visual Sequence Builder
- React component (embeddable)
- Drag-and-drop block arrangement
- Live preview with Orch8 backend
- Export as JSON/YAML

### Plugin Marketplace
- Plugin discovery / search
- Community-contributed handler publishing
- Plugin versioning and update mechanism

### Engine Test Coverage
- Expand Rust unit tests (scheduler, evaluator, signals, recovery)
- Target: >80% coverage on engine crate

---

## Revenue Model

| Model | Revenue/User | Fit |
|-------|-------------|-----|
| Open-core + hosted | $29-199/mo | Best long-term |
| Support tier | $500-2000/mo | Quick revenue |
| Enterprise license | $5K-50K/year | Requires pipeline |

**Recommended path:** Free open-source -> hosted cloud ($29/mo) -> enterprise tier ($199/mo) -> marketplace for paid plugins.

---

## Kill Criteria

- 8 weeks with Docker + landing page, < 50 GitHub stars -> re-evaluate positioning
- AI agent use case doesn't resonate -> pivot to email/notification sequencing
- Well-funded competitor ships same concept -> assess differentiation or pivot to niche
