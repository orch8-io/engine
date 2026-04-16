# Orch8 Engine — Roadmap

> What's left to build, in priority order.

---

## Stage 5 — Remaining (SDK + Polish)

| Priority | Deliverable | Notes |
|---|---|---|
| High | Dockerfile | Multi-stage build, < 30MB, health check, ENV config |
| High | Pre-built binaries | GitHub Actions release workflow (linux/mac, amd64/arm64) |
| Medium | Helm chart | Kubernetes deployment with Postgres dependency |
| Low | Python SDK | Type-safe client, async support |
| Low | Landing pages | Homepage, /for-outreach, /for-notifications, /pricing |

---

## Stage 7 — Ship & Developer Experience

> No new features. Pure distribution and DX.

### 7.1 Docker Distribution
- Multi-stage build: builder (rust) + runtime (distroless or alpine)
- Target: < 30MB compressed
- Published to GitHub Container Registry (ghcr.io)
- Default runs with SQLite (zero-config start): `docker run ghcr.io/orch8/engine`

### 7.2 One-Liner Install
- Pre-built binaries for linux-amd64, linux-arm64, darwin-amd64, darwin-arm64
- `curl -sSL https://install.orch8.io | sh`
- First-run prints getting-started instructions

### 7.3 `orch8 init` Command
- Scaffolds `orch8.toml` with sensible defaults
- Generates example sequence definition (JSON)
- Generates docker-compose.yml (Orch8 + Postgres)

### 7.4 CI Hardening
- Add e2e test job to CI (start server, run Node.js tests)
- Add benchmark job (criterion, fail on regression)
- Add Docker build + push job on tag

### 7.5 Landing Page (Minimal)
- Single page: hero + one-liner install + 3 value props + quick start
- Deploy to orch8.io (Cloudflare Pages or similar)

### Exit Criteria
- `docker run ghcr.io/orch8/engine` starts a working engine
- Zero to first completed instance in < 5 minutes
- Landing page live

---

## Stage 8 — AI Agent Engine

> Position Orch8 as "the execution layer for AI agents."

### 8.1 Agent-Optimized Handlers
- `llm_call` handler (OpenAI/Anthropic API call with retry + streaming)
- `tool_call` handler (dispatch to external tool, collect result)
- `human_review` handler (improved HITL with dedicated UI endpoint)

### 8.2 Agent Patterns as First-Class Sequences
- ReAct loop template (observe -> think -> act -> loop)
- Tool-calling pipeline template
- Multi-agent delegation template
- Guardrail/validation step template

### 8.3 Dynamic Step Injection Improvements
- Inject at specific position (not just append)
- Agent can modify its own sequence at runtime

### 8.4 Streaming Output
- SSE endpoint for instance progress (`GET /instances/{id}/stream`)
- Real-time block output streaming

### 8.5 Client SDKs
- Node.js full client SDK (sequence CRUD, instance lifecycle, signals)
- Python client SDK (same surface, async support)
- Published to npm/PyPI

### Exit Criteria
- AI agent demo: ReAct loop with tool calls, human review gate, crash recovery
- Blog post: "Building Crash-Recoverable AI Agents with Orch8"
- Node.js + Python SDKs published

---

## Stage 9 — Ecosystem & Growth

- Plugin system (WASM or gRPC sidecar handlers, registry)
- Event-driven triggers (inbound webhook, message queue, file watch)
- Go SDK (idiomatic, context support, gRPC + REST)
- Visual sequence builder (React component, drag-and-drop, export JSON/YAML)
- Engine Rust unit tests (>80% coverage on engine crate)

### Exit Criteria
- 3+ community-contributed handler plugins
- 1,000+ GitHub stars
- First paying user

---

## Stage 10 — Revenue

| Model | Revenue/User | Fit |
|-------|-------------|-----|
| Open-core + hosted | $29-199/mo | Best long-term |
| Support tier | $500-2000/mo | Quick revenue |
| Enterprise license | $5K-50K/year | Requires pipeline |

**Recommended path:** Free open-source → hosted cloud ($29/mo starter) → enterprise tier ($199/mo) → marketplace for paid plugins.

---

## Kill Criteria

- 8 weeks with Docker + landing page, < 50 GitHub stars → re-evaluate positioning
- AI agent use case doesn't resonate → pivot to email/notification sequencing
- Well-funded competitor ships same concept → assess differentiation or pivot to niche
