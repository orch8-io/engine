# Orch8.io — Product Requirements Document

> **See also:** [Sales Pitch](docs/SALES_PITCH.md) | [Technical Details](docs/TECHNICAL_DETAILS.md) | [Business Overview](docs/BUSINESS_PRODUCT_DETAILS.md) | [Action Plan](docs/ACTION_PLAN.md) | [Development Stages](docs/DEVELOPMENT_STAGES.md)

## Executive Summary

A self-hosted, embeddable Rust engine for scheduling, branching, and durably executing multi-step task sequences at scale. The product serves two layers:

1. **Scheduling kernel** — campaign-specific primitives (rate limits, send windows, resource rotation, bulk operations) for teams building outreach tools, notification platforms, and lifecycle systems on BullMQ or Postgres polling.
2. **Orchestration engine** — general-purpose durable workflow execution (parallel, race, try-catch, signals, queries, template interpolation) for teams escaping Temporal's complexity tax.

**Biggest strength:** No competitor combines embeddable library + campaign primitives + workflow orchestration + Rust performance + snapshot-based resume (no history replay, no determinism constraints).

**Biggest risk:** Restate ($7M funded, Rust-based, single binary) could expand into this niche within 12 months.

**#1 priority:** Ship the core engine with Node.js SDK, per-resource rate limiting, and relative delay scheduling. Get 3 design partners embedding it in production within 90 days.

---

## North Star Metric

**Licensed task instances actively running in production per month.**

Not trials. Not downloads. Actual instances executing paid sequences in buyers' production systems.

### Input Metrics Tree

```text
NORTH STAR: Active production task instances per month
├── Acquisition: New license trials started per month
├── Activation: % of trials that schedule 1,000+ instances in first 7 days
├── Conversion: % of trials that convert to paid license
├── Expansion: Avg instances per paying customer (growth = upsell signal)
└── Retention: % of customers still running instances at month 6
```

---

## Market Context

### TAM / SAM / SOM

| Layer | Estimate | Assumptions |
|-------|----------|-------------|
| TAM | $50–150M/yr | All software teams globally that need durable multi-step task scheduling beyond cron/queues |
| SAM | $10–30M/yr | B2B campaign tools, notification platforms, lifecycle orchestration builders reachable via English-language developer channels |
| SOM (3-year) | $1–3M ARR | 50–150 paying customers at $500–$8,000/month average |

[ASSUMPTION: estimated from category norms — no primary market data. Based on ~200 identifiable tools in cold email, notification, and lifecycle categories × $2K avg license.]

### Competitive Positioning Snapshot

| Dimension | This Engine | Temporal | Restate | Inngest | BullMQ |
|-----------|-------------|----------|---------|---------|--------|
| Embeddable library | ✅ | ❌ | ❌ | ❌ | ✅ |
| Self-hosted binary | ✅ | ✅ | ✅ | Partial | ✅ |
| No history replay | ✅ | ❌ | ✅ | ✅ | N/A |
| No determinism constraints | ✅ | ❌ | ✅ | ✅ | N/A |
| State directly queryable (no handler boilerplate) | ✅ | ❌ | Partial | Partial | N/A |
| No payload size limits | ✅ | ❌ | ❌ | Partial | ✅ |
| Steps are plain functions (no activity ceremony) | ✅ | ❌ | ✅ | ✅ | ✅ |
| Embedded test mode (no external server) | ✅ | ❌ | ❌ | Partial | ✅ |
| Per-resource rate limits | ✅ | ❌ | ❌ | Partial | ❌ |
| 1M+ instances/node | ✅ | ❌ | Untested | Untested | ❌ |
| Campaign-specific primitives | ✅ | ❌ | ❌ | ❌ | ❌ |
| Parallel / race / structured concurrency | ✅ | ✅ | ✅ | Partial | ❌ |
| Multi-language SDKs | Planned | ✅ | Partial | ✅ | ❌ |
| Commercial license | ✅ | ✅ | BSL | ✅ | MIT |

**Strategic posture:** Two-layer positioning. Layer 1: "Embeddable scheduling kernel for SaaS builders" — owns a category that Temporal/Restate ignore. Layer 2: "Temporal without the tax" — targets teams drowning in history bloat workarounds, determinism constraints, and activity ceremony.

### Temporal Pain Points We Solve

These are real pain points validated against a production Temporal codebase (~4,700 lines of workflow executor, ~2,400 lines of workaround code). Each represents a design constraint this engine eliminates.

| # | Pain Point | What Temporal Forces | What We Do Instead | Lines of Workaround Code Eliminated |
|---|-----------|---------------------|-------------------|--------------------------------------|
| 1 | **History bloat & continue-as-new** | Temporal replays entire workflow history on every wake-up. Long-running workflows hit history limits (50K events). Teams must implement continue-as-new orchestration: externalize all state (block outputs, context, runtime vars) to external storage via 4+ API calls, then reset the workflow. Checkpoint infrastructure, size estimation, manifest tracking — all just to survive history limits. | Snapshot-based state. O(1) resume from last checkpoint. No history replay. No size limits on execution length. State lives in Postgres rows, not event history. | ~300 lines (state externalization, checkpoint management, continue-as-new orchestration) |
| 2 | **Determinism tax** | Temporal's replay model forbids `Date.now()`, `Math.random()`, `structuredClone`, any external I/O in workflow code. Every code change requires `patched()` / `deprecatePatch()` version markers to avoid breaking in-flight executions. Developers maintain parallel code paths indefinitely. SonarCloud exclusions needed to suppress warnings about `JSON.parse(JSON.stringify())` workarounds. | No replay = no determinism constraints. Developers write normal TypeScript/Python/Go. Code changes take effect immediately — no version branching. Explicit state migration functions for breaking changes, not implicit code forks. | ~85 lines (versioning machinery) + ongoing developer cognitive load |
| 3 | **Payload size limits** | Temporal limits payload size per history event. Teams implement: (a) custom gzip compression codecs for payloads >1KB, (b) output externalization for payloads >64KB — store in DB, replace with reference marker, retrieve on demand, (c) signal payload externalization for large messages. All transparent to the workflow but ~350 lines of infrastructure. | State is Postgres rows. No payload limits. Large outputs stored as regular table rows. Compression optional at the storage layer, not the SDK layer. | ~350 lines (compression codec, externalization with fallback handling, reference markers) |
| 4 | **Activity ceremony** | Every side-effect function requires: (a) define in activities file, (b) re-export from central registry (800+ lines in one file), (c) configure `proxyActivities` with timeout/heartbeat/retry policy, (d) manually classify errors as retryable vs non-retryable using `ApplicationFailure.create()` vs `ApplicationFailure.nonRetryable()`. Every activity needs retry-behavior tests. | Steps are plain functions. Register by convention, not central registry. Engine calls them with built-in retry. Error classification via return type or error code convention — no manual `ApplicationFailure` wrapping. | ~800 lines (activity registry) + ~200 lines (error classification boilerplate) |
| 5 | **Invisible state** | Temporal doesn't expose workflow state directly. Teams implement 15+ query and signal handlers (~450 lines) just to let external systems (UI, API, debugger) see what a workflow is doing. Every new piece of visible state needs a new handler, a new query type, and new client-side code. | State is a first-class queryable object. `GET /instances/{id}/state` returns the full context — no handlers needed. State changes emit events automatically. Filter instances by any JSONB field via standard Postgres queries. | ~450 lines (query/signal handler boilerplate) |
| 6 | **Search attribute ceremony** | Custom search attributes must be pre-registered on the Temporal server before use. Limited to a small set of indexed fields. Local test environments don't support them — code must skip search attribute updates in test mode. Can't dynamically index by arbitrary user-defined fields. | Instance metadata is JSONB in Postgres. `CREATE INDEX` on whatever you need. No server-side registration. Works identically in test and production. | ~30 lines + deployment friction |
| 7 | **Testing requires external infrastructure** | Temporal workflows can't be unit-tested without a running Temporal server (or a limited test environment that doesn't support search attributes). Teams build entire dry-run infrastructures: mock responses for every block type, skip-lists for side-effect blocks, custom test harnesses. | Embedded SQLite mode. Same engine, same code, zero external dependencies. `engine.test_mode()` — side-effect steps return mocks unless overridden. Search attributes, queries, signals all work identically. | ~300 lines (dry-run infrastructure, test harness, mock responses) |

**Total workaround code eliminated: ~2,400 lines across 20+ files (~10% of a typical Temporal-based codebase).**

Beyond line count, the determinism constraint imposes ongoing cognitive load on every developer touching workflow code — they must reason about replay safety for every line they write. This engine removes that burden entirely.

---

## User Segments

### Persona 1: CTO at a Growing Outreach Tool

- **Who:** Technical co-founder or CTO at a cold email / sales engagement startup (5–30 people)
- **Primary job:** Replace their homegrown BullMQ + Postgres scheduling layer that breaks at 500K+ active sequences
- **Success metric:** Sequences run reliably at scale without engineering babysitting
- **Key frustration:** Spent 4 months building scheduling infra. Still loses tasks on deploys. Can't branch mid-sequence.
- **Willingness to pay:** High ($2K–$8K/month — cheaper than 1 backend engineer)
- **Segment size:** ~50 identifiable tools globally (Instantly, Smartlead, Saleshandy, Lemlist, Reply.io + dozens of smaller)

### Persona 2: Platform Engineer at a Notification Infra Company

- **Who:** Backend engineer at Novu, Knock, Courier, SuprSend, or similar
- **Primary job:** Schedule multi-channel notification sequences (push → wait → email fallback → SMS escalation)
- **Success metric:** Reliable delivery at scale with per-channel rate limits respected
- **Key frustration:** Building scheduling on top of Redis/Postgres, constantly fighting race conditions and lost jobs
- **Willingness to pay:** Medium-High ($2K–$10K/month)
- **Segment size:** ~30 notification infra companies + hundreds of SaaS teams building in-house

### Persona 3: Solo Founder Launching New SaaS

- **Who:** Indie developer building a new outreach/lifecycle/notification tool
- **Primary job:** Ship product fast without spending 3 months on scheduling infrastructure
- **Success metric:** Time-to-market. Engine works out of the box.
- **Key frustration:** Choosing between BullMQ (fragile) and Temporal (overkill). Needs something in between.
- **Willingness to pay:** Low-Medium ($500–$2K/month)
- **Segment size:** Large (~500+ new tools launched yearly in adjacent categories)

### Persona 4: Fintech/Healthtech Engineer

- **Who:** Backend engineer at a lending platform, debt collection tool, or digital health company
- **Primary job:** Orchestrate long-running regulated workflows (dunning sequences, patient pathways, KYC flows) with audit trails
- **Success metric:** Compliance (full audit log), reliability (zero lost tasks), longevity (runs for months)
- **Willingness to pay:** High ($5K–$30K/month — regulated = high WTP)
- **Segment size:** Medium (~100+ fintech/healthtech platforms with this need)

### Persona 5: Backend Engineer Escaping Temporal

- **Who:** Backend engineer at a mid-stage startup (20–100 people) running Temporal in production for 6–18 months
- **Primary job:** Eliminate the ~2,400 lines of workaround code (history bloat, determinism tax, activity ceremony, state visibility hacks) that accumulated around Temporal
- **Success metric:** Same workflow capabilities, 50%+ less infrastructure code, developers write normal TypeScript
- **Key frustration:** Every code change requires versioning patches. History bloat causes continue-as-new chaos. Activity registry is 800+ lines. Can't see workflow state without query handlers. Testing requires a running Temporal server.
- **Willingness to pay:** High ($3K–$10K/month — cheaper than the engineering time spent on Temporal workarounds)
- **Segment size:** Medium (~500+ companies running Temporal in production, growing rapidly)

---

## Feature Inventory

### CORE ENGINE

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 1 | Durable task instances | Atomic unit: one contact/entity in one sequence. Survives restarts. | P0 | ❌ Build |
| 2 | Step-based execution with memoization | Each step runs once, result persisted. Resume from checkpoint, no replay. | P0 | ❌ Build |
| 3 | Relative delay scheduling | Next step fires relative to previous completion, not wall clock. | P0 | ❌ Build |
| 4 | Business day awareness | Delays skip weekends. Configurable holiday calendars. | P0 | ❌ Build |
| 5 | Timezone-per-instance | Each task instance carries its own timezone for send window calculation. | P0 | ❌ Build |
| 6 | Randomized jitter | Spread task firing across a window (e.g. ±2h) to prevent thundering herd. | P0 | ❌ Build |
| 7 | Conditional branching | Branch on external data at runtime. Webhook/handler returns decision. | P0 | ❌ Build |
| 8 | External event triggers (signals) | Mid-sequence event advances, cancels, or re-routes an instance. | P0 | ❌ Build |
| 9 | Crash recovery via state snapshots | On restart, read last checkpoint per instance. No history replay. | P0 | ❌ Build |
| 10 | Sequence definition API | Define multi-step sequences as code or YAML/JSON. | P0 | ❌ Build |

### RATE LIMITING & RESOURCE MANAGEMENT

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 11 | Per-resource rate limiting | Cap sends per mailbox/channel/API (e.g. 30/day per mailbox). Defer, not reject. | P0 | ❌ Build |
| 12 | Resource pools with rotation | Assign N mailboxes/channels to a campaign. Weighted round-robin. Auto-defer on exhaustion. | P1 | ❌ Build |
| 13 | Warmup ramp schedules | New resource starts at low capacity, ramps over weeks. | P1 | ❌ Build |
| 14 | Send window scheduling | Fire tasks only during configured hours in recipient's timezone. | P1 | ❌ Build |
| 15 | Global rate limiting | Engine-wide throughput cap to protect downstream systems. | P2 | ❌ Build |

### SCALE & OPERATIONS

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 16 | Bulk create | Enqueue 50K+ instances from one API call. | P0 | ❌ Build |
| 17 | Bulk pause/resume/cancel | Operate on all instances matching a filter (campaign, tenant, resource). | P0 | ❌ Build |
| 18 | Bulk re-schedule | Shift all pending steps by ±N hours. | P1 | ❌ Build |
| 19 | Priority queues | Hot leads fire before cold nurture. At least 3 levels: high/normal/low. | P1 | ❌ Build |
| 20 | Multi-tenancy | Tenant-scoped queries, per-tenant rate limits, noisy-neighbor protection. | P1 | ❌ Build |

### RELIABILITY & ERROR HANDLING

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 21 | Configurable retry with backoff | Per-step retry policy: max attempts, exponential backoff, jitter. | P0 | ❌ Build |
| 22 | Dead letter queue | Failed instances after retry exhaustion. Inspectable, manually retryable. | P1 | ❌ Build |
| 23 | Timeout per step | Step must complete within N seconds or be marked failed. | P1 | ❌ Build |
| 24 | Circuit breaker | If a downstream handler fails repeatedly, pause all steps of that type. | P2 | ❌ Build |
| 25 | Idempotency keys | Prevent duplicate step execution on retry. | P1 | ❌ Build |

### API & INTEGRATION

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 26 | gRPC API | Primary API surface for high-performance consumers. | P0 | ❌ Build |
| 27 | HTTP/REST API | Secondary API for easy integration and testing. | P0 | ❌ Build |
| 28 | Webhook event emitter | Push events to buyer's system: step.completed, instance.failed, rate_limit.hit. | P1 | ❌ Build |
| 29 | Node.js/TypeScript SDK | First SDK. Most campaign tools are Node-based. | P0 | ❌ Build |
| 30 | Python SDK | Second SDK. Fintech/healthtech buyers. | P1 | ❌ Build |
| 31 | Go SDK | Third SDK. Infra-oriented buyers. | P2 | ❌ Build |
| 32 | Rust crate (native) | Direct embedding for Rust consumers. | P0 | ❌ Build |

### STORAGE

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 33 | PostgreSQL backend | Primary storage. Most buyers already run Postgres. | P0 | ❌ Build |
| 34 | SQLite backend (embedded) | For dev, testing, and small-scale self-hosted. | P1 | ❌ Build |
| 35 | Storage abstraction trait | Pluggable interface so buyers can implement custom backends. | P1 | ❌ Build |
| 36 | Migration system | Schema versioning. Automatic migration on engine startup. | P0 | ❌ Build |

### OBSERVABILITY

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 37 | Prometheus metrics endpoint | Active instances, steps/sec, queue depth, rate limit saturation, error rate, latency percentiles. | P1 | ❌ Build |
| 38 | Structured JSON logging | Every state transition logged with instance_id, step, tenant, duration. | P0 | ❌ Build |
| 39 | Health check endpoint | Readiness + liveness probes for K8s deployment. | P0 | ❌ Build |
| 40 | Dashboard UI (optional) | Web UI showing engine status, instance inspector, queue visualization. | P2 | ❌ Build |

### COMPLIANCE & ENTERPRISE

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 41 | Audit log | Append-only event log of every state transition. Required for regulated verticals. | P1 | ❌ Build |
| 42 | Sequence versioning | Deploy new sequence definition without killing running instances. V1 instances continue on V1. | P2 | ❌ Build |
| 43 | Hot code migration | Configurable rules for migrating running instances to new sequence version. | P2 | ❌ Build |
| 44 | Encryption at rest | Encrypt instance metadata/state in storage. | P2 | ❌ Build |
| 45 | SLA timers / deadlines | "Must complete step within 72h or escalate." Per-step deadline with escalation handler. | P2 | ❌ Build |

### ADVANCED SCHEDULING

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 46 | A/B split primitive | Route N% of instances to variant A, M% to variant B. For subject line testing, etc. | P2 | ❌ Build |
| 47 | Dynamic step injection | Add steps to a running instance at runtime (for AI agent use cases). | P2 | ❌ Build |
| 48 | Cron-triggered sequences | Recurring sequence instantiation (e.g. "every Monday, create instances for all contacts"). | P2 | ❌ Build |
| 49 | Sub-sequences / composition | A step can invoke another sequence as a sub-workflow. | P2 | ❌ Build |
| 50 | Concurrency control per key | Max N instances running simultaneously for the same entity (prevent duplicate campaigns). | P1 | ❌ Build |

### WORKFLOW ORCHESTRATION

| # | Feature | Description | Priority | Status |
|---|---------|-------------|----------|--------|
| 51 | Bidirectional signals | Multiple named signal types on a running instance: pause, resume, cancel, update context, session lifecycle. Not just advance/cancel — arbitrary typed payloads that modify state or control flow mid-execution. | P0 | ❌ Build |
| 52 | Live state queries | Query a running instance's current state without stopping it. Multiple query types: full context, execution status, debug state, session info, execution trace. | P0 | ❌ Build |
| 53 | Transactional updates | Exactly-once update semantics for operations like approval decisions. Caller sends update, engine guarantees it's applied once and returns the result. Distinct from fire-and-forget signals. | P1 | ❌ Build |
| 54 | Parallel execution | Run multiple branches concurrently within a single instance. All branches complete before proceeding. Independent state per branch merged on completion. | P0 | ❌ Build |
| 55 | Race execution | Run multiple branches concurrently, first to complete wins. Cancel all losing branches cleanly. Supports first-to-resolve and first-to-succeed semantics. | P0 | ❌ Build |
| 56 | Cancellation scopes | Structured concurrency: define scopes where cancellation propagates to all child operations. Non-cancellable scopes for cleanup/finalization work that must complete. | P0 | ❌ Build |
| 57 | Try-catch-finally blocks | Error handling within sequences. Catch errors from a set of steps, execute recovery logic, run finalization regardless of success/failure. | P0 | ❌ Build |
| 58 | Loop / forEach iteration | Iterate over collections within a sequence. forEach over arrays, while-style loops with conditions. Configurable iteration cap to prevent runaway loops. | P0 | ❌ Build |
| 59 | Router / conditional branching (multi-way) | Route to one of N branches based on runtime conditions. More than binary if/else — supports N-way routing with fallback/default branch. | P0 | ❌ Build |
| 60 | Workflow versioning (safe code evolution) | Deploy new sequence logic without breaking running instances. Patch markers allow old instances to follow old path, new instances follow new path. Deprecation tracking for cleanup. | P1 | ❌ Build |
| 61 | Debug mode | Breakpoints on specific steps, step-through execution, pause/resume. Inspect full state at each breakpoint. For development and production debugging. | P1 | ❌ Build |
| 62 | Search attributes | Index running instances by custom attributes (current step, status, definition ID, custom fields). Enable filtering/discovery across thousands of active instances. | P1 | ❌ Build |
| 63 | Template interpolation engine | Resolve `{{path}}` references within step definitions at runtime. Resolution from context, previous step outputs, params, config. Supports expressions and fallback defaults. | P0 | ❌ Build |
| 64 | Pluggable block handler system | Extensible step type registry. Built-in handlers for flow control (parallel, race, loop, router, try-catch). User-defined handlers for domain logic (HTTP, email, LLM, custom). Each handler receives recursive execution capability. | P0 | ❌ Build |
| 65 | Structured context management | Multi-section execution context: data, config, audit, steps (block outputs), workflow metadata, auth, runtime. Sections have different read/write permissions and lifecycle. | P0 | ❌ Build |
| 66 | Output externalization | Large step outputs stored externally (database/object store) with references in execution history. Prevents history/state bloat for long-running instances with large payloads. Configurable size threshold. | P1 | ❌ Build |
| 67 | Checkpointing for long-running instances | Periodic state snapshots for instances that run for days/weeks. On history size threshold, reset execution state while preserving all context and step results. Configurable max history events. | P1 | ❌ Build |
| 68 | Session management | Session-scoped data storage tied to instances. Session lifecycle signals (expired, paused, resumed, completed). Cross-instance session references. | P2 | ❌ Build |
| 69 | Human-in-the-loop steps | Steps that block until a human provides input (approval, review, data entry). Timeout with escalation. Integrates with signals/updates for response delivery. | P1 | ❌ Build |
| 70 | Restart from arbitrary step | Resume a failed/stopped instance from any step, not just the last checkpoint. Previously completed steps provide cached outputs. Selective re-execution of specific branches. | P1 | ❌ Build |
| 71 | Workflow interceptors | Pluggable hooks on workflow lifecycle events: before/after activity, signal received, timer fired, continue-as-new. For observability, logging, custom metrics without modifying core logic. | P2 | ❌ Build |
| 72 | Activity heartbeating | Long-running steps report progress periodically. Engine detects stalled steps (no heartbeat within timeout) and retries on a different worker. Prevents silent step death. | P1 | ❌ Build |
| 73 | Payload compression / custom codecs | Pluggable payload codec layer. Built-in gzip compression for payloads exceeding configurable threshold (e.g. 1KB). Applied transparently on both engine and SDK side. Reduces storage and network overhead. | P1 | ❌ Build |
| 74 | Graceful shutdown | On SIGINT/SIGTERM, stop accepting new work, drain in-flight steps with configurable grace period, persist incomplete state, then exit. Coordinated shutdown of worker, connections, and health probes. | P0 | ❌ Build |
| 75 | Worker tuning / concurrency controls | Configurable max concurrent step executions and max concurrent instance evaluations per worker node. Prevents worker overload under burst traffic. | P1 | ❌ Build |
| 76 | Namespace / environment isolation | Logical isolation of instances, sequences, and state by namespace. Same engine binary serves multiple environments (dev/staging/prod) or tenants without cross-contamination. Distinct from multi-tenancy — namespaces are infra-level, tenants are app-level. | P1 | ❌ Build |
| 77 | Task queue routing | Multiple named task queues per engine. Route specific sequence types or step types to dedicated worker pools. Enables resource isolation (CPU-heavy steps on beefy workers, I/O steps on lightweight ones). | P2 | ❌ Build |

---

## JTBD Analysis

### Core Jobs

| Job | Functional | Emotional | Social |
|-----|-----------|-----------|--------|
| Schedule multi-step sequences reliably | Define sequence, enqueue instances, trust they execute correctly | Confidence my infra won't lose tasks | Look competent to team/investors |
| Scale without rebuilding infra | Handle 1M+ instances without architecture change | Relief — don't have to rewrite scheduling again | Ship features instead of fighting fires |
| Respect downstream rate limits | Engine defers tasks when limits hit, not reject | Safety — won't burn mailbox reputation | Maintain sender reputation with clients |
| React to external events mid-sequence | Signal arrives, instance branches/cancels immediately | Control — system is responsive, not dumb automation | — |
| Resume from crashes without data loss | Process dies, restarts, all instances continue | Trust — sleeping at night | Reliable ops = customer trust |
| Orchestrate complex workflows without Temporal's tax | Parallel, race, try-catch, signals — without history replay, determinism constraints, or activity ceremony | Freedom — write normal code again | Ship features instead of fighting framework |
| See workflow state without boilerplate | Query any instance's context/status via REST, no handler registration | Simplicity — one API call, full visibility | Fewer bugs from missing query handlers |
| Deploy code changes without version patches | New logic applies to new instances, no `patched()` branching, no replay violations | Confidence — deploy without fear of breaking running workflows | Faster iteration, less tech debt |
| Test workflows without external infrastructure | Same engine, embedded SQLite, `test_mode()` — no Temporal server required | Speed — run tests locally in milliseconds | CI/CD pipelines that actually work |

### ODI Scoring (Opportunity)

| Desired Outcome | Importance | Satisfaction (current tools) | Opportunity |
|----------------|------------|------------------------------|-------------|
| Never lose a scheduled task on deploy/crash | 10 | 3 (BullMQ/Redis) | 17 |
| Respect per-mailbox daily send limits automatically | 9 | 2 (all DIY) | 16 |
| Branch a running sequence on external event | 9 | 3 (Temporal complex, BullMQ impossible) | 15 |
| Schedule relative to business days with timezone | 8 | 2 (all DIY) | 14 |
| Enqueue 100K instances in seconds | 8 | 4 (Redis fast, Postgres slow) | 12 |
| Pause/resume entire campaigns atomically | 8 | 3 (manual in most tools) | 13 |
| Get Prometheus metrics without custom instrumentation | 7 | 3 | 11 |
| Embed as library, not run as separate platform | 8 | 2 (Temporal/Inngest = platforms) | 14 |
| Run parallel/race/loop without history bloat | 9 | 3 (Temporal works but O(n) replay, history limits) | 15 |
| Deploy workflow code changes without version patches | 9 | 2 (Temporal requires patched() for every change) | 16 |
| Query workflow state without writing handlers | 8 | 2 (Temporal requires defineQuery for each field) | 14 |
| Call functions without activity registry ceremony | 8 | 2 (Temporal: define, export, proxy, classify errors) | 14 |
| Test workflows without running Temporal server | 8 | 3 (Temporal test env limited, no search attributes) | 13 |
| Visual sequence builder component | 5 | 3 | 7 |

**Highest opportunity (≥ 12):** Crash recovery, per-resource rate limiting, mid-sequence branching, business day scheduling, embeddability, bulk operations. All confirmed P0/P1.

---

## Activation Funnel

### Activation Milestone

**"Buyer schedules 1,000 task instances, kills the process, restarts, and all instances resume correctly."**

This is the aha moment. It proves durability, performance, and the core value proposition in under 10 minutes.

### Funnel Steps

| Step | Description | Target Conversion | TTV | Drop-off Risk |
|------|-------------|-------------------|-----|---------------|
| 1 | Lands on website / GitHub | — | — | Messaging unclear, no benchmark proof |
| 2 | Installs engine (cargo install or docker pull) | 60% of visitors who read docs | < 2 min | Complex install, missing prerequisites |
| 3 | Defines first sequence (YAML or code) | 80% of installers | < 5 min | Bad docs, unclear API surface |
| 4 | Schedules 1,000 instances | 70% of step 3 | < 2 min | Requires running Postgres/SQLite |
| 5 | Kills process, restarts, sees all instances resume | 90% of step 4 | < 1 min | **AHA MOMENT** |
| 6 | Integrates into their app via SDK | 40% of step 5 | < 1 day | SDK quality, migration complexity |
| 7 | Runs in production with real traffic | 50% of step 6 | 1–4 weeks | Edge cases, missing features |
| 8 | Converts to paid license | 30% of step 7 | — | Pricing objection, alternative found |

**Total TTV to aha moment: < 10 minutes.** This is critical for a developer tool.

### Activation Audit Checklist

- [ ] One-command install (docker run or cargo install)
- [ ] "Hello World" sequence runnable in < 5 minutes from first touch
- [ ] SQLite embedded mode for zero-dependency local testing
- [ ] Example sequences for every vertical (outreach, notification, dunning)
- [ ] Benchmark script: "Schedule 100K instances, measure throughput"
- [ ] Kill-and-resume demo built into quickstart guide
- [ ] Interactive API playground or CLI tool for testing
- [ ] Video walkthrough < 3 minutes showing full flow

---

## Unit Economics

| Metric | Target | Benchmark | Notes |
|--------|--------|-----------|-------|
| CAC | < $500 | DevTool avg $200–$1000 | Content + direct outreach, no paid ads initially |
| ARPU | $2,500/month | — | Weighted avg across Starter/Growth/Enterprise |
| LTV | $45,000 | ARPU × 18 months avg retention | Infra products have very low churn once embedded |
| LTV:CAC | 90:1 | > 3x = healthy | Extremely high for infra products with content GTM |
| Gross margin | > 95% | SaaS 70–85% | Binary license, no compute costs |
| Monthly churn | < 1% | Enterprise infra < 0.5% | Switching cost is enormous once embedded |
| Payback period | < 1 month | < 12 months | Content GTM = near-zero CAC per lead |

**Gross margin risk check:** Zero third-party compute or API costs. The product is a binary/library. Margin is effectively 100% minus support labor. This is the best margin profile possible for a software product.

---

## Pricing Model

### Model Fit: Tiered Subscription + Usage Overage (Hybrid)

**Why this model:** Usage-based aligns cost with value (more instances = more value). Tiers provide predictable base revenue and simplify sales. Matches the monetization model reference pattern for "API-first, infra tools."

### Tier Structure

| Dimension | Starter | Growth | Enterprise |
|-----------|---------|--------|------------|
| Monthly price | $500 | $2,500 | $8,000+ |
| Annual price | $5,000/yr ($417/mo) | $25,000/yr ($2,083/mo) | Custom |
| Active instances | Up to 100K | Up to 2M | Unlimited |
| Overage | $5 per 10K instances | $3 per 10K instances | Included |
| Multi-tenancy | ❌ | ✅ | ✅ |
| Resource pools (rotation) | 1 pool | Unlimited | Unlimited |
| Priority support (Slack) | ❌ | ✅ | ✅ (dedicated) |
| Source license | ❌ | ❌ | ✅ |
| SLA | — | 99.5% | 99.9% |
| Node.js SDK | ✅ | ✅ | ✅ |
| Python/Go SDK | ❌ | ✅ | ✅ |
| Audit log | ❌ | ❌ | ✅ |
| **Anchor message** | "Cheaper than 1 week of eng time" | "Replaces 1 FTE backend engineer" | "Replaces your infra team" |

### Pricing Psychology Applied

- **Rule of 3:** Three tiers. Growth is the recommended default (pre-selected on pricing page).
- **Anchor high:** Enterprise price shown first → makes Growth feel reasonable.
- **Annual framing:** Show monthly equivalent on annual plan.
- **Usage metric aligns with value:** More instances = buyer serving more customers = getting more value.

### Upgrade Triggers

- Starter → Growth: Hits 100K instance limit (should happen within 3–6 months for healthy tool)
- Growth → Enterprise: Needs audit log, source license, or SLA
- Free (open core) → Starter: Needs clustering, commercial support, or priority queues

### Expansion Revenue Mechanics

| Mechanic | Description | Expected Impact |
|----------|-------------|-----------------|
| Instance overage | Scales with buyer's growth | High — natural expansion |
| Tier upgrade | Starter → Growth → Enterprise | Medium |
| Add-on: Dashboard UI | $500/month embeddable monitoring component | Medium |
| Add-on: Visual sequence builder | $1,000/month React component | Medium |
| Annual upsell | Monthly → annual at renewal | Medium |
| Multi-product | Same engine, different vertical landing page | High — zero marginal cost |

---

## Growth Loops & Acquisition

| Loop | Active? | Strength | How It Works | Gap / Improvement |
|------|---------|----------|-------------|-------------------|
| Content / SEO | Planned | — | Blog posts on HN, dev blogs: "How we replaced BullMQ with Rust" | Write 2 posts before launch |
| PLG (open core) | Planned | — | MIT-licensed single-node engine → commercial for clustering/multi-tenant | Define free vs paid boundary clearly |
| Direct outreach | Planned | — | Cold email to 50 CTOs of identified outreach tools | Use your own engine to send the sequence |
| Community | Planned | — | Discord for users, GitHub discussions | Launch with first design partners |
| Partnership / integration | Not yet | — | Embed in starter kits, partner with Vercel/Railway for deploy templates | Later (Month 6+) |
| Referral / viral | Not yet | — | "Powered by [Engine]" badge in buyer's admin UI | Free tier incentive |

### Acquisition Channel Plan

| Channel | Priority | Est. CAC | Volume | Scalable? |
|---------|----------|----------|--------|-----------|
| HN / dev content | Now | ~$0 | Medium | No (hit-driven) |
| GitHub open core | Now | ~$0 | Medium | Yes (compounds) |
| Direct CTO outreach | Now | ~$100 | Low | No (founder time) |
| SEO: "BullMQ alternative", "Temporal alternative for campaigns" | Month 3 | ~$50 | Medium | Yes |
| Dev conference talks | Month 6 | ~$200 | Low | No |
| Paid (Reddit, X ads) | Month 9 | ~$300 | Medium | Yes |

---

## Risk Register

| Risk | Category | Likelihood | Impact | Score | Mitigation |
|------|----------|------------|--------|-------|------------|
| Restate adds campaign primitives (rate limits, rotation) | Competitive | 🟡 Medium | 🔴 High | 8 | Ship first. Vertical focus is your moat. They optimize for general-purpose. |
| Temporal adds snapshot resume, removes history limit | Competitive | 🟢 Low | 🔴 High | 6 | Your simplicity + embeddability still wins. Temporal can't un-complex itself. |
| Open-source clone appears | Competitive | 🟡 Medium | 🟡 Medium | 6 | Commercial support + SDK ecosystem + brand = defensible. |
| Market too small (< 200 buyers total) | Market | 🟡 Medium | 🔴 High | 8 | Expand to notification, fintech, healthtech verticals (same engine, different copy). |
| Solo founder bus factor | Business | 🔴 High | 🔴 High | 9 | Enterprise source license de-risks for buyers. Hire Rust contractor for critical path by month 6. |
| Buyer builds their own instead of licensing | Market | 🔴 High | 🟡 Medium | 7 | Make integration so easy that building is never worth it. "10 minutes to aha." |
| Node.js SDK quality blocks adoption | Product | 🟡 Medium | 🔴 High | 8 | Invest heavily in SDK DX. SDK IS the product for most buyers. |

Score: Likelihood (Low=1, Med=2, High=3) × Impact (Low=1, Med=2, High=3)

**Critical (7–9):** Bus factor, market size, SDK quality, Restate competition.

---

## Prioritization Matrix

```text
Score = (Impact × 3) + (Confidence × 2) - (Effort × 1) + (Strategic Fit × 2)
Max = 40 | Ship now ≥ 32 | Next sprint 22–31 | Backlog < 22
```

| # | Feature | Impact | Conf | Effort | Fit | Score | Action |
|---|---------|--------|------|--------|-----|-------|--------|
| 1 | Durable task instances + crash recovery | 5 | 5 | 2 | 5 | 38 | Ship now |
| 2 | Per-resource rate limiting with defer | 5 | 5 | 3 | 5 | 40 | Ship now |
| 3 | Relative delay + business days + jitter | 5 | 5 | 3 | 5 | 40 | Ship now |
| 4 | Conditional branching | 5 | 5 | 3 | 5 | 40 | Ship now |
| 5 | External event triggers (signals) | 5 | 5 | 3 | 5 | 40 | Ship now |
| 6 | Node.js SDK | 5 | 5 | 2 | 5 | 38 | Ship now |
| 7 | Bulk create/pause/cancel | 5 | 4 | 3 | 5 | 37 | Ship now |
| 8 | PostgreSQL backend | 5 | 5 | 3 | 5 | 40 | Ship now |
| 9 | gRPC + REST API | 5 | 5 | 2 | 5 | 38 | Ship now |
| 10 | Structured JSON logging | 4 | 5 | 5 | 4 | 38 | Ship now |
| 11 | Timezone-per-instance | 4 | 5 | 3 | 5 | 37 | Ship now |
| 12 | Retry with configurable backoff | 4 | 5 | 4 | 4 | 37 | Ship now |
| 13 | Resource pools (mailbox rotation) | 4 | 4 | 3 | 5 | 34 | Ship now |
| 14 | Multi-tenancy | 4 | 4 | 2 | 5 | 33 | Ship now |
| 15 | Send window scheduling | 4 | 4 | 3 | 5 | 34 | Ship now |
| 16 | Warmup ramp schedules | 4 | 4 | 3 | 5 | 34 | Ship now |
| 17 | Priority queues | 3 | 4 | 4 | 4 | 31 | Next sprint |
| 18 | SQLite backend | 3 | 4 | 4 | 4 | 31 | Next sprint |
| 19 | Prometheus metrics | 3 | 4 | 4 | 4 | 31 | Next sprint |
| 20 | Webhook event emitter | 3 | 4 | 4 | 4 | 31 | Next sprint |
| 21 | Dead letter queue | 3 | 4 | 4 | 4 | 31 | Next sprint |
| 22 | Python SDK | 3 | 3 | 3 | 4 | 27 | Next sprint |
| 23 | Audit log | 3 | 3 | 3 | 4 | 27 | Next sprint |
| 24 | Concurrency control per key | 3 | 3 | 3 | 4 | 27 | Next sprint |
| 25 | Idempotency keys | 3 | 4 | 4 | 3 | 30 | Next sprint |
| 26 | Bulk re-schedule | 3 | 3 | 4 | 4 | 29 | Next sprint |
| 27 | Storage abstraction trait | 2 | 3 | 3 | 3 | 21 | Backlog |
| 28 | A/B split primitive | 2 | 3 | 3 | 3 | 21 | Backlog |
| 29 | Go SDK | 2 | 2 | 3 | 3 | 18 | Backlog |
| 30 | Dashboard UI | 2 | 2 | 1 | 3 | 16 | Backlog |
| 31 | Visual sequence builder | 2 | 2 | 1 | 3 | 16 | Backlog |
| 32 | Sequence versioning | 2 | 3 | 2 | 3 | 20 | Backlog |
| 33 | Hot code migration | 2 | 2 | 1 | 3 | 15 | Backlog |
| 34 | Dynamic step injection | 2 | 2 | 1 | 3 | 15 | Backlog |
| 35 | Circuit breaker | 2 | 3 | 3 | 3 | 21 | Backlog |
| 36 | Encryption at rest | 2 | 2 | 3 | 3 | 19 | Backlog |
| 37 | SLA timers / deadlines | 2 | 3 | 3 | 3 | 21 | Backlog |
| 38 | Cron-triggered sequences | 2 | 3 | 4 | 3 | 23 | Next sprint |
| 39 | Sub-sequences / composition | 2 | 2 | 2 | 3 | 17 | Backlog |

### Orchestration Layer (Features 51–77)

| # | Feature | Impact | Conf | Effort | Fit | Score | Action |
|---|---------|--------|------|--------|-----|-------|--------|
| 51 | Bidirectional signals | 5 | 5 | 3 | 5 | 37 | Ship now |
| 52 | Live state queries | 5 | 5 | 4 | 5 | 38 | Ship now |
| 53 | Transactional updates | 4 | 4 | 3 | 4 | 33 | Ship now |
| 54 | Parallel execution | 5 | 5 | 2 | 5 | 38 | Ship now |
| 55 | Race execution | 5 | 5 | 2 | 5 | 38 | Ship now |
| 56 | Cancellation scopes | 5 | 5 | 2 | 5 | 38 | Ship now |
| 57 | Try-catch-finally | 5 | 5 | 3 | 5 | 37 | Ship now |
| 58 | Loop / forEach | 5 | 5 | 3 | 5 | 37 | Ship now |
| 59 | Router (multi-way branching) | 5 | 5 | 3 | 5 | 37 | Ship now |
| 60 | Workflow versioning | 3 | 4 | 2 | 4 | 30 | Next sprint |
| 61 | Debug mode | 3 | 3 | 2 | 4 | 26 | Next sprint |
| 62 | Search attributes (JSONB) | 4 | 5 | 4 | 5 | 36 | Ship now |
| 63 | Template interpolation | 5 | 5 | 3 | 5 | 37 | Ship now |
| 64 | Pluggable block handlers | 5 | 5 | 2 | 5 | 38 | Ship now |
| 65 | Structured context management | 5 | 5 | 3 | 5 | 37 | Ship now |
| 66 | Output externalization | 3 | 4 | 3 | 4 | 30 | Next sprint |
| 67 | Checkpointing | 3 | 4 | 3 | 4 | 30 | Next sprint |
| 68 | Session management | 2 | 3 | 2 | 3 | 20 | Backlog |
| 69 | Human-in-the-loop | 4 | 4 | 3 | 4 | 33 | Ship now |
| 70 | Restart from arbitrary step | 3 | 4 | 2 | 4 | 30 | Next sprint |
| 71 | Workflow interceptors | 2 | 3 | 3 | 3 | 21 | Backlog |
| 72 | Activity heartbeating | 3 | 4 | 3 | 4 | 30 | Next sprint |
| 73 | Payload compression | 2 | 4 | 4 | 3 | 25 | Next sprint |
| 74 | Graceful shutdown | 5 | 5 | 4 | 5 | 38 | Ship now |
| 75 | Worker tuning | 3 | 4 | 4 | 4 | 31 | Next sprint |
| 76 | Namespace isolation | 3 | 4 | 3 | 4 | 30 | Next sprint |
| 77 | Task queue routing | 2 | 3 | 3 | 3 | 21 | Backlog |

---

## Roadmap

### 🏃 NOW (Weeks 0–10) — Core Engine + Orchestration Layer + First Design Partners

**Goal:** Shippable engine with both scheduling and orchestration capabilities. 3 design partners embedding in production.

**Scheduling Kernel:**
- Durable task instances with snapshot-based crash recovery
- Step-based execution with memoization (no history replay, no determinism constraints)
- Relative delay scheduling with business days, timezone, jitter
- Conditional branching + external signals
- Per-resource rate limiting with deferred scheduling
- Resource pools with weighted rotation + warmup ramps
- Send window scheduling (timezone-aware)
- Bulk create / pause / resume / cancel
- Multi-tenancy (tenant-scoped everything)

**Orchestration Engine:**
- Execution tree model (parallel, race, loop/forEach, router, try-catch-finally)
- Bidirectional signals (pause, resume, cancel, update context, custom types)
- Live state queries — `GET /instances/{id}/state`, no handler boilerplate
- Structured context management (data, config, audit, steps, runtime sections)
- Template interpolation engine (`{{path}}` resolution from context/outputs/params)
- Pluggable block handler system (built-in flow control + user-defined step types)
- Cancellation scopes (structured concurrency with clean teardown)
- Human-in-the-loop steps with timeout and escalation
- Graceful shutdown with configurable drain period

**Infrastructure:**
- PostgreSQL backend with auto-migration (scheduling + orchestration schemas)
- JSONB metadata indexing (replaces Temporal search attributes — no pre-registration)
- gRPC + REST API
- Node.js SDK (TypeScript, type-safe)
- Rust crate for native embedding
- Structured JSON logging + health check endpoint
- Configurable retry with exponential backoff
- Embedded SQLite test mode (zero external dependencies for testing)

**Launch:**
- Quickstart guide with "10-minute aha" demo
- Benchmark suite (1M instances, memory usage, latency)
- 3 example sequences: cold outreach, notification fallback, dunning
- 1 orchestration example: multi-step approval workflow with parallel + race + signals

### 🚀 NEXT (Weeks 10–22) — Production Hardening + Second SDK

**Goal:** Production-grade reliability. First paying customers.

**Scheduling:**
- Priority queues (high/normal/low)
- Concurrency control per entity
- Bulk re-schedule operations
- Cron-triggered sequence instantiation

**Orchestration:**
- Transactional updates (exactly-once semantics for approvals)
- Workflow versioning (explicit state migration, not determinism patches)
- Debug mode (breakpoints, step-through, state inspection)
- Output externalization (large payloads stored in `externalized_state` table)
- Checkpointing for long-running instances
- Restart from arbitrary step
- Activity heartbeating (detect stalled long-running steps)
- Worker tuning (max concurrent steps/instances per node)
- Namespace / environment isolation
- Payload compression (optional gzip at storage layer)

**Reliability:**
- Dead letter queue with manual retry
- Idempotency keys

**Infrastructure:**
- SQLite embedded backend (for dev/small deploy)
- Prometheus metrics endpoint
- Webhook event emitter
- Python SDK
- Audit log (append-only event journal)
- CLI tool for admin operations
- Docker image + Helm chart

**Go-to-Market:**
- Landing page with vertical-specific sub-pages (/for-outreach, /for-notifications, /for-fintech)
- "Temporal Pain Points We Solve" comparison page
- First 2 case studies published

### 🌍 LATER (Weeks 22+) — Platform Expansion + Enterprise

**Goal:** Enterprise tier. Multi-vertical positioning. Ecosystem.

- Go SDK
- Dashboard UI (embeddable web component)
- Visual sequence builder (React component add-on)
- A/B split primitive
- Sequence versioning + hot code migration
- Dynamic step injection (AI agent use case)
- Sub-sequences / composition
- Session management (session-scoped data, lifecycle signals)
- Workflow interceptors (pluggable lifecycle hooks)
- Task queue routing (route step types to dedicated worker pools)
- Circuit breaker
- SLA timers / deadlines
- Encryption at rest
- Storage abstraction trait (custom backends)
- Clustering / multi-node (enterprise)
- SOC 2 preparation (if pursuing enterprise health/fintech)

---

## Technical Architecture Requirements

### Performance Targets

| Metric | Target | Rationale |
|--------|--------|-----------|
| Task instances per node | 1M+ concurrent active | Must exceed BullMQ/Temporal on single box |
| Scheduling throughput | 10K+ steps/sec | Handles bulk campaign launches |
| Step execution latency (p99) | < 10ms (engine overhead, excluding handler) | Must be invisible to buyers |
| Memory per 1M instances | < 512MB | "Runs on a $50/mo VPS" selling point |
| Crash recovery time | < 5 seconds to resume all instances | Near-instant after restart |
| Bulk create 100K instances | < 3 seconds | CSV import use case |
| Storage per 1M instances | < 2GB PostgreSQL | Predictable DB sizing for buyers |

### Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Language | Rust | Performance, memory safety, no GC pauses, "Rust" as brand signal to technical buyers |
| Execution model | State snapshots per instance, no full history replay | Core differentiator vs Temporal. O(1) resume vs O(n) replay. No determinism constraints — developers write normal code. |
| Orchestration model | Execution tree stored in Postgres, event-driven evaluation | Supports nested parallel/race/loop/try-catch. Tree state IS the workflow state — no separate history to replay. Resume = read tree, find pending nodes, continue. |
| State visibility | State is a Postgres row, queryable via REST + SQL | Eliminates Temporal's query handler ceremony. `GET /instances/{id}/state` returns full context. JSONB metadata queryable without pre-registration. |
| Step invocation | Plain functions, discovered by convention | Eliminates Temporal's activity ceremony (proxyActivities, central registry, ApplicationFailure classification). Steps are functions. Retry is engine-level config, not per-call boilerplate. |
| Versioning | Explicit state migration functions, not determinism patches | New sequence versions apply to new instances. Running instances complete on their version. Breaking changes handled by user-defined migration functions, not `patched()` code forks. |
| Testing | Embedded SQLite mode, same engine binary | `engine.test_mode()` — all side-effect steps return mocks unless overridden. No external server. Search attributes, queries, signals all work identically. |
| Scheduling | Timer wheel + persistent queue | Efficient for millions of future-dated tasks with jitter |
| Storage | Trait-based abstraction, PostgreSQL primary | Most buyers run Postgres. Trait allows SQLite/RocksDB. |
| API transport | gRPC (primary) + REST (secondary) | gRPC for SDK perf, REST for curl/testing/webhooks |
| SDK approach | FFI via C ABI + language-specific wrappers | One core, multiple SDKs without maintaining N codebases |
| Configuration | TOML file + env vars + CLI flags | Standard Rust ecosystem conventions |
| Deployment | Single static binary + Docker image | Minimize ops complexity for buyers |

### Database Schema

#### Scheduling Layer (Campaign Kernel)

```text
sequences
  id              UUID PK
  tenant_id       TEXT
  namespace       TEXT            -- infra-level isolation (dev/staging/prod)
  name            TEXT
  definition      JSONB           -- steps, branches, delays
  version         INTEGER
  created_at      TIMESTAMPTZ

task_instances
  id              UUID PK
  sequence_id     UUID FK
  tenant_id       TEXT
  namespace       TEXT
  state           ENUM (scheduled, running, waiting, paused, completed, failed, cancelled)
  next_fire_at    TIMESTAMPTZ     -- indexed, the scheduling hot path
  priority        SMALLINT
  timezone        TEXT
  metadata        JSONB           -- buyer's custom data, queryable via GIN index
  context         JSONB           -- structured execution context (data, config, audit, runtime)
  created_at      TIMESTAMPTZ
  updated_at      TIMESTAMPTZ

rate_limits
  id              UUID PK
  tenant_id       TEXT
  resource_key    TEXT            -- "mailbox:john@acme.com" or "domain:acme.com"
  max_count       INTEGER
  window_seconds  INTEGER
  current_count   INTEGER
  window_start    TIMESTAMPTZ

resource_pools
  id              UUID PK
  tenant_id       TEXT
  name            TEXT
  rotation        ENUM (round_robin, weighted, random)
  resources       JSONB           -- [{key, weight, daily_cap, warmup_schedule}]
```

#### Orchestration Layer (Workflow Engine)

```text
execution_tree
  id              UUID PK
  instance_id     UUID FK         -- parent task_instance
  block_id        TEXT            -- user-defined block identifier
  parent_id       UUID FK NULL    -- self-referencing for nested blocks (parallel > race > step)
  block_type      TEXT            -- 'step', 'parallel', 'race', 'loop', 'router', 'try-catch'
  branch_index    SMALLINT NULL   -- which branch in parallel/race/router
  state           ENUM (pending, running, completed, failed, cancelled, skipped)
  started_at      TIMESTAMPTZ
  completed_at    TIMESTAMPTZ

block_outputs
  id              UUID PK
  instance_id     UUID FK
  block_id        TEXT            -- matches execution_tree.block_id
  output          JSONB           -- step result, may be NULL if externalized
  output_ref      TEXT NULL       -- reference key if output was externalized (> threshold)
  output_size     INTEGER         -- original size in bytes
  attempt         SMALLINT        -- retry attempt number
  created_at      TIMESTAMPTZ

  UNIQUE (instance_id, block_id)  -- one output per block per instance

externalized_state
  id              UUID PK
  instance_id     UUID FK
  ref_key         TEXT UNIQUE     -- "output:{instance}:{block}:{ts}" or "context:{instance}:{ts}"
  payload         JSONB           -- the actual large payload
  created_at      TIMESTAMPTZ
  expires_at      TIMESTAMPTZ NULL -- optional TTL for cleanup

signal_inbox
  id              UUID PK
  instance_id     UUID FK
  signal_type     TEXT            -- 'pause', 'resume', 'cancel', 'update_context', custom types
  payload         JSONB           -- signal data
  delivered       BOOLEAN DEFAULT FALSE
  created_at      TIMESTAMPTZ
  delivered_at    TIMESTAMPTZ NULL

audit_log (enterprise)
  id              BIGSERIAL PK
  instance_id     UUID
  tenant_id       TEXT
  event_type      TEXT
  block_id        TEXT NULL       -- which block triggered the event
  old_state       TEXT
  new_state       TEXT
  metadata        JSONB
  timestamp       TIMESTAMPTZ
```

#### Indexes

```sql
-- Scheduling hot path
CREATE INDEX idx_instances_fire ON task_instances (next_fire_at) WHERE state = 'scheduled';
CREATE INDEX idx_instances_tenant ON task_instances (tenant_id, state);
CREATE INDEX idx_instances_sequence ON task_instances (sequence_id, state);
CREATE INDEX idx_instances_namespace ON task_instances (namespace, state);

-- Metadata queries (replaces Temporal search attributes — no pre-registration needed)
CREATE INDEX idx_instances_metadata ON task_instances USING GIN (metadata jsonb_path_ops);

-- Orchestration
CREATE INDEX idx_exec_tree_instance ON execution_tree (instance_id, state);
CREATE INDEX idx_exec_tree_parent ON execution_tree (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX idx_block_outputs_instance ON block_outputs (instance_id, block_id);
CREATE INDEX idx_signal_inbox_pending ON signal_inbox (instance_id) WHERE delivered = FALSE;

-- Rate limiting
CREATE INDEX idx_rate_limits_key ON rate_limits (tenant_id, resource_key);

-- Externalized state cleanup
CREATE INDEX idx_externalized_expires ON externalized_state (expires_at) WHERE expires_at IS NOT NULL;
```

### Execution Paths

The engine has two execution paths, both using the same persistence layer.

#### Path 1: Timer-Driven (Scheduling Kernel)

For campaign sequences where steps fire on schedule (delays, send windows, rate limits).

```text
Every tick (configurable, default 100ms):
  1. SELECT instances WHERE next_fire_at <= now() AND state = 'scheduled'
     ORDER BY priority DESC, next_fire_at ASC
     LIMIT batch_size
     FOR UPDATE SKIP LOCKED

  2. For each instance:
     a. Check rate limit for assigned resource
     b. If rate limit allows → set state = 'running', call handler
     c. If rate limit exceeded → calculate next_fire_at from rate limit window, defer
     d. If send window closed → calculate next window open time, defer

  3. Handler returns result:
     a. Persist step result to block_outputs
     b. Evaluate next step (branch condition, delay calculation)
     c. Set next_fire_at based on relative delay + jitter + business days + timezone
     d. Set state = 'scheduled' (or 'waiting' if waiting for signal, or 'completed')
```

#### Path 2: Event-Driven (Orchestration Engine)

For workflow instances with parallel, race, loop, and signal-driven control flow. Steps execute immediately when their preconditions are met — no timer polling.

```text
On instance start or event (signal, step completion, branch resolution):
  1. Load execution_tree for instance
  2. Evaluate current block:
     a. 'step' → call handler function, persist output to block_outputs, advance
     b. 'parallel' → spawn all branches, set state = 'running', wait for all to complete
     c. 'race' → spawn all branches, first to complete/succeed wins, cancel losers
     d. 'loop' / 'forEach' → evaluate condition, execute body, increment, repeat or exit
     e. 'router' → evaluate conditions, route to matching branch
     f. 'try-catch' → execute try block, on failure execute catch, always run finally
  3. On step completion:
     a. Persist block output (externalize if > threshold)
     b. Update execution_tree node state
     c. Check parent: if parallel → are all siblings done? If race → cancel siblings
     d. If parent resolved → recurse up the tree
     e. If next block exists → execute immediately (no timer)
     f. If next block has delay → set next_fire_at, switch to timer-driven path
  4. On all blocks complete → set instance state = 'completed'

State is always the execution_tree + block_outputs + context.
Resume = read current tree state, find 'running'/'pending' nodes, continue.
No replay. No history. O(1) resume.
```

#### Path 3: Signal Processing

```text
POST /instances/{id}/signal { signal_type, payload }

  1. Insert into signal_inbox
  2. Load instance + execution_tree
  3. Find waiting block(s) that match signal_type
  4. Deliver payload:
     a. If waiting for signal → unblock, evaluate branch condition, advance
     b. If 'pause' → set instance state = 'paused', stop all in-flight steps
     c. If 'resume' → set state = 'running', re-enter orchestration path
     d. If 'cancel' → cancel all in-flight steps, set state = 'cancelled'
     e. If 'update_context' → merge payload into instance context, continue
  5. Mark signal as delivered
  6. Emit webhook event: instance.signal_received

State queries (no handler boilerplate):
  GET /instances/{id}/state → returns context + execution_tree + block_outputs
  GET /instances?metadata.campaign_id=X → JSONB query on metadata, no pre-registration
```

---

## Documentation Requirements

### Developer Docs (docs site)

| Section | Content | Priority |
|---------|---------|----------|
| Quickstart | "10 minutes to aha" — install, define sequence, schedule 1K instances, kill/resume | P0 |
| Core Concepts | Instances, sequences, steps, signals, rate limits, resource pools | P0 |
| API Reference | gRPC + REST, auto-generated from protobuf/OpenAPI | P0 |
| SDK Guides | Node.js, Python, Go — each with language-idiomatic examples | P0/P1/P2 |
| Configuration | TOML reference, env vars, CLI flags | P0 |
| Storage | PostgreSQL setup, SQLite for dev, schema details | P0 |
| Deployment | Docker, Kubernetes (Helm), bare metal | P1 |
| Observability | Prometheus metrics reference, Grafana dashboard template | P1 |
| Migration Guide | "From BullMQ", "From Temporal", "From custom Postgres" | P1 |
| Temporal Migration Deep Dive | Side-by-side code comparison: activity ceremony → plain function, query handler → REST call, continue-as-new → automatic, patched() → just deploy | P1 |
| Orchestration Guide | Parallel, race, loop, try-catch — with execution tree visualization | P0 |
| Vertical Guides | "Building cold email sequences", "Building notification fallbacks", "Building dunning flows" | P1 |
| Architecture | Internals: scheduling hot path, orchestration tree, storage layer, rate limit algorithm | P2 |

### Marketing Site

| Page | Purpose | Priority |
|------|---------|----------|
| Homepage | One-sentence value prop, benchmark proof, 3 use cases | P0 |
| /for-outreach | Campaign tool builders — Instantly/Smartlead replacement pitch | P0 |
| /for-notifications | Notification platform builders — Novu/Knock engine pitch | P1 |
| /for-fintech | Dunning, KYC, compliance flows — audit trail pitch | P1 |
| /for-healthtech | Patient pathways, clinical trials — long-running + compliance | P2 |
| /pricing | 3 tiers, annual toggle, FAQ | P0 |
| /benchmark | Reproducible performance benchmarks vs BullMQ, Temporal, Postgres polling | P0 |
| /case-studies | Design partner stories | P1 |
| /vs-temporal | "Temporal without the tax" — side-by-side comparison of pain points and how this engine solves them | P0 |
| /blog | Technical content: "How we...", "Why Rust...", "Replacing BullMQ...", "2,400 lines of Temporal workarounds we eliminated" | P0 |

---

## Top 7 Recommendations

### 1. Ship both layers together — scheduling kernel + orchestration engine

**What:** The P0 feature set across both layers: scheduling (durable instances, delays, rate limits, resource pools) AND orchestration (parallel, race, try-catch, signals, queries, template interpolation, pluggable handlers). PostgreSQL backend, Node.js SDK.

**Why:** The scheduling kernel alone competes with BullMQ. Adding orchestration creates a second acquisition channel — teams escaping Temporal. Two TAMs, one engine. The orchestration layer also makes the scheduling kernel dramatically more powerful (campaigns with parallel steps, race conditions, error recovery).

**Expected outcome:** 3 design partners — at least 1 from the "escaping Temporal" segment.

**How to measure:** Number of instances in production + number of orchestration blocks used.

### 2. Publish "2,400 Lines of Temporal Workarounds We Eliminated" on HackerNews

**What:** Technical post with real code comparisons: activity ceremony → plain function, query handlers → REST endpoint, continue-as-new orchestration → automatic snapshot, `patched()` branching → just deploy. Include benchmarks: "1M instances, 128MB RAM, p99 < 5ms."

**Why:** HN loves two things: Rust benchmarks and Temporal criticism. This post targets both audiences simultaneously. The "lines of workaround code" angle is concrete and relatable — every Temporal user has felt these pain points.

**Expected outcome:** 500+ GitHub stars in first week. Inbound from both campaign tool builders AND Temporal escapees.

**How to measure:** GitHub stars, inbound leads segmented by "BullMQ replacement" vs "Temporal replacement."

### 3. Find 3 design partners — at least 1 Temporal user

**What:** 2 early-stage outreach/notification tool founders (scheduling kernel buyers) + 1 team currently running Temporal in production (orchestration buyer). Offer 6 months free in exchange for logo, quote, co-authored case study, and API feedback.

**Why:** Validates both layers of the product. The Temporal migration partner is especially valuable — their migration story becomes the most compelling case study for the /vs-temporal page.

**Expected outcome:** Real production usage across both layers. 3 logos. Temporal migration case study.

**How to measure:** "Are they running it in production and not complaining?"

### 4. Make the SDK the product, not the engine

**What:** Invest 30% of development time in Node.js SDK quality — types, error messages, examples, autocomplete. The SDK must demonstrate the "no ceremony" advantage: define a step as a plain function, no registry, no proxyActivities, no ApplicationFailure classification.

**Why:** Buyers interact with the SDK, not the engine binary. The SDK IS the anti-Temporal pitch in code form. If defining a step still feels like ceremony, the positioning falls apart.

**Expected outcome:** "It just works" feedback. < 1 hour from npm install to first orchestrated workflow running.

**How to measure:** Time-to-first-workflow for new users. SDK-related GitHub issues.

### 5. Position with two landing pages, not one label

**What:** Don't pick "scheduling kernel" OR "workflow engine." Use both: `/for-campaigns` speaks scheduling kernel language; `/vs-temporal` speaks orchestration language. Homepage bridges both.

**Why:** The original "never say workflow engine" advice was correct for a scheduling-only product. With orchestration, you need to meet Temporal users where they search — and they search for "Temporal alternative." Let the landing page do the positioning, not the category label.

**Expected outcome:** Two distinct acquisition funnels. Campaign builders and Temporal escapees both find the right page.

**How to measure:** Traffic and conversion by landing page.

### 6. Ship embedded test mode as a first-class feature

**What:** `engine.test_mode()` with SQLite backend, same engine binary, all side-effect steps return mocks unless overridden. Signals, queries, search attributes all work identically to production.

**Why:** Testing is Temporal's weakest point. Teams maintain hundreds of lines of dry-run infrastructure, test harnesses, and mock registries. "Same engine, `test_mode()`, done" is a powerful demo that sells itself.

**Expected outcome:** "Our CI pipeline went from 5 minutes to 10 seconds" testimonials.

**How to measure:** Test mode adoption rate among design partners.

### 7. Build the /vs-temporal comparison page before launch

**What:** Side-by-side code comparison page: (a) Activity ceremony: 15 lines of Temporal boilerplate → 3 lines, (b) State query: defineQuery + handler → GET /instances/{id}/state, (c) History bloat: continue-as-new + externalization → automatic, (d) Code changes: patched() branching → just deploy, (e) Testing: Temporal server + test harness → engine.test_mode().

**Why:** Temporal users Google "Temporal alternative" when frustrated. This page captures that traffic with concrete, code-level proof that the pain goes away. More effective than abstract positioning.

**Expected outcome:** Top 3 Google result for "Temporal alternative" within 6 months (low competition keyword).

**How to measure:** Search ranking, organic traffic, conversion from /vs-temporal to trial.