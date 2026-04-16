# Orch8 — Product Overview

## What Is Orch8?

Orch8 is a **self-hosted, durable task orchestration engine** built in Rust. It schedules, branches, and reliably executes multi-step workflows at scale — with first-class support for retries, concurrency control, rate limiting, and polyglot handler execution.

**Core value proposition:** Define complex workflows as composable JSON sequences. Orch8 guarantees every step either completes, retries, or surfaces in a dead-letter queue. No lost work. No silent failures.

**Single binary, one dependency.** PostgreSQL. No Redis. No Kafka. No queue infrastructure.

---

## The Problem

Companies that send emails, push notifications, or manage customer lifecycle workflows face a universal problem: **scheduling at scale breaks**.

- Tasks get lost during server restarts
- Rate limits get violated (burning sender reputation)
- Complex branching logic becomes impossible to maintain
- Engineers spend months building custom infrastructure instead of shipping features

Existing solutions are either:
- **Too simple** (BullMQ, Redis queues) — lose tasks, no branching
- **Too complex** (Temporal) — designed for short-lived distributed workflows, not month-long campaigns. Teams end up with ~2,400 lines of workaround code

---

## Key Capabilities

| Capability | Details |
|-----------|---------|
| **7 composable block types** | Step, Parallel, Race, Router, TryCatch, Loop, ForEach |
| **Durable execution** | Snapshot-based state, crash recovery, step memoization |
| **External workers** | Write handlers in any language via pull-based REST API |
| **Retry & backoff** | Per-step configurable exponential backoff |
| **Rate limiting** | Per-resource sliding window, defers instead of rejects |
| **Concurrency control** | Per-entity key with position-based scheduling |
| **Cron scheduling** | Recurring workflows with timezone support |
| **Signals** | Pause, resume, cancel, update context on running instances |
| **Multi-tenancy** | Tenant-scoped queries, per-tenant rate limits |
| **Observability** | Prometheus metrics, structured logging, webhooks, DLQ |
| **Priority queues** | 4 levels: Low, Normal, High, Critical |
| **Bulk operations** | Create, pause, resume, cancel, reschedule at scale |

### Advanced Features

| Feature | Description |
|---------|-------------|
| Dynamic step injection | Add steps to running instances at runtime (AI agent support) |
| Sub-sequences | Step invokes another sequence as sub-workflow |
| Session management | Session-scoped data with lifecycle |
| Workflow interceptors | Hooks on lifecycle events (before/after step, on-signal) |
| Task queue routing | Route step types to dedicated worker pools |
| Circuit breaker | Pause failing handler types with cooldown |
| A/B split | Deterministic hash-based variant routing |
| Debug mode | Breakpoints, step-through execution |
| Audit log | Append-only state transition journal |

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Scheduler tick | 10 ticks/second (100ms, configurable) |
| Instances per tick | Up to 256 (configurable) |
| Max concurrent steps | 128 (configurable semaphore) |
| DB connection pool | 64 connections |
| Worker heartbeat timeout | 60 seconds |
| Shutdown grace period | 30 seconds |
| Priority levels | 4 |
| Instance states | 7 |
| Block types | 7 (+SubSequence) |
| Built-in handlers | 4 (noop, log, sleep, http_request) |

---

## Target Customers

| Segment | Problem They Pay To Solve |
|---------|---------------------------|
| **Outreach Tools** | Sequences break at scale, can't branch mid-flow |
| **Notification Platforms** | Multi-channel sequences with rate limits |
| **Solo Founders** | Don't want to spend 3 months on scheduling infrastructure |
| **Fintech/Healthtech** | Compliance, zero lost tasks, month-long workflows |
| **Teams Escaping Temporal** | Same capabilities, 50%+ less infrastructure code |
| **AI Agent Frameworks** | Need durable execution for long-running agent tasks |

---

## Competitive Landscape

| Competitor | Why They Lose |
|------------|---------------|
| **BullMQ/Redis** | Lose tasks on restart, no branching, no rate limit deferral |
| **Temporal** | Complex, requires workarounds, history limits, coding restrictions |
| **Restate** | General-purpose only, no campaign-specific features |
| **Inngest** | Cloud-hosted, not embeddable, limited rate limiting |
| **Custom Postgres** | Takes 3-6 months of engineering, breaks at scale |

**Unique advantage:** No competitor combines embeddable library + campaign primitives + workflow orchestration + Rust performance + snapshot-based resume.

---

## Pricing Strategy

| Tier | Price | Active Workflows |
|------|-------|------------------|
| **Starter** | $500/month | Up to 100K |
| **Growth** | $2,500/month | Up to 2M |
| **Enterprise** | $8,000+/month | Unlimited + source license |

**Why it works:** Cheaper than 1 backend engineer ($120K/year vs $30K/year Growth tier). Usage-based alignment — customers pay more as they grow.

---

## Go-To-Market

**Phase 1 (Months 0-3):** Direct outreach — HN technical post, cold email CTOs, design partners.

**Phase 2 (Months 3-6):** Content marketing — SEO for "BullMQ alternative" / "Temporal alternative", vertical landing pages, case studies.

**Phase 3 (Months 6+):** Ecosystem — open-core strategy, conference talks, integration partnerships.

---

## The Strategic Bet: AI Agent Execution Layer

Every AI agent framework (LangChain, CrewAI, AutoGen, Mastra) needs durable execution. Orch8 already has: dynamic step injection, human-in-the-loop pauses, branching on output, embeddable design, crash recovery.

**Positioning:** "The SQLite of Workflow Engines" — single binary you embed in your app.

**Timing window:** 6-12 months before this is obvious. Nobody else is shipping a single Rust binary that AI agent frameworks can embed.
