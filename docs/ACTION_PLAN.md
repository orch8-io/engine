# Orch8.io — Action Plan

> **See also:** [PRD](../README.md) | [Technical Details](TECHNICAL_DETAILS.md) | [Business Overview](BUSINESS_PRODUCT_DETAILS.md) | [Sales Pitch](SALES_PITCH.md) | [Development Stages](DEVELOPMENT_STAGES.md)
>
> **Stage mapping:** Week 0-2 = Stage 0, Week 2-6 = Stage 1, Week 6-10 = Stage 2, Week 10-14 = Stage 3, Week 14-18 = Stage 4, Week 18-22 = Stage 5, Week 22+ = Stage 6

## Executive Summary

This action plan converts the PRD into concrete, time-bound actions across three parallel tracks:
1. **Product Development** — building the engine
2. **Go-To-Market** — acquiring design partners and customers
3. **Content & Positioning** — establishing market presence

---

## Track 1: Product Development

### Weeks 0-2: Foundation

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 0 | Set up Rust workspace | Dev | `orch8-core`, `orch8-storage`, `orch8-api`, `orch8-cli` crates |
| 0 | Configure CI/CD (GitHub Actions) | Dev | Build, test, lint pipeline |
| 0 | Create `docker-compose.yml` with Postgres | Dev | Local dev environment |
| 1 | Define storage trait (`StorageBackend`) | Dev | Rust trait with all required methods |
| 1 | Implement Postgres storage backend | Dev | Connection pool, migrations, table creation |
| 1 | Create database schema (all tables + indexes) | Dev | Migration scripts |
| 2 | Build configuration system (TOML + env + CLI) | Dev | Config parsing and validation |
| 2 | Implement health check endpoints | Dev | `/health/ready`, `/health/live` |
| 2 | Add structured JSON logging | Dev | Every state transition logged |

**Exit Gate:** Engine starts, connects to Postgres, runs migrations, responds to health checks.

---

### Weeks 2-6: Core Scheduling Engine

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 2 | Implement timer wheel | Dev | Efficient scheduling data structure |
| 2 | Build tick loop (100ms default) | Dev | Configurable interval |
| 3 | Implement task instance lifecycle | Dev | State machine: scheduled → running → completed/failed |
| 3 | Build `FOR UPDATE SKIP LOCKED` batch selection | Dev | Concurrent-safe instance claiming |
| 3 | Implement step handler invocation | Dev | Call handler, persist result |
| 4 | Implement step memoization | Dev | Persist to `block_outputs`, no replay |
| 4 | Build relative delay calculation | Dev | Delay from previous completion + jitter |
| 4 | Add business day awareness | Dev | Skip weekends, holiday calendars |
| 5 | Implement timezone-per-instance | Dev | Per-instance timezone for send window |
| 5 | Build conditional branching | Dev | If/else based on step results |
| 5 | Implement step timeout enforcement | Dev | Fail step if exceeds timeout |
| 6 | Build graceful shutdown (SIGINT/SIGTERM) | Dev | Drain, persist, exit |
| 6 | Implement crash recovery | Dev | Resume from checkpoint on restart |
| 6 | Build sequence definition API (code + YAML) | Dev | Define sequences programmatically |

**Exit Gate:** Schedule 1K instances → kill process → restart → all instances resume correctly.

---

### Weeks 6-10: Rate Limits + API + First SDK

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 6 | Implement per-resource rate limiting | Dev | Resource key, max count, window |
| 7 | Build defer scheduling | Dev | Recalculate next_fire_at on rate limit hit |
| 7 | Implement resource pools | Dev | Round-robin, weighted, random rotation |
| 8 | Build warmup ramp schedules | Dev | Gradual capacity increase |
| 8 | Implement send window scheduling | Dev | Fire only during configured hours |
| 8 | Define gRPC protobuf services | Dev | Service definitions |
| 9 | Implement gRPC server | Dev | All RPC methods working |
| 9 | Implement REST API | Dev | HTTP endpoints, OpenAPI spec |
| 9 | Set up NAPI-RS for Node.js bindings | Dev | Rust → Node.js FFI |
| 10 | Build Node.js SDK | Dev | TypeScript client, type-safe |
| 10 | Write SDK documentation | Dev | Examples, API reference |
| 10 | Build multi-tenancy support | Dev | Tenant-scoped queries, per-tenant limits |

**Exit Gate:** Node.js SDK schedules instances via gRPC/REST. Rate limits enforced across resources.

---

### Weeks 10-14: Orchestration Layer

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 10 | Design execution tree model | Dev | Tree structure, node types |
| 11 | Implement execution tree persistence | Dev | Store/load from Postgres |
| 11 | Build tree evaluation engine | Dev | Event-driven (not polled) |
| 11 | Implement parallel execution | Dev | Spawn all branches, wait for all |
| 12 | Implement race execution | Dev | First wins, cancel losers |
| 12 | Build cancellation scopes | Dev | Propagation, non-cancellable scopes |
| 12 | Implement try-catch-finally | Dev | Error handling, recovery, finalization |
| 13 | Build loop/forEach | Dev | Iterate over collections, condition cap |
| 13 | Implement router (multi-way branching) | Dev | N-way routing, fallback |
| 13 | Build bidirectional signals | Dev | Pause, resume, cancel, update_context, custom |
| 14 | Implement live state queries | Dev | `GET /instances/{id}/state`, no boilerplate |
| 14 | Build structured context management | Dev | Multi-section context with permissions |
| 14 | Implement template interpolation | Dev | `{{path}}` resolution, expressions |
| 14 | Build pluggable block handler system | Dev | Built-in + user-defined handlers |
| 14 | Implement human-in-the-loop steps | Dev | Block until input, timeout, escalation |

**Exit Gate:** Multi-block workflow with parallel + race + signals executes correctly. Query returns full state.

---

### Weeks 14-18: Production Hardening

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 14 | Implement bulk create | Dev | 50K+ instances in one call |
| 15 | Build bulk pause/resume/cancel | Dev | Filter-based operations |
| 15 | Implement bulk re-schedule | Dev | Shift by ±N hours |
| 15 | Build configurable retry | Dev | Per-step policy, exponential backoff |
| 16 | Implement dead letter queue | Dev | Inspection API, manual retry |
| 16 | Build priority queues | Dev | High/normal/low ordering |
| 16 | Implement concurrency control per key | Dev | Max N instances per entity |
| 17 | Build idempotency keys | Dev | Prevent duplicate execution |
| 17 | Implement Prometheus metrics | Dev | All metrics + Grafana dashboard |
| 17 | Build webhook event emitter | Dev | step.completed, instance.failed, retry |
| 18 | Implement embedded test mode | Dev | `engine.test_mode()` with SQLite |
| 18 | Build cron-triggered sequences | Dev | Recurring instantiation |

**Exit Gate:** 100K instances created in < 3s. Prometheus scraping. Webhooks firing. Tests pass in CI.

---

### Weeks 18-22: Second SDK + Polish

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 18 | Implement SQLite backend | Dev | Embedded storage, same schema |
| 19 | Build Python SDK | Dev | Type-safe, async support |
| 19 | Write Python SDK docs | Dev | Examples, API reference |
| 20 | Build CLI tool | Dev | Admin ops, deployment, health |
| 20 | Create Docker image | Dev | < 50MB, health check |
| 20 | Create Helm chart | Dev | K8s deployment |
| 21 | Implement audit log | Dev | Append-only journal |
| 21 | Build workflow versioning | Dev | State migration, not determinism patches |
| 21 | Implement debug mode | Dev | Breakpoints, step-through |
| 22 | Build output externalization | Dev | Large outputs in separate table |
| 22 | Implement checkpointing | Dev | Periodic snapshots, history reset |
| 22 | Create landing pages | Dev | Homepage, /for-outreach, /pricing, /benchmark, /vs-temporal |

**Exit Gate:** Docker deploys. Python SDK works. Landing pages live.

---

### Weeks 22+: Enterprise + Scale

| Action | Owner | Deliverable |
|--------|-------|-------------|
| Build Go SDK | Dev | Idiomatic Go client |
| Build dashboard UI | Dev | Web UI, embeddable component |
| Build visual sequence builder | Dev | React drag-and-drop |
| Implement clustering | Dev | Multi-node with SKIP LOCKED |
| Build A/B split primitive | Dev | Variant routing |
| Implement session management | Dev | Session-scoped data, lifecycle |
| Build circuit breaker | Dev | Pause failing step types |
| Implement SLA timers | Dev | Per-step deadlines, escalation |
| Implement encryption at rest | Dev | Encrypt instance data |
| Prepare SOC 2 documentation | Dev | Audit trail, security docs |

---

## Track 2: Go-To-Market

### Weeks 0-4: Design Partner Recruitment

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 0 | Identify 50 target companies | Founder | List of CTOs at outreach/notification tools |
| 0 | Draft design partner offer | Founder | 6 months free for logo, quote, case study |
| 1 | Send first batch of cold emails (25) | Founder | Emails sent using own engine |
| 1 | Set up LinkedIn outreach | Founder | Connection requests + messages |
| 2 | Send second batch of cold emails (25) | Founder | Emails sent |
| 2 | Follow up on first batch | Founder | Follow-up emails sent |
| 3 | Schedule intro calls with interested prospects | Founder | 5-10 calls booked |
| 3 | Prepare demo for intro calls | Dev | Working demo with kill/resume |
| 4 | Close 2 design partners (outreach/notification) | Founder | Signed agreements |
| 4 | Identify 10 Temporal-using companies | Founder | List from LinkedIn, GitHub, Temporal community |
| 4 | Reach out to Temporal users | Founder | "Escape Temporal's tax" messaging |

**Exit Gate:** 2 design partners signed (outreach/notification). 1 Temporal user in conversation.

---

### Weeks 4-10: Content & Launch Preparation

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 4 | Draft HN post: "Analyzing 2,400 Lines of Temporal Workarounds in One Production Codebase" | Founder | Draft ready |
| 5 | Build benchmark suite | Dev | 1M instances, memory, latency, vs BullMQ/Temporal |
| 5 | Write quickstart guide | Dev | "10 minutes to aha" tutorial |
| 6 | Create example sequences | Dev | Cold outreach, notification fallback, dunning |
| 6 | Record demo video (< 3 min) | Founder | Kill-and-resume walkthrough |
| 7 | Write "From BullMQ" migration guide | Dev | Side-by-side code comparison |
| 7 | Write "From Temporal" migration guide | Dev | Activity ceremony → plain function, etc. |
| 8 | Build /vs-temporal comparison page | Dev | Code comparisons, pain point table |
| 8 | Prepare GitHub repository | Dev | README, benchmarks, install instructions |
| 9 | Finalize design partner integrations | Dev + Founder | Partners embedding engine |
| 10 | Publish HN post | Founder | Post live on HackerNews |
| 10 | Announce on X/LinkedIn | Founder | Social posts with benchmark proof |

**Exit Gate:** HN post published. GitHub repo public. Benchmark page live.

---

### Weeks 10-16: Launch & Early Traction

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 10 | Close 1 Temporal design partner | Founder | Signed agreement |
| 11 | Support design partner integrations | Dev | API feedback loop |
| 11 | Monitor GitHub stars and issues | Dev + Founder | Respond to all issues within 24h |
| 12 | Publish first case study (draft) | Founder | Design partner story |
| 12 | Write SEO blog posts | Founder | "BullMQ alternative", "Temporal alternative" |
| 13 | Set up Discord community | Founder | Server with channels: general, help, feedback |
| 13 | Engage with inbound leads | Founder | Respond to all HN/X/LinkedIn comments |
| 14 | Collect design partner testimonials | Founder | Quotes for landing page |
| 15 | Publish case study #1 | Founder | Live on website |
| 16 | Analyze activation funnel | Founder | Where do trial users drop off? |
| 16 | Optimize onboarding based on data | Dev | Fix drop-off points |

**Exit Gate:** 3 design partners in production. 500+ GitHub stars. First inbound leads.

---

### Weeks 16-22: Scale Acquisition

| Week | Action | Owner | Deliverable |
|------|--------|-------|-------------|
| 16 | Launch landing pages | Dev | /for-outreach, /for-notifications, /for-fintech |
| 17 | Publish case study #2 | Founder | Temporal migration story |
| 17 | Start SEO content cadence (1 post/week) | Founder | Blog posts targeting keywords |
| 18 | Apply to dev conference talks | Founder | CFP submissions |
| 18 | Build Vercel/Railway deploy templates | Dev | One-click deploy |
| 19 | Launch referral program | Founder | "Powered by Orch8" badge incentive |
| 20 | Publish Python SDK announcement | Dev + Founder | HN, X, Python subreddit |
| 20 | Begin paid ads test (Reddit, X) | Founder | Small budget, measure CAC |
| 21 | Collect and publish benchmarks vs competitors | Dev | Updated /benchmark page |
| 22 | Review pricing with early customers | Founder | Adjust if needed based on feedback |
| 22 | Prepare Enterprise sales materials | Founder | Deck, case studies, ROI calculator |

**Exit Gate:** Landing pages converting. SEO traffic growing. Paid CAC < $500.

---

## Track 3: Content & Positioning

### Core Content Assets

| Asset | Target Date | Channel | Purpose |
|-------|-------------|---------|---------|
| "Analyzing 2,400 Lines of Temporal Workarounds" | Week 10 | HackerNews | Viral tech post, 500+ GitHub stars |
| Quickstart Guide ("10 min to aha") | Week 5 | Docs site | Activation |
| Benchmark Suite | Week 5 | Website + GitHub | Proof of performance |
| /vs-temporal Comparison Page | Week 8 | Website | Capture "Temporal alternative" searches |
| "From BullMQ" Migration Guide | Week 7 | Blog | Capture "BullMQ alternative" searches |
| "From Temporal" Migration Guide | Week 7 | Blog | Capture "Temporal alternative" searches |
| Case Study #1 (Outreach Tool) | Week 15 | Website | Social proof |
| Case Study #2 (Temporal Escapee) | Week 17 | Website | Social proof for orchestration buyers |
| Demo Video (< 3 min) | Week 6 | YouTube, X | Visual proof of kill/resume |
| Grafana Dashboard Template | Week 17 | GitHub | Community contribution |
| Docker/Railway Deploy Templates | Week 18 | GitHub | Reduce time-to-first-workflow |

### SEO Keyword Strategy

| Keyword | Target Page | Priority |
|---------|-------------|----------|
| "BullMQ alternative" | /vs-temporal (includes BullMQ section) | High |
| "Temporal alternative" | /vs-temporal | High |
| "durable workflow engine" | Homepage | Medium |
| "cold email scheduling" | /for-outreach | Medium |
| "notification orchestration" | /for-notifications | Medium |
| "workflow engine Rust" | Homepage, /benchmark | Medium |
| "Temporal continue as new alternative" | /vs-temporal | Low (long-tail) |
| "job queue with rate limiting" | /for-outreach | Low |

### Social Proof Strategy

| Milestone | Action | Timing |
|-----------|--------|--------|
| First design partner live | Publish quote on homepage | Week 12 |
| 100 GitHub stars | Add badge to README | As it happens |
| First case study | Publish on website, announce on social | Week 15 |
| 500 GitHub stars | Blog post: "What we learned from 500 developers" | As it happens |
| Second case study (Temporal) | Publish, submit to HN | Week 17 |
| First paying customer | Internal celebration, prepare testimonial | Week 16+ |

---

## Weekly Cadence

### Every Week

| Activity | Duration | Owner |
|----------|----------|-------|
| Review development stage progress | 30 min | Dev |
| Review GTM metrics (stars, leads, conversations) | 30 min | Founder |
| Update this action plan | 15 min | Dev + Founder |
| Respond to all GitHub issues | As needed | Dev |
| Engage with community (Discord, X, HN) | 30 min/day | Founder |

### Every 2 Weeks

| Activity | Duration | Owner |
|----------|----------|-------|
| Demo working engine to design partners | 30 min | Dev + Founder |
| Review activation funnel metrics | 30 min | Founder |
| Adjust priorities based on partner feedback | 30 min | Dev + Founder |
| Publish blog post or update docs | 2 hours | Founder |

### Every 4 Weeks

| Activity | Duration | Owner |
|----------|----------|-------|
| Review risk register | 1 hour | Dev + Founder |
| Assess competitive landscape | 1 hour | Founder |
| Review and adjust pricing if needed | 1 hour | Founder |
| Plan next 4 weeks in detail | 2 hours | Dev + Founder |

---

## Success Milestones

| Milestone | Target Date | Metric |
|-----------|-------------|--------|
| Engine handles kill/resume | Week 6 | 1K instances survive restart |
| First SDK functional | Week 10 | Node.js SDK schedules via API |
| First design partner signed | Week 4 | Signed agreement |
| 3 design partners in production | Week 14 | Running real traffic |
| HN post published | Week 10 | Post on HackerNews front page |
| 500 GitHub stars | Week 14 | GitHub star count |
| First inbound lead (non-outreach) | Week 12 | Organic interest |
| Landing pages live | Week 22 | Website public |
| First paying customer | Week 16-20 | Revenue |
| $10K MRR | Month 9 | Monthly recurring revenue |
| $50K MRR | Month 15 | Monthly recurring revenue |
| 50 paying customers | Month 18 | Customer count |

---

## Immediate Next Actions (This Week)

### Day 1-2
- [ ] Create GitHub repository (private)
- [ ] Set up Rust workspace with `cargo init`
- [ ] Configure GitHub Actions (build + test)
- [ ] Create `docker-compose.yml` with Postgres
- [ ] Draft list of 50 target companies for design partner outreach

### Day 3-4
- [ ] Define `StorageBackend` trait in Rust
- [ ] Create database schema SQL (all tables)
- [ ] Implement basic Postgres connection pool
- [ ] Draft design partner outreach email template

### Day 5-7
- [ ] Implement first migration
- [ ] Build health check endpoint skeleton
- [ ] Send first 25 cold emails to target CTOs
- [ ] Set up project management (GitHub Projects or Linear)

---

## Contingency Plans

| Scenario | Trigger | Response |
|----------|---------|---------|
| Timer wheel doesn't scale to 1M | Week 6 benchmark fails | Investigate alternative (hierarchical timer, external scheduler) |
| No design partner interest by Week 6 | 0 signed after 50 emails | Revise messaging, expand target list to fintech/healthtech |
| Node.js SDK quality issues | Design partner complaint | Dedicate 1 full week to SDK polish, add more tests |
| Restate announces competing feature | News/blog post | Accelerate launch, emphasize vertical-specific features |
| Solo founder burnout risk | Working > 60hr/week for 3+ weeks | Hire Rust contractor for non-critical path (tooling, tests) |
| Postgres bottleneck at scale | Benchmark shows < 500K instances | Investigate connection pooling optimization, consider read replicas |
| HN post underperforms | < 100 points, < 100 stars | Try alternative angles: "Building in Rust", "Why we abandoned Temporal" |
