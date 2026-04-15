# Orch8.io — Business Product Overview

> **See also:** [PRD](../README.md) | [Technical Details](TECHNICAL_DETAILS.md) | [Sales Pitch](SALES_PITCH.md) | [Action Plan](ACTION_PLAN.md) | [Development Stages](DEVELOPMENT_STAGES.md)

## What Is It?

**Orch8.io** is a commercial software engine that helps companies reliably schedule and run multi-step automated workflows — things like cold email sequences, notification cascades, customer onboarding flows, and payment dunning sequences.

Think of it as the "plumbing" that SaaS companies build from scratch (taking 3-6 months of engineering time) but can instead license and embed in 10 minutes.

---

## The Problem It Solves

Companies that send emails, push notifications, or manage customer lifecycle workflows face a universal problem: **scheduling at scale breaks**.

- Tasks get lost during server restarts
- Rate limits get violated (burning sender reputation)
- Weekend/holiday scheduling mistakes annoy customers
- Complex branching logic (if opened email → send follow-up, else → try different channel) becomes impossible to maintain
- Engineers spend months building custom infrastructure instead of shipping product features

Existing solutions are either:
- **Too simple** (BullMQ, Redis queues) — lose tasks, no branching
- **Too complex** (Temporal) — designed for short-lived distributed workflows, not month-long campaigns. Teams that use it for campaigns end up building ~2,400 lines of workaround code (continue-as-new, state visibility, activity boilerplate)

---

## The Solution

Orch8.io provides a **self-hosted, embeddable engine** that:

1. **Never loses tasks** — survives crashes, restarts, and deploys
2. **Respects rate limits** — defers instead of rejects (protects sender reputation)
3. **Understands business time** — timezones, business days, send windows built-in
4. **Handles complexity** — parallel execution, race conditions, error recovery
5. **Embeds easily** — integrate as a library, not a separate platform
6. **Scales massively** — 1 million+ concurrent workflows per server node

---

## Target Customers

| Segment | Who | Problem They Pay To Solve | Price Range |
|---------|-----|---------------------------|-------------|
| **Outreach Tools** | CTOs at cold email/sales engagement startups | Sequences break at scale, can't branch mid-flow | $2K-$8K/month |
| **Notification Platforms** | Backend engineers at Novu, Knock, etc. | Multi-channel sequences (push → email → SMS) with rate limits | $2K-$10K/month |
| **Solo Founders** | Indie developers building SaaS tools | Don't want to spend 3 months on scheduling infrastructure | $500-$2K/month |
| **Fintech/Healthtech** | Engineers at lending platforms, digital health | Compliance, zero lost tasks, month-long workflows | $5K-$30K/month |
| **Teams Escaping Temporal** | Backend engineers frustrated with Temporal's complexity | Same capabilities, 50%+ less infrastructure code | $3K-$10K/month |

**Total addressable market:** ~200 identifiable companies globally across outreach, notification, and lifecycle categories. Target: 50-150 paying customers within 3 years.

---

## Pricing Strategy

| Tier | Price | Active Workflows | Key Features |
|------|-------|------------------|--------------|
| **Starter** | $500/month ($5K/year) | Up to 100K | Node.js SDK, basic rate limits |
| **Growth** | $2,500/month ($25K/year) | Up to 2M | Multi-tenancy, Python/Go SDK, priority support |
| **Enterprise** | $8,000+/month | Unlimited | Source license, audit log, SLA, all SDKs |

**Overage pricing:** $3-$5 per 10K additional workflows (scales with customer growth).

### Why This Pricing Works
- **Cheaper than 1 backend engineer** ($120K/year fully-loaded engineer cost vs $30K/year for Growth tier)
- **Usage-based alignment** — customers pay more as they grow (natural expansion)
- **Annual prepay incentive** — 17% discount drives cash flow upfront

---

## Business Model Strengths

| Metric | Target | Why It's Strong |
|--------|--------|-----------------|
| **Gross Margin** | >95% | Binary license, no compute costs |
| **Monthly Churn** | <1% | Once embedded, switching cost is enormous |
| **LTV:CAC Ratio** | 90:1 | Content-driven go-to-market = near-zero acquisition cost |
| **Payback Period** | <1 month | Customer pays for itself in first month |

---

## Competitive Landscape

| Competitor | What They Do | Why They Lose |
|------------|--------------|---------------|
| **BullMQ/Redis** | Open-source job queues | Lose tasks on restart, no branching, no rate limit deferral |
| **Temporal** | Workflow orchestration platform | Complex, requires workarounds, history limits, coding restrictions |
| **Restate** | Rust workflow engine | General-purpose only, no campaign-specific features |
| **Inngest** | Serverless workflow | Cloud-hosted, not embeddable, limited rate limiting |
| **Custom Postgres** | Companies build their own | Takes 3-6 months of engineering, breaks at scale |

**Our unique advantage:** No competitor combines embeddable library + campaign primitives + workflow orchestration + Rust performance + snapshot-based resume.

---

## Go-To-Market Strategy

### Phase 1: Direct Outreach (Months 0-3)
- Publish technical blog post on HackerNews ("What we learned analyzing 2,400 lines of Temporal workaround code in one production project")
- Cold email 50 CTOs of identified outreach tools
- Offer 6 months free to 3 design partners for case studies

### Phase 2: Content Marketing (Months 3-6)
- SEO for "BullMQ alternative" and "Temporal alternative"
- Vertical-specific landing pages (/for-outreach, /for-notifications, /for-fintech)
- Publish case studies from design partners

### Phase 3: Ecosystem (Months 6+)
- GitHub open-core strategy (free single-node, paid clustering)
- Developer conference talks
- Integration partnerships (Vercel, Railway deploy templates)

---

## Key Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Restate adds campaign features | High | Ship first; vertical focus is our moat |
| Market too small (<200 buyers) | High | Expand to fintech, healthtech (same engine, different messaging) |
| Solo founder bus factor | High | Enterprise source license de-risks; hire Rust contractor by month 6 |
| Buyers build their own | Medium | Make integration so easy ("10 minutes to aha") that building is never worth it |
| Node.js SDK quality blocks adoption | High | Invest 30% of dev time in SDK quality |

---

## Success Metrics

**North Star Metric:** Licensed task instances actively running in production per month.

| Metric | Month 3 Target | Month 12 Target |
|--------|----------------|-----------------|
| Design partners in production | 3 | 10+ |
| Active trial starts/month | — | 50+ |
| Trial-to-paid conversion | — | 30% |
| Paying customers | — | 15-25 |
| Monthly recurring revenue | $0 | $25K-$50K |

---

## Why Now?

1. **Email outreach market is exploding** — Instantly, Smartlead, Saleshandy all growing rapidly, all fighting scheduling problems
2. **Notification complexity increasing** — Push, email, SMS, in-app, Slack — companies need orchestration across channels
3. **Temporal adoption growing** — More teams using Temporal means more teams experiencing its pain points
4. **Rust credibility rising** — "Built in Rust" is a brand signal to technical buyers
5. **Developer tool monetization proven** — Teams pay for infrastructure that saves engineering time

---

## The Bottom Line

Orch8.io sells **time-to-market** and **reliability** to SaaS companies that need to schedule automated workflows.

**Value proposition:** "Replace 3 months of scheduling infrastructure with a 10-minute integration. Run 1 million workflows on a $50/month server."

**Revenue potential:** $1-3M ARR within 3 years (50-150 paying customers).

**Strategic opportunity:** Become the default infrastructure layer for the $50-150M workflow orchestration market — or expand horizontally into general-purpose orchestration (competing directly with Temporal at $100M+ ARR).
