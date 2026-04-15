# Orch8.io — The Complete Sales Pitch

> **See also:** [PRD](../README.md) | [Technical Details](TECHNICAL_DETAILS.md) | [Business Overview](BUSINESS_PRODUCT_DETAILS.md) | [Action Plan](ACTION_PLAN.md) | [Development Stages](DEVELOPMENT_STAGES.md)

## "Replace 3 Months of Scheduling Infrastructure with a 10-Minute Integration"

---

## Part 1: The Problem You Already Know

Every team that sends emails, notifications, or manages customer lifecycle workflows hits the same wall:

**Your scheduling layer breaks at scale.**

You started with BullMQ or a simple Postgres cron. It worked for 1,000 sequences. Then 10,000. Then 100,000. And now:

- Tasks vanish during deploys and you can't figure out which ones
- Your team built custom retry logic, rate limit trackers, and defer queues — ~2,000 lines of scheduling infrastructure that nobody asked to build
- You can't branch mid-sequence ("if they opened the email, send follow-up A; otherwise, send B") without building a state machine from scratch
- Your rate limits get violated during traffic spikes, burning your sender reputation
- Weekend sends annoy customers, timezone mistakes cost you deliverability
- Every server restart means lost state and manual reconciliation

**You spent 3-6 months building scheduling infrastructure.** And it still breaks.

If you tried using a general-purpose workflow engine (like Temporal) for campaign sequences, you ran into a different problem: **it wasn't designed for what you're doing.**

Temporal is an excellent tool for distributed systems coordination — orchestrating microservices, handling retries, managing short-to-medium duration workflows (seconds to hours). But it was **never designed for campaign sequences that run for weeks or months.** When you try to use it for long-running workflows, here's what happens:

| The Mismatch | What You End Up Building |
|---------------------|-------------------|
| History replay on every wake-up → history limits at 50K events → workflows that run for months hit the ceiling | Continue-as-new orchestration with state externalization (~300 lines in one real project we analyzed) |
| Determinism constraints → no `Date.now()`, no `Math.random()`, `patched()` version markers for every code change | Version branching infrastructure (~85 lines + ongoing developer cognitive load) |
| 64KB payload limits per history event | Compression codecs, output externalization, reference markers (~350 lines) |
| Activity ceremony → every side-effect needs define, re-export, proxyActivities, error classification | Activity registry boilerplate (~1,000 lines in one real project) |
| No direct state visibility → workflows are opaque without explicit handlers | 15+ query/signal handlers just to see what a workflow is doing (~450 lines) |
| Search attributes must be pre-registered on the server | Server-side config, limited fields, test mode incompatibility (~30 lines + deployment friction) |
| Testing requires a running Temporal server | Dry-run infrastructure, mock registries, test harnesses (~300 lines) |

**In one production codebase we analyzed, this totaled ~2,400 lines of workaround code — roughly 10% of the entire workflow codebase.** Your mileage will vary depending on your use case. But the pattern is consistent: when you use a tool for something it wasn't designed for, you build the missing pieces yourself.

**Orch8.io was designed from day one for long-running campaign and lifecycle sequences.** The features above aren't workarounds — they're the foundation.

---

## Part 2: What Orch8.io Is

**Orch8.io is a self-hosted, embeddable Rust engine that schedules, branches, and durably executes multi-step task sequences at scale.**

Two layers, one engine:

### Layer 1: Scheduling Kernel
Campaign primitives your team would otherwise build from scratch:
- Per-resource rate limiting with **deferral** (not rejection)
- Resource pools with weighted rotation and warmup ramps
- Business day awareness, timezone-per-instance, randomized jitter
- Send window scheduling
- Bulk create 50K+ instances in one API call
- Atomic pause/resume/cancel across campaigns

### Layer 2: Orchestration Engine
General-purpose durable workflow execution:
- Parallel, race, try-catch-finally, loop/forEach, multi-way routing
- Typed signals (pause, resume, cancel, update context, custom)
- Live state queries via REST — no handler boilerplate
- Template interpolation engine
- Pluggable block handler system
- Human-in-the-loop steps with timeout and escalation

**The key differentiator:** Orch8.io uses **state snapshots**, not event history replay. Your sequences resume from their last checkpoint in O(1) time. No history bloat. No determinism constraints. No `patched()` version branching. You write normal code.

---

## Part 3: Every Feature, Explained in Detail

---

### SCHEDULING KERNEL FEATURES

---

#### 1. Durable Task Instances

**What it is:** The atomic unit of work — one contact, one entity, one sequence. Every instance carries its own state, metadata, timezone, and execution context.

**How it works:** When you schedule an instance, Orch8.io persists it to PostgreSQL immediately. The instance moves through a defined state machine: `scheduled → running → completed/failed/waiting/paused/cancelled`. At every transition, state is written to disk before the engine proceeds.

**Why it matters:** If your server crashes mid-sequence, every instance is already persisted. On restart, the engine reads each instance's last checkpoint and continues. No tasks lost. No manual reconciliation. No "which ones fired?" debugging.

**Real-world example:** You schedule 50,000 cold email instances. Halfway through, your server restarts for a deployment. When Orch8.io comes back up, all 50,000 instances resume exactly where they left off. The ones that fired already won't fire again (memoized results). The ones that were mid-flight complete their current step. The ones waiting for their delay fire when their timer arrives.

---

#### 2. Step-Based Execution with Memoization

**What it is:** Each step in a sequence runs exactly once. The result is persisted. On resume, the engine skips completed steps and continues from the checkpoint.

**How it works:** When a step executes, its output is saved to the `block_outputs` table with the instance ID and step ID. On resume, the engine checks: "Has this step already run for this instance?" If yes, it loads the cached output and advances. If no, it executes the step.

**Why it matters:** No step ever runs twice (unless you explicitly re-run it). This prevents duplicate emails, duplicate charges, duplicate notifications — the kind of bug that costs you customer trust.

**Real-world example:** Your sequence is: Send email → Wait 2 days → Check if opened → Send follow-up. If the engine crashes after sending the email but before recording the wait, the email won't be sent again on resume. The memoized output proves it already fired.

---

#### 3. Relative Delay Scheduling

**What it is:** Delays are calculated relative to the previous step's completion time, not as absolute wall-clock timestamps.

**How it works:** Instead of saying "fire step 3 at 2026-04-16 10:00:00 UTC," you say "fire step 3 two days after step 2 completed." The engine calculates the absolute time at the moment step 2 finishes, then schedules accordingly.

**Why it matters:** Wall-clock scheduling breaks when steps don't complete at their expected times (rate limit deferral, retries, system slowdowns). Relative delays automatically adjust — the sequence stays coherent regardless of timing disruptions.

**Real-world example:** You want to send a follow-up 2 days after the initial email. If the initial email was deferred 6 hours due to rate limiting, the follow-up automatically shifts to 2 days from the actual send time — not the originally planned time. The recipient gets the right cadence regardless of delays.

---

#### 4. Business Day Awareness

**What it is:** Delays can skip weekends and configurable holidays automatically.

**How it works:** When calculating the next fire time, the engine checks a business day calendar. If a delay would land on a weekend or holiday, it advances to the next business day. You configure which days count as business days and which holidays to observe (US, UK, EU, custom).

**Why it matters:** Sending B2B emails on Saturday morning is pointless. Sending payment reminders on Christmas Day is embarrassing. Business day awareness prevents both without manual date calculation code in your application.

**Real-world example:** Your sequence says "wait 3 business days, then send follow-up." If step 2 completes on Thursday, the next step fires on Tuesday (skipping Saturday and Sunday). If Monday is a holiday, it fires on Wednesday.

---

#### 5. Timezone-Per-Instance

**What it is:** Each instance carries its own timezone. Delays, send windows, and business day calculations all respect the instance's timezone.

**How it works:** When you schedule an instance, you assign it a timezone (e.g., `America/New_York`, `Europe/London`). The engine converts all timing calculations to that timezone. Different instances in the same sequence can have different timezones.

**Why it matters:** If you're sending to contacts across 12 timezones, you don't want to send emails at 3 AM their time. Per-instance timezone ensures every contact receives messages during their business hours — without you maintaining 12 separate scheduling pipelines.

**Real-world example:** A sequence sends to contacts in New York, London, and Tokyo. Each instance fires at 9 AM in the recipient's local time. The New York contact gets the email at 9 AM EST, the London contact at 9 AM GMT, the Tokyo contact at 9 AM JST — all handled automatically by the engine.

---

#### 6. Randomized Jitter

**What it is:** Task firing is spread across a configurable window (e.g., ±2 hours) to prevent thundering herd problems.

**How it works:** When scheduling the next step, the engine adds a random offset within your configured jitter range. A 2-hour jitter means the step fires at any point within ±2 hours of the calculated time.

**Why it matters:** If 10,000 instances are all scheduled to fire at exactly 9:00 AM, your email API rate limits will be hammered simultaneously, and downstream services will see a spike. Jitter spreads the load naturally, protecting both your infrastructure and your sender reputation.

**Real-world example:** You schedule 5,000 emails for 9 AM Monday. With ±1 hour jitter, they actually fire between 8 AM and 10 AM, smoothing the load on your email provider and making your sending pattern look more human to spam filters.

---

#### 7. Conditional Branching

**What it is:** Sequences branch based on runtime data. If condition A is true, follow path A. Otherwise, follow path B.

**How it works:** After a step completes, the engine evaluates a condition expression against the instance's context (data from previous steps, external API results, metadata). Based on the result, it routes to the next appropriate step.

**Why it matters:** Real sequences aren't linear. "If they opened the email → send follow-up. If they didn't → resend with different subject line." Without conditional branching, you'd need to build this logic yourself — with all the state management, error handling, and edge cases that entails.

**Real-world example:** After sending an initial email, a "check reply" step queries your email provider. If the contact replied (`data.replied === true`), the sequence routes to a "convert" step. If not, it routes to a follow-up email step. If the API call fails, it retries or routes to an error handler.

---

#### 8. External Event Triggers (Signals)

**What it is:** Mid-sequence events can advance, cancel, pause, resume, or re-route a running instance.

**How it works:** Your application sends a signal to the engine via API (`POST /instances/{id}/signal`). The engine delivers the signal to waiting instances. If an instance is waiting for a specific signal type, it unblocks and evaluates the new data. Signals are typed: `pause`, `resume`, `cancel`, `update_context`, or any custom type you define.

**Why it matters:** Real-world workflows aren't self-contained. A contact might reply to your email outside the sequence. A payment might arrive. A user might cancel their subscription. These external events need to modify running workflows in real time — without you polling or building event-listening infrastructure.

**Real-world example:** You send an email. Before the 2-day wait completes, the contact replies. Your application detects the reply and sends a `update_context` signal with `{ replied: true }`. The instance immediately updates its context and, on its next evaluation, routes to the "convert" step instead of sending the follow-up.

---

#### 9. Crash Recovery via State Snapshots

**What it is:** On restart, the engine reads each instance's last checkpoint and resumes. No history replay. No determinism constraints.

**How it works:** Every state change is persisted to PostgreSQL as it happens. When the engine restarts, it queries: "Which instances were `running` when we shut down?" For each one, it loads the current state, execution tree, and step outputs — then continues from exactly that point. This is O(1) per instance (one database read), not O(n) (replaying a history log).

**Why it matters:** Temporal replays the entire execution history to reconstruct state. If a workflow has 10,000 events, it replays all 10,000 events. Orch8.io reads one row. This means faster recovery, no history size limits, and no need to externalize state to survive long-running sequences.

**Real-world example:** Your server crashes. 2 million instances were running. Orch8.io restarts. Within 5 seconds, all 2 million instances are resumed at their exact checkpoint states. No replay. No data loss. No "what happened?" investigation.

---

#### 10. Sequence Definition API

**What it is:** Define multi-step sequences as code (TypeScript, Python, Go, Rust) or as YAML/JSON.

**How it works:** You call `defineSequence()` with your step definitions, delays, conditions, and branches. The engine parses the definition, validates it, and stores it. You can then schedule instances of that sequence by ID.

**Why it matters:** Sequences are versionable, testable, and reviewable in code. Your team can discuss sequence changes in PRs. You can store sequences in your version control. And you can switch to YAML/JSON if you want non-engineers to define sequences.

**Real-world example:**
```typescript
const sequence = defineSequence({
  name: 'Cold Outreach v2',
  steps: [
    { id: 'send_initial', handler: sendEmail, retry: { maxAttempts: 3, backoff: 'exponential' } },
    { id: 'wait_1', delay: { days: 2, businessDays: true, jitter: { hours: 2 } } },
    { id: 'check_reply', condition: 'data.replied', then: 'convert', else: 'follow_up_1' },
    { id: 'follow_up_1', handler: sendFollowUp },
    { id: 'wait_2', delay: { days: 3, businessDays: true } },
    { id: 'check_reply_2', condition: 'data.replied', then: 'convert', else: 'follow_up_2' },
    { id: 'follow_up_2', handler: sendFinalFollowUp },
    { id: 'convert', handler: markAsConverted },
  ],
});
```

---

#### 11. Per-Resource Rate Limiting with Deferral

**What it is:** Cap actions per resource (mailbox, channel, API key) within a time window. When the limit is hit, the engine **defers** the task — it reschedules for the next available slot instead of rejecting it.

**How it works:** Each resource has a rate limit configuration: `max_count` per `window_seconds`. Before firing a step, the engine checks the resource's current count. If under the limit, it proceeds. If at the limit, it calculates when the next slot opens and reschedules the instance to that time.

**Why it matters:** This is the #1 feature outreach tools need. If you send from 10 mailboxes, each capped at 30 emails/day, you need the engine to respect those caps automatically. BullMQ would reject the task (lost send). Temporal would require you to build the deferral logic yourself. Orch8.io does it natively.

**Real-world example:** Mailbox `john@acme.com` has sent 30/30 emails today. Instance #5,001 tries to fire. The engine checks the rate limit, sees it's exhausted, calculates that the next slot opens at midnight, and reschedules the instance to fire at 00:01. No email is lost. No rate limit is violated.

---

#### 12. Resource Pools with Rotation

**What it is:** Assign multiple resources (mailboxes, channels, API keys) to a campaign. The engine distributes instances across them using weighted round-robin or other rotation strategies.

**How it works:** You define a pool with resources and their weights. When an instance needs a resource, the engine selects one from the pool based on the rotation strategy. If a resource hits its rate limit, the engine tries the next one. If all are exhausted, it defers.

**Why it matters:** Sending from a single mailbox doesn't scale. Pools let you send from N mailboxes, automatically balancing load and respecting individual caps. You add/remove mailboxes without changing your sequence logic.

**Real-world example:** You have 5 mailboxes in a pool, each with a daily cap of 30. The engine distributes 150 sends across all 5. When mailbox #3 hits its cap at 2 PM, the engine routes remaining sends to mailboxes 1, 2, 4, and 5. When all five are exhausted, it defers to midnight.

---

#### 13. Warmup Ramp Schedules

**What it is:** New resources start at low capacity and gradually ramp up over weeks.

**How it works:** Each resource in a pool has a warmup schedule: Week 1: 5/day, Week 2: 10/day, Week 3: 20/day, Week 4+: full cap. The engine enforces the current week's cap automatically.

**Why it matters:** New email domains and mailboxes need gradual warmup to build sender reputation. Sending at full capacity from day one triggers spam filters. Warmup ramps automate this process.

**Real-world example:** You add a new mailbox to your pool. Instead of immediately allowing 30 sends/day, the engine limits it to 5 on day one, 10 on day eight, 20 on day fifteen, and the full 30 on day twenty-two. Your sender reputation builds safely.

---

#### 14. Send Window Scheduling

**What it is:** Tasks fire only during configured hours in the recipient's timezone.

**How it works:** You define a send window (e.g., 9 AM - 5 PM). When a step is due, the engine checks the recipient's timezone. If the current time is within the window, it fires. If not, it calculates when the window next opens and reschedules.

**Why it matters:** Sending emails at midnight is both ineffective and spammy. Send windows ensure every contact receives messages during their business hours — regardless of where they are in the world.

**Real-world example:** Your send window is 9 AM - 5 PM. A contact in Tokyo has a step due at 3 AM JST. The engine defers it to 9 AM JST. A contact in New York has a step due at 11 PM EST. The engine defers it to 9 AM EST the next business day.

---

#### 15. Global Rate Limiting

**What it is:** Engine-wide throughput cap to protect downstream systems.

**How it works:** You set a global rate limit (e.g., 500 steps/second). The engine enforces this across all tenants, sequences, and resources. If the global limit is hit, new steps are deferred until the next time window.

**Why it matters:** Even if individual resource limits are respected, the aggregate throughput might overwhelm your email provider, payment gateway, or notification service. Global rate limiting protects your downstream infrastructure.

**Real-world example:** Your SendGrid account is capped at 1,000 emails/second. You have 50 campaigns running. The global rate limit ensures the total across all campaigns never exceeds 1,000/sec, even if individual mailbox limits would allow more.

---

### SCALE & OPERATIONS FEATURES

---

#### 16. Bulk Create

**What it is:** Enqueue 50,000+ instances from a single API call.

**How it works:** You send a `ScheduleInstances` request with an array of entity definitions. The engine uses batched database inserts within a transaction to create all instances atomically. Target: 100K instances in under 3 seconds.

**Why it matters:** CSV imports and bulk campaign launches are real use cases. Making 50,000 individual API calls is impractical. Bulk create lets you launch campaigns at scale with one call.

**Real-world example:** You import a CSV of 25,000 contacts. One API call creates 25,000 instances, each with its own metadata (email, name, company, timezone). The engine returns the count and any errors. Done in 1.5 seconds.

---

#### 17. Bulk Pause / Resume / Cancel

**What it is:** Operate on all instances matching a filter — by campaign, tenant, resource, or metadata.

**How it works:** You send a `BulkUpdate` request with a filter (e.g., `campaign_id = "summer_promo"`) and an action (`pause`, `resume`, `cancel`). The engine applies the action atomically to all matching instances.

**Why it matters:** When a campaign needs to stop immediately (complaint spike, legal issue, wrong content), you can't cancel instances one by one. Bulk operations let you act on thousands of instances in a single command.

**Real-world example:** Your summer promotion has a typo in the email. You bulk-pause all 10,000 instances of the campaign, fix the template, and bulk-resume. Total downtime: 30 seconds.

---

#### 18. Bulk Re-Schedule

**What it is:** Shift all pending steps by ±N hours.

**How it works:** You send a re-schedule request with a filter and a time offset. The engine adjusts the `next_fire_at` for all matching instances by the specified offset.

**Why it matters:** Daylight saving time changes, timezone corrections, or campaign-wide timing adjustments are common. Without bulk re-schedule, you'd need to cancel and recreate all affected instances.

**Real-world example:** You realize all instances in the EU campaign are scheduled 1 hour off due to a timezone misconfiguration. Bulk re-schedule by -1 hour fixes all 5,000 pending instances instantly.

---

#### 19. Priority Queues

**What it is:** At least 3 priority levels (high, normal, low). Higher-priority instances fire before lower-priority ones.

**How it works:** When the tick loop selects due instances, it orders by priority descending, then by `next_fire_at` ascending. High-priority instances are processed first.

**Why it matters:** Hot leads should get instant follow-up. Cold nurture can wait. Priority ensures your most important instances execute first, even when the queue is backed up.

**Real-world example:** A high-priority lead fills out your pricing page. Their instance is marked `priority: high`. Even though 5,000 normal-priority instances are queued, the hot lead's email fires first.

---

#### 20. Multi-Tenancy

**What it is:** Tenant-scoped queries, per-tenant rate limits, and noisy-neighbor protection.

**How it works:** Every instance, sequence, and rate limit is scoped to a tenant. Queries automatically filter by tenant. Rate limits can be configured per-tenant. The engine prevents one tenant's traffic from starving others.

**Why it matters:** If you're building a multi-tenant product (e.g., an outreach platform serving 50 customers), each customer needs isolation. Tenant A's 100K-instance campaign shouldn't slow down Tenant B's 100-instance campaign.

**Real-world example:** You run a cold email SaaS with 200 customers. Customer A launches a 50K-instance campaign. Customer B sends 500 emails. Both run simultaneously on the same engine. Customer B's instances are unaffected by Customer A's volume because the engine schedules per-tenant with isolation guarantees.

---

### RELIABILITY & ERROR HANDLING FEATURES

---

#### 21. Configurable Retry with Backoff

**What it is:** Per-step retry policy: max attempts, exponential backoff, and jitter.

**How it works:** Each step can define a retry policy. If the step fails, the engine retries up to `maxAttempts` times, with exponentially increasing delays between attempts (e.g., 1s, 2s, 4s, 8s, 16s). Jitter prevents retry thundering herds.

**Why it matters:** Transient failures (network timeout, API rate limit, temporary outage) are common. Retry with backoff gives them time to resolve — without you building retry infrastructure.

**Real-world example:** Your email API returns a 503 error. The step's retry policy says: 3 attempts, exponential backoff starting at 5 seconds. The engine retries at T+5s, T+10s, and T+20s. On the third attempt, the API is back. The email sends successfully.

---

#### 22. Dead Letter Queue

**What it is:** Instances that fail after all retries are moved to a dead letter queue for inspection and manual retry.

**How it works:** When an instance exhausts its retry attempts, its state changes to `failed` and it's flagged in the DLQ. You can query the DLQ, inspect the failure reason, and manually retry specific instances.

**Why it matters:** Not all failures are transient. Invalid email addresses, blocked domains, and permission errors won't resolve on their own. The DLQ gives you visibility into permanent failures and the ability to act on them.

**Real-world example:** After 3 retries, an email step fails because the recipient's domain doesn't exist. The instance moves to the DLQ. You inspect it, see the `NXDOMAIN` error, and decide to remove that contact from your database rather than retry.

---

#### 23. Timeout per Step

**What it is:** Each step must complete within N seconds or is marked as failed.

**How it works:** When a step starts, the engine sets a timer. If the step doesn't complete within the configured timeout, the engine marks it as failed and triggers the retry or error handling logic.

**Why it matters:** Hung API calls and infinite loops can stall your entire queue. Timeouts ensure that stuck steps don't block other instances.

**Real-world example:** Your `sendEmail` step calls an API that's experiencing latency. The timeout is set to 30 seconds. After 30 seconds with no response, the engine marks the step as failed and initiates retry. The stuck API call doesn't block the queue.

---

#### 24. Circuit Breaker

**What it is:** If a downstream handler fails repeatedly, the engine pauses all steps of that type across all instances.

**How it works:** The engine tracks failure rates per handler type. If the failure rate exceeds a threshold (e.g., 50% of attempts in the last 100 calls), the circuit opens. All steps using that handler are paused until the circuit closes (after a cooldown period and health check).

**Why it matters:** If your email provider is down, retrying every individual instance wastes resources and time. The circuit breaker pauses all email steps globally, letting the provider recover, then resumes automatically.

**Real-world example:** Your SendGrid account is suspended. The first 10 email steps fail. The circuit breaker detects the pattern and pauses all email steps across 50,000 instances. You fix the account, the health check passes, and the circuit closes. All 50,000 instances resume.

---

#### 25. Idempotency Keys

**What it is:** Prevent duplicate step execution on retry.

**How it works:** Each step can be assigned an idempotency key (a unique identifier). The engine tracks which keys have been executed. If a retry attempt uses the same key, the engine returns the cached result instead of re-executing.

**Why it matters:** In edge cases (crash during step execution, network partition), the engine might not know if a step completed. Idempotency keys ensure the step doesn't run twice — critical for payment processing, email sends, and any action with side effects.

**Real-world example:** A payment step charges $50. The step executes, but the engine crashes before persisting the result. On restart, the engine sees the idempotency key was already used and returns the cached success result — without charging the customer again.

---

### WORKFLOW ORCHESTRATION FEATURES

---

#### 51. Bidirectional Signals

**What it is:** Multiple named signal types that modify a running instance's state or control flow.

**How it works:** Signals are typed payloads sent to a running instance. The engine supports built-in types (`pause`, `resume`, `cancel`, `update_context`) and custom types you define. When a signal arrives, it's placed in the instance's signal inbox. Waiting blocks that match the signal type unblock and process the payload.

**Why it matters:** Workflows don't exist in isolation. External events (user actions, API callbacks, manual approvals) need to influence running workflows. Signals provide this without polling or building event-listening infrastructure.

**Signal types:**
| Type | Effect |
|------|--------|
| `pause` | Stops all in-flight steps, sets instance to `paused` |
| `resume` | Sets instance to `running`, re-enters orchestration |
| `cancel` | Cancels all in-flight steps, sets instance to `cancelled` |
| `update_context` | Merges payload into instance context, continues execution |
| Custom types | Defined by you, matched to waiting blocks |

**Real-world example:** An approval workflow is waiting for a human decision. The manager clicks "Approve" in your UI. Your backend sends an `approval_decision` signal with `{ action: 'approve', comment: 'Looks good' }`. The waiting block receives the signal, processes the approval, and advances the workflow.

---

#### 52. Live State Queries

**What it is:** Query any running instance's full state via REST API — no handler boilerplate needed.

**How it works:** `GET /instances/{id}/state` returns the complete context: execution tree, block outputs, metadata, current step, and runtime state. `GET /instances?metadata.campaign_id=X` filters instances by any JSONB field. No query handlers to write — the state is a database row.

**Why it matters:** Temporal requires you to write a `defineQuery` handler for every piece of state you want to expose. Orch8.io gives you the full state with one API call. Your UI, API, debugger, and monitoring tools can inspect any instance without additional code.

**Real-world example:** Your support team needs to check why a customer's onboarding flow is stuck. They call `GET /instances/{id}/state` and see: the instance is on step 3 (`wait_2`), the next fire time is tomorrow, and the previous step output was `{ email_sent: true }`. No custom query handler was needed.

---

#### 53. Transactional Updates

**What it is:** Exactly-once update semantics for operations like approval decisions.

**How it works:** The caller sends an update request. The engine guarantees it's applied exactly once and returns the result. If the update conflicts (already applied, instance in wrong state), the engine returns a clear error. Distinct from fire-and-forget signals.

**Why it matters:** Approval decisions, payment confirmations, and status changes must be idempotent. The caller needs to know whether their update was applied, not just that it was sent.

**Real-world example:** A manager submits an approval. Due to a network timeout, they click "Approve" again. The transactional update API returns the cached result on the second call — the approval was already applied. The UI shows "Already approved" instead of processing a duplicate.

---

#### 54. Parallel Execution

**What it is:** Run multiple branches concurrently within a single instance. All branches must complete before the workflow proceeds.

**How it works:** You define a parallel block with multiple branches. The engine spawns all branches simultaneously. Each branch has independent state. When all branches complete, the parallel block resolves and the workflow continues.

**Why it matters:** Some workflows have independent tasks that can run simultaneously. Sending to email AND Slack AND creating a CRM record can happen in parallel — saving time compared to sequential execution.

**Real-world example:** When a new customer signs up, three things need to happen: send a welcome email, create a CRM record, and provision their account. All three run in parallel. The workflow proceeds to the next step only when all three are done.

---

#### 55. Race Execution

**What it is:** Run multiple branches concurrently. The first to complete (or succeed) wins. All losing branches are cancelled cleanly.

**How it works:** You define a race block with multiple branches. The engine spawns all branches. When one branch completes, the engine cancels all other branches (running cleanup if defined) and the race block resolves with the winner's output.

**Why it matters:** Sometimes you want the fastest result. Try three different LLM providers and use the first response. Or send a push notification and an email — whichever the user responds to first wins.

**Real-world example:** You need a fast LLM response. You send the prompt to GPT-4, Claude, and Gemini simultaneously. Claude responds first. The engine cancels the GPT-4 and Gemini requests (saving API costs) and uses Claude's response.

---

#### 56. Cancellation Scopes

**What it is:** Structured concurrency — define scopes where cancellation propagates to all child operations. Non-cancellable scopes for cleanup work.

**How it works:** Blocks can be marked as cancellable or non-cancellable. When a cancellation signal arrives, all cancellable child blocks are terminated. Non-cancellable blocks complete their work (cleanup, finalization, logging) before the instance fully cancels.

**Why it matters:** Cancellation needs to be clean. You can't leave half-written database records or dangling API calls. Cancellation scopes ensure that teardown work completes even when the overall workflow is cancelled.

**Real-world example:** A workflow is cancelled mid-execution. The `cleanup` scope is non-cancellable, so it runs to completion — rolling back partial changes, logging the cancellation reason, and emitting a `workflow.cancelled` event. Only then does the instance fully terminate.

---

#### 57. Try-Catch-Finally Blocks

**What it is:** Error handling within sequences. Execute a set of steps. If any fail, run recovery logic. Always run finalization regardless of success or failure.

**How it works:** You wrap steps in a try block. If any step in the try block fails, the catch block executes with the error context. The finally block runs in all cases — success, failure, or cancellation.

**Why it matters:** Workflows need error recovery. If a payment fails, try a backup payment method. Regardless of the outcome, send a notification. Try-catch-finally gives you structured error handling without building it yourself.

**Real-world example:**

```yaml
try:
  - charge_primary_payment()
catch:
  - charge_backup_payment()
  - notify_support_of_failure()
finally:
  - log_transaction()
  - send_receipt_if_successful()
```

---

#### 58. Loop / ForEach Iteration

**What it is:** Iterate over collections within a sequence. `forEach` over arrays, while-style loops with conditions.

**How it works:** You define a loop block with a condition or collection. For `forEach`, the engine executes the body for each item in the collection, with the current item available in context. For condition loops, the body executes until the condition is false. A configurable iteration cap prevents runaway loops.

**Why it matters:** Some workflows process variable-length collections. Send an onboarding email for each product the customer purchased. Retry a failing step up to N times. Process items from a queue until it's empty.

**Real-world example:** A customer buys 3 products. A `forEach` loop iterates over the products array, sending a product-specific welcome email for each one. Each iteration has access to the current product's data via `context.currentItem`.

---

#### 59. Router (Multi-Way Branching)

**What it is:** Route to one of N branches based on runtime conditions. More than binary if/else — supports N-way routing with a fallback/default branch.

**How it works:** You define a router block with multiple branches, each with a condition. The engine evaluates conditions in order and routes to the first matching branch. If no conditions match, the default branch executes.

**Why it matters:** Real workflows have more than two paths. Based on a customer's tier (Free, Starter, Pro, Enterprise), route to different onboarding flows. Router handles N paths cleanly.

**Real-world example:**

```yaml
router:
  - condition: data.tier === 'enterprise' → enterprise_onboarding
  - condition: data.tier === 'pro' → pro_onboarding
  - condition: data.tier === 'starter' → starter_onboarding
  - default → free_onboarding
```

---

#### 60. Workflow Versioning

**What it is:** Deploy new sequence logic without breaking running instances.

**How it works:** When you deploy a new sequence version, new instances follow the new logic. Running instances continue on their original version. For breaking changes, you define explicit state migration functions — not implicit code forks via `patched()` markers.

**Why it matters:** Temporal requires `patched()` / `deprecatePatch()` for every code change that affects in-flight workflows. This creates permanent code branches. Orch8.io applies new logic to new instances immediately and provides explicit migration tools for running ones.

**Real-world example:** You add a new step to your onboarding sequence. New customers get the updated flow. Existing customers complete their current flow uninterrupted. No version markers, no code forks, no `patched()`.

---

#### 61. Debug Mode

**What it is:** Breakpoints on specific steps, step-through execution, pause/resume, and full state inspection at each breakpoint.

**How it works:** You set breakpoints on step IDs in development or production. When an instance reaches a breakpoint, it pauses. You can inspect the full state (context, outputs, execution tree), step through the next block, or resume normal execution.

**Why it matters:** Debugging workflows is hard because they're asynchronous and stateful. Debug mode lets you pause a running instance, see exactly what it knows, and step through one block at a time — in production, not just in tests.

**Real-world example:** An instance is failing on step 4 in production. You set a breakpoint on step 4, trigger a test instance, and inspect the state at the breakpoint. You see that the API response format changed, causing a parsing error. Fix deployed.

---

#### 62. Search Attributes (JSONB Indexing)

**What it is:** Index running instances by custom attributes — no server-side pre-registration needed.

**How it works:** Instance metadata is stored as JSONB in PostgreSQL. You create a GIN index on whatever fields you need: `campaign_id`, `customer_tier`, `source`, anything. Then query: `GET /instances?metadata.campaign_id=X`.

**Why it matters:** Temporal requires you to pre-register search attributes on the server. Orch8.io lets you index on anything with a standard `CREATE INDEX`. Works identically in test and production.

**Real-world example:** You want to find all instances where `metadata.segment === 'churned'` and `state === 'paused'`. You create a GIN index on the metadata column and query directly. No server-side registration, no attribute limits.

---

#### 63. Template Interpolation Engine

**What it is:** Resolve `{{path}}` references within step definitions at runtime.

**How it works:** When a step executes, the engine scans its parameters for `{{path}}` patterns. It resolves paths from the instance context, previous step outputs, parameters, and configuration. Expressions (e.g., `{{data.firstName.toUpperCase()}}`) and fallback defaults (e.g., `{{data.nickname || data.firstName}}`) are supported.

**Why it matters:** Steps need dynamic data. Email subjects, API parameters, message bodies — all depend on runtime context. Template interpolation lets you define this declaratively instead of writing resolution code in every step.

**Real-world example:**
```json
{
  "step": "sendEmail",
  "params": {
    "to": "{{metadata.email}}",
    "subject": "Hey {{data.firstName || 'there'}} 👋",
    "body": "Thanks for trying {{config.productName}}. Your account is {{data.accountType}}."
  }
}
```

---

#### 64. Pluggable Block Handler System

**What it is:** Extensible step type registry. Built-in handlers for flow control. User-defined handlers for domain logic.

**How it works:** Each block type has a handler. Built-in handlers cover flow control (parallel, race, loop, router, try-catch). You register custom handlers for domain logic (HTTP calls, email sends, LLM invocations, database queries). Each handler receives recursive execution capability — it can invoke sub-blocks.

**Why it matters:** You're not locked into the engine's built-in step types. Need a custom `sendSMS` handler? Register it. Need a `callWebhook` handler? Register it. The engine orchestrates; your handlers execute.

**Real-world example:**
```typescript
engine.registerHandler('sendSMS', async (context) => {
  const to = context.resolve('{{metadata.phone}}');
  const body = context.resolve('{{data.smsBody}}');
  const result = await twilio.messages.create({ to, body });
  return { status: 'success', output: { sid: result.sid } };
});
```

---

#### 65. Structured Context Management

**What it is:** Multi-section execution context with different read/write permissions and lifecycles.

**How it works:** Each instance's context is divided into sections:
| Section | Purpose | Permissions |
|---------|---------|-------------|
| `data` | Business data (customer info, API results) | Read/write by steps |
| `config` | Sequence configuration | Read-only for steps |
| `audit` | Event trail | Append-only |
| `steps` | Block outputs | Written by engine, read by steps |
| `runtime` | Engine-managed state | Read-only |
| `auth` | Credentials, tokens | Restricted access |

**Why it matters:** Not all data is equal. Some data is configuration (shouldn't be modified by steps). Some is audit trail (must be append-only). Structured context prevents accidental data corruption and makes debugging easier.

**Real-world example:** A step tries to overwrite `config.retryPolicy` — the engine rejects it (read-only). A step appends to `audit` — the engine allows it (append-only). A step reads `data.customerEmail` — the engine allows it (read/write).

---

#### 66. Output Externalization

**What it is:** Large step outputs stored externally (database table) with references in the execution state.

**How it works:** When a step's output exceeds a configurable threshold (e.g., 64KB), the engine stores the full output in the `externalized_state` table and places a reference key in `block_outputs`. When the step's output is needed, the engine resolves the reference.

**Why it matters:** Long-running workflows with large payloads (API responses, generated content, batch results) would bloat the execution state. Externalization keeps the core state lean while preserving access to large outputs.

**Real-world example:** A step generates a 200KB report. The engine stores the report in `externalized_state` with ref key `output:abc123:report:1713110400`. The `block_outputs` table stores only the reference. The report is accessible but doesn't bloat the instance's core state.

---

#### 67. Checkpointing for Long-Running Instances

**What it is:** Periodic state snapshots for instances that run for days or weeks.

**How it works:** The engine takes a state snapshot every configurable interval (e.g., every 100 steps or every hour). On resume, the engine loads the latest checkpoint rather than replaying from the beginning.

**Why it matters:** Instances that run for weeks accumulate significant state. Without checkpointing, resume would need to process the entire execution history. Checkpoints keep resume fast regardless of instance age.

**Real-world example:** A patient onboarding workflow runs for 90 days. After 60 days, it has 500 step outputs and 200 context updates. A checkpoint was taken at day 30 and day 60. On restart, the engine loads the day-60 checkpoint — not the entire 60-day history.

---

#### 68. Session Management

**What it is:** Session-scoped data storage tied to instances, with lifecycle signals.

**How it works:** Instances can create and reference sessions. Sessions have their own data store, lifecycle signals (`expired`, `paused`, `resumed`, `completed`), and can be shared across instances.

**Why it matters:** Some workflows need persistent context across multiple instances. A customer support session that spans multiple workflow instances. A user's onboarding session that tracks progress across separate sequences.

**Real-world example:** A customer starts an onboarding session. Their progress (completed steps, preferences, answers) is stored in the session. If they abandon and restart a week later, the new instance references the session and picks up where they left off.

---

#### 69. Human-in-the-Loop Steps

**What it is:** Steps that block until a human provides input — approval, review, data entry.

**How it works:** The workflow pauses at the human-in-the-loop step. It waits for a signal (approval decision, data input, review outcome) with a configurable timeout. If the timeout expires without input, an escalation handler executes.

**Why it matters:** Not all steps are automated. Manager approvals, manual reviews, data entry — these require human input. The engine handles the blocking, timeout, and escalation so you don't build it yourself.

**Real-world example:** An expense report workflow reaches an approval step for amounts over $500. It pauses and notifies the manager. The manager approves via your UI, which sends an `approval` signal. The workflow resumes. If the manager doesn't respond within 48 hours, the workflow escalates to the manager's boss.

---

#### 70. Restart from Arbitrary Step

**What it is:** Resume a failed or stopped instance from any step, not just the last checkpoint.

**How it works:** You specify a step ID to restart from. The engine loads previously completed step outputs as cached data and re-executes from the specified step. You can choose selective re-execution of specific branches.

**Why it matters:** Sometimes you need to replay a workflow from a specific point — after fixing a bug, correcting data, or testing a change. Restarting from the beginning is wasteful. Restarting from the arbitrary step is efficient.

**Real-world example:** A workflow failed at step 8 because of bad input data. You fix the data and restart the instance from step 8. Steps 1-7 don't re-execute — their cached outputs are used. Only step 8 onwards runs.

---

#### 71. Workflow Interceptors

**What it is:** Pluggable hooks on workflow lifecycle events.

**How it works:** You register interceptors for events: `beforeActivity`, `afterActivity`, `signalReceived`, `timerFired`, `instanceCompleted`, `instanceFailed`. The interceptor runs before/after the event and can modify behavior (logging, metrics, custom routing).

**Why it matters:** Cross-cutting concerns (logging, metrics, audit, custom routing) shouldn't be mixed into every step's logic. Interceptors let you handle them centrally.

**Real-world example:** An `afterActivity` interceptor logs every step execution to your SIEM system. A `beforeActivity` interceptor injects authentication tokens into the context. An `instanceFailed` interceptor sends an alert to your on-call channel.

---

#### 72. Activity Heartbeating

**What it is:** Long-running steps report progress periodically. The engine detects stalled steps and retries on a different worker.

**How it works:** A step sends heartbeat signals during execution. If no heartbeat is received within the timeout, the engine marks the step as stalled, cancels it, and retries on a different worker node.

**Why it matters:** Some steps take minutes or hours (file processing, batch operations, LLM calls). Without heartbeating, a silently failed step would block the workflow indefinitely. Heartbeating detects stalled steps and recovers automatically.

**Real-world example:** A step processes a 10GB file, taking 30 minutes. Every 30 seconds, it sends a heartbeat. At minute 15, the worker node crashes. The engine detects the missing heartbeat, cancels the stalled step, and retries it on a healthy worker.

---

#### 73. Payload Compression

**What it is:** Pluggable payload codec layer with built-in gzip compression.

**How it works:** When a step's output exceeds a configurable threshold (e.g., 1KB), the engine automatically compresses it with gzip before storage. On read, it decompresses transparently. You can add custom codecs (encryption, custom serialization).

**Why it matters:** Large payloads consume storage and network bandwidth. Compression reduces both without changing your step logic.

**Real-world example:** A step outputs a 50KB JSON response. The engine compresses it to 8KB before storing in PostgreSQL. On read, it decompresses to the original 50KB. Your step logic never sees the compressed form.

---

#### 74. Graceful Shutdown

**What it is:** On SIGINT/SIGTERM, stop accepting new work, drain in-flight steps with a configurable grace period, persist incomplete state, then exit.

**How it works:** When the engine receives a shutdown signal, it:
1. Stops accepting new instances and signals
2. Allows in-flight steps to complete (up to the grace period)
3. Persists the state of any incomplete steps
4. Closes database connections and health probes
5. Exits cleanly

**Why it matters:** Deployments, scaling events, and restarts shouldn't lose work. Graceful shutdown ensures that in-flight steps complete or are safely persisted before the process exits.

**Real-world example:** You deploy a new version. The old process receives SIGTERM. It stops accepting new work, lets 50 in-flight steps complete (they take 2 seconds), persists the state of 3 steps that couldn't finish, and exits. The new process starts and resumes the 3 incomplete steps. Zero tasks lost.

---

#### 75. Worker Tuning / Concurrency Controls

**What it is:** Configurable max concurrent step executions and max concurrent instance evaluations per worker node.

**How it works:** You set `max_concurrent_steps` and `max_concurrent_instances` in the engine configuration. The engine enforces these limits, queuing excess work rather than overloading the worker.

**Why it matters:** Under burst traffic, an unconstrained worker can exhaust memory, CPU, or database connections. Concurrency controls prevent overload and maintain predictable latency.

**Real-world example:** A campaign launches 100K instances at once. Your worker is configured for `max_concurrent_steps: 500`. The engine processes 500 steps at a time, queuing the rest. The worker stays stable — no OOM, no connection pool exhaustion.

---

#### 76. Namespace / Environment Isolation

**What it is:** Logical isolation of instances, sequences, and state by namespace.

**How it works:** Each instance and sequence belongs to a namespace (e.g., `dev`, `staging`, `prod`). Queries are automatically scoped to the active namespace. Instances in one namespace can't affect instances in another.

**Why it matters:** Running the same engine binary across dev, staging, and prod environments without cross-contamination. Test sequences in staging without accidentally triggering production workflows.

**Real-world example:** Your dev environment runs test sequences in the `dev` namespace. Your production sequences run in the `prod` namespace. A bulk-pause in `dev` doesn't affect `prod`. The same engine binary serves both environments with full isolation.

---

#### 77. Task Queue Routing

**What it is:** Multiple named task queues per engine. Route specific sequence types or step types to dedicated worker pools.

**How it works:** You define named queues (e.g., `high-priority`, `cpu-heavy`, `io-heavy`). Sequences or steps are assigned to queues. Workers subscribe to specific queues. CPU-heavy steps run on beefy workers. I/O steps run on lightweight ones.

**Why it matters:** Not all steps have the same resource profile. LLM calls need GPU workers. Email sends need I/O workers. File processing needs CPU workers. Queue routing ensures each step type runs on the right hardware.

**Real-world example:** Your engine has three queues: `email` (I/O workers), `llm` (GPU workers), and `report` (CPU workers). Email steps route to the `email` queue, LLM steps to `llm`, and report generation to `report`. Each worker pool is sized and tuned for its workload.

---

### API & INTEGRATION FEATURES

---

#### 26. gRPC API

**What it is:** Primary API surface for high-performance consumers (SDKs, internal services).

**How it works:** Protobuf-defined service with methods for sequence creation, instance scheduling, state queries, signal delivery, and bulk operations. gRPC provides strong typing, bidirectional streaming, and high throughput.

**Why it matters:** SDKs need a performant, strongly-typed API. gRPC provides automatic code generation for all supported languages and better performance than REST for high-throughput scenarios.

---

#### 27. HTTP/REST API

**What it is:** Secondary API for easy integration, testing, and curl-based workflows.

**How it works:** JSON-based HTTP endpoints mirroring the gRPC surface. OpenAPI/Swagger auto-generated for documentation and client generation. CORS configured for browser access.

**Why it matters:** Not every integration needs an SDK. REST lets you test with curl, integrate with no-code tools (Zapier, Make), and debug with any HTTP client.

---

#### 28. Webhook Event Emitter

**What it is:** Push events to your system — step completed, instance failed, rate limit hit.

**How it works:** You configure webhook URLs. The engine emits events for key lifecycle moments and POSTs them to your endpoints. Failed webhook deliveries are retried with backoff.

**Why it matters:** You shouldn't need to poll for workflow state. Webhooks let your application react to workflow events in real time — update your UI, notify users, trigger downstream processes.

**Event types:**
- `step.completed` — a step finished successfully
- `step.failed` — a step failed (after retries)
- `instance.completed` — an instance finished its sequence
- `instance.failed` — an instance failed irrecoverably
- `instance.cancelled` — an instance was cancelled
- `rate_limit.hit` — a resource hit its rate limit
- `signal.received` — a signal was delivered to an instance

---

#### 29. Node.js / TypeScript SDK

**What it is:** First SDK. Type-safe, idiomatic TypeScript for the majority of campaign tool builders.

**How it works:** The SDK wraps the gRPC/REST API with TypeScript types, error handling, and language-idiomatic patterns. Steps are plain async functions. Sequences are defined as code. No activity registry, no proxy configuration, no error classification boilerplate.

**Why it matters:** The SDK is the product for most buyers. If it's not delightful, the positioning falls apart. We invest 30% of development time in SDK quality.

---

#### 30. Python SDK

**What it is:** Second SDK. Fintech and healthtech buyers are Python-heavy.

**How it works:** Async Python client with type hints, context managers, and Pythonic error handling. Mirrors the Node.js SDK's capabilities.

---

#### 31. Go SDK

**What it is:** Third SDK. Infra-oriented buyers and systems programming.

**How it works:** Idiomatic Go client with context support, gRPC integration, and Go-style error handling.

---

#### 32. Rust Crate (Native)

**What it is:** Direct embedding for Rust consumers — no FFI, no network hop.

**How it works:** The core engine is a Rust library. Rust applications can embed it directly, calling the engine's API as native Rust functions.

---

### STORAGE & INFRASTRUCTURE FEATURES

---

#### 33. PostgreSQL Backend

**What it is:** Primary storage. Most buyers already run Postgres.

**Why it matters:** Zero new infrastructure to adopt. If you run Postgres (which most SaaS companies do), Orch8.io uses your existing database. No Cassandra, no Elasticsearch, no special storage layer.

---

#### 34. SQLite Backend (Embedded)

**What it is:** For dev, testing, and small-scale self-hosted deployments.

**Why it matters:** Zero external dependencies for testing and small deployments. The same engine binary works with SQLite or Postgres — just change the config.

---

#### 35. Storage Abstraction Trait

**What it is:** Pluggable interface so buyers can implement custom backends.

**Why it matters:** If you need Redis, RocksDB, or a custom storage layer, you implement the `StorageBackend` trait. The engine works with any backend.

---

#### 36. Migration System

**What it is:** Schema versioning with automatic migration on engine startup.

**How it works:** Each engine version carries its schema version. On startup, the engine checks the current schema version and applies any necessary migrations automatically.

**Why it matters:** Upgrading the engine shouldn't require manual database changes. Automatic migrations make upgrades seamless.

---

### OBSERVABILITY FEATURES

---

#### 37. Prometheus Metrics Endpoint

**What it is:** Active instances, steps/sec, queue depth, rate limit saturation, error rate, latency percentiles.

**Why it matters:** You can't manage what you can't measure. Prometheus integration means your existing Grafana dashboards work out of the box.

---

#### 38. Structured JSON Logging

**What it is:** Every state transition logged with instance_id, step, tenant, duration, and attempt.

**Why it matters:** Log aggregation tools (Datadog, ELK, Splunk) can parse, filter, and alert on workflow events without custom parsing.

---

#### 39. Health Check Endpoint

**What it is:** Readiness and liveness probes for Kubernetes deployment.

**Why it matters:** K8s needs to know when the engine is ready to accept traffic and when it's dead. Health check endpoints enable zero-downtime deployments.

---

#### 40. Dashboard UI (Optional)

**What it is:** Web UI showing engine status, instance inspector, queue visualization.

**Why it matters:** Non-technical team members need visibility into workflow state. The dashboard provides this without building custom tooling.

---

### COMPLIANCE & ENTERPRISE FEATURES

---

#### 41. Audit Log

**What it is:** Append-only event log of every state transition.

**Why it matters:** Fintech, healthtech, and regulated industries need a complete audit trail. The audit log records every state change with timestamp, instance ID, and metadata — ready for compliance audits.

---

#### 42. Sequence Versioning

**What it is:** Deploy new sequence definitions without killing running instances.

**Why it matters:** V1 instances continue on V1. New instances use V2. You can migrate running instances on your own schedule, not the engine's.

---

#### 43. Hot Code Migration

**What it is:** Configurable rules for migrating running instances to a new sequence version.

**Why it matters:** When you need to migrate running instances (not just new ones), you define the migration rules. The engine applies them without downtime.

---

#### 44. Encryption at Rest

**What it is:** Encrypt instance metadata and state in storage.

**Why it matters:** Regulated industries require data encryption. Encrypting instance data at rest protects PII, payment info, and health data.

---

#### 45. SLA Timers / Deadlines

**What it is:** "Must complete step within 72 hours or escalate." Per-step deadline with escalation handler.

**Why it matters:** Compliance workflows have hard deadlines. A KYC check must complete within 72 hours. If it doesn't, escalate to a human. SLA timers automate this.

---

## Part 4: The Numbers That Matter

### Performance

| Metric | Orch8.io | BullMQ | Temporal |
|--------|----------|--------|----------|
| Instances per node | **1M+** | ~100K | ~100K (with tuning) |
| Crash recovery | **< 5 seconds** | Lost tasks | Minutes (replay) |
| Step overhead (p99) | **< 10ms** | ~5ms | ~50ms+ |
| Memory per 1M instances | **< 512MB** | N/A | N/A |
| Bulk create 100K | **< 3 seconds** | ~10 seconds | N/A |
| Determinism constraints | **None** | None | Required |
| Campaign primitives (rate limits, rotation, warmup) | **Built-in** | DIY (~2,000 lines in one analyzed project) | Not available |

### Pricing

| Tier | Price | What You Get |
|------|-------|--------------|
| Starter | $500/month | Up to 100K instances, Node.js SDK, basic rate limits |
| Growth | $2,500/month | Up to 2M instances, multi-tenancy, Python/Go SDK, priority support |
| Enterprise | $8,000+/month | Unlimited instances, source license, audit log, SLA, all SDKs |

**The ROI:** Starter tier ($6K/year) costs less than 1 week of a backend engineer's time. Growth tier ($30K/year) replaces a full-time backend engineer ($120K+/year fully-loaded).

---

## Part 5: What It Feels Like to Use

### Your First 10 Minutes

```bash
# Install
docker pull orch8/engine

# Run with embedded SQLite
docker run -p 50051:50051 orch8/engine --storage sqlite

# In your TypeScript app:
npm install @orch8/sdk
```

```typescript
import { createEngine, defineSequence } from '@orch8/sdk';

const engine = createEngine({ url: 'localhost:50051' });

// Define a sequence — steps are plain functions, no registry
const sequence = defineSequence({
  name: 'Welcome Flow',
  steps: [
    { id: 'send_welcome', handler: sendWelcomeEmail },
    { id: 'wait', delay: { days: 2, businessDays: true, jitter: { hours: 2 } } },
    { id: 'check_opened', condition: 'data.opened', then: 'done', else: 'nudge' },
    { id: 'nudge', handler: sendNudgeEmail },
    { id: 'done', handler: markComplete },
  ],
});

// Schedule 1,000 instances
await engine.schedule(sequence, contacts); // contacts is an array of 1,000

// That's it. They're running.
```

### Your First Crash Recovery Test

```bash
# Kill the engine mid-execution
docker kill orch8

# Restart it
docker start orch8

# All 1,000 instances resume from their last checkpoint.
# No tasks lost. No duplicate emails sent.
```

### Your First Signal

```typescript
// A contact replied to the email outside the sequence
await engine.signal(instanceId, {
  type: 'update_context',
  payload: { replied: true, replyText: 'Interested! Tell me more.' }
});

// The instance immediately updates and, on its next evaluation,
// routes to the "convert" step instead of sending the follow-up.
```

### Your First Query

```typescript
// What's happening with this instance right now?
const state = await engine.queryInstance(instanceId);

console.log(state.currentStep);    // "wait"
console.log(state.nextFireAt);     // 2026-04-16T10:30:00Z
console.log(state.context.data);   // { opened: true, replied: false, ... }
console.log(state.blockOutputs);   // { send_welcome: { sent: true, ... } }

// No query handler to write. No defineQuery to register.
// The state IS the database row.
```

---

## Part 6: Why Teams Choose Orch8.io

### If You're Building an Outreach Tool

> "We spent 4 months building scheduling on top of BullMQ. It still lost tasks on deploys. We couldn't branch mid-sequence. Orch8.io replaced all of it with a 10-minute integration and handles our 500K active sequences without breaking a sweat."

**What you get:** Per-mailbox rate limiting, mailbox rotation, warmup ramps, business day scheduling, timezone-aware sends, conditional branching based on opens/replies.

### If You're Running Temporal

> "We tried using Temporal for our 3-month onboarding sequences. History limits forced us to build continue-as-new logic. Every code change needed `patched()` markers. Query handlers for state visibility added hundreds of lines. Orch8.io eliminated all of that — we write normal TypeScript now."

**What you get:** Same workflow capabilities (parallel, race, try-catch, signals) without the long-running workflow tax. No determinism constraints. No history replay. State queryable via REST.

### If You're a Solo Founder

> "I was choosing between BullMQ (fragile) and Temporal (overkill). I needed something in between. Orch8.io gave me production-grade scheduling in a single binary. I launched my product 3 months earlier than planned."

**What you get:** Works out of the box. SQLite for zero-dependency dev. Postgres for production. SDK that just works.

### If You're in Fintech or Healthtech

> "We need full audit trails, zero lost tasks, and workflows that run for months. Orch8.io's append-only audit log, snapshot-based recovery, and business day scheduling checked every compliance box."

**What you get:** Audit log, SLA timers, encryption at rest, month-long workflows without state bloat, human-in-the-loop approvals.

---

## Part 7: The Commitment

**Time to first workflow:** 10 minutes.

**Time to production:** 1-4 weeks (depending on integration complexity).

**Time to replace your scheduling infrastructure:** One sprint.

**Switching cost after embedding:** Enormous. (That's why our churn target is < 1% per month.)

**Support:** Priority Slack for Growth and Enterprise. Dedicated engineer for Enterprise.

**SLA:** 99.5% for Growth. 99.9% for Enterprise.

---

## Part 8: The Close

You have three options:

1. **Keep building it yourself.** Spend 3-6 months on scheduling infrastructure. Fight race conditions, lost tasks, and rate limit violations. Maintain hundreds of lines of DIY code for retry logic, deferral queues, and state reconciliation.

2. **Use a general-purpose workflow engine.** Get powerful orchestration, but fight its design assumptions: history replay, determinism constraints, payload limits, and no campaign primitives. It works great for sub-hour workflows — for month-long sequences, you'll build the rest yourself.

3. **Use Orch8.io.** Embed a single binary. Get durable scheduling and workflow orchestration with zero ceremony. Handle 1M+ concurrent workflow instances on a single node. Write normal code.

**We built option 3.**

The question isn't whether you need durable workflow orchestration. You already know you do. The question is whether you want to build it, rent it from Temporal, or license it from us.

**Let's talk.**

---

## Appendix: Competitive Comparison

| Feature | Orch8.io | Temporal | Restate | BullMQ | Inngest |
|---------|----------|----------|---------|--------|---------|
| Embeddable library | ✅ | ❌ | ❌ | ✅ | ❌ |
| Self-hosted binary | ✅ | ✅ | ✅ | ✅ | Partial |
| No history replay | ✅ | ❌ | ✅ | N/A | N/A |
| No determinism constraints | ✅ | ❌ | ✅ | N/A | N/A |
| State queryable via REST | ✅ | ❌ | Partial | N/A | Partial |
| No payload size limits | ✅ | ❌ | ❌ | ✅ | Partial |
| Per-resource rate limiting | ✅ | ❌ | ❌ | ❌ | Partial |
| Business day scheduling | ✅ | ❌ | ❌ | ❌ | ❌ |
| Campaign primitives | ✅ | ❌ | ❌ | ❌ | ❌ |
| Parallel / race execution | ✅ | ✅ | ✅ | ❌ | Partial |
| Embedded test mode | ✅ | ❌ | ❌ | ✅ | Partial |
| 1M+ instances/node | ✅ | ❌ | Untested | ❌ | Untested |
| Commercial license | ✅ | ✅ | BSL | MIT | ✅ |

---

*Orch8.io — Durable task sequencing, without the tax.*
