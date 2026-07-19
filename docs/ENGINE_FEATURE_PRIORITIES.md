# Engine capability priorities

This is the engine-only capability map. UI work, dashboards, visual builders,
and generic “AI assistant” features are intentionally excluded.

| Capability | Current decision | Value | From-scratch estimate | Why |
|---|---|---:|---:|---|
| Bounded concurrent `Parallel` dispatch | Implemented | 10x | 1–2 weeks | Removes serial latency while preserving durable branch state; hard cap is two in-process branches. |
| Durable spawn/join | Keep existing | 10x | 3–5 weeks | Parent/child instances, persisted execution trees, signals, and wake-up semantics already provide it. A second primitive would duplicate state machines. |
| Bounded map/reduce | Keep existing | 10x | 2–4 weeks | `ForEach` plus bounded parallel dispatch already covers the safe engine primitive. Unbounded generic map is rejected. |
| Capability-aware worker/runtime scheduling | Keep and extend | 10x | 3–6 weeks | Queue/version routing and continuity placement already exist; mobile devices can now advertise into the same runtime mesh. |
| Durable event/stream windows | Implemented | 5–10x | 2–4 weeks | Tumbling, sliding, and session windows now evaluate over durable frames, cursors, TTLs, epochs, and retractions. |
| Universal effect ledger | Implemented | 10x | 3–5 weeks | Side-effect receipts now protect ordinary instances as well as explicitly portable executions. |
| Resumable activities | Implemented | 10x | 3–5 weeks | Worker heartbeat checkpoints use lease ownership and monotonic CAS, and survive recovery/retry. |
| Shared agent knowledge store | Implemented, bounded | 5x | 2–4 weeks | Tenant/namespace memory is durable, encrypted, isolated, and capped. Cross-tenant global memory is rejected. |
| Cooperative priority preemption | Implemented | 3–5x | 2–3 weeks | Lower-priority flat workflows yield at durable step boundaries. Killing an in-flight effect is rejected. |
| Federation send/receive | Implemented as primitives | 3–5x | 3–6 weeks | The engine signs outbound short-lived envelopes and verifies/deduplicates inbound ones. Network transport remains explicit. |
| Device capability mesh | Implemented | 3–5x | 2–4 weeks | A tenant-owned mobile device can advertise a bounded, expiring mobile runtime for placement. |
| PostgreSQL storage partitioning | Implemented proactively | 2–3x at scale | 2–4 weeks | Fixed hash partitions avoid a risky conversion after append-heavy tables reach millions of rows. |

## Proposals not needed

- **A second spawn/join DSL:** existing child instances and execution trees are
  the durable primitive. Another representation would create incompatible
  retry, cancellation, and observability semantics.
- **Unbounded parallel map/reduce:** it turns workflow input into an
  uncontrolled task and memory multiplier. Bounds must be explicit.
- **Hard process-style preemption:** an arbitrary handler may already have sent
  an external effect. Aborting it cannot safely roll the provider back.
- **A separate stream-processing database:** current windows operate on the
  engine's durable continuity frames. Add an external processor only after
  measured throughput exceeds the bounded in-engine model.
- **Cross-tenant or global agent memory:** this breaks the engine's tenant
  isolation model and creates data-governance ambiguity.
- **Claims of universal exactly-once execution:** the engine can guarantee
  at-most-once dispatch evidence and idempotent recovery; a remote provider
  still needs an idempotency key for end-to-end protection.
- **Calendar partitions by default:** they require perpetual partition
  creation and can fail future inserts when maintenance stops. Fixed hash
  partitions match the dominant lookup keys without an operator cron job.

Estimates include implementation, migrations, unit/integration tests,
PostgreSQL E2E coverage, operational documentation, and a safe rollout path;
they are not UI estimates.
