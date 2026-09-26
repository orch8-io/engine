# Orch8 features

> **Stability: mixed**, each area below carries its label. See [Stability labels](README.md#stability-labels) for what they mean.

This page lists everything the engine does, grouped by area. For a first run, start with the
entry paths in the [project README](../README.md).

**Durable execution** `stable` — Snapshot-based crash recovery, retry with exponential
backoff and conditional failure policies, idempotency keys, persistent circuit
breakers, dead-letter fingerprinting, automatic incident reproduction, and
checkpoint-based fork/resume. Side-effecting steps use a universal effect
ledger so recovery can distinguish uncommitted, committed, unknown, and
compensated effects.

**Workflow language** `stable` — Step, Parallel, Race, TryCatch, Loop, ForEach, Router,
SubSequence, CancellationScope, AB Split, and Saga; per-step `when` guards;
JSON Schema input/output contracts; dynamic block injection; and bounded
concurrent execution of independent `Parallel` branches.

**Scheduling and dispatch** `stable` — Relative and cron schedules, business calendars,
timezones, jitter, send windows, per-entity concurrency keys, weighted resource
pools, sliding-window limits, daily caps, warmup ramps, four priority levels,
and cooperative preemption at durable step boundaries.

**Workers and extensions** `stable` (REST workers, lease/heartbeat) / `beta` (gRPC stream, sidecars, WASM, MCP, Activepieces) — Lease-based REST workers with resumable heartbeat
checkpoints, queue/version/capability routing, a [negotiated bidirectional gRPC
session](GRPC_WORKER_STREAM.md), gRPC sidecars, WASM plugins, MCP client and
server modes, Activepieces, signed webhooks, and deduplicated events. Worker,
runtime, artifact-transfer, telemetry, and control sessions are bounded and
resumable.

**Data, artifacts, and streams** `beta` — Durable local or S3-compatible encrypted
artifacts, automatic externalization of oversized state, instance and
tenant-namespaced semantic memory, a resumable tenant change feed, and bounded
tumbling/sliding/session windows over durable continuity frames.

**Multi-tenancy and governance** `beta` — Capability-scoped tenant principals,
provider-neutral plan entitlements, tenant rate/concurrency limits,
tenant-isolated breakers and memory, authoritative [tenant partition
routing](TENANT_PARTITION_ROUTING.md), and fail-closed residency,
disclosure, delegation, and federation policy.

**Security** `stable` (API-key and tenant enforcement, encryption at rest, signed webhooks and packages) / `beta` (mTLS workload identity, federation) — Secure-by-default API-key and tenant enforcement, AES-256-GCM
encryption for context, credentials, artifacts, worker checkpoints, and
protected mobile fields; OAuth2 credential refresh; mTLS workload identity;
HMAC-signed webhooks; signed packages/capsules/provenance; nonce and federation
replay boundaries; CORS controls; and outbound URL/SSRF validation.

**AI and human workflows** `beta` — Multi-provider `llm_call` with structured-output
repair, multimodal artifacts, cost/token telemetry, effect-safe provider
failover, durable ReAct agents, governed shared knowledge, bounded cumulative
budgets, evidence-scoped evaluation gates, and lease-safe human attention.

**Release and distribution safety** `stable` (preflight, diff, replay, canary, gate CLI) / `beta` (registry channels, attestations, deltas) — Sequence preflight, typed-dataflow
compilation, semantic diff, historical effect-free replay, guarded canaries,
automatic rollback gates, workflow contracts, signed packages, append-only
registry history, runtime-targeted channels, attestations, dependency locks,
and verified delta fallback. See [Safe Releases](RELEASES.md), [Package
Registry](PACKAGE_REGISTRY.md), and [Governed Distribution](DISTRIBUTION_GOVERNANCE.md).

**Mobile, edge, and portable continuity** `beta`, fault lab `experimental` — Native iOS/Android execution via
Rust + UniFFI, offline-first sync, protected device tools, durable APNs/FCM
wake delivery, capability-aware placement, signed capsule handoff, ownership
epochs, provenance, live migration/rollback, receipt-backed compensation,
what-if simulation, sovereign-edge enforcement, and signed federation
send/receive primitives. See [Mobile SDK](MOBILE_SDK.md), [Continuity
Operations](CONTINUITY_OPERATIONS.md), and [Continuity Debugging](CONTINUITY_DEBUGGING.md).

**Operations and observability** `beta` (node roles, managed control, dashboard); health and metrics endpoints `stable` — Role-specific all-in-one/control/executor/
gateway/edge nodes, secure verified bootstrap, aggregate startup preflight,
auditable draining, outbound managed-control tunnels, redacted support bundles,
Prometheus metrics, OTLP/JSON telemetry, audit and provenance logs, execution
workbench, ranked diagnosis, previewable remediation, and the operator
dashboard. See [Node Roles](NODE_ROLES.md), [Secure Bootstrap](SECURE_BOOTSTRAP.md),
and [Support Bundle](SUPPORT_BUNDLE.md).

The [documentation index](README.md) maps each capability to its guide.
For the exact release-by-release inventory, including migrations and explicit
non-guarantees, see the [changelog](../CHANGELOG.md#unreleased).
