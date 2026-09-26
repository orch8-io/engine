# Orch8 documentation

> **Stability: n/a**, documentation index; see [Stability labels](#stability-labels).

Use this page as the canonical map of repository documentation. It separates
tutorials, task-oriented guides, reference, explanation, and historical audit
records so readers know what kind of answer each document provides.

The running server is the source of truth for HTTP schemas: open `/swagger-ui`
or download `/api-docs/openapi.json`. Published snapshots live at
`https://orch8.io/contracts/openapi.json` and `sequence.schema.json`.

## Stability labels

Every page starts with a one-line **Stability** badge, and entries below carry the same
label. The labels come from [STABILITY.md](../STABILITY.md) and the code:

| Label | Meaning |
|---|---|
| `stable` | Covered by the "Stable at 1.0" list in [STABILITY.md](../STABILITY.md): canonical `/api/v1` REST paths and the error envelope, sequence format `schema_version: 1`, persisted migrations and rolling upgrades, worker lease/heartbeat, signed package verification, and CLI commands not marked experimental. Removals or semantic changes need a major release. |
| `beta` | Shipped, tested, and documented, but not in the stable list. This covers the mobile SDK, continuity, node roles, the gRPC stream protocol, deploy tooling, and similar. Can change in a minor release, and every change is noted in the changelog. |
| `experimental` | Explicitly experimental in STABILITY.md (continuity lab, fault injection, provider preview), off by default, or not yet verified end to end. Can change or be removed in any release. |
| `n/a` | Explanations, procedures, legal summaries, and historical records. These aren't capabilities. |

Orch8 is pre-1.0 (see the README's *Status & Limitations*). Until 1.0, "stable" means the
surface is on the 1.0 contract and breaking changes are kept minimal and announced in
release notes.

## Learn

- `stable` [Progressive quick starts](quick-starts/README.md) — six hands-on levels from a
  local workflow to workers, recovery, PostgreSQL, and guarded releases.
- `stable` [Topic quick starts](quick-starts/topics/README.md) — independent recipes for
  cron, webhooks, approvals, typed dataflow, large payloads, integrations,
  observability, and load testing.
- `stable` [Sequences](SEQUENCES.md) — learn the workflow JSON model and block types.
- `mixed` [Features](FEATURES.md) — the complete capability list, by area.
- `experimental` [Community templates](COMMUNITY_TEMPLATES.md) — use and contribute catalog templates.
- `beta` [Agent patterns](agent-patterns/README.md) — run four composable AI workflow examples.
- [Email classifier](../examples/email-classifier/README.md) — a complete TypeScript worker and webhook application.
- [Portable agent product](../examples/portable-agent-product/README.md) — compile policy, wrap a local worker, score conformance, and validate an OEM plan.

## Operate

- `beta` [Dashboard](DASHBOARD.md) — connect the operator console and use its current surfaces.
- `stable` [Safe releases](RELEASES.md) — diff, validate, canary, evaluate, promote, and roll back.
- `stable` [Deployment](DEPLOYMENT.md) — Docker, Kubernetes, the Helm chart, one-click PaaS templates, cloud targets, and the production checklist.
- `beta` [SQLite in production](SQLITE_PRODUCTION.md) — single-node SQLite + Litestream, limits, and a restore drill.
- `beta` [GitHub Actions](examples/github-actions/README.md) — release gate, PR semantic-diff comments, and ephemeral preview runs.
- `stable` [Migration guides](MIGRATION_GUIDES.md) — move from Temporal, Airflow, or Prefect with a reversible cutover.
- `stable` [Local tunnel](LOCAL_TUNNEL.md) — receive signed webhooks during development.
- `stable` [Authentication and SSO](AUTHENTICATION.md) — engine keys and the Cloud OIDC boundary.
- `stable` [Secure production bootstrap](SECURE_BOOTSTRAP.md) — scaffold, validate, start, and readiness-check a secure node.
- `beta` [Node roles](NODE_ROLES.md) — assemble all-in-one, control, executor, gateway, and edge processes; operate managed-control sessions and fleet draining.
- `stable` [Operator support bundle](SUPPORT_BUNDLE.md) — collect bounded, redacted diagnostics atomically.
- `beta` [Background jobs](JOBS.md) — enqueue a handler with one call (`POST /jobs`), no sequence required.
- `stable` [External workers](WORKERS.md) — poll, heartbeat, complete, and fail work from any language.
- `beta` [Triggers](TRIGGERS.md) — start workflows from webhooks, NATS, Kafka, SQS, Pub/Sub, Redis Streams, and Postgres row changes.
- `beta` [Negotiated gRPC worker stream](GRPC_WORKER_STREAM.md) — worker sessions, control, resumable artifacts, telemetry, and mTLS identity.
- `stable` [Webhooks](WEBHOOKS.md) — delivery, signatures, replay protection, and receiver example.
- `beta` [Durable push delivery](PUSH_DELIVERY.md) — APNs/FCM wake outbox lifecycle and recovery.
- `beta` [Governed execution wakes](PUSH_GOVERNANCE.md) — tenant credential routing, signed wake metadata, collapse, and token quarantine.
- `beta` [Continuity operations](CONTINUITY_OPERATIONS.md) — portable handoff, migration, effects, and provenance.
- `beta` [Continuity debugging](CONTINUITY_DEBUGGING.md) — fault lab, DLQ reproduction, checkpoints, and fixture extraction.
- `beta` [Agent continuity product](AGENT_CONTINUITY_PRODUCT.md) — protocol, portable work offers, wrappers, trust-boundary profiles, conformance, and commercial contracts.

## Reference

- `stable` [REST API](API.md) — curated guide to the most-used routes and payloads.
- `stable` [Live OpenAPI](http://localhost:8080/swagger-ui) — complete generated request/response reference for a running engine.
- `beta` [API entitlements and generated-client gate](API_ENTITLEMENTS_AND_CLIENT_GATE.md) — plan admission limits and OpenAPI compatibility enforcement.
- `stable` [Configuration](CONFIGURATION.md) — TOML and environment variables.
- `stable` [CLI productization commands](CLI_PRODUCTIZATION.md) — contexts, deploy gates, and bounded debugging.
- `beta` [Mobile SDK](MOBILE_SDK.md) — iOS/Android API and build reference.
- `beta` [Mobile protected fields and device tools](MOBILE_PRIVACY_AND_TOOLS.md) — capability descriptors, opaque handles, redaction, and field-key rotation.
- `beta` [Typed dataflow](TYPED_DATAFLOW.md) — static reference checking and generated bindings.
- `beta` [Storage backend conformance](STORAGE_BACKEND_CONFORMANCE.md) — reusable minimum behavioral suite for third-party backends.
- `beta` [Tenant partition routing](TENANT_PARTITION_ROUTING.md) — authoritative backend placement, fencing epochs, and tenant moves.
- `beta` [Externalized state](EXTERNALIZATION.md) — payload offloading behavior and metrics.
- `beta` [Governed durable memory](GOVERNED_MEMORY.md) — memory authorization, retention, residency labels, deletion, and provenance.
- `stable` [Package registry](PACKAGE_REGISTRY.md) — signed object layout, publication, and consumer verification.
- `beta` [MCP authoring](MCP_AUTHORING.md) — Claude/Cursor setup and sequence authoring tools.
- `beta` [Agent-framework adapters](FRAMEWORK_ADAPTERS.md) — portable integration boundary for agent runtimes.
- [1.0 stability contract](../STABILITY.md) — compatibility and support windows.
- `beta` [Governed distribution](DISTRIBUTION_GOVERNANCE.md) — channels, deltas, private policy, attestations, and dependency locks.
- `beta` [Workflow compiler optimization](WORKFLOW_OPTIMIZER.md) — immutable optimization sidecars and equivalence guarantees.
- `stable` [Database migrations](../migrations/README.md) — immutability and checksum rules.
- [Licensing](LICENSING.md) — plain-language "Can I use this?" table derived from the LICENSE text.
- `experimental` [Benchmarks](BENCHMARKS.md) — reproducible cross-engine harness and methodology (no published results yet).
- [SchemaStore submission](SCHEMASTORE_SUBMISSION.md) — prepared editor-schema registration steps.

## Understand

- [Architecture](ARCHITECTURE.md) — crates, execution model, storage, concurrency, and observability.
- [Embedding applications](APPLICATIONS.md) — mobile, desktop, browser, edge, and game-engine use cases.
- [Engine capability priorities](ENGINE_FEATURE_PRIORITIES.md) — implemented engine primitives, deliberate bounds, and rejected duplicate abstractions.

## Component and example documentation

- [Examples index](../examples/README.md) — runnable examples by use case.
- `beta` [Activepieces sidecar](../activepieces/README.md) — execute community integration pieces.
- `beta` [Load generator](../loadgen/README.md) — isolated local traffic and stress generation.

## Historical records

These documents preserve what was reviewed at a point in time. They are useful
evidence, but current source and CI take precedence over their line numbers or
open-item lists.

- [Changelog](../CHANGELOG.md) — release history and unreleased changes.
- [Rust review](RUST_REVIEW.md) — July 2026 best-practice review record.
- [Security audit](SECURITY_AUDIT.md) — second Rust security audit record.

## Accuracy contract

- Canonical API paths start with `/api/v1`; bare paths remain compatibility aliases.
- CLI examples use the `orch8` client binary. Server examples use `orch8-server`.
- Product changes update the nearest guide in the same pull request.
- Commands in tutorials are verified by the relevant build or test suite; generated
  OpenAPI remains authoritative when a prose route description disagrees.
- Point-in-time audit reports never override current source, advisories, or CI.
- Overview docs avoid hard-coded test counts because the inventory changes more
  often than its meaning.
