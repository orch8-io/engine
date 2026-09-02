# Orch8 documentation

Use this page as the canonical map of repository documentation. It separates
tutorials, task-oriented guides, reference, explanation, and historical audit
records so readers know what kind of answer each document provides.

The running server is the source of truth for HTTP schemas: open `/swagger-ui`
or download `/api-docs/openapi.json`. Both require the configured API key;
health probes and inbound webhook routes are intentionally public.

## Learn

- [Progressive quick starts](quick-starts/README.md) — six hands-on levels from a
  local workflow to workers, recovery, PostgreSQL, and guarded releases.
- [Topic quick starts](quick-starts/topics/README.md) — independent recipes for
  cron, webhooks, approvals, typed dataflow, large payloads, integrations,
  observability, and load testing.
- [Sequences](SEQUENCES.md) — learn the workflow JSON model and block types.
- [Agent patterns](agent-patterns/README.md) — run four composable AI workflow examples.
- [Email classifier](../examples/email-classifier/README.md) — a complete TypeScript worker and webhook application.
- [Portable agent product](../examples/portable-agent-product/README.md) — compile policy, wrap a local worker, score conformance, and validate an OEM plan.

## Operate

- [Dashboard](DASHBOARD.md) — connect the operator console and use its current surfaces.
- [Safe releases](RELEASES.md) — diff, validate, canary, evaluate, promote, and roll back.
- [Deployment](DEPLOYMENT.md) — Docker, Kubernetes, cloud targets, and the production checklist.
- [Secure production bootstrap](SECURE_BOOTSTRAP.md) — scaffold, validate, start, and readiness-check a secure node.
- [Node roles](NODE_ROLES.md) — assemble all-in-one, control, executor, gateway, and edge processes; operate managed-control sessions and fleet draining.
- [Operator support bundle](SUPPORT_BUNDLE.md) — collect bounded, redacted diagnostics atomically.
- [External workers](WORKERS.md) — poll, heartbeat, complete, and fail work from any language.
- [Negotiated gRPC worker stream](GRPC_WORKER_STREAM.md) — worker sessions, control, resumable artifacts, telemetry, and mTLS identity.
- [Webhooks](WEBHOOKS.md) — delivery, signatures, replay protection, and receiver example.
- [Durable push delivery](PUSH_DELIVERY.md) — APNs/FCM wake outbox lifecycle and recovery.
- [Governed execution wakes](PUSH_GOVERNANCE.md) — tenant credential routing, signed wake metadata, collapse, and token quarantine.
- [Continuity operations](CONTINUITY_OPERATIONS.md) — portable handoff, migration, effects, and provenance.
- [Continuity debugging](CONTINUITY_DEBUGGING.md) — fault lab, DLQ reproduction, checkpoints, and fixture extraction.
- [Agent continuity product](AGENT_CONTINUITY_PRODUCT.md) — protocol, portable work offers, wrappers, trust-boundary profiles, conformance, and commercial contracts.

## Reference

- [REST API](API.md) — curated guide to the most-used routes and payloads.
- [Live OpenAPI](http://localhost:8080/swagger-ui) — complete generated request/response reference for a running engine.
- [API entitlements and generated-client gate](API_ENTITLEMENTS_AND_CLIENT_GATE.md) — plan admission limits and OpenAPI compatibility enforcement.
- [Configuration](CONFIGURATION.md) — TOML and environment variables.
- [CLI productization commands](CLI_PRODUCTIZATION.md) — contexts, deploy gates, and bounded debugging.
- [Mobile SDK](MOBILE_SDK.md) — iOS/Android API and build reference.
- [Mobile protected fields and device tools](MOBILE_PRIVACY_AND_TOOLS.md) — capability descriptors, opaque handles, redaction, and field-key rotation.
- [Typed dataflow](TYPED_DATAFLOW.md) — static reference checking and generated bindings.
- [Storage backend conformance](STORAGE_BACKEND_CONFORMANCE.md) — reusable minimum behavioral suite for third-party backends.
- [Tenant partition routing](TENANT_PARTITION_ROUTING.md) — authoritative backend placement, fencing epochs, and tenant moves.
- [Externalized state](EXTERNALIZATION.md) — payload offloading behavior and metrics.
- [Governed durable memory](GOVERNED_MEMORY.md) — memory authorization, retention, residency labels, deletion, and provenance.
- [Package registry](PACKAGE_REGISTRY.md) — signed object layout, publication, and consumer verification.
- [Governed distribution](DISTRIBUTION_GOVERNANCE.md) — channels, deltas, private policy, attestations, and dependency locks.
- [Workflow compiler optimization](WORKFLOW_OPTIMIZER.md) — immutable optimization sidecars and equivalence guarantees.
- [Database migrations](../migrations/README.md) — immutability and checksum rules.

## Understand

- [Architecture](ARCHITECTURE.md) — crates, execution model, storage, concurrency, and observability.
- [Embedding applications](APPLICATIONS.md) — mobile, desktop, browser, edge, and game-engine use cases.
- [Engine capability priorities](ENGINE_FEATURE_PRIORITIES.md) — implemented engine primitives, deliberate bounds, and rejected duplicate abstractions.

## Component and example documentation

- [Examples index](../examples/README.md) — runnable examples by use case.
- [Activepieces sidecar](../activepieces/README.md) — execute community integration pieces.
- [Load generator](../loadgen/README.md) — isolated local traffic and stress generation.

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
