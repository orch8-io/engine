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

## Operate

- [Dashboard](DASHBOARD.md) — connect the operator console and use its current surfaces.
- [Safe releases](RELEASES.md) — diff, validate, canary, evaluate, promote, and roll back.
- [Deployment](DEPLOYMENT.md) — Docker, Kubernetes, cloud targets, and the production checklist.
- [External workers](WORKERS.md) — poll, heartbeat, complete, and fail work from any language.
- [Webhooks](WEBHOOKS.md) — delivery, signatures, replay protection, and receiver example.
- [Continuity operations](CONTINUITY_OPERATIONS.md) — portable handoff, migration, effects, and provenance.
- [Continuity debugging](CONTINUITY_DEBUGGING.md) — fault lab, DLQ reproduction, checkpoints, and fixture extraction.

## Reference

- [Engine capability priorities](ENGINE_FEATURE_PRIORITIES.md) — engine-only roadmap, estimates, and proposals deliberately not pursued.
- [REST API](API.md) — curated guide to the most-used routes and payloads.
- [Live OpenAPI](http://localhost:8080/swagger-ui) — complete generated request/response reference for a running engine.
- [Configuration](CONFIGURATION.md) — TOML and environment variables.
- [Mobile SDK](MOBILE_SDK.md) — iOS/Android API and build reference.
- [Typed dataflow](TYPED_DATAFLOW.md) — static reference checking and generated bindings.
- [Externalized state](EXTERNALIZATION.md) — payload offloading behavior and metrics.
- [Database migrations](../migrations/README.md) — immutability and checksum rules.

## Understand

- [Architecture](ARCHITECTURE.md) — crates, execution model, storage, concurrency, and observability.
- [Embedding applications](APPLICATIONS.md) — mobile, desktop, browser, edge, and game-engine use cases.

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
