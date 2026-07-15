# Orch8 documentation

Use this page as the repository documentation index. The running server is the
source of truth for HTTP schemas: open `/swagger-ui` or download
`/api-docs/openapi.json`. Both require the configured API key; health probes and
inbound webhook routes are intentionally public.

## Learn

- [Quick start](QUICK_START.md) — install, start, and complete a first execution.
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

- [REST API](API.md) — hand-written guide to the most-used routes and payloads.
- [Live OpenAPI](http://localhost:8080/swagger-ui) — complete generated request/response reference for a running engine.
- [Configuration](CONFIGURATION.md) — TOML and environment variables.
- [Mobile SDK](MOBILE_SDK.md) — iOS/Android API and build reference.
- [Typed dataflow](TYPED_DATAFLOW.md) — static reference checking and generated bindings.
- [Externalized state](EXTERNALIZATION.md) — payload offloading behavior and metrics.

## Understand

- [Architecture](ARCHITECTURE.md) — crates, execution model, storage, concurrency, and observability.
- [Embedding applications](APPLICATIONS.md) — mobile, desktop, browser, edge, and game-engine use cases.

## Accuracy contract

- Canonical API paths start with `/api/v1`; bare paths remain compatibility aliases.
- CLI examples use the `orch8` client binary. Server examples use `orch8-server`.
- Product changes update the nearest guide in the same pull request.
- Commands in tutorials are verified by the relevant build or test suite; generated
  OpenAPI remains authoritative when a prose route description disagrees.

