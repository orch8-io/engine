# Portable agent product quick start

This directory contains copy-and-run inputs for the framework-neutral Durable
Agent Handoff Protocol. The files are also exercised by Rust integration tests,
so they cannot silently drift away from the CLI and HTTP contracts.

## 1. Compile a placement policy

```bash
orch8 --output json portable compile-policy \
  "$(tr -d '\n' < examples/portable-agent-product/private-rag.policy)"
```

The result is the durable `CapsuleRequirements` and `LocalityPolicy` used during
placement. Unknown keys and missing security-sensitive runtime facts fail closed.

## 2. Validate or run a wrapped worker

[`local-worker.manifest.json`](local-worker.manifest.json) wraps
[`local-worker.sh`](local-worker.sh) without inheriting the host environment.
The process receives `{ "params": ..., "context": ... }` on stdin and must emit
one bounded JSON value on stdout.

```bash
printf '{"params":{"query":"hello"},"context":{}}' | \
  /bin/sh examples/portable-agent-product/local-worker.sh

orch8 portable worker examples/portable-agent-product/local-worker.manifest.json \
  --runtime-id 0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0 --once
```

The second command expects an Orch8 server at `ORCH8_URL`. See
[`profile-offer-request.json`](profile-offer-request.json) for an HTTP request
body that materializes the `private_rag` profile.

## 3. Score conformance

```bash
orch8 --output json portable score \
  examples/portable-agent-product/conformance-results.json
```

A certifiable implementation must submit every fixed check exactly once, attach
a SHA-256-shaped evidence digest, pass all mandatory checks, and score at least
900 millipoints. Send those results to
`POST /api/v1/continuity/conformance/certificates`, then send the returned signed
certificate to `POST /api/v1/continuity/conformance/badge`.

## 4. Validate a commercial deployment

[`commercial-plan.json`](commercial-plan.json) is a safe OEM example for
`POST /api/v1/continuity/commercial/validate`. Tenant isolation, conformance,
signed receipts, evidence retention, and protocol compatibility are mandatory.

For the endpoint reference, security model, and all ten ready-made trust-boundary
profiles, see [Agent continuity product](../../docs/AGENT_CONTINUITY_PRODUCT.md).
