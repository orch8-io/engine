# Agent continuity product surface

Orch8 implements the **Durable Agent Handoff Protocol** as a transport-neutral
contract above the continuity engine. An agent can offer work to a trusted
runtime, move ownership across a device boundary, fence external effects, and
produce a verifiable bill of execution without adopting a new agent framework.

The canonical Rust contracts live in
`orch8-types/src/continuity_product.rs`. They are exposed through stateless HTTP
endpoints and local-first `orch8 portable` commands.

## Quick start

Inspect the protocol:

```bash
orch8 --output json portable protocol
```

Wrap an existing MCP server:

```bash
orch8 portable wrap existing-agent \
  --adapter mcp \
  --entrypoint https://agent.example/mcp \
  --handler tool.run \
  --policy 'classification=restricted;runtime_kinds=desktop;min_trust=attested' \
  --secret-ref vault://source-control
```

Run a local process manifest as a desktop worker. The UUID is deliberately
explicit so the runtime keeps one identity across restarts:

```bash
orch8 portable worker orch8-portable.json \
  --runtime-id 018f4c72-8c3d-7a22-9e12-ff1a2b3c4d5e
```

The worker clears the inherited environment, restores only allowlisted names,
passes task parameters and context as JSON on stdin, requires one bounded JSON
value on stdout, enforces the task timeout, advertises bounded runtime
capabilities on every poll, and completes or fails with the current claim
epoch. A manifest can self-advertise only `registered` trust; signed or attested
placement still requires the server's verified registration flow. This makes
policy text unable to escalate trust and stale desktop processes unable to
mutate a reclaimed task.
For MCP manifests it translates each claimed task into a bounded JSON-RPC
`tools/call` request carrying the task, instance, claim epoch, and durable
context metadata. Generic HTTPS gateways receive the equivalent versioned
handoff envelope. Non-loopback plaintext HTTP is rejected.

Compile placement policy before deployment:

```bash
orch8 portable compile-policy \
  'classification=restricted;runtime_kinds=mobile;min_trust=attested;human_ui=true'
```

Inspect a deployable trust-boundary profile:

```bash
orch8 portable profile executive-airlock
```

Score conformance evidence:

```bash
orch8 --output json portable score \
  examples/portable-agent-product/conformance-results.json
```

The scorer uses a fixed eight-check denominator. Omitting mandatory checks
cannot produce a certificate.

For checked-in inputs that exercise policy compilation, a secret-reference-only
local worker, all conformance checks, profile offer creation, and OEM validation,
start with the [portable agent product example](../examples/portable-agent-product/README.md).
The integration suite parses and runs these exact files.

## HTTP surface

All routes use the canonical `/api/v1` prefix:

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/continuity/protocol` | Discover protocol version, adapters, and invariants |
| `POST` | `/continuity/offers/validate` | Validate a portable work offer |
| `POST` | `/continuity/policies/compile` | Compile placement policy into core types |
| `POST` | `/continuity/gateways/validate` | Validate HTTP, MCP, process, or mobile wrapper manifests |
| `POST` | `/continuity/receipts/verify` | Verify a bill-of-execution digest and terminal effects |
| `POST` | `/continuity/conformance/certificates` | Issue a threshold-gated conformance certificate |
| `POST` | `/continuity/conformance/badge` | Verify a server-signed certificate and render its SVG badge |
| `GET` | `/continuity/profiles` | List deployable trust-boundary profiles |
| `POST` | `/continuity/profiles/{profile}/offers` | Materialize a profile as a claimable portable work offer |
| `POST` | `/continuity/commercial/validate` | Validate hosted, private, or OEM relay invariants |

The routes use the server's normal API-key middleware. When tenant headers are
required, send `x-tenant-id` as well as `x-api-key`. The complete generated
request and response schemas are available at `/swagger-ui` and
`/api-docs/openapi.json` under the `continuity-product` tag.

Materialize and validate a restricted private-RAG offer:

```bash
curl -sS -X POST "$ORCH8_URL/continuity/profiles/private_rag/offers" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H 'content-type: application/json' \
  --data @examples/portable-agent-product/profile-offer-request.json \
  > /tmp/private-rag-offer.json

curl -sS -X POST "$ORCH8_URL/continuity/offers/validate" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H 'content-type: application/json' \
  --data @/tmp/private-rag-offer.json
```

Successful validation returns `{"valid":true}`. Contract violations return
HTTP `400` with the standard Orch8 error envelope and a specific reason such as
an unknown policy key, expired offer, digest mismatch, unsafe manifest, or
missing commercial invariant. Certificate issuance returns `503` when the
continuity signing key is not configured.

The hardened `build_continuity_gateway_router` includes these endpoints and
the durable handoff endpoints, but excludes the general management API.

## Placement policy language

Statements are semicolon-separated `key=value` pairs. Supported keys are:

- `classification`: `public`, `internal`, `confidential`, or `restricted`
- `runtime_kinds`: comma-separated server, edge, mobile, desktop, browser
- `min_trust`: unverified, registered, signed, attested
- `regions`, `handlers`, `plugins`, `credentials`, `hardware`
- `require_hardware`, `require_offline`, `human_ui`, `network`
- `connectivity`, `minimum_battery`, `maximum_cost`, `maximum_latency_ms`

Unknown keys and invalid values fail closed. Work-offer claiming enforces both
the compiled capability requirements and locality policy.

## High-score opportunity coverage

This table maps every opportunity scored above 21 to an executable surface.
Existing core functionality is identified because the product contract builds
on it rather than duplicating it.

| # | Opportunity | Implementation and verification |
|---:|---|---|
| 1 | Durable Agent Handoff Protocol | `ProtocolVersion`, protocol discovery endpoint and CLI |
| 3 | Portable Work Offer API | `PortableWorkOffer::validate` and capability/policy-aware claim check |
| 4 | Runtime Passport | Capability-bound passport with structural and pluggable cryptographic verification |
| 5 | Execution Receipt Standard | Tamper-evident `ExecutionReceipt`, digest and signature verification |
| 6 | Placement Policy Language | Fail-closed compiler to `CapsuleRequirements` and `LocalityPolicy` |
| 10 | Continuity Gateway | Transport-neutral manifest, executable HTTPS gateway adapter, and hardened gateway router |
| 11 | `orch8 wrap` | Atomic local manifest generation through `orch8 portable wrap` |
| 12 | MCP Continuity Bridge | Executable bounded JSON-RPC `tools/call` bridge preserving task, instance, context, and claim-epoch metadata |
| 14 | Local Worker | Cross-platform CLI worker executes bounded local processes with capability-aware claims, stable runtime identity, timeouts, environment isolation, and stale-claim fencing |
| 15 | Handoff Sandbox | Existing `orch8 demo portable-agent` performs a real three-runtime capsule round trip |
| 16 | Continuity Chaos Lab | Existing fault-lab endpoint plus duplicate-effect/offline conformance checks |
| 17 | Agent Continuity Score | Fixed eight-check `score_conformance` implementation and CLI |
| 18 | CI Certification Badge | Threshold-gated, server-signed certificate and verified SVG badge endpoint |
| 21 | Private RAG Relay | `private-rag` restricted local-execution profile |
| 22 | Biometric Approval Rail | Attested mobile biometric profile using the existing native capability bridge |
| 23 | Executable Data Residency | Residency profile, compiled locality policy, and existing residency evidence API |
| 25 | Agent Bill of Execution | Receipt includes models, tools, locations, policy, consent, and effects |
| 29 | Audit Evidence Exporter | Audit profile requiring provenance, policy, and execution-receipt evidence |
| 31 | Secret-Safe Coding Agent | Attested desktop profile with source-control reference and secure hardware |
| 32 | Regulated Onboarding | Attested human-UI profile forbidding identity-document disclosure |
| 35 | Fraud Challenge Handoff | Attested mobile challenge profile |
| 36 | Executive Action Airlock | Biometric, consent, and effect-receipt profile |
| 40 | Personal Data-Vault Agents | Local computation profile forbidding vault-content disclosure |
| 41 | Hosted Continuity Relay | Hosted deployment contract and hardened continuity-only router |
| 42 | Conformance Cloud | Server-signed certificate endpoint, fixed gauntlet, evidence digest, mandatory gates |
| 44 | OEM Runtime Licensing | OEM-embedded plan requiring product identity, isolation, conformance, and receipts |

## Security invariants

- Protocol major versions must match.
- Work offers expire and carry idempotency keys.
- Capability advertisements and passports are bounded and expiring.
- Placement and locality policies fail closed.
- Receipt digests cover locations, tools, models, consent, and effect outcomes.
- Certificates require every mandatory conformance check.
- Hosted and OEM plans require tenant isolation, signed receipts, conformance,
  evidence retention, and the current protocol major.
- Secret values never belong in manifests; only secret references are allowed.

## Verification

Focused checks:

```bash
cargo test -p orch8-types continuity_product
cargo test -p orch8-api --test continuity_product_e2e
cargo test -p orch8-cli --test cli_e2e portable
```

The domain suite covers every scored opportunity plus bounds, malformed policy,
signature, receipt, conformance-gaming, and commercial-invariant failures. The
API suite uses a real listener, versioned router, middleware, JSON, and signing
key. The CLI suite invokes the compiled binary, every profile, wrapper and score
flows, a one-shot real API poll, and all checked-in example fixtures. Existing
storage, mobile, and continuity suites remain authoritative for atomic
ownership, replay safety, capsule signing, encrypted artifacts, and claims.
