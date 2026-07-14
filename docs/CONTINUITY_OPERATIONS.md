# Portable Continuity Operations

## Reserve and settle multidimensional budgets

Reserve estimated usage before dispatch so concurrent workers cannot pass a check-then-act race:

```bash
orch8 execution reserve-budget "$CONTINUITY_ID" reserve.json
```

After dispatch, reconcile the reservation with actual usage. If dispatch never happened, release it instead. Both operations are compare-and-set transitions: only one caller can settle a reservation, and a reservation from an older execution epoch is rejected.

```bash
orch8 execution reconcile-budget "$CONTINUITY_ID" "$RESERVATION_ID" actual.json
orch8 execution release-budget "$CONTINUITY_ID" "$RESERVATION_ID" tenant.json
orch8 execution budget-reservations "$CONTINUITY_ID" --tenant-id "$TENANT_ID"
```

Actual reconciled usage remains cumulative for later reservations. A release contributes zero usage. Requests must include all six usage dimensions and a versioned estimation table identifier; negative usage fails closed.

## Route providers without replaying unsafe effects

Provider choices filter breaker state, region, price, latency, quality, and idempotency before scoring. Retry requests should include `prior_provider` and `operation_idempotent`. If the prior provider may already have observed a non-idempotent operation, the router stays on that provider or returns no eligible provider; cross-provider replay requires `effect_policy_approved: true` from the effect-resolution workflow.

```bash
orch8 execution choose-provider provider-request.json
```

Tenant-linked requests must include both `tenant_id` and `continuity_id`. The selected provider, model, pricing version, stable cohort, and policy findings are appended to execution provenance without recording prompts or provider payloads.

## Gate candidates with retained evaluation evidence

Append direct or deferred evaluation results to each continuity execution with an explicit scope. Scope can bind evidence to a sequence/version, block, model, prompt hash, tool-set hash, and release. Mark delayed results as `outcome: pending`; a stored gate with any matching pending result is inconclusive and cannot pass.

```bash
orch8 execution append-evaluation "$CONTINUITY_ID" score.json
orch8 execution stored-evaluation-gate stored-gate.json
```

The stored gate loads both baseline and candidate scores server-side, matches the evaluator and full scope, weights each score by `sample_size`, and records the report in candidate provenance. A positive `minimum_samples` and non-negative regression allowance are required.

This guide covers the operator actions that require judgment when portable
continuity is enabled. The engine fails closed when it cannot prove whether an
external effect happened.

## Verify execution provenance

Every retained entry contains only a payload digest and a bounded redacted
summary. List a chain with
`GET /continuity/executions/{id}/provenance?tenant_id=...`, then verify it with
`GET /continuity/executions/{id}/provenance/verify?tenant_id=...&expected_head=...`.
Always retain the expected head outside the execution database when deletion
detection matters; a chain cannot prove that its own final entries were removed
without that anchor.

The active continuity signing key is trusted automatically. During rotation,
set `ORCH8_CONTINUITY_TRUSTED_SIGNING_KEYS_JSON` to a JSON object mapping old
key IDs to base64 Ed25519 public keys. Never put private keys in this registry.
Package builders, model routers, and terminal-state observers can append their
already-computed digest with `POST /continuity/executions/{id}/provenance` using
`package_identity`, `model_selected`, or `terminal_outcome`; raw payloads are not
accepted or stored.

## Upgrade to migration 061

Migration `061_continuity_instance_lookup.sql` adds a unique PostgreSQL index
on `(tenant_id, current_instance_id)`. A runtime-local instance may belong to
only one continuity execution. Before upgrading a database that already has
continuity data, check for legacy duplicates:

```sql
SELECT tenant_id,
       record->>'current_instance_id' AS current_instance_id,
       count(*) AS continuity_count
FROM continuity_executions
GROUP BY tenant_id, record->>'current_instance_id'
HAVING count(*) > 1;
```

The query must return no rows. If it reports duplicates, stop the upgrade and
determine the authoritative continuity execution from its ownership, handoff,
capsule, provenance, and effect evidence. Do not delete or merge records based
only on creation time. Preserve the rejected record for audit before applying
an operator-approved repair.

SQLite schema version 27 applies the same invariant to newly initialized
databases.

## Upgrade to migration 062

Migration `062_continuity_locations.sql` creates the immutable owner-per-epoch
location ledger. It backfills the current owner, instance, and epoch for every
existing continuity execution. Historical locations that predate the migration
cannot be reconstructed safely from timestamps, so the backfilled row is the
earliest cryptographically unambiguous boundary for an upgraded installation.
Every later ownership claim is recorded atomically with the owner change.

Query the ordered path with
`GET /continuity/executions/{continuity_id}/locations?tenant_id=...`. Epoch—not
wall-clock time—is authoritative if timestamps from different runtimes disagree.
SQLite schema version 28 creates the same ledger for embedded runtimes.

## Upgrade to migration 064

Migration `064_live_migrations.sql` creates the tenant-scoped durable migration
ledger used for plan and transition compare-and-swap. Apply it before exposing
the live-migration endpoints. Migration records contain rollback context and
checkpoint state; configure `ORCH8_ENCRYPTION_KEY` so the storage decorator
encrypts those fields at rest. SQLite schema version 30 creates the same ledger.

## Live sequence migration

Plan only at a durable waiting or paused boundary. The server—not the caller—
loads the source and exact target definitions, computes their semantic diff,
checks completed checkpoint outputs, replays recorded evidence with real
handlers disabled, and evaluates configured invariants. State transforms are
bounded, deterministic version-1 operations over dotted checkpoint paths:
`copy`, `move`, and `drop`. Unknown versions, operations, oversized paths,
duplicate destinations, and missing sources are rejected.

Use the returned disposition as an authorization boundary. `automatic` may be
applied directly; `approval_required` needs `approved: true`; `pin` and
`incompatible` cannot be applied. Apply rechecks the original state, sequence,
and ownership epoch, then creates a signed encrypted pre-migration capsule and
atomically advances the instance to the target sequence in `paused` state.
Inspect the durable record before explicitly resuming normal execution.

Rollback is intentionally narrow. It is accepted only while the retention
window is open, the target remains paused under the applied epoch, and the
retained capsule artifact passes its byte-count and SHA-256 checks. Any
dispatched, committed, unknown, or verified effect in the target epoch blocks
rollback; investigate or compensate that effect instead of pretending the
external world was restored. A successful rollback atomically restores the
source context, checkpoint, sequence, waiting/paused state, and advances the
epoch again.

The equivalent CLI flow is:

```text
orch8 execution migration-plan plan.json
orch8 execution migration-get <plan-id> --tenant-id tenant-a
orch8 execution migration-apply <plan-id> approval.json
orch8 execution migration-rollback <plan-id> rollback.json
```

## Upgrade to migration 065

Migration `065_compensation_runs.sql` adds the durable compensation run ledger;
`066_compensation_active_run.sql` adds the database-enforced single-active-run
invariant. Its parameters, provider receipt identifiers, and failure evidence
may contain sensitive values, so keep `ORCH8_ENCRYPTION_KEY` configured. SQLite
schema version 31 creates the equivalent embedded table and index.

## Receipt-backed compensation

Compensation is a new external action, not time travel. Configure it on the
step that creates the original effect:

```json
{
  "type": "step",
  "id": "charge",
  "handler": "payments.charge",
  "params": { "amount": 4200 },
  "compensation": {
    "handler": "payments.refund",
    "params": { "amount": 4200 },
    "depends_on": ["reserve_inventory"],
    "verification": "provider_receipt"
  }
}
```

Preview first. The planner considers only `committed` and `verified` effect
receipts and reverses the declared forward dependencies; a block observed in
the execution tree without a committed receipt is never compensated. Missing
rules and unknown original effects appear as stable hazards. Starting a run
does not erase those hazards.

Workers claim exactly one ordered compensation step under a bounded lease.
Every step carries `compensate:<original-effect-id>` as its immutable
idempotency key. A lease that expires before completion becomes `unknown` and
blocks later steps until an operator verifies the provider outcome; it is not
blindly redelivered. `provider_receipt` policy requires provider evidence,
while `manual` holds the step in `verification_pending`. Only accepted evidence
advances the original receipt to `compensated`.

A run may finish as `completed_with_residuals`. This is expected when an
original effect is unknown, no compensation rule exists, a compensation fails,
or outcome evidence remains inconclusive. Treat the residual list as required
operator work; never describe compensation as restoring the world perfectly.

## Capability and locality routing

Runtime advertisements are short-lived, tenant-scoped facts. Alongside
handlers, plugins, hardware, regions, trust, and offline support, a runtime may
advertise its current `connectivity`, `battery_percent`,
`estimated_cost_microunits`, and `estimated_latency_ms`. Set `draining: true`
before maintenance: the runtime remains visible in rejected-candidate evidence
but cannot receive new work. Advertisements live for at most five minutes and
must be refreshed monotonically.

Locality policies are bounded data, not executable code. A rule applies to one
data classification and may constrain exact runtime IDs, runtime kinds,
regions, trust, offline support, hardware, connectivity, battery, cost, and
latency. Multiple matching rules are conjunctive. Contradictory device or
region intersections are rejected statically. Missing connectivity, battery,
cost, latency, or region facts produce `unknown`; confidential, restricted,
residency, and trust-sensitive work is never routed on an unknown result.

For example, restricted PII can be pinned to one device:

```json
{
  "version": 1,
  "rules": [{
    "classification": "restricted",
    "allowed_runtime_ids": ["<device-runtime-id>"],
    "minimum_trust": "registered"
  }]
}
```

Cloud inference can require Wi-Fi and a cost ceiling:

```json
{
  "version": 2,
  "rules": [{
    "classification": "confidential",
    "allowed_connectivity": ["wifi"],
    "maximum_cost_microunits": 50000,
    "maximum_latency_ms": 1000
  }]
}
```

Call the handoff preview with the requirements, policy, and classification.
The response contains a `placement_decision` with the chosen runtime and every
candidate's outcome, score, and finding codes. Create the handoff using the
same inputs plus `placement_decision_id` and `preview_sha256`. The engine
re-evaluates live facts at creation and again at export; a changed or expired
advertisement returns conflict before transfer begins. An explicit destination
may override a soft score preference, such as remaining on the current
runtime, but cannot override a hard denial.

## Portable mobile capsule transport

An isolated mobile runtime cannot read the server's object store and must
never receive the engine master encryption key. For device-bound handoffs, the
device generates a fresh random 32-byte transfer key and a runtime-local
instance UUID. Send the base64 key only over the authenticated handoff request:

```json
{
  "tenant_id": "tenant-a",
  "expires_in_seconds": 300,
  "payload_key_base64": "<32 random bytes, base64>"
}
```

The export response includes `capsule` (the signed manifest) and
`payload_base64` (the separately transported encrypted artifact). The
manifest binds the ciphertext hash, byte count, destination runtime, source
epoch, expiry, and transfer-key identifier. Never reuse a transfer key.

Before disconnecting:

1. Load the exact signed sequence version on the device.
2. Import the bundle into server quarantine with
   `POST /continuity/capsules/import`, including the payload, transfer key, and
   device-selected `destination_instance_id`.
3. Import the same bundle through the mobile SDK's
   `importContinuityCapsule`; it verifies the trusted Ed25519 root, ciphertext
   hash, destination, tenant, epoch, expiry, sequence hash, and AEAD binding,
   then leaves the local instance paused.
4. Accept the server handoff using that same destination-local instance ID.
5. Call mobile `activateContinuityCapsule`. Local ownership advances before
   scheduling, so a process kill can delay work but cannot execute the capsule
   under the source epoch.

Import, activation, and redelivery are idempotent. Preserve the bundle and key
only until both quarantine imports are confirmed; then erase the transfer key.
The device may execute with networking disabled after activation.

To return ownership, refresh the device runtime registration with its raw
base64 Ed25519 `capsule_signing_public_key`, register the destination runtime,
and create a preview-bound return handoff. The device calls
`exportContinuityCapsule` with a destination-generated payload key and a host
`CapsuleSigner`. The signer callback receives only the canonical manifest's
SHA-256 digest, so Secure Enclave/KeyStore private keys remain non-exportable
and never enter Rust memory.

Upload the returned manifest and encrypted payload to
`POST /continuity/handoffs/{id}/attach-device-capsule`. The control plane
requires a live source-runtime registration, an exact match to its signing
key, source runtime, continuity ID, epoch, and requested destination, then
imports the destination quarantine before atomically changing ownership to
`transferring`. Repeating the same attach after a lost response returns the
original imported instance. Accept and resume it through the normal handoff
endpoints. A different capsule or instance cannot reuse that idempotency slot.

## Unknown external effects

An effect receipt enters `unknown` when dispatch was durable but the engine did
not receive conclusive success or failure evidence. This can happen after a
timeout, worker failure callback, process crash, or lost provider response.
The engine blocks later dispatches for the same continuity execution and block
until an operator resolves the uncertainty.

1. List the continuity execution's effect receipts with
   `GET /continuity/executions/{continuity_id}/effects`.
2. Match `effect_id`, `block_id`, request hash, destination fingerprint,
   idempotency key, epoch, and attempt to the external provider's records.
3. If the provider proves the request completed, resolve the receipt as
   `committed` and attach the provider receipt identifier.
4. If the provider proves the request was not applied, resolve it as
   `abandoned` and record the evidence used to reach that conclusion.
5. If evidence remains inconclusive, leave the receipt `unknown` and escalate.

Never blindly retry an `unknown` effect. Orch8 provides at-most-once dispatch
protection, not a universal exactly-once guarantee; a provider-side idempotency
key is still the strongest protection against duplicated external effects.

## External workers

An external-worker effect remains `dispatched` after its task is durably
enqueued. Only the worker that currently owns the claimed task may settle it.
The complete callback commits the receipt before completing the task; the fail
callback marks it unknown before applying task retry handling. Repeated
callbacks are idempotent, but callbacks from another worker are rejected.

## Synchronous at-most-once invariants

Use a synchronous guard only for a safety property that must stop provider
dispatch. Create a sequence/version-scoped `effect_at_most_once` invariant with
`commit_guard: true` and the matching effect kind. Other invariant rules cannot
be commit guards and are rejected at creation time.

At each matching effect, Orch8 atomically compares the effect kind,
destination fingerprint, and canonical request hash against active receipts in
the same continuity execution. The first prepared receipt advances to
`dispatched`; an identical concurrent or later receipt remains `prepared` and
the step fails before invoking its handler or publishing an external-worker
task. This is conservative: `dispatched`, `committed`, `unknown`, and `verified`
evidence all block a duplicate, because provider outcome ambiguity must not be
treated as permission to retry.

Inspect continuously recorded evidence with:

```text
GET /continuity/executions/{continuity_id}/invariants/results?tenant_id={tenant_id}
```

Results are deduplicated by invariant and evidence, so repeated observation of
the same duplicate produces one violation. Keep provider idempotency keys as a
second layer of protection; an invariant guard cannot make a non-idempotent
provider transaction exactly once after network or process failure.
