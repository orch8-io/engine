# Portable Continuity Operations

This guide covers the operator actions that require judgment when portable
continuity is enabled. The engine fails closed when it cannot prove whether an
external effect happened.

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
