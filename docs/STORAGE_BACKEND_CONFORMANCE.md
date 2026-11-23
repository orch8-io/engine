# Storage backend conformance

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Third-party `StorageBackend` implementations can run Orch8's public minimum
behavior suite instead of copying SQLite-specific integration tests:

```rust,no_run
# async fn verify(storage: &dyn orch8_storage::StorageBackend)
#     -> Result<(), orch8_storage::conformance::ConformanceError> {
let report = orch8_storage::conformance::run_core_conformance(storage).await?;
println!("verified {} checks in {}", report.checks.len(), report.scope);
# Ok(())
# }
```

Run it only against an isolated test database. The suite intentionally leaves
one uniquely named deprecated sequence and one cancelled instance as durable
evidence; it does not delete rows because cascading cleanup could conceal a
referential-integrity failure.

The core contract verifies:

1. immutable sequence create/read round trips;
2. duplicate keys classify as `StorageError::Conflict`;
3. tenant-scoped instance idempotency;
4. cross-tenant lookup isolation;
5. exactly one compare-and-swap state transition wins;
6. block-output durability;
7. signal enqueue/delivery visibility; and
8. terminal instance plus deprecated-sequence evidence.

Passing this core suite does not certify performance, schema migrations,
continuity/capsule storage, object artifacts, mobile sync, or every optional
domain trait. Backends intended for production must also run the repository's
full backend-specific integration and concurrency tests under their real
database isolation level.

## Atomic restore

Run `run_atomic_restore_conformance(storage, configure_output_fault)` on an
isolated backend. The async fault hook receives the output ID to reject just
before writing it. Return a storage error containing
`ATOMIC_RESTORE_OUTPUT_FAULT` from that write. When the hook receives `None`,
remove the fault so the retries can proceed.

The suite checks that a failed restore leaves no instance, tree, outputs, or
externalized payloads. It then runs two concurrent retries and verifies that
exactly one publishes a complete snapshot. It leaves a terminal fixture for
inspection.

## Effect ledger

Run `run_effect_ledger_conformance(storage)` on an isolated backend. The suite
dispatches a prepared effect, saves an ambiguous result, and verifies that the
original attempt and a competing guarded attempt cannot dispatch. It also
checks that operator settlement writes its provenance once when completion is
submitted again.

## Worker leases

Run `run_worker_lease_conformance(storage)` on an isolated test database. The
suite races two claimers, renews the winning lease, and forces expiry with a
zero stale threshold. It verifies that the same worker ID gets a new claim
generation and resumes from the saved checkpoint. A completion or checkpoint
from the expired generation must fail.

## Cross-backend replay

The `backend_state_machine` integration test runs identical seeded batches
against fresh SQLite and PostgreSQL databases. It compares operation outcomes
and durable instance, signal, and checkpoint state after each step. On a
difference it saves the shortest reproducing subsequence of the seed and both
backend states under `target/backend-differential/`. CI retains these files
as a workflow artifact when the workspace test step fails.
