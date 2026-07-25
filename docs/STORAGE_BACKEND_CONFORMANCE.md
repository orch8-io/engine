# Storage backend conformance

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
