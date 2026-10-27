# Durable compensation admission and worker settlement

Compensation startup now atomically binds its unchanged owner, instance metadata
and complete effect set, creates the run and parks the source under durable
`compensating` ownership. An intervening effect or source change refuses the
transaction instead of admitting a stale plan. Existing uncertain effects must
remain represented in hazards and residuals.

The same epoch retains authority for existing workers to report outcomes. Native
forward admission, scheduler advancement, worker polling and generic ownership
CAS cannot reopen the source. HTTP/gRPC settlement records the outcome and skips
forward propagation. A subsequent explicit run can handle newly confirmed effects.

The new ownership value requires updated readers. It is an additive persisted
state with no schema migration; old readers fail to decode it. Completing a
rollback does not automatically reopen forward execution. No release is published.

The initial isolated macOS stack passed 189 Rust cases: 150 storage/continuity/ledger/
recovery, six HTTP evidence, 14 gRPC E2E and 19 engine guard tests. All 125
TypeScript invariant/compensation/runtime E2E cases passed against its own server
and isolated PostgreSQL. Strict all-target Clippy passed for storage, engine,
API, gRPC, CLI and mobile; formatting, typecheck and documentation checks passed.

Coverage includes concurrent startup, injected rollback, stale source/evidence,
migration/handoff exclusion, late preparation, native admission, all worker poll
paths (with eligible positive controls), encrypted storage and real late worker
settlement without forward propagation. Poll predicates bind the instance's
tenant for indexed ownership lookup. SQLite startup retains its registered mobile
account eligibility check.

Remaining review work includes immutable invocation-specific parameters/rules,
explicit local archival outcomes and separate-database mobile owner acknowledgements.


This transaction fences work governed by the same authoritative database. An
owner running from an independent offline database still needs a signed stop and
outcome acknowledgement before a coordinator can certify quiescence. Legacy
worker rows without effect bindings also need explicit receipt/outcome accounting;
legacy worker completion currently does not invent a ledger receipt. These open
contracts prevent treating this patch as a full distributed compensation proof.

CI exposed a Linux/PostgreSQL precision mismatch: task timestamps written with
nanoseconds were decoded at PostgreSQL microsecond precision and compared in
Rust with the original value. Startup now binds the expected timestamp in the
locking SQL predicate, applying the same precision as the write. The storage
fixture deliberately uses sub-microsecond timestamps; all 40 cases pass across
PostgreSQL, SQLite and both encryption wrappers. Revision-only context changes
still reject a stale snapshot without freezing or inserting a compensation run.

A source can complete between planning and admission. HTTP startup now retries
a fresh complete snapshot up to eight times; it never relaxes the transactional
source/evidence comparison. Existing-run conflicts return immediately. A
deterministic fault barrier reproduces completion during admission and verifies
success with the refreshed snapshot. All seven HTTP safety cases pass.

The nested cancellation E2E now waits for a durable human-input barrier inside
both scopes before cancelling, then releases that barrier. This tests shielding
across a persisted wait and removes the assumption that CI starts both scopes
within 200 milliseconds.

Full-suite verification also found two test harness gaps. Event-trigger tests
now honor the configured E2E URL/port. SSRF blocking runs in a dedicated server
with private-network access disabled, rather than sharing the mock-friendly
server that explicitly enables it. A new reachable loopback server provides a
positive control and verifies that a blocked workflow delivers no HTTP request.
That case fails with protection disabled and passes with protection enabled; all
five dedicated SSRF cases pass through the real standalone harness. Both E2E
runners consume the same self-managed-suite registry, so the SSRF suite remains
part of CI.

The repaired stack passed all 1,586 shared E2E cases and five dedicated SSRF
cases, with no skips, cancellations or failures. Strict all-target Clippy passed
for storage, engine, API, gRPC, CLI and mobile; formatting, TypeScript typecheck
and documentation checks passed. CI must revalidate the repaired commit.
