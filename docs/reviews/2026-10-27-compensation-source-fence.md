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

The final isolated stack passed 189 Rust cases: 150 storage/continuity/ledger/
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
