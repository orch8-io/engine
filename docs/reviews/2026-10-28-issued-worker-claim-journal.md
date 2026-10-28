# Durable issued worker claims

This is the persistence layer for the original claimant-ownership gate. It does
not finish transport authorization, late factual acceptance or worker artifact
access, and must not be represented as a completed ownership protocol.

Dispatch admission identifies original input and invocation. Accepted completion
and resolution receipts identify winning outcomes. Neither retained every issued
lease's authenticated owner independently of hot task history. The new journal
records each claimed task/generation, actual worker, tenant/physical instance,
block/handler/queue/attempt, original invocation/admission and declared principal.
It contains no task params, application context, checkpoint or credential hashes.

Principal identity distinguishes stored key ID, mapped workload identity, bound
runtime kind/runtime/device, verified root authority, unsecured transport and
legacy claim paths. Unsecured and legacy records explicitly lack authenticated
ownership. The validated request is an internal storage contract: transport
adapters must derive its principal from verified authentication, never worker
request JSON. Session refresh must preserve the stable identity, not infer
ownership from a new token's JTI or a reused worker label.

All five normal claim paths retain metadata in the same transaction as the
lease generation and Claimed event. Existing primitives honestly record Legacy;
the new authorized selector threads an explicit principal through plain,
tenant, queue and capability claims. Error paths await transaction rollback.
Metadata reads and inserts are batched in groups of 128, avoiding one network
round trip per task and keeping SQLite parameter counts bounded. Encryption
continues sealing task bodies; journal metadata requires no payload decryption.

PostgreSQL migration 140 and SQLite schema 70 add immutable issued-claim rows
without hot-task or native-instance foreign keys. Before/after insertion guards
prevent reuse of an issued task UUID after collection, while preserving an
upsert of the existing hot row. The PostgreSQL after-insert check is necessary
when insertion waits for a concurrent deletion. Downgrade refuses retained
claim evidence. Missing historical principals are not invented by backfill.

The first extended run passes 283 cases: 22 issued-claim contracts, 153 admission,
49 failure-resolution, 44 PostgreSQL integration and 15 migration structure
cases. Matrix cases require plain/encrypted SQLite and PostgreSQL. Tests cover
all selector paths, original admission retention, input privacy, native/hot GC,
foreign scope and oversized generations, legacy classification, reclaim under a
new key, immutable records and downgrade. Injected journal failure restores the
lease generation and events. A 260-task test fails after the first 128 journal
inserts and proves every earlier batch, lease and event rolls back before a
successful retry. Strict all-target checks pass across eight affected packages;
broader engine/mobile/embedded/gRPC, source-cut and identity suites also pass
3,572 distinct cases (nested subprocess cases are not counted twice). Final
issued-claim suite passes all 32 cases, including issued-only concurrent
task-ID reuse, all-backend immutability, each principal class, and strict
unknown-field decoding. Empty struct principal variants preserve the valid JSON
shape while rejecting extra owner/scope fields that Serde unit variants ignored.
Versioned records fail validation and ownership checks for unsupported versions.
Trusted root lookup can recover a claim without hot native tenant history; scoped
callers must pass their verified tenant, and every read compares the record
tenant with the indexed SQL tenant. This lookup itself grants no authority.

## Required continuation

HTTP/gRPC poll adapters still call the legacy primitives at this checkpoint.
Authenticated transport principals must be wired to the authorized selector,
then checked before every mutation, reconnect adoption and receipt replay.
Legacy/missing ownership requires an explicit reconciliation policy rather than
silently assigning a new principal. Actual root credentials must be distinguished
from insecure mode, which existing HTTP AdminContext currently conflates.

Old coordinator writers can still omit this Rust-level journal. A rolling
protocol floor and database enforcement remain required before ownership can be
claimed across mixed versions. Native/backup/export compatibility, late factual
outcomes and reconciliation, acknowledged payload compaction, server delegation,
and SDK/platform integration remain governed by the full review ledger.

Worker payload retrieval also remains required. Generic artifact download has
no issued-claim/object grant. The eventual worker path must authorize its exact
admitted objects under the retained claimant; generic tenant-wide artifact read
is not a substitute. Review runtime queue permissions during adapter wiring:
normal poll currently has no queue predicate, while named queue poll checks an
explicit session allowlist. That suspected bypass needs a reproduced test and a
functional scoped fix. The separate pending-offer/paused-parent behavior also
needs its actual contract and database guards checked before changing semantics.

Strict all-target Clippy passes on the final principal format and lookup API
across storage, engine, CLI, API, gRPC, mobile, E2E and server packages.

## CI dependency repair

The published Check & Lint job failed on RUSTSEC-2026-0414 in the
transitive `pulldown-cmark` 0.13.4 build dependency. The lockfile now selects
0.13.5, the patched version identified by the advisory. No advisory exception
was added. `cargo deny --locked check`, the all-feature advisory graph, and
the standalone Node, Python and fuzz advisory graphs pass. Strict checks also
rebuild the gRPC generator using the patched parser.

Final related regression run passes 261 cases: 15 migration, 44 PostgreSQL
integration, 153 dispatch admission and 49 retained failure outcome tests.
Together with the final 32 issued-claim contracts, this pass covers 293 cases.
