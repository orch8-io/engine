# Atomic delegation publication

The claim endpoint consumed its one-time grant before creating the proxy,
mailbox effect/task and parent provenance. A later write failure spent the
only grant and left partial work. Four-backend fault injection reproduced this.
An independent test also showed that changing hot mailbox params after
publication delivered the changed input, including through encrypted storage.

The endpoint now builds a private typed publication intent without durable
preflight writes. PostgreSQL and SQLite transactions commit the following as
one unit:

- The destination-bound grant transition, checked against its signed snapshot,
  token verifier and indexed scope/expiry.
- A new inline sequence when explicitly requested, with deterministic identity,
  NUL-free bounded names and matching existing definition semantics.
- A remote parent's waiting proxy and canonical initial continuity scope, when
  needed; existing hosted parents must remain in an unclaimed waiting/scheduled
  state. Retired or captured physical sources are refused.
- Planned/prepared/dispatched effect admission, including current effect guards,
  the mailbox task and its permanent invocation identity.
- Signed parent-chain provenance and an immutable publication receipt.

Namespace locks precede ownership/native locks. Ownership, grant, stored Operator
key, runtime registry and definition are checked under transaction locks. Runtime
session authority and stored-key deadlines are checked again after possible lock
waits, using fresh database time. PostgreSQL tests hold an actual grant-row lock
until either the delegation or Operator key expires; both requests roll back.
SQLite acquires its write reservation before reading the snapshots.

A provenance-head conflict rolls back all publication work. Only the signed entry
is rebuilt in a bounded retry; the business intent and candidate task ID remain
unchanged. Suppressed grant updates and provenance inserts cannot return success.
No grant-reset compensation or early mailbox exposure is used.

The retained receipt contains metadata and hashes, not raw input, context or the
grant token. It binds the complete request and stable verified caller to the
original task, instance, effect, task owner and sequence fingerprint. Logical and
stored task-spec hashes remain distinct through encryption. The shared batched
claim journal checks the stored spec before a lease commits. A new publication's
invocation marker requires its receipt; missing evidence cannot silently become
legacy dispatch. The tampering regression failed on all four backends before
this guard and passes afterward.

An identical caller/request retry returns the original mailbox task ID before
checking present-day parent ownership or hot/native rows. This acknowledges an
already committed publication; it grants no new lease, executes no new work and
cannot change input. Current HTTP authentication and runtime binding still apply.
A different caller or request conflicts. Concurrent identical requests retain one
task/effect/provenance boundary. Retry survives native/hot cleanup and ownership
movement. A consumed grant without a retained receipt remains unavailable; no
historical publication or issuer is invented.

PostgreSQL migration 142 and SQLite schema 72 retain receipts independently of
hot/native history. Their indexes are permanent and receipts reject update or
delete. Database checks require the current receipt version and consistent
indexed identities; readers also validate metadata. PostgreSQL downgrade refuses
retained receipts. A file-backed SQLite 71 upgrade and reopen test proves that
receipt acknowledgement survives cleanup without recreating the proxy.

The publication suite passes 34 tests across plain/encrypted PostgreSQL
and SQLite, including actual destination polling/completion and lossless NUL and
floating-point input and the established cross-tenant 404 boundary. E2E caught
an initial 409 response for that boundary; the HTTP scope check was repaired,
and the original assertion was retained. The failure matrix exercises
proxy/inline/hosted write points as applicable, including suppressed writes.

Final verification passes 993 distinct Rust cases: 261 complete storage units,
153 admission cases, 56 claim-identity cases, 15 migration checks, 17 continuity
cases, 368 API units, 34 publication cases, 45 reconciliation HTTP cases and 44
distributed/proxy engine cases. All 232 lifecycle/continuity integration E2E
tests pass against a freshly built real server and disposable PostgreSQL.
Strict all-target checks across eight consumers, standalone mobile library
compilation, formatting and diff checks pass. Temporary local E2E port selection
was corrected to keep its derived gRPC address valid; product preflight was not
weakened.

This closes publication atomicity, not the entire server-delegation gate. Remote
sub-sequence retrieval still needs a frozen, verified SDK artifact contract;
the fingerprint captured here only proves the definition at publication. Server
outcome integration still uses separate native output/context/wake writes and
needs its own durable transactional application path. Scoped catalog dispatch
retains its existing separate provenance requirement. Continuing handoff and
revocation, complete coordinator/database enforcement, legacy attribution,
acknowledged compaction, transported history/key/backup compatibility, poisoned
recovery handling and all remaining original review gates remain open. Proxy
and inline admission also need integration with tenant/scoped quota policy.

The portable-continuity CI fixture now checks this durable replay contract:
identical intent returns the exact original acknowledgement; changed input or
a new delegation ID using the consumed grant still conflicts. The former
blanket duplicate-rejection assertion was stale. All 18 portable-continuity
cases pass on the fresh server, including the remaining federation and runtime
trust assertions, with TypeScript checking passing.
