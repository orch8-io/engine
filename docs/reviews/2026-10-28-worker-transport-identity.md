# Verified worker issuance and runtime queue scope

A real HTTP regression reproduced a browser session without any named-queue
grant receiving both a default-queue offer and a private named-queue offer
through `/workers/tasks/poll`. The named selector checked its allowlist, while
the unqualified selector supplied no queue predicate. Filtering a response after
claim would still consume the unauthorized lease.

HTTP poll handlers now derive their stable principal from verified extensions
and call the transactional issued-claim selector. A dedicated private root
marker distinguishes verified root authentication from insecure AdminContext.
Stored key identity includes its authoritative tenant and key ID; runtime
identity includes verified kind, runtime and device, independently of token JTI.
Request JSON and worker labels do not establish a principal.

Unqualified runtime-principal selectors constrain SQL to `queue_name IS NULL`.
Named polling retains the existing transport allowlist and remains functional.
PostgreSQL rechecks queue/default-queue and tenant predicates when locking the
previously scanned candidates; SQLite scans and updates in its write transaction.
Unqualified operator/server selectors preserve their existing broad queue scope.

gRPC polling and streaming demand similarly carry the actual stored-key, mapped
workload, verified-root or unsecured principal into the transaction. Advertising
a runtime capability does not invent an authenticated runtime/device principal.
Stored-key stream revocation guards remain in place.

The HTTP browser/device, worker, distributed-worker and command suites pass 45 tests,
with an additional 367 API unit cases passing,
including SQLite and required PostgreSQL legs where the suites provide them.
The new queue test checks that a private offer remains Pending after an
unqualified poll, that an ungranted explicit poll fails, and that an explicitly
granted named poll can still lease it. Additional tests inspect persisted root,
stored-key, browser and unsecured identity and refuse worker-supplied authority.

The gRPC suite passes 159 unit and 20 real-transport tests. A new test
verifies both poll and streaming delivery under stored-key and root credentials
by inspecting the persisted issued principal, rather than response status alone.
Existing authentication tests also distinguish actual root from unsecured mode.

The storage matrix passes 36 issued-claim cases, including browser and mobile
default/named selection on plain/encrypted SQLite and PostgreSQL. Device-session
minting now accepts the same optional bounded queue allowlist as browser
sessions (16 nonblank names, at most 256 bytes each), signs it and returns its
deduplicated form. The device route gate permits explicit named polling; the
handler enforces the exact signed grant. Existing no-queue tokens retain default
queue access. A real HTTP test exercises distinct devices with the same runtime
UUID and renewed tokens for the original device, checking original journal
identity and rejection of ungranted named polling.

The six distributed API scenarios also pass with current HTTP/gRPC services
and scheduler in-process on SQLite and PostgreSQL. These do not launch the
server executable. Separate hybrid executor and header-routed-front suites pass
with real rebuilt `orch8-server` executor processes, exercising HTTP/gRPC pickup,
drain, process loss/recovery and routed metadata on both databases. The generated
Rust/JavaScript client-contract gate and strict all-target checks pass. The raw
OpenAPI snapshot is regenerated from current server definitions, including prior
server changes that were absent from its checked-in copy.

A second regression found minting accepted NUL-containing handler/queue grants.
The mint endpoints now reject these names and NUL-containing device IDs before issuing
a token or accessing device storage, matching the issued-claim contract. The
final API run passes 412 cases (367 unit plus 45 HTTP cases). The regression
checks browser/device identity and grants on both databases.

## Remaining ownership work

Issuing a lease with original identity is not sufficient authorization. Mutation,
accepted/resolution receipt replay, reconnect adoption and exact artifact reads
still require the retained original principal, before any side effect or replay.
Legacy/missing principal history needs an explicit operator/backend reconciliation
protocol; silently inventing an original owner or leaving offline legacy clients
permanently unable to reconcile does not satisfy the design. Complete mixed
coordinator enforcement, late facts, backup/archive compatibility and public SDK
integration remain open under the full distributed/mobile/embedded ledger.

Final checks pass after the NUL fix: strict all-target Clippy across eight
packages, the authenticated poll/stream issuer scenario after fixture cleanup, a
fresh server build, generated Rust/JavaScript contracts plus the complete API
fingerprint, formatting and whitespace checks. Across the selected suites there
are 635 distinct test cases (repeated target runs are not counted again).

Schema 140 still permits older coordinators to omit the journal and use their
older unqualified polling rules. A mixed-version fleet therefore still needs
the required database/coordinator protocol floor; the new transport wiring must
not be presented as complete mixed-version authorization.
