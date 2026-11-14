# Frozen delegated definitions — staged implementation

The published baseline retains a definition hash but delivers only a sequence
ID/name/version. Fetching that catalog entry later can execute different content.
A four-backend regression changes the catalog after publication and exercises a
new claim-scoped frozen reader. All four legs failed before implementation and
passed after the initial repair.

## Implemented server foundation (unpublished)

Migration 144 / SQLite schema 74 add immutable, content-addressed definition
snapshots keyed by tenant and semantic definition hash. Bodies are shared across
publications, capped at 4 MiB of serialized UTF-8, and contain no delegation request input
or grant token. The bound leaves room below the remote client's 8 MiB response
limit. Snapshot insertion and verification happen before final authority/deadline
checks and commit with the grant, mailbox, publication receipt and provenance.
A missing historical snapshot is never inferred from a mutable catalog entry.

`GET /workers/tasks/{id}/definition` requires the original issued reporter and
current generation, checks the hot task against immutable issuance, verifies the
native run and continuity owner, and rejects terminal/quarantined targets. Reads
verify the frozen body against the publication hash. Browser/device permission
changes allow this exact GET route only. Responses use `Cache-Control: no-store`.

A deterministic storage-pause test caught a revocation window in the initial
reader. The implementation now retains private authentication evidence and
rechecks stored-key activity/current grants directly in storage, plus runtime
session expiry, handler and queue scope. Ownership and authority are checked again
after response encoding. The snapshot getter checks delegation expiry with the
database clock before and after reading the body.

Current selected validation passes 162 delegation cases, 370 API unit cases,
eight definition units, two PostgreSQL session-lock units and 15 migration cases.
The full delegation matrix passed before test-helper extraction; its 23 affected
cases passed again, then the refined catalog-change case passed after its final
helper extraction. Strict all-target lint across nine crates, mobile and SQLite
builds without default features, formatting and whitespace checks pass. Evidence
and final source hashes are recorded in
`/private/tmp/orch8-definition-bundle-validation.json`. This foundation remains
uncommitted and unpublished. Actual SDK migration probes and new real-server
bundle execution have not been validated for schema 74.

## Transactional dependency bundles (unpublished)

`delegation_definition::graph` captures and verifies a typed, bounded closure.
It follows all DSL branches, lifecycle trees and saga compensations; handler
parameter JSON is not interpreted as workflow syntax. Selectors retain explicit
versions and distinguish latest non-deprecated selection. Iterative traversal
preserves cycles and deduplicates bodies. The separate manifest identity changes
when an unpinned child changes, even if the root hash does not.

The component limits definitions, selectors and final serialized bytes. It checks
all returned scope/version selections, requires a complete reachable closure,
and verifies bodies against a separately trusted manifest identity. A regression
showed that accepting two different bodies for one sequence ID or name/version
could certify an incoherent catalog cut; both forms now fail closed. All eight
definition unit tests pass, including the existing exact root byte-boundary test.
Strict storage lint (library and tests), SQLite-only compilation, and formatting
pass after the final change. Verification visits documents by their already
verified identity, so shared children and cycles do not repeatedly hash large
bodies. Evidence is recorded in `/private/tmp/orch8-definition-graph-validation.json`.

Publication now captures the closure inside its transaction. SQLite retains its
immediate write reservation. PostgreSQL uses repeatable read, selecting latest
children from the snapshot established after namespace locks are acquired, rather
than mixing revisions observed by successive queries. Bounded whole-transaction
retries preserve the immutable request and receipt identity on transient storage
errors. Existing authority, ownership and definition row locks remain, and fresh
database deadlines are checked after every possible capture/write wait.

A separate immutable `delegation_definition_manifests` row binds each tenant/task
to its selected closure. Shared bodies remain keyed by tenant and definition
hash; equal roots can have different manifests when their latest child changes.
Readers bound aggregate body bytes in SQL before transferring them to memory,
then verify all bodies, the manifest identity and publication/root scope. The HTTP
response now carries the exact retained graph bytes in a scoped, hash-pinned
base64 envelope; workflow numbers never need a language JSON round trip.
Missing manifests return conflict; missing bodies fail integrity checks. Neither
case is reconstructed from the live catalog. Downgrade refuses either retained
bodies or manifests. Both tables reject updates and deletion.

The first integrated delegation matrix passed 157 cases. It includes nested
children and a cycle after catalog mutation/deletion, distinct latest-child
selection for an identical root, foreign tenant/catalog rejection, concurrent
insert/deprecate/replace/delete during a real grant-row wait, suppressed manifest
writes and rollback after definition/manifest table waits cross the deadline.
Plain and encrypted PostgreSQL/SQLite variants run where applicable.

A further four-backend regression returned success when a child required a
handler absent from the destination. Publication now checks handler names across
the entire captured closure, including lifecycle/recovery hooks, and rolls back
without spending the grant. Final validation includes this regression plus actual
claim-scoped HTTP download and common-verifier round trips.

An additional regression caught a flaw in the initial PostgreSQL integration:
a snapshot established before an advisory namespace-lock wait could miss permanent
physical retirement committed while waiting. That unpublished implementation
returned success and recreated the retired physical ID. Publication now acquires
its sorted namespace mutexes before beginning the repeatable-read transaction.
Session locks bridge this boundary; normal completion explicitly releases them,
and an abandoned guard closes its connection instead of returning locked state
to the pool. Acquisition respects shorter configured lock timeouts and otherwise
caps each wait at one second; the existing setting is restored before BEGIN.
Transient publication retries are bounded to three attempts.

Two real-PostgreSQL guard units pass: normal release reuses an unlocked connection
and restores a shorter configured timeout; cancellation and lock timeout both
close the abandoned session and release an earlier mutex while a later one stays
blocked. The retirement race and complete delegation matrix pass. Further coordinator/older-writer and lifecycle fences remain original
review gates. Capability negotiation, reference delivery on claims, gRPC and SDK execution
consumers are not yet implemented. The exact-byte transport is implemented below.

## Required before SDK activation

1. **Pin the dependency graph — server component validated; SDK execution pending.** A `SubSequenceDef` resolves by tenant, namespace,
   name and optional version; an omitted version selects latest non-deprecated.
   Freezing the root alone leaves mutable children. Capture typed control-flow
   references throughout normal blocks, branches, failure/cancel handlers and saga
   compensations. Never discover dependencies by scanning arbitrary handler
   parameter JSON. Preserve cycles and explicit version choices under bounded
   graph/byte limits. Shared root snapshots need a separate per-publication
   dependency manifest so identical roots can resolve different child revisions.
2. **Make the catalog cut coherent — server capture validated; wider lifecycle audit remains open.** Resolve and validate dependency selections
   under publication's transaction/catalog fences. Define the selection point
   for unpinned versions and test concurrent version insertion, deprecation,
   replacement and deletion. Keep all waits before fresh authority/deadline
   checks. Historical missing manifests require explicit disposition.
3. **Negotiate execution support.** Only runtimes advertising a supported frozen
   definition/bundle contract and sufficient resource limits may receive new
   contract work. Older clients must not silently fetch live definitions. Preserve
   late result reporting for earlier issued generations independently of new
   execution admission.
   The shared byte/hash contract and pure verifier are now implemented and checked
   with Rust/Node/WebCrypto/Python vectors. Actual SDK consumers and negotiated
   claim-reference delivery remain required. Preserve the existing semantic
   receipt identity while passing verified bytes into private materialization.
4. **Materialize privately.** Verify body/manifest hashes and task scope before
   execution. Cache and install under explicit account/catalog/server scope and
   immutable content identity, rather than replacing a shared local sequence ID
   or namespace while another invocation is running. Rewrite nested references
   to their pinned private definitions. Preserve namespace semantics and cycles.
5. **Bind local forward execution to the remote claim.** Persist the mapping from
   remote task/generation to its local run. A restarted local scheduler must
   reconfirm authority before scheduling further delegated work; an orphaned run
   must not resume while the server retires or replaces its claim. Cover process
   death, offline periods, lease loss, pause, cancellation, delayed callbacks,
   duplicate observation and retained outcomes. Provider effects already in flight
   require their existing durable outcome/reconciliation policy.
6. **Complete transport and lifecycle support.** Share claim/read checks across
   HTTP and gRPC, integrate actual mobile/embedded/desktop consumers, and validate
   read authorization during waits, backpressure and credential changes. Keep
   generic binary-artifact access and acknowledgement-driven snapshot/payload
   compaction as explicit original gates; a root definition reader does not
   substitute for them.

The full distributed/mobile/embedded review remains active. The published relay
checkpoint is `6d47623a`; CI fixture repair `1f8fac73` is published. This foundation
is separate pending work.


## Exact-byte transport follow-up (2026-10-29, unpublished)

The pure document/manifest models, dependency verification and wire DTOs now live
in `orch8-types`, usable without SQL features. Storage keeps transactional catalog
capture and publication authority checks. The legacy root fingerprint is
unchanged, pinned by a compatibility vector captured before extraction.

The [versioned byte protocol](../definition-bundle-protocol.md) defines a SHA-256
reference over exact retained UTF-8 graph bytes, a bounded base64 envelope and
canonical decimal text for the full-width claim epoch. Migration 144 / SQLite 74
store wire version, byte hash, byte count and graph resource counts with each
immutable manifest. Readers reconstruct the pinned bytes directly from retained
row text. They do not serialize current structs to regenerate old payloads.
Compatible newly defaulted fields therefore do not change an older retained
semantic fingerprint. Diagnostic output omits encoded workflow bodies.

A captured vector proves that JavaScript parse/stringify alters the payload;
Python reserialization also changes its byte identity. Six Rust protocol units
cover legacy hashes, exact byte limits, full `u64` epochs, large integers, float
bits, Unicode/NUL, altered scope/reference/content and retained defaulted fields.
Node and browser WebCrypto plus Python each verify the same golden bytes (four
cross-language contract cases). The vector checks are added to the local CI
workflow change and remain unpublished with this foundation.

The 162-case delegation target passes with actual scoped HTTP envelope decode.
The eight capture/boundary units, 370 API units and 15 migration cases also pass.
Nine-crate strict lint, browser WebAssembly compilation, mobile/SQLite-only builds
and formatting pass. A later four-backend test refinement verifies that changing
catalog creation time does not change the wire reference for an already retained
shared body, and confirms payload bytes omit delegation input canaries. Evidence
is recorded in `/private/tmp/orch8-definition-wire-validation.json`.

This establishes a checked transport building block. It does not close capability
negotiation, actual claim-side reference delivery, SDK materialization or restart
execution authority. Actual SDK schema-74 reopen probes and bundle execution on a
fresh real server remain required, alongside every original open review gate.


## Bounded decoding and PR integration checkpoint (2026-10-29, unpublished)

PRs #365–#369 are merged into main at `c83199157ff3699588acf3db6fa2291a3a2c8c3d`.
Their current-head applicable checks passed before merge; their remote branches
and the two repair worktrees were removed. Earlier notes about draft PR #365 and
pending repair CI are superseded by this checkpoint. No release was published.
The definition foundation remains local on `review/definition-delivery-wip`.

The staged decoder now enforces collection limits before reading excess elements
and a separate shared 16 MiB logical expansion budget for definition JSON. Public
errors and model debug output redact private values. Oversized/invalid closure
capture returns a conflict, preserving transient backend errors. Four-backend
regressions prove atomic rollback and corrected retry using the same grant;
dynamic input does not consume the definition budget. This does not establish a
whole-task memory bound. A single adversarial unit-test process, including fixture
creation, measured peak RSS falling from 63,717,376 to 32,964,608 bytes. That
measurement is not a general engine or device memory guarantee.

Validation: 717 pure type units, 8 storage capture units, 174 delegation API cases,
370 API units and 15 migration cases pass. Nine-crate strict all-target lint,
WebAssembly, mobile and SQLite-only compilation pass. Node and Python exact-byte
contracts, formatting and documentation checks pass. PostgreSQL tests use a real
isolated PostgreSQL 17 instance with required-backend enforcement. Current
source hashes and log paths are recorded in
`/private/tmp/orch8-definition-bounded-validation.json`; this supersedes the older
wire-validation evidence path.

The next integration uses one shared Rust materializer with private local IDs,
atomic scoped mappings and durable claim-to-run bindings; see the
[protocol requirements](../definition-bundle-protocol.md#required-execution-integration).
Capability negotiation, actual claim reference delivery, SDK schema-74 reopen and
execution, restart forward-authority fencing, and all previously open review
gates remain incomplete. This checkpoint does not activate delegated execution
or complete the distributed/mobile/embedded review.


## Private graph preparation checkpoint (2026-10-29, unpublished)

The shared pure Rust verifier now provides `prepare_local`: a consumed verified
graph becomes an ephemeral plan with fresh local sequence IDs and a fresh private
namespace under the caller's trusted local tenant. Remote embedding ownership is
cleared. Every typed dependency, including branches, lifecycle cleanup and saga
compensation, is pinned to its manifest-selected version. Opaque handler JSON is
unchanged; original semantic hashes remain separate from relocated definitions.

Four new regressions cover complete typed traversal, account/namespace isolation,
explicit older child versions alongside an unversioned selection, and rejection
of an unchecked empty tenant. The initial unimplemented preparation failed both
starting regressions. Final pure type tests pass all 721 cases; strict all-target
no-default-feature type lint and actual WebAssembly compilation pass. Evidence:
`/private/tmp/orch8-definition-materialize-validation.json`.

This is preparation, not persistent installation or execution authority. Each
new preparation intentionally creates new IDs. Durable account/catalog/server
receipts must atomically retain mappings and reuse them on retry. Claim binding,
capability negotiation, current authority/restart checks, execution policy and
credentials/placement admission remain required before use. No public SDK or
remote claim path invokes preparation yet; all previously open gates remain open.


## Durable local definition installation (2026-10-29, unpublished)

SQLite schema 74 now stages claim-definition receipts and source/local mappings.
The internal mobile installer verifies independently supplied scope/reference,
canonical HTTPS server identity, and the active local account/catalog epoch.
Definitions, native fingerprints and the receipt commit atomically. Retrying the
same claim verifies the retained graph and returns its existing local IDs;
changed references or corrupt/missing mappings fail without replacement. Foreign
keys pin the graph. Separate accounts, catalogs, servers, workers, tasks and claim
epochs cannot share physical rows. Full-width remote epochs are stored as text.

An editor-metadata regression exposed a native representation mismatch: SQLite
omits `$schema`, so pre-insert fingerprints rejected unchanged cached rows.
Fingerprints now describe rows read back inside the transaction; original source
and exact-wire identities remain separate. Another reproduced gap allowed large
remote identity metadata outside definition-byte accounting. The installer now
bounds identity descriptors and rejects control characters before installation.

Twelve installation cases pass, covering rollback/retry, two independent database
connections, count/byte quotas, existing retry at capacity, account retirement,
linked pinned dependencies, protected rows, source-mapping corruption, endpoint
and identity rejection, full-width epochs, database reopen and a synthesized
schema-73 upgrade preserving account identity. The full network-enabled mobile
suite passed 712 cases before the final two identity-validation calls; all 12
affected installation cases were rerun after that change. The 701 other mobile
cases do not call this staged installer. Strict no-default-feature all-target
mobile lint also passes. The initial sandboxed full run could not bind local TCP
fixtures; rerunning with network permission passed. Source-bound evidence:
`/private/tmp/orch8-claim-definition-validation.json`.

Quota accounting covers serialized native definitions, not an exact disk/heap
bound: global 128 receipts/64 MiB, account 64/32 MiB, catalog 32/16 MiB, individual
graph 8 MiB. Older epochs remain charged. Explicit acknowledged cache compaction
is still required for long-running operation. Reopen tests supply the verified
envelope again; they do not establish offline receipt-only loading or actual
Swift/Android SDK execution.

No run or provider work is created by installation. The entry remains internal
and unused by the production claim path. Capability/resource negotiation,
claim-side reference delivery, durable remote-task/local-run binding across claim
changes, offline/restart authority fencing, native platform probes, acknowledged
compaction and all original open review gates remain incomplete. The next step
must bind an existing logical run across claim generations without duplicating
provider effects, and gate every forward attempt on current authority.


## Durable claim binding

The staged mobile cache now retains the authenticated manifest and validates
cached definitions without another envelope download. It checks the trusted
manifest/reference, source set, native fingerprints, ownership, root mapping and
byte totals. Absent staged manifests require a verified-wire repair, committed
only if graph validation succeeds; existing corruption remains unavailable.

A separate logical binding keeps one local run ID and its original physical
graph per account/catalog/server/remote tenant/task. Current local and remote
epochs are mutable observations, not part of that stable key. Reclaiming work or
signing back into the same account cannot mint a second logical ID. Worker changes
at the same remote epoch and lower generations fail. Handoff validates both
current and initial receipts and retains full-width epochs as canonical text.
Publication-request identity is pinned separately from definitions: an unchanged
root/graph does not permit a changed trusted request fingerprint. Actual request
hash delivery/derivation and raw input validation are still integration work.

Thirteen new binding/reopen cases plus the twelve installation cases pass in the
full **726-case** mobile suite. Coverage includes 32 account/catalog/server/tenant/
task combinations, independent-pool concurrent binding, original-graph reuse,
reopen without an envelope, retirement and sign-in, stale/conflicting claims,
`u64::MAX` and corrupt overflow, changed definition/request identities, foreign
receipt pointers, handoff rollback, and manifest repair rollback. Strict mobile
all-target lint passes. Evidence:
`/private/tmp/orch8-claim-binding-validation.json`.

This stores identity only: no native instance or provider work is created.
Negotiation and trusted claim/provenance delivery, forward authority checks,
physical callback admission, cancellation/revocation, raw input budgets,
late-fact reconciliation and acknowledged compaction remain open. The new DME200
upgrade compatibility gate also remains open; this checkpoint does not establish
actual Swift/Android SDK execution or complete the original review.


## Native cache codec checkpoint

The DME200 reproduction uses an independently Python-encoded synthetic legacy
row with omitted defaulted step fields, Unicode/NUL data and fixed native digest
and byte count. The typed reserialization loader failed it with `installed
definition changed`. Native cache codec 1 now hashes the exact fixed SQL fields
and verifies that digest before typed JSON parsing. Today's supported defaults
are applied without rewriting stored rows or fingerprints. Source semantic and
wire-byte identities remain separate and unchanged.

An explicit receipt codec selects this algorithm. Missing or unsupported native
codecs remain unavailable even when another envelope is supplied. Pre-codec
unpublished caches require an explicit supported migration/import policy; readers
do not refresh hashes from mutable rows or replace a logical run. Raw field byte
lengths are bounded before fetching JSON text. Cache quota bytes now count the
versioned framed projection. New execution columns or incompatible encoding/DSL
interpretations require a codec/migration strategy.

All **731 mobile cases** and strict mobile all-target lint pass. The 30 focused
cases comprise 12 installation, 13 binding/reopen and five native-codec cases.
Native coverage includes the independently encoded legacy-shaped file reopen,
mutations of every mutable persisted execution field (including malformed private
JSON), absent/future codecs without restamping, oversized stored JSON rejected
before decoding, and unrelated editor columns. The legacy shape keeps its original
raw bytes while the current decoder applies `cancellable=true`. Evidence:
`/private/tmp/orch8-native-projection-validation.json`.

The shared cache root cause is addressed. This synthetic fixture is not an export
from a released SDK. Actual Swift/Android SDK upgrade/reopen and execution probes
remain required, so the full DME200/platform gate is not closed. No delegated
provider execution is activated. Negotiation, claim-side trusted reference and
request identity delivery, native materialization and forward authority/physical
admission, cancellation/late facts, acknowledged compaction and all other original
open gates remain incomplete.


## Decoder allocation follow-up

Retained definition decoding now temporarily removes and restores `created_at`
while computing the legacy semantic hash, rather than cloning the whole decoded
JSON tree. The original field is restored before typed decoding; retained bytes,
wire identity and compatible defaults are unchanged. All 726 type library tests
and strict all-target type lint pass, including the independent exact-wire vector,
legacy fingerprint and shared expansion-budget cases. This removes one full-tree
copy; it is not a measured process-heap bound. Logs:
`/private/tmp/orch8-definition-decode-no-clone.log` and
`/private/tmp/orch8-definition-decode-no-clone-strict.log`. All execution integration
and actual-platform gates above remain open.


## Bounded cached-run rebinding checkpoint

Repeated claim generations and local sign-ins no longer require a duplicate
physical graph installation before binding an existing logical run. A separate
codec-1 latest-observation row commits with the binding and remains bounded to
16 KiB; the immutable graph and original logical run ID stay pinned. The internal
try-bind API distinguishes a missing new-run cache from retained corruption.
Neither a fresh envelope nor a higher claim silently repairs missing advanced
observations or unsupported metadata. Existing staged bindings can initialize an
observation only while their immutable receipt proves the entire old scope.

All 747 mobile cases pass; final affected cache cases number 41, including full
capacity across 64 reclaim changes, sign-in, independent-connection concurrency,
full-width epochs, corruption/size rejection, rollback and reopen without another
envelope. Strict storage/mobile lint, formatting and docs pass. Evidence:
`/private/tmp/orch8-cached-run-rebind-validation.json`. This removes reconnect-driven
graph growth for an existing run; new-run history capacity still requires explicit
acknowledged compaction. Claim negotiation, input provenance, native execution
authority, cancellation/late facts and actual-platform probes remain open.
