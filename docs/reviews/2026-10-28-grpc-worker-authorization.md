# gRPC worker authorization and stream lifetime

Tracing the issued-claim ownership gate found a prerequisite authorization gap:
gRPC authenticated a stored API key and retained its tenant but discarded its
capability grant. A middleware regression proved an Auditor key reached
`PollTasks`. Restricted keys could also reach native mutations, definition
writes, worker streams and tenant artifact reads outside their HTTP grant.

The gRPC auth layer now retains the verified key record in an opaque request
principal and checks an explicit method policy before calling the service. Its
Debug output excludes credential hashes. The policy uses complete RPC names,
not read/write name prefixes, and the test matrix is checked against all 37
methods declared in the actual protobuf service. New or unknown methods remain
denied to restricted keys until their policy is reviewed. Operator retains its
full service grant. Existing legacy keys carrying all capabilities keep that
explicit compatibility grant. Root/insecure and mapped workload authentication
retain their existing authority; this change does not claim to introduce scoped
runtime-session authentication to gRPC.

Worker grants cover the lease and worker-stream protocol. Publisher covers
sequence definitions, Approver covers signals, and Auditor covers enumerated
reads, including artifact downloads. There are no mobile-control RPCs for the
Device grant. Session-only BrowserWorker/DeviceNode capabilities are never
interpreted as stored-key grants. HTTP remains the verified runtime-session
surface. Node-role RPC allowlists remain an independent restriction.

A second pair of real-transport regressions proved that an idle worker stream
stayed open after key expiry or revocation. Stored-key worker and artifact
streams now reauthorize while waiting for the first frame, while idle, and while
processing or blocked on response backpressure. Worker frames also recheck
before mutation. The guard owns the operation future; it leaves no detached
monitor. Consumer disconnect and shutdown drop blocked stream work. Credential
lookup times out after five seconds and refuses entry into the operation.

Rechecks occur every second and use the existing storage-scoped authentication
cache. Local revoke invalidates that cache; remote nodes retain its documented
30-second staleness bound, plus scheduling/lookup latency before reporting the
terminal status. Already admitted or delivered data cannot be recalled. This
is bounded credential enforcement, not instantaneous global revocation.

Authentication status travels on a separate terminal channel, bypassing a full
data queue. A deterministic regression found that data-channel EOF could race
and hide that status; the response now waits for the terminal decision. Normal
completion still drains queued data. Tests also cover operation destruction,
credential-storage unavailability, idle worker revocation/expiry, expiry before
the initial worker frame, and artifact transfers waiting for acknowledgement.

Validation passes 159 gRPC unit cases and 19 real-transport E2E cases. The three
reproduced gaps (capability bypass, live-stream credential lifetime and terminal
status ordering) have red/green evidence. Strict all-target checks pass for gRPC and server integration. Large guarded
worker operations are boxed once at stream creation rather than inflating the
coordinator task future. No database migration or worker
wire-shape change is introduced.

## Remaining implementation requirements

The original durable issued-claim gate remains open. Dispatch identity and
winning-result receipts retain worker ID/epoch, but they do not yet retain the
original authenticated principal for every issued claim. This request principal
must not be represented as that durable journal or used to authorize new cold
factual reports after hot cleanup. Credential refresh needs a stable, explicit
principal mapping rather than allowing a new device/account/key to inherit an
old claimant's identity.

Workers also need claim-scoped artifact access. A generic ArtifactTransfer open
names only an object key, not an issued claim or object grant. Granting Worker
unrestricted tenant artifact reads would preserve the authorization bypass.
Current generic downloads therefore require an Auditor/Operator grant. A secure
worker payload flow bound to its retained issued claim and exact permitted
objects is still required before the full review can close; removing worker
artifact functionality is not the final target.

The broader ledger still requires late factual outcomes and reconciliation,
complete rolling coordinator protocol enforcement, recovery disposition,
acknowledged native payload compaction, transported history/key/backup
compatibility, server delegation and public SDK/platform integration. PR365 stays
draft; this checkpoint does not establish readiness to merge or publish a release.
