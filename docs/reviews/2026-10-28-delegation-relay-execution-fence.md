# Delegation relay execution fence

A remote delegation's server-side proxy carries mailbox and result state. Its
sequence describes destination work; it grants no authority to execute that work
on the server. Pause/resume previously scheduled this proxy like a native
workflow, and native admission allowed it to consume a claim page and per-tenant
capacity. Eight PostgreSQL/SQLite regressions reproduced these faults.

## Execution and ownership boundaries

Resume returns a clean proxy to waiting without a native schedule. The signal
sweep consumes its controls without evaluating destination sequence hooks,
human-input handlers or cancellation compensations on the server. Ordinary
native cancellation hooks remain active. Destination result integration still
completes the relay after pause/resume.

Native admission and priority/fairness queries exclude proxies before limits.
Claimed native writes and run initialization reject them. Retry, fork and
resume-from-block cannot repurpose a proxy as a native workflow. Public HTTP and
gRPC instance creation reserve the legacy relay metadata field. Database triggers
protect existing relay identity and native runtime history.

PostgreSQL migration 143 and SQLite schema 73 derive identity from protected
legacy metadata, immutable publication ownership and retained quarantine facts.
Hosted publications remain native, including coincident instance/delegation
UUIDs. Ordinary metadata strings and arrays containing the reserved word remain
ordinary metadata. Partial indexes support point lookups without scanning retained
hosted publication history.

## Upgrade, cleanup and recovery

The upgrade clears claims and schedules on historical scheduled/running relays.
Native runtime, execution-tree or running-state evidence causes quarantine in
paused state. A relay without native history returns to waiting.

A regression reproduced loss of quarantine after hot-instance deletion and cold
restore. Compact immutable `delegation_proxy_quarantines` rows now retain physical
identity, tenant and first-observed time independently of hot instances and trees.
Backfill and insertion/deletion capture cover publication-backed and legacy relays.
Quarantine remains visible while the hot instance is absent and after restore
with no runtime history. Resume cannot release it, and new destination claims
exclude it. Already issued destination claims retain their existing completion
semantics; accepted facts remain durable for reconciliation.

PostgreSQL downgrade refuses while any relay identity remains, including compact
quarantine or publication receipts after native instance cleanup. Compatible
coordinators must be deployed before activating the schema. General mixed-version
writer enforcement remains an open review gate.

Four backend regressions also reproduced accepting a relay as a native local
migration snapshot. Shared capsule/local-migration readers now reject relay
identity before collecting native state. Logical instance backup refuses relay
records because its archive omits authority and recovery records needed to
restore them. Configuration-only backup remains usable. Full database backups
must retain quarantine records.

## Validation

- **1,471 distinct Rust tests:** storage 821, CLI backup 22, engine 57, HTTP API
  549, gRPC 22. The 103 delegation cases include actual scheduler hooks, hosted
  ID collisions, destination completion, indexed lookups, legacy identity,
  immutable quarantine, cleanup/restore, blocked resume and empty destination
  polls. SQLite file reopen and PostgreSQL upgrade/rollback run against real
  databases, including encrypted wrappers.
- **34 fresh native SDK tests:** Node 14 and Python 20. Both bindings were rebuilt
  from current source; actual SDK opens also upgraded schema-72 files to 73.
- **250 fresh-server end-to-end tests:** lifecycle/continuity 232 and portable
  continuity 18, with zero failures, cancellations or skips. Both runs used the
  same newly rebuilt server binary.
- Strict all-target lint passed across nine crates. Mobile and SQLite-only builds
  with default features disabled, formatting and whitespace checks passed.

The full Rust matrix passed before extracting snapshot worker-boundary checks
into a focused helper. Its 214 affected snapshot cases, 103 delegation cases and
22 CLI cases passed again afterward. The separate reconciliation reopen case
adds the 821st storage test. Evidence is recorded in
`/private/tmp/orch8-durable-proxy-validation.json`, with per-target logs and native
and server binary hashes.

Published-base CI job 123110464870 failed a stale assertion expecting SQLite
schema 71 after correctly opening schema 72. The test now compares against the
bundled schema version and separately verifies the version-71 reconciliation
migration exists. Exact decision retention, lossless values and immutable receipts
remain asserted; the repaired test passed locally on schema 73.

## Open gates

Historical provider side effects require explicit reconciliation and a new safe
physical identity; quarantine records have no automatic expiry or resume bypass.
The original review still has open work on frozen artifact delivery, retained
result readers, acknowledgement/archive compaction, portable-history backup and
full coordinator/older-writer enforcement. This patch is one reviewed part of the
active distributed/mobile/embedded reliability work.
