# Retained worker failure application after pause

The scheduler previously left a paused run stranded after the worker's failure
had been acknowledged. Explicit resume did not apply either the retry policy or
the terminal failure. Two regressions reproduced this on SQLite before the fix.

The scheduler now drains a bounded, rotating failure-recovery queue before native
claims. It reads the original immutable failure report, plans the failure policy,
and asks storage to apply the native action. This also runs through explicit
single ticks used by embedded and mobile hosts. Delegation reports retain their
separate mailbox policy and are excluded from this queue.

Application checks tenant, physical instance, sequence, original run and mode,
continuity owner/epoch/runtime, exact flat-step or tree activation, original task
generation and original effect proof. A changed binding, settled effect or
conflicting accepted success requires reconciliation. Busy PostgreSQL authority, task, effect-receipt
and recovery-clock rows are skipped. Two held-row regressions reproduced a
scheduler stall before the task/effect checks used nonblocking locks. Recovery does not invent an original input or effect.

The native action, immutable application receipt and removal from recovery commit
in one transaction. An injected receipt insertion failure restores the original
failed task, waiting node, paused parent and queue. Repeating an applied report is
idempotent, including after hot-task deletion. Application receipts cannot be
updated or deleted.

A retry reservation is unclaimable until normal dispatch renders and admits the
next attempt. Its context contains only the original run identity and dry-run
flag, without the parent's application data, configuration or audit records.
Applying a tree failure or reserving a retry preserves pause. A flat terminal
failure defers until explicit resume so it cannot terminate a paused parent.
Neither path relabels the original uncertain provider effect as abandoned.

PostgreSQL migration 139 and SQLite schema 69 add the application receipts,
recovery queue and pending-failure capsule guards. Backfill considers failures
of live physical instances, using the physical report index rather than scanning
all retained history on every open. Capsule capture refuses pending failures even
after task cleanup. Downgrade refuses pending or applied evidence; historical
migration tests remove and restore this layer in dependency order.

Validation passes 3,591 distinct selected cases: 2,145 engine units, 701 mobile
units, 39 embedded units, 260 storage units, 40 distributed-engine tests, 49
failure-resolution contracts, 189 capsule-source-cut contracts, 153 admission
contracts and 15 migration structural tests. Nested subprocess unit cases are
not counted twice. The six pause scenarios cover flat/tree execution,
retryable/permanent failures and deleted task rows, on plain/encrypted SQLite
and PostgreSQL. The final distributed/resolution run also verifies that busy
PostgreSQL task/effect rows defer immediately and recover after lock release.
Strict all-target checks pass for storage, engine, mobile, embedded, HTTP, gRPC
and E2E packages. The SQLite-only mobile build also passes. A final four-backend
legacy regression verifies pause/resume after task cleanup without an effect
receipt, without upgrading the report to strong admission.

This closes the reproduced pause/resume gap once final validation passes; it
does not complete the full distributed/mobile/embedded review. Late factual
outcomes and reconciliation, issued-claim principal retention, complete rolling
protocol enforcement, recovery disposition/backoff, native payload compaction,
transported history and key/backup compatibility, server delegation and public
SDK/platform integration remain open. Pending failures for cancelled/deleted
native executions remain retained evidence; their explicit disposition belongs
to that reconciliation/archive work.
