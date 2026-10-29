# Exact context admission for worker completion

HTTP and gRPC measured worker output as ordinary top-level context fields, while
published mailbox application stores a wrapped result under
`context.data.delegations.<id>`. Arrays, strings and null bypassed the old first
check entirely. Both transports accepted a result one byte over the actual
context ceiling on PostgreSQL, SQLite and both encrypted wrappers. Acceptance
consumed the claim and committed the effect; recovery then deferred parent
application. Retrying a smaller body acknowledged the original accepted body,
so it could not repair this first-report validation error.

The old projection also retained an existing delegation entry while measuring
replacement output as unrelated top-level fields. It could reject a replacement
that actually fits after the existing entry is replaced.

Both transports now call the storage projection used by durable application.
The projection reads the immutable invocation and publication for a published
mailbox and verifies the task, tenant, instance, destination and original effect
scope. It does not create an accepted-outcome object, write a receipt, settle an
effect or mutate native history. Ordinary worker output uses its original
object-field merge. Historical mailbox reports retain their wrapped projection.
Published result shape and delegation-field replacement share the same pure
functions with durable recovery.

Admission checks the returned context before accepting the first report. A
rejected report leaves the held claim and dispatched effect available for a
corrected result. Application still reloads and checks context under its native
transaction: a concurrent change can defer an already accepted physical result,
which remains retained for recovery. Existing accepted acknowledgements continue
to use their original body.

The boundary matrix covers both transports, hosted parents and proxies, object,
array, Unicode/NUL string and null output, subnormal/precise numeric fields,
exact-ceiling acceptance, one-byte-over rejection, correction with the same
claim, and replacement of an existing delegation entry while preserving a
sibling. Eight backend test legs cover 144 completion scenarios; scenario and
retry counts are not additional Rust test counts.

Proxy native scheduling and pause/resume semantics, historical/failure result
reconciliation, frozen remote definition delivery and the other original review
gates remain open. This change makes the first completion-size decision agree
with durable application; it does not establish completion of the full review.

Validation passes 1,135 distinct selected Rust cases: storage (557), API (512,
including 66 delegation cases), gRPC E2E (22), distributed engine (40) and proxy
identity (4). Strict all-target Clippy across eight consumers, standalone mobile
compilation without default features, formatting and diff checks pass. A fresh
server passes all 232 lifecycle/continuity integration cases with no failures,
cancellations or skips. A PostgreSQL schema-creation run exhausted local disk;
after removing 6.8 GB of superseded, unopened build artifacts, its affected
49-case suite passed. Successful cases were not counted again.

The published checkpoint's CI also found an outdated portable-continuity test
that expected identical delegation publication replay to fail. That test now
requires the exact original acknowledgement, while changed input and reuse of
the consumed grant for a different delegation still require HTTP 409. No
production trust check was relaxed. All 18 portable-continuity tests, including
configured-peer federation and elevated-runtime-trust rejection, pass against
the same fresh server binary; TypeScript checking passes. Combined fresh-server
coverage is 250 cases, for 1,385 selected Rust and integration cases in total.
