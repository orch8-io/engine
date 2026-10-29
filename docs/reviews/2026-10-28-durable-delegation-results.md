# Durable delegation result application

A completed mailbox task retained its accepted provider result, but HTTP and gRPC
applied that result through separate block-output, context and state writes.
HTTP treated the first block output as proof of complete integration. gRPC
acknowledged a retained result without retrying mailbox application. A failure
between writes could strand a parent permanently; deleting the hot task removed
the information those paths needed to retry.

Fault injection reproduced partial output on PostgreSQL, SQLite and both encrypted
wrappers. A second test reproduced false success when an insert trigger silently
suppressed the block output. Both failures are in the durable application boundary,
not result acceptance: the original provider output was already retained.

Published mailbox results now use the existing accepted-outcome recovery queue.
Their immutable publication receipt supplies a distinct mailbox application target;
the synthetic delegation block is never represented as a native tree or flat step.
Recovery verifies tenant, task, instance, effect, destination runtime, invocation,
continuity owner/epoch, exact run snapshot and current physical authority. It reads
and validates the publication again inside the native transaction. Missing or
changed publication evidence cannot fall back to historical mailbox handling.

The transaction writes the wrapped result block, merges only
`context.data.delegations.<id>`, transitions the parent and records application.
Output, native update and application-receipt writes must each affect one row.
A later failure rolls all native writes back while retaining the accepted result
and recovery candidate. Scheduler recovery can retry without a worker or hot task.
Concurrent recovery appends one result. A worker retry acknowledges the retained
first output, including across HTTP and gRPC, rather than replacing it.

Hosted parents remain paused when paused. A paused proxy defers application until
it can become terminal without scheduling its delegated sequence. A changed owner
requires reconciliation. Proxy classification uses the immutable synthetic owner
identity and verifies the native proxy metadata; removing an editable metadata key
cannot turn it into ordinary schedulable work. Context limits defer application
without losing the completed result. Context and output sealing remain delegated
to the encryption wrapper; NUL and floating-point canaries exercise both wrappers.

Historical unbound mailbox tasks still use an explicit compatibility path while
hot history exists. Completed rows that predate the accepted-outcome journal keep
their existing HTTP same-claim hot-output retry; it does not fabricate an acceptance receipt or
use a changed request body. Published invocation markers cannot enter this fallback.
That path now retries the missing context/wake instead of
trusting a standalone output as completion, but it does not acquire publication
proof retroactively. Atomic historical reconciliation, failure/late-fact handling,
proxy control/scheduler exclusion and remote-definition artifact verification
remain open review gates. This checkpoint does not establish completion of the
full distributed/mobile/embedded review.

Validation of the final implementation:

- 557 selected storage cases pass: all 261 unit cases, current outputs (6), SLA
  output atomicity (12), worker admission (153), issued claims (56), resolution
  identity (20) and resolution outcomes (49).
- 504 API cases pass: all 368 units, delegation publication/results (58),
  distributed workers (14), worker commands (2), reconciliation (45) and workers
  (17). The outcome module includes failed/suppressed output, parent and receipt
  writes for hosted/proxy parents; cold retries; pause/owner change; concurrent
  recovery; metadata reclassification; authenticated cross-transport recovery;
  and pre-journal HTTP completion compatibility. Backend matrices include required
  PostgreSQL, SQLite and both encrypted variants.
- gRPC E2E (22), distributed engine (40) and proxy identity (4) cases pass.
  These total 1,127 distinct Rust cases; subprocess repetitions are not counted
  again. Strict all-target Clippy across eight consumers, standalone mobile
  compilation without default features, formatting and diff checks pass.
- A fresh final server build passes all 232 lifecycle/continuity integration
  cases with no failures, cancellations or skips against disposable PostgreSQL.
