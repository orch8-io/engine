# Complete compensation history

Compensation planning used the legacy 10,000-row receipt list, omitting later
committed effects and unknown-outcome hazards. A real HTTP regression reproduced
a missing 10,001st compensation step.

SQLite and PostgreSQL now stream one complete SELECT snapshot, validating indexed
receipt identity and tenant/continuity scope. Only compact committed/verified/
unknown planning fields are retained; unrelated raw receipts are not accumulated.
Plan and response memory still scale with actionable steps and hazards.

The planner groups receipts by block once, sorts each group newest-first with UUID
tie-breaking, and preserves reverse dependency order. Explicit DFS frames avoid
recursive stack growth for long dependency chains. The public raw-receipt planner
keeps its existing signature and delegates to the compact planner.

All 49 targeted tests passed on the isolated PR359-based checkout: 33 storage
cases, 8 planner cases, and 8 HTTP cases. Strict all-target Clippy for storage,
engine and API, formatting, documentation and whitespace checks passed. Cases include all
four storage configurations, receipt histories beyond 10,000 rows, malformed
stored states, a PostgreSQL barrier-controlled concurrent settlement, reverse
ordering/dependency cycles, a 5,000-block chain on a 256 KiB thread stack, and real
HTTP creation/reload of a durable 10,001-step plan with 21 uncertainty hazards.

Broader lifecycle review remains open: creation does not atomically fence forward
execution, final completion does not reconcile the whole effect ledger, dispatched
receipts are not represented as uncertainty hazards, and templates use current
instance context/outputs rather than frozen per-invocation inputs. This patch
repairs complete-history collection and planning; those existing lifecycle
contracts require separate changes before full compensation safety is established.
