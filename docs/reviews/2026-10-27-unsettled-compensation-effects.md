# Preserve unsettled work in compensation plans

Compensation planning retained Unknown receipts but dropped Planned, Prepared and
Dispatched receipts. Finishing known rollback steps could therefore record a clean
completion without reporting work that was still pending or in flight.

Complete collection now retains all unsettled states alongside committed/verified
outcomes. Planned/prepared receipts produce `PENDING_EFFECT_MUST_BE_SETTLED`
hazards; dispatched receipts produce `IN_FLIGHT_EFFECT_MUST_BE_SETTLED` hazards.
Existing unknown-outcome hazard strings are unchanged. Compensated and abandoned
history stays excluded. Public plan/run shapes and execution ordering are unchanged.

The HTTP regression first rolls back one known committed effect and verifies
CompletedWithResiduals while three unsettled effects remain. After the backend
confirms a late commit and abandons never-dispatched work through normal resolution
endpoints, the next plan contains only the late committed receipt. Finishing it
produces a clean completion without compensating the prior receipt twice.

The isolated patch passed 55 Rust tests (37 storage, 9 planner, 9 HTTP),
124 real-worker TypeScript tests, strict all-target Clippy, TypeScript typechecking,
formatting, documentation and whitespace checks. Coverage includes all receipt states across
SQLite/encrypted SQLite/PostgreSQL/encrypted PostgreSQL, deep dependency planning,
real HTTP settlement/replanning, and a real-worker TypeScript preview regression.

This fixes hazards for evidence observed by the plan. Source-forward fencing,
reconciliation of receipts created after the snapshot and immutable per-invocation
parameter bindings remain separate lifecycle work; no broader compensation safety
claim is made here.
