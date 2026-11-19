# Mobile, worker, and sequence adoption implementation plan

The roadmap in the adoption analysis defines the 50 requested features. Each feature needs an implementation on its normal entry point and a behavioral test. Existing features may satisfy part of the scope when their behavior is directly verified.

## Mobile execution, features 1–17

- Verify the inspection starter by installing published artifacts, capturing offline data, recreating the engine, and confirming a subsequent sync. Test incompatible pins, package ABI, account isolation, pending sync, and acknowledgement.
- Verify lifecycle adapters and diagnostics against start, stop, overlapping windows, restored work, and missed wakes. Exercise approval submission and rejection on duplicate or expired requests.
- Exercise persistent upload retries, conflict handling, and data visibility with interrupted connections. Verify signatures, scope, expiry, and cohort membership before installing or switching catalogs.
- Test permission denial, offline failures, and account switches on the native entry points. Record simulator and real-device evidence separately.

## Distributed execution, features 18–34

- Test placement explanations, recipes, readiness, and forecasts with a matching fleet, stale workers, missing capabilities, and policy-denied targets.
- Test the sandbox and starter workers with a real claim, deadline, checkpoint, acknowledgement, and redacted export. Verify documented wait states against structured runtime evidence.
- Exercise enrollment, drain, fallback, unknown-effect review, and version canaries against lease and effect rules. Verify that a policy change or worker reconnect preserves safety.
- Keep the scoped delegation preview gated on claim negotiation, exact frozen-definition delivery, local scope ownership, and acknowledged outcomes.

## Sequences, features 35–50

- Run authoring-format fixtures through the existing offline contract runner. Cover observed and unobserved nested steps, ordering, retries, approval timeout, cancellation, and compensation.
- Exercise the editor's preflight, setup, binding suggestions, generated schemas, human forms, dependency versions, runtime data boundaries, revision checks, and replay divergence.
- Compile generated handler code against its dataflow types. Run checked-in recipe contracts and import parity fixtures. Reject ambiguous source-node mappings and keep existing artifacts intact on invalid input.
- Bind a target release to a signed package member before any server-side release mutation. Test matching content with environment-specific IDs and rejection when the target differs.

Run Cargo tests for changed crates, the dashboard typecheck, build and tests, and package-level checks. Keep release artifacts and real-device results separate from local fixture evidence.
