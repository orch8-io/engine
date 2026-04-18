# Workflow Validation: Prevent Infinite-Loop Sequences

**Status:** TODO (future work)
**Owner:** unassigned
**Filed:** 2026-04-18

## Context

`orch8-engine/src/handlers/loop_block.rs` now enforces a hard runtime cap
via `LOOP_ABSOLUTE_MAX` (currently `1_000_000`). This cap is a **last line
of defense** — it protects the scheduler against a misconfigured sequence
or a regression in iteration tracking, but it is *not* the right place to
catch bad workflows.

A sequence that hits `LOOP_ABSOLUTE_MAX` in production is already bad:
the engine has burned one million evaluator dispatches on a single loop
before giving up. That is millions of DB round-trips, likely a saturated
worker, and a user instance that looks busy but makes no useful progress.

## The Gap

There is currently **no submit-time validation** of sequences for
termination properties. Any of the following can be POSTed to
`/v1/sequences` today and be accepted:

1. `loop { condition: "true", max_iterations: u32::MAX }` — will run for
   `LOOP_ABSOLUTE_MAX` iterations before the runtime guard trips.
2. `loop { condition: "true" }` with no cap — same as (1) via the
   default.
3. `loop` whose condition references a field the body never mutates, so
   in practice it can only exit via the cap.
4. `for_each` with an unbounded collection expression (e.g. resolving
   from a field a worker can keep appending to).
5. Mutually-referential `sub_sequence` chains (A calls B calls A).

None of these should survive `createSequence`.

## What Good Looks Like

Reject at submit time (HTTP 400 with a specific error code) any sequence
that:

- Declares a `loop` without a finite `max_iterations`, OR with one
  exceeding a policy-level ceiling (e.g. `100_000` — two orders of
  magnitude below the runtime `LOOP_ABSOLUTE_MAX`).
- Declares a `loop` whose `condition` expression does not reference at
  least one context field that the body could plausibly mutate. (This
  is a heuristic, not a proof — but it catches the common mistake of
  `condition: "true"` combined with a missing cap.)
- Contains a `sub_sequence` cycle (A → B → A), detected via a DAG walk
  of cross-sequence references at publish time.
- Has a `for_each` whose `collection` resolves to something unbounded
  (needs a per-handler capability declaration or a run-time cap on
  collection length in `for_each.rs`).

## Implementation Sketch

New module: `orch8-engine/src/validation.rs` (or
`orch8-types/src/validation.rs` if it should be reusable by CLI tools).

```rust
pub struct ValidationError {
    pub block_id: BlockId,
    pub kind: ValidationErrorKind,
}

pub enum ValidationErrorKind {
    LoopNoMaxIterations,
    LoopMaxIterationsTooHigh { requested: u32, ceiling: u32 },
    LoopConditionNeverMutated { field: String },
    SubSequenceCycle { path: Vec<String> },
    ForEachUnboundedCollection,
}

pub fn validate_sequence(seq: &SequenceDefinition) -> Vec<ValidationError> {
    // Walk blocks, apply each rule, collect errors.
}
```

Wire into `orch8-api/src/sequences.rs::create_sequence` before
`storage.create_sequence`. Return 400 with a structured list of
violations so the UI can surface them inline.

Policy constants live in `orch8-types/src/policy.rs`:

```rust
pub const MAX_LOOP_ITERATIONS_ACCEPTED: u32 = 100_000;
pub const MAX_FOR_EACH_COLLECTION_SIZE: usize = 10_000;
```

## Relationship to the Runtime Guard

| Layer | Purpose | Limit |
|-------|---------|-------|
| Submit-time validation | Catch bad design, fail fast with a clear error | 100k |
| Runtime loop counter | Enforce the declared cap per-iteration | user config |
| `LOOP_ABSOLUTE_MAX` | Last-resort safety net, should never trip in prod | 1M |

The runtime guard stays at 1M deliberately so that a *rare* legitimate
use case (e.g. a batch processing loop over 500k records) is still
expressible without a config knob — but the validation layer forces the
user to *choose* a number, rather than implicitly inheriting the cap.

## Acceptance Criteria

1. POSTing a sequence with `loop { condition: "true", max_iterations: 0 }`
   returns HTTP 400 with `LoopMaxIterationsTooHigh` or
   `LoopConditionNeverMutated`.
2. POSTing a sequence with `loop { condition: "true" }` (missing
   max_iterations) returns HTTP 400 with `LoopNoMaxIterations`.
3. An existing passing sequence continues to pass validation.
4. Unit tests in `validation.rs` cover each `ValidationErrorKind`.
5. Integration test: create-sequence rejects each bad shape with the
   documented error code.

## Out of Scope (for now)

- Static analysis of context mutations — proving a loop body *cannot*
  flip its condition is undecidable in general, so we only do the
  "references at least one mutable field" heuristic.
- Runtime telemetry: separate follow-up to emit a metric
  `orch8_loop_absolute_max_reached_total` so an alert fires if the
  runtime guard ever trips in prod (indicating either a validation
  bypass or a genuinely bad workflow that escaped review).
