-- L4-01: the scheduler claim orders by `priority DESC, next_fire_at ASC
-- NULLS FIRST` (scheduled rows with no fire time are due now), but the only
-- claim index sorted NULLS LAST, so every tick on every node read and sorted
-- every `scheduled` row — including far-future timers — to return LIMIT N.
-- This index matches the ORDER BY exactly, turning the claim (and
-- `has_due_higher_priority`, and the tick_once pending probe) into an index
-- range scan that stops after LIMIT rows.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md): the plain
-- build blocks writes to task_instances while it runs. On large installs
-- pre-create it online before upgrading and this statement becomes a no-op:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_instances_claim_due
--       ON task_instances (priority DESC, next_fire_at ASC NULLS FIRST)
--       WHERE state = 'scheduled';
CREATE INDEX IF NOT EXISTS idx_instances_claim_due
    ON task_instances (priority DESC, next_fire_at ASC NULLS FIRST)
    WHERE state = 'scheduled';

-- Superseded: `(state, priority DESC, next_fire_at ASC)` under the same
-- predicate is a strict subset of the index above in usefulness (its NULLS
-- LAST order never matched the claim), and costs a write on every claim.
DROP INDEX IF EXISTS idx_instances_claim_priority;
