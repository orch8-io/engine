-- Multi-row block_outputs: each execution of a block is recorded as its own
-- row, not upserted on (instance_id, block_id). This lets loop and for_each
-- iterations preserve an observable history (N iterations → N rows for the
-- body block_id), which matches the semantics already implemented by the
-- sqlite backend (no UNIQUE constraint there).
--
-- Reads that want "the current state of block X for instance Y" must order
-- by created_at DESC LIMIT 1. The accompanying composite index keeps that
-- query cheap without the uniqueness guarantee.
--
-- Callers that previously relied on the UNIQUE constraint to dedupe (e.g.
-- retry overwrite) now rely on the handler/scheduler to compute the correct
-- attempt number and the "latest" lookup to return the right row. See
-- orch8-storage/src/postgres/outputs.rs and orch8-engine/src/handlers/step.rs.

ALTER TABLE block_outputs
    DROP CONSTRAINT IF EXISTS block_outputs_instance_id_block_id_key;

CREATE INDEX IF NOT EXISTS idx_block_outputs_instance_block_created
    ON block_outputs (instance_id, block_id, created_at DESC);
