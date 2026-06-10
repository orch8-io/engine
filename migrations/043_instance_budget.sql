-- Optional per-instance budget: token/step caps enforced by the scheduler.
-- NULL means "no budget" — the scheduler skips all budget bookkeeping.
-- Shape mirrors orch8_types::instance::Budget:
--   { "max_input_tokens": N, "max_output_tokens": N,
--     "max_total_tokens": N, "max_steps": N }   (all fields optional)
ALTER TABLE task_instances ADD COLUMN IF NOT EXISTS budget JSONB;
