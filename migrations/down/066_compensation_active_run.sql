-- Revert the one-active-compensation-run constraint.
DROP INDEX IF EXISTS idx_compensation_runs_active;
