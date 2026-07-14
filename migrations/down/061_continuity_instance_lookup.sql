-- Revert continuity instance and unresolved-effect lookup indexes.
DROP INDEX IF EXISTS idx_effect_receipts_unresolved_instance;
DROP INDEX IF EXISTS idx_continuity_executions_current_instance;
