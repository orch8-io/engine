-- Undo the instance and ambiguous-effect retention lookup indexes.
DROP INDEX IF EXISTS idx_effect_receipts_ambiguous_gc;
DROP INDEX IF EXISTS idx_effect_receipts_instance_gc;
