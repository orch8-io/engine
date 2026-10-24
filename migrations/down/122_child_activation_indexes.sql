-- Undo indexed current-activation and legacy-block child lookups.
DROP INDEX IF EXISTS idx_child_legacy_block;
DROP INDEX IF EXISTS idx_child_activation;
