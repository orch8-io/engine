-- Revert the embed sequence-ownership index.
DROP INDEX IF EXISTS idx_sequences_owner_sub_tenant;
