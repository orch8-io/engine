-- Revert provenance predecessor CAS indexes.
DROP INDEX IF EXISTS idx_provenance_chain_predecessor_lookup;
DROP INDEX IF EXISTS uq_provenance_chain_predecessor;
