-- One retained entry may consume each chain predecessor. This turns
-- concurrent appends into an explicit CAS conflict instead of silently
-- creating an unverifiable fork. The empty string is reserved for genesis.
CREATE UNIQUE INDEX IF NOT EXISTS uq_provenance_chain_predecessor
    ON provenance_entries (
        tenant_id,
        continuity_id,
        COALESCE(previous_sha256, '')
    );
CREATE INDEX IF NOT EXISTS idx_provenance_chain_predecessor_lookup
    ON provenance_entries (tenant_id, continuity_id, previous_sha256);
