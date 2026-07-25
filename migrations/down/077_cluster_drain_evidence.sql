-- Roll back auditable cluster drain lifecycle columns.
ALTER TABLE cluster_nodes
    DROP COLUMN IF EXISTS execution_handoff_evidence,
    DROP COLUMN IF EXISTS capabilities_withdrawn,
    DROP COLUMN IF EXISTS stopped_at,
    DROP COLUMN IF EXISTS drain_started_at;
