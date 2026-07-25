-- Auditable graceful fleet draining. Stale reaping records stopped_at but
-- deliberately cannot claim scheduler/handoff completion evidence.
ALTER TABLE cluster_nodes
    ADD COLUMN IF NOT EXISTS drain_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS stopped_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS capabilities_withdrawn BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS execution_handoff_evidence TEXT;
