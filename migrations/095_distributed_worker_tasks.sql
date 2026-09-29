-- Distributed execution (runtime nodes: server, mobile, browser, desktop).
--
-- effect_id            deterministic effect-receipt id fixed at dispatch; the
--                      server settles by this stored id (never a recomputed
--                      one, which would miss after an ownership handoff).
-- continuity_epoch     owner epoch at dispatch; lease mutations are fenced on
--                      it so a task dispatched under an old owner cannot
--                      complete after a handoff (NULL = not enrolled).
-- lease_secs           per-claim lease chosen from the claimant's runtime kind
--                      (browser 30s, mobile 120s); NULL = server default.
-- carries_credentials  params referenced credentials:// material; never
--                      claimable by a browser runtime.
-- claimed_runtime_kind claimant kind, for output bounds and provenance.
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS effect_id UUID;
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS continuity_epoch BIGINT
    CHECK (continuity_epoch >= 0);
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS lease_secs INTEGER
    CHECK (lease_secs > 0);
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS carries_credentials BOOLEAN
    NOT NULL DEFAULT FALSE;
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS claimed_runtime_kind TEXT;

-- Handoff fencing: export refuses while an instance has open worker tasks,
-- and the scheduler resolves an instance to the continuity execution whose
-- location history contains it.
CREATE INDEX IF NOT EXISTS idx_worker_tasks_instance_state
    ON worker_tasks (instance_id, state);
CREATE INDEX IF NOT EXISTS idx_continuity_locations_instance
    ON continuity_locations (tenant_id, instance_id);
