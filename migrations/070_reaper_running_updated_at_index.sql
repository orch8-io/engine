-- Partial index for the stale-instance reaper's running-side scan. The
-- reaper sweeps `WHERE state = 'running' AND updated_at < cutoff` on every
-- pass (threshold/2); without this index the query degrades to a full table
-- scan as instance count grows. Mirrors 032's waiting-side style.
--
-- Note on 032's `idx_task_instances_waiting_updated`: it does NOT become
-- dead now that the reaper excludes `waiting` (parked waiters never
-- heartbeat, so the lease sweep churned them every pass — their liveness
-- contract is the deadline/timeout sweep). The waiting-deadline sweep still
-- pages `WHERE state = 'waiting' ORDER BY updated_at`, which that index
-- backs. Left in place regardless — shipped migrations are immutable.
CREATE INDEX IF NOT EXISTS idx_task_instances_running_updated
    ON task_instances(updated_at) WHERE state = 'running';
