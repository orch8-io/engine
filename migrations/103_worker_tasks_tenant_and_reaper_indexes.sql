-- L4-03 / L4-04 / L4-05: worker_tasks indexes for the tenant-scoped poll,
-- the timeout reaper, the stranded-dispatch sweep and the task list.
--
-- * tenant_id (denormalised from task_instances): a tenant-scoped poll used
--   to walk every other tenant's pending tasks for the same handler (one PK
--   probe into task_instances per row) because the tenant predicate could
--   only be applied after the join. New rows carry the tenant from the
--   insert itself; in-flight rows are backfilled here. Rows inserted by an
--   older binary during a rolling deploy are filled lazily by the poll
--   (see idx_worker_tasks_untenanted_pending). Terminal history keeps NULL —
--   nothing reads tenant_id from it. Nullable, no default: metadata-only ADD.
-- * The timeout reaper (`state IN ('pending','claimed') AND timeout_ms IS NOT
--   NULL`) and stranded-dispatch sweep seq-scanned the whole table (retained
--   history included) every 30 s on every node; both partial indexes hold
--   only in-flight rows.
-- * created_at DESC: the unfiltered /workers/tasks list sorted every row.
--
-- The backfill UPDATE touches only pending/claimed rows (the live backlog).
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create the indexes online before upgrading:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_worker_tasks_inflight_created
--       ON worker_tasks (created_at)
--       WHERE state IN ('pending', 'claimed') AND timeout_ms IS NOT NULL;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_worker_tasks_stranded
--       ON worker_tasks (created_at) WHERE state = 'pending' AND awaiting_dispatch;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_worker_tasks_created
--       ON worker_tasks (created_at DESC, id DESC);
--   (the two tenant indexes need the column, so they are cheap to build after
--   the ALTER: they only cover pending rows)
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS tenant_id TEXT;

UPDATE worker_tasks wt
SET tenant_id = ti.tenant_id
FROM task_instances ti
WHERE ti.id = wt.instance_id
  AND wt.tenant_id IS NULL
  AND wt.state IN ('pending', 'claimed');

CREATE INDEX IF NOT EXISTS idx_worker_tasks_tenant_poll
    ON worker_tasks (tenant_id, handler_name, created_at)
    WHERE state = 'pending' AND NOT awaiting_dispatch;
CREATE INDEX IF NOT EXISTS idx_worker_tasks_untenanted_pending
    ON worker_tasks (handler_name)
    WHERE state = 'pending' AND tenant_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_worker_tasks_inflight_created
    ON worker_tasks (created_at)
    WHERE state IN ('pending', 'claimed') AND timeout_ms IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_worker_tasks_stranded
    ON worker_tasks (created_at)
    WHERE state = 'pending' AND awaiting_dispatch;
CREATE INDEX IF NOT EXISTS idx_worker_tasks_created
    ON worker_tasks (created_at DESC, id DESC);
