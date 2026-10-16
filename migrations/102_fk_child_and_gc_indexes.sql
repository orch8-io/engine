-- L4-02 / L4-10 / L1-05: indexes that keep instance deletion and the GC
-- sweeps proportional to the rows they touch, not to table size.
--
-- * signal_inbox(instance_id): the only index was partial
--   (`WHERE delivered = FALSE`), so the ON DELETE CASCADE trigger fired by
--   deleting a task_instances row seq-scanned every delivered signal ever
--   received. One GC batch then outlived statement_timeout and retention
--   stopped for good.
-- * emit_event_dedupe(child_instance_id): FK (031) with no index — same
--   per-row seq scan on every instance delete.
-- * usage_events(instance_id, kind) INCLUDE tokens: the explicit purge in
--   delete_terminal_instances and the per-step token-budget aggregate
--   (query_instance_usage_totals) scanned the whole table.
-- * task_instances(updated_at) WHERE terminal: the GC candidate queries
--   (`state IN terminal AND updated_at < cutoff ORDER BY updated_at`) had no
--   matching index and read + sorted every terminal row each 5 min per node.
-- * (tenant_id, continuity_id) on the continuity child tables that lacked a
--   leading index, so the retention purge of a continuity (L1-01) and the FK
--   checks it triggers stay indexed.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create them online before upgrading (each statement below then
-- becomes a no-op):
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signal_inbox_instance
--       ON signal_inbox (instance_id);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_emit_event_dedupe_child
--       ON emit_event_dedupe (child_instance_id);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usage_events_instance
--       ON usage_events (instance_id, kind) INCLUDE (input_tokens, output_tokens);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_instances_terminal_updated
--       ON task_instances (updated_at)
--       WHERE state IN ('completed', 'failed', 'cancelled');
--   (and the continuity indexes below, verbatim with CONCURRENTLY)
CREATE INDEX IF NOT EXISTS idx_signal_inbox_instance
    ON signal_inbox (instance_id);
CREATE INDEX IF NOT EXISTS idx_emit_event_dedupe_child
    ON emit_event_dedupe (child_instance_id);
CREATE INDEX IF NOT EXISTS idx_usage_events_instance
    ON usage_events (instance_id, kind) INCLUDE (input_tokens, output_tokens);
CREATE INDEX IF NOT EXISTS idx_task_instances_terminal_updated
    ON task_instances (updated_at)
    WHERE state IN ('completed', 'failed', 'cancelled');

CREATE INDEX IF NOT EXISTS idx_execution_capsules_continuity
    ON execution_capsules (tenant_id, continuity_id);
CREATE INDEX IF NOT EXISTS idx_continuation_grants_continuity
    ON continuation_grants (tenant_id, continuity_id);
CREATE INDEX IF NOT EXISTS idx_continuity_stream_frames_continuity
    ON continuity_stream_frames (tenant_id, continuity_id);
CREATE INDEX IF NOT EXISTS idx_attention_tasks_continuity
    ON attention_tasks (tenant_id, continuity_id);
CREATE INDEX IF NOT EXISTS idx_budget_reservations_continuity
    ON budget_reservations (tenant_id, continuity_id);
-- Single-column FKs to continuity_executions(continuity_id): the RI check
-- run when a continuity row is deleted looks up by continuity_id alone.
CREATE INDEX IF NOT EXISTS idx_execution_handoffs_continuity_id
    ON execution_handoffs (continuity_id);
CREATE INDEX IF NOT EXISTS idx_execution_capsules_continuity_id
    ON execution_capsules (continuity_id);
CREATE INDEX IF NOT EXISTS idx_effect_receipts_continuity_id
    ON effect_receipts (continuity_id);
CREATE INDEX IF NOT EXISTS idx_provenance_entries_continuity_id
    ON provenance_entries (continuity_id);
CREATE INDEX IF NOT EXISTS idx_continuity_locations_continuity_id
    ON continuity_locations (continuity_id);
