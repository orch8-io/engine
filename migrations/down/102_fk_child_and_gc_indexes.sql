-- Revert the FK-child / GC indexes.
DROP INDEX IF EXISTS idx_continuity_locations_continuity_id;
DROP INDEX IF EXISTS idx_provenance_entries_continuity_id;
DROP INDEX IF EXISTS idx_effect_receipts_continuity_id;
DROP INDEX IF EXISTS idx_execution_capsules_continuity_id;
DROP INDEX IF EXISTS idx_execution_handoffs_continuity_id;
DROP INDEX IF EXISTS idx_budget_reservations_continuity;
DROP INDEX IF EXISTS idx_attention_tasks_continuity;
DROP INDEX IF EXISTS idx_continuity_stream_frames_continuity;
DROP INDEX IF EXISTS idx_continuation_grants_continuity;
DROP INDEX IF EXISTS idx_execution_capsules_continuity;
DROP INDEX IF EXISTS idx_task_instances_terminal_updated;
DROP INDEX IF EXISTS idx_usage_events_instance;
DROP INDEX IF EXISTS idx_emit_event_dedupe_child;
DROP INDEX IF EXISTS idx_signal_inbox_instance;
