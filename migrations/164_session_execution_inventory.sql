CREATE INDEX IF NOT EXISTS idx_task_instances_session_page ON task_instances(tenant_id, session_id, id DESC) WHERE session_id IS NOT NULL;
