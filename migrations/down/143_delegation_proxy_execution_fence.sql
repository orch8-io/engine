-- Remove the native relay fence only when no relay identity remains.
LOCK TABLE task_instances,delegation_publications,delegation_proxy_quarantines IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed'
    OR EXISTS(SELECT 1 FROM delegation_proxy_instances) THEN
  RAISE EXCEPTION 'retained delegation relays require the current execution fence';
 END IF;
END $$;
DROP TRIGGER delegation_proxy_quarantine_capture_insert ON task_instances;
DROP TRIGGER delegation_proxy_quarantine_capture_delete ON task_instances;
DROP FUNCTION capture_delegation_proxy_quarantine();
DROP TRIGGER delegation_proxy_quarantine_immutable ON delegation_proxy_quarantines;
DROP FUNCTION protect_delegation_proxy_quarantine();
DROP TRIGGER delegation_proxy_instance_fence ON task_instances;
DROP FUNCTION fence_delegation_proxy_instance();
DROP VIEW delegation_proxy_native_history;
DROP VIEW delegation_proxy_instances;
DROP INDEX idx_delegation_publications_proxy_instances;
DROP INDEX idx_task_instances_delegation_proxy;

DROP TABLE delegation_proxy_quarantines;
